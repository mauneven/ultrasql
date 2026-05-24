//! Runtime support for `CREATE AGGREGATING INDEX`.
//!
//! The summary itself is a runtime sidecar: DML marks it dirty, commit/VACUUM
//! maintenance rebuilds dirty summaries from visible heap rows, and matching
//! reads keep the same rebuild path as a correctness backstop. Empty groups
//! are omitted because summary rows are derived only from currently visible
//! base-table tuples. Durable catalog metadata records the exact
//! group/aggregate shape, then restart rebuilds clean summary rows from the
//! heap rather than trusting same-process-only state.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ultrasql_catalog::{IndexEntry, TableEntry};
use ultrasql_core::{DataType, RelationId, Schema, Value, bpchar_semantic_text};
use ultrasql_executor::{Eval, Operator, RowCodec, Sort, ValuesScan};
use ultrasql_mvcc::Snapshot;
use ultrasql_planner::{
    AggregateFunc, BinaryOp, LogicalAggregateExpr, LogicalAggregatingIndex,
    LogicalAggregatingIndexExpr, LogicalPlan, ScalarExpr, SortKey,
};
use ultrasql_txn::TransactionManager;

use crate::error::ServerError;
use crate::pipeline::LowerCtx;
use crate::{BlankPageLoader, RuntimeAggregatingIndex};

const CATALOG_VERSION: &str = "1";
const OPTION_SOURCE_TABLE_OID: &str = "aggregating.source_table_oid";
const OPTION_INDEX_OID: &str = "aggregating.index_oid";
const OPTION_GROUP_COLUMNS: &str = "aggregating.group_columns";
const OPTION_AGGREGATES: &str = "aggregating.aggregates";
const OPTION_STALE: &str = "aggregating.stale";
const OPTION_VERSION: &str = "aggregating.version";
const OPTION_DURABLE_STATE: &str = "aggregating.durable_state";

#[derive(Clone, Debug)]
struct AggregateState {
    func: AggregateFunc,
    int_sum: i64,
    float_sum: f64,
    float_output: bool,
    count: i64,
    saw_value: bool,
}

impl AggregateState {
    const fn new(spec: &LogicalAggregatingIndexExpr) -> Self {
        Self {
            func: spec.func,
            int_sum: 0,
            float_sum: 0.0,
            float_output: matches!(spec.data_type, DataType::Float64 | DataType::Float32),
            count: 0,
            saw_value: false,
        }
    }

    fn add(
        &mut self,
        spec: &LogicalAggregatingIndexExpr,
        row: &[Value],
    ) -> Result<(), ServerError> {
        match self.func {
            AggregateFunc::CountStar => {
                self.count = self.count.checked_add(1).ok_or_else(|| {
                    ServerError::ddl("aggregating index count overflow".to_owned())
                })?;
            }
            AggregateFunc::Sum => {
                let Some(col) = spec.arg_column else {
                    return Err(ServerError::ddl(
                        "aggregating index sum missing argument column".to_owned(),
                    ));
                };
                let value = row.get(col).ok_or_else(|| {
                    ServerError::ddl(format!("aggregating index row missing column {col}"))
                })?;
                match value {
                    Value::Null => {}
                    Value::Int16(v) => self.add_i64(i64::from(*v))?,
                    Value::Int32(v) => self.add_i64(i64::from(*v))?,
                    Value::Int64(v) => self.add_i64(*v)?,
                    Value::Float32(v) => self.add_f64(f64::from(*v)),
                    Value::Float64(v) => self.add_f64(*v),
                    other => {
                        return Err(ServerError::ddl(format!(
                            "aggregating index sum expected numeric input, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            _ => {
                return Err(ServerError::Unsupported(
                    "aggregating index runtime supports sum and count(*) in this wave",
                ));
            }
        }
        Ok(())
    }

    fn add_i64(&mut self, value: i64) -> Result<(), ServerError> {
        if self.float_output {
            let as_float = value.to_string().parse::<f64>().map_err(|e| {
                ServerError::ddl(format!("aggregating index i64 to f64 conversion: {e}"))
            })?;
            self.add_f64(as_float);
            return Ok(());
        }
        self.int_sum = self
            .int_sum
            .checked_add(value)
            .ok_or_else(|| ServerError::ddl("aggregating index sum overflow".to_owned()))?;
        self.saw_value = true;
        Ok(())
    }

    fn add_f64(&mut self, value: f64) {
        self.float_sum += value;
        self.saw_value = true;
    }

    fn finish(&self) -> Value {
        match self.func {
            AggregateFunc::CountStar => Value::Int64(self.count),
            AggregateFunc::Sum if !self.saw_value => Value::Null,
            AggregateFunc::Sum if self.float_output => Value::Float64(self.float_sum),
            AggregateFunc::Sum => Value::Int64(self.int_sum),
            _ => Value::Null,
        }
    }
}

/// Build summary rows for an aggregating index from visible heap tuples.
pub(crate) fn build_aggregating_index_rows(
    table: &TableEntry,
    spec: &LogicalAggregatingIndex,
    heap: &ultrasql_storage::heap::HeapAccess<BlankPageLoader>,
    snapshot: &Snapshot,
    oracle: &TransactionManager,
) -> Result<Vec<Vec<Value>>, ServerError> {
    let relation = RelationId(table.oid);
    let block_count = heap.block_count(relation).max(table.n_blocks);
    let codec = RowCodec::new(table.schema.clone());
    let mut groups: HashMap<Vec<Value>, Vec<AggregateState>> = HashMap::new();

    for tuple in heap.scan_visible(relation, block_count, snapshot, oracle) {
        let tuple =
            tuple.map_err(|e| ServerError::ddl(format!("aggregating index heap scan: {e}")))?;
        let row = codec
            .decode(&tuple.data)
            .map_err(|e| ServerError::ddl(format!("aggregating index decode: {e}")))?;
        let mut key = Vec::with_capacity(spec.group_columns.len());
        for &col in &spec.group_columns {
            key.push(row.get(col).cloned().ok_or_else(|| {
                ServerError::ddl(format!("aggregating index row missing group column {col}"))
            })?);
        }
        let states = groups.entry(key).or_insert_with(|| {
            spec.aggregates
                .iter()
                .map(AggregateState::new)
                .collect::<Vec<_>>()
        });
        for (state, aggregate) in states.iter_mut().zip(&spec.aggregates) {
            state.add(aggregate, &row)?;
        }
    }

    let mut rows = groups
        .into_iter()
        .map(|(mut key, states)| {
            key.extend(states.iter().map(AggregateState::finish));
            key
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| display_key(row));
    Ok(rows)
}

/// Encode the durable metadata needed to rebuild an aggregating-index
/// runtime sidecar from catalog + heap rows after restart.
pub(crate) fn catalog_options_for_aggregating_index(
    spec: &LogicalAggregatingIndex,
    source_table_oid: ultrasql_core::Oid,
    index_oid: ultrasql_core::Oid,
) -> Vec<(String, String)> {
    vec![
        (
            OPTION_SOURCE_TABLE_OID.to_owned(),
            source_table_oid.raw().to_string(),
        ),
        (OPTION_INDEX_OID.to_owned(), index_oid.raw().to_string()),
        (
            OPTION_GROUP_COLUMNS.to_owned(),
            spec.group_columns
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            OPTION_AGGREGATES.to_owned(),
            spec.aggregates
                .iter()
                .map(encode_aggregate_spec)
                .collect::<Vec<_>>()
                .join(";"),
        ),
        (OPTION_STALE.to_owned(), "false".to_owned()),
        (OPTION_VERSION.to_owned(), CATALOG_VERSION.to_owned()),
        (
            OPTION_DURABLE_STATE.to_owned(),
            "rebuild_on_restart".to_owned(),
        ),
    ]
}

/// Decode durable aggregating-index metadata from `pg_index.indoptions`.
pub(crate) fn aggregating_index_spec_from_catalog(
    table: &TableEntry,
    index: &IndexEntry,
) -> Result<Option<LogicalAggregatingIndex>, ServerError> {
    if index.access_method != "aggregating" {
        return Ok(None);
    }
    let version = required_option(index, OPTION_VERSION)?;
    if version != CATALOG_VERSION {
        return Err(ServerError::ddl(format!(
            "aggregating index {} has unsupported metadata version {version}",
            index.name
        )));
    }
    let source_oid = parse_u32_option(index, OPTION_SOURCE_TABLE_OID)?;
    if source_oid != table.oid.raw() {
        return Err(ServerError::ddl(format!(
            "aggregating index {} source table oid {} does not match table oid {}",
            index.name,
            source_oid,
            table.oid.raw()
        )));
    }
    let index_oid = parse_u32_option(index, OPTION_INDEX_OID)?;
    if index_oid != index.oid.raw() {
        return Err(ServerError::ddl(format!(
            "aggregating index {} catalog index oid {} does not match index oid {}",
            index.name,
            index_oid,
            index.oid.raw()
        )));
    }
    let group_columns = parse_group_columns(required_option(index, OPTION_GROUP_COLUMNS)?)?;
    let index_columns = index
        .columns
        .iter()
        .map(|col| usize::from(*col))
        .collect::<Vec<_>>();
    if group_columns != index_columns {
        return Err(ServerError::ddl(format!(
            "aggregating index {} group columns {:?} do not match index columns {:?}",
            index.name, group_columns, index_columns
        )));
    }
    let aggregates = parse_aggregates(table, required_option(index, OPTION_AGGREGATES)?)?;
    if aggregates.is_empty() {
        return Err(ServerError::ddl(format!(
            "aggregating index {} has no aggregate metadata",
            index.name
        )));
    }
    Ok(Some(LogicalAggregatingIndex {
        group_columns,
        aggregates,
    }))
}

fn encode_aggregate_spec(spec: &LogicalAggregatingIndexExpr) -> String {
    match spec.func {
        AggregateFunc::Sum => format!(
            "sum:{}",
            spec.arg_column
                .map(|col| col.to_string())
                .unwrap_or_else(|| "*".to_owned())
        ),
        AggregateFunc::CountStar => "count:*".to_owned(),
        other => format!("{other:?}:*").to_ascii_lowercase(),
    }
}

fn required_option<'a>(index: &'a IndexEntry, name: &str) -> Result<&'a str, ServerError> {
    index
        .options
        .iter()
        .find_map(|(key, value)| (key == name).then_some(value.as_str()))
        .ok_or_else(|| {
            ServerError::ddl(format!(
                "aggregating index {} missing catalog option {name}",
                index.name
            ))
        })
}

fn parse_u32_option(index: &IndexEntry, name: &str) -> Result<u32, ServerError> {
    required_option(index, name)?.parse::<u32>().map_err(|e| {
        ServerError::ddl(format!(
            "aggregating index {} invalid catalog option {name}: {e}",
            index.name
        ))
    })
}

fn parse_group_columns(raw: &str) -> Result<Vec<usize>, ServerError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    raw.split(',')
        .map(|part| {
            part.parse::<usize>().map_err(|e| {
                ServerError::ddl(format!(
                    "aggregating index invalid group column {part}: {e}"
                ))
            })
        })
        .collect()
}

fn parse_aggregates(
    table: &TableEntry,
    raw: &str,
) -> Result<Vec<LogicalAggregatingIndexExpr>, ServerError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    raw.split(';')
        .map(|part| parse_aggregate(table, part))
        .collect()
}

fn parse_aggregate(
    table: &TableEntry,
    raw: &str,
) -> Result<LogicalAggregatingIndexExpr, ServerError> {
    let (func, arg) = raw.split_once(':').ok_or_else(|| {
        ServerError::ddl(format!(
            "aggregating index invalid aggregate metadata {raw}"
        ))
    })?;
    match (func, arg) {
        ("count", "*") => Ok(LogicalAggregatingIndexExpr {
            func: AggregateFunc::CountStar,
            arg_column: None,
            output_name: "count".to_owned(),
            data_type: DataType::Int64,
        }),
        ("sum", arg) => {
            let col = arg.parse::<usize>().map_err(|e| {
                ServerError::ddl(format!("aggregating index invalid sum column {arg}: {e}"))
            })?;
            let field = table.schema.field(col).ok_or_else(|| {
                ServerError::ddl(format!("aggregating index sum column {col} missing"))
            })?;
            if !field.data_type.is_numeric() {
                return Err(ServerError::ddl(format!(
                    "aggregating index sum({}) requires numeric input, got {}",
                    field.name, field.data_type
                )));
            }
            let data_type = match field.data_type {
                DataType::Float32 | DataType::Float64 => DataType::Float64,
                _ => DataType::Int64,
            };
            Ok(LogicalAggregatingIndexExpr {
                func: AggregateFunc::Sum,
                arg_column: Some(col),
                output_name: format!("sum({})", field.name.to_ascii_lowercase()),
                data_type,
            })
        }
        _ => Err(ServerError::ddl(format!(
            "aggregating index unsupported aggregate metadata {raw}"
        ))),
    }
}

fn display_key(row: &[Value]) -> String {
    row.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

/// Mark every aggregating index on `entry` stale.
pub(crate) fn mark_aggregating_indexes_dirty(entry: &TableEntry, ctx: &LowerCtx<'_>) {
    let Some(constraints) = ctx.table_constraints.get(&entry.oid) else {
        return;
    };
    for metadata in constraints.indexes.values() {
        if let Some(runtime) = &metadata.aggregating {
            runtime.mark_dirty();
        }
    }
}

/// Rebuild dirty aggregating summaries for a table.
pub(crate) fn refresh_dirty_aggregating_indexes(
    table: &TableEntry,
    table_constraints: &dashmap::DashMap<ultrasql_core::Oid, Arc<crate::TableRuntimeConstraints>>,
    heap: &ultrasql_storage::heap::HeapAccess<BlankPageLoader>,
    snapshot: &Snapshot,
    oracle: &TransactionManager,
) -> Result<(), ServerError> {
    let Some(constraints) = table_constraints.get(&table.oid) else {
        return Ok(());
    };
    let runtimes = constraints
        .indexes
        .values()
        .filter_map(|metadata| metadata.aggregating.clone())
        .collect::<Vec<_>>();
    drop(constraints);
    for runtime in runtimes {
        rebuild_runtime_if_dirty(table, &runtime, heap, snapshot, oracle)?;
    }
    Ok(())
}

/// Try to lower `Project(Aggregate(...))` through a matching runtime summary.
pub(crate) fn try_lower_aggregating_index_project(
    input: &LogicalPlan,
    exprs: &[(ScalarExpr, String)],
    schema: &Schema,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let (input, sort_keys) = match input {
        LogicalPlan::Sort { input, keys } => {
            if !sort_keys_compatible_with_project(keys, exprs) {
                return Ok(None);
            }
            (input.as_ref(), Some(keys))
        }
        other => (other, None),
    };
    let LogicalPlan::Aggregate {
        input: aggregate_input,
        group_by,
        aggregates,
        ..
    } = input
    else {
        return Ok(None);
    };
    let Some((table_name, predicate)) = aggregate_input_scan(aggregate_input) else {
        return Ok(None);
    };
    let Some(table) = ctx
        .catalog_snapshot
        .tables
        .get(&table_name.to_ascii_lowercase())
    else {
        return Ok(None);
    };
    let Some((_index_name, runtime)) = matching_aggregating_index(table, group_by, aggregates, ctx)
    else {
        return Ok(None);
    };
    let Some(filtered_rows) = current_summary_rows(table, &runtime, predicate, ctx)? else {
        return Ok(None);
    };

    let mut projected = Vec::with_capacity(filtered_rows.len());
    for row in filtered_rows {
        let mut out = Vec::with_capacity(exprs.len());
        for (expr, _alias) in exprs {
            let value = Eval::new(expr.clone())
                .eval(&row)
                .map_err(|e| ServerError::ddl(format!("aggregating index projection: {e}")))?;
            out.push(ScalarExpr::Literal {
                data_type: value.data_type(),
                value,
            });
        }
        projected.push(out);
    }

    let scan: Box<dyn Operator> = Box::new(ValuesScan::new(projected, schema.clone()));
    if let Some(keys) = sort_keys {
        return Ok(Some(Box::new(
            Sort::new(scan, keys.clone(), schema.clone())
                .with_work_mem_budget(Arc::clone(&ctx.work_mem)),
        )));
    }
    Ok(Some(scan))
}

fn sort_keys_compatible_with_project(keys: &[SortKey], exprs: &[(ScalarExpr, String)]) -> bool {
    keys.iter().all(|key| {
        let ScalarExpr::Column { index, .. } = &key.expr else {
            return false;
        };
        exprs
            .get(*index)
            .is_some_and(|(expr, _)| matches!(expr, ScalarExpr::Column { index: project_index, .. } if project_index == index))
    })
}

/// Return EXPLAIN text without requiring a full lowerer context.
pub(crate) fn aggregating_index_note_for_snapshot(
    plan: &LogicalPlan,
    catalog_snapshot: &ultrasql_catalog::CatalogSnapshot,
    table_constraints: &dashmap::DashMap<ultrasql_core::Oid, Arc<crate::TableRuntimeConstraints>>,
) -> String {
    let Some((table_name, group_by, aggregates)) = first_aggregate_shape(plan) else {
        return "not applicable (no aggregate scan shape)".to_owned();
    };
    let Some(table) = catalog_snapshot
        .tables
        .get(&table_name.to_ascii_lowercase())
    else {
        return format!("skipped {table_name}: not a persistent catalog table");
    };
    let Some(indexes) = catalog_snapshot.indexes_by_table.get(&table.oid) else {
        return format!("skipped {table_name}: no indexes on table");
    };
    let Some(constraints) = table_constraints.get(&table.oid) else {
        return format!("skipped {table_name}: no runtime index metadata");
    };
    indexes
        .iter()
        .find_map(|index| {
            let metadata = constraints.indexes.get(&index.oid)?;
            let runtime = metadata.aggregating.as_ref()?;
            if !aggregating_spec_matches(&runtime.spec, group_by, aggregates) {
                return None;
            }
            let stats = runtime.explain_stats_snapshot();
            Some(format!(
                "selected {} on {table_name}; aggregating_index_used={} stale_rebuild_used={} summary_rows_read={} base_rows_skipped={}",
                index.name,
                stats.aggregating_index_used,
                stats.stale_rebuild_used,
                stats.summary_rows_read,
                stats.base_rows_skipped
            ))
        })
        .unwrap_or_else(|| format!("skipped {table_name}: no matching aggregating index"))
}

fn current_summary_rows(
    table: &TableEntry,
    runtime: &Arc<RuntimeAggregatingIndex>,
    predicate: Option<&ScalarExpr>,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Vec<Vec<Value>>>, ServerError> {
    if let Some(pred) = predicate
        && !predicate_uses_only_group_columns(pred, &runtime.spec.group_columns)
    {
        return Ok(None);
    }
    let stale_rebuild_used = rebuild_runtime_if_dirty(
        table,
        runtime,
        ctx.heap.as_ref(),
        &ctx.snapshot,
        ctx.oracle.as_ref(),
    )?;
    let rows = runtime
        .rows
        .read()
        .map_err(|_| ServerError::ddl("aggregating index lock poisoned"))?
        .clone();
    let rows = if let Some(pred) = predicate {
        rows.into_iter()
            .map(|row| {
                let keep = summary_row_matches_predicate(
                    pred,
                    &runtime.spec.group_columns,
                    &row[..runtime.spec.group_columns.len()],
                )?
                .ok_or_else(|| {
                    ServerError::ddl(
                        "aggregating index predicate passed support check but did not evaluate",
                    )
                })?;
                Ok(keep.then_some(row))
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, ServerError>>()?
    } else {
        rows
    };
    let base_rows_skipped = summary_base_rows_represented(&runtime.spec, &rows);
    runtime.record_explain_read(stale_rebuild_used, rows.len(), base_rows_skipped);
    Ok(Some(rows))
}

fn rebuild_runtime_if_dirty(
    table: &TableEntry,
    runtime: &Arc<RuntimeAggregatingIndex>,
    heap: &ultrasql_storage::heap::HeapAccess<BlankPageLoader>,
    snapshot: &Snapshot,
    oracle: &TransactionManager,
) -> Result<bool, ServerError> {
    if runtime
        .dirty
        .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Ok(false);
    }
    match build_aggregating_index_rows(table, &runtime.spec, heap, snapshot, oracle) {
        Ok(rows) => {
            let mut guard = runtime
                .rows
                .write()
                .map_err(|_| ServerError::ddl("aggregating index lock poisoned"))?;
            *guard = rows;
            Ok(true)
        }
        Err(err) => {
            runtime.dirty.store(true, Ordering::Release);
            Err(err)
        }
    }
}

fn summary_base_rows_represented(spec: &LogicalAggregatingIndex, rows: &[Vec<Value>]) -> u64 {
    let Some(count_offset) = spec
        .aggregates
        .iter()
        .position(|aggregate| aggregate.func == AggregateFunc::CountStar)
    else {
        return 0;
    };
    let count_column = spec.group_columns.len().saturating_add(count_offset);
    rows.iter()
        .filter_map(|row| row.get(count_column))
        .filter_map(nonnegative_count_value)
        .fold(0_u64, u64::saturating_add)
}

fn nonnegative_count_value(value: &Value) -> Option<u64> {
    match value {
        Value::Int16(v) => u64::try_from(*v).ok(),
        Value::Int32(v) => u64::try_from(*v).ok(),
        Value::Int64(v) => u64::try_from(*v).ok(),
        _ => None,
    }
}

fn aggregate_input_scan(plan: &LogicalPlan) -> Option<(&str, Option<&ScalarExpr>)> {
    match plan {
        LogicalPlan::Scan { table, .. } => Some((table.as_str(), None)),
        LogicalPlan::Filter { input, predicate } => {
            let LogicalPlan::Scan { table, .. } = input.as_ref() else {
                return None;
            };
            Some((table.as_str(), Some(predicate)))
        }
        _ => None,
    }
}

fn first_aggregate_shape(
    plan: &LogicalPlan,
) -> Option<(&str, &[ScalarExpr], &[LogicalAggregateExpr])> {
    match plan {
        LogicalPlan::Project { input, .. } => first_aggregate_shape(input),
        LogicalPlan::Sort { input, .. } | LogicalPlan::Limit { input, .. } => {
            first_aggregate_shape(input)
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => aggregate_input_scan(input)
            .map(|(table, _)| (table, group_by.as_slice(), aggregates.as_slice())),
        _ => None,
    }
}

fn matching_aggregating_index(
    table: &TableEntry,
    group_by: &[ScalarExpr],
    aggregates: &[LogicalAggregateExpr],
    ctx: &LowerCtx<'_>,
) -> Option<(String, Arc<RuntimeAggregatingIndex>)> {
    let indexes = ctx.catalog_snapshot.indexes_by_table.get(&table.oid)?;
    let constraints = ctx.table_constraints.get(&table.oid)?;
    indexes.iter().find_map(|index| {
        let metadata = constraints.indexes.get(&index.oid)?;
        let runtime = metadata.aggregating.as_ref()?;
        aggregating_spec_matches(&runtime.spec, group_by, aggregates)
            .then(|| (index.name.clone(), Arc::clone(runtime)))
    })
}

fn aggregating_spec_matches(
    spec: &LogicalAggregatingIndex,
    group_by: &[ScalarExpr],
    aggregates: &[LogicalAggregateExpr],
) -> bool {
    if group_by.len() != spec.group_columns.len() || aggregates.len() != spec.aggregates.len() {
        return false;
    }
    if group_by
        .iter()
        .zip(&spec.group_columns)
        .any(|(expr, col)| !matches!(expr, ScalarExpr::Column { index, .. } if index == col))
    {
        return false;
    }
    aggregates
        .iter()
        .zip(&spec.aggregates)
        .all(|(query, stored)| {
            !query.distinct
                && query.func == stored.func
                && aggregate_arg_column(query) == stored.arg_column
        })
}

fn aggregate_arg_column(aggregate: &LogicalAggregateExpr) -> Option<usize> {
    match &aggregate.arg {
        Some(ScalarExpr::Column { index, .. }) => Some(*index),
        None => None,
        _ => None,
    }
}

fn predicate_uses_only_group_columns(predicate: &ScalarExpr, group_columns: &[usize]) -> bool {
    match predicate {
        ScalarExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => {
            predicate_uses_only_group_columns(left, group_columns)
                && predicate_uses_only_group_columns(right, group_columns)
        }
        ScalarExpr::Binary {
            op: BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq,
            left,
            right,
            ..
        } => {
            comparison_uses_group_column(left, right, group_columns)
                || comparison_uses_group_column(right, left, group_columns)
        }
        _ => false,
    }
}

fn comparison_uses_group_column(
    column: &ScalarExpr,
    literal: &ScalarExpr,
    group_columns: &[usize],
) -> bool {
    let ScalarExpr::Column { index, .. } = column else {
        return false;
    };
    group_columns.contains(index) && matches!(literal, ScalarExpr::Literal { .. })
}

fn summary_row_matches_predicate(
    predicate: &ScalarExpr,
    group_columns: &[usize],
    group_values: &[Value],
) -> Result<Option<bool>, ServerError> {
    match predicate {
        ScalarExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => {
            let Some(l) = summary_row_matches_predicate(left, group_columns, group_values)? else {
                return Ok(None);
            };
            let Some(r) = summary_row_matches_predicate(right, group_columns, group_values)? else {
                return Ok(None);
            };
            Ok(Some(l && r))
        }
        ScalarExpr::Binary {
            op: op @ (BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq),
            left,
            right,
            ..
        } => compare_summary_value(*op, left, right, group_columns, group_values)
            .or_else(|| {
                compare_summary_value(flip_op(*op), right, left, group_columns, group_values)
            })
            .transpose(),
        _ => Ok(None),
    }
}

fn compare_summary_value(
    op: BinaryOp,
    column: &ScalarExpr,
    literal: &ScalarExpr,
    group_columns: &[usize],
    group_values: &[Value],
) -> Option<Result<bool, ServerError>> {
    let ScalarExpr::Column { index, .. } = column else {
        return None;
    };
    let group_idx = group_columns.iter().position(|col| col == index)?;
    let ScalarExpr::Literal { value, .. } = literal else {
        return None;
    };
    Some(compare_values(op, &group_values[group_idx], value))
}

fn compare_values(op: BinaryOp, left: &Value, right: &Value) -> Result<bool, ServerError> {
    if let (Some(l), Some(r)) = (value_as_i128(left), value_as_i128(right)) {
        return Ok(match op {
            BinaryOp::Eq => l == r,
            BinaryOp::Lt => l < r,
            BinaryOp::LtEq => l <= r,
            BinaryOp::Gt => l > r,
            BinaryOp::GtEq => l >= r,
            _ => false,
        });
    }
    match (op, left, right) {
        (BinaryOp::Eq, Value::Text(l), Value::Text(r)) => Ok(l == r),
        (BinaryOp::Eq, Value::Char(l), Value::Char(r)) => {
            Ok(bpchar_semantic_text(l) == bpchar_semantic_text(r))
        }
        (BinaryOp::Eq, Value::Char(l), Value::Text(r)) => Ok(bpchar_semantic_text(l) == r),
        (BinaryOp::Eq, Value::Text(l), Value::Char(r)) => Ok(l == bpchar_semantic_text(r)),
        (BinaryOp::Eq, Value::Bool(l), Value::Bool(r)) => Ok(l == r),
        _ => Err(ServerError::Unsupported(
            "aggregating index summary predicate supports integer, text equality, and bool equality",
        )),
    }
}

fn value_as_i128(value: &Value) -> Option<i128> {
    match value {
        Value::Int16(v) => Some(i128::from(*v)),
        Value::Int32(v) => Some(i128::from(*v)),
        Value::Int64(v) => Some(i128::from(*v)),
        _ => None,
    }
}

const fn flip_op(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}
