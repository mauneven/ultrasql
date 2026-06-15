//! Scan-lowering helpers and table-function scan.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use num_traits::ToPrimitive;
use serde_json::Value as JsonValue;
use ultrasql_catalog::TableEntry;
use ultrasql_core::{DataType, Field, RelationId, Schema, Value};
use ultrasql_executor::{
    CteScan, Eval, MemTableScan, Operator, ParallelSeqScan, ParallelSeqScanConfig, Project,
    RowCodec, SeqScan, build_batch, choose_parallel_seq_scan_workers,
    json_path::{parse_json_path, select_json_path_with_vars},
};
use ultrasql_planner::{LogicalPlan, ScalarExpr};

use crate::error::ServerError;

use super::LowerCtx;
use super::catalog_views::try_virtual_catalog_scan;
use super::csv_scan::{CsvPredicate, CsvSniffScan, CsvTableScan};
use super::external_scan::{
    is_external_table_function, lower_external_parquet_scan, lower_external_table_scan,
    read_csv_external_args, read_external_path_specs,
};
use super::json_table_scan::lower_json_table_scan;
use super::parquet_scan::ParquetPredicate;
use super::xml_table_scan::lower_xml_table_scan;

#[derive(Clone, Debug, Default)]
struct SummaryColumnStats {
    null_count: u64,
    unique_values: HashSet<Value>,
    min: Option<Value>,
    max: Option<Value>,
    numeric_count: u64,
    numeric_mean: f64,
    numeric_m2: f64,
}

impl SummaryColumnStats {
    fn observe(&mut self, value: &Value) -> Result<(), ServerError> {
        if matches!(value, Value::Null) {
            self.null_count = checked_summary_count_add(self.null_count, 1, "SUMMARIZE null")?;
            return Ok(());
        }
        self.unique_values.insert(value.clone());
        if self
            .min
            .as_ref()
            .is_some_and(|current| summary_value_cmp(value, current) == Some(Ordering::Less))
            || (self.min.is_none() && summary_value_cmp(value, value).is_some())
        {
            self.min = Some(value.clone());
        }
        if self
            .max
            .as_ref()
            .is_some_and(|current| summary_value_cmp(value, current) == Some(Ordering::Greater))
            || (self.max.is_none() && summary_value_cmp(value, value).is_some())
        {
            self.max = Some(value.clone());
        }
        if let Some(numeric) = summary_numeric_value(value) {
            self.observe_numeric(numeric)?;
        }
        Ok(())
    }

    fn observe_numeric(&mut self, value: f64) -> Result<(), ServerError> {
        self.numeric_count = checked_summary_count_add(self.numeric_count, 1, "SUMMARIZE numeric")?;
        let count = self
            .numeric_count
            .to_f64()
            .ok_or_else(|| summary_count_overflow("SUMMARIZE numeric"))?;
        let delta = value - self.numeric_mean;
        self.numeric_mean += delta / count;
        let delta2 = value - self.numeric_mean;
        self.numeric_m2 += delta * delta2;
        Ok(())
    }

    fn avg(&self) -> Value {
        if self.numeric_count == 0 {
            Value::Null
        } else {
            Value::Float64(self.numeric_mean)
        }
    }

    fn stddev(&self) -> Result<Value, ServerError> {
        if self.numeric_count < 2 {
            return Ok(Value::Null);
        }
        let divisor = self
            .numeric_count
            .checked_sub(1)
            .and_then(|count| count.to_f64())
            .ok_or_else(|| summary_count_overflow("SUMMARIZE stddev"))?;
        Ok(Value::Float64((self.numeric_m2 / divisor).sqrt()))
    }
}

pub(super) fn lower_summarize(
    table: &str,
    namespace: &str,
    target_schema: &Schema,
    schema: &Schema,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let table_key = ultrasql_catalog::table_lookup_key(namespace, table);
    let entry = ctx.catalog_snapshot.tables.get(&table_key).ok_or_else(|| {
        ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(table.to_owned()))
    })?;
    if ctx
        .row_security
        .get(&entry.oid)
        .is_some_and(|runtime| runtime.enabled)
    {
        return Err(ServerError::UnsupportedOwned(format!(
            "SUMMARIZE on row-level security table {namespace}.{table}"
        )));
    }

    let relation = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(relation).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let mut row_count = 0_u64;
    let mut stats = vec![SummaryColumnStats::default(); target_schema.len()];
    for tuple in ctx
        .heap
        .scan_visible(relation, block_count, &ctx.snapshot, ctx.oracle.as_ref())
    {
        let tuple = tuple.map_err(|err| {
            ServerError::Ddl(format!(
                "SUMMARIZE heap scan {}.{}: {err}",
                namespace, table
            ))
        })?;
        let row = codec.decode(&tuple.data).map_err(|err| {
            ServerError::Ddl(format!(
                "SUMMARIZE row decode {}.{}: {err}",
                namespace, table
            ))
        })?;
        row_count = checked_summary_count_add(row_count, 1, "SUMMARIZE row")?;
        if row.len() != stats.len() {
            return Err(ServerError::Ddl(format!(
                "SUMMARIZE row width {} does not match schema width {}",
                row.len(),
                stats.len()
            )));
        }
        for (idx, value) in row.iter().enumerate() {
            stats[idx].observe(value)?;
        }
    }

    let row_count_i64 = summary_u64_to_i64(row_count, "SUMMARIZE row")?;
    let mut rows = Vec::with_capacity(target_schema.len());
    for (field, column_stats) in target_schema.fields().iter().zip(&stats) {
        rows.push(vec![
            Value::Text(field.name.clone()),
            Value::Text(field.data_type.to_string()),
            Value::Int64(row_count_i64),
            Value::Int64(summary_u64_to_i64(
                column_stats.null_count,
                "SUMMARIZE null",
            )?),
            column_stats
                .min
                .as_ref()
                .map_or(Value::Null, summary_value_text),
            column_stats
                .max
                .as_ref()
                .map_or(Value::Null, summary_value_text),
            Value::Int64(summary_usize_to_i64(
                column_stats.unique_values.len(),
                "SUMMARIZE unique",
            )?),
            column_stats.avg(),
            column_stats.stddev()?,
        ]);
    }
    let batch = build_batch(&rows, schema)?;
    Ok(Box::new(MemTableScan::new(schema.clone(), vec![batch])))
}

fn summary_count_overflow(context: &str) -> ServerError {
    ServerError::Execute(ultrasql_executor::ExecError::NumericFieldOverflow(format!(
        "{context} count overflow"
    )))
}

fn checked_summary_count_add(left: u64, right: u64, context: &str) -> Result<u64, ServerError> {
    left.checked_add(right)
        .ok_or_else(|| summary_count_overflow(context))
}

fn summary_u64_to_i64(value: u64, context: &str) -> Result<i64, ServerError> {
    i64::try_from(value).map_err(|_| summary_count_overflow(context))
}

fn summary_usize_to_i64(value: usize, context: &str) -> Result<i64, ServerError> {
    i64::try_from(value).map_err(|_| summary_count_overflow(context))
}

fn summary_value_text(value: &Value) -> Value {
    Value::Text(value.to_string())
}

fn summary_numeric_value(value: &Value) -> Option<f64> {
    match value {
        Value::Int16(value) => Some(f64::from(*value)),
        Value::Int32(value) => Some(f64::from(*value)),
        Value::Int64(value) => value.to_f64(),
        Value::Float32(value) => Some(f64::from(*value)),
        Value::Float64(value) => Some(*value),
        Value::Decimal { value, scale } => Some(value.to_f64()? / 10_f64.powi(*scale)),
        Value::Money(value) => value.to_f64().map(|cents| cents / 100.0),
        _ => None,
    }
}

#[allow(clippy::match_same_arms)]
fn summary_value_cmp(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        (Value::Int16(left), Value::Int16(right)) => Some(left.cmp(right)),
        (Value::Int32(left), Value::Int32(right)) => Some(left.cmp(right)),
        (Value::Int64(left), Value::Int64(right)) => Some(left.cmp(right)),
        (Value::Oid(left), Value::Oid(right))
        | (Value::RegClass(left), Value::RegClass(right))
        | (Value::RegType(left), Value::RegType(right)) => left.raw().partial_cmp(&right.raw()),
        (Value::PgLsn(left), Value::PgLsn(right)) => left.raw().partial_cmp(&right.raw()),
        (Value::Float32(left), Value::Float32(right)) => left.partial_cmp(right),
        (Value::Float64(left), Value::Float64(right)) => left.partial_cmp(right),
        (Value::Text(left), Value::Text(right))
        | (Value::Char(left), Value::Char(right))
        | (Value::Json(left), Value::Json(right))
        | (Value::Jsonb(left), Value::Jsonb(right))
        | (Value::Xml(left), Value::Xml(right)) => Some(left.cmp(right)),
        (Value::Bytea(left), Value::Bytea(right)) => Some(left.cmp(right)),
        (Value::Timestamp(left), Value::Timestamp(right))
        | (Value::TimestampTz(left), Value::TimestampTz(right))
        | (Value::Time(left), Value::Time(right)) => Some(left.cmp(right)),
        (Value::TimeTz { .. }, Value::TimeTz { .. }) => {
            left.to_string().partial_cmp(&right.to_string())
        }
        (Value::Date(left), Value::Date(right)) => Some(left.cmp(right)),
        (Value::Uuid(left), Value::Uuid(right)) => Some(left.cmp(right)),
        (Value::Decimal { .. }, Value::Decimal { .. }) => {
            summary_numeric_value(left)?.partial_cmp(&summary_numeric_value(right)?)
        }
        (Value::Money(left), Value::Money(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

pub(super) fn lower_catalog_or_sample_scan(
    table: &str,
    projection: Option<&[usize]>,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let folded = table.to_ascii_lowercase();
    if let Some(buffer) = ctx.cte_buffers.get(&folded) {
        let scan: Box<dyn Operator> = Box::new(CteScan::new(
            Arc::clone(&buffer.batches),
            buffer.schema.clone(),
        ));
        return apply_projection(scan, projection);
    }
    if let Some(scan) = try_virtual_catalog_scan(table, ctx)? {
        return apply_projection(scan, projection);
    }
    if let Some(scan) =
        super::time_partition::try_lower_time_partition_scan(&folded, projection, ctx)?
    {
        return Ok(scan);
    }
    if let Some(entry) = ctx.catalog_snapshot.tables.get(&folded) {
        return lower_heap_scan(entry, projection, ctx);
    }
    // Legacy path: sample tables.
    let sample = ctx.tables.lookup(table).ok_or_else(|| {
        ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
            table.to_string(),
        ))
    })?;
    let scan: Box<dyn Operator> = Box::new(MemTableScan::new(
        sample.schema.clone(),
        sample.batches.clone(),
    ));
    apply_projection(scan, projection)
}

/// Construct a [`SeqScan`] for a real persistent relation.
pub(super) fn lower_heap_scan(
    entry: &TableEntry,
    projection: Option<&[usize]>,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let rel = RelationId(entry.oid);
    // The catalog's `n_blocks` stat is an estimate; the heap's
    // counter is the truth. Take the larger of the two so a freshly
    // created table (entry.n_blocks = 0) still scans the blocks that
    // the heap has actually allocated through `insert`.
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let workers = choose_parallel_seq_scan_workers(block_count, entry.schema.len());
    if workers > 1 && projection.is_none() {
        let scan = ParallelSeqScan::new(ParallelSeqScanConfig {
            heap: Arc::clone(&ctx.heap),
            relation: rel,
            block_count,
            snapshot: ctx.snapshot.clone(),
            oracle: Arc::clone(&ctx.oracle),
            vm: Arc::clone(&ctx.vm),
            codec,
            cancel_flag: ctx.cancel_flag.clone(),
            worker_count: workers,
        });
        return Ok(Box::new(scan));
    }
    let mut scan = SeqScan::new_with_vm(
        Arc::clone(&ctx.heap),
        rel,
        block_count,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        Arc::clone(&ctx.vm),
        codec,
    );
    // Thread the session's cancel flag through so `next_batch` returns
    // `ExecError::Cancelled` (→ SQLSTATE 57014) when a peer
    // `CancelRequest` flips it.
    if let Some(flag) = &ctx.cancel_flag {
        scan = scan.with_cancel_flag(flag.clone());
    }
    let scan: Box<dyn Operator> = Box::new(scan);
    apply_projection(scan, projection)
}

/// Construct a cache-building single-threaded [`SeqScan`] for a real
/// persistent relation.
///
/// Used by scalar aggregate miss paths where paying one serial scan to
/// populate the relation's `ColumnCache` is cheaper over repeated
/// executions than repeatedly choosing `ParallelSeqScan`, which does
/// not publish cache entries.
pub(super) fn lower_heap_seq_scan(
    entry: &TableEntry,
    ctx: &LowerCtx<'_>,
) -> Result<Box<dyn Operator>, ServerError> {
    let rel = RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    let codec = RowCodec::new(entry.schema.clone());
    let mut scan = SeqScan::new_with_vm(
        Arc::clone(&ctx.heap),
        rel,
        block_count,
        ctx.snapshot.clone(),
        Arc::clone(&ctx.oracle),
        Arc::clone(&ctx.vm),
        codec,
    );
    if let Some(flag) = &ctx.cancel_flag {
        scan = scan.with_cancel_flag(flag.clone());
    }
    Ok(Box::new(scan))
}

fn apply_projection(
    scan: Box<dyn Operator>,
    projection: Option<&[usize]>,
) -> Result<Box<dyn Operator>, ServerError> {
    if let Some(indices) = projection {
        Ok(Box::new(Project::new(scan, indices.to_vec())?))
    } else {
        Ok(scan)
    }
}

/// Lower a `LogicalPlan::FunctionScan { name, args, .. }` into the
/// matching set-returning-function operator.
pub(super) fn lower_function_scan(
    name: &str,
    args: &[ScalarExpr],
    cancel_flag: Option<ultrasql_executor::CancelFlag>,
) -> Result<Box<dyn Operator>, ServerError> {
    if name == "sniff_csv" {
        if args.len() != 1 {
            return Err(ServerError::Unsupported(
                "sniff_csv: expected one path argument",
            ));
        }
        let value = Eval::new(args[0].clone())
            .eval(&[])
            .map_err(|e| ServerError::Ddl(format!("sniff_csv argument evaluation failed: {e}")))?;
        let Value::Text(path) = value else {
            return Err(ServerError::Unsupported(
                "sniff_csv: path argument must be a string literal",
            ));
        };
        return Ok(Box::new(CsvSniffScan::from_path(&path)?));
    }
    if is_external_table_function(name) {
        return lower_external_table_scan(name, args);
    }
    if name == "json_table" {
        return lower_json_table_scan(args);
    }
    if name == "xml_table" {
        return lower_xml_table_scan(args);
    }
    if name == "json_each" {
        return lower_json_each(args);
    }
    if name == "jsonb_path_query" {
        return lower_jsonb_path_query(args);
    }
    if name == "unnest" {
        if args.len() != 1 {
            return Err(ServerError::Unsupported(
                "unnest: expected one array argument",
            ));
        }
        let value = Eval::new(args[0].clone())
            .eval(&[])
            .map_err(|e| ServerError::Ddl(format!("unnest argument evaluation failed: {e}")))?;
        let Value::Array {
            element_type,
            elements,
        } = value
        else {
            return Err(ServerError::Unsupported("unnest: argument must be array"));
        };
        let mut scan = ultrasql_executor::FunctionScan::unnest(element_type, elements);
        if let Some(flag) = cancel_flag {
            scan = scan.with_cancel_flag(flag);
        }
        return Ok(Box::new(scan));
    }
    if name != "generate_series" {
        return Err(ServerError::Unsupported(
            "table function (only generate_series, unnest, json_each, jsonb_path_query, json_table, xmltable, read_csv, read_parquet, read_json, read_ndjson, read_arrow, read_iceberg, iceberg_scan, and sniff_csv supported)",
        ));
    }
    if args.len() < 2 || args.len() > 3 {
        return Err(ServerError::Unsupported(
            "generate_series: expected (start, stop) or (start, stop, step)",
        ));
    }
    fn to_i64(e: &ScalarExpr) -> Result<i64, ServerError> {
        match e {
            ScalarExpr::Literal {
                value: Value::Int64(v),
                ..
            } => Ok(*v),
            ScalarExpr::Literal {
                value: Value::Int32(v),
                ..
            } => Ok(i64::from(*v)),
            ScalarExpr::Unary {
                op: ultrasql_planner::UnaryOp::Neg,
                expr,
                ..
            } => to_i64(expr)?.checked_neg().ok_or(ServerError::Unsupported(
                "generate_series: negation overflow",
            )),
            _ => Err(ServerError::Unsupported(
                "generate_series: arguments must be integer literals",
            )),
        }
    }
    let start = to_i64(&args[0])?;
    let stop = to_i64(&args[1])?;
    let step = if args.len() == 3 {
        to_i64(&args[2])?
    } else {
        1
    };
    let mut scan = ultrasql_executor::FunctionScan::generate_series(start, stop, step);
    if let Some(flag) = cancel_flag {
        scan = scan.with_cancel_flag(flag);
    }
    Ok(Box::new(scan))
}

fn lower_json_each(args: &[ScalarExpr]) -> Result<Box<dyn Operator>, ServerError> {
    if args.len() != 1 {
        return Err(ServerError::Unsupported(
            "json_each: expected one json/jsonb argument",
        ));
    }
    let value = Eval::new(args[0].clone())
        .eval(&[])
        .map_err(|e| ServerError::Ddl(format!("json_each argument evaluation failed: {e}")))?;
    let document = match value {
        Value::Json(text) | Value::Jsonb(text) | Value::Text(text) => {
            serde_json::from_str::<JsonValue>(&text)
                .map_err(|err| ServerError::CopyFormat(format!("json_each parse jsonb: {err}")))?
        }
        Value::Null => JsonValue::Null,
        other => {
            return Err(ServerError::CopyFormat(format!(
                "json_each: argument must be json/jsonb or text, got {:?}",
                other.data_type()
            )));
        }
    };
    let schema = Schema::new([
        Field::required("key", DataType::Text { max_len: None }),
        Field::nullable("value", DataType::Jsonb),
    ])
    .map_err(|err| ServerError::CopyFormat(format!("json_each schema: {err}")))?;
    let rows = match document {
        JsonValue::Object(object) => object
            .into_iter()
            .map(|(key, value)| vec![Value::Text(key), Value::Jsonb(value.to_string())])
            .collect::<Vec<_>>(),
        JsonValue::Array(values) => values
            .into_iter()
            .enumerate()
            .map(|(idx, value)| {
                vec![
                    Value::Text(idx.to_string()),
                    Value::Jsonb(value.to_string()),
                ]
            })
            .collect::<Vec<_>>(),
        JsonValue::Null => Vec::new(),
        other => {
            return Err(ServerError::CopyFormat(format!(
                "json_each: expected object or array, got {other}"
            )));
        }
    };
    let batches = if rows.is_empty() {
        Vec::new()
    } else {
        vec![
            build_batch(&rows, &schema)
                .map_err(|err| ServerError::CopyFormat(format!("json_each batch: {err}")))?,
        ]
    };
    Ok(Box::new(MemTableScan::new(schema, batches)))
}

fn lower_jsonb_path_query(args: &[ScalarExpr]) -> Result<Box<dyn Operator>, ServerError> {
    if !(2..=3).contains(&args.len()) {
        return Err(ServerError::Unsupported(
            "jsonb_path_query: expected jsonb document, path, and optional vars",
        ));
    }
    let document_value = Eval::new(args[0].clone()).eval(&[]).map_err(|e| {
        ServerError::Ddl(format!("jsonb_path_query document evaluation failed: {e}"))
    })?;
    let document = match document_value {
        Value::Json(text) | Value::Jsonb(text) | Value::Text(text) => serde_json::from_str::<
            JsonValue,
        >(&text)
        .map_err(|err| ServerError::CopyFormat(format!("jsonb_path_query parse jsonb: {err}")))?,
        Value::Null => JsonValue::Null,
        other => {
            return Err(ServerError::CopyFormat(format!(
                "jsonb_path_query: document must be json/jsonb or text, got {:?}",
                other.data_type()
            )));
        }
    };
    let path_value = Eval::new(args[1].clone())
        .eval(&[])
        .map_err(|e| ServerError::Ddl(format!("jsonb_path_query path evaluation failed: {e}")))?;
    let Value::Text(path) = path_value else {
        return Err(ServerError::CopyFormat(format!(
            "jsonb_path_query: path must be text, got {:?}",
            path_value.data_type()
        )));
    };
    let path = parse_json_path(&path)
        .map_err(|err| ServerError::CopyFormat(format!("jsonb_path_query invalid path: {err}")))?;
    let vars = if args.len() == 3 {
        let vars_value = Eval::new(args[2].clone()).eval(&[]).map_err(|e| {
            ServerError::Ddl(format!("jsonb_path_query vars evaluation failed: {e}"))
        })?;
        match vars_value {
            Value::Json(text) | Value::Jsonb(text) | Value::Text(text) => {
                Some(serde_json::from_str::<JsonValue>(&text).map_err(|err| {
                    ServerError::CopyFormat(format!("jsonb_path_query parse vars jsonb: {err}"))
                })?)
            }
            Value::Null => None,
            other => {
                return Err(ServerError::CopyFormat(format!(
                    "jsonb_path_query: vars must be json/jsonb or text, got {:?}",
                    other.data_type()
                )));
            }
        }
    } else {
        None
    };
    let selected = select_json_path_with_vars(&document, &path, vars.as_ref())
        .map_err(|err| ServerError::CopyFormat(format!("jsonb_path_query: {err}")))?;
    let schema = Schema::new([Field::nullable("value", DataType::Jsonb)])
        .map_err(|err| ServerError::CopyFormat(format!("jsonb_path_query schema: {err}")))?;
    let rows = selected
        .into_iter()
        .map(|value| vec![Value::Jsonb(value.to_string())])
        .collect::<Vec<_>>();
    let batches = if rows.is_empty() {
        Vec::new()
    } else {
        vec![
            build_batch(&rows, &schema)
                .map_err(|err| ServerError::CopyFormat(format!("jsonb_path_query batch: {err}")))?,
        ]
    };
    Ok(Box::new(MemTableScan::new(schema, batches)))
}

/// Lower `Project(read_csv(...))` with CSV projection pushdown when the
/// expressions are direct column references.
pub(super) fn try_lower_read_csv_project(
    input: &LogicalPlan,
    exprs: &[(ScalarExpr, String)],
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let Some(projection) = projection_names(exprs) else {
        return Ok(None);
    };
    let LogicalPlan::FunctionScan { name, args, .. } = input else {
        return Ok(None);
    };
    if name != "read_csv" {
        return Ok(None);
    }
    let csv_args = read_csv_external_args(args)?;
    Ok(Some(Box::new(CsvTableScan::from_path_specs_with_options(
        &csv_args.path_specs,
        Some(&projection),
        None,
        csv_args.reject_path.as_deref(),
    )?)))
}

/// Lower `Filter(read_csv(...))` with CSV predicate pushdown when the
/// predicate is a simple `column OP literal` comparison.
pub(super) fn try_lower_read_csv_filter(
    input: &LogicalPlan,
    predicate: &ScalarExpr,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let LogicalPlan::FunctionScan { name, args, .. } = input else {
        return Ok(None);
    };
    if name != "read_csv" {
        return Ok(None);
    }
    let Some(predicate) = CsvPredicate::from_scalar(predicate) else {
        return Ok(None);
    };
    let csv_args = read_csv_external_args(args)?;
    Ok(Some(Box::new(CsvTableScan::from_path_specs_with_options(
        &csv_args.path_specs,
        None,
        Some(&predicate),
        csv_args.reject_path.as_deref(),
    )?)))
}

/// Lower `Project(read_parquet(...))` and
/// `Project(Filter(read_parquet(...)))` with Parquet projection and
/// predicate pushdown when the expressions have a direct Parquet shape.
pub(super) fn try_lower_read_parquet_project(
    input: &LogicalPlan,
    exprs: &[(ScalarExpr, String)],
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let Some(projection) = projection_names(exprs) else {
        return Ok(None);
    };
    match input {
        LogicalPlan::FunctionScan { name, args, .. } if name == "read_parquet" => {
            let path_specs = read_external_path_specs("read_parquet", args)?;
            Ok(Some(lower_external_parquet_scan(
                &path_specs,
                Some(&projection),
                None,
            )?))
        }
        LogicalPlan::Filter {
            input, predicate, ..
        } => {
            let LogicalPlan::FunctionScan { name, args, .. } = input.as_ref() else {
                return Ok(None);
            };
            if name != "read_parquet" {
                return Ok(None);
            }
            let Some(predicate) = ParquetPredicate::from_scalar(predicate) else {
                return Ok(None);
            };
            let path_specs = read_external_path_specs("read_parquet", args)?;
            Ok(Some(lower_external_parquet_scan(
                &path_specs,
                Some(&projection),
                Some(&predicate),
            )?))
        }
        _ => Ok(None),
    }
}

/// Lower `Filter(read_parquet(...))` with Parquet predicate pushdown
/// when the predicate is a simple `column OP literal` comparison.
pub(super) fn try_lower_read_parquet_filter(
    input: &LogicalPlan,
    predicate: &ScalarExpr,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let LogicalPlan::FunctionScan { name, args, .. } = input else {
        return Ok(None);
    };
    if name != "read_parquet" {
        return Ok(None);
    }
    let Some(predicate) = ParquetPredicate::from_scalar(predicate) else {
        return Ok(None);
    };
    let path_specs = read_external_path_specs("read_parquet", args)?;
    Ok(Some(lower_external_parquet_scan(
        &path_specs,
        None,
        Some(&predicate),
    )?))
}

fn projection_names(exprs: &[(ScalarExpr, String)]) -> Option<Vec<String>> {
    exprs
        .iter()
        .map(|(expr, alias)| match expr {
            ScalarExpr::Column { name, .. } if name == alias => Some(name.clone()),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, UnaryOp};
    use ultrasql_vec::column::Column;

    fn text_lit(value: String) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Text(value),
            data_type: DataType::Text { max_len: None },
        }
    }

    fn text_col(name: &str, index: usize) -> (ScalarExpr, String) {
        (
            ScalarExpr::Column {
                name: name.to_owned(),
                index,
                data_type: DataType::Text { max_len: None },
            },
            name.to_owned(),
        )
    }

    fn text_column_expr(name: &str, index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.to_owned(),
            index,
            data_type: DataType::Text { max_len: None },
        }
    }

    fn binary_predicate(left: ScalarExpr, op: BinaryOp, right: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            data_type: DataType::Bool,
        }
    }

    fn read_csv_input(csv_path: &std::path::Path, header: &[&str]) -> LogicalPlan {
        let mut fields = header
            .iter()
            .map(|name| Field::nullable(*name, DataType::Text { max_len: None }))
            .collect::<Vec<_>>();
        fields.push(Field::nullable(
            "_filename",
            DataType::Text { max_len: None },
        ));
        fields.push(Field::required("_row_number", DataType::Int64));
        LogicalPlan::FunctionScan {
            name: "read_csv".to_owned(),
            args: vec![text_lit(csv_path.display().to_string())],
            schema: Schema::new(fields).expect("schema"),
        }
    }

    #[test]
    fn read_csv_project_pushdown_builds_only_wanted_columns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("people.csv");
        fs::write(&csv_path, "id,name,unused\n1,Ada,skip\n").expect("write csv");
        let input_schema = Schema::new([
            Field::nullable("id", DataType::Text { max_len: None }),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::nullable("unused", DataType::Text { max_len: None }),
            Field::nullable("_filename", DataType::Text { max_len: None }),
            Field::required("_row_number", DataType::Int64),
        ])
        .expect("schema");
        let input = LogicalPlan::FunctionScan {
            name: "read_csv".to_owned(),
            args: vec![text_lit(csv_path.display().to_string())],
            schema: input_schema,
        };
        let exprs = vec![text_col("name", 1)];

        let mut scan = try_lower_read_csv_project(&input, &exprs)
            .expect("lower read_csv project")
            .expect("read_csv project pushdown");
        assert_eq!(scan.schema().len(), 1);
        assert_eq!(scan.schema().field_at(0).name, "name");

        let batch = scan
            .next_batch()
            .expect("read projected csv batch")
            .expect("projected csv batch");
        assert_eq!(batch.width(), 1);
        assert_eq!(batch.rows(), 1);
    }

    #[test]
    fn read_csv_project_pushdown_preserves_virtual_columns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("people.csv");
        fs::write(&csv_path, "id,name\n1,Ada\n").expect("write csv");
        let input_schema = Schema::new([
            Field::nullable("id", DataType::Text { max_len: None }),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::nullable("_filename", DataType::Text { max_len: None }),
            Field::required("_row_number", DataType::Int64),
        ])
        .expect("schema");
        let input = LogicalPlan::FunctionScan {
            name: "read_csv".to_owned(),
            args: vec![text_lit(csv_path.display().to_string())],
            schema: input_schema,
        };
        let exprs = vec![
            text_col("_filename", 2),
            (
                ScalarExpr::Column {
                    name: "_row_number".to_owned(),
                    index: 3,
                    data_type: DataType::Int64,
                },
                "_row_number".to_owned(),
            ),
        ];

        let mut scan = try_lower_read_csv_project(&input, &exprs)
            .expect("lower read_csv project")
            .expect("read_csv project pushdown");
        assert_eq!(scan.schema().len(), 2);
        assert_eq!(scan.schema().field_at(0).name, "_filename");
        assert_eq!(scan.schema().field_at(1).name, "_row_number");

        let batch = scan
            .next_batch()
            .expect("read projected csv batch")
            .expect("projected csv batch");
        assert_eq!(batch.width(), 2);
        assert_eq!(batch.rows(), 1);
    }

    #[test]
    fn generate_series_lowering_rejects_negated_i64_min_ast() {
        let bad_start = ScalarExpr::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(ScalarExpr::Literal {
                value: Value::Int64(i64::MIN),
                data_type: DataType::Int64,
            }),
            data_type: DataType::Int64,
        };
        let stop = ScalarExpr::Literal {
            value: Value::Int64(0),
            data_type: DataType::Int64,
        };

        let result = lower_function_scan("generate_series", &[bad_start, stop], None);

        assert!(
            matches!(result, Err(ServerError::Unsupported(message)) if message.contains("negation overflow")),
            "generate_series lowering must reject i64::MIN negation, got {result:?}"
        );
    }

    #[test]
    fn read_csv_filter_pushdown_applies_text_equality() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("people.csv");
        fs::write(&csv_path, "id,name\n1,Ada\n2,Bob\n3,Ada\n").expect("write csv");
        let input = read_csv_input(&csv_path, &["id", "name"]);
        let predicate = binary_predicate(
            text_column_expr("name", 1),
            BinaryOp::Eq,
            text_lit("Ada".to_owned()),
        );

        let mut scan = try_lower_read_csv_filter(&input, &predicate)
            .expect("lower read_csv filter")
            .expect("read_csv filter pushdown");
        let batch = scan
            .next_batch()
            .expect("read filtered csv batch")
            .expect("filtered csv batch");
        assert_eq!(batch.rows(), 2);
        let Column::Utf8(ids) = &batch.columns()[0] else {
            panic!("expected id text column");
        };
        assert_eq!(ids.value(0), "1");
        assert_eq!(ids.value(1), "3");
    }

    #[test]
    fn read_csv_filter_pushdown_applies_numeric_comparison() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("people.csv");
        fs::write(&csv_path, "id,name\n1,Ada\n2,Bob\n10,Cy\n").expect("write csv");
        let input = read_csv_input(&csv_path, &["id", "name"]);
        let predicate = binary_predicate(
            text_column_expr("id", 0),
            BinaryOp::Gt,
            ScalarExpr::Literal {
                value: Value::Int32(2),
                data_type: DataType::Int32,
            },
        );

        let mut scan = try_lower_read_csv_filter(&input, &predicate)
            .expect("lower read_csv filter")
            .expect("read_csv filter pushdown");
        let batch = scan
            .next_batch()
            .expect("read filtered csv batch")
            .expect("filtered csv batch");
        assert_eq!(batch.rows(), 1);
        let Column::Utf8(ids) = &batch.columns()[0] else {
            panic!("expected id text column");
        };
        assert_eq!(ids.value(0), "10");
    }

    #[test]
    fn read_csv_filter_pushdown_skips_text_ordering() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("people.csv");
        fs::write(&csv_path, "id,name\n1,Ada\n").expect("write csv");
        let input = read_csv_input(&csv_path, &["id", "name"]);
        let predicate = binary_predicate(
            text_column_expr("name", 1),
            BinaryOp::Gt,
            text_lit("Ada".to_owned()),
        );

        let scan = try_lower_read_csv_filter(&input, &predicate).expect("lower read_csv filter");
        assert!(scan.is_none());
    }
}
