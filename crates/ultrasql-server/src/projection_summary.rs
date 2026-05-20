//! Cached physical projection summaries for repeated grouped analytics.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::BytesMut;
use parking_lot::RwLock;
use ultrasql_catalog::CatalogSnapshot;
use ultrasql_core::{DataType, RelationId, Schema, Value};
use ultrasql_executor::ValuesScan;
use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, LogicalPlan, ScalarExpr, SortKey};
use ultrasql_storage::column_cache::{
    CachedGroupedProjectionAggregateKey, CachedGroupedProjectionFieldKey,
    CachedGroupedProjectionOrderKey, CachedGroupedProjectionWire, CachedGroupedProjectionWireKey,
};
use ultrasql_storage::heap::HeapAccess;
use ultrasql_vec::column::Column;

use crate::BlankPageLoader;
use crate::result_encoder::{self, SelectResult};

type CachedProjectionRows = Arc<[Vec<Value>]>;
type CachedProjectionHit = (
    Schema,
    CachedProjectionRows,
    Arc<CachedGroupedProjectionWire>,
);

/// Try to answer a safe `GROUP BY` query from a version-stamped physical
/// projection summary.
pub(crate) fn try_run_cached_grouped_projection_select(
    plan: &LogicalPlan,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    heap: &HeapAccess<BlankPageLoader>,
    stream_buf: &mut BytesMut,
) -> Option<SelectResult> {
    let (schema, rows, entry) = cached_grouped_projection(plan, catalog_snapshot, heap)?;
    let encoded_rows = u64::try_from(rows.len()).ok()?;
    if let Some(hit) = entry.text_body.read().as_ref().cloned() {
        return Some(result_encoder::run_shared_preencoded_select_streamed(
            hit,
            encoded_rows,
        ));
    }

    let literal_rows = rows_to_literals(rows.as_ref(), &schema);
    let mut scan = ValuesScan::new(literal_rows, schema);
    let result = result_encoder::run_select_streamed(&mut scan, stream_buf).ok()?;
    if let Some(body) = result.streamed_body.as_ref() {
        *entry.text_body.write() = Some(Arc::<[u8]>::from(body.as_ref()));
    }
    Some(result)
}

/// Try to build a reusable scan over cached physical projection rows.
pub(crate) fn try_build_cached_grouped_projection_scan(
    plan: &LogicalPlan,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    heap: &HeapAccess<BlankPageLoader>,
) -> Option<ValuesScan> {
    let (schema, rows, _) = cached_grouped_projection(plan, catalog_snapshot, heap)?;
    Some(ValuesScan::new(
        rows_to_literals(rows.as_ref(), &schema),
        schema,
    ))
}

fn cached_grouped_projection(
    plan: &LogicalPlan,
    catalog_snapshot: &Arc<CatalogSnapshot>,
    heap: &HeapAccess<BlankPageLoader>,
) -> Option<CachedProjectionHit> {
    let shape = GroupedProjectionShape::extract(plan)?;
    let folded = shape.table.to_ascii_lowercase();
    let entry = catalog_snapshot.tables.get(&folded)?;
    let cached = heap.column_cache.get(RelationId(entry.oid))?;
    let key = shape.cache_key()?;

    if let Some(hit) = cached
        .cached_grouped_projection_wire
        .read()
        .get(&key)
        .cloned()
    {
        return Some((shape.output_schema.clone(), Arc::clone(&hit.rows), hit));
    }

    let rows = build_grouped_projection_rows(
        &cached.columns,
        &shape.group_columns()?,
        shape.aggregates,
        &shape.order_keys()?,
    )?;
    let rows: CachedProjectionRows = Arc::from(rows.into_boxed_slice());
    let summary = Arc::new(CachedGroupedProjectionWire {
        rows: Arc::clone(&rows),
        text_body: RwLock::new(None),
    });
    cached
        .cached_grouped_projection_wire
        .write()
        .insert(key, Arc::clone(&summary));
    Some((shape.output_schema.clone(), rows, summary))
}

struct GroupedProjectionShape<'a> {
    table: &'a str,
    group_by: &'a [ScalarExpr],
    aggregates: &'a [LogicalAggregateExpr],
    output_schema: &'a Schema,
    order_by: &'a [SortKey],
}

impl<'a> GroupedProjectionShape<'a> {
    fn extract(plan: &'a LogicalPlan) -> Option<Self> {
        match plan {
            LogicalPlan::Project {
                input,
                exprs,
                schema,
            } => {
                require_identity_projection(exprs)?;
                let (aggregate, order_by) = match input.as_ref() {
                    LogicalPlan::Sort { input, keys } => (input.as_ref(), keys.as_slice()),
                    other => (other, &[][..]),
                };
                Self::from_aggregate(aggregate, schema, order_by)
            }
            LogicalPlan::Sort { input, keys } => match input.as_ref() {
                LogicalPlan::Project {
                    input,
                    exprs,
                    schema,
                } => {
                    require_identity_projection(exprs)?;
                    Self::from_aggregate(input.as_ref(), schema, keys)
                }
                aggregate => Self::from_aggregate(aggregate, aggregate.schema(), keys),
            },
            aggregate => Self::from_aggregate(aggregate, aggregate.schema(), &[]),
        }
    }

    fn from_aggregate(
        plan: &'a LogicalPlan,
        output_schema: &'a Schema,
        order_by: &'a [SortKey],
    ) -> Option<Self> {
        let LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } = plan
        else {
            return None;
        };
        if group_by.is_empty() || aggregates.is_empty() {
            return None;
        }
        let LogicalPlan::Scan { table, .. } = input.as_ref() else {
            return None;
        };
        if output_schema.len() != group_by.len().saturating_add(aggregates.len()) {
            return None;
        }
        Some(Self {
            table,
            group_by,
            aggregates,
            output_schema,
            order_by,
        })
    }

    fn group_columns(&self) -> Option<Vec<usize>> {
        self.group_by.iter().map(column_index).collect()
    }

    fn order_keys(&self) -> Option<Vec<CachedGroupedProjectionOrderKey>> {
        self.order_by
            .iter()
            .map(|key| {
                let output_index = column_index(&key.expr)?;
                if output_index >= self.output_schema.len() {
                    return None;
                }
                Some(CachedGroupedProjectionOrderKey {
                    output_index,
                    asc: key.asc,
                    nulls_first: key.nulls_first,
                })
            })
            .collect()
    }

    fn aggregate_keys(&self) -> Option<Vec<CachedGroupedProjectionAggregateKey>> {
        self.aggregates
            .iter()
            .map(|agg| {
                if agg.distinct {
                    return None;
                }
                match agg.func {
                    AggregateFunc::CountStar => {
                        Some(CachedGroupedProjectionAggregateKey::CountStar)
                    }
                    AggregateFunc::Count => {
                        let (column, data_type) = aggregate_column_arg(agg)?;
                        if !is_supported_projection_type(&data_type) {
                            return None;
                        }
                        Some(CachedGroupedProjectionAggregateKey::Count { column, data_type })
                    }
                    AggregateFunc::Sum => {
                        let (column, data_type) = aggregate_column_arg(agg)?;
                        if !matches!(data_type, DataType::Int32 | DataType::Int64) {
                            return None;
                        }
                        Some(CachedGroupedProjectionAggregateKey::Sum { column, data_type })
                    }
                    _ => None,
                }
            })
            .collect()
    }

    fn cache_key(&self) -> Option<CachedGroupedProjectionWireKey> {
        Some(CachedGroupedProjectionWireKey {
            group_columns: self.group_columns()?,
            aggregates: self.aggregate_keys()?,
            output_fields: self
                .output_schema
                .fields()
                .iter()
                .map(|field| CachedGroupedProjectionFieldKey {
                    name: field.name.clone(),
                    data_type: field.data_type.clone(),
                    nullable: field.nullable,
                })
                .collect(),
            order_by: self.order_keys()?,
        })
    }
}

#[derive(Clone, Debug)]
enum ProjectionAggState {
    Count(i64),
    SumI64(Option<i64>),
}

fn build_grouped_projection_rows(
    columns: &[Column],
    group_columns: &[usize],
    aggregates: &[LogicalAggregateExpr],
    order_by: &[CachedGroupedProjectionOrderKey],
) -> Option<Vec<Vec<Value>>> {
    let rows = columns.first().map_or(0, Column::len);
    let mut groups: HashMap<Vec<Value>, Vec<ProjectionAggState>> = HashMap::new();
    for row in 0..rows {
        let key = group_columns
            .iter()
            .map(|idx| value_from_column(columns.get(*idx)?, row))
            .collect::<Option<Vec<_>>>()?;
        let states = groups
            .entry(key)
            .or_insert_with(|| init_projection_states(aggregates));
        for (state, agg) in states.iter_mut().zip(aggregates) {
            apply_projection_aggregate(state, agg, columns, row)?;
        }
    }

    let mut out = Vec::with_capacity(groups.len());
    for (mut key, states) in groups {
        key.extend(states.iter().map(finalise_projection_state));
        out.push(key);
    }
    sort_projection_rows(&mut out, order_by);
    Some(out)
}

fn init_projection_states(aggregates: &[LogicalAggregateExpr]) -> Vec<ProjectionAggState> {
    aggregates
        .iter()
        .map(|agg| match agg.func {
            AggregateFunc::CountStar | AggregateFunc::Count => ProjectionAggState::Count(0),
            AggregateFunc::Sum => ProjectionAggState::SumI64(None),
            _ => ProjectionAggState::Count(0),
        })
        .collect()
}

fn apply_projection_aggregate(
    state: &mut ProjectionAggState,
    agg: &LogicalAggregateExpr,
    columns: &[Column],
    row: usize,
) -> Option<()> {
    match (state, agg.func) {
        (ProjectionAggState::Count(n), AggregateFunc::CountStar) => {
            *n = n.saturating_add(1);
            Some(())
        }
        (ProjectionAggState::Count(n), AggregateFunc::Count) => {
            let (column, _) = aggregate_column_arg(agg)?;
            let value = value_from_column(columns.get(column)?, row)?;
            if !value.is_null() {
                *n = n.saturating_add(1);
            }
            Some(())
        }
        (ProjectionAggState::SumI64(acc), AggregateFunc::Sum) => {
            let (column, _) = aggregate_column_arg(agg)?;
            match value_from_column(columns.get(column)?, row)? {
                Value::Int32(v) => {
                    *acc = Some(acc.unwrap_or(0).wrapping_add(i64::from(v)));
                }
                Value::Int64(v) => {
                    *acc = Some(acc.unwrap_or(0).wrapping_add(v));
                }
                Value::Null => {}
                _ => return None,
            }
            Some(())
        }
        _ => None,
    }
}

fn finalise_projection_state(state: &ProjectionAggState) -> Value {
    match state {
        ProjectionAggState::Count(n) => Value::Int64(*n),
        ProjectionAggState::SumI64(Some(v)) => Value::Int64(*v),
        ProjectionAggState::SumI64(None) => Value::Null,
    }
}

fn sort_projection_rows(rows: &mut [Vec<Value>], order_by: &[CachedGroupedProjectionOrderKey]) {
    if order_by.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for key in order_by {
            let Some(left_value) = left.get(key.output_index) else {
                continue;
            };
            let Some(right_value) = right.get(key.output_index) else {
                continue;
            };
            let mut ord = compare_projection_values(left_value, right_value, key.nulls_first);
            if !key.asc {
                ord = ord.reverse();
            }
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
}

fn compare_projection_values(left: &Value, right: &Value, nulls_first: bool) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

fn value_from_column(column: &Column, row: usize) -> Option<Value> {
    match column {
        Column::Int32(c) => {
            if c.nulls().is_some_and(|nulls| !nulls.get(row)) {
                Some(Value::Null)
            } else {
                c.data().get(row).copied().map(Value::Int32)
            }
        }
        Column::Int64(c) => {
            if c.nulls().is_some_and(|nulls| !nulls.get(row)) {
                Some(Value::Null)
            } else {
                c.data().get(row).copied().map(Value::Int64)
            }
        }
        Column::Bool(c) => {
            if c.nulls().is_some_and(|nulls| !nulls.get(row)) {
                Some(Value::Null)
            } else {
                c.data().get(row).map(|value| Value::Bool(*value != 0))
            }
        }
        Column::Utf8(_) | Column::DictionaryUtf8(_) => column
            .text_value(row)
            .map(|value| Value::Text(value.to_owned()))
            .or(Some(Value::Null)),
        _ => None,
    }
}

fn rows_to_literals(rows: &[Vec<Value>], schema: &Schema) -> Vec<Vec<ScalarExpr>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(idx, value)| ScalarExpr::Literal {
                    value: value.clone(),
                    data_type: schema.field_at(idx).data_type.clone(),
                })
                .collect()
        })
        .collect()
}

fn aggregate_column_arg(agg: &LogicalAggregateExpr) -> Option<(usize, DataType)> {
    let Some(ScalarExpr::Column {
        index, data_type, ..
    }) = &agg.arg
    else {
        return None;
    };
    Some((*index, data_type.clone()))
}

fn column_index(expr: &ScalarExpr) -> Option<usize> {
    let ScalarExpr::Column { index, .. } = expr else {
        return None;
    };
    Some(*index)
}

fn is_supported_projection_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Bool | DataType::Int32 | DataType::Int64 | DataType::Text { .. }
    )
}

fn require_identity_projection(exprs: &[(ScalarExpr, String)]) -> Option<()> {
    let identity = exprs
        .iter()
        .enumerate()
        .all(|(idx, (expr, _))| matches!(expr, ScalarExpr::Column { index, .. } if *index == idx));
    identity.then_some(())
}
