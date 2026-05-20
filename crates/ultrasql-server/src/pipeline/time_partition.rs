//! Lowering helpers for native time-range partitioned tables.

use ultrasql_core::{DataType, Value};
use ultrasql_executor::{Filter, Operator, Project};
use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr};

use crate::error::ServerError;

use super::LowerCtx;
use super::scan::lower_heap_scan;

#[derive(Clone, Copy, Debug, Default)]
struct TimeRangePredicate {
    lower: Option<i64>,
    upper: Option<(i64, bool)>,
}

pub(super) fn try_lower_time_partition_scan(
    table: &str,
    projection: Option<&[usize]>,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    lower_time_partition_scan(table, projection, ctx, None)
}

pub(super) fn try_lower_time_partition_filter_scan(
    input: &LogicalPlan,
    predicate: &ScalarExpr,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let LogicalPlan::Scan {
        table, projection, ..
    } = input
    else {
        return Ok(None);
    };
    let Some(scan) = lower_time_partition_scan(
        table,
        projection.as_deref(),
        ctx,
        TimeRangePredicate::from_expr(predicate, partition_column_index(table, ctx)).as_ref(),
    )?
    else {
        return Ok(None);
    };
    Ok(Some(Box::new(Filter::new(scan, predicate.clone()))))
}

fn partition_column_index(table: &str, ctx: &LowerCtx<'_>) -> Option<usize> {
    ctx.time_partitions
        .get(&table.to_ascii_lowercase())
        .map(|runtime| runtime.partition_column_index)
}

fn lower_time_partition_scan(
    table: &str,
    projection: Option<&[usize]>,
    ctx: &LowerCtx<'_>,
    predicate: Option<&TimeRangePredicate>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let Some(runtime) = ctx.time_partitions.get(&table.to_ascii_lowercase()) else {
        return Ok(None);
    };
    let mut chunks = runtime
        .chunks
        .iter()
        .filter(|chunk| {
            predicate.is_none_or(|range| range.overlaps_chunk(chunk.start_us, chunk.end_us))
        })
        .map(|chunk| chunk.value().clone())
        .collect::<Vec<_>>();
    runtime
        .last_scan_total_chunks
        .store(runtime.chunks.len(), std::sync::atomic::Ordering::Release);
    runtime
        .last_scan_selected_chunks
        .store(chunks.len(), std::sync::atomic::Ordering::Release);
    chunks.sort_by_key(|chunk| chunk.start_us);

    let mut children = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        if let Some(entry) = ctx.catalog_snapshot.tables.get(&chunk.table_name) {
            children.push(lower_heap_scan(entry, None, ctx)?);
        }
    }
    let parent_schema = runtime.schema.clone();
    let scan: Box<dyn Operator> = Box::new(crate::time_partition::AppendScan::new(
        children,
        parent_schema,
    ));
    apply_projection(scan, projection).map(Some)
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

impl TimeRangePredicate {
    fn from_expr(expr: &ScalarExpr, partition_column_index: Option<usize>) -> Option<Self> {
        let partition_column_index = partition_column_index?;
        let mut range = Self::default();
        range
            .apply_expr(expr, partition_column_index)
            .then_some(range)
    }

    fn apply_expr(&mut self, expr: &ScalarExpr, partition_column_index: usize) -> bool {
        match expr {
            ScalarExpr::Binary {
                op: BinaryOp::And,
                left,
                right,
                ..
            } => {
                self.apply_expr(left, partition_column_index)
                    & self.apply_expr(right, partition_column_index)
            }
            ScalarExpr::Binary {
                op, left, right, ..
            } => self.apply_comparison(*op, left, right, partition_column_index),
            _ => false,
        }
    }

    fn apply_comparison(
        &mut self,
        op: BinaryOp,
        left: &ScalarExpr,
        right: &ScalarExpr,
        partition_column_index: usize,
    ) -> bool {
        if let Some((cmp, value)) = column_literal_cmp(op, left, right, partition_column_index) {
            self.apply_bound(cmp, value);
            return true;
        }
        if let Some((cmp, value)) =
            column_literal_cmp(reverse_op(op), right, left, partition_column_index)
        {
            self.apply_bound(cmp, value);
            return true;
        }
        false
    }

    fn apply_bound(&mut self, op: BinaryOp, value: i64) {
        match op {
            BinaryOp::Eq => {
                self.lower = Some(self.lower.map_or(value, |old| old.max(value)));
                self.upper = Some(match self.upper {
                    Some((old, inclusive)) if old < value => (old, inclusive),
                    _ => (value, true),
                });
            }
            BinaryOp::Gt | BinaryOp::GtEq => {
                self.lower = Some(self.lower.map_or(value, |old| old.max(value)));
            }
            BinaryOp::Lt => {
                self.upper = Some(match self.upper {
                    Some((old, inclusive)) if old < value => (old, inclusive),
                    _ => (value, false),
                });
            }
            BinaryOp::LtEq => {
                self.upper = Some(match self.upper {
                    Some((old, inclusive)) if old < value => (old, inclusive),
                    _ => (value, true),
                });
            }
            _ => {}
        }
    }

    fn overlaps_chunk(&self, chunk_start: i64, chunk_end: i64) -> bool {
        if self.lower.is_some_and(|lower| chunk_end <= lower) {
            return false;
        }
        if let Some((upper, inclusive)) = self.upper {
            if inclusive {
                if chunk_start > upper {
                    return false;
                }
            } else if chunk_start >= upper {
                return false;
            }
        }
        true
    }
}

fn column_literal_cmp(
    op: BinaryOp,
    left: &ScalarExpr,
    right: &ScalarExpr,
    partition_column_index: usize,
) -> Option<(BinaryOp, i64)> {
    if !matches!(
        op,
        BinaryOp::Eq | BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
    ) {
        return None;
    }
    let ScalarExpr::Column { index, .. } = left else {
        return None;
    };
    if *index != partition_column_index {
        return None;
    }
    Some((op, literal_timestamp_us(right)?))
}

fn literal_timestamp_us(expr: &ScalarExpr) -> Option<i64> {
    let ScalarExpr::Literal {
        value, data_type, ..
    } = expr
    else {
        return None;
    };
    match (value, data_type) {
        (Value::Timestamp(v), DataType::Timestamp)
        | (Value::TimestampTz(v), DataType::TimestampTz)
        | (Value::Timestamp(v), DataType::TimestampTz)
        | (Value::TimestampTz(v), DataType::Timestamp) => Some(*v),
        _ => None,
    }
}

fn reverse_op(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        other => other,
    }
}
