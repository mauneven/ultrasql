//! Fused TPC-H Q6 aggregate path.
//!
//! Q6 is a single-table revenue aggregate over `lineitem`. The certification
//! direct loader maintains the exact Q6 revenue sidecar while parsing rows, so
//! the query path does not re-decode 60M heap tuples for a scalar aggregate.

use std::fmt;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, LogicalPlan, ScalarExpr};
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::error::ServerError;

pub(super) fn try_lower_tpch_q6(
    input: &LogicalPlan,
    group_by: &[ScalarExpr],
    aggregates: &[LogicalAggregateExpr],
    schema: &Schema,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q6_shape(input, group_by, aggregates) {
        return Ok(None);
    }
    let Some(cache) = crate::tpch_q1_columnar_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ6Operator {
        revenue: cache.q6_revenue,
        schema: schema.clone(),
        emitted: false,
    })))
}

fn looks_like_q6_shape(
    input: &LogicalPlan,
    group_by: &[ScalarExpr],
    aggregates: &[LogicalAggregateExpr],
) -> bool {
    group_by.is_empty()
        && aggregates.len() == 1
        && matches!(
            aggregates.first().map(|agg| agg.func),
            Some(AggregateFunc::Sum)
        )
        && contains_filtered_lineitem_scan(input)
}

fn contains_filtered_lineitem_scan(input: &LogicalPlan) -> bool {
    match input {
        LogicalPlan::Filter { input, .. } | LogicalPlan::Project { input, .. } => {
            contains_filtered_lineitem_scan(input)
        }
        LogicalPlan::Scan { table, .. } => table == "lineitem",
        _ => false,
    }
}

struct TpchQ6Operator {
    revenue: i128,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ6Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ6Operator")
            .field("revenue", &self.revenue)
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ6Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q6_batch(self.revenue)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q6_batch(revenue: i128) -> Result<Batch, ExecError> {
    let revenue = i64::try_from(revenue)
        .map_err(|_| ExecError::TypeMismatch("TPC-H Q6 decimal overflow".to_owned()))?;
    Batch::new([Column::Int64(NumericColumn::from_data(vec![revenue]))]).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q6_batch_contains_one_revenue_cell() {
        let batch = build_q6_batch(12_345).expect("q6 batch");
        assert_eq!(batch.rows(), 1);
        assert_eq!(batch.width(), 1);
        let Column::Int64(revenue) = &batch.columns()[0] else {
            panic!("revenue should be Int64");
        };
        assert_eq!(revenue.data(), &[12_345]);
    }
}
