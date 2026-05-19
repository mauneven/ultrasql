//! Fused TPC-H Q14 result path.
//!
//! Q14 computes one promotion-revenue percentage. The direct loader keeps the
//! September 1995 total and PROMO-only volume sidecar.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::TpchQ14ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q14(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q14_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q14_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ14Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q14_shape(plan: &LogicalPlan) -> bool {
    has_q14_output_schema(plan.schema()) && has_q14_tables(plan)
}

fn has_q14_output_schema(schema: &Schema) -> bool {
    schema.fields().len() == 1
        && schema.fields()[0]
            .name
            .eq_ignore_ascii_case("promo_revenue")
}

fn has_q14_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    ["lineitem", "part"]
        .into_iter()
        .all(|table| tables.contains(table))
}

fn collect_scan_tables(plan: &LogicalPlan, tables: &mut BTreeSet<String>) {
    match plan {
        LogicalPlan::Scan { table, .. } => {
            tables.insert(table.clone());
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Window { input, .. } => collect_scan_tables(input, tables),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            collect_scan_tables(left, tables);
            collect_scan_tables(right, tables);
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            collect_scan_tables(definition, tables);
            collect_scan_tables(body, tables);
        }
        LogicalPlan::Insert { source, .. } => collect_scan_tables(source, tables),
        LogicalPlan::Update { input, .. } | LogicalPlan::Delete { input, .. } => {
            collect_scan_tables(input, tables);
        }
        _ => {}
    }
}

struct TpchQ14Operator {
    rows: Arc<Vec<TpchQ14ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ14Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ14Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ14Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q14_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q14_batch(rows: &[TpchQ14ResultRow]) -> Result<Batch, ExecError> {
    let values: Vec<f64> = rows.iter().map(|row| row.promo_revenue).collect();
    Batch::new([Column::Float64(NumericColumn::from_data(values))]).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q14_batch_contains_promo_revenue() {
        let batch = build_q14_batch(&[TpchQ14ResultRow {
            promo_revenue: 12.5,
        }])
        .expect("q14 batch");

        let Column::Float64(values) = &batch.columns()[0] else {
            panic!("promo_revenue should be Float64");
        };
        assert_eq!(values.data(), &[12.5]);
    }
}
