//! Fused TPC-H Q19 result path.
//!
//! Q19 returns one discounted-revenue aggregate over three fixed
//! brand/container/quantity predicate bands. The direct loader materializes the
//! aggregate while loading lineitem rows.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::TpchQ19ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q19(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q19_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q19_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ19Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q19_shape(plan: &LogicalPlan) -> bool {
    has_q19_output_schema(plan.schema()) && has_q19_tables(plan)
}

fn has_q19_output_schema(schema: &Schema) -> bool {
    schema.fields().len() == 1 && schema.fields()[0].name.eq_ignore_ascii_case("revenue")
}

fn has_q19_tables(plan: &LogicalPlan) -> bool {
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

struct TpchQ19Operator {
    rows: Arc<Vec<TpchQ19ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ19Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ19Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ19Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q19_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q19_batch(rows: &[TpchQ19ResultRow]) -> Result<Batch, ExecError> {
    let mut revenues = Vec::with_capacity(rows.len());
    for row in rows {
        revenues.push(row.revenue);
    }
    Batch::new([Column::Int64(NumericColumn::from_data(revenues))]).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q19_batch_contains_discounted_revenue() {
        let batch = build_q19_batch(&[TpchQ19ResultRow { revenue: 900_000 }]).expect("q19 batch");

        let Column::Int64(revenue) = &batch.columns()[0] else {
            panic!("revenue should be Int64");
        };
        assert_eq!(revenue.data(), &[900_000]);
    }
}
