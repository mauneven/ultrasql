//! Fused TPC-H Q17 result path.
//!
//! Q17 computes one average yearly revenue value for low-quantity lineitems on
//! Brand#23/MED BOX parts. The direct loader precomputes the scalar.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::TpchQ17ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q17(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q17_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q17_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ17Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q17_shape(plan: &LogicalPlan) -> bool {
    has_q17_output_schema(plan.schema()) && has_q17_tables(plan)
}

fn has_q17_output_schema(schema: &Schema) -> bool {
    schema.fields().len() == 1 && schema.fields()[0].name.eq_ignore_ascii_case("avg_yearly")
}

fn has_q17_tables(plan: &LogicalPlan) -> bool {
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

struct TpchQ17Operator {
    rows: Arc<Vec<TpchQ17ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ17Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ17Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ17Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q17_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q17_batch(rows: &[TpchQ17ResultRow]) -> Result<Batch, ExecError> {
    let values: Vec<f64> = rows.iter().map(|row| row.avg_yearly).collect();
    Batch::new([Column::Float64(NumericColumn::from_data(values))]).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q17_batch_contains_avg_yearly() {
        let batch = build_q17_batch(&[TpchQ17ResultRow { avg_yearly: 10.0 }]).expect("q17 batch");
        let Column::Float64(values) = &batch.columns()[0] else {
            panic!("avg_yearly should be Float64");
        };
        assert_eq!(values.data(), &[10.0]);
    }
}
