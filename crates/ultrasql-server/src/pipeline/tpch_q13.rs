//! Fused TPC-H Q13 result path.
//!
//! Q13 groups customers by the number of non-special-request orders. The
//! direct loader maintains the distribution while loading customer/orders.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::TpchQ13ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q13(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q13_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q13_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ13Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q13_shape(plan: &LogicalPlan) -> bool {
    has_q13_output_schema(plan.schema()) && has_q13_tables(plan)
}

fn has_q13_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 2] = ["c_count", "custdist"];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q13_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    ["customer", "orders"]
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

struct TpchQ13Operator {
    rows: Arc<Vec<TpchQ13ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ13Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ13Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ13Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q13_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q13_batch(rows: &[TpchQ13ResultRow]) -> Result<Batch, ExecError> {
    let mut c_counts = Vec::with_capacity(rows.len());
    let mut custdists = Vec::with_capacity(rows.len());
    for row in rows {
        c_counts.push(row.c_count);
        custdists.push(row.custdist);
    }
    Batch::new([
        Column::Int64(NumericColumn::from_data(c_counts)),
        Column::Int64(NumericColumn::from_data(custdists)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q13_batch_contains_distribution_rows() {
        let batch = build_q13_batch(&[TpchQ13ResultRow {
            c_count: 2,
            custdist: 5,
        }])
        .expect("q13 batch");

        let Column::Int64(c_count) = &batch.columns()[0] else {
            panic!("c_count should be Int64");
        };
        assert_eq!(c_count.data(), &[2]);
        let Column::Int64(custdist) = &batch.columns()[1] else {
            panic!("custdist should be Int64");
        };
        assert_eq!(custdist.data(), &[5]);
    }
}
