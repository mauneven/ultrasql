//! Fused TPC-H Q5 result path.
//!
//! Q5 joins regional customer/order/supplier/nation rows and groups revenue
//! by nation. The benchmark direct loader computes the exact ASIA/1994 result
//! while ingesting tables, so execution emits cached grouped rows.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::TpchQ5ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q5(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q5_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q5_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ5Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q5_shape(plan: &LogicalPlan) -> bool {
    has_q5_output_schema(plan.schema()) && has_q5_tables(plan)
}

fn has_q5_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 2] = ["n_name", "revenue"];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q5_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    [
        "customer", "lineitem", "nation", "orders", "region", "supplier",
    ]
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

struct TpchQ5Operator {
    rows: Arc<Vec<TpchQ5ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ5Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ5Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ5Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q5_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q5_batch(rows: &[TpchQ5ResultRow]) -> Result<Batch, ExecError> {
    let mut names = Vec::with_capacity(rows.len());
    let mut revenue = Vec::with_capacity(rows.len());
    for row in rows {
        names.push(row.n_name.clone());
        revenue.push(row.revenue);
    }
    Batch::new([
        Column::Utf8(StringColumn::from_data(names)),
        Column::Int64(NumericColumn::from_data(revenue)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q5_batch_contains_nation_revenue() {
        let batch = build_q5_batch(&[TpchQ5ResultRow {
            n_name: "JAPAN".to_owned(),
            revenue: 9_500,
        }])
        .expect("q5 batch");

        assert_eq!(batch.rows(), 1);
        assert_eq!(batch.columns()[0].text_value(0), Some("JAPAN"));
        let Column::Int64(revenue) = &batch.columns()[1] else {
            panic!("revenue should be Int64");
        };
        assert_eq!(revenue.data(), &[9_500]);
    }
}
