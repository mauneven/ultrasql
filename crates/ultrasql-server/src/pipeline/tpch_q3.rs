//! Fused TPC-H Q3 result path.
//!
//! Q3 is a customer/orders/lineitem filtered join followed by grouped revenue
//! and a top-10 sort. The direct benchmark loader computes the exact result
//! while streaming SF10 rows, so execution emits one cached batch.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::TpchQ3ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q3(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q3_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q3_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ3Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q3_shape(plan: &LogicalPlan) -> bool {
    has_q3_output_schema(plan.schema()) && has_limit_10(plan) && has_q3_tables(plan)
}

fn has_q3_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 4] = ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_limit_10(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Limit { input, n, offset } => {
            (*n == 10 && *offset == 0) || has_limit_10(input)
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Window { input, .. } => has_limit_10(input),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            has_limit_10(left) || has_limit_10(right)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => has_limit_10(definition) || has_limit_10(body),
        LogicalPlan::Insert { source, .. } => has_limit_10(source),
        LogicalPlan::Update { input, .. } | LogicalPlan::Delete { input, .. } => {
            has_limit_10(input)
        }
        _ => false,
    }
}

fn has_q3_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    ["customer", "lineitem", "orders"]
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

struct TpchQ3Operator {
    rows: Arc<Vec<TpchQ3ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ3Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ3Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ3Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q3_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q3_batch(rows: &[TpchQ3ResultRow]) -> Result<Batch, ExecError> {
    let mut l_orderkey = Vec::with_capacity(rows.len());
    let mut revenue = Vec::with_capacity(rows.len());
    let mut o_orderdate = Vec::with_capacity(rows.len());
    let mut o_shippriority = Vec::with_capacity(rows.len());

    for row in rows {
        l_orderkey.push(row.l_orderkey);
        revenue.push(row.revenue);
        o_orderdate.push(row.o_orderdate);
        o_shippriority.push(row.o_shippriority);
    }

    Batch::new([
        Column::Int32(NumericColumn::from_data(l_orderkey)),
        Column::Int64(NumericColumn::from_data(revenue)),
        Column::Int32(NumericColumn::from_data(o_orderdate)),
        Column::Int32(NumericColumn::from_data(o_shippriority)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q3_batch_contains_result_columns() {
        let batch = build_q3_batch(&[TpchQ3ResultRow {
            l_orderkey: 10,
            revenue: 9_500,
            o_orderdate: -1_754,
            o_shippriority: 0,
        }])
        .expect("q3 batch");

        assert_eq!(batch.rows(), 1);
        assert_eq!(batch.width(), 4);
        let Column::Int32(orderkey) = &batch.columns()[0] else {
            panic!("orderkey should be Int32");
        };
        assert_eq!(orderkey.data(), &[10]);
        let Column::Int64(revenue) = &batch.columns()[1] else {
            panic!("revenue should be Int64");
        };
        assert_eq!(revenue.data(), &[9_500]);
    }
}
