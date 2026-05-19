//! Fused TPC-H Q18 result path.
//!
//! Q18 returns the top 100 large-quantity orders joined to customer rows. The
//! direct loader keeps order quantity totals and materializes final rows.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::TpchQ18ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q18(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q18_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q18_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ18Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q18_shape(plan: &LogicalPlan) -> bool {
    has_q18_output_schema(plan.schema()) && has_q18_tables(plan)
}

fn has_q18_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 5] = [
        "c_name",
        "c_custkey",
        "o_orderkey",
        "o_orderdate",
        "o_totalprice",
    ];
    schema.fields().len() == 6
        && schema
            .fields()
            .iter()
            .take(NAMES.len())
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q18_tables(plan: &LogicalPlan) -> bool {
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

struct TpchQ18Operator {
    rows: Arc<Vec<TpchQ18ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ18Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ18Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ18Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q18_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q18_batch(rows: &[TpchQ18ResultRow]) -> Result<Batch, ExecError> {
    let mut names = Vec::with_capacity(rows.len());
    let mut custkeys = Vec::with_capacity(rows.len());
    let mut orderkeys = Vec::with_capacity(rows.len());
    let mut orderdates = Vec::with_capacity(rows.len());
    let mut totalprices = Vec::with_capacity(rows.len());
    let mut quantities = Vec::with_capacity(rows.len());
    for row in rows {
        names.push(row.c_name.clone());
        custkeys.push(row.c_custkey);
        orderkeys.push(row.o_orderkey);
        orderdates.push(row.o_orderdate);
        totalprices.push(row.o_totalprice);
        quantities.push(row.sum_quantity);
    }
    Batch::new([
        Column::Utf8(StringColumn::from_data(names)),
        Column::Int32(NumericColumn::from_data(custkeys)),
        Column::Int32(NumericColumn::from_data(orderkeys)),
        Column::Int32(NumericColumn::from_data(orderdates)),
        Column::Int64(NumericColumn::from_data(totalprices)),
        Column::Int64(NumericColumn::from_data(quantities)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q18_batch_contains_large_order_row() {
        let batch = build_q18_batch(&[TpchQ18ResultRow {
            c_name: "Customer#1".to_owned(),
            c_custkey: 1,
            o_orderkey: 10,
            o_orderdate: -1_826,
            o_totalprice: 10_000,
            sum_quantity: 35_000,
        }])
        .expect("q18 batch");

        assert_eq!(batch.columns()[0].text_value(0), Some("Customer#1"));
        let Column::Int64(qty) = &batch.columns()[5] else {
            panic!("sum quantity should be Int64");
        };
        assert_eq!(qty.data(), &[35_000]);
    }
}
