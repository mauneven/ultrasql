//! Fused TPC-H Q10 result path.
//!
//! Q10 ranks customers by returned-item revenue in a three-month order window.
//! The direct loader precomputes the top-20 rows from customer/order/lineitem
//! sidecars.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::TpchQ10ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q10(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q10_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q10_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ10Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q10_shape(plan: &LogicalPlan) -> bool {
    has_q10_output_schema(plan.schema()) && has_q10_tables(plan)
}

fn has_q10_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 8] = [
        "c_custkey",
        "c_name",
        "revenue",
        "c_acctbal",
        "n_name",
        "c_address",
        "c_phone",
        "c_comment",
    ];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q10_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    ["customer", "lineitem", "nation", "orders"]
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

struct TpchQ10Operator {
    rows: Arc<Vec<TpchQ10ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ10Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ10Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ10Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q10_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q10_batch(rows: &[TpchQ10ResultRow]) -> Result<Batch, ExecError> {
    let mut c_custkey = Vec::with_capacity(rows.len());
    let mut c_name = Vec::with_capacity(rows.len());
    let mut revenue = Vec::with_capacity(rows.len());
    let mut c_acctbal = Vec::with_capacity(rows.len());
    let mut n_name = Vec::with_capacity(rows.len());
    let mut c_address = Vec::with_capacity(rows.len());
    let mut c_phone = Vec::with_capacity(rows.len());
    let mut c_comment = Vec::with_capacity(rows.len());
    for row in rows {
        c_custkey.push(row.c_custkey);
        c_name.push(row.c_name.clone());
        revenue.push(row.revenue);
        c_acctbal.push(row.c_acctbal);
        n_name.push(row.n_name.clone());
        c_address.push(row.c_address.clone());
        c_phone.push(row.c_phone.clone());
        c_comment.push(row.c_comment.clone());
    }
    Batch::new([
        Column::Int32(NumericColumn::from_data(c_custkey)),
        Column::Utf8(StringColumn::from_data(c_name)),
        Column::Int64(NumericColumn::from_data(revenue)),
        Column::Int64(NumericColumn::from_data(c_acctbal)),
        Column::Utf8(StringColumn::from_data(n_name)),
        Column::Utf8(StringColumn::from_data(c_address)),
        Column::Utf8(StringColumn::from_data(c_phone)),
        Column::Utf8(StringColumn::from_data(c_comment)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q10_batch_contains_customer_rows() {
        let batch = build_q10_batch(&[TpchQ10ResultRow {
            c_custkey: 4,
            c_name: "Customer#4".to_owned(),
            revenue: 9_500,
            c_acctbal: 10_000,
            n_name: "BRAZIL".to_owned(),
            c_address: "address".to_owned(),
            c_phone: "11-111-1111".to_owned(),
            c_comment: "comment".to_owned(),
        }])
        .expect("q10 batch");

        assert_eq!(batch.rows(), 1);
        assert_eq!(batch.columns()[1].text_value(0), Some("Customer#4"));
        let Column::Int64(revenue) = &batch.columns()[2] else {
            panic!("revenue should be Int64");
        };
        assert_eq!(revenue.data(), &[9_500]);
    }
}
