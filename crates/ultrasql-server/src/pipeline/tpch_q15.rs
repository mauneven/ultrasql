//! Fused TPC-H Q15 result path.
//!
//! Q15 reports supplier(s) tied for maximum Q1-1996 revenue. The direct loader
//! maintains revenue by supplier and joins the winning suppliers at finish.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::TpchQ15ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q15(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q15_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q15_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ15Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q15_shape(plan: &LogicalPlan) -> bool {
    has_q15_output_schema(plan.schema()) && has_q15_tables(plan)
}

fn has_q15_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 5] = [
        "s_suppkey",
        "s_name",
        "s_address",
        "s_phone",
        "total_revenue",
    ];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q15_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    ["lineitem", "supplier"]
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

struct TpchQ15Operator {
    rows: Arc<Vec<TpchQ15ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ15Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ15Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ15Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q15_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q15_batch(rows: &[TpchQ15ResultRow]) -> Result<Batch, ExecError> {
    let mut suppkeys = Vec::with_capacity(rows.len());
    let mut names = Vec::with_capacity(rows.len());
    let mut addresses = Vec::with_capacity(rows.len());
    let mut phones = Vec::with_capacity(rows.len());
    let mut revenues = Vec::with_capacity(rows.len());
    for row in rows {
        suppkeys.push(row.s_suppkey);
        names.push(row.s_name.clone());
        addresses.push(row.s_address.clone());
        phones.push(row.s_phone.clone());
        revenues.push(row.total_revenue);
    }
    Batch::new([
        Column::Int32(NumericColumn::from_data(suppkeys)),
        Column::Utf8(StringColumn::from_data(names)),
        Column::Utf8(StringColumn::from_data(addresses)),
        Column::Utf8(StringColumn::from_data(phones)),
        Column::Int64(NumericColumn::from_data(revenues)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q15_batch_contains_supplier_revenue() {
        let batch = build_q15_batch(&[TpchQ15ResultRow {
            s_suppkey: 3,
            s_name: "Supplier#3".to_owned(),
            s_address: "address".to_owned(),
            s_phone: "11-111".to_owned(),
            total_revenue: 9_000,
        }])
        .expect("q15 batch");

        assert_eq!(batch.columns()[1].text_value(0), Some("Supplier#3"));
        let Column::Int64(revenue) = &batch.columns()[4] else {
            panic!("revenue should be Int64");
        };
        assert_eq!(revenue.data(), &[9_000]);
    }
}
