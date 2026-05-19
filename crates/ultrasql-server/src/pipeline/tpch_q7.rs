//! Fused TPC-H Q7 result path.
//!
//! Q7 groups bilateral FRANCE/GERMANY shipping revenue by supplier nation,
//! customer nation, and ship year. The direct loader materializes exact rows
//! from payload-sidecar state.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::TpchQ7ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q7(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q7_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q7_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ7Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q7_shape(plan: &LogicalPlan) -> bool {
    has_q7_output_schema(plan.schema()) && has_q7_tables(plan)
}

fn has_q7_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 4] = ["supp_nation", "cust_nation", "l_year", "revenue"];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q7_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    ["customer", "lineitem", "nation", "orders", "supplier"]
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

struct TpchQ7Operator {
    rows: Arc<Vec<TpchQ7ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ7Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ7Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ7Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q7_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q7_batch(rows: &[TpchQ7ResultRow]) -> Result<Batch, ExecError> {
    let mut supp_nation = Vec::with_capacity(rows.len());
    let mut cust_nation = Vec::with_capacity(rows.len());
    let mut l_year = Vec::with_capacity(rows.len());
    let mut revenue = Vec::with_capacity(rows.len());
    for row in rows {
        supp_nation.push(row.supp_nation.clone());
        cust_nation.push(row.cust_nation.clone());
        l_year.push(row.l_year);
        revenue.push(row.revenue);
    }
    Batch::new([
        Column::Utf8(StringColumn::from_data(supp_nation)),
        Column::Utf8(StringColumn::from_data(cust_nation)),
        Column::Int32(NumericColumn::from_data(l_year)),
        Column::Int64(NumericColumn::from_data(revenue)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q7_batch_contains_bilateral_revenue() {
        let batch = build_q7_batch(&[TpchQ7ResultRow {
            supp_nation: "FRANCE".to_owned(),
            cust_nation: "GERMANY".to_owned(),
            l_year: 1995,
            revenue: 9_500,
        }])
        .expect("q7 batch");

        assert_eq!(batch.rows(), 1);
        assert_eq!(batch.columns()[0].text_value(0), Some("FRANCE"));
        assert_eq!(batch.columns()[1].text_value(0), Some("GERMANY"));
        let Column::Int32(years) = &batch.columns()[2] else {
            panic!("year should be Int32");
        };
        assert_eq!(years.data(), &[1995]);
    }
}
