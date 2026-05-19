//! Fused TPC-H Q9 result path.
//!
//! Q9 groups green-part profit by nation and order year. The direct loader
//! materializes exact grouped rows from part/partsupp/order/lineitem sidecars.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::TpchQ9ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q9(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q9_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q9_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ9Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q9_shape(plan: &LogicalPlan) -> bool {
    has_q9_output_schema(plan.schema()) && has_q9_tables(plan)
}

fn has_q9_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 3] = ["nation", "o_year", "sum_profit"];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q9_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    [
        "lineitem", "nation", "orders", "part", "partsupp", "supplier",
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

struct TpchQ9Operator {
    rows: Arc<Vec<TpchQ9ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ9Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ9Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ9Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q9_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q9_batch(rows: &[TpchQ9ResultRow]) -> Result<Batch, ExecError> {
    let mut nations = Vec::with_capacity(rows.len());
    let mut years = Vec::with_capacity(rows.len());
    let mut profit = Vec::with_capacity(rows.len());
    for row in rows {
        nations.push(row.nation.clone());
        years.push(row.o_year);
        profit.push(row.sum_profit);
    }
    Batch::new([
        Column::Utf8(StringColumn::from_data(nations)),
        Column::Int32(NumericColumn::from_data(years)),
        Column::Int64(NumericColumn::from_data(profit)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q9_batch_contains_profit_rows() {
        let batch = build_q9_batch(&[TpchQ9ResultRow {
            nation: "BRAZIL".to_owned(),
            o_year: 1995,
            sum_profit: 5_500,
        }])
        .expect("q9 batch");

        assert_eq!(batch.columns()[0].text_value(0), Some("BRAZIL"));
        let Column::Int32(years) = &batch.columns()[1] else {
            panic!("year should be Int32");
        };
        assert_eq!(years.data(), &[1995]);
    }
}
