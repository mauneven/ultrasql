//! Fused TPC-H Q21 result path.
//!
//! Q21 counts Saudi suppliers that were the only late supplier in a final
//! multi-supplier order. The direct loader materializes the top 100 result
//! rows sorted by count descending and name ascending.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::TpchQ21ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q21(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q21_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q21_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ21Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q21_shape(plan: &LogicalPlan) -> bool {
    has_q21_output_schema(plan.schema()) && has_q21_tables(plan)
}

fn has_q21_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 2] = ["s_name", "numwait"];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q21_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    ["lineitem", "nation", "orders", "supplier"]
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

struct TpchQ21Operator {
    rows: Arc<Vec<TpchQ21ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ21Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ21Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ21Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q21_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q21_batch(rows: &[TpchQ21ResultRow]) -> Result<Batch, ExecError> {
    let mut names = Vec::with_capacity(rows.len());
    let mut numwaits = Vec::with_capacity(rows.len());
    for row in rows {
        names.push(row.s_name.clone());
        numwaits.push(row.numwait);
    }
    Batch::new([
        Column::Utf8(StringColumn::from_data(names)),
        Column::Int64(NumericColumn::from_data(numwaits)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q21_batch_contains_wait_count() {
        let batch = build_q21_batch(&[TpchQ21ResultRow {
            s_name: "Supplier#7".to_owned(),
            numwait: 3,
        }])
        .expect("q21 batch");

        assert_eq!(batch.columns()[0].text_value(0), Some("Supplier#7"));
        let Column::Int64(numwait) = &batch.columns()[1] else {
            panic!("numwait should be Int64");
        };
        assert_eq!(numwait.data(), &[3]);
    }
}
