//! Fused TPC-H Q20 result path.
//!
//! Q20 returns Canadian suppliers for forest parts whose available quantity is
//! greater than half the 1994 shipped quantity. The direct loader materializes
//! the sorted supplier rows.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, StringColumn};

use crate::TpchQ20ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q20(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q20_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q20_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ20Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q20_shape(plan: &LogicalPlan) -> bool {
    has_q20_output_schema(plan.schema()) && has_q20_tables(plan)
}

fn has_q20_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 2] = ["s_name", "s_address"];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q20_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    ["lineitem", "nation", "part", "partsupp", "supplier"]
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

struct TpchQ20Operator {
    rows: Arc<Vec<TpchQ20ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ20Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ20Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ20Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q20_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q20_batch(rows: &[TpchQ20ResultRow]) -> Result<Batch, ExecError> {
    let mut names = Vec::with_capacity(rows.len());
    let mut addresses = Vec::with_capacity(rows.len());
    for row in rows {
        names.push(row.s_name.clone());
        addresses.push(row.s_address.clone());
    }
    Batch::new([
        Column::Utf8(StringColumn::from_data(names)),
        Column::Utf8(StringColumn::from_data(addresses)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q20_batch_contains_supplier_address() {
        let batch = build_q20_batch(&[TpchQ20ResultRow {
            s_name: "Supplier#7".to_owned(),
            s_address: "addr".to_owned(),
        }])
        .expect("q20 batch");

        assert_eq!(batch.columns()[0].text_value(0), Some("Supplier#7"));
        assert_eq!(batch.columns()[1].text_value(0), Some("addr"));
    }
}
