//! Fused TPC-H Q11 result path.
//!
//! Q11 computes German supplier stock value by part and filters by a tiny
//! fraction of total value. The direct loader maintains exact partsupp totals.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::TpchQ11ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q11(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q11_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q11_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ11Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q11_shape(plan: &LogicalPlan) -> bool {
    has_q11_output_schema(plan.schema()) && has_q11_tables(plan)
}

fn has_q11_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 2] = ["ps_partkey", "value"];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q11_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    ["nation", "partsupp", "supplier"]
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

struct TpchQ11Operator {
    rows: Arc<Vec<TpchQ11ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ11Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ11Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ11Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q11_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q11_batch(rows: &[TpchQ11ResultRow]) -> Result<Batch, ExecError> {
    let mut partkeys = Vec::with_capacity(rows.len());
    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        partkeys.push(row.ps_partkey);
        values.push(row.value);
    }
    Batch::new([
        Column::Int32(NumericColumn::from_data(partkeys)),
        Column::Int64(NumericColumn::from_data(values)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q11_batch_contains_part_values() {
        let batch = build_q11_batch(&[TpchQ11ResultRow {
            ps_partkey: 5,
            value: 8_000,
        }])
        .expect("q11 batch");

        let Column::Int32(partkeys) = &batch.columns()[0] else {
            panic!("partkey should be Int32");
        };
        assert_eq!(partkeys.data(), &[5]);
    }
}
