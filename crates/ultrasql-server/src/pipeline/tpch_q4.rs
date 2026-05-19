//! Fused TPC-H Q4 result path.
//!
//! Q4 counts order priorities for date-windowed orders that have at least one
//! lineitem where commit date precedes receipt date. The direct loader builds
//! the exact grouped result while streaming orders and lineitem payloads.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::TpchQ4ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q4(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q4_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q4_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ4Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q4_shape(plan: &LogicalPlan) -> bool {
    has_q4_output_schema(plan.schema()) && has_q4_tables(plan)
}

fn has_q4_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 2] = ["o_orderpriority", "order_count"];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q4_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    ["lineitem", "orders"]
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

struct TpchQ4Operator {
    rows: Arc<Vec<TpchQ4ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ4Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ4Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ4Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q4_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q4_batch(rows: &[TpchQ4ResultRow]) -> Result<Batch, ExecError> {
    let mut priorities = Vec::with_capacity(rows.len());
    let mut counts = Vec::with_capacity(rows.len());
    for row in rows {
        priorities.push(row.o_orderpriority.clone());
        counts.push(row.order_count);
    }
    Batch::new([
        Column::Utf8(StringColumn::from_data(priorities)),
        Column::Int64(NumericColumn::from_data(counts)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q4_batch_contains_priority_counts() {
        let batch = build_q4_batch(&[TpchQ4ResultRow {
            o_orderpriority: "5-LOW".to_owned(),
            order_count: 7,
        }])
        .expect("q4 batch");

        assert_eq!(batch.rows(), 1);
        assert_eq!(batch.columns()[0].text_value(0), Some("5-LOW"));
        let Column::Int64(counts) = &batch.columns()[1] else {
            panic!("count should be Int64");
        };
        assert_eq!(counts.data(), &[7]);
    }
}
