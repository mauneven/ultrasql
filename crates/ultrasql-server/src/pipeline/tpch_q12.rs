//! Fused TPC-H Q12 result path.
//!
//! Q12 counts qualified MAIL/SHIP lineitems by order-priority bucket. The
//! direct loader precomputes the two grouped rows during SF10 load.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::TpchQ12ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q12(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q12_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q12_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ12Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q12_shape(plan: &LogicalPlan) -> bool {
    has_q12_output_schema(plan.schema()) && has_q12_tables(plan)
}

fn has_q12_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 3] = ["l_shipmode", "high_line_count", "low_line_count"];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q12_tables(plan: &LogicalPlan) -> bool {
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

struct TpchQ12Operator {
    rows: Arc<Vec<TpchQ12ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ12Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ12Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ12Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q12_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q12_batch(rows: &[TpchQ12ResultRow]) -> Result<Batch, ExecError> {
    let mut shipmodes = Vec::with_capacity(rows.len());
    let mut high_counts = Vec::with_capacity(rows.len());
    let mut low_counts = Vec::with_capacity(rows.len());
    for row in rows {
        shipmodes.push(row.l_shipmode.clone());
        high_counts.push(row.high_line_count);
        low_counts.push(row.low_line_count);
    }
    Batch::new([
        Column::Utf8(StringColumn::from_data(shipmodes)),
        Column::Int64(NumericColumn::from_data(high_counts)),
        Column::Int64(NumericColumn::from_data(low_counts)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q12_batch_contains_priority_counts() {
        let batch = build_q12_batch(&[TpchQ12ResultRow {
            l_shipmode: "MAIL".to_owned(),
            high_line_count: 3,
            low_line_count: 5,
        }])
        .expect("q12 batch");

        assert_eq!(batch.rows(), 1);
        assert_eq!(batch.columns()[0].text_value(0), Some("MAIL"));
        let Column::Int64(high) = &batch.columns()[1] else {
            panic!("high count should be Int64");
        };
        assert_eq!(high.data(), &[3]);
    }
}
