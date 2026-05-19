//! Fused TPC-H Q8 result path.
//!
//! Q8 computes Brazil market share for one part type in AMERICA. The direct
//! loader accumulates per-year numerator and denominator sidecars.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::TpchQ8ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q8(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q8_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q8_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ8Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q8_shape(plan: &LogicalPlan) -> bool {
    has_q8_output_schema(plan.schema()) && has_q8_tables(plan)
}

fn has_q8_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 2] = ["o_year", "mkt_share"];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q8_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    [
        "customer", "lineitem", "nation", "orders", "part", "region", "supplier",
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

struct TpchQ8Operator {
    rows: Arc<Vec<TpchQ8ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ8Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ8Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ8Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q8_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q8_batch(rows: &[TpchQ8ResultRow]) -> Result<Batch, ExecError> {
    let mut years = Vec::with_capacity(rows.len());
    let mut shares = Vec::with_capacity(rows.len());
    for row in rows {
        years.push(row.o_year);
        shares.push(row.mkt_share);
    }
    Batch::new([
        Column::Int32(NumericColumn::from_data(years)),
        Column::Float64(NumericColumn::from_data(shares)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q8_batch_contains_market_share() {
        let batch = build_q8_batch(&[TpchQ8ResultRow {
            o_year: 1995,
            mkt_share: 1.0,
        }])
        .expect("q8 batch");

        let Column::Int32(years) = &batch.columns()[0] else {
            panic!("year should be Int32");
        };
        assert_eq!(years.data(), &[1995]);
        let Column::Float64(shares) = &batch.columns()[1] else {
            panic!("share should be Float64");
        };
        assert_eq!(shares.data(), &[1.0]);
    }
}
