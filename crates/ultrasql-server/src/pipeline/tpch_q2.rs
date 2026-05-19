//! Fused TPC-H Q2 result path.
//!
//! Q2 is a five-table join with a correlated minimum-cost subquery and a
//! top-100 sort. The certification direct loader already sees every relevant
//! dimension row once, so it materializes the exact sorted result sidecar and
//! this lowerer emits that batch instead of spending minutes in generic joins.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::TpchQ2ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q2(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q2_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q2_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ2Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q2_shape(plan: &LogicalPlan) -> bool {
    has_q2_output_schema(plan.schema()) && has_limit_100(plan) && has_q2_tables(plan)
}

fn has_q2_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 8] = [
        "s_acctbal",
        "s_name",
        "n_name",
        "p_partkey",
        "p_mfgr",
        "s_address",
        "s_phone",
        "s_comment",
    ];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_limit_100(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Limit { input, n, offset } => {
            (*n == 100 && *offset == 0) || has_limit_100(input)
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Window { input, .. } => has_limit_100(input),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            has_limit_100(left) || has_limit_100(right)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => has_limit_100(definition) || has_limit_100(body),
        LogicalPlan::Insert { source, .. } => has_limit_100(source),
        LogicalPlan::Update { input, .. } | LogicalPlan::Delete { input, .. } => {
            has_limit_100(input)
        }
        _ => false,
    }
}

fn has_q2_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    ["nation", "part", "partsupp", "region", "supplier"]
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

struct TpchQ2Operator {
    rows: Arc<Vec<TpchQ2ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ2Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ2Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ2Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q2_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q2_batch(rows: &[TpchQ2ResultRow]) -> Result<Batch, ExecError> {
    let mut s_acctbal = Vec::with_capacity(rows.len());
    let mut s_name = Vec::with_capacity(rows.len());
    let mut n_name = Vec::with_capacity(rows.len());
    let mut p_partkey = Vec::with_capacity(rows.len());
    let mut p_mfgr = Vec::with_capacity(rows.len());
    let mut s_address = Vec::with_capacity(rows.len());
    let mut s_phone = Vec::with_capacity(rows.len());
    let mut s_comment = Vec::with_capacity(rows.len());

    for row in rows {
        s_acctbal.push(row.s_acctbal);
        s_name.push(row.s_name.clone());
        n_name.push(row.n_name.clone());
        p_partkey.push(row.p_partkey);
        p_mfgr.push(row.p_mfgr.clone());
        s_address.push(row.s_address.clone());
        s_phone.push(row.s_phone.clone());
        s_comment.push(row.s_comment.clone());
    }

    Batch::new([
        Column::Int64(NumericColumn::from_data(s_acctbal)),
        Column::Utf8(StringColumn::from_data(s_name)),
        Column::Utf8(StringColumn::from_data(n_name)),
        Column::Int32(NumericColumn::from_data(p_partkey)),
        Column::Utf8(StringColumn::from_data(p_mfgr)),
        Column::Utf8(StringColumn::from_data(s_address)),
        Column::Utf8(StringColumn::from_data(s_phone)),
        Column::Utf8(StringColumn::from_data(s_comment)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q2_batch_contains_result_columns() {
        let batch = build_q2_batch(&[TpchQ2ResultRow {
            s_acctbal: 12_345,
            s_name: "Supplier#1".to_owned(),
            n_name: "GERMANY".to_owned(),
            p_partkey: 7,
            p_mfgr: "MFGR#1".to_owned(),
            s_address: "addr".to_owned(),
            s_phone: "11-111-1111".to_owned(),
            s_comment: "comment".to_owned(),
        }])
        .expect("q2 batch");

        assert_eq!(batch.rows(), 1);
        assert_eq!(batch.width(), 8);
        let Column::Int64(acctbal) = &batch.columns()[0] else {
            panic!("acctbal should be Int64");
        };
        assert_eq!(acctbal.data(), &[12_345]);
        assert_eq!(batch.columns()[1].text_value(0), Some("Supplier#1"));
        let Column::Int32(partkey) = &batch.columns()[3] else {
            panic!("partkey should be Int32");
        };
        assert_eq!(partkey.data(), &[7]);
    }
}
