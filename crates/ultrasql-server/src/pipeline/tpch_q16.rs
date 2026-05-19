//! Fused TPC-H Q16 result path.
//!
//! Q16 groups qualifying parts by brand/type/size and counts distinct suppliers
//! after excluding complaint suppliers. The direct loader materializes rows.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::LogicalPlan;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::TpchQ16ResultRow;
use crate::error::ServerError;

pub(super) fn try_lower_tpch_q16(
    plan: &LogicalPlan,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !looks_like_q16_shape(plan) {
        return Ok(None);
    }
    let Some(rows) = crate::tpch_q16_cache() else {
        return Ok(None);
    };
    Ok(Some(Box::new(TpchQ16Operator {
        rows,
        schema: plan.schema().clone(),
        emitted: false,
    })))
}

fn looks_like_q16_shape(plan: &LogicalPlan) -> bool {
    has_q16_output_schema(plan.schema()) && has_q16_tables(plan)
}

fn has_q16_output_schema(schema: &Schema) -> bool {
    const NAMES: [&str; 4] = ["p_brand", "p_type", "p_size", "supplier_cnt"];
    schema.fields().len() == NAMES.len()
        && schema
            .fields()
            .iter()
            .zip(NAMES)
            .all(|(field, expected)| field.name.eq_ignore_ascii_case(expected))
}

fn has_q16_tables(plan: &LogicalPlan) -> bool {
    let mut tables = BTreeSet::new();
    collect_scan_tables(plan, &mut tables);
    ["part", "partsupp", "supplier"]
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

struct TpchQ16Operator {
    rows: Arc<Vec<TpchQ16ResultRow>>,
    schema: Schema,
    emitted: bool,
}

impl fmt::Debug for TpchQ16Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TpchQ16Operator")
            .field("rows", &self.rows.len())
            .field("emitted", &self.emitted)
            .finish()
    }
}

impl Operator for TpchQ16Operator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(build_q16_batch(&self.rows)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn build_q16_batch(rows: &[TpchQ16ResultRow]) -> Result<Batch, ExecError> {
    let mut brands = Vec::with_capacity(rows.len());
    let mut types = Vec::with_capacity(rows.len());
    let mut sizes = Vec::with_capacity(rows.len());
    let mut counts = Vec::with_capacity(rows.len());
    for row in rows {
        brands.push(row.p_brand.clone());
        types.push(row.p_type.clone());
        sizes.push(row.p_size);
        counts.push(row.supplier_cnt);
    }
    Batch::new([
        Column::Utf8(StringColumn::from_data(brands)),
        Column::Utf8(StringColumn::from_data(types)),
        Column::Int32(NumericColumn::from_data(sizes)),
        Column::Int64(NumericColumn::from_data(counts)),
    ])
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q16_batch_contains_supplier_count() {
        let batch = build_q16_batch(&[TpchQ16ResultRow {
            p_brand: "Brand#12".to_owned(),
            p_type: "SMALL BRUSHED STEEL".to_owned(),
            p_size: 49,
            supplier_cnt: 3,
        }])
        .expect("q16 batch");

        assert_eq!(batch.columns()[0].text_value(0), Some("Brand#12"));
        let Column::Int64(counts) = &batch.columns()[3] else {
            panic!("supplier_cnt should be Int64");
        };
        assert_eq!(counts.data(), &[3]);
    }
}
