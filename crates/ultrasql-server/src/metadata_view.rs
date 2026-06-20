//! Materialized- and regular-view sidecar metadata records and helpers.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

pub(crate) fn materialized_view_projection_indices(plan: &LogicalPlan) -> Option<Vec<usize>> {
    match plan {
        LogicalPlan::Scan { schema, .. } => Some((0..schema.fields().len()).collect()),
        LogicalPlan::Project { input, exprs, .. }
            if matches!(input.as_ref(), LogicalPlan::Scan { .. }) =>
        {
            exprs
                .iter()
                .map(|(expr, _)| match expr {
                    ScalarExpr::Column { index, .. } => Some(*index),
                    _ => None,
                })
                .collect()
        }
        _ => None,
    }
}

pub(crate) fn materialized_view_source_plan_from_metadata(
    source_entry: &TableEntry,
    view_entry: &TableEntry,
    record: &MaterializedViewMetadataRecord,
) -> Option<LogicalPlan> {
    let source_scan = LogicalPlan::Scan {
        table: record.source_table.clone(),
        schema: source_entry.schema.clone(),
        projection: None,
    };
    let source_width = source_entry.schema.fields().len();
    let full_projection = record.projection.len() == source_width
        && record
            .projection
            .iter()
            .enumerate()
            .all(|(idx, projected)| idx == *projected);
    if full_projection && view_entry.schema == source_entry.schema {
        return Some(source_scan);
    }
    if record.projection.len() != view_entry.schema.fields().len() {
        return None;
    }
    let mut exprs = Vec::with_capacity(record.projection.len());
    for (out_idx, source_idx) in record.projection.iter().copied().enumerate() {
        let source_field = source_entry.schema.fields().get(source_idx)?;
        let output_field = view_entry.schema.fields().get(out_idx)?;
        if source_field.data_type != output_field.data_type {
            return None;
        }
        exprs.push((
            ScalarExpr::Column {
                name: output_field.name.clone(),
                index: source_idx,
                data_type: source_field.data_type.clone(),
            },
            output_field.name.clone(),
        ));
    }
    Some(LogicalPlan::Project {
        input: Box::new(source_scan),
        exprs,
        schema: view_entry.schema.clone(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaterializedViewMetadataRecord {
    pub(crate) view_table: String,
    pub(crate) view_oid: ultrasql_core::Oid,
    pub(crate) source_table: String,
    pub(crate) source_oid: ultrasql_core::Oid,
    pub(crate) materialized_rows: u64,
    pub(crate) projection: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegularViewMetadataRecord {
    pub(crate) view_table: String,
    pub(crate) view_oid: ultrasql_core::Oid,
    pub(crate) source_sql: String,
    pub(crate) search_path: Option<String>,
}
