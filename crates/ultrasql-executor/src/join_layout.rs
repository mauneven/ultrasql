//! Shared join output-layout helpers.
//!
//! `USING` and `NATURAL` joins evaluate predicates against the physical
//! `left ++ right` row, then expose a collapsed logical schema where shared
//! columns appear once. These helpers keep that column routing identical
//! between the standalone physical builder and the server pipeline lowerer.

use std::collections::HashSet;

use ultrasql_core::{Field, Result, Schema};
use ultrasql_planner::LogicalJoinType;

/// Return physical column indices for the logical output of a `USING` join.
///
/// Output order follows SQL: common columns first in `USING`/natural order,
/// then remaining left columns, then remaining right columns.
#[must_use]
pub fn using_projection_indices(
    pairs: &[(usize, usize)],
    left_width: usize,
    right_width: usize,
) -> Vec<usize> {
    let left_using: HashSet<usize> = pairs.iter().map(|(left_idx, _)| *left_idx).collect();
    let right_using: HashSet<usize> = pairs.iter().map(|(_, right_idx)| *right_idx).collect();
    let mut out = Vec::with_capacity(left_width + right_width - right_using.len());
    for (left_idx, _) in pairs {
        out.push(*left_idx);
    }
    for left_idx in 0..left_width {
        if !left_using.contains(&left_idx) {
            out.push(left_idx);
        }
    }
    for right_idx in 0..right_width {
        if !right_using.contains(&right_idx) {
            out.push(left_width + right_idx);
        }
    }
    out
}

/// Build the physical `left ++ right` schema used before `USING` projection.
///
/// Duplicate right-side field names are made unique so the vector row decoder
/// can validate the physical batch shape before the logical projection restores
/// the planner-owned output schema.
///
/// # Errors
///
/// Returns a schema error if physical field names still collide after
/// uniquifying, which would indicate an internal naming bug.
pub fn concat_join_exec_schema(
    left: &Schema,
    right: &Schema,
    join_type: LogicalJoinType,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(left.len() + right.len());
    let mut used_names = HashSet::new();
    for field in left.fields() {
        used_names.insert(field.name.to_ascii_lowercase());
        let nullable = matches!(
            join_type,
            LogicalJoinType::RightOuter | LogicalJoinType::FullOuter
        ) || field.nullable;
        fields.push(Field {
            name: field.name.clone(),
            data_type: field.data_type.clone(),
            nullable,
        });
    }
    for field in right.fields() {
        let nullable = matches!(
            join_type,
            LogicalJoinType::LeftOuter | LogicalJoinType::FullOuter
        ) || field.nullable;
        fields.push(Field {
            name: unique_join_field_name(&field.name, &mut used_names),
            data_type: field.data_type.clone(),
            nullable,
        });
    }
    Schema::new(fields)
}

fn unique_join_field_name(base: &str, used_names: &mut HashSet<String>) -> String {
    if used_names.insert(base.to_ascii_lowercase()) {
        return base.to_owned();
    }
    for suffix in 1.. {
        let candidate = format!("{base}_{suffix}");
        if used_names.insert(candidate.to_ascii_lowercase()) {
            return candidate;
        }
    }
    unreachable!("unbounded suffix search returns before overflow")
}
