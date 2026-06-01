//! Shared join output-layout helpers.
//!
//! `USING` and `NATURAL` joins evaluate predicates against the physical
//! `left ++ right` row, then expose a collapsed logical schema where shared
//! columns appear once. These helpers keep that column routing identical
//! between the standalone physical builder and the server pipeline lowerer.

use std::collections::HashSet;

use ultrasql_core::{Field, Result, Schema};
use ultrasql_planner::{LogicalJoinType, ScalarExpr};

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

/// Return expression projection for the logical output of a `USING` join.
///
/// Right and full outer joins must coalesce common columns because unmatched
/// right rows have `NULL` left-side values in the physical `left ++ right`
/// row. Inner and left joins can use the left common column directly.
#[must_use]
pub fn using_projection_exprs(
    pairs: &[(usize, usize)],
    left: &Schema,
    right: &Schema,
    join_type: LogicalJoinType,
) -> Vec<(ScalarExpr, String)> {
    let left_using: HashSet<usize> = pairs.iter().map(|(left_idx, _)| *left_idx).collect();
    let right_using: HashSet<usize> = pairs.iter().map(|(_, right_idx)| *right_idx).collect();
    let mut out = Vec::with_capacity(left.len() + right.len() - right_using.len());
    for (left_idx, right_idx) in pairs {
        let left_field = left.field_at(*left_idx);
        let right_field = right.field_at(*right_idx);
        let left_expr = column_expr(left_field, *left_idx);
        let expr = if matches!(
            join_type,
            LogicalJoinType::RightOuter | LogicalJoinType::FullOuter
        ) {
            ScalarExpr::FunctionCall {
                name: "coalesce".to_owned(),
                args: vec![left_expr, column_expr(right_field, left.len() + *right_idx)],
                data_type: left_field.data_type.clone(),
            }
        } else {
            left_expr
        };
        out.push((expr, left_field.name.clone()));
    }
    for left_idx in 0..left.len() {
        if !left_using.contains(&left_idx) {
            let field = left.field_at(left_idx);
            out.push((column_expr(field, left_idx), field.name.clone()));
        }
    }
    for right_idx in 0..right.len() {
        if !right_using.contains(&right_idx) {
            let field = right.field_at(right_idx);
            out.push((
                column_expr(field, left.len() + right_idx),
                field.name.clone(),
            ));
        }
    }
    out
}

fn column_expr(field: &Field, index: usize) -> ScalarExpr {
    ScalarExpr::Column {
        name: field.name.clone(),
        index,
        data_type: field.data_type.clone(),
    }
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
