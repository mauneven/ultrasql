//! Explicit JOIN binding (ON / USING / NATURAL) plus the schema- and
//! scope-merging helpers shared with the implicit cross-join path.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_parser::ast::{JoinCondition, JoinOp};

use super::{
    Catalog, LogicalJoinCondition, LogicalJoinType, LogicalPlan, PlanError, ScopeEntry, ScopeStack,
    bind_expr_with_ctes, bind_table_ref, bind_table_ref_maybe_lateral,
    schema_for_qualified_binding,
};

pub(super) fn bind_explicit_join(
    left_ref: &ultrasql_parser::ast::TableRef,
    op: JoinOp,
    right_ref: &ultrasql_parser::ast::TableRef,
    condition: &JoinCondition,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    let (left_plan, left_scope) = bind_table_ref(left_ref, catalog, cte_catalog, scope)?;
    // `JOIN LATERAL (…)` lets the right side correlate to the left; a plain
    // right side sees only the enclosing query's scope.
    let (right_plan, right_scope) = bind_table_ref_maybe_lateral(
        right_ref,
        left_plan.schema(),
        &left_scope,
        catalog,
        cte_catalog,
        scope,
    )?;

    let join_type = match op {
        JoinOp::Inner => LogicalJoinType::Inner,
        JoinOp::LeftOuter => LogicalJoinType::LeftOuter,
        JoinOp::RightOuter => LogicalJoinType::RightOuter,
        JoinOp::FullOuter => LogicalJoinType::FullOuter,
        JoinOp::Cross => LogicalJoinType::Cross,
    };

    match condition {
        JoinCondition::None => {
            let join_schema = concat_schemas_cross(left_plan.schema(), right_plan.schema())?;
            let left_len = left_scope.len();
            let out_scope = merge_scopes(left_scope, right_scope, left_len);
            Ok((
                LogicalPlan::Join {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    join_type,
                    condition: LogicalJoinCondition::None,
                    schema: join_schema,
                },
                out_scope,
            ))
        }
        JoinCondition::On(pred_ast) => {
            let concat_schema =
                concat_schemas_for_join(left_plan.schema(), right_plan.schema(), join_type)?;
            let left_len = left_scope.len();
            let out_scope = merge_scopes(left_scope, right_scope, left_len);
            let binding_schema = schema_for_qualified_binding(&concat_schema, &out_scope)?;
            let pred = bind_expr_with_ctes(pred_ast, &binding_schema, catalog, cte_catalog, scope)?;
            if pred.data_type() != DataType::Bool && pred.data_type() != DataType::Null {
                return Err(PlanError::TypeMismatch(format!(
                    "JOIN ON predicate must be boolean, got {}",
                    pred.data_type()
                )));
            }
            Ok((
                LogicalPlan::Join {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    join_type,
                    condition: LogicalJoinCondition::On(pred),
                    schema: concat_schema,
                },
                out_scope,
            ))
        }
        JoinCondition::Using(cols) => {
            let pairs = resolve_using_pairs(
                cols,
                left_plan.schema(),
                right_plan.schema(),
                &left_scope,
                &right_scope,
            )?;
            bind_using_join(
                left_plan,
                right_plan,
                left_scope,
                right_scope,
                join_type,
                pairs,
            )
        }
        JoinCondition::Natural => {
            let pairs = resolve_natural_pairs(
                left_plan.schema(),
                right_plan.schema(),
                &left_scope,
                &right_scope,
            )?;
            bind_using_join(
                left_plan,
                right_plan,
                left_scope,
                right_scope,
                join_type,
                pairs,
            )
        }
    }
}

fn bind_using_join(
    left_plan: LogicalPlan,
    right_plan: LogicalPlan,
    left_scope: Vec<ScopeEntry>,
    right_scope: Vec<ScopeEntry>,
    join_type: LogicalJoinType,
    pairs: Vec<(usize, usize)>,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    let schema = build_using_schema(left_plan.schema(), right_plan.schema(), &pairs, join_type)?;
    let out_scope = build_using_scope(&left_scope, &right_scope, &pairs);
    Ok((
        LogicalPlan::Join {
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            join_type,
            condition: LogicalJoinCondition::Using(pairs),
            schema,
        },
        out_scope,
    ))
}

fn build_using_scope(
    left_scope: &[ScopeEntry],
    right_scope: &[ScopeEntry],
    pairs: &[(usize, usize)],
) -> Vec<ScopeEntry> {
    let left_using: std::collections::HashSet<usize> =
        pairs.iter().map(|(left_idx, _)| *left_idx).collect();
    let right_using: std::collections::HashSet<usize> =
        pairs.iter().map(|(_, right_idx)| *right_idx).collect();
    let mut out = Vec::with_capacity(left_scope.len() + right_scope.len() - right_using.len());
    for (left_idx, _) in pairs {
        if let Some(entry) = left_scope.get(*left_idx) {
            push_scope_entry(&mut out, entry);
        }
    }
    for (left_idx, entry) in left_scope.iter().enumerate() {
        if !left_using.contains(&left_idx) {
            push_scope_entry(&mut out, entry);
        }
    }
    for (right_idx, entry) in right_scope.iter().enumerate() {
        if !right_using.contains(&right_idx) {
            push_scope_entry(&mut out, entry);
        }
    }
    out
}

fn push_scope_entry(out: &mut Vec<ScopeEntry>, entry: &ScopeEntry) {
    out.push(ScopeEntry {
        qualifier: entry.qualifier.clone(),
        field_index: out.len(),
        field: entry.field.clone(),
    });
}

/// Count how many of `scope`'s columns are named `col_name` (case-insensitive),
/// using the scope's *true* column names. The plan schema deduplicates a
/// repeated name (the 2nd `x` becomes `x_1`), so `Schema::find` would silently
/// see only one — the scope is the only place a genuine duplicate survives.
fn join_column_occurrences(scope: &[ScopeEntry], col_name: &str) -> usize {
    scope
        .iter()
        .filter(|e| e.field.name.eq_ignore_ascii_case(col_name))
        .count()
}

/// PostgreSQL rejects a `USING`/`NATURAL` common column that appears more than
/// once on a side (`SELECT * FROM (t1 CROSS JOIN t2) JOIN t3 USING (x)` with a
/// `t1.x` and a `t2.x`): SQLSTATE 42702. `side` is `"left"` or `"right"`.
fn check_join_column_unique(
    scope: &[ScopeEntry],
    col_name: &str,
    side: &str,
) -> Result<(), PlanError> {
    if join_column_occurrences(scope, col_name) > 1 {
        return Err(PlanError::AmbiguousJoinColumn(format!(
            "common column name \"{col_name}\" appears more than once in {side} table"
        )));
    }
    Ok(())
}

fn resolve_using_pairs(
    cols: &[ultrasql_parser::ast::Identifier],
    left: &Schema,
    right: &Schema,
    left_scope: &[ScopeEntry],
    right_scope: &[ScopeEntry],
) -> Result<Vec<(usize, usize)>, PlanError> {
    let mut pairs: Vec<(usize, usize)> = Vec::with_capacity(cols.len());
    for ident in cols {
        let col_name = &ident.value;
        // A common column duplicated on either side is ambiguous (PG 42702):
        // check before pairing, since the deduplicated schema would otherwise
        // hide the duplicate and `Schema::find` would pair only the first.
        check_join_column_unique(left_scope, col_name, "left")?;
        check_join_column_unique(right_scope, col_name, "right")?;
        let left_idx = left
            .find(col_name)
            .ok_or_else(|| PlanError::ColumnNotFound(col_name.clone()))?
            .0;
        let right_idx = right
            .find(col_name)
            .ok_or_else(|| PlanError::ColumnNotFound(col_name.clone()))?
            .0;
        pairs.push((left_idx, right_idx));
    }
    Ok(pairs)
}

fn resolve_natural_pairs(
    left: &Schema,
    right: &Schema,
    left_scope: &[ScopeEntry],
    right_scope: &[ScopeEntry],
) -> Result<Vec<(usize, usize)>, PlanError> {
    let mut pairs = Vec::new();
    for (left_idx, left_field) in left.fields().iter().enumerate() {
        if let Some((right_idx, _)) = right.find(&left_field.name) {
            // The pair is a common column; reject if it is duplicated on
            // either side (PG 42702), using the scope's true names.
            check_join_column_unique(left_scope, &left_field.name, "left")?;
            check_join_column_unique(right_scope, &left_field.name, "right")?;
            pairs.push((left_idx, right_idx));
        }
    }
    Ok(pairs)
}

fn build_using_schema(
    left: &Schema,
    right: &Schema,
    pairs: &[(usize, usize)],
    join_type: LogicalJoinType,
) -> Result<Schema, PlanError> {
    let using_set: std::collections::HashSet<usize> = pairs.iter().map(|(l, _)| *l).collect();
    let right_using_set: std::collections::HashSet<usize> = pairs.iter().map(|(_, r)| *r).collect();

    let mut out_fields: Vec<Field> = Vec::new();
    for &(left_idx, _) in pairs {
        let f = left.field_at(left_idx);
        let nullable = matches!(join_type, LogicalJoinType::FullOuter) || f.nullable;
        out_fields.push(Field {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            nullable,
        });
    }
    for (i, f) in left.fields().iter().enumerate() {
        if using_set.contains(&i) {
            continue;
        }
        let nullable = matches!(
            join_type,
            LogicalJoinType::RightOuter | LogicalJoinType::FullOuter
        ) || f.nullable;
        out_fields.push(Field {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            nullable,
        });
    }
    for (i, f) in right.fields().iter().enumerate() {
        if right_using_set.contains(&i) {
            continue;
        }
        let nullable = matches!(
            join_type,
            LogicalJoinType::LeftOuter | LogicalJoinType::FullOuter
        ) || f.nullable;
        out_fields.push(Field {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            nullable,
        });
    }
    Schema::new(out_fields).map_err(|e| PlanError::TypeMismatch(format!("USING join schema: {e}")))
}

pub(super) fn concat_schemas_cross(left: &Schema, right: &Schema) -> Result<Schema, PlanError> {
    let mut fields: Vec<Field> = Vec::with_capacity(left.len() + right.len());
    let mut used_names = std::collections::HashSet::new();
    for f in left.fields() {
        used_names.insert(f.name.to_ascii_lowercase());
        fields.push(f.clone());
    }
    for f in right.fields() {
        let name = unique_join_field_name(&f.name, &mut used_names);
        fields.push(Field {
            name,
            data_type: f.data_type.clone(),
            nullable: f.nullable,
        });
    }
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(format!("join schema: {e}")))
}

pub(super) fn concat_schemas_for_join(
    left: &Schema,
    right: &Schema,
    join_type: LogicalJoinType,
) -> Result<Schema, PlanError> {
    let make_left_nullable = matches!(
        join_type,
        LogicalJoinType::RightOuter | LogicalJoinType::FullOuter
    );
    let make_right_nullable = matches!(
        join_type,
        LogicalJoinType::LeftOuter | LogicalJoinType::FullOuter
    );

    let mut fields: Vec<Field> = Vec::with_capacity(left.len() + right.len());
    let mut used_names = std::collections::HashSet::new();
    for f in left.fields() {
        used_names.insert(f.name.to_ascii_lowercase());
        fields.push(Field {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            nullable: f.nullable || make_left_nullable,
        });
    }
    for f in right.fields() {
        let name = unique_join_field_name(&f.name, &mut used_names);
        fields.push(Field {
            name,
            data_type: f.data_type.clone(),
            nullable: f.nullable || make_right_nullable,
        });
    }
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(format!("join schema: {e}")))
}

fn unique_join_field_name(
    base: &str,
    used_names: &mut std::collections::HashSet<String>,
) -> String {
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

pub(super) fn merge_scopes(
    left: Vec<ScopeEntry>,
    right: Vec<ScopeEntry>,
    left_len: usize,
) -> Vec<ScopeEntry> {
    let mut out = left;
    for e in right {
        out.push(ScopeEntry {
            qualifier: e.qualifier,
            field_index: e.field_index + left_len,
            field: e.field,
        });
    }
    out
}
