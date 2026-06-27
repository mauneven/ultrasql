//! FROM clause and JOIN binding. Split out of `binder/mod.rs` to keep each
//! file under the 600-line ceiling.

mod csv_schema;
mod joins;
mod json_reader;
mod paths;
mod pivot;
mod readers;
mod table_function;

#[cfg(test)]
mod tests;

use ultrasql_core::{Field, Schema};
use ultrasql_parser::ast::TableRef;

const READ_CSV_HEADER_SAMPLE_BYTES: u64 = 64 * 1024;
const JSON_STREAM_CHUNK_BYTES: u64 = 64 * 1024;
const PLANNER_JSON_RECORD_LIMIT_BYTES: usize = 16 * 1024 * 1024;
const MAX_JOIN_DEPTH: usize = 64;

use super::{
    AggregateFunc, Catalog, LogicalJoinCondition, LogicalJoinType, LogicalPivotAggregate,
    LogicalPivotValue, LogicalPlan, LogicalUnpivotColumn, PlanError, ScalarExpr, ScopeEntry,
    ScopeFrame, ScopeStack, apply_column_aliases, bind_expr_with_ctes, bind_select_with_ctes,
    lookup_table_reference, plan_contains_outer_column, schema_for_qualified_binding,
};

use joins::{bind_explicit_join, concat_schemas_cross, merge_scopes};
use pivot::{UnpivotRefSpec, bind_pivot_ref, bind_unpivot_ref};
use table_function::{bind_json_table_ref, bind_table_function, bind_xml_table_ref};

pub(super) fn bind_from(
    from_items: &[TableRef],
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    outer_scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    let join_depth = from_clause_join_depth(from_items);
    if join_depth > MAX_JOIN_DEPTH {
        return Err(PlanError::not_supported(format!(
            "join depth {join_depth} exceeds planner limit {MAX_JOIN_DEPTH}"
        )));
    }

    if from_items.is_empty() {
        return Ok((
            LogicalPlan::Empty {
                schema: Schema::empty(),
            },
            vec![],
        ));
    }

    let Some(first) = from_items.first() else {
        return Ok((
            LogicalPlan::Empty {
                schema: Schema::empty(),
            },
            vec![],
        ));
    };
    let iter = from_items.iter().skip(1);
    let (mut plan, mut from_scope) = bind_table_ref(first, catalog, cte_catalog, outer_scope)?;

    for item in iter {
        // A `LATERAL` derived table may correlate to FROM items to its left at
        // this query level: expose the accumulated left side as an extra outer
        // scope frame while binding it. Plain (non-LATERAL) items see only the
        // enclosing query's scope, so a sibling reference fails to resolve —
        // matching PostgreSQL's "invalid reference to FROM-clause entry".
        let (right_plan, right_scope) = bind_table_ref_maybe_lateral(
            item,
            plan.schema(),
            &from_scope,
            catalog,
            cte_catalog,
            outer_scope,
        )?;
        let offset = from_scope.len();
        let join_schema = concat_schemas_cross(plan.schema(), right_plan.schema())?;
        let merged_scope = merge_scopes(from_scope, right_scope, offset);
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right_plan),
            join_type: LogicalJoinType::Cross,
            condition: LogicalJoinCondition::None,
            schema: join_schema,
        };
        from_scope = merged_scope;
    }

    Ok((plan, from_scope))
}

/// Whether a FROM item is a `LATERAL` derived table — the only shape whose
/// inner query may correlate to sibling FROM items at the same query level.
pub(super) const fn table_ref_is_lateral(table_ref: &TableRef) -> bool {
    matches!(table_ref, TableRef::Subquery { lateral: true, .. })
}

/// Bind a FROM item, honouring `LATERAL`.
///
/// A plain (non-`LATERAL`) derived table sees only the enclosing query's scope,
/// never its siblings at this query level — so a sibling reference fails to
/// resolve, matching PostgreSQL's "invalid reference to FROM-clause entry".
///
/// A `LATERAL` derived table *may* correlate to FROM items to its left
/// (`left_plan_schema`/`left_scope`): pushing them as a `frame_depth = 1` scope
/// binds such references to an [`ScalarExpr::OuterColumn`]. The query planner has
/// no decorrelation rule for a lateral correlation that lives directly inside a
/// join input (the executor rejects an outer-column reference that reaches it),
/// so rather than emit a plan that fails at execution with an opaque internal
/// error, reject a *correlated* `LATERAL` derived table up front. An
/// uncorrelated `LATERAL` (no reference to the left) binds and runs normally.
pub(super) fn bind_table_ref_maybe_lateral(
    item: &TableRef,
    left_plan_schema: &Schema,
    left_scope: &[ScopeEntry],
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    if !table_ref_is_lateral(item) {
        return bind_table_ref(item, catalog, cte_catalog, scope);
    }
    let frame_schema = schema_for_qualified_binding(left_plan_schema, left_scope)?;
    scope.push(ScopeFrame {
        schema: frame_schema,
        qualifier: None,
    });
    let result = bind_table_ref(item, catalog, cte_catalog, scope);
    scope.pop();
    let (plan, from_scope) = result?;
    if plan_correlates_to_left(&plan) {
        return Err(PlanError::not_supported(
            "correlated LATERAL derived table (references a column of a preceding FROM item)",
        ));
    }
    Ok((plan, from_scope))
}

/// Whether `plan` (a freshly bound LATERAL derived table) contains an
/// outer-column reference at `frame_depth == 1` — i.e. it correlates to the
/// sibling FROM items the caller exposed, not to a farther-out enclosing query.
/// Deeper references (`frame_depth > 1`) target the real outer query and are
/// handled by the normal subquery decorrelation path, so they are not rejected
/// here. Walks only the relational node shapes a derived-table `SELECT` can
/// produce; the deny-list of statement plans cannot occur inside FROM.
fn plan_correlates_to_left(plan: &LogicalPlan) -> bool {
    use crate::expr::ScalarExpr;

    let refs_left = |e: &ScalarExpr| e.contains_outer_column_depth1();

    match plan {
        LogicalPlan::Filter { input, predicate } => {
            refs_left(predicate) || plan_correlates_to_left(input)
        }
        LogicalPlan::Project { input, exprs, .. } => {
            exprs.iter().any(|(e, _)| refs_left(e)) || plan_correlates_to_left(input)
        }
        LogicalPlan::Sort { input, keys } => {
            keys.iter().any(|k| refs_left(&k.expr)) || plan_correlates_to_left(input)
        }
        LogicalPlan::DistinctOn { input, on_keys } => {
            on_keys.iter().any(refs_left) || plan_correlates_to_left(input)
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            group_by.iter().any(refs_left)
                || aggregates.iter().any(|a| {
                    a.arg.as_ref().is_some_and(refs_left)
                        || a.direct_arg.as_ref().is_some_and(refs_left)
                        || a.order_by.as_ref().is_some_and(|k| refs_left(&k.expr))
                })
                || plan_correlates_to_left(input)
        }
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            plan_correlates_to_left(left) || plan_correlates_to_left(right)
        }
        LogicalPlan::Values { rows, .. } => rows.iter().flat_map(|r| r.iter()).any(refs_left),
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::SingleRowAssert { input, .. }
        | LogicalPlan::Unpivot { input, .. } => plan_correlates_to_left(input),
        LogicalPlan::Pivot {
            input, aggregate, ..
        } => aggregate.arg.as_ref().is_some_and(refs_left) || plan_correlates_to_left(input),
        LogicalPlan::Cte {
            definition, body, ..
        } => plan_correlates_to_left(definition) || plan_correlates_to_left(body),
        // A correlation reaching the executor is rejected anyway; for any other
        // (non-query) node shape, fall back to the conservative whole-plan check
        // so we never let an undecorrelatable lateral reference slip through.
        _ => plan_contains_outer_column(plan),
    }
}

fn from_clause_join_depth(from_items: &[TableRef]) -> usize {
    let mut items = from_items.iter();
    let Some(first) = items.next() else {
        return 0;
    };

    let mut depth = table_ref_join_depth(first);
    for item in items {
        depth = depth.max(table_ref_join_depth(item)).saturating_add(1);
    }
    depth
}

fn table_ref_join_depth(table_ref: &TableRef) -> usize {
    match table_ref {
        TableRef::Join { left, right, .. } => table_ref_join_depth(left)
            .max(table_ref_join_depth(right))
            .saturating_add(1),
        TableRef::Named { .. }
        | TableRef::Subquery { .. }
        | TableRef::Function { .. }
        | TableRef::JsonTable { .. }
        | TableRef::Pivot { .. }
        | TableRef::Unpivot { .. }
        | TableRef::XmlTable { .. } => 0,
    }
}

fn bind_table_ref(
    table_ref: &TableRef,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    match table_ref {
        TableRef::Named { name, alias, .. } => {
            let raw_table_name = name
                .parts
                .last()
                .map_or_else(String::new, |p| p.value.to_ascii_lowercase());
            let system_table_name = qualified_system_name(name);
            let mut table_name = system_table_name
                .clone()
                .unwrap_or_else(|| raw_table_name.clone());
            let qualifier = alias
                .as_ref()
                .map_or_else(|| raw_table_name.clone(), |a| a.value.clone());

            let schema = if let Some((_, s)) = cte_catalog
                .iter()
                .rev()
                .find(|(n, _)| n.eq_ignore_ascii_case(&table_name))
            {
                s.clone()
            } else if system_table_name.is_none() {
                let resolved = lookup_table_reference(catalog, name)?;
                table_name = resolved.plan_name;
                resolved.meta.schema
            } else {
                let meta = catalog
                    .lookup_table(&table_name)
                    .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
                meta.schema
            };

            let from_scope: Vec<ScopeEntry> = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| ScopeEntry {
                    qualifier: qualifier.clone(),
                    field_index: i,
                    field: f.clone(),
                })
                .collect();
            let plan = LogicalPlan::Scan {
                table: table_name,
                schema,
                projection: None,
            };
            Ok((plan, from_scope))
        }
        TableRef::Subquery {
            select,
            alias,
            column_aliases,
            ..
        } => {
            let inner_plan = bind_select_with_ctes(select, catalog, cte_catalog, scope)?;
            let inner_schema = inner_plan.schema().clone();
            let inner_schema = if column_aliases.is_empty() {
                inner_schema
            } else {
                apply_column_aliases(&inner_schema, column_aliases)?
            };
            let qualifier = alias.value.clone();
            let from_scope: Vec<ScopeEntry> = inner_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| ScopeEntry {
                    qualifier: qualifier.clone(),
                    field_index: i,
                    field: f.clone(),
                })
                .collect();
            let plan = rebuild_subquery_plan(inner_plan, &inner_schema)?;
            Ok((plan, from_scope))
        }
        TableRef::Join {
            left,
            op,
            right,
            condition,
            ..
        } => bind_explicit_join(left, *op, right, condition, catalog, cte_catalog, scope),
        TableRef::Function {
            name, args, alias, ..
        } => bind_table_function(name, args, alias.as_ref(), catalog, cte_catalog, scope),
        TableRef::JsonTable {
            context,
            row_path,
            columns,
            alias,
            ..
        } => bind_json_table_ref(
            context,
            row_path,
            columns,
            alias.as_ref(),
            catalog,
            cte_catalog,
            scope,
        ),
        TableRef::Pivot {
            input,
            aggregate,
            value_column,
            pivot_values,
            ..
        } => bind_pivot_ref(
            input,
            aggregate,
            value_column,
            pivot_values,
            catalog,
            cte_catalog,
            scope,
        ),
        TableRef::Unpivot {
            input,
            value_column,
            name_column,
            columns,
            include_nulls,
            ..
        } => bind_unpivot_ref(
            UnpivotRefSpec {
                input,
                value_column,
                name_column,
                columns,
                include_nulls: *include_nulls,
            },
            catalog,
            cte_catalog,
            scope,
        ),
        TableRef::XmlTable {
            context,
            row_path,
            columns,
            alias,
            ..
        } => bind_xml_table_ref(
            context,
            row_path,
            columns,
            alias.as_ref(),
            catalog,
            cte_catalog,
            scope,
        ),
    }
}

fn qualified_system_name(name: &ultrasql_parser::ast::ObjectName) -> Option<String> {
    if name.parts.len() != 2 {
        return None;
    }
    let namespace = name.parts[0].value.to_ascii_lowercase();
    if !matches!(namespace.as_str(), "pg_catalog" | "information_schema") {
        return None;
    }
    let relation = name.parts[1].value.to_ascii_lowercase();
    Some(format!("{namespace}.{relation}"))
}

fn rebuild_subquery_plan(
    inner_plan: LogicalPlan,
    alias_schema: &Schema,
) -> Result<LogicalPlan, PlanError> {
    let exprs: Vec<(ScalarExpr, String)> = alias_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let expr = ScalarExpr::Column {
                name: f.name.clone(),
                index: i,
                data_type: f.data_type.clone(),
            };
            (expr, f.name.clone())
        })
        .collect();
    let proj_fields: Vec<Field> = alias_schema.fields().to_vec();
    let proj_schema = Schema::new(proj_fields)
        .map_err(|e| PlanError::TypeMismatch(format!("subquery alias schema: {e}")))?;
    Ok(LogicalPlan::Project {
        input: Box::new(inner_plan),
        exprs,
        schema: proj_schema,
    })
}
