//! FROM clause and JOIN binding. Split out of `binder/mod.rs` to keep each
//! file under the 600-line ceiling.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_parser::ast::{JoinCondition, JoinOp, TableRef};

use super::{
    Catalog, LogicalJoinCondition, LogicalJoinType, LogicalPlan, PlanError, ScalarExpr, ScopeEntry,
    ScopeStack, apply_column_aliases, bind_expr, bind_select_with_ctes,
};

pub(super) fn bind_from(
    from_items: &[TableRef],
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    outer_scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    if from_items.is_empty() {
        return Ok((
            LogicalPlan::Empty {
                schema: Schema::empty(),
            },
            vec![],
        ));
    }

    let mut iter = from_items.iter();
    let first = iter.next().expect("at least one item checked above");
    let (mut plan, mut from_scope) = bind_table_ref(first, catalog, cte_catalog, outer_scope)?;

    for item in iter {
        let (right_plan, right_scope) = bind_table_ref(item, catalog, cte_catalog, outer_scope)?;
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

fn bind_table_ref(
    table_ref: &TableRef,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    match table_ref {
        TableRef::Named { name, alias, .. } => {
            let table_name = name
                .parts
                .last()
                .map_or_else(String::new, |p| p.value.to_ascii_lowercase());
            let qualifier = alias
                .as_ref()
                .map_or_else(|| table_name.clone(), |a| a.value.clone());

            let schema = if let Some((_, s)) = cte_catalog
                .iter()
                .rev()
                .find(|(n, _)| n.eq_ignore_ascii_case(&table_name))
            {
                s.clone()
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
        } => bind_table_function(name, args, alias.as_ref(), catalog, scope),
    }
}

fn bind_table_function(
    name: &ultrasql_parser::ast::Identifier,
    args: &[ultrasql_parser::ast::Expr],
    alias: Option<&ultrasql_parser::ast::Identifier>,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    let func_name = name.value.to_ascii_lowercase();
    let qualifier = alias.map_or_else(|| func_name.clone(), |a| a.value.clone());
    let (schema, col_name) = match func_name.as_str() {
        "generate_series" => (
            Schema::new([Field::required("generate_series", DataType::Int64)])
                .map_err(|e| PlanError::TypeMismatch(format!("generate_series schema: {e}")))?,
            "generate_series".to_string(),
        ),
        _ => {
            return Err(PlanError::NotSupported(
                "table function (only generate_series supported)",
            ));
        }
    };
    let mut bound_args: Vec<ScalarExpr> = Vec::with_capacity(args.len());
    let empty_schema = Schema::empty();
    for a in args {
        bound_args.push(bind_expr(a, &empty_schema, catalog, scope)?);
    }
    let from_scope = vec![ScopeEntry {
        qualifier,
        field_index: 0,
        field: Field::required(&col_name, DataType::Int64),
    }];
    let plan = LogicalPlan::FunctionScan {
        name: func_name,
        args: bound_args,
        schema,
    };
    Ok((plan, from_scope))
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

fn bind_explicit_join(
    left_ref: &TableRef,
    op: JoinOp,
    right_ref: &TableRef,
    condition: &JoinCondition,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<(LogicalPlan, Vec<ScopeEntry>), PlanError> {
    let (left_plan, left_scope) = bind_table_ref(left_ref, catalog, cte_catalog, scope)?;
    let (right_plan, right_scope) = bind_table_ref(right_ref, catalog, cte_catalog, scope)?;

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
            let pred = bind_expr(pred_ast, &concat_schema, catalog, scope)?;
            if pred.data_type() != DataType::Bool && pred.data_type() != DataType::Null {
                return Err(PlanError::TypeMismatch(format!(
                    "JOIN ON predicate must be boolean, got {}",
                    pred.data_type()
                )));
            }
            let left_len = left_scope.len();
            let out_scope = merge_scopes(left_scope, right_scope, left_len);
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
            let pairs = resolve_using_pairs(cols, left_plan.schema(), right_plan.schema())?;
            let schema =
                build_using_schema(left_plan.schema(), right_plan.schema(), &pairs, join_type)?;
            let left_len = left_scope.len();
            let out_scope = merge_scopes(left_scope, right_scope, left_len);
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
    }
}

fn resolve_using_pairs(
    cols: &[ultrasql_parser::ast::Identifier],
    left: &Schema,
    right: &Schema,
) -> Result<Vec<(usize, usize)>, PlanError> {
    let mut pairs: Vec<(usize, usize)> = Vec::with_capacity(cols.len());
    for ident in cols {
        let col_name = &ident.value;
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
    let left_names: std::collections::HashSet<String> = left
        .fields()
        .iter()
        .map(|f| f.name.to_ascii_lowercase())
        .collect();
    for f in left.fields() {
        fields.push(f.clone());
    }
    for f in right.fields() {
        let name = if left_names.contains(&f.name.to_ascii_lowercase()) {
            format!("{}_1", f.name)
        } else {
            f.name.clone()
        };
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

    let left_names: std::collections::HashSet<String> = left
        .fields()
        .iter()
        .map(|f| f.name.to_ascii_lowercase())
        .collect();

    let mut fields: Vec<Field> = Vec::with_capacity(left.len() + right.len());
    for f in left.fields() {
        fields.push(Field {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            nullable: f.nullable || make_left_nullable,
        });
    }
    for f in right.fields() {
        let name = if left_names.contains(&f.name.to_ascii_lowercase()) {
            format!("{}_1", f.name)
        } else {
            f.name.clone()
        };
        fields.push(Field {
            name,
            data_type: f.data_type.clone(),
            nullable: f.nullable || make_right_nullable,
        });
    }
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(format!("join schema: {e}")))
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
