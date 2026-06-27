//! DML binders. Split out of `binder/mod.rs` to keep each
//! production source file under the 600-line ceiling. Public
//! entry points are `pub(super)` so `binder::bind` can dispatch.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_parser::ast::{
    Assignment, ConflictTarget as AstConflictTarget, DeleteStmt, Expr, InsertSource, InsertStmt,
    MergeAction, MergeMatchKind, MergeStmt, ObjectName, OnConflict, UpdateStmt,
};

use crate::catalog::TableMeta;
use crate::expr::INSERT_DEFAULT_SENTINEL;

use super::expr_bind::coerce_literal_to_type;
use super::{
    Catalog, ConflictTarget, LogicalMergeAction, LogicalMergeClause, LogicalMergeMatchKind,
    LogicalOnConflict, LogicalPlan, PlanError, ScalarExpr, ScopeEntry, ScopeStack, bind_expr,
    bind_from, bind_returning, bind_select, build_returning_schema, lookup_table_reference,
    schema_for_qualified_binding,
};

pub(super) fn bind_insert(
    s: &InsertStmt,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    // 1. Catalog lookup.
    let (table_name, meta) = lookup_target_table(catalog, &s.table)?;
    let table_schema = &meta.schema;

    // 2. Resolve column list.
    let columns: Vec<usize> = if s.columns.is_empty() {
        // All columns in natural order.
        (0..table_schema.len()).collect()
    } else {
        let mut indices = Vec::with_capacity(s.columns.len());
        let mut seen: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(s.columns.len());
        for ident in &s.columns {
            let col_name = ident.value.clone();
            if !seen.insert(col_name.to_ascii_lowercase()) {
                return Err(PlanError::DuplicateColumn(col_name));
            }
            let idx = table_schema
                .find(&col_name)
                .ok_or_else(|| PlanError::ColumnNotFound(col_name.clone()))?
                .0;
            indices.push(idx);
        }
        indices
    };

    let expected_arity = columns.len();

    // 3. Build the source plan.
    let source = match &s.source {
        InsertSource::DefaultValues => {
            // Executor fills in defaults; the plan carries a zero-column
            // placeholder row.
            let empty_schema = Schema::empty();
            LogicalPlan::Values {
                rows: vec![vec![]],
                schema: empty_schema,
            }
        }
        InsertSource::Values(rows) => {
            let mut plan = bind_values_rows(rows, expected_arity, catalog, scope)?;
            // Coerce literal VALUES cells to the target table column type.
            // This covers NULL-only columns and concrete numeric literals
            // like `0.06` flowing into `DECIMAL(15,2)`.
            if let LogicalPlan::Values { rows, schema } = &mut plan {
                for row in rows.iter_mut() {
                    for (i, cell) in row.iter_mut().enumerate() {
                        let target = &table_schema.field_at(columns[i]).data_type;
                        coerce_literal_to_type(cell, target);
                    }
                }
                let mut new_fields: Vec<Field> = Vec::with_capacity(schema.len());
                for (i, field) in schema.fields().iter().enumerate() {
                    let target = &table_schema.field_at(columns[i]).data_type;
                    let resolved = target.clone();
                    new_fields.push(Field::nullable(field.name.clone(), resolved));
                }
                *schema = Schema::new(new_fields).map_err(|e| {
                    PlanError::TypeMismatch(format!("INSERT VALUES schema coercion: {e}"))
                })?;
            }
            plan
        }
        InsertSource::Select(sel) => {
            let plan = bind_select(sel, catalog, scope)?;
            // Arity check.
            if plan.schema().len() != expected_arity {
                return Err(PlanError::TypeMismatch(format!(
                    "INSERT column count ({expected_arity}) does not match SELECT arity ({})",
                    plan.schema().len()
                )));
            }
            plan
        }
    };

    // 4. Bind ON CONFLICT.
    let on_conflict = s
        .on_conflict
        .as_ref()
        .map(|oc| bind_on_conflict(oc, table_schema, catalog, scope))
        .transpose()?;

    // 5. Bind RETURNING.
    let returning = bind_returning(&s.returning, table_schema, catalog, scope)?;
    let returning_schema = build_returning_schema(&returning)?;

    Ok(LogicalPlan::Insert {
        table: table_name,
        columns,
        source: Box::new(source),
        on_conflict,
        returning,
        schema: returning_schema,
    })
}

/// Bind `VALUES (…), (…)` rows into a [`LogicalPlan::Values`].
///
/// Every row must have exactly `expected_arity` cells; ragged rows are
/// rejected with [`PlanError::TypeMismatch`].
fn bind_values_rows(
    rows: &[Vec<Expr>],
    expected_arity: usize,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    // Use an empty schema as the binding context — value cells must be
    // self-contained (literals, parameters, simple expressions). Column
    // references to other tables are not allowed inside a VALUES clause.
    let empty = Schema::empty();

    let mut bound_rows: Vec<Vec<ScalarExpr>> = Vec::with_capacity(rows.len());
    for (row_idx, row) in rows.iter().enumerate() {
        if row.len() != expected_arity {
            return Err(PlanError::TypeMismatch(format!(
                "VALUES row {} has {} column(s), expected {expected_arity}",
                row_idx + 1,
                row.len()
            )));
        }
        let mut bound_cells = Vec::with_capacity(row.len());
        for e in row {
            // `DEFAULT` in a VALUES cell is bound to an inert sentinel call.
            // The server's INSERT lowering substitutes it with the target
            // column's default expression (or NULL when none is declared).
            // It never reaches the executor unrewritten; if it somehow did,
            // the evaluator rejects the unknown function rather than
            // producing a wrong value.
            if matches!(e, Expr::Default { .. }) {
                bound_cells.push(ScalarExpr::FunctionCall {
                    name: INSERT_DEFAULT_SENTINEL.to_owned(),
                    args: Vec::new(),
                    data_type: DataType::Null,
                });
                continue;
            }
            bound_cells.push(bind_expr(e, &empty, catalog, scope)?);
        }
        bound_rows.push(bound_cells);
    }

    // Infer column types: for each column position, take the numeric_join
    // across all rows; fall back to DataType::Null if every cell is null.
    let arity = expected_arity;
    let mut col_types: Vec<DataType> = vec![DataType::Null; arity];
    for row in &bound_rows {
        for (ci, cell) in row.iter().enumerate() {
            let cell_ty = cell.data_type();
            let current = &col_types[ci];
            col_types[ci] = if matches!(current, DataType::Null) {
                cell_ty
            } else if matches!(cell_ty, DataType::Null) {
                current.clone()
            } else if current.is_numeric() && cell_ty.is_numeric() {
                current
                    .numeric_join(&cell_ty)
                    .unwrap_or_else(|_| current.clone())
            } else {
                // Non-numeric non-null: keep the type from the first row
                // (PostgreSQL selects the type of the first non-null cell
                // for simple scalar literals).
                current.clone()
            };
        }
    }

    // Build synthetic column names: column1, column2, …
    let fields: Result<Vec<Field>, _> = col_types
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            // Column names are 1-based like PostgreSQL.
            let name = format!("column{}", i + 1);
            Ok::<_, PlanError>(Field::nullable(name, ty.clone()))
        })
        .collect();
    let schema =
        Schema::new(fields?).map_err(|e| PlanError::TypeMismatch(format!("VALUES schema: {e}")))?;

    Ok(LogicalPlan::Values {
        rows: bound_rows,
        schema,
    })
}

/// Bind an `ON CONFLICT` AST node into its logical form.
///
/// `EXCLUDED` column references in `DO UPDATE SET` assignments are not
/// supported in v0.2; the binder returns
/// [`PlanError::NotSupported`] if the parser produced such a reference.
fn bind_on_conflict(
    oc: &OnConflict,
    table_schema: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalOnConflict, PlanError> {
    match oc {
        OnConflict::DoNothing { target, .. } => {
            let resolved = target
                .as_ref()
                .map(|ct| bind_conflict_target(ct, table_schema))
                .transpose()?;
            Ok(LogicalOnConflict::DoNothing { target: resolved })
        }
        OnConflict::DoUpdate {
            target,
            set,
            r#where,
            ..
        } => {
            let resolved_target = bind_conflict_target(target, table_schema)?;
            let conflict_schema = conflict_update_schema(table_schema)?;
            let assignments =
                bind_assignments(set, table_schema, &conflict_schema, catalog, scope)?;
            let where_expr = r#where
                .as_ref()
                .map(|e| {
                    let pred = bind_expr(e, &conflict_schema, catalog, scope)?;
                    if pred.data_type() != DataType::Bool && pred.data_type() != DataType::Null {
                        return Err(PlanError::TypeMismatch(
                            "ON CONFLICT DO UPDATE WHERE predicate must be boolean".into(),
                        ));
                    }
                    Ok(pred)
                })
                .transpose()?;
            Ok(LogicalOnConflict::DoUpdate {
                target: resolved_target,
                assignments,
                r#where: where_expr,
            })
        }
    }
}

/// Resolve an AST `ConflictTarget` to column indices in `table_schema`.
fn bind_conflict_target(
    ct: &AstConflictTarget,
    table_schema: &Schema,
) -> Result<ConflictTarget, PlanError> {
    let mut columns = Vec::with_capacity(ct.columns.len());
    for ident in &ct.columns {
        let idx = table_schema
            .find(&ident.value)
            .ok_or_else(|| PlanError::ColumnNotFound(ident.value.clone()))?
            .0;
        columns.push(idx);
    }
    Ok(ConflictTarget { columns })
}

fn conflict_update_schema(table_schema: &Schema) -> Result<Schema, PlanError> {
    let mut fields = Vec::with_capacity(table_schema.len() * 2);
    fields.extend(table_schema.fields().iter().cloned());
    fields.extend(table_schema.fields().iter().map(|field| Field {
        name: format!("excluded.{}", field.name),
        data_type: field.data_type.clone(),
        nullable: field.nullable,
    }));
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(e.to_string()))
}

/// Bind a list of `col = expr` assignments into `(index, ScalarExpr)` pairs.
///
/// Each target column name is resolved against `table_schema`. Expression
/// values are bound against `value_schema`. For plain `UPDATE`, this is the
/// pre-update row view. For `ON CONFLICT DO UPDATE`, it is
/// `[target columns..., excluded.target columns...]`.
///
/// PostgreSQL rejects `UPDATE t SET col=1, col=2`; this function mirrors
/// that behaviour by returning [`PlanError::DuplicateColumn`] on the first
/// repeated target.
fn bind_assignments(
    set: &[Assignment],
    table_schema: &Schema,
    value_schema: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<Vec<(usize, ScalarExpr)>, PlanError> {
    let mut out = Vec::with_capacity(set.len());
    let mut seen: std::collections::HashSet<usize> =
        std::collections::HashSet::with_capacity(set.len());
    for a in set {
        let idx = table_schema
            .find(&a.target.value)
            .ok_or_else(|| PlanError::ColumnNotFound(a.target.value.clone()))?
            .0;
        if !seen.insert(idx) {
            return Err(PlanError::DuplicateColumn(a.target.value.clone()));
        }
        let mut expr = bind_expr(&a.value, value_schema, catalog, scope)?;
        let target = &table_schema.field_at(idx).data_type;
        coerce_assignment_expr_to_type(&mut expr, target);
        out.push((idx, expr));
    }
    Ok(out)
}

fn coerce_assignment_expr_to_type(expr: &mut ScalarExpr, target: &DataType) {
    coerce_literal_to_type(expr, target);
    let ScalarExpr::FunctionCall {
        name, data_type, ..
    } = expr
    else {
        return;
    };
    if matches!(target, DataType::Timestamp)
        && matches!(name.as_str(), "now" | "current_timestamp")
        && matches!(data_type, DataType::TimestampTz)
    {
        *data_type = DataType::Timestamp;
    }
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

/// Bind an `UPDATE` statement.
///
/// Produces a `Scan` (wrapped in `Filter` when `WHERE` is present) as
/// the input, plus resolved assignments and optional `RETURNING`.
///
/// `UPDATE … FROM other_table` is not supported in v0.2; a non-empty
/// `from` list returns [`PlanError::NotSupported`].
pub(super) fn bind_update(
    s: &UpdateStmt,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    // UPDATE … FROM: not supported in v0.2.
    if !s.from.is_empty() {
        return Err(PlanError::NotSupported(
            "UPDATE … FROM other_table (join binding lands in wave 3)",
        ));
    }

    let (table_name, meta) = lookup_target_table(catalog, &s.table)?;
    let table_schema = &meta.schema;

    // Build Scan, then optionally wrap in Filter.
    let mut plan = LogicalPlan::Scan {
        table: table_name.clone(),
        schema: table_schema.clone(),
        projection: None,
    };

    if let Some(pred_ast) = &s.r#where {
        let pred = bind_expr(pred_ast, table_schema, catalog, scope)?;
        let pred_ty = pred.data_type();
        if pred_ty != DataType::Bool && pred_ty != DataType::Null {
            return Err(PlanError::TypeMismatch(format!(
                "WHERE predicate must be boolean, got {pred_ty}"
            )));
        }
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: pred,
        };
    }

    // Assignments — value expressions are bound against the table schema.
    let assignments = bind_assignments(&s.set, table_schema, table_schema, catalog, scope)?;

    // RETURNING.
    let returning = bind_returning(&s.returning, table_schema, catalog, scope)?;
    let returning_schema = build_returning_schema(&returning)?;

    Ok(LogicalPlan::Update {
        table: table_name,
        assignments,
        input: Box::new(plan),
        returning,
        schema: returning_schema,
    })
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

/// Bind a `DELETE` statement.
///
/// Produces a `Scan` (wrapped in `Filter` when `WHERE` is present) as
/// the input, plus optional `RETURNING`.
///
/// `DELETE … USING other_table` is not supported in v0.2; a non-empty
/// `using` list returns [`PlanError::NotSupported`].
pub(super) fn bind_delete(
    s: &DeleteStmt,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    // DELETE … USING: not supported in v0.2.
    if !s.using.is_empty() {
        return Err(PlanError::NotSupported(
            "DELETE … USING other_table (join binding lands in wave 3)",
        ));
    }

    let (table_name, meta) = lookup_target_table(catalog, &s.table)?;
    let table_schema = &meta.schema;

    // Build Scan, then optionally wrap in Filter.
    let mut plan = LogicalPlan::Scan {
        table: table_name.clone(),
        schema: table_schema.clone(),
        projection: None,
    };

    if let Some(pred_ast) = &s.r#where {
        let pred = bind_expr(pred_ast, table_schema, catalog, scope)?;
        let pred_ty = pred.data_type();
        if pred_ty != DataType::Bool && pred_ty != DataType::Null {
            return Err(PlanError::TypeMismatch(format!(
                "WHERE predicate must be boolean, got {pred_ty}"
            )));
        }
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: pred,
        };
    }

    // RETURNING.
    let returning = bind_returning(&s.returning, table_schema, catalog, scope)?;
    let returning_schema = build_returning_schema(&returning)?;

    Ok(LogicalPlan::Delete {
        table: table_name,
        input: Box::new(plan),
        returning,
        schema: returning_schema,
    })
}

// ---------------------------------------------------------------------------
// MERGE
// ---------------------------------------------------------------------------

/// Bind a `MERGE INTO` statement.
///
/// Produces a source child plan plus ordered branch metadata. Expressions bind
/// against a combined schema containing target columns first and source columns
/// second, both qualified by their SQL alias/table name so bare references are
/// rejected when ambiguous.
pub(super) fn bind_merge(
    s: &MergeStmt,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    let (target, meta) = lookup_target_table(catalog, &s.target)?;
    let target_schema = meta.schema;
    let target_qualifier = s
        .target_alias
        .as_ref()
        .map_or_else(|| object_name_leaf(&s.target), |alias| alias.value.clone());
    let target_binding_schema = qualified_table_schema(&target_schema, &target_qualifier)?;

    let (source, source_scope) = bind_from(std::slice::from_ref(&s.source), catalog, &[], scope)?;
    let source_binding_schema = schema_for_qualified_binding(source.schema(), &source_scope)?;
    let combined_schema = concat_binding_schemas(&target_binding_schema, &source_binding_schema)?;

    let on = bind_bool_expr(&s.on, &combined_schema, catalog, scope, "MERGE ON")?;

    let mut clauses = Vec::with_capacity(s.clauses.len());
    for clause in &s.clauses {
        let kind = match clause.kind {
            MergeMatchKind::Matched => LogicalMergeMatchKind::Matched,
            MergeMatchKind::NotMatched => LogicalMergeMatchKind::NotMatched,
        };
        let condition = clause
            .condition
            .as_ref()
            .map(|expr| bind_bool_expr(expr, &combined_schema, catalog, scope, "MERGE WHEN"))
            .transpose()?;
        let action = match (&clause.kind, &clause.action) {
            (MergeMatchKind::Matched, MergeAction::Update { set }) => LogicalMergeAction::Update {
                assignments: bind_assignments(
                    set,
                    &target_schema,
                    &combined_schema,
                    catalog,
                    scope,
                )?,
            },
            (MergeMatchKind::Matched, MergeAction::Delete) => LogicalMergeAction::Delete,
            (MergeMatchKind::NotMatched, MergeAction::Insert { columns, values }) => {
                let columns = bind_insert_columns(columns, &target_schema)?;
                if values.len() != columns.len() {
                    return Err(PlanError::TypeMismatch(format!(
                        "MERGE INSERT column count ({}) does not match VALUES arity ({})",
                        columns.len(),
                        values.len()
                    )));
                }
                let mut bound_values = Vec::with_capacity(values.len());
                for (idx, value) in values.iter().enumerate() {
                    let mut expr = bind_expr(value, &combined_schema, catalog, scope)?;
                    let target = &target_schema.field_at(columns[idx]).data_type;
                    coerce_assignment_expr_to_type(&mut expr, target);
                    bound_values.push(expr);
                }
                LogicalMergeAction::Insert {
                    columns,
                    values: bound_values,
                }
            }
            (MergeMatchKind::Matched, MergeAction::Insert { .. }) => {
                return Err(PlanError::NotSupported(
                    "MERGE WHEN MATCHED THEN INSERT has no source-only target row",
                ));
            }
            (MergeMatchKind::NotMatched, MergeAction::Update { .. }) => {
                return Err(PlanError::NotSupported(
                    "MERGE WHEN NOT MATCHED THEN UPDATE has no target row",
                ));
            }
            (MergeMatchKind::NotMatched, MergeAction::Delete) => {
                return Err(PlanError::NotSupported(
                    "MERGE WHEN NOT MATCHED THEN DELETE has no target row",
                ));
            }
        };
        clauses.push(LogicalMergeClause {
            kind,
            condition,
            action,
        });
    }

    Ok(LogicalPlan::Merge {
        target,
        target_alias: s.target_alias.as_ref().map(|alias| alias.value.clone()),
        target_schema,
        source: Box::new(source),
        on,
        clauses,
        schema: Schema::empty(),
    })
}

fn bind_bool_expr(
    expr: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
    label: &'static str,
) -> Result<ScalarExpr, PlanError> {
    let bound = bind_expr(expr, input, catalog, scope)?;
    let ty = bound.data_type();
    if ty != DataType::Bool && ty != DataType::Null {
        return Err(PlanError::TypeMismatch(format!(
            "{label} predicate must be boolean, got {ty}"
        )));
    }
    Ok(bound)
}

fn bind_insert_columns(
    columns: &[ultrasql_parser::ast::Identifier],
    target_schema: &Schema,
) -> Result<Vec<usize>, PlanError> {
    if columns.is_empty() {
        return Ok((0..target_schema.len()).collect());
    }

    let mut out = Vec::with_capacity(columns.len());
    let mut seen: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(columns.len());
    for ident in columns {
        let col_name = ident.value.clone();
        if !seen.insert(col_name.to_ascii_lowercase()) {
            return Err(PlanError::DuplicateColumn(col_name));
        }
        let idx = target_schema
            .find(&col_name)
            .ok_or_else(|| PlanError::ColumnNotFound(col_name.clone()))?
            .0;
        out.push(idx);
    }
    Ok(out)
}

fn qualified_table_schema(schema: &Schema, qualifier: &str) -> Result<Schema, PlanError> {
    let from_scope: Vec<ScopeEntry> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| ScopeEntry {
            qualifier: qualifier.to_owned(),
            field_index: i,
            field: field.clone(),
        })
        .collect();
    schema_for_qualified_binding(schema, &from_scope)
}

fn concat_binding_schemas(left: &Schema, right: &Schema) -> Result<Schema, PlanError> {
    let mut fields = Vec::with_capacity(left.len() + right.len());
    fields.extend(left.fields().iter().cloned());
    fields.extend(right.fields().iter().cloned());
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(format!("MERGE binding schema: {e}")))
}

fn object_name_leaf(name: &ObjectName) -> String {
    name.parts
        .last()
        .map_or_else(String::new, |part| part.value.to_ascii_lowercase())
}

fn lookup_target_table(
    catalog: &dyn Catalog,
    object_name: &ObjectName,
) -> Result<(String, TableMeta), PlanError> {
    let resolved = lookup_table_reference(catalog, object_name)?;
    Ok((resolved.plan_name, resolved.meta))
}
