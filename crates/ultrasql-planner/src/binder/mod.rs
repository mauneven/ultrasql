//! Binder — turn a parser AST into a typed logical plan.
//!
//! The binder is a single pass over the AST. For a `SELECT` statement it:
//!
//! 1. Resolves the `FROM` clause into a join tree. A single named table
//!    becomes a [`crate::plan::LogicalPlan::Scan`]; explicit joins produce
//!    [`crate::plan::LogicalPlan::Join`]; subqueries become inner scopes.
//! 2. Expands `SELECT *` and `SELECT t.*` by walking the FROM scope.
//! 3. Detects aggregate calls in the projection / HAVING; wraps the plan
//!    in [`crate::plan::LogicalPlan::Aggregate`] when needed.
//! 4. Folds `UNION` / `INTERSECT` / `EXCEPT` tails into
//!    [`crate::plan::LogicalPlan::SetOp`].
//! 5. Binds leading CTEs and wraps the body in
//!    [`crate::plan::LogicalPlan::Cte`] nodes.
//! 6. Resolves column references against the producing operator's schema;
//!    bare column names become [`crate::expr::ScalarExpr::Column`] nodes
//!    with a 0-based index.
//! 7. Type-checks expressions, using
//!    [`ultrasql_core::DataType::numeric_join`] for arithmetic and a
//!    simple shape rule for comparisons and boolean operators.
//! 8. Wraps the plan in `Filter` / `Project` / `Sort` / `Limit` in the
//!    canonical SQL evaluation order.
//!
//! For DML statements the binder produces the corresponding plan nodes:
//!
//! - `INSERT` → [`crate::plan::LogicalPlan::Insert`] with a `Values` or
//!   bound-`Select` child for the row source.
//! - `UPDATE` → [`crate::plan::LogicalPlan::Update`] over a `Scan` /
//!   `Filter` child. `UPDATE … FROM other_table` returns
//!   [`crate::error::PlanError::NotSupported`].
//! - `DELETE` → [`crate::plan::LogicalPlan::Delete`] over a `Scan` /
//!   `Filter` child. `DELETE … USING other_table` similarly returns
//!   `NotSupported`.
//! - `TRUNCATE` → [`crate::plan::LogicalPlan::Truncate`]; every table
//!   name is validated against the catalog.
//!
//! `EXCLUDED` column references in `ON CONFLICT DO UPDATE` are not
//! supported in v0.2; the binder returns `NotSupported` for that form.
//!
//! ### Recursive CTEs
//!
//! `WITH RECURSIVE` is parsed and the `recursive` flag is recorded on the
//! produced [`crate::plan::LogicalPlan::Cte`] node. The recursion fixpoint
//! is **not** evaluated at this layer; that is deferred to a future executor
//! wave. Until then a recursive CTE binding resolves the CTE's definition
//! non-recursively.

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_parser::ast::{
    AlterTableAction, AlterTableStmt, Assignment, BinaryOp, ColumnConstraint,
    ConflictTarget as AstConflictTarget, CreateIndexStmt, CreateTableStmt, DeleteStmt, Distinct,
    DropTableStmt, Expr, InsertSource, InsertStmt, JoinCondition, JoinOp, Literal, NullsOrder,
    ObjectName, OnConflict, OrderItem, SelectItem, SelectStmt, SetOp, SetQuantifier, SortDirection,
    Statement, TableRef, TruncateStmt, TypeName, UnaryOp, UpdateStmt,
};

use crate::catalog::Catalog;
use crate::error::PlanError;
use crate::expr::ScalarExpr;
use crate::plan::{
    AggregateFunc, ConflictTarget, LogicalAggregateExpr, LogicalAlterTableAction,
    LogicalJoinCondition, LogicalJoinType, LogicalOnConflict, LogicalPlan, LogicalSetOp,
    LogicalSetQuantifier, SortKey,
};
use crate::scope::{ScopeFrame, ScopeStack};

/// Bind a [`Statement`] against the supplied catalog and produce a
/// typed logical plan.
///
/// # Errors
///
/// Returns a [`PlanError`] for any of:
/// - missing table or column,
/// - ambiguous column reference,
/// - a type mismatch in an operator,
/// - a construct the binder does not yet implement.
pub fn bind(stmt: &Statement, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    let mut scope = ScopeStack::new();
    match stmt {
        Statement::Select(s) => bind_select(s, catalog, &mut scope),
        Statement::Insert(s) => bind_insert(s, catalog, &mut scope),
        Statement::Update(s) => bind_update(s, catalog, &mut scope),
        Statement::Delete(s) => bind_delete(s, catalog, &mut scope),
        Statement::Truncate(s) => bind_truncate(s, catalog),
        Statement::CreateTable(s) => bind_create_table(s, catalog),
        Statement::CreateIndex(s) => bind_create_index(s, catalog),
        Statement::DropTable(s) => bind_drop_table(s, catalog),
        Statement::AlterTable(s) => bind_alter_table(s, catalog),
        // Transaction-control statements have no catalog dependency: the
        // server inspects the per-session [`TxnState`] and dispatches
        // accordingly. The binder emits the corresponding LogicalPlan
        // variants so the Simple- and Extended-Query paths share a single
        // dispatch surface.
        Statement::Begin { .. } => Ok(LogicalPlan::Begin {
            schema: Schema::empty(),
        }),
        Statement::Commit { .. } => Ok(LogicalPlan::Commit {
            schema: Schema::empty(),
        }),
        Statement::Rollback { .. } => Ok(LogicalPlan::Rollback {
            schema: Schema::empty(),
        }),
        // Savepoint statements: lowercase the name so `ROLLBACK TO`
        // matches case-insensitively (PostgreSQL behaviour for unquoted
        // identifiers). Infallible — the parser has already validated
        // the AST shape, so these are direct AST → LogicalPlan
        // translations with no further checking.
        Statement::Savepoint(s) => Ok(LogicalPlan::Savepoint {
            name: s.name.value.to_ascii_lowercase(),
            schema: Schema::empty(),
        }),
        Statement::RollbackToSavepoint(s) => Ok(LogicalPlan::RollbackToSavepoint {
            name: s.name.value.to_ascii_lowercase(),
            schema: Schema::empty(),
        }),
        Statement::ReleaseSavepoint(s) => Ok(LogicalPlan::ReleaseSavepoint {
            name: s.name.value.to_ascii_lowercase(),
            schema: Schema::empty(),
        }),
        Statement::PrepareTransaction { gid, .. } => Ok(LogicalPlan::PrepareTransaction {
            gid: gid.clone(),
            schema: Schema::empty(),
        }),
        Statement::CommitPrepared { gid, .. } => Ok(LogicalPlan::CommitPrepared {
            gid: gid.clone(),
            schema: Schema::empty(),
        }),
        Statement::RollbackPrepared { gid, .. } => Ok(LogicalPlan::RollbackPrepared {
            gid: gid.clone(),
            schema: Schema::empty(),
        }),
        _ => Err(PlanError::NotSupported("statement variant")),
    }
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

/// Bind an `INSERT` statement.
///
/// Steps:
/// 1. Resolve the target table in the catalog.
/// 2. Resolve the explicit column list to schema indices (error on unknown
///    or duplicate names). An empty column list expands to all columns.
/// 3. Build the row source: `Values` rows → `LogicalPlan::Values`;
///    `DEFAULT VALUES` → a zero-column `Values` placeholder; `SELECT` →
///    recursively bound select plan.
/// 4. Validate source arity vs. target column count.
/// 5. Bind `ON CONFLICT` (if present). `EXCLUDED` references in DO UPDATE
///    assignments are not supported in v0.2.
/// 6. Bind `RETURNING` expressions against the table schema.
fn bind_insert(
    s: &InsertStmt,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    // 1. Catalog lookup.
    let table_name = object_name_simple(&s.table);
    let meta = catalog
        .lookup_table(&table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
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
        InsertSource::Values(rows) => bind_values_rows(rows, expected_arity, catalog, scope)?,
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
            let assignments = bind_assignments(set, table_schema, catalog, scope)?;
            let where_expr = r#where
                .as_ref()
                .map(|e| {
                    let pred = bind_expr(e, table_schema, catalog, scope)?;
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

/// Bind a list of `col = expr` assignments into `(index, ScalarExpr)` pairs.
///
/// Each target column name is resolved against `table_schema`. Expression
/// values are bound against the same schema (the pre-update row view).
///
/// PostgreSQL rejects `UPDATE t SET col=1, col=2`; this function mirrors
/// that behaviour by returning [`PlanError::DuplicateColumn`] on the first
/// repeated target.
fn bind_assignments(
    set: &[Assignment],
    table_schema: &Schema,
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
        let expr = bind_expr(&a.value, table_schema, catalog, scope)?;
        out.push((idx, expr));
    }
    Ok(out)
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
fn bind_update(
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

    let table_name = object_name_simple(&s.table);
    let meta = catalog
        .lookup_table(&table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
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
    let assignments = bind_assignments(&s.set, table_schema, catalog, scope)?;

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
fn bind_delete(
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

    let table_name = object_name_simple(&s.table);
    let meta = catalog
        .lookup_table(&table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
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
// SELECT
// ---------------------------------------------------------------------------

/// A per-column scope entry used for wildcard expansion and qualified
/// column resolution.
///
/// Each entry tracks which table qualifier (alias or table name) owns the
/// field, along with the field's position in the combined FROM schema.
struct ScopeEntry {
    /// Table qualifier (alias or lowercased table name). Empty string
    /// for anonymous derived tables without a qualifier.
    qualifier: String,
    /// 0-based index into the full FROM schema.
    field_index: usize,
    /// The field itself (type + name).
    field: Field,
}

/// Bind a `SELECT` statement.
///
/// Handles: CTEs, FROM clause (single tables, explicit joins, subqueries),
/// wildcard expansion, GROUP BY + aggregates, HAVING, set operations,
/// ORDER BY, LIMIT / OFFSET.
fn bind_select(
    select: &SelectStmt,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    if !matches!(select.distinct, Distinct::None | Distinct::All) {
        return Err(PlanError::NotSupported("SELECT DISTINCT"));
    }

    // Build the CTE overlay (maps CTE name → its bound schema + plan index).
    let mut cte_catalog: Vec<(String, Schema)> = Vec::new();
    let mut cte_plans: Vec<(String, bool, LogicalPlan)> = Vec::new();
    for cte in &select.ctes {
        let cte_plan = bind_select_with_ctes(&cte.query, catalog, &cte_catalog, scope)?;
        let cte_schema = cte_plan.schema().clone();
        let cte_name = cte.name.value.to_ascii_lowercase();
        // Apply column aliases if provided.
        let cte_schema = if cte.column_aliases.is_empty() {
            cte_schema
        } else {
            apply_column_aliases(&cte_schema, &cte.column_aliases)?
        };
        cte_catalog.push((cte_name.clone(), cte_schema));
        cte_plans.push((cte_name, cte.recursive, cte_plan));
    }

    // Bind the main query body using the CTE overlay.
    let mut plan = bind_select_body(select, catalog, &cte_catalog, scope)?;

    // Fold set-op tails left-to-right.
    for tail in &select.set_ops {
        let right_plan = bind_select_with_ctes(&tail.right, catalog, &cte_catalog, scope)?;
        plan = bind_set_op(plan, tail.op, tail.quantifier, right_plan)?;
    }

    // Wrap with CTE nodes (innermost first so the outermost CTE wraps last).
    // We reverse so that the first CTE declared is the outermost Cte node,
    // which matches the scoping intent.
    for (cte_name, recursive, def_plan) in cte_plans.into_iter().rev() {
        let body_schema = plan.schema().clone();
        plan = LogicalPlan::Cte {
            name: cte_name,
            recursive,
            definition: Box::new(def_plan),
            body: Box::new(plan),
            schema: body_schema,
        };
    }

    Ok(plan)
}

/// Bind a `SelectStmt` that may reference CTEs in `cte_catalog` plus the
/// regular catalog.
pub(super) fn bind_select_with_ctes(
    select: &SelectStmt,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    // First process any nested CTEs, then the body.
    let mut nested_cte_catalog: Vec<(String, Schema)> = cte_catalog.to_vec();
    let mut nested_cte_plans: Vec<(String, bool, LogicalPlan)> = Vec::new();
    for cte in &select.ctes {
        let cte_plan = bind_select_with_ctes(&cte.query, catalog, &nested_cte_catalog, scope)?;
        let cte_schema = cte_plan.schema().clone();
        let cte_name = cte.name.value.to_ascii_lowercase();
        let cte_schema = if cte.column_aliases.is_empty() {
            cte_schema
        } else {
            apply_column_aliases(&cte_schema, &cte.column_aliases)?
        };
        nested_cte_catalog.push((cte_name.clone(), cte_schema));
        nested_cte_plans.push((cte_name, cte.recursive, cte_plan));
    }

    let mut plan = bind_select_body(select, catalog, &nested_cte_catalog, scope)?;

    // Fold set-op tails.
    for tail in &select.set_ops {
        let right_plan = bind_select_with_ctes(&tail.right, catalog, &nested_cte_catalog, scope)?;
        plan = bind_set_op(plan, tail.op, tail.quantifier, right_plan)?;
    }

    // Wrap with nested CTEs.
    for (cte_name, recursive, def_plan) in nested_cte_plans.into_iter().rev() {
        let body_schema = plan.schema().clone();
        plan = LogicalPlan::Cte {
            name: cte_name,
            recursive,
            definition: Box::new(def_plan),
            body: Box::new(plan),
            schema: body_schema,
        };
    }

    Ok(plan)
}

/// Apply a list of column alias overrides to a schema.
///
/// Alias list length must match schema arity; short lists are padded
/// with the original names (never rejected as an error since PostgreSQL
/// allows partial alias lists in some contexts), but in practice the
/// parser always emits a full list.
fn apply_column_aliases(
    schema: &Schema,
    aliases: &[ultrasql_parser::ast::Identifier],
) -> Result<Schema, PlanError> {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let name = aliases
                .get(i)
                .map_or_else(|| f.name.clone(), |a| a.value.clone());
            Field {
                name,
                data_type: f.data_type.clone(),
                nullable: f.nullable,
            }
        })
        .collect();
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(format!("CTE column aliases: {e}")))
}

/// Bind a set operation between two already-bound plans.
fn bind_set_op(
    left: LogicalPlan,
    op: SetOp,
    quantifier: SetQuantifier,
    right: LogicalPlan,
) -> Result<LogicalPlan, PlanError> {
    let left_arity = left.schema().len();
    let right_arity = right.schema().len();
    if left_arity != right_arity {
        return Err(PlanError::TypeMismatch(format!(
            "set operation: left side has {left_arity} columns, right side has {right_arity}"
        )));
    }

    // Build output schema: left column names, types are numeric_join per column.
    let fields: Result<Vec<Field>, PlanError> = left
        .schema()
        .fields()
        .iter()
        .zip(right.schema().fields().iter())
        .map(|(lf, rf)| {
            let out_ty = if matches!(lf.data_type, DataType::Null) {
                rf.data_type.clone()
            } else if matches!(rf.data_type, DataType::Null) {
                lf.data_type.clone()
            } else if lf.data_type.is_numeric() && rf.data_type.is_numeric() {
                lf.data_type.numeric_join(&rf.data_type).map_err(|_| {
                    PlanError::TypeMismatch(format!(
                        "set operation column type mismatch: {} vs {}",
                        lf.data_type, rf.data_type
                    ))
                })?
            } else {
                // For non-numeric columns, left wins (PostgreSQL convention).
                lf.data_type.clone()
            };
            Ok(Field::nullable(lf.name.clone(), out_ty))
        })
        .collect();
    let schema =
        Schema::new(fields?).map_err(|e| PlanError::TypeMismatch(format!("set op schema: {e}")))?;

    let logical_op = match op {
        SetOp::Union => LogicalSetOp::Union,
        SetOp::Intersect => LogicalSetOp::Intersect,
        SetOp::Except => LogicalSetOp::Except,
    };
    let logical_q = match quantifier {
        SetQuantifier::All => LogicalSetQuantifier::All,
        SetQuantifier::Distinct => LogicalSetQuantifier::Distinct,
    };

    Ok(LogicalPlan::SetOp {
        op: logical_op,
        quantifier: logical_q,
        left: Box::new(left),
        right: Box::new(right),
        schema,
    })
}

/// The core `SELECT` body binding: FROM → WHERE → GROUP BY → HAVING →
/// SELECT list → ORDER BY → LIMIT/OFFSET.
///
/// Does *not* handle set-op tails or CTE wrapping; that is done by
/// [`bind_select`] / [`bind_select_with_ctes`].
#[allow(clippy::too_many_lines)]
fn bind_select_body(
    select: &SelectStmt,
    catalog: &dyn Catalog,
    cte_catalog: &[(String, Schema)],
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    if !matches!(select.distinct, Distinct::None | Distinct::All) {
        return Err(PlanError::NotSupported("SELECT DISTINCT"));
    }

    // ------------------------------------------------------------------
    // FROM clause → join tree
    // ------------------------------------------------------------------
    let (mut plan, from_scope) = bind_from(&select.from, catalog, cte_catalog, scope)?;

    // ------------------------------------------------------------------
    // WHERE
    // ------------------------------------------------------------------
    if let Some(pred_ast) = &select.r#where {
        let pred = bind_expr(pred_ast, plan.schema(), catalog, scope)?;
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

    // ------------------------------------------------------------------
    // Aggregate detection
    // ------------------------------------------------------------------
    // Walk the projection list to detect aggregate calls. If any are
    // present, or if GROUP BY is non-empty, we need an Aggregate node.
    let has_group_by = !select.group_by.is_empty();
    let has_aggregates = select.projection.iter().any(projection_item_has_aggregate);
    let having_has_agg = select.having.as_ref().is_some_and(expr_has_aggregate);

    if has_group_by || has_aggregates || having_has_agg {
        plan = bind_aggregate(plan, select, &from_scope, catalog, scope)?;
        // HAVING goes above the aggregate.
        if let Some(having_ast) = &select.having {
            let pred = bind_expr(having_ast, plan.schema(), catalog, scope)?;
            let pred_ty = pred.data_type();
            if pred_ty != DataType::Bool && pred_ty != DataType::Null {
                return Err(PlanError::TypeMismatch(format!(
                    "HAVING predicate must be boolean, got {pred_ty}"
                )));
            }
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: pred,
            };
        }
        // Projection after aggregation binds against aggregate output schema.
        let projected = bind_projection_agg(&select.projection, plan.schema(), catalog, scope)?;
        let proj_fields: Vec<Field> = projected
            .iter()
            .map(|(e, name)| Field::nullable(name, e.data_type()))
            .collect();
        let proj_schema = Schema::new(proj_fields)
            .map_err(|e| PlanError::TypeMismatch(format!("projection: {e}")))?;

        let sort_keys = bind_order_by(&select.order_by, plan.schema(), catalog, scope)?;
        if !sort_keys.is_empty() {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                keys: sort_keys,
            };
        }

        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: projected,
            schema: proj_schema,
        };
    } else {
        // ------------------------------------------------------------------
        // Non-aggregate path: SELECT list → ORDER BY → projection
        // ------------------------------------------------------------------
        let projected = bind_projection_with_scope(
            &select.projection,
            plan.schema(),
            &from_scope,
            catalog,
            scope,
        )?;
        let proj_fields: Vec<Field> = projected
            .iter()
            .map(|(e, name)| Field::nullable(name, e.data_type()))
            .collect();
        let proj_schema = Schema::new(proj_fields)
            .map_err(|e| PlanError::TypeMismatch(format!("projection: {e}")))?;

        let sort_keys = bind_order_by(&select.order_by, plan.schema(), catalog, scope)?;
        if !sort_keys.is_empty() {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                keys: sort_keys,
            };
        }

        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: projected,
            schema: proj_schema,
        };
    }

    // ------------------------------------------------------------------
    // LIMIT / OFFSET
    // ------------------------------------------------------------------
    let limit_val = match &select.limit {
        Some(e) => Some(bind_unsigned_literal(e, "LIMIT")?),
        None => None,
    };
    let offset_val = match &select.offset {
        Some(e) => bind_unsigned_literal(e, "OFFSET")?,
        None => 0,
    };
    if let Some(n) = limit_val {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n,
            offset: offset_val,
        };
    } else if offset_val != 0 {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n: u64::MAX,
            offset: offset_val,
        };
    }

    Ok(plan)
}

// ---------------------------------------------------------------------------
// FROM clause → join tree
// ---------------------------------------------------------------------------

/// Bind the FROM clause. Returns the plan and a flat scope for wildcard
/// expansion.
///
/// An empty FROM list produces `LogicalPlan::Empty` with an empty scope.
/// A non-empty list is folded into a join tree using the scope entries
/// from all participating tables.
fn bind_from(
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

    // Fold left-to-right.
    let mut iter = from_items.iter();
    let first = iter.next().expect("at least one item checked above");
    let (mut plan, mut from_scope) = bind_table_ref(first, catalog, cte_catalog, outer_scope)?;

    for item in iter {
        let (right_plan, right_scope) = bind_table_ref(item, catalog, cte_catalog, outer_scope)?;
        // Comma-join: CROSS JOIN.
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

/// Bind a single [`TableRef`] AST node into `(LogicalPlan, scope)`.
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

            // Check CTE catalog first.
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
            // Apply column aliases if provided.
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
            // Wrap inner plan with a Scan-like node. Since we don't have a
            // SubqueryScan variant yet, we use the plan directly and construct
            // a new scan-like wrapper by re-projecting to apply the alias schema.
            let plan = rebuild_subquery_plan(inner_plan, &inner_schema, &qualifier)?;
            Ok((plan, from_scope))
        }
        TableRef::Join {
            left,
            op,
            right,
            condition,
            ..
        } => bind_explicit_join(left, *op, right, condition, catalog, cte_catalog, scope),
    }
}

/// Rebuild a subquery plan by wrapping it in a Project that applies the
/// alias schema (possibly renamed by `column_aliases`).
///
/// This gives the subquery a stable schema name for subsequent column
/// resolution without needing a dedicated `SubqueryScan` plan node.
fn rebuild_subquery_plan(
    inner_plan: LogicalPlan,
    alias_schema: &Schema,
    _qualifier: &str,
) -> Result<LogicalPlan, PlanError> {
    // Build a projection that re-names each field.
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
    // The qualifier is tracked in the scope entries (see call site), not in
    // the plan node itself, so it is intentionally unused here.
    Ok(LogicalPlan::Project {
        input: Box::new(inner_plan),
        exprs,
        schema: proj_schema,
    })
}

/// Bind an explicit join node.
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
            // Build the concatenated schema to bind the ON predicate against.
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

/// Resolve USING column names to `(left_idx, right_idx)` pairs.
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

/// Build the output schema for a USING join.
///
/// The schema is: USING columns once (from left), remaining left columns,
/// remaining right columns. Nullability follows the join type.
fn build_using_schema(
    left: &Schema,
    right: &Schema,
    pairs: &[(usize, usize)],
    join_type: LogicalJoinType,
) -> Result<Schema, PlanError> {
    let using_set: std::collections::HashSet<usize> = pairs.iter().map(|(l, _)| *l).collect();
    let right_using_set: std::collections::HashSet<usize> = pairs.iter().map(|(_, r)| *r).collect();

    let mut out_fields: Vec<Field> = Vec::new();
    // USING columns (from left, nullability as per join type).
    for &(left_idx, _) in pairs {
        let f = left.field_at(left_idx);
        let nullable = matches!(join_type, LogicalJoinType::FullOuter) || f.nullable;
        out_fields.push(Field {
            name: f.name.clone(),
            data_type: f.data_type.clone(),
            nullable,
        });
    }
    // Remaining left columns.
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
    // Remaining right columns.
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

/// Concatenate two schemas for a CROSS or non-outer join.
///
/// On name collision, the right side column is prefixed with a disambiguating
/// qualifier to avoid rejecting the join; the optimizer can eliminate the
/// prefix once it knows which column is actually needed.
fn concat_schemas_cross(left: &Schema, right: &Schema) -> Result<Schema, PlanError> {
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
            // Disambiguate by keeping the name as-is; Schema::new will reject
            // duplicates. For joins, the optimizer resolves via the scope's
            // qualifier. We allow duplicates by making the schema with a raw
            // Vec — instead use a safe approach and just push.
            // The Schema::new check only fires here if BOTH sides have the same
            // lowercase name, which is normal for joins (e.g. id = id).
            // We'll use the right field with a suffix only if we can't avoid it.
            // For now: suffix with "_1" only when there's a collision.
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

/// Concatenate two schemas for an explicit join under outer-join nullability
/// rules.
///
/// - `LEFT JOIN`: right columns become nullable.
/// - `RIGHT JOIN`: left columns become nullable.
/// - `FULL OUTER JOIN`: both sides become nullable.
/// - `INNER` / `CROSS`: columns retain their original nullability (cross uses
///   the simpler helper).
fn concat_schemas_for_join(
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

/// Merge two scope lists, adjusting right side field indices by `left_len`.
fn merge_scopes(left: Vec<ScopeEntry>, right: Vec<ScopeEntry>, left_len: usize) -> Vec<ScopeEntry> {
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

// ---------------------------------------------------------------------------
// Aggregate detection and binding
// ---------------------------------------------------------------------------

/// Return `true` if `item` contains an aggregate call anywhere in its
/// expression tree.
fn projection_item_has_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::Expr { expr, .. } => expr_has_aggregate(expr),
        SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
    }
}

/// Return `true` if `expr` contains an aggregate call.
fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Call { name, .. } => {
            is_aggregate_name(name.parts.last().map_or("", |p| p.value.as_str()))
        }
        Expr::Unary { expr: inner, .. }
        | Expr::Paren { expr: inner, .. }
        | Expr::IsNull { expr: inner, .. } => expr_has_aggregate(inner),
        Expr::Binary { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        _ => false,
    }
}

/// Return `true` if `name` is a known aggregate function.
pub(super) fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "bool_and"
            | "bool_or"
            | "string_agg"
            | "array_agg"
    )
}

/// Classify an aggregate function name into [`AggregateFunc`].
fn classify_aggregate(name: &str, args_empty: bool) -> Option<AggregateFunc> {
    match name.to_ascii_lowercase().as_str() {
        "count" if args_empty => Some(AggregateFunc::CountStar),
        "count" => Some(AggregateFunc::Count),
        "sum" => Some(AggregateFunc::Sum),
        "avg" => Some(AggregateFunc::Avg),
        "min" => Some(AggregateFunc::Min),
        "max" => Some(AggregateFunc::Max),
        "bool_and" => Some(AggregateFunc::BoolAnd),
        "bool_or" => Some(AggregateFunc::BoolOr),
        "string_agg" => Some(AggregateFunc::StringAgg),
        "array_agg" => Some(AggregateFunc::ArrayAgg),
        _ => None,
    }
}

/// Return type for a given aggregate function and argument type.
///
/// The widening rules below mirror PostgreSQL and the executor's
/// `add_values` helper in `ultrasql_executor::hash_aggregate`:
///
/// - `SUM` over any integer type returns `Int64` (BIGINT) so an
///   accumulating fold of 32-bit inputs cannot silently overflow.
/// - `SUM` over either floating type returns `Float64`.
/// - `AVG` always returns `Float64`; the executor's `divide_value`
///   helper performs the integer-to-float conversion when finalising.
fn aggregate_return_type(func: AggregateFunc, arg_type: DataType) -> DataType {
    match func {
        AggregateFunc::CountStar | AggregateFunc::Count => DataType::Int64,
        AggregateFunc::Sum => match arg_type {
            DataType::Int16 | DataType::Int32 | DataType::Int64 => DataType::Int64,
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            other if other.is_numeric() => other,
            _ => DataType::Null,
        },
        AggregateFunc::Avg => {
            if arg_type.is_numeric() {
                DataType::Float64
            } else {
                DataType::Null
            }
        }
        AggregateFunc::Min | AggregateFunc::Max => arg_type,
        AggregateFunc::BoolAnd | AggregateFunc::BoolOr => DataType::Bool,
        AggregateFunc::StringAgg => DataType::Text { max_len: None },
        AggregateFunc::ArrayAgg => DataType::Array(Box::new(arg_type)),
    }
}

/// Bind the `GROUP BY` + aggregates into a `LogicalPlan::Aggregate` node.
///
/// The aggregate output schema is: `[group_by_fields ..., aggregate_fields ...]`.
fn bind_aggregate(
    input: LogicalPlan,
    select: &SelectStmt,
    _from_scope: &[ScopeEntry],
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<LogicalPlan, PlanError> {
    let input_schema = input.schema().clone();

    // Bind GROUP BY expressions against the input schema.
    let mut group_by: Vec<ScalarExpr> = Vec::with_capacity(select.group_by.len());
    for e in &select.group_by {
        group_by.push(bind_expr(e, &input_schema, catalog, scope)?);
    }

    // Collect aggregate calls from the SELECT projection and (if present)
    // from HAVING.
    let mut aggregates: Vec<LogicalAggregateExpr> = Vec::new();
    for item in &select.projection {
        if let SelectItem::Expr { expr, alias, .. } = item {
            collect_aggregates(
                expr,
                alias.as_ref(),
                &input_schema,
                &mut aggregates,
                catalog,
                scope,
            )?;
        }
    }
    if let Some(having) = &select.having {
        collect_aggregates(having, None, &input_schema, &mut aggregates, catalog, scope)?;
    }

    // Build the output schema.
    let mut out_fields: Vec<Field> = Vec::new();
    for (i, g) in group_by.iter().enumerate() {
        let name = match g {
            ScalarExpr::Column { name, .. } => name.clone(),
            _ => format!("group{i}"),
        };
        out_fields.push(Field::nullable(name, g.data_type()));
    }
    for agg in &aggregates {
        out_fields.push(Field::nullable(
            agg.output_name.clone(),
            agg.data_type.clone(),
        ));
    }
    // Deduplicate names by appending a suffix for duplicates.
    let agg_schema = build_unique_schema(out_fields)?;

    Ok(LogicalPlan::Aggregate {
        input: Box::new(input),
        group_by,
        aggregates,
        schema: agg_schema,
    })
}

/// Build a schema from fields, disambiguating duplicate names with `_N` suffixes.
fn build_unique_schema(mut fields: Vec<Field>) -> Result<Schema, PlanError> {
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for f in &mut fields {
        let lower = f.name.to_ascii_lowercase();
        let count = seen.entry(lower).or_insert(0);
        if *count > 0 {
            f.name = format!("{}_{}", f.name, *count);
        }
        *count += 1;
    }
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(format!("aggregate schema: {e}")))
}

/// Walk an expression, extracting any aggregate calls into `out`.
///
/// Aggregate calls are not expanded recursively (nested aggregates are
/// rejected by PostgreSQL).
fn collect_aggregates(
    expr: &Expr,
    alias: Option<&ultrasql_parser::ast::Identifier>,
    input_schema: &Schema,
    out: &mut Vec<LogicalAggregateExpr>,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<(), PlanError> {
    match expr {
        Expr::Call {
            name,
            args,
            distinct,
            ..
        } => {
            let func_name = name
                .parts
                .last()
                .map_or("", |p| p.value.as_str())
                .to_ascii_lowercase();
            // The parser encodes COUNT(*) as a single Expr::Column arg whose
            // name is "*". Treat that as an empty arg list for classification.
            let is_star_arg = args.len() == 1
                && matches!(&args[0], Expr::Column { name: n }
                    if n.parts.len() == 1 && n.parts[0].value == "*");
            let args_empty_or_star = args.is_empty() || is_star_arg;
            if let Some(func) = classify_aggregate(&func_name, args_empty_or_star) {
                // Check if already in the list (dedup by position? use all).
                let (arg_expr, arg_ty) = if args_empty_or_star {
                    (None, DataType::Null)
                } else {
                    let bound = bind_expr(&args[0], input_schema, catalog, scope)?;
                    let ty = bound.data_type();
                    (Some(bound), ty)
                };
                let ret_ty = aggregate_return_type(func, arg_ty);
                let output_name = alias.map_or_else(
                    || derive_agg_output_name(&func_name, args),
                    |a| a.value.clone(),
                );
                // Avoid duplicate registration when HAVING references the same agg.
                let already = out.iter().any(|a| {
                    a.output_name == output_name
                        && std::mem::discriminant(&a.func) == std::mem::discriminant(&func)
                });
                if !already {
                    out.push(LogicalAggregateExpr {
                        func,
                        arg: arg_expr,
                        distinct: *distinct,
                        output_name,
                        data_type: ret_ty,
                    });
                }
                Ok(())
            } else {
                Err(PlanError::NotSupported(
                    "non-aggregate function calls in aggregation context",
                ))
            }
        }
        Expr::Paren { expr: inner, .. } | Expr::Unary { expr: inner, .. } => {
            collect_aggregates(inner, alias, input_schema, out, catalog, scope)
        }
        Expr::Binary { left, right, .. } => {
            collect_aggregates(left, None, input_schema, out, catalog, scope)?;
            collect_aggregates(right, None, input_schema, out, catalog, scope)
        }
        // Non-aggregate expressions are fine in GROUP BY columns.
        _ => Ok(()),
    }
}

/// Derive a default output name for an aggregate call.
pub(super) fn derive_agg_output_name(func_name: &str, _args: &[Expr]) -> String {
    func_name.to_string()
}

/// Bind a projection list after aggregation has been applied.
///
/// Aggregate calls in the projection are replaced with column references
/// into the aggregate output schema.
fn bind_projection_agg(
    items: &[SelectItem],
    agg_schema: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<Vec<(ScalarExpr, String)>, PlanError> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                // Wildcard after aggregation: expand all aggregate output columns.
                for (i, f) in agg_schema.fields().iter().enumerate() {
                    out.push((
                        ScalarExpr::Column {
                            name: f.name.clone(),
                            index: i,
                            data_type: f.data_type.clone(),
                        },
                        f.name.clone(),
                    ));
                }
            }
            SelectItem::Expr { expr, alias, .. } => {
                // If this is an aggregate call, replace with a column ref into
                // the aggregate schema.
                let bound = bind_expr_or_agg_ref(expr, agg_schema, catalog, scope)?;
                let name = alias
                    .as_ref()
                    .map_or_else(|| derive_output_name(expr, &bound), |a| a.value.clone());
                out.push((bound, name));
            }
        }
    }
    Ok(out)
}

/// Bind an expression, replacing aggregate calls with column references
/// into the post-aggregate schema.
fn bind_expr_or_agg_ref(
    expr: &Expr,
    agg_schema: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    match expr {
        Expr::Call { name, args, .. } => {
            let func_name = name
                .parts
                .last()
                .map_or("", |p| p.value.as_str())
                .to_ascii_lowercase();
            if is_aggregate_name(&func_name) {
                let agg_name = derive_agg_output_name(&func_name, args);
                // Find in agg_schema.
                if let Some((i, f)) = agg_schema.find(&agg_name) {
                    return Ok(ScalarExpr::Column {
                        name: f.name.clone(),
                        index: i,
                        data_type: f.data_type.clone(),
                    });
                }
            }
            // Not an aggregate or not found by derived name: fall through to
            // regular expression binding against the post-aggregate schema.
            bind_expr(expr, agg_schema, catalog, scope)
        }
        _ => bind_expr(expr, agg_schema, catalog, scope),
    }
}

// ---------------------------------------------------------------------------
// Projection with wildcard expansion
// ---------------------------------------------------------------------------

/// Bind a projection list, expanding `*` and `t.*` using the scope entries.
fn bind_projection_with_scope(
    items: &[SelectItem],
    input: &Schema,
    from_scope: &[ScopeEntry],
    catalog: &dyn Catalog,
    outer_scope: &mut ScopeStack,
) -> Result<Vec<(ScalarExpr, String)>, PlanError> {
    let mut out = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard { .. } => {
                // Expand to all columns in the FROM scope.
                if from_scope.is_empty() {
                    // No FROM: expand from the input schema directly.
                    for (i, f) in input.fields().iter().enumerate() {
                        out.push((
                            ScalarExpr::Column {
                                name: f.name.clone(),
                                index: i,
                                data_type: f.data_type.clone(),
                            },
                            f.name.clone(),
                        ));
                    }
                } else {
                    for entry in from_scope {
                        out.push((
                            ScalarExpr::Column {
                                name: entry.field.name.clone(),
                                index: entry.field_index,
                                data_type: entry.field.data_type.clone(),
                            },
                            entry.field.name.clone(),
                        ));
                    }
                }
            }
            SelectItem::QualifiedWildcard { qualifier, .. } => {
                let q = &qualifier.value;
                let matching: Vec<_> = from_scope
                    .iter()
                    .filter(|e| e.qualifier.eq_ignore_ascii_case(q))
                    .collect();
                if matching.is_empty() {
                    return Err(PlanError::TableNotFound(q.clone()));
                }
                for entry in matching {
                    out.push((
                        ScalarExpr::Column {
                            name: entry.field.name.clone(),
                            index: entry.field_index,
                            data_type: entry.field.data_type.clone(),
                        },
                        entry.field.name.clone(),
                    ));
                }
            }
            SelectItem::Expr { expr, alias, .. } => {
                let bound = bind_expr(expr, input, catalog, outer_scope)?;
                let name = alias
                    .as_ref()
                    .map_or_else(|| derive_output_name(expr, &bound), |a| a.value.clone());
                out.push((bound, name));
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Build the `RETURNING` schema from the resolved `(expr, name)` pairs.
fn build_returning_schema(returning: &[(ScalarExpr, String)]) -> Result<Schema, PlanError> {
    if returning.is_empty() {
        return Ok(Schema::empty());
    }
    let fields: Vec<Field> = returning
        .iter()
        .map(|(e, n)| Field::nullable(n, e.data_type()))
        .collect();
    Schema::new(fields).map_err(|e| PlanError::TypeMismatch(format!("RETURNING schema: {e}")))
}

/// Bind a `RETURNING` projection list against `table_schema`.
fn bind_returning(
    items: &[SelectItem],
    table_schema: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<Vec<(ScalarExpr, String)>, PlanError> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                return Err(PlanError::NotSupported("wildcard in RETURNING clause"));
            }
            SelectItem::Expr { expr, alias, .. } => {
                let bound = bind_expr(expr, table_schema, catalog, scope)?;
                let name = alias
                    .as_ref()
                    .map_or_else(|| derive_output_name(expr, &bound), |a| a.value.clone());
                out.push((bound, name));
            }
        }
    }
    Ok(out)
}

/// Extract the last identifier of an `ObjectName` as a lowercase string.
pub(super) fn object_name_simple(name: &ObjectName) -> String {
    name.parts
        .last()
        .map_or_else(String::new, |p| p.value.to_ascii_lowercase())
}

/// Derive an output column name from an expression. Bare column
/// references inherit the column's name; everything else falls back to
/// a synthetic `"col{n}"`-style label produced by the caller via
/// [`Self::display`]. The synthetic label is the expression's display
/// form, which keeps EXPLAIN readable without claiming any particular
/// stability.
fn derive_output_name(ast: &Expr, bound: &ScalarExpr) -> String {
    match ast {
        Expr::Column { name } => name
            .parts
            .last()
            .map_or_else(String::new, |p| p.value.clone()),
        _ => bound.to_string(),
    }
}

fn bind_order_by(
    items: &[OrderItem],
    input: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<Vec<SortKey>, PlanError> {
    let mut keys = Vec::with_capacity(items.len());
    for item in items {
        let expr = bind_expr(&item.expr, input, catalog, scope)?;
        let asc = matches!(item.direction, SortDirection::Asc);
        let nulls_first = match item.nulls {
            NullsOrder::First => true,
            NullsOrder::Last => false,
            // PostgreSQL default: NULLS LAST for ASC, NULLS FIRST for DESC.
            NullsOrder::Default => !asc,
        };
        keys.push(SortKey {
            expr,
            asc,
            nulls_first,
        });
    }
    Ok(keys)
}

fn bind_unsigned_literal(expr: &Expr, label: &'static str) -> Result<u64, PlanError> {
    match expr {
        Expr::Literal(Literal::Integer { text, .. }) => text.parse::<u64>().map_err(|_| {
            PlanError::TypeMismatch(format!(
                "{label} must be a non-negative integer, got '{text}'"
            ))
        }),
        Expr::Paren { expr, .. } => bind_unsigned_literal(expr, label),
        _ => Err(PlanError::NotSupported(
            "non-literal LIMIT/OFFSET expressions",
        )),
    }
}

/// Walk a bound logical plan and return `true` if any expression node
/// anywhere in the tree is a [`crate::expr::ScalarExpr::OuterColumn`].
///
/// Used after binding a subquery's inner plan to decide whether to mark
/// the enclosing [`crate::expr::ScalarExpr::ScalarSubquery`],
/// [`crate::expr::ScalarExpr::Exists`], or
/// [`crate::expr::ScalarExpr::InSubquery`] as correlated.
pub(super) fn plan_contains_outer_column(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::Begin { .. }
        | LogicalPlan::Commit { .. }
        | LogicalPlan::Rollback { .. }
        | LogicalPlan::Savepoint { .. }
        | LogicalPlan::RollbackToSavepoint { .. }
        | LogicalPlan::ReleaseSavepoint { .. }
        | LogicalPlan::PrepareTransaction { .. }
        | LogicalPlan::CommitPrepared { .. }
        | LogicalPlan::RollbackPrepared { .. } => false,
        LogicalPlan::Filter { input, predicate } => {
            expr_contains_outer(predicate) || plan_contains_outer_column(input)
        }
        LogicalPlan::Project { input, exprs, .. } => {
            exprs.iter().any(|(e, _)| expr_contains_outer(e)) || plan_contains_outer_column(input)
        }
        LogicalPlan::Sort { input, keys } => {
            keys.iter().any(|k| expr_contains_outer(&k.expr)) || plan_contains_outer_column(input)
        }
        LogicalPlan::Limit { input, .. } => plan_contains_outer_column(input),
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            group_by.iter().any(expr_contains_outer)
                || aggregates
                    .iter()
                    .any(|a| a.arg.as_ref().is_some_and(expr_contains_outer))
                || plan_contains_outer_column(input)
        }
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            plan_contains_outer_column(left) || plan_contains_outer_column(right)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => plan_contains_outer_column(definition) || plan_contains_outer_column(body),
        LogicalPlan::Values { rows, .. } => {
            rows.iter().flat_map(|r| r.iter()).any(expr_contains_outer)
        }
        LogicalPlan::Insert {
            source,
            on_conflict,
            returning,
            ..
        } => {
            plan_contains_outer_column(source)
                || on_conflict.as_ref().is_some_and(|oc| match oc {
                    LogicalOnConflict::DoNothing { .. } => false,
                    LogicalOnConflict::DoUpdate {
                        assignments,
                        r#where,
                        ..
                    } => {
                        assignments.iter().any(|(_, e)| expr_contains_outer(e))
                            || r#where.as_ref().is_some_and(expr_contains_outer)
                    }
                })
                || returning.iter().any(|(e, _)| expr_contains_outer(e))
        }
        LogicalPlan::Update {
            assignments,
            input,
            returning,
            ..
        } => {
            assignments.iter().any(|(_, e)| expr_contains_outer(e))
                || plan_contains_outer_column(input)
                || returning.iter().any(|(e, _)| expr_contains_outer(e))
        }
        LogicalPlan::Delete {
            input, returning, ..
        } => {
            plan_contains_outer_column(input)
                || returning.iter().any(|(e, _)| expr_contains_outer(e))
        }
    }
}

/// Return `true` if a [`ScalarExpr`] contains any
/// [`crate::expr::ScalarExpr::OuterColumn`] node.
fn expr_contains_outer(expr: &crate::expr::ScalarExpr) -> bool {
    expr.contains_outer_column()
}

/// Bind a scalar expression into a typed [`ScalarExpr`].
///
/// - `input` is the schema of the operator whose output this expression
///   is evaluated against (e.g. the FROM schema for a WHERE predicate).
/// - `catalog` is the full catalog, needed to bind table references inside
///   subquery expressions.
/// - `scope` is the outer-scope stack used to resolve correlated column
///   references when `bind_expr` is called while already inside a subquery.
///
/// # Errors
// Expression-binding logic lives in `expr_bind.rs` (split for the
// 600-line per-file ceiling). Callers reach the entry points
// through `self::expr_bind::*`; the submodule's items are
// `pub(super)` so siblings can call them without going through a
// re-export (which would conflict with their visibility).
mod ddl;
mod expr_bind;

use self::ddl::{
    bind_alter_table, bind_create_index, bind_create_table, bind_drop_table, bind_truncate,
};
use self::expr_bind::bind_expr;

#[cfg(test)]
mod tests;
