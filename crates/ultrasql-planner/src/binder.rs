//! Binder — turn a parser AST into a typed logical plan.
//!
//! The binder is a single pass over the AST. For a `SELECT` statement it:
//!
//! 1. Resolves the `FROM` clause to a [`crate::plan::LogicalPlan::Scan`]
//!    over a single relation, looked up in the supplied catalog.
//! 2. Resolves column references in `WHERE` / `SELECT` / `ORDER BY`
//!    against the producing operator's schema; bare column names
//!    become [`crate::expr::ScalarExpr::Column`] nodes with an index.
//! 3. Type-checks expressions, using
//!    [`ultrasql_core::DataType::numeric_join`] for arithmetic and a
//!    simple shape rule for comparisons and boolean operators.
//! 4. Wraps the scan in `Filter` / `Project` / `Sort` / `Limit` in
//!    the canonical SQL evaluation order.
//!
//! For DML statements the binder produces the corresponding plan nodes:
//!
//! - `INSERT` → [`crate::plan::LogicalPlan::Insert`] with a `Values` or
//!   bound-`Select` child for the row source.
//! - `UPDATE` → [`crate::plan::LogicalPlan::Update`] over a `Scan` /
//!   `Filter` child. `UPDATE … FROM other_table` returns
//!   [`crate::error::PlanError::NotSupported`] pending wave-3 join binding.
//! - `DELETE` → [`crate::plan::LogicalPlan::Delete`] over a `Scan` /
//!   `Filter` child. `DELETE … USING other_table` similarly returns
//!   `NotSupported`.
//! - `TRUNCATE` → [`crate::plan::LogicalPlan::Truncate`]; every table
//!   name is validated against the catalog.
//!
//! `EXCLUDED` column references in `ON CONFLICT DO UPDATE` are not
//! supported in v0.2; the binder returns `NotSupported` if any expression
//! in the conflict update assignments resolves to a column reference whose
//! qualifier is the synthetic `excluded` pseudo-table. Callers may
//! work around this by rewriting to unconditional values.
//!
//! The binder does *not* expand `SELECT *`; that is rejected with
//! [`crate::error::PlanError::NotSupported`]. Wildcard expansion is a
//! follow-up that needs alias tracking, which the binder will grow when
//! joins land.

use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_parser::ast::{
    Assignment, BinaryOp, ConflictTarget as AstConflictTarget, DeleteStmt, Distinct, Expr,
    InsertSource, InsertStmt, Literal, NullsOrder, ObjectName, OnConflict, OrderItem, SelectItem,
    SelectStmt, SortDirection, Statement, TableRef, TruncateStmt, UnaryOp, UpdateStmt,
};

use crate::catalog::Catalog;
use crate::error::PlanError;
use crate::expr::ScalarExpr;
use crate::plan::{ConflictTarget, LogicalOnConflict, LogicalPlan, SortKey};

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
    match stmt {
        Statement::Select(s) => bind_select(s, catalog),
        Statement::Insert(s) => bind_insert(s, catalog),
        Statement::Update(s) => bind_update(s, catalog),
        Statement::Delete(s) => bind_delete(s, catalog),
        Statement::Truncate(s) => bind_truncate(s, catalog),
        Statement::Begin { .. } | Statement::Commit { .. } | Statement::Rollback { .. } => Err(
            PlanError::NotSupported("transaction control statements are not planner targets"),
        ),
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
fn bind_insert(s: &InsertStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
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
        InsertSource::Values(rows) => bind_values_rows(rows, expected_arity)?,
        InsertSource::Select(sel) => {
            let plan = bind_select(sel, catalog)?;
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
        .map(|oc| bind_on_conflict(oc, table_schema))
        .transpose()?;

    // 5. Bind RETURNING.
    let returning = bind_returning(&s.returning, table_schema)?;
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
fn bind_values_rows(rows: &[Vec<Expr>], expected_arity: usize) -> Result<LogicalPlan, PlanError> {
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
        let bound_cells: Result<Vec<_>, _> = row.iter().map(|e| bind_expr(e, &empty)).collect();
        bound_rows.push(bound_cells?);
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
            let assignments = bind_assignments(set, table_schema)?;
            let where_expr = r#where
                .as_ref()
                .map(|e| {
                    let pred = bind_expr(e, table_schema)?;
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
        let expr = bind_expr(&a.value, table_schema)?;
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
fn bind_update(s: &UpdateStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
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
        let pred = bind_expr(pred_ast, table_schema)?;
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
    let assignments = bind_assignments(&s.set, table_schema)?;

    // RETURNING.
    let returning = bind_returning(&s.returning, table_schema)?;
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
fn bind_delete(s: &DeleteStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
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
        let pred = bind_expr(pred_ast, table_schema)?;
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
    let returning = bind_returning(&s.returning, table_schema)?;
    let returning_schema = build_returning_schema(&returning)?;

    Ok(LogicalPlan::Delete {
        table: table_name,
        input: Box::new(plan),
        returning,
        schema: returning_schema,
    })
}

// ---------------------------------------------------------------------------
// TRUNCATE
// ---------------------------------------------------------------------------

/// Bind a `TRUNCATE` statement.
///
/// Validates every table name against the catalog; returns
/// [`PlanError::TableNotFound`] on the first missing name.
fn bind_truncate(s: &TruncateStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    let mut table_names: Vec<String> = Vec::with_capacity(s.tables.len());
    for obj in &s.tables {
        let name = object_name_simple(obj);
        catalog
            .lookup_table(&name)
            .ok_or_else(|| PlanError::TableNotFound(name.clone()))?;
        table_names.push(name);
    }
    Ok(LogicalPlan::Truncate {
        tables: table_names,
        restart_identity: s.restart_identity,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}

// ---------------------------------------------------------------------------
// SELECT (existing logic, unchanged)
// ---------------------------------------------------------------------------

fn bind_select(select: &SelectStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    if !matches!(select.distinct, Distinct::All) {
        return Err(PlanError::NotSupported("SELECT DISTINCT"));
    }

    // FROM clause. We currently support a single named relation.
    let mut plan = match select.from.as_slice() {
        [TableRef::Named { name, .. }] => {
            let table_name = name
                .parts
                .last()
                .map_or_else(String::new, |p| p.value.clone());
            let meta = catalog
                .lookup_table(&table_name)
                .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
            LogicalPlan::Scan {
                schema: meta.schema,
                table: table_name,
                projection: None,
            }
        }
        [_] => return Err(PlanError::NotSupported("FROM clause variant")),
        [] => LogicalPlan::Empty {
            schema: Schema::empty(),
        },
        _ => return Err(PlanError::NotSupported("multiple FROM items")),
    };

    // WHERE.
    if let Some(pred_ast) = &select.r#where {
        let pred = bind_expr(pred_ast, plan.schema())?;
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

    // SELECT list (must come logically after WHERE for column scope, but
    // before ORDER BY's projection-aware lookup; we resolve ORDER BY
    // against the scan schema below since we do not yet expose
    // projection aliases to ORDER BY).
    let projected = bind_projection(&select.projection, plan.schema())?;
    let proj_fields: Vec<Field> = projected
        .iter()
        .map(|(e, name)| {
            // Projection outputs are nullable in the general case (the
            // expression may produce NULL even from a NOT NULL column,
            // e.g. division). Conservative default.
            Field::nullable(name, e.data_type())
        })
        .collect();
    let proj_schema = Schema::new(proj_fields)
        .map_err(|e| PlanError::TypeMismatch(format!("projection: {e}")))?;

    // ORDER BY — resolved against the *input* (post-filter, pre-project)
    // schema. PostgreSQL allows references to projection aliases too;
    // this binder will grow that in a follow-up.
    let sort_keys = bind_order_by(&select.order_by, plan.schema())?;
    if !sort_keys.is_empty() {
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            keys: sort_keys,
        };
    }

    // Apply the projection.
    plan = LogicalPlan::Project {
        input: Box::new(plan),
        exprs: projected,
        schema: proj_schema,
    };

    // LIMIT / OFFSET.
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

/// Bind a `RETURNING` projection list against `scope`.
fn bind_returning(
    items: &[SelectItem],
    scope: &Schema,
) -> Result<Vec<(ScalarExpr, String)>, PlanError> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                return Err(PlanError::NotSupported("wildcard in RETURNING clause"));
            }
            SelectItem::Expr { expr, alias, .. } => {
                let bound = bind_expr(expr, scope)?;
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
fn object_name_simple(name: &ObjectName) -> String {
    name.parts
        .last()
        .map_or_else(String::new, |p| p.value.to_ascii_lowercase())
}

fn bind_projection(
    items: &[SelectItem],
    input: &Schema,
) -> Result<Vec<(ScalarExpr, String)>, PlanError> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                return Err(PlanError::NotSupported(
                    "wildcard projection (will land with join binding)",
                ));
            }
            SelectItem::Expr { expr, alias, .. } => {
                let bound = bind_expr(expr, input)?;
                let name = alias
                    .as_ref()
                    .map_or_else(|| derive_output_name(expr, &bound), |a| a.value.clone());
                out.push((bound, name));
            }
        }
    }
    Ok(out)
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

fn bind_order_by(items: &[OrderItem], input: &Schema) -> Result<Vec<SortKey>, PlanError> {
    let mut keys = Vec::with_capacity(items.len());
    for item in items {
        let expr = bind_expr(&item.expr, input)?;
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

fn bind_expr(expr: &Expr, input: &Schema) -> Result<ScalarExpr, PlanError> {
    match expr {
        Expr::Literal(lit) => Ok(bind_literal(lit)),
        Expr::Column { name } => bind_column(name, input),
        Expr::Parameter { index, .. } => Ok(ScalarExpr::Parameter {
            index: *index,
            data_type: DataType::Null,
        }),
        Expr::Paren { expr, .. } => bind_expr(expr, input),
        Expr::Unary {
            op, expr: inner, ..
        } => bind_unary(*op, inner, input),
        Expr::Binary {
            op, left, right, ..
        } => bind_binary(*op, left, right, input),
        Expr::IsNull { expr, negated, .. } => Ok(ScalarExpr::IsNull {
            expr: Box::new(bind_expr(expr, input)?),
            negated: *negated,
        }),
        Expr::Call { .. } => Err(PlanError::NotSupported("function calls")),
        Expr::Cast { .. } => Err(PlanError::NotSupported("CAST expressions")),
        _ => Err(PlanError::NotSupported("expression variant")),
    }
}

fn bind_literal(lit: &Literal) -> ScalarExpr {
    match lit {
        Literal::Bool { value, .. } => ScalarExpr::Literal {
            value: Value::Bool(*value),
            data_type: DataType::Bool,
        },
        Literal::Integer { text, .. } => {
            // Pick the narrowest integer width that fits, matching the
            // PostgreSQL convention.
            let (value, data_type) = parse_integer_literal(text);
            ScalarExpr::Literal { value, data_type }
        }
        Literal::Float { text, .. } => {
            // Float literals default to `double precision`. A future
            // pass can recognise an `f` suffix and pick `Float32`.
            let parsed = text.parse::<f64>().unwrap_or(f64::NAN);
            ScalarExpr::Literal {
                value: Value::Float64(parsed),
                data_type: DataType::Float64,
            }
        }
        Literal::String { value, .. } => ScalarExpr::Literal {
            value: Value::Text(value.clone()),
            data_type: DataType::Text { max_len: None },
        },
        // `Literal::Null` and any future non-exhaustive variant both
        // bind to a NULL placeholder; later passes specialize.
        _ => ScalarExpr::Literal {
            value: Value::Null,
            data_type: DataType::Null,
        },
    }
}

/// Pick the narrowest signed integer type that fits a decimal literal.
fn parse_integer_literal(text: &str) -> (Value, DataType) {
    if let Ok(v) = text.parse::<i32>() {
        return (Value::Int32(v), DataType::Int32);
    }
    if let Ok(v) = text.parse::<i64>() {
        return (Value::Int64(v), DataType::Int64);
    }
    // Out of i64 range — fall back to a Decimal placeholder; this
    // matches what `numeric_join` already promotes integer literals to
    // when paired with a Decimal column. We do not yet have a Decimal
    // Value variant, so park it as `Int64::MAX`. A future pass with
    // a Decimal datum will replace this branch.
    (
        Value::Int64(i64::MAX),
        DataType::Decimal {
            precision: None,
            scale: None,
        },
    )
}

fn bind_column(
    name: &ultrasql_parser::ast::ObjectName,
    input: &Schema,
) -> Result<ScalarExpr, PlanError> {
    let col_name = name
        .parts
        .last()
        .map_or_else(String::new, |p| p.value.clone());
    // We do not yet have multi-relation scopes, so we ignore any
    // qualifier and resolve unambiguously by column name in the input
    // schema.
    let mut hits = input
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name.eq_ignore_ascii_case(&col_name));
    let Some((index, field)) = hits.next() else {
        return Err(PlanError::ColumnNotFound(col_name));
    };
    if hits.next().is_some() {
        return Err(PlanError::Ambiguous(col_name));
    }
    Ok(ScalarExpr::Column {
        name: field.name.clone(),
        index,
        data_type: field.data_type.clone(),
    })
}

fn bind_unary(op: UnaryOp, inner: &Expr, input: &Schema) -> Result<ScalarExpr, PlanError> {
    let bound = bind_expr(inner, input)?;
    let inner_ty = bound.data_type();
    let data_type = match op {
        UnaryOp::Neg | UnaryOp::Pos => {
            if inner_ty.is_numeric() {
                inner_ty
            } else if matches!(inner_ty, DataType::Null) {
                DataType::Null
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "unary {} on non-numeric type {inner_ty}",
                    display_unary(op)
                )));
            }
        }
        UnaryOp::Not => {
            if matches!(inner_ty, DataType::Bool | DataType::Null) {
                DataType::Bool
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "NOT on non-boolean type {inner_ty}"
                )));
            }
        }
    };
    Ok(ScalarExpr::Unary {
        op,
        expr: Box::new(bound),
        data_type,
    })
}

fn bind_binary(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    input: &Schema,
) -> Result<ScalarExpr, PlanError> {
    let l = bind_expr(left, input)?;
    let r = bind_expr(right, input)?;
    let lt = l.data_type();
    let rt = r.data_type();
    let data_type = match op {
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::Mod
        | BinaryOp::Pow => {
            if matches!(lt, DataType::Null) {
                rt
            } else if matches!(rt, DataType::Null) {
                lt
            } else {
                lt.numeric_join(&rt).map_err(|_| {
                    PlanError::TypeMismatch(format!(
                        "arithmetic operator {} on incompatible types {lt} and {rt}",
                        display_binary(op)
                    ))
                })?
            }
        }
        BinaryOp::Concat => {
            if (lt.is_textlike() || matches!(lt, DataType::Null))
                && (rt.is_textlike() || matches!(rt, DataType::Null))
            {
                DataType::Text { max_len: None }
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "string concatenation requires text operands, got {lt} and {rt}"
                )));
            }
        }
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => {
            if comparable(&lt, &rt) {
                DataType::Bool
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "cannot compare {lt} and {rt}"
                )));
            }
        }
        BinaryOp::And | BinaryOp::Or => {
            if matches!(lt, DataType::Bool | DataType::Null)
                && matches!(rt, DataType::Bool | DataType::Null)
            {
                DataType::Bool
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "{} requires boolean operands, got {lt} and {rt}",
                    display_binary(op)
                )));
            }
        }
        BinaryOp::Like | BinaryOp::NotLike | BinaryOp::Ilike | BinaryOp::NotIlike => {
            if (lt.is_textlike() || matches!(lt, DataType::Null))
                && (rt.is_textlike() || matches!(rt, DataType::Null))
            {
                DataType::Bool
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "{} requires text operands, got {lt} and {rt}",
                    display_binary(op)
                )));
            }
        }
    };
    Ok(ScalarExpr::Binary {
        op,
        left: Box::new(l),
        right: Box::new(r),
        data_type,
    })
}

fn comparable(a: &DataType, b: &DataType) -> bool {
    if matches!(a, DataType::Null) || matches!(b, DataType::Null) {
        return true;
    }
    if a == b {
        return true;
    }
    if a.is_numeric() && b.is_numeric() {
        return true;
    }
    if a.is_textlike() && b.is_textlike() {
        return true;
    }
    if a.is_temporal() && b.is_temporal() {
        return true;
    }
    false
}

const fn display_unary(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Neg => "-",
        UnaryOp::Pos => "+",
        UnaryOp::Not => "NOT",
    }
}

const fn display_binary(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::Pow => "^",
        BinaryOp::Concat => "||",
        BinaryOp::Eq => "=",
        BinaryOp::NotEq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::LtEq => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::GtEq => ">=",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::Like => "LIKE",
        BinaryOp::NotLike => "NOT LIKE",
        BinaryOp::Ilike => "ILIKE",
        BinaryOp::NotIlike => "NOT ILIKE",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_parser::Parser;

    use super::*;
    use crate::catalog::{InMemoryCatalog, TableMeta};

    /// Catalog with a single `users` table: id INT, name TEXT, score FLOAT8.
    fn users_catalog() -> InMemoryCatalog {
        let schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema ok");
        let mut cat = InMemoryCatalog::new();
        cat.register("users", TableMeta::new(schema));
        cat
    }

    fn parse_and_bind(sql: &str, cat: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
        let stmt = Parser::new(sql)
            .parse_statement()
            .expect("test SQL parses cleanly");
        bind(&stmt, cat)
    }

    fn parse_bind_ok(sql: &str) -> LogicalPlan {
        let cat = users_catalog();
        parse_and_bind(sql, &cat).expect("bind ok")
    }

    // -----------------------------------------------------------------------
    // INSERT — happy paths
    // -----------------------------------------------------------------------

    #[test]
    fn binds_insert_with_column_list_resolves_indices() {
        let plan = parse_bind_ok("INSERT INTO users (name, score) VALUES ('alice', 1.0)");
        let LogicalPlan::Insert {
            table,
            columns,
            source,
            ..
        } = &plan
        else {
            panic!("expected Insert, got {plan:?}");
        };
        assert_eq!(table, "users");
        // name is index 1, score is index 2
        assert_eq!(columns, &[1_usize, 2_usize]);
        assert!(matches!(source.as_ref(), LogicalPlan::Values { .. }));
    }

    #[test]
    fn binds_insert_default_values() {
        let plan = parse_bind_ok("INSERT INTO users DEFAULT VALUES");
        let LogicalPlan::Insert {
            source, columns, ..
        } = &plan
        else {
            panic!("expected Insert");
        };
        // Columns = all three (all-columns expansion)
        assert_eq!(columns.len(), 3);
        // Source is a Values with one zero-width row.
        let LogicalPlan::Values { rows, .. } = source.as_ref() else {
            panic!("expected Values source");
        };
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_empty());
    }

    #[test]
    fn binds_insert_with_multi_row_values() {
        let plan = parse_bind_ok(
            "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')",
        );
        let LogicalPlan::Insert { source, .. } = &plan else {
            panic!("expected Insert");
        };
        let LogicalPlan::Values { rows, .. } = source.as_ref() else {
            panic!("expected Values");
        };
        assert_eq!(rows.len(), 3);
        for r in rows {
            assert_eq!(r.len(), 2);
        }
    }

    #[test]
    fn binds_insert_select() {
        // Must use a single-column select (id only) to match column count 1.
        let plan = parse_bind_ok("INSERT INTO users (id) SELECT id FROM users WHERE id > 0");
        let LogicalPlan::Insert {
            columns, source, ..
        } = &plan
        else {
            panic!("expected Insert");
        };
        assert_eq!(columns, &[0_usize]);
        // Source is a bound Select plan.
        assert!(
            matches!(
                source.as_ref(),
                LogicalPlan::Limit { .. }
                    | LogicalPlan::Sort { .. }
                    | LogicalPlan::Project { .. }
                    | LogicalPlan::Filter { .. }
                    | LogicalPlan::Scan { .. }
            ),
            "unexpected source: {source:?}"
        );
    }

    // -----------------------------------------------------------------------
    // INSERT — error paths
    // -----------------------------------------------------------------------

    #[test]
    fn binds_insert_rejects_ragged_value_rows() {
        let cat = users_catalog();
        let err = parse_and_bind(
            "INSERT INTO users (id, name) VALUES (1, 'alice', 99.0)",
            &cat,
        )
        .unwrap_err();
        // Row 1 has 3 cells but 2 columns expected.
        assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    }

    #[test]
    fn binds_insert_rejects_unknown_column() {
        let cat = users_catalog();
        let err = parse_and_bind("INSERT INTO users (bogus) VALUES (1)", &cat).unwrap_err();
        assert!(
            matches!(err, PlanError::ColumnNotFound(ref c) if c == "bogus"),
            "got {err:?}"
        );
    }

    #[test]
    fn binds_insert_rejects_arity_mismatch_with_select_source() {
        // Column list has 2 entries, SELECT returns 3 columns.
        let cat = users_catalog();
        let err = parse_and_bind(
            "INSERT INTO users (id, name) SELECT id, name, score FROM users",
            &cat,
        )
        .unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    }

    // -----------------------------------------------------------------------
    // INSERT — ON CONFLICT
    // -----------------------------------------------------------------------

    #[test]
    fn binds_on_conflict_do_nothing() {
        let plan = parse_bind_ok("INSERT INTO users (id) VALUES (1) ON CONFLICT DO NOTHING");
        let LogicalPlan::Insert { on_conflict, .. } = &plan else {
            panic!("expected Insert");
        };
        assert!(matches!(
            on_conflict,
            Some(LogicalOnConflict::DoNothing { target: None })
        ));
    }

    #[test]
    fn binds_on_conflict_do_update_targets() {
        let plan = parse_bind_ok(
            "INSERT INTO users (id, name) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET name = 'y'",
        );
        let LogicalPlan::Insert { on_conflict, .. } = &plan else {
            panic!("expected Insert");
        };
        let Some(LogicalOnConflict::DoUpdate {
            target,
            assignments,
            ..
        }) = on_conflict
        else {
            panic!("expected DoUpdate, got {on_conflict:?}");
        };
        // Conflict target: column 'id' is at index 0
        assert_eq!(target.columns, vec![0_usize]);
        // Assignment: name (index 1) = literal 'y'
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].0, 1);
    }

    // -----------------------------------------------------------------------
    // UPDATE
    // -----------------------------------------------------------------------

    #[test]
    fn binds_update_with_filter_and_assignments() {
        let plan = parse_bind_ok("UPDATE users SET score = 9.5 WHERE id = 1");
        let LogicalPlan::Update {
            table,
            assignments,
            input,
            ..
        } = &plan
        else {
            panic!("expected Update, got {plan:?}");
        };
        assert_eq!(table, "users");
        // score is column index 2
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].0, 2);
        assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));
    }

    #[test]
    fn binds_update_rejects_unknown_target_column() {
        let cat = users_catalog();
        let err = parse_and_bind("UPDATE users SET bogus = 1", &cat).unwrap_err();
        assert!(
            matches!(err, PlanError::ColumnNotFound(ref c) if c == "bogus"),
            "got {err:?}"
        );
    }

    #[test]
    fn binds_update_rejects_duplicate_target_column() {
        let cat = users_catalog();
        // PostgreSQL rejects `UPDATE t SET col=1, col=2` — mirror that.
        let err = parse_and_bind("UPDATE users SET score = 1.0, score = 2.0", &cat).unwrap_err();
        assert!(
            matches!(err, PlanError::DuplicateColumn(ref c) if c == "score"),
            "expected DuplicateColumn(score), got {err:?}"
        );
    }

    #[test]
    fn binder_rejects_update_from_other_table_as_not_supported() {
        let cat = users_catalog();
        let err = parse_and_bind(
            "UPDATE users SET score = 1 FROM users AS u2 WHERE users.id = u2.id",
            &cat,
        )
        .unwrap_err();
        assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
    }

    // -----------------------------------------------------------------------
    // DELETE
    // -----------------------------------------------------------------------

    #[test]
    fn binds_delete_emits_scan_filter_delete() {
        let plan = parse_bind_ok("DELETE FROM users WHERE id = 42");
        let LogicalPlan::Delete { table, input, .. } = &plan else {
            panic!("expected Delete, got {plan:?}");
        };
        assert_eq!(table, "users");
        assert!(matches!(input.as_ref(), LogicalPlan::Filter { .. }));
    }

    #[test]
    fn binder_rejects_delete_using_other_table_as_not_supported() {
        let cat = users_catalog();
        let err = parse_and_bind(
            "DELETE FROM users USING users AS u2 WHERE users.id = u2.id",
            &cat,
        )
        .unwrap_err();
        assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
    }

    // -----------------------------------------------------------------------
    // TRUNCATE
    // -----------------------------------------------------------------------

    #[test]
    fn binds_truncate_validates_table_existence() {
        let plan = parse_bind_ok("TRUNCATE TABLE users");
        let LogicalPlan::Truncate {
            tables,
            restart_identity,
            cascade,
            ..
        } = &plan
        else {
            panic!("expected Truncate, got {plan:?}");
        };
        assert_eq!(tables, &["users"]);
        assert!(!restart_identity);
        assert!(!cascade);
        assert!(plan.schema().is_empty());

        // Unknown table should fail.
        let cat = users_catalog();
        let err = parse_and_bind("TRUNCATE TABLE nope", &cat).unwrap_err();
        assert!(
            matches!(err, PlanError::TableNotFound(ref t) if t == "nope"),
            "got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Property test
    // -----------------------------------------------------------------------

    proptest! {
        /// For any arity in 1..=6 and 1..=4 matching VALUES rows, the bound
        /// INSERT plan has a Values source with the same arity.
        #[test]
        fn prop_insert_values_arity_preserved(
            arity in 1_usize..=6_usize,
            nrows in 1_usize..=4_usize,
        ) {
            // Build a catalog with a table that has `arity` INT columns.
            let fields: Vec<Field> = (0..arity)
                .map(|i| Field::nullable(format!("c{i}"), DataType::Int32))
                .collect();
            let schema = Schema::new(fields).expect("schema ok");
            let mut cat = InMemoryCatalog::new();
            cat.register("t", TableMeta::new(schema));

            // Build SQL: INSERT INTO t (c0, c1, …) VALUES (0, 0, …), …
            let cols: Vec<String> = (0..arity).map(|i| format!("c{i}")).collect();
            let one_row = vec!["0"; arity].join(", ");
            let values_clause = std::iter::repeat_n(format!("({one_row})"), nrows)
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT INTO t ({}) VALUES {}",
                cols.join(", "),
                values_clause
            );

            let plan = parse_and_bind(&sql, &cat).expect("bind ok");
            let LogicalPlan::Insert { columns, source, .. } = &plan else {
                panic!("expected Insert");
            };
            prop_assert_eq!(columns.len(), arity);
            let LogicalPlan::Values { rows, .. } = source.as_ref() else {
                panic!("expected Values source");
            };
            prop_assert_eq!(rows.len(), nrows);
            for r in rows {
                prop_assert_eq!(r.len(), arity);
            }
        }
    }
}
