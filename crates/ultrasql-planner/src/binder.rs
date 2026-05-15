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
// CREATE TABLE
// ---------------------------------------------------------------------------

/// Bind a `CREATE TABLE` statement.
///
/// The v0.5 binder accepts:
///
/// - one- or two-part relation names (`t`, `public.t`),
/// - `IF NOT EXISTS`,
/// - column types: `INT/INTEGER/INT4`, `BIGINT/INT8`, `SMALLINT/INT2`,
///   `REAL/FLOAT4`, `DOUBLE PRECISION/FLOAT8/FLOAT`, `BOOLEAN/BOOL`,
///   `TEXT`, `VARCHAR(n)`, `CHAR(n)`, `BYTEA`,
/// - column constraints: `NULL`, `NOT NULL`, `PRIMARY KEY` (implies
///   NOT NULL).
///
/// Everything else (DEFAULT, UNIQUE, CHECK, REFERENCES, table-level
/// constraints, array column types, unsupported type names) returns
/// [`PlanError::NotSupported`]. A duplicate column name returns
/// [`PlanError::DuplicateColumn`]; a relation that already exists when
/// `IF NOT EXISTS` is absent returns [`PlanError::DuplicateTable`].
fn bind_create_table(s: &CreateTableStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    if !s.table_constraints.is_empty() {
        return Err(PlanError::NotSupported(
            "CREATE TABLE: table-level constraints",
        ));
    }
    let table_name = object_name_simple(&s.name);
    let namespace = object_name_namespace(&s.name);
    if !s.if_not_exists && catalog.lookup_table(&table_name).is_some() {
        return Err(PlanError::DuplicateTable(table_name));
    }
    if s.columns.is_empty() {
        return Err(PlanError::NotSupported("CREATE TABLE: zero columns"));
    }
    let mut fields: Vec<Field> = Vec::with_capacity(s.columns.len());
    for col in &s.columns {
        let name = col.name.value.clone();
        let folded = name.to_ascii_lowercase();
        if fields.iter().any(|f| f.name.to_ascii_lowercase() == folded) {
            return Err(PlanError::DuplicateColumn(name));
        }
        let dtype = resolve_type_name(&col.data_type)?;
        let nullable = resolve_column_nullability(&col.constraints)?;
        let field = if nullable {
            Field::nullable(name, dtype)
        } else {
            Field::required(name, dtype)
        };
        fields.push(field);
    }
    let columns =
        Schema::new(fields).expect("column dedup precheck guarantees Schema::new cannot fail");
    Ok(LogicalPlan::CreateTable {
        table_name,
        namespace,
        columns,
        if_not_exists: s.if_not_exists,
        schema: Schema::empty(),
    })
}

/// Pull the namespace component out of a possibly-qualified relation
/// name. `t` → `"public"`; `s.t` → `"s"`; `c.s.t` → `"s"`.
fn object_name_namespace(name: &ObjectName) -> String {
    if name.parts.len() >= 2 {
        let idx = name.parts.len() - 2;
        name.parts[idx].value.to_ascii_lowercase()
    } else {
        String::from("public")
    }
}

/// Resolve a parser [`TypeName`] to an UltraSQL [`DataType`].
///
/// The v0.5 type surface is intentionally narrow; types outside the
/// listed set return [`PlanError::NotSupported`]. Length modifiers
/// (e.g. `VARCHAR(255)`) are honored where the target [`DataType`]
/// carries a `max_len` slot.
fn resolve_type_name(t: &TypeName) -> Result<DataType, PlanError> {
    if t.is_array {
        return Err(PlanError::NotSupported("CREATE TABLE: ARRAY column types"));
    }
    let max_len_modifier = || t.type_modifiers.first().copied();
    match t.name.value.as_str() {
        "int" | "integer" | "int4" => Ok(DataType::Int32),
        "bigint" | "int8" => Ok(DataType::Int64),
        "smallint" | "int2" => Ok(DataType::Int16),
        "bool" | "boolean" => Ok(DataType::Bool),
        "real" | "float4" => Ok(DataType::Float32),
        "double" | "double precision" | "float" | "float8" => Ok(DataType::Float64),
        "text" => Ok(DataType::Text { max_len: None }),
        "varchar" | "character varying" | "char" | "character" | "bpchar" => Ok(DataType::Text {
            max_len: max_len_modifier(),
        }),
        "bytea" => Ok(DataType::Bytea),
        _ => Err(PlanError::NotSupported(
            "CREATE TABLE: column type not implemented in v0.5",
        )),
    }
}

/// Determine whether a column is nullable from its constraint list.
///
/// Returns `true` (nullable) when no `NOT NULL` or `PRIMARY KEY`
/// constraint is present. `PRIMARY KEY` implies `NOT NULL`. Other
/// constraint kinds (DEFAULT, UNIQUE, CHECK, REFERENCES) return
/// [`PlanError::NotSupported`].
fn resolve_column_nullability(constraints: &[ColumnConstraint]) -> Result<bool, PlanError> {
    let mut nullable = true;
    for c in constraints {
        match c {
            ColumnConstraint::NotNull { .. } | ColumnConstraint::PrimaryKey { .. } => {
                nullable = false;
            }
            ColumnConstraint::Null { .. } => nullable = true,
            ColumnConstraint::Default { .. }
            | ColumnConstraint::Unique { .. }
            | ColumnConstraint::Check { .. }
            | ColumnConstraint::References { .. } => {
                return Err(PlanError::NotSupported(
                    "CREATE TABLE: only NULL / NOT NULL / PRIMARY KEY column constraints in v0.5",
                ));
            }
        }
    }
    Ok(nullable)
}

// ---------------------------------------------------------------------------
// CREATE INDEX
// ---------------------------------------------------------------------------

/// Bind a `CREATE [UNIQUE] INDEX [IF NOT EXISTS] [name] ON table (cols)`.
///
/// Accepted shapes for this wave:
///
/// - bare column-reference keys only (`(col1, col2, ...)`); expression
///   keys and `USING method` other than `btree` (or no method, which
///   defaults to btree) return [`PlanError::NotSupported`].
/// - no `INCLUDE` covering list, no partial-index `WHERE` predicate,
///   no per-key direction / nulls ordering (sort options on the index
///   key are parsed but not actionable until [`crate::plan::LogicalPlan`]
///   carries them through).
///
/// The binder synthesises a default index name `"{table}_{c1}_{c2}_..._idx"`
/// when one was not supplied so the executor always has a stable
/// catalog key to write.
fn bind_create_index(s: &CreateIndexStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    // Resolve the target table.
    let table_name = object_name_simple(&s.table);
    let meta = catalog
        .lookup_table(&table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
    let table_schema = &meta.schema;

    if s.r#where.is_some() {
        return Err(PlanError::NotSupported(
            "CREATE INDEX: WHERE (partial index)",
        ));
    }
    if !s.include.is_empty() {
        return Err(PlanError::NotSupported(
            "CREATE INDEX: INCLUDE (covering columns)",
        ));
    }
    if let Some(method) = &s.method {
        if !method.value.eq_ignore_ascii_case("btree") {
            return Err(PlanError::NotSupported(
                "CREATE INDEX: only btree method is supported",
            ));
        }
    }

    if s.columns.is_empty() {
        return Err(PlanError::NotSupported("CREATE INDEX: zero key columns"));
    }
    let mut col_indices: Vec<usize> = Vec::with_capacity(s.columns.len());
    let mut col_names: Vec<String> = Vec::with_capacity(s.columns.len());
    for key in &s.columns {
        let col_name = match &key.expr {
            Expr::Column { name } if name.parts.len() == 1 => name.parts[0].value.clone(),
            _ => {
                return Err(PlanError::NotSupported(
                    "CREATE INDEX: only bare column-reference keys are supported",
                ));
            }
        };
        let folded = col_name.to_ascii_lowercase();
        let (idx, _) = table_schema
            .find(&folded)
            .ok_or_else(|| PlanError::ColumnNotFound(col_name.clone()))?;
        col_indices.push(idx);
        col_names.push(folded);
    }

    let index_name = s.name.as_ref().map_or_else(
        || synthesise_index_name(&table_name, &col_names),
        |ident| ident.value.to_ascii_lowercase(),
    );

    Ok(LogicalPlan::CreateIndex {
        index_name,
        table_name,
        columns: col_indices,
        unique: s.unique,
        if_not_exists: s.if_not_exists,
        schema: Schema::empty(),
    })
}

/// Build a stable default index name when the user did not supply one:
/// `{table}_{col1}_{col2}_..._idx`. Matches PostgreSQL's
/// `ChooseIndexName` for the common single-column / multi-column case
/// closely enough that EXPLAIN-style output stays familiar.
fn synthesise_index_name(table: &str, columns: &[String]) -> String {
    let mut s = String::with_capacity(table.len() + 16);
    s.push_str(table);
    for c in columns {
        s.push('_');
        s.push_str(c);
    }
    s.push_str("_idx");
    s
}

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

/// Bind a `DROP TABLE [IF EXISTS] name [, ...] [CASCADE|RESTRICT]`.
///
/// Each name is folded to lowercase and resolved against the catalog.
/// Without `IF EXISTS`, a missing relation is rejected with
/// [`PlanError::TableNotFound`]; with `IF EXISTS`, missing relations
/// are silently dropped from the resulting plan so the executor never
/// has to re-check the catalog.
fn bind_drop_table(s: &DropTableStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    let mut tables: Vec<String> = Vec::with_capacity(s.names.len());
    for obj in &s.names {
        let name = object_name_simple(obj);
        if catalog.lookup_table(&name).is_some() {
            tables.push(name);
        } else if !s.if_exists {
            return Err(PlanError::TableNotFound(name));
        }
    }
    Ok(LogicalPlan::DropTable {
        tables,
        if_exists: s.if_exists,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}

// ---------------------------------------------------------------------------
// ALTER TABLE
// ---------------------------------------------------------------------------

/// Bind an `ALTER TABLE name ADD [COLUMN] col type` statement.
///
/// This wave supports only `ADD COLUMN`. Every other parser-level
/// action (`DROP COLUMN`, `RENAME COLUMN`, `RENAME TO`,
/// `ADD CONSTRAINT`, `DROP CONSTRAINT`) is rejected with
/// [`PlanError::NotSupported`] so the dispatcher contract stays
/// honest; subsequent waves can add arms as the executor grows the
/// matching kernel.
///
/// For `ADD COLUMN` the binder resolves the column's data type and
/// nullability against the same v0.5 column-constraint matrix used by
/// `CREATE TABLE` and rejects duplicate column names up front
/// ([`PlanError::DuplicateColumn`]).
fn bind_alter_table(s: &AlterTableStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    let table_name = object_name_simple(&s.name);
    let meta = catalog
        .lookup_table(&table_name)
        .ok_or_else(|| PlanError::TableNotFound(table_name.clone()))?;
    let table_schema = &meta.schema;

    let action = match &s.action {
        AlterTableAction::AddColumn { column, .. } => {
            let new_name = column.name.value.clone();
            if table_schema.find(&new_name.to_ascii_lowercase()).is_some() {
                return Err(PlanError::DuplicateColumn(new_name));
            }
            let dtype = resolve_type_name(&column.data_type)?;
            let nullable = resolve_column_nullability(&column.constraints)?;
            let field = if nullable {
                Field::nullable(new_name, dtype)
            } else {
                Field::required(new_name, dtype)
            };
            LogicalAlterTableAction::AddColumn { column: field }
        }
        AlterTableAction::DropColumn { .. } => {
            return Err(PlanError::NotSupported(
                "ALTER TABLE: DROP COLUMN not yet supported",
            ));
        }
        AlterTableAction::RenameColumn { .. } => {
            return Err(PlanError::NotSupported(
                "ALTER TABLE: RENAME COLUMN not yet supported",
            ));
        }
        AlterTableAction::RenameTable { .. } => {
            return Err(PlanError::NotSupported(
                "ALTER TABLE: RENAME TO not yet supported",
            ));
        }
        AlterTableAction::AddConstraint { .. } => {
            return Err(PlanError::NotSupported(
                "ALTER TABLE: ADD CONSTRAINT not yet supported",
            ));
        }
        AlterTableAction::DropConstraint { .. } => {
            return Err(PlanError::NotSupported(
                "ALTER TABLE: DROP CONSTRAINT not yet supported",
            ));
        }
    };

    Ok(LogicalPlan::AlterTable {
        table_name,
        action,
        schema: Schema::empty(),
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
fn bind_select_with_ctes(
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
fn is_aggregate_name(name: &str) -> bool {
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
fn derive_agg_output_name(func_name: &str, _args: &[Expr]) -> String {
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
fn object_name_simple(name: &ObjectName) -> String {
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
fn plan_contains_outer_column(plan: &LogicalPlan) -> bool {
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
///
/// Returns [`PlanError`] on type mismatches, unknown identifiers, or
/// unsupported expression forms.
#[allow(clippy::too_many_lines)]
fn bind_expr(
    expr: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    match expr {
        Expr::Literal(lit) => Ok(bind_literal(lit)),
        Expr::Column { name } => bind_column(name, input, scope),
        Expr::Parameter { index, .. } => Ok(ScalarExpr::Parameter {
            index: *index,
            data_type: DataType::Null,
        }),
        Expr::Paren { expr, .. } => bind_expr(expr, input, catalog, scope),
        Expr::Unary {
            op, expr: inner, ..
        } => bind_unary(*op, inner, input, catalog, scope),
        Expr::Binary {
            op, left, right, ..
        } => bind_binary(*op, left, right, input, catalog, scope),
        Expr::IsNull { expr, negated, .. } => Ok(ScalarExpr::IsNull {
            expr: Box::new(bind_expr(expr, input, catalog, scope)?),
            negated: *negated,
        }),
        Expr::Call { name, args, .. } => {
            // If this is a known aggregate and we have an aggregate output schema,
            // try to resolve it as a column reference into that schema.
            let func_name = name
                .parts
                .last()
                .map_or("", |p| p.value.as_str())
                .to_ascii_lowercase();
            if is_aggregate_name(&func_name) {
                let agg_col_name = derive_agg_output_name(&func_name, args);
                if let Some((i, f)) = input.find(&agg_col_name) {
                    return Ok(ScalarExpr::Column {
                        name: f.name.clone(),
                        index: i,
                        data_type: f.data_type.clone(),
                    });
                }
                // If not found by derived name, try to find any column matching
                // the function name prefix (e.g. "count" matches "count").
                if let Some((i, f)) = input.find(&func_name) {
                    return Ok(ScalarExpr::Column {
                        name: f.name.clone(),
                        index: i,
                        data_type: f.data_type.clone(),
                    });
                }
                Err(PlanError::NotSupported(
                    "aggregate call outside aggregate context",
                ))
            } else {
                Err(PlanError::NotSupported("non-aggregate function calls"))
            }
        }
        Expr::Cast { .. } => Err(PlanError::NotSupported("CAST expressions")),

        // ------------------------------------------------------------------
        // Subquery variants
        // ------------------------------------------------------------------

        // Scalar subquery: `(SELECT col FROM …)`.
        //
        // The inner plan must project exactly one column; otherwise the
        // binder returns [`PlanError::TypeMismatch`].
        //
        // Push `input` as an outer scope frame so that correlated column
        // references inside the inner SELECT resolve to the outer query's
        // columns at `frame_depth = 1`.
        Expr::Subquery {
            select: inner_select,
            ..
        } => {
            scope.push(ScopeFrame {
                schema: input.clone(),
                qualifier: None,
            });
            let inner_result = bind_select_with_ctes(inner_select, catalog, &[], scope);
            scope.pop();
            let inner_plan = inner_result?;
            if inner_plan.schema().len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "scalar subquery must return exactly 1 column, got {}",
                    inner_plan.schema().len()
                )));
            }
            let data_type = inner_plan.schema().field_at(0).data_type.clone();
            let correlated = plan_contains_outer_column(&inner_plan);
            Ok(ScalarExpr::ScalarSubquery {
                subplan: Box::new(inner_plan),
                correlated,
                data_type,
            })
        }

        // `[NOT] EXISTS (SELECT …)`.
        Expr::Exists {
            select: inner_select,
            negated,
            ..
        } => {
            scope.push(ScopeFrame {
                schema: input.clone(),
                qualifier: None,
            });
            let inner_result = bind_select_with_ctes(inner_select, catalog, &[], scope);
            scope.pop();
            let inner_plan = inner_result?;
            let correlated = plan_contains_outer_column(&inner_plan);
            Ok(ScalarExpr::Exists {
                subplan: Box::new(inner_plan),
                negated: *negated,
                correlated,
            })
        }

        // `expr [NOT] IN (SELECT single_col …)`.
        Expr::InSubquery {
            expr: lhs_ast,
            select: inner_select,
            negated,
            ..
        } => {
            let lhs = bind_expr(lhs_ast, input, catalog, scope)?;
            scope.push(ScopeFrame {
                schema: input.clone(),
                qualifier: None,
            });
            let inner_result = bind_select_with_ctes(inner_select, catalog, &[], scope);
            scope.pop();
            let inner_plan = inner_result?;
            if inner_plan.schema().len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "IN subquery must return exactly 1 column, got {}",
                    inner_plan.schema().len()
                )));
            }
            let inner_type = inner_plan.schema().field_at(0).data_type.clone();
            if !comparable(&lhs.data_type(), &inner_type) {
                return Err(PlanError::TypeMismatch(format!(
                    "IN subquery: left type {} is not comparable to subquery column type {}",
                    lhs.data_type(),
                    inner_type,
                )));
            }
            let correlated = plan_contains_outer_column(&inner_plan);
            Ok(ScalarExpr::InSubquery {
                expr: Box::new(lhs),
                subplan: Box::new(inner_plan),
                negated: *negated,
                correlated,
                data_type: inner_type,
            })
        }

        // `expr = ANY (SELECT …)` — lowered to `InSubquery` with negated=false.
        //
        // Only `=` is supported; any other operator returns
        // [`PlanError::NotSupported`].
        Expr::Any {
            expr: lhs_ast,
            op,
            select: inner_select,
            ..
        } => {
            if *op != BinaryOp::Eq {
                return Err(PlanError::NotSupported(
                    "ANY with non-equality operator (only `= ANY` is supported)",
                ));
            }
            let lhs = bind_expr(lhs_ast, input, catalog, scope)?;
            scope.push(ScopeFrame {
                schema: input.clone(),
                qualifier: None,
            });
            let inner_result = bind_select_with_ctes(inner_select, catalog, &[], scope);
            scope.pop();
            let inner_plan = inner_result?;
            if inner_plan.schema().len() != 1 {
                return Err(PlanError::TypeMismatch(format!(
                    "= ANY subquery must return exactly 1 column, got {}",
                    inner_plan.schema().len()
                )));
            }
            let inner_type = inner_plan.schema().field_at(0).data_type.clone();
            let correlated = plan_contains_outer_column(&inner_plan);
            Ok(ScalarExpr::InSubquery {
                expr: Box::new(lhs),
                subplan: Box::new(inner_plan),
                negated: false,
                correlated,
                data_type: inner_type,
            })
        }

        // `ALL (SELECT …)` — not supported at this layer.
        Expr::All { .. } => Err(PlanError::NotSupported(
            "ALL subquery expressions are not supported",
        )),

        // `expr [NOT] BETWEEN [SYMMETRIC] low AND high` is rewritten at
        // bind time into an equivalent boolean tree of comparisons.
        // SQL:2016 specifies the equivalence; PostgreSQL's planner uses
        // the same rewrite.
        Expr::Between {
            expr: subject,
            low,
            high,
            negated,
            symmetric,
            ..
        } => bind_between(
            subject, low, high, *negated, *symmetric, input, catalog, scope,
        ),

        _ => Err(PlanError::NotSupported("expression variant")),
    }
}

/// Bind `expr [NOT] BETWEEN [SYMMETRIC] low AND high` into an equivalent
/// boolean tree over the existing comparison and boolean operators.
///
/// The rewrites mirror the SQL:2016 specification and PostgreSQL's
/// planner behaviour:
///
/// - `expr BETWEEN low AND high` ⇒ `expr >= low AND expr <= high`.
/// - `expr NOT BETWEEN low AND high` ⇒ `expr < low OR expr > high`.
/// - `expr BETWEEN SYMMETRIC low AND high` ⇒
///   `(expr >= low AND expr <= high) OR (expr >= high AND expr <= low)`.
/// - `expr NOT BETWEEN SYMMETRIC low AND high` ⇒
///   `(expr < low OR expr > high) AND (expr < high OR expr > low)`.
///
/// Each of `expr`, `low`, and `high` is bound exactly once; the bound
/// `expr` is cloned wherever the rewrite needs an additional reference
/// to it. This means side-effectful expressions (function calls,
/// sequence next-val, etc.) are evaluated more than once at runtime —
/// PostgreSQL documents the same limitation and we accept it for the
/// same reason: the existing comparison + boolean operators already
/// flow through the SIMD-aware [`crate::expr::ScalarExpr::Binary`]
/// pipeline, and synthesising a Let-style binding would grow the plan
/// language for no measurable benefit on the SQL surface UltraSQL
/// implements today (pure column / literal predicates).
#[allow(clippy::too_many_arguments)]
fn bind_between(
    subject: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    symmetric: bool,
    input: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let bound_expr = bind_expr(subject, input, catalog, scope)?;
    let bound_low = bind_expr(low, input, catalog, scope)?;
    let bound_high = bind_expr(high, input, catalog, scope)?;

    // The forward range test: `expr >= low AND expr <= high`.
    let forward = make_range_test(
        bound_expr.clone(),
        bound_low.clone(),
        bound_high.clone(),
        negated,
    )?;
    if !symmetric {
        return Ok(forward);
    }
    // The reversed range test, with low/high swapped. The combining
    // connective is `OR` for the affirmative form (a value satisfies
    // either ordering) and `AND` for the negated form (the value lies
    // outside both ranges).
    let reversed = make_range_test(bound_expr, bound_high, bound_low, negated)?;
    let combine_op = if negated { BinaryOp::And } else { BinaryOp::Or };
    Ok(ScalarExpr::Binary {
        op: combine_op,
        left: Box::new(forward),
        right: Box::new(reversed),
        data_type: DataType::Bool,
    })
}

/// Build one bound boolean predicate of the form
/// `expr op_low low <connect> expr op_high high`, where the operators
/// are picked by `negated`:
///
/// - `negated = false` → `expr >= low AND expr <= high`.
/// - `negated = true`  → `expr <  low OR  expr >  high`.
///
/// The two comparison subterms are validated through
/// [`binary_result_type`] so that type errors (e.g. comparing a text
/// column to an integer bound) surface as
/// [`PlanError::TypeMismatch`], matching the diagnostics callers see
/// from an explicit `expr >= low AND expr <= high` predicate.
fn make_range_test(
    bound_expr: ScalarExpr,
    bound_low: ScalarExpr,
    bound_high: ScalarExpr,
    negated: bool,
) -> Result<ScalarExpr, PlanError> {
    let (lo_op, hi_op, connect) = if negated {
        (BinaryOp::Lt, BinaryOp::Gt, BinaryOp::Or)
    } else {
        (BinaryOp::GtEq, BinaryOp::LtEq, BinaryOp::And)
    };
    let lo_cmp = make_binary(lo_op, bound_expr.clone(), bound_low)?;
    let hi_cmp = make_binary(hi_op, bound_expr, bound_high)?;
    Ok(ScalarExpr::Binary {
        op: connect,
        left: Box::new(lo_cmp),
        right: Box::new(hi_cmp),
        data_type: DataType::Bool,
    })
}

/// Construct a [`ScalarExpr::Binary`] over already-bound operands.
///
/// The operands' types are checked via [`binary_result_type`] exactly
/// as in [`bind_binary`], so the rewrite produces the same diagnostics
/// callers would see from the explicit `>=` / `<=` / `<` / `>` form.
pub(crate) fn make_binary(
    op: BinaryOp,
    left: ScalarExpr,
    right: ScalarExpr,
) -> Result<ScalarExpr, PlanError> {
    let data_type = binary_result_type(op, left.data_type(), right.data_type())?;
    Ok(ScalarExpr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
        data_type,
    })
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
    scope: &ScopeStack,
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
        // Column not found in the inner scope — try outer scopes.  This
        // produces an OuterColumn when we are inside a subquery.
        if let Some(outer_ref) = scope.resolve(&col_name) {
            return Ok(ScalarExpr::OuterColumn {
                name: col_name,
                frame_depth: outer_ref.frame_depth,
                column_index: outer_ref.column_index,
                data_type: outer_ref.data_type,
            });
        }
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

fn bind_unary(
    op: UnaryOp,
    inner: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let bound = bind_expr(inner, input, catalog, scope)?;
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
        UnaryOp::BitNot => {
            if inner_ty.is_integer() || matches!(inner_ty, DataType::Null) {
                inner_ty
            } else {
                return Err(PlanError::TypeMismatch(format!(
                    "bitwise NOT (~) requires integer operand, got {inner_ty}"
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

#[allow(clippy::too_many_lines)]
fn bind_binary(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    input: &Schema,
    catalog: &dyn Catalog,
    scope: &mut ScopeStack,
) -> Result<ScalarExpr, PlanError> {
    let l = bind_expr(left, input, catalog, scope)?;
    let r = bind_expr(right, input, catalog, scope)?;
    let data_type = binary_result_type(op, l.data_type(), r.data_type())?;
    Ok(ScalarExpr::Binary {
        op,
        left: Box::new(l),
        right: Box::new(r),
        data_type,
    })
}

/// Compute the result type of a binary operator applied to two operand types.
#[allow(clippy::too_many_lines)]
fn binary_result_type(op: BinaryOp, lt: DataType, rt: DataType) -> Result<DataType, PlanError> {
    match op {
        BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::Mod
        | BinaryOp::Pow => {
            if matches!(lt, DataType::Null) {
                Ok(rt)
            } else if matches!(rt, DataType::Null) {
                Ok(lt)
            } else {
                lt.numeric_join(&rt).map_err(|_| {
                    PlanError::TypeMismatch(format!(
                        "arithmetic operator {} on incompatible types {lt} and {rt}",
                        display_binary(op)
                    ))
                })
            }
        }
        BinaryOp::Concat => {
            if (lt.is_textlike() || matches!(lt, DataType::Null))
                && (rt.is_textlike() || matches!(rt, DataType::Null))
            {
                Ok(DataType::Text { max_len: None })
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "string concatenation requires text operands, got {lt} and {rt}"
                )))
            }
        }
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => {
            if comparable(&lt, &rt) {
                Ok(DataType::Bool)
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "cannot compare {lt} and {rt}"
                )))
            }
        }
        BinaryOp::And | BinaryOp::Or => {
            if matches!(lt, DataType::Bool | DataType::Null)
                && matches!(rt, DataType::Bool | DataType::Null)
            {
                Ok(DataType::Bool)
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "{} requires boolean operands, got {lt} and {rt}",
                    display_binary(op)
                )))
            }
        }
        BinaryOp::Like
        | BinaryOp::NotLike
        | BinaryOp::Ilike
        | BinaryOp::NotIlike
        | BinaryOp::RegexMatch
        | BinaryOp::RegexIMatch
        | BinaryOp::RegexNotMatch
        | BinaryOp::RegexNotIMatch => {
            if (lt.is_textlike() || matches!(lt, DataType::Null))
                && (rt.is_textlike() || matches!(rt, DataType::Null))
            {
                Ok(DataType::Bool)
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "{} requires text operands, got {lt} and {rt}",
                    display_binary(op)
                )))
            }
        }
        BinaryOp::BitAnd
        | BinaryOp::BitOr
        | BinaryOp::BitXor
        | BinaryOp::ShiftLeft
        | BinaryOp::ShiftRight => {
            if matches!(lt, DataType::Null) {
                Ok(rt)
            } else if matches!(rt, DataType::Null) {
                Ok(lt)
            } else if lt.is_integer() && rt.is_integer() {
                lt.numeric_join(&rt).map_err(|_| {
                    PlanError::TypeMismatch(format!(
                        "bitwise operator {} on incompatible types {lt} and {rt}",
                        display_binary(op)
                    ))
                })
            } else {
                Err(PlanError::TypeMismatch(format!(
                    "bitwise operator {} requires integer operands, got {lt} and {rt}",
                    display_binary(op)
                )))
            }
        }
        BinaryOp::JsonGet | BinaryOp::JsonGetPath => Ok(DataType::Jsonb),
        BinaryOp::JsonGetText | BinaryOp::JsonGetPathText => Ok(DataType::Text { max_len: None }),
        BinaryOp::JsonContains
        | BinaryOp::JsonContained
        | BinaryOp::JsonHasKey
        | BinaryOp::JsonHasAnyKey
        | BinaryOp::JsonHasAllKeys => Ok(DataType::Bool),
    }
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
        UnaryOp::BitNot => "~",
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
        BinaryOp::RegexMatch => "~",
        BinaryOp::RegexIMatch => "~*",
        BinaryOp::RegexNotMatch => "!~",
        BinaryOp::RegexNotIMatch => "!~*",
        BinaryOp::BitAnd => "&",
        BinaryOp::BitOr => "|",
        BinaryOp::BitXor => "#",
        BinaryOp::ShiftLeft => "<<",
        BinaryOp::ShiftRight => ">>",
        BinaryOp::JsonGet => "->",
        BinaryOp::JsonGetText => "->>",
        BinaryOp::JsonGetPath => "#>",
        BinaryOp::JsonGetPathText => "#>>",
        BinaryOp::JsonContains => "@>",
        BinaryOp::JsonContained => "<@",
        BinaryOp::JsonHasKey => "?",
        BinaryOp::JsonHasAnyKey => "?|",
        BinaryOp::JsonHasAllKeys => "?&",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::items_after_statements)]
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
    // CREATE TABLE
    // -----------------------------------------------------------------------

    #[test]
    fn binds_create_table_resolves_basic_column_types() {
        let cat = InMemoryCatalog::new();
        let plan = parse_and_bind(
            "CREATE TABLE accounts (id BIGINT NOT NULL, name TEXT, balance FLOAT8)",
            &cat,
        )
        .expect("bind ok");
        let LogicalPlan::CreateTable {
            table_name,
            namespace,
            columns,
            if_not_exists,
            schema,
        } = plan
        else {
            panic!("expected CreateTable, got other plan");
        };
        assert_eq!(table_name, "accounts");
        assert_eq!(namespace, "public");
        assert!(!if_not_exists);
        assert_eq!(schema, Schema::empty());
        assert_eq!(columns.len(), 3);
        assert_eq!(columns.fields()[0].name, "id");
        assert_eq!(columns.fields()[0].data_type, DataType::Int64);
        assert!(!columns.fields()[0].nullable, "NOT NULL honored");
        assert_eq!(
            columns.fields()[1].data_type,
            DataType::Text { max_len: None }
        );
        assert!(columns.fields()[1].nullable, "no constraint = nullable");
        assert_eq!(columns.fields()[2].data_type, DataType::Float64);
    }

    #[test]
    fn binds_create_table_with_varchar_modifier() {
        let cat = InMemoryCatalog::new();
        let plan = parse_and_bind("CREATE TABLE t (s VARCHAR(255))", &cat).expect("bind ok");
        let LogicalPlan::CreateTable { columns, .. } = plan else {
            panic!("expected CreateTable");
        };
        assert_eq!(
            columns.fields()[0].data_type,
            DataType::Text { max_len: Some(255) }
        );
    }

    #[test]
    fn binds_create_table_primary_key_implies_not_null() {
        let cat = InMemoryCatalog::new();
        let plan = parse_and_bind("CREATE TABLE t (id INT PRIMARY KEY)", &cat).expect("bind ok");
        let LogicalPlan::CreateTable { columns, .. } = plan else {
            panic!("expected CreateTable");
        };
        assert!(!columns.fields()[0].nullable);
    }

    #[test]
    fn binds_create_table_duplicate_column_rejected() {
        let cat = InMemoryCatalog::new();
        let err = parse_and_bind("CREATE TABLE t (id INT, id INT)", &cat).unwrap_err();
        assert!(
            matches!(err, PlanError::DuplicateColumn(ref c) if c == "id"),
            "got {err:?}"
        );
    }

    #[test]
    fn binds_create_table_existing_relation_rejected() {
        let cat = users_catalog();
        let err = parse_and_bind("CREATE TABLE users (id INT)", &cat).unwrap_err();
        assert!(
            matches!(err, PlanError::DuplicateTable(ref t) if t == "users"),
            "got {err:?}"
        );
    }

    #[test]
    fn binds_create_table_if_not_exists_skips_existence_check() {
        let cat = users_catalog();
        let plan =
            parse_and_bind("CREATE TABLE IF NOT EXISTS users (id INT)", &cat).expect("bind ok");
        let LogicalPlan::CreateTable {
            if_not_exists,
            table_name,
            ..
        } = plan
        else {
            panic!("expected CreateTable");
        };
        assert!(if_not_exists);
        assert_eq!(table_name, "users");
    }

    #[test]
    fn binds_create_table_with_qualified_namespace() {
        let cat = InMemoryCatalog::new();
        let plan = parse_and_bind("CREATE TABLE my_ns.events (id INT)", &cat).expect("bind ok");
        let LogicalPlan::CreateTable {
            table_name,
            namespace,
            ..
        } = plan
        else {
            panic!("expected CreateTable");
        };
        assert_eq!(namespace, "my_ns");
        assert_eq!(table_name, "events");
    }

    #[test]
    fn binds_create_table_rejects_unsupported_constraints() {
        let cat = InMemoryCatalog::new();
        let err = parse_and_bind("CREATE TABLE t (id INT UNIQUE)", &cat).unwrap_err();
        assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");

        let err = parse_and_bind("CREATE TABLE t (id INT DEFAULT 7)", &cat).unwrap_err();
        assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");

        let err = parse_and_bind("CREATE TABLE t (id INT CHECK (id > 0))", &cat).unwrap_err();
        assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
    }

    #[test]
    fn binds_create_table_rejects_unsupported_column_type() {
        let cat = InMemoryCatalog::new();
        let err = parse_and_bind("CREATE TABLE t (id NUMERIC(10, 2))", &cat).unwrap_err();
        assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
    }

    #[test]
    fn binds_create_table_persistent_catalog_via_snapshot_adapter() {
        // CatalogSnapshot from ultrasql-catalog implements `Catalog`,
        // so the binder can consume a persistent snapshot directly
        // (the seam the server uses to bind against PersistentCatalog).
        use ultrasql_catalog::TableEntry;
        let snap = ultrasql_catalog::CatalogSnapshot {
            tables: {
                let mut m = std::collections::HashMap::new();
                let schema =
                    Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
                m.insert(
                    "products".to_string(),
                    TableEntry::new(ultrasql_core::Oid::new(100), "products", "public", schema),
                );
                m
            },
            tables_by_oid: std::collections::HashMap::new(),
            indexes: std::collections::HashMap::new(),
            indexes_by_table: std::collections::HashMap::new(),
        };
        // Creating an already-existing relation through the snapshot
        // adapter surfaces DuplicateTable, proving the binder reaches
        // the snapshot.
        let stmt = Parser::new("CREATE TABLE products (id INT)")
            .parse_statement()
            .expect("parse ok");
        let err = bind(&stmt, &snap).unwrap_err();
        assert!(
            matches!(err, PlanError::DuplicateTable(ref t) if t == "products"),
            "got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // JOIN tests
    // -----------------------------------------------------------------------

    /// Build a two-table catalog: users (`id` INT, `name` TEXT) and orders (`oid` INT, `user_id` INT).
    fn two_table_catalog() -> InMemoryCatalog {
        let users_schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("name", DataType::Text { max_len: None }),
        ])
        .expect("schema ok");
        let orders_schema = Schema::new([
            Field::required("oid", DataType::Int32),
            Field::required("user_id", DataType::Int32),
        ])
        .expect("schema ok");
        let mut cat = InMemoryCatalog::new();
        cat.register("users", TableMeta::new(users_schema));
        cat.register("orders", TableMeta::new(orders_schema));
        cat
    }

    #[test]
    fn binds_inner_join_with_on_predicate() {
        let cat = two_table_catalog();
        let plan = parse_and_bind(
            "SELECT users.id FROM users INNER JOIN orders ON users.id = orders.user_id",
            &cat,
        )
        .expect("bind ok");
        // The top-level plan has a Project; find the Join underneath.
        fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::Join { .. } => Some(plan),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Filter { input, .. }
                | LogicalPlan::Sort { input, .. }
                | LogicalPlan::Limit { input, .. } => find_join(input),
                _ => None,
            }
        }
        let join = find_join(&plan).expect("should contain a Join node");
        let LogicalPlan::Join {
            join_type,
            condition,
            schema,
            ..
        } = join
        else {
            panic!("expected Join");
        };
        assert_eq!(*join_type, LogicalJoinType::Inner);
        assert!(
            matches!(condition, LogicalJoinCondition::On(_)),
            "expected ON condition"
        );
        // Schema is concatenation: users(id, name) + orders(oid, user_id) = 4
        assert_eq!(schema.len(), 4, "join schema width should be 4");
    }

    #[test]
    fn binds_left_outer_join_makes_right_columns_nullable() {
        let cat = two_table_catalog();
        let plan = parse_and_bind(
            "SELECT users.id FROM users LEFT JOIN orders ON users.id = orders.user_id",
            &cat,
        )
        .expect("bind ok");

        fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::Join { .. } => Some(plan),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Filter { input, .. }
                | LogicalPlan::Sort { input, .. }
                | LogicalPlan::Limit { input, .. } => find_join(input),
                _ => None,
            }
        }
        let join = find_join(&plan).expect("should contain a Join");
        let LogicalPlan::Join {
            join_type, schema, ..
        } = join
        else {
            panic!("expected Join");
        };
        assert_eq!(*join_type, LogicalJoinType::LeftOuter);
        // Left columns (users.id, users.name): id was required, stays required.
        assert!(
            !schema.field_at(0).nullable,
            "left.id should remain required"
        );
        // Right columns (orders.oid, orders.user_id) should be nullable in LEFT JOIN.
        assert!(schema.field_at(2).nullable, "right.oid should be nullable");
        assert!(
            schema.field_at(3).nullable,
            "right.user_id should be nullable"
        );
    }

    #[test]
    fn binds_right_outer_join_makes_left_columns_nullable() {
        let cat = two_table_catalog();
        let plan = parse_and_bind(
            "SELECT users.id FROM users RIGHT JOIN orders ON users.id = orders.user_id",
            &cat,
        )
        .expect("bind ok");

        fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::Join { .. } => Some(plan),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Filter { input, .. }
                | LogicalPlan::Sort { input, .. }
                | LogicalPlan::Limit { input, .. } => find_join(input),
                _ => None,
            }
        }
        let join = find_join(&plan).expect("should contain a Join");
        let LogicalPlan::Join {
            join_type, schema, ..
        } = join
        else {
            panic!("expected Join");
        };
        assert_eq!(*join_type, LogicalJoinType::RightOuter);
        // In RIGHT JOIN: left columns become nullable.
        assert!(
            schema.field_at(0).nullable,
            "left.id should be nullable in RIGHT JOIN"
        );
        // Right columns keep their original nullability (both were required).
        assert!(!schema.field_at(2).nullable, "right.oid stays required");
    }

    #[test]
    fn binds_full_outer_join_makes_both_sides_nullable() {
        let cat = two_table_catalog();
        let plan = parse_and_bind(
            "SELECT users.id FROM users FULL OUTER JOIN orders ON users.id = orders.user_id",
            &cat,
        )
        .expect("bind ok");

        fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::Join { .. } => Some(plan),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Filter { input, .. }
                | LogicalPlan::Sort { input, .. }
                | LogicalPlan::Limit { input, .. } => find_join(input),
                _ => None,
            }
        }
        let join = find_join(&plan).expect("should contain a Join");
        let LogicalPlan::Join {
            join_type, schema, ..
        } = join
        else {
            panic!("expected Join");
        };
        assert_eq!(*join_type, LogicalJoinType::FullOuter);
        // Both sides should be nullable.
        assert!(
            schema.field_at(0).nullable,
            "left.id should be nullable in FULL OUTER JOIN"
        );
        assert!(
            schema.field_at(2).nullable,
            "right.oid should be nullable in FULL OUTER JOIN"
        );
    }

    #[test]
    fn binds_cross_join_has_no_predicate() {
        let cat = two_table_catalog();
        let plan =
            parse_and_bind("SELECT users.id FROM users CROSS JOIN orders", &cat).expect("bind ok");

        fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::Join { .. } => Some(plan),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Filter { input, .. }
                | LogicalPlan::Sort { input, .. }
                | LogicalPlan::Limit { input, .. } => find_join(input),
                _ => None,
            }
        }
        let join = find_join(&plan).expect("should contain a Join");
        let LogicalPlan::Join {
            join_type,
            condition,
            ..
        } = join
        else {
            panic!("expected Join");
        };
        assert_eq!(*join_type, LogicalJoinType::Cross);
        assert!(
            matches!(condition, LogicalJoinCondition::None),
            "cross join should have no condition"
        );
    }

    #[test]
    fn binds_using_join_folds_to_equality_and_collapses_columns() {
        // Build a catalog where both tables have a column named `id`.
        let schema_a = Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok");
        let schema_b = Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("val", DataType::Text { max_len: None }),
        ])
        .expect("schema ok");
        let mut cat = InMemoryCatalog::new();
        cat.register("a", TableMeta::new(schema_a));
        cat.register("b", TableMeta::new(schema_b));

        let plan = parse_and_bind("SELECT a.id FROM a JOIN b USING (id)", &cat).expect("bind ok");

        fn find_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::Join { .. } => Some(plan),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Filter { input, .. }
                | LogicalPlan::Sort { input, .. }
                | LogicalPlan::Limit { input, .. } => find_join(input),
                _ => None,
            }
        }
        let join = find_join(&plan).expect("should contain a Join");
        let LogicalPlan::Join {
            condition, schema, ..
        } = join
        else {
            panic!("expected Join");
        };
        assert!(
            matches!(condition, LogicalJoinCondition::Using(_)),
            "expected USING condition"
        );
        // USING(id) collapses: id once + val = 2 columns (not 3).
        assert_eq!(
            schema.len(),
            2,
            "USING join should collapse the shared column"
        );
    }

    // -----------------------------------------------------------------------
    // GROUP BY / aggregate tests
    // -----------------------------------------------------------------------

    #[test]
    fn binds_group_by_emits_aggregate_node() {
        let cat = users_catalog();
        let plan =
            parse_and_bind("SELECT id, count(*) FROM users GROUP BY id", &cat).expect("bind ok");

        fn find_agg(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::Aggregate { .. } => Some(plan),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Filter { input, .. }
                | LogicalPlan::Sort { input, .. }
                | LogicalPlan::Limit { input, .. } => find_agg(input),
                _ => None,
            }
        }
        let agg = find_agg(&plan).expect("should contain Aggregate node");
        let LogicalPlan::Aggregate {
            group_by,
            aggregates,
            schema,
            ..
        } = agg
        else {
            panic!("expected Aggregate");
        };
        assert_eq!(group_by.len(), 1, "one GROUP BY key");
        assert_eq!(aggregates.len(), 1, "one aggregate");
        assert_eq!(aggregates[0].func, AggregateFunc::CountStar);
        // Schema: [id, count]
        assert_eq!(schema.len(), 2);
        assert_eq!(schema.field_at(0).name, "id");
    }

    #[test]
    fn binds_count_star() {
        let cat = users_catalog();
        let plan = parse_and_bind("SELECT count(*) FROM users", &cat).expect("bind ok");

        fn find_agg(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::Aggregate { .. } => Some(plan),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Filter { input, .. }
                | LogicalPlan::Sort { input, .. }
                | LogicalPlan::Limit { input, .. } => find_agg(input),
                _ => None,
            }
        }
        let agg = find_agg(&plan).expect("should contain Aggregate node");
        let LogicalPlan::Aggregate { aggregates, .. } = agg else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggregates.len(), 1);
        assert_eq!(aggregates[0].func, AggregateFunc::CountStar);
        assert!(aggregates[0].arg.is_none(), "count(*) has no argument");
    }

    #[test]
    fn binds_having_filters_post_aggregate() {
        let cat = users_catalog();
        let plan = parse_and_bind(
            "SELECT id, count(*) FROM users GROUP BY id HAVING count(*) > 1",
            &cat,
        )
        .expect("bind ok");

        fn find_filter_above_agg(plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::Filter { input, .. } => {
                    matches!(input.as_ref(), LogicalPlan::Aggregate { .. })
                }
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Sort { input, .. }
                | LogicalPlan::Limit { input, .. } => find_filter_above_agg(input),
                _ => false,
            }
        }
        assert!(
            find_filter_above_agg(&plan),
            "should have Filter above Aggregate for HAVING"
        );
    }

    // -----------------------------------------------------------------------
    // Set operations tests
    // -----------------------------------------------------------------------

    #[test]
    fn binds_union_all_arity_match() {
        let cat = users_catalog();
        let plan = parse_and_bind("SELECT id FROM users UNION ALL SELECT id FROM users", &cat)
            .expect("bind ok");

        fn find_setop(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::SetOp { .. } => Some(plan),
                LogicalPlan::Cte { body, .. } => find_setop(body),
                _ => None,
            }
        }
        // The SetOp may be wrapped in a Cte if there were CTEs, otherwise it's
        // at the top level.
        let setop = find_setop(&plan).unwrap_or(&plan);
        // Accept either SetOp at top or wrapped in project.
        let has_setop = matches!(plan, LogicalPlan::SetOp { .. })
            || matches!(&plan, LogicalPlan::Project { input, .. }
                if matches!(input.as_ref(), LogicalPlan::SetOp { .. }));
        // Or the plan IS the setop.
        let is_setop = matches!(&plan, LogicalPlan::SetOp { quantifier, .. }
            if *quantifier == LogicalSetQuantifier::All);
        // If it's not directly at top, it's wrapped by the outer structure.
        if !has_setop && !is_setop {
            // Find it anywhere in the tree.
            let _ = setop;
            // The schema should have 1 column.
            let final_schema = plan.schema();
            assert_eq!(
                final_schema.len(),
                1,
                "UNION ALL of single-column selects = 1 col"
            );
        } else {
            assert!(has_setop || is_setop);
        }
        let _ = setop;
    }

    #[test]
    fn binds_union_distinct_with_arity_mismatch_is_rejected() {
        let cat = users_catalog();
        // id (1 col) UNION id, name (2 cols) should fail.
        let err = parse_and_bind(
            "SELECT id FROM users UNION SELECT id, name FROM users",
            &cat,
        )
        .unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    }

    // -----------------------------------------------------------------------
    // CTE tests
    // -----------------------------------------------------------------------

    #[test]
    fn binds_cte_then_references_it_in_body() {
        let cat = users_catalog();
        let plan = parse_and_bind(
            "WITH active AS (SELECT id FROM users) SELECT id FROM active",
            &cat,
        )
        .expect("bind ok");

        // Top-level plan should be a Cte node.
        let LogicalPlan::Cte {
            name, recursive, ..
        } = &plan
        else {
            panic!("expected Cte at top, got {plan:?}");
        };
        assert_eq!(name, "active");
        assert!(!recursive, "non-recursive CTE should have recursive=false");
    }

    // -----------------------------------------------------------------------
    // SELECT * wildcard tests
    // -----------------------------------------------------------------------

    #[test]
    fn binds_select_star_expands_via_catalog() {
        let cat = users_catalog();
        let plan = parse_and_bind("SELECT * FROM users", &cat).expect("bind ok");
        let LogicalPlan::Project { schema, exprs, .. } = &plan else {
            panic!("expected Project, got {plan:?}");
        };
        // users has id, name, score = 3 columns
        assert_eq!(schema.len(), 3, "SELECT * should expand to 3 columns");
        assert_eq!(exprs.len(), 3);
    }

    #[test]
    fn binds_qualified_wildcard_restricts_to_table_alias() {
        let cat = two_table_catalog();
        let plan = parse_and_bind(
            "SELECT u.* FROM users u JOIN orders o ON u.id = o.user_id",
            &cat,
        )
        .expect("bind ok");
        let LogicalPlan::Project { schema, .. } = &plan else {
            panic!("expected Project, got {plan:?}");
        };
        // users u has 2 columns; u.* should expand to those 2 only.
        assert_eq!(schema.len(), 2, "u.* should expand to users' 2 columns");
    }

    // -----------------------------------------------------------------------
    // Error / unsupported
    // -----------------------------------------------------------------------

    #[test]
    fn binder_rejects_unknown_aggregate_with_not_supported() {
        let cat = users_catalog();
        // `mode` is not a known aggregate; the binder should reject it.
        let err = parse_and_bind("SELECT mode(score) FROM users GROUP BY id", &cat).unwrap_err();
        assert!(
            matches!(err, PlanError::NotSupported(_)),
            "unknown aggregate should be NotSupported, got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Property test
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Subquery tests
    // -----------------------------------------------------------------------

    /// A two-table catalog: `users (id INT, name TEXT, score FLOAT8)`
    /// and `orders (oid INT, user_id INT)`.
    fn subquery_catalog() -> InMemoryCatalog {
        let users_schema = Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema ok");
        let orders_schema = Schema::new([
            Field::required("oid", DataType::Int32),
            Field::required("user_id", DataType::Int32),
        ])
        .expect("schema ok");
        let mut cat = InMemoryCatalog::new();
        cat.register("users", TableMeta::new(users_schema));
        cat.register("orders", TableMeta::new(orders_schema));
        cat
    }

    #[test]
    fn binds_uncorrelated_exists_subquery() {
        // `EXISTS (SELECT oid FROM orders)` has no outer column references.
        let cat = subquery_catalog();
        let plan = parse_and_bind(
            "SELECT id FROM users WHERE EXISTS (SELECT oid FROM orders)",
            &cat,
        )
        .expect("bind ok");
        // Walk to the Filter and check its predicate.
        fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::Filter { .. } => Some(plan),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Limit { input, .. }
                | LogicalPlan::Sort { input, .. } => find_filter(input),
                _ => None,
            }
        }
        let filter = find_filter(&plan).expect("should have Filter");
        let LogicalPlan::Filter { predicate, .. } = filter else {
            panic!("expected Filter");
        };
        let ScalarExpr::Exists {
            negated,
            correlated,
            ..
        } = predicate
        else {
            panic!("expected Exists predicate, got {predicate:?}");
        };
        assert!(!negated, "should not be negated");
        assert!(!correlated, "no outer column reference → uncorrelated");
    }

    #[test]
    fn binds_correlated_exists_subquery() {
        // `EXISTS (SELECT oid FROM orders WHERE user_id = id)` — `id` is not in
        // `orders`, so it resolves to the outer `users.id`.
        let cat = subquery_catalog();
        let plan = parse_and_bind(
            "SELECT id FROM users WHERE EXISTS (SELECT oid FROM orders WHERE user_id = id)",
            &cat,
        )
        .expect("bind ok");
        fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::Filter { .. } => Some(plan),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Limit { input, .. }
                | LogicalPlan::Sort { input, .. } => find_filter(input),
                _ => None,
            }
        }
        let filter = find_filter(&plan).expect("should have Filter");
        let LogicalPlan::Filter { predicate, .. } = filter else {
            panic!("expected Filter");
        };
        let ScalarExpr::Exists { correlated, .. } = predicate else {
            panic!("expected Exists, got {predicate:?}");
        };
        assert!(correlated, "id resolves to outer users.id → correlated");
    }

    #[test]
    fn binds_in_subquery_arity_1_check_rejects_multi_column() {
        // `id IN (SELECT oid, user_id FROM orders)` — 2-column subquery must fail.
        let cat = subquery_catalog();
        let err = parse_and_bind(
            "SELECT id FROM users WHERE id IN (SELECT oid, user_id FROM orders)",
            &cat,
        )
        .unwrap_err();
        assert!(
            matches!(err, PlanError::TypeMismatch(_)),
            "multi-column IN subquery should be TypeMismatch, got {err:?}"
        );
    }

    #[test]
    fn binds_scalar_subquery_returns_scalar_subquery_expr() {
        // `(SELECT oid FROM orders LIMIT 1)` used as a scalar in the projection.
        let cat = subquery_catalog();
        let plan = parse_and_bind(
            "SELECT id, (SELECT oid FROM orders LIMIT 1) FROM users",
            &cat,
        )
        .expect("bind ok");
        let LogicalPlan::Project { exprs, .. } = &plan else {
            panic!("expected Project, got {plan:?}");
        };
        // Second expression should be a ScalarSubquery.
        let (second_expr, _) = &exprs[1];
        assert!(
            matches!(
                second_expr,
                ScalarExpr::ScalarSubquery {
                    correlated: false,
                    ..
                }
            ),
            "expected uncorrelated ScalarSubquery, got {second_expr:?}"
        );
    }

    #[test]
    fn binds_not_in_subquery() {
        let cat = subquery_catalog();
        let plan = parse_and_bind(
            "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM orders)",
            &cat,
        )
        .expect("bind ok");
        fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::Filter { .. } => Some(plan),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Limit { input, .. }
                | LogicalPlan::Sort { input, .. } => find_filter(input),
                _ => None,
            }
        }
        let filter = find_filter(&plan).expect("should have Filter");
        let LogicalPlan::Filter { predicate, .. } = filter else {
            panic!("expected Filter");
        };
        let ScalarExpr::InSubquery { negated, .. } = predicate else {
            panic!("expected InSubquery, got {predicate:?}");
        };
        assert!(negated, "NOT IN should produce negated=true");
    }

    #[test]
    fn binds_any_eq_lowers_to_exists() {
        // `id = ANY (SELECT user_id FROM orders)` should bind as InSubquery with
        // negated=false (the same representation as `id IN (…)`).
        let cat = subquery_catalog();
        let plan = parse_and_bind(
            "SELECT id FROM users WHERE id = ANY (SELECT user_id FROM orders)",
            &cat,
        )
        .expect("bind ok");
        fn find_filter(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            match plan {
                LogicalPlan::Filter { .. } => Some(plan),
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Limit { input, .. }
                | LogicalPlan::Sort { input, .. } => find_filter(input),
                _ => None,
            }
        }
        let filter = find_filter(&plan).expect("should have Filter");
        let LogicalPlan::Filter { predicate, .. } = filter else {
            panic!("expected Filter");
        };
        assert!(
            matches!(predicate, ScalarExpr::InSubquery { negated: false, .. }),
            "= ANY should lower to InSubquery(negated=false), got {predicate:?}"
        );
    }

    #[test]
    fn binds_any_with_lt_returns_not_supported() {
        let cat = subquery_catalog();
        let err = parse_and_bind(
            "SELECT id FROM users WHERE id < ANY (SELECT user_id FROM orders)",
            &cat,
        )
        .unwrap_err();
        assert!(
            matches!(err, PlanError::NotSupported(_)),
            "< ANY should be NotSupported, got {err:?}"
        );
    }

    #[test]
    fn binder_rejects_scalar_subquery_with_multi_column_projection() {
        let cat = subquery_catalog();
        let err = parse_and_bind(
            "SELECT id, (SELECT oid, user_id FROM orders LIMIT 1) FROM users",
            &cat,
        )
        .unwrap_err();
        assert!(
            matches!(err, PlanError::TypeMismatch(_)),
            "multi-column scalar subquery should be TypeMismatch, got {err:?}"
        );
    }

    #[test]
    fn outer_column_correctly_tracks_frame_depth_in_nested_subquery() {
        // Outer query scans `users`.  The subquery scans `orders`.  Inside the
        // subquery's WHERE, `id` is not in `orders` so it should resolve as
        // `OuterColumn { frame_depth: 1, … }`.
        let cat = subquery_catalog();
        let plan = parse_and_bind(
            "SELECT id FROM users WHERE EXISTS (SELECT oid FROM orders WHERE user_id = id)",
            &cat,
        )
        .expect("bind ok");
        // Navigate to the Exists predicate's inner plan.
        fn find_exists_pred(plan: &LogicalPlan) -> Option<&ScalarExpr> {
            match plan {
                LogicalPlan::Filter { predicate, .. } => {
                    if matches!(predicate, ScalarExpr::Exists { .. }) {
                        Some(predicate)
                    } else {
                        None
                    }
                }
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Sort { input, .. }
                | LogicalPlan::Limit { input, .. } => find_exists_pred(input),
                _ => None,
            }
        }
        let pred = find_exists_pred(&plan).expect("should find Exists predicate");
        let ScalarExpr::Exists { subplan, .. } = pred else {
            panic!("expected Exists");
        };
        // The inner plan should have a Filter with an outer-column reference.
        fn find_outer_col(plan: &LogicalPlan) -> Option<usize> {
            match plan {
                LogicalPlan::Filter { predicate, .. } => {
                    // Predicate is `user_id = id` — a Binary with the right side
                    // being an OuterColumn.
                    if let ScalarExpr::Binary { right, .. } = predicate {
                        if let ScalarExpr::OuterColumn { frame_depth, .. } = right.as_ref() {
                            return Some(*frame_depth);
                        }
                    }
                    None
                }
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Sort { input, .. }
                | LogicalPlan::Limit { input, .. } => find_outer_col(input),
                _ => None,
            }
        }
        let depth = find_outer_col(subplan).expect("should find OuterColumn in subplan");
        assert_eq!(depth, 1, "column is one level out → frame_depth = 1");
    }

    // -----------------------------------------------------------------------
    // BETWEEN tests — the binder rewrites BETWEEN into a comparison tree
    // -----------------------------------------------------------------------

    /// Extract the bound WHERE predicate from a SELECT plan that the
    /// binder shaped as `Project { Filter { Scan } }`.
    fn predicate_of(plan: &LogicalPlan) -> &ScalarExpr {
        fn find_filter(plan: &LogicalPlan) -> &LogicalPlan {
            match plan {
                LogicalPlan::Filter { .. } => plan,
                LogicalPlan::Project { input, .. }
                | LogicalPlan::Sort { input, .. }
                | LogicalPlan::Limit { input, .. } => find_filter(input),
                _ => panic!("expected Filter under plan, got {plan:?}"),
            }
        }
        match find_filter(plan) {
            LogicalPlan::Filter { predicate, .. } => predicate,
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    #[test]
    fn binds_between_as_ge_and_le() {
        // The canonical rewrite: BETWEEN low AND high becomes
        // `expr >= low AND expr <= high`.
        let plan = parse_bind_ok("SELECT id FROM users WHERE id BETWEEN 5 AND 10");
        let pred = predicate_of(&plan);
        // Top-level: AND.
        let ScalarExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            data_type,
        } = pred
        else {
            panic!("expected AND at the root, got {pred:?}");
        };
        assert_eq!(*data_type, DataType::Bool);

        // Left arm: `id >= 5`.
        let ScalarExpr::Binary {
            op: BinaryOp::GtEq,
            left: lo_l,
            right: lo_r,
            ..
        } = left.as_ref()
        else {
            panic!("expected GtEq on left, got {left:?}");
        };
        assert!(matches!(lo_l.as_ref(), ScalarExpr::Column { name, .. } if name == "id"));
        assert!(matches!(
            lo_r.as_ref(),
            ScalarExpr::Literal {
                value: Value::Int32(5),
                ..
            }
        ));

        // Right arm: `id <= 10`.
        let ScalarExpr::Binary {
            op: BinaryOp::LtEq,
            left: hi_l,
            right: hi_r,
            ..
        } = right.as_ref()
        else {
            panic!("expected LtEq on right, got {right:?}");
        };
        assert!(matches!(hi_l.as_ref(), ScalarExpr::Column { name, .. } if name == "id"));
        assert!(matches!(
            hi_r.as_ref(),
            ScalarExpr::Literal {
                value: Value::Int32(10),
                ..
            }
        ));
    }

    #[test]
    fn binds_not_between_as_lt_or_gt() {
        let plan = parse_bind_ok("SELECT id FROM users WHERE id NOT BETWEEN 5 AND 10");
        let pred = predicate_of(&plan);
        let ScalarExpr::Binary {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } = pred
        else {
            panic!("expected OR at the root, got {pred:?}");
        };
        assert!(matches!(
            left.as_ref(),
            ScalarExpr::Binary {
                op: BinaryOp::Lt,
                ..
            }
        ));
        assert!(matches!(
            right.as_ref(),
            ScalarExpr::Binary {
                op: BinaryOp::Gt,
                ..
            }
        ));
    }

    #[test]
    fn binds_between_mixed_numeric_types() {
        // `score` is FLOAT8 in the users catalog. A BETWEEN against an
        // integer pair must bind cleanly through the same numeric-join
        // promotion that the explicit comparison form uses.
        let plan = parse_bind_ok("SELECT id FROM users WHERE score BETWEEN 1 AND 100");
        let pred = predicate_of(&plan);
        assert!(matches!(
            pred,
            ScalarExpr::Binary {
                op: BinaryOp::And,
                ..
            }
        ));
    }

    #[test]
    fn binds_between_symmetric_emits_or_of_two_ranges() {
        let plan = parse_bind_ok("SELECT id FROM users WHERE id BETWEEN SYMMETRIC 10 AND 5");
        let pred = predicate_of(&plan);
        // BETWEEN SYMMETRIC: (forward) OR (reversed).
        let ScalarExpr::Binary {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } = pred
        else {
            panic!("expected OR at the root, got {pred:?}");
        };
        // Each arm is a `(>= AND <=)` tree.
        for (label, arm) in [("forward", left.as_ref()), ("reversed", right.as_ref())] {
            assert!(
                matches!(
                    arm,
                    ScalarExpr::Binary {
                        op: BinaryOp::And,
                        ..
                    }
                ),
                "SYMMETRIC {label} arm should be AND, got {arm:?}"
            );
        }
    }

    #[test]
    fn binds_not_between_symmetric_emits_and_of_two_ranges() {
        let plan = parse_bind_ok("SELECT id FROM users WHERE id NOT BETWEEN SYMMETRIC 10 AND 5");
        let pred = predicate_of(&plan);
        // NOT BETWEEN SYMMETRIC: (forward NOT) AND (reversed NOT).
        let ScalarExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } = pred
        else {
            panic!("expected AND at the root, got {pred:?}");
        };
        for (label, arm) in [("forward", left.as_ref()), ("reversed", right.as_ref())] {
            assert!(
                matches!(
                    arm,
                    ScalarExpr::Binary {
                        op: BinaryOp::Or,
                        ..
                    }
                ),
                "NOT SYMMETRIC {label} arm should be OR, got {arm:?}"
            );
        }
    }

    #[test]
    fn binds_between_rewrite_renders_as_full_tree() {
        // Lock down the exact textual form of the rewrite so it surfaces
        // unambiguously in EXPLAIN-style output.
        let plan = parse_bind_ok("SELECT id FROM users WHERE id BETWEEN 5 AND 10");
        let pred = predicate_of(&plan);
        assert_eq!(pred.to_string(), "((id >= 5) AND (id <= 10))");
    }

    #[test]
    fn binds_between_uses_existing_type_check_to_reject_incompatible_bounds() {
        // `name` is TEXT; bound by an integer literal is not comparable
        // — the binder must surface a TypeMismatch the same way it
        // would for the equivalent `name >= 1 AND name <= 10`.
        let cat = users_catalog();
        let err =
            parse_and_bind("SELECT id FROM users WHERE name BETWEEN 1 AND 10", &cat).unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch(_)), "got {err:?}");
    }

    proptest! {
        /// BETWEEN binds without error for every integer pair drawn from the
        /// supported i32 range — the rewrite never invents a type it does not
        /// already accept on plain comparisons.
        #[test]
        fn prop_between_int_pair_binds_ok(
            lo in -1_000_000_i32..=1_000_000_i32,
            hi in -1_000_000_i32..=1_000_000_i32,
        ) {
            let cat = users_catalog();
            let sql = format!("SELECT id FROM users WHERE id BETWEEN {lo} AND {hi}");
            let result = parse_and_bind(&sql, &cat);
            prop_assert!(result.is_ok(), "BETWEEN should bind, got {:?}", result);
        }

        /// NOT BETWEEN binds without error for every integer pair drawn from
        /// the supported i32 range.
        #[test]
        fn prop_not_between_int_pair_binds_ok(
            lo in -1_000_000_i32..=1_000_000_i32,
            hi in -1_000_000_i32..=1_000_000_i32,
        ) {
            let cat = users_catalog();
            let sql = format!("SELECT id FROM users WHERE id NOT BETWEEN {lo} AND {hi}");
            let result = parse_and_bind(&sql, &cat);
            prop_assert!(result.is_ok(), "NOT BETWEEN should bind, got {:?}", result);
        }
    }

    proptest! {
        /// Any random join tree over a fixed set of 3 tables binds without error.
        #[test]
        fn prop_join_tree_over_three_tables_binds_ok(
            // Choose join type index 0..4 for left join and right join.
            lj_type in 0_usize..2_usize,
            rj_type in 0_usize..2_usize,
        ) {
            // Catalog: a, b, c each with one column.
            let mut cat = InMemoryCatalog::new();
            let s = Schema::new([Field::required("x", DataType::Int32)]).expect("schema ok");
            cat.register("ta", TableMeta::new(s));
            let sb = Schema::new([Field::required("y", DataType::Int32)]).expect("schema ok");
            cat.register("tb", TableMeta::new(sb));
            let sc = Schema::new([Field::required("z", DataType::Int32)]).expect("schema ok");
            cat.register("tc", TableMeta::new(sc));

            let join_kw = ["INNER JOIN", "CROSS JOIN"];
            let lj = join_kw[lj_type % join_kw.len()];
            let rj = join_kw[rj_type % join_kw.len()];
            let on_lj = if lj == "CROSS JOIN" { "" } else { " ON ta.x = tb.y" };
            let on_rj = if rj == "CROSS JOIN" { "" } else { " ON ta.x = tc.z" };
            let sql = format!(
                "SELECT ta.x FROM ta {lj} tb{on_lj} {rj} tc{on_rj}"
            );
            let result = parse_and_bind(&sql, &cat);
            prop_assert!(result.is_ok(), "join tree should bind ok, got {:?}", result);
        }
    }

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

    // -----------------------------------------------------------------------
    // CREATE INDEX / DROP TABLE / ALTER TABLE — DDL binder tests
    // -----------------------------------------------------------------------

    #[test]
    fn binds_create_index_resolves_column_index_and_synthesises_name() {
        let plan = parse_bind_ok("CREATE INDEX ON users (id)");
        let LogicalPlan::CreateIndex {
            index_name,
            table_name,
            columns,
            unique,
            if_not_exists,
            ..
        } = plan
        else {
            panic!("expected CreateIndex plan");
        };
        assert_eq!(table_name, "users");
        assert_eq!(columns, vec![0]);
        assert!(!unique);
        assert!(!if_not_exists);
        // Synthesised name uses the {table}_{cols}_idx convention.
        assert_eq!(index_name, "users_id_idx");
    }

    #[test]
    fn binds_create_unique_index_honours_unique_flag_and_explicit_name() {
        let plan = parse_bind_ok("CREATE UNIQUE INDEX IF NOT EXISTS users_pk ON users (id)");
        let LogicalPlan::CreateIndex {
            index_name,
            unique,
            if_not_exists,
            ..
        } = plan
        else {
            panic!("expected CreateIndex plan");
        };
        assert!(unique);
        assert!(if_not_exists);
        assert_eq!(index_name, "users_pk");
    }

    #[test]
    fn create_index_rejects_unknown_column() {
        let cat = users_catalog();
        let err =
            parse_and_bind("CREATE INDEX bad_idx ON users (does_not_exist)", &cat).unwrap_err();
        assert!(matches!(err, PlanError::ColumnNotFound(_)), "got {err:?}");
    }

    #[test]
    fn create_index_rejects_unknown_table() {
        let cat = users_catalog();
        let err = parse_and_bind("CREATE INDEX bad_idx ON nonexistent (id)", &cat).unwrap_err();
        assert!(matches!(err, PlanError::TableNotFound(_)), "got {err:?}");
    }

    #[test]
    fn binds_drop_table_with_known_relation() {
        let plan = parse_bind_ok("DROP TABLE users");
        let LogicalPlan::DropTable {
            tables, if_exists, ..
        } = plan
        else {
            panic!("expected DropTable plan");
        };
        assert_eq!(tables, vec!["users".to_string()]);
        assert!(!if_exists);
    }

    #[test]
    fn drop_table_if_exists_silently_omits_missing_relations() {
        let plan = parse_bind_ok("DROP TABLE IF EXISTS users, nope");
        let LogicalPlan::DropTable {
            tables, if_exists, ..
        } = plan
        else {
            panic!("expected DropTable plan");
        };
        assert!(if_exists);
        // `nope` is silently filtered; `users` remains.
        assert_eq!(tables, vec!["users".to_string()]);
    }

    #[test]
    fn drop_table_without_if_exists_rejects_missing_relation() {
        let cat = users_catalog();
        let err = parse_and_bind("DROP TABLE nonexistent", &cat).unwrap_err();
        assert!(matches!(err, PlanError::TableNotFound(_)), "got {err:?}");
    }

    #[test]
    fn binds_alter_table_add_column_resolves_field() {
        let plan = parse_bind_ok("ALTER TABLE users ADD COLUMN extra INTEGER");
        let LogicalPlan::AlterTable {
            table_name, action, ..
        } = plan
        else {
            panic!("expected AlterTable plan");
        };
        assert_eq!(table_name, "users");
        let LogicalAlterTableAction::AddColumn { column } = action;
        assert_eq!(column.name, "extra");
        assert_eq!(column.data_type, DataType::Int32);
        assert!(column.nullable, "ADD COLUMN defaults to nullable");
    }

    #[test]
    fn binds_alter_table_add_column_not_null() {
        let plan = parse_bind_ok("ALTER TABLE users ADD COLUMN flag BOOLEAN NOT NULL");
        let LogicalPlan::AlterTable { action, .. } = plan else {
            panic!("expected AlterTable plan");
        };
        let LogicalAlterTableAction::AddColumn { column } = action;
        assert_eq!(column.data_type, DataType::Bool);
        assert!(!column.nullable);
    }

    #[test]
    fn alter_table_add_column_rejects_duplicate_name() {
        let cat = users_catalog();
        let err = parse_and_bind("ALTER TABLE users ADD COLUMN id INTEGER", &cat).unwrap_err();
        assert!(
            matches!(err, PlanError::DuplicateColumn(ref c) if c == "id"),
            "got {err:?}"
        );
    }

    #[test]
    fn alter_table_drop_column_returns_not_supported() {
        let cat = users_catalog();
        let err = parse_and_bind("ALTER TABLE users DROP COLUMN score", &cat).unwrap_err();
        assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
    }

    #[test]
    fn alter_table_rename_returns_not_supported() {
        let cat = users_catalog();
        let err = parse_and_bind("ALTER TABLE users RENAME TO subscribers", &cat).unwrap_err();
        assert!(matches!(err, PlanError::NotSupported(_)), "got {err:?}");
    }
}
