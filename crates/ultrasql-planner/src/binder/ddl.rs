//! DDL binders. Split out of `binder/mod.rs` to keep each
//! production source file under the 600-line ceiling.
//!
//! Public entry points are `pub(super)` so the dispatch in
//! `binder::bind` can route to them; internal helpers
//! (`object_name_namespace`, `resolve_type_name`,
//! `resolve_column_nullability`, `synthesise_index_name`) stay
//! private to this module.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_parser::ast::{
    AlterTableAction, AlterTableStmt, ColumnConstraint, CreateIndexStmt, CreateTableStmt,
    DropTableStmt, Expr, ObjectName, TruncateStmt, TypeName,
};

use super::{Catalog, LogicalAlterTableAction, LogicalPlan, PlanError, object_name_simple};

pub(super) fn bind_create_table(s: &CreateTableStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
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
pub(super) fn bind_create_index(s: &CreateIndexStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
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
pub(super) fn bind_drop_table(s: &DropTableStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
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
pub(super) fn bind_alter_table(s: &AlterTableStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
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
pub(super) fn bind_truncate(s: &TruncateStmt, catalog: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
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
