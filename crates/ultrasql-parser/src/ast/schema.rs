//! Schema-object DDL AST nodes.
//!
//! Column and table constraints, parsed type names, and the
//! `CREATE`/`ALTER`/`DROP` statements for schemas, indexes, sequences,
//! plus `SET`/`SHOW`/`RESET`, `REINDEX`, and `COMMENT ON`.

use crate::ast::{BinaryOp, Expr, Identifier, NullsOrder, ObjectName, SelectStmt, SortDirection};
use crate::span::Span;

/// One column definition inside `CREATE TABLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    /// Column name.
    pub name: Identifier,
    /// Declared SQL type.
    pub data_type: TypeName,
    /// Optional column collation from `COLLATE name`.
    pub collation: Option<ObjectName>,
    /// Column-level constraints.
    pub constraints: Vec<ColumnConstraint>,
    /// Source span.
    pub span: Span,
}

/// Column-level constraint inside a `CREATE TABLE` column definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnConstraint {
    /// `NOT NULL`.
    NotNull {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `NULL` (explicit nullable).
    Null {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `DEFAULT expr`.
    Default {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Default value expression.
        expr: Expr,
        /// Source span.
        span: Span,
    },
    /// `PRIMARY KEY`.
    PrimaryKey {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `UNIQUE`.
    Unique {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `CHECK (expr)`.
    Check {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Constraint expression.
        expr: Expr,
        /// Source span.
        span: Span,
    },
    /// `REFERENCES target_table [(target_columns)]`.
    References {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Referenced table.
        target_table: ObjectName,
        /// Referenced columns (may be empty if targeting the primary key).
        target_columns: Vec<Identifier>,
        /// Action when a referenced row is deleted.
        on_delete: ReferentialAction,
        /// Action when a referenced key is updated.
        on_update: ReferentialAction,
        /// Whether this constraint is deferrable.
        deferrable: bool,
        /// Whether this deferrable constraint starts deferred.
        initially_deferred: bool,
        /// Source span.
        span: Span,
    },
    /// `GENERATED ALWAYS | BY DEFAULT AS IDENTITY [(sequence_options)]`.
    GeneratedIdentity {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// `true` for `ALWAYS`; `false` for `BY DEFAULT`.
        always: bool,
        /// Sequence options inside the optional identity option list.
        options: Vec<SequenceOption>,
        /// Source span.
        span: Span,
    },
    /// `GENERATED ALWAYS AS (expr) STORED`.
    GeneratedStored {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Stored generated expression.
        expr: Expr,
        /// Source span.
        span: Span,
    },
}

/// Referential action attached to a foreign key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReferentialAction {
    /// `NO ACTION`.
    NoAction,
    /// `RESTRICT`.
    Restrict,
    /// `CASCADE`.
    Cascade,
    /// `SET NULL`.
    SetNull,
    /// `SET DEFAULT`.
    SetDefault,
}

/// Table-level constraint inside `CREATE TABLE` or `ALTER TABLE ADD CONSTRAINT`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableConstraint {
    /// `PRIMARY KEY (col, …)`.
    PrimaryKey {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        /// Preserved so `ALTER TABLE … DROP CONSTRAINT name` can identify the
        /// constraint by name.
        name: Option<Identifier>,
        /// Key columns.
        columns: Vec<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `UNIQUE (col, …)`.
    Unique {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Unique columns.
        columns: Vec<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `FOREIGN KEY (col, …) REFERENCES target_table [(target_col, …)]`.
    ForeignKey {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Local columns.
        columns: Vec<Identifier>,
        /// Referenced table.
        target_table: ObjectName,
        /// Referenced columns (may be empty).
        target_columns: Vec<Identifier>,
        /// Action when a referenced row is deleted.
        on_delete: ReferentialAction,
        /// Action when a referenced key is updated.
        on_update: ReferentialAction,
        /// Whether this constraint is deferrable.
        deferrable: bool,
        /// Whether this deferrable constraint starts deferred.
        initially_deferred: bool,
        /// Source span.
        span: Span,
    },
    /// `CHECK (expr)`.
    Check {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Constraint expression.
        expr: Expr,
        /// Source span.
        span: Span,
    },
    /// `EXCLUDE USING method (col WITH op, ...)`.
    Exclude {
        /// Optional `CONSTRAINT name` label. `None` when no name was given.
        name: Option<Identifier>,
        /// Access method name, normally `gist`.
        method: Identifier,
        /// Exclusion elements.
        elements: Vec<ExclusionElement>,
        /// Source span.
        span: Span,
    },
}

/// One `column WITH operator` element inside an exclusion constraint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExclusionElement {
    /// Column participating in exclusion.
    pub column: Identifier,
    /// Operator used to compare this column against existing rows.
    pub op: BinaryOp,
    /// Source span.
    pub span: Span,
}

/// Parsed SQL type name, including optional type modifiers and array suffixes.
///
/// Mirrors the CAST-target structure but is richer: it carries numeric
/// modifiers (e.g. `VARCHAR(255)` -> `[255]`) and array dimension count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeName {
    /// Canonical lower-case type name.
    pub name: Identifier,
    /// Type modifiers: `VARCHAR(255)` → `[255]`, `NUMERIC(10,2)` → `[10, 2]`.
    pub type_modifiers: Vec<u32>,
    /// Whether the type has at least one trailing `[]` suffix.
    pub is_array: bool,
    /// Number of trailing `[]` suffixes.
    pub array_dimensions: u32,
    /// Source span.
    pub span: Span,
}

/// `DROP TABLE [IF EXISTS] name [, …] [CASCADE|RESTRICT]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTableStmt {
    /// Whether `IF EXISTS` was specified.
    pub if_exists: bool,
    /// Tables to drop (one or more).
    pub names: Vec<ObjectName>,
    /// Whether `CASCADE` was specified (vs. `RESTRICT` or omitted).
    pub cascade: bool,
    /// Source span.
    pub span: Span,
}

/// `ALTER TABLE name action`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterTableStmt {
    /// Table to alter.
    pub name: ObjectName,
    /// The single action to perform.
    pub action: AlterTableAction,
    /// Source span.
    pub span: Span,
}

/// `ALTER VIEW name action`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterViewStmt {
    /// View to alter.
    pub name: ObjectName,
    /// The single action to perform.
    pub action: AlterViewAction,
    /// Source span.
    pub span: Span,
}

/// One action clause of an `ALTER VIEW` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterViewAction {
    /// `RENAME TO new_name`.
    RenameView {
        /// New view name.
        new_name: Identifier,
        /// Source span.
        span: Span,
    },
    /// `SET SCHEMA schema_name`.
    SetSchema {
        /// Target schema name.
        schema_name: Identifier,
        /// Source span.
        span: Span,
    },
    /// `AS SELECT ...`.
    ReplaceDefinition {
        /// Replacement SELECT query.
        source: Box<SelectStmt>,
        /// Source SQL text for the SELECT definition, trimmed.
        source_sql: String,
        /// Source span.
        span: Span,
    },
}

/// One action clause of an `ALTER TABLE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterTableAction {
    /// `ADD [COLUMN] col type [constraints]`.
    AddColumn {
        /// Column definition to add.
        column: ColumnDef,
        /// Source span.
        span: Span,
    },
    /// `DROP [COLUMN] col [CASCADE|RESTRICT]`.
    DropColumn {
        /// Column to drop.
        name: Identifier,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Source span.
        span: Span,
    },
    /// `RENAME COLUMN old TO new`.
    RenameColumn {
        /// Old column name.
        old: Identifier,
        /// New column name.
        new: Identifier,
        /// Source span.
        span: Span,
    },
    /// `RENAME TO new_name`.
    RenameTable {
        /// New table name.
        new_name: Identifier,
        /// Source span.
        span: Span,
    },
    /// `ADD CONSTRAINT name constraint`.
    AddConstraint {
        /// Constraint definition.
        constraint: TableConstraint,
        /// Source span.
        span: Span,
    },
    /// `DROP CONSTRAINT name [CASCADE|RESTRICT]`.
    DropConstraint {
        /// Constraint name.
        name: Identifier,
        /// Whether `CASCADE` was specified.
        cascade: bool,
        /// Source span.
        span: Span,
    },
    /// `ENABLE ROW LEVEL SECURITY`.
    EnableRowLevelSecurity {
        /// Source span.
        span: Span,
    },
    /// `SET (option = value, ...)`.
    SetOptions {
        /// Relation storage options.
        options: Vec<IndexOption>,
        /// Source span.
        span: Span,
    },
}

/// `CREATE SCHEMA [IF NOT EXISTS] name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSchemaStmt {
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Schema name.
    pub name: Identifier,
    /// Source span.
    pub span: Span,
}

/// `DROP SCHEMA [IF EXISTS] name [, …] [CASCADE|RESTRICT]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSchemaStmt {
    /// Whether `IF EXISTS` was specified.
    pub if_exists: bool,
    /// Schema names to drop.
    pub names: Vec<Identifier>,
    /// Whether `CASCADE` was specified.
    pub cascade: bool,
    /// Source span.
    pub span: Span,
}

/// `SET [VARIABLE|SESSION|LOCAL] var = val` / `SHOW var` / `RESET var`.
///
/// A single statement covering all GUC (Grand Unified Configuration)
/// manipulation forms supported by PostgreSQL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetVarStmt {
    /// Scope modifier and action type.
    pub scope: SetScope,
    /// Variable name (e.g. `search_path`, `statement_timeout`).
    pub name: Identifier,
    /// Value to assign.
    pub value: SetValue,
    /// Source span.
    pub span: Span,
}

/// Scope or action type for a `SET`/`SHOW`/`RESET` statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetScope {
    /// `SET [SESSION] var = val` — session-level setting (default).
    Session,
    /// `SET LOCAL var = val` — transaction-local setting.
    Local,
    /// `SHOW var` — display current value.
    Show,
    /// `RESET var` — restore to default.
    Reset,
}

/// Value expression(s) for a `SET` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetValue {
    /// `DEFAULT` — restore to default.
    Default,
    /// One or more expressions: `SET search_path TO schema, public`.
    Values(Vec<Expr>),
}

/// `SET ROLE role` / `SET ROLE NONE` / `RESET ROLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetRoleStmt {
    /// Target role. `None` means reset to the session user.
    pub role: Option<Identifier>,
    /// Source span.
    pub span: Span,
}

/// `CREATE [UNIQUE|AGGREGATING] INDEX [IF NOT EXISTS] [name] ON table [USING method] (columns) [INCLUDE (...)] [WITH (...)] [WHERE expr]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateIndexStmt {
    /// Whether `UNIQUE` was specified.
    pub unique: bool,
    /// Whether `AGGREGATING` was specified.
    pub aggregating: bool,
    /// Whether `CONCURRENTLY` was specified.
    pub concurrently: bool,
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Optional explicit index name.
    pub name: Option<Identifier>,
    /// Table to index.
    pub table: ObjectName,
    /// Index method (`btree`, `hash`, `gin`, `gist`, …).
    pub method: Option<Identifier>,
    /// Index key columns / expressions.
    pub columns: Vec<IndexColumn>,
    /// Optional partial-index predicate.
    pub r#where: Option<Expr>,
    /// `INCLUDE (col, …)` covering columns.
    pub include: Vec<Identifier>,
    /// `WITH (name = value, …)` index storage options.
    pub options: Vec<IndexOption>,
    /// Source span.
    pub span: Span,
}

/// One `WITH (…)` storage option of a `CREATE INDEX` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexOption {
    /// Option name.
    pub name: Identifier,
    /// Option value expression.
    pub value: Expr,
    /// Source span.
    pub span: Span,
}

/// One entry in the `CREATE INDEX` column list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexColumn {
    /// Key expression (commonly a bare column reference).
    pub expr: Expr,
    /// Optional operator class name (`vector_l2_ops`, `text_pattern_ops`, …).
    pub opclass: Option<Identifier>,
    /// Sort direction.
    pub direction: SortDirection,
    /// Null ordering.
    pub nulls: NullsOrder,
    /// Source span.
    pub span: Span,
}

/// `DROP INDEX [IF EXISTS] name [, …] [CASCADE|RESTRICT]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropIndexStmt {
    /// Whether `IF EXISTS` was specified.
    pub if_exists: bool,
    /// Index names to drop.
    pub names: Vec<ObjectName>,
    /// Whether `CASCADE` was specified.
    pub cascade: bool,
    /// Source span.
    pub span: Span,
}

/// `REINDEX { INDEX | TABLE } name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReindexStmt {
    /// Whether the target is an index or a table.
    pub kind: ReindexKind,
    /// Target object name.
    pub name: ObjectName,
    /// Source span.
    pub span: Span,
}

/// Target kind for a `REINDEX` statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReindexKind {
    /// `REINDEX INDEX name`.
    Index,
    /// `REINDEX TABLE name`.
    Table,
}

/// `CREATE SEQUENCE [IF NOT EXISTS] name [options]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSequenceStmt {
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Sequence name.
    pub name: ObjectName,
    /// Sequence options.
    pub options: Vec<SequenceOption>,
    /// Source span.
    pub span: Span,
}

/// `ALTER SEQUENCE name [options]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterSequenceStmt {
    /// Sequence name.
    pub name: ObjectName,
    /// Options to change.
    pub options: Vec<SequenceOption>,
    /// Source span.
    pub span: Span,
}

/// `DROP SEQUENCE [IF EXISTS] name [, …] [CASCADE|RESTRICT]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSequenceStmt {
    /// Whether `IF EXISTS` was specified.
    pub if_exists: bool,
    /// Sequence names to drop.
    pub names: Vec<ObjectName>,
    /// Whether `CASCADE` was specified.
    pub cascade: bool,
    /// Source span.
    pub span: Span,
}

/// `COMMENT ON ... IS ...`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommentStmt {
    /// Commented object.
    pub target: CommentTarget,
    /// Comment body. `None` represents `IS NULL`, which removes a comment.
    pub comment: Option<String>,
    /// Source span.
    pub span: Span,
}

/// Object kind accepted by `COMMENT ON`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommentTarget {
    /// `COMMENT ON TABLE rel IS ...`.
    Table(ObjectName),
    /// `COMMENT ON INDEX idx IS ...`.
    Index(ObjectName),
    /// `COMMENT ON COLUMN rel.col IS ...`.
    Column(ObjectName),
}

/// One option clause in `CREATE SEQUENCE` or `ALTER SEQUENCE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequenceOption {
    /// `START [WITH] n`.
    Start(i64),
    /// `RESTART [[WITH] n]`.
    Restart(Option<i64>),
    /// `INCREMENT [BY] n`.
    Increment(i64),
    /// `MINVALUE n` or `NO MINVALUE`.
    MinValue(Option<i64>),
    /// `MAXVALUE n` or `NO MAXVALUE`.
    MaxValue(Option<i64>),
    /// `CACHE n`.
    Cache(u64),
    /// `CYCLE` or `NO CYCLE`.
    Cycle(bool),
}
