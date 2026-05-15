//! SQL abstract syntax tree.
//!
//! AST nodes are owned (no lifetime parameters tying them to the source
//! string). They carry [`Span`]s so error messages can quote source
//! exactly. Identifiers preserve their original quoting state so a
//! later printer can round-trip a parsed statement.

use std::fmt;

use crate::span::Span;

/// Top-level SQL statement.
///
/// `SelectStmt` and the DML statement types are comparatively large, so
/// they live behind a `Box` to keep the enum's stack size small. Pattern
/// matching looks the same to callers.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Statement {
    /// `SELECT ...`.
    Select(Box<SelectStmt>),
    /// `INSERT INTO ...`.
    Insert(Box<InsertStmt>),
    /// `UPDATE ...`.
    Update(Box<UpdateStmt>),
    /// `DELETE FROM ...`.
    Delete(Box<DeleteStmt>),
    /// `TRUNCATE TABLE ...`.
    Truncate(TruncateStmt),
    /// `BEGIN [TRANSACTION]`.
    Begin {
        /// Source span.
        span: Span,
    },
    /// `COMMIT`.
    Commit {
        /// Source span.
        span: Span,
    },
    /// `ROLLBACK`.
    Rollback {
        /// Source span.
        span: Span,
    },
    /// `CREATE TABLE …`.
    CreateTable(Box<CreateTableStmt>),
    /// `CREATE TABLE … AS SELECT …`.
    CreateTableAs(Box<CreateTableAsStmt>),
    /// `DROP TABLE …`.
    DropTable(DropTableStmt),
    /// `ALTER TABLE …`.
    AlterTable(Box<AlterTableStmt>),
    /// `CREATE SCHEMA …`.
    CreateSchema(CreateSchemaStmt),
    /// `DROP SCHEMA …`.
    DropSchema(DropSchemaStmt),
    /// `SET [SESSION|LOCAL] var = val` / `SHOW var` / `RESET var`.
    SetVar(SetVarStmt),
    /// `CREATE [UNIQUE] INDEX …`.
    CreateIndex(Box<CreateIndexStmt>),
    /// `DROP INDEX …`.
    DropIndex(DropIndexStmt),
    /// `REINDEX TABLE/INDEX …`.
    Reindex(ReindexStmt),
    /// `CREATE SEQUENCE …`.
    CreateSequence(Box<CreateSequenceStmt>),
    /// `ALTER SEQUENCE …`.
    AlterSequence(Box<AlterSequenceStmt>),
    /// `DROP SEQUENCE …`.
    DropSequence(DropSequenceStmt),
    /// `SAVEPOINT name`.
    Savepoint(SavepointStmt),
    /// `ROLLBACK TO [SAVEPOINT] name`.
    RollbackToSavepoint(RollbackToSavepointStmt),
    /// `RELEASE [SAVEPOINT] name`.
    ReleaseSavepoint(ReleaseSavepointStmt),
    /// `EXPLAIN [ANALYZE] [VERBOSE] [(FORMAT …)] stmt`.
    Explain(Box<ExplainStmt>),
    /// `PREPARE name [(types)] AS stmt`.
    Prepare(Box<PrepareStmt>),
    /// `EXECUTE name [(args)]`.
    Execute(ExecuteStmt),
    /// `DEALLOCATE [ALL | name]`.
    Deallocate(DeallocateStmt),
    /// `PREPARE TRANSACTION 'gid'` — phase 1 of two-phase commit.
    PrepareTransaction {
        /// Global transaction identifier supplied by the coordinator.
        gid: String,
        /// Source span.
        span: Span,
    },
    /// `COMMIT PREPARED 'gid'` — phase 2 commit of a prepared txn.
    CommitPrepared {
        /// Global transaction identifier to commit.
        gid: String,
        /// Source span.
        span: Span,
    },
    /// `ROLLBACK PREPARED 'gid'` — phase 2 abort of a prepared txn.
    RollbackPrepared {
        /// Global transaction identifier to roll back.
        gid: String,
        /// Source span.
        span: Span,
    },
}

impl Statement {
    /// Source span enclosing this statement.
    #[must_use]
    pub fn span(&self) -> Span {
        match self {
            Self::Select(s) => s.span,
            Self::Insert(s) => s.span,
            Self::Update(s) => s.span,
            Self::Delete(s) => s.span,
            Self::Truncate(s) => s.span,
            Self::Begin { span } | Self::Commit { span } | Self::Rollback { span } => *span,
            Self::CreateTable(s) => s.span,
            Self::CreateTableAs(s) => s.span,
            Self::DropTable(s) => s.span,
            Self::AlterTable(s) => s.span,
            Self::CreateSchema(s) => s.span,
            Self::DropSchema(s) => s.span,
            Self::SetVar(s) => s.span,
            Self::CreateIndex(s) => s.span,
            Self::DropIndex(s) => s.span,
            Self::Reindex(s) => s.span,
            Self::CreateSequence(s) => s.span,
            Self::AlterSequence(s) => s.span,
            Self::DropSequence(s) => s.span,
            Self::Savepoint(s) => s.span,
            Self::RollbackToSavepoint(s) => s.span,
            Self::ReleaseSavepoint(s) => s.span,
            Self::Explain(s) => s.span,
            Self::Prepare(s) => s.span,
            Self::Execute(s) => s.span,
            Self::Deallocate(s) => s.span,
            Self::PrepareTransaction { span, .. }
            | Self::CommitPrepared { span, .. }
            | Self::RollbackPrepared { span, .. } => *span,
        }
    }
}

// ============================================================================
// DDL AST nodes
// ============================================================================

/// `CREATE TABLE t (col type [constraints], …)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableStmt {
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Table name (possibly schema-qualified).
    pub name: ObjectName,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
    /// Table-level constraints.
    pub table_constraints: Vec<TableConstraint>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// `CREATE TABLE t AS SELECT …`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableAsStmt {
    /// Whether `IF NOT EXISTS` was specified.
    pub if_not_exists: bool,
    /// Table name (possibly schema-qualified).
    pub name: ObjectName,
    /// Optional explicit column-name list `(col1, col2, …)`.
    pub columns: Vec<Identifier>,
    /// The SELECT statement that provides rows.
    pub source: Box<SelectStmt>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// One column definition inside `CREATE TABLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    /// Column name.
    pub name: Identifier,
    /// Declared SQL type.
    pub data_type: TypeName,
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
        /// Source span.
        span: Span,
    },
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
}

/// Parsed SQL type name, including optional type modifiers and array suffix.
///
/// Mirrors the CAST-target structure but is richer: it carries numeric
/// modifiers (e.g. `VARCHAR(255)` → `[255]`) and an array flag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeName {
    /// Canonical lower-case type name.
    pub name: Identifier,
    /// Type modifiers: `VARCHAR(255)` → `[255]`, `NUMERIC(10,2)` → `[10, 2]`.
    pub type_modifiers: Vec<u32>,
    /// Whether the type has a trailing `[]` (array type).
    pub is_array: bool,
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

/// `SET [SESSION|LOCAL] var = val` / `SHOW var` / `RESET var`.
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

/// `CREATE [UNIQUE] INDEX [IF NOT EXISTS] [name] ON table [USING method] (columns) [INCLUDE (...)] [WHERE expr]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateIndexStmt {
    /// Whether `UNIQUE` was specified.
    pub unique: bool,
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
    /// Source span.
    pub span: Span,
}

/// One entry in the `CREATE INDEX` column list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexColumn {
    /// Key expression (commonly a bare column reference).
    pub expr: Expr,
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

/// One option clause in `CREATE SEQUENCE` or `ALTER SEQUENCE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequenceOption {
    /// `START [WITH] n`.
    Start(i64),
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

/// An `INSERT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertStmt {
    /// Target table.
    pub table: ObjectName,
    /// Explicit column list (empty means all columns, positional).
    pub columns: Vec<Identifier>,
    /// Source of rows to insert.
    pub source: InsertSource,
    /// Optional `ON CONFLICT` clause.
    pub on_conflict: Option<OnConflict>,
    /// Optional `RETURNING` projection list (empty = no RETURNING).
    pub returning: Vec<SelectItem>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// The source of rows in an `INSERT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InsertSource {
    /// `VALUES (a, b), (c, d), ...` — one `Vec<Expr>` per row.
    Values(Vec<Vec<Expr>>),
    /// `INSERT ... SELECT ...`.
    Select(Box<SelectStmt>),
    /// `INSERT ... DEFAULT VALUES`.
    DefaultValues,
}

/// `ON CONFLICT` clause of an `INSERT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnConflict {
    /// `ON CONFLICT [target] DO NOTHING`.
    DoNothing {
        /// Optional conflict target (columns).
        target: Option<ConflictTarget>,
        /// Source span.
        span: Span,
    },
    /// `ON CONFLICT target DO UPDATE SET ...`.
    DoUpdate {
        /// Conflict target (columns).
        target: ConflictTarget,
        /// `SET` assignments.
        set: Vec<Assignment>,
        /// Optional `WHERE` filter on the update.
        r#where: Option<Expr>,
        /// Source span.
        span: Span,
    },
}

/// Conflict target in an `ON CONFLICT` clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictTarget {
    /// The indexed columns whose uniqueness constraint was violated.
    pub columns: Vec<Identifier>,
    /// Source span.
    pub span: Span,
}

/// A `col = expr` assignment used in `UPDATE … SET` and
/// `ON CONFLICT … DO UPDATE SET`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    /// Target column name.
    pub target: Identifier,
    /// New value expression.
    pub value: Expr,
    /// Source span.
    pub span: Span,
}

/// An `UPDATE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateStmt {
    /// Target table.
    pub table: ObjectName,
    /// Optional alias for the target table.
    pub alias: Option<Identifier>,
    /// `SET` assignments (must be non-empty).
    pub set: Vec<Assignment>,
    /// Optional `FROM` clause (additional table references).
    pub from: Vec<TableRef>,
    /// Optional `WHERE` predicate.
    pub r#where: Option<Expr>,
    /// Optional `RETURNING` projection list (empty = no RETURNING).
    pub returning: Vec<SelectItem>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// A `DELETE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteStmt {
    /// Target table.
    pub table: ObjectName,
    /// Optional alias for the target table.
    pub alias: Option<Identifier>,
    /// Optional `USING` clause (additional table references).
    pub using: Vec<TableRef>,
    /// Optional `WHERE` predicate.
    pub r#where: Option<Expr>,
    /// Optional `RETURNING` projection list (empty = no RETURNING).
    pub returning: Vec<SelectItem>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// A `TRUNCATE TABLE` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TruncateStmt {
    /// Tables to truncate (one or more).
    pub tables: Vec<ObjectName>,
    /// Whether `RESTART IDENTITY` was specified.
    pub restart_identity: bool,
    /// Whether `CASCADE` was specified.
    pub cascade: bool,
    /// Source span of the entire statement.
    pub span: Span,
}

// ============================================================================
// SELECT-level AST — extended for v0.2
// ============================================================================

/// A `SELECT` statement, including optional WITH clause (CTEs) and set
/// operations (UNION / INTERSECT / EXCEPT).
///
/// # Breaking change (v0.2)
/// `from` is now `Vec<TableRef>` (was `Option<TableRef>`). The join tree is
/// encoded as a left-deep chain of [`TableRef::Join`] nodes; comma-separated
/// tables are canonicalised to `JoinOp::Cross`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectStmt {
    /// DISTINCT / DISTINCT ON / ALL / no keyword.
    pub distinct: Distinct,
    /// Output items.
    pub projection: Vec<SelectItem>,
    /// `FROM` clause. Zero entries means no `FROM` (e.g. `SELECT 1`).
    /// Multiple entries represent a left-deep join tree where comma-joins
    /// have been canonicalised to `JoinOp::Cross`.
    pub from: Vec<TableRef>,
    /// `WHERE` predicate.
    pub r#where: Option<Expr>,
    /// `GROUP BY` expressions.
    pub group_by: Vec<Expr>,
    /// `HAVING` predicate (only meaningful when `group_by` is non-empty).
    pub having: Option<Expr>,
    /// `ORDER BY` items.
    pub order_by: Vec<OrderItem>,
    /// `LIMIT n`.
    pub limit: Option<Expr>,
    /// `OFFSET n`.
    pub offset: Option<Expr>,
    /// Chained UNION / INTERSECT / EXCEPT tails. In PostgreSQL, set ops bind
    /// less tightly than ORDER BY / LIMIT. For v0.2 we represent the chain
    /// here and leave precedence enforcement to the binder.
    pub set_ops: Vec<SetOpTail>,
    /// Leading `WITH [RECURSIVE]` CTEs.
    pub ctes: Vec<Cte>,
    /// `FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` / `FOR KEY SHARE`
    /// locking clauses. Multiple clauses are permitted by PostgreSQL when
    /// different `OF table` targets are given; we collect all of them.
    pub locking: Vec<LockingClause>,
    /// Source span.
    pub span: Span,
}

/// `FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` / `FOR KEY SHARE`.
///
/// Describes the row-level lock strength requested by a `SELECT` statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockStrength {
    /// `FOR UPDATE` — exclusive row lock; blocks concurrent writers.
    Update,
    /// `FOR NO KEY UPDATE` — like Update but does not block `KeyShare` locks.
    NoKeyUpdate,
    /// `FOR SHARE` — shared lock; blocks concurrent writes, not other reads.
    Share,
    /// `FOR KEY SHARE` — weakest; only blocks `FOR UPDATE`.
    KeyShare,
}

/// What to do when a requested lock is unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum LockWaitPolicy {
    /// Block until the lock can be acquired (default).
    #[default]
    Wait,
    /// Raise an error immediately if any row is locked.
    NoWait,
    /// Silently skip rows that are currently locked.
    SkipLocked,
}

/// One `FOR …` locking clause in a `SELECT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockingClause {
    /// Lock strength requested.
    pub strength: LockStrength,
    /// Wait policy when a lock cannot be acquired immediately.
    pub wait_policy: LockWaitPolicy,
    /// Optional `OF table [, …]` targets; empty means all relations in `FROM`.
    pub of_tables: Vec<ObjectName>,
}

/// DISTINCT / DISTINCT ON / ALL / (implicit none).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Distinct {
    /// No DISTINCT keyword (implicit ALL rows).
    None,
    /// `ALL` keyword was explicit.
    All,
    /// `DISTINCT` with no ON clause.
    Distinct,
    /// `DISTINCT ON (expr, …)`.
    DistinctOn(Vec<Expr>),
}

/// One UNION / INTERSECT / EXCEPT tail following a SELECT body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetOpTail {
    /// The set operation.
    pub op: SetOp,
    /// ALL or DISTINCT (default is DISTINCT per SQL standard).
    pub quantifier: SetQuantifier,
    /// Right-hand SELECT of the set operation.
    pub right: Box<SelectStmt>,
    /// Source span of this tail (from the keyword through to end of right).
    pub span: Span,
}

/// Set operation kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetOp {
    /// `UNION`.
    Union,
    /// `INTERSECT`.
    Intersect,
    /// `EXCEPT`.
    Except,
}

/// Set quantifier for a set operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetQuantifier {
    /// `DISTINCT` (default per SQL standard).
    Distinct,
    /// `ALL` — keep duplicates.
    All,
}

/// One entry in the `SELECT` projection list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectItem {
    /// `*`.
    Wildcard {
        /// Source span.
        span: Span,
    },
    /// `t.*` (qualified wildcard).
    QualifiedWildcard {
        /// Qualifier (table or alias name).
        qualifier: Identifier,
        /// Source span.
        span: Span,
    },
    /// `expr [AS alias]`.
    Expr {
        /// The expression.
        expr: Expr,
        /// Optional output alias.
        alias: Option<Identifier>,
        /// Source span.
        span: Span,
    },
}

/// A table reference in the `FROM` clause.
///
/// The v0.2 extension adds [`TableRef::Join`] (explicit join syntax and
/// comma-separated table lists canonicalised to CROSS JOIN) and
/// [`TableRef::Subquery`] (derived tables).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableRef {
    /// A bare table name with an optional alias.
    Named {
        /// Schema-qualified name (one or two parts).
        name: ObjectName,
        /// `AS alias` (or implicit alias).
        alias: Option<Identifier>,
        /// Source span.
        span: Span,
    },
    /// An explicit or implicit join between two table references.
    ///
    /// The parser builds a left-deep tree: the first table in the FROM
    /// clause is the leftmost leaf; each subsequent join wraps the
    /// current left side.
    Join {
        /// Left-hand table or sub-tree.
        left: Box<Self>,
        /// Join type.
        op: JoinOp,
        /// Right-hand table factor.
        right: Box<Self>,
        /// Join condition.
        condition: JoinCondition,
        /// Source span.
        span: Span,
    },
    /// A derived table (subquery in FROM).
    ///
    /// PostgreSQL requires an alias on every derived table. Parsing
    /// without an alias is a [`crate::parser::ParseError`].
    Subquery {
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// Required alias for the derived table.
        alias: Identifier,
        /// Optional column-alias list: `AS t(c1, c2, …)`.
        column_aliases: Vec<Identifier>,
        /// Source span.
        span: Span,
    },
}

/// Join operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinOp {
    /// `[INNER] JOIN`.
    Inner,
    /// `LEFT [OUTER] JOIN`.
    LeftOuter,
    /// `RIGHT [OUTER] JOIN`.
    RightOuter,
    /// `FULL [OUTER] JOIN`.
    FullOuter,
    /// `CROSS JOIN` or comma-separated table factor.
    Cross,
}

/// Join condition for an explicit join.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinCondition {
    /// `ON expr`.
    On(Expr),
    /// `USING (col, …)`.
    Using(Vec<Identifier>),
    /// No condition (CROSS JOIN).
    None,
}

/// Common table expression in a `WITH` clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cte {
    /// CTE name.
    pub name: Identifier,
    /// Optional column-alias list: `cte_name(c1, c2, …)`.
    pub column_aliases: Vec<Identifier>,
    /// Whether `WITH RECURSIVE` was specified.
    ///
    /// The flag is replicated on every CTE in the same WITH clause.
    pub recursive: bool,
    /// The body SELECT.
    pub query: Box<SelectStmt>,
    /// Source span.
    pub span: Span,
}

/// Order-by item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderItem {
    /// The sort expression.
    pub expr: Expr,
    /// Sort direction.
    pub direction: SortDirection,
    /// Null ordering.
    pub nulls: NullsOrder,
    /// Source span.
    pub span: Span,
}

/// `ASC` or `DESC`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending (default).
    Asc,
    /// Descending.
    Desc,
}

/// `NULLS FIRST` or `NULLS LAST`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullsOrder {
    /// Database default (PostgreSQL: `NULLS LAST` for `ASC`,
    /// `NULLS FIRST` for `DESC`).
    Default,
    /// `NULLS FIRST`.
    First,
    /// `NULLS LAST`.
    Last,
}

/// Multi-part object name (`db.schema.table`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectName {
    /// Components in left-to-right order.
    pub parts: Vec<Identifier>,
    /// Source span.
    pub span: Span,
}

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, p) in self.parts.iter().enumerate() {
            if i > 0 {
                f.write_str(".")?;
            }
            write!(f, "{}", p.value)?;
        }
        Ok(())
    }
}

/// A SQL identifier — name + quote flag + span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identifier {
    /// The identifier text. For unquoted identifiers this is the
    /// case-folded (lower-case) form; for quoted identifiers it
    /// preserves the source case.
    pub value: String,
    /// Whether the identifier was double-quoted in the source.
    pub quoted: bool,
    /// Source span.
    pub span: Span,
}

/// SQL expression.
///
/// The enum is `#[non_exhaustive]` so that downstream crates are not broken
/// when new expression kinds are added (e.g. window functions).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Expr {
    /// Literal value.
    Literal(Literal),
    /// Column reference, optionally qualified.
    Column {
        /// Name with up to three parts.
        name: ObjectName,
    },
    /// Positional parameter (`$N`).
    Parameter {
        /// 1-based parameter index.
        index: u32,
        /// Source span.
        span: Span,
    },
    /// Unary operator.
    Unary {
        /// Operator.
        op: UnaryOp,
        /// Operand.
        expr: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// Binary operator.
    Binary {
        /// Operator.
        op: BinaryOp,
        /// Left-hand operand.
        left: Box<Self>,
        /// Right-hand operand.
        right: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// Function call.
    Call {
        /// Function name (may be schema-qualified).
        name: ObjectName,
        /// Argument list.
        args: Vec<Self>,
        /// Whether the argument list was `DISTINCT`-prefixed.
        distinct: bool,
        /// Source span.
        span: Span,
    },
    /// `expr IS NULL` / `expr IS NOT NULL`.
    IsNull {
        /// Operand.
        expr: Box<Self>,
        /// `true` for `IS NOT NULL`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// Parenthesized expression.
    Paren {
        /// Inner expression.
        expr: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `CAST(expr AS type)`.
    Cast {
        /// Expression to cast.
        expr: Box<Self>,
        /// Target type identifier (parsed as an identifier; the binder
        /// turns it into a [`ultrasql_core::DataType`]).
        target: Identifier,
        /// Source span.
        span: Span,
    },
    /// Scalar subquery: `(SELECT …)`.
    ///
    /// The surrounding operator (`IN`, `EXISTS`, etc.) disambiguates the role.
    Subquery {
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// Source span.
        span: Span,
    },
    /// `EXISTS (SELECT …)` / `NOT EXISTS (SELECT …)`.
    Exists {
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// `true` for `NOT EXISTS`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `expr [NOT] IN (val, …)`.
    InList {
        /// The test expression.
        expr: Box<Self>,
        /// The list items (arbitrary expressions per PG).
        items: Vec<Self>,
        /// `true` for `NOT IN`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `expr [NOT] IN (SELECT …)`.
    InSubquery {
        /// The test expression.
        expr: Box<Self>,
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// `true` for `NOT IN`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `left_expr <op> ANY (SELECT …)`.
    ///
    /// The parser folds `lhs <op> ANY (sub)` into this node. Only
    /// comparison operators (`=`, `<>`, `<`, `<=`, `>`, `>=`) are valid
    /// with `ANY`.
    Any {
        /// Left-hand expression.
        expr: Box<Self>,
        /// Comparison operator.
        op: BinaryOp,
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// Source span.
        span: Span,
    },
    /// `left_expr <op> ALL (SELECT …)`.
    ///
    /// Like [`Expr::Any`] but with `ALL` semantics.
    All {
        /// Left-hand expression.
        expr: Box<Self>,
        /// Comparison operator.
        op: BinaryOp,
        /// The inner SELECT.
        select: Box<SelectStmt>,
        /// Source span.
        span: Span,
    },
    /// `CASE [operand] WHEN … THEN … [ELSE …] END`.
    ///
    /// When `operand` is `Some` this is a *simple* CASE that compares the
    /// operand against each WHEN value with `=`. When `operand` is `None`
    /// it is a *searched* CASE where each WHEN branch is a boolean condition.
    Case {
        /// Optional operand for a simple CASE (`CASE expr WHEN …`).
        /// `None` for a searched CASE (`CASE WHEN cond …`).
        operand: Option<Box<Self>>,
        /// `(when_expr, then_expr)` branch pairs (at least one).
        branches: Vec<(Self, Self)>,
        /// Optional ELSE expression.
        else_expr: Option<Box<Self>>,
        /// Source span.
        span: Span,
    },
    /// `COALESCE(a, b, …)` — returns the first non-NULL argument.
    Coalesce {
        /// Argument list (at least one per SQL standard).
        args: Vec<Self>,
        /// Source span.
        span: Span,
    },
    /// `NULLIF(a, b)` — returns NULL if `a = b`, otherwise `a`.
    NullIf {
        /// First argument.
        a: Box<Self>,
        /// Second argument.
        b: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `GREATEST(a, b, …)` — returns the largest non-NULL argument.
    Greatest {
        /// Argument list (at least one).
        args: Vec<Self>,
        /// Source span.
        span: Span,
    },
    /// `LEAST(a, b, …)` — returns the smallest non-NULL argument.
    Least {
        /// Argument list (at least one).
        args: Vec<Self>,
        /// Source span.
        span: Span,
    },
    /// `expr [NOT] BETWEEN [SYMMETRIC] low AND high`.
    Between {
        /// The expression being tested.
        expr: Box<Self>,
        /// Lower bound.
        low: Box<Self>,
        /// Upper bound.
        high: Box<Self>,
        /// `true` for `NOT BETWEEN`.
        negated: bool,
        /// `true` for `BETWEEN SYMMETRIC` (ordering of bounds is irrelevant).
        symmetric: bool,
        /// Source span.
        span: Span,
    },
    /// `expr IS [NOT] DISTINCT FROM other`.
    IsDistinctFrom {
        /// Left-hand expression.
        left: Box<Self>,
        /// Right-hand expression.
        right: Box<Self>,
        /// `true` for `IS NOT DISTINCT FROM`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `expr IS [NOT] TRUE / FALSE / UNKNOWN`.
    IsBoolean {
        /// The expression being tested.
        expr: Box<Self>,
        /// The boolean literal being compared against (`true` = TRUE,
        /// `false` = FALSE). Unused when `is_unknown` is `true`.
        value: bool,
        /// `true` when the test is against `UNKNOWN` rather than a boolean
        /// literal.
        is_unknown: bool,
        /// `true` for `IS NOT …`.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `expr::type_name` — PostgreSQL postfix cast syntax.
    PostfixCast {
        /// Expression being cast.
        expr: Box<Self>,
        /// Target type name.
        target: Identifier,
        /// Source span.
        span: Span,
    },
    /// `expr[index]` — single-element array subscript (1-based).
    ArraySubscript {
        /// Array expression.
        expr: Box<Self>,
        /// Index expression.
        index: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `expr[lower:upper]` — array slice.
    ///
    /// Either bound may be `None` (`arr[:3]`, `arr[2:]`, `arr[:]`).
    ArraySlice {
        /// Array expression.
        expr: Box<Self>,
        /// Optional lower bound (absent in `arr[:n]`).
        lower: Option<Box<Self>>,
        /// Optional upper bound (absent in `arr[n:]`).
        upper: Option<Box<Self>>,
        /// Source span.
        span: Span,
    },
    /// `expr AT TIME ZONE zone` — convert to/from a time zone.
    AtTimeZone {
        /// The timestamp or time expression.
        expr: Box<Self>,
        /// Time zone specifier (string literal or identifier).
        zone: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `(a, b) OVERLAPS (c, d)` — period overlap test.
    Overlaps {
        /// Start of the left period.
        left_start: Box<Self>,
        /// End of the left period.
        left_end: Box<Self>,
        /// Start of the right period.
        right_start: Box<Self>,
        /// End of the right period.
        right_end: Box<Self>,
        /// Source span.
        span: Span,
    },
    /// `ROW(a, b, …)` — explicit row constructor.
    Row {
        /// Field expressions.
        fields: Vec<Self>,
        /// Source span.
        span: Span,
    },
}

impl Expr {
    /// Source span.
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Literal(lit) => lit.span(),
            Self::Column { name } => name.span,
            Self::Parameter { span, .. }
            | Self::Unary { span, .. }
            | Self::Binary { span, .. }
            | Self::Call { span, .. }
            | Self::IsNull { span, .. }
            | Self::Paren { span, .. }
            | Self::Cast { span, .. }
            | Self::Subquery { span, .. }
            | Self::Exists { span, .. }
            | Self::InList { span, .. }
            | Self::InSubquery { span, .. }
            | Self::Any { span, .. }
            | Self::All { span, .. }
            | Self::Case { span, .. }
            | Self::Coalesce { span, .. }
            | Self::NullIf { span, .. }
            | Self::Greatest { span, .. }
            | Self::Least { span, .. }
            | Self::Between { span, .. }
            | Self::IsDistinctFrom { span, .. }
            | Self::IsBoolean { span, .. }
            | Self::PostfixCast { span, .. }
            | Self::ArraySubscript { span, .. }
            | Self::ArraySlice { span, .. }
            | Self::AtTimeZone { span, .. }
            | Self::Overlaps { span, .. }
            | Self::Row { span, .. } => *span,
        }
    }
}

/// Literal value.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Literal {
    /// `NULL`.
    Null {
        /// Source span.
        span: Span,
    },
    /// `TRUE` / `FALSE`.
    Bool {
        /// Value.
        value: bool,
        /// Source span.
        span: Span,
    },
    /// Integer literal (kept as a string until the binder picks a
    /// width).
    Integer {
        /// Original text.
        text: String,
        /// Source span.
        span: Span,
    },
    /// Floating-point literal (kept as a string until the binder picks
    /// a width and rounds).
    Float {
        /// Original text.
        text: String,
        /// Source span.
        span: Span,
    },
    /// String literal. The body excludes the surrounding quotes and
    /// has any `''` collapsed.
    String {
        /// Decoded body.
        value: String,
        /// Source span.
        span: Span,
    },
}

impl Literal {
    /// Source span.
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Null { span }
            | Self::Bool { span, .. }
            | Self::Integer { span, .. }
            | Self::Float { span, .. }
            | Self::String { span, .. } => *span,
        }
    }
}

/// Unary operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    /// `-x`.
    Neg,
    /// `+x` (rare but accepted).
    Pos,
    /// `NOT x`.
    Not,
    /// `~x` — bitwise NOT (prefix position).
    ///
    /// The `~` character is also used as the binary POSIX-regex-match operator
    /// (`BinaryOp::RegexMatch`). The parser disambiguates by position: in the
    /// prefix (expression-start) slot `~` is `BitNot`; in the infix slot it is
    /// `RegexMatch`.
    BitNot,
}

/// Binary operators recognized by the parser.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%`
    Mod,
    /// `^` (exponentiation)
    Pow,
    /// `||` (string concat).
    Concat,
    /// `=`
    Eq,
    /// `<>`/`!=`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// `AND`
    And,
    /// `OR`
    Or,
    /// `LIKE`
    Like,
    /// `NOT LIKE`
    NotLike,
    /// `ILIKE`
    Ilike,
    /// `NOT ILIKE`
    NotIlike,
    /// `~` — POSIX regex match (case-sensitive).
    RegexMatch,
    /// `~*` — POSIX regex match (case-insensitive).
    RegexIMatch,
    /// `!~` — POSIX regex non-match (case-sensitive).
    RegexNotMatch,
    /// `!~*` — POSIX regex non-match (case-insensitive).
    RegexNotIMatch,
    /// `&` — bitwise AND.
    BitAnd,
    /// `|` — bitwise OR.
    BitOr,
    /// `#` — bitwise XOR (PostgreSQL syntax).
    BitXor,
    /// `<<` — bitwise shift left.
    ShiftLeft,
    /// `>>` — bitwise shift right.
    ShiftRight,
    /// `->` — JSON/JSONB element access by key, returns JSONB.
    JsonGet,
    /// `->>` — JSON/JSONB element access by key, returns text.
    JsonGetText,
    /// `#>` — JSON/JSONB path access, returns JSONB.
    JsonGetPath,
    /// `#>>` — JSON/JSONB path access, returns text.
    JsonGetPathText,
    /// `@>` — JSONB/array contains.
    JsonContains,
    /// `<@` — JSONB/array contained by.
    JsonContained,
    /// `?` — JSONB has key.
    JsonHasKey,
    /// `?|` — JSONB has any of the given keys.
    JsonHasAnyKey,
    /// `?&` — JSONB has all of the given keys.
    JsonHasAllKeys,
}

impl BinaryOp {
    /// Operator precedence level. Higher values bind more tightly.
    ///
    /// The table mirrors PostgreSQL's operator precedence from lowest to highest:
    ///
    /// ```text
    /// Level 1 — OR
    /// Level 2 — AND
    /// Level 3 — comparison band: < > = <= >= <>, LIKE, ILIKE, regex ops (~, ~*, !~, !~*)
    /// Level 4 — JSON ops (-> ->> #> #>> @> <@ ? ?| ?&), concat ||, bitwise & | #
    /// Level 5 — bitwise shift << >>
    /// Level 6 — addition/subtraction + -
    /// Level 7 — multiplication/division/modulo * / %
    /// Level 8 — exponentiation ^ (right-associative)
    /// ```
    ///
    /// JSON operators and bitwise operators sit between the comparison band
    /// and arithmetic to match the most common PostgreSQL use patterns and
    /// avoid mandatory parentheses in practical queries.
    #[must_use]
    pub const fn precedence(self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And => 2,
            // Comparison and regex band
            Self::Eq
            | Self::NotEq
            | Self::Lt
            | Self::LtEq
            | Self::Gt
            | Self::GtEq
            | Self::Like
            | Self::NotLike
            | Self::Ilike
            | Self::NotIlike
            | Self::RegexMatch
            | Self::RegexIMatch
            | Self::RegexNotMatch
            | Self::RegexNotIMatch => 3,
            // JSON operators, concat, and bitwise and/or/xor
            Self::JsonGet
            | Self::JsonGetText
            | Self::JsonGetPath
            | Self::JsonGetPathText
            | Self::JsonContains
            | Self::JsonContained
            | Self::JsonHasKey
            | Self::JsonHasAnyKey
            | Self::JsonHasAllKeys
            | Self::Concat
            | Self::BitAnd
            | Self::BitOr
            | Self::BitXor => 4,
            // Bitwise shift (tighter than add/sub)
            Self::ShiftLeft | Self::ShiftRight => 5,
            Self::Add | Self::Sub => 6,
            Self::Mul | Self::Div | Self::Mod => 7,
            Self::Pow => 8,
        }
    }

    /// `true` iff this operator is right-associative.
    #[must_use]
    pub const fn is_right_associative(self) -> bool {
        matches!(self, Self::Pow)
    }

    /// `true` iff this operator is a comparison operator valid for
    /// `ANY (subquery)` and `ALL (subquery)`.
    #[must_use]
    pub const fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::NotEq | Self::Lt | Self::LtEq | Self::Gt | Self::GtEq
        )
    }
}

// ============================================================================
// Savepoint statements
// ============================================================================

/// `SAVEPOINT name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavepointStmt {
    /// Savepoint name.
    pub name: Identifier,
    /// Source span.
    pub span: Span,
}

/// `ROLLBACK TO [SAVEPOINT] name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RollbackToSavepointStmt {
    /// Savepoint name.
    pub name: Identifier,
    /// Source span.
    pub span: Span,
}

/// `RELEASE [SAVEPOINT] name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleaseSavepointStmt {
    /// Savepoint name.
    pub name: Identifier,
    /// Source span.
    pub span: Span,
}

// ============================================================================
// EXPLAIN statement
// ============================================================================

/// `EXPLAIN [ANALYZE] [VERBOSE] [(FORMAT TEXT|JSON)] stmt`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExplainStmt {
    /// Whether `ANALYZE` was specified.
    pub analyze: bool,
    /// Whether `VERBOSE` was specified.
    pub verbose: bool,
    /// Output format.
    pub format: ExplainFormat,
    /// The inner statement being explained.
    pub statement: Box<Statement>,
    /// Source span.
    pub span: Span,
}

/// Output format for `EXPLAIN`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExplainFormat {
    /// `FORMAT TEXT` (default).
    Text,
    /// `FORMAT JSON`.
    Json,
}

// ============================================================================
// PREPARE / EXECUTE / DEALLOCATE statements
// ============================================================================

/// `PREPARE name [(param_type, …)] AS stmt`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrepareStmt {
    /// Prepared statement name.
    pub name: Identifier,
    /// Optional parameter type list.
    pub param_types: Vec<Identifier>,
    /// The statement body.
    pub statement: Box<Statement>,
    /// Source span.
    pub span: Span,
}

/// `EXECUTE name [(arg, …)]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecuteStmt {
    /// Prepared statement name.
    pub name: Identifier,
    /// Arguments (may be empty).
    pub args: Vec<Expr>,
    /// Source span.
    pub span: Span,
}

/// `DEALLOCATE { ALL | name }`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeallocateStmt {
    /// Prepared statement name, or `None` when `ALL` was specified.
    pub name: Option<Identifier>,
    /// Whether `ALL` was specified.
    pub all: bool,
    /// Source span.
    pub span: Span,
}
