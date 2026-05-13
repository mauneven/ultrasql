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
        /// Source span.
        span: Span,
    },
    /// `NULL` (explicit nullable).
    Null {
        /// Source span.
        span: Span,
    },
    /// `DEFAULT expr`.
    Default {
        /// Default value expression.
        expr: Expr,
        /// Source span.
        span: Span,
    },
    /// `PRIMARY KEY`.
    PrimaryKey {
        /// Source span.
        span: Span,
    },
    /// `UNIQUE`.
    Unique {
        /// Source span.
        span: Span,
    },
    /// `CHECK (expr)`.
    Check {
        /// Constraint expression.
        expr: Expr,
        /// Source span.
        span: Span,
    },
    /// `REFERENCES target_table [(target_columns)]`.
    References {
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
        /// Key columns.
        columns: Vec<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `UNIQUE (col, …)`.
    Unique {
        /// Unique columns.
        columns: Vec<Identifier>,
        /// Source span.
        span: Span,
    },
    /// `FOREIGN KEY (col, …) REFERENCES target_table [(target_col, …)]`.
    ForeignKey {
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

/// A `SELECT` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectStmt {
    /// Whether `DISTINCT` was specified.
    pub distinct: bool,
    /// Output items.
    pub projection: Vec<SelectItem>,
    /// `FROM` clause (zero or one table reference for now; joins land
    /// in a follow-up).
    pub from: Option<TableRef>,
    /// `WHERE` predicate.
    pub r#where: Option<Expr>,
    /// `ORDER BY` items.
    pub order_by: Vec<OrderItem>,
    /// `LIMIT n`.
    pub limit: Option<Expr>,
    /// `OFFSET n`.
    pub offset: Option<Expr>,
    /// Source span.
    pub span: Span,
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
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
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
            | Self::Cast { span, .. } => *span,
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
}

impl BinaryOp {
    /// Precedence (higher binds tighter). Roughly mirrors PostgreSQL.
    #[must_use]
    pub const fn precedence(self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And => 2,
            Self::Eq
            | Self::NotEq
            | Self::Lt
            | Self::LtEq
            | Self::Gt
            | Self::GtEq
            | Self::Like
            | Self::NotLike
            | Self::Ilike
            | Self::NotIlike => 3,
            Self::Concat => 4,
            Self::Add | Self::Sub => 5,
            Self::Mul | Self::Div | Self::Mod => 6,
            Self::Pow => 7,
        }
    }

    /// `true` iff this operator is right-associative.
    #[must_use]
    pub const fn is_right_associative(self) -> bool {
        matches!(self, Self::Pow)
    }
}
