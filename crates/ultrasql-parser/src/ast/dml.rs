//! Data-manipulation AST nodes.
//!
//! `INSERT` (with `ON CONFLICT`), `UPDATE`, `DELETE`, `MERGE`, and
//! `TRUNCATE`, plus the shared `Assignment` used by `SET` clauses.

use crate::ast::{Expr, Identifier, ObjectName, SelectItem, SelectStmt, TableRef};
use crate::span::Span;

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

/// A `MERGE INTO` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeStmt {
    /// Target table.
    pub target: ObjectName,
    /// Optional alias for the target table.
    pub target_alias: Option<Identifier>,
    /// Source relation from `USING`.
    pub source: TableRef,
    /// Match predicate after `ON`.
    pub on: Expr,
    /// Ordered `WHEN ... THEN ...` clauses.
    pub clauses: Vec<MergeClause>,
    /// Source span of the entire statement.
    pub span: Span,
}

/// One `WHEN ... THEN ...` clause in a `MERGE INTO` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeClause {
    /// Whether this branch handles matched or unmatched source rows.
    pub kind: MergeMatchKind,
    /// Optional branch predicate after `AND`.
    pub condition: Option<Expr>,
    /// Action to run when this branch fires.
    pub action: MergeAction,
    /// Source span of this clause.
    pub span: Span,
}

/// Match class for a `MERGE INTO` branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeMatchKind {
    /// `WHEN MATCHED`.
    Matched,
    /// `WHEN NOT MATCHED`.
    NotMatched,
}

/// Action attached to a `MERGE INTO` branch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeAction {
    /// `THEN UPDATE SET ...`.
    Update {
        /// Target assignments.
        set: Vec<Assignment>,
    },
    /// `THEN DELETE`.
    Delete,
    /// `THEN INSERT [(columns)] VALUES (...)`.
    Insert {
        /// Optional target column list.
        columns: Vec<Identifier>,
        /// Values to insert.
        values: Vec<Expr>,
    },
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
