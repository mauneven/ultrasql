//! Savepoint, `EXPLAIN`, prepared-statement, and cursor AST nodes.
//!
//! `SAVEPOINT` / `ROLLBACK TO` / `RELEASE`, `EXPLAIN`, the
//! `PREPARE` / `EXECUTE` / `DEALLOCATE` statement family, and the
//! `DECLARE` / `FETCH` / `MOVE` / `CLOSE` cursor family.

use crate::ast::{Expr, Identifier, SelectStmt, Statement};
use crate::span::Span;

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

// ============================================================================
// Cursor statements (DECLARE / FETCH / MOVE / CLOSE)
// ============================================================================

/// `DECLARE name [BINARY] [[NO] SCROLL] CURSOR [{WITH|WITHOUT} HOLD]
/// FOR select`.
///
/// The option keywords (`BINARY`, `INSENSITIVE` / `ASENSITIVE`,
/// `[NO] SCROLL`) may appear in any order, matching PostgreSQL. The
/// parser accepts every form; unsupported combinations (`BINARY`,
/// `SCROLL`, `WITH HOLD`) are rejected by the server with SQLSTATE
/// `0A000` so the diagnostics carry a proper hint rather than a
/// syntax error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclareCursorStmt {
    /// Cursor name (case-folded like any identifier).
    pub name: Identifier,
    /// Whether `BINARY` was specified.
    pub binary: bool,
    /// Whether `SCROLL` was specified (`NO SCROLL` and absent are both
    /// `false` — forward-only).
    pub scroll: bool,
    /// Whether `WITH HOLD` was specified (`WITHOUT HOLD` and absent are
    /// both `false`).
    pub hold: bool,
    /// The cursor's query.
    pub select: Box<SelectStmt>,
    /// Source span.
    pub span: Span,
}

/// Direction clause of a `FETCH` / `MOVE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchDirection {
    /// Forward motion: bare `FETCH` / `NEXT` (one row), a non-negative
    /// count, `FORWARD [count]`, or `ALL` / `FORWARD ALL`
    /// (`count: None` fetches every remaining row).
    Forward {
        /// Number of rows to fetch; `None` means all remaining rows.
        count: Option<i64>,
    },
    /// Any direction that requires a scrollable cursor: `PRIOR`,
    /// `BACKWARD …`, `FIRST`, `LAST`, `ABSOLUTE n`, `RELATIVE n`, or a
    /// negative count. Parsed so the server can reject it with SQLSTATE
    /// `0A000` (forward-only cursors) instead of a syntax error.
    Scrollable,
}

/// `FETCH [direction] [FROM | IN] cursor` and
/// `MOVE [direction] [FROM | IN] cursor`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchStmt {
    /// Direction / row-count clause; defaults to one row forward.
    pub direction: FetchDirection,
    /// Cursor name.
    pub cursor: Identifier,
    /// `true` for `MOVE` (reposition without returning rows).
    pub is_move: bool,
    /// Source span.
    pub span: Span,
}

/// `CLOSE { name | ALL }`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseStmt {
    /// Cursor name, or `None` when `ALL` was specified.
    pub cursor: Option<Identifier>,
    /// Source span.
    pub span: Span,
}
