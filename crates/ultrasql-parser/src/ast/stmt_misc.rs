//! Savepoint, `EXPLAIN`, and prepared-statement AST nodes.
//!
//! `SAVEPOINT` / `ROLLBACK TO` / `RELEASE`, `EXPLAIN`, and the
//! `PREPARE` / `EXECUTE` / `DEALLOCATE` statement family.

use crate::ast::{Expr, Identifier, Statement};
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
