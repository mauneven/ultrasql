//! Planner / binder errors.
//!
//! [`PlanError`] is the error type returned by the binder. Every variant
//! carries enough context that the surface error message can be shown to
//! a SQL user without further enrichment.

use thiserror::Error;

/// An error produced during planning or binding.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum PlanError {
    /// The named relation does not exist in the catalog.
    #[error("table not found: '{0}'")]
    TableNotFound(String),

    /// A referenced column does not exist in the scope.
    #[error("column not found: '{0}'")]
    ColumnNotFound(String),

    /// A column reference matches more than one entry in scope.
    #[error("column reference is ambiguous: '{0}'")]
    Ambiguous(String),

    /// The types of an expression's operands do not satisfy the
    /// operator's rule. The string names the failing combination so a
    /// SQL user can correct the query.
    #[error("type mismatch: {0}")]
    TypeMismatch(String),

    /// The construct is syntactically valid but not yet implemented by
    /// the binder. Carries a `'static` reason so this branch stays
    /// cheap.
    #[error("not supported: {0}")]
    NotSupported(&'static str),
}
