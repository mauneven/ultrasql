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

    /// The named index does not exist in the catalog.
    #[error("index not found: '{0}'")]
    IndexNotFound(String),

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

    /// The same column name appeared more than once in a column list.
    #[error("duplicate column: '{0}'")]
    DuplicateColumn(String),

    /// `CREATE TABLE` named a relation that already exists and
    /// `IF NOT EXISTS` was not specified.
    #[error("relation already exists: '{0}'")]
    DuplicateTable(String),

    /// The construct is syntactically valid but not yet implemented by
    /// the binder. Carries a `'static` reason so this branch stays
    /// cheap.
    #[error("not supported: {0}")]
    NotSupported(&'static str),

    /// The construct is syntactically valid but not yet implemented by
    /// the binder, with context computed from the rejected query.
    #[error("not supported: {0}")]
    NotSupportedOwned(String),

    /// A window `OVER (...)` frame clause is illegal (bad bound ordering,
    /// `RANGE` offset without exactly one `ORDER BY` column, `GROUPS`
    /// without `ORDER BY`, etc.). The message carries PostgreSQL-matching
    /// text; the server maps this to SQLSTATE `42P20` (`windowing_error`).
    #[error("{0}")]
    InvalidWindowFrame(String),

    /// The `DISTINCT ON (...)` expressions are not a prefix of the `ORDER BY`
    /// list. The message carries PostgreSQL-matching text; the server maps
    /// this to SQLSTATE `42P10` (`invalid_column_reference`).
    #[error("{0}")]
    DistinctOnOrderByMismatch(String),

    /// A scalar function call named a function that does not exist as a
    /// supported builtin. The server maps this to SQLSTATE `42883`
    /// (`undefined_function`). The string names the missing function.
    #[error("function {0} does not exist")]
    UndefinedFunction(String),
}

impl PlanError {
    /// Build an owned "not supported" error without leaking a formatted
    /// message to satisfy a `'static` lifetime.
    #[must_use]
    pub fn not_supported<M: Into<String>>(message: M) -> Self {
        Self::NotSupportedOwned(message.into())
    }

    /// `true` when the planner rejected an unsupported but syntactically
    /// valid SQL construct.
    #[must_use]
    pub const fn is_not_supported(&self) -> bool {
        matches!(self, Self::NotSupported(_) | Self::NotSupportedOwned(_))
    }
}

#[cfg(test)]
mod tests {
    use super::PlanError;

    #[test]
    fn dynamic_not_supported_owns_message() {
        let err = PlanError::not_supported(format!("window function '{}'", "foo"));

        assert_eq!(
            err,
            PlanError::NotSupportedOwned("window function 'foo'".to_string())
        );
        assert_eq!(err.to_string(), "not supported: window function 'foo'");
        assert!(err.is_not_supported());
    }
}
