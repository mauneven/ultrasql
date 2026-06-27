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

    /// A `USING` / `NATURAL` join common column appears more than once on one
    /// side of the join (e.g. both branches of a `CROSS JOIN` define it). The
    /// payload is the fully-rendered PostgreSQL message. Classified as
    /// `ambiguous_column` (SQLSTATE 42702), like [`Self::Ambiguous`].
    #[error("{0}")]
    AmbiguousJoinColumn(String),

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

    /// A numeric or integer literal could not be represented by the
    /// engine's value model (e.g. an integer literal whose magnitude
    /// exceeds `i64`, or a decimal literal whose unscaled mantissa
    /// overflows the i64-backed `NUMERIC` representation). PostgreSQL
    /// raises `numeric_value_out_of_range`; the server maps this to
    /// SQLSTATE `22003`. Erroring here is deliberate: the alternative
    /// (silently saturating to `i64::MAX`) is undetectable data
    /// corruption on the write path.
    #[error("{0}")]
    NumericValueOutOfRange(String),
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
