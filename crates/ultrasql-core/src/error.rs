//! UltraSQL error type.
//!
//! `Error` is the single error enum returned across every public API
//! surface in UltraSQL. The variants partition recoverable failures
//! (`InvalidArgument`, `NotFound`, etc.) from internal invariant
//! violations (`Internal`). Callers route on variant; humans route on
//! `Display`.

use std::io;

/// Top-level result alias used across the workspace.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Top-level UltraSQL error.
///
/// `Error` is intentionally flat: each variant is a self-contained reason
/// the operation failed. Subsystem-specific context (record offsets,
/// page IDs, transaction IDs) is included as variant data so callers can
/// program against it without parsing strings.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The caller supplied an invalid argument. The string is suitable
    /// for surfacing to a SQL user.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// An object — relation, column, index, schema, etc. — was not
    /// found.
    #[error("not found: {0}")]
    NotFound(String),

    /// The operation conflicts with another in-flight operation
    /// (lock conflict, MVCC conflict, etc.).
    #[error("conflict: {0}")]
    Conflict(String),

    /// The operation exceeds a configured limit (`work_mem`,
    /// `max_locks`, `statement_timeout`, etc.).
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),

    /// Data on disk fails its integrity check.
    #[error("data corruption: {0}")]
    Corruption(String),

    /// A SQL semantic error.
    #[error("sql error: {0}")]
    Sql(String),

    /// A SQL syntax error with a position into the original statement.
    #[error("syntax error at offset {offset}: {message}")]
    Syntax {
        /// Byte offset into the original SQL text.
        offset: usize,
        /// Human-readable message.
        message: String,
    },

    /// A type-checking error during binding/planning.
    #[error("type error: {0}")]
    Type(String),

    /// An I/O error from the storage layer.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// An invariant violation. Reported in production; programmer error
    /// in development. The string literal naming the invariant is
    /// `'static` so we can capture it cheaply on hot paths.
    #[error("internal invariant violation: {0}")]
    Internal(&'static str),

    /// The operation is not yet implemented. Used as a placeholder
    /// during bring-up; CI fails if `NotImplemented` survives into a
    /// release branch.
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

impl Error {
    /// Build an [`Error::InvalidArgument`] without allocating when the
    /// caller already has an owned `String`.
    pub fn invalid<S: Into<String>>(msg: S) -> Self {
        Self::InvalidArgument(msg.into())
    }

    /// Build a [`Error::NotFound`] without allocating when the caller
    /// already has an owned `String`.
    pub fn not_found<S: Into<String>>(msg: S) -> Self {
        Self::NotFound(msg.into())
    }

    /// Build a [`Error::Sql`] from a borrowed or owned message.
    pub fn sql<S: Into<String>>(msg: S) -> Self {
        Self::Sql(msg.into())
    }

    /// Return `true` if this error is permanent — retrying the same
    /// operation will produce the same outcome.
    #[must_use]
    pub const fn is_permanent(&self) -> bool {
        matches!(
            self,
            Self::InvalidArgument(_)
                | Self::NotFound(_)
                | Self::Sql(_)
                | Self::Syntax { .. }
                | Self::Type(_)
                | Self::Corruption(_)
                | Self::Internal(_)
                | Self::NotImplemented(_)
        )
    }

    /// Return `true` if this error is *transient* — retrying the
    /// operation may succeed.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::Conflict(_) | Self::LimitExceeded(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permanent_and_transient_partition_cleanly() {
        let cases: &[Error] = &[
            Error::invalid("bad"),
            Error::not_found("missing"),
            Error::sql("nope"),
            Error::Syntax {
                offset: 0,
                message: "x".into(),
            },
            Error::Type("y".into()),
            Error::Corruption("c".into()),
            Error::Internal("static"),
            Error::NotImplemented("todo"),
            Error::Conflict("c".into()),
            Error::LimitExceeded("l".into()),
            Error::Io(io::Error::other("oops")),
        ];

        for case in cases {
            // Each error is either permanent or transient, and IO is
            // neither (it depends).
            let permanent = case.is_permanent();
            let transient = case.is_transient();
            assert!(!(permanent && transient), "{case:?} cannot be both");
            if matches!(case, Error::Io(_)) {
                assert!(!permanent && !transient);
            } else {
                assert!(permanent || transient, "{case:?} must be one of");
            }
        }
    }

    #[test]
    fn display_includes_offset() {
        let err = Error::Syntax {
            offset: 42,
            message: "unexpected token".into(),
        };
        let s = err.to_string();
        assert!(s.contains("42"), "got {s}");
        assert!(s.contains("unexpected token"));
    }

    #[test]
    fn io_error_round_trips() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "boom");
        let err: Error = io_err.into();
        match err {
            Error::Io(inner) => assert_eq!(inner.kind(), io::ErrorKind::NotFound),
            other => panic!("expected Io, got {other:?}"),
        }
    }
}
