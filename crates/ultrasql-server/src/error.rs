//! `ultrasqld` server error type.
//!
//! [`ServerError`] is the single error returned from every fallible
//! public entry point in this crate. Per-layer errors (parser, planner,
//! executor, codec) are wrapped so a connection-handler caller can pick
//! a single match.
//!
//! Errors fall into two categories:
//!
//! 1. **Connection-fatal** — protocol violations, I/O failures, dropped
//!    sockets. The current connection is torn down.
//! 2. **Query-scoped** — parse, plan, or execute failed for the
//!    in-flight statement. The session continues; the failure is
//!    reported to the client as an `ErrorResponse` followed by
//!    `ReadyForQuery 'I'`.
//!
//! The connection loop classifies errors using
//! [`ServerError::is_query_scoped`]: query-scoped errors are reported
//! and the loop continues; everything else is propagated and the
//! connection is closed.

use std::io;

use thiserror::Error;

/// Errors returned by the server library.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ServerError {
    /// I/O failure on the connection socket.
    #[error("connection I/O error: {0}")]
    Io(#[from] io::Error),

    /// Wire-protocol codec rejected a frame.
    #[error("wire protocol error: {0}")]
    Protocol(#[from] ultrasql_protocol::ProtocolError),

    /// Client disconnected before sending the startup message or
    /// before completing the current frame.
    #[error("client disconnected unexpectedly")]
    UnexpectedEof,

    /// Client sent a startup message with an unsupported protocol
    /// version. The server only accepts the v3.0 framing.
    #[error("unsupported protocol version {major}.{minor}")]
    UnsupportedProtocol {
        /// Reported major version.
        major: u16,
        /// Reported minor version.
        minor: u16,
    },

    /// SQL parser rejected the query text.
    #[error("parse error: {0}")]
    Parse(#[from] ultrasql_parser::ParseError),

    /// Binder or planner rejected the statement.
    #[error("planner error: {0}")]
    Plan(#[from] ultrasql_planner::PlanError),

    /// Executor failed at runtime.
    #[error("execution error: {0}")]
    Execute(#[from] ultrasql_executor::ExecError),

    /// Physical-plan builder rejected the bound logical plan.
    #[error("plan lowering: {0}")]
    Build(#[from] ultrasql_executor::physical::BuildError),

    /// A statement uses a construct the server cannot lower to a
    /// physical plan yet. The string names the construct.
    #[error("unsupported in v0.5: {0}")]
    Unsupported(&'static str),

    /// A DDL kernel (catalog mutation, B-tree build, ALTER rewrite,
    /// etc.) failed at runtime with a dynamic message — for example
    /// a storage error encountered while populating an index, or a
    /// row codec failure during a relation rewrite.
    ///
    /// Distinct from [`ServerError::Unsupported`] (which carries a
    /// static `&'static str` for cheap construction) and from
    /// [`ServerError::Catalog`] (which only wraps the typed catalog
    /// error enum). DDL paths consume this when they bubble up an
    /// error whose context is more useful than a single
    /// thiserror-derived variant.
    #[error("DDL failed: {0}")]
    Ddl(String),

    /// A catalog operation (CREATE/DROP/ALTER) was rejected — for
    /// example, `CREATE TABLE` on an existing relation.
    #[error("catalog error: {0}")]
    Catalog(#[from] ultrasql_catalog::CatalogError),
}

impl ServerError {
    /// Build a [`ServerError::Ddl`] from any displayable message.
    ///
    /// Convenience constructor used by the DDL dispatch paths in
    /// `lib.rs` so the call sites stay compact:
    ///
    /// ```ignore
    /// op.map_err(|e| ServerError::ddl(format_args!("CREATE INDEX: {e}")))?;
    /// ```
    #[must_use]
    pub fn ddl<M: Into<String>>(msg: M) -> Self {
        Self::Ddl(msg.into())
    }
}

impl ServerError {
    /// `true` if this error should be reported to the client as a
    /// query-scoped `ErrorResponse` and the session continued.
    ///
    /// Connection-fatal errors (`Io`, `Protocol`, `UnexpectedEof`,
    /// `UnsupportedProtocol`) return `false`; the caller drops the
    /// session.
    #[must_use]
    pub const fn is_query_scoped(&self) -> bool {
        matches!(
            self,
            Self::Parse(_)
                | Self::Plan(_)
                | Self::Execute(_)
                | Self::Build(_)
                | Self::Unsupported(_)
                | Self::Ddl(_)
                | Self::Catalog(_)
        )
    }

    /// The PostgreSQL SQLSTATE-style error code to report for this
    /// failure. The set is intentionally small for v0.5; richer codes
    /// land as the planner and executor grow.
    #[must_use]
    pub const fn sqlstate(&self) -> &'static str {
        match self {
            Self::Parse(_) => "42601", // syntax_error
            // duplicate_table — name collision from either layer
            Self::Plan(ultrasql_planner::PlanError::DuplicateTable(_))
            | Self::Catalog(ultrasql_catalog::CatalogError::AlreadyExists(_)) => "42P07",
            Self::Plan(ultrasql_planner::PlanError::DuplicateColumn(_)) => "42701", // duplicate_column
            Self::Plan(ultrasql_planner::PlanError::ColumnNotFound(_)) => "42703", // undefined_column
            // undefined_table — coarse planner fallback plus the catalog
            // NotFound that surfaces when DROP / ALTER fails to resolve a name
            Self::Plan(_) | Self::Catalog(ultrasql_catalog::CatalogError::NotFound(_)) => "42P01",
            Self::Build(_) | Self::Unsupported(_) => "0A000", // feature_not_supported
            Self::UnsupportedProtocol { .. } => "08P01",      // protocol_violation
            Self::Catalog(_) => "42000",                      // generic catalog failure
            // Internal-style: Execute/IO/Protocol/UnexpectedEof and the
            // dynamic Ddl message all map to XX000.
            _ => "XX000",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_scoped_errors_do_not_close_connection() {
        let err: ServerError = ultrasql_parser::ParseError::UnexpectedEof {
            expected: "statement",
        }
        .into();
        assert!(err.is_query_scoped());
        assert_eq!(err.sqlstate(), "42601");
    }

    #[test]
    fn io_errors_close_connection() {
        let io_err = io::Error::new(io::ErrorKind::BrokenPipe, "client left");
        let err: ServerError = io_err.into();
        assert!(!err.is_query_scoped());
    }

    #[test]
    fn unexpected_eof_is_connection_fatal() {
        let err = ServerError::UnexpectedEof;
        assert!(!err.is_query_scoped());
    }

    #[test]
    fn unsupported_protocol_is_connection_fatal() {
        let err = ServerError::UnsupportedProtocol { major: 2, minor: 0 };
        assert!(!err.is_query_scoped());
        assert_eq!(err.sqlstate(), "08P01");
    }

    #[test]
    fn build_error_is_query_scoped_feature_not_supported() {
        let err: ServerError = ultrasql_executor::physical::BuildError::Unsupported("test").into();
        assert!(err.is_query_scoped());
        assert_eq!(err.sqlstate(), "0A000");
    }
}
