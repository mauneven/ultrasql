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

    /// DDL was issued inside an explicit transaction block. PostgreSQL
    /// supports transactional DDL; UltraSQL does not yet (the catalog is
    /// mutated globally-in-place and committed durably mid-statement
    /// under a private `ddl_txn`, with no per-transaction overlay, so a
    /// rolled-back transaction's schema change could not be undone — see
    /// `docs/transactional-ddl-design.md`). Maps to PostgreSQL SQLSTATE
    /// `0A000` (`feature_not_supported`) — the PG-faithful code for a
    /// feature PostgreSQL implements but UltraSQL has not yet. The
    /// message carries a `HINT:` telling the caller to run the DDL in
    /// autocommit (outside an explicit transaction) until transactional
    /// DDL lands, so ORM/migration tooling gets a deterministic,
    /// classifiable failure rather than a generic "unsupported" string.
    #[error(
        "DDL inside an explicit transaction block is not yet supported\nHINT:  run the statement in autocommit (outside an explicit BEGIN/COMMIT block); transactional DDL is not yet implemented"
    )]
    DdlInTransaction,

    /// A statement uses an unsupported construct whose explanation is
    /// computed from the rejected query.
    #[error("unsupported in v0.5: {0}")]
    UnsupportedOwned(String),

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

    /// A named non-relation object does not exist. Maps to PostgreSQL
    /// SQLSTATE `42704` (`undefined_object`).
    #[error("{0}")]
    UndefinedObject(String),

    /// A named non-relation object already exists — for example, an
    /// `ALTER TABLE ADD CONSTRAINT` whose name is already taken on the
    /// table. Maps to PostgreSQL SQLSTATE `42710` (`duplicate_object`).
    #[error("{0}")]
    DuplicateObject(String),

    /// A schema referenced by a qualified object name does not exist.
    /// Maps to PostgreSQL SQLSTATE `3F000` (`invalid_schema_name`).
    #[error("{0}")]
    UndefinedSchema(String),

    /// An object cannot be dropped because another object depends on it.
    /// Maps to PostgreSQL SQLSTATE `2BP01`.
    #[error("{0}")]
    DependentObjectsStillExist(String),

    /// A DDL change would leave the table in an invalid definition — for
    /// example `ALTER COLUMN ... DROP NOT NULL` on a primary-key column.
    /// Maps to PostgreSQL SQLSTATE `42P16` (`invalid_table_definition`).
    #[error("{0}")]
    InvalidTableDefinition(String),

    /// A statement was issued while the current explicit transaction
    /// was already aborted by a prior error. Maps to PostgreSQL
    /// SQLSTATE `25P02` (`in_failed_sql_transaction`); the user must
    /// issue `COMMIT` (treated as `ROLLBACK`) or `ROLLBACK` to leave
    /// the failed-block state before any further statements are
    /// accepted.
    #[error("current transaction is aborted, commands ignored until end of transaction block")]
    TransactionAborted,

    /// Serializable Snapshot Isolation detected a dangerous structure.
    /// Maps to SQLSTATE `40001` (`serialization_failure`).
    #[error("serialization failure: {0}")]
    SerializationFailure(String),

    /// A `SELECT ... FOR UPDATE / SHARE ... NOWAIT` could not immediately
    /// acquire a conflicting row lock. Maps to PostgreSQL SQLSTATE
    /// `55P03` (`lock_not_available`). Query-scoped: the surrounding
    /// transaction block is aborted, but the connection survives.
    #[error("{0}")]
    LockNotAvailable(String),

    /// The lock manager's deadlock detector chose this transaction as the
    /// victim of a lock-wait cycle. Maps to PostgreSQL SQLSTATE `40P01`
    /// (`deadlock_detected`). Query-scoped and retryable: the client
    /// should re-issue the transaction.
    #[error("{0}")]
    DeadlockDetected(String),

    /// `nextval` advanced a non-`CYCLE` sequence past its declared
    /// `MAXVALUE`/`MINVALUE`. Maps to PostgreSQL SQLSTATE `2200H`
    /// (`sequence_generator_limit_exceeded`). The string carries the
    /// PostgreSQL-matching message text.
    #[error("{0}")]
    SequenceLimitExceeded(String),

    /// `SAVEPOINT` / `RELEASE` / `ROLLBACK TO SAVEPOINT` was issued
    /// outside a transaction block. Maps to PostgreSQL SQLSTATE
    /// `25P01` (`no_active_sql_transaction`). The string names the
    /// failing construct.
    #[error("{0}")]
    Savepoint(&'static str),

    /// `RELEASE` / `ROLLBACK TO SAVEPOINT` named an unknown savepoint.
    /// Maps to PostgreSQL SQLSTATE `3B001`
    /// (`invalid_savepoint_specification`). The string names the
    /// missing savepoint.
    #[error("savepoint '{0}' does not exist")]
    SavepointNotFound(String),

    /// A data-modifying statement was issued inside a read-only
    /// transaction (`BEGIN READ ONLY` / `SET TRANSACTION READ ONLY`).
    /// Maps to PostgreSQL SQLSTATE `25006` (`read_only_sql_transaction`).
    /// The string names the rejected command (e.g. `INSERT`). Like any
    /// in-transaction error, this aborts the surrounding block.
    #[error("cannot execute {0} in a read-only transaction")]
    ReadOnlyTransaction(&'static str),

    /// Authentication challenge rejected (wrong password, wrong user
    /// name, missing Password message). Maps to PostgreSQL SQLSTATE
    /// `28P01` (`invalid_password`). The connection is closed after
    /// this error is returned.
    #[error("password authentication failed")]
    AuthFailed,

    /// COPY stream contained a malformed row (wrong column count,
    /// unterminated quoted field, non-decodable cell). Maps to PostgreSQL
    /// SQLSTATE `22P04` (`bad_copy_file_format`). Query-scoped: the
    /// session is preserved and a fresh `ReadyForQuery` is emitted.
    #[error("COPY format: {0}")]
    CopyFormat(String),

    /// The client cancelled a `COPY FROM STDIN` by sending `CopyFail`.
    /// Carries the human-readable reason supplied by the client. Maps to
    /// PostgreSQL SQLSTATE `57014` (`query_canceled`).
    #[error("COPY from stdin failed: {0}")]
    CopyAborted(String),

    /// Object exists, but not in the right session-local state for the
    /// requested operation. Used for `currval` / `lastval` before a
    /// session has called `nextval`. Maps to SQLSTATE `55000`.
    #[error("{0}")]
    ObjectNotInPrerequisiteState(String),

    /// Role lacks the required object or column privilege. Maps to
    /// SQLSTATE `42501`.
    #[error("permission denied: {0}")]
    InsufficientPrivilege(String),

    /// A statement's synchronous execution panicked and the panic was
    /// caught by the per-statement `catch_unwind` guard in the session
    /// loop (see `session/run.rs` / `session/ext.rs`). The panic payload
    /// is logged server-side with full detail; this carries only a
    /// fixed, generic message so the panic string is never leaked to the
    /// client. Maps to PostgreSQL SQLSTATE `XX000` (`internal_error`).
    ///
    /// Query-scoped: the connection survives and the client receives a
    /// generic `ErrorResponse`; the active explicit transaction (if any)
    /// is aborted at the catch site, exactly as a normal in-block error
    /// would do.
    #[error("internal error")]
    Internal,
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

    /// Build an owned unsupported-feature error without leaking a
    /// formatted message to satisfy a `'static` lifetime.
    #[must_use]
    pub fn unsupported<M: Into<String>>(message: M) -> Self {
        Self::UnsupportedOwned(message.into())
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
                | Self::UnsupportedOwned(_)
                | Self::DdlInTransaction
                | Self::Ddl(_)
                | Self::Catalog(_)
                | Self::UndefinedObject(_)
                | Self::DuplicateObject(_)
                | Self::UndefinedSchema(_)
                | Self::DependentObjectsStillExist(_)
                | Self::InvalidTableDefinition(_)
                | Self::TransactionAborted
                | Self::SerializationFailure(_)
                | Self::LockNotAvailable(_)
                | Self::DeadlockDetected(_)
                | Self::SequenceLimitExceeded(_)
                | Self::Savepoint(_)
                | Self::SavepointNotFound(_)
                | Self::ReadOnlyTransaction(_)
                | Self::CopyFormat(_)
                | Self::CopyAborted(_)
                | Self::ObjectNotInPrerequisiteState(_)
                | Self::InsufficientPrivilege(_)
                // A caught panic is reported to the client and the session
                // is kept alive: the whole point of the per-statement
                // catch_unwind is that one bad query never tears down the
                // connection (let alone the process).
                | Self::Internal
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
            Self::Plan(ultrasql_planner::PlanError::TypeMismatch(_)) => "42804", // datatype_mismatch
            Self::Plan(
                ultrasql_planner::PlanError::NotSupported(_)
                | ultrasql_planner::PlanError::NotSupportedOwned(_),
            ) => "0A000", // feature_not_supported
            // windowing_error — an illegal window-frame clause (bad bound
            // ordering, RANGE offset without one ORDER BY col, etc.).
            Self::Plan(ultrasql_planner::PlanError::InvalidWindowFrame(_)) => "42P20",
            // invalid_column_reference — DISTINCT ON expressions must be a
            // prefix of ORDER BY.
            Self::Plan(ultrasql_planner::PlanError::DistinctOnOrderByMismatch(_)) => "42P10",
            // ambiguous_column — a column reference matched more than one
            // entry in scope (e.g. unqualified `id` across joined tables).
            Self::Plan(ultrasql_planner::PlanError::Ambiguous(_)) => "42702",
            // undefined_object — a referenced index does not exist in the
            // catalog (DROP INDEX / index hint on a missing name).
            Self::Plan(ultrasql_planner::PlanError::IndexNotFound(_)) => "42704",
            // undefined_function — a call named a function that is not a
            // supported builtin.
            Self::Plan(ultrasql_planner::PlanError::UndefinedFunction(_)) => "42883",
            // numeric_value_out_of_range — a numeric/integer literal could
            // not be represented (magnitude exceeds i64). The binder errors
            // here rather than silently saturating to i64::MAX.
            Self::Plan(ultrasql_planner::PlanError::NumericValueOutOfRange(_)) => "22003",
            // undefined_table — coarse planner fallback plus the catalog
            // NotFound that surfaces when DROP / ALTER fails to resolve a name
            Self::Plan(_) | Self::Catalog(ultrasql_catalog::CatalogError::NotFound(_)) => "42P01",
            Self::UndefinedObject(_) => "42704", // undefined_object
            Self::DuplicateObject(_) => "42710", // duplicate_object
            Self::UndefinedSchema(_) => "3F000", // invalid_schema_name
            Self::Build(_) | Self::Unsupported(_) | Self::UnsupportedOwned(_) => "0A000", // feature_not_supported
            // feature_not_supported — DDL inside an explicit transaction
            // block. PostgreSQL implements transactional DDL; UltraSQL
            // does not yet, so 0A000 is the PG-faithful classification.
            Self::DdlInTransaction => "0A000",
            Self::UnsupportedProtocol { .. } => "08P01", // protocol_violation
            Self::DependentObjectsStillExist(_) => "2BP01", // dependent_objects_still_exist
            Self::InvalidTableDefinition(_) => "42P16",  // invalid_table_definition
            Self::Catalog(_) => "42000",                 // generic catalog failure
            Self::SerializationFailure(_) => "40001",    // serialization_failure
            // lock_not_available — SELECT ... FOR UPDATE/SHARE NOWAIT hit a
            // conflicting row lock held by another transaction.
            Self::LockNotAvailable(_) => "55P03",
            // deadlock_detected — the lock manager's wait-for cycle detector
            // picked this transaction as the victim.
            Self::DeadlockDetected(_) => "40P01",
            // sequence_generator_limit_exceeded — nextval ran a non-CYCLE
            // sequence past its MAXVALUE/MINVALUE.
            Self::SequenceLimitExceeded(_) => "2200H",
            Self::TransactionAborted => "25P02", // in_failed_sql_transaction
            Self::Savepoint(_) => "25P01",       // no_active_sql_transaction
            Self::SavepointNotFound(_) => "3B001", // invalid_savepoint_specification
            Self::ReadOnlyTransaction(_) => "25006", // read_only_sql_transaction
            Self::InsufficientPrivilege(_) => "42501", // insufficient_privilege
            // NOT-NULL constraint violation surfaced by `ModifyTable`
            // on INSERT / UPDATE. Mirrors PostgreSQL's
            // `not_null_violation`.
            Self::Execute(ultrasql_executor::ExecError::NotNullViolation(_)) => "23502",
            // CHECK constraint violation surfaced by `ModifyTable`.
            Self::Execute(ultrasql_executor::ExecError::CheckViolation(_)) => "23514",
            // Duplicate key surfaced by insert-side B-tree index
            // maintenance. Mirrors PostgreSQL's `unique_violation`.
            Self::Execute(ultrasql_executor::ExecError::UniqueViolation(_)) => "23505",
            // FOREIGN KEY violation surfaced by DML constraint checks.
            Self::Execute(ultrasql_executor::ExecError::ForeignKeyViolation(_)) => "23503",
            // EXCLUDE constraint violation surfaced by DML constraint checks.
            Self::Execute(ultrasql_executor::ExecError::ExclusionViolation(_)) => "23P01",
            // generated_always — explicit INSERT value for a GENERATED
            // ALWAYS identity column.
            Self::Execute(ultrasql_executor::ExecError::GeneratedAlwaysViolation(_)) => "428C9",
            // string_data_right_truncation — assignment to CHAR(n) /
            // VARCHAR(n)-style width exceeded declared length.
            Self::Execute(ultrasql_executor::ExecError::StringDataRightTruncation(_)) => "22001",
            // numeric_value_out_of_range — assignment exceeds
            // declared NUMERIC/DECIMAL precision.
            Self::Execute(ultrasql_executor::ExecError::NumericFieldOverflow(_)) => "22003",
            // division_by_zero — scalar runtime arithmetic detected a
            // zero divisor.
            Self::Execute(ultrasql_executor::ExecError::DivisionByZero(_)) => "22012",
            // invalid_text_representation — runtime cast parser rejected
            // a text value for the requested SQL type.
            Self::Execute(ultrasql_executor::ExecError::InvalidTextRepresentation(_)) => "22P02",
            // invalid_parameter_value — runtime parameter rejected by a
            // SQL surface such as generate_series(..., step => 0).
            Self::Execute(ultrasql_executor::ExecError::InvalidParameterValue(_)) => "22023",
            // invalid_preceding_or_following_size — a window-frame offset
            // was negative or NULL at execution.
            Self::Execute(ultrasql_executor::ExecError::WindowFrameError(_)) => "22013",
            // invalid_xml_document — XML cast parser rejected a document
            // value.
            Self::Execute(ultrasql_executor::ExecError::InvalidXmlDocument(_)) => "2200M",
            // serialization_failure — a concurrent transaction held an
            // unresolved in-place write on a tuple this UPDATE/DELETE
            // touched. Mirrors the SSI `Self::SerializationFailure` 40001
            // path above so retry-aware clients classify both alike.
            Self::Execute(ultrasql_executor::ExecError::SerializationFailure(_)) => "40001",
            // cardinality_violation — a scalar subquery used as an
            // expression returned more than one row. Raised by the
            // `SingleRowAssert` guard the decorrelation rule inserts.
            Self::Execute(ultrasql_executor::ExecError::CardinalityViolation) => "21000",
            // query_canceled — operator polled the `CancelFlag` between
            // batches and short-circuited after a peer `CancelRequest`
            // flipped it. Mirrors PostgreSQL's `query_canceled`.
            Self::Execute(ultrasql_executor::ExecError::Cancelled) => "57014",
            // bad_copy_file_format — surfaced when a COPY FROM stream
            // delivers a malformed row.
            Self::CopyFormat(_) => "22P04",
            // query_canceled — client requested `CopyFail`.
            Self::CopyAborted(_) => "57014",
            // object_not_in_prerequisite_state — sequence exists but
            // currval/lastval has no session-local value yet.
            Self::ObjectNotInPrerequisiteState(_) => "55000",
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
    fn out_of_range_numeric_literal_maps_to_22003() {
        // FIX A: an out-of-i64 numeric/integer literal raises
        // numeric_value_out_of_range at bind time. The server must surface
        // SQLSTATE 22003 over the wire (not the 42P01 planner catch-all),
        // and the error must be query-scoped so the connection survives.
        let err: ServerError = ultrasql_planner::PlanError::NumericValueOutOfRange(
            "integer literal 99999999999999999999 is out of range".to_owned(),
        )
        .into();
        assert_eq!(err.sqlstate(), "22003");
        assert!(err.is_query_scoped());
    }

    #[test]
    fn build_error_is_query_scoped_feature_not_supported() {
        let err: ServerError = ultrasql_executor::physical::BuildError::Unsupported("test").into();
        assert!(err.is_query_scoped());
        assert_eq!(err.sqlstate(), "0A000");
    }

    #[test]
    fn dynamic_unsupported_owns_message() {
        let err = ServerError::unsupported(format!("unsupported function '{}'", "foo"));

        assert!(err.is_query_scoped());
        assert_eq!(err.sqlstate(), "0A000");
        assert_eq!(
            err.to_string(),
            "unsupported in v0.5: unsupported function 'foo'"
        );
    }

    #[test]
    fn ddl_in_transaction_is_feature_not_supported_with_hint() {
        let err = ServerError::DdlInTransaction;

        // Query-scoped: the session survives, the block transitions to
        // Failed at the call site, not the connection.
        assert!(err.is_query_scoped());
        // PG-faithful: PostgreSQL supports transactional DDL, so the
        // "not implemented here yet" code is feature_not_supported.
        assert_eq!(err.sqlstate(), "0A000");
        // Deterministic, classifiable message + a HINT so ORM/migration
        // tooling can route the user to autocommit.
        let msg = err.to_string();
        assert!(
            msg.contains("DDL inside an explicit transaction block is not yet supported"),
            "message names the rejected construct: {msg}"
        );
        assert!(
            msg.contains("HINT:") && msg.contains("autocommit"),
            "message carries an autocommit hint: {msg}"
        );
    }

    #[test]
    fn undefined_schema_is_query_scoped_invalid_schema_name() {
        let err = ServerError::UndefinedSchema("schema \"missing\" does not exist".to_owned());

        assert!(err.is_query_scoped());
        assert_eq!(err.sqlstate(), "3F000");
    }

    #[test]
    fn ambiguous_column_is_ambiguous_column_code() {
        // PG: 42702 ambiguous_column. Must not fall into the 42P01 catch-all.
        let err = ServerError::Plan(ultrasql_planner::PlanError::Ambiguous("id".to_owned()));
        assert!(err.is_query_scoped());
        assert_eq!(err.sqlstate(), "42702");
    }

    #[test]
    fn index_not_found_is_undefined_object_code() {
        // PG: 42704 undefined_object. Must not fall into the 42P01 catch-all.
        let err = ServerError::Plan(ultrasql_planner::PlanError::IndexNotFound("idx".to_owned()));
        assert!(err.is_query_scoped());
        assert_eq!(err.sqlstate(), "42704");
    }

    #[test]
    fn undefined_function_is_undefined_function_code() {
        // PG: 42883 undefined_function.
        let err = ServerError::Plan(ultrasql_planner::PlanError::UndefinedFunction(
            "foobar()".to_owned(),
        ));
        assert!(err.is_query_scoped());
        assert_eq!(err.sqlstate(), "42883");
    }

    #[test]
    fn table_not_found_still_maps_to_undefined_table() {
        // Regression guard: the genuine undefined_table path must keep 42P01
        // after the explicit Ambiguous/IndexNotFound arms were inserted ahead
        // of the catch-all.
        let err = ServerError::Plan(ultrasql_planner::PlanError::TableNotFound("t".to_owned()));
        assert_eq!(err.sqlstate(), "42P01");
    }

    #[test]
    fn sequence_limit_exceeded_maps_to_2200h() {
        // PG: 2200H sequence_generator_limit_exceeded.
        let err = ServerError::SequenceLimitExceeded(
            "nextval: reached maximum value of sequence \"s\"".to_owned(),
        );
        assert!(err.is_query_scoped());
        assert_eq!(err.sqlstate(), "2200H");
    }

    #[test]
    fn row_lock_conflict_maps_to_serialization_failure() {
        // PG: 40001 serialization_failure for a blocking-lock-conflict abort.
        let err = ServerError::Execute(ultrasql_executor::ExecError::SerializationFailure(
            "could not serialize access due to concurrent update".to_owned(),
        ));
        assert!(err.is_query_scoped());
        assert_eq!(err.sqlstate(), "40001");
    }

    #[test]
    fn caught_panic_is_query_scoped_internal_error() {
        // A panic caught by the per-statement guard must be reported to the
        // client as a query-scoped XX000 (session survives) and must NOT
        // leak the panic payload — `Display` is the fixed generic string.
        let err = ServerError::Internal;
        assert!(err.is_query_scoped());
        assert_eq!(err.sqlstate(), "XX000");
        assert_eq!(err.to_string(), "internal error");
    }

    #[test]
    fn set_transaction_outside_block_maps_to_no_active_sql_transaction() {
        // PG: 25P01 no_active_sql_transaction. `execute_set_transaction`
        // returns `Savepoint` for the Idle case; both share this code.
        let err = ServerError::Savepoint("SET TRANSACTION can only be used in transaction blocks");
        assert!(err.is_query_scoped());
        assert_eq!(err.sqlstate(), "25P01");
    }
}
