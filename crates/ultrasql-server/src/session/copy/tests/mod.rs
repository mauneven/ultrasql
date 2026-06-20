//! Unit tests for the `COPY` session module, split by topic.

mod binary;
mod format;
mod text;

use std::sync::Arc;

use tokio::io::{DuplexStream, duplex};
use ultrasql_catalog::TableEntry;
use ultrasql_core::{Field, Oid, Schema};
use ultrasql_txn::IsolationLevel;

use super::super::Session;
use super::fs_io::{check_copy_stdin_within_limit, copy_binary_take_limit};
use super::{CopyOptions, ServerCopyFormat, ServerError, add_copy_batch_rows, add_copy_rows};
use crate::Server;

#[test]
fn copy_stdin_cumulative_limit_is_enforced() {
    // Under the limit: OK.
    assert!(check_copy_stdin_within_limit(0, 100, 1000).is_ok());
    assert!(check_copy_stdin_within_limit(900, 100, 1000).is_ok());
    // Exactly at the limit: OK (limit is inclusive).
    assert!(check_copy_stdin_within_limit(500, 500, 1000).is_ok());
    // One byte over: rejected. This is the DoS guard — a stream of frames
    // can no longer grow the buffer without bound.
    assert!(check_copy_stdin_within_limit(900, 101, 1000).is_err());
    assert!(check_copy_stdin_within_limit(1000, 1, 1000).is_err());
    // Saturating add means a pathological length cannot wrap to a small
    // projected size and sneak past the check.
    assert!(check_copy_stdin_within_limit(usize::MAX, 1, 1000).is_err());
}

pub(super) fn copy_opts(format: ServerCopyFormat) -> CopyOptions {
    CopyOptions {
        format,
        delimiter: ',',
        null_str: "\\N".to_owned(),
        header: false,
        auto_detect: false,
        ignore_errors: false,
        max_errors: 0,
        reject_table: None,
    }
}

pub(super) fn schema(fields: impl IntoIterator<Item = Field>) -> Schema {
    Schema::new(fields).expect("test schema")
}

pub(super) fn entry_with_schema(schema: Schema) -> TableEntry {
    TableEntry::new(Oid::new(42), "copy_t", "public", schema)
}

fn test_session() -> Session<DuplexStream> {
    let (io, _peer) = duplex(64);
    Session::new(io, Arc::new(Server::with_sample_database()), None)
}

#[test]
fn copy_row_count_helpers_reject_overflow() {
    let mut rows = u64::MAX;
    let err = add_copy_rows(&mut rows, 1, "COPY test")
        .expect_err("COPY row counter overflow must not saturate");
    assert_eq!(err.sqlstate(), "22003");
    assert_eq!(rows, u64::MAX);

    let mut rows = 1;
    add_copy_batch_rows(&mut rows, 2, "COPY test").expect("small batch count");
    assert_eq!(rows, 3);
}

#[test]
fn copy_cleanup_reports_abort_failure_with_original_error() {
    let session = test_session();
    let txn = session
        .state
        .txn_manager
        .begin(IsolationLevel::ReadCommitted);
    let stale = txn.clone();
    session.state.txn_manager.abort(txn).expect("pre-abort");

    let err = session.rollback_copy_transaction_after_error(
        stale,
        ServerError::CopyFormat("row boom".to_owned()),
        "COPY FROM autocommit rollback after row error",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("COPY FROM autocommit rollback after row error"),
        "unexpected error: {err}"
    );
    assert!(msg.contains("row boom"), "original error lost: {err}");
    assert!(
        msg.contains("transaction abort failed"),
        "abort failure hidden: {err}"
    );
}

#[test]
fn copy_binary_take_limit_rejects_overflow() {
    let err = copy_binary_take_limit(u64::MAX).unwrap_err();
    assert!(err.to_string().contains("read limit is too large"));
}

#[test]
fn binary_copy_end_rejects_overflow() {
    let err = super::binary::binary_copy_end(usize::MAX, 1, 0, "binary COPY field").unwrap_err();
    assert!(err.to_string().contains("offset overflow"));
}

pub(super) fn copy_env_test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("copy env test lock")
}
