//! Round-trip coverage for the structured `ErrorResponse` field set.
//!
//! PostgreSQL transmits errors as a tagged field list: `S` (localized
//! severity), `V` (non-localized severity), `C` (SQLSTATE), `M`
//! (primary message), then optional `D` (detail) and `H` (hint).
//! UltraSQL historically sent only `S`/`C`/`M` and jammed advice into
//! the message text as a `\nHINT:` line. These tests pin the psql- and
//! driver-visible contract: the hint arrives in the dedicated `H`
//! field (surfaced by `tokio_postgres::error::DbError::hint()`), the
//! primary message no longer contains the jammed text, and both
//! severity fields carry `ERROR`.

pub mod support;

use support::start_persistent_server;
use tokio_postgres::error::Severity;

/// The transactional-DDL 0A000 rejection is the canonical hint carrier:
/// a `SERIAL` column inside an explicit transaction block is still out
/// of scope, and PostgreSQL convention is to route the user to a
/// workaround via HINT rather than inside the message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ddl_in_txn_rejection_carries_structured_hint() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "err_fields_hint").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    let err = client
        .batch_execute("CREATE TABLE err_fields_t (id SERIAL)")
        .await
        .expect_err("SERIAL create table in txn is still rejected");
    let db = err.as_db_error().expect("wire error carries DbError");

    assert_eq!(db.code().code(), "0A000", "feature_not_supported");
    assert_eq!(db.severity(), "ERROR", "S field");
    assert_eq!(
        db.parsed_severity(),
        Some(Severity::Error),
        "V field parses as the non-localized severity"
    );
    assert_eq!(
        db.message(),
        "DDL inside an explicit transaction block is not yet supported",
        "primary message no longer jams the hint"
    );
    let hint = db.hint().expect("hint travels in the H field");
    assert!(
        hint.contains("autocommit"),
        "hint routes the user to autocommit: {hint}"
    );
    assert!(
        !db.message().contains("HINT"),
        "no jammed HINT text remains in M"
    );
    assert!(db.detail().is_none(), "no detail is invented");

    client.batch_execute("ROLLBACK").await.expect("rollback");
    support::shutdown(running).await;
}

/// Hint-less errors keep a clean field set: `S`/`V`/`C`/`M` only, with
/// no empty `H`/`D` fields invented.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_errors_have_severity_fields_and_no_hint() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "err_fields_plain").await;
    let client = &running.client;

    let err = client
        .query("SELECT * FROM err_fields_missing_table", &[])
        .await
        .expect_err("undefined table");
    let db = err.as_db_error().expect("wire error carries DbError");

    assert_eq!(db.code().code(), "42P01", "undefined_table");
    assert_eq!(db.severity(), "ERROR");
    assert_eq!(db.parsed_severity(), Some(Severity::Error));
    assert!(db.hint().is_none(), "no hint invented");
    assert!(db.detail().is_none(), "no detail invented");

    support::shutdown(running).await;
}

/// The extended (prepared) protocol path routes errors through the same
/// structured encoder as the simple-query path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_protocol_error_carries_structured_hint() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "err_fields_ext").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    // `query` drives Parse/Bind/Execute — the extended path.
    let err = client
        .query("CREATE TABLE err_fields_ext_t (id SERIAL)", &[])
        .await
        .expect_err("SERIAL create table in txn is still rejected");
    let db = err.as_db_error().expect("wire error carries DbError");

    assert_eq!(db.code().code(), "0A000");
    assert_eq!(db.parsed_severity(), Some(Severity::Error));
    assert!(
        db.hint().is_some_and(|h| h.contains("autocommit")),
        "extended path surfaces the hint field too"
    );

    client.batch_execute("ROLLBACK").await.expect("rollback");
    support::shutdown(running).await;
}
