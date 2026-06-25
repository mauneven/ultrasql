//! `COPY` transaction-participation round-trip tests.
//!
//! These pin the ACID contract that `COPY` now participates in the SESSION
//! transaction rather than opening its own autocommit txn:
//!
//! - `BEGIN; COPY ...; ROLLBACK` discards every COPYed row (text + binary +
//!   file), and `BEGIN; COPY ...; COMMIT` makes them durable atomically.
//! - `COPY ... TO` inside a transaction sees this session's own uncommitted
//!   writes (an in-txn `INSERT`).
//! - A mid-stream COPY error inside a block transitions it to `Failed` (next
//!   statement `25P02`); ROLLBACK then discards everything, and `COMMIT` of a
//!   failed block behaves as ROLLBACK.
//! - A `COPY` issued in an already-`Failed` block is rejected `25P02` without
//!   opening a fresh txn or landing any rows.
//! - The deferred PRIMARY KEY index built at COMMIT sees the COPYed rows.
//! - Autocommit `COPY` stays atomic on a mid-stream error.

use bytes::Bytes;
use futures::SinkExt;

pub mod support;

use support::{connect_as, shutdown, start_persistent_server, start_sample_server};

/// `SELECT COUNT(*) FROM <table>` via simple-query.
async fn select_count(client: &tokio_postgres::Client, table: &str) -> i64 {
    let rows = client
        .simple_query(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .expect("count query");
    rows.into_iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                row.get(0).map(|c| c.parse::<i64>().expect("count parses"))
            }
            _ => None,
        })
        .expect("COUNT(*) returned a row")
}

/// Row count via an ORDER-BY column scan, which reads the heap directly and is
/// therefore unaffected by the cached fast paths (`COUNT(*)` scalar-aggregate
/// cache and the bare single-column projection cache) that a plain
/// `SELECT col FROM t` can hit. This is the authoritative MVCC view: after a
/// ROLLBACK the aborted rows are heap-invisible, so this returns the true
/// post-rollback count.
///
/// (A separate, pre-existing bug — tracked as a follow-up — leaves those cached
/// fast paths stale after a full ROLLBACK; it affects plain INSERT identically
/// and is out of scope for COPY transaction participation. The ORDER BY here
/// sidesteps it so these tests assert the real heap/MVCC outcome.)
async fn select_scan_count(client: &tokio_postgres::Client, col: &str, table: &str) -> usize {
    client
        .query(&format!("SELECT {col} FROM {table} ORDER BY {col}"), &[])
        .await
        .expect("ordered column scan")
        .len()
}

/// Stream `payload` into `sql` (a `COPY ... FROM STDIN`) and finish cleanly,
/// returning the reported row count.
async fn copy_in_payload(client: &tokio_postgres::Client, sql: &str, payload: &[u8]) -> u64 {
    let sink = client
        .copy_in::<_, Bytes>(sql)
        .await
        .expect("copy_in establishes COPY FROM STDIN");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from(payload.to_vec()))
        .await
        .expect("send CopyData");
    sink.finish().await.expect("finish copy_in")
}

fn is_in_failed_txn(err: &tokio_postgres::Error) -> bool {
    err.code().map(|c| c.code() == "25P02").unwrap_or(false)
}

fn is_undefined_table(err: &tokio_postgres::Error) -> bool {
    err.code().map(|c| c.code() == "42P01").unwrap_or(false)
}

fn pg_binary_copy_header(out: &mut Vec<u8>) {
    out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    out.extend_from_slice(&0_i32.to_be_bytes());
    out.extend_from_slice(&0_i32.to_be_bytes());
}

/// Build a binary `PGCOPY` payload for a single-column `INT` table.
fn binary_int_payload(values: &[i32]) -> Vec<u8> {
    let mut out = Vec::new();
    pg_binary_copy_header(&mut out);
    for v in values {
        out.extend_from_slice(&1_i16.to_be_bytes()); // field count
        out.extend_from_slice(&4_i32.to_be_bytes()); // field length
        out.extend_from_slice(&v.to_be_bytes());
    }
    out.extend_from_slice(&(-1_i16).to_be_bytes()); // trailer
    out
}

// ───────────────────────── #1 text ROLLBACK ─────────────────────────
#[tokio::test]
async fn copy_text_from_stdin_rolled_back_leaves_zero_rows() {
    let running = start_sample_server("copy_txn_text_rb").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_text_rb (id INT, label TEXT)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("begin");
    let copied = copy_in_payload(
        client,
        "COPY t_text_rb (id, label) FROM STDIN WITH (FORMAT csv)",
        b"1,alpha\n2,bravo\n",
    )
    .await;
    assert_eq!(copied, 2);
    // Self-visible inside the txn (column scan — does not poison the
    // scalar-aggregate cache with the uncommitted count).
    assert_eq!(select_scan_count(client, "id", "t_text_rb").await, 2);
    client.batch_execute("ROLLBACK").await.expect("rollback");

    assert_eq!(
        select_scan_count(client, "id", "t_text_rb").await,
        0,
        "ROLLBACK must discard COPYed rows"
    );
    shutdown(running).await;
}

// ───────────────────────── #1 binary ROLLBACK ─────────────────────────
#[tokio::test]
async fn copy_binary_from_stdin_rolled_back_leaves_zero_rows() {
    let running = start_sample_server("copy_txn_bin_rb").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_bin_rb (id INT)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("begin");
    let copied = copy_in_payload(
        client,
        "COPY t_bin_rb FROM STDIN WITH (FORMAT binary)",
        &binary_int_payload(&[10, 20, 30]),
    )
    .await;
    assert_eq!(copied, 3);
    // Note: we deliberately do NOT read `t_bin_rb` inside the transaction here.
    // A single-column INT scan inside the txn warms a server-global projection
    // cache that a full ROLLBACK does not currently invalidate (a pre-existing
    // bug, identical for plain INSERT, tracked as a follow-up). Self-visibility
    // of an in-txn COPY is covered by the text/insert tests; this test isolates
    // the post-ROLLBACK heap/MVCC outcome, which is the COPY-atomicity contract.
    client.batch_execute("ROLLBACK").await.expect("rollback");

    assert_eq!(
        select_scan_count(client, "id", "t_bin_rb").await,
        0,
        "ROLLBACK must discard binary-COPYed rows"
    );
    shutdown(running).await;
}

// ───────────────────────── #2 file ROLLBACK ─────────────────────────
#[tokio::test]
async fn copy_from_file_rolled_back_leaves_zero_rows() {
    let dir = tempfile::TempDir::new().unwrap();
    let csv_path = dir.path().join("rb.csv");
    std::fs::write(&csv_path, b"1,alpha\n2,bravo\n3,charlie\n").expect("write csv");

    let running = start_sample_server("copy_txn_file_rb").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_file_rb (id INT, label TEXT)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute(&format!(
            "COPY t_file_rb (id, label) FROM '{}' WITH (FORMAT csv)",
            csv_path.to_str().expect("utf8 path")
        ))
        .await
        .expect("copy from file in-txn");
    assert_eq!(select_scan_count(client, "id", "t_file_rb").await, 3);
    client.batch_execute("ROLLBACK").await.expect("rollback");

    assert_eq!(
        select_scan_count(client, "id", "t_file_rb").await,
        0,
        "ROLLBACK must discard file-COPYed rows"
    );
    shutdown(running).await;
}

// ───────────────────────── #3 COMMIT persists ─────────────────────────
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_from_stdin_committed_persists_on_fresh_connection() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "copy_txn_commit").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_commit (id INT, label TEXT)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("begin");
    let copied = copy_in_payload(
        client,
        "COPY t_commit (id, label) FROM STDIN WITH (FORMAT csv)",
        b"1,alpha\n2,bravo\n",
    )
    .await;
    assert_eq!(copied, 2);
    client.batch_execute("COMMIT").await.expect("commit");

    // Fresh connection sees the committed rows.
    let (client_b, b_handle) = connect_as(running.bound, "tester", "copy_txn_commit_b").await;
    assert_eq!(select_count(&client_b, "t_commit").await, 2);
    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────── #4 COPY TO sees in-txn INSERT ─────────────────────────
#[tokio::test]
async fn copy_to_stdout_sees_in_txn_insert() {
    use futures::StreamExt;
    let running = start_sample_server("copy_txn_to_sees_insert").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_to (id INT, label TEXT)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t_to VALUES (1, 'pre')")
        .await
        .expect("seed row");

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("INSERT INTO t_to VALUES (2, 'mid')")
        .await
        .expect("in-txn insert");

    // COPY t TO STDOUT must see BOTH the pre-existing row and the in-txn one.
    let stream = client
        .copy_out("COPY t_to TO STDOUT WITH (FORMAT csv)")
        .await
        .expect("copy_out");
    let mut stream = Box::pin(stream);
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.expect("CopyData chunk"));
    }
    let text = String::from_utf8(out).expect("utf8");
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2, "COPY TO sees pre + in-txn rows: {text:?}");
    assert!(text.contains("mid"), "COPY TO sees the in-txn INSERT");

    // COPY (SELECT ...) TO STDOUT must see the in-txn INSERT too.
    let stream = client
        .copy_out("COPY (SELECT label FROM t_to WHERE id = 2) TO STDOUT")
        .await
        .expect("copy_out query");
    let mut stream = Box::pin(stream);
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.expect("CopyData chunk"));
    }
    assert_eq!(String::from_utf8(out).expect("utf8").trim(), "mid");

    client.batch_execute("ROLLBACK").await.expect("rollback");
    shutdown(running).await;
}

// ───────────────────────── #5 INSERT + COPY both counted ─────────────────────────
#[tokio::test]
async fn in_txn_insert_then_copy_count_reflects_both() {
    let running = start_sample_server("copy_txn_insert_copy").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_both (id INT)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("INSERT INTO t_both VALUES (1)")
        .await
        .expect("in-txn insert");
    let copied = copy_in_payload(client, "COPY t_both FROM STDIN", b"2\n3\n").await;
    assert_eq!(copied, 2);
    assert_eq!(
        select_count(client, "t_both").await,
        3,
        "count reflects the INSERT and the COPY together"
    );
    client.batch_execute("COMMIT").await.expect("commit");
    assert_eq!(select_count(client, "t_both").await, 3);
    shutdown(running).await;
}

// ───────────────────────── #6 mid-stream error -> Failed ─────────────────────────
#[tokio::test]
async fn copy_midstream_error_aborts_block_then_rolls_back() {
    let running = start_sample_server("copy_txn_midstream").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_mid (id INT NOT NULL)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("begin");
    // A NULL into a NOT NULL column mid-stream fails the COPY.
    let sink = client
        .copy_in::<_, Bytes>("COPY t_mid FROM STDIN")
        .await
        .expect("copy_in establishes");
    futures::pin_mut!(sink);
    // `\N` is the text-format NULL token; into NOT NULL id it errors.
    sink.as_mut()
        .send(Bytes::from_static(b"1\n\\N\n3\n"))
        .await
        .expect("send CopyData");
    let copy_err = sink.finish().await.expect_err("COPY must fail on NOT NULL");
    // The COPY decoder rejects the `\N` into NOT NULL as a bad COPY value
    // (SQLSTATE 22P04, bad_copy_file_format). The exact code is secondary; the
    // block-abort + zero-rows invariants below are the contract under test.
    assert_eq!(copy_err.code().map(|c| c.code()), Some("22P04"));

    // The block is now Failed: the next statement gets 25P02.
    let err = client
        .simple_query("SELECT 1")
        .await
        .expect_err("next stmt in failed block is rejected");
    assert!(is_in_failed_txn(&err), "expected 25P02, got {err}");

    // ROLLBACK clears the block; zero rows landed.
    client.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(
        select_scan_count(client, "id", "t_mid").await,
        0,
        "no rows survive a failed in-txn COPY + ROLLBACK"
    );

    // And COMMIT-as-rollback on a failed block also discards everything.
    client.batch_execute("BEGIN").await.expect("begin 2");
    let sink = client
        .copy_in::<_, Bytes>("COPY t_mid FROM STDIN")
        .await
        .expect("copy_in establishes 2");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from_static(b"7\n\\N\n"))
        .await
        .expect("send CopyData 2");
    let _ = sink.finish().await.expect_err("COPY must fail 2");
    // COMMIT of a failed block is treated as ROLLBACK.
    client
        .batch_execute("COMMIT")
        .await
        .expect("commit-as-rollback");
    assert_eq!(
        select_scan_count(client, "id", "t_mid").await,
        0,
        "COMMIT of a failed block discards the COPY rows"
    );
    shutdown(running).await;
}

// ───────────────────────── #7 COPY in already-Failed block -> 25P02 ─────────────────────────
#[tokio::test]
async fn copy_in_already_failed_block_is_rejected_without_landing_rows() {
    let running = start_sample_server("copy_txn_failed_block").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_failed (id INT)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("begin");
    // Force the block into Failed with a bad statement.
    let err = client
        .simple_query("SELECT * FROM no_such_table_zzz")
        .await
        .expect_err("undefined table aborts the block");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");

    // A COPY now must be rejected 25P02 — NOT run against a fresh autocommit txn.
    // Drive it over the Simple Query protocol (`batch_execute`): the guard sits
    // in `run_copy_inner`, reached identically from both the Simple and Extended
    // dispatchers. (The Extended `copy_in` client reacts to a pre-CopyInResponse
    // ErrorResponse by closing the connection — a tokio-postgres quirk, not a
    // server fault; the Simple path is the faithful way to assert the
    // rejection.)
    let copy_err = client
        .batch_execute("COPY t_failed FROM STDIN")
        .await
        .expect_err("COPY in a failed block must be rejected");
    assert!(
        is_in_failed_txn(&copy_err),
        "expected 25P02, got {copy_err}"
    );

    client.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(
        select_scan_count(client, "id", "t_failed").await,
        0,
        "no rows landed from a COPY rejected in a failed block"
    );
    shutdown(running).await;
}

// ───────────────────────── #10 deferred PK index sees COPY rows ─────────────────────────
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_into_in_txn_pk_table_builds_index_over_copy_rows() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "copy_txn_pk").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE t_pk (id INT PRIMARY KEY)")
        .await
        .expect("in-txn create with PK");
    let copied = copy_in_payload(client, "COPY t_pk FROM STDIN", b"1\n2\n3\n").await;
    assert_eq!(copied, 3);
    // The deferred PK index is built at COMMIT, scanning the txn snapshot — it
    // must see the COPYed rows. A clean commit proves the build succeeded.
    client
        .batch_execute("COMMIT")
        .await
        .expect("commit builds the PK index over the COPY rows");

    // The PK is probe-able post-commit (unique lookup).
    let rows = client
        .query("SELECT id FROM t_pk WHERE id = 2", &[])
        .await
        .expect("PK probe");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 2);

    // A duplicate key is rejected post-commit (the index is live + enforcing).
    let dup = client
        .batch_execute("INSERT INTO t_pk VALUES (2)")
        .await
        .expect_err("duplicate PK must be rejected");
    assert_eq!(dup.code().map(|c| c.code()), Some("23505"));

    // Fresh connection sees the committed rows.
    let (client_b, b_handle) = connect_as(running.bound, "tester", "copy_txn_pk_b").await;
    assert_eq!(select_count(&client_b, "t_pk").await, 3);
    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────── #10b deferred PK detects COPY duplicate ─────────────────────────
// The strongest atomicity proof: a duplicate among the COPYed rows must fail
// the COMMIT's deferred index build (23505) and abort the WHOLE transaction
// (table + rows gone) — impossible if COPY had committed its own txn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_duplicate_pk_fails_commit_index_build_atomically() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "copy_txn_pk_dup").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("CREATE TABLE t_pk_dup (id INT PRIMARY KEY)")
        .await
        .expect("in-txn create with PK");
    // Two rows with id=5 — the deferred PK build at COMMIT must reject these.
    let copied = copy_in_payload(client, "COPY t_pk_dup FROM STDIN", b"5\n5\n").await;
    assert_eq!(copied, 2);
    let err = client
        .batch_execute("COMMIT")
        .await
        .expect_err("duplicate PK among COPY rows must fail the COMMIT index build");
    assert_eq!(err.code().map(|c| c.code()), Some("23505"));

    // The whole txn rolled back: table + rows gone.
    let err = client
        .query("SELECT 1 FROM t_pk_dup", &[])
        .await
        .expect_err("table must be gone after failed COMMIT");
    assert!(is_undefined_table(&err), "expected 42P01, got {err}");
    shutdown(running).await;
}

// ───────────────────────── #11 autocommit atomic on mid-stream error ─────────────────────────
#[tokio::test]
async fn autocommit_copy_midstream_error_lands_zero_rows() {
    let running = start_sample_server("copy_txn_autocommit_atomic").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_auto (id INT NOT NULL)")
        .await
        .expect("create table");

    // No BEGIN: autocommit. A mid-stream NOT NULL error must roll back the
    // single implicit txn, leaving zero rows.
    let sink = client
        .copy_in::<_, Bytes>("COPY t_auto FROM STDIN")
        .await
        .expect("copy_in establishes");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from_static(b"1\n\\N\n3\n"))
        .await
        .expect("send CopyData");
    let err = sink.finish().await.expect_err("autocommit COPY must fail");
    assert_eq!(err.code().map(|c| c.code()), Some("22P04"));

    // The connection is NOT in a failed block (autocommit), so the next
    // statement runs normally and sees zero rows.
    assert_eq!(
        select_count(client, "t_auto").await,
        0,
        "autocommit COPY is atomic: zero rows after mid-stream error"
    );
    shutdown(running).await;
}

// ───────────────────────── #13 large COPY (> batch flush) ROLLBACK then COMMIT ─────────────────────────
// More than COPY_INSERT_BATCH_ROWS (4096) rows forces a mid-stream page flush.
// The flush must NOT durably-commit the InProgress session xid: ROLLBACK still
// yields 0, and a subsequent COMMIT of the same volume persists everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_copy_rollback_then_commit_is_atomic() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "copy_txn_large").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_large (id INT)")
        .await
        .expect("create table");

    const N: i32 = 5000; // > 4096 batch size -> at least one mid-stream flush
    let mut payload = Vec::new();
    for i in 0..N {
        payload.extend_from_slice(format!("{i}\n").as_bytes());
    }

    // ROLLBACK leg. (No in-txn self-read of this single-column INT table — see
    // the binary-rollback test for why that would warm a ROLLBACK-stale cache.)
    client.batch_execute("BEGIN").await.expect("begin");
    let copied = copy_in_payload(client, "COPY t_large FROM STDIN", &payload).await;
    assert_eq!(copied, N as u64);
    client.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(
        select_scan_count(client, "id", "t_large").await,
        0,
        "ROLLBACK discards all rows even after a mid-stream page flush"
    );

    // COMMIT leg — same volume now persists.
    client.batch_execute("BEGIN").await.expect("begin 2");
    let copied = copy_in_payload(client, "COPY t_large FROM STDIN", &payload).await;
    assert_eq!(copied, N as u64);
    client.batch_execute("COMMIT").await.expect("commit");
    assert_eq!(select_scan_count(client, "id", "t_large").await, N as usize);

    // Durable on a fresh connection.
    let (client_b, b_handle) = connect_as(running.bound, "tester", "copy_txn_large_b").await;
    assert_eq!(select_count(&client_b, "t_large").await, i64::from(N));
    drop(client_b);
    let _ = b_handle.await;
    shutdown(running).await;
}

// ───────────────────────── #14 column list / CSV options ROLLBACK ─────────────────────────
#[tokio::test]
async fn copy_with_column_list_and_csv_options_rolled_back_leaves_zero() {
    let running = start_sample_server("copy_txn_collist_csv").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_cl (a INT, b TEXT, c INT)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("begin");
    // Explicit column list (a, c) + CSV with a custom delimiter + header.
    let copied = copy_in_payload(
        client,
        "COPY t_cl (a, c) FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER true)",
        b"a;c\n1;100\n2;200\n",
    )
    .await;
    assert_eq!(copied, 2);
    // The unlisted column b defaulted to NULL for the in-txn rows (column scan
    // — also confirms self-visibility without poisoning the aggregate cache).
    let rows = client
        .query("SELECT a, b, c FROM t_cl ORDER BY a", &[])
        .await
        .expect("self select");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert!(rows[0].get::<_, Option<&str>>(1).is_none());
    assert_eq!(rows[0].get::<_, i32>(2), 100);

    client.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(
        select_scan_count(client, "a", "t_cl").await,
        0,
        "ROLLBACK discards column-list/CSV-option COPY rows"
    );
    shutdown(running).await;
}

// ─────────── #15 take-and-park contract: an in-session COPY error never
//                drops the txn to Idle (the session stays a real Failed block) ───────────
//
// Direct regression for the take-and-park state-machine bug: in session-mode
// COPY the txn is moved out of `self.txn_state` into an owned local and parked
// back afterwards. If an error between take-out and park-back dropped that
// local, `self.txn_state` would be left `Idle` — the session would SILENTLY
// lose its transaction, subsequent statements would run autocommit, and the
// COPY rows' xid/locks would leak.
//
// This test pins the *distinguishing* observable between the two outcomes:
//
//   - CORRECT (`Failed(txn)`): the next write is rejected `25P02`, and it does
//     NOT persist after a ROLLBACK clears the block.
//   - BUGGY  (`Idle`, txn dropped): the next write would run in a *fresh
//     autocommit txn*, succeed, and durably land a row.
//
// A successful write that survives a ROLLBACK is the smoking gun for a dropped
// txn, so we assert the opposite. We trigger the in-session COPY error via the
// already-covered mid-stream `\N`-into-NOT-NULL path (which routes through the
// park-as-`Failed` finaliser); the contract under test is the post-error state,
// not the trigger.
#[tokio::test]
async fn copy_in_session_error_parks_failed_never_drops_txn_to_idle() {
    let running = start_sample_server("copy_txn_no_idle_drop").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE t_park (id INT NOT NULL)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("begin");
    let sink = client
        .copy_in::<_, Bytes>("COPY t_park FROM STDIN")
        .await
        .expect("copy_in establishes");
    futures::pin_mut!(sink);
    // `\N` into NOT NULL id fails the COPY mid-stream.
    sink.as_mut()
        .send(Bytes::from_static(b"1\n\\N\n3\n"))
        .await
        .expect("send CopyData");
    let _ = sink.finish().await.expect_err("COPY must fail on NOT NULL");

    // If the txn had been dropped to Idle, this INSERT would run autocommit and
    // succeed. The take-and-park contract requires the block to be Failed, so it
    // MUST be rejected 25P02.
    let insert_err = client
        .batch_execute("INSERT INTO t_park VALUES (42)")
        .await
        .expect_err("write in a failed block must be rejected, not autocommitted");
    assert!(
        is_in_failed_txn(&insert_err),
        "expected 25P02 (Failed block), got {insert_err} — \
         the session txn was silently dropped to Idle"
    );

    // Clearing the block must discard everything: neither the COPY rows nor the
    // would-be-autocommitted INSERT may survive. A surviving row 42 would be the
    // signature of a dropped-to-Idle txn.
    client.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(
        select_scan_count(client, "id", "t_park").await,
        0,
        "no rows survive a failed in-txn COPY + rejected write + ROLLBACK \
         (a surviving row means the txn leaked to autocommit)"
    );
    shutdown(running).await;
}
