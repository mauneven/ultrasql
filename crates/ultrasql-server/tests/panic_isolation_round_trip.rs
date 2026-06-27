//! Per-connection panic-isolation tests.
//!
//! A single edge-case query that panics mid-execution must NOT take down
//! the server: with `panic = "unwind"` (Cargo.toml) plus the per-statement
//! `catch_unwind` guard in the session loop, a panic unwinds only the
//! offending statement. These tests drive a real `tokio-postgres` client
//! over the wire and assert the four properties that make the fix a fix:
//!
//! 1. PANIC ISOLATION — a deterministic mid-execution panic
//!    (`SELECT __ultrasql_test_panic()`, a debug-only trigger) returns an
//!    error to the panicking connection (not a silent drop / process exit),
//!    the server keeps running, and a *second* connection opened afterwards
//!    runs `SELECT 1` successfully.
//! 2. POISON CASCADE — after the panic, a second connection performing a
//!    catalog read / write that takes shared cross-connection locks still
//!    works (no poisoned-lock panic cascade).
//! 3. TXN ROLLBACK — a panic inside `BEGIN; <panicking stmt>` leaves no
//!    committed effects; a fresh connection sees the pre-txn state.
//! 4. NO LEAK — the client error is the generic internal-error SQLSTATE
//!    (XX000) with a generic message, never the panic payload string.
//!
//! The trigger is compiled only under `debug_assertions`, so it cannot be
//! reached in an optimised release/ship binary; the integration test build
//! is a debug build, so it is available here.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

const PANIC_SQL: &str = "SELECT __ultrasql_test_panic()";

/// `SET col = col + <delta>` value that fires the debug-only panic AFTER the
/// per-tuple Exclusive lock is acquired (see
/// `pipeline::modify::update::TEST_PANIC_AFTER_ROW_LOCK_DELTA`).
const PANIC_AFTER_ROW_LOCK_DELTA: i32 = 0x7654_3210;

/// `Int32` value whose presence in a streamed batch fires the debug-only
/// mid-stream panic (see `result_encoder::TEST_PANIC_STREAM_SENTINEL`).
const PANIC_STREAM_SENTINEL: i32 = 0x5432_1000;

/// `Int32` value whose presence in a visible base-table row fires the
/// debug-only one-shot panic during an aggregating-index rebuild (see
/// `aggregating_index::TEST_PANIC_AGG_REBUILD_SENTINEL`).
const PANIC_AGG_REBUILD_SENTINEL: i32 = 0x6543_2100;

/// Spin up one in-process server on an ephemeral port, shared across all
/// connections in a test (so the panic and the recovery hit the SAME server
/// process and the SAME shared state).
async fn start_server() -> (
    SocketAddr,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, server));
    (bound, server_handle)
}

async fn connect(bound: SocketAddr) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=panic_isolation_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    (client, handle)
}

/// Pull the SQLSTATE + message out of a tokio-postgres error, asserting it is
/// a real `DbError` (i.e. the server reported an `ErrorResponse`, it did not
/// drop the socket).
fn db_error(err: &tokio_postgres::Error) -> (String, String) {
    let db = err
        .as_db_error()
        .expect("server reported an ErrorResponse, not a dropped connection");
    (db.code().code().to_string(), db.message().to_string())
}

/// 1 + 4: a panicking query returns a generic XX000 to the client (not a
/// silent drop / not a process exit, not the panic payload), and the server
/// stays up so a fresh connection runs `SELECT 1`.
#[tokio::test]
async fn panic_in_one_query_is_isolated_and_returns_generic_internal_error() {
    let (bound, _server) = start_server().await;
    let (victim, _victim_conn) = connect(bound).await;

    let err = victim
        .simple_query(PANIC_SQL)
        .await
        .expect_err("a panicking query must surface an error, not succeed");
    let (code, message) = db_error(&err);

    // Generic internal_error code; payload NOT leaked.
    assert_eq!(code, "XX000", "caught panic maps to internal_error XX000");
    assert_eq!(
        message.to_lowercase(),
        "internal error",
        "client must receive a generic message, never the panic payload"
    );
    assert!(
        !message.contains("ultrasql test panic") && !message.contains("debug-only"),
        "panic payload string must not leak to the client: {message}"
    );

    // The server process survived: a brand-new connection works.
    let (survivor, _survivor_conn) = connect(bound).await;
    let rows = survivor
        .simple_query("SELECT 1")
        .await
        .expect("server survived the panic; a fresh connection executes normally");
    assert!(
        rows.iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))),
        "SELECT 1 returns a row after a sibling connection panicked"
    );
}

/// 1 (same-connection survival): the connection that panicked is left in a
/// clean, usable state — a follow-up query on the SAME connection succeeds.
#[tokio::test]
async fn connection_survives_its_own_panic_and_stays_usable() {
    let (bound, _server) = start_server().await;
    let (client, _conn) = connect(bound).await;

    let _ = client
        .simple_query(PANIC_SQL)
        .await
        .expect_err("panicking query errors");

    // Same connection, next statement: still alive and clean.
    let rows = client
        .simple_query("SELECT 42")
        .await
        .expect("the panicking connection is kept alive and reset to a clean state");
    let got = rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(ToOwned::to_owned),
        _ => None,
    });
    assert_eq!(got.as_deref(), Some("42"));
}

/// 2: POISON CASCADE — after a panic (which may have been holding a shared,
/// cross-connection lock such as the catalog write-lock / runtime side maps),
/// a second connection performing catalog writes + reads that take those same
/// shared locks still works. No `.lock().unwrap()` poison panic.
#[tokio::test]
async fn shared_locks_do_not_poison_cascade_after_a_panic() {
    let (bound, _server) = start_server().await;

    // Connection A panics.
    let (a, _a_conn) = connect(bound).await;
    let _ = a.simple_query(PANIC_SQL).await.expect_err("A panics");

    // Connection B exercises the shared catalog (DDL takes the catalog
    // write-lock + runtime side maps) and the heap/txn machinery (INSERT +
    // SELECT take the lock manager, CLOG, buffer pool). All of these are the
    // cross-connection shared locks a query panic could have been holding.
    let (b, _b_conn) = connect(bound).await;
    b.simple_query("CREATE TABLE poison_probe (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("catalog write-lock is not poisoned by a sibling's panic");
    b.simple_query("INSERT INTO poison_probe VALUES (1, 'a'), (2, 'b')")
        .await
        .expect("lock manager / CLOG / buffer pool are not poisoned");
    let rows = b
        .simple_query("SELECT count(*) FROM poison_probe")
        .await
        .expect("catalog read path is not poisoned");
    let count = rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(ToOwned::to_owned),
        _ => None,
    });
    assert_eq!(count.as_deref(), Some("2"), "both rows are visible");
}

/// 3: TXN ROLLBACK — a panic inside an explicit `BEGIN` aborts the block,
/// leaves no committed effects, and a fresh connection sees the pre-txn state.
#[tokio::test]
async fn panic_inside_transaction_rolls_back_with_no_committed_effects() {
    let (bound, _server) = start_server().await;

    // Setup: a table with a known baseline row, committed in autocommit.
    let (setup, _setup_conn) = connect(bound).await;
    setup
        .simple_query("CREATE TABLE txn_probe (id INT PRIMARY KEY)")
        .await
        .expect("create table");
    setup
        .simple_query("INSERT INTO txn_probe VALUES (1)")
        .await
        .expect("baseline insert commits");

    // The offending connection: BEGIN, write a row, then panic.
    let (txn, _txn_conn) = connect(bound).await;
    txn.simple_query("BEGIN").await.expect("begin");
    txn.simple_query("INSERT INTO txn_probe VALUES (2)")
        .await
        .expect("in-txn insert (uncommitted)");
    let _ = txn
        .simple_query(PANIC_SQL)
        .await
        .expect_err("panic inside the block surfaces an error");

    // The block is now Failed: a further statement is rejected with 25P02
    // until the client ends the block, proving the panic aborted the block
    // exactly like a normal in-block error.
    let after = txn
        .simple_query("SELECT 1")
        .await
        .expect_err("aborted block rejects subsequent statements");
    let (code, _) = db_error(&after);
    assert_eq!(
        code, "25P02",
        "panic aborted the explicit transaction block"
    );
    // COMMIT-as-ROLLBACK to leave the failed block; the row must not persist.
    let _ = txn.simple_query("COMMIT").await;

    // A FRESH connection sees only the pre-txn baseline (row 2 rolled back).
    let (verify, _verify_conn) = connect(bound).await;
    let rows = verify
        .simple_query("SELECT count(*) FROM txn_probe")
        .await
        .expect("fresh connection reads committed state");
    let count = rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(ToOwned::to_owned),
        _ => None,
    });
    assert_eq!(
        count.as_deref(),
        Some("1"),
        "the in-txn row was rolled back by the caught panic; only the baseline remains"
    );
}

/// 1 over the Extended Query protocol: the `catch_unwind` guard on the
/// Execute path isolates a panic just like the Simple Query path. A prepared
/// statement that panics returns a generic error and the connection survives.
#[tokio::test]
async fn panic_on_extended_execute_path_is_isolated() {
    let (bound, _server) = start_server().await;
    let (client, _conn) = connect(bound).await;

    // `query`/`execute` use Parse/Bind/Execute (the extended protocol).
    let err = client
        .query(PANIC_SQL, &[])
        .await
        .expect_err("extended-path panic surfaces an error");
    let (code, message) = db_error(&err);
    assert_eq!(code, "XX000", "extended-path caught panic maps to XX000");
    assert!(
        !message.contains("ultrasql test panic"),
        "extended-path panic payload must not leak: {message}"
    );

    // Connection survives and is usable.
    let rows = client
        .query("SELECT 7", &[])
        .await
        .expect("connection survives an extended-path panic");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 7);
}

/// BUG 1 (lock leak): an autocommit `UPDATE ... SET v = v + <delta> WHERE id = 1`
/// that PANICS *after* the per-tuple Exclusive lock is acquired must still
/// release that lock — `Transaction` has no `Drop`, so the autocommit
/// scope-guard has to abort the XID on the unwind. Otherwise the lock leaks
/// permanently and a later writer to the same row blocks until
/// `statement_timeout` (forever, with no orphan-lock reaper).
///
/// Proof: after connection A panics mid-UPDATE, connection B's
/// `UPDATE ... SET v = 2 WHERE id = 1` completes PROMPTLY (well under any
/// timeout), demonstrating the row lock was released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicking_autocommit_update_releases_its_row_lock() {
    let (bound, _server) = start_server().await;

    // Setup: an (int, int) table with a single-column index on `id` so the
    // fused-update fast path takes the indexed per-tuple Exclusive-lock branch.
    let (setup, _setup_conn) = connect(bound).await;
    setup
        .simple_query("CREATE TABLE lock_leak_probe (id INT PRIMARY KEY, v INT NOT NULL)")
        .await
        .expect("create table");
    setup
        .simple_query("INSERT INTO lock_leak_probe VALUES (1, 0)")
        .await
        .expect("seed row");

    // Connection A: autocommit UPDATE that acquires the row lock, then panics.
    let (a, _a_conn) = connect(bound).await;
    let panic_sql =
        format!("UPDATE lock_leak_probe SET v = v + {PANIC_AFTER_ROW_LOCK_DELTA} WHERE id = 1");
    let err = a
        .simple_query(&panic_sql)
        .await
        .expect_err("the UPDATE panics after acquiring the row lock");
    let (code, _) = db_error(&err);
    assert_eq!(
        code, "XX000",
        "the mid-UPDATE panic surfaces a generic XX000"
    );

    // Connection B: a competing writer to the SAME row, via the SAME fused
    // per-tuple Exclusive-lock path (`SET v = v + 1` is a fused `col + lit`
    // shape, so it contends on the identical `LockTag::Tuple(tid)` A held). If
    // A leaked its lock, this blocks on that lock until statement_timeout (we
    // arm one so the failure mode is a bounded error rather than a hang). With
    // the fix it completes promptly.
    let (b, _b_conn) = connect(bound).await;
    b.simple_query("SET statement_timeout = 4000")
        .await
        .expect("set statement_timeout");
    let write = tokio::time::timeout(
        Duration::from_secs(3),
        b.simple_query("UPDATE lock_leak_probe SET v = v + 1 WHERE id = 1"),
    )
    .await
    .expect("second writer must not hang on a leaked row lock")
    .expect("second writer's UPDATE succeeds — the panicking txn released its lock");
    assert!(
        write
            .iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))),
        "the second UPDATE completes (CommandComplete)"
    );

    // The committed value is B's `v + 1` (baseline 0 -> 1); A's panicking
    // UPDATE left no effect (its txn was aborted on the unwind).
    let (verify, _verify_conn) = connect(bound).await;
    let rows = verify
        .simple_query("SELECT v FROM lock_leak_probe WHERE id = 1")
        .await
        .expect("read back");
    let v = rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(ToOwned::to_owned),
        _ => None,
    });
    assert_eq!(
        v.as_deref(),
        Some("1"),
        "B's UPDATE committed (0 + 1); A's panicking UPDATE was rolled back"
    );
}

/// BUG 1 (streaming lock leak): a large autocommit streaming SELECT that
/// PANICS mid-stream (inside `drive_streaming_select`'s drive loop, which runs
/// OUTSIDE the per-statement `catch_unwind`) must abort its autocommit txn so
/// the read locks / CLOG state do not leak. After the panic a subsequent writer
/// proceeds and the server stays up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicking_streaming_select_aborts_its_txn() {
    let (bound, _server) = start_server().await;

    // A table whose SELECT body exceeds the streaming window high-water mark
    // (256 KiB), so the result becomes a live streaming handle and the drive
    // loop runs the panic-prone `encode_window` after window 0. A 64-byte TEXT
    // payload at ~6000 rows is ~80 bytes/row on the wire => well over 256 KiB.
    // The sentinel lives in the Int32 `id` column at a deep value so it
    // surfaces in a LATER window (inside `drive_streaming_select`), not in the
    // window-0 encode that runs under the synchronous `catch_unwind`.
    const SENTINEL_ID: i32 = PANIC_STREAM_SENTINEL;
    let (setup, _setup_conn) = connect(bound).await;
    setup
        .simple_query("CREATE TABLE stream_panic_probe (id INT PRIMARY KEY, payload TEXT NOT NULL)")
        .await
        .expect("create table");
    // Seed ~6000 ordinary rows (ids 0..6000) in chunks, plus the sentinel row.
    let payload = "x".repeat(64);
    let chunk = 500usize;
    let mut start = 0usize;
    let rows = 6000usize;
    while start < rows {
        let end = (start + chunk).min(rows);
        let mut values = String::new();
        for i in start..end {
            if i > start {
                values.push(',');
            }
            values.push_str(&format!("({i}, '{payload}')"));
        }
        setup
            .simple_query(&format!(
                "INSERT INTO stream_panic_probe (id, payload) VALUES {values}"
            ))
            .await
            .expect("bulk seed chunk");
        start = end;
    }
    // The sentinel row: a high `id` so `ORDER BY id` places it after window 0.
    setup
        .simple_query(&format!(
            "INSERT INTO stream_panic_probe (id, payload) VALUES ({SENTINEL_ID}, '{payload}')"
        ))
        .await
        .expect("plant the mid-stream panic sentinel row");

    // Connection A: streaming SELECT that panics mid-drain. The client either
    // gets a mid-stream error or a dropped result; either way the server must
    // not have leaked the autocommit txn.
    let (a, _a_conn) = connect(bound).await;
    let _ = a
        .simple_query("SELECT id, payload FROM stream_panic_probe ORDER BY id")
        .await;

    // The server survived and no txn/lock leaked: a fresh connection writes the
    // sentinel row (taking its Exclusive lock) and reads back, promptly.
    let (b, _b_conn) = connect(bound).await;
    b.simple_query("SET statement_timeout = 4000")
        .await
        .expect("set statement_timeout");
    let write = tokio::time::timeout(
        Duration::from_secs(3),
        b.simple_query(&format!(
            "UPDATE stream_panic_probe SET payload = 'ok' WHERE id = {SENTINEL_ID}"
        )),
    )
    .await
    .expect("a write after a streaming panic must not hang on a leaked lock")
    .expect("the write proceeds — the streaming autocommit txn was aborted");
    assert!(
        write
            .iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))),
        "post-streaming-panic write completes"
    );
}

/// BUG 2 (torn dirty/rows): a panic DURING an aggregating-index rebuild must
/// re-dirty the summary (via the `DirtyRestore` guard) so the NEXT read rebuilds
/// from heap truth — it must NOT serve the stale pre-write summary that omits
/// the committed row which dirtied it.
///
/// Flow: warm a clean summary, COMMIT an INSERT that both dirties the summary
/// and plants the rebuild panic sentinel, then read the rollup. The first
/// rebuild panics (caught, isolated); the guard re-dirties; a second read
/// rebuilds successfully and returns the CORRECT up-to-date aggregate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicking_aggregating_index_rebuild_serves_truth_not_stale() {
    let (bound, _server) = start_server().await;

    let (a, _a_conn) = connect(bound).await;
    a.simple_query(
        "CREATE TABLE agg_panic_probe (\
            tenant_id INT NOT NULL, \
            bucket INT NOT NULL, \
            amount BIGINT NOT NULL)",
    )
    .await
    .expect("create table");
    a.simple_query("INSERT INTO agg_panic_probe VALUES (7, 1, 10), (7, 1, 20), (7, 2, 5)")
        .await
        .expect("seed");
    a.simple_query(
        "CREATE AGGREGATING INDEX agg_panic_rollup \
         ON agg_panic_probe (tenant_id, bucket, sum(amount), count(*))",
    )
    .await
    .expect("create aggregating index");

    // Warm a clean summary (the rollup over the seed rows).
    let rollup_sql = "SELECT tenant_id, bucket, SUM(amount), COUNT(*) \
                      FROM agg_panic_probe \
                      WHERE tenant_id = 7 \
                      GROUP BY tenant_id, bucket \
                      ORDER BY tenant_id, bucket";
    let baseline = a.query(rollup_sql, &[]).await.expect("warm summary");
    assert_eq!(baseline.len(), 2, "two groups before the dirtying write");

    // Commit a write that (a) dirties the summary and (b) plants the rebuild
    // panic sentinel as `bucket` of a new committed row in a NEW group so the
    // correct post-rebuild aggregate is observably different from the stale one.
    // The row commits FIRST; the post-commit aggregating-index maintenance then
    // claims the dirty rebuild and PANICS mid-build on the sentinel row. The
    // per-statement guard isolates it (the client sees a generic XX000), the
    // committed row survives in the heap, and the `DirtyRestore` guard must
    // have re-dirtied the summary so it is never left torn (dirty == false with
    // stale rows that omit the just-committed row).
    let err = a
        .simple_query(&format!(
            "INSERT INTO agg_panic_probe VALUES (7, {PANIC_AGG_REBUILD_SENTINEL}, 100)"
        ))
        .await
        .expect_err("the post-commit rebuild panics mid-build on the sentinel row");
    let (code, _) = db_error(&err);
    assert_eq!(code, "XX000", "the rebuild panic surfaces a generic XX000");

    // Read from a FRESH connection to rule out per-connection caching: the
    // `DirtyRestore` guard re-dirtied, so this rebuild runs from heap truth (the
    // one-shot sentinel no longer panics) and returns the CORRECT aggregate
    // INCLUDING the committed sentinel-bucket row — never the stale 2-group
    // summary that omits it.
    let (b, _b_conn) = connect(bound).await;
    let recovered: Vec<(i32, i32, i64, i64)> = b
        .query(rollup_sql, &[])
        .await
        .expect("the next read rebuilds from heap truth, not a torn stale summary")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect();
    assert_eq!(
        recovered,
        vec![
            (7, 1, 30, 2),
            (7, 2, 5, 1),
            (7, PANIC_AGG_REBUILD_SENTINEL, 100, 1),
        ],
        "after the panicking rebuild the summary is rebuilt to heap truth — the \
         committed sentinel-bucket row is present, proving the dirty flag was \
         restored and no stale aggregate was served"
    );
}

/// BUG 1 (Extended-Query autocommit lock leak): the Extended-Query Execute
/// path opens its OWN autocommit `Transaction` (in `run_portal_routed`), a
/// second leak site distinct from the Simple-Query path. An autocommit
/// fused-UPDATE driven over the extended protocol that panics after acquiring
/// the row lock must still release that lock — the Extended-Execute
/// `AutocommitAbortGuard` aborts the XID on the unwind. A later writer to the
/// same row then proceeds promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicking_extended_autocommit_update_releases_its_row_lock() {
    let (bound, _server) = start_server().await;

    let (setup, _setup_conn) = connect(bound).await;
    setup
        .simple_query("CREATE TABLE ext_lock_leak_probe (id INT PRIMARY KEY, v INT NOT NULL)")
        .await
        .expect("create table");
    setup
        .simple_query("INSERT INTO ext_lock_leak_probe VALUES (1, 0)")
        .await
        .expect("seed row");

    // Connection A: the EXTENDED protocol (`query`, i.e. Parse/Bind/Execute)
    // drives an autocommit UPDATE that acquires the row lock, then panics.
    let (a, _a_conn) = connect(bound).await;
    let panic_sql =
        format!("UPDATE ext_lock_leak_probe SET v = v + {PANIC_AFTER_ROW_LOCK_DELTA} WHERE id = 1");
    let err = a
        .query(&panic_sql, &[])
        .await
        .expect_err("the extended-path UPDATE panics after acquiring the row lock");
    let (code, _) = db_error(&err);
    assert_eq!(
        code, "XX000",
        "the mid-UPDATE panic on the extended path surfaces a generic XX000"
    );

    // Connection B: competing writer on the SAME row via the SAME fused lock
    // path. Completes promptly iff A's lock was released by the guard.
    let (b, _b_conn) = connect(bound).await;
    b.simple_query("SET statement_timeout = 4000")
        .await
        .expect("set statement_timeout");
    let write = tokio::time::timeout(
        Duration::from_secs(3),
        b.simple_query("UPDATE ext_lock_leak_probe SET v = v + 1 WHERE id = 1"),
    )
    .await
    .expect("second writer must not hang on a leaked row lock from the extended path")
    .expect("second writer's UPDATE succeeds — the extended autocommit txn released its lock");
    assert!(
        write
            .iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))),
        "the second UPDATE completes (CommandComplete)"
    );

    let (verify, _verify_conn) = connect(bound).await;
    let rows = verify
        .simple_query("SELECT v FROM ext_lock_leak_probe WHERE id = 1")
        .await
        .expect("read back");
    let v = rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(ToOwned::to_owned),
        _ => None,
    });
    assert_eq!(
        v.as_deref(),
        Some("1"),
        "B's UPDATE committed (0 + 1); A's panicking extended-path UPDATE was rolled back"
    );
}
