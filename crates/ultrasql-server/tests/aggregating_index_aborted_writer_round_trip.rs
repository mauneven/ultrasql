//! Regression tests: the runtime aggregating index must never serve an
//! **aborted** writer's summary as a phantom aggregate.
//!
//! `RuntimeAggregatingIndex` records, at `mark_dirty` time, the XID of the
//! transaction whose write dirtied the summary (`last_writer_xid`). The serve
//! path (`try_lower_aggregating_index_project` → `current_summary_rows` →
//! `rebuild_runtime_for_snapshot`) rebuilds a dirty summary, then gates the
//! clean summary behind `summary_servable_to(snapshot, oracle)`:
//!
//! ```text
//! W == INVALID || snapshot.is_current_xid(W)
//!   || (!snapshot.xid_in_progress(W) && oracle.is_committed(W))
//! ```
//!
//! This mirrors the column-cache coherence gate (commit b4d3e302) exactly and
//! uses the SAME `XidStatusOracle` (`TransactionManager`) the heap visibility
//! path consults.
//!
//! The hole these tests pin: an in-txn read-after-write GROUP BY warms the
//! summary from the writer's OWN uncommitted snapshot; if the writer then
//! ABORTS (plain `ROLLBACK`, `ROLLBACK TO SAVEPOINT`, `ROLLBACK PREPARED`, or
//! an SSI force-abort), no path re-dirties the summary, so a fresh reader used
//! to get the rolled-back writer's aggregate (phantom rows/sums). The gate now
//! forces a fresh rebuild from committed heap truth for any non-servable
//! writer.
//!
//! The SSI force-abort flavor is proven directly at the gate by the
//! `aborted_writer_summary_is_rejected` unit test in `runtime_index.rs` (it is
//! agnostic to *how* the writer reached `XidStatus::Aborted`); driving an SSI
//! dangerous-structure abort that also warms the summary over the wire is
//! fragile, so it is covered there rather than here.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start() -> (
    Arc<Server>,
    String,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, Arc::clone(&server)));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=aggidx_aborted",
        host = bound.ip(),
        port = bound.port()
    );
    (server, conn_str, server_handle)
}

async fn connect(conn_str: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// `GROUP BY` rollup over the aggregating-index shape for `tenant_id = 7`.
async fn rollup(client: &tokio_postgres::Client) -> Vec<(i32, i32, i64, i64)> {
    client
        .query(
            "SELECT tenant_id, bucket, SUM(amount), COUNT(*) \
             FROM fact_events \
             WHERE tenant_id = 7 \
             GROUP BY tenant_id, bucket \
             ORDER BY tenant_id, bucket",
            &[],
        )
        .await
        .expect("rollup")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect()
}

const TRUTH: &[(i32, i32, i64, i64)] = &[(7, 1, 30, 2), (7, 2, 5, 1)];

async fn setup(c: &tokio_postgres::Client) {
    c.batch_execute(
        "CREATE TABLE fact_events (
            tenant_id INT NOT NULL,
            bucket INT NOT NULL,
            amount BIGINT NOT NULL
         )",
    )
    .await
    .expect("create table");
    c.batch_execute(
        "INSERT INTO fact_events VALUES
            (7, 1, 10),
            (7, 1, 20),
            (7, 2, 5)",
    )
    .await
    .expect("seed");
    c.batch_execute(
        "CREATE AGGREGATING INDEX fact_events_rollup
            ON fact_events (tenant_id, bucket, sum(amount), count(*))",
    )
    .await
    .expect("create aggregating index");
}

async fn finish(
    clients: Vec<tokio_postgres::Client>,
    server: Arc<Server>,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(clients);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
    drop(server);
}

/// Case 1: in-txn INSERT + own GROUP BY read [warms summary], plain
/// `ROLLBACK` → a fresh second-connection rollup is heap truth, not the
/// phantom (writer's uncommitted aggregate).
#[tokio::test]
async fn plain_rollback_after_own_rollup_read_leaves_truth() {
    let (server, conn_str, server_handle) = start().await;
    let w = connect(&conn_str).await;
    setup(&w).await;
    assert_eq!(rollup(&w).await, TRUTH);

    w.batch_execute("BEGIN; INSERT INTO fact_events VALUES (7, 1, 100);")
        .await
        .expect("begin+insert");
    assert_eq!(
        rollup(&w).await,
        vec![(7, 1, 130, 3), (7, 2, 5, 1)],
        "writer sees its own updated rollup (own read-after-write)"
    );
    w.batch_execute("ROLLBACK").await.expect("rollback");

    let r = connect(&conn_str).await;
    assert_eq!(
        rollup(&r).await,
        TRUTH,
        "fresh reader after ROLLBACK must see pre-txn truth, not phantom aggregate"
    );
    // Same-connection autocommit snapshot after the abort is also truth.
    assert_eq!(rollup(&w).await, TRUTH, "same-conn after ROLLBACK");

    finish(vec![w, r], server, server_handle).await;
}

/// Case 2: same, but `ROLLBACK TO SAVEPOINT` undoes the dirtying write while
/// the txn continues, then `COMMIT` → fresh rollup is truth (the subtxn rows
/// never committed).
#[tokio::test]
async fn rollback_to_savepoint_after_own_rollup_read_leaves_truth() {
    let (server, conn_str, server_handle) = start().await;
    let w = connect(&conn_str).await;
    setup(&w).await;

    w.batch_execute(
        "BEGIN;
         SAVEPOINT s1;
         INSERT INTO fact_events VALUES (7, 1, 100), (7, 2, 50);",
    )
    .await
    .expect("begin+savepoint+insert");
    assert_eq!(
        rollup(&w).await,
        vec![(7, 1, 130, 3), (7, 2, 55, 2)],
        "writer sees its own updated rollup before the savepoint rollback"
    );
    w.batch_execute("ROLLBACK TO SAVEPOINT s1; COMMIT;")
        .await
        .expect("rollback to savepoint + commit");

    let r = connect(&conn_str).await;
    assert_eq!(
        rollup(&r).await,
        TRUTH,
        "fresh reader after ROLLBACK TO SAVEPOINT must see truth, not the rolled-back subtxn aggregate"
    );

    finish(vec![w, r], server, server_handle).await;
}

/// Case 3: same, but `PREPARE TRANSACTION` then cross-connection `ROLLBACK
/// PREPARED` → fresh rollup is truth.
#[tokio::test]
async fn rollback_prepared_after_own_rollup_read_leaves_truth() {
    let (server, conn_str, server_handle) = start().await;
    let w = connect(&conn_str).await;
    setup(&w).await;

    w.batch_execute("BEGIN; INSERT INTO fact_events VALUES (7, 1, 100), (7, 1, 7);")
        .await
        .expect("begin+insert");
    assert_eq!(
        rollup(&w).await,
        vec![(7, 1, 137, 4), (7, 2, 5, 1)],
        "writer warms the summary from its own view"
    );
    w.batch_execute("PREPARE TRANSACTION 'aggidx-abort-gid'")
        .await
        .expect("prepare");

    // A DIFFERENT connection finalises the rollback.
    let c2 = connect(&conn_str).await;
    c2.batch_execute("ROLLBACK PREPARED 'aggidx-abort-gid'")
        .await
        .expect("rollback prepared");

    let r = connect(&conn_str).await;
    assert_eq!(
        rollup(&r).await,
        TRUTH,
        "fresh reader after cross-connection ROLLBACK PREPARED must see truth"
    );

    finish(vec![w, c2, r], server, server_handle).await;
}

/// Case 4: the writer is force-aborted by a statement error inside the txn
/// (the whole txn aborts), after warming the summary → fresh rollup is truth.
/// This exercises a non-2PC abort path distinct from cases 1-3.
#[tokio::test]
async fn statement_error_abort_after_own_rollup_read_leaves_truth() {
    let (server, conn_str, server_handle) = start().await;
    let w = connect(&conn_str).await;
    setup(&w).await;

    w.batch_execute("BEGIN; INSERT INTO fact_events VALUES (7, 1, 100);")
        .await
        .expect("begin+insert");
    assert_eq!(
        rollup(&w).await,
        vec![(7, 1, 130, 3), (7, 2, 5, 1)],
        "writer warms the summary from its own view"
    );
    // A statement error aborts the transaction (PostgreSQL semantics: the txn
    // enters the failed state and must be rolled back).
    let err = w
        .batch_execute("INSERT INTO fact_events VALUES (7, 1, 'not-a-number')")
        .await;
    assert!(err.is_err(), "the bad INSERT must error and abort the txn");
    w.batch_execute("ROLLBACK")
        .await
        .expect("rollback the aborted txn");

    let r = connect(&conn_str).await;
    assert_eq!(
        rollup(&r).await,
        TRUTH,
        "fresh reader after a statement-error abort must see truth, not the phantom"
    );

    finish(vec![w, r], server, server_handle).await;
}

/// Case 5 (REGRESSION): plain `COMMIT` of the in-txn write → the committed
/// aggregate IS reflected (no over-rejection; summary served/rebuilt
/// correctly).
#[tokio::test]
async fn commit_after_own_rollup_read_reflects_committed_aggregate() {
    let (server, conn_str, server_handle) = start().await;
    let w = connect(&conn_str).await;
    setup(&w).await;

    w.batch_execute("BEGIN; INSERT INTO fact_events VALUES (7, 1, 100);")
        .await
        .expect("begin+insert");
    assert_eq!(
        rollup(&w).await,
        vec![(7, 1, 130, 3), (7, 2, 5, 1)],
        "writer sees its own updated rollup"
    );
    w.batch_execute("COMMIT").await.expect("commit");

    let r = connect(&conn_str).await;
    assert_eq!(
        rollup(&r).await,
        vec![(7, 1, 130, 3), (7, 2, 5, 1)],
        "fresh reader after COMMIT must see the committed aggregate (no over-rejection)"
    );
    // Re-serve from the now-committed summary returns the same value.
    assert_eq!(rollup(&r).await, vec![(7, 1, 130, 3), (7, 2, 5, 1)]);

    finish(vec![w, r], server, server_handle).await;
}

/// Case 5b (REGRESSION): `PREPARE TRANSACTION` then cross-connection `COMMIT
/// PREPARED` → the committed aggregate IS reflected.
#[tokio::test]
async fn commit_prepared_after_own_rollup_read_reflects_committed_aggregate() {
    let (server, conn_str, server_handle) = start().await;
    let w = connect(&conn_str).await;
    setup(&w).await;

    w.batch_execute("BEGIN; INSERT INTO fact_events VALUES (7, 2, 45);")
        .await
        .expect("begin+insert");
    assert_eq!(
        rollup(&w).await,
        vec![(7, 1, 30, 2), (7, 2, 50, 2)],
        "writer sees its own updated rollup"
    );
    w.batch_execute("PREPARE TRANSACTION 'aggidx-commit-gid'")
        .await
        .expect("prepare");

    let c2 = connect(&conn_str).await;
    c2.batch_execute("COMMIT PREPARED 'aggidx-commit-gid'")
        .await
        .expect("commit prepared");

    let r = connect(&conn_str).await;
    assert_eq!(
        rollup(&r).await,
        vec![(7, 1, 30, 2), (7, 2, 50, 2)],
        "fresh reader after COMMIT PREPARED must see the committed aggregate"
    );

    finish(vec![w, c2, r], server, server_handle).await;
}

/// Case 6: own read-after-write within the live txn sees its own updated
/// rollup repeatedly (the `is_current_xid` serve path), even across multiple
/// in-txn writes, then ROLLBACK leaves truth for everyone else.
#[tokio::test]
async fn own_read_after_write_sees_own_rollup_then_rollback_leaves_truth() {
    let (server, conn_str, server_handle) = start().await;
    let w = connect(&conn_str).await;
    setup(&w).await;

    w.batch_execute("BEGIN;").await.expect("begin");
    w.batch_execute("INSERT INTO fact_events VALUES (7, 1, 100);")
        .await
        .expect("insert 1");
    assert_eq!(
        rollup(&w).await,
        vec![(7, 1, 130, 3), (7, 2, 5, 1)],
        "own read after first write"
    );
    w.batch_execute("INSERT INTO fact_events VALUES (7, 2, 95);")
        .await
        .expect("insert 2");
    assert_eq!(
        rollup(&w).await,
        vec![(7, 1, 130, 3), (7, 2, 100, 2)],
        "own read after second write reflects both"
    );
    w.batch_execute("ROLLBACK").await.expect("rollback");

    let r = connect(&conn_str).await;
    assert_eq!(
        rollup(&r).await,
        TRUTH,
        "fresh reader sees truth after rollback"
    );

    finish(vec![w, r], server, server_handle).await;
}

/// Case 7 (interleaving): after an aborted writer poisons the summary and a
/// fresh reader corrects it, a subsequent committed write is reflected — the
/// gate's forced rebuild + writer clear does not wedge the summary stale.
#[tokio::test]
async fn aborted_then_committed_write_round_trip() {
    let (server, conn_str, server_handle) = start().await;
    let w = connect(&conn_str).await;
    setup(&w).await;

    // Abort flavor first: warm + ROLLBACK.
    w.batch_execute("BEGIN; INSERT INTO fact_events VALUES (7, 1, 100);")
        .await
        .expect("begin+insert");
    assert_eq!(rollup(&w).await, vec![(7, 1, 130, 3), (7, 2, 5, 1)]);
    w.batch_execute("ROLLBACK").await.expect("rollback");

    let r = connect(&conn_str).await;
    assert_eq!(rollup(&r).await, TRUTH, "abort corrected to truth");

    // Now a committed autocommit write must be reflected to a fresh reader.
    w.batch_execute("INSERT INTO fact_events VALUES (7, 1, 11)")
        .await
        .expect("autocommit insert");
    let r2 = connect(&conn_str).await;
    assert_eq!(
        rollup(&r2).await,
        vec![(7, 1, 41, 3), (7, 2, 5, 1)],
        "committed write after the abort correction must be reflected"
    );

    finish(vec![w, r, r2], server, server_handle).await;
}
