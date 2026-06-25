//! Regression tests: the column cache must never serve an **aborted**
//! writer's projection.
//!
//! The per-relation column cache (`HeapAccess::column_cache`) records, per
//! relation, the highest XID that mutated it (`last_writer_xid`) and caches
//! a projection / scalar-aggregate / grouped body built from that writer's
//! view. The coherence gate (`ColumnCache::is_snapshot_coherent` /
//! `get_for_snapshot`) serves a cached entry to a fresh reader when the
//! reader either IS the writer or the writer is no longer in progress.
//!
//! The bug these tests pin down: an **aborted** writer is "no longer in
//! progress" exactly like a committed one. A transaction that READS-then-
//! writes (warming the cache from its OWN uncommitted view) and is then
//! ABORTED via a path that cannot reach the plain-ROLLBACK cache
//! invalidation (`ROLLBACK PREPARED`, SSI force-abort) left **phantom
//! rows** visible to fresh cross-connection readers until the next write to
//! that relation or a restart.
//!
//! The fix tightens the gate to require the writer be COMMITTED (per the
//! same `XidStatusOracle` the heap visibility path consults) unless the
//! reader is the writer itself. These tests prove the phantom is gone while
//! the valid committed / own-read cases are preserved.
//!
//! Coverage notes:
//! - The **SSI force-abort** path (`TransactionManager::commit` →
//!   `force_abort` on a serialization failure) has the identical latent
//!   hole: a force-aborted txn that warmed the cache. It is not driven over
//!   the wire here (deterministically provoking an SSI dangerous-structure
//!   abort that *also* warmed the cache is fragile), but it lands in the
//!   **same** gate — `force_abort` sets the writer's CLOG status to
//!   `Aborted`, which the gate now consults. The gate-level rejection is
//!   proven directly by the `aborted_writer_projection_is_rejected` unit
//!   test in `ultrasql-storage`'s `column_cache` module, which is agnostic
//!   to how the writer reached `XidStatus::Aborted`.
//! - The **durable / restart** side was always correct: the CLOG/WAL record
//!   the abort, and the in-memory cache is discarded on restart. The
//!   before-restart same-process count is what was wrong; case 1 now proves
//!   it is 0 in-process, matching the (already-0) post-restart durable
//!   outcome that `two_phase_restart_round_trip` covers.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage};
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
        "host={host} port={port} user=tester application_name=colcache_aborted",
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

async fn scan_count(client: &tokio_postgres::Client) -> i64 {
    let row = client
        .query_one("SELECT COUNT(*) FROM t", &[])
        .await
        .expect("count");
    row.get::<_, i64>(0)
}

async fn count_star_simple(client: &tokio_postgres::Client) -> i64 {
    let msgs = client
        .simple_query("SELECT COUNT(*) FROM t")
        .await
        .expect("simple count");
    for m in msgs {
        if let SimpleQueryMessage::Row(r) = m {
            return r
                .get(0)
                .and_then(|s| s.parse::<i64>().ok())
                .expect("count value");
        }
    }
    panic!("no count row");
}

async fn star_rows(client: &tokio_postgres::Client, sql: &str) -> usize {
    let msgs = client.simple_query(sql).await.expect("simple_query");
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

async fn create_seeded_table(c: &tokio_postgres::Client) {
    c.batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create t");
}

/// Battery case 1: `INSERT; SELECT count(*) [warms]; PREPARE; ROLLBACK
/// PREPARED` → fresh-conn AND same-conn `count(*)` = 0 (not n).
#[tokio::test]
async fn rollback_prepared_after_own_count_read_leaves_zero() {
    let (server, conn_str, server_handle) = start().await;

    let w = connect(&conn_str).await;
    create_seeded_table(&w).await;

    // BEGIN; INSERT 5 rows; SELECT count(*) warms the cache from the
    // writer's own uncommitted view; PREPARE.
    w.batch_execute(
        "BEGIN;
         INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4),(5,5);",
    )
    .await
    .expect("begin+insert");
    // Own read-after-write: warms the cache as the writer's current xid.
    assert_eq!(scan_count(&w).await, 5, "writer sees its own 5 rows");
    assert_eq!(count_star_simple(&w).await, 5);
    w.batch_execute("PREPARE TRANSACTION 'abort-count-gid'")
        .await
        .expect("prepare");
    w.batch_execute("ROLLBACK PREPARED 'abort-count-gid'")
        .await
        .expect("rollback prepared");

    // Same connection, fresh autocommit snapshot.
    assert_eq!(
        count_star_simple(&w).await,
        0,
        "same-conn count after ROLLBACK PREPARED must be 0 (no phantom rows)"
    );

    // Fresh connection.
    let r = connect(&conn_str).await;
    assert_eq!(
        count_star_simple(&r).await,
        0,
        "fresh-conn count after ROLLBACK PREPARED must be 0 (no phantom rows)"
    );

    drop(w);
    drop(r);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
    drop(server);
}

/// Battery case 2: `INSERT; SELECT * WHERE val>5 [warms projection];
/// PREPARE; ROLLBACK PREPARED` → 0 (projection + predicate + scalar-agg).
#[tokio::test]
async fn rollback_prepared_after_own_projection_read_leaves_zero() {
    let (server, conn_str, server_handle) = start().await;

    let w = connect(&conn_str).await;
    create_seeded_table(&w).await;
    w.batch_execute(
        "BEGIN;
         INSERT INTO t VALUES (1,10),(2,20),(3,1),(4,30),(5,2);",
    )
    .await
    .expect("begin+insert");
    // Warm a projection + predicate + scalar agg from the writer's view.
    assert_eq!(star_rows(&w, "SELECT id, val FROM t").await, 5);
    assert_eq!(star_rows(&w, "SELECT * FROM t WHERE val > 5").await, 3);
    assert_eq!(
        scan_count(&w).await,
        5,
        "writer's own scalar-agg count is 5"
    );
    w.batch_execute("PREPARE TRANSACTION 'abort-proj-gid'")
        .await
        .expect("prepare");
    w.batch_execute("ROLLBACK PREPARED 'abort-proj-gid'")
        .await
        .expect("rollback prepared");

    let r = connect(&conn_str).await;
    assert_eq!(count_star_simple(&r).await, 0, "scalar-agg phantom gone");
    assert_eq!(
        star_rows(&r, "SELECT id, val FROM t").await,
        0,
        "projection phantom gone"
    );
    assert_eq!(
        star_rows(&r, "SELECT * FROM t WHERE val > 5").await,
        0,
        "predicate-projection phantom gone"
    );

    drop(w);
    drop(r);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
    drop(server);
}

/// Battery case 3: `ROLLBACK PREPARED` then explicit columnarize then
/// re-read → 0. An explicit columnarization after the abort must not
/// resurrect the phantom either.
#[tokio::test]
async fn rollback_prepared_then_explicit_columnarize_leaves_zero() {
    let (server, conn_str, server_handle) = start().await;

    let w = connect(&conn_str).await;
    create_seeded_table(&w).await;
    w.batch_execute(
        "BEGIN;
         INSERT INTO t VALUES (1,1),(2,2),(3,3);",
    )
    .await
    .expect("begin+insert");
    assert_eq!(scan_count(&w).await, 3);
    w.batch_execute("PREPARE TRANSACTION 'abort-colz-gid'")
        .await
        .expect("prepare");
    w.batch_execute("ROLLBACK PREPARED 'abort-colz-gid'")
        .await
        .expect("rollback prepared");

    // Explicit columnarization cycle after the abort.
    server.run_columnarization_cycle();

    let r = connect(&conn_str).await;
    assert_eq!(
        count_star_simple(&r).await,
        0,
        "explicit columnarize after abort must not resurrect phantom rows"
    );

    drop(w);
    drop(r);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
    drop(server);
}

/// Battery case 4 (REGRESSION guard): the fix must NOT evict a valid
/// COMMITTED projection. `INSERT; SELECT count(*) [warms]; PREPARE; COMMIT
/// PREPARED` → rows correctly PRESENT; and a normal autocommit warm-then-
/// re-serve still returns the cached correct value.
#[tokio::test]
async fn commit_prepared_after_own_read_keeps_rows_present() {
    let (server, conn_str, server_handle) = start().await;

    let w = connect(&conn_str).await;
    create_seeded_table(&w).await;
    w.batch_execute(
        "BEGIN;
         INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4);",
    )
    .await
    .expect("begin+insert");
    assert_eq!(scan_count(&w).await, 4, "writer warms cache with own read");
    w.batch_execute("PREPARE TRANSACTION 'commit-keep-gid'")
        .await
        .expect("prepare");
    w.batch_execute("COMMIT PREPARED 'commit-keep-gid'")
        .await
        .expect("commit prepared");

    // The committed writer's projection must still be served.
    let r = connect(&conn_str).await;
    assert_eq!(
        count_star_simple(&r).await,
        4,
        "COMMIT PREPARED rows must remain present (no over-eviction)"
    );
    assert_eq!(star_rows(&r, "SELECT id, val FROM t").await, 4);

    // Normal autocommit warm-then-re-serve still returns the cached value.
    w.batch_execute("INSERT INTO t VALUES (5,5)")
        .await
        .expect("autocommit insert");
    server.run_columnarization_cycle();
    assert_eq!(count_star_simple(&r).await, 5, "warm");
    assert_eq!(count_star_simple(&r).await, 5, "re-serve from cache");
    assert_eq!(star_rows(&r, "SELECT id, val FROM t").await, 5);

    drop(w);
    drop(r);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
    drop(server);
}

/// Battery case 5 (cross-connection): prepare+own-read on conn 1, ROLLBACK
/// PREPARED on conn 2, read on conn 3 = 0.
#[tokio::test]
async fn cross_connection_rollback_prepared_leaves_zero() {
    let (server, conn_str, server_handle) = start().await;

    let c1 = connect(&conn_str).await;
    create_seeded_table(&c1).await;
    c1.batch_execute(
        "BEGIN;
         INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6);",
    )
    .await
    .expect("begin+insert");
    assert_eq!(scan_count(&c1).await, 6, "conn1 warms cache via own read");
    c1.batch_execute("PREPARE TRANSACTION 'xconn-gid'")
        .await
        .expect("prepare");

    // A DIFFERENT connection finalises the rollback.
    let c2 = connect(&conn_str).await;
    c2.batch_execute("ROLLBACK PREPARED 'xconn-gid'")
        .await
        .expect("rollback prepared on c2");

    // A THIRD fresh connection reads.
    let c3 = connect(&conn_str).await;
    assert_eq!(
        count_star_simple(&c3).await,
        0,
        "cross-connection ROLLBACK PREPARED must leave zero phantom rows"
    );

    drop(c1);
    drop(c2);
    drop(c3);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
    drop(server);
}

/// Battery case 7: savepoint + ROLLBACK PREPARED with an own-read (released
/// and open savepoint) → 0.
#[tokio::test]
async fn savepoint_rollback_prepared_with_own_read_leaves_zero() {
    let (server, conn_str, server_handle) = start().await;

    let w = connect(&conn_str).await;
    create_seeded_table(&w).await;
    // Mix of parent-xid rows, a RELEASEd savepoint, and an open savepoint at
    // PREPARE time; warm the cache from the writer's full view.
    w.batch_execute(
        "BEGIN;
         INSERT INTO t VALUES (1,1),(2,2);
         SAVEPOINT s1;
         INSERT INTO t VALUES (3,3);
         RELEASE SAVEPOINT s1;
         SAVEPOINT s2;
         INSERT INTO t VALUES (4,4),(5,5);",
    )
    .await
    .expect("begin+savepoints+insert");
    assert_eq!(scan_count(&w).await, 5, "writer warms cache with own read");
    w.batch_execute("PREPARE TRANSACTION 'sp-abort-gid'")
        .await
        .expect("prepare");
    w.batch_execute("ROLLBACK PREPARED 'sp-abort-gid'")
        .await
        .expect("rollback prepared");

    let r = connect(&conn_str).await;
    assert_eq!(
        count_star_simple(&r).await,
        0,
        "ROLLBACK PREPARED of a savepoint family must leave zero phantom rows"
    );

    drop(w);
    drop(r);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
    drop(server);
}
