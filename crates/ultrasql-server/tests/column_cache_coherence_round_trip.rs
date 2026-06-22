//! Regression tests for column-cache coherence across transactions.
//!
//! The per-relation column cache (`HeapAccess::column_cache`) is shared
//! and keyed only on the relation's mutation version. The version is
//! bumped at *physical* insert/update/delete time, which is **before**
//! the writing transaction commits. A frozen-snapshot reader (e.g.
//! `REPEATABLE READ`, or any reader whose snapshot was taken before a
//! concurrent writer committed) therefore scans a relation whose version
//! already reflects rows it cannot see. Without a coherence guard such a
//! reader would publish a projection that omits committed rows (or
//! resurrects deleted ones) into the shared cache, and a later reader —
//! served from that version-keyed entry — would observe the stale,
//! incoherent result.
//!
//! `SeqScan::build` gates the cache *publish* on the building snapshot
//! being able to see the relation's latest writer
//! (`ColumnCache::last_writer_xid`). These tests drive two real client
//! sessions to prove a behind-the-commit reader never poisons the cache
//! for a fresh reader, in both the INSERT and DELETE directions. They
//! FAIL without the publish gate (the fresh reader observes the stale
//! subset) and pass with it.

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
        "host={host} port={port} user=tester application_name=colcache_coherence",
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

/// Count rows returned by a simple-query `SELECT * FROM t` — the path
/// that exercises the column-cache fast path.
async fn count_rows(client: &tokio_postgres::Client, sql: &str) -> usize {
    let msgs = client.simple_query(sql).await.expect("simple_query");
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

async fn scan_count(client: &tokio_postgres::Client) -> i64 {
    let row = client
        .query_one("SELECT COUNT(*) FROM t", &[])
        .await
        .expect("count");
    row.get::<_, i64>(0)
}

/// A `REPEATABLE READ` reader whose snapshot predates a committed
/// INSERT must not publish a projection that hides the committed rows
/// from a later fresh reader.
#[tokio::test]
async fn behind_commit_reader_never_hides_inserted_rows_from_fresh_reader() {
    let (server, conn_str, server_handle) = start().await;

    // Writer / schema owner.
    let w = connect(&conn_str).await;
    w.batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create t");
    // Seed two rows and warm the cache so the relation is cache-eligible.
    w.batch_execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .expect("seed");
    server.run_columnarization_cycle();

    // REPEATABLE READ reader: freeze a snapshot at 2 rows, before the
    // writer's new rows exist.
    let r = connect(&conn_str).await;
    r.batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin rr");
    assert_eq!(count_rows(&r, "SELECT * FROM t").await, 2);

    // Writer inserts three more rows and commits (autocommit). This
    // bumps the cache version at insert time and records the writer xid.
    w.batch_execute("INSERT INTO t VALUES (3, 30), (4, 40), (5, 50)")
        .await
        .expect("insert");

    // The RR reader scans again under its frozen snapshot: it still sees
    // only the original 2 rows, and at the post-insert cache version it
    // would (without the guard) publish a stale 2-row projection.
    for _ in 0..4 {
        assert_eq!(
            count_rows(&r, "SELECT * FROM t").await,
            2,
            "RR reader stays at its frozen 2-row view"
        );
    }
    r.batch_execute("COMMIT").await.expect("commit rr");

    // A fresh reader (autocommit / READ COMMITTED) must see all five
    // committed rows, never the stale cached subset.
    let f = connect(&conn_str).await;
    assert_eq!(
        count_rows(&f, "SELECT * FROM t").await,
        5,
        "fresh reader must see all 5 committed rows"
    );
    assert_eq!(scan_count(&f).await, 5, "fresh reader COUNT(*) must be 5");

    drop((w, r, f));
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// The symmetric DELETE case: a behind-the-commit reader must not
/// republish a projection that resurrects committed-deleted rows.
#[tokio::test]
async fn behind_commit_reader_never_resurrects_deleted_rows_for_fresh_reader() {
    let (server, conn_str, server_handle) = start().await;

    let w = connect(&conn_str).await;
    w.batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create t");
    w.batch_execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .await
        .expect("seed");
    server.run_columnarization_cycle();

    // RR reader freezes its snapshot at 5 rows.
    let r = connect(&conn_str).await;
    r.batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin rr");
    assert_eq!(count_rows(&r, "SELECT * FROM t").await, 5);

    // Writer deletes two rows and commits.
    w.batch_execute("DELETE FROM t WHERE id IN (4, 5)")
        .await
        .expect("delete");

    // RR reader still sees the pre-delete 5 rows under its frozen
    // snapshot and would otherwise publish a stale 5-row projection.
    for _ in 0..4 {
        assert_eq!(
            count_rows(&r, "SELECT * FROM t").await,
            5,
            "RR reader stays at its frozen 5-row view"
        );
    }
    r.batch_execute("COMMIT").await.expect("commit rr");

    // Fresh reader must see only the 3 surviving rows.
    let f = connect(&conn_str).await;
    assert_eq!(
        count_rows(&f, "SELECT * FROM t").await,
        3,
        "fresh reader must see 3 rows after the committed delete"
    );
    assert_eq!(scan_count(&f).await, 3, "fresh reader COUNT(*) must be 3");

    drop((w, r, f));
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// The no-contention autocommit hot path must remain a cache producer
/// and consumer: a fresh reader after committed writes still gets a live
/// cache entry built and served. This guards against the publish gate
/// being too aggressive and disabling the cache for the common case.
#[tokio::test]
async fn autocommit_reader_still_publishes_and_reuses_cache() {
    let (server, conn_str, server_handle) = start().await;

    let c = connect(&conn_str).await;
    c.batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create t");
    c.batch_execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed");

    let rel = {
        let snap = server.catalog_snapshot();
        let entry = snap.tables.get("t").expect("t table").clone();
        ultrasql_core::RelationId(entry.oid)
    };

    // No live cache entry yet (writes bumped the version and dropped it).
    assert!(server.heap.column_cache.get(rel).is_none());

    // An autocommit scan over the relation must build + publish the
    // cache as a side effect, because its snapshot can see the writer.
    assert_eq!(count_rows(&c, "SELECT * FROM t").await, 3);

    let cached = server
        .heap
        .column_cache
        .get(rel)
        .expect("autocommit scan must publish a live cache entry");
    assert_eq!(cached.row_count(), 3);

    // A second scan is served from the cache and returns the same rows.
    assert_eq!(count_rows(&c, "SELECT * FROM t").await, 3);

    drop(c);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}
