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
//! The shared cache is gated on a **quiescent, writer-visible snapshot**
//! at both the *publish* and the *read* side
//! (`ColumnCache::is_snapshot_coherent` / `get_for_snapshot`): a snapshot
//! may build or consume an entry only when its in-progress set is empty
//! and it can see the relation's latest writer
//! (`ColumnCache::last_writer_xid`). This admits the cache only when the
//! relation is effectively quiescent for the operating snapshot and falls
//! back to a correct heap scan under any concurrency.
//!
//! These tests drive real client sessions to prove the gate is sound in
//! six directions:
//!
//! - publish side, INSERT and DELETE (a behind-the-commit reader never
//!   poisons the cache for a fresh reader);
//! - read side, HOLE 1 (an RR reader frozen before a committed INSERT
//!   never *consumes* a too-new published projection);
//! - HOLE 2 (a concurrent reader never dirty-reads another txn's
//!   uncommitted, cache-published rows);
//! - HOLE 3 (a lower-xid in-progress writer's rows are never lost when a
//!   higher-xid writer commits and the version does not bump on commit);
//! - and the no-contention autocommit hot path still both publishes and
//!   reuses the cache (the gate is not over-aggressive).

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

    // Explicit assertion that under NO concurrency the cache is actually
    // *usable* by a fresh autocommit-style snapshot (empty in-progress set,
    // last writer committed-and-visible). This guards against the quiescent
    // gate silently disabling the cache for the common read-mostly case: an
    // empty-xip snapshot whose xmax is past the writer must pass
    // `is_snapshot_coherent` and get the live entry from `get_for_snapshot`.
    {
        use ultrasql_core::{CommandId, Xid};
        let snap = server
            .txn_manager
            .statement_snapshot(Xid::INVALID, CommandId::FIRST);
        assert!(
            snap.xip().is_empty(),
            "no-concurrency snapshot must have an empty in-progress set"
        );
        assert!(
            server
                .heap
                .column_cache
                .is_snapshot_coherent(rel, &snap, server.txn_manager.as_ref()),
            "quiescent snapshot must be allowed to use the cache"
        );
        assert!(
            server
                .heap
                .column_cache
                .get_for_snapshot(rel, &snap, server.txn_manager.as_ref())
                .is_some(),
            "quiescent snapshot must be served the live cache entry"
        );
    }

    drop(c);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// HOLE 1 (read side). An RR reader frozen *before* a concurrent
/// INSERT+COMMIT must not see the new rows when it re-reads at the now
/// current cache version, even after a separate fresh reader has published
/// the post-insert projection into the shared cache. The publish gate
/// alone does not fix this — the frozen reader would otherwise consume the
/// too-new entry RAW. A separate fresh reader must still see all rows.
#[tokio::test]
async fn rr_reader_does_not_consume_post_snapshot_committed_rows_from_cache() {
    let (server, conn_str, server_handle) = start().await;

    let w = connect(&conn_str).await;
    w.batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create t");
    w.batch_execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .expect("seed");
    server.run_columnarization_cycle();

    // RR reader freezes a snapshot at 2 rows.
    let r = connect(&conn_str).await;
    r.batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin rr");
    assert_eq!(count_rows(&r, "SELECT * FROM t").await, 2);

    // Writer commits 3 new rows; a separate fresh reader warms/publishes
    // the 5-row projection at the now-current version from a snapshot that
    // legitimately sees the writer.
    w.batch_execute("INSERT INTO t VALUES (3, 30), (4, 40), (5, 50)")
        .await
        .expect("insert");
    let warm = connect(&conn_str).await;
    assert_eq!(count_rows(&warm, "SELECT * FROM t").await, 5);

    // The frozen RR reader must stay at its 2-row view: its snapshot has
    // the writer in-progress relative to its frozen xmax, so the read gate
    // rejects the published entry and it walks the heap.
    for _ in 0..4 {
        assert_eq!(
            count_rows(&r, "SELECT * FROM t").await,
            2,
            "RR reader frozen before the insert must not consume the too-new cache"
        );
    }
    r.batch_execute("COMMIT").await.expect("commit rr");

    // A fresh reader still sees all five committed rows.
    let f = connect(&conn_str).await;
    assert_eq!(
        count_rows(&f, "SELECT * FROM t").await,
        5,
        "fresh reader must see all 5 committed rows"
    );

    drop((w, r, warm, f));
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// HOLE 2. A txn X that does `BEGIN; INSERT; SELECT` (read-after-write)
/// warms the shared cache with a projection *including its own
/// uncommitted rows*. A concurrent reader Y must NOT dirty-read them;
/// after X commits, Y's next snapshot does see them.
#[tokio::test]
async fn concurrent_reader_never_dirty_reads_uncommitted_cache_publish() {
    let (server, conn_str, server_handle) = start().await;

    let setup = connect(&conn_str).await;
    setup
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create t");
    setup
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .expect("seed");
    server.run_columnarization_cycle();

    // X: BEGIN; INSERT (uncommitted); SELECT (read-after-write warms the
    // cache including its own uncommitted rows).
    let x = connect(&conn_str).await;
    x.batch_execute("BEGIN").await.expect("begin x");
    x.batch_execute("INSERT INTO t VALUES (3, 30), (4, 40)")
        .await
        .expect("x insert");
    assert_eq!(
        count_rows(&x, "SELECT * FROM t").await,
        4,
        "X sees its own 4 rows"
    );

    // Y: concurrent autocommit reader. X is in Y's in-progress set, so Y's
    // read gate rejects any cache entry and walks the heap — never the 4.
    let y = connect(&conn_str).await;
    for _ in 0..4 {
        assert_eq!(
            count_rows(&y, "SELECT * FROM t").await,
            2,
            "Y must not dirty-read X's uncommitted cache-published rows"
        );
    }

    // After X commits, Y's next snapshot sees the 4 rows.
    x.batch_execute("COMMIT").await.expect("commit x");
    assert_eq!(
        count_rows(&y, "SELECT * FROM t").await,
        4,
        "after X commits Y sees all 4 rows"
    );

    drop((setup, x, y));
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// HOLE 3. Two concurrent writers: A (lower xid) stays in-progress while
/// B (higher xid) commits. A warm reader publishes a projection that
/// excludes A's row (A invisible). Commit does not bump the version, so
/// when A commits a fresh reader at the same version must still see the
/// full committed set — never the stale, A-less cached subset.
#[tokio::test]
async fn lower_xid_writer_rows_not_lost_across_higher_xid_commit() {
    let (server, conn_str, server_handle) = start().await;

    let setup = connect(&conn_str).await;
    setup
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create t");
    setup
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .expect("seed");
    server.run_columnarization_cycle();

    // A: lower xid, in-progress write.
    let a = connect(&conn_str).await;
    a.batch_execute("BEGIN").await.expect("begin a");
    a.batch_execute("INSERT INTO t VALUES (3, 30)")
        .await
        .expect("a insert");

    // B: higher xid, autocommit insert (the recorded max writer; visible
    // to a fresh reader).
    let b = connect(&conn_str).await;
    b.batch_execute("INSERT INTO t VALUES (4, 40)")
        .await
        .expect("b insert");

    // A warm reader sees rows 1,2,4 (A in-progress and invisible). Without
    // the xip-empty requirement it would publish this 3-row, A-less
    // projection; with it, A in the reader's in-progress set blocks the
    // publish.
    let warm = connect(&conn_str).await;
    assert_eq!(
        count_rows(&warm, "SELECT * FROM t").await,
        3,
        "warm reader sees rows 1,2,4 while A is in-progress"
    );

    // A commits (no version bump on commit).
    a.batch_execute("COMMIT").await.expect("commit a");

    // A fresh reader after A's commit must see all 4 rows.
    let fresh = connect(&conn_str).await;
    for _ in 0..4 {
        assert_eq!(
            count_rows(&fresh, "SELECT * FROM t").await,
            4,
            "fresh reader after A commits must see all 4 rows, not a stale A-less cache"
        );
    }

    drop((setup, a, b, warm, fresh));
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// Row count via an ORDER-BY column scan, which reads the heap directly and so
/// bypasses the cached scalar-aggregate / single-column projection fast paths.
/// This is the authoritative MVCC view, used to cross-check that the cached
/// path agrees with the heap after a rollback.
async fn ordered_scan_count(client: &tokio_postgres::Client) -> usize {
    client
        .query("SELECT id FROM t ORDER BY id", &[])
        .await
        .expect("ordered scan")
        .len()
}

/// Stream a single-column-int `COPY t FROM STDIN` payload and finish cleanly,
/// returning the reported row count.
async fn copy_int_rows(client: &tokio_postgres::Client, ids: &[i32]) -> u64 {
    use bytes::Bytes;
    use futures::SinkExt;

    let mut text = String::new();
    for id in ids {
        text.push_str(&id.to_string());
        text.push('\n');
    }
    let sink = client
        .copy_in::<_, Bytes>("COPY t FROM STDIN")
        .await
        .expect("copy_in establishes COPY FROM STDIN");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from(text.into_bytes()))
        .await
        .expect("send CopyData");
    sink.finish().await.expect("finish copy_in")
}

/// Regression: a full `ROLLBACK` of an in-txn `INSERT` must not leave the
/// shared scalar-aggregate (`COUNT(*)`) cache stale. The in-txn `SELECT
/// COUNT(*)` warms the cache from the writer's own snapshot (= 5); after
/// ROLLBACK the rows are heap-invisible, so a fresh `COUNT(*)` must return 0.
///
/// Pre-fix this returned the stale `5` because `execute_rollback` cleared the
/// pending-DML maps but never evicted the column cache (the INSERT bumped the
/// version at physical-insert time and the writer published a 5-row
/// projection, whose derived COUNT wire then survived the abort).
#[tokio::test]
async fn full_rollback_evicts_stale_count_cache() {
    let (_server, conn_str, server_handle) = start().await;

    let c = connect(&conn_str).await;
    c.batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create t");

    c.batch_execute("BEGIN").await.expect("begin");
    c.batch_execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .await
        .expect("insert");
    // Warm the scalar-aggregate cache from the writer's own snapshot.
    assert_eq!(scan_count(&c).await, 5, "in-txn COUNT sees own 5 rows");
    c.batch_execute("ROLLBACK").await.expect("rollback");

    // The aggregate cache must now be evicted: COUNT(*) must reflect the
    // heap-true post-rollback count (0), not the stale cached 5.
    assert_eq!(
        scan_count(&c).await,
        0,
        "COUNT(*) after ROLLBACK must be 0, not the stale in-txn 5"
    );
    assert_eq!(
        ordered_scan_count(&c).await,
        0,
        "heap scan agrees: 0 rows after rollback"
    );

    drop(c);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// Regression: the single-column / full-projection wire cache (the
/// `(Int32, Int32)` identity full-scan body) must also be evicted on a full
/// ROLLBACK. The in-txn `SELECT *` warms the projection; after ROLLBACK a
/// fresh `SELECT *` must return the pre-txn rows, never the stale projection.
#[tokio::test]
async fn full_rollback_evicts_stale_projection_cache() {
    let (server, conn_str, server_handle) = start().await;

    let c = connect(&conn_str).await;
    c.batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create t");
    // Seed + commit two rows and warm the projection cache for the committed
    // state, so the cache is genuinely live before the aborted txn.
    c.batch_execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .await
        .expect("seed");
    server.run_columnarization_cycle();
    assert_eq!(
        count_rows(&c, "SELECT * FROM t").await,
        2,
        "warm 2-row cache"
    );

    c.batch_execute("BEGIN").await.expect("begin");
    c.batch_execute("INSERT INTO t VALUES (3, 30), (4, 40), (5, 50)")
        .await
        .expect("insert");
    // Warm the projection (and its int32-pair wire) from the writer's own
    // 5-row snapshot.
    assert_eq!(count_rows(&c, "SELECT * FROM t").await, 5, "in-txn sees 5");
    c.batch_execute("ROLLBACK").await.expect("rollback");

    // Projection cache must be evicted: SELECT * must reflect the 2 committed
    // rows, never the stale 5-row projection.
    for _ in 0..3 {
        assert_eq!(
            count_rows(&c, "SELECT * FROM t").await,
            2,
            "SELECT * after ROLLBACK must see 2 committed rows, not stale 5"
        );
    }

    drop(c);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// Regression: the COPY-in-transaction path must evict the cache on ROLLBACK
/// identically to a plain INSERT. COPY shares the same `bump_version`
/// physical-insert warming, so `BEGIN; COPY; SELECT COUNT(*); ROLLBACK` left
/// the same stale aggregate before the fix.
#[tokio::test]
async fn full_rollback_evicts_stale_count_cache_after_copy() {
    let (_server, conn_str, server_handle) = start().await;

    let c = connect(&conn_str).await;
    c.batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create t");

    c.batch_execute("BEGIN").await.expect("begin");
    assert_eq!(copy_int_rows(&c, &[1, 2, 3, 4, 5]).await, 5, "COPY 5 rows");
    assert_eq!(scan_count(&c).await, 5, "in-txn COUNT sees COPYed 5 rows");
    c.batch_execute("ROLLBACK").await.expect("rollback");

    assert_eq!(
        scan_count(&c).await,
        0,
        "COUNT(*) after COPY ROLLBACK must be 0, not the stale 5"
    );

    drop(c);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// The column cache is server-global (shared `HeapAccess::column_cache`), so a
/// stale entry poisoned by connection A's aborted in-txn write would be served
/// to a *different* connection B. After the fix, A's ROLLBACK evicts the entry
/// and B's `COUNT(*)` sees the pre-txn count — proving the invalidation is on
/// the shared cache, not a per-session view.
#[tokio::test]
async fn full_rollback_does_not_poison_other_connection() {
    let (_server, conn_str, server_handle) = start().await;

    let a = connect(&conn_str).await;
    a.batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create t");

    a.batch_execute("BEGIN").await.expect("begin a");
    a.batch_execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .await
        .expect("insert a");
    assert_eq!(scan_count(&a).await, 5, "A sees its own 5 rows");
    a.batch_execute("ROLLBACK").await.expect("rollback a");

    // B is a separate connection. If the shared cache were still poisoned, B
    // would read the stale 5. After the fix it sees the heap-true 0.
    let b = connect(&conn_str).await;
    assert_eq!(
        scan_count(&b).await,
        0,
        "connection B must not be poisoned by A's aborted in-txn write"
    );

    drop((a, b));
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// No-regression: the COMMIT path's cache handling still serves the committed
/// count, and a SAVEPOINT rollback inside a committed txn still leaves the
/// cache coherent. Guards that the new ROLLBACK invalidation did not disturb
/// the positive paths.
#[tokio::test]
async fn commit_and_savepoint_paths_remain_coherent() {
    let (_server, conn_str, server_handle) = start().await;

    let c = connect(&conn_str).await;
    c.batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create t");

    // COMMIT path: in-txn warm then commit must leave the committed count live.
    c.batch_execute("BEGIN").await.expect("begin");
    c.batch_execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .await
        .expect("insert");
    assert_eq!(scan_count(&c).await, 5, "in-txn COUNT warms cache");
    c.batch_execute("COMMIT").await.expect("commit");
    assert_eq!(scan_count(&c).await, 5, "committed COUNT(*) stays 5");

    // SAVEPOINT rollback: a sub-transaction's rows are rolled back; the
    // post-rollback COUNT(*) must reflect only the surviving committed rows.
    c.batch_execute("BEGIN").await.expect("begin 2");
    c.batch_execute("INSERT INTO t VALUES (6, 60)")
        .await
        .expect("insert pre-sp");
    c.batch_execute("SAVEPOINT sp").await.expect("savepoint");
    c.batch_execute("INSERT INTO t VALUES (7, 70), (8, 80)")
        .await
        .expect("insert in sp");
    assert_eq!(scan_count(&c).await, 8, "in-sp COUNT sees 8");
    c.batch_execute("ROLLBACK TO SAVEPOINT sp")
        .await
        .expect("rollback to sp");
    assert_eq!(scan_count(&c).await, 6, "after sp rollback COUNT is 6");
    c.batch_execute("COMMIT").await.expect("commit 2");
    assert_eq!(scan_count(&c).await, 6, "committed count is 6");

    drop(c);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}
