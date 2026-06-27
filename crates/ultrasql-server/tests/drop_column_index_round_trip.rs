//! `ALTER TABLE ... DROP COLUMN` must keep the table's secondary indexes and
//! position-referencing constraints consistent with the physically compacted
//! schema.
//!
//! UltraSQL stores an index's key columns as POSITIONAL attnums into the table
//! schema and physically compacts the schema (and rewrites the heap) on
//! `DROP COLUMN`, shifting every column after the dropped one down one slot.
//! Before the fix, the index metadata was left untouched: an index whose key
//! sat after the dropped column pointed at the wrong column (wrong / missing
//! rows) or out of bounds (panic / error), and an index ON the dropped column
//! was left dangling.
//!
//! This mirrors PostgreSQL's observable behaviour (verified against PG 14):
//! `DROP COLUMN` drops every index/constraint whose key includes the dropped
//! column and re-points the survivors at the shifted positions.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start_server_and_connect() -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, server));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=drop_column_index_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    (client, conn_handle, server_handle)
}

async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// THE REPRO: an index on a column AFTER the dropped one is re-pointed, so an
/// equality probe through it returns the correct rows (not the shifted-column's
/// rows), and a range scan returns the correct order.
#[tokio::test]
async fn drop_column_repoints_index_on_later_column() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT, c INT)")
        .await
        .expect("create");
    client
        .batch_execute("CREATE INDEX idx_b ON t (b)")
        .await
        .expect("create index");
    client
        .batch_execute("INSERT INTO t VALUES (1, 20, 300), (4, 50, 600), (7, 80, 900)")
        .await
        .expect("seed");

    client
        .batch_execute("ALTER TABLE t DROP COLUMN a")
        .await
        .expect("drop column a");

    // idx_b.columns was [1] (pointing at b); after dropping a it must shift to
    // [0] so the probe still hits b. A stale [1] would now read c -> 0 rows.
    let rows = client
        .query("SELECT b, c FROM t WHERE b = 20", &[])
        .await
        .expect("probe via idx_b");
    assert_eq!(rows.len(), 1, "exactly one row has b=20");
    assert_eq!(rows[0].get::<_, i32>(0), 20);
    assert_eq!(rows[0].get::<_, i32>(1), 300);

    // A range scan through the (now correct) index returns ordered rows.
    let ordered = client
        .query("SELECT b FROM t WHERE b >= 20 ORDER BY b", &[])
        .await
        .expect("range scan via idx_b");
    let bs: Vec<i32> = ordered.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(bs, vec![20, 50, 80]);

    shutdown(client, server_handle).await;
}

/// COLLISION CORRECTNESS: after dropping `a`, column `c` shifts into the
/// position the stale `idx_b` used to occupy (attnum 1). A correct
/// implementation re-points `idx_b` to attnum 0 so that nothing claims to index
/// `c` at its new position; a probe on `c` and a probe on `b` both return the
/// right rows. This guards the misrouting hazard the position-shift creates.
#[tokio::test]
async fn drop_column_stale_index_does_not_misroute_other_column_probe() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT, c INT)")
        .await
        .expect("create");
    client
        .batch_execute("CREATE INDEX idx_b ON t (b)")
        .await
        .expect("index on b (attnum 1)");
    client
        .batch_execute("INSERT INTO t VALUES (1, 20, 300), (4, 50, 600)")
        .await
        .expect("seed");

    client
        .batch_execute("ALTER TABLE t DROP COLUMN a")
        .await
        .expect("drop column a");

    // c is now at position 1. A stale idx_b=[1] would misroute this probe.
    let rows = client
        .query("SELECT b, c FROM t WHERE c = 300", &[])
        .await
        .expect("probe on c must not be misrouted to the b-index");
    assert_eq!(rows.len(), 1, "exactly one row has c=300");
    assert_eq!(rows[0].get::<_, i32>(0), 20);
    assert_eq!(rows[0].get::<_, i32>(1), 300);

    // And the b-probe still works through the re-pointed idx_b ([1] -> [0]).
    let by_b = client
        .query("SELECT c FROM t WHERE b = 50", &[])
        .await
        .expect("probe on b through re-pointed idx_b");
    assert_eq!(by_b.len(), 1);
    assert_eq!(by_b[0].get::<_, i32>(0), 600);

    shutdown(client, server_handle).await;
}

/// INDEX ON THE DROPPED COLUMN: the index is removed entirely (PG drops the
/// whole index if any key column is dropped); the table stays queryable with
/// no out-of-bounds error.
#[tokio::test]
async fn drop_column_drops_index_on_dropped_column() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT, c INT)")
        .await
        .expect("create");
    client
        .batch_execute("CREATE INDEX idx_a ON t (a)")
        .await
        .expect("create index on a");
    client
        .batch_execute("INSERT INTO t VALUES (1, 20, 300), (4, 50, 600)")
        .await
        .expect("seed");

    client
        .batch_execute("ALTER TABLE t DROP COLUMN a")
        .await
        .expect("drop column a");

    // The dropped index name is free again -> it really is gone (not dangling).
    client
        .batch_execute("CREATE INDEX idx_a ON t (b)")
        .await
        .expect("idx_a name reusable after the original was dropped");

    let rows = client
        .query("SELECT b, c FROM t ORDER BY b", &[])
        .await
        .expect("table queryable after dropping its indexed column");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 20);
    assert_eq!(rows[1].get::<_, i32>(0), 50);

    shutdown(client, server_handle).await;
}

/// INDEX ON THE LAST COLUMN, drop an earlier one: the surviving index shifts
/// from the now-out-of-bounds position to the correct one (the old OOB panic
/// case). `WHERE c = ...` returns the right row.
#[tokio::test]
async fn drop_column_repoints_index_on_last_column() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT, c INT)")
        .await
        .expect("create");
    client
        .batch_execute("CREATE INDEX idx_c ON t (c)")
        .await
        .expect("create index on c (attnum 2)");
    client
        .batch_execute("INSERT INTO t VALUES (1, 20, 300), (4, 50, 600)")
        .await
        .expect("seed");

    client
        .batch_execute("ALTER TABLE t DROP COLUMN a")
        .await
        .expect("drop column a");

    // idx_c.columns was [2]; new schema has only 2 columns, so a stale [2] is
    // out of bounds. After the shift it is [1] -> probes c correctly.
    let rows = client
        .query("SELECT b, c FROM t WHERE c = 300", &[])
        .await
        .expect("probe via shifted idx_c");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 20);
    assert_eq!(rows[0].get::<_, i32>(1), 300);

    shutdown(client, server_handle).await;
}

/// MULTI-COLUMN INDEX: dropping a column the index does NOT cover shifts both
/// key positions; dropping a column the index DOES cover removes the whole
/// index (PG drops the index if any key column is dropped).
#[tokio::test]
async fn drop_column_multi_column_index_shift_and_drop() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT, c INT)")
        .await
        .expect("create");
    client
        .batch_execute("CREATE INDEX idx_bc ON t (b, c)")
        .await
        .expect("create composite index");
    client
        .batch_execute("INSERT INTO t VALUES (1, 20, 300), (4, 50, 600)")
        .await
        .expect("seed");

    // Drop a (not covered by idx_bc): idx_bc.columns [1,2] -> [0,1].
    client
        .batch_execute("ALTER TABLE t DROP COLUMN a")
        .await
        .expect("drop column a");
    let rows = client
        .query("SELECT b, c FROM t WHERE b = 50 AND c = 600", &[])
        .await
        .expect("probe via shifted idx_bc");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 50);

    // Now drop b (covered by idx_bc): the whole index must go.
    client
        .batch_execute("ALTER TABLE t DROP COLUMN b")
        .await
        .expect("drop column b");
    // idx_bc gone -> its name is reusable.
    client
        .batch_execute("CREATE INDEX idx_bc ON t (c)")
        .await
        .expect("idx_bc removed because it covered the dropped column");

    let rows = client
        .query("SELECT c FROM t WHERE c = 600", &[])
        .await
        .expect("table queryable");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 600);

    shutdown(client, server_handle).await;
}

/// UNIQUE on a SURVIVING column is re-pointed: after dropping an earlier
/// column, the backing index of `UNIQUE (b)` shifts from attnum 1 to 0 and a
/// probe through it still resolves `b` correctly (the constraint and its index
/// survive with their names intact).
///
/// NOTE: this asserts the *metadata* re-pointing this fix is responsible for.
/// Insert-time UNIQUE *enforcement* (the 23505 path) is independently lost
/// across any `DROP COLUMN` on this engine — a pre-existing defect where the
/// heap rewrite re-TIDs rows without rebuilding the unique B-tree maintainer —
/// so it is deliberately not asserted here.
#[tokio::test]
async fn drop_column_unique_constraint_repointed_and_survives() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT, c INT)")
        .await
        .expect("create");
    client
        .batch_execute("ALTER TABLE t ADD CONSTRAINT uq_b UNIQUE (b)")
        .await
        .expect("add unique on b");
    client
        .batch_execute("INSERT INTO t VALUES (1, 20, 300), (4, 50, 600)")
        .await
        .expect("seed");

    client
        .batch_execute("ALTER TABLE t DROP COLUMN a")
        .await
        .expect("drop column a");

    // A stale index would point at c here (or out of bounds); the re-pointed
    // unique index resolves b correctly.
    let rows = client
        .query("SELECT b, c FROM t WHERE b = 20", &[])
        .await
        .expect("probe via re-pointed uq_b backing index");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 20);
    assert_eq!(rows[0].get::<_, i32>(1), 300);

    // The constraint and its backing index survive with their original names
    // (re-adding under the same name would conflict if they were still present).
    let err = client
        .batch_execute("ALTER TABLE t ADD CONSTRAINT uq_b UNIQUE (c)")
        .await
        .expect_err("uq_b still exists after the drop -> duplicate name rejected");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42710");

    shutdown(client, server_handle).await;
}

/// UNIQUE that COVERS the dropped column is removed: after dropping b, the
/// UNIQUE(b) backing index is gone, so re-using the same b value succeeds.
#[tokio::test]
async fn drop_column_drops_unique_covering_dropped_column() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT, c INT)")
        .await
        .expect("create");
    client
        .batch_execute("ALTER TABLE t ADD CONSTRAINT uq_b UNIQUE (b)")
        .await
        .expect("add unique on b");
    client
        .batch_execute("INSERT INTO t VALUES (1, 20, 300)")
        .await
        .expect("seed");

    client
        .batch_execute("ALTER TABLE t DROP COLUMN b")
        .await
        .expect("drop the uniquely-constrained column b");

    // The UNIQUE is gone with its column; the constraint name is reusable on a
    // surviving column, which also proves the old one was fully removed.
    client
        .batch_execute("ALTER TABLE t ADD CONSTRAINT uq_b UNIQUE (c)")
        .await
        .expect("uq_b name reusable after the original was dropped");

    shutdown(client, server_handle).await;
}

/// CHECK referencing the dropped column is DROPPED (matching PG 14): the
/// constraint disappears and no stale wrong-position predicate is enforced.
#[tokio::test]
async fn drop_column_drops_check_referencing_dropped_column() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT, c INT)")
        .await
        .expect("create");
    client
        .batch_execute("ALTER TABLE t ADD CONSTRAINT ck_a CHECK (a > 0)")
        .await
        .expect("add check on a");
    client
        .batch_execute("INSERT INTO t VALUES (1, 200, 5)")
        .await
        .expect("seed");

    client
        .batch_execute("ALTER TABLE t DROP COLUMN a")
        .await
        .expect("drop column a (the column ck_a references)");

    // ck_a is gone -> a value that would have failed `a > 0` (now reading some
    // other column) does not get spuriously rejected; the name is reusable.
    client
        .batch_execute("ALTER TABLE t ADD CONSTRAINT ck_a CHECK (b > 0)")
        .await
        .expect("ck_a name reusable -> original CHECK was dropped, not dangling");

    shutdown(client, server_handle).await;
}

/// CHECK on a SURVIVING column is RE-POINTED so it keeps enforcing the right
/// column after the shift (verified against PG 14): a valid row passes, an
/// invalid row is rejected with 23514.
#[tokio::test]
async fn drop_column_repoints_surviving_check() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT, c INT)")
        .await
        .expect("create");
    // CHECK references b (column index 1). After DROP COLUMN a, b shifts to 0;
    // a stale predicate would read c instead of b.
    client
        .batch_execute("ALTER TABLE t ADD CONSTRAINT ck_b CHECK (b > 100)")
        .await
        .expect("add check on b");
    client
        .batch_execute("INSERT INTO t VALUES (1, 200, 5)")
        .await
        .expect("seed");

    client
        .batch_execute("ALTER TABLE t DROP COLUMN a")
        .await
        .expect("drop column a");

    // A valid b passes (b=300 > 100), regardless of c.
    client
        .batch_execute("INSERT INTO t VALUES (300, 5)")
        .await
        .expect("valid b passes the re-pointed CHECK");

    // An invalid b is rejected with check_violation (23514), regardless of c.
    let err = client
        .batch_execute("INSERT INTO t VALUES (50, 999)")
        .await
        .expect_err("invalid b must violate the re-pointed CHECK");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23514");

    shutdown(client, server_handle).await;
}

/// NO-REGRESSION: `DROP COLUMN` on a table with no indexes still works, and a
/// subsequent INSERT is correctly maintained — an index probe and a seq scan
/// agree.
#[tokio::test]
async fn drop_column_no_indexes_then_maintains_new_index() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT, c INT)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 20, 300)")
        .await
        .expect("seed");
    client
        .batch_execute("ALTER TABLE t DROP COLUMN a")
        .await
        .expect("drop column with no indexes still works");

    // Build an index after the drop and insert through it.
    client
        .batch_execute("CREATE INDEX idx_b ON t (b)")
        .await
        .expect("post-drop index");
    client
        .batch_execute("INSERT INTO t VALUES (50, 600)")
        .await
        .expect("insert maintains the new index");

    // Index probe and a seq scan agree.
    let via_index = client
        .query("SELECT c FROM t WHERE b = 50", &[])
        .await
        .expect("index probe");
    assert_eq!(via_index.len(), 1);
    assert_eq!(via_index[0].get::<_, i32>(0), 600);

    let via_seq = client
        .query("SELECT c FROM t WHERE b + 0 = 50", &[])
        .await
        .expect("seq scan (defeats index)");
    assert_eq!(via_seq.len(), 1);
    assert_eq!(via_seq[0].get::<_, i32>(0), 600);

    shutdown(client, server_handle).await;
}
