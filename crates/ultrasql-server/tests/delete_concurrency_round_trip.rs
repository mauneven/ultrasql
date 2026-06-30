use std::time::Duration;

use tokio_postgres::NoTls;

pub mod support;

use support::{shutdown, start_sample_server};

/// Connect a second client. Returns the client and the connection task handle;
/// the caller aborts the handle before `shutdown` so the server can drain.
async fn connect_peer(
    running: &support::RunningServer,
    app: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host={host} port={port} user=tester application_name={app}",
        host = running.bound.ip(),
        port = running.bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("peer connect");
    let handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("peer connection error: {e}");
        }
    });
    (client, handle)
}

/// A concurrent fused `(Int32, Int32)` DELETE that hits an unresolved foreign
/// writer must surface as a retryable serialization failure (SQLSTATE 40001),
/// not silently skip / double-stamp the contended row (a lost delete). After
/// the holder commits, the retry succeeds — proving the conflict was transient.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_fused_delete_conflict_is_serialization_failure_40001() {
    let running = start_sample_server("delete_conflict_40001_test").await;
    let client_a = &running.client;
    // (Int32, Int32) schema, no index: the fused in-place DELETE path.
    client_a
        .batch_execute(
            "CREATE TABLE del_conflict (id INT NOT NULL, v INT NOT NULL);
             INSERT INTO del_conflict VALUES (1, 10), (2, 20);",
        )
        .await
        .expect("setup conflict rows");
    let (client_b, peer_handle) = connect_peer(&running, "delete_conflict_40001_b").await;

    // A holds an uncommitted in-place DELETE on the only matching row.
    client_a
        .batch_execute("BEGIN; DELETE FROM del_conflict WHERE id = 1;")
        .await
        .expect("client a holds in-place delete");

    // B's DELETE must fail fast with a write conflict, not block and not
    // silently report zero rows deleted.
    let conflict = tokio::time::timeout(
        Duration::from_secs(5),
        client_b.batch_execute("DELETE FROM del_conflict WHERE id = 1;"),
    )
    .await
    .expect("client b delete did not block on the unresolved writer")
    .expect_err("concurrent in-place DELETE must conflict");
    assert_eq!(
        conflict.code().map(|c| c.code().to_owned()),
        Some("40001".to_owned()),
        "concurrent-delete write conflict must be classified 40001, got {conflict:?}"
    );

    // Positive control: once A commits, B's retry succeeds (the row is gone, so
    // it deletes nothing but does not error).
    client_a.batch_execute("COMMIT;").await.expect("a commit");
    tokio::time::timeout(
        Duration::from_secs(5),
        client_b.batch_execute("DELETE FROM del_conflict WHERE id = 1;"),
    )
    .await
    .expect("client b retry did not hang")
    .expect("client b retry succeeds after conflict clears");

    // Row 1 gone (A's committed delete); row 2 untouched.
    let ids: Vec<i32> = client_a
        .query("SELECT id FROM del_conflict ORDER BY id", &[])
        .await
        .expect("read remaining rows")
        .iter()
        .map(|r| r.get::<_, i32>(0))
        .collect();
    assert_eq!(ids, vec![2], "only the uncontended row should remain");

    drop(client_b);
    peer_handle.abort();
    shutdown(running).await;
}

/// Concurrent fused DELETEs of DISJOINT rows must both commit — the unresolved
/// writer on one row must not raise a spurious conflict for a delete that
/// matches a different row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_fused_delete_disjoint_rows_both_commit() {
    let running = start_sample_server("delete_disjoint_test").await;
    let client_a = &running.client;
    client_a
        .batch_execute(
            "CREATE TABLE del_disjoint (id INT NOT NULL, v INT NOT NULL);
             INSERT INTO del_disjoint VALUES (1, 10), (2, 20);",
        )
        .await
        .expect("setup disjoint rows");
    let (client_b, peer_handle) = connect_peer(&running, "delete_disjoint_b").await;

    client_a
        .batch_execute("BEGIN; DELETE FROM del_disjoint WHERE id = 1;")
        .await
        .expect("client a holds in-place delete");

    tokio::time::timeout(
        Duration::from_secs(5),
        client_b.batch_execute("BEGIN; DELETE FROM del_disjoint WHERE id = 2; COMMIT;"),
    )
    .await
    .expect("client b disjoint delete did not hang")
    .expect("client b disjoint delete succeeds");

    client_a.batch_execute("COMMIT;").await.expect("a commit");

    let rows = client_a
        .query("SELECT id FROM del_disjoint ORDER BY id", &[])
        .await
        .expect("read remaining rows");
    assert_eq!(rows.len(), 0, "both disjoint deletes should have applied");

    drop(client_b);
    peer_handle.abort();
    shutdown(running).await;
}

/// The general (indexed, non-fused) delete path takes a blocking EvalPlanQual
/// row lock, so a concurrent deleter of the same row WAITS for the holder
/// instead of double-stamping. After the holder commits, the waiter re-reads,
/// finds the row already deleted, and deletes nothing without error
/// (PostgreSQL READ COMMITTED wait+re-read; no lost delete).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_indexed_delete_waits_then_sees_committed_delete() {
    let running = start_sample_server("delete_indexed_wait_test").await;
    let client_a = &running.client;
    // An index forces the general ModifyTable + EvalPlanQual path (not fused).
    client_a
        .batch_execute(
            "CREATE TABLE del_indexed (id INT NOT NULL, v INT NOT NULL);
             INSERT INTO del_indexed VALUES (1, 10), (2, 20);
             CREATE INDEX del_indexed_id_idx ON del_indexed(id);",
        )
        .await
        .expect("setup indexed rows");
    let (client_b, peer_handle) = connect_peer(&running, "delete_indexed_wait_b").await;

    client_a
        .batch_execute("BEGIN; DELETE FROM del_indexed WHERE id = 1;")
        .await
        .expect("client a holds delete");

    // B's delete of the same row must block on A's EvalPlanQual lock.
    let b_task = tokio::spawn(async move {
        let r = client_b
            .batch_execute("DELETE FROM del_indexed WHERE id = 1;")
            .await;
        (client_b, r)
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !b_task.is_finished(),
        "general-path delete must BLOCK on the holder's row lock, not double-stamp"
    );

    // A commits -> B unblocks, re-reads, finds the row already deleted -> Ok.
    client_a.batch_execute("COMMIT;").await.expect("a commit");
    let (client_b, b_result) = tokio::time::timeout(Duration::from_secs(5), b_task)
        .await
        .expect("b did not hang after a committed")
        .expect("b task joins");
    b_result.expect("b's delete succeeds (row already gone, no lost delete)");

    let ids: Vec<i32> = client_a
        .query("SELECT id FROM del_indexed ORDER BY id", &[])
        .await
        .expect("read remaining rows")
        .iter()
        .map(|r| r.get::<_, i32>(0))
        .collect();
    assert_eq!(ids, vec![2], "row 1 deleted exactly once; row 2 remains");

    drop(client_b);
    peer_handle.abort();
    shutdown(running).await;
}

/// No spurious conflict over a ROLLED-BACK deleter: a delete whose xmax names
/// an *aborted* transaction is not "in progress", so a later DELETE proceeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fused_delete_over_aborted_deleter_succeeds() {
    let running = start_sample_server("delete_aborted_deleter_test").await;
    let client_a = &running.client;
    client_a
        .batch_execute(
            "CREATE TABLE del_aborted (id INT NOT NULL, v INT NOT NULL);
             INSERT INTO del_aborted VALUES (1, 10), (2, 20);",
        )
        .await
        .expect("setup rows");
    let (client_b, peer_handle) = connect_peer(&running, "delete_aborted_b").await;

    // A deletes row 1 then ROLLS BACK — row 1's xmax now names an aborted txn.
    client_a
        .batch_execute("BEGIN; DELETE FROM del_aborted WHERE id = 1; ROLLBACK;")
        .await
        .expect("client a deletes then rolls back");

    // B's delete of row 1 must succeed (no spurious 40001) and remove it.
    tokio::time::timeout(
        Duration::from_secs(5),
        client_b.batch_execute("DELETE FROM del_aborted WHERE id = 1;"),
    )
    .await
    .expect("client b delete did not hang")
    .expect("delete over an aborted deleter must succeed");

    let ids: Vec<i32> = client_a
        .query("SELECT id FROM del_aborted ORDER BY id", &[])
        .await
        .expect("read remaining rows")
        .iter()
        .map(|r| r.get::<_, i32>(0))
        .collect();
    assert_eq!(
        ids,
        vec![2],
        "row 1 (re-deleted by B) is gone; row 2 remains"
    );

    drop(client_b);
    peer_handle.abort();
    shutdown(running).await;
}
