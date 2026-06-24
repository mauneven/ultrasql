use std::time::Duration;

use tokio_postgres::NoTls;

pub mod support;

use support::{shutdown, start_sample_server};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_indexed_updates_wait_and_apply_latest_row() {
    let running = start_sample_server("update_concurrency_test").await;
    let client_a = &running.client;
    client_a
        .batch_execute(
            "CREATE TABLE hot_update (id INT NOT NULL, v INT NOT NULL);
             INSERT INTO hot_update VALUES (1, 0);
             CREATE INDEX hot_update_id_idx ON hot_update(id);",
        )
        .await
        .expect("setup hot row");

    let conn_str = format!(
        "host={host} port={port} user=tester application_name=update_concurrency_b",
        host = running.bound.ip(),
        port = running.bound.port()
    );
    let (client_b, connection_b) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("client b connect");
    let connection_b = tokio::spawn(async move {
        if let Err(e) = connection_b.await {
            eprintln!("connection b error: {e}");
        }
    });

    client_a
        .batch_execute("BEGIN; UPDATE hot_update SET v = v + 1 WHERE id = 1;")
        .await
        .expect("client a holds update");

    let client_b_task = tokio::spawn(async move {
        client_b
            .batch_execute("BEGIN; UPDATE hot_update SET v = v + 1 WHERE id = 1; COMMIT;")
            .await
            .expect("client b waits then updates");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    client_a
        .batch_execute("COMMIT;")
        .await
        .expect("client a commit");
    tokio::time::timeout(Duration::from_secs(2), client_b_task)
        .await
        .expect("client b finishes")
        .expect("client b task joins");
    connection_b.abort();
    let _ = connection_b.await;

    let row = client_a
        .query_one("SELECT v FROM hot_update WHERE id = 1", &[])
        .await
        .expect("read hot row");
    let v: i32 = row.get(0);
    assert_eq!(v, 2);

    shutdown(running).await;
}

/// A non-indexed concurrent in-place UPDATE that hits an unresolved
/// writer must surface as a retryable serialization failure (SQLSTATE
/// 40001), not a generic error.
///
/// The table carries NO index on the predicate column, so the planner
/// lowers the `(Int32, Int32)` `SET v = v + 1 WHERE id = ?` UPDATE onto
/// the fused in-place scan path (`update_int32_pair_inplace_undo`). That
/// path does NOT take a row lock and wait — when it scans a tuple whose
/// in-place pre-image is still owned by an uncommitted writer and the
/// pre-image still matches the predicate, it raises
/// `HeapError::WriteConflict`. The executor now relabels that as
/// `ExecError::SerializationFailure`, which the server maps to 40001.
///
/// Connection A begins and performs the in-place UPDATE but stays open
/// (uncommitted). Connection B runs the same UPDATE; it must NOT wait,
/// and must fail with 40001. After A commits, B's retry succeeds — the
/// positive control proving the conflict was transient, not structural.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_unindexed_update_conflict_is_serialization_failure_40001() {
    let running = start_sample_server("update_conflict_40001_test").await;
    let client_a = &running.client;
    // No index on `id`: forces the fused in-place scan path that errors
    // on an unresolved writer instead of the indexed wait path.
    client_a
        .batch_execute(
            "CREATE TABLE conflict_row (id INT NOT NULL, v INT NOT NULL);
             INSERT INTO conflict_row VALUES (1, 0);",
        )
        .await
        .expect("setup conflict row");

    let conn_str = format!(
        "host={host} port={port} user=tester application_name=update_conflict_40001_b",
        host = running.bound.ip(),
        port = running.bound.port()
    );
    let (client_b, connection_b) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("client b connect");
    let connection_b = tokio::spawn(async move {
        if let Err(e) = connection_b.await {
            eprintln!("connection b error: {e}");
        }
    });

    // A holds an uncommitted in-place UPDATE on the only matching row.
    client_a
        .batch_execute("BEGIN; UPDATE conflict_row SET v = v + 1 WHERE id = 1;")
        .await
        .expect("client a holds in-place update");

    // B's UPDATE must fail fast with a write conflict, not block. Bound
    // it with a timeout so a hang fails the test instead of wedging it.
    let conflict_result = tokio::time::timeout(
        Duration::from_secs(5),
        client_b.batch_execute("UPDATE conflict_row SET v = v + 1 WHERE id = 1;"),
    )
    .await
    .expect("client b update did not block on the unresolved writer");

    let err = conflict_result.expect_err("concurrent in-place UPDATE must conflict");
    let sqlstate = err.code().expect("conflict must carry a server SQLSTATE");
    assert_eq!(
        sqlstate.code(),
        "40001",
        "concurrent-update write conflict must be classified as serialization_failure (40001), got {sqlstate:?}"
    );

    // Positive control: once A resolves its writer, B's retry succeeds.
    client_a
        .batch_execute("COMMIT;")
        .await
        .expect("client a commit");

    tokio::time::timeout(
        Duration::from_secs(5),
        client_b.batch_execute("UPDATE conflict_row SET v = v + 1 WHERE id = 1;"),
    )
    .await
    .expect("client b retry did not hang")
    .expect("client b retry succeeds after conflict clears");

    connection_b.abort();
    let _ = connection_b.await;

    // A's +1 then B's +1 (after retry) leaves the row at 2.
    let row = client_a
        .query_one("SELECT v FROM conflict_row WHERE id = 1", &[])
        .await
        .expect("read conflict row");
    let v: i32 = row.get(0);
    assert_eq!(v, 2);

    shutdown(running).await;
}
