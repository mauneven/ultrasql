use std::time::Duration;

use tokio_postgres::NoTls;

pub mod support;

use support::{shutdown, start_sample_server};

/// `idle_in_transaction_session_timeout` must roll back a transaction left idle
/// past the bound, disconnect the session (SQLSTATE 25P03), and release the
/// row/relation locks it held so other backends are not blocked by an
/// abandoned transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_in_transaction_session_timeout_aborts_and_disconnects() {
    let running = start_sample_server("idle_in_txn_timeout").await;
    let client_a = &running.client;
    // Indexed table -> the general ModifyTable + EvalPlanQual delete path, so
    // B's in-transaction DELETE takes a real blocking row lock.
    client_a
        .batch_execute(
            "CREATE TABLE t (id INT NOT NULL, v INT NOT NULL);
             INSERT INTO t VALUES (1, 10), (2, 20);
             CREATE INDEX t_id_idx ON t(id);",
        )
        .await
        .expect("setup table");

    // Second connection that will go idle inside a transaction.
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=idle_in_txn_b",
        host = running.bound.ip(),
        port = running.bound.port()
    );
    let (client_b, connection_b) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("b connect");
    let connection_b = tokio::spawn(async move {
        let _ = connection_b.await;
    });

    client_b
        .batch_execute("SET idle_in_transaction_session_timeout = 300")
        .await
        .expect("set idle_in_transaction_session_timeout");
    // Open a transaction and take a row lock, then go idle.
    client_b
        .batch_execute("BEGIN; DELETE FROM t WHERE id = 1;")
        .await
        .expect("b holds an open transaction with a row lock");

    // Wait well past the 300ms bound.
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // B's session is terminated: its next statement fails (connection closed).
    let after = client_b.batch_execute("SELECT 1").await;
    assert!(
        after.is_err(),
        "session must be terminated after idle-in-transaction timeout, got {after:?}"
    );

    // B's transaction was rolled back and its lock released: A can DELETE the
    // same row without blocking, and row 1 is still present (B's delete undone).
    tokio::time::timeout(
        Duration::from_secs(5),
        client_a.batch_execute("DELETE FROM t WHERE id = 1;"),
    )
    .await
    .expect("A must not block on the abandoned transaction's lock")
    .expect("A deletes row 1");

    let ids: Vec<i32> = client_a
        .query("SELECT id FROM t ORDER BY id", &[])
        .await
        .expect("read remaining rows")
        .iter()
        .map(|r| r.get::<_, i32>(0))
        .collect();
    assert_eq!(ids, vec![2], "row 1 deleted once by A; row 2 remains");

    drop(client_b);
    connection_b.abort();
    let _ = connection_b.await;
    shutdown(running).await;
}

/// A session idle in a transaction with the timeout DISABLED (0, the default)
/// is not disconnected — the transaction stays open and usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_in_transaction_default_keeps_session_open() {
    let running = start_sample_server("idle_in_txn_default").await;
    let client_a = &running.client;
    client_a
        .batch_execute(
            "CREATE TABLE t2 (id INT NOT NULL, v INT NOT NULL); INSERT INTO t2 VALUES (1, 10);",
        )
        .await
        .expect("setup");

    client_a
        .batch_execute("BEGIN; UPDATE t2 SET v = 11 WHERE id = 1;")
        .await
        .expect("begin + update");
    // No timeout set (default 0): idling does not terminate the session.
    tokio::time::sleep(Duration::from_millis(500)).await;
    client_a
        .batch_execute("COMMIT;")
        .await
        .expect("transaction still open and committable after idling");

    let v: i32 = client_a
        .query_one("SELECT v FROM t2 WHERE id = 1", &[])
        .await
        .expect("read")
        .get(0);
    assert_eq!(v, 11);

    shutdown(running).await;
}
