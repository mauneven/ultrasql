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
