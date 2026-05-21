//! End-to-end checks for the first UltraSQL aggregating-index prototype.
//!
//! This is intentionally narrow: `CREATE AGGREGATING INDEX` stores
//! group-key metadata plus SUM/COUNT summaries, the query lowerer can
//! rewrite the matching dashboard aggregate shape to the summary, and
//! DML marks the summary dirty so the next read rebuilds before serving
//! rows. Page-backed storage is still a later production slice.

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
        "host={host} port={port} user=tester application_name=aggregating_index_test",
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

async fn tenant_rollup(client: &tokio_postgres::Client) -> Vec<(i32, i32, i64, i64)> {
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
        .expect("rollup query")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect()
}

fn collect_plan_text(rows: &[tokio_postgres::Row]) -> String {
    rows.iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn create_aggregating_index_rewrites_rollup_and_refreshes_after_dml() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE fact_events (
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL
             )",
        )
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO fact_events VALUES
                (7, 1, 10),
                (7, 1, 20),
                (7, 2, 5),
                (8, 1, 100)",
        )
        .await
        .expect("seed");
    client
        .batch_execute(
            "CREATE AGGREGATING INDEX fact_events_rollup
                ON fact_events (tenant_id, bucket, sum(amount), count(*))",
        )
        .await
        .expect("setup aggregating index");

    assert_eq!(
        tenant_rollup(&client).await,
        vec![(7, 1, 30, 2), (7, 2, 5, 1)]
    );

    let rows = client
        .query(
            "EXPLAIN ANALYZE SELECT tenant_id, bucket, SUM(amount), COUNT(*) \
             FROM fact_events \
             WHERE tenant_id = 7 \
             GROUP BY tenant_id, bucket \
             ORDER BY tenant_id, bucket",
            &[],
        )
        .await
        .expect("EXPLAIN ANALYZE");
    let text = collect_plan_text(&rows);
    assert!(
        text.contains("Aggregating Index: selected fact_events_rollup"),
        "EXPLAIN ANALYZE must report aggregating-index rewrite, got: {text}"
    );

    client
        .batch_execute("INSERT INTO fact_events VALUES (7, 1, 12)")
        .await
        .expect("insert after index build");
    assert_eq!(
        tenant_rollup(&client).await,
        vec![(7, 1, 42, 3), (7, 2, 5, 1)]
    );

    client
        .batch_execute("DELETE FROM fact_events WHERE amount = 5")
        .await
        .expect("delete after index build");
    assert_eq!(tenant_rollup(&client).await, vec![(7, 1, 42, 3)]);

    shutdown(client, server_handle).await;
}
