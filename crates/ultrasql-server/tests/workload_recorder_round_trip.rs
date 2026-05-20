//! End-to-end workload recorder tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start_server_and_connect() -> (
    Arc<Server>,
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, Arc::clone(&server)));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=workload_recorder_test",
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
    (server, client, conn_handle, server_handle)
}

async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

#[tokio::test]
async fn prepared_queries_record_redacted_bind_workload_stats_and_slow_log() {
    let (server, client, _conn, server_handle) = start_server_and_connect().await;
    server
        .workload_recorder
        .set_slow_query_threshold(Duration::ZERO);

    let stmt = client
        .prepare("SELECT id FROM users WHERE id = $1")
        .await
        .expect("prepare workload query");
    let rows = client.query(&stmt, &[&2_i32]).await.expect("execute $1=2");
    assert_eq!(rows.len(), 1);
    let rows = client.query(&stmt, &[&3_i32]).await.expect("execute $1=3");
    assert_eq!(rows.len(), 1);

    let snapshot = server.workload_recorder.snapshot();
    let stat = snapshot
        .iter()
        .find(|stat| stat.query == "SELECT id FROM users WHERE id = $1")
        .expect("prepared query recorded");
    assert_eq!(stat.calls, 2);
    assert_eq!(stat.rows, 2);
    assert_eq!(stat.bind_param_count, 1);
    assert!(stat.bind_params_redacted);
    assert_ne!(stat.query_id, 0);
    assert_ne!(stat.plan_hash, 0);
    assert!(stat.total_exec_time.as_nanos() > 0);

    let slow = server.workload_recorder.slow_queries();
    let slow_record = slow
        .iter()
        .find(|record| record.query == "SELECT id FROM users WHERE id = $1")
        .expect("prepared query slow-log record");
    assert_eq!(slow_record.bind_param_count, 1);
    assert!(slow_record.bind_params_redacted);
    assert!(
        !slow_record.query.contains('2') && !slow_record.query.contains('3'),
        "slow log must not leak concrete bind values"
    );

    let rows = client
        .query(
            "SELECT query, calls, \"rows\", bind_param_count \
             FROM pg_stat_statements \
             WHERE query = 'SELECT id FROM users WHERE id = $1'",
            &[],
        )
        .await
        .expect("scan pg_stat_statements");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<_, String>(0),
        "SELECT id FROM users WHERE id = $1"
    );
    assert_eq!(rows[0].get::<_, i64>(1), 2);
    assert_eq!(rows[0].get::<_, i64>(2), 2);
    assert_eq!(rows[0].get::<_, i32>(3), 1);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn failed_simple_queries_increment_error_counts() {
    let (server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("SELECT * FROM missing_workload_table")
        .await
        .expect_err("missing table query fails");

    let snapshot = server.workload_recorder.snapshot();
    let stat = snapshot
        .iter()
        .find(|stat| stat.query == "SELECT * FROM missing_workload_table")
        .expect("failed query recorded");
    assert_eq!(stat.calls, 1);
    assert_eq!(stat.errors, 1);
    assert_eq!(stat.rows, 0);
    assert!(
        stat.last_error
            .as_deref()
            .is_some_and(|e| e.contains("missing_workload_table"))
    );

    shutdown(client, server_handle).await;
}
