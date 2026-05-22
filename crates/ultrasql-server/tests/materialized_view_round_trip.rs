//! End-to-end append-only materialized view tests.

use std::net::SocketAddr;
use std::path::Path;
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
        "host={host} port={port} user=tester application_name=materialized_view_test",
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

async fn start_persistent_server_and_connect(
    data_dir: &Path,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::init(data_dir).expect("persistent server init"));
    let server_handle = tokio::spawn(serve_listener(listener, server));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=materialized_view_restart_test",
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
    let _ = server_handle.await;
}

#[tokio::test]
async fn materialized_view_snapshots_then_appends_from_source_inserts() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE mv_src (id INT NOT NULL, amount INT NOT NULL)")
        .await
        .expect("create source");
    client
        .batch_execute("INSERT INTO mv_src VALUES (1, 10), (2, 20)")
        .await
        .expect("seed source");

    client
        .batch_execute("CREATE MATERIALIZED VIEW mv_copy AS SELECT id, amount FROM mv_src")
        .await
        .expect("create materialized view");

    let initial = client
        .query("SELECT id, amount FROM mv_copy ORDER BY id", &[])
        .await
        .expect("select initial materialized rows");
    assert_eq!(initial.len(), 2);
    assert_eq!(initial[0].get::<_, i32>(0), 1);
    assert_eq!(initial[0].get::<_, i32>(1), 10);
    assert_eq!(initial[1].get::<_, i32>(0), 2);
    assert_eq!(initial[1].get::<_, i32>(1), 20);

    client
        .batch_execute("INSERT INTO mv_src VALUES (3, 30)")
        .await
        .expect("append source");

    let after_append = client
        .query("SELECT id, amount FROM mv_copy ORDER BY id", &[])
        .await
        .expect("select appended materialized rows");
    assert_eq!(after_append.len(), 3);
    assert_eq!(after_append[2].get::<_, i32>(0), 3);
    assert_eq!(after_append[2].get::<_, i32>(1), 30);

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("INSERT INTO mv_src VALUES (4, 40)")
        .await
        .expect("append source in transaction");
    client.batch_execute("COMMIT").await.expect("commit");

    let after_commit = client
        .query("SELECT id, amount FROM mv_copy ORDER BY id", &[])
        .await
        .expect("select committed materialized rows");
    assert_eq!(after_commit.len(), 4);
    assert_eq!(after_commit[3].get::<_, i32>(0), 4);
    assert_eq!(after_commit[3].get::<_, i32>(1), 40);

    let update_err = client
        .batch_execute("UPDATE mv_src SET amount = 99 WHERE id = 1")
        .await
        .expect_err("updates to append-only source must be rejected");
    let db_err = update_err
        .as_db_error()
        .expect("server-sent ErrorResponse for update rejection");
    assert!(
        db_err
            .message()
            .contains("append-only materialized view source"),
        "expected append-only materialized view error, got {:?}",
        db_err.message()
    );

    shutdown(client, server_handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appended_materialized_view_rows_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let (client, _conn, server_handle) = start_persistent_server_and_connect(data_dir.path()).await;
    client
        .batch_execute("CREATE TABLE mv_restart_src (id INT NOT NULL, amount INT NOT NULL)")
        .await
        .expect("create source");
    client
        .batch_execute("INSERT INTO mv_restart_src VALUES (1, 10), (2, 20)")
        .await
        .expect("seed source");
    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv_restart_copy AS SELECT id, amount FROM mv_restart_src",
        )
        .await
        .expect("create materialized view");
    client
        .batch_execute("INSERT INTO mv_restart_src VALUES (3, 30)")
        .await
        .expect("append source");
    shutdown(client, server_handle).await;

    let (client, _conn, server_handle) = start_persistent_server_and_connect(data_dir.path()).await;
    let rows = client
        .query("SELECT id, amount FROM mv_restart_copy ORDER BY id", &[])
        .await
        .expect("select materialized view after restart");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].get::<_, i32>(0), 3);
    assert_eq!(rows[2].get::<_, i32>(1), 30);
    shutdown(client, server_handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialized_view_keeps_maintaining_source_after_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let (client, _conn, server_handle) = start_persistent_server_and_connect(data_dir.path()).await;
    client
        .batch_execute("CREATE TABLE mv_runtime_src (id INT NOT NULL, amount INT NOT NULL)")
        .await
        .expect("create source");
    client
        .batch_execute("INSERT INTO mv_runtime_src VALUES (1, 10), (2, 20)")
        .await
        .expect("seed source");
    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW mv_runtime_copy AS SELECT id, amount FROM mv_runtime_src",
        )
        .await
        .expect("create materialized view");
    shutdown(client, server_handle).await;

    let (client, _conn, server_handle) = start_persistent_server_and_connect(data_dir.path()).await;
    client
        .batch_execute("INSERT INTO mv_runtime_src VALUES (3, 30)")
        .await
        .expect("append after restart");
    let rows = client
        .query("SELECT id, amount FROM mv_runtime_copy ORDER BY id", &[])
        .await
        .expect("select materialized view after restarted append");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].get::<_, i32>(0), 3);
    assert_eq!(rows[2].get::<_, i32>(1), 30);
    shutdown(client, server_handle).await;
}
