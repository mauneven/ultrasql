//! `ANALYZE` Simple-Query handler tests.
//!
//! Verifies that the wire surface accepts `ANALYZE` and that the
//! server refreshes relation statistics in the in-memory stats catalog.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

mod support;

use support::{shutdown as graceful_shutdown, start_persistent_server};

async fn start_server_and_connect() -> (
    tokio_postgres::Client,
    Arc<Server>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_for_task = Arc::clone(&server);
    let server_handle = tokio::spawn(serve_listener(listener, server_for_task));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=analyze_test",
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
    (client, server, conn_handle, server_handle)
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
async fn analyze_bare_returns_command_tag() {
    let (client, _server, _conn, server_handle) = start_server_and_connect().await;
    client.batch_execute("ANALYZE").await.expect("ANALYZE");
    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn analyze_table_returns_command_tag() {
    let (client, server, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .expect("seed");
    client.batch_execute("ANALYZE t").await.expect("ANALYZE t");
    let stats = server
        .lookup_relation_stats("t")
        .expect("ANALYZE should register relation stats");
    assert_eq!(stats.row_count, 3, "ANALYZE should see all inserted rows");
    let class_rows = client
        .query(
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = 't'",
            &[],
        )
        .await
        .expect("pg_class query");
    assert_eq!(class_rows.len(), 1);
    let table_oid: i64 = class_rows[0].get(0);
    let stat_sql = format!(
        "SELECT staattnum, stanullfrac, stadistinct \
             FROM pg_catalog.pg_statistic \
             WHERE starelid = {table_oid} \
             ORDER BY staattnum"
    );
    let stat_rows = client
        .query(&stat_sql, &[])
        .await
        .expect("pg_statistic query");
    assert_eq!(stat_rows.len(), 1);
    assert_eq!(stat_rows[0].get::<_, i16>(0), 1);
    assert_eq!(stat_rows[0].get::<_, f32>(1), 0.0);
    // Session survives — subsequent statements work.
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after ANALYZE");
    assert_eq!(rows.len(), 3);
    shutdown(client, server_handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn analyze_statistics_hydrate_optimizer_after_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp dir");

    let running = start_persistent_server(data_dir.path(), "analyze_restart_test").await;
    running
        .client
        .batch_execute("CREATE TABLE analyze_restart (id INT NOT NULL, label TEXT)")
        .await
        .expect("create analyze table");
    running
        .client
        .batch_execute("INSERT INTO analyze_restart VALUES (1, 'alpha'), (2, 'bravo'), (3, NULL)")
        .await
        .expect("seed analyze table");
    running
        .client
        .batch_execute("ANALYZE analyze_restart")
        .await
        .expect("analyze table");
    let before = running
        .server
        .lookup_relation_stats("analyze_restart")
        .expect("ANALYZE registers optimizer stats before restart");
    assert_eq!(before.row_count, 3);
    graceful_shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "analyze_restart_test").await;
    let after = running
        .server
        .lookup_relation_stats("analyze_restart")
        .expect("persistent pg_statistic rows hydrate optimizer stats after restart");
    assert_eq!(after.table, "analyze_restart");
    assert_eq!(after.columns.len(), 2);
    assert_eq!(after.columns[0].column_index, 0);
    assert_eq!(after.columns[1].column_index, 1);
    assert_eq!(after.row_count, 3);

    let table_oid: i64 = running
        .client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'analyze_restart'",
            &[],
        )
        .await
        .expect("pg_class row after restart")
        .get(0);
    let stat_rows = running
        .client
        .query(
            "SELECT staattnum FROM pg_catalog.pg_statistic
             WHERE starelid = $1
             ORDER BY staattnum",
            &[&table_oid],
        )
        .await
        .expect("pg_statistic rows after restart");
    let attnums: Vec<i16> = stat_rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(attnums, vec![1, 2]);

    graceful_shutdown(running).await;
}
