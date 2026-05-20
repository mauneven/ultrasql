//! End-to-end physical projection summary tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_core::RelationId;
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
        "host={host} port={port} user=tester application_name=projection_summary_test",
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
async fn repeated_group_by_order_by_uses_cached_physical_projection_summary() {
    let (server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE projection_src (bucket INT NOT NULL, amount BIGINT NOT NULL)")
        .await
        .expect("create projection source");
    client
        .batch_execute("INSERT INTO projection_src VALUES (1, 10), (1, 15), (2, 7)")
        .await
        .expect("seed projection source");

    let sql = "SELECT bucket, COUNT(*) AS n, SUM(amount) AS total \
               FROM projection_src GROUP BY bucket ORDER BY bucket";
    let first = client.query(sql, &[]).await.expect("first grouped query");
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].get::<_, i32>(0), 1);
    assert_eq!(first[0].get::<_, i64>(1), 2);
    assert_eq!(first[0].get::<_, i64>(2), 25);
    assert_eq!(first[1].get::<_, i32>(0), 2);
    assert_eq!(first[1].get::<_, i64>(1), 1);
    assert_eq!(first[1].get::<_, i64>(2), 7);

    let snapshot = server.catalog_snapshot();
    let entry = snapshot
        .tables
        .get("projection_src")
        .expect("projection source entry");
    let rel = RelationId(entry.oid);
    let cached_after_first = server
        .heap
        .column_cache
        .get(rel)
        .expect("first aggregate scan builds column cache");
    assert_eq!(
        cached_after_first
            .cached_grouped_projection_wire
            .read()
            .len(),
        0,
        "first grouped query scans heap; repeat query builds summary projection"
    );

    let second = client.query(sql, &[]).await.expect("second grouped query");
    assert_eq!(second.len(), 2);
    assert_eq!(second[0].get::<_, i64>(2), 25);
    let cached_after_second = server
        .heap
        .column_cache
        .get(rel)
        .expect("column cache still valid");
    assert_eq!(
        cached_after_second
            .cached_grouped_projection_wire
            .read()
            .len(),
        1,
        "repeat query should populate one physical projection summary"
    );

    client
        .batch_execute("INSERT INTO projection_src VALUES (1, 5)")
        .await
        .expect("mutate projection source");
    assert!(
        server.heap.column_cache.get(rel).is_none(),
        "source insert invalidates cached physical summaries"
    );

    let after_insert = client
        .query(sql, &[])
        .await
        .expect("grouped query after invalidation");
    assert_eq!(after_insert.len(), 2);
    assert_eq!(after_insert[0].get::<_, i64>(1), 3);
    assert_eq!(after_insert[0].get::<_, i64>(2), 30);

    shutdown(client, server_handle).await;
}
