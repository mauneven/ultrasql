//! End-to-end `INSERT ... ON CONFLICT ...` tests over PostgreSQL wire.

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
        "host={host} port={port} user=tester application_name=on_conflict_test",
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

#[tokio::test]
async fn insert_on_conflict_do_nothing_skips_duplicate_key() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("insert original");
    client
        .batch_execute("INSERT INTO t VALUES (1, 20) ON CONFLICT (id) DO NOTHING")
        .await
        .expect("duplicate skipped");

    let rows = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("select");
    let pairs: Vec<(i32, i32)> = rows
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect();
    assert_eq!(pairs, vec![(1, 10)]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn insert_on_conflict_do_update_rewrites_existing_row() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10)")
        .await
        .expect("insert original");

    let rows = client
        .query(
            "INSERT INTO t VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET v = 99 RETURNING id, v",
            &[],
        )
        .await
        .expect("upsert returning");
    let returned: Vec<(i32, i32)> = rows
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect();
    assert_eq!(returned, vec![(1, 99)]);

    let persisted = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("select");
    let pairs: Vec<(i32, i32)> = persisted
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect();
    assert_eq!(pairs, vec![(1, 99)]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn insert_on_conflict_do_update_uses_excluded_row() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL, touched INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10, 0)")
        .await
        .expect("insert original");

    let rows = client
        .query(
            "INSERT INTO t VALUES (1, 20, 1) \
             ON CONFLICT (id) DO UPDATE SET v = excluded.v, touched = touched + excluded.touched \
             WHERE excluded.v > v \
             RETURNING id, v, touched",
            &[],
        )
        .await
        .expect("upsert returning");
    let returned: Vec<(i32, i32, i32)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, i32>(0),
                row.get::<_, i32>(1),
                row.get::<_, i32>(2),
            )
        })
        .collect();
    assert_eq!(returned, vec![(1, 20, 1)]);

    client
        .batch_execute(
            "INSERT INTO t VALUES (1, 15, 1) \
             ON CONFLICT (id) DO UPDATE SET v = excluded.v, touched = touched + excluded.touched \
             WHERE excluded.v > v",
        )
        .await
        .expect("predicate false skips update");

    let persisted = client
        .query("SELECT id, v, touched FROM t ORDER BY id", &[])
        .await
        .expect("select");
    let rows: Vec<(i32, i32, i32)> = persisted
        .iter()
        .map(|row| {
            (
                row.get::<_, i32>(0),
                row.get::<_, i32>(1),
                row.get::<_, i32>(2),
            )
        })
        .collect();
    assert_eq!(rows, vec![(1, 20, 1)]);

    shutdown(client, server_handle).await;
}
