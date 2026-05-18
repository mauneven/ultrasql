//! End-to-end `... RETURNING ...` tests against a real `tokio-postgres`
//! client.
//!
//! The binder and logical plan already carried `RETURNING` metadata for
//! INSERT / UPDATE / DELETE; this file verifies the server/executor path
//! now lowers those plans, emits row descriptions, returns the correct
//! row images, and still tags the command as DML rather than `SELECT`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage};
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
        "host={host} port={port} user=tester application_name=returning_test",
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
async fn insert_returning_works_over_extended_query() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");

    let rows = client
        .query(
            "INSERT INTO t VALUES (1, 10), (2, 20) RETURNING id, v + 1",
            &[],
        )
        .await
        .expect("insert returning succeeds");
    let pairs: Vec<(i32, i32)> = rows
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect();
    assert_eq!(pairs, vec![(1, 11), (2, 21)]);

    let persisted = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("select persisted rows");
    let persisted_pairs: Vec<(i32, i32)> = persisted
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect();
    assert_eq!(persisted_pairs, vec![(1, 10), (2, 20)]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn update_returning_works_over_extended_query() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed");

    let rows = client
        .query("UPDATE t SET v = v + 5 WHERE id >= 2 RETURNING id, v", &[])
        .await
        .expect("update returning succeeds");
    let returned: Vec<(i32, i32)> = rows
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect();
    assert_eq!(returned, vec![(2, 25), (3, 35)]);

    let persisted = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("select persisted rows");
    let persisted_pairs: Vec<(i32, i32)> = persisted
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect();
    assert_eq!(persisted_pairs, vec![(1, 10), (2, 25), (3, 35)]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn delete_returning_works_over_extended_query() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed");

    let rows = client
        .query("DELETE FROM t WHERE id >= 2 RETURNING id, v", &[])
        .await
        .expect("delete returning succeeds");
    let returned: Vec<(i32, i32)> = rows
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect();
    assert_eq!(returned, vec![(2, 20), (3, 30)]);

    let persisted = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("select persisted rows");
    let persisted_pairs: Vec<(i32, i32)> = persisted
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect();
    assert_eq!(persisted_pairs, vec![(1, 10)]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn insert_returning_works_over_simple_query() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");

    let messages = client
        .simple_query("INSERT INTO t VALUES (7, 70) RETURNING id, v")
        .await
        .expect("simple query returning succeeds");

    let mut rows = Vec::new();
    let mut affected = None;
    for msg in messages {
        match msg {
            SimpleQueryMessage::Row(row) => rows.push((
                row.get(0)
                    .expect("id text")
                    .parse::<i32>()
                    .expect("id parses"),
                row.get(1)
                    .expect("v text")
                    .parse::<i32>()
                    .expect("v parses"),
            )),
            SimpleQueryMessage::CommandComplete(count) => affected = Some(count),
            _ => {}
        }
    }

    assert_eq!(rows, vec![(7, 70)]);
    assert_eq!(affected, Some(1));

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn update_and_delete_returning_work_over_simple_query() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed");
    let seeded = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("select seeded rows");
    let seeded_pairs: Vec<(i32, i32)> = seeded
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect();
    assert_eq!(seeded_pairs, vec![(1, 10), (2, 20), (3, 30)]);

    let update_messages = client
        .simple_query("UPDATE t SET v = v + 5 WHERE id >= 2 RETURNING id, v")
        .await
        .expect("simple update returning succeeds");
    let update_count = update_messages.iter().find_map(|msg| match msg {
        SimpleQueryMessage::CommandComplete(count) => Some(*count),
        _ => None,
    });
    let updated_rows: Vec<(i32, i32)> = update_messages
        .iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some((
                row.get(0)
                    .expect("id text")
                    .parse::<i32>()
                    .expect("id parses"),
                row.get(1)
                    .expect("v text")
                    .parse::<i32>()
                    .expect("v parses"),
            )),
            _ => None,
        })
        .collect();
    let persisted_after_update = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("select after update");
    let persisted_pairs: Vec<(i32, i32)> = persisted_after_update
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect();
    assert_eq!(persisted_pairs, vec![(1, 10), (2, 25), (3, 35)]);
    assert_eq!(update_count, Some(2));
    assert_eq!(updated_rows, vec![(2, 25), (3, 35)]);

    let delete_messages = client
        .simple_query("DELETE FROM t WHERE id >= 2 RETURNING id, v")
        .await
        .expect("simple delete returning succeeds");
    let delete_count = delete_messages.iter().find_map(|msg| match msg {
        SimpleQueryMessage::CommandComplete(count) => Some(*count),
        _ => None,
    });
    let deleted_rows: Vec<(i32, i32)> = delete_messages
        .iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some((
                row.get(0)
                    .expect("id text")
                    .parse::<i32>()
                    .expect("id parses"),
                row.get(1)
                    .expect("v text")
                    .parse::<i32>()
                    .expect("v parses"),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(delete_count, Some(2));
    assert_eq!(deleted_rows, vec![(2, 25), (3, 35)]);

    shutdown(client, server_handle).await;
}
