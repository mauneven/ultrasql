//! End-to-end sequence DDL and function tests.

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
        "host={host} port={port} user=tester application_name=sequence_test",
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

async fn start_server_and_connect_two() -> (
    tokio_postgres::Client,
    tokio_postgres::Client,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, server));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=sequence_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (a, a_conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect a");
    tokio::spawn(async move {
        let _ = a_conn.await;
    });
    let (b, b_conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect b");
    tokio::spawn(async move {
        let _ = b_conn.await;
    });
    (a, b, server_handle)
}

async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

async fn simple_i64(client: &tokio_postgres::Client, sql: &str) -> i64 {
    let rows = client.simple_query(sql).await.expect("simple query");
    rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse::<i64>().ok(),
            _ => None,
        })
        .expect("one int8 row")
}

#[tokio::test]
async fn create_sequence_nextval_currval_setval_and_drop() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE SEQUENCE s START WITH 10 INCREMENT BY 5")
        .await
        .expect("create sequence");

    assert_eq!(simple_i64(&client, "SELECT nextval('s')").await, 10);
    assert_eq!(simple_i64(&client, "SELECT nextval('s')").await, 15);
    assert_eq!(simple_i64(&client, "SELECT currval('s')").await, 15);
    assert_eq!(simple_i64(&client, "SELECT lastval()").await, 15);
    assert_eq!(
        simple_i64(&client, "SELECT setval('s', 40, false)").await,
        40
    );
    assert_eq!(simple_i64(&client, "SELECT nextval('s')").await, 40);

    client
        .batch_execute("DROP SEQUENCE s")
        .await
        .expect("drop sequence");
    client
        .batch_execute("DROP SEQUENCE IF EXISTS s")
        .await
        .expect("drop sequence if exists");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn alter_sequence_changes_increment() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE SEQUENCE s START WITH 1")
        .await
        .expect("create sequence");
    assert_eq!(simple_i64(&client, "SELECT nextval('s')").await, 1);
    client
        .batch_execute("ALTER SEQUENCE s INCREMENT BY 10")
        .await
        .expect("alter sequence");
    assert_eq!(simple_i64(&client, "SELECT nextval('s')").await, 11);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn alter_sequence_start_and_restart_follow_postgres_shape() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE SEQUENCE s START WITH 1")
        .await
        .expect("create sequence");
    assert_eq!(simple_i64(&client, "SELECT nextval('s')").await, 1);
    client
        .batch_execute("ALTER SEQUENCE s START WITH 50")
        .await
        .expect("alter start");
    assert_eq!(
        simple_i64(&client, "SELECT nextval('s')").await,
        2,
        "START WITH changes restart seed, not current value"
    );
    client
        .batch_execute("ALTER SEQUENCE s RESTART")
        .await
        .expect("restart at configured start");
    assert_eq!(simple_i64(&client, "SELECT nextval('s')").await, 50);
    client
        .batch_execute("ALTER SEQUENCE s RESTART WITH 7")
        .await
        .expect("restart with explicit value");
    assert_eq!(simple_i64(&client, "SELECT nextval('s')").await, 7);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn currval_is_session_local_but_nextval_is_global() {
    let (a, b, server_handle) = start_server_and_connect_two().await;

    a.batch_execute("CREATE SEQUENCE s")
        .await
        .expect("create sequence");
    assert_eq!(simple_i64(&a, "SELECT nextval('s')").await, 1);

    let b_currval = b
        .simple_query("SELECT currval('s')")
        .await
        .expect_err("b currval before nextval fails");
    assert_eq!(b_currval.code().expect("SQLSTATE").code(), "55000");

    assert_eq!(simple_i64(&b, "SELECT nextval('s')").await, 2);

    drop(a);
    drop(b);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

#[tokio::test]
async fn descending_sequence_uses_maxvalue_default_start() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE SEQUENCE s INCREMENT BY -1 MAXVALUE 5")
        .await
        .expect("create descending sequence");
    assert_eq!(simple_i64(&client, "SELECT nextval('s')").await, 5);
    assert_eq!(simple_i64(&client, "SELECT nextval('s')").await, 4);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn serial_column_creates_sequence_default_and_updates_currval() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id SERIAL, v INT)")
        .await
        .expect("create table with serial");
    client
        .batch_execute("INSERT INTO t (v) VALUES (10), (20)")
        .await
        .expect("insert rows using serial default");

    let rows = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("select serial rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, i32>(1), 20);
    assert_eq!(simple_i64(&client, "SELECT currval('t_id_seq')").await, 2);

    client
        .batch_execute("DROP TABLE t")
        .await
        .expect("drop table");
    client
        .simple_query("SELECT nextval('t_id_seq')")
        .await
        .expect_err("owned serial sequence dropped with table");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn generated_always_identity_uses_sequence_and_rejects_explicit_values() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE t (\
             id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 5), \
             v INT)",
        )
        .await
        .expect("create identity table");
    client
        .batch_execute("INSERT INTO t (v) VALUES (10), (20)")
        .await
        .expect("insert rows using identity default");

    let rows = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("select identity rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i64>(0), 10);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i64>(0), 15);
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    let err = client
        .batch_execute("INSERT INTO t (id, v) VALUES (99, 30)")
        .await
        .expect_err("GENERATED ALWAYS rejects explicit identity value");
    assert_eq!(err.code().expect("SQLSTATE").code(), "428C9");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn generated_by_default_identity_allows_explicit_values() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY, v INT)")
        .await
        .expect("create identity table");
    client
        .batch_execute("INSERT INTO t (id, v) VALUES (42, 10)")
        .await
        .expect("explicit by-default identity value accepted");
    client
        .batch_execute("INSERT INTO t (v) VALUES (20)")
        .await
        .expect("omitted by-default identity value uses sequence");

    let rows = client
        .query("SELECT id, v FROM t ORDER BY v", &[])
        .await
        .expect("select identity rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 42);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    shutdown(client, server_handle).await;
}
