//! End-to-end HAVING tests for aggregate predicates that compare an
//! aggregate result against a grouped scalar value.

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
        "host={host} port={port} user=tester application_name=having_test",
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
async fn having_filters_against_cte_threshold() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute(
            "CREATE TABLE ps (part INT NOT NULL, cost DECIMAL(15, 2) NOT NULL, qty INT NOT NULL)",
        )
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO ps VALUES \
             (1, 100.00, 10), \
             (1,  50.00, 10), \
             (2,   5.00, 10), \
             (3, 10000000.00, 10000)",
        )
        .await
        .expect("insert rows");

    let rows = client
        .simple_query(
            "WITH gp AS (
                 SELECT part AS german_partkey, cost AS german_supplycost, qty AS german_availqty
                 FROM ps
             ),
             threshold AS (
                 SELECT SUM(german_supplycost * german_availqty) * 0.0001 AS min_value
                 FROM gp
             )
             SELECT german_partkey AS part, SUM(german_supplycost * german_availqty) AS value, min_value
             FROM gp, threshold
             GROUP BY german_partkey, min_value
             HAVING SUM(german_supplycost * german_availqty) > min_value
             ORDER BY german_partkey",
        )
        .await
        .expect("query succeeds");

    let got: Vec<(i32, String, String)> = rows
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get("part")?.parse::<i32>().ok()?,
                row.get("value")?.to_owned(),
                row.get("min_value")?.to_owned(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        got,
        vec![(
            3,
            "100000000000.00".to_owned(),
            "10000000.155000".to_owned()
        )]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn group_by_runtime_cast_error_returns_22p02() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE group_cast_items (id INT NOT NULL, raw TEXT NOT NULL);
             INSERT INTO group_cast_items VALUES (1, 'not-int')",
        )
        .await
        .expect("setup");

    let err = client
        .simple_query("SELECT COUNT(*) FROM group_cast_items GROUP BY CAST(raw AS INTEGER)")
        .await
        .expect_err("GROUP BY runtime cast rejects row");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22P02")
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn having_filters_against_scalar_subquery_threshold() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute(
            "CREATE TABLE ps (part INT NOT NULL, cost DECIMAL(15, 2) NOT NULL, qty INT NOT NULL)",
        )
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO ps VALUES \
             (1, 100.00, 10), \
             (1,  50.00, 10), \
             (2,   5.00, 10), \
             (3, 10000000.00, 10000)",
        )
        .await
        .expect("insert rows");

    let rows = client
        .simple_query(
            "SELECT part, SUM(cost * qty) AS value
             FROM ps
             GROUP BY part
             HAVING SUM(cost * qty) > (
                 SELECT SUM(cost * qty) * 0.0001
                 FROM ps
             )
             ORDER BY part",
        )
        .await
        .expect("query succeeds");

    let got: Vec<(i32, String)> = rows
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get("part")?.parse::<i32>().ok()?,
                row.get("value")?.to_owned(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(got, vec![(3, "100000000000.00".to_owned())]);

    shutdown(client, server_handle).await;
}
