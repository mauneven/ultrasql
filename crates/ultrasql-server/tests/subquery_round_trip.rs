//! End-to-end subquery decorrelation checks.

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
        "host={host} port={port} user=tester application_name=subquery_test",
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
async fn correlated_exists_returns_each_outer_row_once() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE sq_orders (o_orderkey INT NOT NULL)")
        .await
        .expect("create orders");
    client
        .batch_execute(
            "CREATE TABLE sq_lineitem (
                 l_orderkey INT NOT NULL,
                 l_commit INT NOT NULL,
                 l_receipt INT NOT NULL
             )",
        )
        .await
        .expect("create lineitem");
    client
        .batch_execute("INSERT INTO sq_orders VALUES (1), (2), (3)")
        .await
        .expect("insert orders");
    client
        .batch_execute(
            "INSERT INTO sq_lineitem VALUES
                 (1, 1, 2),
                 (1, 1, 3),
                 (2, 3, 2)",
        )
        .await
        .expect("insert lineitems");

    let rows = client
        .simple_query(
            "SELECT o_orderkey
             FROM sq_orders
             WHERE EXISTS (
                 SELECT *
                 FROM sq_lineitem
                 WHERE l_orderkey = o_orderkey
                   AND l_commit < l_receipt
             )
             ORDER BY o_orderkey",
        )
        .await
        .expect("query succeeds");
    let keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(keys, vec![1]);

    let rows = client
        .simple_query(
            "SELECT o_orderkey
             FROM sq_orders
             WHERE NOT EXISTS (
                 SELECT *
                 FROM sq_lineitem
                 WHERE l_orderkey = o_orderkey
                   AND l_commit < l_receipt
             )
             ORDER BY o_orderkey",
        )
        .await
        .expect("NOT EXISTS query succeeds");
    let keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(keys, vec![2, 3]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn uncorrelated_in_and_scalar_subqueries_lower_before_execution() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute("CREATE TABLE sq_supplier (s_suppkey INT NOT NULL)")
        .await
        .expect("create supplier");
    client
        .batch_execute("CREATE TABLE sq_blocked (b_suppkey INT NOT NULL)")
        .await
        .expect("create blocked");
    client
        .batch_execute("INSERT INTO sq_supplier VALUES (1), (2), (3)")
        .await
        .expect("insert suppliers");
    client
        .batch_execute("INSERT INTO sq_blocked VALUES (2)")
        .await
        .expect("insert blocked");

    let rows = client
        .simple_query(
            "SELECT s_suppkey
             FROM sq_supplier
             WHERE s_suppkey NOT IN (SELECT b_suppkey FROM sq_blocked)
             ORDER BY s_suppkey",
        )
        .await
        .expect("NOT IN query succeeds");
    let not_in_keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(not_in_keys, vec![1, 3]);

    let rows = client
        .simple_query(
            "SELECT s_suppkey
             FROM sq_supplier
             WHERE s_suppkey > (SELECT b_suppkey FROM sq_blocked)
             ORDER BY s_suppkey",
        )
        .await
        .expect("scalar subquery succeeds");
    let scalar_keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(scalar_keys, vec![3]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn correlated_in_and_not_in_lower_before_execution() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute(
            "CREATE TABLE sq_outer_pair (
                 outer_id INT NOT NULL,
                 outer_group INT NOT NULL
             )",
        )
        .await
        .expect("create outer pair");
    client
        .batch_execute(
            "CREATE TABLE sq_inner_pair (
                 inner_id INT NOT NULL,
                 inner_group INT NOT NULL
             )",
        )
        .await
        .expect("create inner pair");
    client
        .batch_execute("INSERT INTO sq_outer_pair VALUES (1, 10), (2, 10), (3, 20), (4, 30)")
        .await
        .expect("insert outer rows");
    client
        .batch_execute("INSERT INTO sq_inner_pair VALUES (1, 10), (3, 20), (5, 10)")
        .await
        .expect("insert inner rows");

    let rows = client
        .simple_query(
            "SELECT outer_id
             FROM sq_outer_pair o
             WHERE outer_id IN (
                 SELECT inner_id
                 FROM sq_inner_pair i
                 WHERE i.inner_group = o.outer_group
             )
             ORDER BY outer_id",
        )
        .await
        .expect("correlated IN query succeeds");
    let in_keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(in_keys, vec![1, 3]);

    let rows = client
        .simple_query(
            "SELECT outer_id
             FROM sq_outer_pair o
             WHERE outer_id NOT IN (
                 SELECT inner_id
                 FROM sq_inner_pair i
                 WHERE i.inner_group = o.outer_group
             )
             ORDER BY outer_id",
        )
        .await
        .expect("correlated NOT IN query succeeds");
    let not_in_keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(not_in_keys, vec![2, 4]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn correlated_scalar_aggregate_subquery_lowers_before_execution() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute(
            "CREATE TABLE sq_part_price (
                 p_partkey INT NOT NULL,
                 p_limit INT NOT NULL
             )",
        )
        .await
        .expect("create part price");
    client
        .batch_execute(
            "CREATE TABLE sq_supply (
                 ps_partkey INT NOT NULL,
                 ps_cost INT NOT NULL
             )",
        )
        .await
        .expect("create supply");
    client
        .batch_execute("INSERT INTO sq_part_price VALUES (1, 5), (2, 7), (3, 9)")
        .await
        .expect("insert part price");
    client
        .batch_execute("INSERT INTO sq_supply VALUES (1, 5), (1, 8), (2, 6), (2, 10)")
        .await
        .expect("insert supply");

    let rows = client
        .simple_query(
            "SELECT p_partkey
             FROM sq_part_price
             WHERE p_limit = (
                 SELECT MIN(ps_cost)
                 FROM sq_supply
                 WHERE ps_partkey = p_partkey
             )
             ORDER BY p_partkey",
        )
        .await
        .expect("correlated scalar aggregate succeeds");
    let keys: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(keys, vec![1]);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn mixed_exists_not_exists_with_residual_correlation_lowers_before_execution() {
    let (client, _conn, server_handle) = start_server_and_connect().await;
    client
        .batch_execute(
            "CREATE TABLE sq_wait_lineitem (
                 l_orderkey INT NOT NULL,
                 l_suppkey INT NOT NULL,
                 l_receipt INT NOT NULL,
                 l_commit INT NOT NULL
             )",
        )
        .await
        .expect("create wait lineitem");
    client
        .batch_execute(
            "INSERT INTO sq_wait_lineitem VALUES
                 (1, 10, 5, 3),
                 (1, 20, 2, 3),
                 (2, 30, 6, 3),
                 (2, 40, 7, 3),
                 (3, 50, 9, 2)",
        )
        .await
        .expect("insert wait lineitem");

    let rows = client
        .simple_query(
            "SELECT l1.l_orderkey, l1.l_suppkey
             FROM sq_wait_lineitem l1
             WHERE l1.l_receipt > l1.l_commit
               AND EXISTS (
                   SELECT *
                   FROM sq_wait_lineitem l2
                   WHERE l2.l_orderkey = l1.l_orderkey
                     AND l2.l_suppkey <> l1.l_suppkey
               )
               AND NOT EXISTS (
                   SELECT *
                   FROM sq_wait_lineitem l3
                   WHERE l3.l_orderkey = l1.l_orderkey
                     AND l3.l_suppkey <> l1.l_suppkey
                     AND l3.l_receipt > l3.l_commit
               )
             ORDER BY l1.l_orderkey, l1.l_suppkey",
        )
        .await
        .expect("mixed EXISTS/NOT EXISTS query succeeds");
    let keys: Vec<(i32, i32)> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get(0)?.parse().ok()?, row.get(1)?.parse().ok()?))
            }
            _ => None,
        })
        .collect();
    assert_eq!(keys, vec![(1, 10)]);

    shutdown(client, server_handle).await;
}
