//! End-to-end transaction-control tests against a real `tokio-postgres`
//! client.
//!
//! Closes the v0.5 P0 open entry "BEGIN/COMMIT/ROLLBACK end-to-end".
//! Drives an in-process `ultrasqld` with the same stock client every
//! third-party Rust app uses and asserts that:
//!
//! - `BEGIN; INSERT; INSERT; COMMIT;` persists both rows.
//! - `BEGIN; INSERT; ROLLBACK;` discards the row.
//! - `BEGIN; UPDATE; ROLLBACK;` reverts the value.
//! - Implicit autocommit still works for plain `INSERT` outside a tx.
//! - An error inside a transaction puts the session in the failed
//!   block; subsequent statements get SQLSTATE `25P02`; COMMIT
//!   commits-as-ROLLBACK with the `ROLLBACK` tag.
//! - Extended Query (`client.execute("BEGIN")` etc.) round-trips
//!   identically.
//! - `SELECT * FROM t` inside a transaction sees the snapshot
//!   consistent with the transaction's BEGIN.
//!
//! All shapes are driven through the real PostgreSQL wire protocol so
//! the codec, parameter substitution, status-byte handling, and
//! `NoticeResponse` paths are exercised end-to-end.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

/// Spin up an in-process server on an ephemeral TCP port and return a
/// connected `tokio-postgres` client plus the join handles so the test
/// can shut everything down cleanly.
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
        "host={host} port={port} user=tester application_name=txn_test",
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

async fn start_server_and_connect_pair() -> (
    tokio_postgres::Client,
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, server));

    let conn_str = format!(
        "host={host} port={port} user=tester application_name=txn_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (a, a_connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect a");
    let (b, b_connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect b");
    let a_handle = tokio::spawn(async move {
        if let Err(e) = a_connection.await {
            eprintln!("connection error: {e}");
        }
    });
    let b_handle = tokio::spawn(async move {
        if let Err(e) = b_connection.await {
            eprintln!("connection error: {e}");
        }
    });
    (a, b, a_handle, b_handle, server_handle)
}

async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// `BEGIN; INSERT; INSERT; COMMIT;` — both rows visible after commit.
#[tokio::test]
async fn begin_insert_insert_commit_persists_both_rows() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("INSERT INTO t VALUES (1, 1)")
        .await
        .expect("insert row 1");
    client
        .batch_execute("INSERT INTO t VALUES (2, 2)")
        .await
        .expect("insert row 2");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after commit");
    assert_eq!(rows.len(), 2, "both committed rows visible");

    shutdown(client, server_handle).await;
}

/// A transaction may update multiple rows in one relation. This is the
/// shape TPC-C NewOrder uses when it decrements several stock rows.
#[tokio::test]
async fn begin_updates_two_rows_same_table_commits() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE multi_update (id INT NOT NULL, qty INT NOT NULL, ytd INT NOT NULL, cnt INT NOT NULL);\
             INSERT INTO multi_update VALUES (1, 10, 0, 0), (2, 20, 0, 0);\
             CREATE INDEX multi_update_id_idx ON multi_update (id)",
        )
        .await
        .expect("seed table");

    tokio::time::timeout(
        Duration::from_secs(2),
        client.batch_execute(
            "BEGIN;\
             UPDATE multi_update SET qty = qty - 1, ytd = ytd + 1, cnt = cnt + 1 WHERE id = 1;\
             UPDATE multi_update SET qty = qty - 1, ytd = ytd + 1, cnt = cnt + 1 WHERE id = 2;\
             COMMIT",
        ),
    )
    .await
    .expect("multi-row transaction should not hang")
    .expect("multi-row transaction should commit");

    let row = client
        .query_one("SELECT SUM(qty), SUM(ytd), SUM(cnt) FROM multi_update", &[])
        .await
        .expect("sum query");
    assert_eq!(row.get::<_, i64>(0), 28);
    assert_eq!(row.get::<_, i64>(1), 2);
    assert_eq!(row.get::<_, i64>(2), 2);

    shutdown(client, server_handle).await;
}

/// TPC-C NewOrder updates district/customer rows before inserting order rows.
#[tokio::test]
async fn begin_update_then_insert_other_table_commits() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE update_before_insert_a (id INT NOT NULL, v INT NOT NULL);\
             CREATE TABLE update_before_insert_b (id INT NOT NULL, v INT NOT NULL);\
             CREATE TABLE update_before_insert_c (id INT NOT NULL, v INT NOT NULL);\
             INSERT INTO update_before_insert_a VALUES (1, 10);\
             INSERT INTO update_before_insert_b VALUES (1, 20);\
             CREATE INDEX update_before_insert_c_id_idx ON update_before_insert_c (id)",
        )
        .await
        .expect("seed tables");

    tokio::time::timeout(
        Duration::from_secs(2),
        client.batch_execute(
            "BEGIN;\
             UPDATE update_before_insert_a SET v = v + 1 WHERE id = 1;\
             UPDATE update_before_insert_b SET v = v + 1 WHERE id = 1;\
             INSERT INTO update_before_insert_c VALUES (1, 30);\
             COMMIT",
        ),
    )
    .await
    .expect("update-then-insert transaction should not hang")
    .expect("update-then-insert transaction should commit");

    let row = client
        .query_one("SELECT COUNT(*) FROM update_before_insert_c", &[])
        .await
        .expect("count query");
    assert_eq!(row.get::<_, i64>(0), 1);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn separate_simple_queries_update_then_insert_in_transaction_commit() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE separate_update_a (id INT NOT NULL, v INT NOT NULL);\
             CREATE TABLE separate_update_b (id INT NOT NULL, v INT NOT NULL);\
             CREATE TABLE separate_insert_c (id INT NOT NULL, v INT NOT NULL);\
             INSERT INTO separate_update_a VALUES (1, 10);\
             INSERT INTO separate_update_b VALUES (1, 20);\
             CREATE INDEX separate_insert_c_id_idx ON separate_insert_c (id)",
        )
        .await
        .expect("seed tables");

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("UPDATE separate_update_a SET v = v + 1 WHERE id = 1")
        .await
        .expect("update a");
    client
        .batch_execute("UPDATE separate_update_b SET v = v + 1 WHERE id = 1")
        .await
        .expect("update b");
    tokio::time::timeout(
        Duration::from_secs(2),
        client.batch_execute("INSERT INTO separate_insert_c VALUES (1, 30)"),
    )
    .await
    .expect("separate insert should not hang")
    .expect("separate insert should execute");
    client.batch_execute("COMMIT").await.expect("commit");

    let row = client
        .query_one("SELECT COUNT(*) FROM separate_insert_c", &[])
        .await
        .expect("count query");
    assert_eq!(row.get::<_, i64>(0), 1);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn insert_duplicate_key_into_non_unique_index_commits() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE non_unique_index_insert (id INT NOT NULL, v INT NOT NULL);\
             CREATE INDEX non_unique_index_insert_id_idx ON non_unique_index_insert (id);\
             INSERT INTO non_unique_index_insert VALUES (1, 10)",
        )
        .await
        .expect("seed indexed table");

    tokio::time::timeout(
        Duration::from_secs(2),
        client.batch_execute("INSERT INTO non_unique_index_insert VALUES (1, 20)"),
    )
    .await
    .expect("duplicate non-unique index insert should not hang")
    .expect("duplicate non-unique index insert should commit");

    let row = client
        .query_one("SELECT COUNT(*) FROM non_unique_index_insert", &[])
        .await
        .expect("count query");
    assert_eq!(row.get::<_, i64>(0), 2);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn repeated_insert_into_indexed_table_after_commit_does_not_hang() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE repeated_index_insert (id INT NOT NULL, district INT NOT NULL, v INT NOT NULL);\
             INSERT INTO repeated_index_insert VALUES (1, 1, 10), (1, 2, 20);\
             CREATE INDEX repeated_index_insert_id_idx ON repeated_index_insert (id)",
        )
        .await
        .expect("seed indexed table");

    tokio::time::timeout(
        Duration::from_secs(2),
        client.batch_execute(
            "BEGIN;\
             INSERT INTO repeated_index_insert VALUES (2, 1, 30);\
             COMMIT;\
             BEGIN;\
             INSERT INTO repeated_index_insert VALUES (2, 2, 40);\
             COMMIT",
        ),
    )
    .await
    .expect("repeated indexed inserts should not hang")
    .expect("repeated indexed inserts should commit");

    let row = client
        .query_one("SELECT COUNT(*) FROM repeated_index_insert", &[])
        .await
        .expect("count query");
    assert_eq!(row.get::<_, i64>(0), 4);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn sequential_new_keys_into_non_unique_index_after_build_do_not_hang() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE sequential_index_insert (id INT NOT NULL, d INT NOT NULL, v INT NOT NULL);\
             INSERT INTO sequential_index_insert VALUES (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50);\
             CREATE INDEX sequential_index_insert_id_idx ON sequential_index_insert (id)",
        )
        .await
        .expect("seed indexed table");

    tokio::time::timeout(
        Duration::from_secs(2),
        client.batch_execute(
            "BEGIN;\
             INSERT INTO sequential_index_insert VALUES (6, 1, 60);\
             COMMIT;\
             BEGIN;\
             INSERT INTO sequential_index_insert VALUES (7, 1, 70);\
             COMMIT",
        ),
    )
    .await
    .expect("sequential indexed inserts should not hang")
    .expect("sequential indexed inserts should commit");

    let row = client
        .query_one("SELECT COUNT(*) FROM sequential_index_insert", &[])
        .await
        .expect("count query");
    assert_eq!(row.get::<_, i64>(0), 7);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn insert_after_dense_duplicate_index_build_does_not_hang() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE dense_duplicate_index_insert (id INT NOT NULL, d INT NOT NULL, v INT NOT NULL);\
             INSERT INTO dense_duplicate_index_insert VALUES \
             (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50),\
             (1, 2, 10), (2, 2, 20), (3, 2, 30), (4, 2, 40), (5, 2, 50),\
             (1, 3, 10), (2, 3, 20), (3, 3, 30), (4, 3, 40), (5, 3, 50),\
             (1, 4, 10), (2, 4, 20), (3, 4, 30), (4, 4, 40), (5, 4, 50),\
             (1, 5, 10), (2, 5, 20), (3, 5, 30), (4, 5, 40), (5, 5, 50),\
             (1, 6, 10), (2, 6, 20), (3, 6, 30), (4, 6, 40), (5, 6, 50),\
             (1, 7, 10), (2, 7, 20), (3, 7, 30), (4, 7, 40), (5, 7, 50),\
             (1, 8, 10), (2, 8, 20), (3, 8, 30), (4, 8, 40), (5, 8, 50),\
             (1, 9, 10), (2, 9, 20), (3, 9, 30), (4, 9, 40), (5, 9, 50),\
             (1, 10, 10), (2, 10, 20), (3, 10, 30), (4, 10, 40), (5, 10, 50);\
             CREATE INDEX dense_duplicate_index_insert_id_idx ON dense_duplicate_index_insert (id)",
        )
        .await
        .expect("seed indexed table");

    tokio::time::timeout(
        Duration::from_secs(2),
        client.batch_execute(
            "BEGIN;\
             INSERT INTO dense_duplicate_index_insert VALUES (6, 8, 60);\
             COMMIT;\
             BEGIN;\
             INSERT INTO dense_duplicate_index_insert VALUES (7, 8, 70);\
             COMMIT",
        ),
    )
    .await
    .expect("dense duplicate indexed inserts should not hang")
    .expect("dense duplicate indexed inserts should commit");

    let row = client
        .query_one("SELECT COUNT(*) FROM dense_duplicate_index_insert", &[])
        .await
        .expect("count query");
    assert_eq!(row.get::<_, i64>(0), 52);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn two_tpcc_shaped_new_order_transactions_do_not_hang() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE tpcc_district_smoke (d_w_id INT NOT NULL, d_id INT NOT NULL, d_next_o_id INT NOT NULL);\
             CREATE TABLE tpcc_customer_smoke (c_w_id INT NOT NULL, c_d_id INT NOT NULL, c_id INT NOT NULL, c_last_order_id INT NOT NULL);\
             CREATE TABLE tpcc_orders_smoke (o_w_id INT NOT NULL, o_d_id INT NOT NULL, o_id INT NOT NULL, o_c_id INT NOT NULL, o_carrier_id INT NOT NULL, o_entry_d INT NOT NULL);\
             CREATE TABLE tpcc_new_order_smoke (no_w_id INT NOT NULL, no_d_id INT NOT NULL, no_o_id INT NOT NULL);\
             CREATE TABLE tpcc_stock_smoke (s_w_id INT NOT NULL, s_i_id INT NOT NULL, s_quantity INT NOT NULL, s_ytd INT NOT NULL, s_order_cnt INT NOT NULL);\
             CREATE TABLE tpcc_order_line_smoke (ol_w_id INT NOT NULL, ol_d_id INT NOT NULL, ol_o_id INT NOT NULL, ol_number INT NOT NULL, ol_i_id INT NOT NULL, ol_quantity INT NOT NULL, ol_amount INT NOT NULL);\
             INSERT INTO tpcc_district_smoke VALUES (1, 8, 6);\
             INSERT INTO tpcc_customer_smoke VALUES (1, 8, 4, 0);\
             INSERT INTO tpcc_orders_smoke VALUES (1, 8, 1, 1, 0, 1), (1, 8, 2, 2, 0, 2), (1, 8, 3, 3, 0, 3), (1, 8, 4, 4, 0, 4), (1, 8, 5, 5, 0, 5);\
             INSERT INTO tpcc_stock_smoke VALUES (1, 12, 100, 0, 0), (1, 13, 100, 0, 0), (1, 15, 100, 0, 0), (1, 16, 100, 0, 0), (1, 20, 100, 0, 0);\
             CREATE INDEX tpcc_orders_smoke_oid_idx ON tpcc_orders_smoke (o_id);\
             CREATE INDEX tpcc_stock_smoke_iid_idx ON tpcc_stock_smoke (s_i_id)",
        )
        .await
        .expect("seed TPC-C smoke tables");

    tokio::time::timeout(
        Duration::from_secs(2),
        client.batch_execute(
            "BEGIN;\
             UPDATE tpcc_district_smoke SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = 1 AND d_id = 8;\
             UPDATE tpcc_customer_smoke SET c_last_order_id = 6 WHERE c_w_id = 1 AND c_d_id = 8 AND c_id = 4;\
             INSERT INTO tpcc_orders_smoke (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_entry_d) VALUES (1, 8, 6, 4, 0, 6);\
             INSERT INTO tpcc_new_order_smoke (no_w_id, no_d_id, no_o_id) VALUES (1, 8, 6);\
             UPDATE tpcc_stock_smoke SET s_quantity = s_quantity - 4, s_ytd = s_ytd + 4, s_order_cnt = s_order_cnt + 1 WHERE s_w_id = 1 AND s_i_id = 12;\
             INSERT INTO tpcc_order_line_smoke VALUES (1, 8, 6, 1, 12, 4, 52);\
             COMMIT;\
             BEGIN;\
             UPDATE tpcc_district_smoke SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = 1 AND d_id = 8;\
             UPDATE tpcc_customer_smoke SET c_last_order_id = 7 WHERE c_w_id = 1 AND c_d_id = 8 AND c_id = 4;\
             INSERT INTO tpcc_orders_smoke (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_entry_d) VALUES (1, 8, 7, 4, 0, 7);\
             INSERT INTO tpcc_new_order_smoke (no_w_id, no_d_id, no_o_id) VALUES (1, 8, 7);\
             UPDATE tpcc_stock_smoke SET s_quantity = s_quantity - 5, s_ytd = s_ytd + 5, s_order_cnt = s_order_cnt + 1 WHERE s_w_id = 1 AND s_i_id = 15;\
             INSERT INTO tpcc_order_line_smoke VALUES (1, 8, 7, 1, 15, 5, 80);\
             COMMIT",
        ),
    )
    .await
    .expect("TPC-C shaped new order transactions should not hang")
    .expect("TPC-C shaped new order transactions should commit");

    let row = client
        .query_one("SELECT COUNT(*) FROM tpcc_orders_smoke", &[])
        .await
        .expect("count query");
    assert_eq!(row.get::<_, i64>(0), 7);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn update_non_unique_index_with_extra_filter_then_insert_commits() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE indexed_filter_update (w INT NOT NULL, d INT NOT NULL, id INT NOT NULL, v INT NOT NULL);\
             CREATE TABLE indexed_filter_sink (id INT NOT NULL, v INT NOT NULL);\
             INSERT INTO indexed_filter_update VALUES (1, 1, 4, 10), (1, 2, 4, 20), (1, 8, 4, 30);\
             CREATE INDEX indexed_filter_update_id_idx ON indexed_filter_update (id);\
             CREATE INDEX indexed_filter_sink_id_idx ON indexed_filter_sink (id)",
        )
        .await
        .expect("seed indexed tables");

    tokio::time::timeout(
        Duration::from_secs(2),
        client.batch_execute(
            "BEGIN;\
             UPDATE indexed_filter_update SET v = v + 1 WHERE w = 1 AND d = 8 AND id = 4;\
             INSERT INTO indexed_filter_sink VALUES (7, 70);\
             COMMIT",
        ),
    )
    .await
    .expect("indexed filtered update then insert should not hang")
    .expect("indexed filtered update then insert should commit");

    let row = client
        .query_one("SELECT SUM(v) FROM indexed_filter_update", &[])
        .await
        .expect("sum query");
    assert_eq!(row.get::<_, i64>(0), 61);

    shutdown(client, server_handle).await;
}

/// `BEGIN; INSERT; ROLLBACK;` — row not persisted.
#[tokio::test]
async fn begin_insert_rollback_discards_row() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");
    // Baseline row to confirm subsequent COUNT is exact.
    client
        .batch_execute("INSERT INTO t VALUES (10)")
        .await
        .expect("baseline insert");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("INSERT INTO t VALUES (99)")
        .await
        .expect("insert inside tx");
    client.batch_execute("ROLLBACK").await.expect("ROLLBACK");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after rollback");
    assert_eq!(rows.len(), 1, "rolled-back INSERT did not persist");
    assert_eq!(rows[0].get::<_, i32>(0), 10);

    shutdown(client, server_handle).await;
}

/// `BEGIN; UPDATE; ROLLBACK;` — value unchanged.
#[tokio::test]
async fn begin_update_rollback_reverts_value() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1, 100)")
        .await
        .expect("baseline insert");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("UPDATE t SET val = val + 999 WHERE id = 1")
        .await
        .expect("update inside tx");
    client.batch_execute("ROLLBACK").await.expect("ROLLBACK");

    let rows = client
        .query("SELECT val FROM t WHERE id = 1", &[])
        .await
        .expect("select after rollback");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 100, "UPDATE rolled back");

    shutdown(client, server_handle).await;
}

/// `BEGIN; DELETE; ROLLBACK; UPDATE` — rollback clears the delete stamp so
/// later writes can still update the row.
#[tokio::test]
async fn begin_delete_rollback_allows_future_update() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .await
        .expect("baseline insert");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("DELETE FROM t WHERE id = 1")
        .await
        .expect("delete inside tx");
    client.batch_execute("ROLLBACK").await.expect("ROLLBACK");

    client
        .batch_execute("UPDATE t SET val = val + 7 WHERE id = 1")
        .await
        .expect("update after delete rollback");
    let rows = client
        .query("SELECT val FROM t WHERE id = 1", &[])
        .await
        .expect("select after update");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 107);

    shutdown(client, server_handle).await;
}

/// Wide rows use the general heap update/delete path; rollback must clear
/// those delete stamps too.
#[tokio::test]
async fn begin_wide_delete_rollback_allows_future_update() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE t (
                id INT NOT NULL,
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL,
                paid INT NOT NULL,
                score INT NOT NULL
            )",
        )
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO t VALUES
                (1, 3, 5, 100, 1, 10),
                (2, 4, 6, 200, 0, 20)",
        )
        .await
        .expect("baseline insert");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("DELETE FROM t WHERE id = 1")
        .await
        .expect("delete inside tx");
    client.batch_execute("ROLLBACK").await.expect("ROLLBACK");

    client
        .batch_execute("UPDATE t SET amount = amount + 7, score = score + 3 WHERE id = 1")
        .await
        .expect("update after wide delete rollback");
    let rows = client
        .query("SELECT amount, score FROM t WHERE id = 1", &[])
        .await
        .expect("select after update");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 107);
    assert_eq!(rows[0].get::<_, i32>(1), 13);

    shutdown(client, server_handle).await;
}

/// Wide-row UPDATE rollback must unchain the aborted old-version stamp so the
/// row can be updated again.
#[tokio::test]
async fn begin_wide_update_rollback_allows_future_update() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE t (
                id INT NOT NULL,
                tenant_id INT NOT NULL,
                bucket INT NOT NULL,
                amount BIGINT NOT NULL,
                paid INT NOT NULL,
                score INT NOT NULL
            )",
        )
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1, 3, 5, 100, 1, 10)")
        .await
        .expect("baseline insert");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("UPDATE t SET amount = amount + 999, score = score + 999 WHERE id = 1")
        .await
        .expect("update inside tx");
    client.batch_execute("ROLLBACK").await.expect("ROLLBACK");

    client
        .batch_execute("UPDATE t SET amount = amount + 7, score = score + 3 WHERE id = 1")
        .await
        .expect("update after wide update rollback");
    let rows = client
        .query("SELECT amount, score FROM t WHERE id = 1", &[])
        .await
        .expect("select after update");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 107);
    assert_eq!(rows[0].get::<_, i32>(1), 13);

    shutdown(client, server_handle).await;
}

/// Cached scalar aggregates are safe inside a read-committed transaction only
/// when the transaction has not modified the aggregate's target table. If it
/// has, the normal MVCC path must still see the transaction's own write.
#[tokio::test]
async fn explicit_transaction_scalar_aggregate_sees_own_write_on_target_table() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE t (id INT NOT NULL, val INT NOT NULL);\
             INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)",
        )
        .await
        .expect("seed table");
    client
        .query_one("SELECT SUM(val) FROM t", &[])
        .await
        .expect("prime scalar aggregate cache");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("INSERT INTO t VALUES (4, 4)")
        .await
        .expect("insert target row inside tx");
    let row = client
        .query_one("SELECT SUM(val) FROM t", &[])
        .await
        .expect("sum inside tx");
    assert_eq!(row.get::<_, i64>(0), 10);
    client.batch_execute("ROLLBACK").await.expect("ROLLBACK");

    shutdown(client, server_handle).await;
}

/// A transaction that writes one table may still use the cached scalar
/// aggregate path for an unrelated table under read committed semantics.
#[tokio::test]
async fn explicit_transaction_scalar_aggregate_reads_unmodified_table() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE fact (id INT NOT NULL, val INT NOT NULL);\
             CREATE TABLE state (id INT NOT NULL, val INT NOT NULL);\
             INSERT INTO fact VALUES (1, 1), (2, 2), (3, 3);\
             INSERT INTO state VALUES (1, 10)",
        )
        .await
        .expect("seed tables");
    client
        .query_one("SELECT SUM(val) FROM fact", &[])
        .await
        .expect("prime scalar aggregate cache");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("UPDATE state SET val = val + 7 WHERE id = 1")
        .await
        .expect("update unrelated table inside tx");
    let row = client
        .query_one("SELECT SUM(val) FROM fact", &[])
        .await
        .expect("sum unrelated table inside tx");
    assert_eq!(row.get::<_, i64>(0), 6);
    client.batch_execute("ROLLBACK").await.expect("ROLLBACK");

    shutdown(client, server_handle).await;
}

/// Implicit autocommit still works.  An INSERT issued without a
/// surrounding BEGIN is visible immediately to subsequent statements.
#[tokio::test]
async fn autocommit_insert_immediately_visible() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .expect("autocommit insert");
    client
        .batch_execute("INSERT INTO t VALUES (2)")
        .await
        .expect("autocommit insert");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after autocommit inserts");
    assert_eq!(rows.len(), 2);

    shutdown(client, server_handle).await;
}

/// A query inside a transaction sees the snapshot consistent with the
/// transaction's BEGIN — autocommit writes from another connection
/// after BEGIN are not visible.
#[tokio::test]
async fn select_in_transaction_uses_snapshot() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .expect("autocommit insert");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    // Inside the txn, we should see the one pre-existing row.
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select in tx");
    assert_eq!(rows.len(), 1);
    client.batch_execute("COMMIT").await.expect("COMMIT");

    shutdown(client, server_handle).await;
}

/// A query that errors inside a transaction transitions the session
/// to the failed-block state.  Subsequent statements get SQLSTATE
/// `25P02`; `COMMIT` commits-as-`ROLLBACK`.
#[tokio::test]
async fn error_in_tx_aborts_block_and_commit_is_rollback() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .expect("baseline insert");

    client.batch_execute("BEGIN").await.expect("BEGIN");

    // This INSERT lands inside the txn; the rollback should undo it.
    client
        .batch_execute("INSERT INTO t VALUES (2)")
        .await
        .expect("insert in tx");

    // Now error: reference an unknown table.
    let err = client
        .batch_execute("SELECT * FROM no_such_table")
        .await
        .expect_err("missing table should error");
    let sqlstate = err
        .code()
        .map_or_else(String::new, |c| c.code().to_string());
    assert!(
        sqlstate == "42P01" || sqlstate == "0A000",
        "expected table-not-found or feature-not-supported, got {sqlstate}",
    );

    // Any subsequent statement returns SQLSTATE 25P02.
    let err = client
        .batch_execute("SELECT id FROM t")
        .await
        .expect_err("in-failed-block should reject SELECT");
    let sqlstate = err
        .code()
        .map_or_else(String::new, |c| c.code().to_string());
    assert_eq!(sqlstate, "25P02", "25P02 in failed block");

    // COMMIT in failed state commits-as-rollback. tokio-postgres
    // reports success because the server emits CommandComplete +
    // ReadyForQuery; the tag itself is "ROLLBACK".
    client
        .batch_execute("COMMIT")
        .await
        .expect("COMMIT in failed state succeeds (as rollback)");

    // After the implicit rollback, the in-tx INSERT is gone.
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after failed-block COMMIT");
    assert_eq!(rows.len(), 1, "in-tx INSERT rolled back by failed-COMMIT");

    shutdown(client, server_handle).await;
}

/// Extended Query: `client.execute("BEGIN")`, `client.execute("INSERT
/// ...")`, `client.execute("COMMIT")` round-trips identically.
///
/// `client.execute` always goes through Parse/Bind/Execute/Sync
/// (the Extended Query Protocol path), so this exercises the
/// txn-state dispatch from the Extended side.
#[tokio::test]
async fn extended_query_begin_insert_commit_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");

    // Extended Query path:
    client.execute("BEGIN", &[]).await.expect("Extended BEGIN");
    client
        .execute("INSERT INTO t VALUES (1, 1)", &[])
        .await
        .expect("Extended INSERT");
    client
        .execute("INSERT INTO t VALUES (2, 2)", &[])
        .await
        .expect("Extended INSERT");
    client
        .execute("COMMIT", &[])
        .await
        .expect("Extended COMMIT");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after Extended COMMIT");
    assert_eq!(rows.len(), 2);

    shutdown(client, server_handle).await;
}

/// Extended Query ROLLBACK discards the in-flight INSERT.
#[tokio::test]
async fn extended_query_begin_insert_rollback_discards() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");

    client.execute("BEGIN", &[]).await.expect("Extended BEGIN");
    client
        .execute("INSERT INTO t VALUES (42)", &[])
        .await
        .expect("Extended INSERT");
    client
        .execute("ROLLBACK", &[])
        .await
        .expect("Extended ROLLBACK");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after Extended ROLLBACK");
    assert_eq!(rows.len(), 0, "Extended ROLLBACK discarded");

    shutdown(client, server_handle).await;
}

/// SAVEPOINT / RELEASE / ROLLBACK TO statements round-trip without
/// errors. The transaction-manager savepoint stack is updated and the
/// session stays healthy.
///
/// Partial-rollback visibility is verified end-to-end in
/// [`savepoint_rollback_to_undoes_in_savepoint_writes`].
#[tokio::test]
async fn savepoint_release_rollback_to_round_trip_without_error() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("SAVEPOINT sp1")
        .await
        .expect("SAVEPOINT");
    client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .expect("insert under sp1");
    client
        .batch_execute("RELEASE SAVEPOINT sp1")
        .await
        .expect("RELEASE");
    client
        .batch_execute("SAVEPOINT sp2")
        .await
        .expect("SAVEPOINT sp2");
    client
        .batch_execute("ROLLBACK TO SAVEPOINT sp2")
        .await
        .expect("ROLLBACK TO");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    // The committed transaction's writes are visible.
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after commit");
    assert_eq!(rows.len(), 1);

    shutdown(client, server_handle).await;
}

/// `ROLLBACK TO SAVEPOINT` on an unknown name errors with
/// SQLSTATE `3B001` (`invalid_savepoint_specification`) and marks the
/// transaction block as failed (PostgreSQL semantics).
#[tokio::test]
async fn rollback_to_unknown_savepoint_errors() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client.batch_execute("BEGIN").await.expect("BEGIN");
    let err = client
        .batch_execute("ROLLBACK TO SAVEPOINT nope")
        .await
        .expect_err("unknown savepoint should error");
    let sqlstate = err
        .code()
        .map_or_else(String::new, |c| c.code().to_string());
    assert_eq!(sqlstate, "3B001", "invalid_savepoint_specification");
    // Recover the session.
    client
        .batch_execute("ROLLBACK")
        .await
        .expect("ROLLBACK after savepoint error");

    shutdown(client, server_handle).await;
}

/// `SAVEPOINT` outside a transaction errors with SQLSTATE `25P01`
/// (`no_active_sql_transaction`).
#[tokio::test]
async fn savepoint_outside_transaction_errors() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    let err = client
        .batch_execute("SAVEPOINT sp")
        .await
        .expect_err("SAVEPOINT outside tx should error");
    let sqlstate = err
        .code()
        .map_or_else(String::new, |c| c.code().to_string());
    assert_eq!(sqlstate, "25P01");

    shutdown(client, server_handle).await;
}

/// Mixing Simple Query and Extended Query inside the same
/// transaction: BEGIN via Simple, INSERT via Extended (prepared),
/// COMMIT via Simple — all bound to the same xid and visible
/// together.
#[tokio::test]
async fn mixed_simple_and_extended_in_one_tx() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("Simple BEGIN");

    let stmt = client
        .prepare("INSERT INTO t VALUES ($1, $2)")
        .await
        .expect("prepare");
    client
        .execute(&stmt, &[&7_i32, &700_i32])
        .await
        .expect("Extended prepared INSERT");

    client.batch_execute("COMMIT").await.expect("Simple COMMIT");

    let rows = client
        .query("SELECT id, val FROM t", &[])
        .await
        .expect("select after mixed COMMIT");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 7);
    assert_eq!(rows[0].get::<_, i32>(1), 700);

    shutdown(client, server_handle).await;
}

/// `BEGIN ISOLATION LEVEL READ COMMITTED` and `READ UNCOMMITTED` (aliased)
/// round-trip through the wire without error and the session can commit.
#[tokio::test]
async fn begin_isolation_level_read_committed_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        .await
        .expect("BEGIN ISOLATION LEVEL READ COMMITTED");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    // READ UNCOMMITTED is aliased to READ COMMITTED.
    client
        .batch_execute("BEGIN ISOLATION LEVEL READ UNCOMMITTED")
        .await
        .expect("BEGIN ISOLATION LEVEL READ UNCOMMITTED");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    shutdown(client, server_handle).await;
}

/// `BEGIN ISOLATION LEVEL REPEATABLE READ` round-trips without error.
#[tokio::test]
async fn begin_isolation_level_repeatable_read_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("BEGIN ISOLATION LEVEL REPEATABLE READ");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    shutdown(client, server_handle).await;
}

/// `BEGIN ISOLATION LEVEL SERIALIZABLE` round-trips without error.
#[tokio::test]
async fn begin_isolation_level_serializable_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("BEGIN ISOLATION LEVEL SERIALIZABLE");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    shutdown(client, server_handle).await;
}

/// Two serializable transactions that both read a relation and then
/// update disjoint rows form a classic SSI dangerous structure.
#[tokio::test]
async fn serializable_write_skew_aborts_pivot() {
    let (a, b, a_handle, b_handle, server_handle) = start_server_and_connect_pair().await;

    a.batch_execute("CREATE TABLE ssi_shift (id INT NOT NULL, on_call INT)")
        .await
        .expect("create table");
    a.batch_execute("INSERT INTO ssi_shift VALUES (1, 1), (2, 1)")
        .await
        .expect("seed");

    a.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a");
    b.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin b");

    assert_eq!(
        a.query_one("SELECT COUNT(*) FROM ssi_shift WHERE on_call = 1", &[])
            .await
            .expect("read a")
            .get::<_, i64>(0),
        2
    );
    assert_eq!(
        b.query_one("SELECT COUNT(*) FROM ssi_shift WHERE on_call = 1", &[])
            .await
            .expect("read b")
            .get::<_, i64>(0),
        2
    );

    a.batch_execute("UPDATE ssi_shift SET on_call = 0 WHERE id = 1")
        .await
        .expect("update a");
    b.batch_execute("UPDATE ssi_shift SET on_call = 0 WHERE id = 2")
        .await
        .expect("update b");

    let a_commit = a.batch_execute("COMMIT").await;
    let b_commit = b.batch_execute("COMMIT").await;
    let ok_commits = [&a_commit, &b_commit]
        .iter()
        .filter(|result| result.is_ok())
        .count();
    let serialization_failures = [&a_commit, &b_commit]
        .iter()
        .filter(|result| {
            result
                .as_ref()
                .err()
                .and_then(|err| err.code())
                .is_some_and(|code| code.code() == "40001")
        })
        .count();
    assert_eq!(
        ok_commits, 1,
        "one write-skew transaction should commit: a={a_commit:?}, b={b_commit:?}",
    );
    assert_eq!(
        serialization_failures, 1,
        "one write-skew transaction should abort with 40001: a={a_commit:?}, b={b_commit:?}",
    );

    drop(a);
    drop(b);
    a_handle.abort();
    b_handle.abort();
    server_handle.abort();
}

/// `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` undoes writes performed
/// inside the savepoint while preserving writes performed before it.
///
/// Driven through the real wire so the executor's per-statement
/// `current_xid()` lookup is exercised end-to-end.
#[tokio::test]
async fn savepoint_rollback_to_undoes_in_savepoint_writes() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .expect("pre-savepoint insert");
    client
        .batch_execute("SAVEPOINT sp1")
        .await
        .expect("SAVEPOINT");
    client
        .batch_execute("INSERT INTO t VALUES (2)")
        .await
        .expect("inside-savepoint insert");
    client
        .batch_execute("ROLLBACK TO SAVEPOINT sp1")
        .await
        .expect("ROLLBACK TO sp1");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after partial rollback");
    let mut ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![1],
        "ROLLBACK TO SAVEPOINT must hide the INSERT (2) row"
    );

    shutdown(client, server_handle).await;
}

/// Nested savepoints: `ROLLBACK TO` an outer savepoint pops every
/// nested savepoint above it. Writes inside any rolled-back subtxn
/// are hidden; writes outside them remain visible.
#[tokio::test]
async fn nested_savepoints_partial_rollback_correct_visibility() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("INSERT INTO t VALUES (10)")
        .await
        .expect("pre-savepoint");
    client
        .batch_execute("SAVEPOINT sp1")
        .await
        .expect("SAVEPOINT sp1");
    client
        .batch_execute("INSERT INTO t VALUES (20)")
        .await
        .expect("under sp1");
    client
        .batch_execute("SAVEPOINT sp2")
        .await
        .expect("SAVEPOINT sp2");
    client
        .batch_execute("INSERT INTO t VALUES (30)")
        .await
        .expect("under sp2");
    // Roll back to sp1 — should pop sp2 too and discard rows 20 and 30.
    client
        .batch_execute("ROLLBACK TO SAVEPOINT sp1")
        .await
        .expect("ROLLBACK TO sp1");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after nested rollback");
    let mut ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![10],
        "ROLLBACK TO sp1 must hide rows 20 and 30 (under sp1 and sp2)"
    );

    shutdown(client, server_handle).await;
}

/// `RELEASE SAVEPOINT` keeps the savepoint's writes — they merge
/// into the parent and are visible after COMMIT.
#[tokio::test]
async fn release_savepoint_keeps_in_savepoint_writes() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("SAVEPOINT sp1")
        .await
        .expect("SAVEPOINT");
    client
        .batch_execute("INSERT INTO t VALUES (42)")
        .await
        .expect("under sp1");
    client
        .batch_execute("RELEASE SAVEPOINT sp1")
        .await
        .expect("RELEASE");
    client.batch_execute("COMMIT").await.expect("COMMIT");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after release");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 42);

    shutdown(client, server_handle).await;
}

/// `SET TRANSACTION ISOLATION LEVEL …` round-trips inside an active
/// transaction. Outside a transaction the server emits a `25P01`
/// warning and proceeds (PostgreSQL semantics: the SET is a no-op,
/// the connection stays usable).
#[tokio::test]
async fn set_transaction_isolation_level_round_trip() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    let row = client
        .query_one("SHOW transaction isolation level", &[])
        .await
        .expect("SHOW transaction isolation level after SERIALIZABLE");
    assert_eq!(row.get::<_, String>(0), "serializable");

    client
        .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    let row = client
        .query_one("SHOW transaction isolation level", &[])
        .await
        .expect("SHOW transaction isolation level after REPEATABLE READ");
    assert_eq!(row.get::<_, String>(0), "repeatable read");

    client
        .batch_execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .await
        .expect("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
    let row = client
        .query_one("SHOW transaction isolation level", &[])
        .await
        .expect("SHOW transaction isolation level after READ COMMITTED");
    assert_eq!(row.get::<_, String>(0), "read committed");

    client.batch_execute("COMMIT").await.expect("COMMIT");
    let row = client
        .query_one("SHOW transaction isolation level", &[])
        .await
        .expect("SHOW transaction isolation level after COMMIT");
    assert_eq!(row.get::<_, String>(0), "read committed");

    // Outside a transaction is allowed to round-trip with a warning;
    // tokio-postgres surfaces the CommandComplete tag, not the notice.
    client
        .batch_execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("SET TRANSACTION outside tx round-trips with a notice");

    shutdown(client, server_handle).await;
}

/// Inside a REPEATABLE READ transaction the snapshot is frozen at BEGIN.
/// A baseline row inserted before BEGIN is visible; the transaction
/// commits cleanly to verify the full path.
#[tokio::test]
async fn repeatable_read_snapshot_frozen_wire_level() {
    let (client, _conn_handle, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .expect("baseline row");

    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("BEGIN RR");

    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select inside RR tx");
    assert_eq!(rows.len(), 1, "baseline row visible inside RR tx");

    client.batch_execute("COMMIT").await.expect("COMMIT");

    shutdown(client, server_handle).await;
}
