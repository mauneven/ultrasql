//! End-to-end integration test for `BETWEEN` / `NOT BETWEEN` over the
//! real wire path.
//!
//! Brings up an in-process `ultrasqld`, connects with `tokio-postgres`,
//! creates a small `(id INT, val INT)` table, and asserts that the rows
//! returned by `WHERE col BETWEEN low AND high` are identical to the
//! rows returned by the explicit `WHERE col >= low AND col <= high`
//! form — and symmetrically for `NOT BETWEEN` versus
//! `col < low OR col > high`.
//!
//! The test is gated behind the `sql-bench` feature because it depends
//! on the full server stack and the `tokio-postgres` driver, which the
//! bench crate already pulls in conditionally. The mainline workspace
//! test pass exercises the rewrite via the planner unit tests in
//! `crates/ultrasql-planner/src/binder.rs`; this integration test
//! confirms the lowered plan reaches the executor and produces the
//! same rows the explicit form does.

#![cfg(feature = "sql-bench")]

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use ultrasql_server::{Server, bind_listener, serve_listener};

/// Spawn an in-process `ultrasqld` on an ephemeral local port and
/// return the bound address. The server task is detached; the caller
/// is responsible for letting it run for the lifetime of the test
/// (the kernel will reclaim the port when the process exits).
async fn spawn_server() -> Result<SocketAddr> {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let (listener, bound) = bind_listener(bind_addr).await.context("bind listener")?;
    let state = Arc::new(Server::with_sample_database());
    tokio::spawn(async move {
        if let Err(e) = serve_listener(listener, state).await {
            eprintln!("ultrasqld task exited: {e}");
        }
    });
    Ok(bound)
}

/// Open a fresh wire connection to `addr`. The returned client owns
/// the read/write half; the connection driver runs on a detached
/// task whose lifetime ends when the test drops the client.
async fn connect(addr: SocketAddr) -> Result<Client> {
    let conn_str = format!(
        "host=127.0.0.1 port={} user=ultrasql_between_test",
        addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .context("tokio-postgres connect to ultrasqld")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tokio-postgres connection error: {e}");
        }
    });
    Ok(client)
}

/// Collect the `id` column from a `SELECT id FROM t WHERE …` Simple
/// Query response, sorted ascending for set-equality comparison.
async fn ids_for_query(client: &Client, sql: &str) -> Result<Vec<i32>> {
    let messages = client
        .simple_query(sql)
        .await
        .with_context(|| format!("simple_query: {sql}"))?;
    let mut ids: Vec<i32> = Vec::new();
    for msg in messages {
        if let SimpleQueryMessage::Row(row) = msg {
            let raw = row
                .try_get(0)
                .with_context(|| format!("row missing id column from `{sql}`"))?
                .ok_or_else(|| anyhow::anyhow!("null id from `{sql}`"))?;
            let id: i32 = raw
                .parse()
                .with_context(|| format!("id `{raw}` is not an i32"))?;
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn between_matches_ge_le_end_to_end() -> Result<()> {
    let addr = spawn_server().await?;
    let client = connect(addr).await?;

    // Build a table with ids 0..20 and a known val column we ignore.
    client
        .batch_execute("CREATE TABLE between_e2e_t (id INT NOT NULL, val INT)")
        .await
        .context("create test table")?;
    let mut insert = String::from("INSERT INTO between_e2e_t VALUES ");
    for i in 0..20 {
        if i > 0 {
            insert.push(',');
        }
        write!(insert, "({i},{})", i * 10).expect("writing to String never fails");
    }
    client
        .batch_execute(&insert)
        .await
        .context("preload rows")?;

    // Affirmative: BETWEEN should match `>= AND <=`.
    let between = ids_for_query(
        &client,
        "SELECT id FROM between_e2e_t WHERE id BETWEEN 5 AND 10",
    )
    .await?;
    let explicit = ids_for_query(
        &client,
        "SELECT id FROM between_e2e_t WHERE id >= 5 AND id <= 10",
    )
    .await?;
    assert_eq!(
        between, explicit,
        "BETWEEN rows must match explicit `>= AND <=` rows"
    );
    assert_eq!(between, vec![5, 6, 7, 8, 9, 10], "expected ids 5..=10");

    // Negated: NOT BETWEEN should match `< OR >`.
    let not_between = ids_for_query(
        &client,
        "SELECT id FROM between_e2e_t WHERE id NOT BETWEEN 5 AND 10",
    )
    .await?;
    let explicit_not = ids_for_query(
        &client,
        "SELECT id FROM between_e2e_t WHERE id < 5 OR id > 10",
    )
    .await?;
    assert_eq!(
        not_between, explicit_not,
        "NOT BETWEEN rows must match explicit `< OR >` rows"
    );
    // Ids outside [5,10]: 0..=4 ∪ 11..=19.
    let mut expected: Vec<i32> = (0..5).collect();
    expected.extend(11..20);
    assert_eq!(not_between, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn between_empty_range_returns_no_rows() -> Result<()> {
    // Asymmetric BETWEEN with low > high is defined to return no rows
    // (PostgreSQL: `x BETWEEN 10 AND 5` is always false). The rewrite
    // preserves this because `x >= 10 AND x <= 5` is unsatisfiable for
    // every finite x.
    let addr = spawn_server().await?;
    let client = connect(addr).await?;

    client
        .batch_execute("CREATE TABLE between_empty_t (id INT NOT NULL, val INT)")
        .await
        .context("create test table")?;
    let mut insert = String::from("INSERT INTO between_empty_t VALUES ");
    for i in 0..10 {
        if i > 0 {
            insert.push(',');
        }
        write!(insert, "({i},0)").expect("writing to String never fails");
    }
    client
        .batch_execute(&insert)
        .await
        .context("preload rows")?;

    let rows = ids_for_query(
        &client,
        "SELECT id FROM between_empty_t WHERE id BETWEEN 10 AND 5",
    )
    .await?;
    assert!(rows.is_empty(), "empty-range BETWEEN should return 0 rows");

    Ok(())
}
