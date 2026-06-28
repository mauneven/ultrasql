//! End-to-end tests for `INSERT INTO t DEFAULT VALUES`.
//!
//! `DEFAULT VALUES` binds to a zero-column `VALUES` whose single row carries
//! no cells. A zero-column batch cannot derive its row count from a column, so
//! without an explicit row marker the row vanished and nothing was inserted
//! (the server reported `INSERT 0 0`). PostgreSQL inserts exactly one row
//! whose every column takes its DEFAULT / `SERIAL` sequence / identity /
//! `GENERATED ... STORED` value — or NULL when the column has no default,
//! which then trips `NOT NULL` (`23502`). These tests pin that parity.

pub mod support;

use support::{shutdown, start_sample_server};

/// Read a single row's text columns via `simple_query`. Returns `None` for a
/// SQL NULL column, `Some(text)` otherwise.
async fn select_one_row(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    let messages = client.simple_query(sql).await.expect("simple_query");
    for m in messages {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
            return (0..row.len())
                .map(|i| row.get(i).map(str::to_owned))
                .collect();
        }
    }
    panic!("query returned no row: {sql}");
}

async fn scalar(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    select_one_row(client, sql)
        .await
        .into_iter()
        .next()
        .flatten()
}

#[tokio::test]
async fn default_values_inserts_one_all_defaults_row() {
    let running = start_sample_server("insert_default_values").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE dv_plain (a INT DEFAULT 1, b INT DEFAULT 2)")
        .await
        .expect("create table");

    // PostgreSQL: INSERT 0 1, row (1, 2).
    let n = client
        .execute("INSERT INTO dv_plain DEFAULT VALUES", &[])
        .await
        .expect("default values insert");
    assert_eq!(n, 1, "DEFAULT VALUES must insert exactly one row");

    assert_eq!(
        scalar(client, "SELECT count(*) FROM dv_plain").await,
        Some("1".to_owned())
    );
    let row = select_one_row(client, "SELECT a, b FROM dv_plain").await;
    assert_eq!(
        row,
        vec![Some("1".to_owned()), Some("2".to_owned())],
        "every column must take its DEFAULT (PG: 1, 2)"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn default_values_on_never_altered_table_inserts_row() {
    // The exact incidental repro: a table that was never ALTERed still gains
    // its one all-defaults row.
    let running = start_sample_server("insert_default_values").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE dv_repro (a INT DEFAULT 7, b TEXT DEFAULT 'hi')")
        .await
        .expect("create table");

    let n = client
        .execute("INSERT INTO dv_repro DEFAULT VALUES", &[])
        .await
        .expect("default values insert");
    assert_eq!(n, 1);

    let row = select_one_row(client, "SELECT a, b FROM dv_repro").await;
    assert_eq!(row, vec![Some("7".to_owned()), Some("hi".to_owned())]);

    shutdown(running).await;
}

#[tokio::test]
async fn default_values_no_defaults_inserts_all_nulls() {
    let running = start_sample_server("insert_default_values").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE dv_nulls (a INT, b TEXT)")
        .await
        .expect("create table");

    // PG: a column with no default and no NOT NULL defaults to NULL.
    let n = client
        .execute("INSERT INTO dv_nulls DEFAULT VALUES", &[])
        .await
        .expect("default values insert");
    assert_eq!(n, 1);

    let row = select_one_row(client, "SELECT a, b FROM dv_nulls").await;
    assert_eq!(row, vec![None, None], "columns with no default become NULL");

    shutdown(running).await;
}

#[tokio::test]
async fn default_values_not_null_no_default_raises_23502() {
    let running = start_sample_server("insert_default_values").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE dv_nn (a INT NOT NULL, b INT DEFAULT 5)")
        .await
        .expect("create table");

    let err = client
        .execute("INSERT INTO dv_nn DEFAULT VALUES", &[])
        .await
        .expect_err("NOT NULL column with no default must fail");
    let db_error = err.as_db_error().expect("server returns db error");
    assert_eq!(
        db_error.code().code(),
        "23502",
        "must raise not_null_violation: {db_error}"
    );

    // The failed insert must leave the table empty.
    assert_eq!(
        scalar(client, "SELECT count(*) FROM dv_nn").await,
        Some("0".to_owned())
    );

    shutdown(running).await;
}

#[tokio::test]
async fn default_values_advances_serial_sequence() {
    let running = start_sample_server("insert_default_values").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE dv_serial (id SERIAL, a INT DEFAULT 9)")
        .await
        .expect("create table");

    for _ in 0..2 {
        let n = client
            .execute("INSERT INTO dv_serial DEFAULT VALUES", &[])
            .await
            .expect("default values insert");
        assert_eq!(n, 1);
    }

    let rows = client
        .simple_query("SELECT id, a FROM dv_serial ORDER BY id")
        .await
        .expect("select");
    let ids: Vec<String> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        ids,
        vec!["1".to_owned(), "2".to_owned()],
        "each DEFAULT VALUES must consume the next SERIAL value"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn default_values_computes_generated_stored() {
    let running = start_sample_server("insert_default_values").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE dv_gen (a INT DEFAULT 4, g INT GENERATED ALWAYS AS (a * 2) STORED)",
        )
        .await
        .expect("create table");

    let n = client
        .execute("INSERT INTO dv_gen DEFAULT VALUES", &[])
        .await
        .expect("default values insert");
    assert_eq!(n, 1);

    let row = select_one_row(client, "SELECT a, g FROM dv_gen").await;
    assert_eq!(
        row,
        vec![Some("4".to_owned()), Some("8".to_owned())],
        "generated stored column must be computed from the defaulted base (PG: 4, 8)"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn default_values_returning_yields_defaulted_row() {
    let running = start_sample_server("insert_default_values").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE dv_ret (id SERIAL, a INT DEFAULT 11, b INT DEFAULT 22)")
        .await
        .expect("create table");

    let row = select_one_row(
        client,
        "INSERT INTO dv_ret DEFAULT VALUES RETURNING id, a, b",
    )
    .await;
    assert_eq!(
        row,
        vec![
            Some("1".to_owned()),
            Some("11".to_owned()),
            Some("22".to_owned())
        ],
        "RETURNING must project the fully defaulted row"
    );

    shutdown(running).await;
}
