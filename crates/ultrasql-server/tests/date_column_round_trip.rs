//! `DATE` column round-trip test — v0.6 TPC-H milestone surface.
//!
//! Validates the end-to-end path that lands in this turn:
//!
//! - Parser: `DATE 'YYYY-MM-DD'` typed-string literal
//! - Binder: `Literal::Typed { type_name: "date", .. }` → `Value::Date`
//!   via the Howard-Hinnant `civil_from_days` algorithm
//! - DDL: `CREATE TABLE t (d DATE)` accepted (was rejected pre-v0.6)
//! - Row codec: `DataType::Date` encodes as 4-byte little-endian i32
//!   (`days_since_2000_01_01`), decodes back into the `Int32` builder
//! - Visibility / scan: `DATE` column round-trips through `SeqScan`
//!
//! Pinning these here means a regression on any link in the chain
//! trips an integration test rather than the TPC-H runner.

pub mod support;

use support::{shutdown, start_sample_server};

#[tokio::test]
async fn create_table_with_date_column() {
    let running = start_sample_server("date_round_trip").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE events (id INT NOT NULL, d DATE NOT NULL)")
        .await
        .expect("CREATE TABLE with DATE column");
    shutdown(running).await;
}

#[tokio::test]
async fn insert_date_literal_and_scan() {
    let running = start_sample_server("date_round_trip").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE events (id INT NOT NULL, d DATE NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO events VALUES \
             (1, DATE '2000-01-01'), \
             (2, DATE '2024-12-31'), \
             (3, DATE '1994-01-01')",
        )
        .await
        .expect("insert with DATE literals");
    let messages = client
        .simple_query("SELECT id FROM events")
        .await
        .expect("scan");
    let rows: Vec<_> = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(rows.len(), 3, "all three rows survive the round-trip");
    shutdown(running).await;
}

#[tokio::test]
async fn filters_date_column_with_interval_bound() {
    let running = start_sample_server("date_round_trip").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE events (id INT NOT NULL, d DATE NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO events VALUES \
             (1, DATE '2000-01-01'), \
             (2, DATE '2024-12-31'), \
             (3, DATE '1994-01-01')",
        )
        .await
        .expect("insert with DATE literals");

    let messages = client
        .simple_query(
            "SELECT id FROM events \
             WHERE d < DATE '1994-01-01' + INTERVAL '1' YEAR \
             ORDER BY id",
        )
        .await
        .expect("date + interval filter query");
    let ids: Vec<i32> = messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.get("id")
                    .expect("id column present")
                    .parse::<i32>()
                    .expect("id parses as i32"),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![3], "only 1994-01-01 is before 1995-01-01");
    shutdown(running).await;
}

#[tokio::test]
async fn accepts_decimal_column() {
    // DECIMAL columns are wired through the v0.6 milestone landing:
    // scaled i64 codec, Decimal column-builder arm, batch_to_rows
    // re-tagging the value with the schema-side scale.
    let running = start_sample_server("date_round_trip").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE prices (id INT NOT NULL, p DECIMAL(15, 2) NOT NULL)")
        .await
        .expect("CREATE TABLE with DECIMAL column");
    shutdown(running).await;
}

#[tokio::test]
async fn filters_decimal_column_with_decimal_literal() {
    let running = start_sample_server("date_round_trip").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE prices (id INT NOT NULL, p DECIMAL(15, 2) NOT NULL)")
        .await
        .expect("create decimal table");
    client
        .batch_execute(
            "INSERT INTO prices VALUES \
             (1, 0.06), \
             (2, 0.07), \
             (3, 0.08)",
        )
        .await
        .expect("insert decimal rows");

    let messages = client
        .simple_query("SELECT id FROM prices WHERE p = 0.06 ORDER BY id")
        .await
        .expect("decimal predicate query");
    let ids: Vec<i32> = messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.get("id")
                    .expect("id column present")
                    .parse::<i32>()
                    .expect("id parses as i32"),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![1], "only the 0.06 row should match");
    shutdown(running).await;
}

#[tokio::test]
async fn accepts_timestamp_column() {
    // TIMESTAMP / TIMESTAMPTZ / TIME columns wired through the same
    // codec template as Decimal: 8-byte little-endian i64 microsecond
    // payload, Int64 column builder, schema-side semantic tag.
    let running = start_sample_server("date_round_trip").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE evt (id INT NOT NULL, ts TIMESTAMP NOT NULL, t TIME NOT NULL)")
        .await
        .expect("CREATE TABLE with TIMESTAMP/TIME columns");
    shutdown(running).await;
}

#[tokio::test]
async fn scans_decimal_column_in_wide_tpch_like_table() {
    let running = start_sample_server("date_round_trip").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE orders_probe (\
                o_orderkey INT NOT NULL, \
                o_custkey INT NOT NULL, \
                o_orderstatus CHAR(1) NOT NULL, \
                o_totalprice DECIMAL(15, 2) NOT NULL, \
                o_orderdate DATE NOT NULL, \
                o_orderpriority CHAR(15) NOT NULL, \
                o_clerk CHAR(15) NOT NULL, \
                o_shippriority INT NOT NULL, \
                o_comment VARCHAR(79) NOT NULL\
            )",
        )
        .await
        .expect("create wide probe table");
    client
        .batch_execute(
            "INSERT INTO orders_probe VALUES \
             (1, 1, 'O', 173665.47, DATE '1995-03-14', '5-LOW', 'Clerk#000000001', 0, 'note')",
        )
        .await
        .expect("insert probe row");

    let messages = client
        .simple_query("SELECT o_totalprice FROM orders_probe")
        .await
        .expect("scan wide decimal table");
    let values: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.get(0)
                    .expect("first selected column present")
                    .to_owned(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec!["173665.47".to_owned()]);
    shutdown(running).await;
}

#[tokio::test]
async fn filters_wide_tpch_like_table_without_decimal_type_mismatch() {
    let running = start_sample_server("date_round_trip").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE orders_probe (\
                o_orderkey INT NOT NULL, \
                o_custkey INT NOT NULL, \
                o_orderstatus CHAR(1) NOT NULL, \
                o_totalprice DECIMAL(15, 2) NOT NULL, \
                o_orderdate DATE NOT NULL, \
                o_orderpriority CHAR(15) NOT NULL, \
                o_clerk CHAR(15) NOT NULL, \
                o_shippriority INT NOT NULL, \
                o_comment VARCHAR(79) NOT NULL\
            )",
        )
        .await
        .expect("create wide probe table");
    client
        .batch_execute(
            "INSERT INTO orders_probe VALUES \
             (1, 1, 'O', 173665.47, DATE '1995-03-14', '5-LOW', 'Clerk#000000001', 0, 'note')",
        )
        .await
        .expect("insert probe row");

    let messages = client
        .simple_query(
            "SELECT o_orderkey FROM orders_probe \
             WHERE o_orderdate = DATE '1995-03-14'",
        )
        .await
        .expect("filter over wide table");
    let values: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.get(0)
                    .expect("first selected column present")
                    .to_owned(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec!["1".to_owned()]);
    shutdown(running).await;
}

#[tokio::test]
async fn joins_wide_tpch_like_tables_without_hidden_decimal_mismatch() {
    let running = start_sample_server("date_round_trip").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE customer_probe (\
                c_custkey INT NOT NULL, \
                c_name VARCHAR(25) NOT NULL, \
                c_address VARCHAR(40) NOT NULL, \
                c_nationkey INT NOT NULL, \
                c_phone CHAR(15) NOT NULL, \
                c_acctbal DECIMAL(15, 2) NOT NULL, \
                c_mktsegment CHAR(10) NOT NULL, \
                c_comment VARCHAR(117) NOT NULL\
            )",
        )
        .await
        .expect("create customer probe table");
    client
        .batch_execute(
            "CREATE TABLE orders_probe (\
                o_orderkey INT NOT NULL, \
                o_custkey INT NOT NULL, \
                o_orderstatus CHAR(1) NOT NULL, \
                o_totalprice DECIMAL(15, 2) NOT NULL, \
                o_orderdate DATE NOT NULL, \
                o_orderpriority CHAR(15) NOT NULL, \
                o_clerk CHAR(15) NOT NULL, \
                o_shippriority INT NOT NULL, \
                o_comment VARCHAR(79) NOT NULL\
            )",
        )
        .await
        .expect("create orders probe table");
    client
        .batch_execute(
            "INSERT INTO customer_probe VALUES \
             (1, 'Customer#000000001', 'Addr', 17, '25-989-741-2988', 711.56, 'BUILDING', 'note')",
        )
        .await
        .expect("insert customer probe row");
    client
        .batch_execute(
            "INSERT INTO orders_probe VALUES \
             (1, 1, 'O', 173665.47, DATE '1995-03-14', '5-LOW', 'Clerk#000000001', 0, 'note')",
        )
        .await
        .expect("insert orders probe row");

    let messages = client
        .simple_query(
            "SELECT o_orderkey \
             FROM customer_probe c \
             JOIN orders_probe o ON c.c_custkey = o.o_custkey",
        )
        .await
        .expect("join wide tables");
    let values: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                row.get(0)
                    .expect("first selected column present")
                    .to_owned(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec!["1".to_owned()]);
    shutdown(running).await;
}

#[tokio::test]
async fn aggregates_over_joined_wide_tables_without_hidden_decimal_mismatch() {
    let running = start_sample_server("date_round_trip").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE customer_probe (\
                c_custkey INT NOT NULL, \
                c_name VARCHAR(25) NOT NULL, \
                c_address VARCHAR(40) NOT NULL, \
                c_nationkey INT NOT NULL, \
                c_phone CHAR(15) NOT NULL, \
                c_acctbal DECIMAL(15, 2) NOT NULL, \
                c_mktsegment CHAR(10) NOT NULL, \
                c_comment VARCHAR(117) NOT NULL\
            )",
        )
        .await
        .expect("create customer probe table");
    client
        .batch_execute(
            "CREATE TABLE orders_probe (\
                o_orderkey INT NOT NULL, \
                o_custkey INT NOT NULL, \
                o_orderstatus CHAR(1) NOT NULL, \
                o_totalprice DECIMAL(15, 2) NOT NULL, \
                o_orderdate DATE NOT NULL, \
                o_orderpriority CHAR(15) NOT NULL, \
                o_clerk CHAR(15) NOT NULL, \
                o_shippriority INT NOT NULL, \
                o_comment VARCHAR(79) NOT NULL\
            )",
        )
        .await
        .expect("create orders probe table");
    client
        .batch_execute(
            "INSERT INTO customer_probe VALUES \
             (1, 'Customer#000000001', 'Addr', 17, '25-989-741-2988', 711.56, 'BUILDING', 'note')",
        )
        .await
        .expect("insert customer probe row");
    client
        .batch_execute(
            "INSERT INTO orders_probe VALUES \
             (1, 1, 'O', 173665.47, DATE '1995-03-14', '5-LOW', 'Clerk#000000001', 0, 'note')",
        )
        .await
        .expect("insert orders probe row");

    let messages = client
        .simple_query(
            "SELECT c_name, COUNT(*) \
             FROM customer_probe c \
             JOIN orders_probe o ON c.c_custkey = o.o_custkey \
             GROUP BY c_name",
        )
        .await
        .expect("aggregate over joined wide tables");
    let values: Vec<(String, String)> = messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                row.get(0).expect("name column present").to_owned(),
                row.get(1).expect("count column present").to_owned(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        values,
        vec![("Customer#000000001".to_owned(), "1".to_owned())]
    );
    shutdown(running).await;
}

#[tokio::test]
async fn aggregates_empty_filtered_wide_table_without_decimal_mismatch() {
    let running = start_sample_server("date_round_trip").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE orders_probe (\
                o_orderkey INT NOT NULL, \
                o_custkey INT NOT NULL, \
                o_orderstatus CHAR(1) NOT NULL, \
                o_totalprice DECIMAL(15, 2) NOT NULL, \
                o_orderdate DATE NOT NULL, \
                o_orderpriority CHAR(15) NOT NULL, \
                o_clerk CHAR(15) NOT NULL, \
                o_shippriority INT NOT NULL, \
                o_comment VARCHAR(79) NOT NULL\
            )",
        )
        .await
        .expect("create orders probe table");
    client
        .batch_execute(
            "INSERT INTO orders_probe VALUES \
             (1, 1, 'O', 173665.47, DATE '1995-03-14', '5-LOW', 'Clerk#000000001', 0, 'note')",
        )
        .await
        .expect("insert orders probe row");

    let messages = client
        .simple_query(
            "SELECT o_orderpriority, COUNT(*) \
             FROM orders_probe \
             WHERE o_orderdate = DATE '1995-03-15' \
             GROUP BY o_orderpriority",
        )
        .await
        .expect("aggregate over empty filtered wide table");
    let rows: Vec<_> = messages
        .into_iter()
        .filter(|message| matches!(message, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert!(rows.is_empty(), "no groups should survive the empty filter");
    shutdown(running).await;
}
