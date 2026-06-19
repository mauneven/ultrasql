//! Wire-level tests for `STRING_AGG(value, delimiter)`.
//!
//! Regression for the bug where the delimiter argument was dropped, so
//! `STRING_AGG(x, ',')` concatenated with an empty separator ("abc")
//! instead of joining with the delimiter ("a,b,c"). These drive the
//! aggregate through the full parse → bind → plan → execute → wire path.

pub mod support;

use support::{shutdown, start_sample_server};

/// Group 1 has three rows, group 2 a single row, and group 3 a NULL
/// followed by one non-null value (to exercise NULL skipping).
const SEED_SQL: &str = "INSERT INTO items VALUES \
     (1, 'a'), (1, 'b'), (1, 'c'), (2, 'x'), (3, NULL), (3, 'y')";

async fn seed(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE items (grp INT NOT NULL, label TEXT)")
        .await
        .expect("create");
    client.batch_execute(SEED_SQL).await.expect("seed");
}

/// `STRING_AGG(label, ',')` over a single group joins the values with the
/// delimiter rather than concatenating them.
#[tokio::test]
async fn string_agg_joins_with_delimiter() {
    let running = start_sample_server("string_agg_test").await;
    let client = &running.client;
    seed(client).await;

    let r = client
        .query_one(
            "SELECT string_agg(label, ',') FROM items WHERE grp = 1",
            &[],
        )
        .await
        .expect("STRING_AGG");
    let got: String = r.get(0);
    assert_eq!(got, "a,b,c");

    shutdown(running).await;
}

/// `GROUP BY` with `STRING_AGG`: each group joins independently, NULLs
/// are skipped, and a single-row group emits no trailing delimiter.
#[tokio::test]
async fn string_agg_grouped_skips_nulls_and_single_rows() {
    let running = start_sample_server("string_agg_test").await;
    let client = &running.client;
    seed(client).await;

    let rows = client
        .query(
            "SELECT grp, string_agg(label, ',') FROM items GROUP BY grp ORDER BY grp",
            &[],
        )
        .await
        .expect("grouped STRING_AGG");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "a,b,c"); // multi-row join
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, String>(1), "x"); // single row, no delimiter
    assert_eq!(rows[2].get::<_, i32>(0), 3);
    assert_eq!(rows[2].get::<_, String>(1), "y"); // leading NULL skipped

    shutdown(running).await;
}

/// A custom multi-character delimiter is honoured verbatim.
#[tokio::test]
async fn string_agg_honours_multichar_delimiter() {
    let running = start_sample_server("string_agg_test").await;
    let client = &running.client;
    seed(client).await;

    let r = client
        .query_one(
            "SELECT string_agg(label, ' -> ') FROM items WHERE grp = 1",
            &[],
        )
        .await
        .expect("STRING_AGG multichar");
    let got: String = r.get(0);
    assert_eq!(got, "a -> b -> c");

    shutdown(running).await;
}

/// `STRING_AGG` over no non-null input yields SQL NULL, matching
/// PostgreSQL.
#[tokio::test]
async fn string_agg_all_null_returns_null() {
    let running = start_sample_server("string_agg_test").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE empty_items (label TEXT)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO empty_items VALUES (NULL), (NULL)")
        .await
        .expect("seed nulls");

    let r = client
        .query_one("SELECT string_agg(label, ',') FROM empty_items", &[])
        .await
        .expect("STRING_AGG all null");
    let got: Option<String> = r.get(0);
    assert!(got.is_none(), "all-NULL input should aggregate to NULL");

    shutdown(running).await;
}
