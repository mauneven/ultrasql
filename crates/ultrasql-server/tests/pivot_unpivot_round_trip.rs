//! End-to-end `PIVOT` and `UNPIVOT` tests over the PostgreSQL wire protocol.

pub mod support;

use support::{shutdown, start_sample_server};

#[tokio::test]
async fn pivot_sum_groups_implicit_columns_and_emits_null_for_missing_bucket() {
    let running = start_sample_server("pivot_sum").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE pivot_sales (region TEXT, quarter TEXT, amount INT);
             INSERT INTO pivot_sales VALUES
                ('east', 'Q1', 10),
                ('east', 'Q1', 5),
                ('east', 'Q2', 7),
                ('west', 'Q1', 3);",
        )
        .await
        .expect("setup pivot table");

    let rows = client
        .query(
            "SELECT region, q1, q2
             FROM pivot_sales
             PIVOT (SUM(amount) FOR quarter IN ('Q1' AS q1, 'Q2' AS q2))
             ORDER BY region",
            &[],
        )
        .await
        .expect("pivot query");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("region"), "east");
    assert_eq!(rows[0].get::<_, Option<i64>>("q1"), Some(15));
    assert_eq!(rows[0].get::<_, Option<i64>>("q2"), Some(7));
    assert_eq!(rows[1].get::<_, String>("region"), "west");
    assert_eq!(rows[1].get::<_, Option<i64>>("q1"), Some(3));
    assert_eq!(rows[1].get::<_, Option<i64>>("q2"), None);

    shutdown(running).await;
}

#[tokio::test]
async fn unpivot_excludes_nulls_by_default() {
    let running = start_sample_server("unpivot_exclude").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE quarterly_sales (id INT, q1 INT, q2 INT);
             INSERT INTO quarterly_sales VALUES (1, 10, 20), (2, NULL, 5);",
        )
        .await
        .expect("setup unpivot table");

    let rows = client
        .query(
            "SELECT id, quarter, amount
             FROM quarterly_sales
             UNPIVOT (amount FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2'))
             ORDER BY id, quarter",
            &[],
        )
        .await
        .expect("unpivot query");

    let values = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, i32>("id"),
                row.get::<_, String>("quarter"),
                row.get::<_, i32>("amount"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            (1, "Q1".to_owned(), 10),
            (1, "Q2".to_owned(), 20),
            (2, "Q2".to_owned(), 5),
        ]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn unpivot_include_nulls_retains_null_values() {
    let running = start_sample_server("unpivot_include").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE quarterly_sales_nulls (id INT, q1 INT, q2 INT);
             INSERT INTO quarterly_sales_nulls VALUES (1, 10, NULL);",
        )
        .await
        .expect("setup unpivot include table");

    let rows = client
        .query(
            "SELECT id, quarter, amount
             FROM quarterly_sales_nulls
             UNPIVOT INCLUDE NULLS (amount FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2'))
             ORDER BY quarter",
            &[],
        )
        .await
        .expect("unpivot include query");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("quarter"), "Q1");
    assert_eq!(rows[0].get::<_, Option<i32>>("amount"), Some(10));
    assert_eq!(rows[1].get::<_, String>("quarter"), "Q2");
    assert_eq!(rows[1].get::<_, Option<i32>>("amount"), None);

    shutdown(running).await;
}

#[tokio::test]
async fn pivot_empty_grouped_input_emits_no_rows() {
    let running = start_sample_server("pivot_empty").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE pivot_empty_sales (region TEXT, quarter TEXT, amount INT);")
        .await
        .expect("setup empty pivot table");

    let rows = client
        .query(
            "SELECT region, q1
             FROM pivot_empty_sales
             PIVOT (SUM(amount) FOR quarter IN ('Q1' AS q1))",
            &[],
        )
        .await
        .expect("empty pivot query");

    assert!(rows.is_empty());

    shutdown(running).await;
}

#[tokio::test]
async fn unpivot_empty_input_emits_no_rows() {
    let running = start_sample_server("unpivot_empty").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE quarterly_empty (id INT, q1 INT, q2 INT);")
        .await
        .expect("setup empty unpivot table");

    let rows = client
        .query(
            "SELECT id, quarter, amount
             FROM quarterly_empty
             UNPIVOT INCLUDE NULLS (amount FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2'))",
            &[],
        )
        .await
        .expect("empty unpivot query");

    assert!(rows.is_empty());

    shutdown(running).await;
}
