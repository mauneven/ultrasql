//! Wire-level tests for positional ORDER BY / GROUP BY ordinals.
//!
//! Regression for the bug where `GROUP BY 1` bound to an integer constant
//! (so every row collapsed into a single group) and `ORDER BY 1` became a
//! no-op sort on a constant. PostgreSQL treats a bare integer in ORDER BY /
//! GROUP BY as a 1-based reference to the Nth SELECT output column. These
//! drive the query through the full parse -> bind -> plan -> execute -> wire
//! path.

pub mod support;

use support::{shutdown, start_sample_server};

async fn seed(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE sales (region TEXT NOT NULL, amt INT NOT NULL)")
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO sales VALUES \
             ('east', 10), ('east', 5), ('west', 20), \
             ('north', 1), ('north', 2), ('north', 3)",
        )
        .await
        .expect("seed");
}

/// `GROUP BY 1` must group by the first output column (region), producing one
/// row per region — not collapse every row into one group. `ORDER BY 1` then
/// orders those groups by region ascending.
#[tokio::test]
async fn group_by_one_groups_per_region() {
    let running = start_sample_server("positional_test").await;
    let client = &running.client;
    seed(client).await;

    let rows = client
        .query(
            "SELECT region, SUM(amt) FROM sales GROUP BY 1 ORDER BY 1",
            &[],
        )
        .await
        .expect("GROUP BY 1 ORDER BY 1");

    assert_eq!(
        rows.len(),
        3,
        "GROUP BY 1 must produce one row per region, not a single collapsed group"
    );
    assert_eq!(rows[0].get::<_, String>(0), "east");
    assert_eq!(rows[0].get::<_, i64>(1), 15);
    assert_eq!(rows[1].get::<_, String>(0), "north");
    assert_eq!(rows[1].get::<_, i64>(1), 6);
    assert_eq!(rows[2].get::<_, String>(0), "west");
    assert_eq!(rows[2].get::<_, i64>(1), 20);

    shutdown(running).await;
}

/// `ORDER BY 2 DESC` sorts by the second output column (the aggregate)
/// descending — west (20) > east (15) > north (6).
#[tokio::test]
async fn order_by_two_desc_sorts_by_aggregate() {
    let running = start_sample_server("positional_test").await;
    let client = &running.client;
    seed(client).await;

    let rows = client
        .query(
            "SELECT region, SUM(amt) AS total FROM sales GROUP BY 1 ORDER BY 2 DESC",
            &[],
        )
        .await
        .expect("ORDER BY 2 DESC");

    assert_eq!(rows.len(), 3);
    let ordered: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(ordered, ["west", "east", "north"]);

    shutdown(running).await;
}

/// `ORDER BY 1` on a plain projection sorts by the first output column,
/// rather than degenerating into a no-op constant sort.
#[tokio::test]
async fn order_by_one_sorts_first_column() {
    let running = start_sample_server("positional_test").await;
    let client = &running.client;
    seed(client).await;

    let rows = client
        .query(
            "SELECT amt, region FROM sales WHERE region = 'north' ORDER BY 1 DESC",
            &[],
        )
        .await
        .expect("ORDER BY 1 DESC");

    let amounts: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(
        amounts,
        [3, 2, 1],
        "ORDER BY 1 DESC must sort by amt descending"
    );

    shutdown(running).await;
}
