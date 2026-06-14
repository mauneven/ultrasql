//! End-to-end `MERGE INTO` tests over the PostgreSQL wire protocol.

pub mod support;

use support::{shutdown, start_sample_server};

#[tokio::test]
async fn merge_applies_update_delete_and_insert_branches() {
    let running = start_sample_server("merge_branches").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE target (id INT PRIMARY KEY, v INT NOT NULL, marker TEXT);
             CREATE TABLE source (id INT NOT NULL, v INT NOT NULL, op TEXT);
             INSERT INTO target VALUES (1, 10, 'old'), (2, 20, 'old'), (3, 30, 'stay');
             INSERT INTO source VALUES (1, 100, 'update'), (2, 200, 'delete'), (4, 400, 'insert')",
        )
        .await
        .expect("setup");

    client
        .batch_execute(
            "MERGE INTO target AS t
             USING source AS s
             ON t.id = s.id
             WHEN MATCHED AND s.op = 'delete' THEN DELETE
             WHEN MATCHED THEN UPDATE SET v = s.v, marker = s.op
             WHEN NOT MATCHED THEN INSERT (id, v, marker) VALUES (s.id, s.v, s.op)",
        )
        .await
        .expect("merge branches");

    let rows = client
        .query("SELECT id, v, marker FROM target ORDER BY id", &[])
        .await
        .expect("select target");
    let persisted: Vec<(i32, i32, Option<String>)> = rows
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    assert_eq!(
        persisted,
        vec![
            (1, 100, Some("update".to_owned())),
            (3, 30, Some("stay".to_owned())),
            (4, 400, Some("insert".to_owned())),
        ]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn merge_rejects_duplicate_source_matches_without_mutating() {
    let running = start_sample_server("merge_duplicate").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE target (id INT PRIMARY KEY, v INT NOT NULL);
             CREATE TABLE source (id INT NOT NULL, v INT NOT NULL);
             INSERT INTO target VALUES (1, 10);
             INSERT INTO source VALUES (1, 11), (1, 12)",
        )
        .await
        .expect("setup");

    let err = client
        .batch_execute(
            "MERGE INTO target AS t
             USING source AS s
             ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET v = s.v",
        )
        .await
        .expect_err("duplicate source matches must fail");
    let message = err
        .as_db_error()
        .expect("server-sent ErrorResponse")
        .message()
        .to_ascii_lowercase();
    assert!(
        message.contains("merge") && message.contains("match"),
        "unexpected duplicate-match error: {err:?}"
    );

    let row = client
        .query_one("SELECT v FROM target WHERE id = 1", &[])
        .await
        .expect("target remains queryable");
    assert_eq!(row.get::<_, i32>(0), 10);

    shutdown(running).await;
}

#[tokio::test]
async fn merge_on_null_comparison_does_not_match() {
    let running = start_sample_server("merge_null_semantics").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE target (k INT, v INT NOT NULL);
             CREATE TABLE source (k INT, v INT NOT NULL);
             INSERT INTO target VALUES (NULL, 10);
             INSERT INTO source VALUES (NULL, 20)",
        )
        .await
        .expect("setup");

    client
        .batch_execute(
            "MERGE INTO target AS t
             USING source AS s
             ON t.k = s.k
             WHEN MATCHED THEN UPDATE SET v = s.v
             WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)",
        )
        .await
        .expect("merge null semantics");

    let row = client
        .query_one("SELECT COUNT(*) FROM target WHERE k IS NULL", &[])
        .await
        .expect("count null keys");
    assert_eq!(row.get::<_, i64>(0), 2);

    shutdown(running).await;
}

#[tokio::test]
async fn merge_rolls_back_inside_transaction() {
    let running = start_sample_server("merge_rollback").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE target (id INT PRIMARY KEY, v INT NOT NULL);
             CREATE TABLE source (id INT NOT NULL, v INT NOT NULL);
             INSERT INTO target VALUES (1, 10);
             INSERT INTO source VALUES (1, 100), (2, 200)",
        )
        .await
        .expect("setup");

    client
        .batch_execute(
            "BEGIN;
             MERGE INTO target AS t
             USING source AS s
             ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET v = s.v
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
             ROLLBACK",
        )
        .await
        .expect("merge rolls back");

    let rows = client
        .query("SELECT id, v FROM target ORDER BY id", &[])
        .await
        .expect("select after rollback");
    let persisted: Vec<(i32, i32)> = rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    assert_eq!(persisted, vec![(1, 10)]);

    shutdown(running).await;
}

#[tokio::test]
async fn merge_constraint_violation_is_atomic() {
    let running = start_sample_server("merge_constraint").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE target (id INT PRIMARY KEY, v INT NOT NULL);
             CREATE TABLE source (id INT, v INT);
             INSERT INTO target VALUES (1, 10);
             INSERT INTO source VALUES (1, 100), (2, NULL)",
        )
        .await
        .expect("setup");

    let err = client
        .batch_execute(
            "MERGE INTO target AS t
             USING source AS s
             ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET v = s.v
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
        )
        .await
        .expect_err("NOT NULL violation aborts MERGE");
    let sqlstate = err.code().expect("server-sent SQLSTATE present");
    assert_eq!(sqlstate.code(), "23502");

    let rows = client
        .query("SELECT id, v FROM target ORDER BY id", &[])
        .await
        .expect("select after rejected merge");
    let persisted: Vec<(i32, i32)> = rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    assert_eq!(persisted, vec![(1, 10)]);

    shutdown(running).await;
}

#[tokio::test]
async fn merge_maintains_indexes_after_update_and_insert() {
    let running = start_sample_server("merge_indexes").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE target (id INT PRIMARY KEY, v INT NOT NULL);
             CREATE INDEX target_v_idx ON target(v);
             CREATE TABLE source (id INT NOT NULL, v INT NOT NULL);
             INSERT INTO target VALUES (1, 10);
             INSERT INTO source VALUES (1, 99), (2, 20)",
        )
        .await
        .expect("setup");

    client
        .batch_execute(
            "MERGE INTO target AS t
             USING source AS s
             ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET v = s.v
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
        )
        .await
        .expect("merge with index maintenance");

    let rows = client
        .query("SELECT id FROM target WHERE v = 99", &[])
        .await
        .expect("indexed lookup after merge update");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);

    let rows = client
        .query("SELECT id FROM target WHERE v = 20", &[])
        .await
        .expect("indexed lookup after merge insert");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 2);

    shutdown(running).await;
}
