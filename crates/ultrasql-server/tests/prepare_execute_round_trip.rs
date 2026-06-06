//! Wire-level tests for `PREPARE` / `EXECUTE` / `DEALLOCATE` issued
//! through the Simple Query path.
//!
//! Closes the v0.5 ROADMAP item "PREPARE / EXECUTE / DEALLOCATE
//! Simple-Query round-trip".

pub mod support;

use support::{shutdown, start_sample_server};

/// `PREPARE` then `EXECUTE` round-trips with the substituted args.
#[tokio::test]
async fn prepare_then_execute_substitutes_args() {
    let running = start_sample_server("prepare_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)")
        .await
        .expect("seed rows");

    client
        .batch_execute("PREPARE pick AS SELECT val FROM t WHERE id = $1")
        .await
        .expect("PREPARE");

    let rows = client
        .simple_query("EXECUTE pick (2)")
        .await
        .expect("EXECUTE pick (2)");
    let mut got: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).and_then(|s| s.parse().ok()),
            _ => None,
        })
        .collect();
    got.sort_unstable();
    assert_eq!(got, vec![200]);

    let rows = client
        .simple_query("EXECUTE pick (3)")
        .await
        .expect("EXECUTE pick (3)");
    let got: Vec<i32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).and_then(|s| s.parse().ok()),
            _ => None,
        })
        .collect();
    assert_eq!(got, vec![300]);

    shutdown(running).await;
}

/// `PREPARE` of a DML statement, then `EXECUTE` performs the writes.
#[tokio::test]
async fn prepare_insert_then_execute_writes_rows() {
    let running = start_sample_server("prepare_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT)")
        .await
        .expect("create table");

    client
        .batch_execute("PREPARE ins AS INSERT INTO t VALUES ($1, $2)")
        .await
        .expect("PREPARE ins");

    client
        .batch_execute("EXECUTE ins (1, 10)")
        .await
        .expect("EXECUTE ins (1, 10)");
    client
        .batch_execute("EXECUTE ins (2, 20)")
        .await
        .expect("EXECUTE ins (2, 20)");

    let rows = client
        .query("SELECT id, val FROM t ORDER BY id", &[])
        .await
        .expect("select after EXECUTE inserts");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    shutdown(running).await;
}

/// `DEALLOCATE name` removes a prepared statement; subsequent
/// `EXECUTE` of the same name errors.
#[tokio::test]
async fn deallocate_removes_prepared_statement() {
    let running = start_sample_server("prepare_test").await;
    let client = &running.client;

    client
        .batch_execute("PREPARE q AS SELECT 1")
        .await
        .expect("PREPARE");
    client
        .batch_execute("DEALLOCATE q")
        .await
        .expect("DEALLOCATE");

    let err = client
        .batch_execute("EXECUTE q")
        .await
        .expect_err("EXECUTE on deallocated name must error");
    let dbe = err.as_db_error().expect("server-side db error expected");
    assert!(
        dbe.message().contains("does not exist") || dbe.message().contains("\"q\""),
        "expected 'does not exist' error, got: {}",
        dbe.message()
    );

    shutdown(running).await;
}

/// `DEALLOCATE ALL` drops every prepared statement.
#[tokio::test]
async fn deallocate_all_drops_every_prepared_statement() {
    let running = start_sample_server("prepare_test").await;
    let client = &running.client;

    client
        .batch_execute("PREPARE a AS SELECT 1")
        .await
        .expect("PREPARE a");
    client
        .batch_execute("PREPARE b AS SELECT 2")
        .await
        .expect("PREPARE b");
    client
        .batch_execute("DEALLOCATE ALL")
        .await
        .expect("DEALLOCATE ALL");

    assert!(
        client.batch_execute("EXECUTE a").await.is_err(),
        "EXECUTE a must error after DEALLOCATE ALL"
    );
    assert!(
        client.batch_execute("EXECUTE b").await.is_err(),
        "EXECUTE b must error after DEALLOCATE ALL"
    );

    shutdown(running).await;
}

/// Re-PREPARing a name without DEALLOCATE first errors.
#[tokio::test]
async fn duplicate_prepare_errors() {
    let running = start_sample_server("prepare_test").await;
    let client = &running.client;

    client
        .batch_execute("PREPARE dup AS SELECT 1")
        .await
        .expect("first PREPARE");
    let err = client
        .batch_execute("PREPARE dup AS SELECT 2")
        .await
        .expect_err("second PREPARE on same name must error");
    let dbe = err.as_db_error().expect("server-side db error expected");
    assert!(
        dbe.message().contains("already exists") || dbe.message().contains("\"dup\""),
        "expected 'already exists' error, got: {}",
        dbe.message()
    );

    shutdown(running).await;
}
