//! End-to-end `CREATE VIEW` / `ALTER VIEW` coverage.

pub mod support;

use support::{shutdown, start_persistent_server, start_sample_server};

#[tokio::test]
async fn create_view_selects_from_current_base_rows() {
    let running = start_sample_server("view-round-trip").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE view_src (id INT NOT NULL, amount INT NOT NULL);
             INSERT INTO view_src VALUES (1, 10), (2, 20);
             CREATE VIEW view_copy (item_id, doubled) AS
                 SELECT id, amount * 2 FROM view_src",
        )
        .await
        .expect("create view");

    client
        .batch_execute("INSERT INTO view_src VALUES (3, 30)")
        .await
        .expect("append base row");

    let rows = client
        .query(
            "SELECT item_id, doubled FROM view_copy ORDER BY item_id",
            &[],
        )
        .await
        .expect("select view");
    let values = rows
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
        .collect::<Vec<_>>();
    assert_eq!(values, vec![(1, 20), (2, 40), (3, 60)]);

    let catalog_row = client
        .query_one(
            "SELECT c.relkind, v.viewowner \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_views v ON v.viewname = c.relname \
             WHERE c.relname = 'view_copy'",
            &[],
        )
        .await
        .expect("view catalog row");
    assert_eq!(catalog_row.get::<_, String>(0), "v");
    assert_eq!(catalog_row.get::<_, String>(1), "ultrasql");

    let table_count = client
        .query_one(
            "SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE tablename = 'view_copy'",
            &[],
        )
        .await
        .expect("view excluded from pg_tables");
    assert_eq!(table_count.get::<_, i64>(0), 0);

    shutdown(running).await;
}

#[tokio::test]
async fn alter_view_rename_and_set_schema_move_metadata() {
    let running = start_sample_server("view-round-trip").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE SCHEMA app;
             CREATE TABLE alter_view_src (id INT NOT NULL, amount INT NOT NULL);
             INSERT INTO alter_view_src VALUES (1, 10), (2, 20);
             CREATE VIEW alter_view_old AS
                 SELECT id, amount FROM alter_view_src;
             ALTER VIEW alter_view_old RENAME TO alter_view_new;
             ALTER VIEW alter_view_new SET SCHEMA app;",
        )
        .await
        .expect("alter view");

    let rows = client
        .query("SELECT id, amount FROM app.alter_view_new ORDER BY id", &[])
        .await
        .expect("select renamed view");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    let old_err = client
        .query("SELECT id FROM alter_view_old", &[])
        .await
        .expect_err("old view name must not resolve");
    let old_msg = old_err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or("");
    assert!(
        old_msg.contains("alter_view_old"),
        "old view error should name missing object: {old_err}"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn alter_view_rejects_tables_and_dependent_renames() {
    let running = start_sample_server("view-round-trip").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE alter_view_dep_src (id INT NOT NULL);
             CREATE VIEW alter_view_base AS SELECT id FROM alter_view_dep_src;
             CREATE VIEW alter_view_child AS SELECT id FROM alter_view_base;",
        )
        .await
        .expect("create dependent views");

    let table_err = client
        .batch_execute("ALTER VIEW alter_view_dep_src RENAME TO should_fail")
        .await
        .expect_err("ALTER VIEW must reject ordinary table");
    let table_msg = table_err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or("");
    assert!(
        table_msg.contains("is not a view"),
        "table error should be actionable: {table_err}"
    );

    let dep_err = client
        .batch_execute("ALTER VIEW alter_view_base RENAME TO alter_view_base_new")
        .await
        .expect_err("dependent view should block rename");
    let dep_msg = dep_err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or("");
    assert!(
        dep_msg.contains("alter_view_child"),
        "dependent error should name child view: {dep_err}"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn views_reject_dml_targets_and_definition_replacement() {
    let running = start_sample_server("view-round-trip").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE view_dml_src (id INT NOT NULL, amount INT NOT NULL);
             INSERT INTO view_dml_src VALUES (1, 10);
             CREATE VIEW view_dml_v AS SELECT id, amount FROM view_dml_src;",
        )
        .await
        .expect("create view for DML rejection");

    let insert_err = client
        .batch_execute("INSERT INTO view_dml_v VALUES (2, 20)")
        .await
        .expect_err("INSERT into view must fail");
    let insert_msg = insert_err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or("");
    assert!(
        insert_msg.contains("cannot modify view"),
        "DML target error should be actionable: {insert_err}"
    );

    let replace_err = client
        .batch_execute("ALTER VIEW view_dml_v AS SELECT id FROM view_dml_src")
        .await
        .expect_err("ALTER VIEW AS SELECT must fail until replacement is supported");
    let replace_msg = replace_err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or("");
    assert!(
        replace_msg.contains("ALTER VIEW ... AS SELECT is not supported"),
        "replace error should be explicit: {replace_err}"
    );

    client.batch_execute("BEGIN").await.expect("begin");
    let txn_err = client
        .batch_execute("ALTER VIEW view_dml_v RENAME TO view_dml_renamed")
        .await
        .expect_err("ALTER VIEW in explicit transaction must fail");
    // DDL-in-transaction is rejected with SQLSTATE 0A000
    // (feature_not_supported) — PostgreSQL implements transactional DDL;
    // UltraSQL does not yet (see docs/transactional-ddl-design.md).
    let txn_sqlstate = txn_err
        .code()
        .map_or_else(String::new, |c| c.code().to_string());
    assert_eq!(
        txn_sqlstate, "0A000",
        "transactional DDL must be feature_not_supported: {txn_err}"
    );
    let txn_msg = txn_err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or("");
    assert!(
        txn_msg.contains("DDL inside an explicit transaction block"),
        "transactional DDL error should be explicit: {txn_err}"
    );
    client.batch_execute("ROLLBACK").await.expect("rollback");
    client
        .query_one("SELECT COUNT(*) FROM view_dml_v", &[])
        .await
        .expect("view remains after rejected transaction");

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn view_metadata_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("data dir");

    let running = start_persistent_server(data_dir.path(), "view_restart_source").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE view_restart_src (id INT NOT NULL, amount INT NOT NULL);
             INSERT INTO view_restart_src VALUES (1, 10);
             CREATE VIEW view_restart_v AS
                 SELECT id, amount FROM view_restart_src;",
        )
        .await
        .expect("create persistent view");
    shutdown(running).await;

    let restarted = start_persistent_server(data_dir.path(), "view_restart_target").await;
    restarted
        .client
        .batch_execute("INSERT INTO view_restart_src VALUES (2, 20)")
        .await
        .expect("insert after restart");
    let rows = restarted
        .client
        .query("SELECT id, amount FROM view_restart_v ORDER BY id", &[])
        .await
        .expect("select restarted view");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(1), 20);
    shutdown(restarted).await;
}
