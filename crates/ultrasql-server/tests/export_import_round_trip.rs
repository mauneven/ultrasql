//! End-to-end `EXPORT DATABASE` / `IMPORT DATABASE` coverage.

pub mod support;

use std::path::Path;

use support::{shutdown, start_persistent_server};
use tokio_postgres::error::SqlState;

fn sql_string(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "''"))
}

async fn simple_i64(client: &tokio_postgres::Client, sql: &str) -> i64 {
    let rows = client.simple_query(sql).await.expect("simple query");
    rows.iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse::<i64>().ok(),
            _ => None,
        })
        .expect("one int8 row")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_import_round_trips_schema_data_indexes_and_sequences() {
    let source_dir = tempfile::TempDir::new().expect("source data dir");
    let target_dir = tempfile::TempDir::new().expect("target data dir");
    let dump_root = tempfile::TempDir::new().expect("dump parent");
    let dump_dir = dump_root.path().join("ultrasql-export");

    let source = start_persistent_server(source_dir.path(), "export_import_source").await;
    source
        .client
        .batch_execute(
            "CREATE SCHEMA app;
             CREATE SEQUENCE app.ticket_seq START WITH 7 INCREMENT BY 3;
             SELECT nextval('app.ticket_seq');
             CREATE TABLE app.portable_items (
                 id INT NOT NULL,
                 label TEXT,
                 qty INT
             );
             INSERT INTO app.portable_items VALUES
                 (1, 'alpha', 10),
                 (2, 'beta', NULL),
                 (3, 'quote''d', 30);
             CREATE INDEX portable_items_label_idx ON app.portable_items (label);",
        )
        .await
        .expect("setup source database");
    source
        .client
        .batch_execute(&format!("EXPORT DATABASE TO {}", sql_string(&dump_dir)))
        .await
        .expect("export database");
    shutdown(source).await;

    assert!(dump_dir.join("manifest.json").exists());
    assert!(dump_dir.join("checksums.json").exists());
    assert!(dump_dir.join("schema.sql").exists());
    assert!(
        std::fs::read_dir(dump_dir.join("data"))
            .expect("data dir exists")
            .next()
            .is_some(),
        "export should include table data file"
    );

    let target = start_persistent_server(target_dir.path(), "export_import_target").await;
    target
        .client
        .batch_execute(&format!("IMPORT DATABASE FROM {}", sql_string(&dump_dir)))
        .await
        .expect("import database");

    let rows = target
        .client
        .query(
            "SELECT id, label, qty FROM app.portable_items ORDER BY id",
            &[],
        )
        .await
        .expect("query imported table");
    let values = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, i32>(0),
                row.get::<_, String>(1),
                row.get::<_, Option<i32>>(2),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            (1, "alpha".to_owned(), Some(10)),
            (2, "beta".to_owned(), None),
            (3, "quote'd".to_owned(), Some(30)),
        ]
    );
    assert_eq!(
        simple_i64(
            &target.client,
            "SELECT COUNT(*) FROM pg_catalog.pg_indexes \
             WHERE schemaname = 'app' AND indexname = 'portable_items_label_idx'",
        )
        .await,
        1
    );
    assert_eq!(
        simple_i64(&target.client, "SELECT nextval('app.ticket_seq')").await,
        10
    );
    shutdown(target).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_rejects_non_empty_target_database() {
    let source_dir = tempfile::TempDir::new().expect("source data dir");
    let target_dir = tempfile::TempDir::new().expect("target data dir");
    let dump_root = tempfile::TempDir::new().expect("dump parent");
    let dump_dir = dump_root.path().join("ultrasql-export");

    let source = start_persistent_server(source_dir.path(), "export_import_nonempty_src").await;
    source
        .client
        .batch_execute(
            "CREATE TABLE export_nonempty_src (id INT);
             INSERT INTO export_nonempty_src VALUES (1);",
        )
        .await
        .expect("setup source");
    source
        .client
        .batch_execute(&format!("EXPORT DATABASE TO {}", sql_string(&dump_dir)))
        .await
        .expect("export database");
    shutdown(source).await;

    let target = start_persistent_server(target_dir.path(), "export_import_nonempty_target").await;
    target
        .client
        .batch_execute("CREATE TABLE existing_target (id INT);")
        .await
        .expect("setup target");
    let err = target
        .client
        .batch_execute(&format!("IMPORT DATABASE FROM {}", sql_string(&dump_dir)))
        .await
        .expect_err("non-empty import target must fail");
    let db = err.as_db_error().expect("server ErrorResponse");
    assert_eq!(db.code(), &SqlState::FEATURE_NOT_SUPPORTED);
    assert!(
        db.message().contains("target database is not empty"),
        "unexpected error: {db:?}"
    );
    shutdown(target).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_rejects_checksum_mismatch_before_mutating_target() {
    let source_dir = tempfile::TempDir::new().expect("source data dir");
    let target_dir = tempfile::TempDir::new().expect("target data dir");
    let dump_root = tempfile::TempDir::new().expect("dump parent");
    let dump_dir = dump_root.path().join("ultrasql-export");

    let source = start_persistent_server(source_dir.path(), "export_import_checksum_src").await;
    source
        .client
        .batch_execute(
            "CREATE TABLE checksum_src (id INT, note TEXT);
             INSERT INTO checksum_src VALUES (1, 'ok');",
        )
        .await
        .expect("setup source");
    source
        .client
        .batch_execute(&format!("EXPORT DATABASE TO {}", sql_string(&dump_dir)))
        .await
        .expect("export database");
    shutdown(source).await;

    let data_file = std::fs::read_dir(dump_dir.join("data"))
        .expect("data dir")
        .next()
        .expect("one data file")
        .expect("data entry")
        .path();
    std::fs::write(&data_file, b"tampered\n").expect("tamper data file");

    let target = start_persistent_server(target_dir.path(), "export_import_checksum_target").await;
    let err = target
        .client
        .batch_execute(&format!("IMPORT DATABASE FROM {}", sql_string(&dump_dir)))
        .await
        .expect_err("checksum mismatch must fail");
    let db = err.as_db_error().expect("server ErrorResponse");
    assert_eq!(db.code(), &SqlState::FEATURE_NOT_SUPPORTED);
    assert!(
        db.message().contains("checksum mismatch"),
        "unexpected error: {db:?}"
    );
    assert_eq!(
        simple_i64(
            &target.client,
            "SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE tablename = 'checksum_src'",
        )
        .await,
        0
    );
    shutdown(target).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_and_import_are_rejected_inside_explicit_transaction() {
    let source_dir = tempfile::TempDir::new().expect("source data dir");
    let target_dir = tempfile::TempDir::new().expect("target data dir");
    let dump_root = tempfile::TempDir::new().expect("dump parent");
    let dump_dir = dump_root.path().join("ultrasql-export");

    let source = start_persistent_server(source_dir.path(), "export_import_txn_export").await;
    source
        .client
        .batch_execute("CREATE TABLE export_txn_src (id INT);")
        .await
        .expect("setup source");
    source.client.batch_execute("BEGIN").await.expect("begin");
    let err = source
        .client
        .batch_execute(&format!("EXPORT DATABASE TO {}", sql_string(&dump_dir)))
        .await
        .expect_err("export in explicit transaction must fail");
    assert_eq!(
        err.as_db_error().expect("server ErrorResponse").code(),
        &SqlState::FEATURE_NOT_SUPPORTED
    );
    source
        .client
        .batch_execute("ROLLBACK")
        .await
        .expect("rollback failed transaction");
    source
        .client
        .batch_execute(&format!("EXPORT DATABASE TO {}", sql_string(&dump_dir)))
        .await
        .expect("export outside transaction");
    shutdown(source).await;

    let target = start_persistent_server(target_dir.path(), "export_import_txn_import").await;
    target.client.batch_execute("BEGIN").await.expect("begin");
    let err = target
        .client
        .batch_execute(&format!("IMPORT DATABASE FROM {}", sql_string(&dump_dir)))
        .await
        .expect_err("import in explicit transaction must fail");
    assert_eq!(
        err.as_db_error().expect("server ErrorResponse").code(),
        &SqlState::FEATURE_NOT_SUPPORTED
    );
    target
        .client
        .batch_execute("ROLLBACK")
        .await
        .expect("rollback failed transaction");
    shutdown(target).await;
}
