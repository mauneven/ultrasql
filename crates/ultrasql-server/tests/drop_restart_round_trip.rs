//! Persistent `DROP TABLE` restart coverage through the PostgreSQL wire path.

mod support;

use support::{shutdown, start_persistent_server};
use ultrasql_server::Server;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_table_stays_dropped_after_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "drop_restart_test").await;
    running
        .client
        .batch_execute("CREATE TABLE drop_restart (id INT)")
        .await
        .expect("create");
    running
        .client
        .batch_execute("INSERT INTO drop_restart VALUES (7)")
        .await
        .expect("insert");
    running
        .client
        .batch_execute("DROP TABLE drop_restart")
        .await
        .expect("drop");
    assert_undefined_table(&running.client, "SELECT id FROM drop_restart").await;
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "drop_restart_test").await;
    assert_undefined_table(&running.client, "SELECT id FROM drop_restart").await;
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_table_is_removed_from_runtime_metadata() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running = start_persistent_server(data_dir.path(), "drop_runtime_meta_test").await;
    running
        .client
        .batch_execute("CREATE TABLE drop_runtime_meta (id SERIAL, v INT DEFAULT 7)")
        .await
        .expect("create table with runtime metadata");
    let metadata = std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    assert!(
        metadata.contains("drop_runtime_meta"),
        "table runtime metadata should record table before drop: {metadata}"
    );

    running
        .client
        .batch_execute("DROP TABLE drop_runtime_meta")
        .await
        .expect("drop table");
    shutdown(running).await;

    let metadata = std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    assert!(
        !metadata.contains("drop_runtime_meta"),
        "dropped table must be removed from runtime metadata: {metadata}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_runtime_metadata_rejects_duplicate_table_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running = start_persistent_server(data_dir.path(), "table_runtime_duplicate_meta").await;
    running
        .client
        .batch_execute("CREATE TABLE table_runtime_duplicate (id SERIAL, v INT DEFAULT 7)")
        .await
        .expect("create table with runtime metadata");
    shutdown(running).await;

    let mut metadata =
        std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    let table_line = metadata
        .lines()
        .find(|line| line.starts_with("table\t") && line.contains("table_runtime_duplicate"))
        .expect("table runtime metadata row")
        .to_owned();
    metadata.push_str(&table_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate table runtime metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate table metadata rejected");
    assert!(
        err.to_string().contains("duplicate table-runtime metadata"),
        "expected duplicate table-runtime metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_runtime_metadata_rejects_duplicate_default_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running = start_persistent_server(data_dir.path(), "table_runtime_duplicate_default").await;
    running
        .client
        .batch_execute("CREATE TABLE table_runtime_default_dup (id INT, v INT DEFAULT 7)")
        .await
        .expect("create table with default metadata");
    shutdown(running).await;

    let mut metadata =
        std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    let default_line = metadata
        .lines()
        .find(|line| line.starts_with("default\t"))
        .expect("default metadata row")
        .to_owned();
    metadata.push_str(&default_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate default metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate default metadata rejected");
    assert!(
        err.to_string()
            .contains("duplicate table-runtime default metadata"),
        "expected duplicate table-runtime default metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_runtime_metadata_rejects_duplicate_sequence_default_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running =
        start_persistent_server(data_dir.path(), "table_runtime_duplicate_sequence_default").await;
    running
        .client
        .batch_execute("CREATE TABLE table_runtime_seq_dup (id SERIAL, v INT)")
        .await
        .expect("create table with sequence default metadata");
    shutdown(running).await;

    let mut metadata =
        std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    let sequence_line = metadata
        .lines()
        .find(|line| line.starts_with("sequence_default\t"))
        .expect("sequence default metadata row")
        .to_owned();
    metadata.push_str(&sequence_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate sequence default metadata");

    let err =
        Server::init(data_dir.path()).expect_err("duplicate sequence default metadata rejected");
    assert!(
        err.to_string()
            .contains("duplicate table-runtime sequence default metadata"),
        "expected duplicate table-runtime sequence default metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_runtime_metadata_rejects_duplicate_identity_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running =
        start_persistent_server(data_dir.path(), "table_runtime_duplicate_identity").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE table_runtime_identity_dup \
             (id INT GENERATED ALWAYS AS IDENTITY, v INT)",
        )
        .await
        .expect("create table with identity metadata");
    shutdown(running).await;

    let mut metadata =
        std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    let identity_line = metadata
        .lines()
        .find(|line| line.starts_with("identity_always\t"))
        .expect("identity metadata row")
        .to_owned();
    metadata.push_str(&identity_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate identity metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate identity metadata rejected");
    assert!(
        err.to_string()
            .contains("duplicate table-runtime identity metadata"),
        "expected duplicate table-runtime identity metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_runtime_metadata_rejects_duplicate_generated_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running =
        start_persistent_server(data_dir.path(), "table_runtime_duplicate_generated").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE table_runtime_generated_dup \
             (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED)",
        )
        .await
        .expect("create table with generated metadata");
    shutdown(running).await;

    let mut metadata =
        std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    let generated_line = metadata
        .lines()
        .find(|line| line.starts_with("generated_stored\t"))
        .expect("generated metadata row")
        .to_owned();
    metadata.push_str(&generated_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate generated metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate generated metadata rejected");
    assert!(
        err.to_string()
            .contains("duplicate table-runtime generated metadata"),
        "expected duplicate table-runtime generated metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_runtime_metadata_rejects_duplicate_check_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running = start_persistent_server(data_dir.path(), "table_runtime_duplicate_check").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE table_runtime_check_dup \
             (id INT CONSTRAINT table_runtime_check_dup_positive CHECK (id > 0))",
        )
        .await
        .expect("create table with check metadata");
    shutdown(running).await;

    let mut metadata =
        std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    let check_line = metadata
        .lines()
        .find(|line| line.starts_with("check\t"))
        .expect("check metadata row")
        .to_owned();
    metadata.push_str(&check_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate check metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate check metadata rejected");
    assert!(
        err.to_string()
            .contains("duplicate table-runtime check metadata"),
        "expected duplicate table-runtime check metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_runtime_metadata_rejects_duplicate_foreign_key_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running =
        start_persistent_server(data_dir.path(), "table_runtime_duplicate_foreign_key").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE table_runtime_fk_parent (id INT PRIMARY KEY); \
             CREATE TABLE table_runtime_fk_child \
             (parent_id INT CONSTRAINT table_runtime_fk_child_parent \
              REFERENCES table_runtime_fk_parent(id))",
        )
        .await
        .expect("create table with foreign-key metadata");
    shutdown(running).await;

    let mut metadata =
        std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    let foreign_key_line = metadata
        .lines()
        .find(|line| line.starts_with("foreign_key\t"))
        .expect("foreign-key metadata row")
        .to_owned();
    metadata.push_str(&foreign_key_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate foreign-key metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate foreign-key metadata rejected");
    assert!(
        err.to_string()
            .contains("duplicate table-runtime foreign-key metadata"),
        "expected duplicate table-runtime foreign-key metadata rejection, got {err}"
    );
}

async fn assert_undefined_table(client: &tokio_postgres::Client, sql: &str) {
    let err = client.query(sql, &[]).await.expect_err("query should fail");
    let db_error = err.as_db_error().expect("server returns SQLSTATE");
    assert_eq!(db_error.code().code(), "42P01");
}
