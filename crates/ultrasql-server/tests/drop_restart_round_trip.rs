//! Persistent `DROP TABLE` restart coverage through the PostgreSQL wire path.

pub mod support;

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
    support::make_data_dir_private(data_dir.path());
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

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_table_rejects_unsafe_row_security_metadata_slot_before_durable_create() {
    use std::os::unix::fs::symlink;

    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    support::make_data_dir_private(data_dir.path());
    let outside = data_dir.path().join("outside-row-security-meta");
    std::fs::write(&outside, b"keep").expect("outside metadata target");

    let running = start_persistent_server(data_dir.path(), "create_table_rowsec_slot").await;
    let client = &running.client;
    symlink(&outside, data_dir.path().join("pg_row_security.meta.tmp"))
        .expect("row-security temp symlink");

    let err = client
        .batch_execute("CREATE TABLE create_table_slot_guard (id INT)")
        .await
        .expect_err("unsafe row-security metadata slot rejects CREATE TABLE");
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("runtime metadata file")),
        "unexpected error: {err}"
    );
    assert_undefined_table(client, "SELECT id FROM create_table_slot_guard").await;
    shutdown(running).await;

    std::fs::remove_file(data_dir.path().join("pg_row_security.meta.tmp"))
        .expect("remove unsafe temp slot before restart");
    let running = start_persistent_server(data_dir.path(), "create_table_rowsec_restart").await;
    assert_undefined_table(&running.client, "SELECT id FROM create_table_slot_guard").await;
    shutdown(running).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_table_rejects_unsafe_runtime_metadata_slot_before_live_drop() {
    use std::os::unix::fs::symlink;

    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    support::make_data_dir_private(data_dir.path());
    let outside = data_dir.path().join("outside-table-runtime-meta");
    std::fs::write(&outside, b"keep").expect("outside metadata target");

    let running = start_persistent_server(data_dir.path(), "drop_table_runtime_slot").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE TABLE drop_table_slot_guard (id SERIAL, v INT DEFAULT 7); \
             INSERT INTO drop_table_slot_guard (v) VALUES (11)",
        )
        .await
        .expect("create table before failed drop");
    symlink(&outside, data_dir.path().join("pg_table_runtime.meta.tmp"))
        .expect("table-runtime temp symlink");

    let err = client
        .batch_execute("DROP TABLE drop_table_slot_guard")
        .await
        .expect_err("unsafe table runtime metadata slot rejects DROP TABLE");
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("runtime metadata file")),
        "unexpected error: {err}"
    );
    let rows = client
        .query("SELECT id, v FROM drop_table_slot_guard", &[])
        .await
        .expect("table remains queryable after rejected drop");
    assert_eq!(rows.len(), 1, "failed DROP TABLE must not remove table");
    assert_eq!(rows[0].get::<_, i32>(1), 11);

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_runtime_metadata_rejects_duplicate_table_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
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
async fn table_runtime_metadata_rejects_unknown_table_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running = start_persistent_server(data_dir.path(), "table_runtime_unknown_meta").await;
    running
        .client
        .batch_execute("CREATE TABLE table_runtime_known (id INT)")
        .await
        .expect("create table with runtime metadata");
    shutdown(running).await;

    let mut metadata =
        std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    metadata.push_str("table\tghost_table\t424242\n");
    std::fs::write(&metadata_path, metadata).expect("unknown table runtime metadata");

    let err = Server::init(data_dir.path()).expect_err("unknown table metadata rejected");
    assert!(
        err.to_string()
            .contains("unknown table-runtime metadata table"),
        "expected unknown table-runtime metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_runtime_metadata_rejects_orphan_constraint_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running = start_persistent_server(data_dir.path(), "table_runtime_orphan_constraint").await;
    running
        .client
        .batch_execute("CREATE TABLE table_runtime_orphan (id INT, v INT DEFAULT 7)")
        .await
        .expect("create table with runtime metadata");
    shutdown(running).await;

    let mut metadata =
        std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    let default_line = metadata
        .lines()
        .find(|line| line.starts_with("default\t"))
        .expect("default metadata row");
    let mut parts = default_line.split('\t').collect::<Vec<_>>();
    parts[1] = "424242";
    metadata.push_str(&parts.join("\t"));
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("orphan constraint metadata");

    let err = Server::init(data_dir.path()).expect_err("orphan constraint metadata rejected");
    assert!(
        err.to_string()
            .contains("orphan table-runtime metadata rows"),
        "expected orphan table-runtime metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_runtime_metadata_rejects_duplicate_default_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
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
    support::make_data_dir_private(data_dir.path());
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
    support::make_data_dir_private(data_dir.path());
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
    support::make_data_dir_private(data_dir.path());
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
    support::make_data_dir_private(data_dir.path());
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
    support::make_data_dir_private(data_dir.path());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_runtime_metadata_rejects_mismatched_foreign_key_target_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running =
        start_persistent_server(data_dir.path(), "table_runtime_mismatched_foreign_key").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE table_runtime_fk_parent_mismatch (id INT PRIMARY KEY); \
             CREATE TABLE table_runtime_fk_child_mismatch \
             (parent_id INT CONSTRAINT table_runtime_fk_child_mismatch_parent \
              REFERENCES table_runtime_fk_parent_mismatch(id))",
        )
        .await
        .expect("create table with foreign-key metadata");
    shutdown(running).await;

    let mut metadata =
        std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    let old_line = metadata
        .lines()
        .find(|line| line.starts_with("foreign_key\t"))
        .expect("foreign-key metadata row")
        .to_owned();
    let mut parts = old_line.split('\t').collect::<Vec<_>>();
    parts[5] = "424242";
    let new_line = parts.join("\t");
    metadata = metadata.replace(&old_line, &new_line);
    std::fs::write(&metadata_path, metadata).expect("mismatched foreign-key metadata");

    let err = Server::init(data_dir.path()).expect_err("mismatched foreign-key target rejected");
    assert!(
        err.to_string()
            .contains("invalid table-runtime foreign-key target metadata"),
        "expected invalid foreign-key target metadata rejection, got {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_runtime_metadata_rejects_duplicate_exclusion_rows_on_rebuild() {
    let data_dir = tempfile::TempDir::new().unwrap();
    support::make_data_dir_private(data_dir.path());
    let metadata_path = data_dir.path().join("pg_table_runtime.meta");

    let running =
        start_persistent_server(data_dir.path(), "table_runtime_duplicate_exclusion").await;
    running
        .client
        .batch_execute(
            "CREATE TABLE table_runtime_exclusion_dup (\
             room INT NOT NULL, \
             during INT4RANGE NOT NULL, \
             CONSTRAINT table_runtime_exclusion_no_overlap \
             EXCLUDE USING gist (room WITH =, during WITH &&))",
        )
        .await
        .expect("create table with exclusion metadata");
    shutdown(running).await;

    let mut metadata =
        std::fs::read_to_string(&metadata_path).expect("table runtime metadata exists");
    let exclusion_line = metadata
        .lines()
        .find(|line| line.starts_with("exclusion\t"))
        .expect("exclusion metadata row")
        .to_owned();
    metadata.push_str(&exclusion_line);
    metadata.push('\n');
    std::fs::write(&metadata_path, metadata).expect("duplicate exclusion metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate exclusion metadata rejected");
    assert!(
        err.to_string()
            .contains("duplicate table-runtime exclusion metadata"),
        "expected duplicate table-runtime exclusion metadata rejection, got {err}"
    );
}

async fn assert_undefined_table(client: &tokio_postgres::Client, sql: &str) {
    let err = client.query(sql, &[]).await.expect_err("query should fail");
    let db_error = err.as_db_error().expect("server returns SQLSTATE");
    assert_eq!(db_error.code().code(), "42P01");
}
