//! Persistent `ALTER TABLE` restart coverage through the PostgreSQL wire path.

pub mod support;

use support::{shutdown, start_persistent_server};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_table_drop_column_rewrite_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "alter_restart_test").await;
    running
        .client
        .batch_execute("CREATE TABLE alter_restart (id INT, label TEXT)")
        .await
        .expect("create");
    running
        .client
        .batch_execute("INSERT INTO alter_restart VALUES (1, 'alpha'), (2, 'bravo')")
        .await
        .expect("seed");
    running
        .client
        .batch_execute("ALTER TABLE alter_restart DROP COLUMN label")
        .await
        .expect("alter");
    let rows = running
        .client
        .query("SELECT id FROM alter_restart ORDER BY id", &[])
        .await
        .expect("select after alter");
    assert_eq!(rows.len(), 2);
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "alter_restart_test").await;
    let rows = running
        .client
        .query("SELECT id FROM alter_restart ORDER BY id", &[])
        .await
        .expect("select after restart");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_table_rename_column_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "alter_rename_restart_test").await;
    running
        .client
        .batch_execute("CREATE TABLE alter_rename_restart (id INT, label TEXT)")
        .await
        .expect("create");
    running
        .client
        .batch_execute("INSERT INTO alter_rename_restart VALUES (1, 'alpha')")
        .await
        .expect("seed");
    running
        .client
        .batch_execute("ALTER TABLE alter_rename_restart RENAME COLUMN label TO title")
        .await
        .expect("rename column");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "alter_rename_restart_test").await;
    let rows = running
        .client
        .query("SELECT title FROM alter_rename_restart WHERE id = 1", &[])
        .await
        .expect("renamed column resolves after restart");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "alpha");
    let err = running
        .client
        .query("SELECT label FROM alter_rename_restart", &[])
        .await
        .expect_err("old column name stays gone after restart");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42703");
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_table_rename_table_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "alter_rename_table_restart_test").await;
    running
        .client
        .batch_execute("CREATE TABLE alter_rename_old (id INT, label TEXT)")
        .await
        .expect("create");
    running
        .client
        .batch_execute("INSERT INTO alter_rename_old VALUES (1, 'alpha')")
        .await
        .expect("seed");
    running
        .client
        .batch_execute("ALTER TABLE alter_rename_old RENAME TO alter_rename_new")
        .await
        .expect("rename table");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "alter_rename_table_restart_test").await;
    let rows = running
        .client
        .query("SELECT label FROM alter_rename_new WHERE id = 1", &[])
        .await
        .expect("renamed table resolves after restart");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "alpha");
    let err = running
        .client
        .query("SELECT label FROM alter_rename_old", &[])
        .await
        .expect_err("old table name stays gone after restart");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42P01");
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_table_add_check_constraint_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "alter_add_check_restart_test").await;
    running
        .client
        .batch_execute("CREATE TABLE alter_check_restart (id INT, qty INT)")
        .await
        .expect("create");
    running
        .client
        .batch_execute("ALTER TABLE alter_check_restart ADD CONSTRAINT qty_pos CHECK (qty > 0)")
        .await
        .expect("add check");
    shutdown(running).await;

    // After restart the CHECK is still enforced on DML.
    let running = start_persistent_server(data_dir.path(), "alter_add_check_restart_test").await;
    running
        .client
        .batch_execute("INSERT INTO alter_check_restart VALUES (1, 5)")
        .await
        .expect("valid insert after restart");
    let err = running
        .client
        .batch_execute("INSERT INTO alter_check_restart VALUES (2, -1)")
        .await
        .expect_err("CHECK still enforced after restart");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23514");
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_table_drop_check_constraint_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "alter_drop_check_restart_test").await;
    running
        .client
        .batch_execute("CREATE TABLE drop_check_restart (id INT, qty INT)")
        .await
        .expect("create");
    running
        .client
        .batch_execute("ALTER TABLE drop_check_restart ADD CONSTRAINT qty_pos CHECK (qty > 0)")
        .await
        .expect("add check");
    running
        .client
        .batch_execute("ALTER TABLE drop_check_restart DROP CONSTRAINT qty_pos")
        .await
        .expect("drop check");
    shutdown(running).await;

    // After restart the dropped CHECK stays gone: a violating row lands.
    let running = start_persistent_server(data_dir.path(), "alter_drop_check_restart_test").await;
    running
        .client
        .batch_execute("INSERT INTO drop_check_restart VALUES (1, -7)")
        .await
        .expect("dropped CHECK stays gone after restart");
    let rows = running
        .client
        .query("SELECT qty FROM drop_check_restart", &[])
        .await
        .expect("select");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), -7);
    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_table_drop_unique_constraint_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "alter_drop_unique_restart_test").await;
    running
        .client
        .batch_execute("CREATE TABLE drop_unique_restart (id INT, code INT)")
        .await
        .expect("create");
    running
        .client
        .batch_execute("ALTER TABLE drop_unique_restart ADD CONSTRAINT uq_code UNIQUE (code)")
        .await
        .expect("add unique");
    running
        .client
        .batch_execute("ALTER TABLE drop_unique_restart DROP CONSTRAINT uq_code")
        .await
        .expect("drop unique");
    shutdown(running).await;

    // After restart the unique enforcement is gone: duplicates land.
    let running = start_persistent_server(data_dir.path(), "alter_drop_unique_restart_test").await;
    running
        .client
        .batch_execute("INSERT INTO drop_unique_restart VALUES (1, 7), (2, 7)")
        .await
        .expect("dropped UNIQUE stays gone after restart");
    let rows = running
        .client
        .query("SELECT code FROM drop_unique_restart", &[])
        .await
        .expect("select");
    assert_eq!(rows.len(), 2);
    shutdown(running).await;
}

/// The load-bearing persistence proof: apply `SET NOT NULL` and
/// `SET DEFAULT`, restart, and confirm BOTH the NOT-NULL flag and the
/// default reloaded — i.e. the column-metadata mutation persisted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_column_set_not_null_and_default_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "alter_column_meta_restart").await;
    running
        .client
        .batch_execute("CREATE TABLE meta_restart (id INT, v INT)")
        .await
        .expect("create");
    running
        .client
        .batch_execute("INSERT INTO meta_restart (id, v) VALUES (1, 100)")
        .await
        .expect("seed a non-null row");
    running
        .client
        .batch_execute("ALTER TABLE meta_restart ALTER COLUMN v SET NOT NULL")
        .await
        .expect("SET NOT NULL");
    running
        .client
        .batch_execute("ALTER TABLE meta_restart ALTER COLUMN v SET DEFAULT 42")
        .await
        .expect("SET DEFAULT 42");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "alter_column_meta_restart").await;

    // NOT NULL survived: a NULL insert is still rejected 23502.
    let err = running
        .client
        .batch_execute("INSERT INTO meta_restart (id, v) VALUES (2, NULL)")
        .await
        .expect_err("NOT NULL must persist across restart");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23502");

    // DEFAULT survived: an insert omitting v uses the persisted default.
    running
        .client
        .batch_execute("INSERT INTO meta_restart (id) VALUES (3)")
        .await
        .expect("default applies after restart");
    let row = running
        .client
        .query("SELECT v FROM meta_restart WHERE id = 3", &[])
        .await
        .expect("select defaulted row");
    assert_eq!(row[0].get::<_, Option<i32>>(0), Some(42));

    // The pre-existing row is untouched.
    let row = running
        .client
        .query("SELECT v FROM meta_restart WHERE id = 1", &[])
        .await
        .expect("select seed row");
    assert_eq!(row[0].get::<_, Option<i32>>(0), Some(100));

    shutdown(running).await;
}

/// `DROP NOT NULL` persists across restart: NULLs remain allowed after
/// reopening the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_column_drop_not_null_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "alter_drop_nn_restart").await;
    running
        .client
        .batch_execute("CREATE TABLE drop_nn_restart (id INT, v INT NOT NULL)")
        .await
        .expect("create with NOT NULL column");
    running
        .client
        .batch_execute("ALTER TABLE drop_nn_restart ALTER COLUMN v DROP NOT NULL")
        .await
        .expect("DROP NOT NULL");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "alter_drop_nn_restart").await;
    // The column stays nullable after restart: a NULL insert succeeds.
    running
        .client
        .batch_execute("INSERT INTO drop_nn_restart (id, v) VALUES (1, NULL)")
        .await
        .expect("DROP NOT NULL must persist across restart");
    let rows = running
        .client
        .query("SELECT id FROM drop_nn_restart WHERE v IS NULL", &[])
        .await
        .expect("select null rows");
    assert_eq!(rows.len(), 1);
    shutdown(running).await;
}
