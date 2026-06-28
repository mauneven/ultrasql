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

/// `DROP COLUMN` must persist its dependent-index adjustments: bootstrap
/// rebuilds each surviving index at its shifted key position, and an index on
/// the dropped column stays gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_table_drop_column_index_adjustments_survive_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "drop_column_index_restart").await;
    running
        .client
        .batch_execute("CREATE TABLE dci (a INT, b INT, c INT)")
        .await
        .expect("create");
    running
        .client
        .batch_execute("CREATE INDEX idx_c ON dci (c)")
        .await
        .expect("index on c (attnum 2)");
    running
        .client
        .batch_execute("CREATE INDEX idx_a ON dci (a)")
        .await
        .expect("index on a (the column to drop)");
    running
        .client
        .batch_execute("INSERT INTO dci VALUES (1, 20, 300), (4, 50, 600)")
        .await
        .expect("seed");
    running
        .client
        .batch_execute("ALTER TABLE dci DROP COLUMN a")
        .await
        .expect("drop column a");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "drop_column_index_restart").await;
    // idx_c was [2]; after the drop + restart it must resolve to the shifted
    // position so the probe still hits c.
    let rows = running
        .client
        .query("SELECT b, c FROM dci WHERE c = 600", &[])
        .await
        .expect("probe shifted idx_c after restart");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 50);
    assert_eq!(rows[0].get::<_, i32>(1), 600);
    // idx_a (on the dropped column) stays dropped: its name is reusable.
    running
        .client
        .batch_execute("CREATE INDEX idx_a ON dci (b)")
        .await
        .expect("dropped idx_a does not resurrect after restart");
    shutdown(running).await;
}

/// BLAST RADIUS: before this fix, UNIQUE enforcement was silently lost after
/// `DROP COLUMN` (the heap rewrite re-stamped every surviving row as a new
/// tuple version without repopulating the unique index), so a duplicate slipped
/// into the heap — and the NEXT restart's index rebuild aborted on it
/// (`server init failed: ... duplicate key in index`), leaving the server
/// unable to boot. With enforcement restored the duplicate is rejected up
/// front, no duplicate ever lands, the restart boots cleanly, and UNIQUE is
/// still enforced. Matches PostgreSQL 14.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_table_drop_column_keeps_unique_enforced_across_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "drop_column_unique_restart").await;
    running
        .client
        .batch_execute("CREATE TABLE dcu (a INT, b INT UNIQUE)")
        .await
        .expect("create with UNIQUE b");
    running
        .client
        .batch_execute("INSERT INTO dcu VALUES (1, 10)")
        .await
        .expect("seed");
    running
        .client
        .batch_execute("ALTER TABLE dcu DROP COLUMN a")
        .await
        .expect("drop column a");

    // Enforcement resumed immediately: the duplicate is rejected, so no
    // duplicate row can reach the heap to poison the restart rebuild.
    let err = running
        .client
        .batch_execute("INSERT INTO dcu VALUES (10)")
        .await
        .expect_err("duplicate b=10 must be rejected after DROP COLUMN");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23505");
    running
        .client
        .batch_execute("INSERT INTO dcu VALUES (11)")
        .await
        .expect("distinct b inserts through the repopulated unique index");
    shutdown(running).await;

    // The server boots (the rebuild finds no duplicate) — `start_persistent_server`
    // calls `Server::init`, which would PANIC here on the pre-fix duplicate-key
    // rebuild abort — and UNIQUE is still enforced after restart.
    let running = start_persistent_server(data_dir.path(), "drop_column_unique_restart").await;
    let rows = running
        .client
        .query("SELECT b FROM dcu ORDER BY b", &[])
        .await
        .expect("select after restart");
    let bs: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(bs, vec![10, 11]);
    let err = running
        .client
        .batch_execute("INSERT INTO dcu VALUES (10)")
        .await
        .expect_err("UNIQUE still enforced after restart");
    assert_eq!(err.code().expect("SQLSTATE").code(), "23505");
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
