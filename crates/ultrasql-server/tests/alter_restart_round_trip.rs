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
