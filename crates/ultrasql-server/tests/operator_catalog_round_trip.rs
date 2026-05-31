mod support;

use support::{shutdown, start_persistent_server};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_operator_catalog_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "operator_restart_setup").await;
    running
        .client
        .batch_execute(
            "CREATE OPERATOR === (\
             LEFTARG = boolean, \
             RIGHTARG = boolean, \
             PROCEDURE = bool_eq)",
        )
        .await
        .expect("create operator");
    let before = running
        .client
        .query_one(
            "SELECT COUNT(*) FROM pg_operator WHERE oprname = '==='",
            &[],
        )
        .await
        .expect("operator visible before restart")
        .get::<_, i64>(0);
    assert_eq!(before, 1);
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "operator_restart_verify").await;
    let after = running
        .client
        .query_one(
            "SELECT COUNT(*) FROM pg_operator WHERE oprname = '==='",
            &[],
        )
        .await
        .expect("operator catalog query after restart")
        .get::<_, i64>(0);
    assert_eq!(after, 1, "operator catalog row should survive restart");
    shutdown(running).await;
}
