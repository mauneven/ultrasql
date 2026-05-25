//! Admin validation round-trip tests.

mod support;

use ultrasql_server::Server;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validate_persistent_server_after_create_table_skips_internal_catalog_rows() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let running = support::start_persistent_server(data_dir.path(), "validation-round-trip").await;

    running
        .client
        .batch_execute(
            "CREATE TABLE validation_user (id INT, payload TEXT);
             INSERT INTO validation_user VALUES (1, 'alpha');",
        )
        .await
        .expect("create user table");
    support::shutdown(running).await;

    let server = Server::init(data_dir.path()).expect("restart persistent server");
    let report = server.validate();
    assert!(report.is_ok(), "{report:?}");
}
