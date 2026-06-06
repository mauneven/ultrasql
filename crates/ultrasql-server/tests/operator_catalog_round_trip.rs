pub mod support;

use ultrasql_server::Server;

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

#[test]
fn operator_catalog_rejects_unknown_runtime_procedure_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_operator_runtime.meta"),
        "# ultrasql operator runtime v1\noperator\t90000\tpg_catalog\t===\tbool\tbool\tevil\tbool\n",
    )
    .expect("write tampered operator metadata");

    let err = Server::init(data_dir.path()).expect_err("tampered operator metadata rejected");
    assert!(
        err.to_string().contains("operator metadata"),
        "expected operator metadata rejection, got {err}"
    );
}

#[test]
fn operator_catalog_rejects_duplicate_runtime_signature_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_operator_runtime.meta"),
        concat!(
            "# ultrasql operator runtime v1\n",
            "operator\t90000\tpg_catalog\t===\tbool\tbool\tbool_eq\tbool\n",
            "operator\t90001\tpg_catalog\t===\tbool\tbool\tbool_eq\tbool\n"
        ),
    )
    .expect("write duplicate operator metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate operator metadata rejected");
    assert!(
        err.to_string().contains("duplicate operator metadata"),
        "expected duplicate metadata rejection, got {err}"
    );
}
