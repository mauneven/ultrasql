//! Wire-level schema DDL, namespace catalog, and owner dependency coverage.

mod support;

use support::{shutdown, start_persistent_server, start_sample_server};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_drop_schema_survives_restart_and_blocks_owner_drop() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "schema_ddl_setup").await;
    running
        .client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE schema_owner LOGIN; \
             SET ROLE schema_owner; \
             CREATE SCHEMA app; \
             CREATE SCHEMA IF NOT EXISTS app; \
             RESET ROLE",
        )
        .await
        .expect("create owned schema");

    let owner = running
        .client
        .query_one(
            "SELECT r.rolname \
             FROM pg_catalog.pg_namespace n \
             JOIN pg_catalog.pg_roles r ON r.oid = n.nspowner \
             WHERE n.nspname = 'app'",
            &[],
        )
        .await
        .expect("query schema owner before restart")
        .get::<_, String>(0);
    assert_eq!(owner, "schema_owner");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "schema_ddl_verify").await;
    let owner = running
        .client
        .query_one(
            "SELECT r.rolname \
             FROM pg_catalog.pg_namespace n \
             JOIN pg_catalog.pg_roles r ON r.oid = n.nspowner \
             WHERE n.nspname = 'app'",
            &[],
        )
        .await
        .expect("query schema owner after restart")
        .get::<_, String>(0);
    assert_eq!(owner, "schema_owner");

    let restricted = running
        .client
        .batch_execute("DROP ROLE schema_owner")
        .await
        .expect_err("owned schema must block DROP ROLE");
    assert_eq!(restricted.code().expect("SQLSTATE").code(), "2BP01");

    running
        .client
        .batch_execute("DROP SCHEMA app RESTRICT; DROP ROLE schema_owner")
        .await
        .expect("drop schema and then role");
    let rows = running
        .client
        .query(
            "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = 'app'",
            &[],
        )
        .await
        .expect("query dropped schema count");
    assert!(rows.is_empty());
    shutdown(running).await;
}

#[tokio::test]
async fn drop_schema_if_exists_tolerates_missing_schema() {
    let running = start_sample_server("schema_ddl_missing").await;

    running
        .client
        .batch_execute("DROP SCHEMA IF EXISTS missing_schema")
        .await
        .expect("missing DROP SCHEMA IF EXISTS is no-op");

    shutdown(running).await;
}
