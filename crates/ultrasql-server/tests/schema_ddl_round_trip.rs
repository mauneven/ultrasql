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

#[tokio::test]
async fn qualified_object_ddl_requires_existing_schema() {
    let running = start_sample_server("schema_ddl_unknown_namespace").await;

    let statements = [
        "CREATE TABLE missing_schema.t (id INT)",
        "CREATE SEQUENCE missing_schema.s",
        "CREATE TYPE missing_schema.mood AS ENUM ('ok')",
        "CREATE DOMAIN missing_schema.positive_int AS INT CHECK (VALUE > 0)",
        "CREATE MATERIALIZED VIEW missing_schema.mv AS SELECT id, name FROM users",
    ];
    for sql in statements {
        let err = running
            .client
            .batch_execute(sql)
            .await
            .expect_err("missing schema should reject qualified DDL");
        assert_eq!(err.code().expect("SQLSTATE").code(), "3F000", "{sql}");
    }

    shutdown(running).await;
}

#[tokio::test]
async fn qualified_object_ddl_uses_created_schema_namespace() {
    let running = start_sample_server("schema_ddl_object_namespace").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE app.events (id INT); \
             CREATE SEQUENCE app.event_seq; \
             CREATE TYPE app.mood AS ENUM ('ok'); \
             CREATE DOMAIN app.positive_int AS INT CHECK (VALUE > 0); \
             CREATE MATERIALIZED VIEW app.user_names AS SELECT id, name FROM users",
        )
        .await
        .expect("qualified object DDL in existing schema");

    let rels = running
        .client
        .query_one(
            "SELECT COUNT(*) \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'app' \
               AND c.relname IN ('events', 'user_names')",
            &[],
        )
        .await
        .expect("query relation namespaces")
        .get::<_, i64>(0);
    assert_eq!(rels, 2);

    let types = running
        .client
        .query_one(
            "SELECT COUNT(*) \
             FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
             WHERE n.nspname = 'app' \
               AND t.typname IN ('mood', 'positive_int')",
            &[],
        )
        .await
        .expect("query type namespaces")
        .get::<_, i64>(0);
    assert_eq!(types, 2);

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qualified_sequence_schema_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "schema_sequence_setup").await;
    running
        .client
        .batch_execute("CREATE SCHEMA app; CREATE SEQUENCE app.event_seq")
        .await
        .expect("create qualified sequence");

    let before = running
        .client
        .query_one(
            "SELECT schemaname FROM pg_catalog.pg_sequences WHERE sequencename = 'event_seq'",
            &[],
        )
        .await
        .expect("query sequence schema before restart")
        .get::<_, String>(0);
    assert_eq!(before, "app");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "schema_sequence_verify").await;
    let after = running
        .client
        .query_one(
            "SELECT schemaname FROM pg_catalog.pg_sequences WHERE sequencename = 'event_seq'",
            &[],
        )
        .await
        .expect("query sequence schema after restart")
        .get::<_, String>(0);
    assert_eq!(after, "app");

    let restricted = running
        .client
        .batch_execute("DROP SCHEMA app RESTRICT")
        .await
        .expect_err("sequence dependency must block DROP SCHEMA RESTRICT");
    assert_eq!(restricted.code().expect("SQLSTATE").code(), "2BP01");

    running
        .client
        .batch_execute("DROP SEQUENCE event_seq; DROP SCHEMA app RESTRICT")
        .await
        .expect("drop sequence then schema");

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qualified_relation_and_type_schemas_survive_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "schema_object_restart_setup").await;
    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE app.events (id INT); \
             CREATE MATERIALIZED VIEW app.user_names AS SELECT id, name FROM users; \
             CREATE TYPE app.mood AS ENUM ('ok'); \
             CREATE DOMAIN app.positive_int AS INT CHECK (VALUE > 0)",
        )
        .await
        .expect("create qualified relation and type objects");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "schema_object_restart_verify").await;
    let rels = running
        .client
        .query_one(
            "SELECT COUNT(*) \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'app' \
               AND c.relname IN ('events', 'user_names')",
            &[],
        )
        .await
        .expect("query relation namespaces after restart")
        .get::<_, i64>(0);
    assert_eq!(rels, 2);

    let types = running
        .client
        .query_one(
            "SELECT COUNT(*) \
             FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
             WHERE n.nspname = 'app' \
               AND t.typname IN ('mood', 'positive_int')",
            &[],
        )
        .await
        .expect("query type namespaces after restart")
        .get::<_, i64>(0);
    assert_eq!(types, 2);

    shutdown(running).await;
}
