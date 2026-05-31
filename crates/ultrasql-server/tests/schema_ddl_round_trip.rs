//! Wire-level schema DDL, namespace catalog, and owner dependency coverage.

mod support;

use support::{connect_as, shutdown, start_persistent_server, start_sample_server};

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
async fn non_owner_cannot_drop_schema() {
    let running = start_sample_server("schema_ddl_owner_guard").await;

    running
        .client
        .batch_execute(
            "CREATE ROLE schema_owner LOGIN; \
             CREATE ROLE schema_attacker LOGIN",
        )
        .await
        .expect("create schema test roles");

    let (owner, owner_conn) = connect_as(running.bound, "schema_owner", "schema_drop_owner").await;
    owner
        .batch_execute("CREATE SCHEMA private_app")
        .await
        .expect("owner creates schema");
    drop(owner);
    owner_conn.await.expect("owner connection joins");

    let (attacker, attacker_conn) =
        connect_as(running.bound, "schema_attacker", "schema_drop_attacker").await;
    let err = attacker
        .batch_execute("DROP SCHEMA private_app")
        .await
        .expect_err("non-owner cannot drop schema");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42501");
    drop(attacker);
    attacker_conn.await.expect("attacker connection joins");

    let exists = running
        .client
        .query_one(
            "SELECT COUNT(*) FROM pg_catalog.pg_namespace WHERE nspname = 'private_app'",
            &[],
        )
        .await
        .expect("query schema after rejected drop")
        .get::<_, i64>(0);
    assert_eq!(exists, 1);

    let (owner, owner_conn) =
        connect_as(running.bound, "schema_owner", "schema_drop_owner_cleanup").await;
    owner
        .batch_execute("DROP SCHEMA private_app")
        .await
        .expect("owner can drop schema");
    drop(owner);
    owner_conn.await.expect("owner cleanup connection joins");

    running
        .client
        .batch_execute("DROP ROLE schema_owner; DROP ROLE schema_attacker")
        .await
        .expect("drop schema test roles");

    shutdown(running).await;
}

#[tokio::test]
async fn schema_create_privilege_gates_qualified_object_ddl() {
    let running = start_sample_server("schema_create_privilege_guard").await;

    running
        .client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE schema_owner LOGIN; \
             CREATE ROLE schema_writer LOGIN; \
             CREATE ROLE schema_intruder LOGIN; \
             SET ROLE schema_owner; \
             CREATE SCHEMA private_ddl; \
             RESET ROLE",
        )
        .await
        .expect("create private schema and roles");

    let (intruder, intruder_conn) =
        connect_as(running.bound, "schema_intruder", "schema_create_intruder").await;
    let err = intruder
        .batch_execute("CREATE TABLE private_ddl.stolen (id INT)")
        .await
        .expect_err("non-owner without CREATE cannot create in schema");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42501");
    drop(intruder);
    intruder_conn.await.expect("intruder connection joins");

    running
        .client
        .batch_execute("GRANT CREATE ON SCHEMA private_ddl TO schema_writer")
        .await
        .expect("grant create on schema");

    let (writer, writer_conn) =
        connect_as(running.bound, "schema_writer", "schema_create_writer").await;
    writer
        .batch_execute(
            "CREATE TABLE private_ddl.allowed (id INT); \
             CREATE SEQUENCE private_ddl.allowed_seq",
        )
        .await
        .expect("granted role can create qualified objects");
    drop(writer);
    writer_conn.await.expect("writer connection joins");

    running
        .client
        .batch_execute(
            "DROP TABLE private_ddl.allowed; \
             DROP SEQUENCE private_ddl.allowed_seq; \
             DROP SCHEMA private_ddl; \
             DROP ROLE schema_owner; \
             DROP ROLE schema_writer; \
             DROP ROLE schema_intruder",
        )
        .await
        .expect("cleanup schema create privilege test");

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

#[tokio::test]
async fn select_respects_schema_qualifier() {
    let running = start_sample_server("schema_select_qualifier_guard").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE guarded_select (id INT); \
             INSERT INTO guarded_select VALUES (9)",
        )
        .await
        .expect("create public table and separate schema");

    running
        .client
        .query("SELECT id FROM app.guarded_select", &[])
        .await
        .expect_err("qualified SELECT must not resolve public table");

    let row = running
        .client
        .query_one("SELECT id FROM guarded_select", &[])
        .await
        .expect("public table remains readable")
        .get::<_, i32>(0);
    assert_eq!(row, 9);

    running
        .client
        .batch_execute("DROP TABLE guarded_select; DROP SCHEMA app")
        .await
        .expect("cleanup select qualifier guard");

    shutdown(running).await;
}

#[tokio::test]
async fn dml_respects_schema_qualifier() {
    let running = start_sample_server("schema_dml_qualifier_guard").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE guarded_dml (id INT); \
             INSERT INTO guarded_dml VALUES (1)",
        )
        .await
        .expect("create public table and separate schema");

    for sql in [
        "INSERT INTO app.guarded_dml VALUES (2)",
        "UPDATE app.guarded_dml SET id = 3",
        "DELETE FROM app.guarded_dml",
    ] {
        running
            .client
            .batch_execute(sql)
            .await
            .expect_err("qualified DML must not resolve public table");
    }

    let row = running
        .client
        .query_one("SELECT id FROM guarded_dml", &[])
        .await
        .expect("public table survives wrong-qualified DML")
        .get::<_, i32>(0);
    assert_eq!(row, 1);

    running
        .client
        .batch_execute("DROP TABLE guarded_dml; DROP SCHEMA app")
        .await
        .expect("cleanup DML qualifier guard");

    shutdown(running).await;
}

#[tokio::test]
async fn truncate_respects_schema_qualifier() {
    let running = start_sample_server("schema_truncate_qualifier_guard").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE guarded_truncate (id INT); \
             INSERT INTO guarded_truncate VALUES (5)",
        )
        .await
        .expect("create public table and separate schema");

    running
        .client
        .batch_execute("TRUNCATE app.guarded_truncate")
        .await
        .expect_err("qualified TRUNCATE must not resolve public table");

    let row = running
        .client
        .query_one("SELECT id FROM guarded_truncate", &[])
        .await
        .expect("public table survives wrong-qualified TRUNCATE")
        .get::<_, i32>(0);
    assert_eq!(row, 5);

    running
        .client
        .batch_execute("DROP TABLE guarded_truncate; DROP SCHEMA app")
        .await
        .expect("cleanup TRUNCATE qualifier guard");

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

#[tokio::test]
async fn drop_schema_cascade_removes_qualified_sequences() {
    let running = start_sample_server("schema_sequence_cascade").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE SEQUENCE app.event_seq START WITH 4; \
             GRANT USAGE ON SEQUENCE event_seq TO PUBLIC; \
             DROP SCHEMA app CASCADE",
        )
        .await
        .expect("drop schema cascade removes sequence dependency");

    let schema_count = running
        .client
        .query_one(
            "SELECT COUNT(*) FROM pg_catalog.pg_namespace WHERE nspname = 'app'",
            &[],
        )
        .await
        .expect("query dropped schema")
        .get::<_, i64>(0);
    assert_eq!(schema_count, 0);

    let sequence_count = running
        .client
        .query_one(
            "SELECT COUNT(*) FROM pg_catalog.pg_sequences WHERE sequencename = 'event_seq'",
            &[],
        )
        .await
        .expect("query dropped sequence")
        .get::<_, i64>(0);
    assert_eq!(sequence_count, 0);

    running
        .client
        .simple_query("SELECT nextval('event_seq')")
        .await
        .expect_err("schema cascade must remove sequence state");

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
