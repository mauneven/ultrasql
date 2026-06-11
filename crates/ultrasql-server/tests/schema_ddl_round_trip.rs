//! Wire-level schema DDL, namespace catalog, and owner dependency coverage.

pub mod support;

use support::{connect_as, shutdown, start_persistent_server, start_sample_server};
use ultrasql_server::Server;

async fn simple_i64(client: &tokio_postgres::Client, sql: &str) -> i64 {
    let rows = client.simple_query(sql).await.expect("simple query");
    rows.iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse::<i64>().ok(),
            _ => None,
        })
        .expect("one int8 row")
}

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

#[test]
fn schema_metadata_rejects_unknown_owner_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_schema_runtime.meta"),
        concat!(
            "# ultrasql schemas v1\n",
            "schema\torphaned_schema\tmissing_owner\n"
        ),
    )
    .expect("write orphaned schema metadata");

    let err = Server::init(data_dir.path()).expect_err("orphaned schema owner rejected");
    assert!(
        err.to_string()
            .contains("unknown schema metadata owner 'missing_owner'"),
        "expected unknown schema owner rejection, got {err}"
    );
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
async fn same_relation_name_is_isolated_by_schema() {
    let running = start_sample_server("schema_same_relation_name").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE same_name (id INT); \
             CREATE TABLE app.same_name (id INT); \
             INSERT INTO same_name VALUES (1); \
             INSERT INTO app.same_name VALUES (2)",
        )
        .await
        .expect("schemas may contain tables with the same relation name");

    let public_row = running
        .client
        .query_one("SELECT id FROM public.same_name", &[])
        .await
        .expect("public relation resolves by qualified name")
        .get::<_, i32>(0);
    let app_row = running
        .client
        .query_one("SELECT id FROM app.same_name", &[])
        .await
        .expect("app relation resolves by qualified name")
        .get::<_, i32>(0);
    assert_eq!(public_row, 1);
    assert_eq!(app_row, 2);

    running
        .client
        .batch_execute("SET search_path TO app, public")
        .await
        .expect("set app-first search path");
    let app_first = running
        .client
        .query_one("SELECT id FROM same_name", &[])
        .await
        .expect("search path resolves app relation first")
        .get::<_, i32>(0);
    assert_eq!(app_first, 2);

    running
        .client
        .batch_execute("RESET search_path")
        .await
        .expect("reset search path");
    let public_default = running
        .client
        .query_one("SELECT id FROM same_name", &[])
        .await
        .expect("default search path resolves public relation")
        .get::<_, i32>(0);
    assert_eq!(public_default, 1);

    running
        .client
        .batch_execute("DROP TABLE app.same_name; DROP TABLE same_name; DROP SCHEMA app")
        .await
        .expect("cleanup same-name schema isolation");

    shutdown(running).await;
}

#[tokio::test]
async fn relation_keys_distinguish_schema_dot_from_table_dot() {
    let running = start_sample_server("schema_dotted_relation_key").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE app.\"events.log\" (id INT); \
             CREATE SCHEMA \"app.events\"; \
             CREATE TABLE \"app.events\".log (id INT); \
             INSERT INTO app.\"events.log\" VALUES (1); \
             INSERT INTO \"app.events\".log VALUES (2)",
        )
        .await
        .expect("dotted schema and dotted table names do not collide");

    let dotted_table = running
        .client
        .query_one("SELECT id FROM app.\"events.log\"", &[])
        .await
        .expect("select dotted table name")
        .get::<_, i32>(0);
    let dotted_schema = running
        .client
        .query_one("SELECT id FROM \"app.events\".log", &[])
        .await
        .expect("select dotted schema name")
        .get::<_, i32>(0);
    assert_eq!(dotted_table, 1);
    assert_eq!(dotted_schema, 2);

    shutdown(running).await;
}

#[tokio::test]
async fn same_type_name_is_isolated_by_schema() {
    let running = start_sample_server("schema_same_type_name").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TYPE mood AS ENUM ('ok'); \
             CREATE TYPE app.mood AS ENUM ('warm'); \
             CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0); \
             CREATE DOMAIN app.positive_int AS INT CHECK (VALUE > 10); \
             CREATE TYPE address AS (zip INT); \
             CREATE TYPE app.address AS (zip TEXT); \
             CREATE TABLE public_mood (v public.mood); \
             CREATE TABLE app.app_mood (v app.mood); \
             CREATE TABLE public_domain (v public.positive_int); \
             CREATE TABLE app.app_domain (v app.positive_int); \
             CREATE TABLE public_address (v public.address); \
             CREATE TABLE app.app_address (v app.address); \
             INSERT INTO public_mood VALUES ('ok'); \
             INSERT INTO app.app_mood VALUES ('warm'); \
             INSERT INTO public_domain VALUES (5); \
             INSERT INTO app.app_domain VALUES (11)",
        )
        .await
        .expect("schemas may contain user-defined types with the same name");

    running
        .client
        .batch_execute("INSERT INTO public_mood VALUES ('warm')")
        .await
        .expect_err("public enum must reject app-only label");
    running
        .client
        .batch_execute("INSERT INTO app.app_mood VALUES ('ok')")
        .await
        .expect_err("app enum must reject public-only label");
    running
        .client
        .batch_execute("INSERT INTO app.app_domain VALUES (5)")
        .await
        .expect_err("app domain must enforce app-only constraint");

    running
        .client
        .batch_execute("SET search_path TO app, public")
        .await
        .expect("set app-first search path");
    running
        .client
        .batch_execute(
            "CREATE TABLE app.path_mood (v mood); \
             CREATE TABLE app.path_domain (v positive_int); \
             CREATE TABLE app.path_address (v address); \
             INSERT INTO app.path_mood VALUES ('warm'); \
             INSERT INTO app.path_domain VALUES (11)",
        )
        .await
        .expect("search path resolves app user-defined types first");
    running
        .client
        .batch_execute("INSERT INTO app.path_mood VALUES ('ok')")
        .await
        .expect_err("app enum must reject public-only label");
    running
        .client
        .batch_execute("INSERT INTO app.path_domain VALUES (5)")
        .await
        .expect_err("app search-path domain must enforce app-only constraint");

    running
        .client
        .batch_execute("RESET search_path")
        .await
        .expect("reset search path");
    running
        .client
        .batch_execute(
            "CREATE TABLE public_path_mood (v mood); \
             CREATE TABLE public_path_domain (v positive_int); \
             CREATE TABLE public_path_address (v address); \
             INSERT INTO public_path_mood VALUES ('ok'); \
             INSERT INTO public_path_domain VALUES (5)",
        )
        .await
        .expect("default search path resolves public user-defined types");
    running
        .client
        .batch_execute("INSERT INTO public_path_mood VALUES ('warm')")
        .await
        .expect_err("public enum must reject app-only label");
    running
        .client
        .batch_execute("INSERT INTO public_path_domain VALUES (-1)")
        .await
        .expect_err("public search-path domain must enforce public constraint");

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_index_name_is_isolated_by_schema() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let running = start_persistent_server(data_dir.path(), "schema_same_index_name_setup").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE indexed_public (id INT); \
             CREATE TABLE app.indexed_app (id INT); \
             CREATE INDEX same_idx ON indexed_public(id); \
             CREATE INDEX same_idx ON app.indexed_app(id)",
        )
        .await
        .expect("schemas may contain indexes with the same name");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "schema_same_index_name_verify").await;

    let rows = running
        .client
        .query(
            "SELECT schemaname, tablename \
             FROM pg_catalog.pg_indexes \
             WHERE indexname = 'same_idx' \
             ORDER BY schemaname, tablename",
            &[],
        )
        .await
        .expect("query schema-isolated indexes");
    let names = rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            ("app".to_owned(), "indexed_app".to_owned()),
            ("public".to_owned(), "indexed_public".to_owned()),
        ],
        "same index name must be visible in both schemas"
    );

    running
        .client
        .batch_execute("DROP INDEX app.same_idx")
        .await
        .expect("qualified DROP INDEX drops only the app index");

    let rows = running
        .client
        .query(
            "SELECT schemaname, tablename \
             FROM pg_catalog.pg_indexes \
             WHERE indexname = 'same_idx' \
             ORDER BY schemaname, tablename",
            &[],
        )
        .await
        .expect("query indexes after qualified drop");
    let names = rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![("public".to_owned(), "indexed_public".to_owned())],
        "public index must survive qualified drop of app index"
    );

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_sequence_name_is_isolated_by_schema() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let running = start_persistent_server(data_dir.path(), "schema_same_sequence_name_setup").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE SEQUENCE same_seq START WITH 10; \
             CREATE SEQUENCE app.same_seq START WITH 50",
        )
        .await
        .expect("schemas may contain sequences with the same name");
    assert_eq!(
        simple_i64(&running.client, "SELECT nextval('same_seq')").await,
        10
    );
    assert_eq!(
        simple_i64(&running.client, "SELECT nextval('app.same_seq')").await,
        50
    );
    running
        .client
        .batch_execute(
            "CREATE ROLE same_seq_reader LOGIN; \
             GRANT USAGE ON SCHEMA app TO same_seq_reader; \
             GRANT USAGE, UPDATE ON SEQUENCE app.same_seq TO same_seq_reader",
        )
        .await
        .expect("grant schema-qualified sequence privileges");
    let (reader, reader_conn) = connect_as(
        running.bound,
        "same_seq_reader",
        "schema_same_sequence_reader",
    )
    .await;
    assert_eq!(
        simple_i64(&reader, "SELECT nextval('app.same_seq')").await,
        51
    );
    reader
        .simple_query("SELECT nextval('same_seq')")
        .await
        .expect_err("app sequence grant must not leak to public sequence");
    drop(reader);
    reader_conn
        .await
        .expect("same sequence reader connection joins");
    shutdown(running).await;

    let running =
        start_persistent_server(data_dir.path(), "schema_same_sequence_name_verify").await;
    let rows = running
        .client
        .query(
            "SELECT schemaname, sequencename \
             FROM pg_catalog.pg_sequences \
             WHERE sequencename = 'same_seq' \
             ORDER BY schemaname",
            &[],
        )
        .await
        .expect("query schema-isolated sequences after restart");
    let names = rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            ("app".to_owned(), "same_seq".to_owned()),
            ("public".to_owned(), "same_seq".to_owned()),
        ],
        "same sequence name must survive in both schemas"
    );

    running
        .client
        .batch_execute("DROP SEQUENCE app.same_seq")
        .await
        .expect("qualified DROP SEQUENCE drops only the app sequence");
    assert_eq!(
        simple_i64(&running.client, "SELECT nextval('same_seq')").await,
        11
    );

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
async fn unqualified_select_does_not_resolve_non_public_table() {
    let running = start_sample_server("schema_select_default_namespace_guard").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE app.private_select (id INT); \
             INSERT INTO app.private_select VALUES (11)",
        )
        .await
        .expect("create non-public table");

    running
        .client
        .query("SELECT id FROM private_select", &[])
        .await
        .expect_err("unqualified SELECT must not resolve non-public table");

    let row = running
        .client
        .query_one("SELECT id FROM app.private_select", &[])
        .await
        .expect("qualified table remains readable")
        .get::<_, i32>(0);
    assert_eq!(row, 11);

    running
        .client
        .batch_execute("DROP TABLE app.private_select; DROP SCHEMA app")
        .await
        .expect("cleanup default namespace guard");

    shutdown(running).await;
}

#[tokio::test]
async fn unqualified_dml_does_not_resolve_non_public_table() {
    let running = start_sample_server("schema_dml_default_namespace_guard").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE app.private_dml (id INT); \
             INSERT INTO app.private_dml VALUES (1)",
        )
        .await
        .expect("create non-public DML target");

    for sql in [
        "INSERT INTO private_dml VALUES (2)",
        "UPDATE private_dml SET id = 3",
        "DELETE FROM private_dml",
    ] {
        running
            .client
            .batch_execute(sql)
            .await
            .expect_err("unqualified DML must not resolve non-public table");
    }

    let row = running
        .client
        .query_one("SELECT id FROM app.private_dml", &[])
        .await
        .expect("qualified DML target remains readable")
        .get::<_, i32>(0);
    assert_eq!(row, 1);

    running
        .client
        .batch_execute("DROP TABLE app.private_dml; DROP SCHEMA app")
        .await
        .expect("cleanup DML default namespace guard");

    shutdown(running).await;
}

#[tokio::test]
async fn unqualified_table_ddl_does_not_resolve_non_public_table() {
    let running = start_sample_server("schema_table_ddl_default_namespace_guard").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE app.private_truncate (id INT); \
             CREATE TABLE app.private_alter (id INT); \
             CREATE TABLE app.private_index (id INT); \
             INSERT INTO app.private_truncate VALUES (1)",
        )
        .await
        .expect("create non-public DDL targets");

    running
        .client
        .batch_execute("TRUNCATE private_truncate")
        .await
        .expect_err("unqualified TRUNCATE must not resolve non-public table");
    running
        .client
        .batch_execute("ALTER TABLE private_alter ADD COLUMN leaked INT")
        .await
        .expect_err("unqualified ALTER TABLE must not resolve non-public table");
    running
        .client
        .batch_execute("CREATE INDEX private_index_id_idx ON private_index(id)")
        .await
        .expect_err("unqualified CREATE INDEX must not resolve non-public table");

    let row = running
        .client
        .query_one("SELECT id FROM app.private_truncate", &[])
        .await
        .expect("qualified truncate target remains readable")
        .get::<_, i32>(0);
    assert_eq!(row, 1);
    running
        .client
        .query("SELECT leaked FROM app.private_alter", &[])
        .await
        .expect_err("qualified alter target must not gain rejected column");
    let rows = running
        .client
        .query(
            "SELECT indexname \
             FROM pg_catalog.pg_indexes \
             WHERE schemaname = 'app' \
             AND tablename = 'private_index' \
             AND indexname = 'private_index_id_idx'",
            &[],
        )
        .await
        .expect("query rejected non-public index");
    assert!(
        rows.is_empty(),
        "unqualified CREATE INDEX must not create a non-public index"
    );

    running
        .client
        .batch_execute(
            "DROP TABLE app.private_truncate; \
             DROP TABLE app.private_alter; \
             DROP TABLE app.private_index; \
             DROP SCHEMA app",
        )
        .await
        .expect("cleanup table DDL default namespace guard");

    shutdown(running).await;
}

#[tokio::test]
async fn search_path_allows_unqualified_non_public_table_access() {
    let running = start_sample_server("schema_search_path_namespace_guard").await;

    running
        .client
        .batch_execute("CREATE SCHEMA app; CREATE TABLE app.path_target (id INT)")
        .await
        .expect("create search path target");

    running
        .client
        .batch_execute("SET search_path TO app, public")
        .await
        .expect("set search path");
    running
        .client
        .batch_execute("INSERT INTO path_target VALUES (1)")
        .await
        .expect("search path INSERT resolves app table");
    running
        .client
        .batch_execute("UPDATE path_target SET id = 2")
        .await
        .expect("search path UPDATE resolves app table");
    let row = running
        .client
        .query_one("SELECT id FROM path_target", &[])
        .await
        .expect("search path SELECT resolves app table")
        .get::<_, i32>(0);
    assert_eq!(row, 2);

    running
        .client
        .batch_execute("RESET search_path")
        .await
        .expect("reset search path");
    running
        .client
        .query("SELECT id FROM path_target", &[])
        .await
        .expect_err("reset search_path hides non-public table again");

    let row = running
        .client
        .query_one("SELECT id FROM app.path_target", &[])
        .await
        .expect("qualified search path target remains readable")
        .get::<_, i32>(0);
    assert_eq!(row, 2);

    running
        .client
        .batch_execute("DROP TABLE app.path_target; DROP SCHEMA app")
        .await
        .expect("cleanup search path guard");

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

#[tokio::test]
async fn alter_table_respects_schema_qualifier() {
    let running = start_sample_server("schema_alter_qualifier_guard").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE guarded_alter (id INT); \
             INSERT INTO guarded_alter VALUES (6)",
        )
        .await
        .expect("create public table and separate schema");

    running
        .client
        .batch_execute("ALTER TABLE app.guarded_alter ADD COLUMN leaked INT")
        .await
        .expect_err("qualified ALTER TABLE must not resolve public table");

    running
        .client
        .query("SELECT leaked FROM guarded_alter", &[])
        .await
        .expect_err("public table must not gain wrong-qualified column");

    let row = running
        .client
        .query_one("SELECT id FROM guarded_alter", &[])
        .await
        .expect("public table survives wrong-qualified ALTER")
        .get::<_, i32>(0);
    assert_eq!(row, 6);

    running
        .client
        .batch_execute("DROP TABLE guarded_alter; DROP SCHEMA app")
        .await
        .expect("cleanup ALTER qualifier guard");

    shutdown(running).await;
}

#[tokio::test]
async fn create_index_respects_schema_qualifier() {
    let running = start_sample_server("schema_index_qualifier_guard").await;

    running
        .client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE TABLE guarded_index (id INT); \
             INSERT INTO guarded_index VALUES (7)",
        )
        .await
        .expect("create public table and separate schema");

    running
        .client
        .batch_execute("CREATE INDEX guarded_index_id_idx ON app.guarded_index(id)")
        .await
        .expect_err("qualified CREATE INDEX must not resolve public table");

    let rows = running
        .client
        .query(
            "SELECT indexname \
             FROM pg_catalog.pg_indexes \
             WHERE tablename = 'guarded_index' \
             AND indexname = 'guarded_index_id_idx'",
            &[],
        )
        .await
        .expect("pg_indexes query after rejected qualified CREATE INDEX");
    assert!(
        rows.is_empty(),
        "wrong-qualified CREATE INDEX must not create an index on public table"
    );

    running
        .client
        .batch_execute("DROP TABLE guarded_index; DROP SCHEMA app")
        .await
        .expect("cleanup CREATE INDEX qualifier guard");

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
    running
        .client
        .simple_query("SELECT nextval('event_seq')")
        .await
        .expect_err("unqualified sequence lookup must not leak into non-public schema");
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
        .batch_execute("DROP SEQUENCE app.event_seq; DROP SCHEMA app RESTRICT")
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
             GRANT USAGE ON SEQUENCE app.event_seq TO PUBLIC; \
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
