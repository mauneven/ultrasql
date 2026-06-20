use super::*;

#[tokio::test]
async fn grant_schema_rejects_missing_schema() {
    let running = start_sample_server("schema_privilege_missing_guard").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE schema_ghost_reader LOGIN")
        .await
        .expect("create schema ghost roles");

    client
        .batch_execute("GRANT USAGE ON SCHEMA missing_schema TO schema_ghost_reader")
        .await
        .expect_err("schema GRANT must reject missing schemas");

    let visible = client
        .query_one(
            "SELECT has_schema_privilege('schema_ghost_reader', 'missing_schema', 'USAGE')",
            &[],
        )
        .await
        .expect("schema privilege check after rejected grant")
        .get::<_, bool>(0);
    assert!(!visible, "rejected schema GRANT must not persist");

    client
        .batch_execute("DROP ROLE schema_ghost_reader")
        .await
        .expect("cleanup schema ghost roles");

    shutdown(running).await;
}

#[tokio::test]
async fn grant_database_rejects_missing_database() {
    let running = start_sample_server("database_privilege_missing_guard").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN; CREATE ROLE db_ghost_reader LOGIN")
        .await
        .expect("create database ghost role");

    client
        .batch_execute("GRANT CONNECT ON DATABASE missing_db TO db_ghost_reader")
        .await
        .expect_err("database GRANT must reject missing databases");

    let visible = client
        .query_one(
            "SELECT has_database_privilege('db_ghost_reader', 'missing_db', 'CONNECT')",
            &[],
        )
        .await
        .expect("database privilege check after rejected grant")
        .get::<_, bool>(0);
    assert!(!visible, "rejected database GRANT must not persist");

    shutdown(running).await;
}

#[tokio::test]
async fn grant_function_rejects_missing_function() {
    let running = start_sample_server("function_privilege_missing_guard").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN; CREATE ROLE fn_ghost_reader LOGIN")
        .await
        .expect("create function ghost role");

    client
        .batch_execute("GRANT EXECUTE ON FUNCTION missing_function() TO fn_ghost_reader")
        .await
        .expect_err("function GRANT must reject missing functions");

    let visible = client
        .query_one(
            "SELECT has_function_privilege('fn_ghost_reader', 'missing_function()', 'EXECUTE')",
            &[],
        )
        .await
        .expect("function privilege check after rejected grant")
        .get::<_, bool>(0);
    assert!(!visible, "rejected function GRANT must not persist");

    shutdown(running).await;
}

#[tokio::test]
async fn schema_usage_is_required_for_table_access() {
    let running = start_sample_server("schema_usage_gate").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE usage_acl_owner LOGIN; \
             CREATE ROLE usage_acl_reader LOGIN; \
             SET ROLE usage_acl_owner; \
             CREATE SCHEMA usage_acl; \
             CREATE TABLE usage_acl.docs (id INT); \
             INSERT INTO usage_acl.docs VALUES (7); \
             RESET ROLE; \
             GRANT SELECT ON TABLE usage_acl.docs TO usage_acl_reader",
        )
        .await
        .expect("create schema usage test table");

    let (reader, reader_conn) = connect_as(
        running.bound,
        "usage_acl_reader",
        "schema_usage_reader_blocked",
    )
    .await;
    assert_insufficient_privilege(
        reader
            .query("SELECT id FROM usage_acl.docs", &[])
            .await
            .expect_err("schema USAGE required despite table SELECT"),
    );
    drop(reader);
    reader_conn
        .await
        .expect("blocked schema usage reader joins");

    client
        .batch_execute("GRANT USAGE ON SCHEMA usage_acl TO usage_acl_reader")
        .await
        .expect("grant schema usage");

    let (reader, reader_conn) = connect_as(
        running.bound,
        "usage_acl_reader",
        "schema_usage_reader_allowed",
    )
    .await;
    let row = reader
        .query_one("SELECT id FROM usage_acl.docs", &[])
        .await
        .expect("schema USAGE plus table SELECT permits access");
    assert_eq!(row.get::<_, i32>(0), 7);
    drop(reader);
    reader_conn
        .await
        .expect("allowed schema usage reader joins");

    client
        .batch_execute(
            "DROP TABLE usage_acl.docs; \
             DROP SCHEMA usage_acl; \
             DROP ROLE usage_acl_owner; \
             DROP ROLE usage_acl_reader",
        )
        .await
        .expect("cleanup schema usage test");

    shutdown(running).await;
}

#[tokio::test]
async fn schema_usage_is_required_for_quoted_dotted_schema_access() {
    let running = start_sample_server("schema_usage_dotted_gate").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE dotted_usage_owner LOGIN; \
             CREATE ROLE dotted_usage_reader LOGIN; \
             SET ROLE dotted_usage_owner; \
             CREATE SCHEMA \"usage.dot\"; \
             CREATE TABLE \"usage.dot\".docs (id INT); \
             INSERT INTO \"usage.dot\".docs VALUES (9); \
             RESET ROLE; \
             GRANT SELECT ON TABLE \"usage.dot\".docs TO dotted_usage_reader",
        )
        .await
        .expect("create dotted schema usage test table");

    let (reader, reader_conn) = connect_as(
        running.bound,
        "dotted_usage_reader",
        "schema_usage_dotted_reader_blocked",
    )
    .await;
    assert_insufficient_privilege(
        reader
            .query("SELECT id FROM \"usage.dot\".docs", &[])
            .await
            .expect_err("quoted dotted schema USAGE required despite table SELECT"),
    );
    drop(reader);
    reader_conn
        .await
        .expect("blocked dotted schema usage reader joins");

    client
        .batch_execute("GRANT USAGE ON SCHEMA \"usage.dot\" TO dotted_usage_reader")
        .await
        .expect("grant dotted schema usage");

    let (reader, reader_conn) = connect_as(
        running.bound,
        "dotted_usage_reader",
        "schema_usage_dotted_reader_allowed",
    )
    .await;
    let row = reader
        .query_one("SELECT id FROM \"usage.dot\".docs", &[])
        .await
        .expect("quoted dotted schema USAGE plus table SELECT permits access");
    assert_eq!(row.get::<_, i32>(0), 9);
    drop(reader);
    reader_conn
        .await
        .expect("allowed dotted schema usage reader joins");

    client
        .batch_execute(
            "DROP TABLE \"usage.dot\".docs; \
             DROP SCHEMA \"usage.dot\"; \
             DROP ROLE dotted_usage_owner; \
             DROP ROLE dotted_usage_reader",
        )
        .await
        .expect("cleanup dotted schema usage test");

    shutdown(running).await;
}

#[tokio::test]
async fn grant_table_respects_schema_qualifier() {
    let running = start_sample_server("privilege_table_schema_qualifier_guard").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE qualifier_reader LOGIN; \
             CREATE SCHEMA app; \
             CREATE TABLE grant_qualifier_guard (id INT); \
             CREATE TABLE app.grant_qualifier_guard (id INT)",
        )
        .await
        .expect("create same-name public and app tables");

    client
        .batch_execute("GRANT SELECT ON TABLE app.grant_qualifier_guard TO qualifier_reader")
        .await
        .expect("qualified GRANT targets app table");

    let app_visible = client
        .query_one(
            "SELECT has_table_privilege('qualifier_reader', 'app.grant_qualifier_guard', 'SELECT')",
            &[],
        )
        .await
        .expect("qualified app privilege check")
        .get::<_, bool>(0);
    assert!(app_visible, "qualified GRANT must persist on app table");

    let public_visible = client
        .query_one(
            "SELECT has_table_privilege('qualifier_reader', 'grant_qualifier_guard', 'SELECT')",
            &[],
        )
        .await
        .expect("public privilege check")
        .get::<_, bool>(0);
    assert!(
        !public_visible,
        "qualified GRANT must not leak to public same-name table"
    );

    client
        .batch_execute(
            "DROP TABLE app.grant_qualifier_guard; \
             DROP TABLE grant_qualifier_guard; \
             DROP SCHEMA app; \
             DROP ROLE tester; \
             DROP ROLE qualifier_reader",
        )
        .await
        .expect("cleanup table privilege qualifier guard");

    shutdown(running).await;
}

#[tokio::test]
async fn grant_table_preserves_quoted_dot_in_public_name() {
    let running = start_sample_server("privilege_table_quoted_dot_guard").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE dotted_reader LOGIN; \
             CREATE TABLE \"grant.dot\" (id INT); \
             GRANT SELECT ON TABLE \"grant.dot\" TO dotted_reader",
        )
        .await
        .expect("grant quoted dotted table");

    let visible = client
        .query_one(
            "SELECT has_table_privilege('dotted_reader', '\"grant.dot\"', 'SELECT')",
            &[],
        )
        .await
        .expect("quoted dotted table privilege check")
        .get::<_, bool>(0);
    assert!(visible, "quoted dotted table grant must persist");

    client
        .batch_execute(
            "DROP TABLE \"grant.dot\"; \
             DROP ROLE tester; \
             DROP ROLE dotted_reader",
        )
        .await
        .expect("cleanup quoted dotted table privilege guard");

    shutdown(running).await;
}

#[tokio::test]
async fn grant_schema_preserves_quoted_dot_in_name() {
    let running = start_sample_server("privilege_schema_quoted_dot_guard").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE dotted_schema_reader LOGIN; \
             CREATE SCHEMA \"acl.dot\"; \
             GRANT USAGE ON SCHEMA \"acl.dot\" TO dotted_schema_reader",
        )
        .await
        .expect("grant quoted dotted schema");

    let visible = client
        .query_one(
            "SELECT has_schema_privilege('dotted_schema_reader', '\"acl.dot\"', 'USAGE')",
            &[],
        )
        .await
        .expect("quoted dotted schema privilege check")
        .get::<_, bool>(0);
    assert!(visible, "quoted dotted schema grant must persist");

    client
        .batch_execute(
            "DROP SCHEMA \"acl.dot\"; \
             DROP ROLE tester; \
             DROP ROLE dotted_schema_reader",
        )
        .await
        .expect("cleanup quoted dotted schema privilege guard");

    shutdown(running).await;
}

#[tokio::test]
async fn non_superuser_cannot_alter_default_privileges_for_privileged_role() {
    let running = start_sample_server("privilege_catalog_test").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE delegated_admin CREATEROLE LOGIN; \
             CREATE ROLE other_admin SUPERUSER LOGIN; \
             CREATE ROLE analyst LOGIN; \
             GRANT other_admin TO delegated_admin",
        )
        .await
        .expect("create delegated and privileged roles");

    let (delegated, delegated_conn) =
        connect_as(running.bound, "delegated_admin", "default_privilege_reject").await;
    assert_insufficient_privilege(
        delegated
            .batch_execute(
                "ALTER DEFAULT PRIVILEGES FOR ROLE other_admin \
                 GRANT SELECT ON TABLES TO analyst",
            )
            .await
            .expect_err("non-superuser cannot alter privileged role default privileges"),
    );
    drop(delegated);
    delegated_conn.await.expect("delegated connection joins");

    shutdown(running).await;
}

#[tokio::test]
async fn privilege_ddl_rejects_unknown_roles_with_undefined_object() {
    let running = start_sample_server("privilege_catalog_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE missing_role_acl_target (id INT)")
        .await
        .expect("create grant target");

    let missing_grantee = client
        .batch_execute("GRANT SELECT ON TABLE missing_role_acl_target TO missing_acl_role")
        .await
        .expect_err("missing grantee must reject GRANT");
    assert_eq!(missing_grantee.code().expect("SQLSTATE").code(), "42704");

    let missing_owner = client
        .batch_execute(
            "ALTER DEFAULT PRIVILEGES FOR ROLE missing_acl_owner \
             GRANT SELECT ON TABLES TO public",
        )
        .await
        .expect_err("missing owner role must reject ALTER DEFAULT PRIVILEGES");
    assert_eq!(missing_owner.code().expect("SQLSTATE").code(), "42704");

    shutdown(running).await;
}
