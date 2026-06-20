use super::*;

#[tokio::test]
async fn default_privileges_apply_to_future_objects_only() {
    let running = start_sample_server("default_privilege_catalog_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register admin role");
    client
        .batch_execute("CREATE ROLE analyst LOGIN")
        .await
        .expect("create analyst role");
    client
        .batch_execute("CREATE SCHEMA tenant")
        .await
        .expect("create tenant schema");
    client
        .batch_execute("GRANT USAGE ON SCHEMA tenant TO analyst")
        .await
        .expect("grant tenant schema usage");
    client
        .batch_execute(
            "ALTER DEFAULT PRIVILEGES FOR ROLE tester IN SCHEMA tenant \
             GRANT SELECT ON TABLES TO analyst",
        )
        .await
        .expect("grant default table privilege");
    client
        .batch_execute("CREATE TABLE tenant.default_acl_future (id INT, secret TEXT)")
        .await
        .expect("create table after default grant");
    client
        .batch_execute("INSERT INTO tenant.default_acl_future (id, secret) VALUES (1, 'visible')")
        .await
        .expect("seed future table");

    let granted = client
        .query_one(
            "SELECT has_table_privilege('analyst', 'tenant.default_acl_future', 'SELECT')",
            &[],
        )
        .await
        .expect("default privilege check");
    assert!(
        granted.get::<_, bool>(0),
        "default SELECT should apply to future table"
    );

    let (analyst, analyst_conn) =
        connect_as(running.bound, "analyst", "default_privilege_analyst").await;
    let visible = analyst
        .query_one(
            "SELECT secret FROM tenant.default_acl_future WHERE id = 1",
            &[],
        )
        .await
        .expect("default-granted SELECT succeeds");
    assert_eq!(visible.get::<_, String>(0), "visible");
    drop(analyst);
    analyst_conn.await.expect("analyst connection joins");

    client
        .batch_execute(
            "ALTER DEFAULT PRIVILEGES FOR ROLE tester IN SCHEMA tenant \
             REVOKE SELECT ON TABLES FROM analyst",
        )
        .await
        .expect("revoke default table privilege");
    client
        .batch_execute("CREATE TABLE tenant.default_acl_later (id INT)")
        .await
        .expect("create table after default revoke");

    let after_revoke = client
        .query_one(
            "SELECT \
                has_table_privilege('analyst', 'tenant.default_acl_future', 'SELECT'), \
                has_table_privilege('analyst', 'tenant.default_acl_later', 'SELECT')",
            &[],
        )
        .await
        .expect("post-revoke privilege checks");
    assert!(
        after_revoke.get::<_, bool>(0),
        "existing table keeps already-applied default grant"
    );
    assert!(
        !after_revoke.get::<_, bool>(1),
        "revoked default should not apply to later tables"
    );

    client
        .batch_execute(
            "ALTER DEFAULT PRIVILEGES FOR ROLE tester GRANT USAGE ON SEQUENCES TO analyst",
        )
        .await
        .expect("grant default sequence privilege");
    client
        .batch_execute("CREATE SEQUENCE default_acl_seq")
        .await
        .expect("create sequence after default grant");
    let sequence_granted = client
        .query_one(
            "SELECT has_sequence_privilege('analyst', 'default_acl_seq', 'USAGE')",
            &[],
        )
        .await
        .expect("default sequence privilege check");
    assert!(
        sequence_granted.get::<_, bool>(0),
        "default USAGE should apply to future sequence"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn drop_schema_removes_schema_scoped_default_privileges() {
    let running = start_sample_server("default_privilege_drop_schema_test").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE analyst LOGIN; \
             CREATE SCHEMA tenant; \
             ALTER DEFAULT PRIVILEGES FOR ROLE tester IN SCHEMA tenant \
             GRANT SELECT ON TABLES TO analyst; \
             DROP SCHEMA tenant; \
             CREATE SCHEMA tenant; \
             CREATE TABLE tenant.after_schema_recreate (id INT)",
        )
        .await
        .expect("drop and recreate schema with prior default privileges");

    let granted = client
        .query_one(
            "SELECT has_table_privilege('analyst', 'tenant.after_schema_recreate', 'SELECT')",
            &[],
        )
        .await
        .expect("default privilege cleanup check");
    assert!(
        !granted.get::<_, bool>(0),
        "dropped schema must clear schema-scoped default privileges"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn default_privileges_reject_missing_schema() {
    let running = start_sample_server("default_privilege_missing_schema").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN; CREATE ROLE analyst LOGIN")
        .await
        .expect("create default privilege roles");

    client
        .batch_execute(
            "ALTER DEFAULT PRIVILEGES FOR ROLE tester IN SCHEMA missing_schema \
             GRANT SELECT ON TABLES TO analyst",
        )
        .await
        .expect_err("default privileges must reject missing schemas");

    client
        .batch_execute(
            "CREATE SCHEMA missing_schema; CREATE TABLE missing_schema.future_acl (id INT)",
        )
        .await
        .expect("create schema and table after rejected default privilege");

    let granted = client
        .query_one(
            "SELECT has_table_privilege('analyst', 'future_acl', 'SELECT')",
            &[],
        )
        .await
        .expect("default privilege check after rejected missing schema grant")
        .get::<_, bool>(0);
    assert!(
        !granted,
        "rejected missing-schema default privilege must not affect future tables"
    );

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn privilege_catalog_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "privilege_restart_setup").await;
    let client = &running.client;
    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register admin role");
    client
        .batch_execute("CREATE ROLE analyst LOGIN")
        .await
        .expect("create analyst role");
    client
        .batch_execute("CREATE SCHEMA tenant")
        .await
        .expect("create tenant schema");
    client
        .batch_execute("CREATE TABLE priv_restart (id INT, secret TEXT)")
        .await
        .expect("create privilege table");
    client
        .batch_execute("CREATE SEQUENCE priv_restart_seq")
        .await
        .expect("create privilege sequence");
    client
        .batch_execute("GRANT SELECT(id) ON TABLE priv_restart TO analyst")
        .await
        .expect("grant column select");
    client
        .batch_execute("GRANT USAGE ON SEQUENCE priv_restart_seq TO analyst")
        .await
        .expect("grant sequence usage");
    client
        .batch_execute(
            "ALTER DEFAULT PRIVILEGES FOR ROLE tester IN SCHEMA tenant \
             GRANT SELECT ON TABLES TO analyst",
        )
        .await
        .expect("grant default table select");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "privilege_restart_verify").await;
    let checks = running
        .client
        .query_one(
            "SELECT \
                has_column_privilege('analyst', 'priv_restart', 'id', 'SELECT'), \
                has_column_privilege('analyst', 'priv_restart', 'secret', 'SELECT'), \
                has_sequence_privilege('analyst', 'priv_restart_seq', 'USAGE')",
            &[],
        )
        .await
        .expect("privilege checks after restart");
    assert!(checks.get::<_, bool>(0), "column grant should restart");
    assert!(
        !checks.get::<_, bool>(1),
        "ungranted column should remain denied after restart"
    );
    assert!(checks.get::<_, bool>(2), "sequence grant should restart");

    running
        .client
        .batch_execute("CREATE TABLE tenant.priv_restart_future (id INT)")
        .await
        .expect("create future table after restart");
    let default_grant = running
        .client
        .query_one(
            "SELECT has_table_privilege('analyst', 'tenant.priv_restart_future', 'SELECT')",
            &[],
        )
        .await
        .expect("default privilege check after restart");
    assert!(
        default_grant.get::<_, bool>(0),
        "default privilege template should apply after restart"
    );

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_rename_rewrites_privilege_metadata_across_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "privilege_rename_setup").await;
    let client = &running.client;
    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE rename_acl_reader LOGIN; \
             CREATE TABLE privilege_rename_old (id INT, secret TEXT); \
             INSERT INTO privilege_rename_old VALUES (1, 'hidden'); \
             GRANT SELECT(id), UPDATE(id) ON TABLE privilege_rename_old TO rename_acl_reader; \
             GRANT DELETE ON TABLE privilege_rename_old TO rename_acl_reader; \
             ALTER TABLE privilege_rename_old RENAME TO privilege_rename_new",
        )
        .await
        .expect("create grants and rename table");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "privilege_rename_verify").await;
    let checks = running
        .client
        .query_one(
            "SELECT \
                has_column_privilege('rename_acl_reader', 'privilege_rename_new', 'id', 'SELECT'), \
                has_column_privilege('rename_acl_reader', 'privilege_rename_old', 'id', 'SELECT'), \
                has_table_privilege('rename_acl_reader', 'privilege_rename_new', 'DELETE'), \
                has_table_privilege('rename_acl_reader', 'privilege_rename_old', 'DELETE'), \
                has_column_privilege('rename_acl_reader', 'privilege_rename_new', 'secret', 'SELECT')",
            &[],
        )
        .await
        .expect("privilege checks after table rename restart");
    assert!(
        checks.get::<_, bool>(0),
        "renamed table should keep column SELECT grant"
    );
    assert!(
        !checks.get::<_, bool>(1),
        "old table name must not keep stale column SELECT grant"
    );
    assert!(
        checks.get::<_, bool>(2),
        "renamed table should keep object DELETE grant"
    );
    assert!(
        !checks.get::<_, bool>(3),
        "old table name must not keep stale object DELETE grant"
    );
    assert!(
        !checks.get::<_, bool>(4),
        "ungranted column should stay denied after rename"
    );

    let (reader, reader_conn) =
        connect_as(running.bound, "rename_acl_reader", "rename_acl_reader").await;
    let row = reader
        .query_one("SELECT id FROM privilege_rename_new WHERE id = 1", &[])
        .await
        .expect("renamed table column grant permits SELECT");
    assert_eq!(row.get::<_, i32>(0), 1);
    assert_insufficient_privilege(
        reader
            .query_one("SELECT secret FROM privilege_rename_new WHERE id = 1", &[])
            .await
            .expect_err("ungranted renamed table column SELECT fails"),
    );
    drop(reader);
    reader_conn.await.expect("reader connection joins");

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn privilege_metadata_accepts_uncataloged_trust_grantor_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "trust_grantor_setup").await;
    running
        .client
        .batch_execute(
            "CREATE ROLE analyst LOGIN; \
             CREATE TABLE trust_grantor_acl (id INT); \
             GRANT SELECT ON TABLE trust_grantor_acl TO analyst",
        )
        .await
        .expect("trust-mode grant persists grantor");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "trust_grantor_verify").await;
    let visible = running
        .client
        .query_one(
            "SELECT has_table_privilege('analyst', 'trust_grantor_acl', 'SELECT')",
            &[],
        )
        .await
        .expect("grant survives restart with trust grantor")
        .get::<_, bool>(0);
    assert!(visible, "trust-mode grantor metadata should rebuild");

    shutdown(running).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn privilege_catalog_rolls_back_when_metadata_slot_is_unsafe() {
    use std::os::unix::fs::symlink;

    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    support::make_data_dir_private(data_dir.path());
    let outside = data_dir.path().join("outside-privilege-meta");
    std::fs::write(&outside, b"keep").expect("outside metadata target");

    let running = start_persistent_server(data_dir.path(), "privilege_rollback_setup").await;
    let client = &running.client;
    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register admin role");
    client
        .batch_execute("CREATE ROLE analyst LOGIN")
        .await
        .expect("create analyst role");
    client
        .batch_execute("CREATE TABLE privilege_rollback (id INT)")
        .await
        .expect("create rollback table");
    symlink(&outside, data_dir.path().join("pg_privileges.meta.tmp"))
        .expect("privilege temp symlink");

    let err = client
        .batch_execute("GRANT SELECT ON TABLE privilege_rollback TO analyst")
        .await
        .expect_err("unsafe privilege metadata slot rejects GRANT");
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("runtime metadata file")),
        "unexpected error: {err}"
    );
    let visible = client
        .query_one(
            "SELECT has_table_privilege('analyst', 'privilege_rollback', 'SELECT')",
            &[],
        )
        .await
        .expect("rollback privilege check");
    assert!(
        !visible.get::<_, bool>(0),
        "failed GRANT must not remain in memory after metadata failure"
    );

    shutdown(running).await;
}
