//! Wire-level coverage for role-management DDL.

mod support;

use support::{shutdown, start_persistent_server, start_sample_server};
use tokio_postgres::NoTls;

#[tokio::test]
async fn create_alter_drop_role_and_user_update_catalog_views() {
    let running = start_sample_server("role_ddl_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE analytics NOLOGIN CREATEDB CREATEROLE")
        .await
        .expect("create role");
    client
        .batch_execute("ALTER ROLE analytics LOGIN NOCREATEDB")
        .await
        .expect("alter role");
    client
        .batch_execute("CREATE USER app_user PASSWORD 'secret' NOSUPERUSER CREATEDB")
        .await
        .expect("create user");

    let role = client
        .query_one(
            "SELECT rolcanlogin, rolcreatedb, rolcreaterole \
             FROM pg_catalog.pg_roles \
             WHERE rolname = 'analytics'",
            &[],
        )
        .await
        .expect("analytics role visible");
    assert!(role.get::<_, bool>(0), "ALTER ROLE should enable LOGIN");
    assert!(
        !role.get::<_, bool>(1),
        "ALTER ROLE should disable CREATEDB"
    );
    assert!(role.get::<_, bool>(2), "CREATE ROLE should keep CREATEROLE");

    let user = client
        .query_one(
            "SELECT rolcanlogin, rolcreatedb, rolsuper \
             FROM pg_catalog.pg_roles \
             WHERE rolname = 'app_user'",
            &[],
        )
        .await
        .expect("app_user role visible");
    assert!(user.get::<_, bool>(0), "CREATE USER implies LOGIN");
    assert!(user.get::<_, bool>(1), "CREATEDB option should persist");
    assert!(!user.get::<_, bool>(2), "NOSUPERUSER option should persist");

    let app_user_rows = client
        .query(
            "SELECT usename FROM pg_catalog.pg_user WHERE usename = 'app_user'",
            &[],
        )
        .await
        .expect("pg_user query");
    assert_eq!(app_user_rows.len(), 1, "CREATE USER appears in pg_user");

    client
        .batch_execute("CREATE ROLE IF NOT EXISTS analytics")
        .await
        .expect("idempotent create role");
    client
        .batch_execute("DROP USER app_user")
        .await
        .expect("drop user");
    client
        .batch_execute("DROP ROLE IF EXISTS missing_role")
        .await
        .expect("idempotent drop role");
    client
        .batch_execute("DROP ROLE analytics")
        .await
        .expect("drop role");

    let remaining = client
        .query_one(
            "SELECT COUNT(*) FROM pg_catalog.pg_roles \
             WHERE rolname IN ('analytics', 'app_user')",
            &[],
        )
        .await
        .expect("roles removed");
    assert_eq!(remaining.get::<_, i64>(0), 0);

    shutdown(running).await;
}

#[tokio::test]
async fn drop_role_rejects_owned_table_until_object_is_dropped() {
    let running = start_sample_server("role_ddl_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register tester role");
    client
        .batch_execute("CREATE ROLE object_owner LOGIN")
        .await
        .expect("create object owner");
    client
        .batch_execute("SET ROLE object_owner")
        .await
        .expect("set role to object owner");
    client
        .batch_execute("CREATE TABLE role_owned_table (id INT)")
        .await
        .expect("create owned table");
    client
        .batch_execute("RESET ROLE")
        .await
        .expect("reset role");

    let restricted = client
        .batch_execute("DROP ROLE object_owner")
        .await
        .expect_err("owned table must block DROP ROLE");
    assert_eq!(restricted.code().expect("SQLSTATE").code(), "2BP01");

    client
        .batch_execute("DROP TABLE role_owned_table")
        .await
        .expect("drop owned table");
    client
        .batch_execute("DROP ROLE object_owner")
        .await
        .expect("drop role after owned object removed");

    shutdown(running).await;
}

#[tokio::test]
async fn drop_role_rejects_granted_privileges_until_revoked() {
    let running = start_sample_server("role_ddl_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE grant_target LOGIN")
        .await
        .expect("create grant target");
    client
        .batch_execute("CREATE TABLE role_grant_table (id INT)")
        .await
        .expect("create grant table");
    client
        .batch_execute("GRANT SELECT ON TABLE role_grant_table TO grant_target")
        .await
        .expect("grant table privilege");

    let restricted = client
        .batch_execute("DROP ROLE grant_target")
        .await
        .expect_err("privilege grant must block DROP ROLE");
    assert_eq!(restricted.code().expect("SQLSTATE").code(), "2BP01");

    client
        .batch_execute("REVOKE SELECT ON TABLE role_grant_table FROM grant_target")
        .await
        .expect("revoke table privilege");
    client
        .batch_execute("DROP ROLE grant_target")
        .await
        .expect("drop role after revoke");

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn role_catalog_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "role_restart_setup").await;
    let client = &running.client;
    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register tester role");
    client
        .batch_execute("CREATE ROLE parent NOLOGIN")
        .await
        .expect("create parent role");
    client
        .batch_execute("CREATE ROLE persisted LOGIN CREATEDB BYPASSRLS")
        .await
        .expect("create persisted role");
    client
        .batch_execute("ALTER ROLE persisted CREATEROLE")
        .await
        .expect("alter persisted role");
    client
        .batch_execute("GRANT parent TO persisted WITH ADMIN OPTION")
        .await
        .expect("grant persisted membership");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "role_restart_verify").await;
    let role = running
        .client
        .query_one(
            "SELECT rolcanlogin, rolcreatedb, rolcreaterole, rolbypassrls \
             FROM pg_catalog.pg_roles \
             WHERE rolname = 'persisted'",
            &[],
        )
        .await
        .expect("persisted role visible after restart");
    assert!(role.get::<_, bool>(0), "LOGIN should survive restart");
    assert!(role.get::<_, bool>(1), "CREATEDB should survive restart");
    assert!(role.get::<_, bool>(2), "CREATEROLE should survive restart");
    assert!(role.get::<_, bool>(3), "BYPASSRLS should survive restart");

    let parent = running
        .client
        .query_one(
            "SELECT rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = 'parent'",
            &[],
        )
        .await
        .expect("parent role visible after restart");
    assert!(
        !parent.get::<_, bool>(0),
        "NOLOGIN parent should survive restart"
    );

    let conn_str = format!(
        "host={host} port={port} user=persisted application_name=role_restart_member",
        host = running.bound.ip(),
        port = running.bound.port()
    );
    let (persisted, persisted_conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect as persisted role");
    let persisted_conn = tokio::spawn(async move {
        if let Err(err) = persisted_conn.await {
            eprintln!("persisted connection error: {err}");
        }
    });
    persisted
        .batch_execute("SET ROLE parent")
        .await
        .expect("persisted membership permits SET ROLE parent after restart");
    let identity = persisted
        .query_one("SELECT current_user, session_user", &[])
        .await
        .expect("identity after restarted SET ROLE");
    assert_eq!(identity.get::<_, String>(0), "parent");
    assert_eq!(identity.get::<_, String>(1), "persisted");
    drop(persisted);
    persisted_conn.await.expect("persisted connection joins");

    shutdown(running).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn role_catalog_rolls_back_when_metadata_slot_is_unsafe() {
    use std::os::unix::fs::symlink;

    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let outside = data_dir.path().join("outside-auth-meta");
    std::fs::write(&outside, b"keep").expect("outside metadata target");

    let running = start_persistent_server(data_dir.path(), "role_rollback_setup").await;
    running
        .client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register tester role");
    symlink(&outside, data_dir.path().join("pg_auth.meta.tmp")).expect("auth temp symlink");

    let err = running
        .client
        .batch_execute("CREATE ROLE rollback_probe LOGIN")
        .await
        .expect_err("unsafe auth metadata slot rejects role DDL");
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("runtime metadata file")),
        "unexpected error: {err}"
    );
    let count = running
        .client
        .query_one(
            "SELECT COUNT(*) FROM pg_catalog.pg_roles WHERE rolname = 'rollback_probe'",
            &[],
        )
        .await
        .expect("rollback probe role count");
    assert_eq!(
        count.get::<_, i64>(0),
        0,
        "failed role DDL must not remain in memory after metadata failure"
    );

    shutdown(running).await;
}
