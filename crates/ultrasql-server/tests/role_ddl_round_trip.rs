//! Wire-level coverage for role-management DDL.

mod support;

use support::{shutdown, start_persistent_server, start_sample_server};
use tokio_postgres::{NoTls, error::SqlState};
use ultrasql_server::Server;

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
async fn drop_role_rejects_bootstrap_role() {
    let running = start_sample_server("role_ddl_test").await;
    let client = &running.client;

    let err = client
        .batch_execute("DROP ROLE ultrasql")
        .await
        .expect_err("bootstrap role must not be droppable");
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("cannot drop bootstrap role")),
        "expected bootstrap role rejection, got {err}"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn alter_role_rejects_bootstrap_privilege_demotion() {
    let running = start_sample_server("role_ddl_test").await;
    let client = &running.client;

    let err = client
        .batch_execute("ALTER ROLE ultrasql NOSUPERUSER")
        .await
        .expect_err("bootstrap role must not lose superuser");
    assert!(
        err.as_db_error().is_some_and(|db| db
            .message()
            .contains("cannot alter bootstrap role privileges")),
        "expected bootstrap privilege rejection, got {err}"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn non_createrole_role_cannot_manage_roles() {
    let running = start_sample_server("role_ddl_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE limited LOGIN")
        .await
        .expect("create limited role");
    client
        .batch_execute("CREATE ROLE role_drop_candidate LOGIN")
        .await
        .expect("create drop candidate");

    let conn_str = format!(
        "host={host} port={port} user=limited application_name=role_admin_reject",
        host = running.bound.ip(),
        port = running.bound.port()
    );
    let (limited, limited_conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect as limited role");
    let limited_conn = tokio::spawn(async move {
        if let Err(err) = limited_conn.await {
            eprintln!("limited connection error: {err}");
        }
    });

    assert_insufficient_privilege(
        limited
            .batch_execute("CREATE ROLE rogue_role LOGIN")
            .await
            .expect_err("non-CREATEROLE role cannot create roles"),
    );
    assert_insufficient_privilege(
        limited
            .batch_execute("ALTER ROLE role_drop_candidate CREATEDB")
            .await
            .expect_err("non-CREATEROLE role cannot alter roles"),
    );
    assert_insufficient_privilege(
        limited
            .batch_execute("DROP ROLE role_drop_candidate")
            .await
            .expect_err("non-CREATEROLE role cannot drop roles"),
    );

    drop(limited);
    limited_conn.await.expect("limited connection joins");
    shutdown(running).await;
}

#[test]
fn role_metadata_rejects_duplicate_role_names_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_auth.meta"),
        concat!(
            "# ultrasql auth runtime v1\n",
            "role\tdupe\t17000\t\tfalse\ttrue\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\t\n",
            "role\tDUPE\t17001\t\tfalse\ttrue\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\t\n"
        ),
    )
    .expect("write duplicate auth metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate role metadata rejected");
    assert!(
        err.to_string().contains("duplicate role metadata"),
        "expected duplicate role metadata rejection, got {err}"
    );
}

#[test]
fn role_metadata_rejects_empty_role_names_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_auth.meta"),
        concat!(
            "# ultrasql auth runtime v1\n",
            "role\t\t17000\t\tfalse\ttrue\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\t\n"
        ),
    )
    .expect("write empty role metadata");

    let err = match Server::init(data_dir.path()) {
        Ok(_) => panic!("empty role metadata should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("empty role metadata name"),
        "expected empty role metadata rejection, got {err}"
    );
}

#[test]
fn role_metadata_rejects_zero_role_oids_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_auth.meta"),
        concat!(
            "# ultrasql auth runtime v1\n",
            "role\tzero_oid\t0\t\tfalse\ttrue\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\t\n"
        ),
    )
    .expect("write zero oid role metadata");

    let err = match Server::init(data_dir.path()) {
        Ok(_) => panic!("zero role OID metadata should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("invalid role metadata oid 0"),
        "expected zero role OID rejection, got {err}"
    );
}

#[test]
fn role_metadata_rejects_missing_bootstrap_role_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_auth.meta"),
        concat!(
            "# ultrasql auth runtime v1\n",
            "role\tapp_only\t17000\t\tfalse\ttrue\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\t\n"
        ),
    )
    .expect("write auth metadata without bootstrap role");

    let err = match Server::init(data_dir.path()) {
        Ok(_) => panic!("auth metadata without bootstrap role should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("missing bootstrap role metadata"),
        "expected missing bootstrap role rejection, got {err}"
    );
}

#[test]
fn role_metadata_rejects_wrong_bootstrap_oid_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_auth.meta"),
        concat!(
            "# ultrasql auth runtime v1\n",
            "role\tultrasql\t17000\t\ttrue\ttrue\ttrue\ttrue\ttrue\tfalse\tfalse\t-1\t\n"
        ),
    )
    .expect("write auth metadata with wrong bootstrap oid");

    let err = match Server::init(data_dir.path()) {
        Ok(_) => panic!("wrong bootstrap OID metadata should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("invalid bootstrap role metadata"),
        "expected invalid bootstrap OID rejection, got {err}"
    );
}

#[test]
fn role_metadata_rejects_demoted_bootstrap_role_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_auth.meta"),
        concat!(
            "# ultrasql auth runtime v1\n",
            "role\tultrasql\t10\t\tfalse\ttrue\ttrue\ttrue\ttrue\tfalse\tfalse\t-1\t\n"
        ),
    )
    .expect("write demoted bootstrap metadata");

    let err = match Server::init(data_dir.path()) {
        Ok(_) => panic!("demoted bootstrap metadata should be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("invalid bootstrap role metadata privileges"),
        "expected invalid bootstrap privilege rejection, got {err}"
    );
}

#[test]
fn role_metadata_rejects_duplicate_memberships_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_auth.meta"),
        concat!(
            "# ultrasql auth runtime v1\n",
            "role\tparent\t17000\t\tfalse\ttrue\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\t\n",
            "role\tchild\t17001\t\tfalse\ttrue\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\t\n",
            "member\tparent\tchild\tparent\tfalse\n",
            "member\tparent\tchild\tparent\ttrue\n"
        ),
    )
    .expect("write duplicate membership metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate membership metadata rejected");
    assert!(
        err.to_string()
            .contains("duplicate role membership metadata"),
        "expected duplicate membership metadata rejection, got {err}"
    );
}

#[test]
fn role_metadata_rejects_memberships_with_unknown_roles_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_auth.meta"),
        concat!(
            "# ultrasql auth runtime v1\n",
            "role\tparent\t17000\t\tfalse\ttrue\tfalse\tfalse\ttrue\tfalse\tfalse\t-1\t\n",
            "member\tparent\tmissing_child\tparent\tfalse\n"
        ),
    )
    .expect("write dangling membership metadata");

    let err = Server::init(data_dir.path()).expect_err("dangling membership metadata rejected");
    assert!(
        err.to_string().contains("unknown role membership metadata"),
        "expected dangling membership metadata rejection, got {err}"
    );
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

fn assert_insufficient_privilege(err: tokio_postgres::Error) {
    let db = err.as_db_error().expect("database error");
    assert_eq!(
        db.code(),
        &SqlState::INSUFFICIENT_PRIVILEGE,
        "{}",
        db.message()
    );
}

#[tokio::test]
async fn drop_role_rejects_granted_privileges_until_revoked() {
    let running = start_sample_server("role_ddl_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register tester role");
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

#[tokio::test]
async fn drop_role_rejects_rls_policy_roles_until_policy_table_is_dropped() {
    let running = start_sample_server("role_ddl_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register tester role");
    client
        .batch_execute("CREATE ROLE rls_policy_reader LOGIN")
        .await
        .expect("create policy role");
    client
        .batch_execute("CREATE TABLE role_rls_policy_table (tenant_id TEXT NOT NULL)")
        .await
        .expect("create RLS table");
    client
        .batch_execute(
            "CREATE POLICY role_rls_policy_reader_policy \
             ON role_rls_policy_table \
             FOR SELECT TO rls_policy_reader \
             USING (tenant_id = current_setting('ultrasql.tenant_id', true))",
        )
        .await
        .expect("create policy referencing role");

    let restricted = client
        .batch_execute("DROP ROLE rls_policy_reader")
        .await
        .expect_err("RLS policy role reference must block DROP ROLE");
    assert_eq!(restricted.code().expect("SQLSTATE").code(), "2BP01");

    client
        .batch_execute("DROP TABLE role_rls_policy_table")
        .await
        .expect("drop table with policy");
    client
        .batch_execute("DROP ROLE rls_policy_reader")
        .await
        .expect("drop role after policy table removed");

    shutdown(running).await;
}

#[tokio::test]
async fn drop_role_rejects_membership_grantor_until_membership_is_revoked() {
    let running = start_sample_server("role_ddl_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register tester role");
    client
        .batch_execute("CREATE ROLE grant_admin CREATEROLE LOGIN")
        .await
        .expect("create membership grantor");
    client
        .batch_execute("CREATE ROLE membership_parent NOLOGIN")
        .await
        .expect("create parent role");
    client
        .batch_execute("CREATE ROLE membership_child LOGIN")
        .await
        .expect("create member role");

    let conn_str = format!(
        "host={host} port={port} user=grant_admin application_name=role_grantor_drop",
        host = running.bound.ip(),
        port = running.bound.port()
    );
    let (grant_admin, grant_admin_conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect as grant_admin");
    let grant_admin_conn = tokio::spawn(async move {
        if let Err(err) = grant_admin_conn.await {
            eprintln!("grant_admin connection error: {err}");
        }
    });
    grant_admin
        .batch_execute("GRANT membership_parent TO membership_child")
        .await
        .expect("grant membership as grant_admin");
    drop(grant_admin);
    grant_admin_conn
        .await
        .expect("grant_admin connection joins");

    let restricted = client
        .batch_execute("DROP ROLE grant_admin")
        .await
        .expect_err("membership grantor reference must block DROP ROLE");
    assert_eq!(restricted.code().expect("SQLSTATE").code(), "2BP01");

    client
        .batch_execute("REVOKE membership_parent FROM membership_child")
        .await
        .expect("revoke membership");
    client
        .batch_execute("DROP ROLE grant_admin")
        .await
        .expect("drop grantor after membership revoked");

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

    let membership = running
        .client
        .query_one(
            "SELECT granted.rolname, member.rolname, grantor.rolname, m.admin_option \
             FROM pg_catalog.pg_auth_members m \
             JOIN pg_catalog.pg_roles granted ON granted.oid = m.roleid \
             JOIN pg_catalog.pg_roles member ON member.oid = m.member \
             JOIN pg_catalog.pg_roles grantor ON grantor.oid = m.grantor \
             WHERE granted.rolname = 'parent' AND member.rolname = 'persisted'",
            &[],
        )
        .await
        .expect("persisted role membership visible after restart");
    assert_eq!(membership.get::<_, String>(0), "parent");
    assert_eq!(membership.get::<_, String>(1), "persisted");
    assert_eq!(membership.get::<_, String>(2), "tester");
    assert!(
        membership.get::<_, bool>(3),
        "ADMIN OPTION should survive restart"
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
