//! Wire-level coverage for privilege DDL.

mod support;

use std::net::SocketAddr;

use support::{shutdown, start_sample_server};
use tokio_postgres::{NoTls, error::SqlState};

#[tokio::test]
async fn grant_revoke_privileges_update_catalog_checks() {
    let running = start_sample_server("privilege_catalog_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE analyst NOLOGIN")
        .await
        .expect("create analyst role");
    client
        .batch_execute("CREATE TABLE grant_target (id INT)")
        .await
        .expect("create grant target table");
    client
        .batch_execute("CREATE SEQUENCE grant_seq")
        .await
        .expect("create grant target sequence");

    client
        .batch_execute("GRANT SELECT, INSERT ON TABLE grant_target TO analyst")
        .await
        .expect("grant table privileges");
    client
        .batch_execute("GRANT USAGE ON SCHEMA public TO analyst")
        .await
        .expect("grant schema privilege");
    client
        .batch_execute("GRANT CONNECT, TEMPORARY ON DATABASE ultrasql TO analyst")
        .await
        .expect("grant database privileges");
    client
        .batch_execute("GRANT USAGE, SELECT ON SEQUENCE grant_seq TO analyst")
        .await
        .expect("grant sequence privileges");
    client
        .batch_execute("GRANT EXECUTE ON FUNCTION current_database() TO analyst")
        .await
        .expect("grant function privilege");
    client
        .batch_execute("REVOKE INSERT ON TABLE grant_target FROM analyst")
        .await
        .expect("revoke table privilege");
    client
        .batch_execute("REVOKE TEMPORARY ON DATABASE ultrasql FROM analyst")
        .await
        .expect("revoke database privilege");

    let row = client
        .query_one(
            "SELECT \
                has_table_privilege('analyst', 'grant_target', 'SELECT'), \
                has_table_privilege('analyst', 'grant_target', 'INSERT'), \
                has_schema_privilege('analyst', 'public', 'USAGE'), \
                has_database_privilege('analyst', 'ultrasql', 'CONNECT'), \
                has_database_privilege('analyst', 'ultrasql', 'TEMPORARY'), \
                has_sequence_privilege('analyst', 'grant_seq', 'USAGE'), \
                has_function_privilege('analyst', 'current_database()', 'EXECUTE')",
            &[],
        )
        .await
        .expect("privilege checks");

    assert!(row.get::<_, bool>(0), "SELECT grant should persist");
    assert!(!row.get::<_, bool>(1), "INSERT grant should be revoked");
    assert!(row.get::<_, bool>(2), "schema USAGE grant should persist");
    assert!(
        row.get::<_, bool>(3),
        "database CONNECT grant should persist"
    );
    assert!(
        !row.get::<_, bool>(4),
        "database TEMPORARY grant should be revoked"
    );
    assert!(row.get::<_, bool>(5), "sequence USAGE grant should persist");
    assert!(
        row.get::<_, bool>(6),
        "function EXECUTE grant should persist"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn column_privileges_gate_select_insert_and_update_targets() {
    let running = start_sample_server("column_privilege_catalog_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register admin role");
    client
        .batch_execute("CREATE ROLE limited LOGIN")
        .await
        .expect("create limited role");
    client
        .batch_execute("CREATE TABLE column_acl (id INT, secret TEXT)")
        .await
        .expect("create column ACL table");
    client
        .batch_execute("INSERT INTO column_acl (id, secret) VALUES (1, 'hidden')")
        .await
        .expect("seed protected row");
    client
        .batch_execute("GRANT SELECT(id), INSERT(id), UPDATE(id) ON TABLE column_acl TO limited")
        .await
        .expect("grant column privileges");
    let grant_row = client
        .query_one(
            "SELECT \
                has_column_privilege('limited', 'column_acl', 'id', 'SELECT'), \
                has_column_privilege('limited', 'column_acl', 'secret', 'SELECT')",
            &[],
        )
        .await
        .expect("column privilege checks");
    assert!(
        grant_row.get::<_, bool>(0),
        "column SELECT grant should be visible"
    );
    assert!(
        !grant_row.get::<_, bool>(1),
        "ungranted column SELECT should stay invisible"
    );

    let (limited, limited_conn) =
        connect_as(running.bound, "limited", "column_privilege_limited").await;

    let row = limited
        .query_one("SELECT id FROM column_acl WHERE id = 1", &[])
        .await
        .expect("granted column SELECT succeeds");
    assert_eq!(row.get::<_, i32>(0), 1);

    assert_insufficient_privilege(
        limited
            .query_one("SELECT secret FROM column_acl WHERE id = 1", &[])
            .await
            .expect_err("ungranted column SELECT fails"),
    );

    limited
        .execute("INSERT INTO column_acl (id) VALUES ($1)", &[&4_i32])
        .await
        .expect("extended granted column INSERT succeeds");
    assert_insufficient_privilege(
        limited
            .execute("INSERT INTO column_acl (secret) VALUES ($1)", &[&"leak"])
            .await
            .expect_err("extended ungranted column INSERT fails"),
    );

    limited
        .batch_execute("INSERT INTO column_acl (id) VALUES (2)")
        .await
        .expect("granted column INSERT succeeds");
    assert_insufficient_privilege(
        limited
            .batch_execute("INSERT INTO column_acl (secret) VALUES ('leak')")
            .await
            .expect_err("ungranted column INSERT fails"),
    );

    limited
        .batch_execute("UPDATE column_acl SET id = 3 WHERE id = 2")
        .await
        .expect("granted column UPDATE succeeds");
    assert_insufficient_privilege(
        limited
            .batch_execute("UPDATE column_acl SET secret = 'leak' WHERE id = 3")
            .await
            .expect_err("ungranted column UPDATE fails"),
    );
    assert_insufficient_privilege(
        limited
            .execute(
                "UPDATE column_acl SET secret = $1 WHERE id = $2",
                &[&"leak", &3_i32],
            )
            .await
            .expect_err("extended ungranted column UPDATE fails"),
    );

    drop(limited);
    limited_conn.await.expect("limited connection joins");
    shutdown(running).await;
}

#[tokio::test]
async fn role_inheritance_and_set_role_gate_privileges() {
    let running = start_sample_server("role_inheritance_set_role_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register admin role");
    client
        .batch_execute("CREATE ROLE app_group NOLOGIN")
        .await
        .expect("create inherited role");
    client
        .batch_execute("CREATE ROLE support NOLOGIN")
        .await
        .expect("create set-role target");
    client
        .batch_execute("CREATE ROLE outsider NOLOGIN")
        .await
        .expect("create forbidden target");
    client
        .batch_execute("CREATE ROLE app_user LOGIN INHERIT")
        .await
        .expect("create inheriting login role");
    client
        .batch_execute("CREATE ROLE noinherit_user LOGIN NOINHERIT")
        .await
        .expect("create non-inheriting login role");
    client
        .batch_execute("CREATE TABLE role_acl (id INT, secret TEXT)")
        .await
        .expect("create role ACL table");
    client
        .batch_execute("INSERT INTO role_acl (id, secret) VALUES (1, 'hidden')")
        .await
        .expect("seed role ACL row");
    client
        .batch_execute("GRANT SELECT(id) ON TABLE role_acl TO app_group")
        .await
        .expect("grant group column privilege");
    client
        .batch_execute("GRANT SELECT(secret) ON TABLE role_acl TO support")
        .await
        .expect("grant support column privilege");
    client
        .batch_execute("GRANT app_group TO app_user, noinherit_user")
        .await
        .expect("grant role membership");
    client
        .batch_execute("GRANT support TO noinherit_user")
        .await
        .expect("grant set-role membership");

    let inherited = client
        .query_one(
            "SELECT \
                has_column_privilege('app_user', 'role_acl', 'id', 'SELECT'), \
                has_column_privilege('noinherit_user', 'role_acl', 'id', 'SELECT')",
            &[],
        )
        .await
        .expect("inheritance privilege checks");
    assert!(
        inherited.get::<_, bool>(0),
        "INHERIT role should see granted role privileges"
    );
    assert!(
        !inherited.get::<_, bool>(1),
        "NOINHERIT role should not inherit granted role privileges"
    );

    let (app_user, app_conn) =
        connect_as(running.bound, "app_user", "role_inheritance_app_user").await;
    let user_row = app_user
        .query_one("SELECT current_user, session_user", &[])
        .await
        .expect("initial identity functions");
    assert_eq!(user_row.get::<_, String>(0), "app_user");
    assert_eq!(user_row.get::<_, String>(1), "app_user");
    let id_row = app_user
        .query_one("SELECT id FROM role_acl WHERE id = 1", &[])
        .await
        .expect("inherited group SELECT succeeds");
    assert_eq!(id_row.get::<_, i32>(0), 1);
    assert_insufficient_privilege(
        app_user
            .query_one("SELECT secret FROM role_acl WHERE id = 1", &[])
            .await
            .expect_err("ungranted support SELECT fails before SET ROLE"),
    );
    assert_insufficient_privilege(
        app_user
            .batch_execute("SET ROLE outsider")
            .await
            .expect_err("SET ROLE rejects non-member role"),
    );
    assert_insufficient_privilege(
        app_user
            .batch_execute("GRANT support TO outsider")
            .await
            .expect_err("non-CREATEROLE user cannot grant role membership"),
    );
    assert_insufficient_privilege(
        app_user
            .batch_execute("REVOKE app_group FROM app_user")
            .await
            .expect_err("non-CREATEROLE user cannot revoke role membership"),
    );

    let (noinherit, noinherit_conn) = connect_as(
        running.bound,
        "noinherit_user",
        "role_inheritance_noinherit_user",
    )
    .await;
    assert_insufficient_privilege(
        noinherit
            .query_one("SELECT id FROM role_acl WHERE id = 1", &[])
            .await
            .expect_err("NOINHERIT membership does not apply automatically"),
    );
    noinherit
        .batch_execute("SET ROLE support")
        .await
        .expect("SET ROLE to granted role succeeds");
    let switched = noinherit
        .query_one("SELECT current_user, session_user", &[])
        .await
        .expect("switched identity functions");
    assert_eq!(switched.get::<_, String>(0), "support");
    assert_eq!(switched.get::<_, String>(1), "noinherit_user");
    let secret_row = noinherit
        .query_one("SELECT secret FROM role_acl", &[])
        .await
        .expect("SET ROLE support privileges apply");
    assert_eq!(secret_row.get::<_, String>(0), "hidden");
    assert_insufficient_privilege(
        noinherit
            .query_one("SELECT id FROM role_acl", &[])
            .await
            .expect_err("session-user inherited privileges drop under SET ROLE support"),
    );
    noinherit
        .batch_execute("RESET ROLE")
        .await
        .expect("RESET ROLE succeeds");
    let reset = noinherit
        .query_one("SELECT current_user, session_user", &[])
        .await
        .expect("reset identity functions");
    assert_eq!(reset.get::<_, String>(0), "noinherit_user");
    assert_eq!(reset.get::<_, String>(1), "noinherit_user");

    drop(app_user);
    drop(noinherit);
    app_conn.await.expect("app_user connection joins");
    noinherit_conn.await.expect("noinherit connection joins");
    shutdown(running).await;
}

#[tokio::test]
async fn set_role_to_uncataloged_session_user_is_self_reset_compatible() {
    let running = start_sample_server("set_role_self_compatibility_test").await;
    let (client, connection) =
        connect_as(running.bound, "driver_cert", "set_role_self_compatibility").await;

    client
        .batch_execute("SET ROLE 'driver_cert'")
        .await
        .expect("SET ROLE to the session user should be accepted");
    let row = client
        .query_one("SELECT current_user, session_user", &[])
        .await
        .expect("identity functions after self SET ROLE");
    assert_eq!(row.get::<_, String>(0), "driver_cert");
    assert_eq!(row.get::<_, String>(1), "driver_cert");

    drop(client);
    connection.await.expect("driver_cert connection joins");
    shutdown(running).await;
}

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
        .batch_execute("INSERT INTO default_acl_future (id, secret) VALUES (1, 'visible')")
        .await
        .expect("seed future table");

    let granted = client
        .query_one(
            "SELECT has_table_privilege('analyst', 'default_acl_future', 'SELECT')",
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
        .query_one("SELECT secret FROM default_acl_future WHERE id = 1", &[])
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
                has_table_privilege('analyst', 'default_acl_future', 'SELECT'), \
                has_table_privilege('analyst', 'default_acl_later', 'SELECT')",
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

async fn connect_as(
    bound: SocketAddr,
    user: &str,
    application_name: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host={host} port={port} user={user} application_name={application_name}",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    (client, handle)
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
