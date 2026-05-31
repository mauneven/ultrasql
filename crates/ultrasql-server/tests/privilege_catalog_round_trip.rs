//! Wire-level coverage for privilege DDL.

mod support;

use std::net::SocketAddr;

use support::{shutdown, start_persistent_server, start_sample_server};
use tokio_postgres::{NoTls, error::SqlState};
use ultrasql_server::Server;

#[tokio::test]
async fn grant_revoke_privileges_update_catalog_checks() {
    let running = start_sample_server("privilege_catalog_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register admin role");
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
async fn non_owner_cannot_grant_or_revoke_table_privileges() {
    let running = start_sample_server("privilege_catalog_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE ROLE tester SUPERUSER LOGIN")
        .await
        .expect("register admin role");
    client
        .batch_execute("CREATE ROLE acl_owner LOGIN")
        .await
        .expect("create owner role");
    client
        .batch_execute("CREATE ROLE limited_acl LOGIN")
        .await
        .expect("create limited role");
    client
        .batch_execute("CREATE ROLE analyst LOGIN")
        .await
        .expect("create analyst role");
    client
        .batch_execute("SET ROLE acl_owner")
        .await
        .expect("set owner role");
    client
        .batch_execute("CREATE TABLE owned_acl_target (id INT)")
        .await
        .expect("create owner table");
    client
        .batch_execute("RESET ROLE")
        .await
        .expect("reset role");

    let (limited, limited_conn) = connect_as(running.bound, "limited_acl", "non_owner_acl").await;
    assert_insufficient_privilege(
        limited
            .batch_execute("GRANT SELECT ON TABLE owned_acl_target TO analyst")
            .await
            .expect_err("non-owner cannot grant table privileges"),
    );
    assert_insufficient_privilege(
        limited
            .batch_execute("REVOKE SELECT ON TABLE owned_acl_target FROM analyst")
            .await
            .expect_err("non-owner cannot revoke table privileges"),
    );
    drop(limited);
    limited_conn.await.expect("limited connection joins");

    let visible = client
        .query_one(
            "SELECT has_table_privilege('analyst', 'owned_acl_target', 'SELECT')",
            &[],
        )
        .await
        .expect("privilege visibility check");
    assert!(
        !visible.get::<_, bool>(0),
        "failed non-owner GRANT must not persist"
    );

    let (owner, owner_conn) = connect_as(running.bound, "acl_owner", "owner_acl").await;
    owner
        .batch_execute("GRANT SELECT ON TABLE owned_acl_target TO analyst")
        .await
        .expect("owner can grant table privileges");
    drop(owner);
    owner_conn.await.expect("owner connection joins");

    let granted = client
        .query_one(
            "SELECT has_table_privilege('analyst', 'owned_acl_target', 'SELECT')",
            &[],
        )
        .await
        .expect("owner grant visibility check");
    assert!(
        granted.get::<_, bool>(0),
        "owner GRANT must persist table privilege"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn schema_owner_can_grant_and_revoke_schema_create_privilege() {
    let running = start_sample_server("schema_privilege_owner").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE schema_acl_owner LOGIN; \
             CREATE ROLE schema_acl_writer LOGIN; \
             CREATE ROLE schema_acl_intruder LOGIN; \
             SET ROLE schema_acl_owner; \
             CREATE SCHEMA acl_schema; \
             RESET ROLE",
        )
        .await
        .expect("create schema ACL roles and schema");

    let (intruder, intruder_conn) =
        connect_as(running.bound, "schema_acl_intruder", "schema_acl_intruder").await;
    assert_insufficient_privilege(
        intruder
            .batch_execute("GRANT CREATE ON SCHEMA acl_schema TO schema_acl_writer")
            .await
            .expect_err("non-owner cannot grant schema privileges"),
    );
    drop(intruder);
    intruder_conn.await.expect("intruder connection joins");

    let (owner, owner_conn) =
        connect_as(running.bound, "schema_acl_owner", "schema_acl_owner").await;
    owner
        .batch_execute("GRANT CREATE ON SCHEMA acl_schema TO schema_acl_writer")
        .await
        .expect("schema owner can grant CREATE");
    drop(owner);
    owner_conn.await.expect("owner grant connection joins");

    let (writer, writer_conn) =
        connect_as(running.bound, "schema_acl_writer", "schema_acl_writer").await;
    writer
        .batch_execute("CREATE TABLE acl_schema.writer_ok (id INT)")
        .await
        .expect("schema CREATE grant permits qualified table create");
    drop(writer);
    writer_conn.await.expect("writer connection joins");

    let (owner, owner_conn) =
        connect_as(running.bound, "schema_acl_owner", "schema_acl_owner_revoke").await;
    owner
        .batch_execute("REVOKE CREATE ON SCHEMA acl_schema FROM schema_acl_writer")
        .await
        .expect("schema owner can revoke CREATE");
    drop(owner);
    owner_conn.await.expect("owner revoke connection joins");

    let (writer, writer_conn) = connect_as(
        running.bound,
        "schema_acl_writer",
        "schema_acl_writer_after_revoke",
    )
    .await;
    assert_insufficient_privilege(
        writer
            .batch_execute("CREATE SEQUENCE acl_schema.blocked_seq")
            .await
            .expect_err("revoked CREATE prevents later object creation"),
    );
    drop(writer);
    writer_conn.await.expect("writer revoke check joins");

    client
        .batch_execute(
            "DROP TABLE acl_schema.writer_ok; \
             DROP SCHEMA acl_schema; \
             DROP ROLE schema_acl_owner; \
             DROP ROLE schema_acl_writer; \
             DROP ROLE schema_acl_intruder",
        )
        .await
        .expect("cleanup schema privilege owner test");

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

#[test]
fn privilege_metadata_rejects_duplicate_grant_keys_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_privileges.meta"),
        concat!(
            "# ultrasql privilege runtime v1\n",
            "grant\ttable\tdup_acl\tpublic\tselect\t\tultrasql\tfalse\n",
            "grant\ttable\tdup_acl\tpublic\tselect\t\tultrasql\ttrue\n"
        ),
    )
    .expect("write duplicate privilege metadata");

    let err = Server::init(data_dir.path()).expect_err("duplicate privilege metadata rejected");
    assert!(
        err.to_string().contains("duplicate privilege metadata"),
        "expected duplicate privilege metadata rejection, got {err}"
    );
}

#[test]
fn privilege_metadata_rejects_duplicate_default_grant_keys_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_privileges.meta"),
        concat!(
            "# ultrasql privilege runtime v1\n",
            "default\tultrasql\t\ttable\tpublic\tselect\tultrasql\tfalse\n",
            "default\tultrasql\t\ttable\tpublic\tselect\tultrasql\ttrue\n"
        ),
    )
    .expect("write duplicate default privilege metadata");

    let err =
        Server::init(data_dir.path()).expect_err("duplicate default privilege metadata rejected");
    assert!(
        err.to_string()
            .contains("duplicate default privilege metadata"),
        "expected duplicate default privilege metadata rejection, got {err}"
    );
}

#[test]
fn privilege_metadata_rejects_unknown_role_refs_on_rebuild() {
    let cases = [
        (
            "grant grantee",
            "grant\ttable\tacl_target\tmissing_role\tselect\t\tultrasql\tfalse\n",
            "missing_role",
        ),
        (
            "grant grantor",
            "grant\ttable\tacl_target\tpublic\tselect\t\tmissing_role\tfalse\n",
            "missing_role",
        ),
        (
            "default owner",
            "default\tmissing_role\t\ttable\tpublic\tselect\tultrasql\tfalse\n",
            "missing_role",
        ),
        (
            "default grantee",
            "default\tultrasql\t\ttable\tmissing_role\tselect\tultrasql\tfalse\n",
            "missing_role",
        ),
        (
            "default grantor",
            "default\tultrasql\t\ttable\tpublic\tselect\tmissing_role\tfalse\n",
            "missing_role",
        ),
    ];

    for (case, row, role) in cases {
        let data_dir = tempfile::TempDir::new().expect("temp data dir");
        std::fs::write(
            data_dir.path().join("pg_privileges.meta"),
            format!("# ultrasql privilege runtime v1\n{row}"),
        )
        .expect("write privilege metadata with unknown role");

        let err = match Server::init(data_dir.path()) {
            Ok(_) => panic!("{case} should reject unknown role refs"),
            Err(err) => err,
        };
        let message = err.to_string();
        assert!(
            message.contains("unknown privilege metadata role")
                && message.contains(role)
                && message.contains("line 2"),
            "{case} expected unknown role rejection for {role}, got {message}"
        );
    }
}

#[test]
fn privilege_metadata_rejects_unknown_column_refs_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    std::fs::write(
        data_dir.path().join("pg_privileges.meta"),
        concat!(
            "# ultrasql privilege runtime v1\n",
            "grant\ttable\tusers\tpublic\tupdate\tmissing_column\tultrasql\tfalse\n"
        ),
    )
    .expect("write privilege metadata with unknown column");

    let err = match Server::init(data_dir.path()) {
        Ok(_) => panic!("unknown column privilege metadata should be rejected"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(
        message.contains("unknown privilege metadata column")
            && message.contains("missing_column")
            && message.contains("line 2"),
        "expected unknown column rejection, got {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_table_removes_table_privilege_grants() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let metadata_path = data_dir.path().join("pg_privileges.meta");

    let running = start_persistent_server(data_dir.path(), "privilege_drop_table").await;
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
        .batch_execute("CREATE TABLE privilege_drop (id INT, secret TEXT)")
        .await
        .expect("create privilege table");
    client
        .batch_execute("GRANT SELECT ON TABLE privilege_drop TO analyst")
        .await
        .expect("grant table select");
    client
        .batch_execute("GRANT UPDATE(id) ON TABLE privilege_drop TO analyst")
        .await
        .expect("grant column update");
    let before_drop = std::fs::read_to_string(&metadata_path).expect("privilege metadata exists");
    assert!(
        before_drop.contains("privilege_drop"),
        "privilege metadata should record grants before drop: {before_drop}"
    );

    client
        .batch_execute("DROP TABLE privilege_drop")
        .await
        .expect("drop privilege table");
    let stale = client
        .query_one(
            "SELECT \
                has_table_privilege('analyst', 'privilege_drop', 'SELECT'), \
                has_column_privilege('analyst', 'privilege_drop', 'id', 'UPDATE')",
            &[],
        )
        .await
        .expect("privilege checks after drop");
    assert!(
        !stale.get::<_, bool>(0),
        "dropped table must clear object-level grants"
    );
    assert!(
        !stale.get::<_, bool>(1),
        "dropped table must clear column-level grants"
    );
    shutdown(running).await;

    let after_drop = std::fs::read_to_string(&metadata_path).expect("privilege metadata exists");
    assert!(
        !after_drop.contains("privilege_drop"),
        "dropped table grants must be removed from privilege metadata: {after_drop}"
    );

    let running = start_persistent_server(data_dir.path(), "privilege_drop_table_recreate").await;
    running
        .client
        .batch_execute("CREATE TABLE privilege_drop (id INT, secret TEXT)")
        .await
        .expect("recreate privilege table");
    let recreated = running
        .client
        .query_one(
            "SELECT \
                has_table_privilege('analyst', 'privilege_drop', 'SELECT'), \
                has_column_privilege('analyst', 'privilege_drop', 'id', 'UPDATE')",
            &[],
        )
        .await
        .expect("privilege checks after recreate");
    assert!(
        !recreated.get::<_, bool>(0),
        "recreated table must not inherit stale object grant"
    );
    assert!(
        !recreated.get::<_, bool>(1),
        "recreated table must not inherit stale column grant"
    );

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_sequence_removes_sequence_privilege_grants() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let metadata_path = data_dir.path().join("pg_privileges.meta");

    let running = start_persistent_server(data_dir.path(), "privilege_drop_sequence").await;
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
        .batch_execute("CREATE SEQUENCE privilege_drop_seq")
        .await
        .expect("create privilege sequence");
    client
        .batch_execute("GRANT USAGE, SELECT ON SEQUENCE privilege_drop_seq TO analyst")
        .await
        .expect("grant sequence privileges");
    let before_drop = std::fs::read_to_string(&metadata_path).expect("privilege metadata exists");
    assert!(
        before_drop.contains("privilege_drop_seq"),
        "privilege metadata should record sequence grants before drop: {before_drop}"
    );

    client
        .batch_execute("DROP SEQUENCE privilege_drop_seq")
        .await
        .expect("drop privilege sequence");
    let stale = client
        .query_one(
            "SELECT \
                has_sequence_privilege('analyst', 'privilege_drop_seq', 'USAGE'), \
                has_sequence_privilege('analyst', 'privilege_drop_seq', 'SELECT')",
            &[],
        )
        .await
        .expect("privilege checks after sequence drop");
    assert!(
        !stale.get::<_, bool>(0),
        "dropped sequence must clear USAGE grants"
    );
    assert!(
        !stale.get::<_, bool>(1),
        "dropped sequence must clear SELECT grants"
    );
    shutdown(running).await;

    let after_drop = std::fs::read_to_string(&metadata_path).expect("privilege metadata exists");
    assert!(
        !after_drop.contains("privilege_drop_seq"),
        "dropped sequence grants must be removed from privilege metadata: {after_drop}"
    );

    let running =
        start_persistent_server(data_dir.path(), "privilege_drop_sequence_recreate").await;
    running
        .client
        .batch_execute("CREATE SEQUENCE privilege_drop_seq")
        .await
        .expect("recreate privilege sequence");
    let recreated = running
        .client
        .query_one(
            "SELECT \
                has_sequence_privilege('analyst', 'privilege_drop_seq', 'USAGE'), \
                has_sequence_privilege('analyst', 'privilege_drop_seq', 'SELECT')",
            &[],
        )
        .await
        .expect("privilege checks after sequence recreate");
    assert!(
        !recreated.get::<_, bool>(0),
        "recreated sequence must not inherit stale USAGE grant"
    );
    assert!(
        !recreated.get::<_, bool>(1),
        "recreated sequence must not inherit stale SELECT grant"
    );

    shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_table_removes_owned_sequence_privilege_grants() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    let metadata_path = data_dir.path().join("pg_privileges.meta");
    let sequence_name = "privilege_owned_seq_id_seq";

    let running = start_persistent_server(data_dir.path(), "privilege_owned_sequence").await;
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
        .batch_execute("CREATE TABLE privilege_owned_seq (id SERIAL)")
        .await
        .expect("create serial table");
    client
        .batch_execute("GRANT USAGE ON SEQUENCE privilege_owned_seq_id_seq TO analyst")
        .await
        .expect("grant owned sequence usage");
    let before_drop = std::fs::read_to_string(&metadata_path).expect("privilege metadata exists");
    assert!(
        before_drop.contains(sequence_name),
        "privilege metadata should record owned-sequence grant before drop: {before_drop}"
    );

    client
        .batch_execute("DROP TABLE privilege_owned_seq")
        .await
        .expect("drop serial table");
    let stale = client
        .query_one(
            "SELECT has_sequence_privilege('analyst', 'privilege_owned_seq_id_seq', 'USAGE')",
            &[],
        )
        .await
        .expect("owned sequence privilege check after table drop");
    assert!(
        !stale.get::<_, bool>(0),
        "dropping a table must clear grants on its owned sequence"
    );
    shutdown(running).await;

    let after_drop = std::fs::read_to_string(&metadata_path).expect("privilege metadata exists");
    assert!(
        !after_drop.contains(sequence_name),
        "owned sequence grants must be removed from privilege metadata: {after_drop}"
    );

    let running =
        start_persistent_server(data_dir.path(), "privilege_owned_sequence_recreate").await;
    running
        .client
        .batch_execute("CREATE TABLE privilege_owned_seq (id SERIAL)")
        .await
        .expect("recreate serial table");
    let recreated = running
        .client
        .query_one(
            "SELECT has_sequence_privilege('analyst', 'privilege_owned_seq_id_seq', 'USAGE')",
            &[],
        )
        .await
        .expect("owned sequence privilege check after table recreate");
    assert!(
        !recreated.get::<_, bool>(0),
        "recreated owned sequence must not inherit stale grant"
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
async fn set_role_to_uncataloged_session_user_resets_to_self() {
    let running = start_sample_server("set_role_self_reset_test").await;
    let (client, connection) =
        connect_as(running.bound, "driver_cert", "set_role_self_reset").await;

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
        .batch_execute("CREATE SCHEMA tenant")
        .await
        .expect("create tenant schema");
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
            "SELECT has_table_privilege('analyst', 'after_schema_recreate', 'SELECT')",
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
            "SELECT has_table_privilege('analyst', 'priv_restart_future', 'SELECT')",
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

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn privilege_catalog_rolls_back_when_metadata_slot_is_unsafe() {
    use std::os::unix::fs::symlink;

    let data_dir = tempfile::TempDir::new().expect("temp data dir");
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
