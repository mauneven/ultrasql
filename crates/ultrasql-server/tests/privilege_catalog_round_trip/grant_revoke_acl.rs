use super::*;

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
                has_sequence_privilege('analyst', 'public.grant_seq', 'USAGE'), \
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
        "public-qualified sequence USAGE grant should persist"
    );
    assert!(
        row.get::<_, bool>(7),
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
async fn sequence_owner_can_grant_and_revoke_sequence_privileges() {
    let running = start_sample_server("sequence_privilege_owner").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE seq_acl_owner LOGIN; \
             CREATE ROLE seq_acl_reader LOGIN; \
             CREATE ROLE seq_acl_intruder LOGIN",
        )
        .await
        .expect("create sequence ACL roles");

    let (owner, owner_conn) =
        connect_as(running.bound, "seq_acl_owner", "seq_acl_owner_create").await;
    owner
        .batch_execute("CREATE SEQUENCE seq_acl_owned START WITH 10")
        .await
        .expect("owner creates sequence");
    drop(owner);
    owner_conn.await.expect("owner create connection joins");

    let (intruder, intruder_conn) =
        connect_as(running.bound, "seq_acl_intruder", "seq_acl_intruder").await;
    assert_insufficient_privilege(
        intruder
            .batch_execute("GRANT USAGE ON SEQUENCE seq_acl_owned TO seq_acl_reader")
            .await
            .expect_err("non-owner cannot grant sequence privileges"),
    );
    drop(intruder);
    intruder_conn.await.expect("intruder connection joins");

    let (owner, owner_conn) =
        connect_as(running.bound, "seq_acl_owner", "seq_acl_owner_grant").await;
    owner
        .batch_execute("GRANT USAGE ON SEQUENCE seq_acl_owned TO seq_acl_reader")
        .await
        .expect("sequence owner can grant USAGE");
    drop(owner);
    owner_conn.await.expect("owner grant connection joins");

    let (reader, reader_conn) = connect_as(running.bound, "seq_acl_reader", "seq_acl_reader").await;
    assert!(
        reader
            .query_one(
                "SELECT has_sequence_privilege('seq_acl_reader', 'seq_acl_owned', 'USAGE')",
                &[],
            )
            .await
            .expect("query sequence privilege")
            .get::<_, bool>(0)
    );
    drop(reader);
    reader_conn.await.expect("reader connection joins");

    let (owner, owner_conn) =
        connect_as(running.bound, "seq_acl_owner", "seq_acl_owner_revoke").await;
    owner
        .batch_execute("REVOKE USAGE ON SEQUENCE seq_acl_owned FROM seq_acl_reader")
        .await
        .expect("sequence owner can revoke USAGE");
    drop(owner);
    owner_conn.await.expect("owner revoke connection joins");

    let revoked = client
        .query_one(
            "SELECT has_sequence_privilege('seq_acl_reader', 'seq_acl_owned', 'USAGE')",
            &[],
        )
        .await
        .expect("query revoked sequence privilege")
        .get::<_, bool>(0);
    assert!(!revoked);

    client
        .batch_execute(
            "DROP SEQUENCE seq_acl_owned; \
             DROP ROLE seq_acl_owner; \
             DROP ROLE seq_acl_reader; \
             DROP ROLE seq_acl_intruder",
        )
        .await
        .expect("cleanup sequence privilege owner test");

    shutdown(running).await;
}

#[tokio::test]
async fn delete_requires_table_delete_privilege() {
    let running = start_sample_server("delete_privilege_gate").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE delete_acl_owner LOGIN; \
             CREATE ROLE delete_acl_user LOGIN; \
             SET ROLE delete_acl_owner; \
             CREATE TABLE delete_acl_docs (id INT); \
             INSERT INTO delete_acl_docs VALUES (1), (2); \
             RESET ROLE",
        )
        .await
        .expect("create delete privilege table");

    let (user, user_conn) =
        connect_as(running.bound, "delete_acl_user", "delete_acl_user_blocked").await;
    assert_insufficient_privilege(
        user.batch_execute("DELETE FROM delete_acl_docs")
            .await
            .expect_err("DELETE privilege required"),
    );
    drop(user);
    user_conn.await.expect("blocked delete connection joins");

    let (owner, owner_conn) =
        connect_as(running.bound, "delete_acl_owner", "delete_acl_owner_grant").await;
    owner
        .batch_execute("GRANT DELETE ON TABLE delete_acl_docs TO delete_acl_user")
        .await
        .expect("owner grants DELETE");
    drop(owner);
    owner_conn.await.expect("owner grant connection joins");

    let (user, user_conn) =
        connect_as(running.bound, "delete_acl_user", "delete_acl_user_allowed").await;
    user.batch_execute("DELETE FROM delete_acl_docs")
        .await
        .expect("DELETE grant permits delete");
    drop(user);
    user_conn.await.expect("allowed delete connection joins");

    let remaining = client
        .query_one("SELECT COUNT(*) FROM delete_acl_docs", &[])
        .await
        .expect("query rows after delete")
        .get::<_, i64>(0);
    assert_eq!(remaining, 0);

    client
        .batch_execute(
            "DROP TABLE delete_acl_docs; DROP ROLE delete_acl_owner; DROP ROLE delete_acl_user",
        )
        .await
        .expect("cleanup delete privilege test");

    shutdown(running).await;
}

#[tokio::test]
async fn truncate_accepts_table_truncate_privilege() {
    let running = start_sample_server("truncate_privilege_gate").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE truncate_acl_owner LOGIN; \
             CREATE ROLE truncate_acl_user LOGIN; \
             SET ROLE truncate_acl_owner; \
             CREATE TABLE truncate_acl_docs (id INT); \
             INSERT INTO truncate_acl_docs VALUES (1), (2); \
             GRANT TRUNCATE ON TABLE truncate_acl_docs TO truncate_acl_user; \
             RESET ROLE",
        )
        .await
        .expect("create truncate privilege table");

    let (user, user_conn) = connect_as(
        running.bound,
        "truncate_acl_user",
        "truncate_acl_user_allowed",
    )
    .await;
    user.batch_execute("TRUNCATE TABLE truncate_acl_docs")
        .await
        .expect("TRUNCATE grant permits truncate");
    drop(user);
    user_conn.await.expect("truncate connection joins");

    let remaining = client
        .query_one("SELECT COUNT(*) FROM truncate_acl_docs", &[])
        .await
        .expect("query rows after truncate")
        .get::<_, i64>(0);
    assert_eq!(remaining, 0);

    client
        .batch_execute(
            "DROP TABLE truncate_acl_docs; DROP ROLE truncate_acl_owner; DROP ROLE truncate_acl_user",
        )
        .await
        .expect("cleanup truncate privilege test");

    shutdown(running).await;
}

#[tokio::test]
async fn column_less_reads_require_table_select_privilege() {
    // #17: `count(*)`, `SELECT 1`, and `EXISTS (SELECT 1 FROM t)` read a table
    // without observing any column. Without SELECT they must be denied, not
    // silently leak row counts / existence.
    let running = start_sample_server("column_less_select_gate").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE colless_owner LOGIN; \
             CREATE ROLE colless_user LOGIN; \
             SET ROLE colless_owner; \
             CREATE TABLE colless_secret (id INT, val TEXT); \
             INSERT INTO colless_secret VALUES (1, 'a'), (2, 'b'); \
             CREATE TABLE colless_other (id INT); \
             INSERT INTO colless_other VALUES (1); \
             RESET ROLE",
        )
        .await
        .expect("create column-less select tables");
    // colless_user may read colless_other but has NO grant on colless_secret.
    let (owner, owner_conn) =
        connect_as(running.bound, "colless_owner", "colless_other_grant").await;
    owner
        .batch_execute("GRANT SELECT ON TABLE colless_other TO colless_user")
        .await
        .expect("owner grants SELECT on other");
    drop(owner);
    owner_conn.await.expect("owner-other connection joins");

    // No SELECT on colless_secret: every column-less read must be denied.
    let (user, user_conn) = connect_as(running.bound, "colless_user", "colless_blocked").await;
    assert_insufficient_privilege(
        user.batch_execute("SELECT count(*) FROM colless_secret")
            .await
            .expect_err("count(*) needs SELECT"),
    );
    assert_insufficient_privilege(
        user.batch_execute("SELECT 1 FROM colless_secret")
            .await
            .expect_err("SELECT 1 needs SELECT"),
    );
    assert_insufficient_privilege(
        user.batch_execute("SELECT EXISTS (SELECT 1 FROM colless_secret)")
            .await
            .expect_err("EXISTS subquery needs SELECT"),
    );
    // Reading a column the user IS allowed (other) but probing secret via a
    // correlated EXISTS still requires SELECT on secret.
    assert_insufficient_privilege(
        user.batch_execute(
            "SELECT * FROM colless_other WHERE EXISTS (SELECT 1 FROM colless_secret)",
        )
        .await
        .expect_err("EXISTS over secret needs SELECT on secret"),
    );
    drop(user);
    user_conn.await.expect("blocked connection joins");

    // Grant SELECT on colless_secret: the same reads now succeed.
    let (owner, owner_conn) = connect_as(running.bound, "colless_owner", "colless_grant").await;
    owner
        .batch_execute("GRANT SELECT ON TABLE colless_secret TO colless_user")
        .await
        .expect("owner grants SELECT");
    drop(owner);
    owner_conn.await.expect("owner connection joins");

    let (user, user_conn) = connect_as(running.bound, "colless_user", "colless_allowed").await;
    let count = user
        .query_one("SELECT count(*) FROM colless_secret", &[])
        .await
        .expect("count(*) permitted after grant")
        .get::<_, i64>(0);
    assert_eq!(count, 2);
    user.batch_execute("SELECT 1 FROM colless_secret")
        .await
        .expect("SELECT 1 permitted after grant");
    user.batch_execute("SELECT EXISTS (SELECT 1 FROM colless_secret)")
        .await
        .expect("EXISTS permitted after grant");
    drop(user);
    user_conn.await.expect("allowed connection joins");

    client
        .batch_execute(
            "DROP TABLE colless_secret; DROP TABLE colless_other; \
             DROP ROLE colless_owner; DROP ROLE colless_user",
        )
        .await
        .expect("cleanup column-less select test");

    shutdown(running).await;
}
