//! End-to-end sequence DDL and function tests.

use tokio_postgres::NoTls;

pub mod support;

use support::{
    connect_as, shutdown as graceful_shutdown, start_persistent_server, start_sample_server,
};
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

#[tokio::test]
async fn create_sequence_nextval_currval_setval_and_drop() {
    let running = start_sample_server("sequence_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE SEQUENCE s START WITH 10 INCREMENT BY 5")
        .await
        .expect("create sequence");

    assert_eq!(simple_i64(client, "SELECT nextval('s')").await, 10);
    assert_eq!(simple_i64(client, "SELECT nextval('s')").await, 15);
    assert_eq!(simple_i64(client, "SELECT currval('s')").await, 15);
    assert_eq!(simple_i64(client, "SELECT lastval()").await, 15);
    assert_eq!(
        simple_i64(client, "SELECT setval('s', 40, false)").await,
        40
    );
    assert_eq!(simple_i64(client, "SELECT nextval('s')").await, 40);

    client
        .batch_execute("DROP SEQUENCE s")
        .await
        .expect("drop sequence");
    client
        .batch_execute("DROP SEQUENCE IF EXISTS s")
        .await
        .expect("drop sequence if exists");

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn sequence_functions_preserve_quoted_dot_in_public_name() {
    let running = start_sample_server("sequence_dotted_public_name").await;
    let client = &running.client;

    client
        .batch_execute("CREATE SEQUENCE \"seq.dot\" START WITH 3")
        .await
        .expect("create quoted dotted sequence");

    assert_eq!(simple_i64(client, "SELECT nextval('\"seq.dot\"')").await, 3);
    assert_eq!(simple_i64(client, "SELECT currval('\"seq.dot\"')").await, 3);

    let catalog_name = client
        .query_one(
            "SELECT sequencename FROM pg_catalog.pg_sequences \
             WHERE schemaname = 'public' AND sequencename = 'seq.dot'",
            &[],
        )
        .await
        .expect("quoted dotted sequence appears in pg_sequences")
        .get::<_, String>(0);
    assert_eq!(catalog_name, "seq.dot");

    graceful_shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_sequence_stays_dropped_after_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "sequence_drop_restart_setup").await;
    running
        .client
        .batch_execute("CREATE SEQUENCE seq_drop_restart START WITH 7")
        .await
        .expect("create sequence");
    assert_eq!(
        simple_i64(&running.client, "SELECT nextval('seq_drop_restart')").await,
        7
    );
    running
        .client
        .batch_execute("DROP SEQUENCE seq_drop_restart")
        .await
        .expect("drop sequence");
    graceful_shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "sequence_drop_restart_verify").await;
    running
        .client
        .simple_query("SELECT nextval('seq_drop_restart')")
        .await
        .expect_err("dropped sequence must not restart");
    running
        .client
        .batch_execute("CREATE SEQUENCE seq_drop_restart START WITH 11")
        .await
        .expect("recreate sequence after restart");
    assert_eq!(
        simple_i64(&running.client, "SELECT nextval('seq_drop_restart')").await,
        11
    );
    graceful_shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequence_owner_survives_restart_in_catalog_views() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "sequence_owner_restart_setup").await;
    running
        .client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE persisted_sequence_owner LOGIN; \
             SET ROLE persisted_sequence_owner; \
             CREATE SEQUENCE persisted_owner_sequence; \
             RESET ROLE",
        )
        .await
        .expect("create owned sequence");
    let owner = running
        .client
        .query_one(
            "SELECT sequenceowner \
             FROM pg_catalog.pg_sequences \
             WHERE sequencename = 'persisted_owner_sequence'",
            &[],
        )
        .await
        .expect("query sequence owner before restart")
        .get::<_, String>(0);
    assert_eq!(owner, "persisted_sequence_owner");
    graceful_shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "sequence_owner_restart_verify").await;
    let owner = running
        .client
        .query_one(
            "SELECT sequenceowner \
             FROM pg_catalog.pg_sequences \
             WHERE sequencename = 'persisted_owner_sequence'",
            &[],
        )
        .await
        .expect("query sequence owner after restart")
        .get::<_, String>(0);
    assert_eq!(owner, "persisted_sequence_owner");
    graceful_shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequence_owner_metadata_rejects_unknown_owner_on_rebuild() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");

    let running = start_persistent_server(data_dir.path(), "sequence_owner_orphan_setup").await;
    running
        .client
        .batch_execute("CREATE SEQUENCE orphan_owner_sequence")
        .await
        .expect("create sequence");
    graceful_shutdown(running).await;

    std::fs::write(
        data_dir.path().join("pg_sequence_owner.meta"),
        concat!(
            "# ultrasql sequence owners v2\n",
            "sequence\torphan_owner_sequence\tmissing_owner\tpublic\n"
        ),
    )
    .expect("write orphaned sequence owner metadata");

    let err = Server::init(data_dir.path()).expect_err("orphaned sequence owner rejected");
    assert!(
        err.to_string()
            .contains("unknown sequence owner metadata role 'missing_owner'"),
        "expected unknown sequence owner rejection, got {err}"
    );
}

#[tokio::test]
async fn non_owner_cannot_alter_or_drop_sequence() {
    let running = start_sample_server("sequence_owner_guard").await;

    running
        .client
        .batch_execute(
            "CREATE ROLE sequence_owner LOGIN; \
             CREATE ROLE sequence_attacker LOGIN",
        )
        .await
        .expect("create sequence test roles");

    let (owner, owner_conn) = connect_as(
        running.bound,
        "sequence_owner",
        "sequence_owner_guard_owner",
    )
    .await;
    owner
        .batch_execute("CREATE SEQUENCE private_sequence START WITH 5")
        .await
        .expect("owner creates sequence");
    drop(owner);
    owner_conn.await.expect("owner connection joins");

    let (attacker, attacker_conn) = connect_as(
        running.bound,
        "sequence_attacker",
        "sequence_owner_guard_attacker",
    )
    .await;
    let alter_err = attacker
        .batch_execute("ALTER SEQUENCE private_sequence INCREMENT BY 3")
        .await
        .expect_err("non-owner cannot alter sequence");
    assert_eq!(alter_err.code().expect("SQLSTATE").code(), "42501");
    let drop_err = attacker
        .batch_execute("DROP SEQUENCE private_sequence")
        .await
        .expect_err("non-owner cannot drop sequence");
    assert_eq!(drop_err.code().expect("SQLSTATE").code(), "42501");
    drop(attacker);
    attacker_conn.await.expect("attacker connection joins");

    let (owner, owner_conn) = connect_as(
        running.bound,
        "sequence_owner",
        "sequence_owner_guard_cleanup",
    )
    .await;
    assert_eq!(
        simple_i64(&owner, "SELECT nextval('private_sequence')").await,
        5
    );
    owner
        .batch_execute("DROP SEQUENCE private_sequence")
        .await
        .expect("owner can drop sequence");
    drop(owner);
    owner_conn.await.expect("owner cleanup connection joins");

    running
        .client
        .batch_execute("DROP ROLE sequence_owner; DROP ROLE sequence_attacker")
        .await
        .expect("drop sequence test roles");

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn sequence_functions_enforce_usage_and_update_privileges() {
    let running = start_sample_server("sequence_function_acl").await;

    running
        .client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE seq_fn_owner LOGIN; \
             CREATE ROLE seq_fn_reader LOGIN; \
             CREATE ROLE seq_fn_blocked LOGIN",
        )
        .await
        .expect("create sequence function ACL roles");

    let (owner, owner_conn) = connect_as(
        running.bound,
        "seq_fn_owner",
        "sequence_function_owner_create",
    )
    .await;
    owner
        .batch_execute("CREATE SEQUENCE seq_fn_acl START WITH 10")
        .await
        .expect("owner creates sequence");
    drop(owner);
    owner_conn.await.expect("owner create connection joins");

    let (blocked, blocked_conn) =
        connect_as(running.bound, "seq_fn_blocked", "sequence_function_blocked").await;
    let err = blocked
        .simple_query("SELECT nextval('seq_fn_acl')")
        .await
        .expect_err("missing sequence USAGE rejects nextval");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42501");
    drop(blocked);
    blocked_conn.await.expect("blocked connection joins");

    let (owner, owner_conn) = connect_as(
        running.bound,
        "seq_fn_owner",
        "sequence_function_owner_grant",
    )
    .await;
    owner
        .batch_execute("GRANT USAGE ON SEQUENCE seq_fn_acl TO seq_fn_reader")
        .await
        .expect("owner grants USAGE");
    drop(owner);
    owner_conn.await.expect("owner grant connection joins");

    let (reader, reader_conn) =
        connect_as(running.bound, "seq_fn_reader", "sequence_function_reader").await;
    assert_eq!(
        simple_i64(&reader, "SELECT nextval('seq_fn_acl')").await,
        10
    );
    assert_eq!(
        simple_i64(&reader, "SELECT currval('seq_fn_acl')").await,
        10
    );
    let err = reader
        .simple_query("SELECT setval('seq_fn_acl', 50)")
        .await
        .expect_err("USAGE alone rejects setval");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42501");

    let (owner, owner_conn) = connect_as(
        running.bound,
        "seq_fn_owner",
        "sequence_function_owner_update",
    )
    .await;
    owner
        .batch_execute("GRANT UPDATE ON SEQUENCE seq_fn_acl TO seq_fn_reader")
        .await
        .expect("owner grants UPDATE");
    drop(owner);
    owner_conn
        .await
        .expect("owner update grant connection joins");

    assert_eq!(
        simple_i64(&reader, "SELECT setval('seq_fn_acl', 50)").await,
        50
    );
    drop(reader);
    reader_conn.await.expect("reader connection joins");

    running
        .client
        .batch_execute(
            "DROP SEQUENCE seq_fn_acl; \
             DROP ROLE seq_fn_owner; \
             DROP ROLE seq_fn_reader; \
             DROP ROLE seq_fn_blocked",
        )
        .await
        .expect("cleanup sequence function ACL test");

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn sequence_functions_require_schema_usage_privilege() {
    let running = start_sample_server("sequence_schema_usage_acl").await;

    running
        .client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE seq_schema_owner LOGIN; \
             CREATE ROLE seq_schema_reader LOGIN; \
             SET ROLE seq_schema_owner; \
             CREATE SCHEMA seq_schema_acl; \
             CREATE SEQUENCE seq_schema_acl.seq_schema_private START WITH 21; \
             RESET ROLE; \
             GRANT USAGE ON SEQUENCE seq_schema_acl.seq_schema_private TO seq_schema_reader",
        )
        .await
        .expect("create private sequence usage test");

    let (reader, reader_conn) = connect_as(
        running.bound,
        "seq_schema_reader",
        "sequence_schema_usage_reader_blocked",
    )
    .await;
    let err = reader
        .simple_query("SELECT nextval('seq_schema_acl.seq_schema_private')")
        .await
        .expect_err("schema USAGE required despite sequence USAGE");
    assert_eq!(err.code().expect("SQLSTATE").code(), "42501");
    drop(reader);
    reader_conn
        .await
        .expect("blocked sequence schema usage reader joins");

    running
        .client
        .batch_execute("GRANT USAGE ON SCHEMA seq_schema_acl TO seq_schema_reader")
        .await
        .expect("grant sequence schema usage");

    let (reader, reader_conn) = connect_as(
        running.bound,
        "seq_schema_reader",
        "sequence_schema_usage_reader_allowed",
    )
    .await;
    assert_eq!(
        simple_i64(
            &reader,
            "SELECT nextval('seq_schema_acl.seq_schema_private')"
        )
        .await,
        21
    );
    drop(reader);
    reader_conn
        .await
        .expect("allowed sequence schema usage reader joins");

    running
        .client
        .batch_execute(
            "DROP SEQUENCE seq_schema_acl.seq_schema_private; \
             DROP SCHEMA seq_schema_acl; \
             DROP ROLE seq_schema_owner; \
             DROP ROLE seq_schema_reader",
        )
        .await
        .expect("cleanup private sequence usage test");

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn grant_sequence_respects_schema_qualifier() {
    let running = start_sample_server("sequence_grant_schema_qualifier_guard").await;

    running
        .client
        .batch_execute(
            "CREATE ROLE tester SUPERUSER LOGIN; \
             CREATE ROLE seq_qualifier_reader LOGIN; \
             CREATE SCHEMA app; \
             CREATE SEQUENCE seq_grant_guard START WITH 31",
        )
        .await
        .expect("create public sequence and separate schema");

    running
        .client
        .batch_execute("GRANT USAGE ON SEQUENCE app.seq_grant_guard TO seq_qualifier_reader")
        .await
        .expect_err("qualified sequence GRANT must not target public same-name sequence");

    let (reader, reader_conn) = connect_as(
        running.bound,
        "seq_qualifier_reader",
        "sequence_qualifier_reader",
    )
    .await;
    reader
        .simple_query("SELECT nextval('seq_grant_guard')")
        .await
        .expect_err("rejected qualified sequence GRANT must not persist");
    drop(reader);
    reader_conn.await.expect("sequence reader joins");

    running
        .client
        .batch_execute(
            "DROP SEQUENCE seq_grant_guard; \
             DROP SCHEMA app; \
             DROP ROLE tester; \
             DROP ROLE seq_qualifier_reader",
        )
        .await
        .expect("cleanup sequence privilege qualifier guard");

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn alter_sequence_changes_increment() {
    let running = start_sample_server("sequence_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE SEQUENCE s START WITH 1")
        .await
        .expect("create sequence");
    assert_eq!(simple_i64(client, "SELECT nextval('s')").await, 1);
    client
        .batch_execute("ALTER SEQUENCE s INCREMENT BY 10")
        .await
        .expect("alter sequence");
    assert_eq!(simple_i64(client, "SELECT nextval('s')").await, 11);

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn alter_and_drop_sequence_respect_schema_qualifier() {
    let running = start_sample_server("sequence_schema_qualifier_guard").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE SCHEMA app; \
             CREATE SEQUENCE guarded_seq START WITH 5",
        )
        .await
        .expect("create public sequence and separate schema");

    client
        .batch_execute("ALTER SEQUENCE app.guarded_seq INCREMENT BY 10")
        .await
        .expect_err("qualified ALTER SEQUENCE must not resolve public sequence");
    assert_eq!(simple_i64(client, "SELECT nextval('guarded_seq')").await, 5);
    assert_eq!(simple_i64(client, "SELECT nextval('guarded_seq')").await, 6);

    client
        .batch_execute("DROP SEQUENCE app.guarded_seq")
        .await
        .expect_err("qualified DROP SEQUENCE must not resolve public sequence");
    assert_eq!(simple_i64(client, "SELECT nextval('guarded_seq')").await, 7);

    client
        .batch_execute("DROP SEQUENCE guarded_seq; DROP SCHEMA app")
        .await
        .expect("cleanup sequence qualifier guard");

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn alter_sequence_start_and_restart_follow_postgres_shape() {
    let running = start_sample_server("sequence_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE SEQUENCE s START WITH 1")
        .await
        .expect("create sequence");
    assert_eq!(simple_i64(client, "SELECT nextval('s')").await, 1);
    client
        .batch_execute("ALTER SEQUENCE s START WITH 50")
        .await
        .expect("alter start");
    assert_eq!(
        simple_i64(client, "SELECT nextval('s')").await,
        2,
        "START WITH changes restart seed, not current value"
    );
    client
        .batch_execute("ALTER SEQUENCE s RESTART")
        .await
        .expect("restart at configured start");
    assert_eq!(simple_i64(client, "SELECT nextval('s')").await, 50);
    client
        .batch_execute("ALTER SEQUENCE s RESTART WITH 7")
        .await
        .expect("restart with explicit value");
    assert_eq!(simple_i64(client, "SELECT nextval('s')").await, 7);

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn currval_is_session_local_but_nextval_is_global() {
    let running = start_sample_server("sequence_test").await;
    let a = &running.client;
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=sequence_test",
        host = running.bound.ip(),
        port = running.bound.port()
    );
    let (b, b_conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect b");
    let b_handle = tokio::spawn(async move {
        let _ = b_conn.await;
    });

    a.batch_execute("CREATE SEQUENCE s")
        .await
        .expect("create sequence");
    assert_eq!(simple_i64(a, "SELECT nextval('s')").await, 1);

    let b_currval = b
        .simple_query("SELECT currval('s')")
        .await
        .expect_err("b currval before nextval fails");
    assert_eq!(b_currval.code().expect("SQLSTATE").code(), "55000");

    assert_eq!(simple_i64(&b, "SELECT nextval('s')").await, 2);

    drop(b);
    b_handle.await.expect("b connection task joins");
    graceful_shutdown(running).await;
}

#[tokio::test]
async fn descending_sequence_uses_maxvalue_default_start() {
    let running = start_sample_server("sequence_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE SEQUENCE s INCREMENT BY -1 MAXVALUE 5")
        .await
        .expect("create descending sequence");
    assert_eq!(simple_i64(client, "SELECT nextval('s')").await, 5);
    assert_eq!(simple_i64(client, "SELECT nextval('s')").await, 4);

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn serial_column_creates_sequence_default_and_updates_currval() {
    let running = start_sample_server("sequence_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id SERIAL, v INT)")
        .await
        .expect("create table with serial");
    client
        .batch_execute("INSERT INTO t (v) VALUES (10), (20)")
        .await
        .expect("insert rows using serial default");

    let rows = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("select serial rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, i32>(1), 20);
    assert_eq!(simple_i64(client, "SELECT currval('t_id_seq')").await, 2);

    client
        .batch_execute("DROP TABLE t")
        .await
        .expect("drop table");
    client
        .simple_query("SELECT nextval('t_id_seq')")
        .await
        .expect_err("owned serial sequence dropped with table");

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn drop_sequence_restricts_and_cascade_detaches_serial_default() {
    let running = start_sample_server("sequence_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t_seq_dep (id SERIAL, v INT)")
        .await
        .expect("create serial table");
    client
        .batch_execute("INSERT INTO t_seq_dep (v) VALUES (10)")
        .await
        .expect("insert using serial default");

    let restricted = client
        .batch_execute("DROP SEQUENCE t_seq_dep_id_seq")
        .await
        .expect_err("sequence default dependency must restrict drop");
    assert_eq!(restricted.code().expect("SQLSTATE").code(), "2BP01");

    client
        .batch_execute("DROP SEQUENCE t_seq_dep_id_seq CASCADE")
        .await
        .expect("cascade detaches serial default");
    client
        .batch_execute("INSERT INTO t_seq_dep (id, v) VALUES (7, 70)")
        .await
        .expect("explicit insert still works after default detached");
    client
        .simple_query("SELECT nextval('t_seq_dep_id_seq')")
        .await
        .expect_err("dropped sequence is gone");

    let rows = client
        .query("SELECT id, v FROM t_seq_dep ORDER BY v", &[])
        .await
        .expect("select rows after sequence cascade");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i32>(0), 7);
    assert_eq!(rows[1].get::<_, i32>(1), 70);

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn generated_always_identity_uses_sequence_and_rejects_explicit_values() {
    let running = start_sample_server("sequence_test").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE t (\
             id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 5), \
             v INT)",
        )
        .await
        .expect("create identity table");
    client
        .batch_execute("INSERT INTO t (v) VALUES (10), (20)")
        .await
        .expect("insert rows using identity default");

    let rows = client
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .await
        .expect("select identity rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i64>(0), 10);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i64>(0), 15);
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    let err = client
        .batch_execute("INSERT INTO t (id, v) VALUES (99, 30)")
        .await
        .expect_err("GENERATED ALWAYS rejects explicit identity value");
    assert_eq!(err.code().expect("SQLSTATE").code(), "428C9");

    graceful_shutdown(running).await;
}

#[tokio::test]
async fn generated_by_default_identity_allows_explicit_values() {
    let running = start_sample_server("sequence_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY, v INT)")
        .await
        .expect("create identity table");
    client
        .batch_execute("INSERT INTO t (id, v) VALUES (42, 10)")
        .await
        .expect("explicit by-default identity value accepted");
    client
        .batch_execute("INSERT INTO t (v) VALUES (20)")
        .await
        .expect("omitted by-default identity value uses sequence");

    let rows = client
        .query("SELECT id, v FROM t ORDER BY v", &[])
        .await
        .expect("select identity rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 42);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    graceful_shutdown(running).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identity_default_survives_restart() {
    let data_dir = tempfile::TempDir::new().unwrap();

    let running = start_persistent_server(data_dir.path(), "sequence_restart_test").await;
    running
        .client
        .batch_execute("CREATE TABLE seq_restart (id INT GENERATED BY DEFAULT AS IDENTITY, v INT)")
        .await
        .expect("create identity table");
    running
        .client
        .batch_execute("INSERT INTO seq_restart (v) VALUES (10)")
        .await
        .expect("first default insert");
    graceful_shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "sequence_restart_test").await;
    running
        .client
        .batch_execute("INSERT INTO seq_restart (v) VALUES (20)")
        .await
        .expect("default insert after restart");
    let rows = running
        .client
        .query("SELECT id, v FROM seq_restart ORDER BY v", &[])
        .await
        .expect("select rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    graceful_shutdown(running).await;
}
