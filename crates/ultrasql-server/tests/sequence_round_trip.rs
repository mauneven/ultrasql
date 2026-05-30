//! End-to-end sequence DDL and function tests.

use tokio_postgres::NoTls;

mod support;

use support::{shutdown as graceful_shutdown, start_persistent_server, start_sample_server};

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
