//! Wire-level ownership checks for table-mutating DDL.

pub mod support;

use std::net::SocketAddr;

use support::{shutdown, start_sample_server};
use tokio_postgres::{NoTls, error::SqlState};

#[tokio::test]
async fn non_owner_cannot_alter_truncate_or_drop_table() {
    let running = start_sample_server("table_ownership_test").await;
    let client = &running.client;

    for sql in [
        "CREATE ROLE tester SUPERUSER LOGIN",
        "CREATE ROLE ddl_owner LOGIN",
        "CREATE ROLE ddl_attacker LOGIN",
        "SET ROLE ddl_owner",
        "CREATE TABLE ddl_owned_table (id INT NOT NULL)",
        "INSERT INTO ddl_owned_table VALUES (1)",
        "CREATE INDEX ddl_owned_idx ON ddl_owned_table (id)",
        "RESET ROLE",
    ] {
        client.batch_execute(sql).await.expect(sql);
    }

    let (attacker, attacker_conn) =
        connect_as(running.bound, "ddl_attacker", "table_ownership_attacker").await;
    assert_insufficient_privilege(
        attacker
            .batch_execute("CREATE INDEX ddl_attacker_idx ON ddl_owned_table (id)")
            .await
            .expect_err("non-owner cannot create index on table"),
    );
    assert_insufficient_privilege(
        attacker
            .batch_execute("DROP INDEX ddl_owned_idx")
            .await
            .expect_err("non-owner cannot drop table index"),
    );
    assert_insufficient_privilege(
        attacker
            .batch_execute("COMMENT ON TABLE ddl_owned_table IS 'stolen docs'")
            .await
            .expect_err("non-owner cannot comment on table"),
    );
    assert_insufficient_privilege(
        attacker
            .batch_execute("COMMENT ON COLUMN ddl_owned_table.id IS 'stolen docs'")
            .await
            .expect_err("non-owner cannot comment on table column"),
    );
    assert_insufficient_privilege(
        attacker
            .batch_execute("COMMENT ON INDEX ddl_owned_idx IS 'stolen docs'")
            .await
            .expect_err("non-owner cannot comment on table index"),
    );
    assert_insufficient_privilege(
        attacker
            .batch_execute("ALTER TABLE ddl_owned_table ADD COLUMN stolen INT")
            .await
            .expect_err("non-owner cannot alter table"),
    );
    assert_insufficient_privilege(
        attacker
            .batch_execute("TRUNCATE TABLE ddl_owned_table")
            .await
            .expect_err("non-owner cannot truncate table"),
    );
    assert_insufficient_privilege(
        attacker
            .batch_execute("DROP TABLE ddl_owned_table")
            .await
            .expect_err("non-owner cannot drop table"),
    );
    drop(attacker);
    attacker_conn.await.expect("attacker connection joins");

    let rows = client
        .query_one("SELECT COUNT(*) FROM ddl_owned_table", &[])
        .await
        .expect("table survived rejected DDL");
    assert_eq!(rows.get::<_, i64>(0), 1);

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
