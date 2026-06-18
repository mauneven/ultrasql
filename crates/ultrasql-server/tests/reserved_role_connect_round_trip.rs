//! Reservation of the `ultrasql_` role-name namespace at connect time, and
//! the crash-recovery durability that reservation protects.
//!
//! A login under the reserved prefix that is not a persisted role used to be
//! waved through by the default `Trust` policy yet was never recorded in the
//! role catalog. Once such a phantom role owned an object, the catalog/RLS
//! sidecar replay on the next restart rejected the unknown owner and the
//! whole database refused to start — total data loss. These tests pin the
//! fix: the reserved-prefix login is refused up front, while legitimately
//! persisted owners (the bootstrap `ultrasql` role) still round-trip through
//! a restart.

use std::net::SocketAddr;

use tokio_postgres::NoTls;

pub mod support;

use support::{make_data_dir_private, shutdown, start_persistent_server};

/// Open a fresh connection as `user`, returning the live client + driver
/// task on success. The full startup handshake (including any FATAL
/// `ErrorResponse`) completes before `connect` resolves, so a rejected
/// login surfaces here as `Err`.
async fn connect_as(
    bound: SocketAddr,
    user: &str,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
    let conn_str = format!(
        "host={host} port={port} user={user} application_name=reserved_role_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok((client, handle))
}

/// A login whose name carries the reserved `ultrasql_` prefix and is not a
/// persisted role must be refused with SQLSTATE 42939 (`reserved_name`),
/// while ordinary names and the bootstrap `ultrasql` role still connect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reserved_prefix_login_is_rejected() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    make_data_dir_private(data_dir.path());

    let running = start_persistent_server(data_dir.path(), "reserved_login").await;
    let bound = running.bound;

    // The exact phantom user the vector benchmarks used.
    let err = connect_as(bound, "ultrasql_bench")
        .await
        .expect_err("reserved-prefix login must be rejected");
    let db = err
        .as_db_error()
        .expect("server should send an ErrorResponse, not just drop the socket");
    assert_eq!(
        db.code().code(),
        "42939",
        "expected reserved_name SQLSTATE, got {} ({})",
        db.code().code(),
        db.message()
    );
    assert!(
        db.message().contains("reserved"),
        "rejection message should explain the reservation, got {:?}",
        db.message()
    );

    // Case-insensitive: the same reservation applies regardless of casing.
    let err_upper = connect_as(bound, "ULTRASQL_Bench")
        .await
        .expect_err("reserved-prefix login is case-insensitive");
    assert_eq!(
        err_upper
            .as_db_error()
            .expect("ErrorResponse")
            .code()
            .code(),
        "42939"
    );

    // The bootstrap superuser is named `ultrasql` (no trailing underscore),
    // so it is NOT covered by the reservation and still logs in.
    let (boot_client, boot_handle) = connect_as(bound, "ultrasql")
        .await
        .expect("bootstrap ultrasql login must succeed");
    let one: i32 = boot_client
        .query_one("SELECT 1", &[])
        .await
        .expect("bootstrap session is usable")
        .get(0);
    assert_eq!(one, 1);
    drop(boot_client);
    let _ = boot_handle.await;

    // An ordinary (non-reserved) name is unaffected.
    let (normal_client, normal_handle) = connect_as(bound, "analyst")
        .await
        .expect("non-reserved login must succeed");
    drop(normal_client);
    let _ = normal_handle.await;

    shutdown(running).await;
}

/// With the reservation in place, the only way to own an object under a
/// system-looking role is to be a *persisted* role. Confirm the legitimate
/// path is intact: a table owned by the bootstrap `ultrasql` role (with RLS
/// enabled, which records the owner in the sidecar) survives a restart, so
/// recovery's owner validation accepts it and the database starts cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_owned_rls_table_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp data dir");
    make_data_dir_private(data_dir.path());

    {
        let running = start_persistent_server(data_dir.path(), "reserved_recovery_setup").await;
        let (client, handle) = connect_as(running.bound, "ultrasql")
            .await
            .expect("bootstrap login must succeed");
        for sql in [
            "CREATE TABLE reserved_recovery_docs (tenant TEXT NOT NULL, body TEXT)",
            "INSERT INTO reserved_recovery_docs VALUES ('t1', 'hello'), ('t2', 'world')",
            "ALTER TABLE reserved_recovery_docs ENABLE ROW LEVEL SECURITY",
        ] {
            client.batch_execute(sql).await.expect(sql);
        }
        drop(client);
        let _ = handle.await;
        shutdown(running).await;
    }

    // Restart: the table's owner ("ultrasql") is a persisted role, so the
    // RLS sidecar replay accepts it and the rows are recoverable.
    let running = start_persistent_server(data_dir.path(), "reserved_recovery_verify").await;
    let count: i64 = running
        .client
        .query_one("SELECT COUNT(*) FROM reserved_recovery_docs", &[])
        .await
        .expect("RLS table must survive restart")
        .get(0);
    assert_eq!(count, 2);
    shutdown(running).await;
}
