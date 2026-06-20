//! Wire-level privilege checks for reading server-LOCAL files through the
//! external table functions (`read_csv`, `read_parquet`, `read_json`,
//! `read_ndjson`, `read_arrow`, `read_iceberg`, `iceberg_scan`) and
//! `sniff_csv`.
//!
//! These functions open files on the database host using the server
//! process's own filesystem privileges — exactly like a server-side
//! `COPY ... FROM/TO '<path>'`. Without a gate any authenticated
//! non-superuser could read e.g. TLS keys or other databases' files via
//! `SELECT * FROM read_csv('/etc/...')`. The gate mirrors the COPY
//! server-file gate: a local read requires SUPERUSER, while object-store
//! (`s3://`) reads — which are not host-filesystem access — stay allowed for
//! every role.

pub mod support;

use std::fs;
use std::net::SocketAddr;

use support::{shutdown, start_sample_server};
use tokio_postgres::{NoTls, error::SqlState};

/// A non-superuser is denied a server-LOCAL `read_csv`, while the superuser
/// `tester` reads the same file, and an `s3://` URI is NOT denied by this gate.
#[tokio::test]
async fn non_superuser_cannot_read_local_files_via_external_functions() {
    let running = start_sample_server("external_read_privilege_test").await;
    let client = &running.client;

    // A real local CSV on the server host's filesystem.
    let dir = tempfile::tempdir().expect("tempdir");
    let csv_path = dir.path().join("local.csv");
    fs::write(&csv_path, "id,name\n1,Ada\n2,Grace\n").expect("write csv");
    let csv_path = csv_path.to_string_lossy().replace('\'', "''");

    // The default `tester` login is a superuser-equivalent: it CAN read the
    // local file (the ALLOW case).
    let rows = client
        .query(&format!("SELECT COUNT(*) FROM read_csv('{csv_path}')"), &[])
        .await
        .expect("superuser reads local csv");
    assert_eq!(rows[0].get::<_, i64>(0), 2);

    // Create a non-superuser login for the DENY case.
    client
        .batch_execute("CREATE ROLE low_priv_reader NOSUPERUSER LOGIN")
        .await
        .expect("create non-superuser role");

    let (low, low_conn) = connect_as(running.bound, "low_priv_reader", "external_read_low").await;

    // (a) DENY: non-superuser SELECT from read_csv on a LOCAL path.
    assert_server_file_denied(
        low.query(&format!("SELECT * FROM read_csv('{csv_path}')"), &[])
            .await
            .expect_err("non-superuser cannot read a local file via read_csv"),
    );

    // The gate fires regardless of query shape: subquery, CTE, and join.
    assert_server_file_denied(
        low.query(
            &format!("SELECT * FROM (SELECT * FROM read_csv('{csv_path}')) t",),
            &[],
        )
        .await
        .expect_err("subquery shape is gated"),
    );
    assert_server_file_denied(
        low.query(
            &format!("WITH c AS (SELECT * FROM read_csv('{csv_path}')) SELECT * FROM c",),
            &[],
        )
        .await
        .expect_err("CTE shape is gated"),
    );

    // (d) DENY: sniff_csv on a local path as non-superuser.
    assert_server_file_denied(
        low.query(&format!("SELECT * FROM sniff_csv('{csv_path}')"), &[])
            .await
            .expect_err("non-superuser cannot sniff a local file"),
    );

    // EXPLAIN (without ANALYZE) of a local read is gated too — the plan
    // summary opens the file outside `lower_query`.
    assert_server_file_denied(
        low.query(
            &format!("EXPLAIN SELECT * FROM read_csv('{csv_path}')"),
            &[],
        )
        .await
        .expect_err("EXPLAIN of a local read is gated"),
    );

    // (c) NOT denied by this gate: an object-store (`s3://`) URI. The read
    // fails later (no such bucket / no network), but it must NOT return the
    // server-file privilege error.
    let s3_err = low
        .query(
            "SELECT * FROM read_csv('s3://ultrasql-no-such-bucket/missing.csv')",
            &[],
        )
        .await
        .expect_err("s3 read fails for unrelated reasons");
    assert_not_server_file_denied(&s3_err);

    drop(low);
    low_conn.await.expect("low-priv connection joins");

    // (b) ALLOW (revisited): after RESET-equivalent, the superuser still reads.
    let rows = client
        .query(&format!("SELECT COUNT(*) FROM read_csv('{csv_path}')"), &[])
        .await
        .expect("superuser still reads local csv");
    assert_eq!(rows[0].get::<_, i64>(0), 2);

    shutdown(running).await;
}

/// `SET ROLE` to a non-superuser is gated, and `RESET ROLE` restores access —
/// confirming the gate keys on the *effective* role, like the COPY gate.
#[tokio::test]
async fn set_role_non_superuser_then_reset_round_trips_local_read() {
    let running = start_sample_server("external_read_set_role_test").await;
    let client = &running.client;

    let dir = tempfile::tempdir().expect("tempdir");
    let csv_path = dir.path().join("set_role.csv");
    fs::write(&csv_path, "id\n1\n2\n3\n").expect("write csv");
    let csv_path = csv_path.to_string_lossy().replace('\'', "''");

    // Register `tester` as a real superuser so it may SET ROLE to any role
    // (mirroring the rls/ownership round-trip suites' setup).
    for sql in [
        "CREATE ROLE tester SUPERUSER LOGIN",
        "CREATE ROLE set_role_reader NOSUPERUSER",
    ] {
        client.batch_execute(sql).await.expect(sql);
    }

    // Under the non-superuser effective role, the local read is denied.
    client
        .batch_execute("SET ROLE set_role_reader")
        .await
        .expect("set role");
    assert_server_file_denied(
        client
            .query(&format!("SELECT * FROM read_csv('{csv_path}')"), &[])
            .await
            .expect_err("SET ROLE non-superuser is denied a local read"),
    );

    // RESET ROLE returns to the superuser `tester` and access is restored.
    client
        .batch_execute("RESET ROLE")
        .await
        .expect("reset role");
    let rows = client
        .query(&format!("SELECT COUNT(*) FROM read_csv('{csv_path}')"), &[])
        .await
        .expect("superuser reads after RESET ROLE");
    assert_eq!(rows[0].get::<_, i64>(0), 3);

    shutdown(running).await;
}

/// The superuser still performs a server-side `COPY ... TO '<file>'`, proving
/// the new gate does not regress the existing (separately-gated) COPY path.
#[tokio::test]
async fn superuser_copy_to_server_file_still_works() {
    let running = start_sample_server("external_read_copy_test").await;
    let client = &running.client;

    let dir = tempfile::tempdir().expect("tempdir");
    let out_path = dir.path().join("copy_out.csv");
    let out_path_sql = out_path.to_string_lossy().replace('\'', "''");

    client
        .batch_execute("CREATE TABLE copy_src (id INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO copy_src VALUES (1), (2), (3)")
        .await
        .expect("insert rows");

    client
        .batch_execute(&format!(
            "COPY copy_src TO '{out_path_sql}' WITH (FORMAT CSV)",
        ))
        .await
        .expect("superuser COPY TO server file succeeds");

    let written = fs::read_to_string(&out_path).expect("read copied file");
    assert_eq!(written.lines().count(), 3, "three rows copied: {written:?}");

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

/// Assert that `err` is the server-file privilege denial: SQLSTATE 42501 with
/// the gate's message.
fn assert_server_file_denied(err: tokio_postgres::Error) {
    let db = err.as_db_error().expect("database error");
    assert_eq!(
        db.code(),
        &SqlState::INSUFFICIENT_PRIVILEGE,
        "expected insufficient_privilege, got: {}",
        db.message()
    );
    assert!(
        db.message()
            .contains("permission denied for reading server-side files"),
        "unexpected denial message: {}",
        db.message()
    );
}

/// Assert that `err` is NOT the server-file privilege denial — an object-store
/// read may fail for unrelated reasons, but never via this gate.
fn assert_not_server_file_denied(err: &tokio_postgres::Error) {
    if let Some(db) = err.as_db_error() {
        assert!(
            !db.message()
                .contains("permission denied for reading server-side files"),
            "object-store read was wrongly denied by the server-file gate: {}",
            db.message()
        );
    }
}
