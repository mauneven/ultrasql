//! Wire-level tests for MD5 password authentication.
//!
//! Closes the v0.5 ROADMAP item "MD5 password auth (legacy, behind
//! config flag)". The flag here is the builder method
//! [`Server::require_md5_password`] — the default Server still
//! accepts every connection unchallenged.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

/// Spin up an MD5-required server and try a happy-path connect.
#[tokio::test]
async fn md5_handshake_succeeds_with_correct_password() {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database().require_md5_password("alice", "s3cr3t"));
    let server_handle = tokio::spawn(serve_listener(listener, server));

    let conn_str = format!(
        "host={host} port={port} user=alice password=s3cr3t application_name=md5_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("MD5 connect must succeed with correct password");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    // Confirm the session is fully usable post-auth.
    let rows = client
        .query("SELECT 1", &[])
        .await
        .expect("post-auth query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);

    drop(client);
    let _ = conn_handle.await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

/// Wrong password path: server must close with SQLSTATE 28P01 and
/// the client connect attempt must error out.
#[tokio::test]
async fn md5_handshake_rejects_wrong_password() {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database().require_md5_password("alice", "correct"));
    let server_handle = tokio::spawn(serve_listener(listener, server));

    let conn_str = format!(
        "host={host} port={port} user=alice password=WRONG application_name=md5_test_bad",
        host = bound.ip(),
        port = bound.port()
    );
    let err = match tokio_postgres::connect(&conn_str, NoTls).await {
        Ok(_) => panic!("connect with wrong password must fail"),
        Err(e) => e,
    };
    let dbe = err
        .as_db_error()
        .expect("server-side ErrorResponse expected");
    assert_eq!(
        dbe.code().code(),
        "28P01",
        "expected SQLSTATE 28P01 (invalid_password), got {}",
        dbe.code().code()
    );

    server_handle.abort();
}

/// Wrong user name (does not match the configured role): same
/// SQLSTATE 28P01, no MD5 challenge sent.
#[tokio::test]
async fn md5_handshake_rejects_unknown_user() {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database().require_md5_password("alice", "s3cr3t"));
    let server_handle = tokio::spawn(serve_listener(listener, server));

    let conn_str = format!(
        "host={host} port={port} user=bob password=s3cr3t",
        host = bound.ip(),
        port = bound.port()
    );
    let err = match tokio_postgres::connect(&conn_str, NoTls).await {
        Ok(_) => panic!("connect with unknown user must fail"),
        Err(e) => e,
    };
    let dbe = err
        .as_db_error()
        .expect("server-side ErrorResponse expected");
    assert_eq!(dbe.code().code(), "28P01");

    server_handle.abort();
}
