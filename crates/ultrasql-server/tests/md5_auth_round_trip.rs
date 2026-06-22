//! Wire-level tests for MD5 password authentication.
//!
//! Closes the v0.5 open item "MD5 password auth (legacy, behind
//! config flag)". The flag here is the builder method
//! [`Server::require_md5_password`] — the default Server still
//! accepts every connection unchallenged.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener_with_shutdown};

struct AuthServer {
    bound: SocketAddr,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
    shutdown_tx: oneshot::Sender<()>,
}

async fn start_md5_server(user: &str, password: &str) -> AuthServer {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database().require_md5_password(user, password));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(serve_listener_with_shutdown(listener, server, async move {
        let _ = shutdown_rx.await;
    }));
    AuthServer {
        bound,
        server_handle,
        shutdown_tx,
    }
}

async fn shutdown_server(server: AuthServer) {
    let _ = server.shutdown_tx.send(());
    tokio::time::timeout(std::time::Duration::from_secs(2), server.server_handle)
        .await
        .expect("server shutdown completes")
        .expect("server task joins")
        .expect("listener exits cleanly");
}

/// Spin up an MD5-required server and try a happy-path connect.
#[tokio::test]
async fn md5_handshake_succeeds_with_correct_password() {
    let server = start_md5_server("alice", "s3cr3t").await;

    let conn_str = format!(
        "host={host} port={port} user=alice password=s3cr3t application_name=md5_test",
        host = server.bound.ip(),
        port = server.bound.port()
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
    shutdown_server(server).await;
}

/// Wrong password path: server must close with SQLSTATE 28P01 and
/// the client connect attempt must error out.
#[tokio::test]
async fn md5_handshake_rejects_wrong_password() {
    let server = start_md5_server("alice", "correct").await;

    let conn_str = format!(
        "host={host} port={port} user=alice password=WRONG application_name=md5_test_bad",
        host = server.bound.ip(),
        port = server.bound.port()
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

    shutdown_server(server).await;
}

/// Wrong user name (does not match the configured role): same
/// SQLSTATE 28P01, no MD5 challenge sent.
#[tokio::test]
async fn md5_handshake_rejects_unknown_user() {
    let server = start_md5_server("alice", "s3cr3t").await;

    let conn_str = format!(
        "host={host} port={port} user=bob password=s3cr3t",
        host = server.bound.ip(),
        port = server.bound.port()
    );
    let err = match tokio_postgres::connect(&conn_str, NoTls).await {
        Ok(_) => panic!("connect with unknown user must fail"),
        Err(e) => e,
    };
    let dbe = err
        .as_db_error()
        .expect("server-side ErrorResponse expected");
    assert_eq!(dbe.code().code(), "28P01");

    shutdown_server(server).await;
}
