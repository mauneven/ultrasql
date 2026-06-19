//! Wire-level tests for SCRAM-SHA-256 password authentication.
//!
//! Exercises the full SASL handshake end-to-end: the server offers
//! `SCRAM-SHA-256` (`AuthenticationSASL`), `tokio-postgres` runs the client
//! side of RFC 7677, and the server verifies the client proof against a stored
//! verifier — the plaintext password is never held by the server and never
//! crosses the wire. The configuration entry point is the
//! [`Server::with_auth`] builder with [`AuthConfig::Scram`].

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio_postgres::NoTls;
use ultrasql_server::auth::PasswordHash;
use ultrasql_server::auth::scram::DEFAULT_ITERATIONS;
use ultrasql_server::{AuthConfig, Server, bind_listener, serve_listener_with_shutdown};

struct AuthServer {
    bound: SocketAddr,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
    shutdown_tx: oneshot::Sender<()>,
}

async fn start_scram_server(user: &str, password: &str) -> AuthServer {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    // Derive the SCRAM verifier once; the server stores only this, never the
    // plaintext.
    let salt = PasswordHash::random_salt();
    let verifier =
        PasswordHash::hash_password(password, &salt, DEFAULT_ITERATIONS).expect("derive verifier");
    let server = Arc::new(Server::with_sample_database().with_auth(AuthConfig::Scram {
        username: user.to_owned(),
        verifier,
    }));
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

/// Happy path: a correct password completes the SASL exchange and the session
/// is fully usable afterwards.
#[tokio::test]
async fn scram_handshake_succeeds_with_correct_password() {
    let server = start_scram_server("alice", "s3cr3t").await;

    let conn_str = format!(
        "host={host} port={port} user=alice password=s3cr3t application_name=scram_test",
        host = server.bound.ip(),
        port = server.bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("SCRAM connect must succeed with correct password");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

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

/// A wrong password fails the proof check; the server rejects with SQLSTATE
/// 28P01 and the connect attempt errors out.
#[tokio::test]
async fn scram_handshake_rejects_wrong_password() {
    let server = start_scram_server("alice", "correct").await;

    let conn_str = format!(
        "host={host} port={port} user=alice password=WRONG application_name=scram_bad",
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

/// A user name that does not match the configured role is rejected with
/// SQLSTATE 28P01 before any SASL challenge is offered.
#[tokio::test]
async fn scram_handshake_rejects_unknown_user() {
    let server = start_scram_server("alice", "s3cr3t").await;

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
