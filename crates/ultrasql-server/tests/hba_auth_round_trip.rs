//! Wire-level tests for `pg_hba`-driven per-role authentication.
//!
//! A connection is matched against the rules by (connection kind, database,
//! role, client IP); the first matching rule's method decides the outcome.
//! `trust` admits, `reject` / no-matching-rule deny, and `scram-sha-256` runs a
//! SCRAM exchange against the role's OWN verifier stored in the role catalog
//! (set by `CREATE ROLE ... PASSWORD`). The configuration entry point is
//! [`Server::with_auth`] with [`AuthConfig::Hba`].

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio_postgres::{Client, NoTls};
use ultrasql_server::auth::HbaConfig;
use ultrasql_server::{AuthConfig, Server, bind_listener, serve_listener_with_shutdown};

struct AuthServer {
    bound: SocketAddr,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
    shutdown_tx: oneshot::Sender<()>,
}

async fn start_hba_server(rules: &str) -> AuthServer {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let hba = HbaConfig::parse(rules).expect("parse pg_hba rules");
    let server = Arc::new(Server::with_sample_database().with_auth(AuthConfig::Hba(hba)));
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

/// Connect and return the client plus the connection driver's join handle.
async fn connect(
    bound: SocketAddr,
    user: &str,
    password: &str,
) -> Result<(Client, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
    let conn_str = format!(
        "host={host} port={port} user={user} password={password}",
        host = bound.ip(),
        port = bound.port(),
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok((client, handle))
}

/// `tester` is trusted (used to provision the role); `alice` must SCRAM against
/// her own catalog password.
#[tokio::test]
async fn hba_scram_per_role_login() {
    let rules = "host all tester 127.0.0.1/32 trust\n\
                 host all alice 127.0.0.1/32 scram-sha-256";
    let server = start_hba_server(rules).await;

    // 1. Provision a password-bearing role over the trusted admin connection.
    let (admin, admin_conn) = connect(server.bound, "tester", "ignored")
        .await
        .expect("trust admin login");
    admin
        .batch_execute("CREATE ROLE alice LOGIN PASSWORD 's3cr3t'")
        .await
        .expect("create role alice");
    drop(admin);
    let _ = admin_conn.await;

    // 2. alice logs in via SCRAM against her stored verifier.
    let (client, conn) = connect(server.bound, "alice", "s3cr3t")
        .await
        .expect("alice SCRAM login must succeed");
    let rows = client
        .query("SELECT 1", &[])
        .await
        .expect("post-auth query");
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    drop(client);
    let _ = conn.await;

    // 3. Wrong password → SQLSTATE 28P01.
    let err = connect(server.bound, "alice", "WRONG")
        .await
        .expect_err("wrong password must be rejected");
    assert_eq!(
        err.as_db_error()
            .expect("server ErrorResponse")
            .code()
            .code(),
        "28P01"
    );

    shutdown_server(server).await;
}

/// A role with no matching rule (and an explicit `reject`) is denied.
#[tokio::test]
async fn hba_no_matching_rule_denies() {
    let rules = "host all tester 127.0.0.1/32 trust\n\
                 host all mallory 127.0.0.1/32 reject";
    let server = start_hba_server(rules).await;

    // Explicit reject rule.
    let err = connect(server.bound, "mallory", "x")
        .await
        .expect_err("reject rule must deny");
    assert_eq!(
        err.as_db_error()
            .expect("server ErrorResponse")
            .code()
            .code(),
        "28P01"
    );

    // No rule matches 'bob' at all → deny (PostgreSQL default).
    let err = connect(server.bound, "bob", "x")
        .await
        .expect_err("absent rule must deny");
    assert_eq!(
        err.as_db_error()
            .expect("server ErrorResponse")
            .code()
            .code(),
        "28P01"
    );

    shutdown_server(server).await;
}
