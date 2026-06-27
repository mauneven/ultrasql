//! Listener shutdown tests.
//!
//! These cover production teardown behavior that abort-based test helpers
//! cannot prove: stop accepting new connections, let the listener future
//! return `Ok(())`, then restart from the same data directory.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Notify, oneshot};
use tokio_postgres::NoTls;
use ultrasql_server::{
    Server, bind_listener, serve_listener_with_graceful_shutdown, serve_listener_with_shutdown,
};

struct RunningServer {
    client: tokio_postgres::Client,
    conn_handle: tokio::task::JoinHandle<()>,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
    shutdown_tx: oneshot::Sender<()>,
}

async fn start_persistent_server(data_dir: &Path) -> RunningServer {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::init(data_dir).expect("persistent server init"));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(serve_listener_with_shutdown(listener, server, async move {
        let _ = shutdown_rx.await;
    }));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=graceful_shutdown_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    RunningServer {
        client,
        conn_handle,
        server_handle,
        shutdown_tx,
    }
}

async fn shutdown_server(running: RunningServer) {
    drop(running.client);
    tokio::time::timeout(Duration::from_secs(2), running.conn_handle)
        .await
        .expect("connection task exits")
        .expect("connection task joins");
    let _ = running.shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), running.server_handle)
        .await
        .expect("server shutdown completes")
        .expect("server task joins")
        .expect("listener exits cleanly");
}

#[tokio::test]
async fn graceful_shutdown_flushes_table_before_restart() {
    let temp = tempfile::tempdir().expect("tempdir");
    let running = start_persistent_server(temp.path()).await;

    running
        .client
        .batch_execute("CREATE TABLE graceful_shutdown_t (id INT NOT NULL, v INT NOT NULL)")
        .await
        .expect("create before shutdown");
    running
        .client
        .batch_execute("INSERT INTO graceful_shutdown_t VALUES (1, 10), (2, 20)")
        .await
        .expect("insert before shutdown");
    let rows = running
        .client
        .query("SELECT id FROM graceful_shutdown_t ORDER BY id", &[])
        .await
        .expect("select before shutdown");
    assert_eq!(rows.len(), 2);

    shutdown_server(running).await;

    let running = start_persistent_server(temp.path()).await;
    let rows = running
        .client
        .query("SELECT id, v FROM graceful_shutdown_t ORDER BY id", &[])
        .await
        .expect("select after restart");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 10);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, i32>(1), 20);

    shutdown_server(running).await;
}

// --------------------------------------------------------------------------
// `serve_listener_with_graceful_shutdown` — bounded, signal-aware drain.
//
// This is the path the production binary (`main.rs`) drives from its
// SIGTERM/SIGINT handler: the first signal trips `begin_shutdown` (stop
// accepting + drain), and a second signal trips `force_shutdown` to abort a
// drain that is taking too long.
// --------------------------------------------------------------------------

async fn connect(bound: SocketAddr) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=graceful_shutdown_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, handle)
}

/// Triggering shutdown lets the in-flight session finish, then the listener
/// returns `Ok(())` once that session ends.
#[tokio::test]
async fn graceful_shutdown_drains_in_flight_then_exits() {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());

    let begin = Arc::new(Notify::new());
    let force = Arc::new(Notify::new());
    let begin_gate = Arc::clone(&begin);
    let force_gate = Arc::clone(&force);
    let server_handle = tokio::spawn(serve_listener_with_graceful_shutdown(
        listener,
        server,
        async move { begin_gate.notified().await },
        async move { force_gate.notified().await },
        Duration::from_secs(10),
    ));

    let (client, _conn) = connect(bound).await;
    client
        .batch_execute("CREATE TABLE gs (id INT NOT NULL); INSERT INTO gs VALUES (1), (2), (3)")
        .await
        .expect("setup");

    // Request shutdown; the established session can still complete work.
    begin.notify_one();
    let rows = client
        .simple_query("SELECT COUNT(*) FROM gs")
        .await
        .expect("in-flight query completes during drain");
    let count = rows
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some(row.get(0).expect("count").to_owned())
            }
            _ => None,
        })
        .expect("count row");
    assert_eq!(count, "3");

    // Ending the session lets the drain finish and the listener exit Ok.
    drop(client);
    let result = tokio::time::timeout(Duration::from_secs(10), server_handle)
        .await
        .expect("listener exits within deadline")
        .expect("listener task joins");
    assert!(result.is_ok(), "graceful shutdown returns Ok: {result:?}");
}

/// A second (force) signal ends the drain promptly even with a very long
/// deadline and a still-open session.
#[tokio::test]
async fn graceful_shutdown_force_signal_aborts_long_drain() {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());

    let begin = Arc::new(Notify::new());
    let force = Arc::new(Notify::new());
    let begin_gate = Arc::clone(&begin);
    let force_gate = Arc::clone(&force);
    let server_handle = tokio::spawn(serve_listener_with_graceful_shutdown(
        listener,
        server,
        async move { begin_gate.notified().await },
        async move { force_gate.notified().await },
        // Hour-long deadline: only the force signal can end the drain quickly.
        Duration::from_secs(3600),
    ));

    // Keep a session open so a plain drain would block on the deadline.
    let (_client, _conn) = connect(bound).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    begin.notify_one();
    force.notify_one();

    let result = tokio::time::timeout(Duration::from_secs(5), server_handle)
        .await
        .expect("forced shutdown exits well before the 3600s deadline")
        .expect("listener task joins");
    assert!(result.is_ok(), "forced shutdown returns Ok: {result:?}");
}

/// The drain deadline bounds shutdown even with no second signal: a wedged
/// session is aborted once the deadline elapses.
#[tokio::test]
async fn graceful_shutdown_drain_deadline_bounds_wait() {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());

    let begin = Arc::new(Notify::new());
    let begin_gate = Arc::clone(&begin);
    let server_handle = tokio::spawn(serve_listener_with_graceful_shutdown(
        listener,
        server,
        async move { begin_gate.notified().await },
        std::future::pending::<()>(),
        // Short deadline: the still-open session below is aborted after it.
        Duration::from_millis(200),
    ));

    let (_client, _conn) = connect(bound).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    begin.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(5), server_handle)
        .await
        .expect("deadline-bounded shutdown exits")
        .expect("listener task joins");
    assert!(result.is_ok(), "deadline drain returns Ok: {result:?}");
}

/// Normal run is unaffected until shutdown is requested.
#[tokio::test]
async fn graceful_shutdown_normal_run_unaffected() {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());

    let begin = Arc::new(Notify::new());
    let begin_gate = Arc::clone(&begin);
    let server_handle = tokio::spawn(serve_listener_with_graceful_shutdown(
        listener,
        server,
        async move { begin_gate.notified().await },
        std::future::pending::<()>(),
        Duration::from_secs(10),
    ));

    let (client, _conn) = connect(bound).await;
    let rows = client.simple_query("SELECT 1").await.expect("query runs");
    assert!(
        rows.iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))),
        "a normal query returns a row before any shutdown"
    );
    assert!(
        !server_handle.is_finished(),
        "listener keeps running until shutdown is requested"
    );

    drop(client);
    begin.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(10), server_handle)
        .await
        .expect("listener exits after shutdown")
        .expect("listener joins");
    assert!(result.is_ok());
}
