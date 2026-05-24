//! End-to-end idle-session timeout coverage.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener_with_shutdown};

struct RunningServer {
    bound: SocketAddr,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
    shutdown_tx: oneshot::Sender<()>,
}

async fn start_server(idle_timeout_ms: u64) -> RunningServer {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let mut server = Server::with_sample_database();
    server.set_idle_session_timeout_ms(idle_timeout_ms);
    let server = Arc::new(server);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(serve_listener_with_shutdown(listener, server, async move {
        let _ = shutdown_rx.await;
    }));
    RunningServer {
        bound,
        server_handle,
        shutdown_tx,
    }
}

async fn shutdown_server(running: RunningServer) {
    let _ = running.shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), running.server_handle)
        .await
        .expect("server shutdown completes")
        .expect("server task joins")
        .expect("listener exits cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_idle_session_timeout_closes_post_startup_idle_client() {
    let running = start_server(50).await;
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=idle_timeout_test",
        host = running.bound.ip(),
        port = running.bound.port()
    );
    let (_client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");

    let started = Instant::now();
    let connection_result = tokio::time::timeout(Duration::from_secs(2), connection)
        .await
        .expect("idle timeout should close the connection future");

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "idle timeout closure took {:?}",
        started.elapsed()
    );
    assert!(
        connection_result.is_err(),
        "server-side idle close should end the client connection with an error"
    );

    shutdown_server(running).await;
}
