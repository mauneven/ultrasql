//! End-to-end `LISTEN` / `NOTIFY` / `UNLISTEN` test against a real
//! `tokio-postgres` client.
//!
//! Closes the v0.9 open rows "LISTEN/NOTIFY end-to-end". The test
//! opens two `tokio-postgres` clients against the same `ultrasqld`
//! process, has session A `LISTEN orders`, session B `NOTIFY orders,
//! 'hello'`, and asserts that session A's connection forwards the
//! resulting `NotificationResponse` via the async-message stream that
//! the `Connection::poll_message` API exposes.
//!
//! The connection-side plumbing mirrors the recipe in
//! `tokio-postgres/tests/test/main.rs::notifications`: rather than
//! dropping the `Connection` into `tokio::spawn` as a bare future, the
//! test wraps `poll_message` in a `stream::poll_fn` and forwards the
//! emitted `AsyncMessage` values into an `mpsc` channel the test body
//! can drain. The default `Future` implementation of `Connection`
//! swallows notification messages (it only logs notices), so a test
//! that needs to observe them must drive the message stream itself.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, stream};
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::{AsyncMessage, NoTls};
use ultrasql_server::{Server, bind_listener, serve_listener_with_shutdown};

struct RunningServer {
    bound: SocketAddr,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
    shutdown_tx: oneshot::Sender<()>,
}

/// Spin up an in-process `ultrasqld` on an ephemeral port and return its
/// bound address along with a graceful shutdown handle.
async fn start_server() -> RunningServer {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
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

/// Connect a `tokio-postgres` client against `bound` and spawn a
/// connection driver that forwards every `AsyncMessage` (notifications
/// + notices) into an `mpsc::UnboundedReceiver`.
///
/// The returned tuple is `(client, async_message_rx, connection_task)`.
/// Drop the connection task when the test is finished.
async fn connect_with_async_stream(
    bound: SocketAddr,
) -> (
    tokio_postgres::Client,
    mpsc::UnboundedReceiver<AsyncMessage>,
    tokio::task::JoinHandle<()>,
) {
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=listen_notify_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, mut connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");

    let (tx, rx) = mpsc::unbounded_channel();
    let driver = tokio::spawn(async move {
        // Mirror tokio-postgres's own `notifications` test recipe: drive
        // `Connection::poll_message` manually so async messages reach
        // the consumer rather than being absorbed by the default
        // `Future` impl. Errors are propagated as panics so the test
        // fails loudly if the connection drops.
        let mut s = stream::poll_fn(move |cx| connection.poll_message(cx));
        while let Some(msg) = s.next().await {
            match msg {
                Ok(m) => {
                    // Send may fail once the test dropped the receiver;
                    // exit the loop in that case.
                    if tx.send(m).is_err() {
                        break;
                    }
                }
                Err(e) => panic!("connection error: {e}"),
            }
        }
    });

    (client, rx, driver)
}

/// LISTEN on session A, NOTIFY from session B, assert the
/// `NotificationResponse` reaches A's async-message stream with the
/// expected channel + payload.
#[tokio::test]
async fn listen_notify_round_trip_delivers_to_other_session() {
    let server = start_server().await;
    let bound = server.bound;

    // Session A subscribes.
    let (client_a, mut async_rx_a, driver_a) = connect_with_async_stream(bound).await;
    client_a
        .batch_execute("LISTEN orders")
        .await
        .expect("LISTEN orders");

    // Session B publishes. A second connection so the NOTIFY truly
    // crosses the hub fan-out rather than short-circuiting to a
    // listener on the same connection.
    let (client_b, _async_rx_b, driver_b) = connect_with_async_stream(bound).await;
    client_b
        .batch_execute("NOTIFY orders, 'hello'")
        .await
        .expect("NOTIFY orders, 'hello'");

    // Wait up to two seconds for the notification to arrive on A's
    // async-message stream. The hub delivers synchronously, but the
    // wire-side drain runs on the next `Sync` boundary; in practice
    // the receive resolves well under 100 ms.
    let notification = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(msg) = async_rx_a.recv().await {
            if let AsyncMessage::Notification(n) = msg {
                return n;
            }
        }
        panic!("async stream closed before a notification arrived");
    })
    .await
    .expect("notification within 2 s");

    assert_eq!(notification.channel(), "orders");
    assert_eq!(notification.payload(), "hello");

    // Cleanup. Drop the listening client first so the driver loop
    // observes EOF and exits cleanly.
    drop(client_a);
    drop(client_b);
    let _ = tokio::time::timeout(Duration::from_millis(200), async {
        let _ = driver_a.await;
        let _ = driver_b.await;
    })
    .await;
    shutdown_server(server).await;
}

/// `UNLISTEN channel` removes the subscription so subsequent `NOTIFY`
/// calls no longer reach the unsubscribed session.
#[tokio::test]
async fn unlisten_drops_subscription() {
    let server = start_server().await;
    let bound = server.bound;

    let (client_a, mut async_rx_a, driver_a) = connect_with_async_stream(bound).await;
    client_a
        .batch_execute("LISTEN orders")
        .await
        .expect("LISTEN orders");
    client_a
        .batch_execute("UNLISTEN orders")
        .await
        .expect("UNLISTEN orders");

    let (client_b, _async_rx_b, driver_b) = connect_with_async_stream(bound).await;
    client_b
        .batch_execute("NOTIFY orders, 'late'")
        .await
        .expect("NOTIFY orders, 'late'");

    // Issue one round-trip on session A so the read-side loop processes
    // any queued notifications. After UNLISTEN there should be none.
    client_a
        .batch_execute("SELECT 1")
        .await
        .expect("SELECT 1 round-trip");

    // Drain whatever is waiting and assert no notification arrived.
    // We give the runtime a short scheduling window so messages already
    // in flight surface before we declare success.
    let captured = tokio::time::timeout(Duration::from_millis(200), async {
        let mut found = Vec::new();
        while let Ok(Some(msg)) =
            tokio::time::timeout(Duration::from_millis(50), async_rx_a.recv()).await
        {
            if let AsyncMessage::Notification(n) = msg {
                found.push(n);
            }
        }
        found
    })
    .await;
    let notifications = captured.unwrap_or_default();
    assert!(
        notifications.is_empty(),
        "UNLISTEN should have dropped the subscription, but got: {notifications:?}"
    );

    drop(client_a);
    drop(client_b);
    let _ = tokio::time::timeout(Duration::from_millis(200), async {
        let _ = driver_a.await;
        let _ = driver_b.await;
    })
    .await;
    shutdown_server(server).await;
}

/// `UNLISTEN *` removes every subscription owned by the session.
#[tokio::test]
async fn unlisten_star_drops_every_subscription() {
    let server = start_server().await;
    let bound = server.bound;

    let (client_a, mut async_rx_a, driver_a) = connect_with_async_stream(bound).await;
    client_a
        .batch_execute("LISTEN a; LISTEN b")
        .await
        .expect("LISTEN a; LISTEN b");
    client_a
        .batch_execute("UNLISTEN *")
        .await
        .expect("UNLISTEN *");

    let (client_b, _async_rx_b, driver_b) = connect_with_async_stream(bound).await;
    client_b
        .batch_execute("NOTIFY a, 'x'; NOTIFY b, 'y'")
        .await
        .expect("NOTIFY a; NOTIFY b");

    // Force a sync round on A so any queued notifications would surface.
    client_a
        .batch_execute("SELECT 1")
        .await
        .expect("SELECT 1 round-trip");

    let captured = tokio::time::timeout(Duration::from_millis(200), async {
        let mut found = Vec::new();
        while let Ok(Some(msg)) =
            tokio::time::timeout(Duration::from_millis(50), async_rx_a.recv()).await
        {
            if let AsyncMessage::Notification(n) = msg {
                found.push(n);
            }
        }
        found
    })
    .await;
    let notifications = captured.unwrap_or_default();
    assert!(
        notifications.is_empty(),
        "UNLISTEN * should drop every subscription; got: {notifications:?}"
    );

    drop(client_a);
    drop(client_b);
    let _ = tokio::time::timeout(Duration::from_millis(200), async {
        let _ = driver_a.await;
        let _ = driver_b.await;
    })
    .await;
    shutdown_server(server).await;
}

/// Sanity check: the bare `NOTIFY` form (no payload) round-trips to a
/// listener with an empty-string payload, matching PostgreSQL.
#[tokio::test]
async fn notify_without_payload_delivers_empty_string() {
    let server = start_server().await;
    let bound = server.bound;

    let (client_a, mut async_rx_a, driver_a) = connect_with_async_stream(bound).await;
    client_a
        .batch_execute("LISTEN events")
        .await
        .expect("LISTEN events");

    let (client_b, _async_rx_b, driver_b) = connect_with_async_stream(bound).await;
    client_b
        .batch_execute("NOTIFY events")
        .await
        .expect("NOTIFY events");

    let notification = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(msg) = async_rx_a.recv().await {
            if let AsyncMessage::Notification(n) = msg {
                return n;
            }
        }
        panic!("async stream closed before a notification arrived");
    })
    .await
    .expect("notification within 2 s");

    assert_eq!(notification.channel(), "events");
    assert_eq!(notification.payload(), "");

    drop(client_a);
    drop(client_b);
    let _ = tokio::time::timeout(Duration::from_millis(200), async {
        let _ = driver_a.await;
        let _ = driver_b.await;
    })
    .await;
    shutdown_server(server).await;
}
