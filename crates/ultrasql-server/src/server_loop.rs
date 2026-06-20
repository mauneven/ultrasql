//! TCP accept loops and per-connection session entry points.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

/// Bind to `addr` and serve PostgreSQL-wire-protocol sessions until
/// the listener errors out.
///
/// Each accepted connection runs on its own Tokio task. The function
/// returns when the listener fails irrecoverably (e.g. the port is
/// closed by an external signal); per-connection errors are logged
/// and the loop continues.
/// Global cap on concurrently-served client sessions. Each accepted connection
/// holds one permit for its entire lifetime, bounding total resident session
/// state and the pre-auth memory a connection flood can pin (without a cap, N
/// connections can each buffer up to a full message before auth). Excess
/// connection attempts are rejected immediately (the socket is closed) rather
/// than queued, keeping the accept loop responsive. Tunable via
/// `ULTRASQL_MAX_CONNECTIONS` (default 256).
pub(crate) fn connection_limit_semaphore() -> Arc<tokio::sync::Semaphore> {
    const DEFAULT_MAX_CONNECTIONS: usize = 256;
    let limit = std::env::var("ULTRASQL_MAX_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_MAX_CONNECTIONS);
    Arc::new(tokio::sync::Semaphore::new(limit))
}

pub async fn run_server(addr: SocketAddr, state: Arc<Server>) -> Result<(), ServerError> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    info!(target: "ultrasqld", listen = %bound, "ultrasqld is ready");
    let conn_limiter = connection_limit_semaphore();
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!(target: "ultrasqld", error = %e, "accept failed; continuing");
                continue;
            }
        };
        // Enforce the global connection cap. At capacity we drop the new socket
        // rather than block the accept loop or grow session state without bound.
        let permit = match Arc::clone(&conn_limiter).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                warn!(target: "ultrasqld", %peer, "connection limit reached; rejecting connection");
                drop(stream);
                continue;
            }
        };
        debug!(target: "ultrasqld", %peer, "connection accepted");
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = handle_connection_with_peer(stream, state, Some(peer.ip())).await {
                if matches!(e, ServerError::UnexpectedEof) {
                    debug!(target: "ultrasqld", %peer, "connection closed by peer");
                } else {
                    error!(target: "ultrasqld", %peer, error = %e, "session terminated");
                }
            }
        });
    }
}

/// Bind a TCP listener and report the actually-bound address.
///
/// Used by integration tests that need to read the ephemeral port the
/// kernel chose. The caller drives the listener with
/// [`serve_listener`].
pub async fn bind_listener(addr: SocketAddr) -> Result<(TcpListener, SocketAddr), ServerError> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    Ok((listener, bound))
}

/// Drive an already-bound [`TcpListener`] forever.
///
/// Equivalent to [`run_server`] without the bind step. Useful for
/// integration tests that need the chosen ephemeral port before they
/// start serving.
pub async fn serve_listener(listener: TcpListener, state: Arc<Server>) -> Result<(), ServerError> {
    serve_listener_with_shutdown(listener, state, std::future::pending::<()>()).await
}

/// Drive an already-bound [`TcpListener`] until `shutdown` resolves.
///
/// This is the production-safe sibling of [`serve_listener`]. It stops
/// accepting new sockets and returns `Ok(())` when the shutdown future
/// completes, allowing the owning task to drop its [`Server`] reference
/// cleanly instead of aborting the accept loop.
pub async fn serve_listener_with_shutdown<F>(
    listener: TcpListener,
    state: Arc<Server>,
    shutdown: F,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send,
{
    tokio::pin!(shutdown);
    let mut sessions = tokio::task::JoinSet::new();
    let conn_limiter = connection_limit_semaphore();
    loop {
        let (stream, peer) = tokio::select! {
            biased;
            () = &mut shutdown => {
                info!(target: "ultrasqld", "listener shutdown requested");
                while let Some(joined) = sessions.join_next().await {
                    if let Err(e) = joined {
                        warn!(target: "ultrasqld", error = %e, "session task failed during shutdown");
                    }
                }
                return Ok(());
            }
            joined = sessions.join_next(), if !sessions.is_empty() => {
                match joined {
                    Some(Ok(())) => {}
                    Some(Err(e)) => {
                        warn!(target: "ultrasqld", error = %e, "session task failed");
                    }
                    None => {
                        debug!(target: "ultrasqld", "session set drained before join");
                    }
                }
                continue;
            }
            accepted = listener.accept() => match accepted {
                Ok(v) => v,
                Err(e) => {
                    warn!(target: "ultrasqld", error = %e, "accept failed; continuing");
                    continue;
                }
            },
        };
        // Disable Nagle's algorithm: queries and their responses are
        // dispatched in single coalesced `write_all` calls already, so
        // there is no batching for Nagle to add to. With Nagle on, the
        // kernel can hold a small reply for up to ~40 ms waiting for a
        // companion segment that never arrives, blowing the latency
        // budget of every simple-query roundtrip. Logged-and-ignored
        // failure: the stream still works without TCP_NODELAY, just
        // slower, and we do not want a transient setsockopt error to
        // kill an otherwise-fine connection.
        if let Err(e) = stream.set_nodelay(true) {
            warn!(target: "ultrasqld", %peer, error = %e, "TCP_NODELAY failed");
        }
        // Enforce the global connection cap. At capacity we drop the new socket
        // rather than grow session state without bound.
        let permit = match Arc::clone(&conn_limiter).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                warn!(target: "ultrasqld", %peer, "connection limit reached; rejecting connection");
                drop(stream);
                continue;
            }
        };
        debug!(target: "ultrasqld", %peer, "connection accepted");
        let state = Arc::clone(&state);
        sessions.spawn(async move {
            let _permit = permit;
            if let Err(e) = handle_connection_with_peer(stream, state, Some(peer.ip())).await {
                if matches!(e, ServerError::UnexpectedEof) {
                    debug!(target: "ultrasqld", %peer, "connection closed by peer");
                } else {
                    error!(target: "ultrasqld", %peer, error = %e, "session terminated");
                }
            }
        });
    }
}

/// Drive a single PostgreSQL session over `io`.
///
/// On the happy path: reads a `StartupMessage`, replies with the
/// canonical authentication / parameter handshake, then loops over
/// frontend messages until the client sends `Terminate` or
/// disconnects. Per-query execution is delegated to [`run_select`].
pub async fn handle_connection<RW>(io: RW, state: Arc<Server>) -> Result<(), ServerError>
where
    RW: AsyncRead + AsyncWrite + Unpin + Send,
{
    handle_connection_with_peer(io, state, None).await
}

/// Drive a single PostgreSQL session, recording the client IP for `pg_hba`
/// host-rule matching.
///
/// [`handle_connection`] is the `peer = None` form used by in-process / test
/// connections (which match `local` rules); the TCP accept loops call this with
/// the accepted socket's address so `host` rules can match on source IP.
pub async fn handle_connection_with_peer<RW>(
    io: RW,
    state: Arc<Server>,
    peer: Option<std::net::IpAddr>,
) -> Result<(), ServerError>
where
    RW: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut session = Session::new(io, state, peer);
    // Slow-loris guard. A peer that opens the TCP connection and then
    // sits silently must not keep the session task alive forever — the
    // accept loop also stops accepting new connections beyond the
    // listen backlog if every worker task is parked here. The 30-s
    // budget covers the StartupMessage exchange plus the
    // authentication handshake; legitimate clients finish in < 100 ms
    // even on slow links. The error path drops the socket without
    // sending a reply because the client never advanced past startup.
    const STARTUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    match tokio::time::timeout(STARTUP_TIMEOUT, session.startup()).await {
        Ok(res) => res?,
        Err(_) => {
            tracing::warn!("dropping connection: startup handshake exceeded 30 s");
            return Ok(());
        }
    }
    session.run().await
}
