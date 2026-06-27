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
    // Ensure the panic-logging hook is installed for every server that
    // actually serves connections — including in-process / embedded / test
    // servers that bypass the binary's `main`. Idempotent.
    crate::install_panic_hook();
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
///
/// On shutdown it drains all in-flight sessions to completion with no
/// deadline and no forced abort — the behaviour the integration tests rely
/// on. The production binary uses [`serve_listener_with_graceful_shutdown`]
/// to bound the drain and honour a second signal.
pub async fn serve_listener_with_shutdown<F>(
    listener: TcpListener,
    state: Arc<Server>,
    shutdown: F,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send,
{
    serve_listener_with_graceful_shutdown(
        listener,
        state,
        shutdown,
        std::future::pending::<()>(),
        // Effectively unbounded: preserve the original "drain to completion"
        // semantics for callers (tests) that pass no force/deadline.
        std::time::Duration::from_secs(u64::MAX),
    )
    .await
}

/// Drive an already-bound [`TcpListener`] with a bounded, signal-aware
/// graceful shutdown.
///
/// * `begin_shutdown` resolving stops the accept loop (no new connections)
///   and starts draining in-flight sessions.
/// * The drain runs until either every session finishes, `drain_deadline`
///   elapses, or `force_shutdown` resolves (e.g. a second SIGTERM/SIGINT) —
///   whichever comes first. Any sessions still running when the drain ends
///   are aborted so the process can exit promptly.
///
/// Returns `Ok(())` in every shutdown case; a drain that times out or is
/// forced is a clean stop, not an error.
pub async fn serve_listener_with_graceful_shutdown<B, K>(
    listener: TcpListener,
    state: Arc<Server>,
    begin_shutdown: B,
    force_shutdown: K,
    drain_deadline: std::time::Duration,
) -> Result<(), ServerError>
where
    B: Future<Output = ()> + Send,
    K: Future<Output = ()> + Send,
{
    // Idempotent: covers test/embedded servers that drive a listener directly.
    crate::install_panic_hook();
    tokio::pin!(begin_shutdown);
    tokio::pin!(force_shutdown);
    let mut sessions = tokio::task::JoinSet::new();
    let conn_limiter = connection_limit_semaphore();
    loop {
        let (stream, peer) = tokio::select! {
            biased;
            () = &mut begin_shutdown => {
                info!(target: "ultrasqld", in_flight = sessions.len(), "listener shutdown requested; draining in-flight sessions");
                drain_sessions(&mut sessions, force_shutdown.as_mut(), drain_deadline).await;
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

/// Await every in-flight session in `sessions`, bounded by `deadline` and
/// interruptible by `force`.
///
/// Returns once all sessions have joined, the deadline elapses, or `force`
/// resolves. Any sessions still running at that point are aborted (the
/// `JoinSet` aborts its remaining tasks on drop), so the caller can exit
/// promptly even if a session is wedged. The WAL/checkpointer are flushed
/// separately when the owning `Server` `Arc` is dropped.
async fn drain_sessions<K>(
    sessions: &mut tokio::task::JoinSet<()>,
    mut force: std::pin::Pin<&mut K>,
    deadline: std::time::Duration,
) where
    K: Future<Output = ()> + Send,
{
    // Join sessions one at a time, racing each wait against the overall
    // drain deadline and a forced second-signal shutdown. Looping (rather
    // than awaiting one combined `drain` future) keeps the `&mut sessions`
    // borrow scoped to each `join_next()` call so we can still
    // `abort_all()` whatever remains afterwards.
    let sleep = tokio::time::sleep(deadline);
    tokio::pin!(sleep);
    let mut completed = false;
    while !sessions.is_empty() {
        tokio::select! {
            biased;
            () = force.as_mut() => {
                warn!(target: "ultrasqld", "second shutdown signal; aborting in-flight sessions");
                break;
            }
            () = &mut sleep => {
                warn!(target: "ultrasqld", "drain deadline elapsed; aborting in-flight sessions");
                break;
            }
            joined = sessions.join_next() => {
                match joined {
                    Some(Ok(())) => {}
                    Some(Err(e)) => {
                        warn!(target: "ultrasqld", error = %e, "session task failed during shutdown");
                    }
                    None => {
                        completed = true;
                        break;
                    }
                }
            }
        }
    }
    if completed || sessions.is_empty() {
        info!(target: "ultrasqld", "all in-flight sessions drained");
    }
    // Abort whatever is left so the JoinSet drop does not block exit.
    sessions.abort_all();
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
