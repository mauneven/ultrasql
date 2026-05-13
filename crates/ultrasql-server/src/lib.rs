//! `ultrasql-server` library: PostgreSQL-wire-compatible session loop.
//!
//! The crate exposes two top-level async entry points:
//!
//! - [`run_server`] binds a TCP listener and dispatches one
//!   [`handle_connection`] task per accepted socket.
//! - [`handle_connection`] runs a single session: startup handshake,
//!   `ReadyForQuery` loop, simple-query execution, polite
//!   termination.
//!
//! The handler is generic over any [`AsyncRead`] + [`AsyncWrite`]
//! transport. Production uses [`tokio::net::TcpStream`]; tests pin a
//! [`tokio::io::duplex`] pair against the handler to exercise the full
//! state machine without a real socket.
//!
//! ## Wire-protocol coverage in v0.5
//!
//! - `StartupMessage` / `AuthenticationOk` / `ParameterStatus` /
//!   `BackendKeyData` / `ReadyForQuery` — full handshake.
//! - Simple Query (`'Q'`) — parsed, bound, lowered, and executed.
//! - Terminate (`'X'`) — closes the session.
//! - Extended-protocol messages (`Parse`/`Bind`/`Describe`/`Execute`/`Sync`,
//!   `Password`) — answered with a single `ErrorResponse` + a
//!   `ReadyForQuery 'E'`. The extended protocol lands in a follow-up.
//!
//! ## Execution
//!
//! The handler delegates physical-plan construction to
//! [`pipeline::lower_plan`] and result emission to
//! [`result_encoder::run_select`]. Both modules document their
//! supported subsets and surface unsupported constructs as
//! [`ServerError::Unsupported`]; the handler reports those as
//! query-scoped `ErrorResponse`s so the session continues.

#![forbid(unsafe_op_in_unsafe_fn)]

pub mod error;
pub mod pipeline;
pub mod result_encoder;

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};
use ultrasql_parser::Parser;
use ultrasql_planner::{InMemoryCatalog, bind};
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_frontend, encode_backend};

pub use error::ServerError;
pub use pipeline::{SampleTables, build_sample_database};
pub use result_encoder::{SelectResult, run_select};

/// Default initial read buffer. Picked to fit a small startup message
/// without resizing; the buffer grows on demand.
const READ_BUFFER_INITIAL: usize = 1 << 12;

/// Shared connection state: the catalog used by the binder plus the
/// sample-table registry the lowerer consults.
///
/// Lives behind [`Arc`] so connection tasks share a single instance.
#[derive(Debug)]
pub struct Server {
    /// Planner-facing catalog.
    pub catalog: InMemoryCatalog,
    /// Registry of sample tables (schema + pre-built batches).
    pub tables: SampleTables,
}

impl Server {
    /// Build a server pre-loaded with the canonical sample database.
    #[must_use]
    pub fn with_sample_database() -> Self {
        let mut catalog = InMemoryCatalog::new();
        let tables = build_sample_database(&mut catalog);
        Self { catalog, tables }
    }
}

/// Bind to `addr` and serve PostgreSQL-wire-protocol sessions until
/// the listener errors out.
///
/// Each accepted connection runs on its own Tokio task. The function
/// returns when the listener fails irrecoverably (e.g. the port is
/// closed by an external signal); per-connection errors are logged
/// and the loop continues.
pub async fn run_server(addr: SocketAddr, state: Arc<Server>) -> Result<(), ServerError> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    info!(target: "ultrasqld", listen = %bound, "ultrasqld is ready");
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!(target: "ultrasqld", error = %e, "accept failed; continuing");
                continue;
            }
        };
        debug!(target: "ultrasqld", %peer, "connection accepted");
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, state).await {
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
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!(target: "ultrasqld", error = %e, "accept failed; continuing");
                continue;
            }
        };
        debug!(target: "ultrasqld", %peer, "connection accepted");
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, state).await {
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
    RW: AsyncRead + AsyncWrite + Unpin,
{
    let mut session = Session::new(io, state);
    session.startup().await?;
    session.run().await
}

/// Per-connection state machine.
struct Session<RW> {
    io: RW,
    read_buf: BytesMut,
    write_buf: BytesMut,
    state: Arc<Server>,
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    fn new(io: RW, state: Arc<Server>) -> Self {
        Self {
            io,
            read_buf: BytesMut::with_capacity(READ_BUFFER_INITIAL),
            write_buf: BytesMut::with_capacity(READ_BUFFER_INITIAL),
            state,
        }
    }

    /// Read the startup message and emit the canonical handshake.
    async fn startup(&mut self) -> Result<(), ServerError> {
        let msg = self.read_frontend().await?;
        let (major, minor) = match msg {
            FrontendMessage::StartupMessage {
                protocol_major,
                protocol_minor,
                ..
            } => (protocol_major, protocol_minor),
            // The spec allows an SSLRequest as the very first message
            // (which decodes to a startup-shaped payload); v0.5 does
            // not negotiate TLS yet. We treat anything else as a
            // protocol violation.
            other => {
                debug!(target: "ultrasqld", ?other, "expected startup, got other");
                return Err(ServerError::UnexpectedEof);
            }
        };
        if major != 3 {
            // Reply with an `ErrorResponse` so a libpq client that
            // happens to advertise a future protocol version sees a
            // proper SQLSTATE and a human-readable message before the
            // socket closes; without this the client sees only EOF and
            // reports a confusing "connection closed before handshake"
            // error. The reply is best-effort — if it fails we still
            // propagate the original UnsupportedProtocol error.
            let _ = self
                .send(&BackendMessage::ErrorResponse {
                    fields: vec![
                        (b'S', "FATAL".to_string()),
                        (b'C', "08P01".to_string()),
                        (
                            b'M',
                            format!(
                                "unsupported frontend protocol {major}.{minor}; server supports 3.0"
                            ),
                        ),
                    ],
                })
                .await;
            return Err(ServerError::UnsupportedProtocol { major, minor });
        }

        // AuthenticationOk — v0.5 has no real auth.
        self.send(&BackendMessage::AuthenticationOk).await?;
        // Server-version & client_encoding are the two parameters
        // libpq actually reads back during connection setup.
        self.send(&BackendMessage::ParameterStatus {
            name: "server_version".to_string(),
            value: format!("ultrasql-{}", env!("CARGO_PKG_VERSION")),
        })
        .await?;
        self.send(&BackendMessage::ParameterStatus {
            name: "client_encoding".to_string(),
            value: "UTF8".to_string(),
        })
        .await?;
        // BackendKeyData — cancellation handle. Zeroed until we wire
        // an actual cancel-request handler.
        self.send(&BackendMessage::BackendKeyData {
            process_id: 0,
            secret_key: 0,
        })
        .await?;
        self.send(&BackendMessage::ReadyForQuery { status: b'I' })
            .await?;
        Ok(())
    }

    /// Main per-query loop. Returns on clean termination.
    async fn run(&mut self) -> Result<(), ServerError> {
        loop {
            let msg = match self.read_frontend().await {
                Ok(m) => m,
                Err(ServerError::UnexpectedEof) => return Ok(()),
                Err(other) => return Err(other),
            };
            match msg {
                FrontendMessage::Query { sql } => {
                    self.handle_query(&sql).await?;
                }
                FrontendMessage::Terminate => return Ok(()),
                FrontendMessage::Parse { .. }
                | FrontendMessage::Bind { .. }
                | FrontendMessage::Describe { .. }
                | FrontendMessage::Execute { .. }
                | FrontendMessage::Sync => {
                    self.send_error("extended query not supported in v0.5", "0A000")
                        .await?;
                    self.send(&BackendMessage::ReadyForQuery { status: b'E' })
                        .await?;
                }
                FrontendMessage::Password { .. } => {
                    // Auth is not yet a state in the loop; if a client
                    // sends a Password out of nowhere we treat it as
                    // a query-scoped error.
                    self.send_error("password message outside auth flow", "08P01")
                        .await?;
                    self.send(&BackendMessage::ReadyForQuery { status: b'E' })
                        .await?;
                }
                FrontendMessage::StartupMessage { .. } => {
                    // A second StartupMessage is a protocol violation.
                    return Err(ServerError::UnexpectedEof);
                }
                // The protocol enum is `#[non_exhaustive]`; future
                // additions trigger this arm and are reported as
                // query-scoped feature-not-supported.
                _ => {
                    self.send_error("unsupported frontend message", "0A000")
                        .await?;
                    self.send(&BackendMessage::ReadyForQuery { status: b'E' })
                        .await?;
                }
            }
        }
    }

    /// Execute a simple `'Q'` query end-to-end and write the response.
    async fn handle_query(&mut self, sql: &str) -> Result<(), ServerError> {
        let trimmed = sql.trim();
        if trimmed.is_empty() || trimmed == ";" {
            self.send(&BackendMessage::EmptyQueryResponse).await?;
            self.send(&BackendMessage::ReadyForQuery { status: b'I' })
                .await?;
            return Ok(());
        }

        match self.execute_query(trimmed) {
            Ok(result) => {
                for msg in &result.messages {
                    self.send(msg).await?;
                }
            }
            Err(err) => {
                if !err.is_query_scoped() {
                    return Err(err);
                }
                self.send_error(&err.to_string(), err.sqlstate()).await?;
            }
        }
        self.send(&BackendMessage::ReadyForQuery { status: b'I' })
            .await?;
        Ok(())
    }

    /// Synchronous core of query execution: parse, bind, lower, run.
    ///
    /// Kept synchronous because none of the steps perform I/O. The
    /// async handler invokes this from the connection task; the
    /// executor's reactor stays responsive because the sample tables
    /// have a bounded fixed size.
    fn execute_query(&self, sql: &str) -> Result<SelectResult, ServerError> {
        let stmt = Parser::new(sql).parse_statement()?;
        let plan = bind(&stmt, &self.state.catalog)?;
        let mut op = pipeline::lower_plan(&plan, &self.state.tables)?;
        run_select(op.as_mut())
    }

    /// Read one frontend message, growing the buffer until the codec
    /// has a complete frame.
    async fn read_frontend(&mut self) -> Result<FrontendMessage, ServerError> {
        loop {
            if let Some(msg) = decode_frontend(&mut self.read_buf)? {
                return Ok(msg);
            }
            // Pull more bytes from the socket.
            let n = self.io.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(ServerError::UnexpectedEof);
            }
        }
    }

    /// Encode and flush a single backend message.
    async fn send(&mut self, msg: &BackendMessage) -> Result<(), ServerError> {
        self.write_buf.clear();
        encode_backend(msg, &mut self.write_buf);
        self.io.write_all(&self.write_buf).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Send a PostgreSQL-compatible `ErrorResponse`. The fields are
    /// the minimal set every libpq client expects: severity, code,
    /// message.
    async fn send_error(&mut self, message: &str, sqlstate: &str) -> Result<(), ServerError> {
        let msg = BackendMessage::ErrorResponse {
            fields: vec![
                (b'S', "ERROR".to_string()),
                (b'C', sqlstate.to_string()),
                (b'M', message.to_string()),
            ],
        };
        self.send(&msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    /// Read every backend message currently buffered on `io`, stopping
    /// once a `ReadyForQuery` is observed. Returns the collected
    /// messages.
    async fn drain_until_ready(io: &mut (impl AsyncRead + Unpin)) -> Vec<BackendMessage> {
        let mut buf = BytesMut::with_capacity(4096);
        let mut out = Vec::new();
        let mut tmp = [0_u8; 1024];
        loop {
            // Try to decode messages already in `buf`.
            while let Some(msg) = ultrasql_protocol::decode_backend(&mut buf).expect("decode") {
                let is_ready = matches!(msg, BackendMessage::ReadyForQuery { .. });
                out.push(msg);
                if is_ready {
                    return out;
                }
            }
            let n = io.read(&mut tmp).await.expect("read");
            if n == 0 {
                return out;
            }
            buf.extend_from_slice(&tmp[..n]);
        }
    }

    /// Send a frontend message and flush.
    async fn send_frontend(io: &mut (impl AsyncWrite + Unpin), msg: &FrontendMessage) {
        let mut buf = BytesMut::new();
        ultrasql_protocol::encode_frontend(msg, &mut buf);
        io.write_all(&buf).await.expect("write");
        io.flush().await.expect("flush");
    }

    fn server() -> Arc<Server> {
        Arc::new(Server::with_sample_database())
    }

    async fn complete_startup(client: &mut (impl AsyncRead + AsyncWrite + Unpin)) {
        send_frontend(
            client,
            &FrontendMessage::StartupMessage {
                protocol_major: 3,
                protocol_minor: 0,
                params: vec![("user".to_string(), "tester".to_string())],
            },
        )
        .await;
        let msgs = drain_until_ready(client).await;
        // Sanity-check the handshake shape: ends in ReadyForQuery 'I'.
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));
        // AuthenticationOk must appear at least once.
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::AuthenticationOk))
        );
    }

    #[tokio::test]
    async fn startup_handshake_completes() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let handle = tokio::spawn(handle_connection(server_side, state));

        complete_startup(&mut client).await;
        // Send Terminate to let the handler return cleanly.
        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    #[tokio::test]
    async fn simple_query_returns_three_data_rows() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let handle = tokio::spawn(handle_connection(server_side, state));

        complete_startup(&mut client).await;
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM users".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;

        let row_desc = msgs
            .iter()
            .find(|m| matches!(m, BackendMessage::RowDescription { .. }))
            .expect("row description present");
        match row_desc {
            BackendMessage::RowDescription { fields } => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name, "id");
                assert_eq!(fields[0].type_oid, 23); // int4
            }
            _ => unreachable!(),
        }

        let rows: Vec<_> = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .collect();
        assert_eq!(rows.len(), 3);
        match rows[0] {
            BackendMessage::DataRow { columns } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].as_deref(), Some(b"1".as_slice()));
            }
            _ => unreachable!(),
        }

        let cc = msgs
            .iter()
            .find(|m| matches!(m, BackendMessage::CommandComplete { .. }))
            .expect("command complete present");
        match cc {
            BackendMessage::CommandComplete { tag } => assert_eq!(tag, "SELECT 3"),
            _ => unreachable!(),
        }
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    #[tokio::test]
    async fn filter_and_limit_narrow_result_to_one_row() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let handle = tokio::spawn(handle_connection(server_side, state));

        complete_startup(&mut client).await;
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM users WHERE id = 1 LIMIT 1".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;

        let rows: Vec<_> = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .collect();
        assert_eq!(rows.len(), 1);
        match rows[0] {
            BackendMessage::DataRow { columns } => {
                assert_eq!(columns[0].as_deref(), Some(b"1".as_slice()));
            }
            _ => unreachable!(),
        }

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    #[tokio::test]
    async fn unknown_table_reports_error_then_ready_idle() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let handle = tokio::spawn(handle_connection(server_side, state));

        complete_startup(&mut client).await;
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM nope".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;

        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
        );
        // The session continues — ready-for-query is 'I' (idle), not
        // 'E' (in failed transaction), because we are not in a tx.
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    #[tokio::test]
    async fn parse_error_reports_error_response() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let handle = tokio::spawn(handle_connection(server_side, state));

        complete_startup(&mut client).await;
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "GIBBERISH NOT SQL".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;

        let err = msgs
            .iter()
            .find(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
            .expect("error response present");
        match err {
            BackendMessage::ErrorResponse { fields } => {
                // Severity, code, and message fields are populated.
                let codes: Vec<u8> = fields.iter().map(|(c, _)| *c).collect();
                assert!(codes.contains(&b'S'));
                assert!(codes.contains(&b'C'));
                assert!(codes.contains(&b'M'));
            }
            _ => unreachable!(),
        }

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    #[tokio::test]
    async fn terminate_ends_the_session_cleanly() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let handle = tokio::spawn(handle_connection(server_side, state));

        complete_startup(&mut client).await;
        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        // Closing the client confirms the server returns cleanly.
        drop(client);
        let result = handle.await.expect("task joins");
        result.expect("clean exit on Terminate");
    }

    #[tokio::test]
    async fn empty_query_returns_empty_query_response() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let handle = tokio::spawn(handle_connection(server_side, state));

        complete_startup(&mut client).await;
        send_frontend(&mut client, &FrontendMessage::Query { sql: String::new() }).await;
        let msgs = drain_until_ready(&mut client).await;
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::EmptyQueryResponse))
        );
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    #[tokio::test]
    async fn extended_protocol_parse_is_rejected_with_error() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let handle = tokio::spawn(handle_connection(server_side, state));

        complete_startup(&mut client).await;
        send_frontend(
            &mut client,
            &FrontendMessage::Parse {
                name: String::new(),
                sql: "SELECT 1".to_string(),
                param_types: vec![],
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
        );
        // Extended-protocol rejection sets the ready-for-query status
        // to 'E' (error) so libpq clients sync.
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'E' }
        ));

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// Adversarial input: a client that advertises `protocol_major =
    /// 0xFFFF` (or any non-3 value, including the negotiated future
    /// minor protocol number used by clients targeting newer servers)
    /// must be rejected cleanly with an `ErrorResponse` carrying
    /// SQLSTATE 08P01, followed by a clean connection close — not a
    /// panic, not a silent EOF.
    #[tokio::test]
    async fn unsupported_protocol_major_returns_error_response() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let handle = tokio::spawn(handle_connection(server_side, state));

        // Send a startup with a wildly future major.
        send_frontend(
            &mut client,
            &FrontendMessage::StartupMessage {
                protocol_major: 0xFFFF,
                protocol_minor: 0,
                params: vec![("user".to_string(), "anyone".to_string())],
            },
        )
        .await;

        // Drain whatever bytes the server sent back before closing.
        let mut buf = BytesMut::with_capacity(1024);
        let mut tmp = [0_u8; 1024];
        loop {
            let n = client.read(&mut tmp).await.expect("read");
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
        }

        // The first decoded backend message must be an ErrorResponse
        // with SQLSTATE 08P01.
        let msg = ultrasql_protocol::decode_backend(&mut buf)
            .expect("decode")
            .expect("non-empty");
        match msg {
            BackendMessage::ErrorResponse { fields } => {
                let code = fields
                    .iter()
                    .find_map(|(c, v)| (*c == b'C').then(|| v.clone()))
                    .expect("SQLSTATE field present");
                assert_eq!(code, "08P01");
            }
            other => panic!("expected ErrorResponse, got {other:?}"),
        }

        // The handler task must have returned with the
        // UnsupportedProtocol classification (not a panic).
        let result = handle.await.expect("task joins");
        assert!(matches!(
            result,
            Err(ServerError::UnsupportedProtocol { major: 0xFFFF, .. })
        ));
    }

    #[tokio::test]
    async fn integration_real_tcp_select_round_trips_rows() {
        // Use port 0 to let the kernel pick an ephemeral port.
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
        let (listener, bound) = bind_listener(addr).await.expect("bind");
        let state = server();
        let server_handle = tokio::spawn(serve_listener(listener, state));

        let mut stream = tokio::net::TcpStream::connect(bound)
            .await
            .expect("connect");
        complete_startup(&mut stream).await;
        send_frontend(
            &mut stream,
            &FrontendMessage::Query {
                sql: "SELECT id FROM users LIMIT 2".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut stream).await;
        let row_count = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .count();
        assert_eq!(row_count, 2);

        send_frontend(&mut stream, &FrontendMessage::Terminate).await;
        drop(stream);
        server_handle.abort();
    }
}
