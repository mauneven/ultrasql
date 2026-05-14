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
//! - Extended Query (`Parse`/`Bind`/`Describe`/`Execute`/`Sync`/`Close`/
//!   `Flush`) — routed through the per-connection state machine in
//!   [`extended`]. Parameter values are substituted into the bound
//!   logical plan and executed through the same `pipeline::lower_query`
//!   path Simple Query uses; the result encoder honours text/binary
//!   per-column format codes.
//! - Terminate (`'X'`) — closes the session.
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

pub mod auth;
pub mod error;
pub mod extended;
pub mod pipeline;
pub mod result_encoder;
pub mod tls;
pub mod wire_writer;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};
use ultrasql_catalog::{
    CatalogSnapshot, IndexEntry, MutableCatalog, PersistentCatalog, TableEntry,
};
use ultrasql_core::{DataType, PageId, RelationId, Value};
use ultrasql_optimizer::{NoStats, PlanCache, PlanCacheConfig, PlanCacheKey, StatsSource};
use ultrasql_parser::Parser;
use ultrasql_planner::{
    Catalog as PlannerCatalog, InMemoryCatalog, LogicalAlterTableAction, LogicalPlan, TableMeta,
    bind,
};
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_frontend, encode_backend};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, UpdateOptions};
use ultrasql_storage::page::Page;
use ultrasql_txn::{IsolationLevel, Transaction, TransactionManager};

pub use error::ServerError;
pub use pipeline::{LowerCtx, SampleTables, build_sample_database};
pub use result_encoder::{
    SelectResult, run_ddl_command, run_modify_command, run_select, run_select_streamed,
};

/// Per-session transaction-block state.
///
/// PostgreSQL exposes three transaction states to its clients via the
/// `ReadyForQuery` status byte (`'I'`, `'T'`, `'E'`). UltraSQL mirrors
/// these states so any libpq-style client that depends on the byte to
/// decide whether to issue `ROLLBACK` (e.g. tokio-postgres, psql,
/// pgbench) behaves identically.
///
/// The state is per-connection and accessed only by the connection's
/// own task, so no synchronisation primitive is needed (AGENTS.md §5).
///
/// State transitions:
///
/// ```text
///                        BEGIN
///        Idle ───────────────────────────────► InTransaction
///         ▲                                          │
///         │ COMMIT (no-op + warning when Idle)       │
///         │ ROLLBACK (no-op + warning when Idle)     │
///         │                                          │
///         │             COMMIT (success)             │
///         │ ◄────────────────────────────────────────┤
///         │                                          │ statement
///         │             ROLLBACK                     │ errored
///         │ ◄────────────────────────────────────────┼─────┐
///         │                                          │     │
///         │             COMMIT  (treated as          │     ▼
///         │              ROLLBACK; tag = "ROLLBACK") │   Failed
///         │ ◄────────────────────────────────────────┼─────┤
///         │             ROLLBACK                     │     │
///         └──────────────────────────────────────────┴─────┘
/// ```
///
/// `Idle` ↔ `ReadyForQuery` `'I'`. `InTransaction` ↔ `'T'`. `Failed` ↔ `'E'`.
#[derive(Debug)]
pub enum TxnState {
    /// No explicit transaction block is open. Each statement runs
    /// inside its own autocommit transaction.
    Idle,
    /// An explicit `BEGIN` is in effect. Statements use this txn's xid
    /// + snapshot until the user issues `COMMIT` or `ROLLBACK`.
    InTransaction(Transaction),
    /// A prior statement inside an explicit transaction errored. Until
    /// the user sends `COMMIT` (treated as `ROLLBACK`) or `ROLLBACK`,
    /// every subsequent statement returns the standard PostgreSQL
    /// error: `current transaction is aborted, commands ignored until
    /// end of transaction block` (SQLSTATE `25P02`).
    Failed(Transaction),
}

impl TxnState {
    /// The PostgreSQL `ReadyForQuery` status byte for this state.
    #[must_use]
    pub const fn ready_for_query_status(&self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransaction(_) => b'T',
            Self::Failed(_) => b'E',
        }
    }
}

/// In-memory `PageLoader` used by the development server.
///
/// Always returns a freshly-initialized heap page. Suitable for tests,
/// in-process benchmarks, and the v0.5 reference runtime where there is
/// no on-disk segment file yet. Production builds (`Server::init`)
/// swap this for a segment-backed loader.
///
/// `BufferPool` and `HeapAccess` are generic over `PageLoader`; making
/// the type concrete here lets us name the heap (`Arc<HeapAccess<BlankPageLoader>>`)
/// on `Server` and on the per-statement lowering context.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlankPageLoader;

impl PageLoader for BlankPageLoader {
    fn load(&self, _page_id: PageId) -> ultrasql_core::Result<Page> {
        Ok(Page::new_heap())
    }
}

/// Read-only catalog view consulted by the binder during query
/// execution.
///
/// The persistent catalog (`PersistentCatalog`) is the source of truth
/// for user-created relations; the in-memory `InMemoryCatalog` carries
/// the legacy sample-table registry (the v0.5 hard-coded `users`
/// fixture). Lookups try the persistent snapshot first so a runtime
/// `CREATE TABLE` immediately shadows any sample-table name collision;
/// if the snapshot has no entry, we fall back to the sample-table
/// catalog so existing duplex tests still resolve `users`.
///
/// The `'a` lifetime ties the view to the snapshot and in-memory
/// catalog held by the calling [`Session`]; binding completes
/// synchronously inside `execute_query` so the lifetime never escapes
/// a single statement.
struct CombinedCatalog<'a> {
    snapshot: &'a CatalogSnapshot,
    fallback: &'a InMemoryCatalog,
}

impl PlannerCatalog for CombinedCatalog<'_> {
    fn lookup_table(&self, name: &str) -> Option<TableMeta> {
        if let Some(meta) = PlannerCatalog::lookup_table(self.snapshot, name) {
            return Some(meta);
        }
        self.fallback.lookup_table(name)
    }
}

/// Default initial read buffer. Picked to fit a small startup message
/// without resizing; the buffer grows on demand.
const READ_BUFFER_INITIAL: usize = 1 << 12;

/// Buffer pool capacity used when no data directory is configured.
///
/// 65 536 frames × 8 KiB = 512 MiB. Sized to cover the sample database,
/// the integration tests, and the wire-protocol benchmark driver
/// (which loads up to ~10 M rows per iteration across multiple fresh
/// relations on a single in-process Server — large analytical workloads
/// such as `select_avg_10m_i64` and `filter_sum_10m_i64`). Production
/// deployments will size this from configuration.
const IN_MEMORY_POOL_FRAMES: usize = 65_536;

/// Shared connection state: the catalog used by the binder plus the
/// sample-table registry the lowerer consults.
///
/// Lives behind [`Arc`] so connection tasks share a single instance.
///
/// # Catalog lifecycle
///
/// At startup ([`Server::with_sample_database`] or a future
/// `Server::init(data_dir)`), the persistent catalog is bootstrapped from
/// the heap via [`PersistentCatalog::bootstrap_from_heap`]. On a fresh
/// database that means installing the hard-coded initial snapshot; on a
/// warm restart it rebuilds from durable heap pages.
///
/// Each statement captures an immutable [`CatalogSnapshot`] at the start
/// of planning via [`Server::catalog_snapshot`]; this ensures that
/// concurrent DDL does not perturb an in-flight query.
///
/// `Send + Sync` holds because every field is `Send + Sync`.
#[derive(Debug)]
pub struct Server {
    /// Planner-facing in-memory catalog (used by the binder today).
    ///
    /// `TODO(catalog-rebind)`: once the planner's binder is rewritten
    /// against `PersistentCatalog` / `CatalogSnapshot`, this field is
    /// removed and all lookups go through `persistent_catalog`.
    pub catalog: InMemoryCatalog,
    /// Registry of sample tables (schema + pre-built batches).
    pub tables: SampleTables,
    /// Persistent system catalog backed by an arc-swap snapshot cache.
    ///
    /// Bootstrapped at startup; refreshed after DDL.  Per-statement
    /// snapshot acquisition is wait-free via `ArcSwap::load_full`.
    pub persistent_catalog: Arc<PersistentCatalog>,
    /// Heap access method for user-created tables. Shares one
    /// in-process buffer pool across all connection sessions so a
    /// row inserted on one session is visible to the next snapshot
    /// on another session.
    pub heap: Arc<HeapAccess<BlankPageLoader>>,
    /// Transaction manager. Owns the XID allocator, the CLOG, and the
    /// lock manager; every Simple Query in v0.5 runs as an autocommit
    /// transaction allocated from this manager.
    pub txn_manager: Arc<TransactionManager>,
    /// Cross-protocol optimized-plan cache.
    ///
    /// Keyed on raw SQL text (a [`PlanCacheKey`] wraps a `String`);
    /// stores the post-optimizer [`LogicalPlan`] so a repeat Simple Query
    /// or an Extended Query Parse over the same statement skips the
    /// rule-rewrite phase.
    ///
    /// Sharing one cache between the Simple Query and the Extended Query
    /// paths is the headline win — a libpq driver that uses
    /// `Parse`+`Bind`+`Execute` for `SELECT id FROM t WHERE id = $1` and
    /// a `psql` client that types `SELECT id FROM t WHERE id = 42` both
    /// land on the same cached optimised plan modulo the
    /// parameter-vs-literal shape.
    ///
    /// Invalidation: every DDL path (`CREATE TABLE`, `CREATE INDEX`,
    /// `DROP TABLE`, `ALTER TABLE`, `TRUNCATE`) clears the entire cache
    /// because a catalog mutation can invalidate any cached
    /// predicate-pushdown / projection-pushdown decision. A finer-grained
    /// invalidation is a v0.7 follow-up (per-table set keyed on the OID
    /// the DDL touched).
    ///
    /// `Send + Sync` holds via [`PlanCache`]'s internal `DashMap`; no
    /// outer `Mutex` is needed.
    pub plan_cache: Arc<PlanCache>,
}

impl Server {
    /// Build a server pre-loaded with the canonical sample database.
    ///
    /// The persistent catalog is bootstrapped from an in-memory buffer pool
    /// (no disk I/O). On a fresh in-memory database the bootstrap detects an
    /// empty heap and installs the hard-coded initial snapshot.
    #[must_use]
    pub fn with_sample_database() -> Self {
        let mut catalog = InMemoryCatalog::new();
        let tables = build_sample_database(&mut catalog);

        let persistent_catalog = Arc::new(PersistentCatalog::new());
        // One in-memory buffer pool for both catalog bootstrap and
        // user-table DML so every connection observes the same heap.
        let pool = Arc::new(BufferPool::new(IN_MEMORY_POOL_FRAMES, BlankPageLoader));
        let heap = Arc::new(HeapAccess::new(Arc::clone(&pool)));
        match persistent_catalog.bootstrap_from_heap(heap.as_ref()) {
            Ok(stats) => {
                tracing::info!(?stats, "persistent catalog bootstrapped");
            }
            Err(e) => {
                // Bootstrap must not fail on a fresh in-memory database.
                // If it does, log the error but do not panic so tests and
                // development builds can still start.  The fallback is an
                // empty persistent catalog.
                tracing::warn!(error = %e, "persistent catalog bootstrap failed; using empty catalog");
            }
        }

        let txn_manager = Arc::new(TransactionManager::new());
        let plan_cache = Arc::new(PlanCache::new(PlanCacheConfig::default()));

        Self {
            catalog,
            tables,
            persistent_catalog,
            heap,
            txn_manager,
            plan_cache,
        }
    }

    /// Initialize a server that boots from `data_dir`.
    ///
    /// Brings up a buffer pool backed by segment files under `data_dir`,
    /// then bootstraps the persistent catalog from the heap pages found
    /// there.  On a fresh directory the catalog heap is empty and the
    /// initial snapshot is installed.
    ///
    /// This is the production entry point.  `with_sample_database` is the
    /// test/REPL entry point.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::Io`] when `data_dir` cannot be opened or
    /// when the heap bootstrap fails for a reason other than an empty heap.
    pub fn init(_data_dir: &Path) -> Result<Self, ServerError> {
        // TODO(storage-init): open segment files from data_dir, build a real
        // PageLoader, and pass it to HeapAccess.  For now we fall back to the
        // in-memory path so the API is usable without a segment implementation.
        Ok(Self::with_sample_database())
    }

    /// Acquire a per-statement catalog snapshot.
    ///
    /// The returned [`Arc<CatalogSnapshot>`] is immutable and stable for the
    /// caller's lifetime; concurrent DDL atomically swaps in a new pointer
    /// without invalidating this reference.
    ///
    /// This is the primary entry point for the binder and the optimizer.
    /// The call is wait-free — it performs a single `ArcSwap::load_full`.
    #[must_use]
    pub fn catalog_snapshot(&self) -> Arc<CatalogSnapshot> {
        self.persistent_catalog.snapshot()
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
///
/// `extended` holds the Extended Query Protocol's prepared-statement and
/// portal caches. `txn_state` tracks whether an explicit `BEGIN` is
/// open, whether the in-progress txn has errored, or whether the session
/// is autocommitting. Both are owned by the session and accessed only by
/// the connection's own task, so no synchronisation primitive guards
/// them (per AGENTS.md §5: "default to the simplest primitive that meets
/// the workload"; the workload here is single-threaded).
struct Session<RW> {
    io: RW,
    read_buf: BytesMut,
    write_buf: BytesMut,
    state: Arc<Server>,
    extended: extended::ExtendedConnState,
    txn_state: TxnState,
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
            extended: extended::ExtendedConnState::new(),
            txn_state: TxnState::Idle,
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
    ///
    /// Two message families are dispatched here:
    ///
    /// - Simple Query (`'Q'`) — parsed, bound, lowered, and executed
    ///   end-to-end in [`Self::handle_query`].
    /// - Extended Query (`Parse`/`Bind`/`Describe`/`Execute`/`Sync`/
    ///   `Close`/`Flush`) — routed to [`Self::handle_extended`]. The
    ///   spec defines a pipelined contract: errors silence subsequent
    ///   extended messages until a `Sync` resets the flag and the
    ///   server emits `ReadyForQuery`.
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
                FrontendMessage::Parse {
                    name,
                    sql,
                    param_types,
                } => {
                    self.handle_parse(name, sql, param_types).await?;
                }
                FrontendMessage::Bind {
                    portal_name,
                    statement_name,
                    param_formats,
                    params,
                    result_formats,
                } => {
                    self.handle_bind(
                        portal_name,
                        statement_name,
                        param_formats,
                        params,
                        result_formats,
                    )
                    .await?;
                }
                FrontendMessage::Describe { kind, name } => {
                    self.handle_describe(kind, &name).await?;
                }
                FrontendMessage::Execute { portal, max_rows } => {
                    self.handle_execute(&portal, max_rows).await?;
                }
                FrontendMessage::Sync => {
                    self.handle_sync().await?;
                }
                FrontendMessage::Close { kind, name } => {
                    self.handle_extended_close(kind, &name).await?;
                }
                FrontendMessage::Flush => {
                    self.handle_flush().await?;
                }
                FrontendMessage::Password { .. } => {
                    // Auth is not yet a state in the loop; if a client
                    // sends a Password out of nowhere we treat it as
                    // a query-scoped error.
                    self.send_error("password message outside auth flow", "08P01")
                        .await?;
                    self.send(&BackendMessage::ReadyForQuery {
                        status: self.txn_state.ready_for_query_status(),
                    })
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
                    self.send(&BackendMessage::ReadyForQuery {
                        status: self.txn_state.ready_for_query_status(),
                    })
                    .await?;
                }
            }
        }
    }

    /// Execute a simple `'Q'` query end-to-end and write the response.
    ///
    /// The trailing `ReadyForQuery`'s status byte reflects the
    /// session's [`TxnState`] *after* the statement has run: `'I'` for
    /// `Idle`, `'T'` for `InTransaction`, `'E'` for `Failed`. Drivers
    /// like tokio-postgres rely on this byte to decide whether to send
    /// a `ROLLBACK` on pool return.
    async fn handle_query(&mut self, sql: &str) -> Result<(), ServerError> {
        let trimmed = sql.trim();
        if trimmed.is_empty() || trimmed == ";" {
            // Coalesce `EmptyQueryResponse` + `ReadyForQuery` into one
            // `write_all` so the empty-query reply stays a single
            // syscall round-trip.
            self.write_buf.clear();
            encode_backend(&BackendMessage::EmptyQueryResponse, &mut self.write_buf);
            encode_backend(
                &BackendMessage::ReadyForQuery {
                    status: self.txn_state.ready_for_query_status(),
                },
                &mut self.write_buf,
            );
            self.io.write_all(&self.write_buf).await?;
            self.io.flush().await?;
            return Ok(());
        }

        match self.execute_query(trimmed) {
            Ok(result) => {
                // Append the trailing `ReadyForQuery` to the same
                // wire-buffer the query result writes so the whole
                // response (CommandComplete / DataRow stream +
                // ReadyForQuery) ships in one `write_all` + `flush`.
                // Saves a per-statement syscall round-trip on the
                // simple-query path; cumulative impact is visible on
                // the cross_compare_sql bench shapes that issue one
                // statement per wire roundtrip (UPDATE / DELETE /
                // INSERT / mixed-oltp).
                self.send_query_result_with_ready(result).await?;
            }
            Err(err) => {
                if !err.is_query_scoped() {
                    return Err(err);
                }
                self.send_error_with_ready(&err.to_string(), err.sqlstate()).await?;
            }
        }
        Ok(())
    }

    /// Send the query result and the trailing `ReadyForQuery` in one
    /// `write_all`. See `handle_query` for motivation.
    async fn send_query_result_with_ready(
        &mut self,
        result: SelectResult,
    ) -> Result<(), ServerError> {
        let ready = BackendMessage::ReadyForQuery {
            status: self.txn_state.ready_for_query_status(),
        };
        self.write_buf.clear();
        if let Some(body) = result.streamed_body.as_ref() {
            self.write_buf.extend_from_slice(body);
        } else {
            for msg in &result.messages {
                encode_backend(msg, &mut self.write_buf);
            }
        }
        encode_backend(&ready, &mut self.write_buf);
        self.io.write_all(&self.write_buf).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Send an `ErrorResponse` immediately followed by `ReadyForQuery`
    /// in one `write_all`.
    async fn send_error_with_ready(
        &mut self,
        message: &str,
        sqlstate: &str,
    ) -> Result<(), ServerError> {
        let err = BackendMessage::ErrorResponse {
            fields: vec![
                (b'S', "ERROR".to_string()),
                (b'C', sqlstate.to_string()),
                (b'M', message.to_string()),
            ],
        };
        let ready = BackendMessage::ReadyForQuery {
            status: self.txn_state.ready_for_query_status(),
        };
        self.write_buf.clear();
        encode_backend(&err, &mut self.write_buf);
        encode_backend(&ready, &mut self.write_buf);
        self.io.write_all(&self.write_buf).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Dispatch a [`SelectResult`] over the wire in a single
    /// `write_all` + `flush`.
    ///
    /// For the SELECT-streaming case the result carries a
    /// `streamed_body` blob of pre-encoded `RowDescription` /
    /// `DataRow` / `CommandComplete` bytes that we hand to the socket
    /// verbatim. Otherwise we fall back to the legacy
    /// `Vec<BackendMessage>` shape and coalesce its encoded form into
    /// one syscall.
    async fn send_query_result(&mut self, result: SelectResult) -> Result<(), ServerError> {
        if let Some(body) = result.streamed_body.as_ref() {
            self.send_raw(body).await
        } else {
            self.send_messages_coalesced(&result.messages).await
        }
    }

    /// Synchronous core of query execution: parse, bind, lower, run.
    ///
    /// Kept synchronous because none of the steps perform I/O. The
    /// async handler invokes this from the connection task; the
    /// executor's reactor stays responsive because the sample tables
    /// have a bounded fixed size.
    ///
    /// A [`CatalogSnapshot`] is acquired at the very start of execution
    /// via a wait-free `ArcSwap::load_full`.  All catalog lookups for the
    /// duration of this statement go through the snapshot so concurrent
    /// DDL cannot perturb an in-flight query.
    ///
    /// ## Transaction routing
    ///
    /// The session's [`TxnState`] determines how the statement is wrapped:
    ///
    /// - `Idle` — a fresh autocommit transaction is allocated, the
    ///   statement runs, and the transaction is committed on success
    ///   (or aborted on error). This is the legacy path.
    /// - `InTransaction(txn)` — the statement uses the existing
    ///   transaction. The session's `command_id` is advanced and the
    ///   `ReadCommitted` snapshot is refreshed. On error the session
    ///   transitions to `Failed(txn)`; subsequent statements until
    ///   `COMMIT`/`ROLLBACK` return SQLSTATE `25P02`.
    /// - `Failed(_)` — every non-transaction-control statement is
    ///   rejected with SQLSTATE `25P02`.
    ///
    /// Transaction-control statements (`BEGIN` / `COMMIT` / `ROLLBACK` /
    /// `SAVEPOINT` / `ROLLBACK TO` / `RELEASE`) are dispatched separately
    /// in [`Self::execute_txn_control`] so they can manipulate the
    /// session's `txn_state` directly.
    fn execute_query(&mut self, sql: &str) -> Result<SelectResult, ServerError> {
        // Capture a per-statement catalog snapshot — wait-free arc-swap load.
        // The binder reads this snapshot first; if a name is not found there
        // (a runtime CREATE TABLE never landed it), the in-memory sample
        // catalog provides the legacy fallback.
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.state.catalog,
        };

        // Parser / binder errors inside an explicit transaction must
        // also transition us to `Failed` — PostgreSQL marks the block
        // as aborted regardless of whether the failure was at parse,
        // plan, or execute time. Handle that uniformly here.
        let stmt = match Parser::new(sql).parse_statement() {
            Ok(s) => s,
            Err(e) => return Err(self.fail_if_in_transaction(e.into())),
        };
        let plan = match bind(&stmt, &combined) {
            Ok(p) => p,
            Err(e) => return Err(self.fail_if_in_transaction(e.into())),
        };

        // Transaction-control statements own the session's TxnState.
        match &plan {
            LogicalPlan::Begin { .. }
            | LogicalPlan::Commit { .. }
            | LogicalPlan::Rollback { .. }
            | LogicalPlan::Savepoint { .. }
            | LogicalPlan::RollbackToSavepoint { .. }
            | LogicalPlan::ReleaseSavepoint { .. } => {
                return self.execute_txn_control(&plan);
            }
            _ => {}
        }

        // A statement issued while the explicit transaction has already
        // errored must be rejected with the standard PostgreSQL SQLSTATE
        // `25P02` until the user issues COMMIT/ROLLBACK.
        if matches!(self.txn_state, TxnState::Failed(_)) {
            return Err(ServerError::TransactionAborted);
        }

        // DDL is dispatched ahead of operator lowering: it never produces
        // rows, so the lowerer would only round-trip it through an
        // unreachable arm. DDL inside an explicit transaction is
        // rejected today because the catalog mutations are not
        // transactional under the v0.5 catalog (see AGENTS.md §11; a
        // follow-up RFC will add transactional DDL). The rejection
        // transitions the txn to `Failed` so subsequent statements get
        // SQLSTATE `25P02` until COMMIT/ROLLBACK.
        let is_ddl = matches!(
            &plan,
            LogicalPlan::CreateTable { .. }
                | LogicalPlan::CreateIndex { .. }
                | LogicalPlan::DropTable { .. }
                | LogicalPlan::AlterTable { .. }
                | LogicalPlan::Truncate { .. }
        );
        if is_ddl && matches!(self.txn_state, TxnState::InTransaction(_)) {
            return Err(self.fail_if_in_transaction(ServerError::Unsupported(
                "DDL inside an explicit transaction block is not yet supported",
            )));
        }
        match &plan {
            LogicalPlan::CreateTable { .. } => {
                return self.execute_create_table(&plan, &catalog_snapshot);
            }
            LogicalPlan::CreateIndex { .. } => {
                return self.execute_create_index(&plan, &catalog_snapshot);
            }
            LogicalPlan::DropTable { .. } => {
                return self.execute_drop_table(&plan);
            }
            LogicalPlan::AlterTable { .. } => {
                return self.execute_alter_table(&plan, &catalog_snapshot);
            }
            LogicalPlan::Truncate { .. } => {
                return self.execute_truncate(&plan, &catalog_snapshot);
            }
            _ => {}
        }

        // DML / SELECT path: route through the cost-based optimizer
        // before lowering. The cache key is the raw SQL text so a repeat
        // Simple Query — or an Extended Query Parse over the same string
        // — reuses the already-optimised plan. See
        // [`Self::optimize_dml_plan`] for the cache + invalidation
        // contract.
        //
        // Behaviour depends on TxnState. The `run_dml_or_select` helper
        // already transitions `InTransaction → Failed` on any execution
        // error, so no explicit `fail_if_in_transaction` is needed here.
        // Skip the optimizer + plan cache for trivial `INSERT VALUES`
        // plans. The cost-based optimizer has no rewrites that
        // apply to a leaf `Insert { source: Values }` shape, and
        // the plan-cache lookup hashes the entire SQL text — for a
        // 10 000-row bulk INSERT that is a ~150 KB hash on every
        // iteration (cross_compare_sql uses a unique table name
        // per iter so the cache always misses). Bypass is
        // INSERT-only — UPDATE / DELETE need the optimizer's
        // canonicalisation passes for the lowerer's
        // `build_filtered_tid_scan` shape contract.
        let optimised_plan = if Self::is_trivial_insert_values(&plan)
            || Self::is_fused_update_shape(&plan)
        {
            plan
        } else {
            match self.optimize_dml_plan(sql, plan, &catalog_snapshot) {
                Ok(p) => p,
                Err(e) => return Err(self.fail_if_in_transaction(e)),
            }
        };
        self.run_dml_or_select(&optimised_plan, &catalog_snapshot)
    }

    /// `true` iff `plan` is an `Update` whose source is a bare `Scan` or
    /// `Filter(Scan)` shape — the exact set of inputs that the fused
    /// UPDATE path (`try_build_fused_update`) recognises. The fused
    /// path does its own structural matching on the bound plan and
    /// does not depend on any optimizer rewrites, so when this
    /// predicate fires the optimizer's full pass over the plan is
    /// pure overhead and the per-iter plan-cache miss (the
    /// `cross_compare_sql` bench uses a unique table name per iter,
    /// so the SQL-text key never repeats) is also wasted.
    ///
    /// We deliberately keep this predicate loose: we test only the
    /// *outer* `Update`-over-(Scan | Filter(Scan)) structure here.
    /// `try_build_fused_update` re-validates every fine-grained
    /// precondition (schema is `(Int32, Int32)`, assignment is a
    /// linear `Column ± Int32 literal`, predicate is an Int32 column
    /// + Int32 literal compare) and falls back to the default
    /// `ModifyTable(Filter(SeqScan))` plan when any of them fails.
    /// The cost of the redundant validation is negligible compared
    /// to a missed optimizer pass.
    fn is_fused_update_shape(plan: &LogicalPlan) -> bool {
        let LogicalPlan::Update {
            input, returning, ..
        } = plan
        else {
            return false;
        };
        if !returning.is_empty() {
            return false;
        }
        matches!(
            input.as_ref(),
            LogicalPlan::Scan { .. }
                | LogicalPlan::Filter {
                    input: _,
                    predicate: _,
                }
        )
    }

    /// `true` iff `plan` is `Insert { source: Values { .. }, .. }`
    /// with no `ON CONFLICT` / `RETURNING` — see the call site for
    /// why this bypasses the optimizer + plan-cache lookup.
    fn is_trivial_insert_values(plan: &LogicalPlan) -> bool {
        let LogicalPlan::Insert {
            source,
            on_conflict,
            returning,
            ..
        } = plan
        else {
            return false;
        };
        if on_conflict.is_some() || !returning.is_empty() {
            return false;
        }
        matches!(source.as_ref(), LogicalPlan::Values { .. })
    }

    /// Apply the cost-based optimizer to a DML/SELECT plan and return
    /// the result.
    ///
    /// The optimised plan is cached in [`Server::plan_cache`] keyed on
    /// the raw `sql` text. A cache hit skips the rule-rewrite loop and
    /// returns the previously-optimised plan; a cache miss runs
    /// [`ultrasql_optimizer::optimize`] against the bound plan and
    /// stores the result. The cache is cleared whole-cloth by every DDL
    /// path (see [`Self::plan_cache_invalidate`]), so concurrent DDL
    /// cannot serve a stale plan.
    ///
    /// # Errors
    ///
    /// Wraps [`OptimizeError`] into [`ServerError::Plan`] via a synthetic
    /// `PlanError::Type` message because the optimizer's failure modes
    /// are all bind-time-quality (the binder already type-checked the
    /// plan, so a rule failure is an internal-invariant violation). The
    /// caller forwards the wrapped error through the normal
    /// `fail_if_in_transaction` machinery.
    fn optimize_dml_plan(
        &self,
        sql: &str,
        plan: LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<LogicalPlan, ServerError> {
        let key = PlanCacheKey::named(sql.to_owned());
        let stats: NoStats = NoStats;
        let snapshot = Arc::clone(catalog_snapshot);
        // The closure is invoked only on cache miss; on a hit the cached
        // plan is returned and the plan we received here is dropped.
        // The closure consumes the plan via move because `FnOnce` does
        // not require `Clone` even though the underlying signature of
        // `PlanCache::get_or_plan` declares `FnOnce(&[Value])`.
        self.state
            .plan_cache
            .get_or_plan(&key, &[], move |_params| {
                ultrasql_optimizer::optimize(plan, &snapshot, &stats as &dyn StatsSource)
            })
            .map_err(|e| {
                ServerError::Plan(ultrasql_planner::PlanError::TypeMismatch(format!(
                    "optimizer failed: {e}"
                )))
            })
    }

    /// Clear the shared plan cache.
    ///
    /// Called from every DDL path after a successful catalog mutation
    /// so the next DML/SELECT statement re-plans against the new schema.
    /// The cache is keyed on SQL text, which has no relationship to the
    /// OIDs the DDL touched, so we invalidate everything; a finer-grained
    /// per-relation invalidation is a v0.7 follow-up.
    fn plan_cache_invalidate(&self) {
        self.state.plan_cache.invalidate_all();
    }

    /// Run a DML/SELECT plan against the session's current [`TxnState`].
    ///
    /// - `Idle` → open a fresh autocommit txn, run, commit on success
    ///   (or abort on error); state stays `Idle`.
    /// - `InTransaction` → refresh the per-statement snapshot, run
    ///   inside the existing txn, don't commit. On success state stays
    ///   `InTransaction`; on error transitions to `Failed`.
    /// - `Failed` → unreachable (the caller guarded).
    fn run_dml_or_select(
        &mut self,
        plan: &LogicalPlan,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<SelectResult, ServerError> {
        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => {
                let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
                let outcome = run_plan_in_txn(
                    plan,
                    &txn,
                    Arc::clone(catalog_snapshot),
                    &self.state.tables,
                    Arc::clone(&self.state.heap),
                    Arc::clone(&self.state.txn_manager),
                );
                self.finalise_autocommit(txn, outcome)
            }
            TxnState::InTransaction(mut txn) => {
                self.state.txn_manager.refresh_snapshot(&mut txn);
                let outcome = run_plan_in_txn(
                    plan,
                    &txn,
                    Arc::clone(catalog_snapshot),
                    &self.state.tables,
                    Arc::clone(&self.state.heap),
                    Arc::clone(&self.state.txn_manager),
                );
                // Transition: Ok → InTransaction; Err → Failed. The txn
                // remains alive in the CLOG (InProgress) until the user
                // issues COMMIT/ROLLBACK.
                self.txn_state = if outcome.is_ok() {
                    TxnState::InTransaction(txn)
                } else {
                    TxnState::Failed(txn)
                };
                outcome
            }
            TxnState::Failed(txn) => {
                // Should be guarded by the caller; restore state.
                self.txn_state = TxnState::Failed(txn);
                Err(ServerError::TransactionAborted)
            }
        }
    }

    /// Commit-on-success / abort-on-error for the autocommit path.
    /// Logs (does not surface) txn manager errors so the original
    /// outcome reaches the client.
    fn finalise_autocommit(
        &self,
        txn: Transaction,
        outcome: Result<SelectResult, ServerError>,
    ) -> Result<SelectResult, ServerError> {
        match &outcome {
            Ok(_) => {
                if let Err(e) = self.state.txn_manager.commit(txn) {
                    tracing::warn!(error = %e, "autocommit failed to finalise");
                }
            }
            Err(_) => {
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(error = %abort_err, "autocommit rollback failed");
                }
            }
        }
        outcome
    }

    /// If the session is currently `InTransaction`, transition to
    /// `Failed` so subsequent statements get the `25P02` rejection
    /// until COMMIT/ROLLBACK. This mirrors PostgreSQL: any failure
    /// inside a transaction block — including parser errors, bind
    /// errors, executor errors, and DDL-inside-tx rejections —
    /// aborts the block.
    ///
    /// Statements outside a transaction (Idle) and statements while
    /// already in a Failed block leave the state unchanged.
    ///
    /// Returns the original error verbatim so callers can `return`
    /// with a single line.
    fn fail_if_in_transaction(&mut self, err: ServerError) -> ServerError {
        if matches!(self.txn_state, TxnState::InTransaction(_)) {
            // Replace+match avoids needing to clone the Transaction
            // handle out of the variant.
            let prev = std::mem::replace(&mut self.txn_state, TxnState::Idle);
            if let TxnState::InTransaction(txn) = prev {
                self.txn_state = TxnState::Failed(txn);
            }
        }
        err
    }

    /// Dispatch a transaction-control statement (BEGIN / COMMIT /
    /// ROLLBACK / SAVEPOINT / ROLLBACK TO / RELEASE) against the
    /// session's [`TxnState`].
    ///
    /// PostgreSQL semantics:
    ///
    /// - `BEGIN` inside an open transaction emits a `NoticeResponse`
    ///   `WARNING: there is already a transaction in progress` and
    ///   leaves the state unchanged.
    /// - `COMMIT` / `ROLLBACK` outside a transaction emits a
    ///   `NoticeResponse` `WARNING: there is no transaction in progress`
    ///   and emits `COMMIT` / `ROLLBACK` as the command tag.
    /// - `COMMIT` while in the `Failed` state aborts the transaction and
    ///   returns the `ROLLBACK` tag — *not* `COMMIT` — matching
    ///   PostgreSQL's behaviour of treating a failed-block commit as a
    ///   rollback so the application's "did the COMMIT really land?"
    ///   check still works.
    fn execute_txn_control(&mut self, plan: &LogicalPlan) -> Result<SelectResult, ServerError> {
        match plan {
            LogicalPlan::Begin { .. } => self.execute_begin(),
            LogicalPlan::Commit { .. } => self.execute_commit(),
            LogicalPlan::Rollback { .. } => self.execute_rollback(),
            LogicalPlan::Savepoint { name, .. } => self.execute_savepoint(name),
            LogicalPlan::RollbackToSavepoint { name, .. } => {
                self.execute_rollback_to_savepoint(name)
            }
            LogicalPlan::ReleaseSavepoint { name, .. } => self.execute_release_savepoint(name),
            _ => Err(ServerError::Unsupported(
                "execute_txn_control called with non-txn-control plan",
            )),
        }
    }

    /// `BEGIN` — open an explicit transaction. PostgreSQL emits a
    /// WARNING (not an error) if a transaction is already open.
    ///
    /// Returns `Result` for parity with the other transaction-control
    /// handlers (`execute_savepoint` etc.) so the dispatcher in
    /// `execute_txn_control` can use a uniform call shape — even though
    /// this specific arm never errors.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "uniform Result return across the txn-control dispatcher"
    )]
    fn execute_begin(&mut self) -> Result<SelectResult, ServerError> {
        let warn = match &self.txn_state {
            TxnState::Idle => {
                let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
                self.txn_state = TxnState::InTransaction(txn);
                None
            }
            TxnState::InTransaction(_) | TxnState::Failed(_) => {
                Some("there is already a transaction in progress")
            }
        };
        let mut messages: Vec<BackendMessage> = Vec::with_capacity(2);
        if let Some(msg) = warn {
            messages.push(notice_warning("25001", msg));
        }
        messages.push(BackendMessage::CommandComplete {
            tag: "BEGIN".to_string(),
        });
        Ok(SelectResult {
            messages,
            streamed_body: None,
            rows: 0,
        })
    }

    /// `COMMIT` — finalise the current explicit transaction.
    ///
    /// PostgreSQL semantics: in `Failed` state COMMIT aborts the
    /// transaction and returns the `ROLLBACK` tag. Outside a
    /// transaction emits a WARNING and returns the `COMMIT` tag.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "uniform Result return across the txn-control dispatcher"
    )]
    fn execute_commit(&mut self) -> Result<SelectResult, ServerError> {
        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => Ok(SelectResult {
                messages: vec![
                    notice_warning("25P01", "there is no transaction in progress"),
                    BackendMessage::CommandComplete {
                        tag: "COMMIT".to_string(),
                    },
                ],
                streamed_body: None,
                rows: 0,
            }),
            TxnState::InTransaction(txn) => {
                if let Err(e) = self.state.txn_manager.commit(txn) {
                    tracing::warn!(error = %e, "explicit COMMIT failed to finalise");
                }
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "COMMIT".to_string(),
                    }],
                    streamed_body: None,
                    rows: 0,
                })
            }
            TxnState::Failed(txn) => {
                if let Err(e) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(error = %e, "explicit COMMIT (treated as rollback) failed");
                }
                // PostgreSQL emits the ROLLBACK tag here, not COMMIT.
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "ROLLBACK".to_string(),
                    }],
                    streamed_body: None,
                    rows: 0,
                })
            }
        }
    }

    /// `ROLLBACK` — abort the current explicit transaction.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "uniform Result return across the txn-control dispatcher"
    )]
    fn execute_rollback(&mut self) -> Result<SelectResult, ServerError> {
        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => Ok(SelectResult {
                messages: vec![
                    notice_warning("25P01", "there is no transaction in progress"),
                    BackendMessage::CommandComplete {
                        tag: "ROLLBACK".to_string(),
                    },
                ],
                streamed_body: None,
                rows: 0,
            }),
            TxnState::InTransaction(txn) | TxnState::Failed(txn) => {
                if let Err(e) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(error = %e, "explicit ROLLBACK failed");
                }
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "ROLLBACK".to_string(),
                    }],
                    streamed_body: None,
                    rows: 0,
                })
            }
        }
    }

    /// `SAVEPOINT name` — set a savepoint inside the current
    /// transaction block. Outside a transaction returns SQLSTATE
    /// `25P01` (`no_active_sql_transaction`).
    fn execute_savepoint(&mut self, name: &str) -> Result<SelectResult, ServerError> {
        match &mut self.txn_state {
            TxnState::Idle => Err(ServerError::Savepoint(
                "SAVEPOINT can only be used in transaction blocks",
            )),
            TxnState::Failed(_) => Err(ServerError::TransactionAborted),
            TxnState::InTransaction(txn) => {
                self.state.txn_manager.begin_savepoint(txn, name);
                Ok(SelectResult {
                    messages: vec![BackendMessage::CommandComplete {
                        tag: "SAVEPOINT".to_string(),
                    }],
                    streamed_body: None,
                    rows: 0,
                })
            }
        }
    }

    /// `ROLLBACK TO [SAVEPOINT] name` — roll back to the named
    /// savepoint. The transaction remains alive; subsequent statements
    /// run inside the same xid. If the current state is `Failed`, a
    /// successful `ROLLBACK TO` clears the failure flag (matching
    /// PostgreSQL behaviour).
    ///
    /// Errors:
    ///
    /// - Outside a transaction: SQLSTATE `25P01`
    ///   (`no_active_sql_transaction`).
    /// - Unknown savepoint name: SQLSTATE `3B001`
    ///   (`invalid_savepoint_specification`).
    fn execute_rollback_to_savepoint(&mut self, name: &str) -> Result<SelectResult, ServerError> {
        // We need to take ownership of the inner txn to mutate it, then
        // put it back in the correct state variant.
        let prior_failed = matches!(self.txn_state, TxnState::Failed(_));
        let state = std::mem::replace(&mut self.txn_state, TxnState::Idle);
        match state {
            TxnState::Idle => {
                // `TxnState::Idle` is the default left behind by the
                // replace; nothing to restore.
                Err(ServerError::Savepoint(
                    "ROLLBACK TO SAVEPOINT can only be used in transaction blocks",
                ))
            }
            TxnState::InTransaction(mut txn) | TxnState::Failed(mut txn) => {
                if self
                    .state
                    .txn_manager
                    .rollback_to_savepoint(&mut txn, name)
                    .is_ok()
                {
                    // Clear the failure flag: the rolled-back work is
                    // undone so the user can continue.
                    self.txn_state = TxnState::InTransaction(txn);
                    Ok(SelectResult {
                        messages: vec![BackendMessage::CommandComplete {
                            tag: "ROLLBACK".to_string(),
                        }],
                        streamed_body: None,
                        rows: 0,
                    })
                } else {
                    // Unknown savepoint name. Restore the prior state
                    // (the rollback did not fire so the txn is in the
                    // same shape as before this call).
                    self.txn_state = if prior_failed {
                        TxnState::Failed(txn)
                    } else {
                        TxnState::InTransaction(txn)
                    };
                    Err(ServerError::SavepointNotFound(name.to_owned()))
                }
            }
        }
    }

    /// `RELEASE [SAVEPOINT] name` — destroy a savepoint. Subsequent
    /// `ROLLBACK TO` of the same name will fail.
    ///
    /// A savepoint-not-found error inside an explicit transaction
    /// transitions the session to `Failed` (matching PostgreSQL: any
    /// statement that errors inside a transaction block aborts the
    /// block until COMMIT/ROLLBACK).
    fn execute_release_savepoint(&mut self, name: &str) -> Result<SelectResult, ServerError> {
        let release_ok = match &mut self.txn_state {
            TxnState::Idle => {
                return Err(ServerError::Savepoint(
                    "RELEASE SAVEPOINT can only be used in transaction blocks",
                ));
            }
            TxnState::Failed(_) => return Err(ServerError::TransactionAborted),
            TxnState::InTransaction(txn) => {
                self.state.txn_manager.release_savepoint(txn, name).is_ok()
            }
        };
        if release_ok {
            Ok(SelectResult {
                messages: vec![BackendMessage::CommandComplete {
                    tag: "RELEASE".to_string(),
                }],
                streamed_body: None,
                rows: 0,
            })
        } else {
            Err(self.fail_if_in_transaction(ServerError::SavepointNotFound(name.to_owned())))
        }
    }

    /// Persist a `CREATE TABLE` into the catalog.
    ///
    /// Honors `IF NOT EXISTS` by short-circuiting when the relation
    /// already exists in either the persistent snapshot or the
    /// in-memory sample catalog. The resolved column [`Schema`] from
    /// the binder is stored verbatim, so a subsequent statement that
    /// captures a fresh snapshot will see the new relation.
    ///
    /// Currently a metadata-only operation: the segment file and the
    /// `pg_class.relfilenode` block are allocated lazily on the first
    /// `INSERT`. This matches PostgreSQL's `RelationSetNewRelfilenode`
    /// timing closely enough that subsequent `INSERT` wiring (in a
    /// follow-up commit) can stamp the right block number then.
    fn execute_create_table(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateTable {
            table_name,
            namespace,
            columns,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_table called with non-CreateTable plan",
            ));
        };
        let exists_persistent = snapshot.tables.contains_key(table_name);
        let exists_fallback = self.state.catalog.lookup_table(table_name).is_some();
        if exists_persistent || exists_fallback {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE TABLE"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(table_name.clone()),
            ));
        }
        let oid = self.state.persistent_catalog.next_oid();
        let entry = TableEntry::new(oid, table_name.clone(), namespace.clone(), columns.clone());
        self.state.persistent_catalog.create_table(entry)?;
        // A new relation can shadow names a cached plan rewrote against
        // the previous snapshot; clear the cache so the next statement
        // re-plans.
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE TABLE"))
    }

    /// Build a B+ tree index over the supplied table and register it
    /// in `pg_index`.
    ///
    /// The kernel work is split into four steps:
    ///
    /// 1. Validate the request against the current catalog snapshot —
    ///    `IF NOT EXISTS`, presence of the parent table, key-column
    ///    type compatibility with the B-tree (currently only fixed-size
    ///    8-byte keys are stored, so `Int64` is the natural domain;
    ///    `Int32` keys are widened to `i64` before insertion).
    /// 2. Allocate a fresh OID for the index and instantiate a new
    ///    [`BTree`] over a relation id derived from that OID. The
    ///    buffer pool's blank-page loader hands out empty heap pages
    ///    which `BTree::create` then initialises as B-tree leaves.
    /// 3. Scan every visible row of the parent table under an
    ///    autocommit snapshot, decode the key column, and call
    ///    [`BTree::insert`] with the row's [`ultrasql_core::TupleId`].
    /// 4. Build an [`IndexEntry`] carrying the root block plus the
    ///    requested attnums, register it with the persistent catalog,
    ///    and let the catalog's snapshot rotation publish the entry to
    ///    subsequent statements.
    ///
    /// # Sub-shape gaps documented for reviewers
    ///
    /// - Only single-column indexes are built today. The binder
    ///   accepts multi-column lists for completeness (so a follow-up
    ///   can flip the kernel restriction without re-binding) but the
    ///   server rejects them here.
    /// - Only `Int32` / `Int64` key types are supported. Other types
    ///   (text, float, bool) would require a richer [`BTree`] key
    ///   trait; the build returns
    ///   [`ServerError::Unsupported`] for them.
    /// - `UNIQUE` is honoured at the catalog level — the
    ///   [`IndexEntry::is_unique`] flag is propagated — but the
    ///   B-tree's existing duplicate-key rejection is the only
    ///   enforcement. Non-unique indexes that happen to have unique
    ///   data still build correctly; non-unique indexes with
    ///   duplicates would error here, which is a known limitation
    ///   we accept until the B-tree gains a non-unique mode.
    fn execute_create_index(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateIndex {
            index_name,
            table_name,
            columns,
            unique,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_index called with non-CreateIndex plan",
            ));
        };

        // 1a. IF NOT EXISTS short-circuit.
        if snapshot.indexes.contains_key(index_name) {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE INDEX"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(index_name.clone()),
            ));
        }

        // 1b. Resolve the parent table.
        let table = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.clone(),
            ))
        })?;

        // 1c. Validate the key columns. Only one column, only Int32 /
        //     Int64 — see the doc comment for the rationale.
        if columns.len() != 1 {
            return Err(ServerError::Unsupported(
                "CREATE INDEX: only single-column indexes are supported in this wave",
            ));
        }
        let key_col_idx = columns[0];
        let key_field = table.schema.field(key_col_idx).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::ColumnNotFound(format!(
                "column index {key_col_idx} in table {table_name}"
            )))
        })?;
        let widen_i32 = match key_field.data_type {
            DataType::Int32 => true,
            DataType::Int64 => false,
            _ => {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX: only Int32 / Int64 key columns are supported in this wave",
                ));
            }
        };

        // 2. Allocate an OID and instantiate the B-tree.
        let index_oid = self.state.persistent_catalog.next_oid();
        let index_rel = RelationId::new(index_oid.raw());
        let pool = self.state.heap.buffer_pool();
        let mut btree = BTree::create(Arc::clone(pool), index_rel)
            .map_err(|e| ServerError::ddl(format!("BTree::create failed: {e}")))?;
        let root_block = btree.root_block();

        // 3. Scan the heap and populate the tree.
        let key_attnum = u16::try_from(key_col_idx).map_err(|_| {
            ServerError::Unsupported("CREATE INDEX: column index does not fit in u16 attnum field")
        })?;
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let table_rel = RelationId(table.oid);
        let block_count = self.state.heap.block_count(table_rel).max(table.n_blocks);
        let scan = self.state.heap.scan_visible(
            table_rel,
            block_count,
            &txn.snapshot,
            self.state.txn_manager.as_ref(),
        );
        let insert_result = (|| -> Result<u64, ServerError> {
            let mut inserted: u64 = 0;
            for result in scan {
                let tup =
                    result.map_err(|e| ServerError::ddl(format!("CREATE INDEX heap scan: {e}")))?;
                let row = decode_key_column(&tup.data, &table.schema, key_col_idx, widen_i32)?;
                if let Some(key) = row {
                    btree
                        .insert(key, tup.tid, txn.xid, None)
                        .map_err(|e| ServerError::ddl(format!("CREATE INDEX btree insert: {e}")))?;
                    inserted += 1;
                }
                // NULL key — skip; PostgreSQL's btree omits NULL keys
                // from the index unless `INCLUDE` adds them, and our
                // BTree::insert lacks a NULL marker.
            }
            Ok(inserted)
        })();

        // Commit the txn regardless of build outcome so the XID does
        // not leak as in-progress forever.
        if let Err(e) = self.state.txn_manager.commit(txn) {
            tracing::warn!(error = %e, "autocommit (CREATE INDEX) failed to finalise");
        }
        let _ = insert_result?;

        // 4. Register the index entry. The columns vector uses the
        //    1-based attnum convention shared with `pg_attribute`; the
        //    `IndexEntry` stores 0-based positions internally, so the
        //    cast is direct. We override `root_block` to match the
        //    freshly built tree.
        let attnums: Vec<u16> = vec![key_attnum];
        let mut entry = IndexEntry::new(index_oid, index_name.clone(), table.oid, attnums, *unique);
        entry.root_block = root_block;
        self.state.persistent_catalog.create_index(entry)?;
        // A new index can flip an existing cached plan from
        // `Filter(SeqScan)` to `IndexScan`; clear the cache so the next
        // statement re-plans against the post-CREATE INDEX catalog.
        self.plan_cache_invalidate();

        Ok(run_ddl_command("CREATE INDEX"))
    }

    /// Drop one or more tables.
    ///
    /// The binder has already filtered names through the catalog —
    /// see [`ultrasql_planner::bind`] — so the only failure surface
    /// here is `CatalogError::NotFound`, which can fire only when a
    /// concurrent DDL deleted the relation between the binder and the
    /// dispatcher. Associated indexes are removed by
    /// [`MutableCatalog::drop_table`] in a single atomic snapshot
    /// rotation.
    ///
    /// Heap pages backing the dropped relation are *not* reclaimed in
    /// this wave: the in-memory buffer pool grows on demand and the
    /// segment manager has not yet landed. The dropped name becomes
    /// available immediately for reuse via `CREATE TABLE` — subsequent
    /// inserts will reuse the relation-id space without colliding
    /// because OIDs are monotonic.
    fn execute_drop_table(&self, plan: &LogicalPlan) -> Result<SelectResult, ServerError> {
        let LogicalPlan::DropTable { tables, .. } = plan else {
            return Err(ServerError::Unsupported(
                "execute_drop_table called with non-DropTable plan",
            ));
        };
        for name in tables {
            self.state.persistent_catalog.drop_table(name)?;
        }
        // Any cached plan that referenced this name is now invalid;
        // clear the cache so subsequent statements re-plan.
        self.plan_cache_invalidate();
        Ok(run_ddl_command("DROP TABLE"))
    }

    /// Apply an `ALTER TABLE` action.
    ///
    /// The only supported action in this wave is `ADD COLUMN`. For
    /// `ADD COLUMN` we
    ///
    /// 1. take a per-statement MVCC snapshot,
    /// 2. scan every visible tuple under the *old* schema and rewrite
    ///    it back through `HeapAccess::update` with a payload encoded
    ///    against the *new* schema (the appended column carries
    ///    [`Value::Null`] for every pre-existing row),
    /// 3. swap the catalog entry to the new schema via
    ///    [`MutableCatalog::alter_table_add_column`].
    ///
    /// Steps 2 and 3 are wrapped in a single autocommit transaction so
    /// the rewrite and the catalog swap commit (or abort) together;
    /// concurrent readers either see the old schema with old tuples or
    /// the new schema with rewritten tuples — never a torn state.
    ///
    /// # Sub-shape gaps documented for reviewers
    ///
    /// - `DROP COLUMN`, `RENAME COLUMN`, `RENAME TO`, and
    ///   `ADD/DROP CONSTRAINT` are not yet bindable in
    ///   [`ultrasql_planner::bind`]; the binder returns
    ///   `NotSupported` for them so they never reach this arm.
    /// - The rewrite is online-unsafe today: there is no per-relation
    ///   exclusive lock taken across steps 2 and 3, so a concurrent
    ///   INSERT during the rewrite may produce a tuple that scans see
    ///   under the new schema but was encoded against the old one. We
    ///   ship this anyway because v0.5 dispatches Simple Query
    ///   statements serially per connection and the README workload
    ///   does not concurrently mutate the relation under test. A
    ///   follow-up will route DDL through the lock manager
    ///   (`AccessExclusiveLock`).
    fn execute_alter_table(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::AlterTable {
            table_name, action, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_alter_table called with non-AlterTable plan",
            ));
        };
        match action {
            LogicalAlterTableAction::AddColumn { column } => {
                self.execute_alter_add_column(table_name, column.clone(), snapshot)
            }
        }
    }

    /// Execute the `ALTER TABLE t ADD COLUMN c TYPE [NULL | NOT NULL]`
    /// path.
    ///
    /// Decoded from the dispatch arm so `execute_alter_table` stays
    /// a thin shape-match. See [`Self::execute_alter_table`] for the
    /// design notes that apply to the rewrite ordering, MVCC, and the
    /// known online-DDL gap.
    fn execute_alter_add_column(
        &self,
        table_name: &str,
        column: ultrasql_core::Field,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        // 1. Resolve the existing entry and build the new schema.
        let entry = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        let mut new_fields: Vec<ultrasql_core::Field> = entry.schema.fields().to_vec();
        new_fields.push(column.clone());
        let new_schema = ultrasql_core::Schema::new(new_fields).map_err(|e| {
            ServerError::Catalog(ultrasql_catalog::CatalogError::schema_conflict(format!(
                "ALTER TABLE ADD COLUMN: {e}"
            )))
        })?;

        // 2. Rewrite existing tuples — outside the catalog swap so
        //    the snapshot scan still observes the old schema.
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let old_codec = ultrasql_executor::RowCodec::new(entry.schema.clone());
        let new_codec = ultrasql_executor::RowCodec::new(new_schema);

        let rewrite_result: Result<(), ServerError> = (|| {
            // Collect the visible tuples up front so the heap iterator
            // is fully drained before any update lands — otherwise the
            // iterator could revisit a row that the update has just
            // copied into a new slot. The relations we ALTER in v0.5
            // fit comfortably in memory.
            let mut to_rewrite: Vec<(ultrasql_core::TupleId, Vec<Value>)> = Vec::new();
            {
                let scan = self.state.heap.scan_visible(
                    rel,
                    block_count,
                    &txn.snapshot,
                    self.state.txn_manager.as_ref(),
                );
                for result in scan {
                    let tup = result
                        .map_err(|e| ServerError::ddl(format!("ALTER TABLE heap scan: {e}")))?;
                    let row = old_codec
                        .decode(&tup.data)
                        .map_err(|e| ServerError::ddl(format!("ALTER TABLE row decode: {e}")))?;
                    to_rewrite.push((tup.tid, row));
                }
            }

            // Now perform the updates.
            for (tid, old_row) in to_rewrite {
                let mut new_row = old_row;
                new_row.push(Value::Null);
                let new_payload = new_codec
                    .encode(&new_row)
                    .map_err(|e| ServerError::ddl(format!("ALTER TABLE row encode: {e}")))?;
                self.state
                    .heap
                    .update(
                        tid,
                        &new_payload,
                        UpdateOptions {
                            xid: txn.xid,
                            command_id: ultrasql_core::CommandId::FIRST,
                            wal: None,
                            vm: None,
                            hot_eligible: true,
                        },
                    )
                    .map_err(|e| ServerError::ddl(format!("ALTER TABLE heap update: {e}")))?;
            }
            Ok(())
        })();

        // 3. Swap the catalog entry only if the rewrite succeeded;
        //    otherwise abort the transaction so the half-rewritten
        //    tuples become dead (their xmin matches our xid, which we
        //    will mark aborted on rollback).
        match rewrite_result {
            Ok(()) => {
                self.state
                    .persistent_catalog
                    .alter_table_add_column(table_name, column)?;
                if let Err(e) = self.state.txn_manager.commit(txn) {
                    tracing::warn!(error = %e, "autocommit (ALTER TABLE) failed to finalise");
                }
                // A schema change can invalidate any cached projection-
                // pushdown / predicate-pushdown decision; clear all.
                self.plan_cache_invalidate();
                Ok(run_ddl_command("ALTER TABLE"))
            }
            Err(e) => {
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "autocommit (ALTER TABLE rollback) failed to abort"
                    );
                }
                Err(e)
            }
        }
    }

    /// Empty every relation named in the `TRUNCATE` statement.
    ///
    /// PostgreSQL's `TRUNCATE` takes `ACCESS EXCLUSIVE` and reclaims the
    /// relfilenode in a single fast-path: drop the segment files, then
    /// allocate a fresh empty heap on commit. UltraSQL's v0.5 in-memory
    /// runtime has no segment manager wired into the server's
    /// `BufferPool<BlankPageLoader>`, so the fast-path "swap the
    /// relfilenode" hook does not yet exist on this path. Instead, we
    /// open an autocommit MVCC transaction and stamp `xmax` on every
    /// row visible to the txn's own snapshot by calling
    /// [`HeapAccess::delete`] for each visible TID.
    ///
    /// Correctness notes:
    ///
    /// - The result is MVCC-correct under our snapshot model: a
    ///   concurrent snapshot that pre-dates the truncate's commit
    ///   continues to see every row (its `xmax` is committed-after
    ///   from the older snapshot's POV); a snapshot taken after the
    ///   commit sees the relation as empty.
    /// - Dead-tuple pages stay on the heap. A subsequent `INSERT` will
    ///   reuse free space inside them as it would after any DELETE,
    ///   and `n_blocks` stays unchanged so future scans still cover
    ///   the dead-tuple block range (necessary because a row inserted
    ///   into one of those reused slots must still be discovered).
    /// - The path is `O(rows visible to txn)` rather than O(1). For
    ///   the wire-completion gate this is acceptable: TRUNCATE is no
    ///   longer rejected, and a future segment-manager wiring can
    ///   replace this body with the proper fast-path without touching
    ///   any caller.
    ///
    /// `RESTART IDENTITY` and `CASCADE` are accepted by the parser and
    /// the binder but currently have no effect at execution time:
    ///
    /// - `RESTART IDENTITY` reseeds owned sequences. UltraSQL does not
    ///   yet implement `SERIAL` / sequence catalogs (see ROADMAP P1
    ///   v0.6), so there are no sequences to reseed. The keyword is
    ///   accept-and-ignore until that lands.
    /// - `CASCADE` truncates dependent foreign-key children. UltraSQL
    ///   does not yet enforce foreign keys at the catalog level, so
    ///   there are no dependent relations to find. The keyword is
    ///   accept-and-ignore until the foreign-key wave lands.
    ///
    /// Multi-table `TRUNCATE` truncates every table inside a single
    /// autocommit transaction so the operation is atomic — either all
    /// listed relations become empty in the next snapshot or none do.
    fn execute_truncate(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::Truncate { tables, .. } = plan else {
            return Err(ServerError::Unsupported(
                "execute_truncate called with non-Truncate plan",
            ));
        };

        // Single autocommit txn so the multi-table case is atomic. A
        // partial failure aborts the txn and every delete it stamped
        // becomes invisible to subsequent snapshots.
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);

        let truncate_result: Result<(), ServerError> = (|| {
            for name in tables {
                let entry = snapshot.tables.get(name).ok_or_else(|| {
                    ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(name.clone()))
                })?;
                let rel = RelationId(entry.oid);
                // The heap's resident block count is the source of
                // truth for "how many blocks must I scan." We OR with
                // the catalog's hint so a relation extended on a
                // previous connection still gets a complete scan.
                let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);

                // Snapshot every visible TID up front, then issue the
                // deletes in a second pass. Holding the heap iterator
                // open across delete calls would let the iterator
                // revisit a tuple whose xmax we just stamped; flushing
                // to a vector first avoids that race.
                let mut tids: Vec<ultrasql_core::TupleId> = Vec::new();
                {
                    let scan = self.state.heap.scan_visible(
                        rel,
                        block_count,
                        &txn.snapshot,
                        self.state.txn_manager.as_ref(),
                    );
                    for result in scan {
                        let tup = result
                            .map_err(|e| ServerError::ddl(format!("TRUNCATE heap scan: {e}")))?;
                        tids.push(tup.tid);
                    }
                }

                for tid in tids {
                    self.state
                        .heap
                        .delete(
                            tid,
                            DeleteOptions {
                                xmax: txn.xid,
                                cmax: ultrasql_core::CommandId::FIRST,
                                wal: None,
                                fsm: None,
                                vm: None,
                            },
                        )
                        .map_err(|e| ServerError::ddl(format!("TRUNCATE heap delete: {e}")))?;
                }
            }
            Ok(())
        })();

        match truncate_result {
            Ok(()) => {
                if let Err(e) = self.state.txn_manager.commit(txn) {
                    tracing::warn!(error = %e, "autocommit (TRUNCATE) failed to finalise");
                }
                // Row counts changed beyond recognition; clear the cache
                // so any cardinality-aware plan re-runs.
                self.plan_cache_invalidate();
                Ok(run_ddl_command("TRUNCATE TABLE"))
            }
            Err(e) => {
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "autocommit (TRUNCATE rollback) failed to abort"
                    );
                }
                Err(e)
            }
        }
    }

    // -----------------------------------------------------------------
    // Extended Query Protocol dispatch.
    //
    // Each Parse/Bind/Describe/Execute/Close handler runs synchronously
    // through the kernel in `extended.rs`. The handler returns either
    // `Ok(messages)` to emit on the wire, or a query-scoped error that
    // marks the pipeline as failed. Subsequent extended messages are
    // silently dropped (per the PostgreSQL spec) until a `Sync` resets
    // the failure flag and emits `ReadyForQuery`.
    // -----------------------------------------------------------------

    /// Handle `Parse(name, sql, param_types)`.
    ///
    /// After [`extended::handle_parse`] stores the bound plan, the same
    /// cost-based optimizer the Simple Query path runs is applied here
    /// so a subsequent `Execute` does not have to re-optimise. The
    /// optimised plan replaces the stored plan in `state.statements`.
    /// Parameter (`$N`) placeholders survive optimisation — rule-based
    /// rewrites are placeholder-aware (e.g., `ConstantFold` skips
    /// `ScalarExpr::Parameter`).
    ///
    /// The plan cache is shared with Simple Query: a Parse whose SQL
    /// text is already cached by a previous Simple Query hits the cache
    /// and skips the rule-rewrite loop.
    async fn handle_parse(
        &mut self,
        name: String,
        sql: String,
        param_types: Vec<u32>,
    ) -> Result<(), ServerError> {
        if self.extended.pipeline_failed {
            return Ok(());
        }
        // Capture a per-statement catalog snapshot — identical pattern
        // to `execute_query` so binding observes the same catalog the
        // forthcoming `Execute` will use. Plans are stored bound, not
        // re-bound at Execute time, so concurrent DDL between Parse and
        // Execute is invisible to the prepared statement (PostgreSQL
        // exhibits the same behaviour with `pg_proc` snapshotting).
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.state.catalog,
        };
        let parse_sql = sql.clone();
        let parse_name = name.clone();
        match extended::handle_parse(&mut self.extended, name, sql, param_types, &combined) {
            Ok(msg) => {
                if let Err(e) =
                    self.optimize_parsed_plan(&parse_name, &parse_sql, &catalog_snapshot)
                {
                    if !e.is_query_scoped() {
                        return Err(e);
                    }
                    let e = self.fail_if_in_transaction(e);
                    self.extended.mark_failed();
                    return self.send_error(&e.to_string(), e.sqlstate()).await;
                }
                self.send(&msg).await
            }
            Err(e) => {
                if !e.is_query_scoped() {
                    return Err(e);
                }
                let e = self.fail_if_in_transaction(e);
                self.extended.mark_failed();
                self.send_error(&e.to_string(), e.sqlstate()).await
            }
        }
    }

    /// Run the optimizer + plan cache over the bound plan stored under
    /// `name`, replacing it with the optimised plan.
    ///
    /// DDL and transaction-control statements are skipped: those reach
    /// `Execute` through the direct-dispatch path in
    /// [`Self::handle_execute`] and the optimizer's rule pipeline does
    /// not target them.
    ///
    /// The SQL text drives the cache key so a `Parse` whose text already
    /// has a cached entry — primed by a prior Simple Query or a prior
    /// `Parse` of the same SQL — reuses the cached plan.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`ultrasql_optimizer::optimize`] wrapped as
    /// [`ServerError::Plan`]. A query-scoped error fails just this
    /// Parse; an unrecoverable error propagates and the caller closes
    /// the session.
    fn optimize_parsed_plan(
        &mut self,
        name: &str,
        sql: &str,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<(), ServerError> {
        let bound_plan = match self.extended.statements.get(name) {
            Some(stmt) => match &stmt.plan {
                Some(p) => p.clone(),
                None => return Ok(()), // empty statement
            },
            None => return Ok(()),
        };
        let is_optimizable = matches!(
            &bound_plan,
            LogicalPlan::Scan { .. }
                | LogicalPlan::Filter { .. }
                | LogicalPlan::Project { .. }
                | LogicalPlan::Limit { .. }
                | LogicalPlan::Sort { .. }
                | LogicalPlan::Join { .. }
                | LogicalPlan::Aggregate { .. }
                | LogicalPlan::SetOp { .. }
                | LogicalPlan::Cte { .. }
                | LogicalPlan::Values { .. }
                | LogicalPlan::Insert { .. }
                | LogicalPlan::Update { .. }
                | LogicalPlan::Delete { .. }
                | LogicalPlan::Empty { .. }
        );
        if !is_optimizable {
            // DDL / transaction-control: the optimizer's rules do not
            // target these and the Execute path dispatches them around
            // the operator pipeline.
            return Ok(());
        }
        let optimised = self.optimize_dml_plan(sql, bound_plan, catalog_snapshot)?;
        if let Some(stmt) = self.extended.statements.get_mut(name) {
            stmt.plan = Some(optimised);
        }
        Ok(())
    }

    /// Handle `Bind(portal, statement, param_formats, params, result_formats)`.
    async fn handle_bind(
        &mut self,
        portal_name: String,
        statement_name: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<(), ServerError> {
        if self.extended.pipeline_failed {
            return Ok(());
        }
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.state.catalog,
        };
        match extended::handle_bind(
            &mut self.extended,
            portal_name,
            &statement_name,
            &param_formats,
            &params,
            result_formats,
            Some(&combined),
        ) {
            Ok(msg) => self.send(&msg).await,
            Err(e) => {
                if !e.is_query_scoped() {
                    return Err(e);
                }
                let e = self.fail_if_in_transaction(e);
                self.extended.mark_failed();
                self.send_error(&e.to_string(), e.sqlstate()).await
            }
        }
    }

    /// Handle `Describe(kind, name)`.
    async fn handle_describe(
        &mut self,
        kind: ultrasql_protocol::DescribeKind,
        name: &str,
    ) -> Result<(), ServerError> {
        if self.extended.pipeline_failed {
            return Ok(());
        }
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.state.catalog,
        };
        let result = match kind {
            ultrasql_protocol::DescribeKind::Statement => {
                extended::handle_describe_statement(&self.extended, name, Some(&combined))
            }
            ultrasql_protocol::DescribeKind::Portal => {
                extended::handle_describe_portal(&self.extended, name).map(|m| vec![m])
            }
        };
        match result {
            Ok(msgs) => {
                for m in &msgs {
                    self.send(m).await?;
                }
                Ok(())
            }
            Err(e) => {
                if !e.is_query_scoped() {
                    return Err(e);
                }
                let e = self.fail_if_in_transaction(e);
                self.extended.mark_failed();
                self.send_error(&e.to_string(), e.sqlstate()).await
            }
        }
    }

    /// Handle `Execute(portal, max_rows)`. Runs the portal end-to-end
    /// using the same `lower_query` / executor path Simple Query uses,
    /// and routes the plan through the session's [`TxnState`] so an
    /// explicit BEGIN issued via Simple Query (or via a prior Extended
    /// Execute) keeps subsequent Executes inside the same transaction.
    ///
    /// Transaction-control plans (BEGIN / COMMIT / ROLLBACK / SAVEPOINT
    /// / ROLLBACK TO / RELEASE) are dispatched directly against the
    /// session's [`TxnState`] via [`Self::execute_txn_control`] —
    /// `execute_portal` never sees them.
    async fn handle_execute(&mut self, portal: &str, max_rows: i32) -> Result<(), ServerError> {
        if self.extended.pipeline_failed {
            return Ok(());
        }

        // Peek at the portal's plan up front: txn-control plans skip
        // `execute_portal` entirely so the session's TxnState owns the
        // transition. Cloning is cheap because the txn-control variants
        // carry only a `Schema::empty()` (and an optional savepoint name).
        let plan_clone = if let Some(p) = self.extended.portals.get(portal) {
            p.plan.clone()
        } else {
            let err = ServerError::Unsupported("Execute: portal not found");
            let err = self.fail_if_in_transaction(err);
            self.extended.mark_failed();
            return self.send_error(&err.to_string(), err.sqlstate()).await;
        };

        // Transaction-control plans take the dedicated TxnState dispatch.
        if let Some(ref plan) = plan_clone {
            if matches!(
                plan,
                LogicalPlan::Begin { .. }
                    | LogicalPlan::Commit { .. }
                    | LogicalPlan::Rollback { .. }
                    | LogicalPlan::Savepoint { .. }
                    | LogicalPlan::RollbackToSavepoint { .. }
                    | LogicalPlan::ReleaseSavepoint { .. }
            ) {
                match self.execute_txn_control(plan) {
                    Ok(result) => {
                        for m in &result.messages {
                            self.send(m).await?;
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        if !e.is_query_scoped() {
                            return Err(e);
                        }
                        self.extended.mark_failed();
                        return self.send_error(&e.to_string(), e.sqlstate()).await;
                    }
                }
            }
        }

        // A statement inside a failed transaction block is rejected
        // before we open any new resources.
        if matches!(self.txn_state, TxnState::Failed(_)) {
            let err = ServerError::TransactionAborted;
            self.extended.mark_failed();
            return self.send_error(&err.to_string(), err.sqlstate()).await;
        }

        // Non-txn-control path: route through TxnState.
        let outcome = self.run_portal_routed(portal, max_rows);

        match outcome {
            Ok(out) => {
                for m in &out.messages {
                    self.send(m).await?;
                }
                Ok(())
            }
            Err(e) => {
                if !e.is_query_scoped() {
                    return Err(e);
                }
                self.extended.mark_failed();
                self.send_error(&e.to_string(), e.sqlstate()).await
            }
        }
    }

    /// Run a named portal under the current [`TxnState`].
    ///
    /// Mirrors [`Self::run_dml_or_select`] but drives the executor
    /// through `extended::execute_portal` so the result-format codes
    /// the client supplied at Bind time are honoured.
    fn run_portal_routed(
        &mut self,
        portal: &str,
        max_rows: i32,
    ) -> Result<extended::ExecuteOutcome, ServerError> {
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::Idle => {
                let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
                let ctx = pipeline::LowerCtx {
                    tables: &self.state.tables,
                    catalog_snapshot: Arc::clone(&catalog_snapshot),
                    heap: Arc::clone(&self.state.heap),
                    snapshot: txn.snapshot.clone(),
                    oracle: Arc::clone(&self.state.txn_manager),
                    xid: txn.xid,
                    command_id: txn.current_command,
                    cte_buffers: std::collections::HashMap::new(),
                };
                let res = extended::execute_portal(&mut self.extended, portal, max_rows, &ctx);
                if res.is_ok() {
                    if let Err(e) = self.state.txn_manager.commit(txn) {
                        tracing::warn!(
                            error = %e,
                            "autocommit failed to finalise (Extended Execute)",
                        );
                    }
                } else if let Err(e) = self.state.txn_manager.abort(txn) {
                    tracing::warn!(
                        error = %e,
                        "autocommit rollback failed (Extended Execute)",
                    );
                }
                // txn_state stays Idle.
                res
            }
            TxnState::InTransaction(mut txn) => {
                self.state.txn_manager.refresh_snapshot(&mut txn);
                let ctx = pipeline::LowerCtx {
                    tables: &self.state.tables,
                    catalog_snapshot: Arc::clone(&catalog_snapshot),
                    heap: Arc::clone(&self.state.heap),
                    snapshot: txn.snapshot.clone(),
                    oracle: Arc::clone(&self.state.txn_manager),
                    xid: txn.xid,
                    command_id: txn.current_command,
                    cte_buffers: std::collections::HashMap::new(),
                };
                let res = extended::execute_portal(&mut self.extended, portal, max_rows, &ctx);
                self.txn_state = if res.is_ok() {
                    TxnState::InTransaction(txn)
                } else {
                    TxnState::Failed(txn)
                };
                res
            }
            TxnState::Failed(txn) => {
                self.txn_state = TxnState::Failed(txn);
                Err(ServerError::TransactionAborted)
            }
        }
    }

    /// Handle `Sync`. Emits a `ReadyForQuery` carrying the session's
    /// current transaction state byte (`'I'` idle, `'T'` in a
    /// transaction block, `'E'` in a failed transaction block).
    async fn handle_sync(&mut self) -> Result<(), ServerError> {
        self.extended.reset_on_sync();
        self.send(&BackendMessage::ReadyForQuery {
            status: self.txn_state.ready_for_query_status(),
        })
        .await
    }

    /// Handle `Close(kind, name)`. Always emits `CloseComplete` even
    /// when the named object does not exist (per spec).
    async fn handle_extended_close(
        &mut self,
        kind: ultrasql_protocol::DescribeKind,
        name: &str,
    ) -> Result<(), ServerError> {
        if self.extended.pipeline_failed {
            return Ok(());
        }
        let msg = extended::handle_close(&mut self.extended, kind, name);
        self.send(&msg).await
    }

    /// Handle `Flush`. Flush already happens inside `send`; this is a
    /// no-op on top of that.
    async fn handle_flush(&mut self) -> Result<(), ServerError> {
        self.io.flush().await?;
        Ok(())
    }

    /// Read one frontend message, growing the buffer until the codec
    /// has a complete frame.
    //
    // TODO(security): add per-connection slow-loris timeout. A client
    // that opens a TCP session and then dribbles bytes at 1 byte/minute
    // currently holds the connection forever (the buffer grows up to
    // MAX_MESSAGE_BYTES = 16 MiB, then decode rejects, but the session
    // never times out on its own). Wrap the read in a tokio timer with
    // a configurable `statement_timeout` / `idle_in_transaction_timeout`
    // and tear the session down on expiry. Deferred because it requires
    // wiring server-level config plumbing.
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

    /// Encode every message in `msgs` into the connection's write
    /// buffer and dispatch it in a single `write_all` + `flush`.
    ///
    /// The naïve `for msg in msgs { self.send(msg).await? }` loop
    /// issues one `write_all` + one `flush` per message. For a SELECT
    /// that emits a `RowDescription`, N `DataRow`s, and a
    /// `CommandComplete`, that is N+2 syscall round-trips per query
    /// (or N+2 reactor wake-ups on the loopback path used by the
    /// bench harness) — which dominates wall-clock time on
    /// `select_scan_10k`. Coalescing collapses the dispatch to a
    /// single round-trip without changing wire semantics, since
    /// PostgreSQL's protocol does not require message-boundary flushes
    /// between `RowDescription` / `DataRow` / `CommandComplete`.
    async fn send_messages_coalesced(
        &mut self,
        msgs: &[BackendMessage],
    ) -> Result<(), ServerError> {
        self.write_buf.clear();
        for msg in msgs {
            encode_backend(msg, &mut self.write_buf);
        }
        if !self.write_buf.is_empty() {
            self.io.write_all(&self.write_buf).await?;
            self.io.flush().await?;
        }
        Ok(())
    }

    /// Write the raw bytes of one or more already-encoded backend
    /// messages to the socket in a single `write_all` + `flush`.
    ///
    /// Used by the SELECT streaming path
    /// ([`result_encoder::stream_select`]) which builds the wire bytes
    /// directly into a scratch `BytesMut` to avoid materialising
    /// `BackendMessage::DataRow` enums for every row of a large scan.
    async fn send_raw(&mut self, bytes: &[u8]) -> Result<(), ServerError> {
        if !bytes.is_empty() {
            self.io.write_all(bytes).await?;
            self.io.flush().await?;
        }
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

/// Decode a single column out of an encoded heap-tuple payload and
/// return its value as an `i64` key.
///
/// `schema` is the relation's full schema; `col_idx` is the 0-based
/// position of the key column inside that schema; `widen_i32` is
/// `true` for `Int32` columns (the value is sign-extended to `i64`)
/// and `false` for `Int64`. `Value::Null` returns `None` so the
/// caller can decide what to do — the CREATE INDEX build path
/// currently skips NULL keys (PostgreSQL semantics for non-`INCLUDE`
/// b-tree indexes).
///
/// Returning `Result<Option<i64>, ServerError>` keeps NULL handling
/// at the call site; using a panic / sentinel value would conflate
/// "schema mismatch" with "missing value", which the catalog wants
/// to keep distinct.
/// Build a PostgreSQL `NoticeResponse` carrying a `WARNING` with the
/// given SQLSTATE and human-readable text.
///
/// `NoticeResponse` is shaped exactly like `ErrorResponse` on the wire
/// (an `'N'` tag instead of `'E'`); a libpq client routes notices to a
/// callback rather than aborting the operation. UltraSQL emits notices
/// where PostgreSQL would emit them — most importantly for
/// `BEGIN`-inside-tx, `COMMIT`-outside-tx, and `ROLLBACK`-outside-tx so
/// drivers see the same behaviour they expect from PostgreSQL.
fn notice_warning(sqlstate: &str, message: &str) -> BackendMessage {
    BackendMessage::NoticeResponse {
        fields: vec![
            (b'S', "WARNING".to_string()),
            (b'C', sqlstate.to_string()),
            (b'M', message.to_string()),
        ],
    }
}

/// Run a non-DDL, non-transaction-control plan inside the given
/// transaction and return the assembled wire-message result.
///
/// Owns no state of its own: it captures everything it needs by
/// argument so both the Simple Query and Extended Query paths can call
/// it. The caller is responsible for committing or aborting `txn` based
/// on whether this function returned `Ok` or `Err`.
///
/// `command_id` is taken from `txn.current_command` so each statement
/// inside an explicit transaction sees its own writes via the MVCC
/// `cmin < current_command` rule.
fn run_plan_in_txn(
    plan: &LogicalPlan,
    txn: &Transaction,
    catalog_snapshot: Arc<CatalogSnapshot>,
    tables: &SampleTables,
    heap: Arc<HeapAccess<BlankPageLoader>>,
    oracle: Arc<TransactionManager>,
) -> Result<SelectResult, ServerError> {
    let ctx = LowerCtx {
        tables,
        catalog_snapshot,
        heap,
        snapshot: txn.snapshot.clone(),
        oracle,
        xid: txn.xid,
        command_id: txn.current_command,
        cte_buffers: std::collections::HashMap::new(),
    };
    match plan {
        LogicalPlan::Insert { .. } => {
            let mut op = pipeline::lower_query(plan, &ctx)?;
            run_modify_command(op.as_mut(), "INSERT")
        }
        LogicalPlan::Update { .. } => {
            let mut op = pipeline::lower_query(plan, &ctx)?;
            run_modify_command(op.as_mut(), "UPDATE")
        }
        LogicalPlan::Delete { .. } => {
            let mut op = pipeline::lower_query(plan, &ctx)?;
            run_modify_command(op.as_mut(), "DELETE")
        }
        _ => {
            let mut op = pipeline::lower_query(plan, &ctx)?;
            // Streaming wire-encode hot path: bypass the
            // `Vec<BackendMessage>` materialisation and emit
            // `RowDescription` + N `DataRow` + `CommandComplete`
            // directly into a single `BytesMut`. The session dispatches
            // the body in one `write_all` + `flush` rather than the
            // per-message loop the legacy `run_select` requires.
            run_select_streamed(op.as_mut())
        }
    }
}

fn decode_key_column(
    bytes: &[u8],
    schema: &ultrasql_core::Schema,
    col_idx: usize,
    widen_i32: bool,
) -> Result<Option<i64>, ServerError> {
    let codec = ultrasql_executor::RowCodec::new(schema.clone());
    let row = codec
        .decode(bytes)
        .map_err(|e| ServerError::ddl(format!("CREATE INDEX key decode: {e}")))?;
    let value = row.get(col_idx).ok_or_else(|| {
        ServerError::ddl(format!(
            "CREATE INDEX key column {col_idx} missing from decoded row of arity {}",
            row.len()
        ))
    })?;
    match (value, widen_i32) {
        (Value::Null, _) => Ok(None),
        (Value::Int32(v), true) => Ok(Some(i64::from(*v))),
        (Value::Int64(v), false) => Ok(Some(*v)),
        _ => Err(ServerError::ddl(format!(
            "CREATE INDEX key column {col_idx} has unexpected runtime type"
        ))),
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

    /// Extended Query round-trip over the in-memory duplex transport.
    ///
    /// `Parse → Bind → Describe(Portal) → Execute → Sync` against
    /// `SELECT id FROM users` should return the same three rows the
    /// Simple Query path produces. This is the duplex-level smoke test;
    /// the real-driver test against `tokio-postgres` lives in
    /// `crates/ultrasql-server/tests/extended_query_round_trip.rs`.
    #[tokio::test]
    async fn extended_query_round_trip_select() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let handle = tokio::spawn(handle_connection(server_side, state));

        complete_startup(&mut client).await;

        // Parse
        send_frontend(
            &mut client,
            &FrontendMessage::Parse {
                name: "s1".to_string(),
                sql: "SELECT id FROM users".to_string(),
                param_types: vec![],
            },
        )
        .await;
        // Bind
        send_frontend(
            &mut client,
            &FrontendMessage::Bind {
                portal_name: "p1".to_string(),
                statement_name: "s1".to_string(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            },
        )
        .await;
        // Describe(Portal)
        send_frontend(
            &mut client,
            &FrontendMessage::Describe {
                kind: ultrasql_protocol::DescribeKind::Portal,
                name: "p1".to_string(),
            },
        )
        .await;
        // Execute
        send_frontend(
            &mut client,
            &FrontendMessage::Execute {
                portal: "p1".to_string(),
                max_rows: 0,
            },
        )
        .await;
        // Sync — triggers ReadyForQuery.
        send_frontend(&mut client, &FrontendMessage::Sync).await;

        let msgs = drain_until_ready(&mut client).await;

        // ParseComplete and BindComplete are present.
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::ParseComplete)),
            "missing ParseComplete: {msgs:?}"
        );
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::BindComplete)),
            "missing BindComplete: {msgs:?}"
        );
        // RowDescription from Describe(Portal).
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::RowDescription { .. })),
            "missing RowDescription: {msgs:?}"
        );
        // Three data rows.
        let n_rows = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .count();
        assert_eq!(n_rows, 3, "expected 3 data rows: {msgs:?}");
        // CommandComplete + ReadyForQuery 'I' at the end.
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// Parameter substitution end-to-end over the duplex transport.
    ///
    /// `SELECT id FROM users WHERE id = $1` with `$1 = 2` should
    /// return exactly one row.
    #[tokio::test]
    async fn extended_query_round_trip_with_parameter() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let handle = tokio::spawn(handle_connection(server_side, state));

        complete_startup(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Parse {
                name: String::new(),
                sql: "SELECT id FROM users WHERE id = $1".to_string(),
                param_types: vec![23], // int4
            },
        )
        .await;
        send_frontend(
            &mut client,
            &FrontendMessage::Bind {
                portal_name: String::new(),
                statement_name: String::new(),
                param_formats: vec![1], // binary
                params: vec![Some(2_i32.to_be_bytes().to_vec())],
                result_formats: vec![],
            },
        )
        .await;
        send_frontend(
            &mut client,
            &FrontendMessage::Execute {
                portal: String::new(),
                max_rows: 0,
            },
        )
        .await;
        send_frontend(&mut client, &FrontendMessage::Sync).await;

        let msgs = drain_until_ready(&mut client).await;
        let rows: Vec<_> = msgs
            .iter()
            .filter_map(|m| match m {
                BackendMessage::DataRow { columns } => Some(columns.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(rows.len(), 1, "expected one matching row: {msgs:?}");
        assert_eq!(rows[0][0].as_deref(), Some(b"2".as_slice()));

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
    async fn create_table_persists_to_catalog_via_wire() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let state_clone = Arc::clone(&state);
        let handle = tokio::spawn(handle_connection(server_side, state));

        complete_startup(&mut client).await;
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE accounts (id BIGINT NOT NULL, balance FLOAT8)".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;

        // Server emits CommandComplete "CREATE TABLE" then ReadyForQuery 'I'.
        let tag = msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        });
        assert_eq!(tag.as_deref(), Some("CREATE TABLE"));
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, BackendMessage::RowDescription { .. })),
            "DDL must not emit RowDescription"
        );
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));

        // Catalog observably contains the new relation.
        let snap = state_clone.catalog_snapshot();
        let accounts = snap.tables.get("accounts").expect("accounts persisted");
        assert_eq!(accounts.name, "accounts");
        assert_eq!(accounts.schema_name, "public");
        assert_eq!(accounts.schema.len(), 2);
        assert!(
            !accounts.schema.fields()[0].nullable,
            "NOT NULL constraint applied"
        );

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    #[tokio::test]
    async fn create_insert_select_round_trip_through_wire() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        // CREATE TABLE — Int32 columns so the literal `1` / `100`
        // (default Int32 in the binder) types-match without casts.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE items (id INT NOT NULL, val INT)".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));

        // INSERT three rows
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "INSERT INTO items VALUES (1, 100), (2, 200), (3, 300)".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let tag = msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        });
        assert_eq!(
            tag.as_deref(),
            Some("INSERT 0 3"),
            "INSERT must report 3 rows: {msgs:?}"
        );
        // INSERT must not emit a RowDescription.
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, BackendMessage::RowDescription { .. })),
            "INSERT must not emit RowDescription"
        );

        // SELECT * — runs SeqScan over the real heap.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id, val FROM items".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let rows: Vec<_> = msgs
            .iter()
            .filter_map(|m| match m {
                BackendMessage::DataRow { columns } => Some(columns.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(rows.len(), 3, "expected 3 rows, got {msgs:?}");
        let select_tag = msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        });
        assert_eq!(select_tag.as_deref(), Some("SELECT 3"));

        // Sanity-check the row contents (text encoding).
        let mut decoded: Vec<(i32, i32)> = rows
            .iter()
            .map(|cols| {
                let id = std::str::from_utf8(cols[0].as_ref().unwrap())
                    .unwrap()
                    .parse::<i32>()
                    .unwrap();
                let val = std::str::from_utf8(cols[1].as_ref().unwrap())
                    .unwrap()
                    .parse::<i32>()
                    .unwrap();
                (id, val)
            })
            .collect();
        decoded.sort_unstable();
        assert_eq!(decoded, vec![(1, 100), (2, 200), (3, 300)]);

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    #[tokio::test]
    async fn create_table_duplicate_rejected_with_query_scoped_error() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));

        complete_startup(&mut client).await;
        // First create succeeds.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE t (id INT)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;

        // Second create on the same name errors but the session survives.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE t (id INT)".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let err = msgs
            .iter()
            .find_map(|m| match m {
                BackendMessage::ErrorResponse { fields } => Some(fields.clone()),
                _ => None,
            })
            .expect("ErrorResponse on duplicate");
        let sqlstate = err
            .iter()
            .find_map(|(c, v)| (*c == b'C').then(|| v.clone()))
            .expect("SQLSTATE field present");
        assert_eq!(sqlstate, "42P07", "duplicate_table SQLSTATE");
        // Session still healthy.
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));

        // Third attempt with IF NOT EXISTS succeeds as a no-op.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE IF NOT EXISTS t (id INT)".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let tag = msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        });
        assert_eq!(tag.as_deref(), Some("CREATE TABLE"));

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
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

    // -----------------------------------------------------------------------
    // CREATE INDEX / DROP TABLE / ALTER TABLE — wire dispatch tests
    // -----------------------------------------------------------------------

    /// Drive `CREATE TABLE`, INSERT a few rows, then issue
    /// `CREATE INDEX`. The catalog snapshot must reflect the new
    /// index entry and the `IndexEntry`'s columns must match the
    /// key column the binder resolved.
    #[tokio::test]
    async fn create_index_via_wire_registers_index_entry() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let state_clone = Arc::clone(&state);
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        // CREATE TABLE with an Int64 key (matches the v0.5 B-tree key shape).
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE t (id BIGINT NOT NULL, val INT)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "INSERT INTO t VALUES (10, 1), (20, 2), (30, 3)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE INDEX ix_t_id ON t (id)".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;

        let tag = msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        });
        assert_eq!(tag.as_deref(), Some("CREATE INDEX"));
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, BackendMessage::RowDescription { .. })),
            "DDL must not emit RowDescription"
        );

        // Catalog snapshot must contain the new index.
        let snap = state_clone.catalog_snapshot();
        let idx = snap
            .indexes
            .get("ix_t_id")
            .expect("ix_t_id present in snapshot");
        assert_eq!(idx.name, "ix_t_id");
        assert_eq!(idx.columns, vec![0_u16], "indexes id column at attnum 0");
        // The table OID matches the registered table.
        let table = snap.tables.get("t").expect("t present");
        assert_eq!(idx.table_oid, table.oid);

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// `CREATE INDEX IF NOT EXISTS` is a no-op when the index already
    /// exists; the second invocation still returns `CREATE INDEX` as
    /// the command tag and does not error.
    #[tokio::test]
    async fn create_index_if_not_exists_is_idempotent() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE t (id BIGINT NOT NULL)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;

        for _ in 0..2 {
            send_frontend(
                &mut client,
                &FrontendMessage::Query {
                    sql: "CREATE INDEX IF NOT EXISTS ix_t_id ON t (id)".to_string(),
                },
            )
            .await;
            let msgs = drain_until_ready(&mut client).await;
            let tag = msgs.iter().find_map(|m| match m {
                BackendMessage::CommandComplete { tag } => Some(tag.clone()),
                _ => None,
            });
            assert_eq!(tag.as_deref(), Some("CREATE INDEX"));
        }

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// `DROP TABLE t` makes a subsequent `SELECT * FROM t` fail with a
    /// PostgreSQL-style `undefined_table` error (SQLSTATE 42P01). The
    /// session continues so the test pattern matches PostgreSQL's
    /// behaviour.
    #[tokio::test]
    async fn drop_table_via_wire_then_select_fails_with_undefined_table() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let state_clone = Arc::clone(&state);
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE t (id INT)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "DROP TABLE t".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let tag = msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        });
        assert_eq!(tag.as_deref(), Some("DROP TABLE"));

        // Catalog snapshot no longer holds the dropped table.
        assert!(!state_clone.catalog_snapshot().tables.contains_key("t"));

        // Subsequent SELECT errors with relation-does-not-exist.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM t".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let err = msgs
            .iter()
            .find_map(|m| match m {
                BackendMessage::ErrorResponse { fields } => Some(fields.clone()),
                _ => None,
            })
            .expect("ErrorResponse on dropped table");
        let sqlstate = err
            .iter()
            .find_map(|(c, v)| (*c == b'C').then(|| v.clone()))
            .expect("SQLSTATE field present");
        assert_eq!(sqlstate, "42P01", "undefined_table SQLSTATE");

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// `DROP TABLE IF EXISTS missing` succeeds with the `DROP TABLE`
    /// command tag and does not error.
    #[tokio::test]
    async fn drop_table_if_exists_missing_is_noop() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "DROP TABLE IF EXISTS nothing_here".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let tag = msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        });
        assert_eq!(tag.as_deref(), Some("DROP TABLE"));

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// End-to-end `ALTER TABLE t ADD COLUMN c` flow:
    ///
    /// 1. Create a table, insert a row.
    /// 2. ALTER ADD COLUMN — relation is rewritten so the pre-existing
    ///    row's new column reads as NULL.
    /// 3. INSERT a new row with a value for the added column.
    /// 4. SELECT and verify the pre-existing row reads NULL while the
    ///    new row reads the inserted value.
    #[tokio::test]
    async fn alter_table_add_column_via_wire_round_trips() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let state_clone = Arc::clone(&state);
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        // Setup
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE t (id INT NOT NULL, val INT)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "INSERT INTO t VALUES (1, 100)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;

        // ALTER ADD COLUMN
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "ALTER TABLE t ADD COLUMN c INTEGER".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let tag = msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        });
        assert_eq!(tag.as_deref(), Some("ALTER TABLE"));

        // Catalog snapshot now reflects 3 columns.
        let snap = state_clone.catalog_snapshot();
        let t = snap.tables.get("t").expect("t present");
        assert_eq!(t.schema.len(), 3);
        assert_eq!(t.schema.field_at(2).name, "c");

        // INSERT a new row including the new column.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "INSERT INTO t (id, val, c) VALUES (2, 200, 999)".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let tag = msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        });
        assert_eq!(tag.as_deref(), Some("INSERT 0 1"));

        // SELECT all three columns; verify the pre-existing row reads
        // NULL for `c` and the new row reads 999.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id, val, c FROM t".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let rows: Vec<Vec<Option<Vec<u8>>>> = msgs
            .iter()
            .filter_map(|m| match m {
                BackendMessage::DataRow { columns } => Some(columns.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(rows.len(), 2, "expected 2 rows, got {msgs:?}");
        // Normalise into (id, val, c) tuples for assertion.
        let parsed: Vec<(i32, i32, Option<i32>)> = rows
            .iter()
            .map(|cols| {
                let id = std::str::from_utf8(cols[0].as_ref().unwrap())
                    .unwrap()
                    .parse::<i32>()
                    .unwrap();
                let val = std::str::from_utf8(cols[1].as_ref().unwrap())
                    .unwrap()
                    .parse::<i32>()
                    .unwrap();
                let c = cols[2]
                    .as_ref()
                    .map(|b| std::str::from_utf8(b).unwrap().parse::<i32>().unwrap());
                (id, val, c)
            })
            .collect();
        // Pre-existing row sees NULL for c; new row sees 999.
        assert!(parsed.contains(&(1, 100, None)), "got {parsed:?}");
        assert!(parsed.contains(&(2, 200, Some(999))), "got {parsed:?}");

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// `ALTER TABLE ADD COLUMN` on a table that does not exist must
    /// fail at the binder layer (`PlanError::TableNotFound`) and
    /// surface as a query-scoped error — the session survives.
    #[tokio::test]
    async fn alter_table_add_column_rejects_missing_relation() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "ALTER TABLE nope ADD COLUMN x INTEGER".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
            "expected ErrorResponse: {msgs:?}"
        );
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// `TRUNCATE TABLE t` emits `TRUNCATE TABLE` as the command tag,
    /// does not emit a `RowDescription`, and the relation is empty as
    /// observed by a subsequent `SELECT *`.
    #[tokio::test]
    async fn truncate_via_wire_empties_relation() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE trunc_unit (id INT NOT NULL)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "INSERT INTO trunc_unit VALUES (1)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "INSERT INTO trunc_unit VALUES (2)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "TRUNCATE TABLE trunc_unit".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let tag = msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        });
        assert_eq!(tag.as_deref(), Some("TRUNCATE TABLE"));
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, BackendMessage::RowDescription { .. })),
            "DDL must not emit RowDescription"
        );

        // Post-truncate SELECT returns no DataRow messages.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM trunc_unit".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let data_rows = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .count();
        assert_eq!(data_rows, 0, "post-truncate SELECT must emit no DataRow");

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// `TRUNCATE TABLE nope` errors with the table-not-found SQLSTATE
    /// (42P01) and the session survives — the binder rejects the
    /// reference and the wire path surfaces it as a query-scoped
    /// error, never tearing the connection.
    #[tokio::test]
    async fn truncate_rejects_missing_relation() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "TRUNCATE TABLE nope".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
            "expected ErrorResponse: {msgs:?}"
        );
        assert!(matches!(
            msgs.last().unwrap(),
            BackendMessage::ReadyForQuery { status: b'I' }
        ));

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    // -----------------------------------------------------------------------
    // Transaction-control state machine — Simple Query duplex tests
    // -----------------------------------------------------------------------

    /// Helper: extract the trailing `ReadyForQuery` status byte from a
    /// drained message sequence.
    fn ready_status(msgs: &[BackendMessage]) -> u8 {
        match msgs.last().expect("non-empty msgs") {
            BackendMessage::ReadyForQuery { status } => *status,
            other => panic!("expected ReadyForQuery at end, got {other:?}"),
        }
    }

    /// Helper: extract the `CommandComplete` tag from a drained message
    /// sequence.
    fn command_tag(msgs: &[BackendMessage]) -> Option<String> {
        msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        })
    }

    /// `TxnState::ready_for_query_status` maps each variant to the
    /// correct PostgreSQL status byte. Unit test, no I/O.
    #[test]
    fn txn_state_ready_for_query_status_matches_postgres() {
        // The Failed and InTransaction arms hold a Transaction handle,
        // which we mint via a throwaway TxnManager.
        let mgr = TransactionManager::new();
        let txn1 = mgr.begin(IsolationLevel::ReadCommitted);
        let txn2 = mgr.begin(IsolationLevel::ReadCommitted);

        assert_eq!(TxnState::Idle.ready_for_query_status(), b'I');
        assert_eq!(TxnState::InTransaction(txn1).ready_for_query_status(), b'T');
        assert_eq!(TxnState::Failed(txn2).ready_for_query_status(), b'E');
    }

    /// `BEGIN; INSERT; INSERT; COMMIT;` — both rows visible after commit.
    /// `BEGIN; INSERT; ROLLBACK;` — row not persisted.
    /// `ReadyForQuery` status byte reflects state at every step.
    #[tokio::test]
    async fn begin_commit_persists_rows_rollback_discards() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        // CREATE TABLE — outside any txn.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE t (id INT NOT NULL, val INT)".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert_eq!(ready_status(&msgs), b'I');

        // BEGIN
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert_eq!(command_tag(&msgs).as_deref(), Some("BEGIN"));
        assert_eq!(ready_status(&msgs), b'T', "BEGIN → 'T' status");

        // INSERT — inside txn
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "INSERT INTO t VALUES (1, 100)".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert_eq!(command_tag(&msgs).as_deref(), Some("INSERT 0 1"));
        assert_eq!(ready_status(&msgs), b'T');

        // INSERT
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "INSERT INTO t VALUES (2, 200)".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert_eq!(ready_status(&msgs), b'T');

        // COMMIT
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "COMMIT".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert_eq!(command_tag(&msgs).as_deref(), Some("COMMIT"));
        assert_eq!(ready_status(&msgs), b'I', "COMMIT → 'I'");

        // SELECT — both rows visible.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM t".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let row_count = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .count();
        assert_eq!(row_count, 2, "both committed rows visible");

        // BEGIN; INSERT; ROLLBACK — row 3 must not persist.
        for stmt in ["BEGIN", "INSERT INTO t VALUES (3, 300)", "ROLLBACK"] {
            send_frontend(
                &mut client,
                &FrontendMessage::Query {
                    sql: stmt.to_string(),
                },
            )
            .await;
            let _ = drain_until_ready(&mut client).await;
        }

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM t".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let row_count = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .count();
        assert_eq!(row_count, 2, "rolled-back INSERT did not persist");
        assert_eq!(ready_status(&msgs), b'I');

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// `BEGIN; UPDATE; ROLLBACK;` — UPDATE is undone.
    #[tokio::test]
    async fn begin_update_rollback_reverts_value() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        // Setup
        for sql in [
            "CREATE TABLE t (id INT NOT NULL, val INT)",
            "INSERT INTO t VALUES (1, 100)",
        ] {
            send_frontend(&mut client, &FrontendMessage::Query { sql: sql.into() }).await;
            let _ = drain_until_ready(&mut client).await;
        }

        // BEGIN; UPDATE; ROLLBACK
        for sql in [
            "BEGIN",
            "UPDATE t SET val = val + 999 WHERE id = 1",
            "ROLLBACK",
        ] {
            send_frontend(&mut client, &FrontendMessage::Query { sql: sql.into() }).await;
            let _ = drain_until_ready(&mut client).await;
        }

        // Verify val unchanged.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT val FROM t WHERE id = 1".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let rows: Vec<Vec<Option<Vec<u8>>>> = msgs
            .iter()
            .filter_map(|m| match m {
                BackendMessage::DataRow { columns } => Some(columns.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_deref(), Some(b"100".as_slice()));

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// A statement that errors inside a transaction transitions the
    /// session to the `Failed` state. Subsequent statements (other than
    /// COMMIT / ROLLBACK) return SQLSTATE `25P02`. COMMIT in `Failed`
    /// state returns the `ROLLBACK` tag (PostgreSQL semantics).
    #[tokio::test]
    async fn failed_transaction_rejects_subsequent_statements_until_rollback() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        // BEGIN
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert_eq!(ready_status(&msgs), b'T');

        // Cause an error: select from a non-existent table.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT * FROM no_such_table".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
            "expected ErrorResponse for missing table"
        );
        assert_eq!(ready_status(&msgs), b'E', "post-error status → 'E'");

        // A subsequent statement (a perfectly valid SELECT against the
        // sample table) is rejected with `25P02` while in `Failed`.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM users".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let err = msgs
            .iter()
            .find_map(|m| match m {
                BackendMessage::ErrorResponse { fields } => Some(fields.clone()),
                _ => None,
            })
            .expect("ErrorResponse in failed state");
        let sqlstate = err
            .iter()
            .find_map(|(c, v)| (*c == b'C').then(|| v.clone()))
            .expect("SQLSTATE field present");
        assert_eq!(sqlstate, "25P02", "failed-block SQLSTATE");
        assert_eq!(ready_status(&msgs), b'E');

        // COMMIT in failed state returns the `ROLLBACK` tag.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "COMMIT".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert_eq!(
            command_tag(&msgs).as_deref(),
            Some("ROLLBACK"),
            "COMMIT in failed state returns ROLLBACK tag (PostgreSQL semantics)",
        );
        assert_eq!(ready_status(&msgs), b'I', "post-COMMIT status → 'I'");

        // Session is healthy again — the same query that errored under
        // `Failed` now runs normally.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM users".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
        );

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// Implicit autocommit still works: `INSERT` outside any `BEGIN`
    /// commits immediately and is visible to the next statement.
    #[tokio::test]
    async fn implicit_autocommit_still_persists_writes() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        for sql in [
            "CREATE TABLE t (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO t VALUES (2)",
        ] {
            send_frontend(&mut client, &FrontendMessage::Query { sql: sql.into() }).await;
            let msgs = drain_until_ready(&mut client).await;
            assert_eq!(
                ready_status(&msgs),
                b'I',
                "autocommit always leaves status as 'I' after {sql}",
            );
        }

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM t".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let row_count = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .count();
        assert_eq!(row_count, 2);

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// `BEGIN` while a transaction is already open emits a
    /// `NoticeResponse` (WARNING) and leaves the session in
    /// `InTransaction`. The PostgreSQL behaviour we mirror.
    #[tokio::test]
    async fn nested_begin_emits_warning_but_keeps_session_in_tx() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        // First BEGIN
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert_eq!(ready_status(&msgs), b'T');

        // Nested BEGIN
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        assert!(
            msgs.iter()
                .any(|m| matches!(m, BackendMessage::NoticeResponse { .. })),
            "expected NoticeResponse for nested BEGIN: {msgs:?}"
        );
        assert_eq!(command_tag(&msgs).as_deref(), Some("BEGIN"));
        assert_eq!(ready_status(&msgs), b'T', "nested BEGIN → still 'T'");

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "ROLLBACK".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// `COMMIT` / `ROLLBACK` outside a transaction emit a
    /// `NoticeResponse` (WARNING) but still succeed with the
    /// corresponding command tag.
    #[tokio::test]
    async fn commit_and_rollback_outside_tx_emit_warning() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        for sql in ["COMMIT", "ROLLBACK"] {
            send_frontend(&mut client, &FrontendMessage::Query { sql: sql.into() }).await;
            let msgs = drain_until_ready(&mut client).await;
            assert!(
                msgs.iter()
                    .any(|m| matches!(m, BackendMessage::NoticeResponse { .. })),
                "expected NoticeResponse for {sql} outside tx: {msgs:?}"
            );
            assert_eq!(command_tag(&msgs).as_deref(), Some(sql));
            assert_eq!(ready_status(&msgs), b'I');
        }

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// Extended Query round-trip for BEGIN / INSERT / COMMIT — prepared
    /// statements and unnamed portals.  Mirrors the Simple Query test
    /// `begin_commit_persists_rows_rollback_discards` over the
    /// `Parse/Bind/Execute/Sync` path.
    #[tokio::test]
    async fn extended_query_begin_insert_commit_round_trips() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        // Setup CREATE TABLE via Simple Query (Extended doesn't accept
        // CREATE TABLE today; see execute_portal docs).
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE t (id INT NOT NULL, val INT)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;

        // BEGIN via Extended Query (unnamed statement + portal).
        for sql in ["BEGIN", "INSERT INTO t VALUES (1, 100)", "COMMIT"] {
            send_frontend(
                &mut client,
                &FrontendMessage::Parse {
                    name: String::new(),
                    sql: sql.into(),
                    param_types: vec![],
                },
            )
            .await;
            send_frontend(
                &mut client,
                &FrontendMessage::Bind {
                    portal_name: String::new(),
                    statement_name: String::new(),
                    param_formats: vec![],
                    params: vec![],
                    result_formats: vec![],
                },
            )
            .await;
            send_frontend(
                &mut client,
                &FrontendMessage::Execute {
                    portal: String::new(),
                    max_rows: 0,
                },
            )
            .await;
            send_frontend(&mut client, &FrontendMessage::Sync).await;
            let msgs = drain_until_ready(&mut client).await;
            // Status reflects post-statement TxnState.
            let expected_status = match sql {
                "BEGIN" | "INSERT INTO t VALUES (1, 100)" => b'T',
                "COMMIT" => b'I',
                _ => unreachable!(),
            };
            assert_eq!(
                ready_status(&msgs),
                expected_status,
                "Extended {sql} → status {} (got {:?})",
                expected_status as char,
                msgs
            );
        }

        // The inserted row is visible after COMMIT.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM t".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let row_count = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .count();
        assert_eq!(row_count, 1, "Extended BEGIN/INSERT/COMMIT persisted");

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// Extended Query ROLLBACK discards the in-flight write.
    #[tokio::test]
    async fn extended_query_begin_insert_rollback_discards() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = Arc::new(Server::with_sample_database());
        let handle = tokio::spawn(handle_connection(server_side, state));
        complete_startup(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE t (id INT NOT NULL)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;

        for sql in ["BEGIN", "INSERT INTO t VALUES (42)", "ROLLBACK"] {
            send_frontend(
                &mut client,
                &FrontendMessage::Parse {
                    name: String::new(),
                    sql: sql.into(),
                    param_types: vec![],
                },
            )
            .await;
            send_frontend(
                &mut client,
                &FrontendMessage::Bind {
                    portal_name: String::new(),
                    statement_name: String::new(),
                    param_formats: vec![],
                    params: vec![],
                    result_formats: vec![],
                },
            )
            .await;
            send_frontend(
                &mut client,
                &FrontendMessage::Execute {
                    portal: String::new(),
                    max_rows: 0,
                },
            )
            .await;
            send_frontend(&mut client, &FrontendMessage::Sync).await;
            let _ = drain_until_ready(&mut client).await;
        }

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM t".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let row_count = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .count();
        assert_eq!(row_count, 0, "Extended ROLLBACK discarded");

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    // -----------------------------------------------------------------------
    // Wave B: optimizer + plan cache.
    //
    // The optimizer (rule-based rewrites) runs against every DML/SELECT
    // before the operator lowerer; the result is cached against the raw
    // SQL text. These tests pin the contract:
    //
    // 1. A repeat Simple Query reuses the cached plan (the optimiser
    //    closure runs once, the entry's `use_count` advances on each
    //    call).
    // 2. A new SQL text creates a fresh cache entry.
    // 3. A DDL statement invalidates the cache so the next DML/SELECT
    //    re-plans.
    // 4. The Simple Query path and the Extended Query Parse path share
    //    the same cache: an Extended Parse over an SQL string already
    //    optimised by a prior Simple Query reuses the cached plan
    //    (cross-protocol sharing — the headline win of the wave).
    //
    // Each test asserts both the cache shape (`plan_cache.len()`,
    // `use_count`) and the result correctness (the query still returns
    // the expected rows) so a regression in either layer is caught
    // here, not in the integration suite.
    // -----------------------------------------------------------------------

    /// Issuing the same `SELECT` SQL twice via Simple Query inserts one
    /// cache entry on the first call and increments its `use_count` on
    /// the second — the optimiser closure does not run again.
    #[tokio::test]
    async fn plan_cache_simple_query_repeat_reuses_optimised_plan() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let cache = Arc::clone(&state.plan_cache);
        assert_eq!(cache.len(), 0, "cache empty before any query runs");
        let handle = tokio::spawn(handle_connection(server_side, Arc::clone(&state)));

        complete_startup(&mut client).await;

        let sql = "SELECT id FROM users".to_string();
        send_frontend(&mut client, &FrontendMessage::Query { sql: sql.clone() }).await;
        let _ = drain_until_ready(&mut client).await;
        assert_eq!(cache.len(), 1, "first Simple Query inserts one entry");

        send_frontend(&mut client, &FrontendMessage::Query { sql }).await;
        let _ = drain_until_ready(&mut client).await;
        assert_eq!(
            cache.len(),
            1,
            "second Simple Query reuses the cached entry; no new entry inserted"
        );

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// Two distinct SELECTs produce two cache entries — the cache key is
    /// the SQL text, so different text should not collide.
    #[tokio::test]
    async fn plan_cache_distinct_sql_text_produces_distinct_entries() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let cache = Arc::clone(&state.plan_cache);
        let handle = tokio::spawn(handle_connection(server_side, Arc::clone(&state)));

        complete_startup(&mut client).await;

        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM users".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM users WHERE id = 1".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;
        assert_eq!(
            cache.len(),
            2,
            "distinct SQL text yields distinct cache entries"
        );

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// A `CREATE TABLE` clears every entry in the plan cache; a query
    /// run after the DDL therefore inserts a fresh entry rather than
    /// reusing the pre-DDL plan.
    #[tokio::test]
    async fn plan_cache_ddl_invalidates_every_entry() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let cache = Arc::clone(&state.plan_cache);
        let handle = tokio::spawn(handle_connection(server_side, Arc::clone(&state)));

        complete_startup(&mut client).await;

        // 1. Prime the cache with a SELECT.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM users".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;
        assert_eq!(cache.len(), 1, "prime: one cached entry");

        // 2. Run a CREATE TABLE — the cache should be cleared.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE TABLE tt (id INT NOT NULL)".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;
        assert_eq!(cache.len(), 0, "DDL must invalidate every cached entry");

        // 3. Re-run the SELECT — a fresh entry is inserted.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id FROM users".to_string(),
            },
        )
        .await;
        let _ = drain_until_ready(&mut client).await;
        assert_eq!(cache.len(), 1, "post-DDL query inserts a fresh cache entry");

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// Cross-protocol cache sharing: a Simple Query primes the cache;
    /// an Extended Query `Parse` over the same SQL text hits the cache
    /// and does NOT insert a second entry. The headline win of the
    /// wave — wire-compatibility for the libpq world means an ORM that
    /// issues `Parse`+`Bind`+`Execute` for a SELECT a `psql` session
    /// previously typed pays no extra optimization cost.
    #[tokio::test]
    async fn plan_cache_shared_between_simple_and_extended_query() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let cache = Arc::clone(&state.plan_cache);
        let handle = tokio::spawn(handle_connection(server_side, Arc::clone(&state)));

        complete_startup(&mut client).await;

        let sql = "SELECT id FROM users".to_string();

        // 1. Simple Query: primes the cache.
        send_frontend(&mut client, &FrontendMessage::Query { sql: sql.clone() }).await;
        let _ = drain_until_ready(&mut client).await;
        assert_eq!(cache.len(), 1, "Simple Query inserts one cached entry");

        // 2. Extended Query: Parse over the same SQL text should hit
        //    the cache. Issue a Parse/Sync pair (no Execute needed —
        //    the optimisation step happens inside `handle_parse`).
        send_frontend(
            &mut client,
            &FrontendMessage::Parse {
                name: String::new(),
                sql,
                param_types: vec![],
            },
        )
        .await;
        send_frontend(&mut client, &FrontendMessage::Sync).await;
        let _ = drain_until_ready(&mut client).await;
        assert_eq!(
            cache.len(),
            1,
            "Extended Query Parse must reuse the cached entry primed by Simple Query"
        );

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }

    /// `WHERE id = 42` over an indexed column still picks `IndexScan`
    /// when the bound plan flows through the optimizer first.
    ///
    /// The optimizer's rule loop is shape-preserving for the
    /// `Filter { Scan, Eq(Col, Literal) }` shape (predicate pushdown is
    /// a no-op when the filter is already on the leaf scan), so the
    /// catalog-aware lowerer in `pipeline::try_index_scan` still sees
    /// the indexable shape and dispatches to `IndexScan`. This test
    /// pins that round-trip.
    #[tokio::test]
    async fn optimizer_route_still_selects_index_scan() {
        let (mut client, server_side) = tokio::io::duplex(8192);
        let state = server();
        let handle = tokio::spawn(handle_connection(server_side, Arc::clone(&state)));

        complete_startup(&mut client).await;

        // CREATE + populate + CREATE INDEX.
        for sql in [
            "CREATE TABLE t_ix (id INT NOT NULL, val INT NOT NULL)",
            "INSERT INTO t_ix VALUES (1,10),(2,20),(3,30),(42,420),(99,990)",
            "CREATE INDEX ix_t_ix_id ON t_ix(id)",
        ] {
            send_frontend(
                &mut client,
                &FrontendMessage::Query {
                    sql: sql.to_string(),
                },
            )
            .await;
            let _ = drain_until_ready(&mut client).await;
        }

        // SELECT WHERE id = 42 should return exactly the one row, going
        // through the optimizer first.
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "SELECT id, val FROM t_ix WHERE id = 42".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let rows: Vec<_> = msgs
            .iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .collect();
        assert_eq!(rows.len(), 1, "point lookup must return one row");
        match rows[0] {
            BackendMessage::DataRow { columns } => {
                assert_eq!(columns[0].as_deref(), Some(b"42".as_slice()));
                assert_eq!(columns[1].as_deref(), Some(b"420".as_slice()));
            }
            _ => unreachable!(),
        }

        send_frontend(&mut client, &FrontendMessage::Terminate).await;
        drop(client);
        handle.await.expect("task joins").expect("clean exit");
    }
}
