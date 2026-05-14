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
pub(crate) const READ_BUFFER_INITIAL: usize = 1 << 12;

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
mod session;
use session::Session;


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
mod tests;
