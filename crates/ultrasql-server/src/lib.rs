//! `ultrasql-server` library: wire-protocol session loop.
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
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]
// Panic hardening: production (non-test) server code must not `.unwrap()`,
// `.expect()`, or `panic!`. Fallible sites propagate errors; proven invariants
// carry a per-site `#[allow]` with an `// INVARIANT:` justification.
// `#[cfg(test)]` modules are exempt.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

mod aggregating_index;
pub mod auth;
mod cached_select;
pub mod cancel;
pub mod catalog_version;
pub mod columnar_storage;
mod combined_catalog;
mod config;
pub mod copy;
pub mod embedded;
pub mod error;
pub mod extended;
pub mod index_key;
mod metadata_io;
mod metadata_scalar;
mod metadata_tokens;
mod metadata_view;
pub mod notify;
mod page_loader;
pub mod pipeline;
mod projection_summary;
mod recovery_target;
pub mod replication;
pub mod result_encoder;
mod runtime_index;
mod runtime_types;
mod search_path;
mod serializable;
mod server_index_rebuild;
mod server_lifecycle;
mod server_loop;
mod server_maintenance;
mod server_meta_domain_table;
mod server_meta_role_priv;
mod server_meta_rls_view;
mod server_meta_seq_schema_op;
mod server_recovery_rebuild;
mod server_wal_recovery;
mod session_state;
mod snapshots;
mod stats_hydration;
pub mod time_partition;
pub mod tls;
mod tpch_cache;
mod txn_exec;
pub mod wal_sink;
pub mod wire_writer;
pub mod workload;

// Re-export every item that moved out of the crate root during the lib.rs
// module split so all previously-public and crate-internal paths
// (`crate::…` / `ultrasql_server::…`) keep resolving identically. The globs
// preserve each item's original visibility (`pub` stays `pub`, `pub(crate)`
// stays `pub(crate)`).
pub(crate) use cached_select::*;
pub(crate) use combined_catalog::*;
pub use config::*;
pub(crate) use metadata_io::*;
pub(crate) use metadata_scalar::*;
pub(crate) use metadata_tokens::*;
pub(crate) use metadata_view::*;
pub use page_loader::*;
pub(crate) use recovery_target::*;
pub use runtime_index::*;
pub use runtime_types::*;
pub(crate) use search_path::*;
pub use server_loop::*;
pub use session_state::*;
pub(crate) use snapshots::*;
pub(crate) use stats_hydration::*;
pub use tpch_cache::*;
pub(crate) use txn_exec::*;

/// Numeric `server_version` exposed in startup
/// `ParameterStatus` and `pg_settings`. Drivers parse this as a PostgreSQL
/// feature baseline; UltraSQL's own product version remains `version()`.
pub(crate) const REPORTED_SERVER_VERSION: &str = "14.0";
const RECOVERY_TARGETS_FILE_LIMIT_BYTES: u64 = 64 * 1024;
const RUNTIME_METADATA_FILE_LIMIT_BYTES: u64 = 16 * 1024 * 1024;

#[cfg(test)]
pub(crate) static TPCH_TEST_CACHE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

use std::future::Future;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use num_traits::ToPrimitive;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};
use ultrasql_catalog::{
    Catalog, CatalogError, CatalogSnapshot, CatalogStats, DomainTypeEntry, IndexEntry,
    MutableCatalog, PersistentCatalog, StatisticRow, TableEntry,
};
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{BlockNumber, DataType, Lsn, Oid, PageId, RelationId, Schema, Value, Xid};
use ultrasql_executor::{Eval, ExecError, MemTableScan, Operator, RowCodec, SeqScan};
use ultrasql_optimizer::{
    AnalyzeOptions, AnalyzeRunner, ColumnStats, InMemoryStatsCatalog, PgStatisticRow, PlanCache,
    PlanCacheConfig, RelationStats, StatsCatalog,
};
use ultrasql_parser::Parser;
use ultrasql_planner::plan::{LockStrength, LockWaitPolicy};
use ultrasql_planner::{
    AggregateFunc, BinaryOp, Catalog as PlannerCatalog, InMemoryCatalog, LogicalIndexMethod,
    LogicalPlan, LogicalReferentialAction, ScalarExpr, TableMeta, UnaryOp, bind,
};
use ultrasql_protocol::BackendMessage;
use ultrasql_storage::access_method::{
    AccessMethod, AnnPayloadKind, BrinIndex, HnswMetric, PageBackedHnswIndex,
    PageBackedIvfFlatIndex,
};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{HeapAccess, HeapError, InsertOptions};
use ultrasql_storage::page::Page;
use ultrasql_storage::segment::{SegmentConfig, SegmentError, SegmentFileManager};
use ultrasql_storage::sequence::{Sequence, SequenceSnapshot};
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_txn::{
    IsolationLevel, LockManager, LockMode, LockRequest, LockTag, RowLockMode, SsiManager,
    Transaction, TransactionManager, TxnError,
};
use ultrasql_vec::Batch;
use ultrasql_vec::column::Column;
use ultrasql_vec::column::NumericColumn;
use ultrasql_vec::kernels::{
    CmpOp, cmp_i32_scalar, cmp_i64_scalar, filter_sum_i32_widening_gt, filter_sum_i64_gt,
    sum_i32_widening, sum_i32_widening_with_mask, sum_i64, sum_i64_with_mask,
};
use ultrasql_wal::applier::{ApplyError, HeapTarget};
use ultrasql_wal::payload::{
    AbortPayload, BTreeOpPayload, CheckpointPayload, CommitPayload, FullPageWritePayload,
    HeapDeleteInPlaceBatchPayload, HeapDeleteInPlacePayload, HeapDeleteInPlaceRangeBatchPayload,
    HeapDeletePayload, HeapInsertBatchPayload, HeapInsertPayload, HeapUpdateInPlaceBatchPayload,
    HeapUpdateInPlacePayload, HeapUpdateInt32PairDeltaBatchPayload,
    HeapUpdateInt32PairDeltaRangeBatchPayload, HeapUpdatePayload, SequenceOpKind,
    SequenceOpPayload,
};
use ultrasql_wal::{RecordType, WalRecord};

pub use embedded::EmbeddedDatabase;
pub use error::ServerError;
pub use pipeline::{LowerCtx, SampleTables, build_sample_database};
pub use result_encoder::{
    SelectResult, run_ddl_command, run_modify_command, run_modify_returning, run_select,
    run_select_streamed,
};
pub(crate) use serializable::{
    record_serializable_predicate_locks, record_serializable_write_conflicts,
};

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
    /// Optional data directory used by WAL-backed server instances.
    ///
    /// `None` means in-memory sample mode. When present, operational SQL
    /// shims such as `pg_start_backup()` can leave marker files in the same
    /// directory that CLI backup/restore commands use.
    pub data_dir: Option<std::path::PathBuf>,
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
    /// Backing loader used to spill dirty heap pages out of the buffer pool.
    pub(crate) page_loader: BlankPageLoader,
    /// Background checkpointer for persistent server instances.
    ///
    /// `None` means sample/in-memory mode. `Some` periodically flushes
    /// WAL-safe dirty heap pages into `<data_dir>/base` and is shut down
    /// before the WAL writer drops.
    pub(crate) checkpointer: Option<ultrasql_storage::Checkpointer>,
    /// Shared visibility map for heap relations. Mutations clear touched
    /// pages; maintenance marks pages all-visible after certification.
    pub vm: Arc<VisibilityMap>,
    /// Transaction manager. Owns the XID allocator, the CLOG, and the
    /// lock manager; every Simple Query in v0.5 runs as an autocommit
    /// transaction allocated from this manager.
    pub txn_manager: Arc<TransactionManager>,
    /// Cross-protocol optimized-plan cache.
    ///
    /// Keyed on raw SQL text (a `PlanCacheKey` wraps a `String`);
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
    /// Successful-commit counter used to trigger periodic undo-log GC.
    ///
    /// Every successful commit (explicit `COMMIT` or autocommit) calls
    /// [`Server::note_commit_for_gc`], which bumps this counter and,
    /// every [`UNDO_GC_INTERVAL_COMMITS`] commits, fires
    /// [`HeapAccess::vacuum_undo_log`] with the txn manager's current
    /// `oldest_in_progress()`. Trimming on a counter rather than per
    /// commit keeps the hot path cheap (one atomic add) and amortises
    /// the GC walk across many small transactions.
    pub vacuum_commit_counter: std::sync::atomic::AtomicU64,
    /// Runtime relation statistics populated by manual `ANALYZE`, by
    /// autovacuum-triggered analyze runs, and on WAL-backed restart from
    /// durable `pg_statistic` rows.
    pub stats_catalog: parking_lot::RwLock<InMemoryStatsCatalog>,
    /// Same-process runtime defaults/CHECK constraints keyed by table OID.
    ///
    /// The v0.8 runtime enforces these for INSERT/UPDATE. Persistence and
    /// restart bootstrap are tracked separately because the catalog heap does
    /// not yet encode bound expressions.
    pub table_constraints: Arc<dashmap::DashMap<ultrasql_core::Oid, Arc<TableRuntimeConstraints>>>,
    /// Same-process domain CHECK metadata keyed by domain OID.
    pub domain_constraints:
        Arc<dashmap::DashMap<ultrasql_core::Oid, Arc<DomainRuntimeConstraints>>>,
    /// Same-process row-level security policies keyed by table OID.
    pub row_security: Arc<dashmap::DashMap<ultrasql_core::Oid, Arc<TableRowSecurity>>>,
    /// Same-process sequence registry keyed by folded sequence name.
    pub sequences: Arc<dashmap::DashMap<String, Arc<ultrasql_storage::sequence::Sequence>>>,
    /// Runtime sequence owners keyed by folded sequence name.
    pub sequence_owners: Arc<dashmap::DashMap<String, String>>,
    /// Runtime sequence namespaces keyed by folded sequence name.
    pub sequence_namespaces: Arc<dashmap::DashMap<String, String>>,
    /// Runtime SQL schemas keyed by folded schema name.
    pub schemas: Arc<dashmap::DashMap<String, Arc<RuntimeSchema>>>,
    /// Same-process user-defined operator registry keyed by signature.
    pub operators: Arc<dashmap::DashMap<String, Arc<RuntimeOperator>>>,
    /// Same-process append-only materialized-view registry keyed by view name.
    pub materialized_views: Arc<dashmap::DashMap<String, Arc<MaterializedViewRuntime>>>,
    /// Same-process regular-view registry keyed by canonical view name.
    pub regular_views: Arc<dashmap::DashMap<String, Arc<RegularViewRuntime>>>,
    /// Same-process columnar secondary-storage registry.
    pub columnar_storage: Arc<columnar_storage::ColumnarSecondaryStore>,
    /// Same-process time-range partition registry keyed by canonical parent table key.
    pub time_partitions: Arc<dashmap::DashMap<String, Arc<time_partition::TimePartitionRuntime>>>,
    /// Same-process logical replication publication registry and CDC stream.
    pub logical_replication: Arc<replication::LogicalReplicationRuntime>,
    /// Same-process workload recorder for query timings and slow logs.
    pub workload_recorder: Arc<workload::WorkloadRecorder>,
    /// Accumulated tuple modifications since the last VACUUM pass,
    /// keyed by folded table name.
    pub table_modifications: dashmap::DashMap<String, u64>,
    /// Accumulated tuple modifications since the last ANALYZE scheduling pass,
    /// keyed by folded table name.
    pub table_analyze_modifications: dashmap::DashMap<String, u64>,
    /// Tables that crossed the autovacuum ANALYZE threshold and are
    /// waiting for the next maintenance pass.
    pub pending_analyze_tables: dashmap::DashMap<String, ()>,
    /// Runtime autovacuum thresholds used by the launcher and `pg_settings`.
    pub autovacuum_config: AutovacuumConfig,
    /// Runtime statement logging knobs used by SQL execution and `pg_settings`.
    pub logging_config: LoggingConfig,
    /// Idle-session timeout in milliseconds; `0` disables idle disconnects.
    pub idle_session_timeout_ms: u64,
    /// Runtime WAL archive command exposed through `pg_settings`.
    pub wal_archive_config: WalArchiveConfig,
    /// Two-phase commit coordinator. Owns the on-disk state directory
    /// for prepared transactions; consulted by
    /// `PREPARE TRANSACTION 'gid'`, `COMMIT PREPARED 'gid'`, and
    /// `ROLLBACK PREPARED 'gid'`.
    pub two_phase: Arc<ultrasql_txn::two_phase::TwoPhaseCoordinator>,
    /// Auth method this server requires from incoming connections.
    /// `Trust` accepts any startup, `Md5` runs a real password
    /// challenge with [`crate::auth::md5`].
    pub auth: AuthConfig,
    /// Optional TLS server configuration. When present, a client `SSLRequest`
    /// is answered with `'S'` and the connection is upgraded to TLS before the
    /// PostgreSQL startup handshake; when absent, `SSLRequest` is declined
    /// (`'N'`) and the session continues in plaintext.
    pub tls_server_config: Option<Arc<rustls::ServerConfig>>,
    /// Same-process role catalog backing role DDL and virtual auth views.
    pub role_catalog: Arc<auth::InMemoryAuthCatalog>,
    /// Same-process per-role live-session counter for `CONNECTION LIMIT`.
    pub role_connection_limiter: Arc<auth::RoleConnectionLimiter>,
    /// Same-process privilege catalog backing GRANT/REVOKE behavior.
    pub privilege_catalog: Arc<auth::InMemoryPrivilegeCatalog>,
    /// Async pub-sub hub backing `LISTEN` / `NOTIFY` / `UNLISTEN`.
    ///
    /// Shared across every connection task: a `NOTIFY` issued on one
    /// session dispatches a [`notify::NotificationRecord`] into the
    /// `mpsc::UnboundedSender` registered by each listening session.
    pub notify_hub: Arc<notify::NotifyHub>,
    /// Process-id allocator for new connections.
    ///
    /// The PostgreSQL wire layer identifies each backend by a 32-bit
    /// process id used for `BackendKeyData`, `CancelRequest`, and
    /// `NotificationResponse`. UltraSQL is single-process so the
    /// counter is a monotonic per-server allocator rather than a real
    /// kernel PID. Starts at 1 to leave 0 reserved for "unset".
    pub next_pid: std::sync::atomic::AtomicU32,
    /// Registry of (pid, secret) → `CancelFlag` for in-flight queries.
    ///
    /// Populated by each `Session` on construction so a peer
    /// `CancelRequest` carrying matching `(pid, secret)` flips the
    /// session's `CancelFlag`. Operators that loop over batches
    /// (`SeqScan`, `HashAggregate`) poll the flag between batches and
    /// short-circuit with [`ultrasql_executor::ExecError::Cancelled`]
    /// → SQLSTATE `57014`.
    pub cancel_registry: Arc<cancel::CancelRegistry>,
    /// Hot-standby read-only flag.
    ///
    /// Set when the server boots from a data directory containing
    /// `standby.signal` or `recovery.signal`. Sessions accept reads and
    /// reject writes before planning so a standby can safely serve analytical
    /// queries while WAL shipping/replay catches up.
    pub standby_mode: std::sync::atomic::AtomicBool,
    /// Background WAL writer owned by WAL-backed server instances.
    ///
    /// `None` means in-memory sample mode. `Some` means `Server::init`
    /// installed a [`wal_sink::WalBufferSink`] into the buffer pool and this
    /// handle keeps the drain/fsync thread alive until the server drops.
    pub(crate) wal_writer: Option<ultrasql_wal::WalWriter>,
    /// Typed handle to the WAL sink, kept so the checkpoint can bound WAL
    /// segment recycling by the oldest in-progress transaction's first written
    /// LSN. `None` in in-memory sample mode.
    pub(crate) wal_buffer_sink: Option<Arc<wal_sink::WalBufferSink>>,
    /// WAL segment directory, needed to recycle segments at checkpoint. `None`
    /// in in-memory sample mode.
    pub(crate) wal_dir: Option<std::path::PathBuf>,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(checkpointer) = self.checkpointer.take()
            && let Err(e) = checkpointer.shutdown()
        {
            warn!(error = %e, "checkpointer shutdown failed during server drop");
        }
        if self.wal_writer.is_some()
            && let Err(e) = self.flush_dirty_heap_pages()
        {
            warn!(error = %e, "final dirty heap page flush failed during server drop");
        }
    }
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

#[cfg(test)]
mod tests;
