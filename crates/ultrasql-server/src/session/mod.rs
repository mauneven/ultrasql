//! Per-connection [`Session`] state machine + impl pieces.
//!
//! The implementation is intentionally fragmented across several
//! files in this directory so no single unit exceeds the 600-line
//! ceiling. `mod.rs` carries the struct definition and the smallest
//! constructor; every other method lives in a sibling file that
//! reopens the same `impl<RW> Session<RW>` block.

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::extended::ExtendedConnState;
use crate::notify::NotificationRecord;
use crate::replication::LogicalChangeKind;
use crate::{READ_BUFFER_INITIAL, Server, TxnState};

mod abort_guard;
pub(crate) use abort_guard::AutocommitAbortGuard;
mod advisory;
mod alter;
mod alter_in_txn;
mod catalog_overlay;
mod copy;
mod cursor;
mod ddl;
mod drop_in_txn;
mod embedded;
mod execute;
pub(crate) use execute::{DEFAULT_WORK_MEM_BYTES, work_mem_budget_from_settings};
mod explain;
mod export_import;
mod ext;
mod io;
mod jsonb_ingest;
mod meta_stmt;
mod notify;
mod ownership;
mod parquet_copy;
mod privilege;
mod privilege_enforce;
mod privilege_sources;
mod replication;
mod role;
pub(crate) use role::role_is_superuser;
mod run;
mod schema;
mod sequence;
mod startup;
mod timeout;
mod txn;

pub(crate) struct Session<RW> {
    pub(super) io: crate::tls::MaybeTlsStream<RW>,
    pub(super) read_buf: BytesMut,
    pub(super) write_buf: BytesMut,
    pub(super) state: Arc<Server>,
    /// Startup `user` parameter, folded as the stable `session_user`.
    pub(super) auth_user: String,
    /// Effective role used by `current_user` and privilege checks.
    pub(super) current_user: String,
    pub(super) extended: ExtendedConnState,
    pub(super) txn_state: TxnState,
    /// Transaction-start instant (engine-epoch micros) for an explicit
    /// transaction block, captured when the block's first command (`BEGIN`)
    /// runs and held until the block ends.
    ///
    /// PostgreSQL pins `now()` / `current_timestamp` / `current_date` to this
    /// instant for every statement in the transaction. `None` outside an
    /// explicit block — autocommit statements use their own statement-start
    /// instant as the transaction timestamp (each statement is its own
    /// implicit transaction). Cleared on COMMIT / ROLLBACK / error rollback.
    pub(super) txn_start_micros: Option<i64>,
    /// Per-session parse+bind cache for Simple Query traffic.
    ///
    /// Key: trimmed SQL text. Value: `Arc`-wrapped `LogicalPlan` — the
    /// output of `Parser::new(sql).parse_statement()` followed by
    /// `bind(...)`. The `Arc` makes the cache-hit clone an
    /// atomic-refcount bump instead of a deep `LogicalPlan` walk; on
    /// dispatch we deref once and `Arc::unwrap_or_clone` to get an
    /// owned plan for the optimizer.
    ///
    /// A hit skips both passes on the hot path; a cold statement
    /// still pays them once. The cache is flushed by every DDL hook
    /// that already invalidates the optimizer's `PlanCache` (see
    /// `plan_cache_invalidate`), so a catalog change can never
    /// resurrect a stale plan.
    ///
    /// Interior mutability lets the `&self` DDL dispatchers reset the
    /// cache without rippling `&mut self` across the session API.
    pub(super) stmt_cache:
        std::cell::RefCell<std::collections::HashMap<String, Arc<ultrasql_planner::LogicalPlan>>>,
    /// Logical plans whose static DML safety checks already passed.
    ///
    /// Identity is the cached [`Arc<LogicalPlan>`](ultrasql_planner::LogicalPlan)
    /// allocation, not a bare heap address: the map is keyed by the Arc's
    /// pointer for O(1) lookup but the stored `Arc` is the source of truth,
    /// so membership is confirmed with [`Arc::ptr_eq`]. Because the entry
    /// keeps a strong reference, the allocation can never be freed and its
    /// address reused by an unrelated plan while it is cached — closing the
    /// ABA hazard that a raw-address `HashSet` would have. The only plans
    /// looked up here are the pointer-stable `stmt_cache` `Arc`s, so a
    /// freshly-allocated short-lived plan can never produce a false hit.
    ///
    /// This is intentionally narrower than `stmt_cache`: entries are added
    /// only for simple fused DML shapes with no row-security rewrite and no
    /// materialized-view source guard. `plan_cache_invalidate` clears it
    /// alongside `stmt_cache`, so role, privilege, RLS, or DDL changes force
    /// the next execution back through the full checks.
    pub(super) prechecked_fast_dml:
        std::cell::RefCell<std::collections::HashMap<usize, Arc<ultrasql_planner::LogicalPlan>>>,
    /// Per-session split cache for repeated multi-statement Simple Query text.
    ///
    /// The hot mixed benchmark sends the same `INSERT; UPDATE; SELECT` batch
    /// many times. Caching the parser's statement boundaries lets each child
    /// statement still flow through the normal parse/bind/plan caches without
    /// reparsing the outer batch envelope first.
    pub(super) simple_batch_cache:
        std::cell::RefCell<std::collections::HashMap<String, Arc<Vec<String>>>>,
    /// Session-local JSONB shape cache used by COPY ingest.
    ///
    /// Repeated AI/event rows tend to share object keys and structural
    /// layout. The cache records those shapes while the SIMD parser
    /// validates bytes, giving the ingest path a stable hook for
    /// shape-specific fast paths without weakening correctness.
    pub(in crate::session) jsonb_shape_cache: std::cell::RefCell<jsonb_ingest::JsonbShapeCache>,
    /// Per-connection process id allocated at session construction.
    ///
    /// Used as the `pid` field in `BackendKeyData` and as the routing
    /// key into [`crate::notify::NotifyHub`] / [`crate::cancel::
    /// CancelRegistry`]. Stable for the lifetime of the session.
    pub(super) pid: u32,
    /// Per-connection secret echoed in `BackendKeyData` and required by
    /// the peer's `CancelRequest`. A mismatch silently fails the cancel.
    pub(super) secret: u32,
    /// Cancel flag clone for this session's in-flight query. Cloned
    /// into every [`crate::pipeline::LowerCtx`] built for an Execute /
    /// Simple Query so the executor can poll it between batches.
    pub(super) cancel_flag: ultrasql_executor::CancelFlag,
    /// Catalogued role whose startup connection slot was admitted.
    ///
    /// `None` means startup failed before admission, or the peer used a
    /// legacy uncatalogued trust user. `Drop` releases the slot when present.
    pub(super) connection_limit_role: Option<String>,
    /// Client IP address, for `pg_hba` host-rule matching. `None` for
    /// in-process / Unix-socket connections (tests, embedded), which match
    /// `local` rules rather than `host` rules.
    pub(super) peer_ip: Option<std::net::IpAddr>,
    /// Session-local JIT enable flag, controlled by `SET jit`.
    pub(super) jit_enabled: bool,
    /// Session-local row threshold, controlled by `SET jit_above_cost`.
    pub(super) jit_above_rows: usize,
    /// Session-local `statement_timeout` in milliseconds; `0` disables it.
    pub(super) statement_timeout_ms: u64,
    /// Session-local `lock_timeout` in milliseconds; `0` (the default,
    /// matching PostgreSQL) disables it. When set, any statement that
    /// blocks on a heap row / relation / advisory lock longer than this
    /// aborts with SQLSTATE `55P03` (`lock_not_available`) instead of
    /// waiting indefinitely; the timed-out waiter is removed from the
    /// lock manager's wait queue.
    pub(super) lock_timeout_ms: u64,
    /// Session-local `idle_in_transaction_session_timeout` in milliseconds;
    /// `0` disables it. When set, a session left idle inside an open
    /// transaction longer than this is rolled back and disconnected (SQLSTATE
    /// 25P03), releasing the row/relation locks it held so other backends are
    /// not blocked indefinitely by an abandoned transaction.
    pub(super) idle_in_transaction_session_timeout_ms: u64,
    /// Session-local `log_statement` class filter, controlled by
    /// `SET log_statement` (`none` | `ddl` | `mod` | `all`). Starts at the
    /// server-config value; `RESET` restores that default.
    pub(super) log_statement: crate::LogStatementMode,
    /// Session-local `log_min_duration_statement` in milliseconds; `-1`
    /// disables duration-based statement logging, `0` logs every statement.
    /// Starts at the server-config value; `RESET` restores that default.
    pub(super) log_min_duration_statement_ms: i64,
    /// Session-local custom GUCs used by row-level security policies.
    pub(super) session_settings: std::collections::HashMap<String, String>,
    /// Receiver half of the per-connection notification channel.
    ///
    /// `LISTEN` registers the session under [`Self::pid`] and the hub
    /// fans `NOTIFY` payloads in here. The read-side wire loop drains
    /// the channel between `Sync` boundaries and writes each pending
    /// `NotificationResponse` before the trailing `ReadyForQuery`.
    pub(super) notify_rx: mpsc::UnboundedReceiver<NotificationRecord>,
    /// Per-table modified-row counters accumulated inside an explicit
    /// transaction block. Flushed to server-level maintenance hooks on
    /// COMMIT, cleared on ROLLBACK.
    pub(super) pending_table_modifications: std::collections::HashMap<String, u64>,
    /// Transaction-scoped catalog overlay holding an in-progress
    /// `CREATE TABLE` issued inside an explicit transaction block. `None`
    /// outside a transaction or when no transactional DDL has run; the hot
    /// catalog read path stays wait-free in that case. Merged into the
    /// global catalog on COMMIT, discarded on ROLLBACK (transactional-DDL
    /// milestone 1; see [`catalog_overlay`]).
    pub(super) pending_catalog_ddl: Option<catalog_overlay::CatalogOverlay>,
    /// Pending logical CDC changes emitted only after COMMIT succeeds.
    pub(super) pending_logical_changes: Vec<PendingLogicalChange>,
    /// Materialized-view row counters accumulated inside the current
    /// transaction. Applied only after COMMIT so rollback cannot advance
    /// append offsets.
    pub(super) pending_materialized_view_rows: Vec<(Arc<crate::MaterializedViewRuntime>, u64)>,
    /// Per-session sequence state used by `currval` / `lastval`.
    pub(super) sequence_state: crate::SequenceSessionState,
    /// Per-session PostgreSQL advisory locks.
    pub(super) advisory_state: crate::AdvisorySessionState,
    /// Open server-side cursors, keyed by case-folded cursor name.
    /// Every cursor is `WITHOUT HOLD` and forward-only: `DECLARE`
    /// requires an open transaction block, materializes the SELECT, and
    /// `COMMIT` / `ROLLBACK` / `PREPARE TRANSACTION` drops the whole
    /// registry (see [`cursor`]).
    pub(super) cursors: std::collections::HashMap<String, cursor::SessionCursor>,
    /// `true` when an autocommit statement committed successfully and
    /// its background-ish maintenance hook should run after the reply
    /// bytes are already on the wire.
    pub(super) pending_post_commit_maintenance: bool,
    /// `true` when the startup packet set the `replication` parameter to a
    /// truthy value. Such a connection is routed to the physical-replication
    /// walsender command loop ([`Session::run_replication`]) instead of the
    /// SQL [`run`](Session::run) loop. Set during [`Session::startup`].
    pub(super) is_replication: bool,
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(io: RW, state: Arc<Server>, peer_ip: Option<std::net::IpAddr>) -> Self {
        // Register with the cancel registry first: it owns the canonical
        // per-session (pid, secret) tuple. Using the registry's pid for
        // the NotifyHub keeps a single u32 space for both subsystems,
        // so a peer's `(pid, secret)` cancel and a `NOTIFY` to the same
        // pid route consistently.
        let cancel_flag = ultrasql_executor::CancelFlag::new();
        let (pid, secret) = state.cancel_registry.register(cancel_flag.clone());
        // Register this session with the hub up front so a `NOTIFY`
        // racing against the `LISTEN` on this connection always finds a
        // live sender. Sending happens regardless of whether anyone is
        // listening; the receiver just buffers.
        let notify_rx = state.notify_hub.register_connection(pid);
        state.workload_recorder.register_session(pid, "tester");
        // Sessions start at the server-wide default statement timeout; a
        // client may `SET statement_timeout` to any value (including 0).
        let statement_timeout_ms = state.default_statement_timeout_ms;
        // Statement-logging GUCs likewise start at the server-config
        // defaults and are runtime-settable per session.
        let logging_defaults = state.logging_config();
        Self {
            // Start plaintext; a client `SSLRequest` upgrades it in place when
            // the server has a TLS config (see `startup`).
            io: crate::tls::MaybeTlsStream::Plain(io),
            read_buf: BytesMut::with_capacity(READ_BUFFER_INITIAL),
            write_buf: BytesMut::with_capacity(READ_BUFFER_INITIAL),
            state,
            auth_user: "tester".to_owned(),
            current_user: "tester".to_owned(),
            extended: crate::extended::ExtendedConnState::new(),
            txn_state: TxnState::Idle,
            txn_start_micros: None,
            pid,
            secret,
            cancel_flag,
            connection_limit_role: None,
            peer_ip,
            jit_enabled: false,
            jit_above_rows: ultrasql_vec::jit::DEFAULT_JIT_ABOVE_ROWS,
            statement_timeout_ms,
            lock_timeout_ms: 0,
            idle_in_transaction_session_timeout_ms: 0,
            log_statement: logging_defaults.log_statement,
            log_min_duration_statement_ms: logging_defaults.log_min_duration_statement_ms,
            session_settings: std::collections::HashMap::new(),
            notify_rx,
            stmt_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            prechecked_fast_dml: std::cell::RefCell::new(std::collections::HashMap::new()),
            simple_batch_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            jsonb_shape_cache: std::cell::RefCell::new(jsonb_ingest::JsonbShapeCache::default()),
            pending_table_modifications: std::collections::HashMap::new(),
            pending_catalog_ddl: None,
            pending_logical_changes: Vec::new(),
            pending_materialized_view_rows: Vec::new(),
            sequence_state: crate::SequenceSessionState::default(),
            advisory_state: crate::AdvisorySessionState::new(pid),
            cursors: std::collections::HashMap::new(),
            pending_post_commit_maintenance: false,
            is_replication: false,
        }
    }

    pub(super) fn jit_config(&self) -> ultrasql_vec::jit::JitConfig {
        ultrasql_vec::jit::JitConfig {
            enabled: self.jit_enabled,
            above_rows: self.jit_above_rows,
        }
    }

    /// Build the per-statement `work_mem` budget for this session.
    ///
    /// Reads the session-local `work_mem` GUC (stored canonically as a byte
    /// count in `session_settings` by the SET handler) and falls back to
    /// [`crate::session::execute::DEFAULT_WORK_MEM_BYTES`] when unset. The
    /// returned budget is shared by reference across every memory-heavy
    /// operator in the statement's plan, so once a query's working set
    /// crosses the budget the executor's spill paths engage instead of
    /// growing the heap without bound.
    pub(super) fn work_mem_budget(&self) -> Arc<ultrasql_executor::work_mem::WorkMemBudget> {
        crate::session::execute::work_mem_budget_from_settings(&self.session_settings)
    }

    /// Build the per-statement blocking-lock wait options for this session.
    ///
    /// Carries the session's `lock_timeout` (55P03 on expiry) and a clone
    /// of the statement [`CancelFlag`](ultrasql_executor::CancelFlag) so a
    /// `statement_timeout` deadline or a client `CancelRequest` interrupts
    /// a lock wait with 57014 instead of blocking until the lock frees.
    pub(super) fn lock_wait(&self) -> ultrasql_txn::LockWait {
        crate::lock_wait_options(self.lock_timeout_ms, Some(&self.cancel_flag))
    }
}

#[derive(Clone, Debug)]
pub(super) struct PendingLogicalChange {
    pub(super) table: String,
    pub(super) kind: LogicalChangeKind,
    pub(super) rows_affected: u64,
}

impl<RW> Drop for Session<RW> {
    /// Deregister the connection from the notification hub *and* the
    /// cancel registry on drop so the per-pid sender is released and any
    /// orphaned subscriptions are removed.
    fn drop(&mut self) {
        // A client that disconnects (EOF / Terminate / error) with an
        // explicit transaction still open must not leave that transaction
        // in-progress forever: its row/relation locks would never release
        // and any peer blocked on them would hang indefinitely, and its
        // xid would pin vacuum. Abort it here — a disconnect is a
        // rollback, matching PostgreSQL.
        match std::mem::replace(&mut self.txn_state, TxnState::Idle) {
            TxnState::InTransaction(txn) | TxnState::Failed(txn) => {
                if let Err(e) = self.state.abort_transaction(
                    txn,
                    false,
                    "session disconnect with open transaction",
                ) {
                    tracing::warn!(
                        pid = self.pid,
                        error = %e,
                        "failed to abort open transaction on session drop"
                    );
                }
            }
            TxnState::Idle => {}
        }
        // A client that disconnects mid-transaction after an in-txn
        // CREATE TABLE (no COMMIT/ROLLBACK) must not leave the staged,
        // non-MVCC global side maps (runtime constraints / RLS / privileges)
        // dirty for the lifetime of the process. The durable catalog rows
        // ride the now-orphaned user xid (swept to aborted by recovery) and
        // were never published to the global catalog, so only the staged
        // in-memory side effects need reverting here.
        self.revert_staged_catalog_ddl_side_effects();
        self.advisory_state
            .release_all(&self.state.txn_manager.lock_manager);
        self.state.notify_hub.deregister_connection(self.pid);
        self.state.cancel_registry.deregister(self.pid);
        if let Some(role) = self.connection_limit_role.take() {
            self.state.role_connection_limiter.release(&role);
        }
        self.state.workload_recorder.deregister_session(self.pid);
    }
}
