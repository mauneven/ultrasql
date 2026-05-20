//! Per-connection [`Session`] state machine + impl pieces.
//!
//! The implementation is intentionally fragmented across several
//! files in this directory so no single unit exceeds the 600-line
//! ceiling. `mod.rs` carries the struct definition and the smallest
//! constructor; every other method lives in a sibling file that
//! reopens the same `impl<RW> Session<RW>` block.

#![allow(unused_imports)]

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::extended::ExtendedConnState;
use crate::notify::NotificationRecord;
use crate::replication::LogicalChangeKind;
use crate::{READ_BUFFER_INITIAL, Server, TxnState};

mod alter;
mod copy;
mod ddl;
mod execute;
mod explain;
mod ext;
mod io;
mod jsonb_ingest;
mod meta_stmt;
mod notify;
mod parquet_copy;
mod run;
mod sequence;
mod startup;
mod txn;

pub(crate) struct Session<RW> {
    pub(super) io: RW,
    pub(super) read_buf: BytesMut,
    pub(super) write_buf: BytesMut,
    pub(super) state: Arc<Server>,
    pub(super) extended: ExtendedConnState,
    pub(super) txn_state: TxnState,
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
    /// Session-local JIT enable flag, controlled by `SET jit`.
    pub(super) jit_enabled: bool,
    /// Session-local row threshold, controlled by `SET jit_above_cost`.
    pub(super) jit_above_rows: usize,
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
    /// Pending logical CDC changes emitted only after COMMIT succeeds.
    pub(super) pending_logical_changes: Vec<PendingLogicalChange>,
    /// Materialized-view row counters accumulated inside the current
    /// transaction. Applied only after COMMIT so rollback cannot advance
    /// append offsets.
    pub(super) pending_materialized_view_rows: Vec<(Arc<crate::MaterializedViewRuntime>, u64)>,
    /// Per-session sequence state used by `currval` / `lastval`.
    pub(super) sequence_state: crate::SequenceSessionState,
    /// `true` when an autocommit statement committed successfully and
    /// its background-ish maintenance hook should run after the reply
    /// bytes are already on the wire.
    pub(super) pending_post_commit_maintenance: bool,
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(io: RW, state: Arc<Server>) -> Self {
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
        Self {
            io,
            read_buf: BytesMut::with_capacity(READ_BUFFER_INITIAL),
            write_buf: BytesMut::with_capacity(READ_BUFFER_INITIAL),
            state,
            extended: crate::extended::ExtendedConnState::new(),
            txn_state: TxnState::Idle,
            pid,
            secret,
            cancel_flag,
            jit_enabled: false,
            jit_above_rows: ultrasql_vec::jit::DEFAULT_JIT_ABOVE_ROWS,
            notify_rx,
            stmt_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            jsonb_shape_cache: std::cell::RefCell::new(jsonb_ingest::JsonbShapeCache::default()),
            pending_table_modifications: std::collections::HashMap::new(),
            pending_logical_changes: Vec::new(),
            pending_materialized_view_rows: Vec::new(),
            sequence_state: crate::SequenceSessionState::default(),
            pending_post_commit_maintenance: false,
        }
    }

    pub(super) fn jit_config(&self) -> ultrasql_vec::jit::JitConfig {
        ultrasql_vec::jit::JitConfig {
            enabled: self.jit_enabled,
            above_rows: self.jit_above_rows,
        }
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
        self.state.notify_hub.deregister_connection(self.pid);
        self.state.cancel_registry.deregister(self.pid);
    }
}
