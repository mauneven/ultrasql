//! EvalPlanQual: the per-row Exclusive tuple lock + latest-version
//! re-check that the **general** UPDATE / DELETE write path takes before
//! it writes a new tuple version.
//!
//! # Why this exists
//!
//! The columnar int32-pair fast path
//! ([`crate::fused_update::FusedUpdateInt32Add`]) is the only write path
//! that, until now, took an Exclusive row lock and re-read the latest
//! committed version after the lock unblocked. The general
//! `ModifyTable(Filter(SeqScan))` path read its target tuples under the
//! statement snapshot and wrote new versions through
//! [`HeapAccess::update_many`] / [`HeapAccess::delete_many`] **without**
//! locking the old version or re-checking it — so two concurrent ordinary
//! UPDATEs to the same row would each compute their new value against the
//! same stale snapshot and the second writer's version would silently
//! overwrite (lose) the first's. A held `SELECT ... FOR UPDATE` did not
//! stop a general UPDATE either, because that path never consulted the
//! lock manager.
//!
//! [`EvalPlanQual`] gives the general path the **same** locking discipline
//! the fast path uses (one shared write-path lock+refresh contract): for
//! every targeted base TID it acquires the Exclusive `LockTag::Tuple` lock
//! (blocking + deadlock-aware) under the session xid, then, once the lock
//! is granted (the conflicting writer committed or aborted), re-reads the
//! latest committed version and decides what to do per the isolation
//! level — mirroring PostgreSQL's `EvalPlanQual` / `heap_lock_tuple`
//! follow-the-update-chain machinery.
//!
//! # Isolation
//!
//! - **READ COMMITTED**: re-read the latest committed version of the
//!   tuple. If it was deleted by the concurrent committed txn, SKIP it.
//!   If the latest version no longer satisfies the UPDATE/DELETE's WHERE
//!   predicate, SKIP it. Otherwise apply the mutation to the **latest**
//!   row image (the SET expressions are re-evaluated against the latest
//!   row by the caller). This matches PostgreSQL READ COMMITTED.
//! - **REPEATABLE READ / SERIALIZABLE**: a concurrent committed update or
//!   delete to the target row is a write-write conflict → abort with
//!   SQLSTATE 40001 (`serialization_failure`), first-updater-wins. This is
//!   the same outcome the fused fast path produces (it surfaces
//!   `HeapError::WriteConflict` → `ExecError::SerializationFailure` when no
//!   refresh is permitted).

use std::sync::Arc;

use ultrasql_core::{TupleId, Value};
use ultrasql_mvcc::{Snapshot, TupleHeader, Visibility, XidStatus, XidStatusOracle, is_visible};
use ultrasql_storage::PageLoader;
use ultrasql_storage::heap::{HeapAccess, HeapTuple};
use ultrasql_txn::IsolationLevel;

use super::helpers::updated_ctid_target;
use crate::eval::Eval;
use crate::row_codec::RowCodec;
use crate::{ExecError, eval_error_to_exec_error};

/// Closure that acquires the Exclusive tuple lock on a base TID under the
/// session xid, blocking (deadlock-aware) on a conflict. Returns `Ok(true)`
/// when the caller had to wait for a conflicting holder, `Ok(false)` when the
/// grant was immediate / already held — the READ COMMITTED re-check ignores
/// this flag (it always re-reads the latest committed version) and the fused
/// fast path still uses it. `Err` is already classified by the caller:
/// [`ExecError::DeadlockDetected`] (→ 40P01) for a lock-wait cycle victim, or
/// [`ExecError::SerializationFailure`] (→ 40001) for any other lock-manager
/// failure.
pub type TupleLockFn = dyn Fn(TupleId) -> Result<bool, ExecError> + Send + Sync;

/// Factory that produces a fresh snapshot — the latest committed state —
/// that the READ COMMITTED EvalPlanQual re-check resolves every targeted
/// row's latest version against.
pub type FreshSnapshotFn = dyn Fn() -> Snapshot + Send + Sync;

/// Heap-fetch closure binding the operator's `Arc<HeapAccess<L>>` so the
/// EvalPlanQual struct can stay free of the `L: PageLoader` type parameter
/// (one `Option<EvalPlanQual>` field works for every `ModifyTable<L>`).
pub type EpqFetchFn = dyn Fn(TupleId) -> Result<HeapTuple, ExecError> + Send + Sync;

/// The decision the EvalPlanQual re-check reaches for one targeted tuple.
#[derive(Debug)]
pub enum EpqDecision {
    /// Apply the mutation to this base TID against the supplied latest row
    /// image (RC re-reads the latest version; the row equals the snapshot
    /// row when nothing changed underneath us).
    Apply {
        /// The latest committed base TID to write (follows the update
        /// chain; equals the original TID for an in-place update).
        tid: TupleId,
        /// The latest committed relation row image. The caller re-evaluates
        /// the SET expressions / RETURNING against this.
        latest_row: Vec<Value>,
    },
    /// Skip this tuple: the latest version was deleted by a concurrent
    /// committed txn, or it no longer satisfies the WHERE predicate (RC).
    Skip,
}

/// Construction inputs for [`EvalPlanQual`]. Bundled so the lowering layer
/// builds the re-check from one named struct rather than a long positional
/// argument list.
pub struct EvalPlanQualConfig {
    /// Blocking, deadlock-aware Exclusive tuple-lock acquisition.
    pub lock: Arc<TupleLockFn>,
    /// Fresh-snapshot factory for the READ COMMITTED latest-version re-read.
    pub fresh_snapshot: Arc<FreshSnapshotFn>,
    /// Heap tuple fetch (binds the operator's heap; see [`make_epq_fetch`]).
    pub fetch: Arc<EpqFetchFn>,
    /// Status oracle for resolving xmin/xmax commit state.
    pub oracle: Arc<dyn XidStatusOracle>,
    /// Statement snapshot the child scan read the targets under.
    pub snapshot: Snapshot,
    /// Transaction isolation (RC re-read vs RR/SSI 40001).
    pub isolation: IsolationLevel,
    /// Optional un-shifted WHERE predicate for the RC predicate re-check.
    pub predicate: Option<Eval>,
    /// Codec for decoding a heap tuple into a relation row.
    pub codec: RowCodec,
}

impl std::fmt::Debug for EvalPlanQualConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvalPlanQualConfig")
            .field("isolation", &self.isolation)
            .field("has_predicate", &self.predicate.is_some())
            .finish_non_exhaustive()
    }
}

/// Per-row Exclusive tuple lock + latest-version EvalPlanQual re-check for
/// the general UPDATE / DELETE path. See the module header.
pub struct EvalPlanQual {
    /// Blocking, deadlock-aware Exclusive tuple-lock acquisition.
    lock: Arc<TupleLockFn>,
    /// Fresh-snapshot factory for the RC latest-version re-read.
    fresh_snapshot: Arc<FreshSnapshotFn>,
    /// Heap tuple fetch (binds the operator's heap; see [`make_epq_fetch`]).
    fetch: Arc<EpqFetchFn>,
    /// Status oracle for resolving xmin/xmax commit state.
    oracle: Arc<dyn XidStatusOracle>,
    /// Statement snapshot under which the child scan read the targets.
    /// Used by the RR/SSI conflict test to decide whether the latest
    /// version was modified by a transaction this snapshot did not see.
    snapshot: Snapshot,
    /// Transaction isolation, which selects RC re-read vs RR/SSI 40001.
    isolation: IsolationLevel,
    /// Optional un-shifted WHERE predicate, evaluated against the latest
    /// relation row for the RC predicate re-check. `None` means the
    /// statement had no WHERE clause (every row matches).
    predicate: Option<Eval>,
    /// Codec for decoding the latest heap tuple into a relation row.
    codec: RowCodec,
}

impl std::fmt::Debug for EvalPlanQual {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvalPlanQual")
            .field("isolation", &self.isolation)
            .field("has_predicate", &self.predicate.is_some())
            .finish_non_exhaustive()
    }
}

impl EvalPlanQual {
    /// Build the EvalPlanQual re-check for a general UPDATE/DELETE.
    #[must_use]
    pub fn new(config: EvalPlanQualConfig) -> Self {
        let EvalPlanQualConfig {
            lock,
            fresh_snapshot,
            fetch,
            oracle,
            snapshot,
            isolation,
            predicate,
            codec,
        } = config;
        Self {
            lock,
            fresh_snapshot,
            fetch,
            oracle,
            snapshot,
            isolation,
            predicate,
            codec,
        }
    }

    /// Acquire the Exclusive lock on `tid` and re-check the latest version.
    ///
    /// `tid` is the base TID the child scan emitted (visible under the
    /// statement snapshot). On return:
    ///
    /// - [`EpqDecision::Apply`] — the caller must write the mutation to the
    ///   returned `tid` using the returned `latest_row` as the row image
    ///   the SET / RETURNING expressions evaluate against.
    /// - [`EpqDecision::Skip`] — the caller must not write this tuple.
    ///
    /// # Errors
    ///
    /// - A deadlock or lock-manager failure from the lock closure
    ///   (propagated as [`ExecError::SerializationFailure`]; the server
    ///   classifies the deadlock variant as 40P01 upstream).
    /// - Under REPEATABLE READ / SERIALIZABLE, a concurrent committed
    ///   update/delete to the target row → [`ExecError::SerializationFailure`]
    ///   (40001).
    pub fn lock_and_recheck(&self, tid: TupleId) -> Result<EpqDecision, ExecError> {
        // 1. Exclusive tuple lock (blocking, deadlock-aware). Under READ
        //    COMMITTED we re-read the latest committed version regardless of
        //    whether the grant blocked — the per-row `waited` flag is NOT a
        //    sound "snapshot is fresh" signal on a multi-row statement: a
        //    concurrent writer can hold a lock on a *later* target row,
        //    commit (releasing every lock it held) while we are parked on an
        //    *earlier* row, so our subsequent grant on that later row is
        //    immediate (`waited == false`) even though its committed version
        //    just changed. Always following the latest-version path closes
        //    that stale-snapshot lost-update window.
        (self.lock)(tid)?;

        match self.isolation {
            IsolationLevel::ReadCommitted => self.recheck_read_committed(tid),
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                self.recheck_repeatable_read(tid)
            }
        }
    }

    /// READ COMMITTED EvalPlanQual: re-read the latest committed version of
    /// the tuple. Follow the update chain to the live terminal version,
    /// locking each version we land on (so a concurrent writer of the
    /// *resolved* version also serializes), skip if it was deleted, and
    /// re-check the WHERE predicate against the latest row.
    fn recheck_read_committed(&self, tid: TupleId) -> Result<EpqDecision, ExecError> {
        // Always resolve against a *fresh* snapshot (the latest committed
        // state), never the statement snapshot the child scan read under.
        // The grant blocking is not a reliable signal that the snapshot is
        // current: on a multi-row statement a concurrent writer can hold a
        // lock on a different target row, commit (dropping all its locks)
        // while we are parked elsewhere, and then our grant on this row is
        // immediate even though its committed version changed. Re-reading the
        // latest version unconditionally is what PostgreSQL READ COMMITTED
        // EvalPlanQual does; when nothing changed underneath us the fresh
        // snapshot resolves to the same version the statement snapshot saw,
        // so the result is identical (one extra heap fetch).
        let mut resolve_from = tid;
        // Chase the update chain, re-locking each resolved latest version.
        // When an out-of-place UPDATE redirected our locked version, the new
        // version could be the live target of a *third* concurrent writer
        // that scanned after the redirect committed; locking the resolved
        // TID (PostgreSQL `heap_lock_updated_tuple`) keeps it serialized too.
        // Bounded by the chain length the fetch helper already walks.
        for _ in 0..64 {
            let fresh = (self.fresh_snapshot)();

            let Some((latest_tid, latest_row)) = self.fetch_latest_visible(resolve_from, &fresh)?
            else {
                // The tuple (or its update-chain head) is no longer visible:
                // a concurrent committed txn deleted it. Skip.
                return Ok(EpqDecision::Skip);
            };

            // The resolved version differs from the one we hold the lock on:
            // lock it (it may have its own concurrent writer) and re-resolve
            // — the act of acquiring the lock could itself force a wait that
            // exposes a newer committed version, so loop back and re-read.
            if latest_tid != resolve_from {
                (self.lock)(latest_tid)?;
                resolve_from = latest_tid;
                continue;
            }

            // Predicate re-check against the latest row image. If the row no
            // longer matches the UPDATE/DELETE's WHERE, skip it (PostgreSQL
            // READ COMMITTED EvalPlanQual: the new version may have moved out
            // of the qualifying set).
            if !self.row_matches_predicate(&latest_row)? {
                return Ok(EpqDecision::Skip);
            }

            return Ok(EpqDecision::Apply {
                tid: latest_tid,
                latest_row,
            });
        }
        Err(ExecError::Internal(
            "EvalPlanQual lock chain exceeded 64 hops",
        ))
    }

    /// REPEATABLE READ / SERIALIZABLE: first-updater-wins. The base version
    /// our snapshot read is `tid`. If a *foreign* (not our own) transaction
    /// has committed an update or delete of that exact version — i.e. its
    /// `xmax` names a committed foreign xid — then the row changed under us
    /// after we read it: a write-write conflict → abort with 40001.
    ///
    /// This holds regardless of the physical update style: an out-of-place
    /// UPDATE leaves the OLD version `Visibility::Visible` to a snapshot that
    /// predates the writer's commit (correct MVCC — the snapshot must still
    /// see the old row), so a visibility test is the *wrong* signal here.
    /// The committed-foreign-`xmax` test is the direct first-updater-wins
    /// check PostgreSQL applies in `heap_update`'s `HeapTupleUpdated` path.
    fn recheck_repeatable_read(&self, tid: TupleId) -> Result<EpqDecision, ExecError> {
        let tuple = (self.fetch)(tid)?;

        // We deleted / updated this exact version ourselves earlier in this
        // txn? `DeletedByOwn` means an own delete at a prior command — there
        // is nothing left to mutate. Own in-place updates keep the row
        // `Visible`, so they fall through to Apply.
        if matches!(
            is_visible(&tuple.header, &self.snapshot, &*self.oracle),
            Visibility::DeletedByOwn
        ) {
            return Ok(EpqDecision::Skip);
        }

        // First-updater-wins: a committed foreign writer modified the exact
        // version we read.
        if self.foreign_xmax_committed(&tuple.header) {
            return Err(ExecError::SerializationFailure(
                "could not serialize access due to concurrent update".to_owned(),
            ));
        }

        // The row is still ours to update. Decode the row our snapshot sees.
        let row = self
            .codec
            .decode(&tuple.data)
            .map_err(|e| ExecError::TypeMismatch(format!("EvalPlanQual decode: {e}")))?;
        Ok(EpqDecision::Apply {
            tid,
            latest_row: row,
        })
    }

    /// `true` iff `header.xmax` is a *foreign* (not our own) transaction
    /// that has committed — i.e. a concurrent committed update/delete.
    fn foreign_xmax_committed(&self, header: &TupleHeader) -> bool {
        if header.xmax.is_invalid() {
            return false;
        }
        if self.snapshot.is_current_xid(header.xmax) {
            return false;
        }
        matches!(
            self.oracle.status(header.xmax),
            XidStatus::Committed | XidStatus::Frozen
        )
    }

    /// Follow the update chain from `tid` to the latest version that is
    /// visible under `snapshot`, returning its `(TupleId, decoded row)`, or
    /// `None` if the latest version is a committed delete (gone).
    fn fetch_latest_visible(
        &self,
        tid: TupleId,
        snapshot: &Snapshot,
    ) -> Result<Option<(TupleId, Vec<Value>)>, ExecError> {
        let mut current = tid;
        for _ in 0..64 {
            let tuple = (self.fetch)(current)?;
            match is_visible(&tuple.header, snapshot, &*self.oracle) {
                Visibility::Visible | Visibility::VisiblePreImage => {
                    // Visible terminal version is the latest committed row
                    // to mutate. `VisiblePreImage` reaching a fresh snapshot
                    // means the chain still redirects (an out-of-place
                    // update in progress) — follow it; otherwise the current
                    // bytes are the row.
                    if let Some(next) = updated_ctid_target(&tuple.header, current) {
                        current = next;
                        continue;
                    }
                    let row = self.codec.decode(&tuple.data).map_err(|e| {
                        ExecError::TypeMismatch(format!("EvalPlanQual decode: {e}"))
                    })?;
                    return Ok(Some((current, row)));
                }
                Visibility::Invisible | Visibility::DeletedByOwn => {
                    // Either a committed delete (gone) or an out-of-place
                    // update redirecting to a newer version. Follow the
                    // chain when it redirects; otherwise the latest version
                    // is gone.
                    if let Some(next) = updated_ctid_target(&tuple.header, current) {
                        current = next;
                        continue;
                    }
                    return Ok(None);
                }
            }
        }
        Err(ExecError::Internal(
            "EvalPlanQual update chain exceeded 64 hops",
        ))
    }

    /// Re-evaluate the UPDATE/DELETE WHERE predicate against the latest
    /// relation row. No predicate (no WHERE clause) means every row matches.
    fn row_matches_predicate(&self, row: &[Value]) -> Result<bool, ExecError> {
        let Some(predicate) = &self.predicate else {
            return Ok(true);
        };
        match predicate.eval(row).map_err(eval_error_to_exec_error)? {
            Value::Bool(true) => Ok(true),
            Value::Bool(false) | Value::Null => Ok(false),
            other => Err(ExecError::TypeMismatch(format!(
                "EvalPlanQual WHERE re-check returned {:?}, expected Bool",
                other.data_type()
            ))),
        }
    }
}

/// Build the boxed heap-fetch closure used by [`EvalPlanQual`]. Keeps the
/// EvalPlanQual struct free of the `L: PageLoader` type parameter so a
/// single `Option<EvalPlanQual>` field works for every `ModifyTable<L>`.
#[must_use]
pub fn make_epq_fetch<L: PageLoader + Send + Sync + 'static>(
    heap: Arc<HeapAccess<L>>,
) -> Arc<EpqFetchFn> {
    Arc::new(move |tid: TupleId| {
        heap.fetch(tid)
            .map_err(|e| ExecError::TypeMismatch(format!("EvalPlanQual fetch: {e}")))
    })
}
