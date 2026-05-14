//! See `crate::heap` for the public API.
//!
//! VACUUM-related entry points for the heap access method. Part of
//! the `heap` module split — each `impl<L: PageLoader> HeapAccess<L>`
//! block here adds methods to the type defined in `heap/mod.rs`.

use ultrasql_core::{RelationId, Xid};

use crate::buffer_pool::PageLoader;

use super::{HeapAccess, HeapError};

impl<L: PageLoader> HeapAccess<L> {
    /// Drop every per-relation undo-log entry whose `writer_xid` is
    /// strictly less than `oldest_active_xid`.
    ///
    /// `oldest_active_xid` is the smallest XID still in progress, as
    /// reported by `ultrasql_txn::TransactionManager::oldest_in_progress`.
    /// When the heap's [`super::UndoRelationLog`] holds an entry whose
    /// writer XID is below that threshold, the writer must already
    /// have committed or aborted: aborted xids had their entries
    /// drained by [`Self::rollback_in_place_updates`] at abort time,
    /// so any remaining entry is from a committed writer whose
    /// updates are now visible to every live snapshot. The pre-image
    /// is dead weight.
    ///
    /// Returns the number of entries trimmed across every relation.
    /// Walks each per-relation log under its own write lock; concurrent
    /// readers via `super::HeapAccess::for_each_visible_with_undo`
    /// see the trimmed log on their next lock acquire and fall back
    /// to the on-page bytes (which already reflect the post-image
    /// from the committed writer), matching the visibility predicate's
    /// "missing undo entry → invisible" branch.
    pub fn vacuum_undo_log(&self, oldest_active_xid: Xid) -> Result<usize, HeapError> {
        let mut total_trimmed: usize = 0;
        let rels: Vec<RelationId> = self.undo_log.iter().map(|e| *e.key()).collect();
        for rel in rels {
            let Some(log_handle) = self.undo_log.get(&rel) else {
                continue;
            };
            let mut log = log_handle.write();
            if log.entries.is_empty() {
                continue;
            }
            // `retain` walks once, `O(n)`, dropping entries whose
            // writer is older than the oldest live snapshot. The
            // log's monotonic-tid invariant is preserved because the
            // retained entries keep their relative order.
            let before = log.entries.len();
            log.entries.retain(|e| e.writer_xid >= oldest_active_xid);
            let after = log.entries.len();
            total_trimmed += before - after;
        }
        Ok(total_trimmed)
    }

    /// Convenience accessor: number of undo-log entries currently
    /// retained for `rel`, or `0` if the relation has no log.
    ///
    /// Exposed for tests and observability; callers that want the
    /// total across every relation should sum this over the keys of
    /// [`HeapAccess::undo_log`].
    #[must_use]
    pub fn undo_log_len(&self, rel: RelationId) -> usize {
        self.undo_log
            .get(&rel)
            .map_or(0, |h| h.read().entries.len())
    }
}
