//! See `crate::heap` for the public API.
//!
//! VACUUM-related entry points for the heap access method. Part of
//! the `heap` module split — each `impl<L: PageLoader> HeapAccess<L>`
//! block here adds methods to the type defined in `heap/mod.rs`.

use ultrasql_core::{BlockNumber, PageId, RelationId, Xid};
use ultrasql_mvcc::{TupleHeader, XidStatusOracle};
use ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE;

use crate::buffer_pool::PageLoader;

use super::{HeapAccess, HeapError};

/// Statistics returned by [`HeapAccess::vacuum_heap`].
#[derive(Clone, Copy, Debug, Default)]
pub struct VacuumStats {
    /// Number of pages on which at least one dead slot was reclaimed.
    pub pages_compacted: u32,
    /// Total dead tuple slots reclaimed across all compacted pages.
    pub tuples_reclaimed: u32,
}

impl<L: PageLoader> HeapAccess<L> {
    /// Reclaim dead tuple slots on every page of `rel`.
    ///
    /// A slot is eligible when:
    /// 1. Its `ItemId` flags are `Normal` (not already dead/unused).
    /// 2. Its `xmax` field is non-zero (the tuple has been deleted or
    ///    superseded by an UPDATE).
    /// 3. `xmax < oldest_active_xid` — the deleter cannot be in-progress.
    /// 4. `oracle.is_committed(xmax)` — the deletion actually committed
    ///    (as opposed to rolled back).
    ///
    /// When any slot on a page meets all four conditions, the heap:
    /// 1. Acquires a write lock on the page.
    /// 2. Calls `Page::delete_tuple` (marks `ItemId` as `Dead`).
    /// 3. Calls `Page::compact` (shifts live tuples to the high end,
    ///    resets dead `ItemId`s to `Unused`).
    ///
    /// Index entries pointing at the now-dead TIDs must be cleaned
    /// separately via `BTree::vacuum`; this function handles only the
    /// heap side.
    ///
    /// The caller supplies `oldest_active_xid` — the lowest XID still
    /// in-progress, as reported by
    /// `ultrasql_txn::TransactionManager::oldest_in_progress`. Every
    /// XID below that threshold has either committed or aborted; those
    /// that committed are identified by `oracle.is_committed`.
    pub fn vacuum_heap<O>(
        &self,
        rel: RelationId,
        oldest_active_xid: Xid,
        oracle: &O,
    ) -> Result<VacuumStats, HeapError>
    where
        O: XidStatusOracle + ?Sized,
    {
        let block_count = self.block_count(rel);
        let mut stats = VacuumStats::default();

        for block in 0..block_count {
            let page_id = PageId::new(rel, BlockNumber::new(block));
            let guard = self.pool.get_page(page_id)?;

            // Read pass: collect slots eligible for reclamation.
            // We decode only xmax (bytes 8..16) from the raw page to
            // avoid paying a full TupleHeader::decode per slot.
            let dead_slots: Vec<u16> = {
                let page = guard.read();
                let page_bytes = page.as_bytes();
                let slot_count = page.header().slot_count();
                let mut dead = Vec::new();

                for slot in 0..slot_count {
                    let item_id_off = crate::page::PAGE_HEADER_SIZE
                        + usize::from(slot) * crate::page::ITEMID_SIZE;
                    let raw = u32::from_le_bytes([
                        page_bytes[item_id_off],
                        page_bytes[item_id_off + 1],
                        page_bytes[item_id_off + 2],
                        page_bytes[item_id_off + 3],
                    ]);
                    // ItemIdFlags::Normal == 1; skip Unused / Dead / Redirect.
                    if raw & 0b11 != 1 {
                        continue;
                    }
                    let length = ((raw >> 2) & 0x7FFF) as usize;
                    let offset = ((raw >> 17) & 0x7FFF) as usize;
                    if length < TUPLE_HEADER_SIZE {
                        continue;
                    }
                    let Some(end) = offset.checked_add(length) else {
                        continue;
                    };
                    if end > page_bytes.len() {
                        continue;
                    }
                    let slot_bytes = &page_bytes[offset..end];
                    // xmax lives at bytes 8..16 in the tuple header.
                    let xmax_raw =
                        u64::from_le_bytes(slot_bytes[8..16].try_into().expect("8-byte slice"));
                    if xmax_raw == 0 {
                        // Alive tuple — no xmax.
                        continue;
                    }
                    let xmax = Xid::new(xmax_raw);
                    if xmax >= oldest_active_xid {
                        // Deleter might still be in-progress.
                        continue;
                    }
                    if !oracle.is_committed(xmax) {
                        // Deleter aborted — tuple is visible again via xmax rollback.
                        // Leave it; a future VACUUM pass after abort-cleanup may
                        // handle it, or the heap will simply grow until then.
                        continue;
                    }

                    // Full header decode to confirm the tuple is actually
                    // dead (xmax set and committed). We already confirmed
                    // xmax < oldest_active_xid and committed above; this
                    // decode also handles the is_alive() check for the
                    // infomask bits that mark HOT chains.
                    let Some((hdr, _)) =
                        TupleHeader::decode(&slot_bytes[..TUPLE_HEADER_SIZE])
                    else {
                        // Malformed header — skip conservatively.
                        continue;
                    };
                    if !hdr.is_alive() {
                        dead.push(slot);
                    }
                }
                dead
            };

            if dead_slots.is_empty() {
                continue;
            }

            // Write pass: mark each dead slot and compact.
            {
                let mut page = guard.write();
                for slot in &dead_slots {
                    // delete_tuple errors only if the slot is already dead/out-of-range.
                    // Both are benign (concurrent vacuum or shrinkage); ignore.
                    let _ = page.delete_tuple(*slot);
                }
                page.compact();
            }

            let reclaimed = u32::try_from(dead_slots.len()).unwrap_or(u32::MAX);
            stats.tuples_reclaimed = stats.tuples_reclaimed.saturating_add(reclaimed);
            stats.pages_compacted = stats.pages_compacted.saturating_add(1);
        }

        Ok(stats)
    }

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
