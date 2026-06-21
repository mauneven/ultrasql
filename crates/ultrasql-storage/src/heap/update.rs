//! See `crate::heap` for the public API.
//!
//! Part of the `heap` module split — each `impl<L: PageLoader>
//! HeapAccess<L>` block here adds methods to the type defined in
//! `heap/mod.rs`. Splitting across files keeps each unit under the
//! 600-line ceiling without changing semantics.

use ultrasql_core::{RelationId, TupleId};
use ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE;
use ultrasql_mvcc::{Snapshot, XidStatusOracle};

use crate::buffer_pool::PageLoader;

use super::scan::{HeapScan, VisibleHeapScan};
use super::{
    HeapAccess, HeapError, InsertOptions, UpdateOptions, UpdateOutcome, UpdatePayload,
    checked_heap_count_add,
};

impl<L: PageLoader> HeapAccess<L> {
    /// Replace a tuple's payload with HOT-chain support.
    ///
    /// **Algorithm:**
    ///
    /// 1. Pin the page holding `old_tid` exclusively.
    /// 2. Reject if the slot's current header has `xmax != INVALID`
    ///    (i.e. the tuple is not alive): returns
    ///    [`HeapError::MalformedHeader`].
    /// 3. **HOT path** (`opts.hot_eligible == true` and the page has
    ///    room): allocate a new slot on the *same* page for the new
    ///    version, set the new tuple's `infomask = HOT_UPDATED | UPDATED`
    ///    and `xmin = opts.xid`, then patch the old tuple's header in
    ///    place: `xmax = opts.xid`, `cmax = opts.command_id`,
    ///    `infomask |= HOT_UPDATED`, `ctid = new_tid`.  Returns
    ///    `UpdateOutcome { hot: true }`.
    /// 4. **Non-HOT path**: insert the new version on any page with room
    ///    (may grow the relation), then stamp the old tuple's `xmax`,
    ///    `cmax`, `infomask |= UPDATED`, and `ctid = new_tid`.  Returns
    ///    `UpdateOutcome { hot: false }`.
    ///
    /// **Lock order (intended for future multi-page case):** the design
    /// intention is to pin the new-version page before the old-version
    /// page when they differ (lower block number first in a fully
    /// concurrent implementation).  The current implementation does not
    /// hold both page guards simultaneously — `insert` pins and releases
    /// the new page, then `stamp_updated_old` separately pins the old
    /// page — so the ordering is not yet structurally enforced.  This
    /// comment tracks the intended invariant for the concurrent path.
    pub fn update(
        &self,
        old_tid: TupleId,
        new_payload: &[u8],
        opts: UpdateOptions<'_>,
    ) -> Result<UpdateOutcome, HeapError> {
        let new_tuple_size = TUPLE_HEADER_SIZE
            .checked_add(new_payload.len())
            .ok_or(HeapError::MalformedHeader("tuple size overflow"))?;
        // --- HOT path: try the same page first --------------------------------
        if opts.hot_eligible {
            // Encode the WAL payload BEFORE the page mutation. An encode
            // failure cleanly aborts without touching the page.
            //
            // We pass a placeholder outcome here; if the page has no room we
            // fall through to non-HOT and discard the pre-encoded bytes.
            // The WAL record is fully formed only after we know new_tid.
            //
            // Emit FPW before the HOT-update mutation if WAL is present.
            if let Some(sink) = opts.wal {
                Self::maybe_emit_fpw(
                    &self.pool,
                    old_tid.page,
                    sink,
                    &self.last_checkpoint_lsn,
                    opts.xid,
                )?;
            }
            let hot_tid: Option<TupleId> = {
                let guard = self.get_page_relieved(old_tid.page)?;
                let result =
                    Self::try_hot_update(&guard, old_tid, new_payload, opts, new_tuple_size)?;
                // guard drops here — pin released before WAL I/O
                result
            };
            if let Some(new_tid) = hot_tid {
                let outcome = UpdateOutcome {
                    old_tid,
                    new_tid,
                    hot: true,
                };
                self.remember_rollback_stamp_page(opts.xid, old_tid.page);
                // WAL append is outside any pin scope.
                Self::emit_update_wal(&self.pool, outcome, &opts, || self.fetch(new_tid))?;
                // HOT update: both versions on the same page; clear VM once.
                if let Some(vm) = opts.vm {
                    vm.clear(new_tid.page.relation, new_tid.page.block);
                }
                self.invalidate_int32_pair_payload_stats_relation(old_tid.page.relation);
                self.column_cache.bump_version(old_tid.page.relation);
                return Ok(outcome);
            }
            // Page had no room; fall through to non-HOT path.
        }

        // --- Non-HOT path: insert on any page, then stamp old -----------------
        //
        // Build insert options from the update's xid/cid.  Pass wal: None
        // here because the outer update path emits its own HeapUpdate record
        // that covers both old and new positions; we do not want a
        // spurious HeapInsert record for the internal insert.
        let n_atts = self.fetch(old_tid)?.header.n_atts;
        let insert_opts = InsertOptions {
            xmin: opts.xid,
            command_id: opts.command_id,
            n_atts,
            wal: None,
            fsm: None,
            vm: None,
        };
        let new_tid = self.insert(old_tid.page.relation, new_payload, insert_opts)?;

        // Emit FPW for the old page before stamping it. The new page FPW
        // would be emitted by the internal insert's WAL path, but since
        // insert_opts.wal is None the caller is responsible for covering the
        // new page via emit_update_wal. The HeapUpdate record already carries
        // the new tuple bytes so recovery can redo both pages.
        if let Some(sink) = opts.wal {
            Self::maybe_emit_fpw(
                &self.pool,
                old_tid.page,
                sink,
                &self.last_checkpoint_lsn,
                opts.xid,
            )?;
        }

        // Stamp the old tuple with xmax and redirect ctid. Pin the page,
        // apply the stamp, then drop the guard before WAL I/O.
        {
            let old_guard = self.get_page_relieved(old_tid.page)?;
            Self::stamp_updated_old(&old_guard, old_tid, new_tid, opts)?;
            self.remember_rollback_stamp_page(opts.xid, old_tid.page);
            // old_guard drops here — pin released before WAL append
        }

        let outcome = UpdateOutcome {
            old_tid,
            new_tid,
            hot: false,
        };
        // WAL append is outside any pin scope.
        Self::emit_update_wal(&self.pool, outcome, &opts, || self.fetch(new_tid))?;
        // Non-HOT: old and new may be on different pages — clear VM on both.
        if let Some(vm) = opts.vm {
            vm.clear(old_tid.page.relation, old_tid.page.block);
            if old_tid.page != new_tid.page {
                vm.clear(new_tid.page.relation, new_tid.page.block);
            }
        }
        self.invalidate_int32_pair_payload_stats_relation(old_tid.page.relation);
        self.column_cache.bump_version(old_tid.page.relation);
        Ok(outcome)
    }

    /// Bulk-UPDATE the tuples in `edits`, grouped by page so each
    /// affected page is pinned **at most once** for the HOT batch.
    ///
    /// `edits` is an iterator of `(old_tid, new_payload)` pairs.
    /// `update_many` groups them by `old_tid.page` and attempts a
    /// `try_hot_update` for every targeted slot under a single
    /// [`crate::buffer_pool::PageGuard`] per page. Entries whose HOT path returns
    /// `None` (the page lacks room for the new version) fall back
    /// to the per-tuple non-HOT path via [`Self::update`].
    ///
    /// Semantics are equivalent to invoking [`Self::update`] N
    /// times in order, except:
    ///
    /// - The per-page `BufferPool::get_page` (one `DashMap` shard
    ///   probe + one atomic pin/unpin) is paid **once** rather than
    ///   N times.
    /// - The `try_hot_update` internal `guard.write()` is acquired
    ///   uncontended for every entry on the page (the prior call
    ///   has already released).
    /// - WAL emission still happens once per row inside
    ///   `Self::emit_update_wal`; when `opts.wal` is `None` (the
    ///   bulk DML executor path on the `cross_compare_sql` bench)
    ///   `emit_update_wal` is a no-op.
    ///
    /// Returns the count of successfully-updated tuples.
    ///
    /// # Errors
    ///
    /// Mirror [`Self::update`]: [`HeapError::BufferPool`] on pin
    /// failure, [`HeapError::Page`] / [`HeapError::MalformedHeader`]
    /// on slot decode failure, [`HeapError::WalPayload`] on encode
    /// failure (encode happens before the page is mutated).
    ///
    /// # Concurrency
    ///
    /// At most one [`crate::buffer_pool::PageGuard`] is held at any instant for the HOT
    /// batch on a given page. The guard is dropped before WAL I/O.
    /// The non-HOT fallback re-enters [`Self::update`] for each
    /// affected tuple — same locking discipline as the single-tuple
    /// path.
    pub fn update_many<I>(&self, edits: I, opts: UpdateOptions<'_>) -> Result<usize, HeapError>
    where
        I: IntoIterator<Item = (TupleId, UpdatePayload)>,
    {
        let (count, _) = self.update_many_inner(edits, opts, false)?;
        Ok(count)
    }

    /// Bulk-UPDATE and return each `(old_tid, new_tid)` outcome.
    ///
    /// This is the same physical update path as [`Self::update_many`],
    /// but callers that maintain secondary indexes need the new TID
    /// for each old tuple after the heap write succeeds.
    pub fn update_many_with_outcomes<I>(
        &self,
        edits: I,
        opts: UpdateOptions<'_>,
    ) -> Result<Vec<UpdateOutcome>, HeapError>
    where
        I: IntoIterator<Item = (TupleId, UpdatePayload)>,
    {
        let (_, outcomes) = self.update_many_inner(edits, opts, true)?;
        Ok(outcomes)
    }

    fn update_many_inner<I>(
        &self,
        edits: I,
        opts: UpdateOptions<'_>,
        collect_update_outcomes: bool,
    ) -> Result<(usize, Vec<UpdateOutcome>), HeapError>
    where
        I: IntoIterator<Item = (TupleId, UpdatePayload)>,
    {
        // Materialise the edits up front. The previous implementation
        // built an `AHashMap<PageId, Vec<(slot, payload)>>` to group
        // entries by source page; the hash inserts plus per-page Vec
        // growth amortised to ~80 µs on a 10 000-row bulk UPDATE.
        //
        // The bulk-UPDATE caller (`ModifyTable`) consumes batches
        // emitted by a sequential `SeqScan`, which yields tuples in
        // `PageId` order. So the materialised Vec is *already* sorted
        // by page; one sort pass + a linear group-by-run walk replaces
        // the HashMap entirely.
        let mut edits_vec: Vec<(TupleId, UpdatePayload)> = edits.into_iter().collect();
        if edits_vec.is_empty() {
            return Ok((0, Vec::new()));
        }
        // Caller contract: `edits` arrives in `(page, slot)` ascending
        // order. The two in-tree callers
        // ([`ultrasql_executor::ModifyTable`] and the fused
        // `FusedUpdateInt32Add` operator) both source their TIDs from
        // a sequential heap walker that already yields tuples in
        // block-major + slot-major order, so they meet the contract
        // without an explicit sort. The previous implementation did a
        // defensive `sort_unstable_by` on every call; that pass runs
        // in O(n) on sorted input but still costs ~5-10 µs per
        // 10 000-row UPDATE (mostly the comparator-call setup). A
        // `debug_assert` documents the contract so a regression in a
        // caller's input order trips a test run rather than silently
        // producing per-page write storms.
        debug_assert!(
            edits_vec
                .windows(2)
                .all(|w| (w[0].0.page, w[0].0.slot) <= (w[1].0.page, w[1].0.slot)),
            "update_many requires edits sorted by (PageId, slot)",
        );

        let mut total = 0_usize;
        let mut outcomes: Vec<UpdateOutcome> = if collect_update_outcomes {
            Vec::with_capacity(edits_vec.len())
        } else {
            Vec::new()
        };
        // HOT-failed entries fall back to the bulk-non-HOT path. We
        // build the fallback Vec by appending in the same page-major
        // order we walk; `fallback` is therefore implicitly sorted by
        // `old_tid.page`, which the post-`insert_batch` stamp loop
        // exploits with another run-length walk.
        let mut fallback: Vec<(TupleId, UpdatePayload)> = Vec::new();
        // Single column-cache invalidation per call rather than per
        // page-run. The cache is keyed by relation; one bump after
        // every HOT outcome on the relation is equivalent to bumping
        // once per page-run for the same relation.
        let mut hot_touched_relation: Option<RelationId> = None;

        // Linear walk: identify each maximal run of entries sharing a
        // `PageId` and process them under a single per-page guard.
        let mut i = 0;
        while i < edits_vec.len() {
            let page_id = edits_vec[i].0.page;
            let mut j = i + 1;
            while j < edits_vec.len() && edits_vec[j].0.page == page_id {
                j += 1;
            }
            let group_len = j - i;

            if opts.hot_eligible {
                // FPW once per page before any HOT mutation on this page.
                if let Some(sink) = opts.wal {
                    Self::maybe_emit_fpw(
                        &self.pool,
                        page_id,
                        sink,
                        &self.last_checkpoint_lsn,
                        opts.xid,
                    )?;
                }

                // Single pin + single write lock for the entire
                // page-run. Each row in the run is dispatched through
                // [`Self::try_hot_update_inplace`] which writes directly
                // into the held `PageWrite` — eliminating the per-row
                // `parking_lot::RwLock::write` acquire/release the
                // legacy `try_hot_update` paid (~30-50 ns × N rows
                // uncontended), and the duplicate `get_page` the
                // free-space precheck used to need.
                //
                // When `opts.wal.is_none() && opts.vm.is_none()` the
                // post-write outcome loop has nothing to do per tuple
                // beyond incrementing `total`; we skip building
                // `hot_outcomes` entirely on that path (the bulk-DML
                // executor exercises this branch).
                let collect_hot_outcomes =
                    collect_update_outcomes || opts.wal.is_some() || opts.vm.is_some();
                let mut hot_outcomes: Vec<(TupleId, TupleId)> = if collect_hot_outcomes {
                    Vec::with_capacity(group_len)
                } else {
                    Vec::new()
                };
                let mut hot_count: usize = 0;
                let mut scratch: Vec<u8> = Vec::with_capacity(64);
                {
                    let guard = self.get_page_relieved(page_id)?;
                    let mut page = guard.write();
                    for k in i..j {
                        let new_tuple_size = TUPLE_HEADER_SIZE
                            .checked_add(edits_vec[k].1.len())
                            .ok_or(HeapError::MalformedHeader("tuple size overflow"))?;
                        let old_tid = edits_vec[k].0;
                        match Self::try_hot_update_inplace(
                            &mut page,
                            old_tid,
                            &edits_vec[k].1,
                            opts,
                            new_tuple_size,
                            &mut scratch,
                        )? {
                            Some(new_tid) => {
                                if collect_hot_outcomes {
                                    hot_outcomes.push((old_tid, new_tid));
                                }
                                hot_count += 1;
                            }
                            None => {
                                let (tid, payload) = std::mem::replace(
                                    &mut edits_vec[k],
                                    (TupleId::new(page_id, 0), UpdatePayload::new()),
                                );
                                fallback.push((tid, payload));
                            }
                        }
                    }
                    // page + guard drop here — pin and write lock
                    // released before WAL I/O.
                }

                // Per-HOT-success: emit WAL, clear VM. When
                // `opts.wal` is `None` this is a no-op (the bulk DML
                // path on cross_compare_sql).
                let mut had_hot_outcome = hot_count > 0;
                total = checked_heap_count_add(total, hot_count, "updated tuple count overflow")?;
                for (old_tid, new_tid) in hot_outcomes {
                    let outcome = UpdateOutcome {
                        old_tid,
                        new_tid,
                        hot: true,
                    };
                    Self::emit_update_wal(&self.pool, outcome, &opts, || self.fetch(new_tid))?;
                    if let Some(vm) = opts.vm {
                        vm.clear(new_tid.page.relation, new_tid.page.block);
                    }
                    if collect_update_outcomes {
                        outcomes.push(outcome);
                    }
                    had_hot_outcome = true;
                }
                if had_hot_outcome {
                    self.remember_rollback_stamp_page(opts.xid, page_id);
                    hot_touched_relation = Some(page_id.relation);
                }
            } else {
                // Caller disabled HOT for every entry in the run —
                // funnel them straight to the non-HOT fallback.
                fallback.reserve(group_len);
                for k in i..j {
                    let (tid, payload) = std::mem::replace(
                        &mut edits_vec[k],
                        (TupleId::new(page_id, 0), UpdatePayload::new()),
                    );
                    fallback.push((tid, payload));
                }
            }

            i = j;
        }

        // One column-cache bump per relation, covering every HOT
        // outcome across every page-run.
        if let Some(rel) = hot_touched_relation {
            self.invalidate_int32_pair_payload_stats_relation(rel);
            self.column_cache.bump_version(rel);
        }

        // Non-HOT fallback — bulk path.
        //
        // Every entry on `fallback` is here because the page-bulk
        // HOT loop already proved `try_hot_update` returns `None`
        // for the source page. Looping `Self::update` per entry
        // would pay two `BufferPool::get_page` pins per tuple
        // (one inside `Self::insert`'s linear scan, one inside
        // `stamp_updated_old`). For 10 000 fallback rows that is
        // 20 000 pin operations.
        //
        // The bulk path runs in two phases:
        //
        //   Phase 1 — `Self::insert_batch` bulk-writes every new
        //   tuple version. Pages are pinned at most once each;
        //   `insert_batch` walks slot-by-slot under one write
        //   guard per destination page.
        //
        //   Phase 2 — group `(old_tid, new_tid)` pairs by
        //   `old_tid.page` and `stamp_updated_old` for every
        //   entry under one `PageWrite` per source page.
        //
        // WAL is force-disabled for the inner insert path so the
        // logical UPDATE record (emitted by callers wiring up WAL
        // sinks) is not duplicated; the bench path passes
        // `opts.wal == None` so this is moot in practice today.
        if !fallback.is_empty() {
            let payloads: Vec<&[u8]> = fallback.iter().map(|(_, p)| p.as_slice()).collect();
            let insert_opts = InsertOptions {
                xmin: opts.xid,
                command_id: opts.command_id,
                n_atts: self.fetch(fallback[0].0)?.header.n_atts,
                wal: None,
                fsm: None,
                vm: None,
            };
            let rel = fallback[0].0.page.relation;
            let new_tids = self.insert_batch(rel, &payloads, insert_opts)?;

            // `fallback` was built by appending in old-page order
            // during the page-run walk above, so the zip pairs
            // `(old_tid, new_tid)` are already grouped by
            // `old_tid.page`. A linear run-length walk replaces the
            // former `by_old_page` HashMap: every page-group is
            // processed under one `PageWrite`, exactly as before, but
            // without the per-row hash + Vec-grow overhead the
            // HashMap accumulator paid.
            debug_assert_eq!(fallback.len(), new_tids.len());
            let mut k = 0;
            while k < fallback.len() {
                let page_id = fallback[k].0.page;
                let mut m = k + 1;
                while m < fallback.len() && fallback[m].0.page == page_id {
                    m += 1;
                }
                // [k, m) is one source-page run.
                let guard = self.get_page_relieved(page_id)?;
                let mut page = guard.write();
                let page_bytes = page.as_bytes_mut();
                for idx in k..m {
                    Self::stamp_updated_old_inline(
                        page_bytes,
                        fallback[idx].0.slot,
                        new_tids[idx],
                        opts.xid,
                        opts.command_id,
                    )?;
                }
                drop(page);
                drop(guard);
                self.remember_rollback_stamp_page(opts.xid, page_id);
                if let Some(vm) = opts.vm {
                    vm.clear(page_id.relation, page_id.block);
                }
                k = m;
            }
            if collect_update_outcomes {
                outcomes.extend(fallback.iter().zip(new_tids.iter()).map(
                    |((old_tid, _), new_tid)| UpdateOutcome {
                        old_tid: *old_tid,
                        new_tid: *new_tid,
                        hot: false,
                    },
                ));
            }
            total = checked_heap_count_add(total, fallback.len(), "updated tuple count overflow")?;
            self.invalidate_int32_pair_payload_stats_relation(rel);
            self.column_cache.bump_version(rel);
        }

        Ok((total, outcomes))
    }

    /// Sequential scan over `rel`'s pages. The first version starts at
    /// block 0 and walks to `block_count - 1`. Pages are pinned one at
    /// a time; the iterator owns no concurrent state.
    ///
    /// The iterator yields every *normal* slot. `Unused`, `Dead`, and
    /// `Redirect` slots are skipped; a future revision will accept a
    /// snapshot + oracle and apply visibility inline.
    pub const fn scan(&self, rel: RelationId, block_count: u32) -> HeapScan<'_, L> {
        HeapScan {
            pool: &self.pool,
            rel,
            block_count,
            current_block: 0,
            current_slot: 0,
            slot_cap: 0,
            current_guard: None,
        }
    }

    /// Sequential scan with MVCC visibility applied inline.
    ///
    /// Tuples invisible to `snapshot` under `oracle` are silently
    /// skipped — the caller never sees them. This replaces the bare
    /// [`Self::scan`] for executor code that holds a snapshot; the
    /// original `scan` is kept for tools that genuinely want every
    /// slot regardless of visibility.
    pub const fn scan_visible<'a, O: XidStatusOracle + ?Sized>(
        &'a self,
        rel: RelationId,
        block_count: u32,
        snapshot: &'a Snapshot,
        oracle: &'a O,
    ) -> VisibleHeapScan<'a, L, O> {
        VisibleHeapScan {
            inner: self.scan(rel, block_count),
            undo_log: &self.undo_log,
            snapshot,
            oracle,
            xmin_cache: None,
        }
    }
}
