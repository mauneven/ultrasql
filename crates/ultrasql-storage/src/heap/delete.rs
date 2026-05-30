//! See `crate::heap` for the public API.
//!
//! Part of the `heap` module split — each `impl<L: PageLoader>
//! HeapAccess<L>` block here adds methods to the type defined in
//! `heap/mod.rs`. Splitting across files keeps each unit under the
//! 600-line ceiling without changing semantics.

use std::sync::atomic::Ordering;

use ultrasql_core::{BlockNumber, CommandId, Lsn, PageId, RelationId, TupleId, Xid};
use ultrasql_mvcc::tuple_header::{InfoMask, TUPLE_HEADER_SIZE};
use ultrasql_mvcc::{Snapshot, TupleHeader, Visibility, XidStatusOracle, is_visible};
use ultrasql_wal::WalRecord;
use ultrasql_wal::payload::HeapDeletePayload;
use ultrasql_wal::record::RecordType;

use crate::buffer_pool::{PageGuard, PageLoader};
use crate::wal_sink::WalSink;

use super::{DeleteOptions, HeapAccess, HeapError};

#[inline]
fn read_le_u16(bytes: &[u8], start: usize, error: &'static str) -> Result<u16, HeapError> {
    let end = start
        .checked_add(2)
        .ok_or(HeapError::MalformedHeader(error))?;
    let slice = bytes
        .get(start..end)
        .ok_or(HeapError::MalformedHeader(error))?;
    Ok(u16::from_le_bytes([slice[0], slice[1]]))
}

#[inline]
fn read_le_u64(bytes: &[u8], start: usize, error: &'static str) -> Result<u64, HeapError> {
    let end = start
        .checked_add(8)
        .ok_or(HeapError::MalformedHeader(error))?;
    let slice = bytes
        .get(start..end)
        .ok_or(HeapError::MalformedHeader(error))?;
    Ok(u64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
}

#[inline]
fn read_le_i32(bytes: &[u8], start: usize, error: &'static str) -> Result<i32, HeapError> {
    let end = start
        .checked_add(4)
        .ok_or(HeapError::MalformedHeader(error))?;
    let slice = bytes
        .get(start..end)
        .ok_or(HeapError::MalformedHeader(error))?;
    Ok(i32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

#[inline]
fn itemid_window(item_raw: u32) -> Result<(usize, usize), HeapError> {
    let length = u16::try_from((item_raw >> 2) & 0x7FFF)
        .map_err(|_| HeapError::MalformedHeader("item length overflow"))?;
    let offset = u16::try_from((item_raw >> 17) & 0x7FFF)
        .map_err(|_| HeapError::MalformedHeader("item offset overflow"))?;
    Ok((usize::from(length), usize::from(offset)))
}

impl<L: PageLoader> HeapAccess<L> {
    /// Clear `xmax` stamps for an aborted transaction.
    ///
    /// Regular MVCC visibility can treat an aborted `xmax` as visible,
    /// but the heap update helpers must also see the slot as physically
    /// alive before stamping a new `xmax`. Abort cleanup therefore clears
    /// `xmax`/`cmax` for DELETE stamps and aborted classical UPDATE old
    /// versions. In-place UPDATEs are skipped here because their payload
    /// must be restored from the undo log before their header is cleared.
    pub(crate) fn rollback_delete_stamps(&self, xid: Xid) -> Result<usize, HeapError> {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE, PageHeader};

        let mut total_restored = 0_usize;
        let rels: Vec<RelationId> = self.block_counters.iter().map(|e| *e.key()).collect();
        for rel in rels {
            let Some(counter) = self.block_counters.get(&rel) else {
                continue;
            };
            let block_count = counter.load(Ordering::Acquire);
            drop(counter);

            let mut relation_restored = false;
            for block in 0..block_count {
                let page_id = PageId::new(rel, BlockNumber::new(block));
                let guard = self.pool.get_page(page_id)?;
                let mut page = guard.write();
                let bytes = page.as_bytes_mut();
                let slot_count = PageHeader::decode(bytes)
                    .map_err(HeapError::Page)?
                    .slot_count();

                for slot in 0..slot_count {
                    let item_id_off = PAGE_HEADER_SIZE + usize::from(slot) * ITEMID_SIZE;
                    let item_raw = u32::from_le_bytes([
                        bytes[item_id_off],
                        bytes[item_id_off + 1],
                        bytes[item_id_off + 2],
                        bytes[item_id_off + 3],
                    ]);
                    if item_raw & 0b11 != 1 {
                        continue;
                    }
                    let (length, offset) = itemid_window(item_raw)?;
                    if length < TUPLE_HEADER_SIZE
                        || offset.checked_add(length).is_none_or(|e| e > bytes.len())
                    {
                        return Err(HeapError::MalformedHeader("slot shorter than header"));
                    }
                    let (header, _) =
                        TupleHeader::decode(&bytes[offset..offset + TUPLE_HEADER_SIZE])
                            .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                    if header.xmax != xid || header.infomask.contains(InfoMask::UPDATED_IN_PLACE) {
                        continue;
                    }

                    let tid = TupleId::new(page_id, slot);
                    let mut restored = header;
                    restored.xmax = Xid::INVALID;
                    restored.cmax = CommandId::FIRST;
                    restored.ctid = tid;
                    restored.infomask.clear(
                        InfoMask::UPDATED
                            | InfoMask::HOT_UPDATED
                            | InfoMask::UPDATED_IN_PLACE
                            | InfoMask::XMAX_COMMITTED
                            | InfoMask::XMAX_INVALID,
                    );
                    let mut header_bytes = [0_u8; TUPLE_HEADER_SIZE];
                    restored.encode(&mut header_bytes);
                    bytes[offset..offset + TUPLE_HEADER_SIZE].copy_from_slice(&header_bytes);
                    relation_restored = true;
                    total_restored += 1;
                }
            }

            if relation_restored {
                self.column_cache.bump_version(rel);
            }
        }

        Ok(total_restored)
    }

    /// Mark a tuple deleted.
    ///
    /// The slot stays allocated and the payload is left untouched; only
    /// the header's `xmax`/`cmax` fields move. A later visibility check
    /// will hide the tuple from snapshots that observe `xmax` as committed.
    ///
    /// If `opts.wal` is `Some`, a `RecordType::HeapDelete` record is appended
    /// to the sink after the in-place stamp succeeds. The page guard is
    /// dropped before the WAL append so a future blocking WAL writer cannot
    /// starve buffer-pool eviction.
    ///
    /// Payload encoding runs before the page mutation so an encode failure
    /// short-circuits without touching the page. If encoding succeeds but
    /// the WAL append later fails the process panics: the page state has
    /// already diverged from the WAL and continuing would risk silent data
    /// loss.
    pub fn delete(&self, tid: TupleId, opts: DeleteOptions<'_>) -> Result<(), HeapError> {
        // Encode the WAL payload BEFORE the page mutation so that an encode
        // failure cleanly aborts without touching the page.
        let wal_record = if let Some(sink) = opts.wal {
            // Emit a full-page-write record if this is the first mutation of
            // the page since the last checkpoint. FPW must precede the mutation
            // record so recovery can restore the page before applying the delete.
            Self::maybe_emit_fpw(
                &self.pool,
                tid.page,
                sink,
                &self.last_checkpoint_lsn,
                opts.xmax,
            )?;
            let prev_lsn = sink.last_lsn_for(opts.xmax);
            let payload_bytes = HeapDeletePayload {
                tid,
                xmax: opts.xmax,
                cmax: opts.cmax,
            }
            .encode()?;
            let record = WalRecord::new(
                RecordType::HeapDelete,
                opts.xmax,
                prev_lsn,
                0,
                payload_bytes,
            )?;
            Some((sink, record))
        } else {
            None
        };

        // Mutate the page. Guard is dropped at the end of this block so
        // the pin is released before WAL I/O begins.
        {
            let guard = self.pool.get_page(tid.page)?;
            Self::delete_in_place(&guard, tid, opts.xmax, opts.cmax)?;
            // guard drops here — pin released before WAL append
        }

        // Append the WAL record outside the pin scope. If append returns
        // Err the page has already been mutated; the only safe response is
        // to panic and let the process restart from a consistent WAL state.
        if let Some((sink, record)) = wal_record {
            let lsn: Lsn = sink.append(record).expect(
                "wal append must succeed after a committed page mutation; failure is unrecoverable",
            );
            // Stamp the page LSN so recovery knows the on-page state was
            // logged at this LSN. WAL append completes before stamp so the
            // page LSN is never ahead of the WAL.
            Self::stamp_page_lsn(&self.pool, tid.page, lsn)?;
        }
        // Update FSM (optimistically record the dead tuple's space as free so
        // future inserters can find this block) and clear the VM all-visible
        // bit (the page now has a deleted tuple invisible to future snapshots).
        Self::post_delete_fsm_vm(&self.pool, tid.page, opts);
        // Invalidate the columnar projection cache for this
        // relation — a mutated row makes any cached `Vec<Column>`
        // stale until the next `SeqScan` re-builds it.
        self.column_cache.bump_version(tid.page.relation);
        Ok(())
    }

    /// Bulk-delete every tuple in `tids`, grouped by page so each
    /// affected page is pinned and write-locked **exactly once**.
    ///
    /// [`Self::delete`] pins, write-locks, mutates and releases a
    /// page on every row. For a bulk DELETE over `N` rows on `P`
    /// pages that is `N` `DashMap` shard probes + `N` pin/unpin
    /// pairs + `N` write-lock acquisitions when only `P` are
    /// strictly necessary. `delete_many` groups the input by
    /// `page_id`, takes **one** write guard per page, stamps every
    /// slot on that page under that single guard, then drops the
    /// guard before its WAL append / FSM hook batch.
    ///
    /// Semantics are equivalent to invoking [`Self::delete`] N
    /// times in order: each tuple's header is stamped with
    /// `opts.xmax` / `opts.cmax`; WAL emission, when configured,
    /// emits one `HeapDelete` record per stamped slot (the WAL
    /// applier replays them identically to `delete`); FSM hints
    /// and VM clears, when `opts.fsm` / `opts.vm` are configured,
    /// run **once per page touched** (record the final free-space
    /// after every delete on the page lands).
    ///
    /// Slots within a page are stamped in ascending slot order; the
    /// between-page order is the iteration order of the
    /// page-grouping `AHashMap`, which is non-deterministic. Per-
    /// tuple deletes have no ordering-dependent semantics so this is
    /// safe.
    ///
    /// Returns the number of slots successfully stamped.
    ///
    /// # Errors
    ///
    /// - [`HeapError::BufferPool`] on pin failure for any affected page.
    /// - [`HeapError::Page`] / [`HeapError::MalformedHeader`] on slot
    ///   decode failure.
    /// - [`HeapError::WalPayload`] on WAL encode failure (encode happens
    ///   before the page is mutated, so the page is left untouched).
    ///
    /// # Concurrency
    ///
    /// At most one [`PageGuard`] is held at any instant. The guard is
    /// dropped before WAL I/O begins, so a concurrent reader on
    /// another page is never blocked by this method's pin.
    pub fn delete_many<I>(&self, tids: I, opts: DeleteOptions<'_>) -> Result<usize, HeapError>
    where
        I: IntoIterator<Item = TupleId>,
    {
        // Group TIDs by page. `ahash::AHashMap` is the workspace
        // default hash table; `PageId` already hashes well.
        let mut by_page: ahash::AHashMap<PageId, Vec<u16>> = ahash::AHashMap::new();
        for tid in tids {
            by_page.entry(tid.page).or_default().push(tid.slot);
        }
        if by_page.is_empty() {
            return Ok(0);
        }

        let mut total = 0_usize;
        for (page_id, mut slots) in by_page {
            // Sort within a page so the slot directory is touched in
            // ascending order — keeps page cache lines hot.
            slots.sort_unstable();

            // Pre-encode WAL payloads BEFORE mutating the page so an
            // encode failure aborts cleanly (the contract `delete`
            // upholds for the single-tuple case).
            let wal_payloads: Option<Vec<Vec<u8>>> = if let Some(sink) = opts.wal {
                Self::maybe_emit_fpw(
                    &self.pool,
                    page_id,
                    sink,
                    &self.last_checkpoint_lsn,
                    opts.xmax,
                )?;
                let mut payloads = Vec::with_capacity(slots.len());
                for &slot in &slots {
                    let tid = TupleId::new(page_id, slot);
                    let bytes = HeapDeletePayload {
                        tid,
                        xmax: opts.xmax,
                        cmax: opts.cmax,
                    }
                    .encode()?;
                    payloads.push(bytes);
                }
                Some(payloads)
            } else {
                None
            };

            // Mutate every slot on this page under one write guard.
            {
                let guard = self.pool.get_page(page_id)?;
                for &slot in &slots {
                    let tid = TupleId::new(page_id, slot);
                    Self::delete_in_place(&guard, tid, opts.xmax, opts.cmax)?;
                }
                // guard drops here — pin released before WAL append.
            }

            // Append every per-tuple WAL record outside the pin scope.
            // The page LSN is stamped once at the final LSN of the
            // batch (recovery replays records in append order, so the
            // final per-slot stamp is the only state recovery needs).
            if let (Some(sink), Some(payloads)) = (opts.wal, wal_payloads) {
                let mut last_lsn: Lsn = Lsn::ZERO;
                for payload in payloads {
                    let prev_lsn = sink.last_lsn_for(opts.xmax);
                    let record =
                        WalRecord::new(RecordType::HeapDelete, opts.xmax, prev_lsn, 0, payload)?;
                    last_lsn = sink.append(record).expect(
                        "wal append must succeed after a committed page mutation; \
                         failure is unrecoverable",
                    );
                }
                Self::stamp_page_lsn(&self.pool, page_id, last_lsn)?;
            }

            // FSM/VM hooks fire once per page touched.
            Self::post_delete_fsm_vm(&self.pool, page_id, opts);
            // Column-cache invalidation: bump the relation's version
            // for every page we touch. The first bump invalidates the
            // entry; subsequent bumps just move the version forward.
            self.column_cache.bump_version(page_id.relation);
            total += slots.len();
        }
        Ok(total)
    }

    /// Single-pass MVCC-correct DELETE for the narrow
    /// `(Int32, Int32) [WHERE col_j cmp lit]` shape.
    ///
    /// Mirrors [`Self::update_int32_pair_inplace_undo`]: page-major
    /// traversal, one source-page write guard at a time, ItemId +
    /// minimal-visibility + payload decode inline. The slot's
    /// payload is unchanged (DELETE leaves the bytes intact and uses
    /// `xmax` to hide the tuple); only the header's `xmax / cmax /
    /// infomask` triple is stamped.
    ///
    /// What this saves versus `ModifyTable(Filter(SeqScan))` →
    /// `delete_many`:
    /// - No intermediate `Vec<TupleId>` of qualifying TIDs.
    /// - No per-page `AHashMap<PageId, Vec<u16>>` grouping pass.
    /// - One write-pin per source page; the prior plan paid one for
    ///   the scan's visibility walker and one for the stamp pass.
    ///
    /// # Concurrency
    ///
    /// Holds **one** write-exclusive page guard at a time.
    ///
    /// # Durability
    ///
    /// When `wal` is `Some`, per-row [`RecordType::HeapDeleteInPlace`]
    /// records are appended after the page guard is dropped and the
    /// page LSN is stamped with the last assigned LSN, mirroring the
    /// FPW + per-row + page-LSN pattern in
    /// [`Self::update_int32_pair_inplace_undo`]. A `None` value
    /// retains the non-durable benchmark path for the executor's
    /// fused operator (the pipeline lowerer threads the live sink in
    /// when present).
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn delete_int32_pair_inplace<O, P>(
        &self,
        rel: RelationId,
        block_count: u32,
        snapshot: &Snapshot,
        oracle: &O,
        predicate: P,
        xid: Xid,
        command_id: CommandId,
        wal: Option<&dyn WalSink>,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + ?Sized,
        P: Fn(i32, i32) -> bool,
    {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE};

        let mut total_deleted: usize = 0;
        let mut xmin_cache: Option<(Xid, u16, bool)> = None;

        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();

        // Per-page TID scratch: collect under the write guard, emit
        // WAL once the guard is dropped, same shape as the update
        // path. Reused across pages.
        let mut wal_scratch: Vec<TupleId> = if wal.is_some() {
            Vec::with_capacity(256)
        } else {
            Vec::new()
        };

        for src_block in 0..block_count {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));
            let mut page_deleted = false;

            // FPW: emit the canonical page image first if this is
            // the first mutation since the last checkpoint. Matches
            // the contract used by `delete_many` and
            // `update_int32_pair_inplace_undo`.
            if let Some(sink) = wal {
                Self::maybe_emit_fpw(
                    &self.pool,
                    src_page_id,
                    sink,
                    &self.last_checkpoint_lsn,
                    xid,
                )?;
            }

            let src_guard = self.pool.get_page(src_page_id)?;
            let mut src_page = src_guard.write();
            let src_bytes = src_page.as_bytes_mut();
            let src_slot_count = {
                let hdr = crate::page::PageHeader::decode(src_bytes).map_err(HeapError::Page)?;
                hdr.slot_count()
            };

            for src_slot in 0..src_slot_count {
                let item_id_off = PAGE_HEADER_SIZE + usize::from(src_slot) * ITEMID_SIZE;
                let item_raw = u32::from_le_bytes([
                    src_bytes[item_id_off],
                    src_bytes[item_id_off + 1],
                    src_bytes[item_id_off + 2],
                    src_bytes[item_id_off + 3],
                ]);
                if item_raw & 0b11 != 1 {
                    continue;
                }
                let (length, offset) = itemid_window(item_raw)?;
                if length < TUPLE_HEADER_SIZE
                    || offset
                        .checked_add(length)
                        .is_none_or(|e| e > src_bytes.len())
                {
                    return Err(HeapError::MalformedHeader("slot shorter than header"));
                }

                let xmin_raw = read_le_u64(src_bytes, offset, "xmin out of bounds")?;
                let xmax_raw = read_le_u64(src_bytes, offset + 8, "xmax out of bounds")?;
                let infomask_bits = read_le_u16(src_bytes, offset + 24, "infomask out of bounds")?;
                let xmin_xid = Xid::new(xmin_raw);

                let visible = if xmax_raw == 0 {
                    match xmin_cache {
                        Some((cxmin, cinfo, cv)) if cxmin == xmin_xid && cinfo == infomask_bits => {
                            cv
                        }
                        _ => {
                            let (h, _) =
                                TupleHeader::decode(&src_bytes[offset..offset + TUPLE_HEADER_SIZE])
                                    .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                            let v =
                                matches!(is_visible(&h, snapshot, oracle), Visibility::Visible,);
                            xmin_cache = Some((h.xmin, h.infomask.bits(), v));
                            v
                        }
                    }
                } else {
                    let (h, _) =
                        TupleHeader::decode(&src_bytes[offset..offset + TUPLE_HEADER_SIZE])
                            .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                    matches!(is_visible(&h, snapshot, oracle), Visibility::Visible)
                };
                if !visible {
                    continue;
                }

                // Decode (id, val) so the predicate can decide.
                let payload_off = offset + TUPLE_HEADER_SIZE;
                if payload_off + 9 > offset + length {
                    return Err(HeapError::MalformedHeader(
                        "payload shorter than (Int32, Int32)",
                    ));
                }
                let id = i32::from_le_bytes([
                    src_bytes[payload_off + 1],
                    src_bytes[payload_off + 2],
                    src_bytes[payload_off + 3],
                    src_bytes[payload_off + 4],
                ]);
                let val = i32::from_le_bytes([
                    src_bytes[payload_off + 5],
                    src_bytes[payload_off + 6],
                    src_bytes[payload_off + 7],
                    src_bytes[payload_off + 8],
                ]);
                if !predicate(id, val) {
                    continue;
                }

                // Stamp xmax / cmax / infomask | UPDATED.
                src_bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
                src_bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
                let new_infomask = infomask_bits | InfoMask::UPDATED;
                src_bytes[offset + 24..offset + 26].copy_from_slice(&new_infomask.to_le_bytes());

                if wal.is_some() {
                    wal_scratch.push(TupleId::new(src_page_id, src_slot));
                }

                total_deleted += 1;
                page_deleted = true;
            }

            drop(src_page);
            drop(src_guard);

            // Emit one WAL record per stamped slot with the page guard
            // dropped. Stamp the page LSN with the last assigned LSN
            // so recovery's redo-skip check covers every record on
            // this page.
            if let Some(sink) = wal {
                let mut last_lsn = ultrasql_core::Lsn::ZERO;
                for tid in wal_scratch.iter().copied() {
                    let lsn = Self::emit_delete_in_place_wal(sink, tid, xid, command_id)?;
                    last_lsn = lsn;
                }
                if !wal_scratch.is_empty() {
                    Self::stamp_page_lsn(&self.pool, src_page_id, last_lsn)?;
                }
                wal_scratch.clear();
            }
            if page_deleted && let Some(vm) = vm {
                vm.clear(src_page_id.relation, src_page_id.block);
            }
        }

        if total_deleted > 0 {
            self.column_cache.bump_version(rel);
        }

        Ok(total_deleted)
    }

    /// Parallel no-WAL variant for large in-memory fused `(Int32, Int32)`
    /// DELETEs.
    ///
    /// WAL-backed deletes stay sequential so per-transaction WAL chain ordering
    /// remains unchanged. Without WAL, each worker owns a disjoint page range
    /// and stamps matching visible tuples under the same MVCC rules as
    /// [`Self::delete_int32_pair_inplace`].
    #[allow(clippy::too_many_arguments)]
    pub fn delete_int32_pair_inplace_parallel_no_wal<O, P>(
        &self,
        rel: RelationId,
        block_count: u32,
        snapshot: &Snapshot,
        oracle: &O,
        predicate: P,
        xid: Xid,
        command_id: CommandId,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + Sync + ?Sized,
        P: Fn(i32, i32) -> bool + Sync,
    {
        let available_workers = std::thread::available_parallelism().map_or(1, |n| n.get());
        if block_count < 2_048 || available_workers <= 1 {
            return self.delete_int32_pair_inplace(
                rel,
                block_count,
                snapshot,
                oracle,
                predicate,
                xid,
                command_id,
                None,
                vm,
            );
        }

        let block_count_usize = usize::try_from(block_count)
            .map_err(|_| HeapError::MalformedHeader("block count overflow"))?;
        let workers = available_workers
            .min(block_count_usize.div_ceil(512))
            .min(block_count_usize)
            .max(1);
        if workers <= 1 {
            return self.delete_int32_pair_inplace(
                rel,
                block_count,
                snapshot,
                oracle,
                predicate,
                xid,
                command_id,
                None,
                vm,
            );
        }

        let workers_u32 =
            u32::try_from(workers).map_err(|_| HeapError::MalformedHeader("worker overflow"))?;
        let chunk_blocks = block_count.div_ceil(workers_u32).max(1);
        let predicate_ref = &predicate;
        let mut total_deleted = 0_usize;

        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            let mut start_block = 0_u32;
            while start_block < block_count {
                let end_block = start_block.saturating_add(chunk_blocks).min(block_count);
                handles.push(scope.spawn(move || {
                    self.delete_int32_pair_range_no_wal(
                        rel,
                        start_block,
                        end_block,
                        snapshot,
                        oracle,
                        predicate_ref,
                        xid,
                        command_id,
                        vm,
                    )
                }));
                start_block = end_block;
            }

            for handle in handles {
                total_deleted = total_deleted.saturating_add(handle.join().map_err(|_| {
                    HeapError::MalformedHeader("parallel delete worker panicked")
                })??);
            }
            Ok::<(), HeapError>(())
        })?;

        if total_deleted > 0 {
            self.column_cache.bump_version(rel);
        }

        Ok(total_deleted)
    }

    #[allow(clippy::too_many_arguments)]
    fn delete_int32_pair_range_no_wal<O, P>(
        &self,
        rel: RelationId,
        start_block: u32,
        end_block: u32,
        snapshot: &Snapshot,
        oracle: &O,
        predicate: &P,
        xid: Xid,
        command_id: CommandId,
        vm: Option<&crate::vm::VisibilityMap>,
    ) -> Result<usize, HeapError>
    where
        O: XidStatusOracle + ?Sized,
        P: Fn(i32, i32) -> bool,
    {
        use crate::page::{ITEMID_SIZE, PAGE_HEADER_SIZE};

        let mut total_deleted: usize = 0;
        let mut xmin_cache: Option<(Xid, u16, bool)> = None;
        let xid_bytes = xid.raw().to_le_bytes();
        let cmd_bytes = command_id.raw().to_le_bytes();

        for src_block in start_block..end_block {
            let src_page_id = PageId::new(rel, BlockNumber::new(src_block));
            let mut page_deleted = false;

            let src_guard = self.pool.get_page(src_page_id)?;
            let mut src_page = src_guard.write();
            let src_bytes = src_page.as_bytes_mut();
            let src_slot_count = {
                let hdr = crate::page::PageHeader::decode(src_bytes).map_err(HeapError::Page)?;
                hdr.slot_count()
            };

            for src_slot in 0..src_slot_count {
                let item_id_off = PAGE_HEADER_SIZE + usize::from(src_slot) * ITEMID_SIZE;
                let item_raw = u32::from_le_bytes([
                    src_bytes[item_id_off],
                    src_bytes[item_id_off + 1],
                    src_bytes[item_id_off + 2],
                    src_bytes[item_id_off + 3],
                ]);
                if item_raw & 0b11 != 1 {
                    continue;
                }
                let (length, offset) = itemid_window(item_raw)?;
                if length < TUPLE_HEADER_SIZE
                    || offset
                        .checked_add(length)
                        .is_none_or(|e| e > src_bytes.len())
                {
                    return Err(HeapError::MalformedHeader("slot shorter than header"));
                }

                let xmin_raw = read_le_u64(src_bytes, offset, "xmin out of bounds")?;
                let xmax_raw = read_le_u64(src_bytes, offset + 8, "xmax out of bounds")?;
                let infomask_bits = read_le_u16(src_bytes, offset + 24, "infomask out of bounds")?;
                let xmin_xid = Xid::new(xmin_raw);

                let visible = if xmax_raw == 0 {
                    match xmin_cache {
                        Some((cxmin, cinfo, cv)) if cxmin == xmin_xid && cinfo == infomask_bits => {
                            cv
                        }
                        _ => {
                            let (h, _) =
                                TupleHeader::decode(&src_bytes[offset..offset + TUPLE_HEADER_SIZE])
                                    .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                            let v =
                                matches!(is_visible(&h, snapshot, oracle), Visibility::Visible,);
                            xmin_cache = Some((h.xmin, h.infomask.bits(), v));
                            v
                        }
                    }
                } else {
                    let (h, _) =
                        TupleHeader::decode(&src_bytes[offset..offset + TUPLE_HEADER_SIZE])
                            .ok_or(HeapError::MalformedHeader("header decode failed"))?;
                    matches!(is_visible(&h, snapshot, oracle), Visibility::Visible)
                };
                if !visible {
                    continue;
                }

                let payload_off = offset + TUPLE_HEADER_SIZE;
                if payload_off + 9 > offset + length {
                    return Err(HeapError::MalformedHeader(
                        "payload shorter than (Int32, Int32)",
                    ));
                }
                let id = read_le_i32(src_bytes, payload_off + 1, "id payload out of bounds")?;
                let val = read_le_i32(src_bytes, payload_off + 5, "val payload out of bounds")?;
                if !predicate(id, val) {
                    continue;
                }

                src_bytes[offset + 8..offset + 16].copy_from_slice(&xid_bytes);
                src_bytes[offset + 20..offset + 24].copy_from_slice(&cmd_bytes);
                let new_infomask = infomask_bits | InfoMask::UPDATED;
                src_bytes[offset + 24..offset + 26].copy_from_slice(&new_infomask.to_le_bytes());

                total_deleted += 1;
                page_deleted = true;
            }

            drop(src_page);
            drop(src_guard);

            if page_deleted && let Some(vm) = vm {
                vm.clear(src_page_id.relation, src_page_id.block);
            }
        }

        Ok(total_deleted)
    }

    /// Apply a deletion stamp to the tuple identified by `tid` while
    /// holding `guard`'s exclusive write lock.
    ///
    /// The buffer pool exposes only `read_tuple` (immutable) for
    /// payload access; we re-encode the header into a fresh buffer
    /// and overwrite the slot via the page's mutable bytes.
    /// `Page::insert_tuple` allocates a *new* slot — we want to
    /// overwrite the existing one in place. This is safe because:
    ///
    /// - The new header has the same size as the old one
    ///   ([`TUPLE_HEADER_SIZE`]).
    /// - The slot's `ItemId` offset/length is unchanged.
    /// - The payload trailing the header is untouched.
    ///
    /// If the page module grows an in-place `update_tuple_header`
    /// helper, we should migrate to it.
    ///
    /// Clippy's `significant_drop_tightening` would prefer the
    /// [`PageWrite`](crate::buffer_pool::PageWrite) be dropped before
    /// the closing brace, but `page_bytes` borrows from `page`, so
    /// the borrow checker requires the guard to live until function
    /// exit.
    #[allow(clippy::significant_drop_tightening)]
    pub(super) fn delete_in_place(
        guard: &PageGuard<L>,
        tid: TupleId,
        xmax: Xid,
        cmax: CommandId,
    ) -> Result<(), HeapError> {
        let mut page = guard.write();
        let bytes = page.read_tuple(tid.slot)?;
        if bytes.len() < TUPLE_HEADER_SIZE {
            return Err(HeapError::MalformedHeader("slot shorter than header"));
        }
        let (mut header, _) = TupleHeader::decode(&bytes[..TUPLE_HEADER_SIZE])
            .ok_or(HeapError::MalformedHeader("header decode failed"))?;
        header.mark_deleted(xmax, cmax);
        let header_bytes = Self::collect_header_bytes(&header);

        let page_bytes = page.as_bytes_mut();
        let (slot_offset, slot_length) = Self::slot_window(page_bytes, tid.slot)?;
        if slot_length < TUPLE_HEADER_SIZE {
            return Err(HeapError::MalformedHeader("slot shorter than header"));
        }
        page_bytes[slot_offset..slot_offset + TUPLE_HEADER_SIZE].copy_from_slice(&header_bytes);
        Ok(())
    }
}
