//! See `crate::heap` for the public API.
//!
//! Part of the `heap` module split — each `impl<L: PageLoader>
//! HeapAccess<L>` block here adds methods to the type defined in
//! `heap/mod.rs`. Splitting across files keeps each unit under the
//! 600-line ceiling without changing semantics.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    reason = "on-disk format / fixed-width packing; narrowings bounded by PAGE_SIZE / relation size"
)]
#![allow(unused_imports)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use ahash::AHashMap;
use ultrasql_core::{BlockNumber, CommandId, Lsn, PageId, RelationId, TupleId, Xid};
use ultrasql_mvcc::tuple_header::{InfoMask, TUPLE_HEADER_SIZE};
use ultrasql_mvcc::{Snapshot, TupleHeader, Visibility, XidStatusOracle, is_visible};
use ultrasql_wal::WalRecord;
use ultrasql_wal::payload::{
    FullPageWritePayload, HeapDeletePayload, HeapInsertPayload, HeapUpdatePayload,
};
use ultrasql_wal::record::RecordType;

use crate::buffer_pool::{BufferPool, PageGuard, PageLoader};
use crate::page::PageError;
use crate::wal_sink::WalSink;

use super::{
    DeleteOptions, HeapAccess, HeapError, HeapTuple, InsertOptions, UndoEntry, UndoRelationLog,
    UpdateOptions, UpdateOutcome, UpdatePayload,
};

impl<L: PageLoader> HeapAccess<L> {
    /// Insert a tuple into the relation.
    ///
    /// The header is built in-place from `opts` and the tuple's own
    /// [`TupleId`] (which is fixed after a slot is assigned). The
    /// caller's `payload` is appended verbatim — encoding the user
    /// columns into a byte buffer is the planner/executor's job, not
    /// the heap's.
    ///
    /// Algorithm:
    ///
    /// 1. If `opts.fsm` is `Some`, consult it for a block with at least
    ///    `tuple_size` bytes free and try that block first.
    /// 2. Walk existing blocks `0..N` in ascending order. For each
    ///    block, pin the page exclusive, try to insert; on success,
    ///    backfill the header's `ctid` with the chosen [`TupleId`]
    ///    and return.
    /// 3. If no existing block has room, allocate a new block, pin it
    ///    exclusively (the buffer pool materializes the page from the
    ///    loader, which is expected to hand back a fresh heap page),
    ///    and insert there.
    /// 4. If allocation fails because the block counter has been
    ///    exhausted, return [`HeapError::OutOfBlocks`].
    ///
    /// After a successful insert, if `opts.fsm` is `Some` the FSM is
    /// updated with the page's new free space, and if `opts.vm` is `Some`
    /// the page's all-visible bit is cleared.
    pub fn insert(
        &self,
        rel: RelationId,
        payload: &[u8],
        opts: InsertOptions<'_>,
    ) -> Result<TupleId, HeapError> {
        let tid = self.insert_inner(rel, payload, opts)?;
        // Invalidate the columnar projection cache for this
        // relation — a new row makes any cached `Vec<Column>`
        // stale until the next `SeqScan` re-builds it.
        self.column_cache.bump_version(rel);
        Ok(tid)
    }

    pub(super) fn insert_inner(
        &self,
        rel: RelationId,
        payload: &[u8],
        opts: InsertOptions<'_>,
    ) -> Result<TupleId, HeapError> {
        let counter = self.counter_for(rel);
        let cursor = self.cursor_for(rel);
        let existing = counter.load(Ordering::Acquire);

        // TODO(attr-count): heap does not yet know per-tuple attribute counts;
        // planner-side encoding will populate this in v0.4. Until then, store 0
        // explicitly so future readers cannot mistake a clipped byte-length for
        // a real count.
        let n_atts: u16 = 0;
        let tuple_size = TUPLE_HEADER_SIZE
            .checked_add(payload.len())
            .ok_or(HeapError::MalformedHeader("tuple size overflow"))?;

        // If an FSM is present, try its hint first before the linear scan.
        if let Some(fsm) = opts.fsm {
            let min_free = u32::try_from(tuple_size + crate::page::ITEMID_SIZE).unwrap_or(u32::MAX);
            if let Some(hint_block) = fsm.find_block_with_at_least(rel, min_free) {
                let page_id = PageId::new(rel, hint_block);
                match self.try_insert_into(page_id, payload, opts, n_atts, tuple_size) {
                    Ok(tid) => {
                        Self::emit_insert_wal(&self.pool, tid, &opts, || self.fetch(tid))?;
                        // Update FSM and VM after the successful insert.
                        Self::post_insert_fsm_vm(&self.pool, tid.page, opts);
                        cursor.store(tid.page.block.raw(), Ordering::Release);
                        return Ok(tid);
                    }
                    // Hint was stale (page filled up since we recorded it);
                    // fall through to the linear scan.
                    Err(HeapError::Page(PageError::NoSpace { .. })) => {
                        fsm.invalidate_block(rel, hint_block);
                    }
                    Err(other) => return Err(other),
                }
            }
        }

        // Start the linear scan at the cached cursor block (the tail of
        // the relation in the common append-only case). Pages before the
        // cursor are statistically unlikely to have room — skipping them
        // turns the per-row insert from O(N) into amortised O(1).
        //
        // The cursor is purely advisory; if it points past `existing` we
        // clamp to zero. If the cached page is full we fall through to
        // the forward scan from the cursor.
        let start = cursor
            .load(Ordering::Acquire)
            .min(existing.saturating_sub(1));
        for block in start..existing {
            let page_id = PageId::new(rel, BlockNumber::new(block));
            match self.try_insert_into(page_id, payload, opts, n_atts, tuple_size) {
                Ok(tid) => {
                    Self::emit_insert_wal(&self.pool, tid, &opts, || self.fetch(tid))?;
                    Self::post_insert_fsm_vm(&self.pool, tid.page, opts);
                    cursor.store(block, Ordering::Release);
                    return Ok(tid);
                }
                Err(HeapError::Page(PageError::NoSpace { .. })) => {}
                Err(other) => return Err(other),
            }
        }

        // Cursor was past the first page with room. Sweep from block 0
        // up to `start` so this method remains semantically equivalent
        // to "try every page before extending". This branch only fires
        // when the cursor is stale (older inserts deleted on a page
        // before the tail, or concurrent extension).
        for block in 0..start {
            let page_id = PageId::new(rel, BlockNumber::new(block));
            match self.try_insert_into(page_id, payload, opts, n_atts, tuple_size) {
                Ok(tid) => {
                    Self::emit_insert_wal(&self.pool, tid, &opts, || self.fetch(tid))?;
                    Self::post_insert_fsm_vm(&self.pool, tid.page, opts);
                    cursor.store(block, Ordering::Release);
                    return Ok(tid);
                }
                Err(HeapError::Page(PageError::NoSpace { .. })) => {}
                Err(other) => return Err(other),
            }
        }

        // No room anywhere. Grow.
        loop {
            let new_block = counter.fetch_add(1, Ordering::AcqRel);
            if new_block == u32::MAX {
                // Roll back so the counter doesn't overflow on repeat.
                counter.store(u32::MAX, Ordering::Release);
                return Err(HeapError::OutOfBlocks);
            }
            let page_id = PageId::new(rel, BlockNumber::new(new_block));
            match self.try_insert_into(page_id, payload, opts, n_atts, tuple_size) {
                Ok(tid) => {
                    Self::emit_insert_wal(&self.pool, tid, &opts, || self.fetch(tid))?;
                    Self::post_insert_fsm_vm(&self.pool, tid.page, opts);
                    cursor.store(new_block, Ordering::Release);
                    return Ok(tid);
                }
                // A concurrent thread could have raced into this block
                // and used the space — extend again.
                Err(HeapError::Page(PageError::NoSpace { .. })) => {}
                Err(other) => return Err(other),
            }
        }
    }

    /// Insert many tuples into the same relation under a single page
    /// pin per page touched.
    ///
    /// `insert` pins, mutates, and releases a page for every row, and
    /// scans `0..block_count` blocks on every call. For bulk loads this
    /// is O(N²) in the number of inserted rows because every late row
    /// re-walks every earlier (full) page before finding the tail.
    ///
    /// `insert_batch` short-circuits that walk by holding a single
    /// page-write guard across as many rows as fit on the current page,
    /// then moving to the next page exactly when the page returns
    /// [`PageError::NoSpace`]. Pages already known to be full are
    /// skipped without taking the write lock — only the guarded
    /// candidate is locked.
    ///
    /// Semantics are byte-for-byte equivalent to calling
    /// [`Self::insert`] N times in order, but with a much tighter
    /// inner loop:
    ///
    /// - The MVCC header for every row uses `opts.xmin` /
    ///   `opts.command_id`, identical to `insert`.
    /// - Each tuple's `ctid` is patched to its assigned [`TupleId`]
    ///   after slot allocation, identical to `insert`.
    /// - WAL emission, if `opts.wal` is `Some`, runs exactly once per
    ///   tuple (just like `insert`); the batch path is meaningful only
    ///   when `opts.wal` is `None` (the WAL emission path serializes on
    ///   the sink and dominates the per-row cost).
    /// - FSM hints and VM clears, when `opts.fsm` / `opts.vm` are
    ///   `Some`, run once per *page touched*, not per row. This is a
    ///   strict improvement over `insert`'s per-row hooks because the
    ///   page's free-space at the end of the batch is the only value
    ///   that matters for the next batch.
    ///
    /// # Returns
    ///
    /// A `Vec<TupleId>` of length `rows.len()`, in the same order as
    /// the input. On error, the returned vec contains the [`TupleId`]s
    /// of every row inserted so far — the partial result is
    /// recoverable by the caller (no rollback is attempted because
    /// callers that need transactional rollback must use the
    /// txn-manager surface, not the heap directly).
    ///
    /// # Errors
    ///
    /// - [`HeapError::BufferPool`] if the buffer pool cannot pin a
    ///   target page.
    /// - [`HeapError::Page`] for any non-`NoSpace` page error
    ///   (`NoSpace` is handled internally by advancing to the next
    ///   block).
    /// - [`HeapError::MalformedHeader`] if a per-row tuple size
    ///   overflows `usize`.
    /// - [`HeapError::OutOfBlocks`] if the relation's block counter
    ///   reaches `u32::MAX`.
    /// - [`HeapError::Wal`] or [`HeapError::WalPayload`] if WAL
    ///   emission is configured and fails.
    ///
    /// # Concurrency
    ///
    /// `insert_batch` holds at most one page-write lock at any moment.
    /// It is safe to call concurrently from multiple threads against
    /// the same relation; concurrent batches may interleave on pages
    /// and produce non-contiguous slot assignments within a page.
    /// Block-counter monotonicity is preserved through `AcqRel`
    /// `fetch_add` on the per-relation counter, matching
    /// [`Self::insert`].
    pub fn insert_batch(
        &self,
        rel: RelationId,
        rows: &[&[u8]],
        opts: InsertOptions<'_>,
    ) -> Result<Vec<TupleId>, HeapError> {
        let mut out: Vec<TupleId> = Vec::with_capacity(rows.len());
        if rows.is_empty() {
            return Ok(out);
        }

        // TODO(attr-count): heap does not yet know per-tuple attribute counts;
        // planner-side encoding will populate this in v0.4. Until then, store 0
        // explicitly so future readers cannot mistake a clipped byte-length for
        // a real count.
        let n_atts: u16 = 0;

        let counter = self.counter_for(rel);
        let insert_cursor = self.cursor_for(rel);
        let mut block_count = counter.load(Ordering::Acquire);
        // Per-relation cursor: start scanning for room at the cached
        // tail hint, falling back to the highest known block. Most
        // relations are append-only; the tail page is overwhelmingly
        // the page with free space.
        let cached = insert_cursor.load(Ordering::Acquire);
        let mut cursor: u32 = cached.min(block_count.saturating_sub(1));
        let mut row_idx: usize = 0;

        while row_idx < rows.len() {
            // (1) If no block has ever been allocated, grow first.
            if block_count == 0 {
                let new_block = counter.fetch_add(1, Ordering::AcqRel);
                if new_block == u32::MAX {
                    counter.store(u32::MAX, Ordering::Release);
                    return Err(HeapError::OutOfBlocks);
                }
                block_count = new_block.saturating_add(1);
                cursor = new_block;
            }

            let page_id = PageId::new(rel, BlockNumber::new(cursor));
            let drained =
                Self::batch_fill_page(&self.pool, page_id, rows, &mut out, row_idx, opts, n_atts)?;
            row_idx += drained;
            // After this page, the post hooks fire once for the affected page.
            Self::post_insert_fsm_vm(&self.pool, page_id, opts);

            if row_idx == rows.len() {
                // Cache the last block we wrote into so the next batch
                // (or per-row `insert`) starts here.
                insert_cursor.store(cursor, Ordering::Release);
                break;
            }

            // (2) Page is full. Advance to the next known block, or
            // allocate a new one when we've walked past the tail.
            cursor = cursor.saturating_add(1);
            if cursor >= block_count {
                let new_block = counter.fetch_add(1, Ordering::AcqRel);
                if new_block == u32::MAX {
                    counter.store(u32::MAX, Ordering::Release);
                    return Err(HeapError::OutOfBlocks);
                }
                block_count = new_block.saturating_add(1);
                cursor = new_block;
            }
        }

        // WAL emission, if configured. Runs in the same per-row pattern
        // as `insert`, after every page mutation and outside any pin.
        if opts.wal.is_some() {
            for &tid in &out {
                Self::emit_insert_wal(&self.pool, tid, &opts, || self.fetch(tid))?;
            }
        }

        // Invalidate columnar projection cache.
        self.column_cache.bump_version(rel);
        Ok(out)
    }

    /// Append pre-encoded tuples by packing heap pages locally and writing
    /// full pages through `writer`.
    ///
    /// This is a benchmark/recovery bulk path for append-only loads. It skips
    /// the buffer-pool page table entirely: pages become visible through the
    /// relation block counter and are materialized later by the configured
    /// [`PageLoader`]. The normal OLTP [`Self::insert_batch`] path remains the
    /// concurrency-safe choice for user DML.
    pub fn bulk_load_encoded_batch<F>(
        &self,
        rel: RelationId,
        rows: &[Vec<u8>],
        opts: InsertOptions<'_>,
        mut writer: F,
    ) -> Result<u64, HeapError>
    where
        F: FnMut(PageId, &crate::page::Page) -> ultrasql_core::Result<()>,
    {
        if rows.is_empty() {
            return Ok(0);
        }
        if opts.wal.is_some() {
            return Err(HeapError::MalformedHeader(
                "bulk_load_encoded_batch does not emit WAL",
            ));
        }

        let counter = self.counter_for(rel);
        let insert_cursor = self.cursor_for(rel);
        let n_atts: u16 = 0;
        let mut row_idx = 0_usize;
        let mut inserted = 0_u64;

        while row_idx < rows.len() {
            let block = counter.fetch_add(1, Ordering::AcqRel);
            if block == u32::MAX {
                counter.store(u32::MAX, Ordering::Release);
                return Err(HeapError::OutOfBlocks);
            }
            let page_id = PageId::new(rel, BlockNumber::new(block));
            let mut page = crate::page::Page::new_heap();
            let drained =
                Self::bulk_fill_local_page(page_id, &mut page, rows, row_idx, opts, n_atts)?;
            if drained == 0 {
                return Err(HeapError::Page(PageError::NoSpace {
                    needed: rows[row_idx]
                        .len()
                        .saturating_add(TUPLE_HEADER_SIZE)
                        .saturating_add(crate::page::ITEMID_SIZE),
                    available: page.header().free_space(),
                }));
            }
            writer(page_id, &page)?;
            if let Some(vm) = opts.vm {
                vm.mark_all_visible(page_id.relation, page_id.block);
            }
            row_idx += drained;
            inserted = inserted.saturating_add(
                u64::try_from(drained)
                    .map_err(|_| HeapError::MalformedHeader("bulk load count overflow"))?,
            );
            insert_cursor.store(block, Ordering::Release);
        }

        self.column_cache.bump_version(rel);
        Ok(inserted)
    }

    /// Fill `page_id` with as many of `rows[row_idx..]` as fit under a
    /// single exclusive page guard.
    ///
    /// Returns the number of rows consumed. Per-row [`TupleId`]s are
    /// appended to `out`. A return of zero means the page had no room
    /// for even the first remaining row; the caller should advance to
    /// the next page.
    ///
    /// FSM/VM hooks are *not* invoked here: this helper is responsible
    /// only for the page-local fill loop. The caller fires the
    /// per-page post-hooks once after each call so the FSM sees the
    /// final post-batch free space (and the page guard is released
    /// before the FSM/VM lookup).
    ///
    /// Clippy's `significant_drop_tightening` would prefer the
    /// [`PageWrite`](crate::buffer_pool::PageWrite) be dropped before
    /// the closing brace, but `page_bytes` borrows from `page`, so the
    /// borrow checker requires the guard to live until function exit.
    #[allow(clippy::significant_drop_tightening)]
    pub(super) fn batch_fill_page(
        pool: &Arc<BufferPool<L>>,
        page_id: PageId,
        rows: &[&[u8]],
        out: &mut Vec<TupleId>,
        row_idx: usize,
        opts: InsertOptions<'_>,
        n_atts: u16,
    ) -> Result<usize, HeapError> {
        use crate::page::{ITEMID_SIZE, ItemId, ItemIdFlags, Page};

        let guard = pool.get_page(page_id)?;
        let mut page = guard.write();
        let mut filled: usize = 0;

        // Decode the page header ONCE and drive every per-row insert
        // by mutating local `lower`/`upper`/`slot_count` cursors. The
        // header is re-encoded into the page bytes a single time, at
        // the end of the loop, instead of once per row. For a 4 096-
        // tuple bulk insert this drops ~4 096 `header.decode` +
        // 4 096 `header.encode` round-trips down to one of each.
        let mut header = page.header();
        let mut cur_lower = header.lower as usize;
        let mut cur_upper = header.upper as usize;

        for row in &rows[row_idx..] {
            let tuple_size = TUPLE_HEADER_SIZE
                .checked_add(row.len())
                .ok_or(HeapError::MalformedHeader("tuple size overflow"))?;

            // Fast-path: skip pages that obviously cannot hold this tuple.
            let free = cur_upper.saturating_sub(cur_lower);
            if free < tuple_size + ITEMID_SIZE {
                break;
            }
            let tuple_len_u32 = u32::try_from(tuple_size)
                .map_err(|_| HeapError::Page(PageError::Malformed("tuple too large for page")))?;
            if tuple_len_u32 > ItemId::MAX_LENGTH {
                return Err(HeapError::Page(PageError::Malformed(
                    "tuple length exceeds itemid",
                )));
            }

            // Slot is predictable: `insert_tuple_appended` always uses
            // `slot_count` as the new slot id. We compute it from the
            // local `lower` cursor and bake the final `ctid` into the
            // tuple header up front so no post-insert patch is needed.
            let slot_count =
                u16::try_from((cur_lower - crate::page::PAGE_HEADER_SIZE) / ITEMID_SIZE)
                    .map_err(|_| HeapError::MalformedHeader("slot count overflow"))?;
            let final_tid = TupleId::new(page_id, slot_count);
            let tuple_header = TupleHeader::fresh(opts.xmin, opts.command_id, final_tid, n_atts);

            // Reserve `tuple_size` bytes at the top of the page body.
            cur_upper -= tuple_size;
            let page_bytes = page.as_bytes_mut();
            // Encode the tuple header directly into the page bytes
            // (zero scratch copy).
            tuple_header.encode(&mut page_bytes[cur_upper..cur_upper + TUPLE_HEADER_SIZE]);
            // Copy the row body straight in.
            page_bytes[cur_upper + TUPLE_HEADER_SIZE..cur_upper + tuple_size].copy_from_slice(row);

            // Write the slot's `ItemId` (4 bytes) inline.
            let item = ItemId::new(cur_upper as u32, tuple_len_u32, ItemIdFlags::Normal);
            let id_off = Page::item_id_offset(slot_count);
            page_bytes[id_off..id_off + ITEMID_SIZE]
                .copy_from_slice(&item.into_raw().to_le_bytes());

            cur_lower += ITEMID_SIZE;
            out.push(final_tid);
            filled += 1;
        }

        // Re-encode the page header once, outside the per-tuple loop.
        if filled > 0 {
            header.lower = cur_lower as u16;
            header.upper = cur_upper as u16;
            header.encode(page.as_bytes_mut());
        }

        Ok(filled)
    }

    fn bulk_fill_local_page(
        page_id: PageId,
        page: &mut crate::page::Page,
        rows: &[Vec<u8>],
        row_idx: usize,
        opts: InsertOptions<'_>,
        n_atts: u16,
    ) -> Result<usize, HeapError> {
        use crate::page::{ITEMID_SIZE, ItemId, ItemIdFlags, Page};

        let mut filled: usize = 0;
        let mut header = page.header();
        let mut cur_lower = header.lower as usize;
        let mut cur_upper = header.upper as usize;

        for row in &rows[row_idx..] {
            let tuple_size = TUPLE_HEADER_SIZE
                .checked_add(row.len())
                .ok_or(HeapError::MalformedHeader("tuple size overflow"))?;
            let free = cur_upper.saturating_sub(cur_lower);
            if free < tuple_size + ITEMID_SIZE {
                break;
            }
            let tuple_len_u32 = u32::try_from(tuple_size)
                .map_err(|_| HeapError::Page(PageError::Malformed("tuple too large for page")))?;
            if tuple_len_u32 > ItemId::MAX_LENGTH {
                return Err(HeapError::Page(PageError::Malformed(
                    "tuple length exceeds itemid",
                )));
            }

            let slot_count =
                u16::try_from((cur_lower - crate::page::PAGE_HEADER_SIZE) / ITEMID_SIZE)
                    .map_err(|_| HeapError::MalformedHeader("slot count overflow"))?;
            let final_tid = TupleId::new(page_id, slot_count);
            let tuple_header = TupleHeader::fresh(opts.xmin, opts.command_id, final_tid, n_atts);

            cur_upper -= tuple_size;
            let page_bytes = page.as_bytes_mut();
            tuple_header.encode(&mut page_bytes[cur_upper..cur_upper + TUPLE_HEADER_SIZE]);
            page_bytes[cur_upper + TUPLE_HEADER_SIZE..cur_upper + tuple_size].copy_from_slice(row);

            let item = ItemId::new(cur_upper as u32, tuple_len_u32, ItemIdFlags::Normal);
            let id_off = Page::item_id_offset(slot_count);
            page_bytes[id_off..id_off + ITEMID_SIZE]
                .copy_from_slice(&item.into_raw().to_le_bytes());

            cur_lower += ITEMID_SIZE;
            filled += 1;
        }

        if filled > 0 {
            header.lower = cur_lower as u16;
            header.upper = cur_upper as u16;
            header.encode(page.as_bytes_mut());
        }

        Ok(filled)
    }

    pub(super) fn try_insert_into(
        &self,
        page_id: PageId,
        payload: &[u8],
        opts: InsertOptions<'_>,
        n_atts: u16,
        tuple_size: usize,
    ) -> Result<TupleId, HeapError> {
        // Emit a full-page-write record if this is the first modification of
        // the page since the last checkpoint. The FPW is emitted under a
        // shared pin so the read and the WAL append complete before we take
        // the exclusive write lock below.
        if let Some(sink) = opts.wal {
            Self::maybe_emit_fpw(
                &self.pool,
                page_id,
                sink,
                &self.last_checkpoint_lsn,
                opts.xmin,
            )?;
        }
        let guard = self.pool.get_page(page_id)?;
        Self::insert_into_pinned(&guard, page_id, payload, opts, n_atts, tuple_size)
    }

    /// Pin-and-insert helper. Splitting this out of
    /// [`Self::try_insert_into`] keeps the
    /// [`PageWrite`](crate::buffer_pool::PageWrite) lifetime tight:
    /// the per-frame write lock is released the instant this
    /// function returns.
    ///
    /// Clippy's `significant_drop_tightening` would prefer the
    /// [`PageWrite`](crate::buffer_pool::PageWrite) be dropped before
    /// the closing brace, but the `page_bytes` slice in this function
    /// borrows from `page`, so the borrow checker requires the guard
    /// to live until the slice is no longer in use — i.e. function
    /// exit.
    #[allow(clippy::significant_drop_tightening)]
    pub(super) fn insert_into_pinned(
        guard: &PageGuard<L>,
        page_id: PageId,
        payload: &[u8],
        opts: InsertOptions<'_>,
        n_atts: u16,
        tuple_size: usize,
    ) -> Result<TupleId, HeapError> {
        let mut page = guard.write();

        // Skip pages that obviously cannot hold this tuple to save
        // the construction of the header buffer.
        let free = page.header().free_space();
        if free < tuple_size + crate::page::ITEMID_SIZE {
            return Err(HeapError::Page(PageError::NoSpace {
                needed: tuple_size + crate::page::ITEMID_SIZE,
                available: free,
            }));
        }

        // Build the tuple bytes: header || payload. We don't know
        // the final slot yet, so the header's ctid initially points
        // at slot 0; we patch it after `insert_tuple` returns the
        // assigned slot.
        let mut tuple_bytes = Vec::with_capacity(tuple_size);
        tuple_bytes.resize(TUPLE_HEADER_SIZE, 0);
        let tentative_tid = TupleId::new(page_id, 0);
        let header = TupleHeader::fresh(opts.xmin, opts.command_id, tentative_tid, n_atts);
        header.encode(&mut tuple_bytes[..TUPLE_HEADER_SIZE]);
        tuple_bytes.extend_from_slice(payload);

        let slot = page.insert_tuple(&tuple_bytes)?;

        // Patch the header's ctid to point at the assigned slot.
        let final_tid = TupleId::new(page_id, slot);
        let mut patched_header = header;
        patched_header.ctid = final_tid;
        let header_bytes = Self::collect_header_bytes(&patched_header);
        let page_bytes = page.as_bytes_mut();
        let (slot_offset, _) = Self::slot_window(page_bytes, slot)?;
        page_bytes[slot_offset..slot_offset + TUPLE_HEADER_SIZE].copy_from_slice(&header_bytes);
        Ok(final_tid)
    }
}
