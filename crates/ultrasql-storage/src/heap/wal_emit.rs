//! See `crate::heap` for the public API.
//!
//! Part of the `heap` module split — each `impl<L: PageLoader>
//! HeapAccess<L>` block here adds methods to the type defined in
//! `heap/mod.rs`. Splitting across files keeps each unit under the
//! 600-line ceiling without changing semantics.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ultrasql_core::endian::{write_u16_le, write_u32_le};
use ultrasql_core::{CommandId, Lsn, PageId, TupleId, Xid};
use ultrasql_mvcc::{TupleHeader, tuple_header::TUPLE_HEADER_SIZE};
use ultrasql_wal::WalRecord;
use ultrasql_wal::payload::{
    FullPageWritePayload, HeapDeleteInPlaceBatchPayload, HeapDeleteInPlaceRangeBatchPayload,
    HeapInsertPayload, HeapUpdateInPlacePayload, HeapUpdateInt32PairDeltaBatchPayload,
    HeapUpdateInt32PairDeltaRangeBatchPayload, HeapUpdatePayload, MAX_VARIABLE_PAYLOAD_BYTES,
    PayloadError,
};
use ultrasql_wal::record::RecordType;

use crate::buffer_pool::{BufferPool, PageGuard, PageLoader};
use crate::wal_sink::WalSink;

use super::{HeapAccess, HeapError, InsertOptions, UpdateOptions, UpdateOutcome};

fn contiguous_slot_range(slots: &[u16]) -> Option<(u16, u16)> {
    let (&first, rest) = slots.split_first()?;
    let count = u16::try_from(slots.len()).ok()?;
    for (offset, slot) in rest.iter().copied().enumerate() {
        let delta = u16::try_from(offset.checked_add(1)?).ok()?;
        let expected = first.checked_add(delta)?;
        if slot != expected {
            return None;
        }
    }
    Some((first, count))
}

impl<L: PageLoader> HeapAccess<L> {
    /// Append a WAL record after its page mutation has already landed.
    ///
    /// A rejection here means dirty page bytes may exist without a WAL record
    /// that can replay them. Mark the pool poisoned before returning so the
    /// service rejects later page access and can restart from a consistent WAL
    /// position instead of continuing with unsafe state.
    pub(super) fn append_after_page_mutation(
        pool: &Arc<BufferPool<L>>,
        sink: &dyn WalSink,
        record: WalRecord,
    ) -> Result<Lsn, HeapError> {
        match sink.append(record) {
            Ok(lsn) => Ok(lsn),
            Err(err) => {
                pool.poison_after_wal_error();
                Err(HeapError::Wal(err))
            }
        }
    }

    /// Raise a pinned page's LSN to at least `lsn`.
    ///
    /// Concurrent heap mutators can finish WAL appends out of page-mutation
    /// order. A plain assignment would let the later finisher overwrite a
    /// newer page LSN with an older one, making the checkpointer believe the
    /// whole page is covered by an older durable prefix. Monotonic stamping
    /// preserves the newest WAL dependency regardless of completion order.
    pub(super) fn stamp_pinned_page_lsn(guard: &PageGuard<L>, lsn: Lsn) {
        let mut page = guard.write();
        if page.header().lsn < lsn.raw() {
            page.set_lsn(lsn.raw());
        }
    }

    /// Emit a `RecordType::FullPageWrite` WAL record for `page_id` if the
    /// page's current on-disk LSN is older than `last_checkpoint_lsn`.
    ///
    /// Full-page-write records carry a verbatim copy of the 8 KiB page image
    /// so that crash recovery can restore the page to a known-consistent state
    /// even if a previous write left a torn partial image on disk. The FPW
    /// must be appended **before** the mutation record so the replay sequence
    /// is: restore page → apply mutation.
    ///
    /// This function is called before every page mutation when a WAL sink is
    /// present. If the page's LSN is already ≥ `last_checkpoint_lsn` no FPW
    /// is needed (the page has been modified since the last checkpoint, so a
    /// full copy was already emitted earlier in the current checkpoint cycle).
    ///
    /// The cheap "already covered" check uses a shared latch. When an image is
    /// needed, the function rechecks under the exclusive page latch and keeps
    /// that latch through image capture, WAL append, and LSN stamp. This rare
    /// checkpoint-boundary path must serialize the image with page mutations:
    /// releasing the latch between capture and append could place a stale FPW
    /// after a concurrent mutation record and erase that mutation during redo.
    ///
    /// [`WalSink`] implementations may block on their own WAL I/O but must not
    /// re-enter the heap buffer pool, so appending under this page latch cannot
    /// create a page/WAL lock cycle.
    pub(super) fn maybe_emit_fpw(
        pool: &Arc<BufferPool<L>>,
        page_id: PageId,
        sink: &dyn WalSink,
        last_checkpoint_lsn: &AtomicU64,
        xid: Xid,
    ) -> Result<(), HeapError> {
        use ultrasql_core::constants::PAGE_SIZE;

        let checkpoint_lsn = last_checkpoint_lsn.load(Ordering::Acquire);
        if checkpoint_lsn == 0 {
            // No checkpoint has occurred yet; FPW not needed.
            return Ok(());
        }

        let guard = pool.get_page_relieved(page_id)?;
        // Most hot pages have already been touched in the current checkpoint
        // cycle, so avoid an exclusive latch on the common covered-page path.
        {
            let page = guard.read();
            if page.header().lsn >= checkpoint_lsn {
                return Ok(());
            }
        }

        // Recheck after upgrading by release/reacquire: another writer may have
        // emitted the first FPW while we waited for the exclusive latch.
        let mut page = guard.write();
        if page.header().lsn >= checkpoint_lsn {
            return Ok(());
        }
        let page_bytes = page.as_bytes().to_vec();

        // Sanity: page_bytes must be exactly PAGE_SIZE.
        if page_bytes.len() != PAGE_SIZE {
            // This should never happen given the buffer pool's invariants.
            return Err(HeapError::MalformedHeader(
                "page_bytes length is not PAGE_SIZE; cannot emit FPW",
            ));
        }

        let payload = FullPageWritePayload {
            page: page_id,
            page_bytes,
        };
        let prev_lsn = sink.last_lsn_for(xid);
        let record = WalRecord::new(
            RecordType::FullPageWrite,
            xid,
            prev_lsn,
            0,
            payload.encode()?,
        )?;
        // FPW is emitted before the mutation. If the sink rejects it, no page
        // bytes have changed yet, so normal error propagation is safe. Keep
        // the exclusive latch through the append so this image cannot be
        // reordered after a mutation it does not contain.
        let lsn: Lsn = sink.append(record)?;
        if page.header().lsn < lsn.raw() {
            page.set_lsn(lsn.raw());
        }
        Ok(())
    }

    /// Emit a `HeapInsert` WAL record if `opts.wal` is `Some`, then stamp
    /// the page's LSN with the assigned WAL LSN.
    ///
    /// `guard` is the original pin under which the tuple became dirty. It stays
    /// alive through append and LSN stamp, so the checkpointer cannot flush the
    /// post-insert bytes with the page's previous LSN. The page latch itself is
    /// released while appending; readers are not blocked by a slow sink.
    pub(super) fn emit_insert_wal(
        pool: &Arc<BufferPool<L>>,
        tid: TupleId,
        opts: &InsertOptions<'_>,
        guard: &PageGuard<L>,
    ) -> Result<(), HeapError> {
        if let Some(sink) = opts.wal {
            let result: Result<(), HeapError> = (|| {
                let tuple_bytes = Self::copy_slot_bytes(guard, tid.slot)?;
                let prev_lsn = sink.last_lsn_for(opts.xmin);
                let payload_bytes = HeapInsertPayload { tid, tuple_bytes }.encode()?;
                let record = WalRecord::new(
                    RecordType::HeapInsert,
                    opts.xmin,
                    prev_lsn,
                    0,
                    payload_bytes,
                )?;
                let lsn = sink.append(record).map_err(HeapError::Wal)?;
                Self::stamp_pinned_page_lsn(guard, lsn);
                Ok(())
            })();
            if result.is_err() {
                pool.poison_after_wal_error();
            }
            result?;
        }
        Ok(())
    }

    /// Emit one `HeapInsertBatch` WAL record from the input row payloads.
    ///
    /// `insert_batch` already knows the final [`TupleId`] for each payload in
    /// `rows`, so this avoids re-pinning the page and fetching every tuple
    /// after the page fill. The encoded tuple image is the same canonical
    /// `TupleHeader::fresh(...) || payload` bytes that `batch_fill_page` wrote.
    #[expect(
        clippy::too_many_arguments,
        reason = "WAL batch encoding needs the pinned page plus caller-owned row/TID/payload buffers to preserve ordering without allocation"
    )]
    pub(super) fn emit_insert_batch_wal_from_payloads(
        pool: &Arc<BufferPool<L>>,
        guard: &PageGuard<L>,
        page_id: PageId,
        tids: &[TupleId],
        rows: &[&[u8]],
        opts: &InsertOptions<'_>,
        n_atts: u16,
        payload_buf: &mut Vec<u8>,
    ) -> Result<(), HeapError> {
        if let Some(sink) = opts.wal {
            let result: Result<(), HeapError> = (|| {
                if tids.len() != rows.len() {
                    return Err(HeapError::MalformedHeader(
                        "heap insert batch WAL tids/rows length mismatch",
                    ));
                }
                if tids.is_empty() {
                    return Ok(());
                }

                let prev_lsn = sink.last_lsn_for(opts.xmin);
                Self::encode_insert_batch_payload_from_rows(
                    page_id,
                    tids,
                    rows,
                    opts.xmin,
                    opts.command_id,
                    n_atts,
                    payload_buf,
                )?;
                let mut record = WalRecord::new(
                    RecordType::HeapInsertBatch,
                    opts.xmin,
                    prev_lsn,
                    0,
                    std::mem::take(payload_buf),
                )?;
                let lsn = match sink.append_ref(&record) {
                    Ok(lsn) => lsn,
                    Err(err) => {
                        *payload_buf = std::mem::take(&mut record.payload);
                        return Err(HeapError::Wal(err));
                    }
                };
                *payload_buf = std::mem::take(&mut record.payload);
                Self::stamp_pinned_page_lsn(guard, lsn);
                Ok(())
            })();
            if result.is_err() {
                // Every error in this block occurs after the page batch became
                // dirty, so continuing could expose bytes with no replay
                // record. Poison before the original pin is released.
                pool.poison_after_wal_error();
            }
            result?;
        }
        Ok(())
    }

    fn encode_insert_batch_payload_from_rows(
        page_id: PageId,
        tids: &[TupleId],
        rows: &[&[u8]],
        xmin: Xid,
        command_id: CommandId,
        n_atts: u16,
        out: &mut Vec<u8>,
    ) -> Result<(), HeapError> {
        const FIXED: usize = 8 + 4;
        const ENTRY_FIXED: usize = 2 + 2 + 4;

        let entry_count = u32::try_from(tids.len()).map_err(|_| {
            HeapError::WalPayload(PayloadError::Malformed(
                "heap_insert_batch entry_count overflow",
            ))
        })?;
        let mut total = FIXED;
        for (&tid, row) in tids.iter().zip(rows.iter().copied()) {
            if tid.page != page_id {
                return Err(HeapError::MalformedHeader(
                    "heap insert batch spans multiple pages",
                ));
            }
            let tuple_len =
                TUPLE_HEADER_SIZE
                    .checked_add(row.len())
                    .ok_or(HeapError::WalPayload(PayloadError::Malformed(
                        "heap_insert_batch tuple_len overflow",
                    )))?;
            if tuple_len > MAX_VARIABLE_PAYLOAD_BYTES {
                return Err(HeapError::WalPayload(PayloadError::Malformed(
                    "heap_insert_batch tuple_len exceeds ceiling",
                )));
            }
            total = total
                .checked_add(ENTRY_FIXED)
                .and_then(|value| value.checked_add(tuple_len))
                .ok_or(HeapError::WalPayload(PayloadError::Malformed(
                    "heap_insert_batch length overflow",
                )))?;
        }
        if total > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(HeapError::WalPayload(PayloadError::Malformed(
                "heap_insert_batch length exceeds ceiling",
            )));
        }

        out.clear();
        out.resize(total, 0);
        write_u32_le(&mut out[0..4], page_id.relation.oid().raw());
        write_u32_le(&mut out[4..8], page_id.block.raw());
        write_u32_le(&mut out[8..12], entry_count);

        let mut off = FIXED;
        for (&tid, row) in tids.iter().zip(rows.iter().copied()) {
            write_u16_le(&mut out[off..off + 2], tid.slot);
            write_u16_le(&mut out[off + 2..off + 4], 0);
            let tuple_len = TUPLE_HEADER_SIZE + row.len();
            let tuple_len_u32 = u32::try_from(tuple_len).map_err(|_| {
                HeapError::WalPayload(PayloadError::Malformed(
                    "heap_insert_batch tuple_len overflow",
                ))
            })?;
            write_u32_le(&mut out[off + 4..off + 8], tuple_len_u32);
            off += ENTRY_FIXED;
            let header = TupleHeader::fresh(xmin, command_id, tid, n_atts);
            header.encode(&mut out[off..off + TUPLE_HEADER_SIZE]);
            off += TUPLE_HEADER_SIZE;
            out[off..off + row.len()].copy_from_slice(row);
            off += row.len();
        }
        Ok(())
    }

    /// Emit a `HeapUpdate` WAL record if `opts.wal` is `Some`, then stamp
    /// the affected pages' LSN with the assigned WAL LSN.
    ///
    /// `flags` has [`ultrasql_wal::payload::HEAP_UPDATE_HOT`] set when
    /// `outcome.hot` is `true`.
    ///
    /// `new_guard` and `old_guard` are the original pins under which the two
    /// page mutations became dirty. They remain pinned through append and LSN
    /// publication, closing the checkpointer window without holding either
    /// page latch during WAL backpressure.
    ///
    /// When the old and new pages differ (non-HOT), both pages are stamped
    /// with the same LSN so recovery can skip redo on either if the page is
    /// already up-to-date.
    pub(super) fn emit_update_wal(
        pool: &Arc<BufferPool<L>>,
        outcome: UpdateOutcome,
        opts: &UpdateOptions<'_>,
        new_guard: &PageGuard<L>,
        old_guard: &PageGuard<L>,
    ) -> Result<(), HeapError> {
        if let Some(sink) = opts.wal {
            let result: Result<(), HeapError> = (|| {
                let new_tuple_bytes = Self::copy_slot_bytes(new_guard, outcome.new_tid.slot)?;
                let flags = if outcome.hot {
                    ultrasql_wal::payload::HEAP_UPDATE_HOT
                } else {
                    0
                };
                let prev_lsn = sink.last_lsn_for(opts.xid);
                let payload_bytes = HeapUpdatePayload {
                    old_tid: outcome.old_tid,
                    new_tid: outcome.new_tid,
                    flags,
                    new_tuple_bytes,
                }
                .encode()?;
                let record =
                    WalRecord::new(RecordType::HeapUpdate, opts.xid, prev_lsn, 0, payload_bytes)?;
                let lsn = sink.append(record).map_err(HeapError::Wal)?;
                Self::stamp_pinned_page_lsn(new_guard, lsn);
                if outcome.old_tid.page != outcome.new_tid.page {
                    Self::stamp_pinned_page_lsn(old_guard, lsn);
                }
                Ok(())
            })();
            if result.is_err() {
                pool.poison_after_wal_error();
            }
            result?;
        }
        Ok(())
    }

    /// Emit a `RecordType::HeapUpdateInPlace` WAL record covering one
    /// row of an in-place UPDATE.
    ///
    /// Carries both the pre-image and the post-image so recovery can
    /// (a) restore the page bytes to the post-image, and (b) re-insert
    /// the pre-image into the in-memory `UndoRelationLog` so any
    /// in-flight snapshot that pre-dates the writer's commit still
    /// observes the right view through `for_each_visible` / the walker.
    ///
    /// Returns the assigned LSN. The caller finishes every page-local
    /// validation first, appends this record, publishes undo, then performs
    /// only proven-infallible byte writes and stamps the page before releasing
    /// its guard. A rejection therefore leaves both page and undo unchanged.
    pub(super) fn emit_update_in_place_wal(
        sink: &dyn WalSink,
        tid: TupleId,
        writer_xid: Xid,
        command_id: CommandId,
        pre_image: &[u8],
        post_image: &[u8],
    ) -> Result<Lsn, HeapError> {
        let prev_lsn = sink.last_lsn_for(writer_xid);
        let payload_bytes = HeapUpdateInPlacePayload {
            tid,
            writer_xid,
            command_id,
            pre_image_bytes: pre_image.to_vec(),
            post_image_bytes: post_image.to_vec(),
        }
        .encode()?;
        let record = WalRecord::new(
            RecordType::HeapUpdateInPlace,
            writer_xid,
            prev_lsn,
            0,
            payload_bytes,
        )?;
        sink.append(record).map_err(HeapError::Wal)
    }

    /// Emit a compact `(Int32, Int32)` delta UPDATE record before page bytes
    /// are changed.
    ///
    /// Buffered WAL sinks can accept this while the caller still owns the page
    /// write guard. If append fails, the page has not been mutated, so the
    /// buffer pool must not be poisoned.
    #[allow(
        clippy::too_many_arguments,
        reason = "WAL emit helper mirrors fixed wire fields and reuses caller scratch"
    )]
    /// Linked-chain variant of
    /// [`Self::emit_update_int32_pair_delta_batch_wal_before_reuse`]: the
    /// per-transaction chain link is resolved atomically with the append, so
    /// concurrent parallel-update workers keep a linear chain lock-free.
    #[allow(
        clippy::too_many_arguments,
        reason = "WAL emit helper mirrors fixed wire fields and reuses caller scratch"
    )]
    pub(super) fn emit_update_int32_pair_delta_batch_wal_linked(
        sink: &dyn WalSink,
        page_id: PageId,
        writer_xid: Xid,
        command_id: CommandId,
        target_col: u8,
        delta: i32,
        slots: &[u16],
        chain: &std::sync::atomic::AtomicU64,
        payload_buf: &mut Vec<u8>,
    ) -> Result<Lsn, HeapError> {
        let record_type = if let Some((first_slot, slot_count)) = contiguous_slot_range(slots) {
            HeapUpdateInt32PairDeltaRangeBatchPayload {
                page: page_id,
                writer_xid,
                command_id,
                target_col,
                delta,
                first_slot,
                slot_count,
            }
            .encode_into(payload_buf)?;
            RecordType::HeapUpdateInt32PairDeltaRangeBatch
        } else {
            HeapUpdateInt32PairDeltaBatchPayload::encode_slots_into(
                page_id,
                writer_xid,
                command_id,
                target_col,
                delta,
                slots,
                payload_buf,
            )?;
            RecordType::HeapUpdateInt32PairDeltaBatch
        };
        sink.append_borrowed_linked(record_type, writer_xid, 0, payload_buf, chain)
            .map_err(HeapError::Wal)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "WAL emit helper mirrors fixed wire fields and reuses caller scratch"
    )]
    pub(super) fn emit_update_int32_pair_delta_batch_wal_before_reuse(
        sink: &dyn WalSink,
        page_id: PageId,
        writer_xid: Xid,
        command_id: CommandId,
        target_col: u8,
        delta: i32,
        slots: &[u16],
        prev_lsn: Lsn,
        payload_buf: &mut Vec<u8>,
    ) -> Result<Lsn, HeapError> {
        let record_type = if let Some((first_slot, slot_count)) = contiguous_slot_range(slots) {
            HeapUpdateInt32PairDeltaRangeBatchPayload {
                page: page_id,
                writer_xid,
                command_id,
                target_col,
                delta,
                first_slot,
                slot_count,
            }
            .encode_into(payload_buf)?;
            RecordType::HeapUpdateInt32PairDeltaRangeBatch
        } else {
            HeapUpdateInt32PairDeltaBatchPayload::encode_slots_into(
                page_id,
                writer_xid,
                command_id,
                target_col,
                delta,
                slots,
                payload_buf,
            )?;
            RecordType::HeapUpdateInt32PairDeltaBatch
        };
        sink.append_borrowed(record_type, writer_xid, prev_lsn, 0, payload_buf)
            .map_err(HeapError::Wal)
    }

    /// Emit a compact DELETE record before page bytes are changed.
    ///
    /// Buffered WAL sinks can accept this while the caller still owns the page
    /// write guard. If append fails, the page has not been mutated, so the
    /// buffer pool must not be poisoned.
    #[allow(
        clippy::too_many_arguments,
        reason = "WAL emit helper mirrors fixed wire fields and reuses caller scratch"
    )]
    pub(super) fn emit_delete_in_place_batch_wal_before_reuse(
        sink: &dyn WalSink,
        page_id: PageId,
        xmax: Xid,
        cmax: CommandId,
        slots: &[u16],
        payload_buf: &mut Vec<u8>,
        prev_lsn: Lsn,
    ) -> Result<Lsn, HeapError> {
        let record_type = if let Some((first_slot, slot_count)) = contiguous_slot_range(slots) {
            HeapDeleteInPlaceRangeBatchPayload::encode_range_into(
                page_id,
                xmax,
                cmax,
                first_slot,
                slot_count,
                payload_buf,
            )?;
            RecordType::HeapDeleteInPlaceRangeBatch
        } else {
            HeapDeleteInPlaceBatchPayload::encode_slots_into(
                page_id,
                xmax,
                cmax,
                slots,
                payload_buf,
            )?;
            RecordType::HeapDeleteInPlaceBatch
        };
        sink.append_borrowed(record_type, xmax, prev_lsn, 0, payload_buf)
            .map_err(HeapError::Wal)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "WAL emit helper mirrors fixed wire fields and reuses caller scratch"
    )]
    /// Linked-chain variant of
    /// [`Self::emit_delete_in_place_range_batch_wal_before_reuse`]: the
    /// per-transaction chain link is resolved atomically with the append, so
    /// concurrent parallel-delete workers keep a linear chain lock-free.
    #[allow(
        clippy::too_many_arguments,
        reason = "WAL emit helper mirrors fixed wire fields and reuses caller scratch"
    )]
    pub(super) fn emit_delete_in_place_range_batch_wal_linked(
        sink: &dyn WalSink,
        page_id: PageId,
        xmax: Xid,
        cmax: CommandId,
        first_slot: u16,
        slot_count: u16,
        payload_buf: &mut Vec<u8>,
        chain: &std::sync::atomic::AtomicU64,
    ) -> Result<Lsn, HeapError> {
        HeapDeleteInPlaceRangeBatchPayload::encode_range_into(
            page_id,
            xmax,
            cmax,
            first_slot,
            slot_count,
            payload_buf,
        )?;
        sink.append_borrowed_linked(
            RecordType::HeapDeleteInPlaceRangeBatch,
            xmax,
            0,
            payload_buf,
            chain,
        )
        .map_err(HeapError::Wal)
    }

    /// Linked-chain variant of the sparse-slot batch emit (see
    /// [`Self::emit_delete_in_place_range_batch_wal_linked`]). Mirrors the
    /// sequential helper exactly, including collapsing a contiguous slot set
    /// into the compact range payload.
    pub(super) fn emit_delete_in_place_batch_wal_linked(
        sink: &dyn WalSink,
        page_id: PageId,
        xmax: Xid,
        cmax: CommandId,
        slots: &[u16],
        payload_buf: &mut Vec<u8>,
        chain: &std::sync::atomic::AtomicU64,
    ) -> Result<Lsn, HeapError> {
        let record_type = if let Some((first_slot, slot_count)) = contiguous_slot_range(slots) {
            HeapDeleteInPlaceRangeBatchPayload::encode_range_into(
                page_id,
                xmax,
                cmax,
                first_slot,
                slot_count,
                payload_buf,
            )?;
            RecordType::HeapDeleteInPlaceRangeBatch
        } else {
            HeapDeleteInPlaceBatchPayload::encode_slots_into(
                page_id,
                xmax,
                cmax,
                slots,
                payload_buf,
            )?;
            RecordType::HeapDeleteInPlaceBatch
        };
        sink.append_borrowed_linked(record_type, xmax, 0, payload_buf, chain)
            .map_err(HeapError::Wal)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "WAL emit helper mirrors fixed wire fields and reuses caller scratch"
    )]
    pub(super) fn emit_delete_in_place_range_batch_wal_before_reuse(
        sink: &dyn WalSink,
        page_id: PageId,
        xmax: Xid,
        cmax: CommandId,
        first_slot: u16,
        slot_count: u16,
        payload_buf: &mut Vec<u8>,
        prev_lsn: Lsn,
    ) -> Result<Lsn, HeapError> {
        HeapDeleteInPlaceRangeBatchPayload::encode_range_into(
            page_id,
            xmax,
            cmax,
            first_slot,
            slot_count,
            payload_buf,
        )?;
        sink.append_borrowed(
            RecordType::HeapDeleteInPlaceRangeBatch,
            xmax,
            prev_lsn,
            0,
            payload_buf,
        )
        .map_err(HeapError::Wal)
    }
}
