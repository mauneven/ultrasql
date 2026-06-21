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

use crate::buffer_pool::{BufferPool, PageLoader};
use crate::wal_sink::WalSink;

use super::{HeapAccess, HeapError, HeapTuple, InsertOptions, UpdateOptions, UpdateOutcome};

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

    /// Stamp the page-LSN field of `page_id` with `lsn`.
    ///
    /// This must be called **after** the WAL append that assigned `lsn` so
    /// the page's LSN is never ahead of the WAL. Recovery uses the page LSN
    /// to determine whether a given page on disk already reflects a WAL
    /// record (redo-skip optimisation).
    ///
    /// The stamp takes a fresh pin on the page, modifies the header under an
    /// exclusive write lock, and releases the pin. The cost is one pin +
    /// one write lock per WAL append — acceptable for correctness.
    pub(super) fn stamp_page_lsn(
        pool: &Arc<BufferPool<L>>,
        page_id: PageId,
        lsn: Lsn,
    ) -> Result<(), HeapError> {
        let guard = pool.get_page_relieved(page_id)?;
        guard.write().set_lsn(lsn.raw());
        Ok(())
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
    /// The function pins the page under a **shared** read lock to capture the
    /// image, appends the FPW record, then releases the pin. No exclusive lock
    /// is held during WAL I/O, which is consistent with the pattern used by
    /// `emit_insert_wal` and friends.
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

        // Read the page under a shared lock. Most hot pages have already
        // been touched in the current checkpoint cycle, so check the cheap
        // header LSN before copying the 8 KiB image.
        let page_bytes = {
            let guard = pool.get_page_relieved(page_id)?;
            let page = guard.read();
            let page_lsn = page.header().lsn;
            if page_lsn >= checkpoint_lsn {
                return Ok(());
            }
            // Copy the full page image into an owned Vec so we release the
            // shared pin before appending to the WAL (no pin during WAL I/O).
            let bytes = page.as_bytes().to_vec();
            drop(page);
            bytes
        };

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
        // bytes have changed yet, so normal error propagation is safe.
        let lsn: Lsn = sink.append(record)?;
        // Stamp the page LSN with the FPW LSN so we don't emit duplicate FPWs
        // for subsequent mutations in the same checkpoint cycle.
        Self::stamp_page_lsn(pool, page_id, lsn)?;
        Ok(())
    }

    /// Emit a `HeapInsert` WAL record if `opts.wal` is `Some`, then stamp
    /// the page's LSN with the assigned WAL LSN.
    ///
    /// `fetch_tuple` is a closure that reads the canonical on-page tuple bytes;
    /// it is called only when the sink is present to avoid a redundant fetch in
    /// the no-WAL path.
    ///
    /// This function must be called **after** the page guard has been dropped
    /// so no buffer-pool pin is held during WAL I/O. If the sink rejects the
    /// record after the page has been written, the buffer pool is poisoned and
    /// [`HeapError::Wal`] is returned; callers must treat that as fatal and
    /// restart from WAL before accepting more work.
    pub(super) fn emit_insert_wal(
        pool: &Arc<BufferPool<L>>,
        tid: TupleId,
        opts: &InsertOptions<'_>,
        fetch_tuple: impl FnOnce() -> Result<HeapTuple, HeapError>,
    ) -> Result<(), HeapError> {
        if let Some(sink) = opts.wal {
            let tup = fetch_tuple()?;
            // Reconstruct full on-page bytes: header || payload.
            let mut tuple_bytes = Vec::with_capacity(TUPLE_HEADER_SIZE + tup.data.len());
            tuple_bytes.resize(TUPLE_HEADER_SIZE, 0);
            tup.header.encode(&mut tuple_bytes[..TUPLE_HEADER_SIZE]);
            tuple_bytes.extend_from_slice(&tup.data);

            let prev_lsn = sink.last_lsn_for(opts.xmin);
            // Encoding is fallible and runs here (post-page-write), but
            // PayloadError is an internal format invariant violation and
            // should not occur in practice with a well-formed HeapInsertPayload.
            let payload_bytes = HeapInsertPayload { tid, tuple_bytes }.encode()?;
            let record = WalRecord::new(
                RecordType::HeapInsert,
                opts.xmin,
                prev_lsn,
                0,
                payload_bytes,
            )?;
            let lsn: Lsn = Self::append_after_page_mutation(pool, sink, record)?;
            // Stamp the page LSN now that the WAL record is durable.
            // WAL append happened before stamp so page LSN is never ahead of WAL.
            Self::stamp_page_lsn(pool, tid.page, lsn)?;
        }
        Ok(())
    }

    /// Emit one `HeapInsertBatch` WAL record from the input row payloads.
    ///
    /// `insert_batch` already knows the final [`TupleId`] for each payload in
    /// `rows`, so this avoids re-pinning the page and fetching every tuple
    /// after the page fill. The encoded tuple image is the same canonical
    /// `TupleHeader::fresh(...) || payload` bytes that `batch_fill_page` wrote.
    pub(super) fn emit_insert_batch_wal_from_payloads(
        pool: &Arc<BufferPool<L>>,
        page_id: PageId,
        tids: &[TupleId],
        rows: &[&[u8]],
        opts: &InsertOptions<'_>,
        n_atts: u16,
        payload_buf: &mut Vec<u8>,
    ) -> Result<(), HeapError> {
        if let Some(sink) = opts.wal {
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
            let lsn: Lsn = match sink.append_ref(&record) {
                Ok(lsn) => lsn,
                Err(err) => {
                    pool.poison_after_wal_error();
                    *payload_buf = std::mem::take(&mut record.payload);
                    return Err(HeapError::Wal(err));
                }
            };
            *payload_buf = std::mem::take(&mut record.payload);
            Self::stamp_page_lsn(pool, page_id, lsn)?;
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
    /// `fetch_new_tuple` is a closure that reads the new version's on-page
    /// bytes; it is only called when the sink is present.
    ///
    /// This function must be called **after** all page guards have been
    /// dropped. If the sink rejects the record after both the old and new
    /// versions have been written, the buffer pool is poisoned and
    /// [`HeapError::Wal`] is returned (same reasoning as
    /// [`Self::emit_insert_wal`]).
    ///
    /// When the old and new pages differ (non-HOT), both pages are stamped
    /// with the same LSN so recovery can skip redo on either if the page is
    /// already up-to-date.
    pub(super) fn emit_update_wal(
        pool: &Arc<BufferPool<L>>,
        outcome: UpdateOutcome,
        opts: &UpdateOptions<'_>,
        fetch_new_tuple: impl FnOnce() -> Result<HeapTuple, HeapError>,
    ) -> Result<(), HeapError> {
        if let Some(sink) = opts.wal {
            let new_tup = fetch_new_tuple()?;
            let mut new_tuple_bytes = Vec::with_capacity(TUPLE_HEADER_SIZE + new_tup.data.len());
            new_tuple_bytes.resize(TUPLE_HEADER_SIZE, 0);
            new_tup
                .header
                .encode(&mut new_tuple_bytes[..TUPLE_HEADER_SIZE]);
            new_tuple_bytes.extend_from_slice(&new_tup.data);

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
            let lsn: Lsn = Self::append_after_page_mutation(pool, sink, record)?;
            // Stamp the new page with the WAL LSN.
            Self::stamp_page_lsn(pool, outcome.new_tid.page, lsn)?;
            // For non-HOT updates the old and new pages differ; stamp
            // both so the page LSN reflects the mutation on both sides.
            if outcome.old_tid.page != outcome.new_tid.page {
                Self::stamp_page_lsn(pool, outcome.old_tid.page, lsn)?;
            }
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
    /// Returns the assigned LSN; the caller stamps the page LSN with
    /// it. The caller MUST call this with the page write guard
    /// dropped (no buffer-pool pin during WAL I/O), exactly like the
    /// existing `emit_update_wal`. Per-row append rather than
    /// batched-per-page so a torn write that cuts the WAL mid-batch
    /// still has every applied row covered up to the cut.
    ///
    /// Failure semantics match `emit_update_wal`: a sink rejection
    /// after the page mutation poisons the buffer pool and returns a
    /// fatal WAL error so the service can restart from the WAL.
    pub(super) fn emit_update_in_place_wal(
        pool: &Arc<BufferPool<L>>,
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
        let lsn: Lsn = Self::append_after_page_mutation(pool, sink, record)?;
        Ok(lsn)
    }

    /// Emit one compact page-level `(Int32, Int32)` delta UPDATE record.
    ///
    /// The fused update path already knows every changed row applied the same
    /// `target_col += delta` edit, so recovery can reconstruct both the
    /// post-image and undo pre-image from the page bytes plus slot list.
    #[allow(
        clippy::too_many_arguments,
        reason = "WAL emit helper mirrors fixed wire fields and reuses caller scratch"
    )]
    pub(super) fn emit_update_int32_pair_delta_batch_wal_reuse(
        pool: &Arc<BufferPool<L>>,
        sink: &dyn WalSink,
        page_id: PageId,
        writer_xid: Xid,
        command_id: CommandId,
        target_col: u8,
        delta: i32,
        slots: &[u16],
        payload_buf: &mut Vec<u8>,
    ) -> Result<Lsn, HeapError> {
        let prev_lsn = sink.last_lsn_for(writer_xid);
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
        let lsn = match sink.append_borrowed(record_type, writer_xid, prev_lsn, 0, payload_buf) {
            Ok(lsn) => lsn,
            Err(err) => {
                pool.poison_after_wal_error();
                return Err(HeapError::Wal(err));
            }
        };
        Ok(lsn)
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

    /// Emit one page-level in-place DELETE record covering every slot
    /// stamped on `page_id`.
    ///
    /// The caller mutates one source page under one write guard, drops
    /// the guard, appends this WAL record, then stamps the page with
    /// the returned LSN. This preserves the same FPW + redo-skip
    /// contract as the per-row record while avoiding one WAL append
    /// per tuple on bulk deletes.
    /// Emit one page-level in-place DELETE record, reusing `payload_buf`.
    #[allow(
        clippy::too_many_arguments,
        reason = "WAL emit helper mirrors fixed wire fields and reuses caller scratch"
    )]
    pub(super) fn emit_delete_in_place_batch_wal_reuse_after(
        pool: &Arc<BufferPool<L>>,
        sink: &dyn WalSink,
        page_id: PageId,
        xmax: Xid,
        cmax: CommandId,
        slots: &[u16],
        payload_buf: &mut Vec<u8>,
        prev_lsn: Lsn,
    ) -> Result<Lsn, HeapError> {
        Self::emit_delete_in_place_batch_wal_reuse_after_inner(
            pool,
            sink,
            page_id,
            xmax,
            cmax,
            slots,
            payload_buf,
            prev_lsn,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "WAL emit helper mirrors fixed wire fields and reuses caller scratch"
    )]
    pub(super) fn emit_delete_in_place_range_batch_wal_reuse_after(
        pool: &Arc<BufferPool<L>>,
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
        let lsn = match sink.append_borrowed(
            RecordType::HeapDeleteInPlaceRangeBatch,
            xmax,
            prev_lsn,
            0,
            payload_buf,
        ) {
            Ok(lsn) => lsn,
            Err(err) => {
                pool.poison_after_wal_error();
                return Err(HeapError::Wal(err));
            }
        };
        Ok(lsn)
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

    #[allow(
        clippy::too_many_arguments,
        reason = "WAL emit helper mirrors fixed wire fields and reuses caller scratch"
    )]
    fn emit_delete_in_place_batch_wal_reuse_after_inner(
        pool: &Arc<BufferPool<L>>,
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
        let lsn = match sink.append_borrowed(record_type, xmax, prev_lsn, 0, payload_buf) {
            Ok(lsn) => lsn,
            Err(err) => {
                pool.poison_after_wal_error();
                return Err(HeapError::Wal(err));
            }
        };
        Ok(lsn)
    }
}
