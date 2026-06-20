//! [`Operator`] implementation for [`SeqScan`].
//!
//! Drives the short-lived [`VisibleHeapWalker`] across each
//! `next_batch` call, decoding tuple payloads into the per-column
//! builders and emitting [`BATCH_TARGET_ROWS`](super::BATCH_TARGET_ROWS)-capped
//! [`Batch`]es. The column-cache fast path short-circuits this loop —
//! see [`cache`](super::cache).

use ultrasql_core::Schema;
use ultrasql_mvcc::XidStatusOracle;
use ultrasql_storage::PageLoader;
use ultrasql_vec::Batch;

use super::{BATCH_TARGET_ROWS, SeqScan, build_initial_builders};
use crate::row_codec::RowCodec;
use crate::{ExecError, Operator};

impl<L, O> Operator for SeqScan<L, O>
where
    L: PageLoader + Send + Sync + std::fmt::Debug + 'static,
    O: XidStatusOracle + Send + Sync + std::fmt::Debug + 'static,
{
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }
        // Cancellation poll at batch boundary. A `SeqScan` over a
        // large heap is typically the longest-running operator in the
        // pipeline; checking the flag here is the cheapest place to
        // observe a CancelRequest.
        if let Some(flag) = self.cancel_flag.as_ref()
            && flag.is_set()
        {
            return Err(ExecError::Cancelled);
        }

        // Fast path: replay from cached columnar projection. Skips
        // the heap walk + per-tuple decode entirely. See
        // `CacheReadState`.
        if self.cache_read.is_some() {
            return self.next_batch_from_cache();
        }
        if let Some(error) = self.init_error.as_ref() {
            return Err(ExecError::TypeMismatch(error.clone()));
        }

        let tid_offset = usize::from(self.with_tids) * 2;
        let mut rows_buffered: usize = 0;
        let mut iter_exhausted = true;

        if self.next_block < self.block_count {
            let mut walker = if let Some(vm) = self.vm.as_deref() {
                self.heap.scan_visible_walker_range_from_position_with_vm(
                    self.relation,
                    (self.next_block, self.next_slot),
                    self.block_count,
                    &self.snapshot,
                    self.oracle.as_ref(),
                    vm,
                )
            } else {
                self.heap.scan_visible_walker_range_from_position(
                    self.relation,
                    (self.next_block, self.next_slot),
                    self.block_count,
                    &self.snapshot,
                    self.oracle.as_ref(),
                )
            };
            while rows_buffered < BATCH_TARGET_ROWS {
                let item = walker.try_next().map_err(|e| {
                    tracing::warn!(error = %e, "heap scan error");
                    ExecError::Internal("heap scan failed")
                })?;
                let Some((tid, _header, payload)) = item else {
                    break;
                };
                if self.with_tids {
                    // PostgreSQL's `BlockNumber` is u32; the TID
                    // columns are i32 (matching the v0.5 `ModifyTable`
                    // extractor).
                    let block_i32 = i32::try_from(tid.page.block.raw()).map_err(|_| {
                        ExecError::Internal("BlockNumber exceeds i32 range; TID column overflow")
                    })?;
                    let slot_i32 = i32::from(tid.slot);
                    RowCodec::push_i32_into(&mut self.builders, 0, block_i32);
                    RowCodec::push_i32_into(&mut self.builders, 1, slot_i32);
                }
                self.codec
                    .decode_into_builders(payload, &mut self.builders[tid_offset..])
                    .map_err(|e| {
                        ExecError::TypeMismatch(format!(
                            "row decode failed: relation={:?}, schema={:?}, payload_len={}, payload_prefix={}, error={}",
                            self.relation,
                            self.codec.schema(),
                            payload.len(),
                            payload_prefix(payload),
                            e
                        ))
                    })?;
                // Mirror the decoded row into the cache-build
                // accumulator when populating the column cache.
                // Skipped on the TID-prefixed scan (cache_build is
                // `None` there).
                if let Some(build) = self.cache_build.as_mut() {
                    self.codec
                        .decode_into_builders(payload, &mut build.builders)
                        .map_err(|e| {
                            ExecError::TypeMismatch(format!(
                                "row cache decode failed: relation={:?}, schema={:?}, payload_len={}, payload_prefix={}, error={}",
                                self.relation,
                                self.codec.schema(),
                                payload.len(),
                                payload_prefix(payload),
                                e
                            ))
                        })?;
                }
                rows_buffered += 1;
            }
            // Mark "not exhausted" only when we hit the row cap (the
            // walker may still hold more rows for the next call).
            if rows_buffered >= BATCH_TARGET_ROWS {
                iter_exhausted = false;
            }
            let (next_block, next_slot) = walker.resume_position();
            self.next_block = next_block;
            self.next_slot = next_slot;
        }

        if rows_buffered == 0 {
            self.eof = true;
            // Finalise the cache build, if any. The walker is
            // exhausted: we have every visible row in
            // `cache_build.builders`. Store the result and let the
            // next scan over this relation reach `cache_read`.
            self.finalise_cache_build();
            return Ok(None);
        }

        // Swap out the current builders so we can finish them into a
        // batch; the replacement builders' Vec<T> allocations are
        // fresh — see report below. This is the only per-batch
        // allocation the streaming path performs (excluding the
        // backing batch itself).
        let replacement = build_initial_builders(&self.codec, self.with_tids)?;
        let finished = std::mem::replace(&mut self.builders, replacement);
        let batch = RowCodec::finish_batch(finished)
            .map_err(|err| ExecError::TypeMismatch(err.to_string()))?;

        if iter_exhausted {
            self.eof = true;
            // Walker is done — finalise the cache build before the
            // operator emits its EOF marker on the next call.
            self.finalise_cache_build();
        }
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        // Cache-read path knows the relation's total cardinality
        // up front; advertise it so the wire-encoder can pre-reserve
        // the response buffer and skip mid-stream `BytesMut::reserve`
        // reallocations.
        self.cache_read.as_ref().and_then(|state| {
            state
                .columns
                .columns
                .first()
                .map(ultrasql_vec::column::Column::len)
        })
    }
}

pub(super) fn payload_prefix(payload: &[u8]) -> String {
    let mut out = String::with_capacity(payload.len().min(32) * 2);
    for byte in payload.iter().take(32) {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}
