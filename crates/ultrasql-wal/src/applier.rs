//! Replay-side dispatcher.
//!
//! [`HeapTarget`] is the trait the storage layer implements so this
//! crate can drive a WAL recovery pass over arbitrary heap-shaped
//! state. Tests in this crate provide an in-memory `MockHeap` that
//! records every dispatch so the test suite can assert what the
//! applier did.
//!
//! `replay_into(wal_dir, target)` walks the WAL and dispatches each
//! record to the appropriate `apply_*` method. Commit, Abort, and
//! Checkpoint records are passed to dedicated hooks so recovery can
//! observe transaction lifecycle. Unknown record types surface as
//! [`ApplyError::UnknownPayload`].

use crate::payload::{
    AbortPayload, BTreeOpPayload, CheckpointPayload, CommitPayload, FullPageWritePayload,
    HashOpPayload, HeapDeleteInPlaceBatchPayload, HeapDeleteInPlacePayload,
    HeapDeleteInPlaceRangeBatchPayload, HeapDeletePayload, HeapInsertBatchPayload,
    HeapInsertPayload, HeapUpdateInPlaceBatchPayload, HeapUpdateInPlacePayload,
    HeapUpdateInt32PairDeltaBatchPayload, HeapUpdateInt32PairDeltaRangeBatchPayload,
    HeapUpdatePayload, HnswOpPayload, IvfFlatOpPayload, PayloadError, SequenceOpPayload,
};
use crate::record::{RecordType, WalRecord};
use crate::recovery::RecoveryError;
use ultrasql_core::Lsn;

/// Errors that can arise when dispatching a WAL record to a [`HeapTarget`].
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    /// Payload bytes could not be decoded into the expected typed struct.
    #[error("payload decode: {0}")]
    Payload(#[from] PayloadError),

    /// The target rejected an operation it was asked to perform.
    ///
    /// The `operation` field names the call site (e.g. `"heap_insert"`);
    /// `detail` carries the implementation-supplied reason.
    #[error("target refused {operation}: {detail}")]
    Refused {
        /// Name of the WAL operation that was refused.
        operation: &'static str,
        /// Implementation-supplied reason for the refusal.
        detail: String,
    },

    /// The record header declared a type byte that `dispatch_record` does not
    /// know how to map to a typed payload. This should not arise for well-formed
    /// WAL streams written by this version of the software.
    #[error("unknown payload type for record-type {record_type}")]
    UnknownPayload {
        /// The raw record-type byte from the WAL header.
        record_type: u8,
    },
}

/// Storage-side surface for applying decoded WAL payloads.
///
/// Methods are `&self` so the implementation can be wrapped in `Arc`
/// and shared between recovery threads if needed. The default
/// implementations return [`ApplyError::Refused`] so partial implementations
/// fail loudly rather than silently skip operations. Override every method
/// relevant to your storage implementation; leave the rest as the default
/// refusal.
///
/// # Thread safety
///
/// The bound `Send + Sync` is required so `replay_into` can hand a
/// `&dyn HeapTarget` across thread boundaries in a future parallel
/// recovery path.
pub trait HeapTarget: Send + Sync {
    /// Apply a heap-insert record by writing the tuple bytes to the given slot.
    fn apply_insert(&self, payload: &HeapInsertPayload) -> Result<(), ApplyError> {
        let _ = payload;
        Err(ApplyError::Refused {
            operation: "heap_insert",
            detail: String::from("not implemented"),
        })
    }

    /// Apply a heap-insert record at its WAL stream LSN.
    fn apply_insert_at_lsn(
        &self,
        payload: &HeapInsertPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let _ = record_lsn;
        self.apply_insert(payload)
    }

    /// Apply a page-batched heap-insert record.
    fn apply_insert_batch(&self, payload: &HeapInsertBatchPayload) -> Result<(), ApplyError> {
        let _ = payload;
        Err(ApplyError::Refused {
            operation: "heap_insert_batch",
            detail: String::from("not implemented"),
        })
    }

    /// Apply a page-batched heap-insert record at its WAL stream LSN.
    fn apply_insert_batch_at_lsn(
        &self,
        payload: &HeapInsertBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let _ = record_lsn;
        self.apply_insert_batch(payload)
    }

    /// Apply a heap-update record by superseding the old slot with the new tuple.
    fn apply_update(&self, payload: &HeapUpdatePayload) -> Result<(), ApplyError> {
        let _ = payload;
        Err(ApplyError::Refused {
            operation: "heap_update",
            detail: String::from("not implemented"),
        })
    }

    /// Apply a heap-update record at its WAL stream LSN.
    fn apply_update_at_lsn(
        &self,
        payload: &HeapUpdatePayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let _ = record_lsn;
        self.apply_update(payload)
    }

    /// Apply a heap-delete record by stamping `xmax`/`cmax` into the tuple header.
    fn apply_delete(&self, payload: &HeapDeletePayload) -> Result<(), ApplyError> {
        let _ = payload;
        Err(ApplyError::Refused {
            operation: "heap_delete",
            detail: String::from("not implemented"),
        })
    }

    /// Apply a heap-delete record at its WAL stream LSN.
    fn apply_delete_at_lsn(
        &self,
        payload: &HeapDeletePayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let _ = record_lsn;
        self.apply_delete(payload)
    }

    /// Apply an in-place update record: rewrite the slot's payload
    /// bytes with `post_image_bytes`, stamp `xmax`/`cmax`/
    /// `infomask | UPDATED | UPDATED_IN_PLACE` on the header, and
    /// re-insert the `(tid, writer_xid, pre_image_bytes)` triple
    /// into the in-memory undo log so concurrent readers with
    /// snapshots that pre-date the writer's commit observe the
    /// pre-image. Default refuses; the heap implementor overrides.
    fn apply_update_in_place(&self, payload: &HeapUpdateInPlacePayload) -> Result<(), ApplyError> {
        let _ = payload;
        Err(ApplyError::Refused {
            operation: "heap_update_in_place",
            detail: String::from("not implemented"),
        })
    }

    /// Apply an in-place update record at its WAL stream LSN.
    fn apply_update_in_place_at_lsn(
        &self,
        payload: &HeapUpdateInPlacePayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let _ = record_lsn;
        self.apply_update_in_place(payload)
    }

    /// Apply a page-batched in-place update record.
    fn apply_update_in_place_batch(
        &self,
        payload: &HeapUpdateInPlaceBatchPayload,
    ) -> Result<(), ApplyError> {
        let _ = payload;
        Err(ApplyError::Refused {
            operation: "heap_update_in_place_batch",
            detail: String::from("not implemented"),
        })
    }

    /// Apply a page-batched in-place update record at its WAL stream LSN.
    fn apply_update_in_place_batch_at_lsn(
        &self,
        payload: &HeapUpdateInPlaceBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let _ = record_lsn;
        self.apply_update_in_place_batch(payload)
    }

    /// Apply a compact page-batched `(Int32, Int32)` delta update record.
    fn apply_update_int32_pair_delta_batch(
        &self,
        payload: &HeapUpdateInt32PairDeltaBatchPayload,
    ) -> Result<(), ApplyError> {
        let _ = payload;
        Err(ApplyError::Refused {
            operation: "heap_update_int32_pair_delta_batch",
            detail: String::from("not implemented"),
        })
    }

    /// Apply a compact page-batched `(Int32, Int32)` delta update record at
    /// its WAL stream LSN.
    fn apply_update_int32_pair_delta_batch_at_lsn(
        &self,
        payload: &HeapUpdateInt32PairDeltaBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let _ = record_lsn;
        self.apply_update_int32_pair_delta_batch(payload)
    }

    /// Apply a compact page-batched `(Int32, Int32)` delta update range record.
    fn apply_update_int32_pair_delta_range_batch(
        &self,
        payload: &HeapUpdateInt32PairDeltaRangeBatchPayload,
    ) -> Result<(), ApplyError> {
        let _ = payload;
        Err(ApplyError::Refused {
            operation: "heap_update_int32_pair_delta_range_batch",
            detail: String::from("not implemented"),
        })
    }

    /// Apply a compact page-batched `(Int32, Int32)` delta update range record
    /// at its WAL stream LSN.
    fn apply_update_int32_pair_delta_range_batch_at_lsn(
        &self,
        payload: &HeapUpdateInt32PairDeltaRangeBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let _ = record_lsn;
        self.apply_update_int32_pair_delta_range_batch(payload)
    }

    /// Apply an in-place delete record: stamp `xmax`/`cmax` on the
    /// tuple header. Same semantic shape as `apply_delete`; kept as
    /// a distinct method so a future implementor can branch on
    /// "this came from the fused single-pass path" for telemetry or
    /// VACUUM hints.
    fn apply_delete_in_place(&self, payload: &HeapDeleteInPlacePayload) -> Result<(), ApplyError> {
        let _ = payload;
        Err(ApplyError::Refused {
            operation: "heap_delete_in_place",
            detail: String::from("not implemented"),
        })
    }

    /// Apply an in-place delete record at its WAL stream LSN.
    fn apply_delete_in_place_at_lsn(
        &self,
        payload: &HeapDeleteInPlacePayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let _ = record_lsn;
        self.apply_delete_in_place(payload)
    }

    /// Apply a page-batched in-place delete record.
    fn apply_delete_in_place_batch(
        &self,
        payload: &HeapDeleteInPlaceBatchPayload,
    ) -> Result<(), ApplyError> {
        let _ = payload;
        Err(ApplyError::Refused {
            operation: "heap_delete_in_place_batch",
            detail: String::from("not implemented"),
        })
    }

    /// Apply a page-batched in-place delete record at its WAL stream LSN.
    fn apply_delete_in_place_batch_at_lsn(
        &self,
        payload: &HeapDeleteInPlaceBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let _ = record_lsn;
        self.apply_delete_in_place_batch(payload)
    }

    /// Apply a compact page-batched in-place delete slot range record.
    fn apply_delete_in_place_range_batch(
        &self,
        payload: &HeapDeleteInPlaceRangeBatchPayload,
    ) -> Result<(), ApplyError> {
        let _ = payload;
        Err(ApplyError::Refused {
            operation: "heap_delete_in_place_range_batch",
            detail: String::from("not implemented"),
        })
    }

    /// Apply a compact page-batched in-place delete slot range record at its
    /// WAL stream LSN.
    fn apply_delete_in_place_range_batch_at_lsn(
        &self,
        payload: &HeapDeleteInPlaceRangeBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let _ = record_lsn;
        self.apply_delete_in_place_range_batch(payload)
    }

    /// Apply a full-page-write record by restoring the page image verbatim.
    fn apply_full_page_write(&self, payload: &FullPageWritePayload) -> Result<(), ApplyError> {
        let _ = payload;
        Err(ApplyError::Refused {
            operation: "fpw",
            detail: String::from("not implemented"),
        })
    }

    /// Apply a full-page-write record at its WAL stream LSN.
    fn apply_full_page_write_at_lsn(
        &self,
        payload: &FullPageWritePayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        let _ = record_lsn;
        self.apply_full_page_write(payload)
    }

    /// Apply a B-tree mutation record.
    ///
    /// Heap-only recovery targets may ignore these records, so the default is
    /// a no-op. Storage targets that own index pages override this method.
    fn apply_btree_op(&self, payload: &BTreeOpPayload) -> Result<(), ApplyError> {
        let _ = payload;
        Ok(())
    }

    /// Apply a hash-index mutation record.
    ///
    /// Heap-only recovery targets may ignore these records, so the default is
    /// a no-op. Storage targets that own hash pages override this method.
    fn apply_hash_op(&self, payload: &HashOpPayload) -> Result<(), ApplyError> {
        let _ = payload;
        Ok(())
    }

    /// Return whether this target wants decoded HNSW vector-index records.
    ///
    /// Heap-only recovery targets keep this false so corrupt vector-index WAL
    /// cannot block heap recovery before the vector-index replay pass decides
    /// whether to trust or disable the index.
    fn wants_hnsw_op(&self) -> bool {
        false
    }

    /// Apply an HNSW vector-index graph mutation record.
    ///
    /// Called only when [`Self::wants_hnsw_op`] returns true. This lets
    /// heap-only recovery skip decoding vector-index payloads whose index will
    /// be validated by the vector-index recovery pass.
    fn apply_hnsw_op(&self, payload: &HnswOpPayload) -> Result<(), ApplyError> {
        let _ = payload;
        Ok(())
    }

    /// Return whether this target wants decoded IVFFlat vector-index records.
    ///
    /// Heap-only recovery targets keep this false so corrupt vector-index WAL
    /// cannot block heap recovery before the vector-index replay pass decides
    /// whether to trust or disable the index.
    fn wants_ivfflat_op(&self) -> bool {
        false
    }

    /// Apply an IVFFlat vector-index inverted-list mutation record.
    ///
    /// Called only when [`Self::wants_ivfflat_op`] returns true. Heap-only
    /// recovery leaves payload validation to the vector-index recovery pass.
    fn apply_ivfflat_op(&self, payload: &IvfFlatOpPayload) -> Result<(), ApplyError> {
        let _ = payload;
        Ok(())
    }

    /// Apply a sequence state change record.
    ///
    /// Heap-only recovery targets may ignore these records. A server-level
    /// recovery target that owns the sequence registry should override this
    /// hook and install the state carried by the payload.
    fn apply_sequence_op(&self, payload: &SequenceOpPayload) -> Result<(), ApplyError> {
        let _ = payload;
        Ok(())
    }

    /// Observe a commit record. The default implementation is a no-op.
    ///
    /// Implementors that maintain a CLOG or a transaction state table should
    /// override this to mark the transaction as committed.
    fn observe_commit(&self, payload: &CommitPayload) -> Result<(), ApplyError> {
        let _ = payload;
        Ok(())
    }

    /// Observe an abort record. The default implementation is a no-op.
    ///
    /// Implementors that maintain a CLOG or a transaction state table should
    /// override this to mark the transaction as aborted.
    fn observe_abort(&self, payload: &AbortPayload) -> Result<(), ApplyError> {
        let _ = payload;
        Ok(())
    }

    /// Observe a checkpoint record. The default implementation is a no-op.
    ///
    /// Implementors may use the checkpoint's `redo_from` LSN to skip
    /// applying records that are already reflected in flushed page images.
    fn observe_checkpoint(&self, payload: &CheckpointPayload) -> Result<(), ApplyError> {
        let _ = payload;
        Ok(())
    }
}

/// Dispatch one [`WalRecord`] into the appropriate [`HeapTarget`] method.
///
/// `BTreeOp` records are routed to [`HeapTarget::apply_btree_op`]. `Nop`
/// records are ignored by definition.
///
/// # Errors
///
/// Returns [`ApplyError::Payload`] if the record's payload bytes fail to
/// decode for the declared record type, or [`ApplyError::Refused`] if the
/// target implementation declines the operation.
pub fn dispatch_record(target: &dyn HeapTarget, record: &WalRecord) -> Result<(), ApplyError> {
    dispatch_record_at_lsn(target, record, Lsn::ZERO)
}

/// Dispatch one [`WalRecord`] into `target` with the record's WAL stream LSN.
///
/// Recovery callers use this variant so page-oriented storage engines can
/// compare `record_lsn` with the page header's LSN and skip redo already
/// reflected in a flushed page image.
pub fn dispatch_record_at_lsn(
    target: &dyn HeapTarget,
    record: &WalRecord,
    record_lsn: Lsn,
) -> Result<(), ApplyError> {
    let bytes = &record.payload;
    match record.header.record_type {
        RecordType::HeapInsert => {
            target.apply_insert_at_lsn(&HeapInsertPayload::decode(bytes)?, record_lsn)
        }
        RecordType::HeapInsertBatch => {
            target.apply_insert_batch_at_lsn(&HeapInsertBatchPayload::decode(bytes)?, record_lsn)
        }
        RecordType::HeapUpdate => {
            target.apply_update_at_lsn(&HeapUpdatePayload::decode(bytes)?, record_lsn)
        }
        RecordType::HeapDelete => {
            target.apply_delete_at_lsn(&HeapDeletePayload::decode(bytes)?, record_lsn)
        }
        RecordType::HeapUpdateInPlace => target
            .apply_update_in_place_at_lsn(&HeapUpdateInPlacePayload::decode(bytes)?, record_lsn),
        RecordType::HeapUpdateInPlaceBatch => target.apply_update_in_place_batch_at_lsn(
            &HeapUpdateInPlaceBatchPayload::decode(bytes)?,
            record_lsn,
        ),
        RecordType::HeapUpdateInt32PairDeltaBatch => target
            .apply_update_int32_pair_delta_batch_at_lsn(
                &HeapUpdateInt32PairDeltaBatchPayload::decode(bytes)?,
                record_lsn,
            ),
        RecordType::HeapUpdateInt32PairDeltaRangeBatch => target
            .apply_update_int32_pair_delta_range_batch_at_lsn(
                &HeapUpdateInt32PairDeltaRangeBatchPayload::decode(bytes)?,
                record_lsn,
            ),
        RecordType::HeapDeleteInPlaceBatch => target.apply_delete_in_place_batch_at_lsn(
            &HeapDeleteInPlaceBatchPayload::decode(bytes)?,
            record_lsn,
        ),
        RecordType::HeapDeleteInPlaceRangeBatch => target.apply_delete_in_place_range_batch_at_lsn(
            &HeapDeleteInPlaceRangeBatchPayload::decode(bytes)?,
            record_lsn,
        ),
        RecordType::HeapDeleteInPlace => target
            .apply_delete_in_place_at_lsn(&HeapDeleteInPlacePayload::decode(bytes)?, record_lsn),
        RecordType::FullPageWrite => {
            target.apply_full_page_write_at_lsn(&FullPageWritePayload::decode(bytes)?, record_lsn)
        }
        RecordType::Commit => target.observe_commit(&CommitPayload::decode(bytes)?),
        RecordType::Abort => target.observe_abort(&AbortPayload::decode(bytes)?),
        RecordType::Checkpoint => target.observe_checkpoint(&CheckpointPayload::decode(bytes)?),
        RecordType::BTreeOp => target.apply_btree_op(&BTreeOpPayload::decode(bytes)?),
        RecordType::SequenceOp => target.apply_sequence_op(&SequenceOpPayload::decode(bytes)?),
        RecordType::HashOp => target.apply_hash_op(&HashOpPayload::decode(bytes)?),
        RecordType::HnswOp => {
            if target.wants_hnsw_op() {
                target.apply_hnsw_op(&HnswOpPayload::decode(bytes)?)
            } else {
                Ok(())
            }
        }
        RecordType::IvfFlatOp => {
            if target.wants_ivfflat_op() {
                target.apply_ivfflat_op(&IvfFlatOpPayload::decode(bytes)?)
            } else {
                Ok(())
            }
        }
        RecordType::Nop => Ok(()),
    }
}

/// Walk every record in `wal_dir` and dispatch each one into `target`.
///
/// Returns the LSN of the last successfully-applied record. Decoding
/// stops at the first torn write or CRC mismatch, which is delegated to
/// [`crate::recovery::recover`]'s existing semantics. An empty WAL
/// directory returns [`ultrasql_core::Lsn::ZERO`] and leaves `target`
/// untouched.
///
/// # Errors
///
/// Returns [`RecoveryError::Io`] for segment-file I/O failures,
/// [`RecoveryError::Record`] for fatal record-format errors (unknown type,
/// etc.), and [`RecoveryError::Applier`] if any `HeapTarget` method returns
/// an error.
pub fn replay_into(
    wal_dir: impl AsRef<std::path::Path>,
    target: &dyn HeapTarget,
) -> Result<ultrasql_core::Lsn, RecoveryError> {
    let mut record_lsn = ultrasql_core::Lsn::ZERO;
    crate::recovery::recover(wal_dir, |record| {
        let current_lsn = record_lsn;
        record_lsn = advance_replay_lsn(record_lsn, record.header.total_length)?;
        dispatch_record_at_lsn(target, record, current_lsn)
            .map_err(|e| RecoveryError::Applier(e.to_string()))
    })
}

fn advance_replay_lsn(
    current: ultrasql_core::Lsn,
    record_len: u32,
) -> Result<ultrasql_core::Lsn, RecoveryError> {
    current
        .raw()
        .checked_add(u64::from(record_len))
        .map(ultrasql_core::Lsn::new)
        .ok_or(RecoveryError::Record(
            crate::record::WalRecordError::Malformed("replay lsn overflow"),
        ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use proptest::prelude::*;
    use tempfile::TempDir;
    use ultrasql_core::{BlockNumber, CommandId, Lsn, PageId, RelationId, TupleId, Xid};

    use super::*;
    use crate::buffer::WalBuffer;
    use crate::payload::{
        AbortPayload, BTreeOpKind, BTreeOpPayload, CheckpointPayload, CommitPayload, HashOpKind,
        HashOpPayload, HeapDeleteInPlaceBatchEntry, HeapDeleteInPlaceBatchPayload,
        HeapDeleteInPlaceRangeBatchPayload, HeapInsertBatchEntry, HeapInsertBatchPayload,
        HeapUpdateInPlaceBatchEntry, HeapUpdateInPlaceBatchPayload,
        HeapUpdateInt32PairDeltaBatchPayload, HeapUpdateInt32PairDeltaRangeBatchPayload,
        HeapUpdatePayload, SequenceOpKind, SequenceOpPayload,
    };
    use crate::record::{RecordType, WalRecord, WalRecordError};
    use crate::writer::{WalWriter, WalWriterConfig};

    // ── MockHeap ─────────────────────────────────────────────────────────────

    /// In-memory [`HeapTarget`] that records every dispatched payload so tests
    /// can assert dispatch routing without a real storage layer.
    #[derive(Default)]
    struct MockHeap {
        inserts: Mutex<Vec<HeapInsertPayload>>,
        insert_lsns: Mutex<Vec<Lsn>>,
        insert_batches: Mutex<Vec<HeapInsertBatchPayload>>,
        updates: Mutex<Vec<HeapUpdatePayload>>,
        update_in_place_batches: Mutex<Vec<HeapUpdateInPlaceBatchPayload>>,
        update_int32_pair_delta_batches: Mutex<Vec<HeapUpdateInt32PairDeltaBatchPayload>>,
        update_int32_pair_delta_range_batches:
            Mutex<Vec<HeapUpdateInt32PairDeltaRangeBatchPayload>>,
        delete_in_place_batches: Mutex<Vec<HeapDeleteInPlaceBatchPayload>>,
        delete_in_place_range_batches: Mutex<Vec<HeapDeleteInPlaceRangeBatchPayload>>,
        deletes: Mutex<Vec<HeapDeletePayload>>,
        btree_ops: Mutex<Vec<BTreeOpPayload>>,
        hash_ops: Mutex<Vec<HashOpPayload>>,
        sequence_ops: Mutex<Vec<SequenceOpPayload>>,
        fpws: Mutex<Vec<FullPageWritePayload>>,
        commits: Mutex<Vec<CommitPayload>>,
        aborts: Mutex<Vec<AbortPayload>>,
        checkpoints: Mutex<Vec<CheckpointPayload>>,
    }

    impl HeapTarget for MockHeap {
        fn apply_insert(&self, p: &HeapInsertPayload) -> Result<(), ApplyError> {
            self.inserts.lock().push(p.clone());
            Ok(())
        }

        fn apply_insert_at_lsn(
            &self,
            p: &HeapInsertPayload,
            record_lsn: Lsn,
        ) -> Result<(), ApplyError> {
            self.inserts.lock().push(p.clone());
            self.insert_lsns.lock().push(record_lsn);
            Ok(())
        }

        fn apply_insert_batch(&self, p: &HeapInsertBatchPayload) -> Result<(), ApplyError> {
            self.insert_batches.lock().push(p.clone());
            Ok(())
        }

        fn apply_update(&self, p: &HeapUpdatePayload) -> Result<(), ApplyError> {
            self.updates.lock().push(p.clone());
            Ok(())
        }

        fn apply_update_in_place_batch(
            &self,
            p: &HeapUpdateInPlaceBatchPayload,
        ) -> Result<(), ApplyError> {
            self.update_in_place_batches.lock().push(p.clone());
            Ok(())
        }

        fn apply_update_int32_pair_delta_batch(
            &self,
            p: &HeapUpdateInt32PairDeltaBatchPayload,
        ) -> Result<(), ApplyError> {
            self.update_int32_pair_delta_batches.lock().push(p.clone());
            Ok(())
        }

        fn apply_update_int32_pair_delta_range_batch(
            &self,
            p: &HeapUpdateInt32PairDeltaRangeBatchPayload,
        ) -> Result<(), ApplyError> {
            self.update_int32_pair_delta_range_batches
                .lock()
                .push(p.clone());
            Ok(())
        }

        fn apply_delete_in_place_batch(
            &self,
            p: &HeapDeleteInPlaceBatchPayload,
        ) -> Result<(), ApplyError> {
            self.delete_in_place_batches.lock().push(p.clone());
            Ok(())
        }

        fn apply_delete_in_place_range_batch(
            &self,
            p: &HeapDeleteInPlaceRangeBatchPayload,
        ) -> Result<(), ApplyError> {
            self.delete_in_place_range_batches.lock().push(p.clone());
            Ok(())
        }

        fn apply_delete(&self, p: &HeapDeletePayload) -> Result<(), ApplyError> {
            self.deletes.lock().push(p.clone());
            Ok(())
        }

        fn apply_full_page_write(&self, p: &FullPageWritePayload) -> Result<(), ApplyError> {
            self.fpws.lock().push(p.clone());
            Ok(())
        }

        fn apply_btree_op(&self, p: &BTreeOpPayload) -> Result<(), ApplyError> {
            self.btree_ops.lock().push(p.clone());
            Ok(())
        }

        fn apply_hash_op(&self, p: &HashOpPayload) -> Result<(), ApplyError> {
            self.hash_ops.lock().push(p.clone());
            Ok(())
        }

        fn apply_sequence_op(&self, p: &SequenceOpPayload) -> Result<(), ApplyError> {
            self.sequence_ops.lock().push(p.clone());
            Ok(())
        }

        fn observe_commit(&self, p: &CommitPayload) -> Result<(), ApplyError> {
            self.commits.lock().push(p.clone());
            Ok(())
        }

        fn observe_abort(&self, p: &AbortPayload) -> Result<(), ApplyError> {
            self.aborts.lock().push(p.clone());
            Ok(())
        }

        fn observe_checkpoint(&self, p: &CheckpointPayload) -> Result<(), ApplyError> {
            self.checkpoints.lock().push(p.clone());
            Ok(())
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn tid(rel: u32, block: u32, slot: u16) -> TupleId {
        TupleId::new(
            PageId::new(RelationId::new(rel), BlockNumber::new(block)),
            slot,
        )
    }

    fn page_id(rel: u32, block: u32) -> PageId {
        PageId::new(RelationId::new(rel), BlockNumber::new(block))
    }

    /// Build a [`WalRecord`] that wraps an already-encoded payload.
    fn make_record(record_type: RecordType, payload_bytes: Vec<u8>) -> WalRecord {
        WalRecord::new(record_type, Xid::new(1), Lsn::ZERO, 0, payload_bytes)
            .expect("test WAL record should fit size limits")
    }

    /// Open a writer, apply `f`, then shut it down and return the dir.
    fn write_wal<F>(dir: &TempDir, f: F)
    where
        F: FnOnce(&Arc<WalBuffer>),
    {
        let buffer = Arc::new(WalBuffer::new(4 * 1024 * 1024, Lsn::ZERO));
        let writer = WalWriter::open(
            dir.path(),
            Arc::clone(&buffer),
            WalWriterConfig {
                segment_size_bytes: 16 * 1024 * 1024,
                fsync_window_us: 100,
                fsync_batch_bytes: 1,
            },
        )
        .unwrap();
        f(&buffer);
        writer.notify();
        writer.shutdown().unwrap();
    }

    // ── Test 1: routing ───────────────────────────────────────────────────────

    /// Every record type routes to the correct `MockHeap` method.
    #[test]
    fn dispatch_routes_each_record_type_to_correct_method() {
        use ultrasql_core::constants::PAGE_SIZE;

        let mock = MockHeap::default();

        // HeapInsert
        let insert_payload = HeapInsertPayload {
            tid: tid(1, 0, 0),
            tuple_bytes: b"row".to_vec(),
        };
        let rec = make_record(RecordType::HeapInsert, insert_payload.encode().unwrap());
        dispatch_record(&mock, &rec).unwrap();

        // HeapInsertBatch
        let insert_batch_payload = HeapInsertBatchPayload {
            page: page_id(1, 0),
            entries: vec![HeapInsertBatchEntry {
                slot: 1,
                tuple_bytes: b"row-b".to_vec(),
            }],
        };
        let rec = make_record(
            RecordType::HeapInsertBatch,
            insert_batch_payload.encode().unwrap(),
        );
        dispatch_record(&mock, &rec).unwrap();

        // HeapUpdate
        let update_payload = HeapUpdatePayload {
            old_tid: tid(1, 0, 0),
            new_tid: tid(1, 0, 1),
            flags: 0,
            new_tuple_bytes: b"updated".to_vec(),
        };
        let rec = make_record(RecordType::HeapUpdate, update_payload.encode().unwrap());
        dispatch_record(&mock, &rec).unwrap();

        // HeapUpdateInPlaceBatch
        let update_batch_payload = HeapUpdateInPlaceBatchPayload {
            page: page_id(1, 0),
            writer_xid: Xid::new(88),
            command_id: CommandId::new(2),
            entries: vec![HeapUpdateInPlaceBatchEntry {
                slot: 4,
                pre_image: [0; 9],
                post_image: [1; 9],
            }],
        };
        let rec = make_record(
            RecordType::HeapUpdateInPlaceBatch,
            update_batch_payload.encode().unwrap(),
        );
        dispatch_record(&mock, &rec).unwrap();

        // HeapUpdateInt32PairDeltaBatch
        let update_delta_payload = HeapUpdateInt32PairDeltaBatchPayload {
            page: page_id(1, 0),
            writer_xid: Xid::new(90),
            command_id: CommandId::new(4),
            target_col: 1,
            delta: 7,
            slots: vec![4, 5],
        };
        let rec = make_record(
            RecordType::HeapUpdateInt32PairDeltaBatch,
            update_delta_payload.encode().unwrap(),
        );
        dispatch_record(&mock, &rec).unwrap();

        // HeapUpdateInt32PairDeltaRangeBatch
        let update_delta_range_payload = HeapUpdateInt32PairDeltaRangeBatchPayload {
            page: page_id(1, 0),
            writer_xid: Xid::new(92),
            command_id: CommandId::new(6),
            target_col: 1,
            delta: 7,
            first_slot: 4,
            slot_count: 2,
        };
        let rec = make_record(
            RecordType::HeapUpdateInt32PairDeltaRangeBatch,
            update_delta_range_payload.encode().unwrap(),
        );
        dispatch_record(&mock, &rec).unwrap();

        // HeapDeleteInPlaceBatch
        let delete_batch_payload = HeapDeleteInPlaceBatchPayload {
            page: page_id(1, 0),
            xmax: Xid::new(89),
            cmax: CommandId::new(3),
            entries: vec![HeapDeleteInPlaceBatchEntry { slot: 4 }],
        };
        let rec = make_record(
            RecordType::HeapDeleteInPlaceBatch,
            delete_batch_payload.encode().unwrap(),
        );
        dispatch_record(&mock, &rec).unwrap();

        // HeapDeleteInPlaceRangeBatch
        let delete_range_payload = HeapDeleteInPlaceRangeBatchPayload {
            page: page_id(1, 0),
            xmax: Xid::new(91),
            cmax: CommandId::new(5),
            first_slot: 4,
            slot_count: 2,
        };
        let rec = make_record(
            RecordType::HeapDeleteInPlaceRangeBatch,
            delete_range_payload.encode().unwrap(),
        );
        dispatch_record(&mock, &rec).unwrap();

        // HeapDelete
        let delete_payload = HeapDeletePayload {
            tid: tid(2, 3, 7),
            xmax: Xid::new(99),
            cmax: CommandId::new(1),
        };
        let rec = make_record(RecordType::HeapDelete, delete_payload.encode().unwrap());
        dispatch_record(&mock, &rec).unwrap();

        // FullPageWrite
        let fpw_payload = FullPageWritePayload {
            page: page_id(5, 10),
            page_bytes: vec![0xAB_u8; PAGE_SIZE],
        };
        let rec = make_record(RecordType::FullPageWrite, fpw_payload.encode().unwrap());
        dispatch_record(&mock, &rec).unwrap();

        // Commit
        let commit_payload = CommitPayload {
            commit_lsn: Lsn::new(42),
            commit_timestamp_micros: 12345,
        };
        let rec = make_record(RecordType::Commit, commit_payload.encode());
        dispatch_record(&mock, &rec).unwrap();

        // Abort
        let abort_payload = AbortPayload {
            abort_lsn: Lsn::new(100),
        };
        let rec = make_record(RecordType::Abort, abort_payload.encode());
        dispatch_record(&mock, &rec).unwrap();

        // Checkpoint
        let checkpoint_payload = CheckpointPayload {
            redo_from: Lsn::new(10),
            oldest_in_progress: Xid::new(5),
            next_xid: Xid::new(50),
        };
        let rec = make_record(RecordType::Checkpoint, checkpoint_payload.encode());
        dispatch_record(&mock, &rec).unwrap();

        // BTreeOp
        let btree_payload = BTreeOpPayload {
            op: BTreeOpKind::Insert,
            index_rel: RelationId::new(44),
            page: PageId::new(RelationId::new(44), BlockNumber::new(0)),
            key_bytes: 7_i64.to_le_bytes().to_vec(),
            child_or_value: vec![0_u8; 12],
        };
        let rec = make_record(RecordType::BTreeOp, btree_payload.encode().unwrap());
        dispatch_record(&mock, &rec).unwrap();

        // HashOp
        let hash_payload = HashOpPayload {
            op: HashOpKind::Insert,
            index_rel: RelationId::new(77),
            bucket: 3,
            page: PageId::new(RelationId::new(77), BlockNumber::new(1)),
            key_hash: 0xABCD,
            key_bytes: 7_i64.to_le_bytes().to_vec(),
            value_bytes: vec![0_u8; 12],
        };
        let rec = make_record(RecordType::HashOp, hash_payload.encode().unwrap());
        dispatch_record(&mock, &rec).unwrap();

        // SequenceOp
        let sequence_payload = SequenceOpPayload {
            op: SequenceOpKind::Advance,
            seqrelid: RelationId::new(55),
            name: "orders_id_seq".to_owned(),
            start_value: 1,
            last_value: 2,
            min_value: 1,
            max_value: i64::MAX,
            increment: 1,
            cache_size: 1,
            is_called: true,
            cycle: false,
        };
        let rec = make_record(RecordType::SequenceOp, sequence_payload.encode().unwrap());
        dispatch_record(&mock, &rec).unwrap();

        // Nop — silently ignored
        let rec = make_record(RecordType::Nop, vec![]);
        dispatch_record(&mock, &rec).unwrap();

        assert_eq!(mock.inserts.lock().len(), 1);
        assert_eq!(mock.insert_batches.lock().len(), 1);
        assert_eq!(mock.updates.lock().len(), 1);
        assert_eq!(mock.update_in_place_batches.lock().len(), 1);
        assert_eq!(mock.update_int32_pair_delta_batches.lock().len(), 1);
        assert_eq!(mock.update_int32_pair_delta_range_batches.lock().len(), 1);
        assert_eq!(mock.delete_in_place_batches.lock().len(), 1);
        assert_eq!(mock.delete_in_place_range_batches.lock().len(), 1);
        assert_eq!(mock.deletes.lock().len(), 1);
        assert_eq!(mock.btree_ops.lock().len(), 1);
        assert_eq!(mock.hash_ops.lock().len(), 1);
        assert_eq!(mock.sequence_ops.lock().len(), 1);
        assert_eq!(mock.fpws.lock().len(), 1);
        assert_eq!(mock.commits.lock().len(), 1);
        assert_eq!(mock.aborts.lock().len(), 1);
        assert_eq!(mock.checkpoints.lock().len(), 1);
    }

    #[test]
    fn dispatch_record_at_lsn_passes_record_lsn_to_target() {
        let mock = MockHeap::default();
        let payload = HeapInsertPayload {
            tid: tid(1, 0, 0),
            tuple_bytes: b"row".to_vec(),
        };
        let rec = make_record(RecordType::HeapInsert, payload.encode().unwrap());

        dispatch_record_at_lsn(&mock, &rec, Lsn::new(4096)).unwrap();

        assert_eq!(mock.inserts.lock().len(), 1);
        assert_eq!(mock.insert_lsns.lock().as_slice(), &[Lsn::new(4096)]);
    }

    // ── Test 2: decode failure surfaces as ApplyError::Payload ────────────────

    /// Corrupted payload bytes produce `ApplyError::Payload`.
    #[test]
    fn dispatch_surfaces_decode_failure_as_apply_error() {
        let mock = MockHeap::default();
        // A valid HeapInsert has at least TID_SIZE + 4 = 16 bytes. Pass empty
        // bytes to force a truncation error in the payload decoder.
        let rec = make_record(RecordType::HeapInsert, vec![]);
        let err = dispatch_record(&mock, &rec).unwrap_err();
        assert!(
            matches!(err, ApplyError::Payload(_)),
            "expected Payload error, got {err:?}"
        );
    }

    // ── Test 3: default impls return Refused ─────────────────────────────────

    /// A `HeapTarget` that inherits all default methods refuses every
    /// heap-mutating operation with `ApplyError::Refused`.
    #[test]
    fn default_impls_return_refused() {
        struct NullTarget;
        impl HeapTarget for NullTarget {}

        let target = NullTarget;
        let insert_payload = HeapInsertPayload {
            tid: tid(1, 0, 0),
            tuple_bytes: vec![],
        };
        let rec = make_record(RecordType::HeapInsert, insert_payload.encode().unwrap());
        let err = dispatch_record(&target, &rec).unwrap_err();
        assert!(
            matches!(
                err,
                ApplyError::Refused {
                    operation: "heap_insert",
                    ..
                }
            ),
            "expected Refused(heap_insert), got {err:?}"
        );
    }

    // ── Test 4: full integration round-trip ──────────────────────────────────

    /// Write 3 `HeapInsert` + 1 `Commit` + 1 `HeapDelete` through the real WAL writer,
    /// then replay via `replay_into` and assert the mock received them all.
    #[test]
    fn replay_into_walks_segments_and_applies_each_record() {
        let dir = TempDir::new().unwrap();

        let insert_payload = HeapInsertPayload {
            tid: tid(1, 0, 0),
            tuple_bytes: b"tuple-a".to_vec(),
        };
        let insert_payload2 = HeapInsertPayload {
            tid: tid(1, 0, 1),
            tuple_bytes: b"tuple-b".to_vec(),
        };
        let insert_payload3 = HeapInsertPayload {
            tid: tid(1, 0, 2),
            tuple_bytes: b"tuple-c".to_vec(),
        };
        let commit_payload = CommitPayload {
            commit_lsn: Lsn::new(0),
            commit_timestamp_micros: 0,
        };
        let delete_payload = HeapDeletePayload {
            tid: tid(1, 0, 0),
            xmax: Xid::new(7),
            cmax: CommandId::new(1),
        };

        write_wal(&dir, |buf| {
            buf.append(&make_record(
                RecordType::HeapInsert,
                insert_payload.encode().unwrap(),
            ))
            .unwrap();
            buf.append(&make_record(
                RecordType::HeapInsert,
                insert_payload2.encode().unwrap(),
            ))
            .unwrap();
            buf.append(&make_record(
                RecordType::HeapInsert,
                insert_payload3.encode().unwrap(),
            ))
            .unwrap();
            buf.append(&make_record(RecordType::Commit, commit_payload.encode()))
                .unwrap();
            buf.append(&make_record(
                RecordType::HeapDelete,
                delete_payload.encode().unwrap(),
            ))
            .unwrap();
        });

        let mock = MockHeap::default();
        let final_lsn = replay_into(dir.path(), &mock).unwrap();

        assert!(
            final_lsn > Lsn::ZERO,
            "expected non-zero final LSN; got {final_lsn:?}"
        );
        assert_eq!(mock.inserts.lock().len(), 3, "expected 3 inserts");
        let insert_lsns = mock.insert_lsns.lock().clone();
        assert_eq!(insert_lsns.len(), 3, "expected 3 insert LSNs");
        assert_eq!(insert_lsns[0], Lsn::ZERO);
        assert!(insert_lsns[1] > insert_lsns[0]);
        assert!(insert_lsns[2] > insert_lsns[1]);
        assert_eq!(mock.commits.lock().len(), 1, "expected 1 commit");
        assert_eq!(mock.deletes.lock().len(), 1, "expected 1 delete");
    }

    // ── Test 5: empty WAL ─────────────────────────────────────────────────────

    /// An empty WAL directory returns `Lsn::ZERO` and dispatches nothing.
    #[test]
    fn replay_returns_lsn_zero_for_empty_wal() {
        let dir = TempDir::new().unwrap();
        let mock = MockHeap::default();
        let lsn = replay_into(dir.path(), &mock).unwrap();
        assert_eq!(lsn, Lsn::ZERO);
        assert_eq!(mock.inserts.lock().len(), 0);
        assert_eq!(mock.commits.lock().len(), 0);
        assert_eq!(mock.deletes.lock().len(), 0);
    }

    #[test]
    fn replay_lsn_advance_rejects_overflow() {
        let err = super::advance_replay_lsn(Lsn::new(u64::MAX), 1)
            .expect_err("replay LSN overflow must not saturate");
        assert!(
            matches!(
                err,
                RecoveryError::Record(WalRecordError::Malformed("replay lsn overflow"))
            ),
            "{err:?}"
        );
    }

    // ── Test 6: proptest round-trip ───────────────────────────────────────────

    proptest! {
        /// Any vector of well-formed `HeapInsertPayload`s written through the WAL
        /// writer round-trips through `replay_into` in the same order with
        /// identical payloads.
        #[test]
        fn proptest_insert_payloads_round_trip_through_wal(
            records in proptest::collection::vec(
                (
                    0_u32..0x00FF_FFFFu32, // block (24-bit)
                    0_u16..1024_u16,       // slot
                    proptest::collection::vec(any::<u8>(), 0..256_usize), // tuple_bytes
                ),
                1..16_usize,
            )
        ) {
            let dir = TempDir::new().unwrap();

            let payloads: Vec<HeapInsertPayload> = records
                .into_iter()
                .map(|(block, slot, tuple_bytes)| HeapInsertPayload {
                    tid: tid(1, block, slot),
                    tuple_bytes,
                })
                .collect();

            let payloads_clone = payloads.clone();
            write_wal(&dir, |buf| {
                for p in &payloads_clone {
                    let record = make_record(RecordType::HeapInsert, p.encode().unwrap());
                    buf.append(&record).unwrap();
                }
            });

            let mock = MockHeap::default();
            let lsn = replay_into(dir.path(), &mock).unwrap();
            prop_assert!(lsn > Lsn::ZERO);

            let replayed = mock.inserts.lock();
            prop_assert_eq!(replayed.len(), payloads.len());
            for (expected, actual) in payloads.iter().zip(replayed.iter()) {
                prop_assert_eq!(expected, actual);
            }
        }
    }
}
