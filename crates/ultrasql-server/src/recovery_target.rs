//! WAL-replay heap/sequence target used during recovery, plus `TxnState`
//! wire-status mapping.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

pub(crate) struct ServerRecoveryTarget {
    pub(crate) heap: Arc<HeapAccess<BlankPageLoader>>,
    pub(crate) sequences: Arc<dashmap::DashMap<String, Arc<Sequence>>>,
}

impl ServerRecoveryTarget {
    fn sequence_snapshot(payload: &SequenceOpPayload) -> SequenceSnapshot {
        SequenceSnapshot {
            start_value: payload.start_value,
            last_value: payload.last_value,
            is_called: payload.is_called,
            min_value: payload.min_value,
            max_value: payload.max_value,
            increment: payload.increment,
            cycle: payload.cycle,
            cache_size: payload.cache_size,
        }
    }
}

impl HeapTarget for ServerRecoveryTarget {
    fn apply_insert(&self, payload: &HeapInsertPayload) -> Result<(), ApplyError> {
        HeapTarget::apply_insert(self.heap.as_ref(), payload)
    }

    fn apply_insert_batch(&self, payload: &HeapInsertBatchPayload) -> Result<(), ApplyError> {
        HeapTarget::apply_insert_batch(self.heap.as_ref(), payload)
    }

    fn apply_insert_batch_at_lsn(
        &self,
        payload: &HeapInsertBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        HeapTarget::apply_insert_batch_at_lsn(self.heap.as_ref(), payload, record_lsn)
    }

    fn apply_update(&self, payload: &HeapUpdatePayload) -> Result<(), ApplyError> {
        HeapTarget::apply_update(self.heap.as_ref(), payload)
    }

    fn apply_delete(&self, payload: &HeapDeletePayload) -> Result<(), ApplyError> {
        HeapTarget::apply_delete(self.heap.as_ref(), payload)
    }

    fn apply_update_in_place(&self, payload: &HeapUpdateInPlacePayload) -> Result<(), ApplyError> {
        HeapTarget::apply_update_in_place(self.heap.as_ref(), payload)
    }

    fn apply_update_in_place_batch(
        &self,
        payload: &HeapUpdateInPlaceBatchPayload,
    ) -> Result<(), ApplyError> {
        HeapTarget::apply_update_in_place_batch(self.heap.as_ref(), payload)
    }

    fn apply_update_int32_pair_delta_batch(
        &self,
        payload: &HeapUpdateInt32PairDeltaBatchPayload,
    ) -> Result<(), ApplyError> {
        HeapTarget::apply_update_int32_pair_delta_batch_at_lsn(
            self.heap.as_ref(),
            payload,
            Lsn::ZERO,
        )
    }

    fn apply_update_int32_pair_delta_batch_at_lsn(
        &self,
        payload: &HeapUpdateInt32PairDeltaBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        HeapTarget::apply_update_int32_pair_delta_batch_at_lsn(
            self.heap.as_ref(),
            payload,
            record_lsn,
        )
    }

    fn apply_update_int32_pair_delta_range_batch(
        &self,
        payload: &HeapUpdateInt32PairDeltaRangeBatchPayload,
    ) -> Result<(), ApplyError> {
        HeapTarget::apply_update_int32_pair_delta_range_batch_at_lsn(
            self.heap.as_ref(),
            payload,
            Lsn::ZERO,
        )
    }

    fn apply_update_int32_pair_delta_range_batch_at_lsn(
        &self,
        payload: &HeapUpdateInt32PairDeltaRangeBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        HeapTarget::apply_update_int32_pair_delta_range_batch_at_lsn(
            self.heap.as_ref(),
            payload,
            record_lsn,
        )
    }

    fn apply_delete_in_place(&self, payload: &HeapDeleteInPlacePayload) -> Result<(), ApplyError> {
        HeapTarget::apply_delete_in_place(self.heap.as_ref(), payload)
    }

    fn apply_delete_in_place_batch(
        &self,
        payload: &HeapDeleteInPlaceBatchPayload,
    ) -> Result<(), ApplyError> {
        HeapTarget::apply_delete_in_place_batch(self.heap.as_ref(), payload)
    }

    fn apply_delete_in_place_range_batch(
        &self,
        payload: &HeapDeleteInPlaceRangeBatchPayload,
    ) -> Result<(), ApplyError> {
        HeapTarget::apply_delete_in_place_range_batch_at_lsn(self.heap.as_ref(), payload, Lsn::ZERO)
    }

    fn apply_delete_in_place_range_batch_at_lsn(
        &self,
        payload: &HeapDeleteInPlaceRangeBatchPayload,
        record_lsn: Lsn,
    ) -> Result<(), ApplyError> {
        HeapTarget::apply_delete_in_place_range_batch_at_lsn(
            self.heap.as_ref(),
            payload,
            record_lsn,
        )
    }

    fn apply_full_page_write(&self, payload: &FullPageWritePayload) -> Result<(), ApplyError> {
        HeapTarget::apply_full_page_write(self.heap.as_ref(), payload)
    }

    fn apply_btree_op(&self, payload: &BTreeOpPayload) -> Result<(), ApplyError> {
        HeapTarget::apply_btree_op(self.heap.as_ref(), payload)
    }

    fn apply_sequence_op(&self, payload: &SequenceOpPayload) -> Result<(), ApplyError> {
        let name = payload.name.to_ascii_lowercase();
        if payload.op == SequenceOpKind::Drop {
            self.sequences.remove(&name);
            return Ok(());
        }
        let snapshot = Self::sequence_snapshot(payload);
        if let Some(existing) = self.sequences.get(&name) {
            existing
                .apply_snapshot(snapshot)
                .map_err(|e| ApplyError::Refused {
                    operation: "sequence_replay",
                    detail: e.to_string(),
                })?;
            return Ok(());
        }
        let seq = Sequence::from_snapshot(snapshot).map_err(|e| ApplyError::Refused {
            operation: "sequence_replay",
            detail: e.to_string(),
        })?;
        self.sequences.insert(name, Arc::new(seq));
        Ok(())
    }

    fn observe_commit(&self, payload: &CommitPayload) -> Result<(), ApplyError> {
        HeapTarget::observe_commit(self.heap.as_ref(), payload)
    }

    fn observe_abort(&self, payload: &AbortPayload) -> Result<(), ApplyError> {
        HeapTarget::observe_abort(self.heap.as_ref(), payload)
    }

    fn observe_checkpoint(&self, payload: &CheckpointPayload) -> Result<(), ApplyError> {
        HeapTarget::observe_checkpoint(self.heap.as_ref(), payload)
    }
}

impl TxnState {
    /// The PostgreSQL `ReadyForQuery` status byte for this state.
    #[must_use]
    pub const fn ready_for_query_status(&self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransaction(_) => b'T',
            Self::Failed(_) => b'E',
        }
    }
}
