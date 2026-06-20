//! `PageBackedIvfFlatIndex` DML, search, and WAL-apply operations.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use super::*;

impl PageBackedIvfFlatIndex {
    /// Train centroids and bulk-load vectors into page-backed lists.
    pub fn bulk_load(&self, rows: Vec<(Vec<f32>, TupleId)>) -> Result<(), AccessMethodError> {
        self.bulk_load_logged(rows, Xid::FIRST_USER, None)
    }

    /// Train centroids, bulk-load vectors, and emit logical IVFFlat WAL.
    pub fn bulk_load_logged(
        &self,
        rows: Vec<(Vec<f32>, TupleId)>,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        let mut seen_tids = BTreeSet::new();
        for (vector, tid) in &rows {
            self.validate_vector(vector)?;
            if !seen_tids.insert(*tid) {
                return Err(AccessMethodError::DuplicateKey);
            }
        }
        {
            let mut storage = self.storage.lock();
            storage.clear(self.page_context())?;
        }
        if rows.is_empty() {
            return Ok(());
        }

        let centroid_count = self.lists.min(rows.len());
        let centroids = self.train_centroids(&rows, centroid_count);
        for (list_id, centroid) in centroids.iter().enumerate() {
            let page_lsn =
                self.emit_ivfflat_wal(IvfFlatOpKind::Centroid, list_id, None, centroid, xid, wal)?;
            self.apply_centroid_internal(list_id, centroid, false, page_lsn)?;
        }
        for (vector, tid) in rows {
            let list_id = nearest_vector(&centroids, &vector, self.metric).ok_or_else(|| {
                AccessMethodError::Storage("page-backed ivfflat centroids missing".to_owned())
            })?;
            let page_lsn = self.emit_ivfflat_wal(
                IvfFlatOpKind::Insert,
                list_id,
                Some(tid),
                &vector,
                xid,
                wal,
            )?;
            self.apply_insert_internal(list_id, &vector, tid, false, page_lsn)?;
        }
        Ok(())
    }

    /// Insert one vector into the nearest trained page-backed list.
    pub fn insert_vector(&self, vector: &[f32], tid: TupleId) -> Result<(), AccessMethodError> {
        self.insert_vector_logged(vector, tid, Xid::FIRST_USER, None)
    }

    /// Insert one vector and emit logical IVFFlat WAL.
    pub fn insert_vector_logged(
        &self,
        vector: &[f32],
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        let mut centroids = self.storage.lock().centroids.clone();
        if centroids.is_empty() {
            let page_lsn =
                self.emit_ivfflat_wal(IvfFlatOpKind::Centroid, 0, None, vector, xid, wal)?;
            self.apply_centroid_internal(0, vector, false, page_lsn)?;
            centroids.push(vector.to_vec());
        }
        let list_id = nearest_vector(&centroids, vector, self.metric).ok_or_else(|| {
            AccessMethodError::Storage("page-backed ivfflat centroids missing".to_owned())
        })?;
        let page_lsn =
            self.emit_ivfflat_wal(IvfFlatOpKind::Insert, list_id, Some(tid), vector, xid, wal)?;
        self.apply_insert_internal(list_id, vector, tid, false, page_lsn)
    }

    /// Search nearest `k` tuples by probing nearest page-backed lists.
    pub fn search(
        &self,
        probe: &[f32],
        k: usize,
    ) -> Result<Vec<IvfFlatSearchResult>, AccessMethodError> {
        self.search_with_probes(probe, k, self.probes)
    }

    /// Search probing a caller-supplied number of nearest lists, overriding the
    /// index default `probes`.
    ///
    /// A larger `probes` scans more inverted lists — trading latency for recall
    /// — the per-query knob filtered ANN uses to over-fetch candidates before a
    /// metadata predicate prunes them. Probing every list
    /// (`probes >= list_count`) is exact. This is the IVFFlat analog of HNSW's
    /// `search_with_ef`.
    pub fn search_with_probes(
        &self,
        probe: &[f32],
        k: usize,
        probes: usize,
    ) -> Result<Vec<IvfFlatSearchResult>, AccessMethodError> {
        self.validate_vector(probe)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let probes = probes.max(1);
        let storage = self.storage.lock();
        if !storage.valid || storage.centroids.is_empty() {
            return Ok(Vec::new());
        }
        let list_ids = nearest_vectors(&storage.centroids, probe, self.metric, probes);
        let mut candidate_indices = Vec::new();
        for list_id in list_ids {
            let Some(list) = storage.lists.get(list_id) else {
                continue;
            };
            candidate_indices.extend(list.iter().copied().filter(|idx| {
                storage
                    .entries
                    .get(*idx)
                    .is_some_and(|entry| !entry.deleted)
            }));
        }
        if candidate_indices.is_empty() {
            return Ok(Vec::new());
        }
        let vectors: Vec<&[f32]> = candidate_indices
            .iter()
            .map(|idx| storage.entries[*idx].vector.as_slice())
            .collect();
        let hits = ultrasql_vec::kernels::vector::exact_top_k_f32(
            &vectors,
            probe,
            self.metric.vector_metric(),
            k,
        );
        let mut out: Vec<IvfFlatSearchResult> = hits
            .into_iter()
            .map(|hit| {
                let entry = &storage.entries[candidate_indices[hit.row]];
                IvfFlatSearchResult {
                    tid: entry.tid,
                    distance: hit.distance,
                }
            })
            .collect();
        out.sort_by(compare_ivfflat_hits);
        Ok(out)
    }

    /// Mark an indexed tuple ID deleted.
    pub fn mark_deleted(&self, tid: TupleId) -> Result<(), AccessMethodError> {
        self.mark_deleted_logged(tid, Xid::FIRST_USER, None)
    }

    /// Mark an indexed tuple ID deleted and emit logical IVFFlat WAL.
    pub fn mark_deleted_logged(
        &self,
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        let page_lsn = self.emit_ivfflat_wal(IvfFlatOpKind::Delete, 0, Some(tid), &[], xid, wal)?;
        let mut storage = self.storage.lock();
        storage.mark_deleted(self.page_context(), tid, false, page_lsn)
    }

    /// Compact tombstoned entries out of page-backed lists.
    pub fn compact_deleted(&self) -> Result<usize, AccessMethodError> {
        self.compact_deleted_logged(Xid::FIRST_USER, None)
    }

    /// Compact tombstoned entries and emit logical IVFFlat WAL.
    pub fn compact_deleted_logged(
        &self,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<usize, AccessMethodError> {
        if self.tombstone_count() == 0 {
            return Ok(0);
        }
        let page_lsn = self.emit_ivfflat_wal(IvfFlatOpKind::Compact, 0, None, &[], xid, wal)?;
        let mut storage = self.storage.lock();
        storage.compact_deleted(self.page_context(), page_lsn)
    }

    /// Replay one decoded logical IVFFlat WAL payload into this page arena.
    pub fn apply_wal_payload(&self, payload: &IvfFlatOpPayload) -> Result<(), AccessMethodError> {
        self.apply_wal_payload_at(Lsn::ZERO, payload)
    }

    /// Replay one decoded logical IVFFlat WAL payload at its assigned WAL LSN.
    pub fn apply_wal_payload_at(
        &self,
        lsn: Lsn,
        payload: &IvfFlatOpPayload,
    ) -> Result<(), AccessMethodError> {
        if payload.index_rel != self.index_rel {
            return Ok(());
        }
        {
            // Skip records a loaded snapshot already covers (mirrors HNSW). Without
            // this gate, replaying a pre-snapshot record on top of a post-compaction
            // snapshot could resurrect a compacted-away entry.
            let storage = self.storage.lock();
            if !storage.valid || storage.redo_covered(lsn) {
                return Ok(());
            }
        }
        let list_id = usize::try_from(payload.list_id).map_err(|_| {
            AccessMethodError::Storage("page-backed ivfflat list_id overflow".to_owned())
        })?;
        match payload.op {
            IvfFlatOpKind::Centroid => {
                self.apply_centroid_internal(list_id, &payload.vector, true, lsn)
            }
            IvfFlatOpKind::Insert => {
                self.apply_insert_internal(list_id, &payload.vector, payload.tid, true, lsn)
            }
            IvfFlatOpKind::Delete => {
                let mut storage = self.storage.lock();
                storage.mark_deleted(self.page_context(), payload.tid, true, lsn)
            }
            IvfFlatOpKind::Compact => {
                let mut storage = self.storage.lock();
                storage
                    .compact_deleted(self.page_context(), lsn)
                    .map(|_| ())
            }
        }
    }

    /// Replay one WAL record, ignoring records that are not IVFFlat mutations.
    pub fn apply_wal_record(&self, record: &WalRecord) -> Result<(), AccessMethodError> {
        self.apply_wal_record_at(Lsn::ZERO, record)
    }

    /// Replay one WAL record at its assigned WAL LSN.
    pub fn apply_wal_record_at(
        &self,
        lsn: Lsn,
        record: &WalRecord,
    ) -> Result<(), AccessMethodError> {
        if record.header.record_type != RecordType::IvfFlatOp {
            return Ok(());
        }
        if let Some(index_rel) = ann_wal_index_rel(&record.payload, "ivfflat")?
            && index_rel != self.index_rel
        {
            return Ok(());
        }
        let payload = IvfFlatOpPayload::decode(&record.payload)
            .map_err(|e| AccessMethodError::Storage(format!("decode ivfflat WAL payload: {e}")))?;
        self.apply_wal_payload_at(lsn, &payload)
    }

    pub(crate) fn apply_centroid_internal(
        &self,
        list_id: usize,
        vector: &[f32],
        replay: bool,
        page_lsn: Lsn,
    ) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        let mut storage = self.storage.lock();
        if let Some(existing) = storage.centroids.get(list_id) {
            if existing == vector {
                return Ok(());
            }
            if replay {
                storage.centroids[list_id] = vector.to_vec();
                storage.sync_pages(self.page_context(), page_lsn)?;
                return Ok(());
            }
            return Err(AccessMethodError::DuplicateKey);
        }
        storage.ensure_list_slot(list_id)?;
        storage.centroids[list_id] = vector.to_vec();
        storage.sync_pages(self.page_context(), page_lsn)
    }

    pub(crate) fn apply_insert_internal(
        &self,
        list_id: usize,
        vector: &[f32],
        tid: TupleId,
        replay: bool,
        page_lsn: Lsn,
    ) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        let mut storage = self.storage.lock();
        if storage.tid_to_entry.contains_key(&tid) {
            if replay {
                return Ok(());
            }
            return Err(AccessMethodError::DuplicateKey);
        }
        storage.ensure_list_slot(list_id)?;
        if storage.centroids.get(list_id).is_none() {
            if replay {
                storage.centroids[list_id] = vector.to_vec();
            } else {
                return Err(AccessMethodError::Storage(
                    "page-backed ivfflat insert target list has no centroid".to_owned(),
                ));
            }
        }
        let idx = storage.entries.len();
        storage.entries.push(IvfFlatEntry {
            vector: vector.to_vec(),
            tid,
            list_id,
            deleted: false,
        });
        storage.lists[list_id].push(idx);
        storage.tid_to_entry.insert(tid, idx);
        storage.sync_pages(self.page_context(), page_lsn)
    }

    pub(crate) fn validate_vector(&self, vector: &[f32]) -> Result<(), AccessMethodError> {
        if vector.len() != self.dims {
            return Err(AccessMethodError::Storage(format!(
                "page-backed ivfflat vector dimension mismatch: expected {}, got {}",
                self.dims,
                vector.len()
            )));
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(AccessMethodError::Storage(
                "page-backed ivfflat vector elements must be finite".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn train_centroids(
        &self,
        rows: &[(Vec<f32>, TupleId)],
        centroid_count: usize,
    ) -> Vec<Vec<f32>> {
        let mut centroids: Vec<Vec<f32>> = (0..centroid_count)
            .map(|idx| rows[(idx * rows.len()) / centroid_count].0.clone())
            .collect();
        for _ in 0..8 {
            let mut sums = vec![vec![0.0_f32; self.dims]; centroid_count];
            let mut counts = vec![0_usize; centroid_count];
            for (vector, _) in rows {
                if let Some(list_id) = nearest_vector(&centroids, vector, self.metric) {
                    for (sum, value) in sums[list_id].iter_mut().zip(vector) {
                        *sum += *value;
                    }
                    counts[list_id] += 1;
                }
            }
            for idx in 0..centroid_count {
                let count = counts[idx];
                if count == 0 {
                    continue;
                }
                let denom = count_to_f32(count);
                for value in &mut sums[idx] {
                    *value /= denom;
                }
                centroids[idx] = sums[idx].clone();
            }
        }
        centroids
    }

    pub(crate) fn emit_ivfflat_wal(
        &self,
        op: IvfFlatOpKind,
        list_id: usize,
        tid: Option<TupleId>,
        vector: &[f32],
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<Lsn, AccessMethodError> {
        let Some(sink) = wal else {
            return Ok(Lsn::ZERO);
        };
        let list_id = u32::try_from(list_id).map_err(|_| {
            AccessMethodError::Storage("page-backed ivfflat list_id does not fit u32".to_owned())
        })?;
        let tid = tid
            .unwrap_or_else(|| TupleId::new(PageId::new(self.index_rel, BlockNumber::new(0)), 0));
        let payload = IvfFlatOpPayload {
            op,
            index_rel: self.index_rel,
            tid,
            list_id,
            vector: vector.to_vec(),
        }
        .encode()
        .map_err(|e| {
            AccessMethodError::Storage(format!("page-backed ivfflat WAL payload encode: {e}"))
        })?;
        let prev_lsn = sink.last_lsn_for(xid);
        let record =
            WalRecord::new(RecordType::IvfFlatOp, xid, prev_lsn, 0, payload).map_err(|e| {
                AccessMethodError::Storage(format!("page-backed ivfflat WAL record encode: {e}"))
            })?;
        sink.append(record)
            .map_err(|e| AccessMethodError::Storage(format!("page-backed ivfflat WAL append: {e}")))
    }
}
