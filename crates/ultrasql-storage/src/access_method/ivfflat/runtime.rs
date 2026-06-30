//! Runtime in-memory IVFFlat index implementation.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use super::*;

impl IvfFlatIndex {
    /// Create an empty runtime IVFFlat index.
    pub fn new(
        dims: u32,
        metric: HnswMetric,
        lists: usize,
        probes: usize,
    ) -> Result<Self, AccessMethodError> {
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return Err(AccessMethodError::Storage(
                "ivfflat dims outside supported range".to_owned(),
            ));
        }
        if lists == 0 {
            return Err(AccessMethodError::Storage(
                "ivfflat lists must be greater than zero".to_owned(),
            ));
        }
        if probes == 0 {
            return Err(AccessMethodError::Storage(
                "ivfflat probes must be greater than zero".to_owned(),
            ));
        }
        let dims = usize::try_from(dims)
            .map_err(|_| AccessMethodError::Storage("ivfflat dims do not fit usize".to_owned()))?;
        Ok(Self {
            storage: Mutex::new(IvfFlatStorage::default()),
            dims,
            metric,
            lists,
            probes,
        })
    }

    /// Return this index's distance metric.
    #[must_use]
    pub const fn metric(&self) -> HnswMetric {
        self.metric
    }

    /// Return this index's vector dimension.
    #[must_use]
    pub const fn dims(&self) -> usize {
        self.dims
    }

    /// Return configured probe count.
    #[must_use]
    pub const fn probes(&self) -> usize {
        self.probes
    }

    /// Return number of trained centroids.
    #[must_use]
    pub fn centroid_count(&self) -> usize {
        self.storage.lock().centroids.len()
    }

    /// Return number of inverted lists currently materialized.
    #[must_use]
    pub fn list_count(&self) -> usize {
        self.storage.lock().lists.len()
    }

    /// Return number of live entries.
    #[must_use]
    pub fn live_len(&self) -> usize {
        self.storage
            .lock()
            .entries
            .iter()
            .filter(|entry| !entry.deleted)
            .count()
    }

    /// Return number of tombstoned entries awaiting compaction.
    #[must_use]
    pub fn tombstone_count(&self) -> usize {
        self.storage
            .lock()
            .entries
            .iter()
            .filter(|entry| entry.deleted)
            .count()
    }

    /// Return whether the runtime IVFFlat lists can currently be used.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.storage.lock().available
    }

    /// Train centroids and bulk-load vectors into inverted lists.
    pub fn bulk_load(&self, rows: Vec<(Vec<f32>, TupleId)>) -> Result<(), AccessMethodError> {
        let mut seen_tids = BTreeSet::new();
        for (vector, tid) in &rows {
            self.validate_vector(vector)?;
            if !seen_tids.insert(*tid) {
                return Err(AccessMethodError::DuplicateKey);
            }
        }
        let mut storage = self.storage.lock();
        storage.entries.clear();
        storage.centroids.clear();
        storage.lists.clear();
        storage.available = false;
        if rows.is_empty() {
            return Ok(());
        }

        let centroid_count = self.lists.min(rows.len());
        storage.centroids = self.train_centroids(&rows, centroid_count);
        storage.lists = vec![Vec::new(); storage.centroids.len()];
        for (vector, tid) in rows {
            let list_id =
                nearest_vector(&storage.centroids, &vector, self.metric).ok_or_else(|| {
                    AccessMethodError::Storage("ivfflat centroids missing".to_owned())
                })?;
            let idx = storage.entries.len();
            storage.entries.push(IvfFlatEntry {
                vector,
                tid,
                list_id,
                deleted: false,
            });
            storage.lists[list_id].push(idx);
        }
        storage.available = true;
        Ok(())
    }

    /// Insert one vector into the nearest trained list.
    pub fn insert_vector(&self, vector: &[f32], tid: TupleId) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        let mut storage = self.storage.lock();
        if storage.centroids.is_empty() {
            storage.centroids.push(vector.to_vec());
            storage.lists.push(Vec::new());
        }
        let list_id = nearest_vector(&storage.centroids, vector, self.metric)
            .ok_or_else(|| AccessMethodError::Storage("ivfflat centroids missing".to_owned()))?;
        let idx = storage.entries.len();
        storage.entries.push(IvfFlatEntry {
            vector: vector.to_vec(),
            tid,
            list_id,
            deleted: false,
        });
        storage.lists[list_id].push(idx);
        storage.available = true;
        Ok(())
    }

    /// Search nearest `k` tuples by probing nearest inverted lists.
    pub fn search(
        &self,
        probe: &[f32],
        k: usize,
    ) -> Result<Vec<IvfFlatSearchResult>, AccessMethodError> {
        self.validate_vector(probe)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let storage = self.storage.lock();
        if !storage.available || storage.centroids.is_empty() {
            return Ok(Vec::new());
        }
        let list_ids = nearest_vectors(&storage.centroids, probe, self.metric, self.probes);
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
        let mut storage = self.storage.lock();
        if let Some(entry) = storage
            .entries
            .iter_mut()
            .find(|entry| entry.tid == tid && !entry.deleted)
        {
            entry.deleted = true;
            return Ok(());
        }
        Err(AccessMethodError::NotFound)
    }

    /// Compact tombstoned entries out of inverted lists.
    pub fn compact_deleted(&self) -> Result<usize, AccessMethodError> {
        let mut storage = self.storage.lock();
        let before = storage.entries.len();
        if before == 0 {
            return Ok(0);
        }
        let mut remap = vec![None; before];
        let mut entries = Vec::with_capacity(before);
        for (old_idx, entry) in storage.entries.iter().enumerate() {
            if entry.deleted {
                continue;
            }
            remap[old_idx] = Some(entries.len());
            entries.push(IvfFlatEntry {
                vector: entry.vector.clone(),
                tid: entry.tid,
                list_id: entry.list_id,
                deleted: false,
            });
        }
        let removed = before.saturating_sub(entries.len());
        if removed == 0 {
            return Ok(0);
        }
        let mut lists = vec![Vec::new(); storage.centroids.len()];
        for entry in &entries {
            if entry.list_id >= lists.len() {
                return Err(AccessMethodError::Storage(
                    "ivfflat compact found invalid list id".to_owned(),
                ));
            }
        }
        for old_list in &storage.lists {
            for old_idx in old_list {
                if let Some(new_idx) = remap.get(*old_idx).and_then(|idx| *idx) {
                    let list_id = entries[new_idx].list_id;
                    lists[list_id].push(new_idx);
                }
            }
        }
        storage.entries = entries;
        storage.lists = lists;
        storage.available = !storage.entries.is_empty() && !storage.centroids.is_empty();
        Ok(removed)
    }

    pub(crate) fn validate_vector(&self, vector: &[f32]) -> Result<(), AccessMethodError> {
        crate::access_method::validate_vector_dims_finite(vector, self.dims, "ivfflat")
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
}

impl AccessMethod for IvfFlatIndex {
    fn name(&self) -> &'static str {
        "ivfflat"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let vector = decode_vector_key(key, self.dims, "ivfflat")?;
        self.insert_vector(&vector, tid)
    }

    fn lookup(&self, _key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        Err(AccessMethodError::NotImplemented(
            "ivfflat lookup requires vector top-k search",
        ))
    }

    fn delete(&self, _key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        self.mark_deleted(tid)
    }
}
