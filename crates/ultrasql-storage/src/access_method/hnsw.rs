//! Runtime in-memory HNSW vector index.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use parking_lot::Mutex;
use ultrasql_core::{BlockNumber, MAX_VECTOR_DIMS, PageId, RelationId, TupleId, Xid};
use ultrasql_wal::WalRecord;
use ultrasql_wal::payload::{HnswOpKind, HnswOpPayload};
use ultrasql_wal::record::RecordType;

use super::ann::{HnswMetric, HnswSearchResult};
use super::hnsw_build::{
    HNSW_DEFAULT_EF_CONSTRUCTION, compare_hnsw_hits, decode_vector_key, select_neighbors_heuristic,
};
use super::{AccessMethod, AccessMethodError};
use crate::wal_sink::WalSink;

/// First in-memory HNSW-style vector index.
///
/// This implementation is intentionally runtime-only. It gives the SQL layer
/// a real ANN access-method target behind `CREATE INDEX USING hnsw`, while the
/// production buffer-pool wiring, page-LSN redo checks, MVCC-aware executor
/// path, and rebuild protocol from `docs/hnsw-index-design.md` remain separate
/// storage slices. The graph uses one navigable layer: inserts connect each
/// new vector to its nearest `m` existing live nodes, and searches perform
/// bounded best-first traversal.
///
/// The `available` flag lets callers fall back to exact top-k after DML or
/// restart invalidates the runtime graph.
#[derive(Debug)]
pub struct HnswIndex {
    storage: Mutex<HnswStorage>,
    dims: usize,
    metric: HnswMetric,
    m: usize,
    ef_search: usize,
}

#[derive(Debug, Default)]
struct HnswStorage {
    entries: Vec<HnswEntry>,
    entry_node: Option<usize>,
    available: bool,
}

#[derive(Debug, Clone)]
struct HnswEntry {
    vector: Vec<f32>,
    tid: TupleId,
    neighbors: Vec<usize>,
    deleted: bool,
}

impl HnswIndex {
    /// Create an empty runtime HNSW graph.
    ///
    /// `dims` must be in `1..=MAX_VECTOR_DIMS`; `m` and `ef_search` must be
    /// non-zero. The implementation stores vectors as finite `f32` values.
    pub fn new(
        dims: u32,
        metric: HnswMetric,
        m: usize,
        ef_search: usize,
    ) -> Result<Self, AccessMethodError> {
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return Err(AccessMethodError::Storage(
                "hnsw dims outside supported range".to_owned(),
            ));
        }
        if m == 0 {
            return Err(AccessMethodError::Storage(
                "hnsw m must be greater than zero".to_owned(),
            ));
        }
        if ef_search == 0 {
            return Err(AccessMethodError::Storage(
                "hnsw ef_search must be greater than zero".to_owned(),
            ));
        }
        let dims = usize::try_from(dims)
            .map_err(|_| AccessMethodError::Storage("hnsw dims do not fit usize".to_owned()))?;
        Ok(Self {
            storage: Mutex::new(HnswStorage::default()),
            dims,
            metric,
            m,
            ef_search,
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

    /// Return whether the runtime graph can currently be used.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.storage.lock().available
    }

    /// Return number of live, non-tombstoned nodes in the graph.
    #[must_use]
    pub fn live_len(&self) -> usize {
        self.storage
            .lock()
            .entries
            .iter()
            .filter(|entry| !entry.deleted)
            .count()
    }

    /// Return number of tombstoned nodes awaiting VACUUM compaction.
    #[must_use]
    pub fn tombstone_count(&self) -> usize {
        self.storage
            .lock()
            .entries
            .iter()
            .filter(|entry| entry.deleted)
            .count()
    }

    /// Estimate heap memory currently owned by this runtime graph.
    ///
    /// The value includes the index object, storage vectors, vector payload
    /// capacity, and neighbor-list capacity. It is an in-process accounting
    /// artifact for benchmarks, not an on-disk size contract.
    #[must_use]
    pub fn estimated_memory_bytes(&self) -> usize {
        let storage = self.storage.lock();
        let mut bytes = std::mem::size_of::<Self>() + std::mem::size_of::<HnswStorage>();
        bytes += storage.entries.capacity() * std::mem::size_of::<HnswEntry>();
        for entry in &storage.entries {
            bytes += entry.vector.capacity() * std::mem::size_of::<f32>();
            bytes += entry.neighbors.capacity() * std::mem::size_of::<usize>();
        }
        bytes
    }

    /// Mark the runtime graph unavailable.
    ///
    /// The SQL layer calls this when DML touches a table whose HNSW graph is
    /// not yet maintained online. Later queries then use exact top-k fallback.
    pub fn invalidate(&self) {
        self.storage.lock().available = false;
    }

    /// Insert one finite vector into the graph.
    pub fn insert_vector(&self, vector: &[f32], tid: TupleId) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        let mut storage = self.storage.lock();
        let new_idx = storage.entries.len();
        let mut candidates: Vec<(usize, f32, Vec<f32>)> = storage
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| !entry.deleted)
            .map(|(idx, entry)| {
                (
                    idx,
                    self.metric.distance(vector, &entry.vector),
                    entry.vector.clone(),
                )
            })
            .collect();
        candidates.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        candidates.truncate(HNSW_DEFAULT_EF_CONSTRUCTION.max(self.m));
        let neighbor_ids = select_neighbors_heuristic(&candidates, self.m, self.metric);

        storage.entries.push(HnswEntry {
            vector: vector.to_vec(),
            tid,
            neighbors: neighbor_ids.clone(),
            deleted: false,
        });
        if storage.entry_node.is_none() {
            storage.entry_node = Some(new_idx);
        }
        storage.available = true;

        for neighbor in neighbor_ids {
            if let Some(entry) = storage.entries.get_mut(neighbor)
                && !entry.neighbors.contains(&new_idx)
            {
                entry.neighbors.push(new_idx);
            }
            self.trim_neighbors(&mut storage, neighbor);
        }
        Ok(())
    }

    /// Insert one finite vector and emit an HNSW WAL mutation record when set.
    pub fn insert_vector_logged(
        &self,
        index_rel: RelationId,
        vector: &[f32],
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        self.emit_hnsw_wal(HnswOpKind::Insert, index_rel, tid, vector, xid, wal)?;
        self.insert_vector(vector, tid)
    }

    /// Search for the nearest `k` tuple IDs.
    ///
    /// Returns an empty result when the runtime graph is unavailable so callers
    /// can fall back to exact scan without treating invalidation as an error.
    pub fn search(
        &self,
        probe: &[f32],
        k: usize,
    ) -> Result<Vec<HnswSearchResult>, AccessMethodError> {
        self.search_with_ef(probe, k, self.ef_search)
    }

    /// Search for the nearest `k` tuple IDs with a caller-supplied
    /// `ef_search` exploration budget, overriding the index default.
    ///
    /// A larger `ef_search` explores more graph nodes, trading latency for
    /// recall — the per-query knob that filtered ANN uses to over-fetch
    /// candidates before applying a metadata predicate, and that recall/latency
    /// sweeps use to trace the curve. When `ef_search >= live_count` the search
    /// is exact.
    pub fn search_with_ef(
        &self,
        probe: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<HnswSearchResult>, AccessMethodError> {
        self.validate_vector(probe)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let ef_search = ef_search.max(1);
        let storage = self.storage.lock();
        if !storage.available {
            return Ok(Vec::new());
        }
        let live_count = storage
            .entries
            .iter()
            .filter(|entry| !entry.deleted)
            .count();
        if live_count == 0 {
            return Ok(Vec::new());
        }
        if live_count <= ef_search {
            return Ok(self.exact_search_locked(&storage, probe, k));
        }

        let Some(mut entry_idx) = storage
            .entry_node
            .filter(|idx| {
                storage
                    .entries
                    .get(*idx)
                    .is_some_and(|entry| !entry.deleted)
            })
            .or_else(|| storage.entries.iter().position(|entry| !entry.deleted))
        else {
            return Ok(Vec::new());
        };

        let mut improved = true;
        while improved {
            improved = false;
            let current_distance = self
                .metric
                .distance(probe, &storage.entries[entry_idx].vector);
            for &neighbor in &storage.entries[entry_idx].neighbors {
                let Some(candidate) = storage.entries.get(neighbor) else {
                    continue;
                };
                if candidate.deleted {
                    continue;
                }
                let distance = self.metric.distance(probe, &candidate.vector);
                if distance < current_distance {
                    entry_idx = neighbor;
                    improved = true;
                    break;
                }
            }
        }

        let mut visited = vec![false; storage.entries.len()];
        let mut frontier = vec![entry_idx];
        visited[entry_idx] = true;
        let mut explored = Vec::with_capacity(ef_search.min(live_count));

        while !frontier.is_empty() && explored.len() < ef_search {
            let best_pos = best_frontier_position(&frontier, &storage, probe, self.metric);
            let idx = frontier.swap_remove(best_pos);
            let entry = &storage.entries[idx];
            if !entry.deleted {
                explored.push(idx);
            }
            for &neighbor in &entry.neighbors {
                if neighbor >= visited.len() || visited[neighbor] {
                    continue;
                }
                visited[neighbor] = true;
                if !storage.entries[neighbor].deleted {
                    frontier.push(neighbor);
                }
            }
        }

        let mut hits: Vec<HnswSearchResult> = explored
            .into_iter()
            .map(|idx| {
                let entry = &storage.entries[idx];
                HnswSearchResult {
                    tid: entry.tid,
                    distance: self.metric.distance(probe, &entry.vector),
                }
            })
            .collect();
        hits.sort_by(compare_hnsw_hits);
        hits.truncate(k);
        Ok(hits)
    }

    /// Mark an indexed tuple ID deleted.
    pub fn mark_deleted(&self, tid: TupleId) -> Result<(), AccessMethodError> {
        let mut storage = self.storage.lock();
        if let Some(pos) = storage
            .entries
            .iter()
            .position(|entry| entry.tid == tid && !entry.deleted)
        {
            storage.entries[pos].deleted = true;
            if storage.entry_node == Some(pos) {
                storage.entry_node = storage.entries.iter().position(|entry| !entry.deleted);
            }
            return Ok(());
        }
        Err(AccessMethodError::NotFound)
    }

    /// Mark an indexed tuple ID deleted and emit an HNSW WAL mutation record.
    pub fn mark_deleted_logged(
        &self,
        index_rel: RelationId,
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        let mut storage = self.storage.lock();
        if let Some(pos) = storage
            .entries
            .iter()
            .position(|entry| entry.tid == tid && !entry.deleted)
        {
            self.emit_hnsw_wal(HnswOpKind::Delete, index_rel, tid, &[], xid, wal)?;
            storage.entries[pos].deleted = true;
            if storage.entry_node == Some(pos) {
                storage.entry_node = storage.entries.iter().position(|entry| !entry.deleted);
            }
            return Ok(());
        }
        Err(AccessMethodError::NotFound)
    }

    /// Compact tombstoned nodes out of the graph, preserving live reachability.
    pub fn compact_deleted(&self) -> Result<usize, AccessMethodError> {
        let mut storage = self.storage.lock();
        Ok(self.compact_deleted_locked(&mut storage))
    }

    /// Compact tombstoned nodes and emit an HNSW WAL mutation record when set.
    pub fn compact_deleted_logged(
        &self,
        index_rel: RelationId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<usize, AccessMethodError> {
        let mut storage = self.storage.lock();
        let removed = storage.entries.iter().filter(|entry| entry.deleted).count();
        if removed == 0 {
            return Ok(0);
        }
        let tid = TupleId::new(PageId::new(index_rel, BlockNumber::new(0)), 0);
        self.emit_hnsw_wal(HnswOpKind::Compact, index_rel, tid, &[], xid, wal)?;
        Ok(self.compact_deleted_locked(&mut storage))
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<(), AccessMethodError> {
        crate::access_method::validate_vector_dims_finite(vector, self.dims, "hnsw")
    }

    fn compact_deleted_locked(&self, storage: &mut HnswStorage) -> usize {
        let before = storage.entries.len();
        if before == 0 {
            return 0;
        }
        let mut remap = vec![None; before];
        let mut entries = Vec::with_capacity(before);
        for (old_idx, entry) in storage.entries.iter().enumerate() {
            if entry.deleted {
                continue;
            }
            remap[old_idx] = Some(entries.len());
            entries.push(HnswEntry {
                vector: entry.vector.clone(),
                tid: entry.tid,
                neighbors: Vec::new(),
                deleted: false,
            });
        }
        let removed = before.saturating_sub(entries.len());
        if removed == 0 {
            return 0;
        }
        for (old_idx, old_entry) in storage.entries.iter().enumerate() {
            let Some(new_idx) = remap[old_idx] else {
                continue;
            };
            let mut neighbors: Vec<usize> = old_entry
                .neighbors
                .iter()
                .filter_map(|old_neighbor| remap.get(*old_neighbor).and_then(|idx| *idx))
                .filter(|neighbor| *neighbor != new_idx)
                .collect();
            neighbors.sort_unstable();
            neighbors.dedup();
            entries[new_idx].neighbors = neighbors;
        }
        storage.entry_node = storage
            .entry_node
            .and_then(|idx| remap.get(idx).and_then(|new_idx| *new_idx))
            .or_else(|| (!entries.is_empty()).then_some(0));
        storage.entries = entries;
        storage.available = !storage.entries.is_empty();
        for idx in 0..storage.entries.len() {
            self.trim_neighbors(storage, idx);
        }
        removed
    }

    fn emit_hnsw_wal(
        &self,
        op: HnswOpKind,
        index_rel: RelationId,
        tid: TupleId,
        vector: &[f32],
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        let Some(sink) = wal else {
            return Ok(());
        };
        let payload = HnswOpPayload {
            op,
            index_rel,
            tid,
            vector: vector.to_vec(),
        }
        .encode()
        .map_err(|e| AccessMethodError::Storage(format!("hnsw WAL payload encode: {e}")))?;
        let prev_lsn = sink.last_lsn_for(xid);
        let record = WalRecord::new(RecordType::HnswOp, xid, prev_lsn, 0, payload)
            .map_err(|e| AccessMethodError::Storage(format!("hnsw WAL record encode: {e}")))?;
        sink.append(record)
            .map(|_| ())
            .map_err(|e| AccessMethodError::Storage(format!("hnsw WAL append: {e}")))
    }

    fn exact_search_locked(
        &self,
        storage: &HnswStorage,
        probe: &[f32],
        k: usize,
    ) -> Vec<HnswSearchResult> {
        let mut hits: Vec<HnswSearchResult> = storage
            .entries
            .iter()
            .filter(|entry| !entry.deleted)
            .map(|entry| HnswSearchResult {
                tid: entry.tid,
                distance: self.metric.distance(probe, &entry.vector),
            })
            .collect();
        hits.sort_by(compare_hnsw_hits);
        hits.truncate(k);
        hits
    }

    fn trim_neighbors(&self, storage: &mut HnswStorage, idx: usize) {
        if idx >= storage.entries.len() {
            return;
        }
        let origin = storage.entries[idx].vector.clone();
        let mut neighbors = std::mem::take(&mut storage.entries[idx].neighbors);
        neighbors.sort_unstable();
        neighbors.dedup();
        let mut candidates: Vec<(usize, f32, Vec<f32>)> = Vec::with_capacity(neighbors.len());
        for neighbor in neighbors {
            let Some(entry) = storage.entries.get(neighbor) else {
                continue;
            };
            if entry.deleted {
                continue;
            }
            let distance = self.metric.distance(&origin, &entry.vector);
            candidates.push((neighbor, distance, entry.vector.clone()));
        }
        candidates.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        // Diversity heuristic keeps the navigable bridge edges on trim, matching
        // the persistent index so both layers stay searchable.
        storage.entries[idx].neighbors =
            select_neighbors_heuristic(&candidates, self.m, self.metric);
    }
}

impl AccessMethod for HnswIndex {
    fn name(&self) -> &'static str {
        "hnsw"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let vector = decode_hnsw_vector_key(key, self.dims)?;
        self.insert_vector(&vector, tid)
    }

    fn lookup(&self, _key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        Err(AccessMethodError::NotImplemented(
            "hnsw lookup requires vector top-k search",
        ))
    }

    fn delete(&self, _key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        self.mark_deleted(tid)
    }
}

fn decode_hnsw_vector_key(key: &[u8], dims: usize) -> Result<Vec<f32>, AccessMethodError> {
    decode_vector_key(key, dims, "hnsw")
}

fn best_frontier_position(
    frontier: &[usize],
    storage: &HnswStorage,
    probe: &[f32],
    metric: HnswMetric,
) -> usize {
    let mut best = 0usize;
    for idx in 1..frontier.len() {
        let current = &storage.entries[frontier[idx]];
        let best_entry = &storage.entries[frontier[best]];
        let current_distance = metric.distance(probe, &current.vector);
        let best_distance = metric.distance(probe, &best_entry.vector);
        if current_distance
            .total_cmp(&best_distance)
            .then_with(|| current.tid.cmp(&best_entry.tid))
            .is_lt()
        {
            best = idx;
        }
    }
    best
}
