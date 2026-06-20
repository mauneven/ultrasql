//! `PageBackedHnswIndex` DML, search, vacuum, and WAL-apply operations.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use super::*;

impl PageBackedHnswIndex {
    /// Whether the graph has at least one live node available for search.
    #[must_use]
    pub fn is_available(&self) -> bool {
        let storage = self.storage.lock();
        storage.valid && storage.meta.live_nodes > 0
    }

    /// Whether recovery still trusts this index relation.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.storage.lock().valid
    }

    /// Mark this index unavailable after corrupt or incomplete recovery.
    pub fn invalidate(&self) {
        self.storage.lock().valid = false;
    }

    /// Return the physical ANN payload family used by new entries.
    #[must_use]
    pub const fn payload_kind(&self) -> AnnPayloadKind {
        self.payload_kind
    }

    /// Return the final candidate rerank policy.
    #[must_use]
    pub const fn rerank_policy(&self) -> AnnRerankPolicy {
        AnnRerankPolicy::ExactF32
    }

    /// Insert one finite vector into page-backed HNSW pages.
    pub fn insert_vector(&self, vector: &[f32], tid: TupleId) -> Result<(), AccessMethodError> {
        self.insert_vector_internal(vector, tid, false, Lsn::ZERO)
    }

    /// Insert one vector and emit a logical HNSW WAL record when `wal` is set.
    pub fn insert_vector_logged(
        &self,
        vector: &[f32],
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        let page_lsn = self.emit_hnsw_wal(HnswOpKind::Insert, tid, vector, xid, wal)?;
        self.insert_vector_internal(vector, tid, false, page_lsn)
    }

    /// Search live nodes using exact distance over page-backed vectors.
    ///
    /// The page format stores graph adjacency, but this first persistent slice
    /// keeps the query path exact so it can serve as a recovery correctness
    /// oracle while page-backed ANN traversal is hardened separately.
    pub fn search(
        &self,
        probe: &[f32],
        k: usize,
    ) -> Result<Vec<HnswSearchResult>, AccessMethodError> {
        self.search_with_ef(probe, k, self.ef_search)
    }

    /// Search the persistent index with a caller-supplied `ef_search`.
    ///
    /// The page-backed arena persists the navigable graph, so this traverses it
    /// (greedy descent + best-first expansion) for real ANN speedup on large
    /// indexes. When the live set is no larger than `ef_search` the search is an
    /// exact exhaustive scan (cheap and exact at small scale), so a per-query
    /// `ef_search >= live count` is exact — the knob filtered ANN uses to
    /// over-fetch candidates and recall/latency sweeps use to trace the curve.
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
        if !storage.valid || storage.meta.live_nodes == 0 {
            return Ok(Vec::new());
        }
        if storage.meta.live_nodes <= ef_search {
            return storage.exact_scan(probe, k);
        }
        storage.graph_search(probe, k, ef_search)
    }

    /// Mark a node tombstoned. VACUUM reclaims its pages later.
    pub fn mark_deleted(&self, tid: TupleId) -> Result<(), AccessMethodError> {
        let mut storage = self.storage.lock();
        storage.mark_deleted(tid, false, Lsn::ZERO)
    }

    /// Mark a node tombstoned and emit a logical HNSW WAL record.
    pub fn mark_deleted_logged(
        &self,
        tid: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), AccessMethodError> {
        let page_lsn = self.emit_hnsw_wal(HnswOpKind::Delete, tid, &[], xid, wal)?;
        let mut storage = self.storage.lock();
        storage.mark_deleted(tid, false, page_lsn)
    }

    /// Reclaim tombstoned node and overflow pages into the free-list page.
    pub fn vacuum_deleted(&self) -> Result<usize, AccessMethodError> {
        let mut storage = self.storage.lock();
        storage.vacuum_deleted(self.metric, self.m, Lsn::ZERO)
    }

    /// VACUUM tombstoned pages and emit a logical compact WAL record.
    pub fn vacuum_deleted_logged(
        &self,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<usize, AccessMethodError> {
        if self.page_stats().tombstones == 0 {
            return Ok(0);
        }
        let tid = TupleId::new(PageId::new(self.index_rel, BlockNumber::new(0)), 0);
        let page_lsn = self.emit_hnsw_wal(HnswOpKind::Compact, tid, &[], xid, wal)?;
        let mut storage = self.storage.lock();
        storage.vacuum_deleted(self.metric, self.m, page_lsn)
    }

    /// Replay one decoded logical HNSW WAL payload into this page arena.
    pub fn apply_wal_payload(&self, payload: &HnswOpPayload) -> Result<(), AccessMethodError> {
        self.apply_wal_payload_at(Lsn::ZERO, payload)
    }

    /// Replay one decoded logical HNSW WAL payload at its assigned WAL LSN.
    pub fn apply_wal_payload_at(
        &self,
        lsn: Lsn,
        payload: &HnswOpPayload,
    ) -> Result<(), AccessMethodError> {
        if payload.index_rel != self.index_rel {
            return Ok(());
        }
        {
            let storage = self.storage.lock();
            if !storage.valid || storage.redo_covered(lsn) {
                return Ok(());
            }
        }
        match payload.op {
            HnswOpKind::Insert => {
                self.insert_vector_internal(&payload.vector, payload.tid, true, lsn)
            }
            HnswOpKind::Delete => {
                let mut storage = self.storage.lock();
                storage.mark_deleted(payload.tid, true, lsn)
            }
            HnswOpKind::Compact => {
                let mut storage = self.storage.lock();
                storage.vacuum_deleted(self.metric, self.m, lsn).map(|_| ())
            }
        }
    }

    /// Replay one WAL record, ignoring records that are not HNSW mutations.
    pub fn apply_wal_record(&self, record: &WalRecord) -> Result<(), AccessMethodError> {
        self.apply_wal_record_at(Lsn::ZERO, record)
    }

    /// Replay one WAL record at its assigned WAL LSN.
    pub fn apply_wal_record_at(
        &self,
        lsn: Lsn,
        record: &WalRecord,
    ) -> Result<(), AccessMethodError> {
        if record.header.record_type != RecordType::HnswOp {
            return Ok(());
        }
        if let Some(index_rel) = ann_wal_index_rel(&record.payload, "hnsw")?
            && index_rel != self.index_rel
        {
            return Ok(());
        }
        let payload = HnswOpPayload::decode(&record.payload)
            .map_err(|e| AccessMethodError::Storage(format!("decode hnsw WAL payload: {e}")))?;
        self.apply_wal_payload_at(lsn, &payload)
    }

    pub(crate) fn insert_vector_internal(
        &self,
        vector: &[f32],
        tid: TupleId,
        replay: bool,
        page_lsn: Lsn,
    ) -> Result<(), AccessMethodError> {
        self.validate_vector(vector)?;
        let mut storage = self.storage.lock();
        if storage.tid_to_node.contains_key(&tid) {
            if replay {
                return Ok(());
            }
            return Err(AccessMethodError::DuplicateKey);
        }

        let ef_construction = HNSW_DEFAULT_EF_CONSTRUCTION.max(self.m);

        // Assign id and a deterministic hierarchical level, then materialize the
        // node (page + vector chain + mirror entry) before linking.
        let node_id = storage.meta.next_node_id;
        storage.meta.next_node_id = storage
            .meta
            .next_node_id
            .checked_add(1)
            .ok_or_else(|| AccessMethodError::Storage("hnsw node id overflow".to_owned()))?;
        let node_level = hnsw_assign_level(node_id, self.m);
        let vector_head = storage.allocate_vector_chain(node_id, vector, self.payload_kind)?;
        let node_block = storage.allocate_block()?;
        let node_page = HnswNodePage {
            page_id: PageId::new(self.index_rel, node_block),
            lsn: Lsn::ZERO,
            node_id,
            tid,
            vector_len: vector.len(),
            vector_head,
            neighbor_count: 0,
            neighbor_head: None,
            level: node_level,
            upper_levels: vec![
                HnswLevelNeighbors {
                    head: None,
                    count: 0,
                };
                node_level
            ],
            deleted: false,
        };
        storage
            .pages
            .insert(node_block, HnswPersistentPage::Node(node_page));
        storage.node_to_block.insert(node_id, node_block);
        storage.tid_to_node.insert(tid, node_id);
        storage.meta.live_nodes += 1;
        storage.mirror_put(
            node_id,
            MirrorNode {
                vector: vector.to_vec(),
                levels: vec![Vec::new(); node_level + 1],
                tid,
                deleted: false,
            },
        );

        // First live node: it becomes the entry point with no neighbors.
        let Some(entry) = storage
            .meta
            .entry_node
            .filter(|id| storage.mirror_node(*id).is_some_and(|node| !node.deleted))
        else {
            storage.meta.entry_node = Some(node_id);
            storage.sync_control_pages();
            storage.stamp_all_pages(page_lsn);
            return Ok(());
        };
        let entry_level = storage.mirror_level(entry);
        let Some((entry_distance, _)) = storage.node_probe_distance(vector, entry)? else {
            storage.meta.entry_node = Some(node_id);
            storage.sync_control_pages();
            storage.stamp_all_pages(page_lsn);
            return Ok(());
        };

        // Greedy descent (ef=1) through the layers above this node's top level.
        let mut ep = vec![(entry_distance, entry)];
        for level in ((node_level + 1)..=entry_level).rev() {
            let nearest = storage.search_layer(vector, &ep, 1, level)?;
            if let Some(&best) = nearest.first() {
                ep = vec![best];
            }
        }

        // Connect at each layer from min(node_level, entry_level) down to 0,
        // selecting a diverse neighbor subset so the navigable graph stays
        // searchable. The base layer keeps the small-graph exhaustive scan
        // (exact + faster) below the work threshold; otherwise it traverses.
        let top_connect = node_level.min(entry_level);
        for level in (0..=top_connect).rev() {
            let m_max = hnsw_level_max_neighbors(level, self.m);
            let scan_work = storage.meta.live_nodes.saturating_mul(self.dims);
            let mut candidates: Vec<(HnswNodeId, f32, Vec<f32>)> =
                if level == 0 && scan_work <= self.build_traversal_work_threshold {
                    storage
                        .live_node_snapshot()?
                        .into_iter()
                        .filter(|(id, _, _)| *id != node_id)
                        .map(|(id, _tid, node_vector)| {
                            let distance = self.metric.distance(vector, &node_vector);
                            (id, distance, node_vector)
                        })
                        .collect()
                } else {
                    storage
                        .search_layer(vector, &ep, ef_construction, level)?
                        .into_iter()
                        .filter(|(_, id)| *id != node_id)
                        .filter_map(|(distance, id)| {
                            storage
                                .mirror_node(id)
                                .map(|node| (id, distance, node.vector.clone()))
                        })
                        .collect()
                };
            candidates.sort_by(|left, right| {
                left.1
                    .total_cmp(&right.1)
                    .then_with(|| left.0.cmp(&right.0))
            });
            candidates.truncate(ef_construction);
            // Entry points for the next lower layer = this layer's candidate set.
            ep = candidates
                .iter()
                .map(|(id, dist, _)| (*dist, *id))
                .collect();

            let selected = select_neighbors_heuristic(&candidates, m_max, self.metric);
            storage.write_neighbors_at_level(node_id, level, &selected)?;
            for neighbor_id in selected {
                let mut neighbor_list = storage.neighbors_at_level(neighbor_id, level);
                if !neighbor_list.contains(&node_id) {
                    neighbor_list.push(node_id);
                }
                let trimmed =
                    storage.trim_neighbor_list(neighbor_id, neighbor_list, m_max, self.metric)?;
                storage.write_neighbors_at_level(neighbor_id, level, &trimmed)?;
            }
        }

        // A taller node than the current entry becomes the new entry point.
        if node_level > entry_level {
            storage.meta.entry_node = Some(node_id);
        }
        storage.sync_control_pages();
        storage.stamp_all_pages(page_lsn);
        Ok(())
    }

    pub(crate) fn validate_vector(&self, vector: &[f32]) -> Result<(), AccessMethodError> {
        if vector.len() != self.dims {
            return Err(AccessMethodError::Storage(format!(
                "page-backed hnsw vector dimension mismatch: expected {}, got {}",
                self.dims,
                vector.len()
            )));
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw vector elements must be finite".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn emit_hnsw_wal(
        &self,
        op: HnswOpKind,
        tid: TupleId,
        vector: &[f32],
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<Lsn, AccessMethodError> {
        let Some(sink) = wal else {
            return Ok(Lsn::ZERO);
        };
        let payload = HnswOpPayload {
            op,
            index_rel: self.index_rel,
            tid,
            vector: vector.to_vec(),
        }
        .encode()
        .map_err(|e| {
            AccessMethodError::Storage(format!("page-backed hnsw WAL payload encode: {e}"))
        })?;
        let prev_lsn = sink.last_lsn_for(xid);
        let record =
            WalRecord::new(RecordType::HnswOp, xid, prev_lsn, 0, payload).map_err(|e| {
                AccessMethodError::Storage(format!("page-backed hnsw WAL record encode: {e}"))
            })?;
        sink.append(record)
            .map_err(|e| AccessMethodError::Storage(format!("page-backed hnsw WAL append: {e}")))
    }
}
