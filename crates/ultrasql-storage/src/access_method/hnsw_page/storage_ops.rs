//! `PageBackedHnswStorage`: allocation, graph search, and DML mutation paths.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use super::*;

impl PageBackedHnswStorage {
    pub(crate) fn redo_covered(&self, lsn: Lsn) -> bool {
        lsn != Lsn::ZERO && self.meta.lsn >= lsn
    }

    pub(crate) fn allocate_block(&mut self) -> Result<BlockNumber, AccessMethodError> {
        if let Some(block) = self.free_list.blocks.pop() {
            self.sync_free_list_page();
            return Ok(block);
        }
        let block = BlockNumber::new(self.meta.next_block_number);
        self.meta.next_block_number =
            self.meta.next_block_number.checked_add(1).ok_or_else(|| {
                AccessMethodError::Storage("hnsw block number overflow".to_owned())
            })?;
        self.sync_meta_page();
        Ok(block)
    }

    pub(crate) fn free_page(&mut self, block: BlockNumber) -> Result<(), AccessMethodError> {
        if block.raw() < HNSW_FIRST_ALLOC_BLOCK {
            return Err(AccessMethodError::Storage(
                "hnsw cannot free control page".to_owned(),
            ));
        }
        self.pages.remove(&block);
        if !self.free_list.blocks.contains(&block) {
            self.free_list.blocks.push(block);
        }
        self.sync_free_list_page();
        Ok(())
    }

    pub(crate) fn allocate_vector_chain(
        &mut self,
        node_id: HnswNodeId,
        vector: &[f32],
        payload_kind: AnnPayloadKind,
    ) -> Result<BlockNumber, AccessMethodError> {
        let chunks = vector.chunks(HNSW_VECTOR_VALUES_PER_OVERFLOW_PAGE);
        let mut head = None;
        let mut previous = None;
        for chunk in chunks {
            let block = self.allocate_block()?;
            let page = HnswOverflowPage {
                page_id: PageId::new(self.meta.page_id.relation, block),
                lsn: Lsn::ZERO,
                owner_node: node_id,
                next: None,
                payload: HnswOverflowPayload::Vector(AnnVectorPayload::new(payload_kind, chunk)?),
            };
            self.pages.insert(block, HnswPersistentPage::Overflow(page));
            if let Some(prev_block) = previous {
                self.set_overflow_next(prev_block, Some(block))?;
            } else {
                head = Some(block);
            }
            previous = Some(block);
        }
        head.ok_or_else(|| AccessMethodError::Storage("hnsw vector chain empty".to_owned()))
    }

    pub(crate) fn allocate_neighbor_chain(
        &mut self,
        node_id: HnswNodeId,
        neighbors: &[HnswNodeId],
    ) -> Result<Option<BlockNumber>, AccessMethodError> {
        if neighbors.is_empty() {
            return Ok(None);
        }
        let mut head = None;
        let mut previous = None;
        for chunk in neighbors.chunks(HNSW_NEIGHBOR_IDS_PER_OVERFLOW_PAGE) {
            let block = self.allocate_block()?;
            let page = HnswOverflowPage {
                page_id: PageId::new(self.meta.page_id.relation, block),
                lsn: Lsn::ZERO,
                owner_node: node_id,
                next: None,
                payload: HnswOverflowPayload::Neighbors(chunk.to_vec()),
            };
            self.pages.insert(block, HnswPersistentPage::Overflow(page));
            if let Some(prev_block) = previous {
                self.set_overflow_next(prev_block, Some(block))?;
            } else {
                head = Some(block);
            }
            previous = Some(block);
        }
        Ok(head)
    }

    pub(crate) fn set_overflow_next(
        &mut self,
        block: BlockNumber,
        next: Option<BlockNumber>,
    ) -> Result<(), AccessMethodError> {
        let Some(HnswPersistentPage::Overflow(page)) = self.pages.get_mut(&block) else {
            return Err(AccessMethodError::Storage(
                "hnsw overflow chain points to non-overflow page".to_owned(),
            ));
        };
        page.next = next;
        Ok(())
    }

    pub(crate) fn node_page(
        &self,
        node_id: HnswNodeId,
    ) -> Result<Option<&HnswNodePage>, AccessMethodError> {
        let Some(block) = self.node_to_block.get(&node_id) else {
            return Ok(None);
        };
        match self.pages.get(block) {
            Some(HnswPersistentPage::Node(node)) => Ok(Some(node)),
            _ => Err(AccessMethodError::Storage(
                "hnsw node map points to non-node page".to_owned(),
            )),
        }
    }

    pub(crate) fn node_page_mut(
        &mut self,
        node_id: HnswNodeId,
    ) -> Result<Option<&mut HnswNodePage>, AccessMethodError> {
        let Some(block) = self.node_to_block.get(&node_id) else {
            return Ok(None);
        };
        match self.pages.get_mut(block) {
            Some(HnswPersistentPage::Node(node)) => Ok(Some(node)),
            _ => Err(AccessMethodError::Storage(
                "hnsw node map points to non-node page".to_owned(),
            )),
        }
    }

    pub(crate) fn live_node_snapshot(
        &self,
    ) -> Result<Vec<(HnswNodeId, TupleId, Vec<f32>)>, AccessMethodError> {
        // Mirror order is ascending `node_id`, the same order
        // `node_to_block.keys()` yields, so the candidate pool (and thus the
        // built graph) is identical to the page-based scan it replaces.
        let mut out = Vec::with_capacity(self.meta.live_nodes);
        for (idx, slot) in self.mirror.iter().enumerate() {
            let Some(node) = slot else {
                continue;
            };
            if node.deleted {
                continue;
            }
            let node_id = HnswNodeId::try_from(idx).map_err(|_| {
                AccessMethodError::Storage("hnsw mirror index does not fit node id".to_owned())
            })?;
            out.push((node_id, node.tid, node.vector.clone()));
        }
        Ok(out)
    }

    pub(crate) fn vector_for_node(
        &self,
        node: &HnswNodePage,
    ) -> Result<Vec<f32>, AccessMethodError> {
        let mut vector = Vec::with_capacity(node.vector_len);
        let mut current = Some(node.vector_head);
        while let Some(block) = current {
            let Some(HnswPersistentPage::Overflow(page)) = self.pages.get(&block) else {
                return Err(AccessMethodError::Storage(
                    "hnsw vector chain points to non-overflow page".to_owned(),
                ));
            };
            match &page.payload {
                HnswOverflowPayload::Vector(payload) => vector.extend(payload.exact_f32()),
                HnswOverflowPayload::Neighbors(_) => {
                    return Err(AccessMethodError::Storage(
                        "hnsw vector chain points to neighbor payload".to_owned(),
                    ));
                }
            }
            current = page.next;
        }
        if vector.len() != node.vector_len {
            return Err(AccessMethodError::Storage(
                "hnsw vector chain length mismatch".to_owned(),
            ));
        }
        Ok(vector)
    }

    /// Zero-copy view of a node's vector when it lives in a single overflow page
    /// — the common case, since `HNSW_VECTOR_VALUES_PER_OVERFLOW_PAGE` is ~2k so
    /// Adjacency from the durable neighbor chain. Used only to (re)build the
    /// mirror; the hot read path is `neighbors_for_node`, which reads the mirror.
    pub(crate) fn neighbors_from_pages_at_level(
        &self,
        node_id: HnswNodeId,
        level: usize,
    ) -> Result<Vec<HnswNodeId>, AccessMethodError> {
        let Some(node) = self.node_page(node_id)? else {
            return Ok(Vec::new());
        };
        let (head, count) = if level == 0 {
            (node.neighbor_head, node.neighbor_count)
        } else {
            match node.upper_levels.get(level - 1) {
                Some(upper) => (upper.head, upper.count),
                None => return Ok(Vec::new()),
            }
        };
        let mut neighbors = Vec::with_capacity(count);
        let mut current = head;
        while let Some(block) = current {
            let Some(HnswPersistentPage::Overflow(page)) = self.pages.get(&block) else {
                return Err(AccessMethodError::Storage(
                    "hnsw neighbor chain points to non-overflow page".to_owned(),
                ));
            };
            match &page.payload {
                HnswOverflowPayload::Neighbors(ids) => neighbors.extend(ids),
                HnswOverflowPayload::Vector(_) => {
                    return Err(AccessMethodError::Storage(
                        "hnsw neighbor chain points to vector payload".to_owned(),
                    ));
                }
            }
            current = page.next;
        }
        neighbors.truncate(count);
        Ok(neighbors)
    }

    /// Layer-`level` adjacency of a node, read from the in-memory mirror (O(1)).
    /// The mirror is kept in lockstep with the durable chains, so this matches
    /// `neighbors_from_pages_at_level` without the chain walk.
    pub(crate) fn neighbors_at_level(&self, node_id: HnswNodeId, level: usize) -> Vec<HnswNodeId> {
        self.mirror_neighbors_at_level(node_id, level)
    }

    /// Distance from `probe` to a live node, or `None` when the node is missing
    /// or tombstoned. Reads the node's vector from the mirror (O(1), no
    /// per-probe allocation or page-chain walk).
    pub(crate) fn node_probe_distance(
        &self,
        probe: &[f32],
        node_id: HnswNodeId,
    ) -> Result<Option<(f32, TupleId)>, AccessMethodError> {
        let Some(node) = self.mirror_node(node_id) else {
            return Ok(None);
        };
        if node.deleted {
            return Ok(None);
        }
        Ok(Some((
            self.meta.metric.distance(probe, &node.vector),
            node.tid,
        )))
    }

    /// Exact brute-force scan over every live node. Used when the live set is
    /// small enough that exhaustive search is both cheap and exact.
    pub(crate) fn exact_scan(
        &self,
        probe: &[f32],
        k: usize,
    ) -> Result<Vec<HnswSearchResult>, AccessMethodError> {
        let mut hits = Vec::with_capacity(self.meta.live_nodes.min(k.max(1)));
        for node_id in self.node_to_block.keys() {
            if let Some((distance, tid)) = self.node_probe_distance(probe, *node_id)? {
                hits.push(HnswSearchResult { tid, distance });
            }
        }
        hits.sort_by(compare_hnsw_hits);
        hits.truncate(k);
        Ok(hits)
    }

    /// Canonical HNSW SEARCH-LAYER: best-first expansion of one layer from the
    /// given entry points, keeping the `ef` nearest results. Used everywhere —
    /// `ef = 1` for the greedy descent through upper layers, `ef = ef_construction`
    /// to gather build candidates, and `ef = ef_search` for the base-layer query.
    /// Reads vectors and adjacency from the mirror (O(1)), so the cost is bounded
    /// by the nodes the beam touches, not the live-set size. Deterministic via
    /// the total order on `DistNode`. Returns results sorted ascending by
    /// distance.
    pub(crate) fn search_layer(
        &self,
        query: &[f32],
        entry_points: &[(f32, HnswNodeId)],
        ef: usize,
        level: usize,
    ) -> Result<Vec<(f32, HnswNodeId)>, AccessMethodError> {
        let ef = ef.max(1);
        let mut visited: std::collections::BTreeSet<HnswNodeId> =
            entry_points.iter().map(|(_, id)| *id).collect();
        // candidates: min-heap on distance (expand the nearest first).
        let mut candidates: std::collections::BinaryHeap<std::cmp::Reverse<DistNode>> =
            std::collections::BinaryHeap::new();
        // result: max-heap on distance (peek = current worst, capped at `ef`).
        let mut result: std::collections::BinaryHeap<DistNode> =
            std::collections::BinaryHeap::new();
        for &(dist, id) in entry_points {
            candidates.push(std::cmp::Reverse(DistNode { dist, id }));
            result.push(DistNode { dist, id });
        }
        while result.len() > ef {
            result.pop();
        }
        while let Some(std::cmp::Reverse(nearest)) = candidates.pop() {
            let worst = result.peek().map_or(f32::INFINITY, |node| node.dist);
            if result.len() >= ef && nearest.dist > worst {
                break;
            }
            for neighbor in self.mirror_neighbors_at_level(nearest.id, level) {
                if !visited.insert(neighbor) {
                    continue;
                }
                let Some((dist, _)) = self.node_probe_distance(query, neighbor)? else {
                    continue;
                };
                let worst = result.peek().map_or(f32::INFINITY, |node| node.dist);
                if result.len() < ef || dist < worst {
                    candidates.push(std::cmp::Reverse(DistNode { dist, id: neighbor }));
                    result.push(DistNode { dist, id: neighbor });
                    if result.len() > ef {
                        result.pop();
                    }
                }
            }
        }
        Ok(result
            .into_sorted_vec()
            .into_iter()
            .map(|node| (node.dist, node.id))
            .collect())
    }

    /// Multi-layer approximate nearest-neighbor search: greedy `ef=1` descent
    /// from the top-layer entry point down to layer 1, then an `ef_search` beam
    /// at the base layer. A single-layer graph (every node level 0, e.g. one
    /// loaded from a v1 snapshot) simply skips the descent, so behavior is
    /// unchanged there. Read-only.
    pub(crate) fn graph_search(
        &self,
        probe: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<HnswSearchResult>, AccessMethodError> {
        let entry = match self.meta.entry_node {
            Some(id) if self.mirror_node(id).is_some_and(|node| !node.deleted) => Some(id),
            _ => self.highest_level_live_node()?,
        };
        let Some(entry) = entry else {
            return Ok(Vec::new());
        };
        let Some((entry_distance, _)) = self.node_probe_distance(probe, entry)? else {
            return Ok(Vec::new());
        };
        let mut ep = vec![(entry_distance, entry)];
        for level in (1..=self.mirror_level(entry)).rev() {
            let nearest = self.search_layer(probe, &ep, 1, level)?;
            if let Some(&best) = nearest.first() {
                ep = vec![best];
            }
        }
        let found = self.search_layer(probe, &ep, ef_search, 0)?;
        let mut hits: Vec<HnswSearchResult> = found
            .into_iter()
            .filter_map(|(distance, node_id)| {
                self.mirror_node(node_id)
                    .filter(|node| !node.deleted)
                    .map(|node| HnswSearchResult {
                        tid: node.tid,
                        distance,
                    })
            })
            .collect();
        hits.sort_by(compare_hnsw_hits);
        hits.truncate(k);
        Ok(hits)
    }

    pub(crate) fn write_neighbors_at_level(
        &mut self,
        node_id: HnswNodeId,
        level: usize,
        neighbors: &[HnswNodeId],
    ) -> Result<(), AccessMethodError> {
        let old_head = if level == 0 {
            self.node_page(node_id)?.and_then(|node| node.neighbor_head)
        } else {
            self.node_page(node_id)?
                .and_then(|node| node.upper_levels.get(level - 1))
                .and_then(|upper| upper.head)
        };
        self.release_overflow_chain(old_head)?;
        let new_head = self.allocate_neighbor_chain(node_id, neighbors)?;
        let Some(node) = self.node_page_mut(node_id)? else {
            return Err(AccessMethodError::Storage(
                "hnsw write neighbors missing node".to_owned(),
            ));
        };
        if level == 0 {
            node.neighbor_head = new_head;
            node.neighbor_count = neighbors.len();
        } else {
            let Some(upper) = node.upper_levels.get_mut(level - 1) else {
                return Err(AccessMethodError::Storage(
                    "hnsw write neighbors above node level".to_owned(),
                ));
            };
            upper.head = new_head;
            upper.count = neighbors.len();
        }
        self.mirror_set_neighbors_at_level(node_id, level, neighbors);
        Ok(())
    }

    pub(crate) fn trim_neighbor_list(
        &self,
        node_id: HnswNodeId,
        mut neighbors: Vec<HnswNodeId>,
        max_neighbors: usize,
        metric: HnswMetric,
    ) -> Result<Vec<HnswNodeId>, AccessMethodError> {
        neighbors.sort_unstable();
        neighbors.dedup();
        neighbors.retain(|neighbor| *neighbor != node_id);
        let Some(origin_node) = self.mirror_node(node_id) else {
            return Ok(Vec::new());
        };
        let origin = origin_node.vector.clone();
        let mut candidates: Vec<(HnswNodeId, f32, Vec<f32>)> = Vec::with_capacity(neighbors.len());
        for neighbor in neighbors {
            let Some(neighbor_node) = self.mirror_node(neighbor) else {
                continue;
            };
            if neighbor_node.deleted {
                continue;
            }
            let distance = metric.distance(&origin, &neighbor_node.vector);
            candidates.push((neighbor, distance, neighbor_node.vector.clone()));
        }
        candidates.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        // Apply the same diversity heuristic on trim so re-linking keeps the
        // navigable bridge edges rather than greedily collapsing to the nearest.
        Ok(select_neighbors_heuristic(
            &candidates,
            max_neighbors,
            metric,
        ))
    }

    pub(crate) fn mark_deleted(
        &mut self,
        tid: TupleId,
        replay: bool,
        page_lsn: Lsn,
    ) -> Result<(), AccessMethodError> {
        let Some(node_id) = self.tid_to_node.get(&tid).copied() else {
            return if replay {
                Ok(())
            } else {
                Err(AccessMethodError::NotFound)
            };
        };
        let Some(node) = self.node_page_mut(node_id)? else {
            return if replay {
                Ok(())
            } else {
                Err(AccessMethodError::NotFound)
            };
        };
        if node.deleted {
            return if replay {
                Ok(())
            } else {
                Err(AccessMethodError::NotFound)
            };
        }
        node.deleted = true;
        self.mirror_mark_deleted(node_id);
        self.meta.live_nodes = self.meta.live_nodes.saturating_sub(1);
        self.meta.tombstones += 1;
        if self.meta.entry_node == Some(node_id) {
            self.meta.entry_node = self.highest_level_live_node()?;
        }
        self.sync_meta_page();
        self.stamp_all_pages(page_lsn);
        Ok(())
    }

    pub(crate) fn vacuum_deleted(
        &mut self,
        metric: HnswMetric,
        max_neighbors: usize,
        page_lsn: Lsn,
    ) -> Result<usize, AccessMethodError> {
        let deleted_nodes: Vec<HnswNodeId> = self
            .node_to_block
            .keys()
            .filter_map(|node_id| {
                self.node_page(*node_id)
                    .ok()
                    .flatten()
                    .is_some_and(|node| node.deleted)
                    .then_some(*node_id)
            })
            .collect();
        if deleted_nodes.is_empty() {
            return Ok(0);
        }

        // Re-link every live node at every layer it participates in, dropping
        // edges to vacuumed nodes and re-applying the diversity heuristic.
        let live_nodes: Vec<HnswNodeId> = self
            .node_to_block
            .keys()
            .copied()
            .filter(|node_id| !deleted_nodes.contains(node_id))
            .collect();
        for node_id in live_nodes {
            for level in 0..=self.mirror_level(node_id) {
                let neighbors = self
                    .neighbors_at_level(node_id, level)
                    .into_iter()
                    .filter(|neighbor| !deleted_nodes.contains(neighbor))
                    .collect::<Vec<_>>();
                let m_max = hnsw_level_max_neighbors(level, max_neighbors);
                let trimmed = self.trim_neighbor_list(node_id, neighbors, m_max, metric)?;
                self.write_neighbors_at_level(node_id, level, &trimmed)?;
            }
        }

        for node_id in &deleted_nodes {
            let Some(block) = self.node_to_block.remove(node_id) else {
                continue;
            };
            let Some(HnswPersistentPage::Node(node)) = self.pages.get(&block).cloned() else {
                continue;
            };
            self.tid_to_node.remove(&node.tid);
            self.release_overflow_chain(Some(node.vector_head))?;
            self.release_overflow_chain(node.neighbor_head)?;
            for upper in &node.upper_levels {
                self.release_overflow_chain(upper.head)?;
            }
            self.free_page(block)?;
            self.mirror_remove(*node_id);
        }
        self.meta.tombstones = 0;
        self.meta.live_nodes = self
            .node_to_block
            .keys()
            .filter(|node_id| {
                self.node_page(**node_id)
                    .ok()
                    .flatten()
                    .is_some_and(|node| !node.deleted)
            })
            .count();
        self.meta.entry_node = self.highest_level_live_node()?;
        self.sync_control_pages();
        self.stamp_all_pages(page_lsn);
        Ok(deleted_nodes.len())
    }

    /// The live node with the highest hierarchical level — the HNSW entry point.
    /// Ties break to the lowest node id (mirror iterates ids ascending), so the
    /// entry is deterministic and WAL replay reconstructs the same one.
    pub(crate) fn highest_level_live_node(&self) -> Result<Option<HnswNodeId>, AccessMethodError> {
        let mut best: Option<(usize, HnswNodeId)> = None;
        for (idx, slot) in self.mirror.iter().enumerate() {
            let Some(node) = slot else {
                continue;
            };
            if node.deleted {
                continue;
            }
            let id = HnswNodeId::try_from(idx).map_err(|_| {
                AccessMethodError::Storage("hnsw mirror index does not fit node id".to_owned())
            })?;
            if best.is_none_or(|(best_level, _)| node.level() > best_level) {
                best = Some((node.level(), id));
            }
        }
        Ok(best.map(|(_, id)| id))
    }

    pub(crate) fn release_overflow_chain(
        &mut self,
        head: Option<BlockNumber>,
    ) -> Result<(), AccessMethodError> {
        let mut current = head;
        while let Some(block) = current {
            let next = match self.pages.get(&block) {
                Some(HnswPersistentPage::Overflow(page)) => page.next,
                _ => {
                    return Err(AccessMethodError::Storage(
                        "hnsw release chain found non-overflow page".to_owned(),
                    ));
                }
            };
            self.free_page(block)?;
            current = next;
        }
        Ok(())
    }

    pub(crate) fn sync_meta_page(&mut self) {
        self.pages.insert(
            BlockNumber::new(HNSW_META_BLOCK),
            HnswPersistentPage::Meta(self.meta.clone()),
        );
    }

    pub(crate) fn sync_free_list_page(&mut self) {
        self.pages.insert(
            BlockNumber::new(HNSW_FREE_LIST_BLOCK),
            HnswPersistentPage::FreeList(self.free_list.clone()),
        );
    }

    pub(crate) fn sync_control_pages(&mut self) {
        self.sync_meta_page();
        self.sync_free_list_page();
    }

    pub(crate) fn stamp_all_pages(&mut self, lsn: Lsn) {
        if lsn == Lsn::ZERO {
            return;
        }
        self.meta.lsn = lsn;
        self.free_list.lsn = lsn;
        for page in self.pages.values_mut() {
            page.set_lsn(lsn);
        }
        self.sync_control_pages();
    }
}
