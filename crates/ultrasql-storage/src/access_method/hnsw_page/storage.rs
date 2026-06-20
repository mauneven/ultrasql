//! `PageBackedHnswStorage`: load, mirror maintenance, and page reconstruction.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use super::*;

impl PageBackedHnswStorage {
    pub(crate) fn new(
        index_rel: RelationId,
        dims: usize,
        metric: HnswMetric,
        m: usize,
        ef_search: usize,
        payload_kind: AnnPayloadKind,
    ) -> Self {
        let meta_block = BlockNumber::new(HNSW_META_BLOCK);
        let free_block = BlockNumber::new(HNSW_FREE_LIST_BLOCK);
        let meta = HnswMetaPage {
            page_id: PageId::new(index_rel, meta_block),
            lsn: Lsn::ZERO,
            dims,
            metric,
            m,
            ef_search,
            payload_kind,
            entry_node: None,
            next_node_id: 0,
            live_nodes: 0,
            tombstones: 0,
            next_block_number: HNSW_FIRST_ALLOC_BLOCK,
            free_list_page: free_block,
        };
        let free_list = HnswFreeListPage {
            page_id: PageId::new(index_rel, free_block),
            lsn: Lsn::ZERO,
            blocks: Vec::new(),
        };
        let mut pages = BTreeMap::new();
        pages.insert(meta_block, HnswPersistentPage::Meta(meta.clone()));
        pages.insert(free_block, HnswPersistentPage::FreeList(free_list.clone()));
        Self {
            valid: true,
            pages,
            meta,
            free_list,
            tid_to_node: BTreeMap::new(),
            node_to_block: BTreeMap::new(),
            mirror: Vec::new(),
        }
    }

    /// Rebuild the in-memory mirror from the authoritative page state. Called
    /// once after construction from page images (load / snapshot restore /
    /// WAL replay seed) so the mirror exactly reflects what is on disk.
    pub(crate) fn rebuild_mirror(&mut self) -> Result<(), AccessMethodError> {
        let len = usize::try_from(self.meta.next_node_id).map_err(|_| {
            AccessMethodError::Storage("hnsw next_node_id does not fit usize".to_owned())
        })?;
        let mut mirror: Vec<Option<MirrorNode>> = Vec::new();
        mirror.resize_with(len, || None);
        let node_ids: Vec<HnswNodeId> = self.node_to_block.keys().copied().collect();
        for node_id in node_ids {
            let (tid, deleted, vector, node_level) = {
                let Some(node) = self.node_page(node_id)? else {
                    continue;
                };
                (
                    node.tid,
                    node.deleted,
                    self.vector_for_node(node)?,
                    node.level,
                )
            };
            let mut levels = Vec::with_capacity(node_level + 1);
            for level in 0..=node_level {
                levels.push(self.neighbors_from_pages_at_level(node_id, level)?);
            }
            let idx = usize::try_from(node_id).map_err(|_| {
                AccessMethodError::Storage("hnsw node id does not fit usize".to_owned())
            })?;
            if idx >= mirror.len() {
                mirror.resize_with(idx + 1, || None);
            }
            mirror[idx] = Some(MirrorNode {
                vector,
                levels,
                tid,
                deleted,
            });
        }
        self.mirror = mirror;
        Ok(())
    }

    /// O(1) shared view of a node's mirror entry, or `None` if the id is unused.
    pub(crate) fn mirror_node(&self, node_id: HnswNodeId) -> Option<&MirrorNode> {
        usize::try_from(node_id)
            .ok()
            .and_then(|idx| self.mirror.get(idx))
            .and_then(Option::as_ref)
    }

    /// Insert or replace a node's mirror entry, growing the backing vec as the
    /// monotonic `node_id` space advances.
    pub(crate) fn mirror_put(&mut self, node_id: HnswNodeId, node: MirrorNode) {
        let Ok(idx) = usize::try_from(node_id) else {
            return;
        };
        if idx >= self.mirror.len() {
            self.mirror.resize_with(idx + 1, || None);
        }
        self.mirror[idx] = Some(node);
    }

    /// Drop a node's mirror entry (vacuum reclaim). The slot stays as `None`.
    pub(crate) fn mirror_remove(&mut self, node_id: HnswNodeId) {
        if let Ok(idx) = usize::try_from(node_id)
            && idx < self.mirror.len()
        {
            self.mirror[idx] = None;
        }
    }

    /// Replace a node's mirrored layer-`level` adjacency, keeping it in lockstep
    /// with the durable neighbor chain written by `write_neighbors_at_level`.
    pub(crate) fn mirror_set_neighbors_at_level(
        &mut self,
        node_id: HnswNodeId,
        level: usize,
        neighbors: &[HnswNodeId],
    ) {
        if let Ok(idx) = usize::try_from(node_id)
            && let Some(Some(node)) = self.mirror.get_mut(idx)
            && let Some(slot) = node.levels.get_mut(level)
        {
            slot.clear();
            slot.extend_from_slice(neighbors);
        }
    }

    /// Layer-`level` neighbors of a node from the mirror (O(1)). Empty when the
    /// node is absent or not present in that layer.
    pub(crate) fn mirror_neighbors_at_level(
        &self,
        node_id: HnswNodeId,
        level: usize,
    ) -> Vec<HnswNodeId> {
        self.mirror_node(node_id)
            .map(|node| node.neighbors_at(level).to_vec())
            .unwrap_or_default()
    }

    /// A node's top layer from the mirror (0 if absent).
    pub(crate) fn mirror_level(&self, node_id: HnswNodeId) -> usize {
        self.mirror_node(node_id).map_or(0, MirrorNode::level)
    }

    /// Mark a node's mirror entry tombstoned, matching the durable page flag.
    pub(crate) fn mirror_mark_deleted(&mut self, node_id: HnswNodeId) {
        if let Ok(idx) = usize::try_from(node_id)
            && let Some(Some(node)) = self.mirror.get_mut(idx)
        {
            node.deleted = true;
        }
    }

    /// Assert the mirror is byte-for-byte consistent with the durable page state:
    /// every node in `node_to_block` has a mirror entry whose vector, adjacency,
    /// tid, and tombstone flag match the pages, and there are no stray entries.
    #[cfg(test)]
    pub(crate) fn assert_mirror_consistent(&self) {
        let mut durable = 0usize;
        for node_id in self.node_to_block.keys().copied() {
            let (page_tid, page_deleted, page_vector, page_level) = {
                let node = self
                    .node_page(node_id)
                    .expect("node page")
                    .expect("node present");
                (
                    node.tid,
                    node.deleted,
                    self.vector_for_node(node).expect("vec"),
                    node.level,
                )
            };
            let mirror = self
                .mirror_node(node_id)
                .unwrap_or_else(|| panic!("missing mirror entry for node {node_id}"));
            assert_eq!(
                mirror.vector, page_vector,
                "mirror vector mismatch {node_id}"
            );
            assert_eq!(
                mirror.level(),
                page_level,
                "mirror level mismatch {node_id}"
            );
            for level in 0..=page_level {
                let page_neighbors = self
                    .neighbors_from_pages_at_level(node_id, level)
                    .expect("page neighbors");
                assert_eq!(
                    mirror.neighbors_at(level),
                    page_neighbors.as_slice(),
                    "mirror neighbors mismatch node {node_id} level {level}"
                );
            }
            assert_eq!(mirror.tid, page_tid, "mirror tid mismatch {node_id}");
            assert_eq!(
                mirror.deleted, page_deleted,
                "mirror deleted mismatch {node_id}"
            );
            durable += 1;
        }
        let present = self.mirror.iter().filter(|slot| slot.is_some()).count();
        assert_eq!(
            present, durable,
            "mirror has stray entries not in node_to_block"
        );
    }

    pub(crate) fn from_page_images(
        index_rel: RelationId,
        dims: usize,
        metric: HnswMetric,
        m: usize,
        ef_search: usize,
        images: Vec<PageBackedHnswPageImage>,
    ) -> Result<Self, AccessMethodError> {
        if images.is_empty() {
            return Err(AccessMethodError::Storage(
                "hnsw page image set is empty".to_owned(),
            ));
        }
        let mut pages = BTreeMap::new();
        for image in images {
            if image.page_id.relation != index_rel {
                return Err(AccessMethodError::Storage(
                    "hnsw page image relation mismatch".to_owned(),
                ));
            }
            let block = image.page_id.block;
            let mut page = image.page;
            if page.page_id() != image.page_id {
                return Err(AccessMethodError::Storage(
                    "hnsw page image id mismatch".to_owned(),
                ));
            }
            page.set_lsn(image.lsn);
            if pages.insert(block, page).is_some() {
                return Err(AccessMethodError::Storage(
                    "hnsw duplicate page image block".to_owned(),
                ));
            }
        }

        let meta = match pages.get(&BlockNumber::new(HNSW_META_BLOCK)) {
            Some(HnswPersistentPage::Meta(meta)) => meta.clone(),
            _ => {
                return Err(AccessMethodError::Storage(
                    "hnsw page image set missing meta page".to_owned(),
                ));
            }
        };
        if meta.dims != dims || meta.metric != metric || meta.m != m || meta.ef_search != ef_search
        {
            return Err(AccessMethodError::Storage(
                "hnsw page image metadata mismatch".to_owned(),
            ));
        }
        let free_list = match pages.get(&BlockNumber::new(HNSW_FREE_LIST_BLOCK)) {
            Some(HnswPersistentPage::FreeList(free_list)) => free_list.clone(),
            _ => {
                return Err(AccessMethodError::Storage(
                    "hnsw page image set missing free-list page".to_owned(),
                ));
            }
        };

        let mut tid_to_node = BTreeMap::new();
        let mut node_to_block = BTreeMap::new();
        let mut live_nodes = 0;
        let mut tombstones = 0;
        for (block, page) in &pages {
            if let HnswPersistentPage::Node(node) = page {
                if node.vector_len != meta.dims {
                    return Err(AccessMethodError::Storage(
                        "hnsw node vector length mismatch".to_owned(),
                    ));
                }
                if node.node_id >= meta.next_node_id {
                    return Err(AccessMethodError::Storage(
                        "hnsw node id exceeds metadata".to_owned(),
                    ));
                }
                // Base layer keeps up to 2*m neighbors (M_max0); upper layers m.
                if node.neighbor_count > meta.m.saturating_mul(2) {
                    return Err(AccessMethodError::Storage(
                        "hnsw node base-layer neighbor count exceeds metadata".to_owned(),
                    ));
                }
                if node.level > HNSW_MAX_LEVEL || node.upper_levels.len() != node.level {
                    return Err(AccessMethodError::Storage(
                        "hnsw node level/upper-layer count inconsistent".to_owned(),
                    ));
                }
                if node.upper_levels.iter().any(|upper| upper.count > meta.m) {
                    return Err(AccessMethodError::Storage(
                        "hnsw node upper-layer neighbor count exceeds metadata".to_owned(),
                    ));
                }
                if tid_to_node.insert(node.tid, node.node_id).is_some() {
                    return Err(AccessMethodError::Storage(
                        "hnsw duplicate tuple id in page images".to_owned(),
                    ));
                }
                if node_to_block.insert(node.node_id, *block).is_some() {
                    return Err(AccessMethodError::Storage(
                        "hnsw duplicate node id in page images".to_owned(),
                    ));
                }
                if node.deleted {
                    tombstones += 1;
                } else {
                    live_nodes += 1;
                }
            }
        }

        let mut storage = Self {
            valid: true,
            pages,
            meta,
            free_list,
            tid_to_node,
            node_to_block,
            mirror: Vec::new(),
        };
        storage.meta.live_nodes = live_nodes;
        storage.meta.tombstones = tombstones;
        // Build the mirror first, then pick the entry point by level (the entry
        // selection reads node levels from the mirror).
        storage.rebuild_mirror()?;
        storage.meta.entry_node = storage.highest_level_live_node()?;
        storage.sync_control_pages();
        Ok(storage)
    }
}
