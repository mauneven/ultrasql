//! `PageBackedHnswIndex` construction, snapshots, and page-stat helpers.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use super::*;

impl PageBackedHnswIndex {
    /// Create an empty page-backed HNSW graph arena.
    pub fn new(
        index_rel: RelationId,
        dims: u32,
        metric: HnswMetric,
        m: usize,
        ef_search: usize,
    ) -> Result<Self, AccessMethodError> {
        Self::new_with_payload_kind(index_rel, dims, metric, m, ef_search, AnnPayloadKind::F32)
    }

    /// Create an empty page-backed HNSW graph arena with an ANN payload kind.
    pub fn new_with_payload_kind(
        index_rel: RelationId,
        dims: u32,
        metric: HnswMetric,
        m: usize,
        ef_search: usize,
        payload_kind: AnnPayloadKind,
    ) -> Result<Self, AccessMethodError> {
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw dims outside supported range".to_owned(),
            ));
        }
        if m == 0 {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw m must be greater than zero".to_owned(),
            ));
        }
        if ef_search == 0 {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw ef_search must be greater than zero".to_owned(),
            ));
        }
        let dims = usize::try_from(dims).map_err(|_| {
            AccessMethodError::Storage("page-backed hnsw dims do not fit usize".to_owned())
        })?;
        Ok(Self {
            storage: Mutex::new(PageBackedHnswStorage::new(
                index_rel,
                dims,
                metric,
                m,
                ef_search,
                payload_kind,
            )),
            index_rel,
            dims,
            metric,
            m,
            ef_search,
            payload_kind,
            build_traversal_work_threshold: HNSW_BUILD_TRAVERSAL_WORK_THRESHOLD,
        })
    }

    /// Rebuild a page-backed HNSW graph from buffer-pool page images.
    pub fn from_page_images(
        index_rel: RelationId,
        dims: u32,
        metric: HnswMetric,
        m: usize,
        ef_search: usize,
        images: Vec<PageBackedHnswPageImage>,
    ) -> Result<Self, AccessMethodError> {
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw dims outside supported range".to_owned(),
            ));
        }
        if m == 0 {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw m must be greater than zero".to_owned(),
            ));
        }
        if ef_search == 0 {
            return Err(AccessMethodError::Storage(
                "page-backed hnsw ef_search must be greater than zero".to_owned(),
            ));
        }
        let dims = usize::try_from(dims).map_err(|_| {
            AccessMethodError::Storage("page-backed hnsw dims do not fit usize".to_owned())
        })?;
        let storage =
            PageBackedHnswStorage::from_page_images(index_rel, dims, metric, m, ef_search, images)?;
        let payload_kind = storage.meta.payload_kind;
        Ok(Self {
            storage: Mutex::new(storage),
            index_rel,
            dims,
            metric,
            m,
            ef_search,
            payload_kind,
            build_traversal_work_threshold: HNSW_BUILD_TRAVERSAL_WORK_THRESHOLD,
        })
    }

    /// Override the build-time exhaustive-scan vs graph-traversal work threshold.
    /// Test-only: lets a small fixture exercise the traversal build path without
    /// inserting the ~8k vectors the production threshold would otherwise need.
    #[cfg(test)]
    pub(crate) fn with_build_traversal_work_threshold(mut self, threshold: usize) -> Self {
        self.build_traversal_work_threshold = threshold;
        self
    }

    /// The index's configured default exploration budget (`ef_search`).
    ///
    /// Callers that override `ef_search` per query (filtered ANN over-fetch,
    /// recall/latency sweeps) use this as a floor so a query never explores less
    /// than the index was built to.
    #[must_use]
    pub const fn ef_search(&self) -> usize {
        self.ef_search
    }

    /// Export buffer-pool-style page images in block-number order.
    #[must_use]
    pub fn page_images(&self) -> Vec<PageBackedHnswPageImage> {
        let storage = self.storage.lock();
        storage
            .pages
            .values()
            .map(|page| PageBackedHnswPageImage {
                page_id: page.page_id(),
                lsn: page.lsn(),
                page: page.clone(),
            })
            .collect()
    }

    /// Return the high-water WAL LSN reflected in this index's meta page.
    ///
    /// This is the LSN a durable snapshot is consistent as of; callers compare
    /// it against the replayed WAL tail to decide whether the snapshot can be
    /// trusted or a full replay is required.
    #[must_use]
    pub fn snapshot_lsn(&self) -> Lsn {
        self.storage.lock().meta.lsn
    }

    /// Serialize the page-backed graph to a self-describing, checksummed byte
    /// buffer that can later be reloaded with [`Self::from_snapshot_bytes`].
    ///
    /// The buffer is versioned, length-explicit, little-endian, and ends with a
    /// `crc32c` checksum over all preceding bytes. It captures every page image
    /// plus the index parameters under a single storage lock so the snapshot is
    /// internally consistent. This is purely additive: it never mutates the
    /// index and adds no production call sites, so runtime behavior is
    /// unchanged.
    #[must_use]
    pub fn encode_snapshot(&self) -> Vec<u8> {
        // Capture everything under one lock for a consistent snapshot.
        let (images, snapshot_lsn) = {
            let storage = self.storage.lock();
            let images: Vec<PageBackedHnswPageImage> = storage
                .pages
                .values()
                .map(|page| PageBackedHnswPageImage {
                    page_id: page.page_id(),
                    lsn: page.lsn(),
                    page: page.clone(),
                })
                .collect();
            (images, storage.meta.lsn)
        };

        let mut out = Vec::new();
        out.extend_from_slice(HNSW_SNAPSHOT_MAGIC);
        out.extend_from_slice(&HNSW_SNAPSHOT_VERSION.to_le_bytes());
        out.extend_from_slice(&self.index_rel.oid().raw().to_le_bytes());
        // `dims` is validated to fit u32 on construction; encode losslessly.
        let dims_u32 = u32::try_from(self.dims).unwrap_or(u32::MAX);
        out.extend_from_slice(&dims_u32.to_le_bytes());
        out.push(encode_hnsw_metric(self.metric));
        let m_u32 = u32::try_from(self.m).unwrap_or(u32::MAX);
        out.extend_from_slice(&m_u32.to_le_bytes());
        let ef_u32 = u32::try_from(self.ef_search).unwrap_or(u32::MAX);
        out.extend_from_slice(&ef_u32.to_le_bytes());
        out.push(encode_ann_payload_kind(self.payload_kind));
        out.extend_from_slice(&snapshot_lsn.raw().to_le_bytes());
        let page_count = u32::try_from(images.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&page_count.to_le_bytes());

        for image in &images {
            encode_hnsw_page_record(&mut out, image);
        }

        let checksum = crc32c::crc32c(&out);
        out.extend_from_slice(&checksum.to_le_bytes());
        out
    }

    /// Reconstruct a page-backed graph from a buffer produced by
    /// [`Self::encode_snapshot`].
    ///
    /// Validation is strict: the magic, version, trailing `crc32c`, the encoded
    /// index relation oid (which must equal `index_rel`), every embedded length
    /// and tag, and every bounds check must pass. Any mismatch or short read
    /// returns [`AccessMethodError`] rather than panicking, so a corrupt
    /// snapshot can never silently yield a wrong index — callers fall back to a
    /// full WAL replay.
    pub fn from_snapshot_bytes(
        index_rel: RelationId,
        bytes: &[u8],
    ) -> Result<Self, AccessMethodError> {
        let body_len = bytes.len().checked_sub(4).ok_or_else(|| {
            AccessMethodError::Storage("hnsw snapshot too short for checksum".to_owned())
        })?;
        let (body, checksum_bytes) = bytes.split_at(body_len);
        let stored_checksum =
            u32::from_le_bytes(checksum_bytes.try_into().map_err(|_| {
                AccessMethodError::Storage("hnsw snapshot checksum read".to_owned())
            })?);
        if crc32c::crc32c(body) != stored_checksum {
            return Err(AccessMethodError::Storage(
                "hnsw snapshot checksum mismatch".to_owned(),
            ));
        }

        let mut cursor = SnapshotCursor::new(body);
        let magic = cursor.take(HNSW_SNAPSHOT_MAGIC.len())?;
        if magic != HNSW_SNAPSHOT_MAGIC {
            return Err(AccessMethodError::Storage(
                "hnsw snapshot magic mismatch".to_owned(),
            ));
        }
        let version = cursor.take_u32()?;
        // v1 snapshots predate hierarchical layers: every node is base-only
        // (`level == 0`) and has no upper-layer trailer. v2 adds the trailer.
        if version == 0 || version > HNSW_SNAPSHOT_VERSION {
            return Err(AccessMethodError::Storage(format!(
                "hnsw snapshot version {version} unsupported"
            )));
        }
        let rel_oid = cursor.take_u32()?;
        if rel_oid != index_rel.oid().raw() {
            return Err(AccessMethodError::Storage(
                "hnsw snapshot relation mismatch".to_owned(),
            ));
        }
        let dims = cursor.take_u32()?;
        let metric = decode_hnsw_metric(cursor.take_u8()?)?;
        let m = cursor.take_usize_len_u32()?;
        let ef_search = cursor.take_usize_len_u32()?;
        let payload_kind = decode_ann_payload_kind(cursor.take_u8()?)?;
        let snapshot_lsn = Lsn::new(cursor.take_u64()?);
        let page_count = cursor.take_u32()?;
        let page_count_usize = usize::try_from(page_count).map_err(|_| {
            AccessMethodError::Storage("hnsw snapshot page count overflow".to_owned())
        })?;

        let mut images = Vec::with_capacity(page_count_usize.min(1 << 16));
        for _ in 0..page_count_usize {
            images.push(decode_hnsw_page_record(
                &mut cursor,
                index_rel,
                payload_kind,
                version,
            )?);
        }
        if !cursor.is_empty() {
            return Err(AccessMethodError::Storage(
                "hnsw snapshot has trailing bytes".to_owned(),
            ));
        }

        // The meta page (rebuilt inside `from_page_images`) is the source of
        // truth for `payload_kind`; the header copy above is only used to drive
        // per-page vector decoding, and `from_page_images` cross-checks the rest.
        let index = Self::from_page_images(index_rel, dims, metric, m, ef_search, images)?;
        let _ = snapshot_lsn;
        Ok(index)
    }

    /// Return page and tuple counts for this page-backed graph.
    #[must_use]
    pub fn page_stats(&self) -> PageBackedHnswStats {
        let storage = self.storage.lock();
        let mut stats = PageBackedHnswStats {
            live_nodes: storage.meta.live_nodes,
            tombstones: storage.meta.tombstones,
            reusable_pages: storage.free_list.blocks.len(),
            next_block_number: storage.meta.next_block_number,
            ..PageBackedHnswStats::default()
        };
        for page in storage.pages.values() {
            match page {
                HnswPersistentPage::Meta(meta) => {
                    let _ = (
                        meta.page_id,
                        meta.dims,
                        meta.metric,
                        meta.m,
                        meta.ef_search,
                        meta.payload_kind,
                        meta.free_list_page,
                    );
                    stats.meta_pages += 1;
                }
                HnswPersistentPage::Node(node) => {
                    let _ = (node.page_id, node.node_id);
                    stats.node_pages += 1;
                }
                HnswPersistentPage::Overflow(overflow) => {
                    let _ = (overflow.page_id, overflow.owner_node);
                    stats.overflow_pages += 1;
                }
                HnswPersistentPage::FreeList(free_list) => {
                    let _ = free_list.page_id;
                    stats.free_list_pages += 1;
                }
            }
        }
        stats
    }

    /// Deterministic snapshot of every node's level and per-layer neighbor lists,
    /// ordered by node id. Used by tests to assert that two builds of the same
    /// insert sequence produce an identical graph (the property WAL replay relies
    /// on). Reads the durable pages (not the mirror) so it asserts the on-disk
    /// graph that recovery reconstructs.
    #[cfg(test)]
    pub(crate) fn debug_neighbor_lists(&self) -> Vec<(HnswNodeId, usize, Vec<Vec<HnswNodeId>>)> {
        let storage = self.storage.lock();
        let mut out = Vec::with_capacity(storage.node_to_block.len());
        for node_id in storage.node_to_block.keys() {
            let level = storage
                .node_page(*node_id)
                .expect("node page")
                .map_or(0, |node| node.level);
            let mut levels = Vec::with_capacity(level + 1);
            for lvl in 0..=level {
                levels.push(
                    storage
                        .neighbors_from_pages_at_level(*node_id, lvl)
                        .expect("read neighbor list"),
                );
            }
            out.push((*node_id, level, levels));
        }
        out
    }

    /// Assert the in-memory mirror matches the durable page state.
    #[cfg(test)]
    pub(crate) fn assert_mirror_consistent(&self) {
        self.storage.lock().assert_mirror_consistent();
    }

    /// Distance metric attached to this graph.
    #[must_use]
    pub const fn metric(&self) -> HnswMetric {
        self.metric
    }

    /// Vector dimensionality this graph indexes.
    #[must_use]
    pub const fn dims(&self) -> usize {
        self.dims
    }
}
