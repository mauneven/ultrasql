//! `PageBackedIvfFlatIndex` construction, snapshots, and stats.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use super::*;

impl PageBackedIvfFlatIndex {
    /// Create an empty page-backed IVFFlat index.
    pub fn new(
        index_rel: RelationId,
        dims: u32,
        metric: HnswMetric,
        lists: usize,
        probes: usize,
    ) -> Result<Self, AccessMethodError> {
        Self::new_with_payload_kind(index_rel, dims, metric, lists, probes, AnnPayloadKind::F32)
    }

    /// Create an empty page-backed IVFFlat index with an ANN payload kind.
    pub fn new_with_payload_kind(
        index_rel: RelationId,
        dims: u32,
        metric: HnswMetric,
        lists: usize,
        probes: usize,
        payload_kind: AnnPayloadKind,
    ) -> Result<Self, AccessMethodError> {
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return Err(AccessMethodError::Storage(
                "page-backed ivfflat dims outside supported range".to_owned(),
            ));
        }
        if lists == 0 {
            return Err(AccessMethodError::Storage(
                "page-backed ivfflat lists must be greater than zero".to_owned(),
            ));
        }
        if probes == 0 {
            return Err(AccessMethodError::Storage(
                "page-backed ivfflat probes must be greater than zero".to_owned(),
            ));
        }
        let dims = usize::try_from(dims).map_err(|_| {
            AccessMethodError::Storage("page-backed ivfflat dims do not fit usize".to_owned())
        })?;
        let storage =
            PageBackedIvfFlatStorage::new(index_rel, dims, metric, lists, probes, payload_kind)?;
        Ok(Self {
            storage: Mutex::new(storage),
            index_rel,
            dims,
            metric,
            lists,
            probes,
            payload_kind,
        })
    }

    pub(crate) fn page_context(&self) -> IvfFlatPageContext {
        IvfFlatPageContext {
            index_rel: self.index_rel,
            dims: self.dims,
            metric: self.metric,
            lists: self.lists,
            probes: self.probes,
            payload_kind: self.payload_kind,
        }
    }

    /// Return page and tuple counts for this page-backed index.
    #[must_use]
    pub fn page_stats(&self) -> PageBackedIvfFlatStats {
        let storage = self.storage.lock();
        let mut stats = PageBackedIvfFlatStats {
            live_entries: storage
                .entries
                .iter()
                .filter(|entry| !entry.deleted)
                .count(),
            tombstones: storage.entries.iter().filter(|entry| entry.deleted).count(),
            next_block_number: storage.next_block_number,
            ..PageBackedIvfFlatStats::default()
        };
        for page in storage.pages.values() {
            match page {
                IvfFlatPersistentPage::Meta(meta) => {
                    let _ = (
                        meta.page_id,
                        meta.lsn,
                        meta.dims,
                        meta.metric,
                        meta.lists,
                        meta.probes,
                        meta.payload_kind,
                        meta.live_entries,
                        meta.tombstones,
                        meta.next_block_number,
                    );
                    stats.meta_pages += 1;
                }
                IvfFlatPersistentPage::Centroid(centroid) => {
                    let _ = (
                        centroid.page_id,
                        centroid.lsn,
                        centroid.list_id,
                        centroid.vector.len(),
                    );
                    stats.centroid_pages += 1;
                }
                IvfFlatPersistentPage::List(list) => {
                    let _ = (
                        list.page_id,
                        list.lsn,
                        list.list_id,
                        list.entry_indices.len(),
                    );
                    stats.list_pages += 1;
                }
                IvfFlatPersistentPage::Entry(entry) => {
                    let _ = (
                        entry.page_id,
                        entry.lsn,
                        entry.entry_id,
                        entry.list_id,
                        entry.payload.quantized_len_bytes(),
                        entry.tid,
                        entry.deleted,
                    );
                    stats.entry_pages += 1;
                }
            }
        }
        stats
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

    /// The WAL LSN a durable snapshot is consistent as of (the high-water mark of
    /// applied mutations). Callers compare it against the replayed WAL tail to
    /// decide whether a snapshot can be trusted or a full replay is required.
    #[must_use]
    pub fn snapshot_lsn(&self) -> Lsn {
        self.storage.lock().meta_lsn
    }

    /// Return number of trained centroids.
    #[must_use]
    pub fn centroid_count(&self) -> usize {
        self.storage.lock().centroids.len()
    }

    /// Return number of materialized inverted lists.
    #[must_use]
    pub fn list_count(&self) -> usize {
        self.storage.lock().lists.len()
    }

    /// Return number of live entries.
    #[must_use]
    pub fn live_len(&self) -> usize {
        self.page_stats().live_entries
    }

    /// Return number of tombstoned entries awaiting compaction.
    #[must_use]
    pub fn tombstone_count(&self) -> usize {
        self.page_stats().tombstones
    }

    /// Return whether the page-backed IVFFlat lists can currently be used.
    #[must_use]
    pub fn is_available(&self) -> bool {
        let storage = self.storage.lock();
        storage.valid
            && storage.entries.iter().any(|entry| !entry.deleted)
            && !storage.centroids.is_empty()
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

    /// Serialize the index's logical state to a self-describing, checksummed byte
    /// buffer reloadable with [`Self::from_snapshot_bytes`].
    ///
    /// Unlike HNSW (whose `pages` are authoritative), an IVFFlat index's pages,
    /// `tid_to_entry`, and `next_block_number` are all *derived* from the logical
    /// vectors via `sync_pages`. So the snapshot captures only the authoritative
    /// logical state — config, the snapshot LSN, the centroid slots, and the
    /// entries (exact f32 vectors, tid, list, tombstone flag) — and the loader
    /// re-derives the rest deterministically. Captured under one storage lock for
    /// internal consistency. Purely additive: it never mutates the index.
    #[must_use]
    pub fn encode_snapshot(&self) -> Vec<u8> {
        let (centroids, entries, snapshot_lsn) = {
            let storage = self.storage.lock();
            (
                storage.centroids.clone(),
                storage.entries.clone(),
                storage.meta_lsn,
            )
        };

        let mut out = Vec::new();
        out.extend_from_slice(IVFFLAT_SNAPSHOT_MAGIC);
        out.extend_from_slice(&IVFFLAT_SNAPSHOT_VERSION.to_le_bytes());
        out.extend_from_slice(&self.index_rel.oid().raw().to_le_bytes());
        let dims_u32 = u32::try_from(self.dims).unwrap_or(u32::MAX);
        out.extend_from_slice(&dims_u32.to_le_bytes());
        out.push(encode_hnsw_metric(self.metric));
        let lists_u32 = u32::try_from(self.lists).unwrap_or(u32::MAX);
        out.extend_from_slice(&lists_u32.to_le_bytes());
        let probes_u32 = u32::try_from(self.probes).unwrap_or(u32::MAX);
        out.extend_from_slice(&probes_u32.to_le_bytes());
        out.push(encode_ann_payload_kind(self.payload_kind));
        out.extend_from_slice(&snapshot_lsn.raw().to_le_bytes());

        // Centroid slots, preserving empty slots so list ids stay stable indices.
        let centroid_slots = u32::try_from(centroids.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&centroid_slots.to_le_bytes());
        for centroid in &centroids {
            push_vec_f32(&mut out, centroid);
        }

        // Entries (including tombstones — search filters them, but they must
        // survive so a later compaction record replays consistently).
        let entry_count = u32::try_from(entries.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&entry_count.to_le_bytes());
        for entry in &entries {
            push_vec_f32(&mut out, &entry.vector);
            push_tuple_id(&mut out, entry.tid);
            let list_u32 = u32::try_from(entry.list_id).unwrap_or(u32::MAX);
            out.extend_from_slice(&list_u32.to_le_bytes());
            out.push(u8::from(entry.deleted));
        }

        let checksum = crc32c::crc32c(&out);
        out.extend_from_slice(&checksum.to_le_bytes());
        out
    }

    /// Reconstruct a page-backed IVFFlat index from a buffer produced by
    /// [`Self::encode_snapshot`].
    ///
    /// Validation is strict: magic, version, trailing `crc32c`, the encoded
    /// relation oid (must equal `index_rel`), every length/tag/bound, vector
    /// finiteness, list-id range, centroid presence for non-empty lists, and tid
    /// uniqueness must all pass. Any mismatch or short read returns an error
    /// rather than panicking, so a corrupt snapshot can never silently yield a
    /// wrong index — callers fall back to full WAL replay.
    pub fn from_snapshot_bytes(
        index_rel: RelationId,
        bytes: &[u8],
    ) -> Result<Self, AccessMethodError> {
        let body_len = bytes.len().checked_sub(4).ok_or_else(|| {
            AccessMethodError::Storage("ivfflat snapshot too short for checksum".to_owned())
        })?;
        let (body, checksum_bytes) = bytes.split_at(body_len);
        let stored_checksum = u32::from_le_bytes(checksum_bytes.try_into().map_err(|_| {
            AccessMethodError::Storage("ivfflat snapshot checksum read".to_owned())
        })?);
        if crc32c::crc32c(body) != stored_checksum {
            return Err(AccessMethodError::Storage(
                "ivfflat snapshot checksum mismatch".to_owned(),
            ));
        }

        let mut cursor = SnapshotCursor::new(body);
        let magic = cursor.take(IVFFLAT_SNAPSHOT_MAGIC.len())?;
        if magic != IVFFLAT_SNAPSHOT_MAGIC {
            return Err(AccessMethodError::Storage(
                "ivfflat snapshot magic mismatch".to_owned(),
            ));
        }
        let version = cursor.take_u32()?;
        if version != IVFFLAT_SNAPSHOT_VERSION {
            return Err(AccessMethodError::Storage(format!(
                "ivfflat snapshot version {version} unsupported"
            )));
        }
        let rel_oid = cursor.take_u32()?;
        if rel_oid != index_rel.oid().raw() {
            return Err(AccessMethodError::Storage(
                "ivfflat snapshot relation mismatch".to_owned(),
            ));
        }
        let dims_u32 = cursor.take_u32()?;
        if dims_u32 == 0 || dims_u32 > MAX_VECTOR_DIMS {
            return Err(AccessMethodError::Storage(
                "ivfflat snapshot dims outside supported range".to_owned(),
            ));
        }
        let dims = usize::try_from(dims_u32).map_err(|_| {
            AccessMethodError::Storage("ivfflat snapshot dims do not fit usize".to_owned())
        })?;
        let metric = decode_hnsw_metric(cursor.take_u8()?)?;
        let lists = cursor.take_usize_len_u32()?;
        let probes = cursor.take_usize_len_u32()?;
        if lists == 0 || probes == 0 {
            return Err(AccessMethodError::Storage(
                "ivfflat snapshot lists/probes must be greater than zero".to_owned(),
            ));
        }
        let payload_kind = decode_ann_payload_kind(cursor.take_u8()?)?;
        let snapshot_lsn = Lsn::new(cursor.take_u64()?);

        // Centroid slots.
        let centroid_slots = cursor.take_usize_len_u32()?;
        if centroid_slots > lists {
            return Err(AccessMethodError::Storage(
                "ivfflat snapshot centroid slots exceed configured lists".to_owned(),
            ));
        }
        let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(centroid_slots.min(1 << 16));
        for _ in 0..centroid_slots {
            let centroid = take_vec_f32(&mut cursor, dims, true)?;
            centroids.push(centroid);
        }

        // Entries; derive `lists` (ascending entry-index grouping) and
        // `tid_to_entry` exactly as the live insert path maintains them.
        let entry_count = cursor.take_usize_len_u32()?;
        let mut entries: Vec<IvfFlatEntry> = Vec::with_capacity(entry_count.min(1 << 20));
        let mut list_buckets: Vec<Vec<usize>> = vec![Vec::new(); centroids.len()];
        let mut tid_to_entry: BTreeMap<TupleId, usize> = BTreeMap::new();
        for idx in 0..entry_count {
            let vector = take_vec_f32(&mut cursor, dims, false)?;
            let tid = decode_tuple_id(&mut cursor)?;
            let list_id = cursor.take_usize_len_u32()?;
            let deleted = cursor.take_bool()?;
            if list_id >= centroids.len() {
                return Err(AccessMethodError::Storage(
                    "ivfflat snapshot entry list id out of range".to_owned(),
                ));
            }
            if tid_to_entry.insert(tid, idx).is_some() {
                return Err(AccessMethodError::Storage(
                    "ivfflat snapshot has a duplicate tuple id".to_owned(),
                ));
            }
            list_buckets[list_id].push(idx);
            entries.push(IvfFlatEntry {
                vector,
                tid,
                list_id,
                deleted,
            });
        }
        if !cursor.is_empty() {
            return Err(AccessMethodError::Storage(
                "ivfflat snapshot has trailing bytes".to_owned(),
            ));
        }

        // Every populated list must have a real centroid of the right dimension,
        // or search over it would be undefined.
        for (list_id, bucket) in list_buckets.iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            match centroids.get(list_id) {
                Some(centroid) if centroid.len() == dims => {}
                _ => {
                    return Err(AccessMethodError::Storage(
                        "ivfflat snapshot entry references a list without a centroid".to_owned(),
                    ));
                }
            }
        }

        let ctx = IvfFlatPageContext {
            index_rel,
            dims,
            metric,
            lists,
            probes,
            payload_kind,
        };
        let mut storage = PageBackedIvfFlatStorage {
            valid: true,
            pages: BTreeMap::new(),
            entries,
            centroids,
            lists: list_buckets,
            tid_to_entry,
            next_block_number: IVFFLAT_FIRST_ALLOC_BLOCK,
            meta_lsn: snapshot_lsn,
        };
        // Re-derive pages, the meta page, and `next_block_number`; also stamps the
        // page LSNs and re-quantizes entry payloads from the exact vectors.
        storage.sync_pages(ctx, snapshot_lsn)?;
        Ok(Self {
            storage: Mutex::new(storage),
            index_rel,
            dims,
            metric,
            lists,
            probes,
            payload_kind,
        })
    }
}
