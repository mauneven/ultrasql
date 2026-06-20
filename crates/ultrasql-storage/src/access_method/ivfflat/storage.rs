//! `PageBackedIvfFlatStorage` page persistence and the page-backed AM impl.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use super::*;

impl PageBackedIvfFlatStorage {
    pub(crate) fn new(
        index_rel: RelationId,
        dims: usize,
        metric: HnswMetric,
        lists: usize,
        probes: usize,
        payload_kind: AnnPayloadKind,
    ) -> Result<Self, AccessMethodError> {
        let ctx = IvfFlatPageContext {
            index_rel,
            dims,
            metric,
            lists,
            probes,
            payload_kind,
        };
        let mut storage = Self {
            valid: true,
            pages: BTreeMap::new(),
            entries: Vec::new(),
            centroids: Vec::new(),
            lists: Vec::new(),
            tid_to_entry: BTreeMap::new(),
            next_block_number: IVFFLAT_FIRST_ALLOC_BLOCK,
            meta_lsn: Lsn::ZERO,
        };
        storage
            .sync_pages(ctx, Lsn::ZERO)
            .map_err(|err| AccessMethodError::Storage(format!("ivfflat metadata init: {err}")))?;
        Ok(storage)
    }

    pub(crate) fn clear(&mut self, ctx: IvfFlatPageContext) -> Result<(), AccessMethodError> {
        self.entries.clear();
        self.centroids.clear();
        self.lists.clear();
        self.tid_to_entry.clear();
        self.sync_pages(ctx, Lsn::ZERO)
    }

    pub(crate) fn ensure_list_slot(&mut self, list_id: usize) -> Result<(), AccessMethodError> {
        let needed = list_id
            .checked_add(1)
            .ok_or_else(|| AccessMethodError::Storage("ivfflat list id overflow".to_owned()))?;
        while self.centroids.len() < needed {
            self.centroids.push(Vec::new());
        }
        while self.lists.len() < needed {
            self.lists.push(Vec::new());
        }
        Ok(())
    }

    pub(crate) fn mark_deleted(
        &mut self,
        ctx: IvfFlatPageContext,
        tid: TupleId,
        replay: bool,
        page_lsn: Lsn,
    ) -> Result<(), AccessMethodError> {
        let Some(idx) = self.tid_to_entry.get(&tid).copied() else {
            if replay {
                return Ok(());
            }
            return Err(AccessMethodError::NotFound);
        };
        let Some(entry) = self.entries.get_mut(idx) else {
            if replay {
                return Ok(());
            }
            return Err(AccessMethodError::NotFound);
        };
        if entry.deleted {
            return Ok(());
        }
        entry.deleted = true;
        self.sync_pages(ctx, page_lsn)
    }

    pub(crate) fn compact_deleted(
        &mut self,
        ctx: IvfFlatPageContext,
        page_lsn: Lsn,
    ) -> Result<usize, AccessMethodError> {
        let before = self.entries.len();
        if before == 0 {
            return Ok(0);
        }
        let mut remap = vec![None; before];
        let mut entries = Vec::with_capacity(before);
        for (old_idx, entry) in self.entries.iter().enumerate() {
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
        let mut new_lists = vec![Vec::new(); self.centroids.len()];
        for old_list in &self.lists {
            for old_idx in old_list {
                if let Some(new_idx) = remap.get(*old_idx).and_then(|idx| *idx) {
                    let list_id = entries[new_idx].list_id;
                    if list_id >= new_lists.len() {
                        return Err(AccessMethodError::Storage(
                            "page-backed ivfflat compact found invalid list id".to_owned(),
                        ));
                    }
                    new_lists[list_id].push(new_idx);
                }
            }
        }
        self.entries = entries;
        self.lists = new_lists;
        self.tid_to_entry.clear();
        for (idx, entry) in self.entries.iter().enumerate() {
            self.tid_to_entry.insert(entry.tid, idx);
        }
        self.sync_pages(ctx, page_lsn)?;
        Ok(removed)
    }

    pub(crate) fn sync_pages(&mut self, ctx: IvfFlatPageContext, lsn: Lsn) -> Result<(), AccessMethodError> {
        self.pages.clear();
        let live_entries = self.entries.iter().filter(|entry| !entry.deleted).count();
        let tombstones = self.entries.iter().filter(|entry| entry.deleted).count();
        let mut next_block = IVFFLAT_FIRST_ALLOC_BLOCK;
        self.pages.insert(
            BlockNumber::new(IVFFLAT_META_BLOCK),
            IvfFlatPersistentPage::Meta(IvfFlatMetaPage {
                page_id: PageId::new(ctx.index_rel, BlockNumber::new(IVFFLAT_META_BLOCK)),
                lsn,
                dims: ctx.dims,
                metric: ctx.metric,
                lists: ctx.lists,
                probes: ctx.probes,
                payload_kind: ctx.payload_kind,
                live_entries,
                tombstones,
                next_block_number: next_block,
            }),
        );
        for (list_id, centroid) in self.centroids.iter().enumerate() {
            if centroid.is_empty() {
                continue;
            }
            let block = alloc_ivfflat_block(&mut next_block)?;
            self.pages.insert(
                block,
                IvfFlatPersistentPage::Centroid(IvfFlatCentroidPage {
                    page_id: PageId::new(ctx.index_rel, block),
                    lsn,
                    list_id,
                    vector: centroid.clone(),
                }),
            );
        }
        for (list_id, entry_indices) in self.lists.iter().enumerate() {
            let block = alloc_ivfflat_block(&mut next_block)?;
            self.pages.insert(
                block,
                IvfFlatPersistentPage::List(IvfFlatListPage {
                    page_id: PageId::new(ctx.index_rel, block),
                    lsn,
                    list_id,
                    entry_indices: entry_indices.clone(),
                }),
            );
        }
        for (entry_id, entry) in self.entries.iter().enumerate() {
            let block = alloc_ivfflat_block(&mut next_block)?;
            self.pages.insert(
                block,
                IvfFlatPersistentPage::Entry(IvfFlatEntryPage {
                    page_id: PageId::new(ctx.index_rel, block),
                    lsn,
                    entry_id,
                    list_id: entry.list_id,
                    payload: AnnVectorPayload::new(ctx.payload_kind, &entry.vector)?,
                    tid: entry.tid,
                    deleted: entry.deleted,
                }),
            );
        }
        self.next_block_number = next_block;
        if let Some(IvfFlatPersistentPage::Meta(meta)) =
            self.pages.get_mut(&BlockNumber::new(IVFFLAT_META_BLOCK))
        {
            meta.next_block_number = next_block;
        }
        // Advance the snapshot high-water mark. WAL LSNs are monotonic, so the
        // last applied op carries the largest LSN; `max` also makes the
        // `Lsn::ZERO` calls from `new`/`clear` (which carry no durability point)
        // harmless rather than regressing the mark.
        if lsn.raw() > self.meta_lsn.raw() {
            self.meta_lsn = lsn;
        }
        Ok(())
    }

    /// Whether the WAL record at `lsn` is already reflected in this state — used
    /// to skip records at or below a loaded snapshot during restart replay.
    /// `Lsn::ZERO` is never "covered": unstamped (non-logged) state imposes no
    /// replay floor.
    pub(crate) fn redo_covered(&self, lsn: Lsn) -> bool {
        lsn != Lsn::ZERO && self.meta_lsn.raw() >= lsn.raw()
    }
}

impl AccessMethod for PageBackedIvfFlatIndex {
    fn name(&self) -> &'static str {
        "ivfflat"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let vector = decode_vector_key(key, self.dims, "page-backed ivfflat")?;
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
