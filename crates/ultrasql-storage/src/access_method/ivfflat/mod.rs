//! IVFFlat (inverted-file flat) vector index: runtime and page-backed types.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

pub(crate) use std::collections::{BTreeMap, BTreeSet};

pub(crate) use num_traits::ToPrimitive;
pub(crate) use parking_lot::Mutex;
pub(crate) use ultrasql_core::{
    BlockNumber, Lsn, MAX_VECTOR_DIMS, PageId, RelationId, TupleId, Xid,
};
pub(crate) use ultrasql_wal::WalRecord;
pub(crate) use ultrasql_wal::payload::{IvfFlatOpKind, IvfFlatOpPayload};
pub(crate) use ultrasql_wal::record::RecordType;

pub(crate) use super::ann::{AnnPayloadKind, AnnRerankPolicy, AnnVectorPayload, HnswMetric};
pub(crate) use super::hnsw_build::{ann_wal_index_rel, decode_vector_key};
pub(crate) use super::hnsw_page::{
    SnapshotCursor, decode_ann_payload_kind, decode_hnsw_metric, decode_tuple_id,
    encode_ann_payload_kind, encode_hnsw_metric, push_tuple_id, push_vec_f32, take_vec_f32,
};
pub(crate) use super::{AccessMethod, AccessMethodError};
pub(crate) use crate::wal_sink::WalSink;

mod index;
mod ops;
mod runtime;
mod storage;

/// One result from an IVFFlat search.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IvfFlatSearchResult {
    /// Heap tuple identifier stored in the inverted list.
    pub tid: TupleId,
    /// Exact distance from the search probe after candidate rerank.
    pub distance: f32,
}

/// In-memory IVFFlat vector index.
///
/// Bulk load trains deterministic centroids, assigns vectors into inverted
/// lists, and search probes the nearest `probes` lists before reranking all
/// candidates with the same exact SIMD-aware kernels used by scalar vector SQL.
/// Online DML appends to the nearest trained list and tombstones deletes; a
/// full page-backed build/replay format remains future storage work.
#[derive(Debug)]
pub struct IvfFlatIndex {
    storage: Mutex<IvfFlatStorage>,
    dims: usize,
    metric: HnswMetric,
    lists: usize,
    probes: usize,
}

#[derive(Debug, Default)]
struct IvfFlatStorage {
    entries: Vec<IvfFlatEntry>,
    centroids: Vec<Vec<f32>>,
    lists: Vec<Vec<usize>>,
    available: bool,
}

#[derive(Debug, Clone)]
struct IvfFlatEntry {
    vector: Vec<f32>,
    tid: TupleId,
    list_id: usize,
    deleted: bool,
}


const IVFFLAT_META_BLOCK: u32 = 0;
const IVFFLAT_FIRST_ALLOC_BLOCK: u32 = 1;

/// Magic for a durable page-backed IVFFlat snapshot. Distinct from HNSW's
/// `USQLHNS1` so a cross-loaded file is rejected by the magic check.
const IVFFLAT_SNAPSHOT_MAGIC: &[u8; 8] = b"USQLIFF1";
const IVFFLAT_SNAPSHOT_VERSION: u32 = 1;

/// Page counts and MVCC-visible entry counts for a page-backed IVFFlat index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PageBackedIvfFlatStats {
    /// Number of IVFFlat meta pages. Always one for a single index relation.
    pub meta_pages: usize,
    /// Number of centroid pages.
    pub centroid_pages: usize,
    /// Number of inverted-list directory pages.
    pub list_pages: usize,
    /// Number of physical entry pages, including tombstones before compaction.
    pub entry_pages: usize,
    /// Number of non-tombstoned entries.
    pub live_entries: usize,
    /// Number of tombstoned entries waiting for VACUUM.
    pub tombstones: usize,
    /// Next block number that would be allocated by the page arena.
    pub next_block_number: u32,
}

/// First page-backed IVFFlat storage model.
///
/// The arena stores centroids, list directories, and entry records as
/// page-shaped structures with logical WAL replay. Search still reranks exact
/// distances from selected lists, so this serves as the persistent IVFFlat
/// correctness baseline before a full buffer-pool integration.
#[derive(Debug)]
pub struct PageBackedIvfFlatIndex {
    storage: Mutex<PageBackedIvfFlatStorage>,
    index_rel: RelationId,
    dims: usize,
    metric: HnswMetric,
    lists: usize,
    probes: usize,
    payload_kind: AnnPayloadKind,
}

#[derive(Debug)]
struct PageBackedIvfFlatStorage {
    valid: bool,
    pages: BTreeMap<BlockNumber, IvfFlatPersistentPage>,
    entries: Vec<IvfFlatEntry>,
    centroids: Vec<Vec<f32>>,
    lists: Vec<Vec<usize>>,
    tid_to_entry: BTreeMap<TupleId, usize>,
    next_block_number: u32,
    /// Highest WAL LSN whose effect is reflected in this state — the snapshot
    /// high-water mark. Advanced monotonically by [`Self::sync_pages`] on every
    /// applied mutation. A durable snapshot stamps this so restart replay can
    /// skip records at or below it (see [`Self::redo_covered`]), mirroring HNSW.
    meta_lsn: Lsn,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct IvfFlatPageContext {
    index_rel: RelationId,
    dims: usize,
    metric: HnswMetric,
    lists: usize,
    probes: usize,
    payload_kind: AnnPayloadKind,
}

#[derive(Debug, Clone)]
enum IvfFlatPersistentPage {
    Meta(IvfFlatMetaPage),
    Centroid(IvfFlatCentroidPage),
    List(IvfFlatListPage),
    Entry(IvfFlatEntryPage),
}

#[derive(Debug, Clone)]
struct IvfFlatMetaPage {
    page_id: PageId,
    lsn: Lsn,
    dims: usize,
    metric: HnswMetric,
    lists: usize,
    probes: usize,
    payload_kind: AnnPayloadKind,
    live_entries: usize,
    tombstones: usize,
    next_block_number: u32,
}

#[derive(Debug, Clone)]
struct IvfFlatCentroidPage {
    page_id: PageId,
    lsn: Lsn,
    list_id: usize,
    vector: Vec<f32>,
}

#[derive(Debug, Clone)]
struct IvfFlatListPage {
    page_id: PageId,
    lsn: Lsn,
    list_id: usize,
    entry_indices: Vec<usize>,
}

#[derive(Debug, Clone)]
struct IvfFlatEntryPage {
    page_id: PageId,
    lsn: Lsn,
    entry_id: usize,
    list_id: usize,
    payload: AnnVectorPayload,
    tid: TupleId,
    deleted: bool,
}


fn count_to_f32(count: usize) -> f32 {
    count.to_f32().unwrap_or(f32::MAX)
}

fn alloc_ivfflat_block(next_block: &mut u32) -> Result<BlockNumber, AccessMethodError> {
    let block = *next_block;
    *next_block = next_block
        .checked_add(1)
        .ok_or_else(|| AccessMethodError::Storage("ivfflat block number overflow".to_owned()))?;
    Ok(BlockNumber::new(block))
}

pub(crate) fn nearest_vector(centroids: &[Vec<f32>], probe: &[f32], metric: HnswMetric) -> Option<usize> {
    nearest_vectors(centroids, probe, metric, 1)
        .into_iter()
        .next()
}

pub(crate) fn nearest_vectors(
    centroids: &[Vec<f32>],
    probe: &[f32],
    metric: HnswMetric,
    limit: usize,
) -> Vec<usize> {
    let mut scored: Vec<(usize, f32)> = centroids
        .iter()
        .enumerate()
        // Skip unpopulated centroid slots. They carry no vector, so they have no
        // distance to the probe; computing one would hit the distance kernels'
        // length-equality assert (the probe is `dims`-long, the slot is empty) and
        // panic. Empty slots only ever pair with empty inverted lists (the decode
        // path rejects a populated list without a centroid, and the live op order
        // installs a centroid before any insert), so skipping them drops no
        // searchable entry — it just hardens search against a crafted/corrupt
        // snapshot or a degenerate replay state that planted an empty slot.
        .filter(|(_, centroid)| !centroid.is_empty())
        .map(|(idx, centroid)| (idx, metric.distance(probe, centroid)))
        .collect();
    scored.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored
        .into_iter()
        .take(limit.min(centroids.len()))
        .map(|(idx, _)| idx)
        .collect()
}

fn compare_ivfflat_hits(
    left: &IvfFlatSearchResult,
    right: &IvfFlatSearchResult,
) -> std::cmp::Ordering {
    left.distance
        .total_cmp(&right.distance)
        .then_with(|| left.tid.cmp(&right.tid))
}
