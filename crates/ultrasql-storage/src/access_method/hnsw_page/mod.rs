//! Page-backed HNSW storage model: durable page structs and the index type.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

pub(crate) use std::collections::BTreeMap;

pub(crate) use parking_lot::Mutex;
pub(crate) use ultrasql_core::constants::PAGE_SIZE;
pub(crate) use ultrasql_core::{
    BlockNumber, Lsn, MAX_VECTOR_DIMS, PageId, RelationId, TupleId, Xid,
};
pub(crate) use ultrasql_wal::WalRecord;
pub(crate) use ultrasql_wal::payload::{HnswOpKind, HnswOpPayload};
pub(crate) use ultrasql_wal::record::RecordType;

pub(crate) use super::AccessMethodError;
pub(crate) use super::ann::{
    AnnPayloadKind, AnnQuantizedPayload, AnnRerankPolicy, AnnVectorPayload, HnswMetric,
    HnswSearchResult,
};
pub(crate) use super::hnsw_build::{
    DistNode, HNSW_BUILD_TRAVERSAL_WORK_THRESHOLD, HNSW_DEFAULT_EF_CONSTRUCTION, HNSW_MAX_LEVEL,
    HnswNodeId, ann_wal_index_rel, compare_hnsw_hits, hnsw_assign_level, hnsw_level_max_neighbors,
    select_neighbors_heuristic,
};
pub(crate) use crate::wal_sink::WalSink;

mod index;
mod ops;
mod serde;
mod storage;
mod storage_ops;

pub(crate) use serde::{
    HNSW_SNAPSHOT_MAGIC, HNSW_SNAPSHOT_VERSION, SnapshotCursor, decode_ann_payload_kind,
    decode_hnsw_metric, decode_hnsw_page_record, decode_tuple_id, encode_ann_payload_kind,
    encode_hnsw_metric, encode_hnsw_page_record, push_tuple_id, push_vec_f32, take_vec_f32,
};
// Used only by the unit tests, which construct and round-trip raw page records.
#[cfg(test)]
pub(crate) use serde::{HNSW_PAGE_KIND_NODE, push_len, push_opt_block};

const HNSW_META_BLOCK: u32 = 0;
const HNSW_FREE_LIST_BLOCK: u32 = 1;
const HNSW_FIRST_ALLOC_BLOCK: u32 = 2;
const HNSW_PAGE_OVERHEAD_BYTES: usize = 64;
const HNSW_VECTOR_VALUES_PER_OVERFLOW_PAGE: usize =
    (PAGE_SIZE - HNSW_PAGE_OVERHEAD_BYTES) / std::mem::size_of::<f32>();
const HNSW_NEIGHBOR_IDS_PER_OVERFLOW_PAGE: usize =
    (PAGE_SIZE - HNSW_PAGE_OVERHEAD_BYTES) / std::mem::size_of::<u64>();

/// Page counts and MVCC-visible node counts for a page-backed HNSW graph.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PageBackedHnswStats {
    /// Number of HNSW meta pages. Always one for a single index relation.
    pub meta_pages: usize,
    /// Number of live physical node pages, including tombstoned nodes before
    /// VACUUM compaction reclaims them.
    pub node_pages: usize,
    /// Number of overflow pages used for vector payloads and adjacency lists.
    pub overflow_pages: usize,
    /// Number of free-list pages. Always one until the relation outgrows a
    /// single free-list page.
    pub free_list_pages: usize,
    /// Number of non-tombstoned nodes.
    pub live_nodes: usize,
    /// Number of tombstoned nodes waiting for VACUUM.
    pub tombstones: usize,
    /// Number of reusable blocks currently recorded by the free list.
    pub reusable_pages: usize,
    /// Next block number that would be allocated if the free list were empty.
    pub next_block_number: u32,
}

/// Snapshot of one page-backed HNSW page as it would cross the buffer-pool
/// boundary.
#[derive(Clone, Debug)]
pub struct PageBackedHnswPageImage {
    /// Physical page identifier in the index relation.
    pub page_id: PageId,
    /// Last WAL LSN whose effects are reflected in this page image.
    pub lsn: Lsn,
    pub(crate) page: HnswPersistentPage,
}

/// First page-backed HNSW storage model.
///
/// This is deliberately narrower than the runtime [`HnswIndex`](crate::access_method::HnswIndex): it stores
/// nodes in page-sized records, spills vectors and adjacency lists into
/// overflow-page chains, tracks a meta page and a free-list page, replays
/// logical HNSW WAL records, and lets VACUUM reclaim tombstoned nodes. It is
/// not a production ANN claim until the arena is wired through the buffer
/// pool, page LSN checks, crash restart, and MVCC-visible executor paths.
#[derive(Debug)]
pub struct PageBackedHnswIndex {
    storage: Mutex<PageBackedHnswStorage>,
    index_rel: RelationId,
    dims: usize,
    metric: HnswMetric,
    m: usize,
    ef_search: usize,
    payload_kind: AnnPayloadKind,
    /// `live_nodes × dims` work budget above which build switches from an
    /// exhaustive candidate scan to graph-traversal candidate selection. See
    /// [`HNSW_BUILD_TRAVERSAL_WORK_THRESHOLD`].
    build_traversal_work_threshold: usize,
}

#[derive(Debug)]
struct PageBackedHnswStorage {
    valid: bool,
    pages: BTreeMap<BlockNumber, HnswPersistentPage>,
    meta: HnswMetaPage,
    free_list: HnswFreeListPage,
    tid_to_node: BTreeMap<TupleId, HnswNodeId>,
    node_to_block: BTreeMap<HnswNodeId, BlockNumber>,
    /// In-memory, `node_id`-indexed mirror of every node's vector, adjacency,
    /// tid, and tombstone flag. A pure read accelerator derived from `pages`:
    /// the durable page chains stay authoritative (and are what snapshots
    /// serialize), but graph traversal and search read the mirror so per-node
    /// access is O(1) array indexing instead of `BTreeMap` block lookups plus
    /// overflow-chain walks. Rebuilt wholesale from `pages` on load
    /// (`rebuild_mirror`) and maintained in lockstep with `pages` on insert,
    /// neighbor rewrite, delete, and vacuum. Never serialized.
    mirror: Vec<Option<MirrorNode>>,
}

/// One node's in-memory mirror entry. See [`PageBackedHnswStorage::mirror`].
#[derive(Debug, Clone)]
struct MirrorNode {
    vector: Vec<f32>,
    /// Per-layer adjacency, `levels[k]` = layer-`k` neighbors. `levels.len()` is
    /// this node's level + 1 (every node has a base layer 0). Unified in memory
    /// even though the durable page keeps layer 0 separate from upper layers.
    levels: Vec<Vec<HnswNodeId>>,
    tid: TupleId,
    deleted: bool,
}

impl MirrorNode {
    /// This node's top layer (0 = base only).
    fn level(&self) -> usize {
        self.levels.len().saturating_sub(1)
    }

    /// Layer-`level` neighbors, or an empty slice when the node is not in that
    /// layer.
    fn neighbors_at(&self, level: usize) -> &[HnswNodeId] {
        self.levels.get(level).map_or(&[], Vec::as_slice)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum HnswPersistentPage {
    Meta(HnswMetaPage),
    Node(HnswNodePage),
    Overflow(HnswOverflowPage),
    FreeList(HnswFreeListPage),
}

#[derive(Debug, Clone)]
pub(crate) struct HnswMetaPage {
    page_id: PageId,
    lsn: Lsn,
    dims: usize,
    metric: HnswMetric,
    m: usize,
    ef_search: usize,
    payload_kind: AnnPayloadKind,
    entry_node: Option<HnswNodeId>,
    next_node_id: HnswNodeId,
    live_nodes: usize,
    tombstones: usize,
    next_block_number: u32,
    free_list_page: BlockNumber,
}

#[derive(Debug, Clone)]
pub(crate) struct HnswNodePage {
    pub(crate) page_id: PageId,
    pub(crate) lsn: Lsn,
    pub(crate) node_id: HnswNodeId,
    pub(crate) tid: TupleId,
    pub(crate) vector_len: usize,
    pub(crate) vector_head: BlockNumber,
    /// Level-0 (base layer) neighbor chain. The base layer is mandatory and is
    /// where every node lives, so it keeps the original fields and on-disk
    /// layout — a v1 snapshot is exactly a v2 snapshot with `level == 0`.
    pub(crate) neighbor_count: usize,
    pub(crate) neighbor_head: Option<BlockNumber>,
    /// Top layer this node participates in (0 = base only). Hierarchical HNSW
    /// gives upper layers progressively fewer nodes for O(log N) descent.
    pub(crate) level: usize,
    /// Neighbor chains for layers `1..=level` (index `k-1` is layer `k`); empty
    /// for a base-only node. Persisted after the base fields, so older readers
    /// that stop at `level == 0` stay correct.
    pub(crate) upper_levels: Vec<HnswLevelNeighbors>,
    pub(crate) deleted: bool,
}

/// One layer's neighbor chain head and length, for layers above the base.
#[derive(Debug, Clone)]
pub(crate) struct HnswLevelNeighbors {
    pub(crate) head: Option<BlockNumber>,
    pub(crate) count: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct HnswOverflowPage {
    page_id: PageId,
    lsn: Lsn,
    owner_node: HnswNodeId,
    next: Option<BlockNumber>,
    payload: HnswOverflowPayload,
}

#[derive(Debug, Clone)]
pub(crate) enum HnswOverflowPayload {
    Vector(AnnVectorPayload),
    Neighbors(Vec<HnswNodeId>),
}

#[derive(Debug, Clone)]
pub(crate) struct HnswFreeListPage {
    page_id: PageId,
    lsn: Lsn,
    blocks: Vec<BlockNumber>,
}

impl HnswPersistentPage {
    fn page_id(&self) -> PageId {
        match self {
            Self::Meta(page) => page.page_id,
            Self::Node(page) => page.page_id,
            Self::Overflow(page) => page.page_id,
            Self::FreeList(page) => page.page_id,
        }
    }

    fn lsn(&self) -> Lsn {
        match self {
            Self::Meta(page) => page.lsn,
            Self::Node(page) => page.lsn,
            Self::Overflow(page) => page.lsn,
            Self::FreeList(page) => page.lsn,
        }
    }

    fn set_lsn(&mut self, lsn: Lsn) {
        match self {
            Self::Meta(page) => page.lsn = lsn,
            Self::Node(page) => page.lsn = lsn,
            Self::Overflow(page) => page.lsn = lsn,
            Self::FreeList(page) => page.lsn = lsn,
        }
    }
}
