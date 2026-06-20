//! Shared HNSW/ANN build and distance helpers.
//!
//! These are the pure, struct-private-free helpers used by both the runtime
//! [`super::hnsw::HnswIndex`] and the page-backed
//! [`super::hnsw_page::PageBackedHnswIndex`] graph builders.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use ultrasql_core::RelationId;

use super::AccessMethodError;
use super::ann::{HnswMetric, HnswSearchResult};

pub(crate) type HnswNodeId = u64;

pub(crate) fn ann_wal_index_rel(
    payload: &[u8],
    context: &str,
) -> Result<Option<RelationId>, AccessMethodError> {
    if payload.len() < 8 {
        return Ok(None);
    }
    let raw = u32::from_le_bytes(payload[4..8].try_into().map_err(|_| {
        AccessMethodError::Storage(format!("{context} WAL index relation decode failed"))
    })?);
    Ok(Some(RelationId::new(raw)))
}

/// Maximum candidate pool examined when selecting a node's neighbors at build
/// time — the standard HNSW `ef_construction`. The pool is the exact nearest
/// live nodes, so this bounds the diversity heuristic's pairwise-distance cost
/// while keeping graph quality high. Larger trades build time for recall.
pub(crate) const HNSW_DEFAULT_EF_CONSTRUCTION: usize = 200;

/// Hard cap on a node's hierarchical level. `P(level >= 1) = 1/max(m, 2)`
/// (see `hnsw_assign_level`), so for the usual `m` (8..=64) the natural maximum
/// stays well under this cap and it is just headroom that bounds the per-node
/// upper-layer vector. For a pathologically small `m` (1 or 2) the decay is only
/// `p = 1/2`, the cap is genuinely reached, and the top layer flattens — those
/// `m` values give a poor hierarchy regardless and are not a realistic config.
pub(crate) const HNSW_MAX_LEVEL: usize = 16;

/// Build-time work budget — in vector-element comparisons (`live_nodes × dims`)
/// — above which gathering a new node's neighbor candidates by traversing the
/// partially-built graph beats an exhaustive scan of every live node.
///
/// An exhaustive scan costs ~`live_nodes × dims` element comparisons but is
/// sequential and allocation-light, so it stays the faster *and* exact choice
/// while the live set is small. A graph traversal touches far fewer nodes once
/// the set is large, but pays a fixed per-node page-lookup cost (the page-backed
/// arena chases `BTreeMap` blocks per probe) that only amortizes past this
/// budget. Calibrated from the page-backed build sweep: the crossover is ~8k
/// live nodes at 128 dims (≈ this value), where the exhaustive build first
/// exceeds the traversal build. Below it, full-scan candidate selection is kept.
pub(crate) const HNSW_BUILD_TRAVERSAL_WORK_THRESHOLD: usize = 1_000_000;

/// Deterministically assign a node's hierarchical level from its id.
///
/// Standard HNSW draws the level from a geometric distribution
/// `floor(-ln(U) / ln(m))`. Drawing it from a hash of the (monotonic) node id
/// instead of an RNG keeps it reproducible: WAL replay and snapshot-resumed
/// replay recompute the identical level for every node, so the reconstructed
/// multi-layer graph is byte-identical to the original. The level is also
/// persisted, so loaded nodes never need recomputation — only fresh inserts do,
/// under the same binary.
pub(crate) fn hnsw_assign_level(node_id: HnswNodeId, m: usize) -> usize {
    // splitmix64: a good integer hash so consecutive ids get well-spread levels.
    let mut z = node_id.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    // Uniform in (0, 1): top 53 bits as a mantissa, shifted off zero.
    #[expect(clippy::cast_precision_loss, reason = "53-bit mantissa fits f64")]
    let mantissa = (z >> 11) as f64;
    let unit = (mantissa + 1.0) / 9_007_199_254_740_993.0;
    let scale = 1.0 / (m.max(2) as f64).ln();
    let level = (-(unit.ln()) * scale).floor();
    if level.is_finite() && level >= 0.0 {
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "bounded by HNSW_MAX_LEVEL"
        )]
        let level = level as usize;
        level.min(HNSW_MAX_LEVEL)
    } else {
        0
    }
}

/// Per-layer neighbor cap: the base layer keeps `2*m` for connectivity, upper
/// layers keep `m`, matching the canonical HNSW `M_max0` / `M_max`.
pub(crate) fn hnsw_level_max_neighbors(level: usize, m: usize) -> usize {
    if level == 0 {
        m.saturating_mul(2).max(1)
    } else {
        m.max(1)
    }
}

/// A `(distance, node_id)` heap element with a total order — distance via
/// `total_cmp`, then `node_id` — so no two distinct nodes compare equal and the
/// binary-heap pop order is fully deterministic (required for WAL-replay graph
/// reproducibility).
#[derive(Clone, Copy, PartialEq)]
pub(crate) struct DistNode {
    pub(crate) dist: f32,
    pub(crate) id: HnswNodeId,
}

impl Eq for DistNode {}

impl Ord for DistNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dist
            .total_cmp(&other.dist)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for DistNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// HNSW select-neighbors heuristic (Malkov & Yashunin 2018, Algorithm 4).
///
/// From `candidates` — each paired with its exact distance to the node being
/// connected (`dist_to_q`) and the candidate's own vector, sorted by
/// `dist_to_q` ascending — keep up to `m` that are mutually *diverse*: a
/// candidate is pruned when it lies closer to an already-kept neighbor than to
/// the node itself. Dropping such redundant same-cluster edges is what
/// preserves the long-range "bridge" links that keep a single navigable layer
/// searchable; a plain "m nearest" graph traps greedy descent in local clusters
/// and caps recall. Pruned candidates backfill nearest-first so a node never
/// loses degree — and thus connectivity — when few survive the diversity test.
pub(crate) fn select_neighbors_heuristic<Id: Copy>(
    candidates: &[(Id, f32, Vec<f32>)],
    m: usize,
    metric: HnswMetric,
) -> Vec<Id> {
    let mut kept: Vec<(Id, &[f32])> = Vec::with_capacity(m);
    let mut pruned: Vec<Id> = Vec::new();
    for (id, dist_to_q, vector) in candidates {
        if kept.len() >= m {
            break;
        }
        let diverse = kept
            .iter()
            .all(|(_, kept_vec)| metric.distance(vector, kept_vec) >= *dist_to_q);
        if diverse {
            kept.push((*id, vector.as_slice()));
        } else {
            pruned.push(*id);
        }
    }
    let mut result: Vec<Id> = kept.iter().map(|(id, _)| *id).collect();
    for id in pruned {
        if result.len() >= m {
            break;
        }
        result.push(id);
    }
    result
}

pub(crate) fn decode_vector_key(
    key: &[u8],
    dims: usize,
    prefix: &'static str,
) -> Result<Vec<f32>, AccessMethodError> {
    let expected = dims
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| AccessMethodError::Storage(format!("{prefix} key length overflow")))?;
    if key.len() != expected {
        return Err(AccessMethodError::Storage(format!(
            "{prefix} key length mismatch: expected {expected}, got {}",
            key.len()
        )));
    }
    let mut vector = Vec::with_capacity(dims);
    for chunk in key.chunks_exact(std::mem::size_of::<f32>()) {
        let bytes: [u8; 4] = chunk
            .try_into()
            .map_err(|_| AccessMethodError::Storage(format!("{prefix} key chunk width")))?;
        let value = f32::from_le_bytes(bytes);
        if !value.is_finite() {
            return Err(AccessMethodError::Storage(format!(
                "{prefix} vector elements must be finite"
            )));
        }
        vector.push(value);
    }
    Ok(vector)
}

pub(crate) fn compare_hnsw_hits(
    left: &HnswSearchResult,
    right: &HnswSearchResult,
) -> std::cmp::Ordering {
    left.distance
        .total_cmp(&right.distance)
        .then_with(|| left.tid.cmp(&right.tid))
}
