//! Deterministic AI-gauntlet benchmark artifact generation.
//!
//! These runners produce local UltraSQL-only artifacts for AI database
//! certification gaps. They record raw measurements and required metrics; they
//! do not publish comparative claims.

use std::collections::HashSet;
use std::mem::size_of;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};
use ultrasql_storage::access_method::{
    HnswIndex, HnswMetric, PageBackedHnswIndex, PageBackedHnswStats, PageBackedIvfFlatIndex,
    PageBackedIvfFlatStats,
};

use crate::registry::HostInfo;

const TIDS_PER_BLOCK: usize = 32_768;
const FILTERED_VECTOR_RELATION: RelationId = RelationId::new(70_031);
const MEMORY_HNSW_RELATION: RelationId = RelationId::new(70_032);
const MEMORY_IVFFLAT_RELATION: RelationId = RelationId::new(70_033);

/// Configuration for filtered vector search certification artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilteredVectorConfig {
    /// Stable workload id written into the artifact.
    pub workload: String,
    /// AI gauntlet profile label.
    pub profile: String,
    /// Number of generated vectors.
    pub rows: usize,
    /// Vector dimensions.
    pub dims: usize,
    /// Number of nearest neighbors requested after filtering.
    pub top_k: usize,
    /// Number of measured queries.
    pub queries: usize,
    /// Warmup queries excluded from latency percentiles.
    pub warmup_queries: usize,
    /// Tenant cardinality used by deterministic metadata.
    pub tenant_count: usize,
    /// Category cardinality used by deterministic metadata.
    pub category_count: usize,
    /// Tenant id selected by the filter.
    pub tenant_id: usize,
    /// Category id selected by the filter.
    pub category_id: usize,
    /// HNSW neighbor cap.
    pub m: usize,
    /// HNSW search breadth.
    pub ef_search: usize,
    /// Deterministic data/probe seed.
    pub seed: u64,
}

impl Default for FilteredVectorConfig {
    fn default() -> Self {
        Self {
            workload: "ai_gauntlet_filtered_vector_search_smoke".to_owned(),
            profile: "smoke".to_owned(),
            rows: 10_000,
            dims: 8,
            top_k: 10,
            queries: 50,
            warmup_queries: 5,
            tenant_count: 8,
            category_count: 4,
            tenant_id: 3,
            category_id: 2,
            m: 16,
            ef_search: 1_024,
            seed: 0x51_7e_c0_de,
        }
    }
}

/// JSON artifact emitted by the filtered vector search runner.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilteredVectorArtifact {
    /// Artifact schema version.
    pub schema_version: u32,
    /// AI gauntlet suite name.
    pub suite: String,
    /// Engine/access-method label.
    pub engine: String,
    /// Stable workload id.
    pub workload: String,
    /// AI gauntlet profile label.
    pub profile: String,
    /// Measurement status.
    pub status: String,
    /// Required metrics present in this artifact.
    pub required_metrics: Vec<String>,
    /// Host descriptor captured at run time.
    pub host: HostInfo,
    /// Number of generated vectors.
    pub n_rows: usize,
    /// Number of vectors that matched the metadata filter.
    pub filtered_rows: usize,
    /// Vector dimensions.
    pub vector_dims: usize,
    /// Requested top-k.
    pub top_k: usize,
    /// Measured query count.
    pub queries: usize,
    /// Warmup query count.
    pub warmup_queries: usize,
    /// Tenant cardinality.
    pub tenant_count: usize,
    /// Category cardinality.
    pub category_count: usize,
    /// Tenant id selected by the filter.
    pub tenant_id: usize,
    /// Category id selected by the filter.
    pub category_id: usize,
    /// Fraction of rows accepted by the filter.
    pub filter_selectivity: f64,
    /// Mean ANN recall@k against exact filtered scan.
    pub recall_at_k: f64,
    /// ANN p50 latency in microseconds.
    pub p50_latency_us: f64,
    /// ANN p95 latency in microseconds.
    pub p95_latency_us: f64,
    /// ANN p99 latency in microseconds.
    pub p99_latency_us: f64,
    /// Exact filtered scan p50 latency in microseconds.
    pub exact_p50_latency_us: f64,
    /// Exact filtered scan p95 latency in microseconds.
    pub exact_p95_latency_us: f64,
    /// Exact filtered scan p99 latency in microseconds.
    pub exact_p99_latency_us: f64,
    /// ANN p50 latency in microseconds.
    pub ann_p50_latency_us: f64,
    /// ANN p95 latency in microseconds.
    pub ann_p95_latency_us: f64,
    /// ANN p99 latency in microseconds.
    pub ann_p99_latency_us: f64,
    /// Largest ANN candidate request needed before metadata filtering.
    pub candidate_expansion_count: usize,
    /// HNSW neighbor cap.
    pub m: usize,
    /// HNSW search breadth.
    pub ef_search: usize,
    /// Deterministic data/probe seed.
    pub seed: u64,
    /// Full graph build time in microseconds.
    pub build_time_us: f64,
    /// Estimated runtime HNSW heap memory in bytes.
    pub memory_bytes: usize,
    /// Raw exact scan latencies in execution order.
    pub exact_iterations_us: Vec<f64>,
    /// Raw ANN latencies in execution order.
    pub ann_iterations_us: Vec<f64>,
    /// Per-query recall values in execution order.
    pub recall_iterations: Vec<f64>,
    /// Per-query candidate request counts in execution order.
    pub candidate_expansion_iterations: Vec<usize>,
    /// Exact filtered answer for first measured query.
    pub first_exact_answer: Vec<usize>,
    /// ANN filtered answer for first measured query.
    pub first_ann_answer: Vec<usize>,
    /// Artifact policy statement.
    pub policy: String,
}

/// Configuration for page-backed vector memory artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorMemoryConfig {
    /// Stable workload id written into the artifact.
    pub workload: String,
    /// AI gauntlet profile label.
    pub profile: String,
    /// Number of generated vectors.
    pub rows: usize,
    /// Vector dimensions.
    pub dims: usize,
    /// HNSW neighbor cap.
    pub m: usize,
    /// HNSW search breadth.
    pub ef_search: usize,
    /// IVFFlat list count.
    pub lists: usize,
    /// IVFFlat probe count.
    pub probes: usize,
    /// Deterministic data seed.
    pub seed: u64,
}

impl Default for VectorMemoryConfig {
    fn default() -> Self {
        Self {
            workload: "ai_gauntlet_memory_per_million_vectors_smoke".to_owned(),
            profile: "smoke".to_owned(),
            rows: 10_000,
            dims: 8,
            m: 16,
            ef_search: 64,
            lists: 64,
            probes: 8,
            seed: 0x51_7e_c0_de,
        }
    }
}

/// JSON artifact emitted by the vector memory runner.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorMemoryArtifact {
    /// Artifact schema version.
    pub schema_version: u32,
    /// AI gauntlet suite name.
    pub suite: String,
    /// Engine/access-method label.
    pub engine: String,
    /// Stable workload id.
    pub workload: String,
    /// AI gauntlet profile label.
    pub profile: String,
    /// Measurement status.
    pub status: String,
    /// Required metrics present in this artifact.
    pub required_metrics: Vec<String>,
    /// Host descriptor captured at run time.
    pub host: HostInfo,
    /// Number of generated vectors.
    pub n_rows: usize,
    /// Vector dimensions.
    pub vector_dims: usize,
    /// Deterministic data seed.
    pub seed: u64,
    /// Combined page-backed index size in bytes.
    pub index_size_bytes: usize,
    /// Combined accounted bytes for input vectors plus page-backed indexes.
    pub memory_bytes: usize,
    /// Accounted bytes per vector.
    pub bytes_per_vector: f64,
    /// Accounted bytes normalized to one million vectors.
    pub memory_bytes_per_million_vectors: f64,
    /// Page-backed index bytes normalized to one million vectors.
    pub index_bytes_per_million_vectors: f64,
    /// Combined HNSW + IVFFlat build time in microseconds.
    pub build_time_us: f64,
    /// Page-backed HNSW build time in microseconds.
    pub hnsw_build_time_us: f64,
    /// Page-backed IVFFlat build time in microseconds.
    pub ivfflat_build_time_us: f64,
    /// Page-backed HNSW index size in bytes.
    pub hnsw_index_size_bytes: usize,
    /// Page-backed IVFFlat index size in bytes.
    pub ivfflat_index_size_bytes: usize,
    /// Raw input vector bytes held by the runner during build.
    pub vector_data_bytes: usize,
    /// HNSW neighbor cap.
    pub m: usize,
    /// HNSW search breadth.
    pub ef_search: usize,
    /// IVFFlat list count.
    pub lists: usize,
    /// IVFFlat probe count.
    pub probes: usize,
    /// Page counts and live-node stats for page-backed HNSW.
    pub hnsw_pages: HnswPageCounts,
    /// Page counts and live-entry stats for page-backed IVFFlat.
    pub ivfflat_pages: IvfFlatPageCounts,
    /// Artifact policy statement.
    pub policy: String,
}

/// Serializable page-backed HNSW page counts.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct HnswPageCounts {
    /// Number of meta pages.
    pub meta_pages: usize,
    /// Number of node pages.
    pub node_pages: usize,
    /// Number of overflow pages.
    pub overflow_pages: usize,
    /// Number of free-list pages.
    pub free_list_pages: usize,
    /// Number of live nodes.
    pub live_nodes: usize,
    /// Number of tombstones.
    pub tombstones: usize,
}

/// Serializable page-backed IVFFlat page counts.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct IvfFlatPageCounts {
    /// Number of meta pages.
    pub meta_pages: usize,
    /// Number of centroid pages.
    pub centroid_pages: usize,
    /// Number of list pages.
    pub list_pages: usize,
    /// Number of entry pages.
    pub entry_pages: usize,
    /// Number of live entries.
    pub live_entries: usize,
    /// Number of tombstones.
    pub tombstones: usize,
}

#[derive(Clone, Debug)]
struct BenchVector {
    vector: Vec<f32>,
    tenant_id: usize,
    category_id: usize,
}

/// Run deterministic filtered vector search certification.
pub fn run_filtered_vector_search(
    config: &FilteredVectorConfig,
    host: HostInfo,
) -> Result<FilteredVectorArtifact> {
    validate_filtered_config(config)?;
    let dims_u32 = u32::try_from(config.dims).context("vector dims do not fit u32")?;
    let index = HnswIndex::new(dims_u32, HnswMetric::L2, config.m, config.ef_search)
        .map_err(|err| anyhow::anyhow!("create hnsw index: {err}"))?;
    let data = (0..config.rows)
        .map(|row_id| BenchVector {
            vector: vector_for_row(row_id, config.dims, config.seed),
            tenant_id: row_id % config.tenant_count,
            category_id: (row_id / config.tenant_count) % config.category_count,
        })
        .collect::<Vec<_>>();
    let filtered_rows = data
        .iter()
        .filter(|row| row_matches_filter(row, config))
        .count();
    if filtered_rows == 0 {
        bail!("filtered vector benchmark filter selected zero rows");
    }

    let build_started = Instant::now();
    for (row_id, row) in data.iter().enumerate() {
        index
            .insert_vector(&row.vector, row_tid(FILTERED_VECTOR_RELATION, row_id)?)
            .map_err(|err| anyhow::anyhow!("insert filtered vector row {row_id}: {err}"))?;
    }
    let build_time_us = build_started.elapsed().as_secs_f64() * 1e6;
    let memory_bytes = index.estimated_memory_bytes();

    let total_queries = config.warmup_queries + config.queries;
    let mut exact_iterations_us = Vec::with_capacity(config.queries);
    let mut ann_iterations_us = Vec::with_capacity(config.queries);
    let mut recall_iterations = Vec::with_capacity(config.queries);
    let mut expansion_iterations = Vec::with_capacity(config.queries);
    let mut first_exact_answer = Vec::new();
    let mut first_ann_answer = Vec::new();

    for query_id in 0..total_queries {
        let probe = vector_for_probe(query_id, config.dims, config.seed);

        let exact_started = Instant::now();
        let exact = exact_filtered_top_k(&data, config, &probe);
        let exact_elapsed_us = exact_started.elapsed().as_secs_f64() * 1e6;

        let ann_started = Instant::now();
        let (ann, expansion_count) =
            ann_filtered_top_k(&index, &data, config, &probe, exact.len())?;
        let ann_elapsed_us = ann_started.elapsed().as_secs_f64() * 1e6;

        if query_id >= config.warmup_queries {
            if first_exact_answer.is_empty() {
                first_exact_answer = exact.clone();
                first_ann_answer = ann.clone();
            }
            exact_iterations_us.push(exact_elapsed_us);
            ann_iterations_us.push(ann_elapsed_us);
            recall_iterations.push(recall_at_k(&exact, &ann));
            expansion_iterations.push(expansion_count);
        }
    }

    let exact_latencies = sorted_copy(&exact_iterations_us);
    let ann_latencies = sorted_copy(&ann_iterations_us);
    let recall_at_k = mean(&recall_iterations);
    let candidate_expansion_count = expansion_iterations.iter().copied().max().unwrap_or(0);
    let filter_selectivity = filtered_rows as f64 / config.rows as f64;

    Ok(FilteredVectorArtifact {
        schema_version: 1,
        suite: "filtered_vector_search".to_owned(),
        engine: "ultrasql_hnsw".to_owned(),
        workload: config.workload.clone(),
        profile: config.profile.clone(),
        status: "measured".to_owned(),
        required_metrics: filtered_vector_required_metrics(),
        host,
        n_rows: config.rows,
        filtered_rows,
        vector_dims: config.dims,
        top_k: config.top_k,
        queries: config.queries,
        warmup_queries: config.warmup_queries,
        tenant_count: config.tenant_count,
        category_count: config.category_count,
        tenant_id: config.tenant_id,
        category_id: config.category_id,
        filter_selectivity,
        recall_at_k,
        p50_latency_us: percentile_nearest_rank(&ann_latencies, 0.50),
        p95_latency_us: percentile_nearest_rank(&ann_latencies, 0.95),
        p99_latency_us: percentile_nearest_rank(&ann_latencies, 0.99),
        exact_p50_latency_us: percentile_nearest_rank(&exact_latencies, 0.50),
        exact_p95_latency_us: percentile_nearest_rank(&exact_latencies, 0.95),
        exact_p99_latency_us: percentile_nearest_rank(&exact_latencies, 0.99),
        ann_p50_latency_us: percentile_nearest_rank(&ann_latencies, 0.50),
        ann_p95_latency_us: percentile_nearest_rank(&ann_latencies, 0.95),
        ann_p99_latency_us: percentile_nearest_rank(&ann_latencies, 0.99),
        candidate_expansion_count,
        m: config.m,
        ef_search: config.ef_search,
        seed: config.seed,
        build_time_us,
        memory_bytes,
        exact_iterations_us,
        ann_iterations_us,
        recall_iterations,
        candidate_expansion_iterations: expansion_iterations,
        first_exact_answer,
        first_ann_answer,
        policy: "Filtered vector artifact compares exact metadata-filtered scan with runtime HNSW candidate expansion; no cross-engine ranking.".to_owned(),
    })
}

/// Run deterministic page-backed vector memory certification.
pub fn run_vector_memory(
    config: &VectorMemoryConfig,
    host: HostInfo,
) -> Result<VectorMemoryArtifact> {
    validate_memory_config(config)?;
    let dims_u32 = u32::try_from(config.dims).context("vector dims do not fit u32")?;
    let data = (0..config.rows)
        .map(|row_id| vector_for_row(row_id, config.dims, config.seed))
        .collect::<Vec<_>>();
    let vector_data_bytes = checked_vector_bytes(config.rows, config.dims)?;

    let hnsw = PageBackedHnswIndex::new(
        MEMORY_HNSW_RELATION,
        dims_u32,
        HnswMetric::L2,
        config.m,
        config.ef_search,
    )
    .map_err(|err| anyhow::anyhow!("create page-backed hnsw: {err}"))?;
    let hnsw_started = Instant::now();
    for (row_id, vector) in data.iter().enumerate() {
        hnsw.insert_vector(vector, row_tid(MEMORY_HNSW_RELATION, row_id)?)
            .map_err(|err| anyhow::anyhow!("insert page-backed hnsw row {row_id}: {err}"))?;
    }
    let hnsw_build_time_us = hnsw_started.elapsed().as_secs_f64() * 1e6;
    let hnsw_stats = hnsw.page_stats();
    let hnsw_index_size_bytes = page_backed_hnsw_bytes(hnsw_stats)?;

    let ivfflat = PageBackedIvfFlatIndex::new(
        MEMORY_IVFFLAT_RELATION,
        dims_u32,
        HnswMetric::L2,
        config.lists,
        config.probes,
    )
    .map_err(|err| anyhow::anyhow!("create page-backed ivfflat: {err}"))?;
    let ivfflat_rows = data
        .iter()
        .enumerate()
        .map(|(row_id, vector)| Ok((vector.clone(), row_tid(MEMORY_IVFFLAT_RELATION, row_id)?)))
        .collect::<Result<Vec<_>>>()?;
    let ivfflat_started = Instant::now();
    ivfflat
        .bulk_load(ivfflat_rows)
        .map_err(|err| anyhow::anyhow!("bulk-load page-backed ivfflat: {err}"))?;
    let ivfflat_build_time_us = ivfflat_started.elapsed().as_secs_f64() * 1e6;
    let ivfflat_stats = ivfflat.page_stats();
    let ivfflat_index_size_bytes = page_backed_ivfflat_bytes(ivfflat_stats)?;

    let index_size_bytes = checked_add(hnsw_index_size_bytes, ivfflat_index_size_bytes)?;
    let memory_bytes = checked_add(index_size_bytes, vector_data_bytes)?;
    let bytes_per_vector = memory_bytes as f64 / config.rows as f64;
    let index_bytes_per_vector = index_size_bytes as f64 / config.rows as f64;

    Ok(VectorMemoryArtifact {
        schema_version: 1,
        suite: "memory_per_million_vectors".to_owned(),
        engine: "ultrasql_page_backed_ann".to_owned(),
        workload: config.workload.clone(),
        profile: config.profile.clone(),
        status: "measured".to_owned(),
        required_metrics: vector_memory_required_metrics(),
        host,
        n_rows: config.rows,
        vector_dims: config.dims,
        seed: config.seed,
        index_size_bytes,
        memory_bytes,
        bytes_per_vector,
        memory_bytes_per_million_vectors: bytes_per_vector * 1_000_000.0,
        index_bytes_per_million_vectors: index_bytes_per_vector * 1_000_000.0,
        build_time_us: hnsw_build_time_us + ivfflat_build_time_us,
        hnsw_build_time_us,
        ivfflat_build_time_us,
        hnsw_index_size_bytes,
        ivfflat_index_size_bytes,
        vector_data_bytes,
        m: config.m,
        ef_search: config.ef_search,
        lists: config.lists,
        probes: config.probes,
        hnsw_pages: hnsw_stats.into(),
        ivfflat_pages: ivfflat_stats.into(),
        policy: "Vector memory artifact records page-backed HNSW and IVFFlat accounting for deterministic vectors; no cross-engine ranking.".to_owned(),
    })
}

impl From<PageBackedHnswStats> for HnswPageCounts {
    fn from(stats: PageBackedHnswStats) -> Self {
        Self {
            meta_pages: stats.meta_pages,
            node_pages: stats.node_pages,
            overflow_pages: stats.overflow_pages,
            free_list_pages: stats.free_list_pages,
            live_nodes: stats.live_nodes,
            tombstones: stats.tombstones,
        }
    }
}

impl From<PageBackedIvfFlatStats> for IvfFlatPageCounts {
    fn from(stats: PageBackedIvfFlatStats) -> Self {
        Self {
            meta_pages: stats.meta_pages,
            centroid_pages: stats.centroid_pages,
            list_pages: stats.list_pages,
            entry_pages: stats.entry_pages,
            live_entries: stats.live_entries,
            tombstones: stats.tombstones,
        }
    }
}

fn validate_filtered_config(config: &FilteredVectorConfig) -> Result<()> {
    validate_common_vector_shape(config.rows, config.dims)?;
    if config.top_k == 0 {
        bail!("top_k must be greater than zero");
    }
    if config.queries == 0 {
        bail!("queries must be greater than zero");
    }
    if config.tenant_count == 0 {
        bail!("tenant_count must be greater than zero");
    }
    if config.category_count == 0 {
        bail!("category_count must be greater than zero");
    }
    if config.tenant_id >= config.tenant_count {
        bail!("tenant_id must be less than tenant_count");
    }
    if config.category_id >= config.category_count {
        bail!("category_id must be less than category_count");
    }
    if config.m == 0 {
        bail!("m must be greater than zero");
    }
    if config.ef_search == 0 {
        bail!("ef_search must be greater than zero");
    }
    Ok(())
}

fn validate_memory_config(config: &VectorMemoryConfig) -> Result<()> {
    validate_common_vector_shape(config.rows, config.dims)?;
    if config.m == 0 {
        bail!("m must be greater than zero");
    }
    if config.ef_search == 0 {
        bail!("ef_search must be greater than zero");
    }
    if config.lists == 0 {
        bail!("lists must be greater than zero");
    }
    if config.probes == 0 {
        bail!("probes must be greater than zero");
    }
    Ok(())
}

fn validate_common_vector_shape(rows: usize, dims: usize) -> Result<()> {
    if rows == 0 {
        bail!("rows must be greater than zero");
    }
    if dims == 0 {
        bail!("dims must be greater than zero");
    }
    let max_block = rows.saturating_sub(1) / TIDS_PER_BLOCK;
    u32::try_from(max_block).context("rows exceed benchmark TupleId range")?;
    Ok(())
}

fn exact_filtered_top_k(
    data: &[BenchVector],
    config: &FilteredVectorConfig,
    probe: &[f32],
) -> Vec<usize> {
    let mut scored = data
        .iter()
        .enumerate()
        .filter(|(_, row)| row_matches_filter(row, config))
        .map(|(row_id, row)| {
            (
                ultrasql_vec::kernels::vector::l2_distance_f32(&row.vector, probe),
                row_id,
            )
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    scored
        .into_iter()
        .take(config.top_k)
        .map(|(_, row_id)| row_id)
        .collect()
}

fn ann_filtered_top_k(
    index: &HnswIndex,
    data: &[BenchVector],
    config: &FilteredVectorConfig,
    probe: &[f32],
    expected_len: usize,
) -> Result<(Vec<usize>, usize)> {
    let mut requested = config.top_k.max(1);
    loop {
        let candidate_count = requested.min(config.rows);
        let hits = index
            .search(probe, candidate_count)
            .map_err(|err| anyhow::anyhow!("filtered vector hnsw search: {err}"))?;
        let filtered = hits
            .iter()
            .map(|hit| row_id_from_tid(FILTERED_VECTOR_RELATION, hit.tid))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|row_id| {
                data.get(*row_id)
                    .is_some_and(|row| row_matches_filter(row, config))
            })
            .take(config.top_k)
            .collect::<Vec<_>>();
        if filtered.len() >= expected_len || candidate_count == config.rows {
            return Ok((filtered, candidate_count));
        }
        requested = requested.saturating_mul(2).max(requested.saturating_add(1));
    }
}

fn row_matches_filter(row: &BenchVector, config: &FilteredVectorConfig) -> bool {
    row.tenant_id == config.tenant_id && row.category_id == config.category_id
}

fn vector_for_row(row_id: usize, dims: usize, seed: u64) -> Vec<f32> {
    (0..dims)
        .map(|dim| {
            let value = mix(seed, row_id as u64, dim as u64, 0x9e37_79b9_7f4a_7c15) % 2_003;
            (i32::try_from(value).unwrap_or(0) - 1_001) as f32 / 37.0
        })
        .collect()
}

fn vector_for_probe(query_id: usize, dims: usize, seed: u64) -> Vec<f32> {
    (0..dims)
        .map(|dim| {
            let value = mix(seed ^ 0xa5a5_a5a5_a5a5_a5a5, query_id as u64, dim as u64, 0) % 2_003;
            (i32::try_from(value).unwrap_or(0) - 1_001) as f32 / 41.0
        })
        .collect()
}

fn mix(seed: u64, left: u64, right: u64, salt: u64) -> u64 {
    let mut x = seed ^ salt;
    x = x.wrapping_add(left.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = x.wrapping_add(right.wrapping_mul(0x94d0_49bb_1331_11eb));
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn row_tid(relation: RelationId, row_id: usize) -> Result<TupleId> {
    let block = u32::try_from(row_id / TIDS_PER_BLOCK).context("row block does not fit u32")?;
    let slot = u16::try_from(row_id % TIDS_PER_BLOCK).context("row slot does not fit u16")?;
    Ok(TupleId::new(
        PageId::new(relation, BlockNumber::new(block)),
        slot,
    ))
}

fn row_id_from_tid(relation: RelationId, tid: TupleId) -> Result<usize> {
    if tid.page.relation != relation {
        bail!("unexpected benchmark relation in TupleId {tid}");
    }
    let block = usize::try_from(tid.page.block.raw()).context("block does not fit usize")?;
    Ok(block * TIDS_PER_BLOCK + usize::from(tid.slot))
}

fn recall_at_k(exact: &[usize], ann: &[usize]) -> f64 {
    if exact.is_empty() {
        return 0.0;
    }
    let exact_set = exact.iter().copied().collect::<HashSet<_>>();
    let matches = ann
        .iter()
        .take(exact.len())
        .filter(|row_id| exact_set.contains(row_id))
        .count();
    matches as f64 / exact.len() as f64
}

fn sorted_copy(values: &[f64]) -> Vec<f64> {
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    sorted
}

fn percentile_nearest_rank(sorted_values: &[f64], quantile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let sample_count = sorted_values.len().to_f64().unwrap_or(f64::INFINITY);
    let rank = (quantile.clamp(0.0, 1.0) * sample_count).ceil();
    let index = rank
        .max(1.0)
        .to_usize()
        .unwrap_or(usize::MAX)
        .saturating_sub(1);
    sorted_values[index.min(sorted_values.len() - 1)]
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn page_backed_hnsw_bytes(stats: PageBackedHnswStats) -> Result<usize> {
    let pages = checked_add(
        checked_add(stats.meta_pages, stats.node_pages)?,
        checked_add(stats.overflow_pages, stats.free_list_pages)?,
    )?;
    pages
        .checked_mul(PAGE_SIZE)
        .context("hnsw page bytes overflow")
}

fn page_backed_ivfflat_bytes(stats: PageBackedIvfFlatStats) -> Result<usize> {
    let pages = checked_add(
        checked_add(stats.meta_pages, stats.centroid_pages)?,
        checked_add(stats.list_pages, stats.entry_pages)?,
    )?;
    pages
        .checked_mul(PAGE_SIZE)
        .context("ivfflat page bytes overflow")
}

fn checked_vector_bytes(rows: usize, dims: usize) -> Result<usize> {
    rows.checked_mul(dims)
        .and_then(|values| values.checked_mul(size_of::<f32>()))
        .context("vector byte accounting overflow")
}

fn checked_add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right).context("byte accounting overflow")
}

fn filtered_vector_required_metrics() -> Vec<String> {
    [
        "recall_at_k",
        "p50_latency_us",
        "p95_latency_us",
        "p99_latency_us",
        "filter_selectivity",
        "candidate_expansion_count",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn vector_memory_required_metrics() -> Vec<String> {
    [
        "index_size_bytes",
        "memory_bytes",
        "bytes_per_vector",
        "build_time_us",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filtered_vector_reports_required_metrics() {
        let config = FilteredVectorConfig {
            rows: 128,
            dims: 4,
            top_k: 3,
            queries: 2,
            warmup_queries: 0,
            tenant_count: 4,
            category_count: 2,
            category_id: 1,
            ef_search: 128,
            ..FilteredVectorConfig::default()
        };
        let artifact =
            run_filtered_vector_search(&config, HostInfo::from_env()).expect("filtered artifact");

        assert_eq!(artifact.status, "measured");
        assert_eq!(artifact.suite, "filtered_vector_search");
        assert!(artifact.filter_selectivity > 0.0);
        assert!(artifact.candidate_expansion_count >= artifact.top_k);
        assert_eq!(artifact.ann_iterations_us.len(), 2);
        assert!(
            artifact
                .required_metrics
                .contains(&"recall_at_k".to_owned())
        );
    }

    #[test]
    fn vector_memory_reports_page_backed_sizes() {
        let config = VectorMemoryConfig {
            rows: 32,
            dims: 4,
            lists: 4,
            probes: 2,
            ..VectorMemoryConfig::default()
        };
        let artifact = run_vector_memory(&config, HostInfo::from_env()).expect("memory artifact");

        assert_eq!(artifact.status, "measured");
        assert_eq!(artifact.suite, "memory_per_million_vectors");
        assert!(artifact.index_size_bytes > 0);
        assert!(artifact.memory_bytes >= artifact.index_size_bytes);
        assert_eq!(artifact.hnsw_pages.live_nodes, 32);
        assert_eq!(artifact.ivfflat_pages.live_entries, 32);
    }
}
