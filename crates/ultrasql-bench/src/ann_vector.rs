//! Deterministic ANN vector benchmark artifact generation.
//!
//! This module measures the first runtime `HnswIndex` implementation directly:
//! build cost, query latency distribution, recall@k against an exact oracle,
//! and graph memory accounting. It intentionally emits artifacts only; it does
//! not publish comparative claims.

use std::collections::HashSet;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};
use ultrasql_storage::access_method::{HnswIndex, HnswMetric};

use crate::registry::HostInfo;

const TIDS_PER_BLOCK: usize = 32_768;
const HNSW_BENCH_RELATION: RelationId = RelationId::new(70_017);

/// Configuration for one HNSW ANN benchmark artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnnBenchmarkConfig {
    /// Number of indexed vectors.
    pub rows: usize,
    /// Dimension count per vector.
    pub dims: usize,
    /// Number of nearest neighbors requested per query.
    pub top_k: usize,
    /// Number of measured queries.
    pub queries: usize,
    /// Number of warmup queries excluded from latency percentiles.
    pub warmup_queries: usize,
    /// HNSW neighbor cap.
    pub m: usize,
    /// HNSW search breadth.
    pub ef_search: usize,
    /// Deterministic data/probe seed.
    pub seed: u64,
}

impl Default for AnnBenchmarkConfig {
    fn default() -> Self {
        Self {
            rows: 10_000,
            dims: 8,
            top_k: 10,
            queries: 50,
            warmup_queries: 5,
            m: 16,
            ef_search: 64,
            seed: 0x51_7e_c0_de,
        }
    }
}

/// JSON artifact emitted by the HNSW ANN benchmark.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnnBenchmarkArtifact {
    /// Artifact schema version.
    pub schema_version: u32,
    /// Engine/access-method label.
    pub engine: String,
    /// Stable workload id.
    pub workload: String,
    /// Result status consumed by benchmark renderers.
    pub status: String,
    /// Number of indexed vectors.
    pub n_rows: usize,
    /// Number of measured query samples.
    pub samples: usize,
    /// Median measured query latency in microseconds.
    pub median_us: f64,
    /// Minimum measured query latency in microseconds.
    pub min_us: f64,
    /// Raw measured query latencies, sorted for generic result renderers.
    pub iterations_us: Vec<f64>,
    /// Host descriptor captured at run time.
    pub host: HostInfo,
    /// Dimension count per vector.
    pub vector_dims: usize,
    /// Top-k requested by every query.
    pub top_k: usize,
    /// Number of measured queries.
    pub queries: usize,
    /// Number of warmup queries.
    pub warmup_queries: usize,
    /// Distance metric.
    pub metric: String,
    /// HNSW neighbor cap.
    pub m: usize,
    /// HNSW search breadth.
    pub ef_search: usize,
    /// Deterministic data/probe seed.
    pub seed: u64,
    /// Mean recall@k across measured queries.
    pub recall_at_k: f64,
    /// 50th-percentile query latency in microseconds.
    pub p50_latency_us: f64,
    /// 95th-percentile query latency in microseconds.
    pub p95_latency_us: f64,
    /// 99th-percentile query latency in microseconds.
    pub p99_latency_us: f64,
    /// Full graph build time in microseconds.
    pub build_time_us: f64,
    /// Estimated heap memory owned by the runtime graph.
    pub memory_bytes: usize,
    /// Raw measured query latencies in execution order.
    pub query_iterations_us: Vec<f64>,
    /// Per-query recall@k values in execution order.
    pub recall_iterations: Vec<f64>,
    /// Exact top-k answer for the first measured query.
    pub first_exact_answer: Vec<usize>,
    /// ANN top-k answer for the first measured query.
    pub first_ann_answer: Vec<usize>,
}

/// Run the deterministic runtime-HNSW ANN benchmark.
///
/// The exact oracle scans the generated vectors and sorts by L2 distance, then
/// row id. The ANN path searches [`HnswIndex`] and computes recall@k against
/// that oracle for every measured query.
pub fn run_hnsw_ann_benchmark(
    config: &AnnBenchmarkConfig,
    host: HostInfo,
) -> Result<AnnBenchmarkArtifact> {
    validate_config(config)?;

    let dims_u32 = u32::try_from(config.dims).context("vector dims do not fit u32")?;
    let index = HnswIndex::new(dims_u32, HnswMetric::L2, config.m, config.ef_search)
        .map_err(|err| anyhow::anyhow!("create hnsw index: {err}"))?;
    let data = (0..config.rows)
        .map(|row_id| vector_for_row(row_id, config.dims, config.seed))
        .collect::<Vec<_>>();

    let build_started = Instant::now();
    for (row_id, vector) in data.iter().enumerate() {
        index
            .insert_vector(vector, row_tid(row_id)?)
            .map_err(|err| anyhow::anyhow!("insert row {row_id} into hnsw: {err}"))?;
    }
    let build_time_us = build_started.elapsed().as_secs_f64() * 1e6;
    let memory_bytes = index.estimated_memory_bytes();

    let total_queries = config.warmup_queries + config.queries;
    let mut query_iterations_us = Vec::with_capacity(config.queries);
    let mut recall_iterations = Vec::with_capacity(config.queries);
    let mut first_exact_answer = Vec::new();
    let mut first_ann_answer = Vec::new();

    for query_id in 0..total_queries {
        let probe = vector_for_probe(query_id, config.dims, config.seed);
        let exact = exact_top_k(&data, &probe, config.top_k);

        let started = Instant::now();
        let ann_hits = index
            .search(&probe, config.top_k)
            .map_err(|err| anyhow::anyhow!("hnsw query {query_id}: {err}"))?;
        let elapsed_us = started.elapsed().as_secs_f64() * 1e6;
        let ann = ann_hits
            .iter()
            .map(|hit| row_id_from_tid(hit.tid))
            .collect::<Result<Vec<_>>>()?;

        if query_id >= config.warmup_queries {
            if first_exact_answer.is_empty() {
                first_exact_answer = exact.clone();
                first_ann_answer = ann.clone();
            }
            query_iterations_us.push(elapsed_us);
            recall_iterations.push(recall_at_k(&exact, &ann));
        }
    }

    let mut sorted_latencies = query_iterations_us.clone();
    sorted_latencies.sort_by(|left, right| left.total_cmp(right));
    let recall_at_k = if recall_iterations.is_empty() {
        0.0
    } else {
        recall_iterations.iter().sum::<f64>() / recall_iterations.len() as f64
    };
    let median_us = median_sorted(&sorted_latencies);
    let min_us = sorted_latencies.first().copied().unwrap_or(0.0);

    Ok(AnnBenchmarkArtifact {
        schema_version: 1,
        engine: "ultrasql_hnsw".to_owned(),
        workload: workload_id(config.rows, config.dims, config.top_k),
        status: "measured".to_owned(),
        n_rows: config.rows,
        samples: query_iterations_us.len(),
        median_us,
        min_us,
        iterations_us: sorted_latencies.clone(),
        host,
        vector_dims: config.dims,
        top_k: config.top_k,
        queries: config.queries,
        warmup_queries: config.warmup_queries,
        metric: "l2".to_owned(),
        m: config.m,
        ef_search: config.ef_search,
        seed: config.seed,
        recall_at_k,
        p50_latency_us: percentile_nearest_rank(&sorted_latencies, 0.50),
        p95_latency_us: percentile_nearest_rank(&sorted_latencies, 0.95),
        p99_latency_us: percentile_nearest_rank(&sorted_latencies, 0.99),
        build_time_us,
        memory_bytes,
        query_iterations_us,
        recall_iterations,
        first_exact_answer,
        first_ann_answer,
    })
}

/// Stable workload id for HNSW ANN artifacts.
#[must_use]
pub fn workload_id(rows: usize, dims: usize, top_k: usize) -> String {
    format!("vector_ann_hnsw_{}_{}d_k{}", k_or_raw(rows), dims, top_k)
}

fn validate_config(config: &AnnBenchmarkConfig) -> Result<()> {
    if config.rows == 0 {
        bail!("rows must be greater than zero");
    }
    if config.dims == 0 {
        bail!("dims must be greater than zero");
    }
    if config.top_k == 0 {
        bail!("top_k must be greater than zero");
    }
    if config.queries == 0 {
        bail!("queries must be greater than zero");
    }
    if config.m == 0 {
        bail!("m must be greater than zero");
    }
    if config.ef_search == 0 {
        bail!("ef_search must be greater than zero");
    }
    let max_block = config.rows.saturating_sub(1) / TIDS_PER_BLOCK;
    u32::try_from(max_block).context("rows exceed benchmark TupleId range")?;
    Ok(())
}

fn vector_for_row(row_id: usize, dims: usize, seed: u64) -> Vec<f32> {
    (0..dims)
        .map(|dim| {
            let value = mix(
                seed,
                usize_to_u64(row_id),
                usize_to_u64(dim),
                0x9e37_79b9_7f4a_7c15,
            ) % 2_003;
            centered_component(value, 37.0)
        })
        .collect()
}

fn vector_for_probe(query_id: usize, dims: usize, seed: u64) -> Vec<f32> {
    (0..dims)
        .map(|dim| {
            let value = mix(
                seed ^ 0xa5a5_a5a5_a5a5_a5a5,
                usize_to_u64(query_id),
                usize_to_u64(dim),
                0,
            ) % 2_003;
            centered_component(value, 41.0)
        })
        .collect()
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

fn centered_component(value: u64, scale: f32) -> f32 {
    let raw = i16::try_from(value).unwrap_or_default();
    f32::from(raw - 1_001) / scale
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

fn row_tid(row_id: usize) -> Result<TupleId> {
    let block = u32::try_from(row_id / TIDS_PER_BLOCK).context("row block does not fit u32")?;
    let slot = u16::try_from(row_id % TIDS_PER_BLOCK).context("row slot does not fit u16")?;
    Ok(TupleId::new(
        PageId::new(HNSW_BENCH_RELATION, BlockNumber::new(block)),
        slot,
    ))
}

fn row_id_from_tid(tid: TupleId) -> Result<usize> {
    if tid.page.relation != HNSW_BENCH_RELATION {
        bail!("unexpected benchmark relation in TupleId {tid}");
    }
    let block = usize::try_from(tid.page.block.raw()).context("block does not fit usize")?;
    Ok(block * TIDS_PER_BLOCK + usize::from(tid.slot))
}

fn exact_top_k(data: &[Vec<f32>], probe: &[f32], top_k: usize) -> Vec<usize> {
    let mut scored = data
        .iter()
        .enumerate()
        .map(|(row_id, vector)| {
            let distance = ultrasql_vec::kernels::vector::l2_distance_f32(vector, probe);
            (distance, row_id)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    scored
        .into_iter()
        .take(top_k.min(data.len()))
        .map(|(_, row_id)| row_id)
        .collect()
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

fn median_sorted(sorted_values: &[f64]) -> f64 {
    match sorted_values.len() {
        0 => 0.0,
        len if len % 2 == 1 => sorted_values[len / 2],
        len => (sorted_values[(len / 2) - 1] + sorted_values[len / 2]) / 2.0,
    }
}

fn k_or_raw(n: usize) -> String {
    if n >= 1_000_000 && n % 1_000_000 == 0 {
        format!("{}m", n / 1_000_000)
    } else if n >= 1_000 && n % 1_000 == 0 {
        format!("{}k", n / 1_000)
    } else if n == 65_536 {
        "65k".to_owned()
    } else {
        n.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_id_uses_existing_row_labels() {
        assert_eq!(workload_id(10_000, 8, 10), "vector_ann_hnsw_10k_8d_k10");
        assert_eq!(workload_id(65_536, 4, 5), "vector_ann_hnsw_65k_4d_k5");
    }

    #[test]
    fn recall_counts_intersection_over_exact_top_k() {
        assert_eq!(recall_at_k(&[1, 2, 3, 4], &[4, 9, 2, 8]), 0.5);
    }
}
