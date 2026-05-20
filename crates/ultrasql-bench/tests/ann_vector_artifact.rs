//! Contract tests for ANN vector benchmark artifacts.

use ultrasql_bench::ann_vector::{AnnBenchmarkConfig, run_hnsw_ann_benchmark};
use ultrasql_bench::registry::HostInfo;

fn test_host() -> HostInfo {
    HostInfo {
        cpu: "test-cpu".to_owned(),
        cores: 1,
        ram_gb: 1,
        os: "test-os".to_owned(),
    }
}

#[test]
fn hnsw_ann_artifact_records_recall_latency_build_and_memory() {
    let config = AnnBenchmarkConfig {
        rows: 128,
        dims: 4,
        top_k: 5,
        queries: 6,
        warmup_queries: 2,
        m: 8,
        ef_search: 16,
        seed: 0x5eed,
    };

    let artifact = run_hnsw_ann_benchmark(&config, test_host()).expect("run ann benchmark");

    assert_eq!(artifact.engine, "ultrasql_hnsw");
    assert_eq!(artifact.workload, "vector_ann_hnsw_128_4d_k5");
    assert_eq!(artifact.n_rows, config.rows);
    assert_eq!(artifact.vector_dims, config.dims);
    assert_eq!(artifact.top_k, config.top_k);
    assert_eq!(artifact.query_iterations_us.len(), config.queries);
    assert!((0.0..=1.0).contains(&artifact.recall_at_k));
    assert!(artifact.p50_latency_us > 0.0);
    assert!(artifact.p95_latency_us >= artifact.p50_latency_us);
    assert!(artifact.p99_latency_us >= artifact.p95_latency_us);
    assert!(artifact.build_time_us > 0.0);
    assert!(artifact.memory_bytes >= config.rows * config.dims * std::mem::size_of::<f32>());

    let json = serde_json::to_value(&artifact).expect("serialize artifact");
    for key in [
        "recall_at_k",
        "p50_latency_us",
        "p95_latency_us",
        "p99_latency_us",
        "build_time_us",
        "memory_bytes",
    ] {
        assert!(json.get(key).is_some(), "missing artifact field {key}");
    }
}

#[test]
fn hnsw_ann_config_rejects_empty_shapes() {
    let config = AnnBenchmarkConfig {
        rows: 0,
        dims: 4,
        top_k: 5,
        queries: 6,
        warmup_queries: 2,
        m: 8,
        ef_search: 16,
        seed: 1,
    };

    let err = run_hnsw_ann_benchmark(&config, test_host()).expect_err("zero rows rejected");
    assert!(err.to_string().contains("rows"));
}
