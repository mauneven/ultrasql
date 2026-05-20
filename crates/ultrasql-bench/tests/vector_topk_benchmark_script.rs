//! Contract tests for the exact vector top-k benchmark wrapper.

use std::fs;
use std::path::PathBuf;

#[test]
fn vector_topk_script_prefers_pgvector_then_duckdb_list_fallback() {
    let script_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("benchmarks/vector_topk_exact.sh");
    let script = fs::read_to_string(&script_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", script_path.display()));

    assert!(script.contains("target/release/cross_compare_sql"));
    assert!(script.contains("CREATE EXTENSION IF NOT EXISTS vector"));
    assert!(script.contains("postgres17_pgvector"));
    assert!(script.contains("list_distance"));
    assert!(script.contains("array_distance"));
    assert!(script.contains("duckdb_list"));
    assert!(script.contains("\"status\":\"not_available\""));
}
