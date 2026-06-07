//! v0.7 vectorized-pipeline benchmarks.
//!
//! These benchmarks exercise the push-based pipeline and SIMD kernel paths
//! at the micro-workload level. They are regression watchdogs, not
//! production-claim benchmarks; all numbers must be reproduced from a
//! specific host (see `benchmarks/results/host.yaml`).
//!
//! ## Benchmarks
//!
//! | Group | What it measures |
//! |---|---|
//! | `vec/seq_scan_filter_1m` | `VectorizedSeqScan` + `VectorizedFilter` over 1 M rows |
//! | `vec/hash_agg_sum_count_1m` | `VectorizedHashAggregate` SUM + COUNT over 1 M rows |
//! | `vec/hash_join_100k_x_1m` | `VectorizedHashJoin` — 100 K build × 1 M probe |
//! | `vec/dict_filter_vs_raw` | dictionary-code filter vs. direct `filter_eq_i64` |
//! | `vec/filter_eq_i32_simd` | SIMD `filter_eq_i32` kernel throughput |

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_executor::mem_table_scan::MemTableScan;
use ultrasql_executor::push_pipeline::{CollectSink, VectorizedOperator};
use ultrasql_executor::vec_ops::hash_aggregate::{AggSpec, VectorizedHashAggregate};
use ultrasql_executor::vec_ops::hash_join::VectorizedHashJoin;
use ultrasql_executor::vec_ops::scan::VectorizedSeqScan;
use ultrasql_executor::vec_ops::vec_filter::VectorizedFilter;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};
use ultrasql_vec::dict::DictionaryColumn;
use ultrasql_vec::kernels::{filter_eq_i32, filter_eq_i64};

// ============================================================================
// Constants
// ============================================================================

/// Batch size used when building test inputs. Matches the executor ceiling.
const BATCH_SIZE: usize = 4096;

// ============================================================================
// Helpers
// ============================================================================

/// Build a schema with two `Int64` columns.
fn schema_i64x2(a: &'static str, b: &'static str) -> Schema {
    Schema::new([
        Field::required(a, DataType::Int64),
        Field::required(b, DataType::Int64),
    ])
    .expect("schema ok")
}

fn bench_row_count(rows: usize) -> u64 {
    u64::try_from(rows).expect("benchmark row count fits u64")
}

fn bench_i64_index(index: usize) -> i64 {
    i64::try_from(index).expect("benchmark row index fits i64")
}

fn bench_i32_index(index: usize) -> i32 {
    i32::try_from(index).expect("benchmark row index fits i32")
}

/// Produce `n` batches of two `Int64` columns: key cycling in `[0, card)` and
/// value = row index.
fn make_kv_batches(n_rows: usize, key_cardinality: i64) -> Vec<Batch> {
    let mut batches = Vec::new();
    let mut offset = 0usize;
    while offset < n_rows {
        let keys: Vec<i64> = (offset..offset + BATCH_SIZE)
            .map(|i| bench_i64_index(i) % key_cardinality)
            .collect();
        let vals: Vec<i64> = (offset..offset + BATCH_SIZE).map(bench_i64_index).collect();
        batches.push(
            Batch::new([
                Column::Int64(NumericColumn::from_data(keys)),
                Column::Int64(NumericColumn::from_data(vals)),
            ])
            .expect("batch ok"),
        );
        offset += BATCH_SIZE;
    }
    batches
}

// ============================================================================
// Benchmark: VectorizedSeqScan + VectorizedFilter (1 M rows)
// ============================================================================

fn bench_seq_scan_filter_1m(c: &mut Criterion) {
    use ultrasql_planner::{BinaryOp, ScalarExpr};

    let n_rows: usize = 1_000_000;
    let cardinality = 100i64;
    let batches = make_kv_batches(n_rows, cardinality);
    let schema = schema_i64x2("k", "v");

    // Predicate: k == 42
    let pred = ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(ScalarExpr::Column {
            name: "k".into(),
            index: 0,
            data_type: DataType::Int64,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int64(42),
            data_type: DataType::Int64,
        }),
        data_type: DataType::Bool,
    };

    let mut group = c.benchmark_group("vec/seq_scan_filter_1m");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.throughput(Throughput::Elements(bench_row_count(n_rows)));
    group.bench_function("filter_k_eq_42", |b| {
        b.iter(|| {
            let scan = MemTableScan::new(schema.clone(), batches.clone());
            let vscan = VectorizedSeqScan::new(Box::new(scan));
            let mut filter = VectorizedFilter::new(Box::new(vscan), black_box(pred.clone()));
            let mut sink = CollectSink::new();
            filter.drive(&mut sink).expect("filter drive ok");
            black_box(sink.finish());
        });
    });
    group.finish();
}

// ============================================================================
// Benchmark: VectorizedHashAggregate SUM + COUNT (1 M rows)
// ============================================================================

fn bench_hash_agg_1m(c: &mut Criterion) {
    let n_rows: usize = 1_000_000;
    let n_groups = 100i64;
    let batches = make_kv_batches(n_rows, n_groups);
    let schema = schema_i64x2("k", "v");
    let agg_schema = Schema::new([
        Field::required("k", DataType::Int64),
        Field::required("cnt", DataType::Int64),
        Field::required("sum_v", DataType::Int64),
    ])
    .expect("schema ok");

    let mut group = c.benchmark_group("vec/hash_agg_sum_count_1m");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.throughput(Throughput::Elements(bench_row_count(n_rows)));
    group.bench_function("count_star_sum_v", |b| {
        b.iter(|| {
            let scan = MemTableScan::new(schema.clone(), batches.clone());
            let vscan = VectorizedSeqScan::new(Box::new(scan));
            let mut agg = VectorizedHashAggregate::new(
                Box::new(vscan),
                0, // group key = column 0 (k)
                vec![AggSpec::CountStar, AggSpec::Sum(1)],
                agg_schema.clone(),
            );
            let mut sink = CollectSink::new();
            agg.drive(&mut sink).expect("agg drive ok");
            black_box(sink.finish());
        });
    });
    group.finish();
}

// ============================================================================
// Benchmark: VectorizedHashJoin 100K × 1M
// ============================================================================

fn bench_hash_join_100k_x_1m(c: &mut Criterion) {
    let n_build: usize = 100_000;
    let n_probe: usize = 1_000_000;
    let cardinality = 100_000i64;

    let build_batches = make_kv_batches(n_build, cardinality);
    let probe_batches = make_kv_batches(n_probe, cardinality);

    let build_schema = schema_i64x2("bk", "bv");
    let probe_schema = schema_i64x2("pk", "pv");
    let out_schema = Schema::new([
        Field::required("bk", DataType::Int64),
        Field::required("bv", DataType::Int64),
        Field::required("pk", DataType::Int64),
        Field::required("pv", DataType::Int64),
    ])
    .expect("schema ok");

    let mut group = c.benchmark_group("vec/hash_join_100k_x_1m");
    // Use sample_size(10) because this benchmark is intentionally heavy.
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.throughput(Throughput::Elements(bench_row_count(n_probe)));
    group.bench_function("inner_join_on_k", |b| {
        b.iter(|| {
            let build_scan = MemTableScan::new(build_schema.clone(), build_batches.clone());
            let probe_scan = MemTableScan::new(probe_schema.clone(), probe_batches.clone());
            let probe_vec = VectorizedSeqScan::new(Box::new(probe_scan));
            let mut join = VectorizedHashJoin::new(
                Box::new(probe_vec),  // probe side (VectorizedOperator)
                Box::new(build_scan), // build side (Operator / pull)
                0,                    // probe key col
                0,                    // build key col
                out_schema.clone(),
            );
            let mut sink = CollectSink::new();
            join.drive(&mut sink).expect("join drive ok");
            black_box(sink.finish());
        });
    });
    group.finish();
}

// ============================================================================
// Benchmark: Dictionary filter vs raw filter_eq_i64
// ============================================================================

fn bench_dict_filter_vs_raw(c: &mut Criterion) {
    let n = 65_536usize;
    // Low-cardinality string column (100 distinct values)
    let strings: Vec<Option<&str>> = (0..n)
        .map(|i| {
            Some(match i % 100 {
                0 => "alpha",
                1 => "beta",
                2 => "gamma",
                3 => "delta",
                4 => "epsilon",
                5 => "zeta",
                6 => "eta",
                7 => "theta",
                8 => "iota",
                9 => "kappa",
                _ => "other",
            })
        })
        .collect();
    let dict =
        DictionaryColumn::from_strings(strings).expect("bench dictionary should fit u32 codes");
    let target_code = dict.code_for("alpha").expect("alpha in dict");

    // Equivalent i64 column for raw filter comparison
    let raw_i64: Vec<i64> = (0..n).map(|i| bench_i64_index(i) % 100).collect();
    let raw_col = NumericColumn::from_data(raw_i64);
    let target_i64 = 0i64;

    let mut group = c.benchmark_group("vec/dict_filter_vs_raw");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.throughput(Throughput::Elements(bench_row_count(n)));

    group.bench_function("dict_code_filter", |b| {
        b.iter(|| {
            let mask =
                ultrasql_vec::dict::filter_eq_dict_code(black_box(&dict), black_box(target_code));
            black_box(mask);
        });
    });

    group.bench_function("raw_filter_eq_i64", |b| {
        b.iter(|| {
            let mask = filter_eq_i64(black_box(&raw_col), black_box(target_i64));
            black_box(mask);
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: SIMD filter_eq_i32 kernel throughput
// ============================================================================

fn bench_filter_eq_i32_simd(c: &mut Criterion) {
    let sizes: &[usize] = &[1_024, 4_096, 65_536, 1_048_576];

    let mut group = c.benchmark_group("vec/filter_eq_i32_simd");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    for &n in sizes {
        let data: Vec<i32> = (0..n).map(|x| bench_i32_index(x) % 100).collect();
        let col = NumericColumn::from_data(data);
        group.throughput(Throughput::Elements(bench_row_count(n)));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let mask = filter_eq_i32(black_box(&col), black_box(42));
                black_box(mask);
            });
        });
    }
    group.finish();
}

// ============================================================================
// criterion harness
// ============================================================================

criterion_group!(
    benches,
    bench_seq_scan_filter_1m,
    bench_hash_agg_1m,
    bench_hash_join_100k_x_1m,
    bench_dict_filter_vs_raw,
    bench_filter_eq_i32_simd,
);
criterion_main!(benches);
