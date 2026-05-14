//! Push-pipeline vs pull-pipeline benchmark.
//!
//! Measures the throughput difference between the vectorized push pipeline
//! (`VectorizedSeqScan` → `VectorizedFilter` → `SumSink`) and the equivalent
//! pull pipeline (`MemTableScan` → `Filter` → accumulate via `next_batch`
//! loop) over a 1 M-row `Int64` dataset.
//!
//! ## What each pipeline does
//!
//! Both pipelines process the same in-memory data:
//! - **Dataset**: 1 M rows of `(x: Int64)` where `x = row_index % cardinality`.
//! - **Filter**: `x > threshold` (passes ~half the rows).
//! - **Aggregate**: sum of `x` across all surviving rows.
//!
//! ## Why push should be faster
//!
//! The push pipeline (`VectorizedFilter`) applies a SIMD selection vector
//! (`filter_eq_i64`) per batch in a tight loop. The pull pipeline
//! (`Filter`) decodes every batch into `Vec<Vec<Value>>` rows, evaluates
//! the predicate via the `Eval` interpreter, and reconstructs a new batch
//! — O(n) allocations per batch.
//!
//! ## Benchmark groups
//!
//! | Group | Description |
//! |---|---|
//! | `pipeline/push_scan_filter_sum_1m` | Push: VectorizedSeqScan + VectorizedFilter + SumSink |
//! | `pipeline/pull_scan_filter_collect_1m` | Pull: MemTableScan + Filter + next_batch drain |
//!
//! ## Reproducing results
//!
//! ```sh
//! cargo bench --bench push_vs_pull -p ultrasql-bench
//! ```
//!
//! Results are logged to `target/criterion/`. The ratio printed at the end
//! of each run is push_throughput / pull_throughput (higher = push wins).

#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_executor::push_pipeline::CollectSink;
use ultrasql_executor::sinks::SumSink;
use ultrasql_executor::vec_ops::scan::VectorizedSeqScan;
use ultrasql_executor::vec_ops::vec_filter::VectorizedFilter;
use ultrasql_executor::{Filter, MemTableScan, Operator, VectorizedPipeline};
use ultrasql_planner::{BinaryOp, ScalarExpr};
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

// ============================================================================
// Constants
// ============================================================================

/// Total number of rows in the benchmark dataset.
const N_ROWS: usize = 1_000_000;

/// Rows per batch, matching the executor ceiling from ARCHITECTURE.md §9.
const BATCH_SIZE: usize = 4_096;

/// Cardinality of the `x` column: values cycle in `[0, CARDINALITY)`.
const CARDINALITY: i64 = 1_000;

/// Filter threshold: `x > THRESHOLD` passes ~half the rows.
const THRESHOLD: i64 = 499;

// ============================================================================
// Helpers
// ============================================================================

/// Single-column `(x: Int64)` schema.
fn schema_x() -> Schema {
    Schema::new([Field::required("x", DataType::Int64)]).expect("schema ok")
}

/// Build the 1 M-row dataset as a `Vec<Batch>` of `BATCH_SIZE`-row batches.
///
/// Each row `i` has `x = i as i64 % CARDINALITY`.
fn make_batches() -> Vec<Batch> {
    let mut batches = Vec::with_capacity(N_ROWS / BATCH_SIZE);
    let mut offset = 0usize;
    while offset < N_ROWS {
        let end = (offset + BATCH_SIZE).min(N_ROWS);
        let xs: Vec<i64> = (offset..end).map(|i| (i as i64) % CARDINALITY).collect();
        batches.push(Batch::new([Column::Int64(NumericColumn::from_data(xs))]).expect("batch ok"));
        offset = end;
    }
    batches
}

/// Build the `x > THRESHOLD` predicate expression (Int64).
fn pred_gt_threshold() -> ScalarExpr {
    ScalarExpr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(ScalarExpr::Column {
            name: "x".into(),
            index: 0,
            data_type: DataType::Int64,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int64(THRESHOLD),
            data_type: DataType::Int64,
        }),
        data_type: DataType::Bool,
    }
}

// ============================================================================
// Push pipeline benchmark
// ============================================================================

/// Benchmark: vectorized push pipeline over 1 M rows.
///
/// Chain: `VectorizedSeqScan` → `VectorizedFilter` → `SumSink`.
///
/// `VectorizedFilter` uses a SIMD-accelerated selection vector via
/// `filter_eq_i64` for the equality fast-path; for `Gt` predicates it falls
/// back to the `Eval` interpreter but still benefits from the push model's
/// tight loop and avoidance of per-batch `Vec<Vec<Value>>` allocation.
fn bench_push_scan_filter_sum(c: &mut Criterion) {
    let batches = make_batches();
    let schema = schema_x();

    let mut group = c.benchmark_group("pipeline/push_scan_filter_sum_1m");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.warm_up_time(std::time::Duration::from_secs(2));
    group.throughput(Throughput::Elements(N_ROWS as u64));

    group.bench_function("push", |b| {
        b.iter(|| {
            let scan = MemTableScan::new(schema.clone(), batches.clone());
            let vscan = VectorizedSeqScan::new(Box::new(scan));
            let filter = VectorizedFilter::new(Box::new(vscan), black_box(pred_gt_threshold()));
            let mut pipeline = VectorizedPipeline::builder()
                .source(Box::new(filter))
                .build()
                .expect("pipeline builds");
            let mut sink = SumSink::new();
            pipeline.drive(&mut sink).expect("push pipeline ok");
            black_box(sink.final_value())
        });
    });

    group.finish();
}

// ============================================================================
// Pull pipeline benchmark
// ============================================================================

/// Benchmark: scalar pull pipeline over 1 M rows.
///
/// Chain: `MemTableScan` → `Filter` → drain via `Operator::next_batch`.
///
/// `Filter` decodes each batch into `Vec<Vec<Value>>` rows, evaluates the
/// predicate via the `Eval` interpreter, and rebuilds a new batch — one
/// allocation per surviving row per batch. The caller then accumulates the
/// sum by iterating the output batch columns.
fn bench_pull_scan_filter_collect(c: &mut Criterion) {
    let batches = make_batches();
    let schema = schema_x();

    let mut group = c.benchmark_group("pipeline/pull_scan_filter_collect_1m");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.warm_up_time(std::time::Duration::from_secs(2));
    group.throughput(Throughput::Elements(N_ROWS as u64));

    group.bench_function("pull", |b| {
        b.iter(|| {
            let scan = MemTableScan::new(schema.clone(), batches.clone());
            let mut filter = Filter::new(Box::new(scan), black_box(pred_gt_threshold()));
            let mut sum: i64 = 0;
            while let Some(batch) = filter.next_batch().expect("pull pipeline ok") {
                if batch.is_empty() {
                    continue;
                }
                match &batch.columns()[0] {
                    Column::Int64(c) => {
                        for &v in c.data() {
                            sum = sum.wrapping_add(v);
                        }
                    }
                    _ => panic!("expected Int64"),
                }
            }
            black_box(sum)
        });
    });

    group.finish();
}

// ============================================================================
// Push vs pull: side-by-side comparison in one group
// ============================================================================

/// Side-by-side comparison: push and pull in a single criterion group so the
/// report includes a direct ratio.
///
/// Both variants process the same 1 M-row dataset with a `x > 499` filter
/// and accumulate `SUM(x)`.
fn bench_push_vs_pull_side_by_side(c: &mut Criterion) {
    let batches = make_batches();
    let schema = schema_x();

    let mut group = c.benchmark_group("pipeline/push_vs_pull_1m");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.warm_up_time(std::time::Duration::from_secs(2));
    group.throughput(Throughput::Elements(N_ROWS as u64));

    // ---- Push ----
    group.bench_function("push", |b| {
        b.iter(|| {
            let scan = MemTableScan::new(schema.clone(), batches.clone());
            let vscan = VectorizedSeqScan::new(Box::new(scan));
            let filter = VectorizedFilter::new(Box::new(vscan), black_box(pred_gt_threshold()));
            let mut pipeline = VectorizedPipeline::builder()
                .source(Box::new(filter))
                .build()
                .expect("pipeline builds");
            let mut sink = CollectSink::new();
            pipeline.drive(&mut sink).expect("push pipeline ok");
            let output = sink.finish();
            let sum: i64 = output
                .iter()
                .flat_map(|b| match &b.columns()[0] {
                    Column::Int64(c) => c.data().to_vec(),
                    _ => panic!("expected Int64"),
                })
                .sum();
            black_box(sum)
        });
    });

    // ---- Pull ----
    group.bench_function("pull", |b| {
        b.iter(|| {
            let scan = MemTableScan::new(schema.clone(), batches.clone());
            let mut filter = Filter::new(Box::new(scan), black_box(pred_gt_threshold()));
            let mut sum: i64 = 0;
            while let Some(batch) = filter.next_batch().expect("pull pipeline ok") {
                if batch.is_empty() {
                    continue;
                }
                match &batch.columns()[0] {
                    Column::Int64(c) => {
                        for &v in c.data() {
                            sum = sum.wrapping_add(v);
                        }
                    }
                    _ => panic!("expected Int64"),
                }
            }
            black_box(sum)
        });
    });

    group.finish();
}

// ============================================================================
// criterion harness
// ============================================================================

criterion_group!(
    benches,
    bench_push_scan_filter_sum,
    bench_pull_scan_filter_collect,
    bench_push_vs_pull_side_by_side,
);
criterion_main!(benches);
