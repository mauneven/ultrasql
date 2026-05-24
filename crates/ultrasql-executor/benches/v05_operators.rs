//! Criterion benchmarks for v0.5 executor operators.
//!
//! Each benchmark exercises one operator category. The input size is fixed
//! at 4096 rows (one batch) so comparisons are within the hot path rather
//! than dominated by allocation.
//!
//! Run with:
//!   cargo bench -p ultrasql-executor --bench `v05_operators`

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use ultrasql_core::{DataType, Field, Schema};
use ultrasql_executor::{
    FunctionScan, HashAggregate, MemTableScan, Operator, Sort, SortAggregate, Unique,
    merge_join::MergeJoin, unique::UniqueMode,
};
use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, LogicalJoinType, ScalarExpr, SortKey};
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

const N: usize = 4096;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn schema_i32() -> Schema {
    Schema::new([Field::required("v", DataType::Int32)]).expect("schema ok")
}

fn schema_i32_i64() -> Schema {
    Schema::new([
        Field::required("k", DataType::Int32),
        Field::required("v", DataType::Int64),
    ])
    .expect("schema ok")
}

fn int32_batch(n: usize) -> Batch {
    let data: Vec<i32> = (0..i32::try_from(n).expect("fits")).collect();
    Batch::new([Column::Int32(NumericColumn::from_data(data))]).expect("batch ok")
}

fn int32_i64_batch(n: usize) -> Batch {
    let keys: Vec<i32> = (0..i32::try_from(n).expect("fits")).collect();
    let vals: Vec<i64> = (0..i64::try_from(n).expect("fits")).collect();
    Batch::new([
        Column::Int32(NumericColumn::from_data(keys)),
        Column::Int64(NumericColumn::from_data(vals)),
    ])
    .expect("batch ok")
}

fn drain(op: &mut dyn Operator) {
    while let Some(_b) = op.next_batch().expect("no error") {}
}

// ---------------------------------------------------------------------------
// Bench 1: Sort — ascending i32 key, 4096 rows already in-order
// ---------------------------------------------------------------------------

fn bench_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.bench_function("4096_i32_sorted_input", |b| {
        b.iter_batched(
            || {
                let batch = int32_batch(N);
                let schema = schema_i32();
                let scan = MemTableScan::new(schema.clone(), vec![batch]);
                let keys = vec![SortKey {
                    expr: ScalarExpr::Column {
                        name: "v".into(),
                        index: 0,
                        data_type: DataType::Int32,
                    },
                    asc: true,
                    nulls_first: false,
                }];
                Sort::new(Box::new(scan), keys, schema)
            },
            |mut op| drain(&mut op),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Bench 2: HashAggregate — COUNT(*) over 4096 rows, no GROUP BY
// ---------------------------------------------------------------------------

fn bench_hash_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_aggregate");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.bench_function("count_star_4096_rows", |b| {
        b.iter_batched(
            || {
                let batch = int32_batch(N);
                let scan = MemTableScan::new(schema_i32(), vec![batch]);
                let out_schema =
                    Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");
                HashAggregate::new(
                    Box::new(scan),
                    vec![],
                    vec![LogicalAggregateExpr {
                        func: AggregateFunc::CountStar,
                        arg: None,
                        direct_arg: None,
                        order_by: None,
                        distinct: false,
                        output_name: "cnt".into(),
                        data_type: DataType::Int64,
                    }],
                    out_schema,
                )
            },
            |mut op| drain(&mut op),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Bench 3: SortAggregate — COUNT(*) over 4096 already-sorted rows, 1 group key
// ---------------------------------------------------------------------------

fn bench_sort_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_aggregate");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.bench_function("count_star_4096_rows_grouped", |b| {
        b.iter_batched(
            || {
                let batch = int32_i64_batch(N);
                let scan = MemTableScan::new(schema_i32_i64(), vec![batch]);
                let out_schema = Schema::new([
                    Field::required("k", DataType::Int32),
                    Field::required("cnt", DataType::Int64),
                ])
                .expect("schema ok");
                SortAggregate::new(
                    Box::new(scan),
                    vec![ScalarExpr::Column {
                        name: "k".into(),
                        index: 0,
                        data_type: DataType::Int32,
                    }],
                    vec![LogicalAggregateExpr {
                        func: AggregateFunc::CountStar,
                        arg: None,
                        direct_arg: None,
                        order_by: None,
                        distinct: false,
                        output_name: "cnt".into(),
                        data_type: DataType::Int64,
                    }],
                    out_schema,
                )
            },
            |mut op| drain(&mut op),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Bench 4: Unique (hash mode) — 4096 rows, 1024 unique values
// ---------------------------------------------------------------------------

fn bench_unique_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("unique");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.bench_function("hash_mode_4096_rows_1024_unique", |b| {
        b.iter_batched(
            || {
                // Values 0..1024 repeated four times.
                let data: Vec<i32> = (0..i32::try_from(N).expect("fits"))
                    .map(|i| i % 1024)
                    .collect();
                let batch =
                    Batch::new([Column::Int32(NumericColumn::from_data(data))]).expect("batch ok");
                let scan = MemTableScan::new(schema_i32(), vec![batch]);
                Unique::new(Box::new(scan), UniqueMode::Hash)
            },
            |mut op| drain(&mut op),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Bench 5: MergeJoin — inner join of two sorted 4096-row inputs on equal keys
// ---------------------------------------------------------------------------

fn bench_merge_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_join");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.bench_function("inner_4096x4096_sorted_keys", |b| {
        b.iter_batched(
            || {
                let left_schema = schema_i32();
                let right_schema = schema_i32();
                let join_schema = Schema::new([
                    Field::required("l", DataType::Int32),
                    Field::required("r", DataType::Int32),
                ])
                .expect("schema ok");
                let left_batch = int32_batch(N);
                let right_batch = int32_batch(N);
                let left_scan = MemTableScan::new(left_schema.clone(), vec![left_batch]);
                let right_scan = MemTableScan::new(right_schema.clone(), vec![right_batch]);
                MergeJoin::new(
                    Box::new(left_scan),
                    Box::new(right_scan),
                    ScalarExpr::Column {
                        name: "v".into(),
                        index: 0,
                        data_type: DataType::Int32,
                    },
                    ScalarExpr::Column {
                        name: "v".into(),
                        index: 0,
                        data_type: DataType::Int32,
                    },
                    LogicalJoinType::Inner,
                    join_schema,
                    left_schema,
                    right_schema,
                )
            },
            |mut op| drain(&mut op),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Bench 6: generate_series — 4096 rows [0, 4095]
// ---------------------------------------------------------------------------

fn bench_generate_series(c: &mut Criterion) {
    let mut group = c.benchmark_group("function_scan");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(3));
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.bench_function("generate_series_4096", |b| {
        b.iter_batched(
            || FunctionScan::generate_series(0, i64::try_from(N - 1).expect("fits"), 1),
            |mut op| drain(&mut op),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_sort,
    bench_hash_aggregate,
    bench_sort_aggregate,
    bench_unique_hash,
    bench_merge_join,
    bench_generate_series,
);
criterion_main!(benches);
