//! Microbenchmarks for the optimizer rules and plan cache.
//!
//! Three groups:
//!
//! - `optimizer/decorrelation` — rewrite a correlated-EXISTS plan using the
//!   decorrelation test helpers over 1 000 synthetic plan variants.
//! - `optimizer/cse` — detect and hoist a duplicate sub-expression from a
//!   `Filter` predicate.
//! - `optimizer/plan_cache` — concurrent cache lookups measuring throughput
//!   for the cache hot path (cache hit, no re-plan).
//!
//! **Host description:** results are valid only when compared on the same host
//! with the same Rust toolchain. Record the host descriptor in
//! `benchmarks/results/host.yaml` alongside criterion output.

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_optimizer::plan_cache::{PlanCache, PlanCacheConfig, PlanCacheKey};
use ultrasql_optimizer::rules::RewriteRule;
use ultrasql_optimizer::rules::common_subexpr::CommonSubExprElimination;
use ultrasql_optimizer::rules::subquery_decorrelation::SubqueryDecorrelation;
use ultrasql_planner::{
    BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr, UnaryOp,
};

// ============================================================================
// Helpers
// ============================================================================

fn two_col_scan(table: &str) -> LogicalPlan {
    LogicalPlan::Scan {
        table: table.into(),
        schema: Schema::new(vec![
            Field::required("id", DataType::Int32),
            Field::nullable("val", DataType::Int32),
        ])
        .expect("schema ok"),
        projection: None,
    }
}

fn col(name: &str, idx: usize) -> ScalarExpr {
    ScalarExpr::Column {
        name: name.into(),
        index: idx,
        data_type: DataType::Int32,
    }
}

const fn lit_i32(v: i32) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Int32(v),
        data_type: DataType::Int32,
    }
}

fn add(l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
    ScalarExpr::Binary {
        op: BinaryOp::Add,
        left: Box::new(l),
        right: Box::new(r),
        data_type: DataType::Int32,
    }
}

fn neg(e: ScalarExpr) -> ScalarExpr {
    ScalarExpr::Unary {
        op: UnaryOp::Neg,
        expr: Box::new(e),
        data_type: DataType::Int32,
    }
}

/// Build a decorrelated `Filter(LeftOuter(outer, sub), IS NOT NULL(sub.id))`.
///
/// Mimics the EXISTS-lowering output that `SubqueryDecorrelation` would
/// produce, so the decorrelation benchmark can apply the rule on realistic
/// (already-lowered) plan shapes without depending on a `#[cfg(test)]` helper.
fn exists_lowered_plan(outer: LogicalPlan, sub: LogicalPlan) -> LogicalPlan {
    // Concatenate the schemas by collecting all fields.
    let outer_schema = outer.schema();
    let sub_schema = sub.schema();
    let mut fields: Vec<Field> = outer_schema.fields().to_vec();
    fields.extend_from_slice(sub_schema.fields());
    let join_schema = Schema::new(fields).expect("join schema ok");

    // Use the first column of the sub as the IS NOT NULL witness.
    let witness_idx = outer_schema.len(); // first sub column after outer
    let witness_col = ScalarExpr::Column {
        name: sub_schema
            .fields()
            .first()
            .map_or("__wit__", |f| f.name.as_str())
            .into(),
        index: witness_idx,
        data_type: DataType::Int32,
    };

    let join = LogicalPlan::Join {
        left: Box::new(outer),
        right: Box::new(sub),
        join_type: LogicalJoinType::LeftOuter,
        condition: LogicalJoinCondition::None,
        schema: join_schema,
    };

    LogicalPlan::Filter {
        input: Box::new(join),
        predicate: ScalarExpr::IsNull {
            expr: Box::new(witness_col),
            negated: true, // IS NOT NULL
        },
    }
}

/// Build `Filter(Scan("outer"), neg(id+val) * neg(id+val) = 0)` — a plan
/// with a duplicate sub-expression of size 4.
fn plan_with_cse_candidate() -> LogicalPlan {
    let dup = ScalarExpr::Binary {
        op: BinaryOp::Mul,
        left: Box::new(neg(add(col("id", 0), col("val", 1)))),
        right: Box::new(neg(add(col("id", 0), col("val", 1)))),
        data_type: DataType::Int32,
    };
    LogicalPlan::Filter {
        input: Box::new(two_col_scan("t")),
        predicate: ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(dup),
            right: Box::new(lit_i32(0)),
            data_type: DataType::Bool,
        },
    }
}

// ============================================================================
// Benchmark: decorrelation
// ============================================================================

fn bench_decorrelation(c: &mut Criterion) {
    const N: u64 = 1_000;

    // Pre-build 1 000 synthetic EXISTS-lowered plans.
    let plans: Vec<LogicalPlan> = (0..N)
        .map(|i| {
            // Vary the filter literal so plans are structurally distinct.
            let outer = LogicalPlan::Filter {
                input: Box::new(two_col_scan("outer")),
                predicate: ScalarExpr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(col("id", 0)),
                    right: Box::new(lit_i32(i32::try_from(i % 1_000).unwrap_or(0))),
                    data_type: DataType::Bool,
                },
            };
            exists_lowered_plan(outer, two_col_scan("sub"))
        })
        .collect();

    let rule = SubqueryDecorrelation;
    let mut group = c.benchmark_group("optimizer/decorrelation");
    group.throughput(Throughput::Elements(N));

    group.bench_function("exists_1k", |b| {
        b.iter(|| {
            for plan in &plans {
                let _ = black_box(rule.apply(black_box(plan)));
            }
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: CSE
// ============================================================================

fn bench_cse(c: &mut Criterion) {
    let plan = plan_with_cse_candidate();
    let rule = CommonSubExprElimination;

    let mut group = c.benchmark_group("optimizer/cse");
    group.throughput(Throughput::Elements(1));

    group.bench_function("hoist_duplicate_subexpr", |b| {
        b.iter(|| {
            let result = rule.apply(black_box(&plan));
            black_box(result)
        });
    });

    group.finish();
}

// ============================================================================
// Benchmark: plan cache
// ============================================================================

fn bench_plan_cache(c: &mut Criterion) {
    const KEYS: u64 = 100; // 100 distinct prepared statements
    const ITERATIONS: u64 = 10_000;

    // Pre-populate cache.
    let cache = PlanCache::new(PlanCacheConfig::default());
    for i in 0..KEYS {
        let key = PlanCacheKey::named(format!("stmt{i}"));
        cache
            .get_or_plan(&key, &[], |_| {
                Ok(LogicalPlan::Empty {
                    schema: Schema::empty(),
                })
            })
            .expect("plan ok");
    }

    let mut group = c.benchmark_group("optimizer/plan_cache");
    group.throughput(Throughput::Elements(ITERATIONS));

    group.bench_function("hot_cache_10k_lookups", |b| {
        b.iter(|| {
            for i in 0..ITERATIONS {
                let key = PlanCacheKey::named(format!("stmt{}", i % KEYS));
                let plan = cache
                    .get_or_plan(black_box(&key), &[], |_| {
                        Ok(LogicalPlan::Empty {
                            schema: Schema::empty(),
                        })
                    })
                    .expect("ok");
                black_box(plan);
            }
        });
    });

    group.finish();
}

// ============================================================================
// Harness
// ============================================================================

criterion_group!(benches, bench_decorrelation, bench_cse, bench_plan_cache);
criterion_main!(benches);
