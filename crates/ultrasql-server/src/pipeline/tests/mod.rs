//! Unit tests for the pipeline lowerer.

use super::*;
use ultrasql_core::{DataType, Value};
use ultrasql_parser::Parser;
use ultrasql_planner::{InMemoryCatalog, LogicalPlan, ScalarExpr, bind};

mod modify_index;
mod select;

pub(super) fn fixture() -> (InMemoryCatalog, SampleTables) {
    let mut catalog = InMemoryCatalog::new();
    let tables = build_sample_database(&mut catalog);
    (catalog, tables)
}

pub(super) fn plan(sql: &str, catalog: &InMemoryCatalog) -> LogicalPlan {
    let stmt = Parser::new(sql).parse_statement().expect("parses");
    bind(&stmt, catalog).expect("binds")
}

#[test]
fn lowers_simple_scan_and_project() {
    let (catalog, tables) = fixture();
    let p = plan("SELECT id FROM users", &catalog);
    let mut op = lower_plan(&p, &tables).expect("lowers");
    let batch = op.next_batch().unwrap().expect("first batch");
    assert_eq!(batch.rows(), 3);
    assert_eq!(batch.width(), 1);
}

#[test]
fn lowers_filter_eq_int() {
    let (catalog, tables) = fixture();
    let p = plan("SELECT id FROM users WHERE id = 2", &catalog);
    let mut op = lower_plan(&p, &tables).expect("lowers");
    let batch = op.next_batch().unwrap().expect("first batch");
    assert_eq!(batch.rows(), 1);
}

#[test]
fn lowers_limit() {
    let (catalog, tables) = fixture();
    let p = plan("SELECT id FROM users LIMIT 1", &catalog);
    let mut op = lower_plan(&p, &tables).expect("lowers");
    let batch = op.next_batch().unwrap().expect("first batch");
    assert_eq!(batch.rows(), 1);
}

/// `LIMIT 1 OFFSET 1` over the 3-row sample skips the first row and
/// emits the second. Confirms the sample-path lowerer threads
/// `offset` through to the executor's `Limit::with_offset`.
#[test]
fn lowers_limit_with_offset() {
    let (catalog, tables) = fixture();
    let p = plan("SELECT id FROM users LIMIT 1 OFFSET 1", &catalog);
    let mut op = lower_plan(&p, &tables).expect("lowers");
    let mut ids: Vec<i32> = Vec::new();
    while let Some(batch) = op.next_batch().expect("ok") {
        if let ultrasql_vec::column::Column::Int32(col) = &batch.columns()[0] {
            ids.extend_from_slice(col.data());
        }
    }
    // Sample has ids [1,2,3]; LIMIT 1 OFFSET 1 yields the middle id.
    assert_eq!(ids, vec![2]);
}

/// `OFFSET 2` with no `LIMIT` emits every row past the skip. The
/// binder lowers this as `Limit { n: u64::MAX, offset: 2 }`; the
/// pipeline saturates `u64::MAX` into the executor's
/// "no limit" sentinel.
#[test]
fn lowers_offset_only_without_limit() {
    let (catalog, tables) = fixture();
    let p = plan("SELECT id FROM users OFFSET 2", &catalog);
    let mut op = lower_plan(&p, &tables).expect("lowers");
    let mut ids: Vec<i32> = Vec::new();
    while let Some(batch) = op.next_batch().expect("ok") {
        if let ultrasql_vec::column::Column::Int32(col) = &batch.columns()[0] {
            ids.extend_from_slice(col.data());
        }
    }
    // Sample has 3 rows; OFFSET 2 → 1 row remaining (id=3).
    assert_eq!(ids, vec![3]);
}

/// `LIMIT 0 OFFSET m` returns zero rows.
#[test]
fn lowers_zero_limit_with_offset() {
    let (catalog, tables) = fixture();
    let p = plan("SELECT id FROM users LIMIT 0 OFFSET 1", &catalog);
    let mut op = lower_plan(&p, &tables).expect("lowers");
    let first = op.next_batch().expect("ok");
    assert!(first.is_none(), "LIMIT 0 must emit nothing");
}

#[test]
fn lowers_order_by_asc_via_sample_path() {
    // `users` fixture has ids = [1, 2, 3]; an ASC sort by id leaves
    // them in the same order, but the plan still routes through
    // `Sort` — confirmed by `lower_plan` accepting the plan rather
    // than rejecting it with `Unsupported`.
    let (catalog, tables) = fixture();
    let p = plan("SELECT id FROM users ORDER BY id ASC", &catalog);
    let mut op = lower_plan(&p, &tables).expect("lowers");
    let mut ids: Vec<i32> = Vec::new();
    while let Some(batch) = op.next_batch().expect("ok") {
        if let ultrasql_vec::column::Column::Int32(col) = &batch.columns()[0] {
            ids.extend_from_slice(col.data());
        }
    }
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn lowers_order_by_desc_via_sample_path() {
    let (catalog, tables) = fixture();
    let p = plan("SELECT id FROM users ORDER BY id DESC", &catalog);
    let mut op = lower_plan(&p, &tables).expect("lowers");
    let mut ids: Vec<i32> = Vec::new();
    while let Some(batch) = op.next_batch().expect("ok") {
        if let ultrasql_vec::column::Column::Int32(col) = &batch.columns()[0] {
            ids.extend_from_slice(col.data());
        }
    }
    assert_eq!(ids, vec![3, 2, 1]);
}

/// Sort wrapped over a hand-built Values-like input runs through
/// `lower_query` and produces ascending output.
///
/// This is the headline contract for the wire wiring: a
/// `LogicalPlan::Sort` constructed in code (synthetic, no parser
/// involvement) lowers through `lower_query` and the resulting
/// operator emits a non-decreasing sequence on the sort column.
#[test]
fn lower_query_sorts_values_in_ascending_order() {
    use std::sync::Arc as StdArc;
    use ultrasql_catalog::PersistentCatalog;
    use ultrasql_core::{CommandId, DataType, Field, Schema, Value, Xid};
    use ultrasql_planner::SortKey;
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::heap::HeapAccess;
    use ultrasql_txn::TransactionManager;

    // Build a Values plan with three out-of-order rows.
    let values_schema = Schema::new([
        Field::nullable("a", DataType::Int32),
        Field::nullable("b", DataType::Int32),
    ])
    .expect("values schema");
    let row = |v: i32, w: i32| -> Vec<ScalarExpr> {
        vec![
            ScalarExpr::Literal {
                value: Value::Int32(v),
                data_type: DataType::Int32,
            },
            ScalarExpr::Literal {
                value: Value::Int32(w),
                data_type: DataType::Int32,
            },
        ]
    };
    let values_plan = LogicalPlan::Values {
        rows: vec![row(3, 30), row(1, 10), row(2, 20)],
        schema: values_schema,
    };
    let sort_plan = LogicalPlan::Sort {
        input: Box::new(values_plan),
        keys: vec![SortKey {
            expr: ScalarExpr::Column {
                name: "a".into(),
                index: 0,
                data_type: DataType::Int32,
            },
            asc: true,
            nulls_first: false,
        }],
    };

    // Build a minimal `LowerCtx`. We never reference the heap because
    // `Values` does not touch it, but the constructor still needs a
    // valid handle. The transaction is allocated only to materialise
    // a valid MVCC snapshot; we never commit it because the test
    // does not write to the heap.
    let catalog = StdArc::new(PersistentCatalog::new());
    let pool = StdArc::new(BufferPool::new(64, BlankPageLoader));
    let heap = StdArc::new(HeapAccess::new(pool));
    let vm = StdArc::new(ultrasql_storage::vm::VisibilityMap::new());
    let txn = StdArc::new(TransactionManager::new());
    let mvcc_snapshot = txn
        .begin(ultrasql_txn::IsolationLevel::ReadCommitted)
        .snapshot;
    let ctx = LowerCtx {
        tables: &SampleTables::new(),
        catalog_snapshot: catalog.snapshot(),
        table_constraints: StdArc::new(dashmap::DashMap::new()),
        sequences: StdArc::new(dashmap::DashMap::new()),
        sequence_state: None,
        heap,
        vm,
        snapshot: mvcc_snapshot,
        oracle: StdArc::clone(&txn),
        xid: Xid::new(0),
        command_id: CommandId::FIRST,
        cte_buffers: HashMap::new(),
        jit: ultrasql_vec::jit::JitConfig::OFF,
        cancel_flag: None,
        work_mem: std::sync::Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(u64::MAX)),
    };

    let mut op = lower_query(&sort_plan, &ctx).expect("lowers");
    let mut a_col: Vec<i32> = Vec::new();
    let mut b_col: Vec<i32> = Vec::new();
    while let Some(batch) = op.next_batch().expect("ok") {
        match (&batch.columns()[0], &batch.columns()[1]) {
            (ultrasql_vec::column::Column::Int32(a), ultrasql_vec::column::Column::Int32(b)) => {
                a_col.extend_from_slice(a.data());
                b_col.extend_from_slice(b.data());
            }
            _ => panic!("unexpected column layout"),
        }
    }
    assert_eq!(a_col, vec![1, 2, 3]);
    assert_eq!(b_col, vec![10, 20, 30]);
}

#[test]
fn rejects_unknown_table_via_plan_error() {
    // We hand-build the plan directly (the binder catches unknown
    // tables earlier), to exercise the lowerer's own fallback.
    let (_, tables) = fixture();
    let p = LogicalPlan::Scan {
        table: "nope".into(),
        schema: Schema::new([Field::required("id", DataType::Int32)]).unwrap(),
        projection: None,
    };
    let err = lower_plan(&p, &tables).expect_err("must reject");
    assert!(matches!(err, ServerError::Plan(_)));
}

// ----------------------------------------------------------------
// JOIN dispatch (Wave A item A4)
// ----------------------------------------------------------------

/// Helper: build a typed `Column` reference. Index is the column's
/// position in the *concatenated* (left++right) schema for join-on
/// predicates, or its native position when the column lives on a
/// single side.
pub(super) fn column(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
    ScalarExpr::Column {
        name: name.into(),
        index,
        data_type,
    }
}

/// Helper: build an Int32 literal.
pub(super) fn lit_i32(v: i32) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Int32(v),
        data_type: DataType::Int32,
    }
}

/// Helper: build an int32 single-column schema named `name`.
pub(super) fn schema_int_col(name: &str) -> Schema {
    Schema::new([Field::required(name, DataType::Int32)]).expect("schema ok")
}

/// Helper: build a `(name, val)` row of Int32 literals.
pub(super) fn int_row(v: i32) -> Vec<ScalarExpr> {
    vec![lit_i32(v)]
}

/// Walk a fresh operator producing `(Int32, Int32)` batches and
/// collect `(left, right)` pairs. NULLs decode to `0` because the
/// v0.5 `build_batch` does not emit a per-column null bitmap (see
/// `hash_join.rs::hash_join_left_outer_unmatched_rows` for the
/// documented behaviour).
pub(super) fn collect_pairs(op: &mut dyn Operator) -> Vec<(i32, i32)> {
    let mut out = Vec::new();
    while let Some(batch) = op.next_batch().expect("operator must not error") {
        assert_eq!(batch.width(), 2, "expected two-column join output");
        match (&batch.columns()[0], &batch.columns()[1]) {
            (ultrasql_vec::column::Column::Int32(l), ultrasql_vec::column::Column::Int32(r)) => {
                assert_eq!(l.data().len(), r.data().len());
                for (a, b) in l.data().iter().zip(r.data().iter()) {
                    out.push((*a, *b));
                }
            }
            other => panic!("unexpected column layout: {other:?}"),
        }
    }
    out
}

/// Build a minimal `LowerCtx` suitable for `lower_query` calls that
/// never touch the real heap (Values-rooted plans).
pub(super) fn synthetic_ctx(tables: &SampleTables) -> LowerCtx<'_> {
    use std::sync::Arc as StdArc;
    use ultrasql_catalog::PersistentCatalog;
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::heap::HeapAccess;
    use ultrasql_txn::TransactionManager;

    let catalog = StdArc::new(PersistentCatalog::new());
    let pool = StdArc::new(BufferPool::new(64, BlankPageLoader));
    let heap = StdArc::new(HeapAccess::new(pool));
    let vm = StdArc::new(ultrasql_storage::vm::VisibilityMap::new());
    let txn = StdArc::new(TransactionManager::new());
    let mvcc_snapshot = txn
        .begin(ultrasql_txn::IsolationLevel::ReadCommitted)
        .snapshot;
    LowerCtx {
        tables,
        catalog_snapshot: catalog.snapshot(),
        table_constraints: StdArc::new(dashmap::DashMap::new()),
        sequences: StdArc::new(dashmap::DashMap::new()),
        sequence_state: None,
        heap,
        vm,
        snapshot: mvcc_snapshot,
        oracle: StdArc::clone(&txn),
        xid: Xid::new(0),
        command_id: CommandId::FIRST,
        cte_buffers: HashMap::new(),
        jit: ultrasql_vec::jit::JitConfig::OFF,
        cancel_flag: None,
        work_mem: std::sync::Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(u64::MAX)),
    }
}

/// Build two single-column `Int32` Values children with the given
/// rows, the binder-shaped concatenated join schema, and a typed
/// `LogicalPlan::Join` ready to be lowered.
pub(super) fn build_int_join_plan(
    left_rows: &[i32],
    right_rows: &[i32],
    join_type: LogicalJoinType,
    condition: LogicalJoinCondition,
) -> LogicalPlan {
    let left_schema = schema_int_col("l");
    let right_schema = schema_int_col("r");
    let out_schema = Schema::new([
        Field::required("l", DataType::Int32),
        Field::required("r", DataType::Int32),
    ])
    .expect("concat schema ok");
    let left = LogicalPlan::Values {
        rows: left_rows.iter().map(|v| int_row(*v)).collect(),
        schema: left_schema,
    };
    let right = LogicalPlan::Values {
        rows: right_rows.iter().map(|v| int_row(*v)).collect(),
        schema: right_schema,
    };
    LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type,
        condition,
        schema: out_schema,
    }
}

/// Equi predicate over a binder-shaped concatenated schema where
/// the right column lives at index 1.
pub(super) fn equi_eq_predicate() -> LogicalJoinCondition {
    LogicalJoinCondition::On(ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(column("l", 0, DataType::Int32)),
        right: Box::new(column("r", 1, DataType::Int32)),
        data_type: DataType::Bool,
    })
}

/// Non-equi predicate `left.l < right.r` — should fall through to
/// NLJ in [`lower_join`].
pub(super) fn non_equi_lt_predicate() -> LogicalJoinCondition {
    LogicalJoinCondition::On(ScalarExpr::Binary {
        op: BinaryOp::Lt,
        left: Box::new(column("l", 0, DataType::Int32)),
        right: Box::new(column("r", 1, DataType::Int32)),
        data_type: DataType::Bool,
    })
}

/// Lower a synthetic Inner equi-join through `lower_query` and
/// assert the operator picked is [`HashJoin`] (via `Debug` output —
/// the operator type appears in the `{op:?}` rendering).
#[test]
fn lower_query_inner_equi_join_picks_hash_join() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let plan = build_int_join_plan(
        &[1, 2, 3, 4],
        &[2, 3, 5],
        LogicalJoinType::Inner,
        equi_eq_predicate(),
    );
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    // The debug representation of `HashJoin` begins with that name.
    let debug = format!("{op:?}");
    assert!(
        debug.starts_with("HashJoin"),
        "expected HashJoin, got: {debug}"
    );
    let mut pairs = collect_pairs(op.as_mut());
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(2, 2), (3, 3)]);
}

/// Lower a synthetic Inner non-equi join. The predicate is
/// `l.l < r.r`, which is not hash-eligible, so the dispatch must
/// pick [`NestedLoopJoin`].
#[test]
fn lower_query_inner_non_equi_join_falls_back_to_nlj() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let plan = build_int_join_plan(
        &[1, 2, 3],
        &[2, 4],
        LogicalJoinType::Inner,
        non_equi_lt_predicate(),
    );
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    let debug = format!("{op:?}");
    assert!(
        debug.starts_with("NestedLoopJoin"),
        "expected NestedLoopJoin, got: {debug}"
    );
    // 1<2, 1<4, 2<4, 3<4 = 4 matches.
    let mut pairs = collect_pairs(op.as_mut());
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(1, 2), (1, 4), (2, 4), (3, 4)]);
}

/// Lower a LEFT OUTER equi join. Build = left so unmatched left
/// rows survive; `HashJoin` is the chosen operator.
///
/// Unmatched right columns decode to `0` here because `build_batch`
/// does not yet emit a per-column null bitmap (the same v0.5
/// limitation documented in `hash_join.rs::hash_join_left_outer_unmatched_rows`).
#[test]
fn lower_query_left_outer_equi_join_picks_hash_join_and_pads() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let plan = build_int_join_plan(
        &[1, 2, 3],
        &[2, 4],
        LogicalJoinType::LeftOuter,
        equi_eq_predicate(),
    );
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    let debug = format!("{op:?}");
    assert!(
        debug.starts_with("HashJoin"),
        "expected HashJoin, got: {debug}"
    );
    let mut pairs = collect_pairs(op.as_mut());
    pairs.sort_unstable();
    // (2,2) is the match; (1,*) and (3,*) are unmatched left rows
    // emitted with right-side NULLs encoded as 0.
    assert_eq!(pairs, vec![(1, 0), (2, 2), (3, 0)]);
}

/// LEFT OUTER over a non-equi predicate must dispatch to NLJ (the
/// only operator that can serve it correctly today).
#[test]
fn lower_query_left_outer_non_equi_join_falls_back_to_nlj() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let plan = build_int_join_plan(
        &[1, 5, 10],
        &[2, 7],
        LogicalJoinType::LeftOuter,
        non_equi_lt_predicate(),
    );
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    let debug = format!("{op:?}");
    assert!(
        debug.starts_with("NestedLoopJoin"),
        "expected NestedLoopJoin, got: {debug}"
    );
    // 1 matches 2 and 7; 5 matches 7; 10 matches nothing (LeftOuter
    // emits (10, NULL)).
    let mut pairs = collect_pairs(op.as_mut());
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(1, 2), (1, 7), (5, 7), (10, 0)]);
}

/// CROSS JOIN dispatches to NLJ with no condition. Output is the
/// Cartesian product.
#[test]
fn lower_query_cross_join_dispatches_to_nlj() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let plan = build_int_join_plan(
        &[1, 2],
        &[10, 20, 30],
        LogicalJoinType::Cross,
        LogicalJoinCondition::None,
    );
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    let debug = format!("{op:?}");
    assert!(
        debug.starts_with("NestedLoopJoin"),
        "expected NestedLoopJoin, got: {debug}"
    );
    let pairs = collect_pairs(op.as_mut());
    assert_eq!(pairs.len(), 6, "2 × 3 Cartesian = 6 rows");
}
