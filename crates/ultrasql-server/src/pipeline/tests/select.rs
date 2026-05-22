//! SELECT-shape tests.

use super::{
    build_int_join_plan, collect_pairs, equi_eq_predicate, fixture, int_row, lit_i32, plan,
    schema_int_col, synthetic_ctx,
};
use crate::pipeline::*;
use ultrasql_core::{DataType, Field, RelationId, Schema, TupleId, Value};
use ultrasql_executor::Operator;
use ultrasql_planner::{
    AggregateFunc, BinaryOp, LogicalAggregateExpr, LogicalJoinType, LogicalPlan, ScalarExpr,
};
use ultrasql_storage::heap::{HeapAccess, InsertOptions};

#[test]
fn lower_query_right_outer_equi_join_uses_nlj_not_hash_join() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let plan = build_int_join_plan(
        &[2],
        &[1, 2, 3],
        LogicalJoinType::RightOuter,
        equi_eq_predicate(),
    );
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    let debug = format!("{op:?}");
    assert!(
        debug.starts_with("NestedLoopJoin"),
        "RightOuter must not pick HashJoin; got: {debug}"
    );
    // Inner match: (2,2). RightOuter emits (NULL, 1) and (NULL, 3).
    let mut pairs = collect_pairs(op.as_mut());
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(0, 1), (0, 3), (2, 2)]);
}

#[test]
fn direct_scalar_avg_miss_populates_column_cache_and_next_lowering_uses_cached_avg() {
    let tables = SampleTables::new();
    let rows: Vec<(i32, i32)> = (0..2048_i32).map(|i| (i, i)).collect();
    let (fixture, entry, _tids) = build_index_fixture("bench_avg_cache", &rows, false);
    let ctx = fixture.ctx(&tables);

    let plan = LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::Scan {
            table: "bench_avg_cache".into(),
            schema: entry.schema.clone(),
            projection: None,
        }),
        group_by: Vec::new(),
        aggregates: vec![LogicalAggregateExpr {
            func: AggregateFunc::Avg,
            arg: Some(ScalarExpr::Column {
                name: "val".into(),
                index: 1,
                data_type: DataType::Int32,
            }),
            distinct: false,
            output_name: "avg".into(),
            data_type: DataType::Float64,
        }],
        schema: Schema::new([Field::required("avg", DataType::Float64)]).expect("schema ok"),
    };

    let mut first = lower_query(&plan, &ctx).expect("first lowering");
    assert!(first.next_batch().expect("first batch").is_some());
    assert!(first.next_batch().expect("eof").is_none());

    let rel = RelationId(entry.oid);
    assert!(
        fixture.heap.column_cache.get(rel).is_some(),
        "first scalar aggregate should populate column cache"
    );

    let second = lower_query(&plan, &ctx).expect("second lowering");
    let debug = format!("{second:?}");
    assert!(
        debug.starts_with("CachedAvgI32Scan"),
        "expected cached avg path on second lowering, got: {debug}"
    );
}

#[test]
fn fused_filter_sum_miss_populates_column_cache_and_next_lowering_uses_cached_filter_sum() {
    let tables = SampleTables::new();
    let rows: Vec<(i32, i32)> = (0..2048_i32).map(|i| (i, i)).collect();
    let (fixture, entry, _tids) = build_index_fixture("bench_filter_sum_cache", &rows, false);
    let ctx = fixture.ctx(&tables);

    let scan = LogicalPlan::Scan {
        table: "bench_filter_sum_cache".into(),
        schema: entry.schema.clone(),
        projection: None,
    };
    let filter = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate: ScalarExpr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(ScalarExpr::Column {
                name: "val".into(),
                index: 1,
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(1024),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Bool,
        },
    };
    let plan = LogicalPlan::Aggregate {
        input: Box::new(filter),
        group_by: Vec::new(),
        aggregates: vec![LogicalAggregateExpr {
            func: AggregateFunc::Sum,
            arg: Some(ScalarExpr::Column {
                name: "val".into(),
                index: 1,
                data_type: DataType::Int32,
            }),
            distinct: false,
            output_name: "sum".into(),
            data_type: DataType::Int64,
        }],
        schema: Schema::new([Field::required("sum", DataType::Int64)]).expect("schema ok"),
    };

    let mut first = lower_query(&plan, &ctx).expect("first lowering");
    assert!(first.next_batch().expect("first batch").is_some());
    assert!(first.next_batch().expect("eof").is_none());

    let rel = RelationId(entry.oid);
    assert!(
        fixture.heap.column_cache.get(rel).is_some(),
        "first filter-sum should populate column cache"
    );

    let second = lower_query(&plan, &ctx).expect("second lowering");
    let debug = format!("{second:?}");
    assert!(
        debug.starts_with("CachedFilterSumI32Scan"),
        "expected cached filter-sum path on second lowering, got: {debug}"
    );
}

// ----------------------------------------------------------------
// SetOp dispatch (Wave A item A7)
// ----------------------------------------------------------------

/// Build a single-column `Int32` [`LogicalPlan::Values`] from a slice
/// of integers. Helper for the `SetOp` unit tests below.
fn build_int_values_plan(rows: &[i32]) -> LogicalPlan {
    LogicalPlan::Values {
        rows: rows.iter().map(|v| int_row(*v)).collect(),
        schema: schema_int_col("v"),
    }
}

/// Build a [`LogicalPlan::SetOp`] over two `Values` children with a
/// single `Int32` column. The output schema is built the same way
/// `bind_set_op` does (nullable copies of the left side's columns)
/// so the kernel-shaped plan exactly mirrors what the binder emits.
fn build_int_set_op_plan(
    left_rows: &[i32],
    right_rows: &[i32],
    op: ultrasql_planner::LogicalSetOp,
    quantifier: ultrasql_planner::LogicalSetQuantifier,
) -> LogicalPlan {
    let out_schema = Schema::new([Field::nullable("v", DataType::Int32)]).expect("schema ok");
    LogicalPlan::SetOp {
        op,
        quantifier,
        left: Box::new(build_int_values_plan(left_rows)),
        right: Box::new(build_int_values_plan(right_rows)),
        schema: out_schema,
    }
}

/// Walk a `SetOp` operator and collect its emitted Int32 values into a
/// sorted `Vec` for order-independent assertion. The kernel emits
/// rows in left-insertion order; the tests sort to keep assertions
/// robust against any future ordering refinement that does not
/// change the multiset of rows.
fn drain_int_setop(op: &mut dyn Operator) -> Vec<i32> {
    let mut out: Vec<i32> = Vec::new();
    while let Some(batch) = op.next_batch().expect("setop operator must not error") {
        assert_eq!(batch.width(), 1, "SetOp output schema is one column wide");
        if let ultrasql_vec::column::Column::Int32(col) = &batch.columns()[0] {
            out.extend_from_slice(col.data());
        } else {
            panic!("unexpected column layout for single-Int32 set-op output");
        }
    }
    out.sort_unstable();
    out
}

/// `SELECT v FROM l UNION SELECT v FROM r` — duplicates removed,
/// surviving rows are the distinct union.
#[test]
fn lower_query_union_distinct_deduplicates() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let plan = build_int_set_op_plan(
        &[1, 2, 2, 3],
        &[2, 3, 4],
        ultrasql_planner::LogicalSetOp::Union,
        ultrasql_planner::LogicalSetQuantifier::Distinct,
    );
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    assert_eq!(drain_int_setop(op.as_mut()), vec![1, 2, 3, 4]);
}

/// `SELECT v FROM l UNION ALL SELECT v FROM r` — duplicates kept.
#[test]
fn lower_query_union_all_concatenates() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let plan = build_int_set_op_plan(
        &[1, 2, 2],
        &[2, 3, 3],
        ultrasql_planner::LogicalSetOp::Union,
        ultrasql_planner::LogicalSetQuantifier::All,
    );
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    assert_eq!(drain_int_setop(op.as_mut()), vec![1, 2, 2, 2, 3, 3]);
}

/// `SELECT v FROM l INTERSECT SELECT v FROM r` — distinct rows in both.
#[test]
fn lower_query_intersect_distinct_returns_common_distinct_rows() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let plan = build_int_set_op_plan(
        &[1, 2, 2, 3],
        &[2, 3, 3, 4],
        ultrasql_planner::LogicalSetOp::Intersect,
        ultrasql_planner::LogicalSetQuantifier::Distinct,
    );
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    assert_eq!(drain_int_setop(op.as_mut()), vec![2, 3]);
}

/// `SELECT v FROM l INTERSECT ALL SELECT v FROM r` — multiset
/// intersection: emit each row up to `min(left_count, right_count)`
/// times.
#[test]
fn lower_query_intersect_all_respects_multiset_min_counts() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    // left: 1×{1}, 3×{2}, 1×{3}; right: 2×{2}, 1×{3}, 1×{4}.
    // multiset min: 0×{1}, 2×{2}, 1×{3} → [2, 2, 3].
    let plan = build_int_set_op_plan(
        &[1, 2, 2, 2, 3],
        &[2, 2, 3, 4],
        ultrasql_planner::LogicalSetOp::Intersect,
        ultrasql_planner::LogicalSetQuantifier::All,
    );
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    assert_eq!(drain_int_setop(op.as_mut()), vec![2, 2, 3]);
}

/// `SELECT v FROM l EXCEPT SELECT v FROM r` — distinct left rows
/// absent from right.
#[test]
fn lower_query_except_distinct_returns_left_minus_right() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let plan = build_int_set_op_plan(
        &[1, 2, 2, 3],
        &[2, 4],
        ultrasql_planner::LogicalSetOp::Except,
        ultrasql_planner::LogicalSetQuantifier::Distinct,
    );
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    assert_eq!(drain_int_setop(op.as_mut()), vec![1, 3]);
}

/// `SELECT v FROM l EXCEPT ALL SELECT v FROM r` — multiset
/// difference: subtract right counts from left counts.
#[test]
fn lower_query_except_all_subtracts_right_counts_from_left() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    // left: 1×{1}, 3×{2}, 1×{3}; right: 1×{2}, 1×{4}.
    // multiset diff: 1×{1}, 2×{2}, 1×{3} → [1, 2, 2, 3].
    let plan = build_int_set_op_plan(
        &[1, 2, 2, 2, 3],
        &[2, 4],
        ultrasql_planner::LogicalSetOp::Except,
        ultrasql_planner::LogicalSetQuantifier::All,
    );
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    assert_eq!(drain_int_setop(op.as_mut()), vec![1, 2, 2, 3]);
}

/// Hand-built `SetOp` plan whose two children have different arities
/// must be rejected by the lowerer with a precise `Unsupported`
/// error rather than panicking inside the kernel.
#[test]
fn lower_query_set_op_rejects_arity_mismatch() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    // Left has 1 column, right has 2.
    let left_schema = schema_int_col("v");
    let right_schema = Schema::new([
        Field::required("a", DataType::Int32),
        Field::required("b", DataType::Int32),
    ])
    .expect("two-col schema");
    let left_plan = LogicalPlan::Values {
        rows: vec![int_row(1)],
        schema: left_schema.clone(),
    };
    let right_plan = LogicalPlan::Values {
        rows: vec![vec![lit_i32(1), lit_i32(2)]],
        schema: right_schema,
    };
    let plan = LogicalPlan::SetOp {
        op: ultrasql_planner::LogicalSetOp::Union,
        quantifier: ultrasql_planner::LogicalSetQuantifier::All,
        left: Box::new(left_plan),
        right: Box::new(right_plan),
        schema: left_schema,
    };
    let err = lower_query(&plan, &ctx).expect_err("must reject arity mismatch");
    assert!(matches!(err, ServerError::Unsupported(_)));
}

/// The sample-table lowerer accepts `SetOp` too — keep both lowering
/// paths bit-identical in dispatch semantics. We use a parsed SQL
/// `SELECT id FROM users UNION ALL SELECT id FROM users` plan over
/// the sample fixture so the test exercises the binder, the lowerer,
/// and the kernel together.
#[test]
fn lower_plan_union_all_via_sample_path() {
    let (catalog, tables) = fixture();
    let p = plan(
        "SELECT id FROM users UNION ALL SELECT id FROM users",
        &catalog,
    );
    let mut op = lower_plan(&p, &tables).expect("lowers");
    let mut ids: Vec<i32> = Vec::new();
    while let Some(batch) = op.next_batch().expect("ok") {
        if let ultrasql_vec::column::Column::Int32(col) = &batch.columns()[0] {
            ids.extend_from_slice(col.data());
        }
    }
    ids.sort_unstable();
    // The fixture has ids = [1, 2, 3]; UNION ALL of two copies =
    // [1, 1, 2, 2, 3, 3] (sorted for stable comparison).
    assert_eq!(ids, vec![1, 1, 2, 2, 3, 3]);
}

// ----------------------------------------------------------------
// IndexScan dispatch (Wave A item A5)
// ----------------------------------------------------------------

use std::sync::Arc as StdArc;

use ultrasql_catalog::{MutableCatalog, PersistentCatalog};
use ultrasql_executor::{ExecError, RowCodec};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::BufferPool;
use ultrasql_txn::TransactionManager;

/// Fixture for `IndexScan` tests: a populated persistent catalog,
/// a heap with rows, and (optionally) a B-tree index registered
/// against the catalog. The catalog snapshot is rebuilt after the
/// index is registered so a subsequent `LowerCtx::catalog_snapshot`
/// observation sees it.
pub(super) struct IndexFixture {
    pub(super) catalog: StdArc<PersistentCatalog>,
    pub(super) heap: StdArc<HeapAccess<BlankPageLoader>>,
    vm: StdArc<ultrasql_storage::vm::VisibilityMap>,
    pub(super) txn_manager: StdArc<TransactionManager>,
    /// XID under which the rows were inserted (committed before the
    /// fixture is handed out).
    loader_xid: Xid,
    /// Snapshot captured *after* the loader transaction committed,
    /// so `is_visible` returns `Visible` for every fixture row.
    reader_snapshot: ultrasql_mvcc::Snapshot,
}

/// Construct a fresh fixture and load `rows` of
/// `(id INT NOT NULL, val INT NOT NULL)` data, registering an
/// (optionally-present) B-tree index over `id`.
pub(super) fn build_index_fixture(
    table_name: &str,
    rows: &[(i32, i32)],
    with_index: bool,
) -> (IndexFixture, ultrasql_catalog::TableEntry, Vec<TupleId>) {
    let catalog = StdArc::new(PersistentCatalog::new());
    let pool = StdArc::new(BufferPool::new(64, BlankPageLoader::new()));
    let heap = StdArc::new(HeapAccess::new(StdArc::clone(&pool)));
    let vm = StdArc::new(ultrasql_storage::vm::VisibilityMap::new());
    let txn_manager = StdArc::new(TransactionManager::new());

    // Create the table in the catalog under a fresh OID.
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("val", DataType::Int32),
    ])
    .expect("schema ok");
    let oid = catalog.next_oid();
    let entry = ultrasql_catalog::TableEntry::new(oid, table_name, "public", schema.clone());
    catalog.create_table(entry.clone()).expect("create table");

    // Load rows under a single autocommit-style transaction. The
    // schema is moved into the codec here — no later use.
    let txn = txn_manager.begin(ultrasql_txn::IsolationLevel::ReadCommitted);
    let codec = RowCodec::new(schema);
    let rel = RelationId(oid);
    let mut tids: Vec<TupleId> = Vec::with_capacity(rows.len());
    for (id, val) in rows {
        let payload = codec
            .encode(&[Value::Int32(*id), Value::Int32(*val)])
            .expect("encode row");
        let opts = InsertOptions {
            xmin: txn.xid,
            command_id: CommandId::FIRST,
            wal: None,
            fsm: None,
            vm: None,
        };
        let tid = heap.insert(rel, &payload, opts).expect("heap insert");
        tids.push(tid);
    }
    let loader_xid = txn.xid;
    txn_manager.commit(txn).expect("commit loader");

    // Build the B-tree index (if requested) using the same shape
    // `Server::execute_create_index` uses.
    if with_index {
        let index_oid = catalog.next_oid();
        let index_rel = RelationId::new(index_oid.raw());
        let mut btree = BTree::create(StdArc::clone(&pool), index_rel).expect("btree create");
        let root_block = btree.root_block();
        for (i, (id, _val)) in rows.iter().enumerate() {
            let key: i64 = i64::from(*id);
            btree
                .insert::<i64>(key, tids[i], loader_xid, None)
                .expect("btree insert");
        }
        let mut idx_entry =
            ultrasql_catalog::IndexEntry::new(index_oid, "ix_id", oid, vec![0_u16], false);
        idx_entry.root_block = root_block;
        catalog.create_index(idx_entry).expect("index register");
    }

    // Snapshot *after* the loader commits so visibility sees the rows.
    let reader_txn = txn_manager.begin(ultrasql_txn::IsolationLevel::ReadCommitted);
    let reader_snapshot = reader_txn.snapshot.clone();
    txn_manager.commit(reader_txn).expect("commit reader-stub");

    (
        IndexFixture {
            catalog,
            heap,
            vm,
            txn_manager,
            loader_xid,
            reader_snapshot,
        },
        entry,
        tids,
    )
}

impl IndexFixture {
    pub(super) fn mark_all_visible(&self, table: &ultrasql_catalog::TableEntry, tids: &[TupleId]) {
        let rel = RelationId(table.oid);
        for tid in tids {
            self.heap
                .vacuum_set_all_visible(rel, tid.page.block, &self.vm);
        }
    }

    pub(super) fn ctx<'a>(&'a self, tables: &'a SampleTables) -> LowerCtx<'a> {
        LowerCtx {
            tables,
            catalog_snapshot: self.catalog.snapshot(),
            table_constraints: StdArc::new(dashmap::DashMap::new()),
            sequences: StdArc::new(dashmap::DashMap::new()),
            persistent_catalog: StdArc::new(ultrasql_catalog::persistent::PersistentCatalog::new()),
            time_partitions: StdArc::new(dashmap::DashMap::new()),
            workload_recorder: StdArc::new(crate::workload::WorkloadRecorder::new()),
            autovacuum_config: crate::AutovacuumConfig::default(),
            logging_config: crate::LoggingConfig::default(),
            data_dir: None,
            logical_replication: Arc::new(crate::replication::LogicalReplicationRuntime::new()),
            sequence_state: None,
            advisory_state: None,
            heap: StdArc::clone(&self.heap),
            vm: StdArc::clone(&self.vm),
            snapshot: self.reader_snapshot.clone(),
            oracle: StdArc::clone(&self.txn_manager),
            xid: self.loader_xid,
            command_id: CommandId::FIRST,
            cte_buffers: HashMap::new(),
            jit: ultrasql_vec::jit::JitConfig::OFF,
            cancel_flag: None,
            work_mem: std::sync::Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(
                u64::MAX,
            )),
            profile_operators: false,
        }
    }
}

/// Build a `Filter { Scan(table), predicate }` plan over `table_name`
/// with the canonical `(id INT, val INT)` schema.
pub(super) fn build_filter_scan_plan(table_name: &str, predicate: ScalarExpr) -> LogicalPlan {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("val", DataType::Int32),
    ])
    .expect("schema ok");
    LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan {
            table: table_name.into(),
            schema,
            projection: None,
        }),
        predicate,
    }
}

/// Build `id = lit` over the canonical fixture schema. `id` is
/// column index 0 with `Int32` type.
pub(super) fn eq_id_literal(v: i32) -> ScalarExpr {
    ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(ScalarExpr::Column {
            name: "id".into(),
            index: 0,
            data_type: DataType::Int32,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }),
        data_type: DataType::Bool,
    }
}

/// Build `id BETWEEN lo AND hi` as the binder would: rewrites into
/// `id >= lo AND id <= hi`.
pub(super) fn between_id_literal(lo: i32, hi: i32) -> ScalarExpr {
    let id_col = || ScalarExpr::Column {
        name: "id".into(),
        index: 0,
        data_type: DataType::Int32,
    };
    let lit = |v: i32| ScalarExpr::Literal {
        value: Value::Int32(v),
        data_type: DataType::Int32,
    };
    ScalarExpr::Binary {
        op: BinaryOp::And,
        left: Box::new(ScalarExpr::Binary {
            op: BinaryOp::GtEq,
            left: Box::new(id_col()),
            right: Box::new(lit(lo)),
            data_type: DataType::Bool,
        }),
        right: Box::new(ScalarExpr::Binary {
            op: BinaryOp::LtEq,
            left: Box::new(id_col()),
            right: Box::new(lit(hi)),
            data_type: DataType::Bool,
        }),
        data_type: DataType::Bool,
    }
}

/// Drain a (id INT, val INT) operator and return the row pairs.
pub(super) fn drain_id_val(op: &mut dyn Operator) -> Result<Vec<(i32, i32)>, ExecError> {
    let mut out = Vec::new();
    while let Some(b) = op.next_batch()? {
        match (&b.columns()[0], &b.columns()[1]) {
            (
                ultrasql_vec::column::Column::Int32(ids),
                ultrasql_vec::column::Column::Int32(vals),
            ) => {
                for (i, v) in ids.data().iter().zip(vals.data().iter()) {
                    out.push((*i, *v));
                }
            }
            _ => panic!("unexpected column layout"),
        }
    }
    Ok(out)
}
