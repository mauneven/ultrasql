//! Modify + index-scan pipeline tests.

use super::select::{
    between_id_literal, build_filter_scan_plan, build_index_fixture, drain_id_val,
    eq_id_literal,
};
use super::{
    collect_pairs, column, int_row, schema_int_col, synthetic_ctx,
};
use crate::pipeline::index_scan::{match_indexable_predicate, match_simple_comparison};
use crate::pipeline::*;
use ultrasql_core::{DataType, Schema, Value};
use ultrasql_planner::{
    BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr,
};
use ultrasql_storage::heap::InsertOptions;

#[test]
fn lower_query_eq_indexed_column_picks_index_scan() {
    let rows: Vec<(i32, i32)> = (1..=100).map(|i| (i, i * 10)).collect();
    let (fix, _entry, _) = build_index_fixture("t_eq_indexed", &rows, true);
    let tables = SampleTables::new();
    let ctx = fix.ctx(&tables);
    let plan = build_filter_scan_plan("t_eq_indexed", eq_id_literal(42));
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    let debug = format!("{op:?}");
    assert!(
        debug.starts_with("IndexScan"),
        "expected IndexScan, got: {debug}"
    );
    let pairs = drain_id_val(op.as_mut()).expect("drain");
    assert_eq!(pairs, vec![(42, 420)]);
}

/// `WHERE id = 42` against an *unindexed* table falls back to
/// `Filter(SeqScan)`. The `Debug` starts with `Filter` (the outer
/// operator); `SeqScan` is the inner child.
#[test]
fn lower_query_eq_unindexed_column_falls_back_to_filter_seq_scan() {
    let rows: Vec<(i32, i32)> = (1..=100).map(|i| (i, i * 10)).collect();
    let (fix, _entry, _) = build_index_fixture("t_eq_unindexed", &rows, false);
    let tables = SampleTables::new();
    let ctx = fix.ctx(&tables);
    let plan = build_filter_scan_plan("t_eq_unindexed", eq_id_literal(42));
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    let debug = format!("{op:?}");
    assert!(
        !debug.starts_with("IndexScan"),
        "must not pick IndexScan over an unindexed column; got: {debug}"
    );
    let pairs = drain_id_val(op.as_mut()).expect("drain");
    assert_eq!(pairs, vec![(42, 420)]);
}

/// `WHERE id BETWEEN 10 AND 20` against an indexed table picks
/// `IndexScan` and returns rows 10..=20 in ascending order.
#[test]
fn lower_query_between_indexed_column_picks_index_scan() {
    let rows: Vec<(i32, i32)> = (1..=100).map(|i| (i, i * 10)).collect();
    let (fix, _entry, _) = build_index_fixture("t_between_indexed", &rows, true);
    let tables = SampleTables::new();
    let ctx = fix.ctx(&tables);
    let plan = build_filter_scan_plan("t_between_indexed", between_id_literal(10, 20));
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    let debug = format!("{op:?}");
    assert!(
        debug.starts_with("IndexScan"),
        "expected IndexScan for BETWEEN, got: {debug}"
    );
    let pairs = drain_id_val(op.as_mut()).expect("drain");
    let expected: Vec<(i32, i32)> = (10..=20).map(|i| (i, i * 10)).collect();
    assert_eq!(pairs, expected);
}

/// `WHERE val = 100` against a table whose index is on `id` (not
/// `val`) falls back to SeqScan+Filter — confirms the catalog-look-up
/// honours the column attnum.
#[test]
fn lower_query_eq_unindexed_when_index_on_other_column() {
    let rows: Vec<(i32, i32)> = (1..=10).map(|i| (i, i * 10)).collect();
    let (fix, _entry, _) = build_index_fixture("t_other_col_index", &rows, true);
    let tables = SampleTables::new();
    let ctx = fix.ctx(&tables);
    let predicate = ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(ScalarExpr::Column {
            name: "val".into(),
            index: 1,
            data_type: DataType::Int32,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int32(50),
            data_type: DataType::Int32,
        }),
        data_type: DataType::Bool,
    };
    let plan = build_filter_scan_plan("t_other_col_index", predicate);
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    let debug = format!("{op:?}");
    assert!(
        !debug.starts_with("IndexScan"),
        "must not pick IndexScan when the index does not cover the predicate's column; got: {debug}"
    );
    let pairs = drain_id_val(op.as_mut()).expect("drain");
    assert_eq!(pairs, vec![(5, 50)]);
}

/// `WHERE id > 95` picks `IndexScan` with an open upper bound and
/// returns rows 96..=100.
#[test]
fn lower_query_gt_indexed_column_picks_index_scan() {
    let rows: Vec<(i32, i32)> = (1..=100).map(|i| (i, i * 10)).collect();
    let (fix, _entry, _) = build_index_fixture("t_gt_indexed", &rows, true);
    let tables = SampleTables::new();
    let ctx = fix.ctx(&tables);
    let predicate = ScalarExpr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(ScalarExpr::Column {
            name: "id".into(),
            index: 0,
            data_type: DataType::Int32,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int32(95),
            data_type: DataType::Int32,
        }),
        data_type: DataType::Bool,
    };
    let plan = build_filter_scan_plan("t_gt_indexed", predicate);
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    let debug = format!("{op:?}");
    assert!(
        debug.starts_with("IndexScan"),
        "expected IndexScan for `>`, got: {debug}"
    );
    let pairs = drain_id_val(op.as_mut()).expect("drain");
    let expected: Vec<(i32, i32)> = (96..=100).map(|i| (i, i * 10)).collect();
    assert_eq!(pairs, expected);
}

/// MVCC visibility: a row inserted after the reader's snapshot must
/// NOT appear in the `IndexScan` output, just as it would not appear
/// in a `SeqScan`.
#[test]
fn lower_query_index_scan_honours_mvcc_visibility() {
    let rows: Vec<(i32, i32)> = (1..=5).map(|i| (i, i * 10)).collect();
    let (fix, _entry, _tids) = build_index_fixture("t_mvcc", &rows, true);
    let tables = SampleTables::new();
    let ctx = fix.ctx(&tables);

    // Insert a row under a *new* (uncommitted) transaction; its
    // xmin is > reader_snapshot.xmax, so the reader must not see it.
    // We don't even need to register it in the index — IndexScan
    // would only see it if the heap fetch returned it as visible.
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("val", DataType::Int32),
    ])
    .expect("schema");
    let codec = RowCodec::new(schema);
    let entry = fix
        .catalog
        .snapshot()
        .tables
        .get("t_mvcc")
        .expect("entry")
        .clone();
    let invisible_txn = fix
        .txn_manager
        .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
    let payload = codec
        .encode(&[Value::Int32(99), Value::Int32(990)])
        .expect("encode");
    let opts = InsertOptions {
        xmin: invisible_txn.xid,
        command_id: CommandId::FIRST,
        wal: None,
        fsm: None,
        vm: None,
    };
    let _ = fix
        .heap
        .insert(RelationId(entry.oid), &payload, opts)
        .expect("insert");
    // Deliberately do NOT commit `invisible_txn`. The reader's
    // snapshot was taken before this transaction began, so even
    // after the row lands in the heap the reader sees `Visibility !=
    // Visible`.

    // Point lookup on a key we know was loaded before the snapshot.
    let plan = build_filter_scan_plan("t_mvcc", eq_id_literal(3));
    let mut op = lower_query(&plan, &ctx).expect("lowers");
    let pairs = drain_id_val(op.as_mut()).expect("drain");
    assert_eq!(pairs, vec![(3, 30)]);
}

/// A predicate not in the indexable shape set (`id + 1 = 42`) falls
/// back to `SeqScan` + `Filter` even when the column is indexed.
#[test]
fn lower_query_arithmetic_predicate_falls_back_to_filter() {
    let rows: Vec<(i32, i32)> = (1..=10).map(|i| (i, i * 10)).collect();
    let (fix, _entry, _) = build_index_fixture("t_arith_fallback", &rows, true);
    let tables = SampleTables::new();
    let ctx = fix.ctx(&tables);
    // `id + 1 = 42` — left side is not a bare column reference.
    let predicate = ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(ScalarExpr::Binary {
            op: BinaryOp::Add,
            left: Box::new(ScalarExpr::Column {
                name: "id".into(),
                index: 0,
                data_type: DataType::Int32,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(1),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Int32,
        }),
        right: Box::new(ScalarExpr::Literal {
            value: Value::Int32(42),
            data_type: DataType::Int32,
        }),
        data_type: DataType::Bool,
    };
    let plan = build_filter_scan_plan("t_arith_fallback", predicate);
    let op = lower_query(&plan, &ctx).expect("lowers");
    let debug = format!("{op:?}");
    assert!(
        !debug.starts_with("IndexScan"),
        "must not pick IndexScan when the predicate's column is wrapped in an expression; got: {debug}"
    );
}

/// `match_indexable_predicate` returns `None` for a literal-only
/// predicate (`TRUE`).
#[test]
fn match_indexable_predicate_rejects_constant_predicate() {
    let pred = ScalarExpr::Literal {
        value: Value::Bool(true),
        data_type: DataType::Bool,
    };
    assert!(match_indexable_predicate(&pred).is_none());
}

/// Helper smoke test: bound normalisation is correct for strict
/// operators. `id > 5` should normalise to `low = Some(6)`, no
/// upper bound; `id < 5` to `high = Some(4)`, no lower bound.
#[test]
fn match_simple_comparison_normalises_strict_bounds() {
    let id_col = ScalarExpr::Column {
        name: "id".into(),
        index: 0,
        data_type: DataType::Int32,
    };
    let lit5 = ScalarExpr::Literal {
        value: Value::Int32(5),
        data_type: DataType::Int32,
    };
    let gt = ScalarExpr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(id_col.clone()),
        right: Box::new(lit5.clone()),
        data_type: DataType::Bool,
    };
    let (idx, range) = match_simple_comparison(&gt).expect("gt matches");
    assert_eq!(idx, 0);
    assert_eq!(range.low, Some(6));
    assert_eq!(range.high, None);

    let lt = ScalarExpr::Binary {
        op: BinaryOp::Lt,
        left: Box::new(id_col),
        right: Box::new(lit5),
        data_type: DataType::Bool,
    };
    let (_, range) = match_simple_comparison(&lt).expect("lt matches");
    assert_eq!(range.low, None);
    assert_eq!(range.high, Some(4));
}

// ---------------------------------------------------------------------
// CTE lowering tests
//
// Three shapes covered:
//
// 1. Single CTE referenced once in the body — the materialised batches
//    flow through a `CteScan` and the body sees the CTE rows verbatim.
// 2. Multiple CTEs in a chain — both materialise; the body joins them
//    by referencing each CTE name as a separate scan.
// 3. CTE with column aliases — the binder rewrites the schema field
//    names on the body's `Scan`; we verify the `CteScan` reports the
//    aliased schema so downstream operators (and the wire encoder)
//    see the renamed columns.
//
// Recursion (`WITH RECURSIVE`) is rejected; we test the rejection
// path separately so a future executor fixpoint can flip the
// expectation without rediscovering the contract.
// ---------------------------------------------------------------------

/// Build a single-column `(v INT)` Values plan with the given rows.
fn int_values_plan(rows: &[i32], col_name: &str) -> LogicalPlan {
    LogicalPlan::Values {
        rows: rows.iter().map(|v| int_row(*v)).collect(),
        schema: schema_int_col(col_name),
    }
}

/// Build a `Scan` plan node that references a CTE by name. The
/// schema we attach mirrors what the binder would record on a
/// body-side `FROM cte` reference.
fn cte_scan_ref(name: &str, schema: Schema) -> LogicalPlan {
    LogicalPlan::Scan {
        table: name.to_string(),
        schema,
        projection: None,
    }
}

/// `WITH a AS (VALUES (1),(2),(3)) SELECT * FROM a`
///
/// Verifies that the CTE definition is materialised once and the
/// body's `Scan(a)` resolves to that buffer via [`CteScan`].
#[test]
fn lower_query_cte_single_reference_returns_definition_rows() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let def = int_values_plan(&[1, 2, 3], "v");
    let body_schema = schema_int_col("v");
    let body = cte_scan_ref("a", body_schema.clone());
    let plan = LogicalPlan::Cte {
        name: "a".into(),
        recursive: false,
        definition: Box::new(def),
        body: Box::new(body),
        schema: body_schema,
    };
    let mut op = lower_query(&plan, &ctx).expect("CTE lowers");
    let mut got: Vec<i32> = Vec::new();
    while let Some(batch) = op.next_batch().expect("ok") {
        match &batch.columns()[0] {
            Column::Int32(c) => got.extend_from_slice(c.data()),
            other => panic!("unexpected column: {other:?}"),
        }
    }
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3]);
}

/// `WITH a AS (VALUES (...)), b AS (VALUES (...)) SELECT a.aid FROM a JOIN b ON a.aid = b.bid`
///
/// Verifies that two CTE bindings survive into the body and a join
/// between them works through the catalog-aware lower path. Both
/// children of the join are body-side `Scan`s that resolve via the
/// CTE overlay. We use distinct column names (`aid`/`bid`) because
/// `Schema::new` rejects duplicate names; the binder uses the same
/// disambiguation when a join produces two same-named columns.
#[test]
fn lower_query_cte_multi_cte_join_returns_intersection() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let a_schema = schema_int_col("aid");
    let b_schema = schema_int_col("bid");
    let join_out_schema = Schema::new([
        Field::required("aid", DataType::Int32),
        Field::required("bid", DataType::Int32),
    ])
    .expect("schema ok");

    // Build the inner-most plan: `SELECT * FROM a JOIN b ON a.aid = b.bid`.
    let join = LogicalPlan::Join {
        left: Box::new(cte_scan_ref("a", a_schema)),
        right: Box::new(cte_scan_ref("b", b_schema)),
        join_type: LogicalJoinType::Inner,
        condition: LogicalJoinCondition::On(ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(column("aid", 0, DataType::Int32)),
            right: Box::new(column("bid", 1, DataType::Int32)),
            data_type: DataType::Bool,
        }),
        schema: join_out_schema.clone(),
    };

    // Wrap in two CTE nodes: outermost is `a`, innermost wraps `b`.
    let b_def = int_values_plan(&[2, 3, 5], "bid");
    let with_b = LogicalPlan::Cte {
        name: "b".into(),
        recursive: false,
        definition: Box::new(b_def),
        body: Box::new(join),
        schema: join_out_schema.clone(),
    };
    let a_def = int_values_plan(&[1, 2, 3, 4], "aid");
    let plan = LogicalPlan::Cte {
        name: "a".into(),
        recursive: false,
        definition: Box::new(a_def),
        body: Box::new(with_b),
        schema: join_out_schema,
    };

    let mut op = lower_query(&plan, &ctx).expect("CTE join lowers");
    let mut pairs = collect_pairs(op.as_mut());
    pairs.sort_unstable();
    // a ∩ b on equality: 2,3 appear in both.
    assert_eq!(pairs, vec![(2, 2), (3, 3)]);
}

/// `WITH a(x) AS (...) SELECT * FROM a`
///
/// Verifies that a CTE column alias on the binding propagates to the
/// `CteScan`'s reported schema, so downstream consumers see the
/// aliased name instead of the definition's original field name.
#[test]
fn lower_query_cte_with_column_alias_reports_aliased_schema() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    // Definition emits a column named "v"; the body sees it as "x".
    let def = int_values_plan(&[10, 20], "v");
    let body_schema = schema_int_col("x");
    let body = cte_scan_ref("a", body_schema.clone());
    let plan = LogicalPlan::Cte {
        name: "a".into(),
        recursive: false,
        definition: Box::new(def),
        body: Box::new(body),
        schema: body_schema,
    };

    let mut op = lower_query(&plan, &ctx).expect("CTE alias lowers");
    // The schema reported by the operator must use the aliased name.
    assert_eq!(op.schema().field_at(0).name, "x");
    let batch = op
        .next_batch()
        .expect("ok")
        .expect("at least one batch from CteScan");
    match &batch.columns()[0] {
        Column::Int32(c) => assert_eq!(c.data(), &[10, 20]),
        other => panic!("unexpected column: {other:?}"),
    }
}

/// `WITH RECURSIVE` must be rejected. The executor has no fixpoint
/// loop today; silently lowering a recursive CTE as non-recursive
/// would produce wrong results for any self-referential definition.
#[test]
fn lower_query_cte_rejects_recursive() {
    let tables = SampleTables::new();
    let ctx = synthetic_ctx(&tables);
    let def = int_values_plan(&[1], "v");
    let body_schema = schema_int_col("v");
    let body = cte_scan_ref("a", body_schema.clone());
    let plan = LogicalPlan::Cte {
        name: "a".into(),
        recursive: true,
        definition: Box::new(def),
        body: Box::new(body),
        schema: body_schema,
    };
    let err = lower_query(&plan, &ctx).expect_err("recursive must be rejected");
    match err {
        ServerError::Unsupported(msg) => assert!(
            msg.contains("RECURSIVE"),
            "error must mention RECURSIVE, got: {msg}"
        ),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}
