//! Join lowering: hash-join when equi-keys are discoverable, else NLJ.

use std::collections::HashMap;
use std::sync::Arc;

use ultrasql_catalog::{CatalogSnapshot, IndexEntry, TableEntry};
use ultrasql_core::{CommandId, DataType, Field, RelationId, Schema, Value, Xid};
use ultrasql_executor::filter_sum_op::{
    CachedAvgI32Scan, CachedFilterSumI32Scan, CachedSumI32Scan, FilterSumI32Scan,
};
use ultrasql_executor::fused_delete::FusedDeleteInt32Pair;
use ultrasql_executor::fused_update::{FusedCmp, FusedPredicate, FusedUpdateInt32Add};
use ultrasql_executor::physical::{BuildError, DataSource};
use ultrasql_executor::{
    CteScan, Filter, FilterEqI32, HashAggregate, HashJoin, IndexScan, Limit, MemTableScan,
    MergeJoin, ModifyKind, ModifyTable, NestedLoopJoin, Operator, Project, ResultOp, RightFactory,
    RowCodec, SeqScan, SetOp, Sort, ValuesScan,
};
use ultrasql_mvcc::{Snapshot, Visibility, is_visible};
use ultrasql_planner::{
    BinaryOp, InMemoryCatalog, LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr,
    TableMeta,
};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_txn::TransactionManager;
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

use crate::BlankPageLoader;
use crate::error::ServerError;

use super::LowerCtx;
use super::lower_query::lower_query;

/// Attempt to lower an `Inner`/`LeftOuter` `Join` whose children are both
/// [`LogicalPlan::Sort`] over keys that align with the equi-key predicate
/// to a [`MergeJoin`].
///
/// Returns `Ok(Some(op))` when the merge-join shape is recognised — the
/// Sort wrappers are skipped and the inner plans are lowered as the
/// merge inputs (no re-sort). Returns `Ok(None)` when the shape does
/// not match, leaving the caller to fall back to the hash/NL join
/// dispatcher.
///
/// Match rules (all must hold):
/// - `condition` is `On(pred)` with an equi `Column = Column` predicate
///   extractable by [`extract_hash_friendly_equi_keys`].
/// - `join_type` is `Inner` or `LeftOuter` (the kinds MergeJoin
///   accepts today — `Cross` is explicitly rejected by the executor and
///   `RightOuter`/`FullOuter`/`Semi`/`Anti` need follow-up coverage).
/// - Both children are `LogicalPlan::Sort { input, keys }` with exactly
///   one ascending key whose `ScalarExpr` matches the equi-key on the
///   corresponding side (column reference equality).
pub(super) fn try_lower_merge_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    join_type: LogicalJoinType,
    condition: &LogicalJoinCondition,
    out_schema: &Schema,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    if !matches!(
        join_type,
        LogicalJoinType::Inner | LogicalJoinType::LeftOuter
    ) {
        return Ok(None);
    }
    let LogicalJoinCondition::On(pred) = condition else {
        return Ok(None);
    };
    let left_schema_width = left.schema().len();
    let Some((left_key, right_key)) = extract_hash_friendly_equi_keys(pred, left_schema_width)
    else {
        return Ok(None);
    };

    let LogicalPlan::Sort {
        input: left_inner,
        keys: left_keys,
    } = left
    else {
        return Ok(None);
    };
    let LogicalPlan::Sort {
        input: right_inner,
        keys: right_keys,
    } = right
    else {
        return Ok(None);
    };
    if left_keys.len() != 1 || right_keys.len() != 1 {
        return Ok(None);
    }
    if !left_keys[0].asc || !right_keys[0].asc {
        return Ok(None);
    }
    if left_keys[0].expr != left_key || right_keys[0].expr != right_key {
        return Ok(None);
    }

    let left_inner_schema = left_inner.schema().clone();
    let right_inner_schema = right_inner.schema().clone();
    let left_op = lower_query(left_inner, ctx)?;
    let right_op = lower_query(right_inner, ctx)?;

    Ok(Some(Box::new(MergeJoin::new(
        left_op,
        right_op,
        left_key,
        right_key,
        join_type,
        out_schema.clone(),
        left_inner_schema,
        right_inner_schema,
    ))))
}

pub(super) fn lower_join(
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    left_schema: Schema,
    right_schema: Schema,
    join_type: LogicalJoinType,
    condition: &LogicalJoinCondition,
    out_schema: Schema,
) -> Result<Box<dyn Operator>, ServerError> {
    match condition {
        LogicalJoinCondition::On(pred) => {
            if matches!(
                join_type,
                LogicalJoinType::Inner | LogicalJoinType::LeftOuter
            ) {
                if let Some((left_key, right_key)) =
                    extract_hash_friendly_equi_keys(pred, left_schema.len())
                {
                    // HashJoin: left = build, right = probe. See the
                    // function docs for the rationale.
                    return Ok(Box::new(HashJoin::new(
                        left,
                        right,
                        left_key,
                        right_key,
                        join_type,
                        out_schema,
                        left_schema,
                        right_schema,
                    )));
                }
            }
            // Non-equi predicate, type-ineligible equi predicate, or an
            // outer-join kind the HashJoin does not yet support → NLJ.
            build_nested_loop_join(
                left,
                right,
                Some(pred.clone()),
                join_type,
                out_schema,
                left_schema,
                right_schema,
            )
        }
        LogicalJoinCondition::Using(pairs) => {
            let cond = build_using_predicate(pairs, &left_schema, &right_schema);
            build_nested_loop_join(
                left,
                right,
                cond,
                join_type,
                out_schema,
                left_schema,
                right_schema,
            )
        }
        LogicalJoinCondition::None => build_nested_loop_join(
            left,
            right,
            None,
            join_type,
            out_schema,
            left_schema,
            right_schema,
        ),
    }
}

/// Drain `right` into a memory-resident batch list, then wrap the
/// result in a [`NestedLoopJoin`] whose right factory replays the
/// drained batches.
///
/// The materialisation is necessary because [`NestedLoopJoin`] re-opens
/// the right side once per left row through its `RightFactory`
/// closure. A streaming right child cannot be replayed; spooling it
/// into batch storage gives the closure an O(1) `clone()` per
/// iteration. See `physical.rs::build_nlj` for the same approach.
///
/// # Errors
///
/// Returns a [`ServerError::Execute`] if the right child errors during

pub(super) fn build_nested_loop_join(
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    condition: Option<ScalarExpr>,
    join_type: LogicalJoinType,
    out_schema: Schema,
    left_schema: Schema,
    right_schema: Schema,
) -> Result<Box<dyn Operator>, ServerError> {
    // Spool the right side once so each left-row iteration cheaply
    // clones the batch list rather than re-running the upstream
    // pipeline (which might be a real heap scan over thousands of
    // blocks).
    let mut right_op = right;
    let mut batches: Vec<Batch> = Vec::new();
    while let Some(batch) = right_op.next_batch()? {
        batches.push(batch);
    }
    let shared: Arc<Vec<Batch>> = Arc::new(batches);
    let factory_schema = right_schema.clone();
    let factory: RightFactory = Box::new(move || {
        Ok(
            Box::new(MemTableScan::new(factory_schema.clone(), (*shared).clone()))
                as Box<dyn Operator>,
        )
    });
    Ok(Box::new(NestedLoopJoin::new(
        left,
        factory,
        join_type,
        condition,
        out_schema,
        left_schema,
        right_schema,
    )))
}

/// Return `true` if `dt` is a scalar type for which `Value::Hash` is
/// well-defined and `==` is reflexive (no NaN games).
///
/// Floats are excluded so a join key with `Float32::NAN` keeps NULL-like
/// semantics under SQL (NaN never equals NaN per IEEE-754, even though
/// the [`HashJoin`] hash impl currently hashes the bit pattern). Lifting
/// floats into `HashJoin` can land once the binder rewrites
/// `a.x = b.x` to `a.x = b.x AND a.x = a.x` for floats (or once we
/// formally specify the semantics).
const fn is_hash_friendly(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Bool
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Text { .. }
            | DataType::Bytea
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Uuid
    )
}

/// Recognise a binary-`Eq` predicate of the form `Column(left) = Column(right)`
/// (or its commuted form) where the left column lives in the left schema
/// and the right column lives in the right schema (i.e. its raw index is
/// ≥ `left_width`).
///
/// Returns the `(left_key, right_key)` expression pair, with the right
/// key's index *rebased* to be local to the right schema (subtracts
/// `left_width`). Returns `None` when:
///
/// - The top-level operator is not [`BinaryOp::Eq`].
/// - Either operand is not a bare column reference.
/// - Both columns live on the same side.
/// - The column data type is not [`is_hash_friendly`].
///
/// Mirrors `physical::extract_equi_keys` so the dispatcher in
/// [`lower_join`] picks the same operator the optimizer's builder
/// would. The type-friendliness gate is the addition: the builder
/// accepts any data type, but the server prefers a deterministic
/// fallback to NLJ for float keys until the binder's float-NULL rewrite

pub(super) fn extract_hash_friendly_equi_keys(
    pred: &ScalarExpr,
    left_width: usize,
) -> Option<(ScalarExpr, ScalarExpr)> {
    let ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = pred
    else {
        return None;
    };
    let (l_col, r_col) = match (left.as_ref(), right.as_ref()) {
        (
            ScalarExpr::Column {
                index: li,
                data_type: lt,
                name: ln,
            },
            ScalarExpr::Column {
                index: ri,
                data_type: rt,
                name: rn,
            },
        ) if *li < left_width && *ri >= left_width => {
            if !is_hash_friendly(lt) || !is_hash_friendly(rt) {
                return None;
            }
            (
                ScalarExpr::Column {
                    name: ln.clone(),
                    index: *li,
                    data_type: lt.clone(),
                },
                ScalarExpr::Column {
                    name: rn.clone(),
                    index: ri - left_width,
                    data_type: rt.clone(),
                },
            )
        }
        // Commuted form: right-side column is the *left* operand.
        (
            ScalarExpr::Column {
                index: li,
                data_type: lt,
                name: ln,
            },
            ScalarExpr::Column {
                index: ri,
                data_type: rt,
                name: rn,
            },
        ) if *li >= left_width && *ri < left_width => {
            if !is_hash_friendly(lt) || !is_hash_friendly(rt) {
                return None;
            }
            (
                ScalarExpr::Column {
                    name: rn.clone(),
                    index: *ri,
                    data_type: rt.clone(),
                },
                ScalarExpr::Column {
                    name: ln.clone(),
                    index: li - left_width,
                    data_type: lt.clone(),
                },
            )
        }
        _ => return None,
    };
    Some((l_col, r_col))
}

/// Build a composite equality predicate from `USING (left_idx, right_idx)`
/// pairs, AND-conjoining each `left_col = right_col` equality.
///
/// Right-side column indices are offset by `left_schema.len()` so the
/// predicate evaluates against the concatenated left++right row layout
/// the join produces. Returns `None` when `pairs` is empty (degenerate
/// USING clause).
///
/// Mirrors `physical::build_using_predicate`. Lives here so the
/// server-side lowerer is self-contained; converging on a single shared
/// helper lands when the server delegates to `physical::build_operator`
/// in v0.6 (see ROADMAP P0 "Server invokes optimizer").
pub(super) fn build_using_predicate(
    pairs: &[(usize, usize)],
    left_schema: &Schema,
    right_schema: &Schema,
) -> Option<ScalarExpr> {
    let mut iter = pairs.iter().map(|(li, ri)| {
        let lf = left_schema.field_at(*li);
        let rf = right_schema.field_at(*ri);
        ScalarExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(ScalarExpr::Column {
                index: *li,
                data_type: lf.data_type.clone(),
                name: lf.name.clone(),
            }),
            right: Box::new(ScalarExpr::Column {
                index: left_schema.len() + ri,
                data_type: rf.data_type.clone(),
                name: rf.name.clone(),
            }),
            data_type: DataType::Bool,
        }
    });
    let first = iter.next()?;
    Some(iter.fold(first, |acc, next| ScalarExpr::Binary {
        op: BinaryOp::And,
        left: Box::new(acc),
        right: Box::new(next),
        data_type: DataType::Bool,
    }))
}
