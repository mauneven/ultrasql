//! Join lowering: hash-join when equi-keys are discoverable, else NLJ.

use std::sync::Arc;

use ultrasql_core::{DataType, Schema};
use ultrasql_executor::{
    ExecError, HashJoin, MemTableScan, MergeJoin, NestedLoopJoin, Operator, Project, RightFactory,
    WorkMemBudget,
    join_layout::{concat_join_exec_schema, using_projection_indices},
};
use ultrasql_planner::{
    BinaryOp, LogicalJoinCondition, LogicalJoinType, LogicalPlan, LogicalSetQuantifier, ScalarExpr,
};
use ultrasql_vec::Batch;

use crate::error::ServerError;

use super::LowerCtx;
use super::lower_query::lower_query;

type JoinKeyPair = (ScalarExpr, ScalarExpr);
type HashJoinKeysWithResidual = (Vec<JoinKeyPair>, Option<ScalarExpr>);

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
///   accepts today — `Cross`, `Semi`, and `Anti` are handled by
///   hash/NL lowering; `RightOuter`/`FullOuter` need follow-up coverage).
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

pub(super) struct LowerJoinArgs<'a> {
    pub(super) left_plan: &'a LogicalPlan,
    pub(super) right_plan: &'a LogicalPlan,
    pub(super) left: Box<dyn Operator>,
    pub(super) right: Box<dyn Operator>,
    pub(super) left_schema: Schema,
    pub(super) right_schema: Schema,
    pub(super) join_type: LogicalJoinType,
    pub(super) condition: &'a LogicalJoinCondition,
    pub(super) out_schema: Schema,
    pub(super) work_mem: Option<Arc<WorkMemBudget>>,
}

pub(super) fn lower_join(args: LowerJoinArgs<'_>) -> Result<Box<dyn Operator>, ServerError> {
    let LowerJoinArgs {
        left_plan,
        right_plan,
        left,
        right,
        left_schema,
        right_schema,
        join_type,
        condition,
        out_schema,
        work_mem,
    } = args;
    match condition {
        LogicalJoinCondition::On(pred) => {
            if matches!(
                join_type,
                LogicalJoinType::Inner
                    | LogicalJoinType::LeftOuter
                    | LogicalJoinType::Semi
                    | LogicalJoinType::Anti
            ) {
                if let Some((key_pairs, residual)) =
                    extract_hash_friendly_equi_key_pairs_with_residual(pred, left_schema.len())
                {
                    let (left_keys, right_keys): (Vec<_>, Vec<_>) = key_pairs.into_iter().unzip();
                    if join_type == LogicalJoinType::Inner
                        && residual.is_none()
                        && should_build_inner_hash_join_on_right(&*left, &*right)
                    {
                        return build_swapped_inner_hash_join(SwappedInnerHashJoinArgs {
                            left,
                            right,
                            left_keys,
                            right_keys,
                            out_schema,
                            left_schema,
                            right_schema,
                            work_mem,
                        });
                    }
                    if matches!(join_type, LogicalJoinType::Semi | LogicalJoinType::Anti)
                        && should_build_semi_anti_hash_join_on_right(
                            left_plan, right_plan, &*left, &*right,
                        )
                    {
                        let join = HashJoin::new_multi_with_residual_build_right(
                            left,
                            right,
                            left_keys,
                            right_keys,
                            residual,
                            join_type,
                            out_schema,
                            left_schema,
                            right_schema,
                        );
                        return Ok(Box::new(attach_work_mem(join, &work_mem)));
                    }
                    // HashJoin: left = build, right = probe. See the
                    // function docs for the rationale.
                    let join = HashJoin::new_multi_with_residual(
                        left,
                        right,
                        left_keys,
                        right_keys,
                        residual,
                        join_type,
                        out_schema,
                        left_schema,
                        right_schema,
                    );
                    return Ok(Box::new(attach_work_mem(join, &work_mem)));
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
            let projection = using_projection_indices(pairs, left_schema.len(), right_schema.len());
            let exec_schema = concat_join_exec_schema(&left_schema, &right_schema, join_type)
                .map_err(|err| {
                    ServerError::Execute(ExecError::TypeMismatch(format!("join schema: {err}")))
                })?;
            let joined = build_nested_loop_join(
                left,
                right,
                cond,
                join_type,
                exec_schema,
                left_schema,
                right_schema,
            )?;
            Ok(Box::new(Project::with_schema(
                joined, projection, out_schema,
            )?))
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

fn should_build_inner_hash_join_on_right(left: &dyn Operator, right: &dyn Operator) -> bool {
    match (left.estimated_row_count(), right.estimated_row_count()) {
        (Some(left_rows), Some(right_rows)) => right_rows < left_rows,
        (None, Some(_)) => true,
        _ => false,
    }
}

fn should_build_semi_anti_hash_join_on_right(
    left_plan: &LogicalPlan,
    right_plan: &LogicalPlan,
    left: &dyn Operator,
    right: &dyn Operator,
) -> bool {
    match (left.estimated_row_count(), right.estimated_row_count()) {
        (Some(left_rows), Some(right_rows)) => right_rows < left_rows,
        (None, Some(_)) => true,
        (Some(_), None) | (None, None) => {
            logical_plan_looks_compact_for_semi_anti_build(right_plan)
                && !logical_plan_looks_compact_for_semi_anti_build(left_plan)
        }
    }
}

fn logical_plan_looks_compact_for_semi_anti_build(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::LockRows { input, .. } => {
            logical_plan_looks_compact_for_semi_anti_build(input)
        }
        LogicalPlan::Aggregate { group_by, .. } => !group_by.is_empty(),
        LogicalPlan::SetOp { quantifier, .. } => *quantifier == LogicalSetQuantifier::Distinct,
        LogicalPlan::Values { .. } | LogicalPlan::Empty { .. } => true,
        _ => false,
    }
}

struct SwappedInnerHashJoinArgs {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    left_keys: Vec<ScalarExpr>,
    right_keys: Vec<ScalarExpr>,
    out_schema: Schema,
    left_schema: Schema,
    right_schema: Schema,
    work_mem: Option<Arc<WorkMemBudget>>,
}

fn build_swapped_inner_hash_join(
    args: SwappedInnerHashJoinArgs,
) -> Result<Box<dyn Operator>, ServerError> {
    let SwappedInnerHashJoinArgs {
        left,
        right,
        left_keys,
        right_keys,
        out_schema,
        left_schema,
        right_schema,
        work_mem,
    } = args;
    let left_width = left_schema.len();
    let right_width = right_schema.len();
    let swapped_schema = concat_schemas(&right_schema, &left_schema)?;
    let join = HashJoin::new_multi(
        right,
        left,
        right_keys,
        left_keys,
        LogicalJoinType::Inner,
        swapped_schema,
        right_schema,
        left_schema,
    );
    let join = Box::new(attach_work_mem(join, &work_mem));

    let mut indices = Vec::with_capacity(left_width + right_width);
    indices.extend(right_width..(right_width + left_width));
    indices.extend(0..right_width);

    Ok(Box::new(Project::with_schema(join, indices, out_schema)?))
}

fn attach_work_mem(join: HashJoin, work_mem: &Option<Arc<WorkMemBudget>>) -> HashJoin {
    if let Some(budget) = work_mem {
        join.with_work_mem_budget(Arc::clone(budget))
    } else {
        join
    }
}

fn concat_schemas(left: &Schema, right: &Schema) -> Result<Schema, ServerError> {
    let mut fields = Vec::with_capacity(left.len() + right.len());
    let left_names: std::collections::HashSet<String> = left
        .fields()
        .iter()
        .map(|field| field.name.to_ascii_lowercase())
        .collect();
    for idx in 0..left.len() {
        fields.push(left.field_at(idx).clone());
    }
    for idx in 0..right.len() {
        let field = right.field_at(idx);
        let name = if left_names.contains(&field.name.to_ascii_lowercase()) {
            format!("{}_1", field.name)
        } else {
            field.name.clone()
        };
        fields.push(ultrasql_core::Field {
            name,
            data_type: field.data_type.clone(),
            nullable: field.nullable,
        });
    }
    Schema::new(fields)
        .map_err(|err| ServerError::Execute(ExecError::TypeMismatch(format!("join schema: {err}"))))
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
            | DataType::Char { .. }
            | DataType::Bytea
            | DataType::Date
            | DataType::Time
            | DataType::TimeTz
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

/// Recognise hash-friendly equi-key predicates and retain any residual
/// conjuncts that must be evaluated after hash lookup.
pub(super) fn extract_hash_friendly_equi_key_pairs_with_residual(
    pred: &ScalarExpr,
    left_width: usize,
) -> Option<HashJoinKeysWithResidual> {
    let conjuncts = split_and(pred);
    let mut pairs = Vec::new();
    let mut residuals = Vec::new();
    for conjunct in conjuncts {
        if let Some(pair) = extract_hash_friendly_equi_keys(&conjunct, left_width) {
            pairs.push(pair);
        } else {
            residuals.push(conjunct);
        }
    }
    if pairs.is_empty() {
        None
    } else {
        Some((pairs, conjuncts_to_and_opt(residuals)))
    }
}

fn split_and(expr: &ScalarExpr) -> Vec<ScalarExpr> {
    if let ScalarExpr::Binary {
        op: BinaryOp::And,
        left,
        right,
        ..
    } = expr
    {
        let mut out = split_and(left);
        out.extend(split_and(right));
        return out;
    }
    vec![expr.clone()]
}

fn conjuncts_to_and_opt(mut predicates: Vec<ScalarExpr>) -> Option<ScalarExpr> {
    if predicates.is_empty() {
        return None;
    }
    let mut result = predicates.remove(0);
    for predicate in predicates {
        result = ScalarExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(result),
            right: Box::new(predicate),
            data_type: DataType::Bool,
        };
    }
    Some(result)
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
