//! Physical plan builder.
//!
//! Lowers a [`LogicalPlan`] tree from the planner crate into a tree of
//! [`Operator`] trait objects the pull-mode executor can drive.
//!
//! # Lowering rules (v0.5)
//!
//! - `Scan` → [`MemTableScan`] + optional [`Project`].
//! - `Filter` → [`Filter`].
//! - `Project` (column-only) → [`Project`].
//! - `Limit` → [`Limit::with_offset`] (`LIMIT n` and `LIMIT n OFFSET m`).
//! - `Sort` → [`Sort`].
//! - `Aggregate` → [`HashAggregate`] (default) or `SortAggregate`
//!   when the hint field is set to `SortBased`.
//! - `SetOp` → [`SetOp`].
//! - `Cte` → materialise the definition, then serve via [`CteScan`].
//! - `Join` with `On` condition → [`HashJoin`] for `Inner`/`LeftOuter`,
//!   [`NestedLoopJoin`] otherwise.
//! - `Empty` → zero-batch [`MemTableScan`].
//! - `Values` → [`ValuesScan`].
//! - `Insert / Update / Delete / Truncate` → `Unsupported` (build directly).
//!
//! The data source is injected through [`DataSource`] rather than
//! resolved via the catalog so this layer stays decoupled from the
//! heap and buffer-pool stack that has not yet landed.

use std::sync::Arc;

use ultrasql_core::Schema;
use ultrasql_planner::{LogicalJoinCondition, LogicalJoinType, LogicalPlan, ScalarExpr};
use ultrasql_vec::Batch;

use crate::cte_scan::CteScan;
use crate::filter_op::Filter;
use crate::hash_aggregate::HashAggregate;
use crate::hash_join::HashJoin;
use crate::merge_join::MergeJoin;
use crate::nested_loop_join::{NestedLoopJoin, RightFactory};
use crate::set_op::SetOp;
use crate::sort::Sort;
use crate::values_scan::ValuesScan;
use crate::{Limit, MemTableScan, Operator, Project};

/// Pluggable backing store for [`build_operator`].
///
/// Implementations return the full schema and the materialised batches
/// for a single table. The trait is intentionally minimal: anything
/// richer (streaming, predicate pushdown, statistics) lives below the
/// storage layer the builder does not yet talk to. The trait is
/// `Send + Sync` so a single handle can be shared across worker
/// threads — the lowering itself is single-threaded but the operators
/// it builds may be moved between threads.
pub trait DataSource: Send + Sync {
    /// Materialise `table` to a `(Schema, Vec<Batch>)` pair.
    ///
    /// Returns [`BuildError::Source`] if the table is unknown or the
    /// implementation cannot serve the request.
    fn scan(&self, table: &str) -> Result<(Schema, Vec<Batch>), BuildError>;
}

/// Errors raised while lowering a [`LogicalPlan`] to an [`Operator`].
///
/// `Source` is a deliberate `String` (not a wrapped error) so the
/// builder stays decoupled from any specific catalog/storage error
/// surface; the caller is expected to log the underlying cause and
/// surface a protocol-friendly summary.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BuildError {
    /// The data source failed to satisfy a [`LogicalPlan::Scan`] —
    /// most commonly because the named table does not exist. The
    /// string is suitable for inclusion in a protocol error message.
    #[error("data source error: {0}")]
    Source(String),

    /// The plan requested a construct outside the v0.5 lowering set.
    /// The static string names the construct so the caller can map it
    /// to a stable SQLSTATE.
    #[error("not supported in v0.5: {0}")]
    Unsupported(&'static str),

    /// A bound expression's type was inconsistent with what the
    /// executor's runtime operators can consume. The string names
    /// the expectation; the binder should normally prevent these from
    /// reaching the builder.
    #[error("type error: {0}")]
    Type(String),

    /// An invariant inside the builder was violated — for example, a
    /// well-typed plan whose column index referenced an out-of-range
    /// schema position. The string literal names the invariant.
    #[error("internal invariant violation: {0}")]
    Internal(&'static str),
}

/// Convert an [`ExecError`] surfaced from an operator constructor
/// during lowering into a [`BuildError`].
///
/// Operator constructors validate their inputs (column-index ranges,
/// column-type expectations) and may report failures the binder
/// should already have caught. We translate those to
/// [`BuildError::Type`] so the surface stays uniform; truly
/// unexpected errors collapse to [`BuildError::Internal`].
fn map_exec_error(err: crate::ExecError) -> BuildError {
    match err {
        crate::ExecError::TypeMismatch(msg) => BuildError::Type(msg),
        crate::ExecError::Core(inner) => BuildError::Type(inner.to_string()),
        crate::ExecError::Internal(msg) => BuildError::Internal(msg),
        // Catch-all for variants the builder cannot reach in practice
        // (BatchTooLarge, Batch construction). They surface as Internal
        // so a bug is visible rather than swallowed.
        _ => BuildError::Internal("operator constructor failed during lowering"),
    }
}

/// Construct a physical operator tree from a bound logical plan.
///
/// The `data_source` callback materialises any table named by a
/// `LogicalPlan::Scan` node; the planner-side catalog is *not*
/// consulted here. Both arguments are borrowed for the duration of
/// the call — the returned operator owns the built batches.
///
/// # Errors
///
/// Returns [`BuildError`] on any of the conditions documented in the
/// module-level lowering rules.
#[allow(clippy::too_many_lines)]
pub fn build_operator(
    plan: &LogicalPlan,
    data_source: &dyn DataSource,
) -> Result<Box<dyn Operator>, BuildError> {
    match plan {
        LogicalPlan::Scan {
            table, projection, ..
        } => build_scan(table, projection.as_deref(), data_source),

        LogicalPlan::Filter { input, predicate } => {
            let child = build_operator(input, data_source)?;
            Ok(Box::new(Filter::new(child, predicate.clone())))
        }

        LogicalPlan::Project { input, exprs, .. } => {
            let child = build_operator(input, data_source)?;
            build_project(child, exprs)
        }

        LogicalPlan::Limit { input, n, offset } => {
            let child = build_operator(input, data_source)?;
            // Saturate both `n` and `offset` into `usize`. The binder
            // legitimately produces `u64::MAX` for `LIMIT NULL OFFSET m`
            // (i.e. "no upper bound"), and the executor's `Limit`
            // treats `usize::MAX` as that same sentinel. On 32-bit
            // targets a literal `LIMIT 5_000_000_000` saturates to
            // "all rows," which is the only sane outcome — we have no
            // batch big enough to hit the cap regardless.
            let limit = usize::try_from(*n).unwrap_or(usize::MAX);
            let offset = usize::try_from(*offset).unwrap_or(usize::MAX);
            Ok(Box::new(Limit::with_offset(child, limit, offset)))
        }

        LogicalPlan::Sort { input, keys } => {
            let child = build_operator(input, data_source)?;
            let schema = child.schema().clone();
            Ok(Box::new(Sort::new(child, keys.clone(), schema)))
        }

        LogicalPlan::Empty { schema } => {
            Ok(Box::new(MemTableScan::new(schema.clone(), Vec::new())))
        }

        LogicalPlan::Values { rows, schema } => {
            Ok(Box::new(ValuesScan::new(rows.clone(), schema.clone())))
        }

        LogicalPlan::Insert { .. } => Err(BuildError::Unsupported("INSERT not supported in v0.5")),
        LogicalPlan::Update { .. } => Err(BuildError::Unsupported("UPDATE not supported in v0.5")),
        LogicalPlan::Delete { .. } => Err(BuildError::Unsupported("DELETE not supported in v0.5")),
        LogicalPlan::Truncate { .. } => {
            Err(BuildError::Unsupported("TRUNCATE not supported in v0.5"))
        }
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. } => Err(BuildError::Unsupported(
            "DDL is dispatched outside the operator pipeline",
        )),

        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => build_join(left, right, *join_type, condition, schema, data_source),

        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => {
            // Default to HashAggregate. SortAggregate is chosen when the
            // planner eventually annotates the node with a sort-based hint;
            // for now we always emit HashAggregate.
            let child = build_operator(input, data_source)?;
            Ok(Box::new(HashAggregate::new(
                child,
                group_by.clone(),
                aggregates.clone(),
                schema.clone(),
            )))
        }

        LogicalPlan::SetOp {
            op,
            quantifier,
            left,
            right,
            schema,
        } => {
            let left_op = build_operator(left, data_source)?;
            let right_op = build_operator(right, data_source)?;
            Ok(Box::new(SetOp::new(
                left_op,
                right_op,
                *op,
                *quantifier,
                schema.clone(),
            )))
        }

        LogicalPlan::Cte {
            definition, body, ..
        } => {
            // Materialise the CTE definition eagerly into an Arc-shared buffer.
            // The body plan is lowered normally; Scan nodes that reference the
            // CTE by name will be resolved by the data_source in v0.6. When the
            // body is Empty we expose the CteScan so tests can exercise the
            // operator through the builder.
            let mut def_op = build_operator(definition, data_source)?;
            let def_schema = def_op.schema().clone();
            let mut batches: Vec<Batch> = Vec::new();
            loop {
                match def_op.next_batch() {
                    Ok(Some(b)) => batches.push(b),
                    Ok(None) => break,
                    Err(e) => return Err(map_exec_error(e)),
                }
            }
            let shared = Arc::new(batches);
            match body.as_ref() {
                LogicalPlan::Empty { .. } => Ok(Box::new(CteScan::new(shared, def_schema))),
                _ => build_operator(body, data_source),
            }
        }
    }
}

/// Lower a [`LogicalPlan::Join`] node to the best available physical join.
///
/// Selection rules (v0.5):
/// - `On` condition, `Inner` or `LeftOuter`: [`HashJoin`].
/// - `On` condition, any other type: [`MergeJoin`] (assumes pre-sorted input).
/// - `Using` pairs or `None` (CROSS): [`NestedLoopJoin`].
#[allow(clippy::too_many_arguments)]
fn build_join(
    left: &LogicalPlan,
    right: &LogicalPlan,
    join_type: LogicalJoinType,
    condition: &LogicalJoinCondition,
    schema: &Schema,
    data_source: &dyn DataSource,
) -> Result<Box<dyn Operator>, BuildError> {
    let left_schema = left.schema().clone();
    let right_schema = right.schema().clone();
    let left_op = build_operator(left, data_source)?;
    let right_op = build_operator(right, data_source)?;

    match condition {
        LogicalJoinCondition::On(pred) => {
            // Extract a single equi-join key pair when the predicate is a
            // binary Eq with Column on both sides.
            if let Some((left_key, right_key)) = extract_equi_keys(pred, left_schema.len()) {
                match join_type {
                    LogicalJoinType::Inner | LogicalJoinType::LeftOuter => {
                        return Ok(Box::new(HashJoin::new(
                            left_op,
                            right_op,
                            left_key,
                            right_key,
                            join_type,
                            schema.clone(),
                            left_schema,
                            right_schema,
                        )));
                    }
                    _ => {
                        return Ok(Box::new(MergeJoin::new(
                            left_op,
                            right_op,
                            left_key,
                            right_key,
                            join_type,
                            schema.clone(),
                            left_schema,
                            right_schema,
                        )));
                    }
                }
            }
            // Non-equi predicate: use NestedLoopJoin with predicate.
            build_nlj(
                left_op,
                right_op,
                Some(pred.clone()),
                join_type,
                schema.clone(),
                left_schema,
                right_schema,
            )
        }
        LogicalJoinCondition::Using(pairs) => {
            // Translate USING pairs into a composite equality predicate.
            let cond = build_using_predicate(pairs, &left_schema, &right_schema);
            build_nlj(
                left_op,
                right_op,
                cond,
                join_type,
                schema.clone(),
                left_schema,
                right_schema,
            )
        }
        LogicalJoinCondition::None => {
            // CROSS JOIN: no condition.
            build_nlj(
                left_op,
                right_op,
                None,
                join_type,
                schema.clone(),
                left_schema,
                right_schema,
            )
        }
    }
}

/// Attempt to extract an equi-join key pair `(left_expr, right_expr)` from a
/// binary `Eq` predicate whose operands are `Column` references from the left
/// and right sides respectively (right-side indices ≥ `left_width`).
///
/// Returns `None` if the predicate is not in this canonical form.
fn extract_equi_keys(pred: &ScalarExpr, left_width: usize) -> Option<(ScalarExpr, ScalarExpr)> {
    use ultrasql_planner::BinaryOp;
    if let ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = pred
    {
        match (left.as_ref(), right.as_ref()) {
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
            ) if *ri >= left_width => {
                let left_key = ScalarExpr::Column {
                    index: *li,
                    data_type: lt.clone(),
                    name: ln.clone(),
                };
                let right_key = ScalarExpr::Column {
                    index: ri - left_width,
                    data_type: rt.clone(),
                    name: rn.clone(),
                };
                return Some((left_key, right_key));
            }
            // Mirrored form: left operand is the right-side column.
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
            ) if *li >= left_width => {
                let left_key = ScalarExpr::Column {
                    index: *ri,
                    data_type: rt.clone(),
                    name: rn.clone(),
                };
                let right_key = ScalarExpr::Column {
                    index: li - left_width,
                    data_type: lt.clone(),
                    name: ln.clone(),
                };
                return Some((left_key, right_key));
            }
            _ => {}
        }
    }
    None
}

/// Build a composite equality predicate from `USING (left_idx, right_idx)` pairs.
///
/// Each pair produces `left_col = right_col` (right column offset by left
/// schema width); multiple pairs are `AND`ed together. Returns `None` when the
/// list is empty.
fn build_using_predicate(
    pairs: &[(usize, usize)],
    left_schema: &Schema,
    right_schema: &Schema,
) -> Option<ScalarExpr> {
    use ultrasql_planner::BinaryOp;
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
            data_type: ultrasql_core::DataType::Bool,
        }
    });
    let first = iter.next()?;
    Some(iter.fold(first, |acc, next| ScalarExpr::Binary {
        op: BinaryOp::And,
        left: Box::new(acc),
        right: Box::new(next),
        data_type: ultrasql_core::DataType::Bool,
    }))
}

/// Build a [`NestedLoopJoin`].
///
/// The right child is materialised once and replayed via a factory closure
/// that clones the `Arc`-backed batch list. This is O(|right|) per left row;
/// a future wave will add operator spooling for cheaper right re-scans.
#[allow(clippy::too_many_arguments)]
fn build_nlj(
    left_op: Box<dyn Operator>,
    right_op: Box<dyn Operator>,
    condition: Option<ScalarExpr>,
    join_type: LogicalJoinType,
    schema: Schema,
    left_schema: Schema,
    right_schema: Schema,
) -> Result<Box<dyn Operator>, BuildError> {
    // Drain the right side into memory so we can replay it cheaply.
    let mut batches: Vec<Batch> = Vec::new();
    let mut right_op = right_op;
    loop {
        match right_op.next_batch() {
            Ok(Some(b)) => batches.push(b),
            Ok(None) => break,
            Err(e) => return Err(map_exec_error(e)),
        }
    }
    let shared: Arc<Vec<Batch>> = Arc::new(batches);
    let rs = right_schema.clone();
    let factory: RightFactory = Box::new(move || {
        Ok(Box::new(MemTableScan::new(rs.clone(), (*shared).clone())) as Box<dyn Operator>)
    });
    Ok(Box::new(NestedLoopJoin::new(
        left_op,
        factory,
        join_type,
        condition,
        schema,
        left_schema,
        right_schema,
    )))
}

/// Build a scan operator and apply the optional projection.
fn build_scan(
    table: &str,
    projection: Option<&[usize]>,
    data_source: &dyn DataSource,
) -> Result<Box<dyn Operator>, BuildError> {
    let (schema, batches) = data_source.scan(table)?;
    let scan: Box<dyn Operator> = Box::new(MemTableScan::new(schema, batches));
    match projection {
        Some(indices) => Project::new(scan, indices.to_vec())
            .map(|p| Box::new(p) as Box<dyn Operator>)
            .map_err(map_exec_error),
        None => Ok(scan),
    }
}

/// Build a [`Project`] iff every output expression is a bare column
/// reference. Anything richer is `Unsupported`.
fn build_project(
    child: Box<dyn Operator>,
    exprs: &[(ScalarExpr, String)],
) -> Result<Box<dyn Operator>, BuildError> {
    let mut indices = Vec::with_capacity(exprs.len());
    for (expr, _name) in exprs {
        if let ScalarExpr::Column { index, .. } = expr {
            indices.push(*index);
        } else {
            return Err(BuildError::Unsupported(
                "computed projections not supported in v0.5",
            ));
        }
    }
    Project::new(child, indices)
        .map(|p| Box::new(p) as Box<dyn Operator>)
        .map_err(map_exec_error)
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{
        AggregateFunc, BinaryOp, LogicalAggregateExpr, LogicalJoinCondition, LogicalJoinType,
        LogicalPlan, LogicalSetOp, LogicalSetQuantifier, ScalarExpr, SortKey,
    };
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::{BuildError, DataSource, build_operator};
    use crate::Operator;

    /// `(id i32, val i64)` — the fixture schema reused across the
    /// builder tests, mirroring the planner's own test catalog shape.
    fn users_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int64),
        ])
        .expect("schema is well-formed")
    }

    /// Pack `(id, val)` rows into a single batch.
    fn batch(rows: &[(i32, i64)]) -> Batch {
        let ids: Vec<i32> = rows.iter().map(|(i, _)| *i).collect();
        let vals: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
        Batch::new([
            Column::Int32(NumericColumn::from_data(ids)),
            Column::Int64(NumericColumn::from_data(vals)),
        ])
        .expect("test batch is well-formed")
    }

    /// In-memory [`DataSource`] that stores tables by name. Mirrors
    /// the shape of the planner's `InMemoryCatalog` so the same
    /// fixture wires both sides of the lowering.
    struct StaticSource {
        tables: std::collections::HashMap<String, (Schema, Vec<Batch>)>,
    }

    impl StaticSource {
        fn new() -> Self {
            Self {
                tables: std::collections::HashMap::new(),
            }
        }

        fn with_users() -> Self {
            let mut s = Self::new();
            s.tables.insert(
                "users".to_string(),
                (
                    users_schema(),
                    vec![
                        batch(&[(1, 10), (7, 20), (3, 30)]),
                        batch(&[(7, 40), (2, 50), (7, 60)]),
                    ],
                ),
            );
            s
        }
    }

    impl DataSource for StaticSource {
        fn scan(&self, table: &str) -> Result<(Schema, Vec<Batch>), BuildError> {
            self.tables
                .get(table)
                .cloned()
                .ok_or_else(|| BuildError::Source(format!("unknown table: {table}")))
        }
    }

    /// Drain every batch from an operator and collect the `(id, val)`
    /// pairs from the canonical `(Int32, Int64)` schema. Panics if a
    /// batch arrives with an unexpected shape — the test fails fast.
    fn drain_id_val(op: &mut dyn Operator) -> Vec<(i32, i64)> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("operator must not error") {
            let cols = b.columns();
            assert_eq!(cols.len(), 2, "expected two columns");
            match (&cols[0], &cols[1]) {
                (Column::Int32(ids), Column::Int64(vals)) => {
                    assert_eq!(ids.data().len(), vals.data().len());
                    for (i, v) in ids.data().iter().zip(vals.data().iter()) {
                        out.push((*i, *v));
                    }
                }
                other => panic!("unexpected column variants: {other:?}"),
            }
        }
        out
    }

    /// Drain every batch as a flat `Vec<i64>`, asserting a single
    /// `Int64` column. Used for projection tests that narrow to `val`.
    fn drain_i64(op: &mut dyn Operator) -> Vec<i64> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("operator must not error") {
            assert_eq!(b.width(), 1, "expected single-column output");
            match &b.columns()[0] {
                Column::Int64(c) => out.extend_from_slice(c.data()),
                other => panic!("expected Int64 column, got {other:?}"),
            }
        }
        out
    }

    /// Helper: build a typed `Column { id }` expression against the
    /// fixture schema.
    fn col_id() -> ScalarExpr {
        ScalarExpr::Column {
            name: "id".into(),
            index: 0,
            data_type: DataType::Int32,
        }
    }

    /// Helper: build a typed `Column { val }` expression against the
    /// fixture schema.
    fn col_val() -> ScalarExpr {
        ScalarExpr::Column {
            name: "val".into(),
            index: 1,
            data_type: DataType::Int64,
        }
    }

    /// Helper: build an Int32 literal expression.
    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn scan_plan() -> LogicalPlan {
        LogicalPlan::Scan {
            table: "users".into(),
            schema: users_schema(),
            projection: None,
        }
    }

    #[test]
    fn scan_emits_every_batch_from_data_source() {
        let src = StaticSource::with_users();
        let mut op = build_operator(&scan_plan(), &src).expect("scan builds");
        let rows = drain_id_val(&mut *op);
        assert_eq!(
            rows,
            vec![(1, 10), (7, 20), (3, 30), (7, 40), (2, 50), (7, 60)],
            "scan returns every row in batch order"
        );
    }

    #[test]
    fn scan_with_projection_narrows_to_val_column() {
        let src = StaticSource::with_users();
        let plan = LogicalPlan::Scan {
            table: "users".into(),
            schema: users_schema(),
            projection: Some(vec![1]),
        };
        let mut op = build_operator(&plan, &src).expect("projected scan builds");
        assert_eq!(op.schema().len(), 1);
        assert_eq!(op.schema().field_at(0).name, "val");
        assert_eq!(op.schema().field_at(0).data_type, DataType::Int64);
        let vals = drain_i64(&mut *op);
        assert_eq!(vals, vec![10, 20, 30, 40, 50, 60]);
    }

    #[test]
    fn filter_eq_int32_keeps_matching_rows() {
        let src = StaticSource::with_users();
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(col_id()),
                right: Box::new(lit_i32(7)),
                data_type: DataType::Bool,
            },
        };
        let mut op = build_operator(&plan, &src).expect("filter builds");
        let rows = drain_id_val(&mut *op);
        assert_eq!(rows, vec![(7, 20), (7, 40), (7, 60)]);
    }

    #[test]
    fn filter_accepts_mirrored_literal_on_left() {
        let src = StaticSource::with_users();
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(lit_i32(7)),
                right: Box::new(col_id()),
                data_type: DataType::Bool,
            },
        };
        let mut op = build_operator(&plan, &src).expect("mirrored filter builds");
        let rows = drain_id_val(&mut *op);
        assert_eq!(rows, vec![(7, 20), (7, 40), (7, 60)]);
    }

    #[test]
    fn project_over_filter_over_scan_end_to_end() {
        let src = StaticSource::with_users();
        let val_schema = Schema::new([Field::required("val", DataType::Int64)]).expect("schema ok");
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan()),
                predicate: ScalarExpr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(col_id()),
                    right: Box::new(lit_i32(7)),
                    data_type: DataType::Bool,
                },
            }),
            exprs: vec![(col_val(), "val".into())],
            schema: val_schema,
        };
        let mut op = build_operator(&plan, &src).expect("project+filter+scan builds");
        assert_eq!(op.schema().len(), 1);
        assert_eq!(op.schema().field_at(0).name, "val");
        let vals = drain_i64(&mut *op);
        assert_eq!(vals, vec![20, 40, 60]);
    }

    #[test]
    fn limit_truncates_longer_scan() {
        let src = StaticSource::with_users();
        let plan = LogicalPlan::Limit {
            input: Box::new(scan_plan()),
            n: 2,
            offset: 0,
        };
        let mut op = build_operator(&plan, &src).expect("limit builds");
        let rows = drain_id_val(&mut *op);
        assert_eq!(rows, vec![(1, 10), (7, 20)]);
    }

    #[test]
    fn unknown_table_surfaces_as_source_error() {
        let src = StaticSource::new();
        let err = build_operator(&scan_plan(), &src).expect_err("scan must fail");
        assert!(
            matches!(err, BuildError::Source(ref s) if s.contains("users")),
            "got {err:?}"
        );
    }

    #[test]
    fn filter_with_arithmetic_predicate_builds_successfully() {
        // The general Filter operator accepts any ScalarExpr; build-time
        // validation no longer rejects non-equality predicates.
        let src = StaticSource::with_users();
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan()),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Add,
                left: Box::new(col_id()),
                right: Box::new(lit_i32(1)),
                data_type: DataType::Int32,
            },
        };
        // Builds without error now that Filter accepts all predicates.
        let mut op = build_operator(&plan, &src).expect("general filter must build");
        // Runtime evaluation of a non-boolean predicate surfaces as TypeMismatch.
        let err = op
            .next_batch()
            .expect_err("non-boolean predicate must error at runtime");
        assert!(
            matches!(err, crate::ExecError::TypeMismatch(_)),
            "expected TypeMismatch, got {err:?}"
        );
    }

    #[test]
    fn sort_lowers_and_produces_sorted_output() {
        let src = StaticSource::with_users();
        let plan = LogicalPlan::Sort {
            input: Box::new(scan_plan()),
            keys: vec![SortKey {
                expr: col_id(),
                asc: true,
                nulls_first: false,
            }],
        };
        let mut op = build_operator(&plan, &src).expect("sort builds");
        let rows = drain_id_val(&mut *op);
        let ids: Vec<i32> = rows.iter().map(|(i, _)| *i).collect();
        assert_eq!(ids, vec![1, 2, 3, 7, 7, 7], "sort by id asc");
    }

    #[test]
    fn aggregate_count_star_lowers_to_hash_aggregate() {
        let src = StaticSource::with_users();
        let agg_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan_plan()),
            group_by: vec![],
            aggregates: vec![LogicalAggregateExpr {
                func: AggregateFunc::CountStar,
                arg: None,
                distinct: false,
                output_name: "cnt".into(),
                data_type: DataType::Int64,
            }],
            schema: agg_schema,
        };
        let mut op = build_operator(&plan, &src).expect("aggregate builds");
        let vals = drain_i64(&mut *op);
        assert_eq!(vals, vec![6], "count(*) over 6 rows");
    }

    #[test]
    fn set_op_union_all_lowers_and_concatenates() {
        let src = StaticSource::with_users();
        let plan = LogicalPlan::SetOp {
            op: LogicalSetOp::Union,
            quantifier: LogicalSetQuantifier::All,
            left: Box::new(scan_plan()),
            right: Box::new(scan_plan()),
            schema: users_schema(),
        };
        let mut op = build_operator(&plan, &src).expect("set op builds");
        let rows = drain_id_val(&mut *op);
        assert_eq!(rows.len(), 12, "UNION ALL doubles the 6 rows");
    }

    #[test]
    fn join_inner_equi_lowers_to_hash_join() {
        // Self-join users ON left.id = right.id (concatenated schema index 2).
        let join_schema = Schema::new([
            Field::required("l_id", DataType::Int32),
            Field::required("l_val", DataType::Int64),
            Field::required("r_id", DataType::Int32),
            Field::required("r_val", DataType::Int64),
        ])
        .expect("join schema ok");
        let right_id = ScalarExpr::Column {
            name: "r_id".into(),
            index: 2,
            data_type: DataType::Int32,
        };
        let plan = LogicalPlan::Join {
            left: Box::new(scan_plan()),
            right: Box::new(scan_plan()),
            join_type: LogicalJoinType::Inner,
            condition: LogicalJoinCondition::On(ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(col_id()),
                right: Box::new(right_id),
                data_type: DataType::Bool,
            }),
            schema: join_schema,
        };
        let src = StaticSource::with_users();
        let mut op = build_operator(&plan, &src).expect("join builds");
        let mut count = 0usize;
        while let Some(b) = op.next_batch().expect("join must not error") {
            count += b.rows();
        }
        // id=1 (1×1), id=2 (1×1), id=3 (1×1), id=7 (3×3 = 9) → total 12.
        assert_eq!(count, 12, "inner self-join row count");
    }

    #[test]
    fn limit_with_offset_skips_then_takes() {
        // `users` fixture (StaticSource::with_users) is 6 rows split as
        // 3+3. OFFSET 1 LIMIT 2 should skip the first row and emit the
        // next two, confirming the builder lowers offset through to
        // the executor.
        let src = StaticSource::with_users();
        let plan = LogicalPlan::Limit {
            input: Box::new(scan_plan()),
            n: 2,
            offset: 1,
        };
        let mut op = build_operator(&plan, &src).expect("OFFSET builds");
        let mut count = 0usize;
        while let Some(b) = op.next_batch().expect("offset must not error") {
            count += b.rows();
        }
        assert_eq!(count, 2, "limit 2 offset 1 emits 2 rows");
    }

    #[test]
    fn limit_null_with_offset_only_emits_tail() {
        // OFFSET with no LIMIT is encoded by the binder as `n = u64::MAX`.
        // Confirm the builder saturates and emits every row past the
        // skip window without erroring on the "huge LIMIT" value.
        let src = StaticSource::with_users();
        let plan = LogicalPlan::Limit {
            input: Box::new(scan_plan()),
            n: u64::MAX,
            offset: 3,
        };
        let mut op = build_operator(&plan, &src).expect("OFFSET-only builds");
        let mut count = 0usize;
        while let Some(b) = op.next_batch().expect("offset must not error") {
            count += b.rows();
        }
        // `with_users` is 6 rows total (2 batches of 3); 6 - 3 = 3 rows
        // past the skip window.
        assert_eq!(count, 3, "offset 3 with no limit emits 3 rows");
    }

    #[test]
    fn computed_projection_is_unsupported() {
        let src = StaticSource::with_users();
        let plan = LogicalPlan::Project {
            input: Box::new(scan_plan()),
            exprs: vec![(
                ScalarExpr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(col_id()),
                    right: Box::new(lit_i32(1)),
                    data_type: DataType::Int32,
                },
                "id_plus_one".into(),
            )],
            schema: Schema::new([Field::required("id_plus_one", DataType::Int32)])
                .expect("schema ok"),
        };
        let err = build_operator(&plan, &src).expect_err("computed projection must reject");
        assert!(matches!(err, BuildError::Unsupported(_)), "got {err:?}");
    }

    #[test]
    fn empty_plan_lowers_to_eof_source() {
        let src = StaticSource::new();
        let plan = LogicalPlan::Empty {
            schema: users_schema(),
        };
        let mut op = build_operator(&plan, &src).expect("empty builds");
        assert!(
            op.next_batch().expect("empty op must not error").is_none(),
            "empty plan emits no batches"
        );
        assert_eq!(
            op.schema().len(),
            2,
            "empty plan still reports its declared schema"
        );
    }
}
