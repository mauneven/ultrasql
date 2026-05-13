//! Physical plan builder.
//!
//! Lowers a [`LogicalPlan`] tree from the planner crate into a tree of
//! [`Operator`] trait objects the pull-mode executor can drive. The
//! builder is intentionally narrow at v0.5: only the operators the
//! executor crate ships today are reachable through this lowering.
//!
//! Everything outside that subset surfaces as
//! [`BuildError::Unsupported`] so the server can return a clean
//! protocol-level error rather than panic.
//!
//! # Lowering rules (v0.5)
//!
//! - `LogicalPlan::Scan { table, projection, .. }` calls
//!   [`DataSource::scan`] for `table`, builds a [`MemTableScan`] over
//!   the returned batches, and wraps it in a [`Project`] when
//!   `projection` is set.
//! - `LogicalPlan::Filter { predicate, .. }` lowers to [`Filter`]
//!   (the general expression-interpreter-backed filter). Any predicate
//!   that the [`Eval`] interpreter does not support at runtime surfaces
//!   as an [`ExecError`] during execution, not a build-time error.
//! - `LogicalPlan::Values` lowers to [`ValuesScan`].
//! - `LogicalPlan::Project` lowers to [`Project`] iff every output
//!   expression is a bare column reference; computed expressions are
//!   `Unsupported`.
//! - `LogicalPlan::Limit { offset, n, .. }` lowers to [`Limit`] only
//!   when `offset == 0`; non-zero offsets are `Unsupported`.
//! - `LogicalPlan::Sort` is `Unsupported`.
//! - `LogicalPlan::Empty` lowers to a [`MemTableScan`] over its
//!   declared schema with zero batches — an EOF source.
//! - `LogicalPlan::Insert / Update / Delete` are `Unsupported` pending
//!   the datasource-handle refactor (wave 5+). [`ModifyTable`] and
//!   [`ValuesScan`] exist as stand-alone operators that callers can
//!   construct directly.
//!
//! The data source is injected through [`DataSource`] rather than
//! resolved via the catalog so this layer stays decoupled from the
//! heap and buffer-pool stack that has not yet landed. Production
//! callers will supply an implementation backed by the storage engine;
//! the v0.5 tests and the bring-up CLI supply an in-memory one.

use ultrasql_core::Schema;
use ultrasql_planner::{LogicalPlan, ScalarExpr};
use ultrasql_vec::Batch;

use crate::filter_op::Filter;
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
            if *offset != 0 {
                return Err(BuildError::Unsupported("OFFSET not supported in v0.5"));
            }
            let child = build_operator(input, data_source)?;
            let capped = usize::try_from(*n).map_err(|_| {
                BuildError::Type(format!("LIMIT value {n} exceeds platform pointer width"))
            })?;
            Ok(Box::new(Limit::new(child, capped)))
        }

        LogicalPlan::Sort { .. } => Err(BuildError::Unsupported("sort not supported in v0.5")),

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
        LogicalPlan::Join { .. } => Err(BuildError::Unsupported("JOIN not supported in v0.5")),
        LogicalPlan::Aggregate { .. } => {
            Err(BuildError::Unsupported("aggregate not supported in v0.5"))
        }
        LogicalPlan::SetOp { .. } => Err(BuildError::Unsupported("set op not supported in v0.5")),
        LogicalPlan::Cte { .. } => Err(BuildError::Unsupported("CTE not supported in v0.5")),
    }
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
    use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr, SortKey};
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
        // `7 = id` — the symmetric form — should lower exactly like
        // `id = 7`. The binder may emit either shape today; the
        // builder normalises.
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
        // validation no longer rejects non-equality predicates. An Add
        // predicate like `id + 1` is accepted at build time and will
        // surface a TypeMismatch at runtime (the result is Int32, not Bool).
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
    fn sort_is_unsupported() {
        let src = StaticSource::with_users();
        let plan = LogicalPlan::Sort {
            input: Box::new(scan_plan()),
            keys: vec![SortKey {
                expr: col_id(),
                asc: true,
                nulls_first: false,
            }],
        };
        let err = build_operator(&plan, &src).expect_err("sort must reject");
        assert!(matches!(err, BuildError::Unsupported(_)), "got {err:?}");
    }

    #[test]
    fn limit_with_offset_is_unsupported() {
        let src = StaticSource::with_users();
        let plan = LogicalPlan::Limit {
            input: Box::new(scan_plan()),
            n: 2,
            offset: 1,
        };
        let err = build_operator(&plan, &src).expect_err("OFFSET must reject");
        assert!(matches!(err, BuildError::Unsupported(_)), "got {err:?}");
    }

    #[test]
    fn computed_projection_is_unsupported() {
        // `SELECT id + 1 FROM users` — a non-column projection.
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
