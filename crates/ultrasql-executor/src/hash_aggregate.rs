//! Hash-based GROUP BY and aggregate operator.
//!
//! # Two-phase execution
//!
//! **Build phase** (first `next_batch` call): the child is drained completely.
//! For each input row the group-key expressions are evaluated to produce a
//! `Vec<Value>` tuple. A `HashMap<GroupKey, Vec<AggState>>` accumulates
//! one aggregate state per group. After the child is exhausted, the groups
//! are collected into an output row buffer.
//!
//! **Probe phase**: the output buffer is emitted in 4096-row chunks until
//! empty.
//!
//! # NULL semantics
//!
//! - `COUNT(expr)` skips NULL argument values; `COUNT(*)` counts all rows.
//! - `SUM`, `AVG`, `MIN`, `MAX` skip NULL argument values; they return NULL
//!   for a group where all values were NULL (or the group was empty).
//! - `BOOL_AND` / `BOOL_OR` skip NULL values.
//! - `STRING_AGG` and `ARRAY_AGG` skip NULL values.
//!
//! # Empty-input rule (SQL standard)
//!
//! If the input is empty and there are **no** group keys, a single row is
//! emitted with all aggregates at their identity (COUNT = 0, SUM/AVG/MIN/MAX
//! = NULL, etc.). This matches `SELECT COUNT(*) FROM empty_table = 0`.
//! If the input is empty and there **are** group keys, no rows are emitted.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use ultrasql_core::{Schema, Value};
use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, ScalarExpr};
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

/// Maximum rows per emitted batch, matching the `ARCHITECTURE.md` section 9 contract.
const BATCH_TARGET_ROWS: usize = 4096;

// ---------------------------------------------------------------------------
// Hash-map key wrapper
// ---------------------------------------------------------------------------

/// A wrapper around [`Value`] that implements `Hash + Eq` so it can serve
/// as a component of a hash-map key.
///
/// [`Value`] derives only `PartialEq` (not `Eq`) because `f32`/`f64` are not
/// `Eq`. We implement `Eq` manually: for floating-point values we use
/// bitwise equality (NaN == NaN in this context, consistent with join
/// semantics for floating-point GROUP BY keys).
#[derive(Debug)]
struct KeyValue(Value);

impl PartialEq for KeyValue {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Value::Float32(a), Value::Float32(b)) => a.to_bits() == b.to_bits(),
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            _ => self.0 == other.0,
        }
    }
}

impl Eq for KeyValue {}

impl Hash for KeyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_value(&self.0, state);
    }
}

/// Hash a [`Value`] by discriminant + bit-pattern for floating-point types.
fn hash_value<H: Hasher>(v: &Value, state: &mut H) {
    match v {
        Value::Null => state.write_u8(0),
        Value::Bool(b) => {
            state.write_u8(1);
            b.hash(state);
        }
        Value::Int16(x) => {
            state.write_u8(2);
            x.hash(state);
        }
        Value::Int32(x) => {
            state.write_u8(3);
            x.hash(state);
        }
        Value::Int64(x) => {
            state.write_u8(4);
            x.hash(state);
        }
        Value::Float32(x) => {
            state.write_u8(5);
            x.to_bits().hash(state);
        }
        Value::Float64(x) => {
            state.write_u8(6);
            x.to_bits().hash(state);
        }
        Value::Text(s) => {
            state.write_u8(7);
            s.hash(state);
        }
        Value::Bytea(b) => {
            state.write_u8(8);
            b.hash(state);
        }
        Value::Timestamp(x) | Value::TimestampTz(x) | Value::Time(x) => {
            state.write_u8(9);
            x.hash(state);
        }
        Value::Date(x) => {
            state.write_u8(10);
            x.hash(state);
        }
        Value::Uuid(u) => {
            state.write_u8(11);
            u.hash(state);
        }
    }
}

/// A group key — a sequence of zero or more [`KeyValue`]s.
///
/// Uses a newtype wrapper so we can implement `Hash + Eq` for a
/// `Vec<Value>` without a coherence violation.
#[derive(Debug, PartialEq, Eq)]
struct GroupKey(Vec<KeyValue>);

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for kv in &self.0 {
            kv.hash(state);
        }
    }
}

impl GroupKey {
    fn from_values(values: Vec<Value>) -> Self {
        Self(values.into_iter().map(KeyValue).collect())
    }

    fn into_values(self) -> Vec<Value> {
        self.0.into_iter().map(|kv| kv.0).collect()
    }
}

// ---------------------------------------------------------------------------
// Operator
// ---------------------------------------------------------------------------

/// Hash-based group-by and aggregate operator.
///
/// Evaluates `group_keys` per row to determine the group, then accumulates
/// one `AggState` per aggregate per group. On the probe phase, emits one
/// output row per group consisting of the group key values followed by the
/// finalised aggregate values.
///
/// The operator is `Send` because `HashMap`, `Vec`, `Schema`, and the
/// `Box<dyn Operator>` child are all `Send`.
#[derive(Debug)]
pub struct HashAggregate {
    child: Box<dyn Operator>,
    /// Compiled evaluators for the GROUP BY expressions.
    group_key_evals: Vec<Eval>,
    /// Aggregate function descriptors.
    aggregates: Vec<LogicalAggregateExpr>,
    schema: Schema,
    /// Output row buffer built during the build phase.
    /// `None` until the build phase completes.
    output: Option<std::vec::IntoIter<Vec<Value>>>,
    /// `true` after `Ok(None)` has been returned.
    eof: bool,
}

impl HashAggregate {
    /// Construct a hash aggregate operator.
    ///
    /// - `child` — the input operator.
    /// - `group_keys` — expressions evaluated per row to form the group key.
    ///   An empty slice means no GROUP BY (whole-relation aggregate).
    /// - `aggregates` — aggregate function descriptors from the planner.
    /// - `schema` — output schema: group key columns followed by aggregate
    ///   columns. Must have `group_keys.len() + aggregates.len()` fields.
    #[must_use]
    pub fn new(
        child: Box<dyn Operator>,
        group_keys: Vec<ScalarExpr>,
        aggregates: Vec<LogicalAggregateExpr>,
        schema: Schema,
    ) -> Self {
        let group_key_evals = group_keys.into_iter().map(Eval::new).collect();
        Self {
            child,
            group_key_evals,
            aggregates,
            schema,
            output: None,
            eof: false,
        }
    }
}

impl Operator for HashAggregate {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        if self.output.is_none() {
            let output_rows = self.build()?;
            self.output = Some(output_rows.into_iter());
        }

        let iter = self.output.as_mut().expect("just-set");
        let chunk: Vec<Vec<Value>> = iter.by_ref().take(BATCH_TARGET_ROWS).collect();
        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&chunk, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl HashAggregate {
    /// Execute the build phase: drain child, accumulate aggregates, finalise.
    fn build(&mut self) -> Result<Vec<Vec<Value>>, ExecError> {
        let mut table: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
        let child_schema = self.child.schema().clone();
        let has_group_keys = !self.group_key_evals.is_empty();
        let mut saw_any_row = false;

        loop {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            let rows = batch_to_rows(&batch, &child_schema)?;
            for row in &rows {
                saw_any_row = true;
                // Evaluate group keys.
                let key_values: Vec<Value> = self
                    .group_key_evals
                    .iter()
                    .map(|ev| ev.eval(row).unwrap_or(Value::Null))
                    .collect();
                let key = GroupKey::from_values(key_values);

                // Get or insert agg states for this group.
                let states = table
                    .entry(key)
                    .or_insert_with(|| init_states(&self.aggregates));

                // Accumulate.
                for (state, agg) in states.iter_mut().zip(self.aggregates.iter()) {
                    accumulate(state, agg, row)?;
                }
            }
        }

        // Empty input: if no group keys, emit a single identity row.
        if !saw_any_row && !has_group_keys {
            let identity: Vec<Value> = self
                .aggregates
                .iter()
                .map(|agg| finalise(&init_state_for(agg)))
                .collect();
            return Ok(vec![identity]);
        }

        // Collect groups into output rows.
        let mut output: Vec<Vec<Value>> = Vec::with_capacity(table.len());
        for (key, states) in table {
            let mut row = key.into_values(); // group key values first
            for state in &states {
                row.push(finalise(state));
            }
            output.push(row);
        }
        Ok(output)
    }
}

// ---------------------------------------------------------------------------
// Aggregate state machine
// ---------------------------------------------------------------------------

/// Per-row accumulator for a single aggregate function instance.
#[derive(Debug)]
enum AggState {
    /// `COUNT(*)` — counts all rows regardless of NULLs.
    CountStar(i64),
    /// `COUNT(expr)` — counts non-NULL values.
    Count(i64),
    /// `SUM(expr)` — running sum, `None` if all values were NULL.
    Sum(Option<Value>),
    /// `AVG(expr)` — (running sum, count of non-NULLs).
    Avg(Option<Value>, i64),
    /// `MIN(expr)` — current minimum, `None` if no non-NULL seen yet.
    Min(Option<Value>),
    /// `MAX(expr)` — current maximum, `None` if no non-NULL seen yet.
    Max(Option<Value>),
    /// `BOOL_AND(expr)` — `None` until a non-NULL is seen.
    BoolAnd(Option<bool>),
    /// `BOOL_OR(expr)` — `None` until a non-NULL is seen.
    BoolOr(Option<bool>),
    /// `STRING_AGG(expr, sep)` — accumulated (values, separator).
    StringAgg(Vec<String>, String),
    /// `ARRAY_AGG(expr)` — accumulated non-NULL values.
    ArrayAgg(Vec<Value>),
}

/// Initialise one [`AggState`] for the given aggregate descriptor.
#[allow(clippy::missing_const_for_fn)] // not const due to Vec::new() in variants
fn init_state_for(agg: &LogicalAggregateExpr) -> AggState {
    match agg.func {
        AggregateFunc::CountStar => AggState::CountStar(0),
        AggregateFunc::Count => AggState::Count(0),
        AggregateFunc::Sum => AggState::Sum(None),
        AggregateFunc::Avg => AggState::Avg(None, 0),
        AggregateFunc::Min => AggState::Min(None),
        AggregateFunc::Max => AggState::Max(None),
        AggregateFunc::BoolAnd => AggState::BoolAnd(None),
        AggregateFunc::BoolOr => AggState::BoolOr(None),
        AggregateFunc::StringAgg => AggState::StringAgg(Vec::new(), String::new()),
        AggregateFunc::ArrayAgg => AggState::ArrayAgg(Vec::new()),
    }
}

/// Initialise all aggregate states for a new group.
fn init_states(aggregates: &[LogicalAggregateExpr]) -> Vec<AggState> {
    aggregates.iter().map(init_state_for).collect()
}

/// Feed one input `row` into `state` using the aggregate descriptor `agg`.
fn accumulate(
    state: &mut AggState,
    agg: &LogicalAggregateExpr,
    row: &[Value],
) -> Result<(), ExecError> {
    // Evaluate the argument expression (if any).
    let arg_val: Option<Value> = agg
        .arg
        .as_ref()
        .map(|expr| Eval::new(expr.clone()).eval(row).unwrap_or(Value::Null));

    match state {
        AggState::CountStar(n) => {
            *n = n.saturating_add(1);
        }
        AggState::Count(n) => {
            if !matches!(arg_val, Some(Value::Null) | None) {
                *n = n.saturating_add(1);
            }
        }
        AggState::Sum(acc) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    *acc = Some(match acc.take() {
                        None => v,
                        Some(existing) => add_values(existing, v)?,
                    });
                }
            }
        }
        AggState::Avg(sum, cnt) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    *sum = Some(match sum.take() {
                        None => v,
                        Some(existing) => add_values(existing, v)?,
                    });
                    *cnt = cnt.saturating_add(1);
                }
            }
        }
        AggState::Min(current) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    *current = Some(match current.take() {
                        None => v,
                        Some(existing) => {
                            if value_lt(&v, &existing) {
                                v
                            } else {
                                existing
                            }
                        }
                    });
                }
            }
        }
        AggState::Max(current) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    *current = Some(match current.take() {
                        None => v,
                        Some(existing) => {
                            if value_lt(&existing, &v) {
                                v
                            } else {
                                existing
                            }
                        }
                    });
                }
            }
        }
        AggState::BoolAnd(acc) => {
            if let Some(Value::Bool(b)) = arg_val {
                *acc = Some(acc.unwrap_or(true) && b);
            }
        }
        AggState::BoolOr(acc) => {
            if let Some(Value::Bool(b)) = arg_val {
                *acc = Some(acc.unwrap_or(false) || b);
            }
        }
        AggState::StringAgg(parts, _sep) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    match v {
                        Value::Text(s) => parts.push(s),
                        other => parts.push(other.to_string()),
                    }
                }
            }
        }
        AggState::ArrayAgg(items) => {
            if let Some(v) = arg_val {
                if !v.is_null() {
                    items.push(v);
                }
            }
        }
    }
    Ok(())
}

/// Finalise an [`AggState`] into its result [`Value`].
fn finalise(state: &AggState) -> Value {
    match state {
        AggState::CountStar(n) | AggState::Count(n) => Value::Int64(*n),
        AggState::Sum(acc) | AggState::Min(acc) | AggState::Max(acc) => {
            acc.clone().unwrap_or(Value::Null)
        }
        AggState::Avg(sum, cnt) => {
            if *cnt == 0 {
                return Value::Null;
            }
            sum.as_ref()
                .map_or(Value::Null, |s| divide_value(s.clone(), *cnt))
        }
        AggState::BoolAnd(b) | AggState::BoolOr(b) => b.map_or(Value::Null, Value::Bool),
        AggState::StringAgg(parts, sep) => {
            if parts.is_empty() {
                Value::Null
            } else {
                Value::Text(parts.join(sep))
            }
        }
        AggState::ArrayAgg(items) => {
            // Encode as a Text representation for the v0.5 row model.
            // A native Array value type is a v1.0 task.
            Value::Text(format!(
                "{{{}}}",
                items
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Arithmetic helpers
// ---------------------------------------------------------------------------

/// Add two numeric values, widening to Int64 or Float64 as appropriate.
///
/// The `Sum` and `Avg` accumulators store the running total as the
/// widened type (Int64 for integers, Float64 for floats) after the
/// first non-null input — but the *new* row arrives unwidened from
/// the child operator, so this helper must accept any mix of
/// narrower-on-the-right and widened-on-the-left integer and float
/// types. The output type is always the widened type to match.
fn add_values(a: Value, b: Value) -> Result<Value, ExecError> {
    match (a, b) {
        // Pure narrow-narrow promotions (first-step folding).
        (Value::Int16(x), Value::Int16(y)) => Ok(Value::Int64(i64::from(x) + i64::from(y))),
        (Value::Int32(x), Value::Int32(y)) => Ok(Value::Int64(i64::from(x) + i64::from(y))),
        (Value::Int64(x), Value::Int64(y)) => Ok(Value::Int64(x.wrapping_add(y))),
        // Widened accumulator + narrower fresh row (the common case in
        // SUM / AVG once the accumulator has stepped through one input).
        (Value::Int64(x), Value::Int16(y)) | (Value::Int16(y), Value::Int64(x)) => {
            Ok(Value::Int64(x.wrapping_add(i64::from(y))))
        }
        (Value::Int64(x), Value::Int32(y)) | (Value::Int32(y), Value::Int64(x)) => {
            Ok(Value::Int64(x.wrapping_add(i64::from(y))))
        }
        (Value::Float32(x), Value::Float32(y)) => Ok(Value::Float64(f64::from(x) + f64::from(y))),
        (Value::Float64(x), Value::Float64(y)) => Ok(Value::Float64(x + y)),
        (Value::Float64(x), Value::Float32(y)) | (Value::Float32(y), Value::Float64(x)) => {
            Ok(Value::Float64(x + f64::from(y)))
        }
        (a, b) => Err(ExecError::TypeMismatch(format!(
            "sum type mismatch: {a:?} and {b:?}"
        ))),
    }
}

/// Divide a running sum by the count to produce an average.
fn divide_value(sum: Value, count: i64) -> Value {
    match sum {
        Value::Int64(s) => Value::Float64(s as f64 / count as f64),
        Value::Float64(s) => Value::Float64(s / count as f64),
        other => other,
    }
}

/// Returns `true` if `a < b` under the natural total order.
fn value_lt(a: &Value, b: &Value) -> bool {
    use crate::sort::compare_values_nullable;
    matches!(
        compare_values_nullable(a, b, false),
        std::cmp::Ordering::Less
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, ScalarExpr};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn, StringColumn};

    use super::HashAggregate;
    use crate::Operator;
    use crate::mem_table_scan::MemTableScan;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    fn col(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.into(),
            index,
            data_type,
        }
    }

    fn count_star_agg() -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func: AggregateFunc::CountStar,
            arg: None,
            distinct: false,
            output_name: "cnt".into(),
            data_type: DataType::Int64,
        }
    }

    fn sum_agg(name: &str, index: usize) -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func: AggregateFunc::Sum,
            arg: Some(col(name, index, DataType::Int64)),
            distinct: false,
            output_name: "total".into(),
            data_type: DataType::Int64,
        }
    }

    fn min_agg(name: &str, index: usize) -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func: AggregateFunc::Min,
            arg: Some(col(name, index, DataType::Int64)),
            distinct: false,
            output_name: "mn".into(),
            data_type: DataType::Int64,
        }
    }

    fn max_agg(name: &str, index: usize) -> LogicalAggregateExpr {
        LogicalAggregateExpr {
            func: AggregateFunc::Max,
            arg: Some(col(name, index, DataType::Int64)),
            distinct: false,
            output_name: "mx".into(),
            data_type: DataType::Int64,
        }
    }

    /// Schema: (group i32, val i64)
    fn schema_group_val() -> Schema {
        Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("val", DataType::Int64),
        ])
        .expect("schema ok")
    }

    fn make_batch_i32_i64(rows: &[(i32, i64)]) -> Batch {
        Batch::new([
            Column::Int32(NumericColumn::from_data(
                rows.iter().map(|(a, _)| *a).collect(),
            )),
            Column::Int64(NumericColumn::from_data(
                rows.iter().map(|(_, b)| *b).collect(),
            )),
        ])
        .expect("batch ok")
    }

    fn drain_all(op: &mut dyn Operator) -> Vec<Vec<Value>> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("no error") {
            let rows = crate::filter_op::batch_to_rows(&b, &schema).expect("decode ok");
            out.extend(rows);
        }
        out
    }

    // -------------------------------------------------------------------------
    // Test 1: COUNT(*) with no GROUP BY
    // -------------------------------------------------------------------------

    #[test]
    fn hash_agg_count_star_no_group() {
        let schema = schema_group_val();
        let scan = MemTableScan::new(
            schema,
            vec![make_batch_i32_i64(&[(1, 10), (2, 20), (3, 30)])],
        );
        let out_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");
        let mut op = HashAggregate::new(Box::new(scan), vec![], vec![count_star_agg()], out_schema);
        let rows = drain_all(&mut op);
        assert_eq!(rows.len(), 1, "scalar aggregate emits exactly one row");
        assert_eq!(rows[0][0], Value::Int64(3), "COUNT(*) = 3");
    }

    // -------------------------------------------------------------------------
    // Test 2: empty input, no group keys → single COUNT=0 row
    // -------------------------------------------------------------------------

    #[test]
    fn hash_agg_empty_input_no_group_emits_identity_row() {
        let schema = schema_group_val();
        let scan = MemTableScan::new(schema, vec![]);
        let out_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");
        let mut op = HashAggregate::new(Box::new(scan), vec![], vec![count_star_agg()], out_schema);
        let rows = drain_all(&mut op);
        assert_eq!(rows.len(), 1, "empty table + no group keys = one row");
        assert_eq!(rows[0][0], Value::Int64(0), "COUNT(*) = 0");
    }

    // -------------------------------------------------------------------------
    // Test 3: empty input with group keys → no rows
    // -------------------------------------------------------------------------

    #[test]
    fn hash_agg_empty_input_with_group_keys_emits_nothing() {
        let schema = schema_group_val();
        let scan = MemTableScan::new(schema, vec![]);
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("cnt", DataType::Int64),
        ])
        .expect("schema ok");
        let mut op = HashAggregate::new(
            Box::new(scan),
            vec![col("group", 0, DataType::Int32)],
            vec![count_star_agg()],
            out_schema,
        );
        let rows = drain_all(&mut op);
        assert!(rows.is_empty(), "empty table + group keys = no rows");
    }

    // -------------------------------------------------------------------------
    // Test 4: GROUP BY with SUM, MIN, MAX
    // -------------------------------------------------------------------------

    #[test]
    fn hash_agg_group_by_sum_min_max() {
        let schema = schema_group_val();
        // group=1 has val: 10, 30; group=2 has val: 20
        let scan = MemTableScan::new(
            schema,
            vec![make_batch_i32_i64(&[(1, 10), (2, 20), (1, 30)])],
        );
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("total", DataType::Int64),
            Field::required("mn", DataType::Int64),
            Field::required("mx", DataType::Int64),
        ])
        .expect("schema ok");
        let mut op = HashAggregate::new(
            Box::new(scan),
            vec![col("group", 0, DataType::Int32)],
            vec![sum_agg("val", 1), min_agg("val", 1), max_agg("val", 1)],
            out_schema,
        );
        let mut rows = drain_all(&mut op);
        // Sort by group key for deterministic comparison.
        rows.sort_by_key(|r| match &r[0] {
            Value::Int32(v) => *v,
            _ => i32::MAX,
        });
        assert_eq!(rows.len(), 2);
        // group=1: sum=40 (10+30), min=10, max=30
        assert_eq!(rows[0][0], Value::Int32(1));
        assert_eq!(rows[0][1], Value::Int64(40));
        assert_eq!(rows[0][2], Value::Int64(10));
        assert_eq!(rows[0][3], Value::Int64(30));
        // group=2: sum=20, min=20, max=20
        assert_eq!(rows[1][0], Value::Int32(2));
        assert_eq!(rows[1][1], Value::Int64(20));
    }

    // -------------------------------------------------------------------------
    // Test 5: COUNT(expr) counts non-null values
    // -------------------------------------------------------------------------

    #[test]
    fn hash_agg_count_expr_counts_non_null_values() {
        let schema = Schema::new([Field::nullable("v", DataType::Text { max_len: None })])
            .expect("schema ok");
        let scan = MemTableScan::new(
            schema,
            vec![
                Batch::new([Column::Utf8(StringColumn::from_data(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                ]))])
                .expect("batch ok"),
            ],
        );
        let out_schema = Schema::new([Field::required("cnt", DataType::Int64)]).expect("schema ok");
        let count_expr_agg = LogicalAggregateExpr {
            func: AggregateFunc::Count,
            arg: Some(col("v", 0, DataType::Text { max_len: None })),
            distinct: false,
            output_name: "cnt".into(),
            data_type: DataType::Int64,
        };
        let mut op = HashAggregate::new(Box::new(scan), vec![], vec![count_expr_agg], out_schema);
        let rows = drain_all(&mut op);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0],
            Value::Int64(3),
            "COUNT(v) counts all non-null values"
        );
    }

    // -------------------------------------------------------------------------
    // Test 6: multi-row group (duplicate hash keys handled correctly)
    // -------------------------------------------------------------------------

    #[test]
    fn hash_agg_many_groups_with_duplicates() {
        let schema = schema_group_val();
        // 100 rows, 10 groups of 10 rows each.
        let row_data: Vec<(i32, i64)> = (0_i32..100).map(|i| (i % 10, i64::from(i))).collect();
        let scan = MemTableScan::new(schema, vec![make_batch_i32_i64(&row_data)]);
        let out_schema = Schema::new([
            Field::required("group", DataType::Int32),
            Field::required("cnt", DataType::Int64),
        ])
        .expect("schema ok");
        let mut op = HashAggregate::new(
            Box::new(scan),
            vec![col("group", 0, DataType::Int32)],
            vec![count_star_agg()],
            out_schema,
        );
        let rows = drain_all(&mut op);
        assert_eq!(rows.len(), 10, "expected 10 groups");
        for row in &rows {
            assert_eq!(row[1], Value::Int64(10), "each group has 10 rows");
        }
    }
}
