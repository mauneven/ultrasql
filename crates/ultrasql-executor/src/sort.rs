//! Sort operator with optional external runs.
//!
//! Drains all input on the first [`Operator::next_batch`] call, evaluates
//! sort keys per row via [`Eval`], sorts the rows using the documented
//! PostgreSQL ordering semantics, then emits the result in 4096-row chunks.
//! With a finite [`crate::WorkMemBudget`], the operator writes sorted runs
//! to temp files once observed row bytes exceed `work_mem` and k-way merges
//! those runs while emitting output.
//!
//! # Ordering semantics
//!
//! Per key: `ASC` + `NULLS LAST` is the PostgreSQL default (NULLs sort
//! after non-NULLs in ascending order). `DESC` + `NULLS FIRST` is the
//! PostgreSQL `DESC` default (NULLs sort before non-NULLs in descending
//! order). Each [`SortKey`] carries explicit `asc` and `nulls_first` flags
//! so the caller controls both independently.
//!
use std::cmp::Ordering;
use std::sync::Arc;

use ultrasql_core::{Schema, Value, bpchar_semantic_text, timetz_utc_micros};
use ultrasql_planner::SortKey;
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::row_codec::RowCodec;
use crate::row_spill::{RowSpillFile, encoded_row_bytes};
use crate::seq_scan::build_batch;
use crate::work_mem::WorkMemBudget;
use crate::{ExecError, Operator, OperatorSpillProfile};

/// Maximum rows per emitted batch, matching the `ARCHITECTURE.md` §9 contract.
const BATCH_TARGET_ROWS: usize = 4096;

/// In-memory sort operator.
///
/// On the first call to [`Operator::next_batch`] the operator drains its
/// child completely, evaluates each sort key expression against every row
/// via the expression interpreter, sorts the rows with the key-order
/// specified by `keys`, and buffers the result. Subsequent calls emit the
/// sorted rows as 4096-row [`Batch`]es until the buffer is empty, then
/// return `Ok(None)`.
///
/// # Sort key semantics
///
/// - `asc = true`: ascending; `asc = false`: descending.
/// - `nulls_first = true`: NULLs precede non-NULLs.
/// - `nulls_first = false`: NULLs follow non-NULLs (PostgreSQL default for ASC).
///
/// Comparison across [`Value`] variants of the same type uses the natural
/// total order. Mixed-type comparisons are not supported at runtime and will
/// surface as [`ExecError::TypeMismatch`] if the expression evaluator
/// produces them.
///
/// # Send bound
///
/// The operator is `Send` because all owned types — `Box<dyn Operator>`,
/// `Vec<SortKey>`, `Schema`, and the row buffer — are `Send`.
#[derive(Debug)]
pub struct Sort {
    child: Box<dyn Operator>,
    /// Sort key descriptors with compiled evaluators.
    keys: Vec<CompiledKey>,
    schema: Schema,
    /// Sorted row buffer. `None` until the first `next_batch` call.
    sorted: Option<std::vec::IntoIter<Vec<Value>>>,
    /// External sorted-run cursor used when `work_mem` forces spill.
    external: Option<ExternalSortCursor>,
    /// Optional per-query memory budget.
    work_mem: Option<Arc<WorkMemBudget>>,
    /// Whether this sort wrote at least one sorted run to temp storage.
    spilled_to_disk: bool,
    /// `true` after the final `Ok(None)` is returned.
    eof: bool,
}

/// A sort key with its expression evaluator pre-compiled.
///
/// Keeps the evaluator alongside the direction and NULL placement so the
/// hot sort loop does not need to reconstruct it per comparison.
#[derive(Debug)]
struct CompiledKey {
    eval: Eval,
    asc: bool,
    nulls_first: bool,
}

impl Sort {
    /// Construct a sort operator.
    ///
    /// - `child` — the input operator to sort.
    /// - `keys` — sort key descriptors; each carries an expression, a
    ///   direction flag, and a NULL placement flag.
    /// - `schema` — the output schema; must match the child's schema since
    ///   sort is a non-projecting operator.
    #[must_use]
    pub fn new(child: Box<dyn Operator>, keys: Vec<SortKey>, schema: Schema) -> Self {
        let compiled = keys
            .into_iter()
            .map(|k| CompiledKey {
                eval: Eval::new(k.expr),
                asc: k.asc,
                nulls_first: k.nulls_first,
            })
            .collect();
        Self {
            child,
            keys: compiled,
            schema,
            sorted: None,
            external: None,
            work_mem: None,
            spilled_to_disk: false,
            eof: false,
        }
    }

    /// Attach a per-query work-memory budget.
    #[must_use]
    pub fn with_work_mem_budget(mut self, budget: Arc<WorkMemBudget>) -> Self {
        self.work_mem = Some(budget);
        self
    }

    /// Whether this execution wrote external sort runs.
    #[must_use]
    pub const fn spilled_to_disk(&self) -> bool {
        self.spilled_to_disk
    }
}

impl Operator for Sort {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        // Materialise and sort on the first call.
        if self.sorted.is_none() && self.external.is_none() {
            if self
                .work_mem
                .as_ref()
                .is_some_and(|budget| budget.limit_bytes() != u64::MAX)
            {
                self.build_external_or_memory_sort()?;
            } else {
                self.build_in_memory_sort()?;
            }
        }

        if self.external.is_some() {
            return self.next_external_batch();
        }

        let iter = self
            .sorted
            .as_mut()
            .ok_or(ExecError::Internal("sort output iterator missing"))?;
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

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.child.as_ref()]
    }

    fn spill_profile(&self) -> OperatorSpillProfile {
        if !self.spilled_to_disk {
            return OperatorSpillProfile::default();
        }
        self.external.as_ref().map_or(
            OperatorSpillProfile {
                spills: 1,
                bytes: 0,
            },
            ExternalSortCursor::spill_profile,
        )
    }

    fn io_bytes(&self) -> u64 {
        let spill = self.spill_profile();
        spill.bytes.saturating_mul(2)
    }
}

impl Sort {
    fn build_in_memory_sort(&mut self) -> Result<(), ExecError> {
        let mut rows: Vec<Vec<Value>> = Vec::new();

        loop {
            match self.child.next_batch()? {
                None => break,
                Some(batch) => {
                    let decoded = batch_to_rows(&batch, &self.schema)?;
                    rows.extend(decoded);
                }
            }
        }

        let sorted_rows = sorted_rows_from(rows, &self.keys)?;
        self.sorted = Some(sorted_rows.into_iter());
        Ok(())
    }

    fn build_external_or_memory_sort(&mut self) -> Result<(), ExecError> {
        let limit = self
            .work_mem
            .as_ref()
            .map_or(u64::MAX, |budget| budget.limit_bytes());
        let codec = RowCodec::new(self.schema.clone());
        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut bytes = 0_u64;
        let mut runs = Vec::new();

        loop {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            let decoded = batch_to_rows(&batch, &self.schema)?;
            for row in decoded {
                bytes = bytes.saturating_add(encoded_row_bytes(&codec, &row, "sort")?);
                rows.push(row);
                if bytes > limit {
                    runs.push(write_sorted_run(&mut rows, &codec, &self.keys, "sort")?);
                    bytes = 0;
                    self.spilled_to_disk = true;
                }
            }
        }

        if runs.is_empty() {
            let sorted_rows = sorted_rows_from(rows, &self.keys)?;
            self.sorted = Some(sorted_rows.into_iter());
        } else {
            if !rows.is_empty() {
                runs.push(write_sorted_run(&mut rows, &codec, &self.keys, "sort")?);
            }
            self.external = Some(ExternalSortCursor::new(runs, codec, &self.keys)?);
        }
        Ok(())
    }

    fn next_external_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        let external = self
            .external
            .as_mut()
            .ok_or(ExecError::Internal("external sort cursor missing"))?;
        let mut chunk = Vec::new();
        while chunk.len() < BATCH_TARGET_ROWS {
            let Some(row) = external.pop_next(&self.keys)? else {
                break;
            };
            chunk.push(row);
        }
        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&chunk, &self.schema).map(Some)
    }
}

#[derive(Debug)]
struct ExternalSortCursor {
    codec: RowCodec,
    runs: Vec<SortedRunCursor>,
}

impl ExternalSortCursor {
    fn new(
        runs: Vec<RowSpillFile>,
        codec: RowCodec,
        keys: &[CompiledKey],
    ) -> Result<Self, ExecError> {
        let mut cursors = Vec::with_capacity(runs.len());
        for mut spill in runs {
            spill.rewind()?;
            let head = read_sort_head(&mut spill, &codec, keys)?;
            if head.is_some() {
                cursors.push(SortedRunCursor { spill, head });
            }
        }
        Ok(Self {
            codec,
            runs: cursors,
        })
    }

    fn pop_next(&mut self, keys: &[CompiledKey]) -> Result<Option<Vec<Value>>, ExecError> {
        let Some(best_idx) = self.best_run_index(keys)? else {
            return Ok(None);
        };
        let head = self.runs[best_idx]
            .head
            .take()
            .ok_or(ExecError::Internal("external sort run head missing"))?;
        self.runs[best_idx].head =
            read_sort_head(&mut self.runs[best_idx].spill, &self.codec, keys)?;
        Ok(Some(head.row))
    }

    fn best_run_index(&self, keys: &[CompiledKey]) -> Result<Option<usize>, ExecError> {
        let mut best: Option<usize> = None;
        for (idx, run) in self.runs.iter().enumerate() {
            let Some(head) = &run.head else {
                continue;
            };
            let Some(best_idx) = best else {
                best = Some(idx);
                continue;
            };
            let Some(best_head) = self.runs[best_idx].head.as_ref() else {
                continue;
            };
            if try_compare_key_vecs(&head.key_values, &best_head.key_values, keys)?
                == Ordering::Less
            {
                best = Some(idx);
            }
        }
        Ok(best)
    }

    fn spill_profile(&self) -> OperatorSpillProfile {
        self.runs
            .iter()
            .fold(OperatorSpillProfile::default(), |mut acc, run| {
                acc.spills = acc.spills.saturating_add(1);
                acc.bytes = acc.bytes.saturating_add(run.spill.bytes());
                acc
            })
    }
}

#[derive(Debug)]
struct SortedRunCursor {
    spill: RowSpillFile,
    head: Option<SortHead>,
}

#[derive(Debug)]
struct SortHead {
    row: Vec<Value>,
    key_values: Vec<Value>,
}

fn read_sort_head(
    spill: &mut RowSpillFile,
    codec: &RowCodec,
    keys: &[CompiledKey],
) -> Result<Option<SortHead>, ExecError> {
    let Some(row) = spill.read_next_row(codec)? else {
        return Ok(None);
    };
    let key_values = eval_sort_keys(&row, keys)?;
    Ok(Some(SortHead { row, key_values }))
}

fn write_sorted_run(
    rows: &mut Vec<Vec<Value>>,
    codec: &RowCodec,
    keys: &[CompiledKey],
    label: &'static str,
) -> Result<RowSpillFile, ExecError> {
    let sorted_rows = sorted_rows_from(std::mem::take(rows), keys)?;
    let mut spill = RowSpillFile::new(label)?;
    for row in sorted_rows {
        spill.append_row(codec, &row)?;
    }
    Ok(spill)
}

fn sorted_rows_from(
    rows: Vec<Vec<Value>>,
    keys: &[CompiledKey],
) -> Result<Vec<Vec<Value>>, ExecError> {
    let mut annotated: Vec<(Vec<Value>, Vec<Value>)> = rows
        .into_iter()
        .map(|row| {
            let key_vals = eval_sort_keys(&row, keys)?;
            Ok((row, key_vals))
        })
        .collect::<Result<_, ExecError>>()?;
    validate_sort_key_matrix(&annotated, keys)?;
    annotated.sort_by(|(_, ak), (_, bk)| compare_key_vecs(ak, bk, keys));
    Ok(annotated.into_iter().map(|(row, _)| row).collect())
}

fn eval_sort_keys(row: &[Value], keys: &[CompiledKey]) -> Result<Vec<Value>, ExecError> {
    keys.iter()
        .map(|key| {
            key.eval
                .eval(row)
                .map_err(|err| ExecError::TypeMismatch(err.to_string()))
        })
        .collect()
}

/// Compare two key-value vectors using the compiled sort key descriptors.
///
/// Returns [`Ordering`] suitable for `sort_by`. Each key is compared in
/// declaration order; the first non-equal result wins. NULLs are ordered
/// per `nulls_first`: `true` places NULL before any non-NULL value;
/// `false` places NULL after all non-NULL values.
fn compare_key_vecs(ak: &[Value], bk: &[Value], keys: &[CompiledKey]) -> Ordering {
    try_compare_key_vecs(ak, bk, keys).unwrap_or(Ordering::Equal)
}

fn try_compare_key_vecs(
    ak: &[Value],
    bk: &[Value],
    keys: &[CompiledKey],
) -> Result<Ordering, ExecError> {
    for (i, key) in keys.iter().enumerate() {
        let av = &ak[i];
        let bv = &bk[i];
        let ord = try_compare_values_nullable(av, bv, key.nulls_first)?;
        let ord = if key.asc { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(Ordering::Equal)
}

fn validate_sort_key_matrix(
    annotated: &[(Vec<Value>, Vec<Value>)],
    keys: &[CompiledKey],
) -> Result<(), ExecError> {
    for key_idx in 0..keys.len() {
        let mut first_non_null: Option<&Value> = None;
        for (_, key_values) in annotated {
            let Some(value) = key_values.get(key_idx) else {
                return Err(ExecError::Internal("sort key arity mismatch"));
            };
            if value.is_null() {
                continue;
            }
            if let Some(first) = first_non_null {
                try_compare_values_nullable(first, value, keys[key_idx].nulls_first)?;
            } else {
                try_compare_values_nullable(value, value, keys[key_idx].nulls_first)?;
                first_non_null = Some(value);
            }
        }
    }
    Ok(())
}

/// Compare two [`Value`]s with explicit NULL ordering.
///
/// - `nulls_first = true` : NULL is less than any non-NULL value.
/// - `nulls_first = false`: NULL is greater than any non-NULL value.
/// - NULL vs NULL: `Equal`.
/// - Non-NULL vs non-NULL: natural total order of the value type.
#[allow(unreachable_pub)]
pub fn compare_values_nullable(a: &Value, b: &Value, nulls_first: bool) -> Ordering {
    try_compare_values_nullable(a, b, nulls_first).unwrap_or(Ordering::Equal)
}

/// Compare two [`Value`]s with explicit NULL ordering and checked type support.
///
/// Returns [`ExecError::TypeMismatch`] when non-NULL values do not share an
/// order-compatible scalar family. Operators that can report execution errors
/// should use this path instead of treating unsupported values as equal.
#[allow(unreachable_pub)]
pub fn try_compare_values_nullable(
    a: &Value,
    b: &Value,
    nulls_first: bool,
) -> Result<Ordering, ExecError> {
    match (a, b) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Null, _) => {
            if nulls_first {
                Ok(Ordering::Less)
            } else {
                Ok(Ordering::Greater)
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Ok(Ordering::Greater)
            } else {
                Ok(Ordering::Less)
            }
        }
        _ => compare_non_null_values(a, b).ok_or_else(|| {
            ExecError::TypeMismatch(format!(
                "values of type {:?} and {:?} are not orderable together",
                a.data_type(),
                b.data_type()
            ))
        }),
    }
}

fn compare_non_null_values(a: &Value, b: &Value) -> Option<Ordering> {
    Some(match (a, b) {
        (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
        (Value::Int16(l), Value::Int16(r)) => l.cmp(r),
        (Value::Int32(l), Value::Int32(r)) => l.cmp(r),
        (Value::Int64(l), Value::Int64(r)) => l.cmp(r),
        (Value::Oid(l), Value::Oid(r))
        | (Value::RegClass(l), Value::RegClass(r))
        | (Value::RegType(l), Value::RegType(r)) => l.cmp(r),
        (Value::PgLsn(l), Value::PgLsn(r)) => l.cmp(r),
        (Value::Float32(l), Value::Float32(r)) => compare_f32_sql(*l, *r),
        (Value::Float64(l), Value::Float64(r)) => compare_f64_sql(*l, *r),
        (Value::Money(l), Value::Money(r)) => l.cmp(r),
        (Value::Uuid(l), Value::Uuid(r)) => l.cmp(r),
        (Value::Text(l), Value::Text(r)) => l.cmp(r),
        (Value::Json(l), Value::Json(r))
        | (Value::Jsonb(l), Value::Jsonb(r))
        | (Value::Json(l), Value::Jsonb(r))
        | (Value::Jsonb(l), Value::Json(r))
        | (Value::Xml(l), Value::Xml(r)) => l.cmp(r),
        (Value::Char(l), Value::Char(r)) => bpchar_semantic_text(l).cmp(bpchar_semantic_text(r)),
        (Value::Char(l), Value::Text(r)) => bpchar_semantic_text(l).cmp(r),
        (Value::Text(l), Value::Char(r)) => l.as_str().cmp(bpchar_semantic_text(r)),
        (Value::Bytea(l), Value::Bytea(r)) => l.cmp(r),
        (
            Value::BitVec {
                dims: left_dims,
                bytes: left_bytes,
            },
            Value::BitVec {
                dims: right_dims,
                bytes: right_bytes,
            },
        ) => left_dims
            .cmp(right_dims)
            .then_with(|| left_bytes.cmp(right_bytes)),
        (Value::BitString(l), Value::BitString(r)) => l.to_bit_text().cmp(&r.to_bit_text()),
        (Value::Network(l), Value::Network(r)) => (*l)
            .cmp_network(*r)
            .unwrap_or_else(|| l.to_string().cmp(&r.to_string())),
        (Value::Timestamp(l), Value::Timestamp(r))
        | (Value::TimestampTz(l), Value::TimestampTz(r))
        | (Value::Time(l), Value::Time(r)) => l.cmp(r),
        (
            Value::TimeTz {
                micros: lm,
                offset_seconds: lo,
            },
            Value::TimeTz {
                micros: rm,
                offset_seconds: ro,
            },
        ) => timetz_utc_micros(*lm, *lo).cmp(&timetz_utc_micros(*rm, *ro)),
        (Value::Date(l), Value::Date(r)) => l.cmp(r),
        (
            Value::Decimal {
                value: l,
                scale: l_scale,
            },
            Value::Decimal {
                value: r,
                scale: r_scale,
            },
        ) => compare_decimals(*l, *l_scale, *r, *r_scale),
        (
            Value::Interval {
                months: lm,
                days: ld,
                microseconds: lus,
            },
            Value::Interval {
                months: rm,
                days: rd,
                microseconds: rus,
            },
        ) => interval_total_micros(*lm, *ld, *lus).cmp(&interval_total_micros(*rm, *rd, *rus)),
        _ => return None,
    })
}

fn interval_total_micros(months: i32, days: i32, microseconds: i64) -> i128 {
    const MICROS_PER_DAY: i128 = 86_400_000_000;
    (i128::from(months) * 30 + i128::from(days)) * MICROS_PER_DAY + i128::from(microseconds)
}

/// Compare `REAL` values using SQL-compatible NaN ordering.
///
/// Finite values keep normal numeric order; `NaN` sorts after every finite
/// value and compares equal to other `NaN` values for ordering purposes.
#[allow(unreachable_pub)]
pub fn compare_f32_sql(left: f32, right: f32) -> Ordering {
    compare_f64_sql(f64::from(left), f64::from(right))
}

/// Compare `DOUBLE PRECISION` values using SQL-compatible NaN ordering.
///
/// Finite values keep normal numeric order; `NaN` sorts after every finite
/// value and compares equal to other `NaN` values for ordering purposes.
#[allow(unreachable_pub)]
pub fn compare_f64_sql(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
    }
}

fn compare_decimals(l: i64, l_scale: i32, r: i64, r_scale: i32) -> Ordering {
    match (l.cmp(&0), r.cmp(&0)) {
        (Ordering::Equal, Ordering::Equal) => return Ordering::Equal,
        (Ordering::Equal, Ordering::Less) | (Ordering::Greater, Ordering::Less) => {
            return Ordering::Greater;
        }
        (Ordering::Less, Ordering::Equal) | (Ordering::Less, Ordering::Greater) => {
            return Ordering::Less;
        }
        _ => {}
    }

    let left = DecimalMagnitude::new(l, l_scale);
    let right = DecimalMagnitude::new(r, r_scale);
    let magnitude_order = left.cmp_abs(&right);
    if l < 0 {
        magnitude_order.reverse()
    } else {
        magnitude_order
    }
}

#[derive(Debug)]
struct DecimalMagnitude {
    digits: String,
    integer_digits: i64,
}

impl DecimalMagnitude {
    fn new(value: i64, scale: i32) -> Self {
        let mut magnitude = i128::from(value);
        if magnitude < 0 {
            magnitude = -magnitude;
        }
        let mut scale = i64::from(scale);
        while magnitude != 0 && magnitude % 10 == 0 {
            magnitude /= 10;
            scale = scale.saturating_sub(1);
        }
        let digits = magnitude.to_string();
        let digit_count = i64::try_from(digits.len()).unwrap_or(i64::MAX);
        Self {
            digits,
            integer_digits: digit_count.saturating_sub(scale),
        }
    }

    fn cmp_abs(&self, other: &Self) -> Ordering {
        match self.integer_digits.cmp(&other.integer_digits) {
            Ordering::Equal => {}
            non_equal => return non_equal,
        }

        let max_len = self.digits.len().max(other.digits.len());
        let left = self.digits.as_bytes();
        let right = other.digits.as_bytes();
        for idx in 0..max_len {
            let l = left.get(idx).copied().unwrap_or(b'0');
            let r = right.get(idx).copied().unwrap_or(b'0');
            match l.cmp(&r) {
                Ordering::Equal => {}
                non_equal => return non_equal,
            }
        }
        Ordering::Equal
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, ScalarExpr, SortKey};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::{Sort, compare_decimals, compare_values_nullable, try_compare_values_nullable};
    use crate::filter_op::batch_to_rows;
    use crate::mem_table_scan::MemTableScan;
    use crate::seq_scan::build_batch;
    use crate::{Operator, WorkMemBudget};

    // -------------------------------------------------------------------------
    // Test helpers
    // -------------------------------------------------------------------------

    fn schema_i32_i64() -> Schema {
        Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("b", DataType::Int64),
        ])
        .expect("schema ok")
    }

    fn make_batch(rows: &[(i32, i64)]) -> Batch {
        let as_: Vec<i32> = rows.iter().map(|(a, _)| *a).collect();
        let bs: Vec<i64> = rows.iter().map(|(_, b)| *b).collect();
        Batch::new([
            Column::Int32(NumericColumn::from_data(as_)),
            Column::Int64(NumericColumn::from_data(bs)),
        ])
        .expect("batch ok")
    }

    fn col_a() -> ScalarExpr {
        ScalarExpr::Column {
            name: "a".into(),
            index: 0,
            data_type: DataType::Int32,
        }
    }

    fn col_b() -> ScalarExpr {
        ScalarExpr::Column {
            name: "b".into(),
            index: 1,
            data_type: DataType::Int64,
        }
    }

    fn divide_by_zero_key() -> ScalarExpr {
        ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(col_a()),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int32(0),
                data_type: DataType::Int32,
            }),
            data_type: DataType::Int32,
        }
    }

    fn drain_rows(op: &mut dyn Operator) -> Vec<(i32, i64)> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("no error") {
            let cols = b.columns();
            match (&cols[0], &cols[1]) {
                (Column::Int32(a), Column::Int64(b)) => {
                    for (&av, &bv) in a.data().iter().zip(b.data().iter()) {
                        out.push((av, bv));
                    }
                }
                _ => panic!("unexpected columns"),
            }
        }
        out
    }

    // -------------------------------------------------------------------------
    // Test 1: happy path — ascending sort produces expected order
    // -------------------------------------------------------------------------

    #[test]
    fn sort_ascending_produces_correct_order() {
        let schema = schema_i32_i64();
        let input = vec![make_batch(&[(3, 30), (1, 10), (4, 40), (2, 20)])];
        let scan = MemTableScan::new(schema.clone(), input);
        let keys = vec![SortKey {
            expr: col_a(),
            asc: true,
            nulls_first: false,
        }];
        let mut sort = Sort::new(Box::new(scan), keys, schema);
        let rows = drain_rows(&mut sort);
        assert_eq!(rows, vec![(1, 10), (2, 20), (3, 30), (4, 40)]);
    }

    #[test]
    fn sort_key_eval_error_propagates() {
        let schema = schema_i32_i64();
        let input = vec![make_batch(&[(1, 10), (2, 20)])];
        let scan = MemTableScan::new(schema.clone(), input);
        let keys = vec![SortKey {
            expr: divide_by_zero_key(),
            asc: true,
            nulls_first: false,
        }];
        let mut sort = Sort::new(Box::new(scan), keys, schema);
        let err = sort.next_batch().expect_err("sort key error must surface");
        assert!(
            err.to_string().contains("division by zero"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn sort_rejects_unsupported_order_value() {
        let schema = schema_i32_i64();
        let input = vec![make_batch(&[(1, 10), (2, 20)])];
        let scan = MemTableScan::new(schema.clone(), input);
        let keys = vec![SortKey {
            expr: ScalarExpr::Literal {
                value: Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(1)],
                },
                data_type: DataType::Array(Box::new(DataType::Int32)),
            },
            asc: true,
            nulls_first: false,
        }];
        let mut sort = Sort::new(Box::new(scan), keys, schema);
        let err = sort
            .next_batch()
            .expect_err("unsupported sort key must surface");
        assert!(
            err.to_string().contains("not orderable"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn decimal_compare_does_not_treat_scale_overflow_as_equal() {
        assert_eq!(compare_decimals(1, 0, 2, 100), Ordering::Greater);
        assert_eq!(compare_decimals(-1, 0, -2, 100), Ordering::Less);
    }

    #[test]
    fn bytea_values_compare_lexicographically() {
        assert_eq!(
            compare_values_nullable(&Value::Bytea(vec![0x02]), &Value::Bytea(vec![0x01]), false),
            Ordering::Greater
        );
    }

    #[test]
    fn float_nan_sorts_after_finite_values() {
        assert_eq!(
            compare_values_nullable(&Value::Float64(f64::NAN), &Value::Float64(1.0), false),
            Ordering::Greater
        );
        assert_eq!(
            compare_values_nullable(&Value::Float32(f32::NAN), &Value::Float32(1.0), false),
            Ordering::Greater
        );
    }

    #[test]
    fn checked_compare_rejects_unsupported_order_values() {
        let left = Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(1)],
        };
        let right = Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(2)],
        };
        let err = try_compare_values_nullable(&left, &right, false)
            .expect_err("arrays are not orderable");
        assert!(
            err.to_string().contains("not orderable"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn sort_spills_to_disk_when_work_mem_is_too_small() {
        let schema = schema_i32_i64();
        let input = vec![
            make_batch(&[(7, 70), (1, 10), (5, 50)]),
            make_batch(&[(2, 20), (6, 60), (3, 30), (4, 40)]),
        ];
        let scan = MemTableScan::new(schema.clone(), input);
        let keys = vec![SortKey {
            expr: col_a(),
            asc: true,
            nulls_first: false,
        }];
        let mut sort = Sort::new(Box::new(scan), keys, schema)
            .with_work_mem_budget(std::sync::Arc::new(WorkMemBudget::new(1)));

        let rows = drain_rows(&mut sort);

        assert_eq!(
            rows,
            vec![
                (1, 10),
                (2, 20),
                (3, 30),
                (4, 40),
                (5, 50),
                (6, 60),
                (7, 70)
            ]
        );
        assert!(sort.spilled_to_disk(), "sort must switch to external runs");
    }

    // -------------------------------------------------------------------------
    // Test 2: empty input returns None immediately
    // -------------------------------------------------------------------------

    #[test]
    fn sort_empty_input_returns_none() {
        let schema = schema_i32_i64();
        let scan = MemTableScan::new(schema.clone(), vec![]);
        let keys = vec![SortKey {
            expr: col_a(),
            asc: true,
            nulls_first: false,
        }];
        let mut sort = Sort::new(Box::new(scan), keys, schema);
        assert!(sort.next_batch().unwrap().is_none());
    }

    // -------------------------------------------------------------------------
    // Test 3: NULL ordering — compare_values_nullable unit test
    // -------------------------------------------------------------------------

    #[test]
    fn sort_null_ordering_semantics() {
        assert_eq!(
            compare_values_nullable(&Value::Null, &Value::Null, true),
            Ordering::Equal,
            "NULL vs NULL is always Equal"
        );
        assert_eq!(
            compare_values_nullable(&Value::Null, &Value::Int32(1), true),
            Ordering::Less,
            "nulls_first=true: NULL < non-NULL"
        );
        assert_eq!(
            compare_values_nullable(&Value::Null, &Value::Int32(1), false),
            Ordering::Greater,
            "nulls_first=false: NULL > non-NULL"
        );
        assert_eq!(
            compare_values_nullable(&Value::Int32(1), &Value::Null, false),
            Ordering::Less,
            "nulls_first=false: non-NULL < NULL"
        );
    }

    #[test]
    fn sort_compares_date_and_decimal_values() {
        assert_eq!(
            compare_values_nullable(&Value::Date(0), &Value::Date(1), false),
            Ordering::Less
        );
        assert_eq!(
            compare_values_nullable(
                &Value::Decimal {
                    value: 100,
                    scale: 1,
                },
                &Value::Decimal {
                    value: 999,
                    scale: 2,
                },
                false,
            ),
            Ordering::Greater
        );
    }

    #[test]
    fn sort_mixed_q2_style_keys() {
        let schema = Schema::new([
            Field::required(
                "acctbal",
                DataType::Decimal {
                    precision: Some(15),
                    scale: Some(2),
                },
            ),
            Field::required("nation", DataType::Text { max_len: None }),
            Field::required("supplier", DataType::Text { max_len: None }),
            Field::required("partkey", DataType::Int32),
        ])
        .expect("schema ok");
        let rows = vec![
            vec![
                Value::Decimal {
                    value: 931_297,
                    scale: 2,
                },
                Value::Text("RUSSIA".into()),
                Value::Text("Supplier#000007807".into()),
                Value::Int32(100_276),
            ],
            vec![
                Value::Decimal {
                    value: 931_297,
                    scale: 2,
                },
                Value::Text("RUSSIA".into()),
                Value::Text("Supplier#000007807".into()),
                Value::Int32(90_279),
            ],
        ];
        let batch = build_batch(&rows, &schema).expect("batch ok");
        let scan = MemTableScan::new(schema.clone(), vec![batch]);
        let keys = vec![
            SortKey {
                expr: ScalarExpr::Column {
                    name: "acctbal".into(),
                    index: 0,
                    data_type: schema.field_at(0).data_type.clone(),
                },
                asc: false,
                nulls_first: true,
            },
            SortKey {
                expr: ScalarExpr::Column {
                    name: "nation".into(),
                    index: 1,
                    data_type: schema.field_at(1).data_type.clone(),
                },
                asc: true,
                nulls_first: false,
            },
            SortKey {
                expr: ScalarExpr::Column {
                    name: "supplier".into(),
                    index: 2,
                    data_type: schema.field_at(2).data_type.clone(),
                },
                asc: true,
                nulls_first: false,
            },
            SortKey {
                expr: ScalarExpr::Column {
                    name: "partkey".into(),
                    index: 3,
                    data_type: DataType::Int32,
                },
                asc: true,
                nulls_first: false,
            },
        ];
        let mut sort = Sort::new(Box::new(scan), keys, schema);
        let batch = sort
            .next_batch()
            .expect("sort ok")
            .expect("one output batch");
        let rows = batch_to_rows(&batch, sort.schema()).expect("decode rows");

        assert_eq!(rows[0][3], Value::Int32(90_279));
        assert_eq!(rows[1][3], Value::Int32(100_276));
    }

    // -------------------------------------------------------------------------
    // Test 4: multi-key sort (secondary key breaks ties)
    // -------------------------------------------------------------------------

    #[test]
    fn sort_multi_key_secondary_breaks_ties() {
        let schema = schema_i32_i64();
        let input = vec![make_batch(&[(2, 30), (1, 20), (2, 10), (1, 40)])];
        let scan = MemTableScan::new(schema.clone(), input);
        let keys = vec![
            SortKey {
                expr: col_a(),
                asc: true,
                nulls_first: false,
            },
            SortKey {
                expr: col_b(),
                asc: true,
                nulls_first: false,
            },
        ];
        let mut sort = Sort::new(Box::new(scan), keys, schema);
        let rows = drain_rows(&mut sort);
        // Primary: a ASC, secondary: b ASC
        assert_eq!(rows, vec![(1, 20), (1, 40), (2, 10), (2, 30)]);
    }

    // -------------------------------------------------------------------------
    // Property test: output is always sorted ascending on column `a`
    // -------------------------------------------------------------------------

    proptest::proptest! {
        #[test]
        fn prop_sort_output_is_ordered(mut values in proptest::collection::vec(i32::MIN..=i32::MAX, 0..256usize)) {
            let schema = schema_i32_i64();
            let rows: Vec<(i32, i64)> = values.iter().copied().map(|v| (v, i64::from(v))).collect();
            let scan = MemTableScan::new(schema.clone(), vec![make_batch(&rows)]);
            let keys = vec![SortKey { expr: col_a(), asc: true, nulls_first: false }];
            let mut sort = Sort::new(Box::new(scan), keys, schema);
            let out = drain_rows(&mut sort);
            let out_ids: Vec<i32> = out.iter().map(|(a, _)| *a).collect();
            values.sort_unstable();
            proptest::prop_assert_eq!(out_ids, values, "Sort output must be non-decreasing");
        }
    }

    // -------------------------------------------------------------------------
    // Test 5: output is chunked into 4096-row batches
    // -------------------------------------------------------------------------

    #[test]
    fn sort_chunks_output_into_4096_row_batches() {
        let schema = schema_i32_i64();
        let total: usize = 4100;
        let row_data: Vec<(i32, i64)> = (0..i32::try_from(total).expect("fits"))
            .rev()
            .map(|i| (i, i64::from(i)))
            .collect();
        let input = vec![make_batch(&row_data)];
        let scan = MemTableScan::new(schema.clone(), input);
        let keys = vec![SortKey {
            expr: col_a(),
            asc: true,
            nulls_first: false,
        }];
        let mut sort = Sort::new(Box::new(scan), keys, schema);

        let mut batch_sizes: Vec<usize> = Vec::new();
        while let Some(b) = sort.next_batch().unwrap() {
            batch_sizes.push(b.rows());
        }

        let scanned: usize = batch_sizes.iter().sum();
        assert_eq!(scanned, total);
        assert!(batch_sizes.contains(&4096));
    }
}
