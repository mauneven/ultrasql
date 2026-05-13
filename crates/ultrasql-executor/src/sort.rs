//! In-memory sort operator.
//!
//! Drains all input on the first [`Operator::next_batch`] call, evaluates
//! sort keys per row via [`Eval`], sorts the rows using the documented
//! PostgreSQL ordering semantics, then emits the result in 4096-row chunks.
//!
//! # Ordering semantics
//!
//! Per key: `ASC` + `NULLS LAST` is the PostgreSQL default (NULLs sort
//! after non-NULLs in ascending order). `DESC` + `NULLS FIRST` is the
//! PostgreSQL `DESC` default (NULLs sort before non-NULLs in descending
//! order). Each [`SortKey`] carries explicit `asc` and `nulls_first` flags
//! so the caller controls both independently.
//!
//! # v0.5 limitation
//!
//! The entire input is materialised in memory before the first row is
//! emitted. An external spill path will be added in v0.6 when the per-query
//! `work_mem` budget is enforced.
//!
//! TODO(spill): external sort for v0.6.

use std::cmp::Ordering;

use ultrasql_core::{Schema, Value};
use ultrasql_planner::SortKey;
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

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
            eof: false,
        }
    }
}

impl Operator for Sort {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        // Materialise and sort on the first call.
        if self.sorted.is_none() {
            let mut rows: Vec<Vec<Value>> = Vec::new();

            // Drain the child completely.
            loop {
                match self.child.next_batch()? {
                    None => break,
                    Some(batch) => {
                        let decoded = batch_to_rows(&batch, &self.schema)?;
                        rows.extend(decoded);
                    }
                }
            }

            // Pre-compute all key values once to avoid re-evaluating during
            // comparisons. Each entry is `(row, [key0_val, key1_val, ...])`.
            let mut annotated: Vec<(Vec<Value>, Vec<Value>)> = rows
                .into_iter()
                .map(|row| {
                    let key_vals: Vec<Value> = self
                        .keys
                        .iter()
                        .map(|k| k.eval.eval(&row).unwrap_or(Value::Null))
                        .collect();
                    (row, key_vals)
                })
                .collect();

            // Sort by the pre-computed key vectors.
            let keys = &self.keys;
            annotated.sort_by(|(_, ak), (_, bk)| compare_key_vecs(ak, bk, keys));

            #[allow(clippy::needless_collect)] // IntoIter needed for Debug-able field type
            let sorted_rows: Vec<Vec<Value>> = annotated.into_iter().map(|(row, _)| row).collect();
            self.sorted = Some(sorted_rows.into_iter());
        }

        let iter = self.sorted.as_mut().expect("just-set above");
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

/// Compare two key-value vectors using the compiled sort key descriptors.
///
/// Returns [`Ordering`] suitable for `sort_by`. Each key is compared in
/// declaration order; the first non-equal result wins. NULLs are ordered
/// per `nulls_first`: `true` places NULL before any non-NULL value;
/// `false` places NULL after all non-NULL values.
fn compare_key_vecs(ak: &[Value], bk: &[Value], keys: &[CompiledKey]) -> Ordering {
    for (i, key) in keys.iter().enumerate() {
        let av = &ak[i];
        let bv = &bk[i];
        let ord = compare_values_nullable(av, bv, key.nulls_first);
        let ord = if key.asc { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Compare two [`Value`]s with explicit NULL ordering.
///
/// - `nulls_first = true` : NULL is less than any non-NULL value.
/// - `nulls_first = false`: NULL is greater than any non-NULL value.
/// - NULL vs NULL: `Equal`.
/// - Non-NULL vs non-NULL: natural total order of the value type.
#[allow(unreachable_pub)]
pub fn compare_values_nullable(a: &Value, b: &Value, nulls_first: bool) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
        (Value::Int16(l), Value::Int16(r)) => l.cmp(r),
        (Value::Int32(l), Value::Int32(r)) => l.cmp(r),
        (Value::Int64(l), Value::Int64(r)) => l.cmp(r),
        (Value::Float32(l), Value::Float32(r)) => l.partial_cmp(r).unwrap_or(Ordering::Equal),
        (Value::Float64(l), Value::Float64(r)) => l.partial_cmp(r).unwrap_or(Ordering::Equal),
        (Value::Text(l), Value::Text(r)) => l.cmp(r),
        // Mixed types or unsupported types: treat as equal to avoid panics.
        // The planner/binder is responsible for preventing mixed-type keys.
        _ => Ordering::Equal,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{ScalarExpr, SortKey};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::{Sort, compare_values_nullable};
    use crate::Operator;
    use crate::mem_table_scan::MemTableScan;

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
