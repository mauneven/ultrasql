//! Hash equi-join operator.
//!
//! Implements Inner and Left Outer equi-joins using a classical build+probe
//! hash table. The left (build) side is drained first and hashed by the
//! build key expression; the right (probe) side is then streamed, and each
//! right row's probe key is looked up in the hash table.
//!
//! # Join type support
//!
//! | Join type   | Status |
//! |-------------|--------|
//! | `Inner`     | Supported. |
//! | `LeftOuter` | Supported: unmatched left rows are emitted with NULL right columns at the end of the probe phase. |
//! | `RightOuter`, `FullOuter`, `Cross` | Return [`ExecError::Unsupported`] — pending wave 6. |
//!
//! # NULL key semantics
//!
//! NULL keys never match (SQL standard: `NULL = NULL` is unknown, not true).
//! Rows with a NULL build key are placed in the hash table under a
//! `Value::Null` bucket but are never returned because the probe lookup also
//! skips NULL probe keys.
//!
//! # Duplicate build keys
//!
//! Multiple left rows with the same (non-NULL) key are all stored; the probe
//! emits one output row per (right, left) pair.

use std::collections::HashMap;

use ultrasql_core::{Schema, Value};
use ultrasql_planner::{LogicalJoinType, ScalarExpr};
use ultrasql_vec::Batch;

use crate::eval::Eval;
use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

/// Maximum rows per emitted batch, matching the `ARCHITECTURE.md` section 9 contract.
const BATCH_TARGET_ROWS: usize = 4096;

/// Hash equi-join operator.
///
/// Performs a two-phase hash join:
///
/// 1. **Build phase** — drain `left`, hash each row by `left_key`.
/// 2. **Probe phase** — stream `right`, look up each row's `right_key` in the
///    hash table, emit matching pairs.
///
/// After the probe phase, unmatched left rows are emitted (for `LeftOuter`).
///
/// # Send bound
///
/// All owned fields are `Send`: `Box<dyn Operator>`, `Eval`, `Schema`, and
/// `HashMap`.
#[derive(Debug)]
pub struct HashJoin {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    left_key_eval: Eval,
    right_key_eval: Eval,
    join_type: LogicalJoinType,
    schema: Schema,
    left_schema: Schema,
    right_schema: Schema,
    /// Output row buffer. `None` until the build+probe phases complete.
    output: Option<std::vec::IntoIter<Vec<Value>>>,
    /// `true` after the final `Ok(None)` is returned.
    eof: bool,
}

impl HashJoin {
    /// Construct a hash join operator.
    ///
    /// - `left` — the build side.
    /// - `right` — the probe side.
    /// - `left_key` — expression evaluated over left rows to produce the build key.
    /// - `right_key` — expression evaluated over right rows to produce the probe key.
    /// - `join_type` — must be `Inner` or `LeftOuter`; other variants return
    ///   `ExecError::Unsupported` at runtime.
    /// - `schema` — output schema (left columns followed by right columns).
    /// - `left_schema` — schema of the left child's output.
    /// - `right_schema` — schema of the right child's output.
    #[must_use]
    #[allow(clippy::too_many_arguments)] // all 8 parameters are distinct logical inputs
    pub fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        left_key: ScalarExpr,
        right_key: ScalarExpr,
        join_type: LogicalJoinType,
        schema: Schema,
        left_schema: Schema,
        right_schema: Schema,
    ) -> Self {
        Self {
            left,
            right,
            left_key_eval: Eval::new(left_key),
            right_key_eval: Eval::new(right_key),
            join_type,
            schema,
            left_schema,
            right_schema,
            output: None,
            eof: false,
        }
    }
}

impl Operator for HashJoin {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        if self.output.is_none() {
            let rows = self.execute()?;
            self.output = Some(rows.into_iter());
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

impl HashJoin {
    /// Execute the full build+probe and return all output rows.
    fn execute(&mut self) -> Result<Vec<Vec<Value>>, ExecError> {
        // Validate join type early so the error surfaces before doing any work.
        match self.join_type {
            LogicalJoinType::Inner | LogicalJoinType::LeftOuter => {}
            LogicalJoinType::RightOuter => {
                return Err(ExecError::Unsupported(
                    "hash join outer variant pending: RightOuter",
                ));
            }
            LogicalJoinType::FullOuter => {
                return Err(ExecError::Unsupported(
                    "hash join outer variant pending: FullOuter",
                ));
            }
            LogicalJoinType::Cross => {
                return Err(ExecError::Unsupported(
                    "hash join outer variant pending: Cross (use NestedLoopJoin)",
                ));
            }
        }

        // ----- Build phase -----
        // Key: left key value. Value: (row_index, row_data).
        // We use a multi-map: HashMap<Value, Vec<usize>> + a row array.
        let mut left_rows: Vec<Vec<Value>> = Vec::new();
        let mut hash_table: HashMap<OrderedValue, Vec<usize>> = HashMap::new();

        loop {
            let Some(batch) = self.left.next_batch()? else {
                break;
            };
            let rows = batch_to_rows(&batch, &self.left_schema)?;
            for row in rows {
                let key = self.left_key_eval.eval(&row).unwrap_or(Value::Null);
                if !key.is_null() {
                    hash_table
                        .entry(OrderedValue(key))
                        .or_default()
                        .push(left_rows.len());
                }
                left_rows.push(row);
            }
        }

        // ----- Probe phase -----
        let null_right = vec![Value::Null; self.right_schema.len()];
        // Track which left rows were matched (for LeftOuter).
        let mut left_matched = vec![false; left_rows.len()];

        let mut output: Vec<Vec<Value>> = Vec::new();

        loop {
            let Some(batch) = self.right.next_batch()? else {
                break;
            };
            let rows = batch_to_rows(&batch, &self.right_schema)?;
            for right_row in &rows {
                let probe_key = self.right_key_eval.eval(right_row).unwrap_or(Value::Null);
                if probe_key.is_null() {
                    // NULL keys never match.
                    continue;
                }
                if let Some(indices) = hash_table.get(&OrderedValue(probe_key)) {
                    for &li in indices {
                        left_matched[li] = true;
                        let joined = concat_rows(&left_rows[li], right_row);
                        output.push(joined);
                    }
                }
            }
        }

        // LeftOuter: emit unmatched left rows with NULL right padding.
        if self.join_type == LogicalJoinType::LeftOuter {
            for (li, matched) in left_matched.iter().enumerate() {
                if !matched {
                    output.push(concat_rows(&left_rows[li], &null_right));
                }
            }
        }

        Ok(output)
    }
}

// ---------------------------------------------------------------------------
// Hash-map key wrapper
// ---------------------------------------------------------------------------

/// A wrapper around [`Value`] that implements `Hash + Eq` so it can serve
/// as a `HashMap` key.
///
/// `Value` itself does not implement `Hash` because `f32`/`f64` are not
/// `Hash` (NaN != NaN). We implement an approximate hash that is consistent
/// with the join semantics: NaN values compare equal to themselves here.
#[derive(Debug)]
struct OrderedValue(Value);

impl PartialEq for OrderedValue {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            // Bit-pattern equality for floats so NaN == NaN in hash tables.
            (Value::Float32(a), Value::Float32(b)) => a.to_bits() == b.to_bits(),
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            _ => self.0 == other.0,
        }
    }
}

impl Eq for OrderedValue {}

impl std::hash::Hash for OrderedValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.0 {
            Value::Null => state.write_u8(0),
            Value::Bool(b) => {
                state.write_u8(1);
                b.hash(state);
            }
            Value::Int16(v) => {
                state.write_u8(2);
                v.hash(state);
            }
            Value::Int32(v) => {
                state.write_u8(3);
                v.hash(state);
            }
            Value::Int64(v) => {
                state.write_u8(4);
                v.hash(state);
            }
            Value::Float32(v) => {
                state.write_u8(5);
                // Hash the bit pattern so NaN is stable.
                v.to_bits().hash(state);
            }
            Value::Float64(v) => {
                state.write_u8(6);
                v.to_bits().hash(state);
            }
            Value::Text(s) => {
                state.write_u8(7);
                s.hash(state);
            }
            Value::Bytea(b) => {
                state.write_u8(8);
                b.hash(state);
            }
            Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => {
                state.write_u8(9);
                v.hash(state);
            }
            Value::Date(v) => {
                state.write_u8(10);
                v.hash(state);
            }
            Value::Uuid(u) => {
                state.write_u8(11);
                u.hash(state);
            }
            Value::Decimal { value, scale } => {
                state.write_u8(12);
                value.hash(state);
                scale.hash(state);
            }
            Value::Interval {
                months,
                days,
                microseconds,
            } => {
                state.write_u8(13);
                months.hash(state);
                days.hash(state);
                microseconds.hash(state);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn concat_rows(left: &[Value], right: &[Value]) -> Vec<Value> {
    let mut row = Vec::with_capacity(left.len() + right.len());
    row.extend_from_slice(left);
    row.extend_from_slice(right);
    row
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{LogicalJoinType, ScalarExpr};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::HashJoin;
    use crate::mem_table_scan::MemTableScan;
    use crate::{ExecError, Operator};

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    fn schema_id() -> Schema {
        Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok")
    }

    fn schema_val() -> Schema {
        Schema::new([Field::required("val", DataType::Int32)]).expect("schema ok")
    }

    fn schema_id_val() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int32),
        ])
        .expect("schema ok")
    }

    fn i32_batch(rows: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(rows.to_vec()))]).expect("batch ok")
    }

    fn col_idx0_i32(name: &str) -> ScalarExpr {
        ScalarExpr::Column {
            name: name.into(),
            index: 0,
            data_type: DataType::Int32,
        }
    }

    fn drain_rows(op: &mut dyn Operator) -> Vec<(i32, i32)> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("no error") {
            let rows = crate::filter_op::batch_to_rows(&b, &schema).expect("decode ok");
            for row in rows {
                // batch_to_rows now reports `Value::Null` for the null
                // probe-side rows produced by LEFT OUTER unmatched
                // padding (the underlying NumericColumn validity bitmap
                // distinguishes them from real zeros). Map back to 0
                // here so the test assertions stay readable.
                let l = match &row[0] {
                    Value::Int32(v) => *v,
                    Value::Null => 0,
                    _ => panic!("unexpected left value: {:?}", row[0]),
                };
                let r = match &row[1] {
                    Value::Int32(v) => *v,
                    Value::Null => 0,
                    _ => panic!("unexpected right value: {:?}", row[1]),
                };
                out.push((l, r));
            }
        }
        out
    }

    // -------------------------------------------------------------------------
    // Test 1: INNER hash join happy path
    // -------------------------------------------------------------------------

    #[test]
    fn hash_join_inner_happy_path() {
        // left: [1, 2, 3], right: [2, 3, 4]
        // Matches: (2,2), (3,3)
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2, 3])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[2, 3, 4])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::Inner,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_rows(&mut op);
        rows.sort_unstable();
        assert_eq!(rows, vec![(2, 2), (3, 3)]);
    }

    // -------------------------------------------------------------------------
    // Test 2: empty build side returns no rows
    // -------------------------------------------------------------------------

    #[test]
    fn hash_join_empty_left_returns_no_rows() {
        let left = MemTableScan::new(schema_id(), vec![]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[1, 2, 3])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::Inner,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        assert!(drain_rows(&mut op).is_empty());
    }

    // -------------------------------------------------------------------------
    // Test 3: LEFT OUTER — unmatched left rows emit NULL right
    // -------------------------------------------------------------------------

    #[test]
    fn hash_join_left_outer_unmatched_rows() {
        // left: [1, 2], right: [2]
        // Inner match: (2,2). LeftOuter also emits: (1, NULL)
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[1, 2])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[2])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::LeftOuter,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_rows(&mut op);
        rows.sort_unstable();
        assert!(rows.contains(&(2, 2)), "matched pair present");
        // build_batch encodes NULL as 0 for Int32 columns (v0.5 no-null-bitmap
        // format), so the decoded sentinel is 0, not i32::MIN.
        assert!(
            rows.contains(&(1, 0)),
            "unmatched left row with NULL right (encoded as 0)"
        );
    }

    // -------------------------------------------------------------------------
    // Test 4: duplicate build keys — multiple matches emitted
    // -------------------------------------------------------------------------

    #[test]
    fn hash_join_duplicate_build_keys_all_emitted() {
        // left: [2, 2, 3], right: [2, 3]
        // Matches: (2,2), (2,2) (two from left), (3,3)
        let left = MemTableScan::new(schema_id(), vec![i32_batch(&[2, 2, 3])]);
        let right = MemTableScan::new(schema_val(), vec![i32_batch(&[2, 3])]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::Inner,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        let mut rows = drain_rows(&mut op);
        rows.sort_unstable();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], (2, 2));
        assert_eq!(rows[1], (2, 2));
        assert_eq!(rows[2], (3, 3));
    }

    // -------------------------------------------------------------------------
    // Test 5: unsupported join types return ExecError::Unsupported
    // -------------------------------------------------------------------------

    #[test]
    fn hash_join_right_outer_returns_unsupported() {
        let left = MemTableScan::new(schema_id(), vec![]);
        let right = MemTableScan::new(schema_val(), vec![]);
        let mut op = HashJoin::new(
            Box::new(left),
            Box::new(right),
            col_idx0_i32("id"),
            col_idx0_i32("val"),
            LogicalJoinType::RightOuter,
            schema_id_val(),
            schema_id(),
            schema_val(),
        );
        let err = op.next_batch().expect_err("RightOuter must error");
        assert!(matches!(err, ExecError::Unsupported(_)), "got {err:?}");
    }
}
