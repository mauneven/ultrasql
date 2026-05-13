//! DISTINCT deduplication operator.
//!
//! [`Unique`] removes duplicate rows from its child's output. Two modes
//! are provided:
//!
//! - **Hash mode** — buffers all input, deduplicates via a `HashSet` keyed
//!   on row values, then emits the survivors in unspecified order. Requires
//!   O(n) memory but does not require sorted input.
//! - **Sort mode** — assumes the child's output is already sorted on all
//!   output columns. Emits a row only when it differs from the previous
//!   emitted row. O(1) extra state, O(n) time.
//!
//! The caller chooses the mode at construction time. The optimizer selects
//! between the two based on whether a sort node is already in the plan.
//!
//! # NULL semantics
//!
//! SQL `DISTINCT` treats two NULL values as equal — two rows that are both
//! NULL in every column are considered duplicates. This differs from the
//! equality operator (`NULL = NULL` is UNKNOWN in SQL) but matches the
//! PostgreSQL `DISTINCT` semantics.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use ultrasql_core::{Schema, Value};
use ultrasql_vec::Batch;

use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

/// The deduplication algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniqueMode {
    /// Hash-based deduplication (unordered output).
    Hash,
    /// Sort-based deduplication (requires sorted input).
    Sort,
}

// ---------------------------------------------------------------------------
// Hash-map key wrapper for Value
// ---------------------------------------------------------------------------

/// A wrapper around [`Value`] that implements `Hash + Eq` treating two
/// NULLs as equal and using bit-pattern equality for floats.
#[derive(Debug)]
struct KeyValue(Value);

impl PartialEq for KeyValue {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Value::Null, Value::Null) => true,
            (Value::Float32(a), Value::Float32(b)) => a.to_bits() == b.to_bits(),
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            _ => self.0 == other.0,
        }
    }
}

impl Eq for KeyValue {}

impl Hash for KeyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
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
}

/// A row key wrapping `Vec<KeyValue>`.
#[derive(Debug, PartialEq, Eq)]
struct RowKey(Vec<KeyValue>);

impl Hash for RowKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for kv in &self.0 {
            kv.hash(state);
        }
    }
}

impl RowKey {
    fn from_row(row: &[Value]) -> Self {
        Self(row.iter().cloned().map(KeyValue).collect())
    }
}

// ---------------------------------------------------------------------------
// Operator
// ---------------------------------------------------------------------------

const BATCH_TARGET_ROWS: usize = 4096;

/// DISTINCT deduplication operator.
///
/// See the module documentation for details on the two operating modes.
///
/// # Send
///
/// `Box<dyn Operator>`, `Schema`, and `HashSet` are all `Send`.
#[derive(Debug)]
pub struct Unique {
    child: Box<dyn Operator>,
    schema: Schema,
    mode: UniqueMode,
    // Hash mode state — populated during build phase.
    output: Option<std::vec::IntoIter<Vec<Value>>>,
    // Sort mode state.
    last_row: Option<Vec<Value>>,
    eof: bool,
}

impl Unique {
    /// Construct a `Unique` operator.
    ///
    /// - `child` — the input operator.
    /// - `mode` — `Hash` for unsorted input, `Sort` for pre-sorted input.
    #[must_use]
    pub fn new(child: Box<dyn Operator>, mode: UniqueMode) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            schema,
            mode,
            output: None,
            last_row: None,
            eof: false,
        }
    }
}

impl Operator for Unique {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        match self.mode {
            UniqueMode::Hash => self.next_batch_hash(),
            UniqueMode::Sort => self.next_batch_sort(),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl Unique {
    fn next_batch_hash(&mut self) -> Result<Option<Batch>, ExecError> {
        // Build phase: drain child, deduplicate.
        if self.output.is_none() {
            let mut seen: HashSet<RowKey> = HashSet::new();
            let mut unique_rows: Vec<Vec<Value>> = Vec::new();
            loop {
                let Some(batch) = self.child.next_batch()? else {
                    break;
                };
                let rows = batch_to_rows(&batch, &self.schema)?;
                for row in rows {
                    let key = RowKey::from_row(&row);
                    if seen.insert(key) {
                        unique_rows.push(row);
                    }
                }
            }
            self.output = Some(unique_rows.into_iter());
        }

        let iter = self.output.as_mut().expect("just-set");
        let chunk: Vec<Vec<Value>> = iter.by_ref().take(BATCH_TARGET_ROWS).collect();
        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&chunk, &self.schema).map(Some)
    }

    fn next_batch_sort(&mut self) -> Result<Option<Batch>, ExecError> {
        // Stream deduplication: emit a row only when it differs from last.
        let mut survivors: Vec<Vec<Value>> = Vec::new();

        loop {
            let Some(batch) = self.child.next_batch()? else {
                // EOF from child.
                break;
            };
            let rows = batch_to_rows(&batch, &self.schema)?;
            for row in rows {
                let is_dup = self
                    .last_row
                    .as_ref()
                    .is_some_and(|prev| rows_equal_for_distinct(prev, &row));
                if !is_dup {
                    self.last_row = Some(row.clone());
                    survivors.push(row);
                    if survivors.len() >= BATCH_TARGET_ROWS {
                        return build_batch(&survivors, &self.schema).map(Some);
                    }
                }
            }
            if !survivors.is_empty() {
                return build_batch(&survivors, &self.schema).map(Some);
            }
        }

        if survivors.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&survivors, &self.schema).map(Some)
    }
}

/// Compare two rows for DISTINCT equality (NULL == NULL).
fn rows_equal_for_distinct(a: &[Value], b: &[Value]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(av, bv)| match (av, bv) {
        (Value::Null, Value::Null) => true,
        (Value::Float32(x), Value::Float32(y)) => x.to_bits() == y.to_bits(),
        (Value::Float64(x), Value::Float64(y)) => x.to_bits() == y.to_bits(),
        _ => av == bv,
    })
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::{Unique, UniqueMode};
    use crate::Operator;
    use crate::filter_op::batch_to_rows;
    use crate::mem_table_scan::MemTableScan;

    fn schema_i32() -> Schema {
        Schema::new([Field::required("v", DataType::Int32)]).expect("schema ok")
    }

    fn i32_batch(vals: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(vals.to_vec()))]).expect("batch ok")
    }

    fn drain_i32(op: &mut dyn Operator) -> Vec<i32> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("ok") {
            let rows = batch_to_rows(&b, &schema).expect("decode");
            for row in rows {
                if let Value::Int32(v) = &row[0] {
                    out.push(*v);
                }
            }
        }
        out
    }

    #[test]
    fn unique_hash_deduplicates_unordered_input() {
        let scan = MemTableScan::new(schema_i32(), vec![i32_batch(&[3, 1, 2, 1, 3, 2, 1])]);
        let mut op = Unique::new(Box::new(scan), UniqueMode::Hash);
        let mut vals = drain_i32(&mut op);
        vals.sort_unstable();
        assert_eq!(vals, vec![1, 2, 3]);
    }

    #[test]
    fn unique_sort_deduplicates_sorted_input() {
        let scan = MemTableScan::new(schema_i32(), vec![i32_batch(&[1, 1, 2, 2, 3])]);
        let mut op = Unique::new(Box::new(scan), UniqueMode::Sort);
        let vals = drain_i32(&mut op);
        assert_eq!(vals, vec![1, 2, 3]);
    }

    #[test]
    fn unique_empty_input_returns_none() {
        let scan = MemTableScan::new(schema_i32(), vec![]);
        let mut op = Unique::new(Box::new(scan), UniqueMode::Hash);
        assert!(op.next_batch().expect("ok").is_none());
    }
}
