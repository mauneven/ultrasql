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

use ultrasql_core::{DataType, Schema, Value, bpchar_semantic_text, timetz_utc_micros};
use ultrasql_vec::Batch;
use ultrasql_vec::column::Column;

use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::value_key::{decimal_values_equal, hash_decimal_key};
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
            (Value::Vector(a), Value::Vector(b)) | (Value::HalfVec(a), Value::HalfVec(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(l, r)| l.to_bits() == r.to_bits())
            }
            (
                Value::Decimal {
                    value: left_value,
                    scale: left_scale,
                },
                Value::Decimal {
                    value: right_value,
                    scale: right_scale,
                },
            ) => decimal_values_equal(*left_value, *left_scale, *right_value, *right_scale),
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
            Value::Money(v) => {
                state.write_u8(23);
                v.hash(state);
            }
            Value::Oid(v) => {
                state.write_u8(27);
                v.hash(state);
            }
            Value::RegClass(v) => {
                state.write_u8(28);
                v.hash(state);
            }
            Value::RegType(v) => {
                state.write_u8(29);
                v.hash(state);
            }
            Value::PgLsn(v) => {
                state.write_u8(30);
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
            Value::Char(s) => {
                state.write_u8(24);
                bpchar_semantic_text(s).hash(state);
            }
            Value::Json(s) => {
                state.write_u8(16);
                s.hash(state);
            }
            Value::Jsonb(s) => {
                state.write_u8(17);
                s.hash(state);
            }
            Value::Xml(s) => {
                state.write_u8(31);
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
            Value::TimeTz {
                micros,
                offset_seconds,
            } => {
                state.write_u8(9);
                timetz_utc_micros(*micros, *offset_seconds).hash(state);
            }
            Value::Date(x) => {
                state.write_u8(10);
                x.hash(state);
            }
            Value::Uuid(u) => {
                state.write_u8(11);
                u.hash(state);
            }
            Value::Decimal { value, scale } => {
                state.write_u8(12);
                hash_decimal_key(state, *value, *scale);
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
            Value::Range(v) => {
                state.write_u8(14);
                v.hash(state);
            }
            Value::Geometry(v) => {
                state.write_u8(15);
                v.hash(state);
            }
            Value::Array {
                element_type,
                elements,
            } => {
                state.write_u8(18);
                element_type.hash(state);
                elements.hash(state);
            }
            Value::Vector(values) | Value::HalfVec(values) => {
                state.write_u8(19);
                for value in values {
                    value.to_bits().hash(state);
                }
            }
            Value::SparseVec(value) => {
                state.write_u8(20);
                value.hash(state);
            }
            Value::BitVec { dims, bytes } => {
                state.write_u8(21);
                dims.hash(state);
                bytes.hash(state);
            }
            Value::BitString(bits) => {
                state.write_u8(25);
                bits.hash(state);
            }
            Value::Network(network) => {
                state.write_u8(26);
                network.hash(state);
            }
            Value::Record(fields) => {
                state.write_u8(22);
                fields.hash(state);
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

    fn estimated_row_count(&self) -> Option<usize> {
        self.child.estimated_row_count()
    }

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.child.as_ref()]
    }
}

impl Unique {
    fn next_batch_hash(&mut self) -> Result<Option<Batch>, ExecError> {
        // Build phase: drain child, deduplicate.
        if self.output.is_none() {
            let unique_rows = if let Some(unique_rows) = self.build_single_numeric_hash()? {
                unique_rows
            } else {
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
                unique_rows
            };
            self.output = Some(unique_rows.into_iter());
        }

        let iter = self
            .output
            .as_mut()
            .ok_or(ExecError::Internal("unique output iterator missing"))?;
        let chunk: Vec<Vec<Value>> = iter.by_ref().take(BATCH_TARGET_ROWS).collect();
        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }
        build_batch(&chunk, &self.schema).map(Some)
    }

    fn build_single_numeric_hash(&mut self) -> Result<Option<Vec<Vec<Value>>>, ExecError> {
        if self.schema.len() != 1 {
            return Ok(None);
        }
        match self.schema.field_at(0).data_type {
            DataType::Int32 => self.build_single_i32_hash().map(Some),
            DataType::Int64 => self.build_single_i64_hash().map(Some),
            _ => Ok(None),
        }
    }

    fn build_single_i32_hash(&mut self) -> Result<Vec<Vec<Value>>, ExecError> {
        let mut seen: HashSet<Option<i32>> = HashSet::new();
        let mut unique_rows = Vec::new();
        loop {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            let Some(Column::Int32(column)) = batch.columns().first() else {
                return Err(ExecError::TypeMismatch(
                    "unique input column type does not match Int32 schema".to_owned(),
                ));
            };
            for (row, &value) in column.data().iter().enumerate() {
                let key = if column.nulls().is_some_and(|nulls| !nulls.get(row)) {
                    None
                } else {
                    Some(value)
                };
                if seen.insert(key) {
                    unique_rows.push(vec![key.map_or(Value::Null, Value::Int32)]);
                }
            }
        }
        Ok(unique_rows)
    }

    fn build_single_i64_hash(&mut self) -> Result<Vec<Vec<Value>>, ExecError> {
        let mut seen: HashSet<Option<i64>> = HashSet::new();
        let mut unique_rows = Vec::new();
        loop {
            let Some(batch) = self.child.next_batch()? else {
                break;
            };
            let Some(Column::Int64(column)) = batch.columns().first() else {
                return Err(ExecError::TypeMismatch(
                    "unique input column type does not match Int64 schema".to_owned(),
                ));
            };
            for (row, &value) in column.data().iter().enumerate() {
                let key = if column.nulls().is_some_and(|nulls| !nulls.get(row)) {
                    None
                } else {
                    Some(value)
                };
                if seen.insert(key) {
                    unique_rows.push(vec![key.map_or(Value::Null, Value::Int64)]);
                }
            }
        }
        Ok(unique_rows)
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
pub(crate) fn rows_equal_for_distinct(a: &[Value], b: &[Value]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(av, bv)| match (av, bv) {
        (Value::Null, Value::Null) => true,
        (Value::Float32(x), Value::Float32(y)) => x.to_bits() == y.to_bits(),
        (Value::Float64(x), Value::Float64(y)) => x.to_bits() == y.to_bits(),
        (
            Value::Decimal {
                value: left_value,
                scale: left_scale,
            },
            Value::Decimal {
                value: right_value,
                scale: right_scale,
            },
        ) => decimal_values_equal(*left_value, *left_scale, *right_value, *right_scale),
        _ => av == bv,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use ultrasql_core::{
        BitString, DataType, Field, GeometryType, GeometryValue, Lsn, NetworkValue, Oid, RangeType,
        RangeValue, Schema, SparseVector, Value,
    };
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::{KeyValue, RowKey, Unique, UniqueMode, rows_equal_for_distinct};
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

    fn key_hash(value: Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        KeyValue(value).hash(&mut hasher);
        hasher.finish()
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

    #[test]
    fn unique_hash_fast_paths_cover_nulls_and_i64_type_errors() {
        let mut valid = ultrasql_vec::Bitmap::new(5, true);
        valid.set(1, false);
        valid.set(3, false);
        let scan = MemTableScan::new(
            Schema::new([Field::required("v", DataType::Int64)]).expect("schema"),
            vec![
                Batch::new([Column::Int64(
                    NumericColumn::with_nulls(vec![7, 0, 7, 0, 9], valid)
                        .expect("matching lengths"),
                )])
                .expect("batch"),
            ],
        );
        let mut op = Unique::new(Box::new(scan), UniqueMode::Hash);
        let rows = drain_all(&mut op);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().any(|row| row[0] == Value::Null));
        assert!(rows.iter().any(|row| row[0] == Value::Int64(7)));
        assert!(rows.iter().any(|row| row[0] == Value::Int64(9)));

        let bad_scan = MemTableScan::new(
            Schema::new([Field::required("v", DataType::Int64)]).expect("schema"),
            vec![i32_batch(&[1])],
        );
        let mut bad = Unique::new(Box::new(bad_scan), UniqueMode::Hash);
        let err = bad.next_batch().expect_err("type mismatch");
        assert!(err.to_string().contains("does not match Int64 schema"));
    }

    #[test]
    fn distinct_row_keys_hash_supported_value_families() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int16(7),
            Value::Int32(8),
            Value::Int64(9),
            Value::Money(1234),
            Value::Oid(Oid::new(10)),
            Value::RegClass(Oid::new(11)),
            Value::RegType(Oid::new(12)),
            Value::PgLsn(Lsn::new(0x1_0000_0002)),
            Value::Float32(-0.0),
            Value::Float64(f64::NAN),
            Value::Text("text".to_owned()),
            Value::Char("x   ".to_owned()),
            Value::Json(r#"{"a":1}"#.to_owned()),
            Value::Jsonb(r#"{"a":1}"#.to_owned()),
            Value::Xml("<x/>".to_owned()),
            Value::Bytea(vec![1, 2, 3]),
            Value::Timestamp(1),
            Value::TimestampTz(2),
            Value::Time(3),
            Value::TimeTz {
                micros: 4,
                offset_seconds: 3600,
            },
            Value::Date(5),
            Value::Uuid([6; 16]),
            Value::Decimal {
                value: 12345,
                scale: 2,
            },
            Value::Interval {
                months: 1,
                days: 2,
                microseconds: 3,
            },
            Value::Range(RangeValue::parse(RangeType::Int4, "[1,4)").expect("range")),
            Value::Geometry(GeometryValue::parse(GeometryType::Point, "(1,2)").expect("geometry")),
            Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(1), Value::Null],
            },
            Value::Vector(vec![1.0, -0.0]),
            Value::HalfVec(vec![1.0, 2.0]),
            Value::SparseVec(SparseVector::new(8, vec![(2, 1.5)]).expect("sparse")),
            Value::BitVec {
                dims: 8,
                bytes: vec![0b1010_0000],
            },
            Value::BitString(BitString::parse("1010").expect("bits")),
            Value::Network(
                NetworkValue::parse_for_type(&DataType::Inet, "127.0.0.1").expect("inet"),
            ),
            Value::Record(vec![("a".to_owned(), Value::Int32(1))]),
        ];

        for value in values {
            let first = key_hash(value.clone());
            let second = key_hash(value);
            assert_eq!(first, second);
        }

        assert_eq!(
            KeyValue(Value::Float32(f32::NAN)),
            KeyValue(Value::Float32(f32::NAN))
        );
        assert_eq!(
            KeyValue(Value::Float64(-0.0)),
            KeyValue(Value::Float64(-0.0))
        );
        assert_ne!(
            KeyValue(Value::Float64(-0.0)),
            KeyValue(Value::Float64(0.0))
        );
        assert_eq!(
            KeyValue(Value::Char("x".to_owned())),
            KeyValue(Value::Char("x   ".to_owned()))
        );
        assert_eq!(
            KeyValue(Value::Decimal {
                value: 10,
                scale: 1
            }),
            KeyValue(Value::Decimal { value: 1, scale: 0 })
        );
        assert_eq!(
            key_hash(Value::Decimal {
                value: 10,
                scale: 1
            }),
            key_hash(Value::Decimal { value: 1, scale: 0 })
        );

        let row_a = RowKey::from_row(&[Value::Null, Value::Int32(1)]);
        let row_b = RowKey::from_row(&[Value::Null, Value::Int32(1)]);
        assert_eq!(row_a, row_b);
    }

    #[test]
    fn distinct_sort_row_equality_handles_nulls_floats_and_widths() {
        assert!(rows_equal_for_distinct(&[Value::Null], &[Value::Null]));
        assert!(rows_equal_for_distinct(
            &[Value::Float32(f32::NAN)],
            &[Value::Float32(f32::NAN)]
        ));
        assert!(!rows_equal_for_distinct(
            &[Value::Float64(-0.0)],
            &[Value::Float64(0.0)]
        ));
        assert!(!rows_equal_for_distinct(
            &[Value::Int32(1)],
            &[Value::Int32(1), Value::Int32(2)]
        ));
        assert!(rows_equal_for_distinct(
            &[Value::Decimal {
                value: 10,
                scale: 1,
            }],
            &[Value::Decimal { value: 1, scale: 0 }]
        ));
    }

    fn drain_all(op: &mut dyn Operator) -> Vec<Vec<Value>> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("ok") {
            out.extend(batch_to_rows(&b, &schema).expect("decode"));
        }
        out
    }
}
