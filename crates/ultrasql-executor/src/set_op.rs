//! Set operation operators: UNION, INTERSECT, EXCEPT.
//!
//! [`SetOp`] implements all six SQL set operations:
//!
//! | SQL | `op` | `all` |
//! |-----|------|-------|
//! | `UNION ALL` | `Union` | `true` |
//! | `UNION [DISTINCT]` | `Union` | `false` |
//! | `INTERSECT ALL` | `Intersect` | `true` |
//! | `INTERSECT [DISTINCT]` | `Intersect` | `false` |
//! | `EXCEPT ALL` | `Except` | `true` |
//! | `EXCEPT [DISTINCT]` | `Except` | `false` |
//!
//! # Algorithm
//!
//! All modes are implemented with a hash-table approach:
//!
//! - **UNION ALL**: concatenate left and right without deduplication.
//! - **UNION DISTINCT**: union with hash-deduplication.
//! - **INTERSECT \[ALL\]**: count rows in left, match against right.
//! - **EXCEPT \[ALL\]**: count rows in left, subtract right counts.
//!
//! # NULL semantics
//!
//! Two NULLs are treated as equal for set operations (same as `DISTINCT`
//! semantics, matching PostgreSQL behaviour).

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use ultrasql_core::{Schema, Value, bpchar_semantic_text, timetz_utc_micros};
use ultrasql_planner::{LogicalSetOp, LogicalSetQuantifier};
use ultrasql_vec::Batch;

use crate::filter_op::batch_to_rows;
use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

const BATCH_TARGET_ROWS: usize = 4096;

// ---------------------------------------------------------------------------
// Row key (same NULL-equal semantics as Unique)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
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
    fn into_values(self) -> Vec<Value> {
        self.0.into_iter().map(|kv| kv.0).collect()
    }
}

#[derive(Debug, Clone)]
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

// ---------------------------------------------------------------------------
// Operator
// ---------------------------------------------------------------------------

/// Set operation operator.
///
/// Implements UNION / INTERSECT / EXCEPT with ALL or DISTINCT quantifier.
///
/// # Send
///
/// `Box<dyn Operator>`, `Schema`, and `HashMap` are all `Send`.
#[derive(Debug)]
pub struct SetOp {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    op: LogicalSetOp,
    quantifier: LogicalSetQuantifier,
    schema: Schema,
    output: Option<std::vec::IntoIter<Vec<Value>>>,
    eof: bool,
}

impl SetOp {
    /// Construct a set operation operator.
    ///
    /// - `left`, `right` — inputs (must have matching arity and types).
    /// - `op` — Union, Intersect, or Except.
    /// - `quantifier` — All or Distinct.
    /// - `schema` — output schema (from the left side per SQL standard).
    #[must_use]
    pub fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        op: LogicalSetOp,
        quantifier: LogicalSetQuantifier,
        schema: Schema,
    ) -> Self {
        Self {
            left,
            right,
            op,
            quantifier,
            schema,
            output: None,
            eof: false,
        }
    }
}

impl Operator for SetOp {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }
        if self.output.is_none() {
            let rows = self.execute()?;
            self.output = Some(rows.into_iter());
        }
        let iter = self
            .output
            .as_mut()
            .ok_or(ExecError::Internal("set operation output iterator missing"))?;
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
        vec![self.left.as_ref(), self.right.as_ref()]
    }
}

impl SetOp {
    fn drain(op: &mut Box<dyn Operator>, schema: &Schema) -> Result<Vec<Vec<Value>>, ExecError> {
        let mut rows = Vec::new();
        loop {
            let Some(batch) = op.next_batch()? else { break };
            rows.extend(batch_to_rows(&batch, schema)?);
        }
        Ok(rows)
    }

    fn execute(&mut self) -> Result<Vec<Vec<Value>>, ExecError> {
        let left_schema = self.left.schema().clone();
        let right_schema = self.right.schema().clone();

        let left_rows = Self::drain(&mut self.left, &left_schema)?;
        let right_rows = Self::drain(&mut self.right, &right_schema)?;

        let all = self.quantifier == LogicalSetQuantifier::All;

        let result = match self.op {
            LogicalSetOp::Union => {
                if all {
                    // UNION ALL — concatenate.
                    let mut out = left_rows;
                    out.extend(right_rows);
                    out
                } else {
                    // UNION DISTINCT — deduplicate combined set.
                    let mut seen: HashSet<RowKey> = HashSet::new();
                    let mut out = Vec::new();
                    for row in left_rows.into_iter().chain(right_rows) {
                        let key = RowKey::from_row(&row);
                        if seen.insert(key) {
                            out.push(row);
                        }
                    }
                    out
                }
            }
            LogicalSetOp::Intersect => {
                if all {
                    // INTERSECT ALL: count occurrences in right, emit left
                    // row up to min(left_count, right_count) times.
                    let mut right_counts: HashMap<RowKey, usize> = HashMap::new();
                    for row in &right_rows {
                        *right_counts.entry(RowKey::from_row(row)).or_insert(0) += 1;
                    }
                    let mut out = Vec::new();
                    for row in left_rows {
                        let key = RowKey::from_row(&row);
                        let cnt = right_counts.entry(key).or_insert(0);
                        if *cnt > 0 {
                            *cnt -= 1;
                            out.push(row);
                        }
                    }
                    out
                } else {
                    // INTERSECT DISTINCT: rows in both sets.
                    let right_set: HashSet<RowKey> =
                        right_rows.iter().map(|r| RowKey::from_row(r)).collect();
                    let mut seen: HashSet<RowKey> = HashSet::new();
                    let mut out = Vec::new();
                    for row in left_rows {
                        let key = RowKey::from_row(&row);
                        if right_set.contains(&key) && seen.insert(key) {
                            out.push(row);
                        }
                    }
                    out
                }
            }
            LogicalSetOp::Except => {
                if all {
                    // EXCEPT ALL: subtract right counts from left.
                    let mut right_counts: HashMap<RowKey, usize> = HashMap::new();
                    for row in &right_rows {
                        *right_counts.entry(RowKey::from_row(row)).or_insert(0) += 1;
                    }
                    let mut out = Vec::new();
                    for row in left_rows {
                        let key = RowKey::from_row(&row);
                        let cnt = right_counts.entry(key).or_insert(0);
                        if *cnt > 0 {
                            *cnt -= 1;
                        } else {
                            out.push(row);
                        }
                    }
                    out
                } else {
                    // EXCEPT DISTINCT: rows in left but not in right.
                    let right_set: HashSet<RowKey> =
                        right_rows.iter().map(|r| RowKey::from_row(r)).collect();
                    let mut seen: HashSet<RowKey> = HashSet::new();
                    let mut out = Vec::new();
                    for row in left_rows {
                        let key = RowKey::from_row(&row);
                        if !right_set.contains(&key) && seen.insert(key) {
                            out.push(row);
                        }
                    }
                    out
                }
            }
        };

        // Silence "unused variable" for into_values in non-All paths.
        let _ = RowKey::from_row(&[]).into_values();

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use ultrasql_core::{
        BitString, DataType, Field, GeometryType, GeometryValue, Lsn, NetworkValue, Oid, RangeType,
        RangeValue, Schema, SparseVector, Value,
    };
    use ultrasql_planner::{LogicalSetOp, LogicalSetQuantifier};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::{RowKey, SetOp};
    use crate::Operator;
    use crate::filter_op::batch_to_rows;
    use crate::mem_table_scan::MemTableScan;

    fn schema_i32() -> Schema {
        Schema::new([Field::required("v", DataType::Int32)]).expect("schema ok")
    }

    fn i32_batch(vals: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(vals.to_vec()))]).expect("batch ok")
    }

    fn drain_sorted(op: &mut dyn Operator) -> Vec<i32> {
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
        out.sort_unstable();
        out
    }

    #[test]
    fn union_all_concatenates() {
        let left = MemTableScan::new(schema_i32(), vec![i32_batch(&[1, 2])]);
        let right = MemTableScan::new(schema_i32(), vec![i32_batch(&[2, 3])]);
        let mut op = SetOp::new(
            Box::new(left),
            Box::new(right),
            LogicalSetOp::Union,
            LogicalSetQuantifier::All,
            schema_i32(),
        );
        let vals = drain_sorted(&mut op);
        assert_eq!(vals, vec![1, 2, 2, 3]);
    }

    #[test]
    fn union_distinct_deduplicates() {
        let left = MemTableScan::new(schema_i32(), vec![i32_batch(&[1, 2, 2])]);
        let right = MemTableScan::new(schema_i32(), vec![i32_batch(&[2, 3])]);
        let mut op = SetOp::new(
            Box::new(left),
            Box::new(right),
            LogicalSetOp::Union,
            LogicalSetQuantifier::Distinct,
            schema_i32(),
        );
        let vals = drain_sorted(&mut op);
        assert_eq!(vals, vec![1, 2, 3]);
    }

    #[test]
    fn intersect_distinct_returns_common_rows() {
        let left = MemTableScan::new(schema_i32(), vec![i32_batch(&[1, 2, 3])]);
        let right = MemTableScan::new(schema_i32(), vec![i32_batch(&[2, 3, 4])]);
        let mut op = SetOp::new(
            Box::new(left),
            Box::new(right),
            LogicalSetOp::Intersect,
            LogicalSetQuantifier::Distinct,
            schema_i32(),
        );
        let vals = drain_sorted(&mut op);
        assert_eq!(vals, vec![2, 3]);
    }

    #[test]
    fn except_distinct_removes_right_rows() {
        let left = MemTableScan::new(schema_i32(), vec![i32_batch(&[1, 2, 3])]);
        let right = MemTableScan::new(schema_i32(), vec![i32_batch(&[2, 4])]);
        let mut op = SetOp::new(
            Box::new(left),
            Box::new(right),
            LogicalSetOp::Except,
            LogicalSetQuantifier::Distinct,
            schema_i32(),
        );
        let vals = drain_sorted(&mut op);
        assert_eq!(vals, vec![1, 3]);
    }

    #[test]
    fn except_all_respects_counts() {
        // left: [1, 2, 2, 3], right: [2] → result: [1, 2, 3]
        let left = MemTableScan::new(schema_i32(), vec![i32_batch(&[1, 2, 2, 3])]);
        let right = MemTableScan::new(schema_i32(), vec![i32_batch(&[2])]);
        let mut op = SetOp::new(
            Box::new(left),
            Box::new(right),
            LogicalSetOp::Except,
            LogicalSetQuantifier::All,
            schema_i32(),
        );
        let vals = drain_sorted(&mut op);
        assert_eq!(vals, vec![1, 2, 3]);
    }

    #[test]
    fn intersect_all_respects_duplicate_counts_and_eof() {
        let left = MemTableScan::new(schema_i32(), vec![i32_batch(&[1, 2, 2, 2, 3])]);
        let right = MemTableScan::new(schema_i32(), vec![i32_batch(&[2, 2, 4])]);
        let mut op = SetOp::new(
            Box::new(left),
            Box::new(right),
            LogicalSetOp::Intersect,
            LogicalSetQuantifier::All,
            schema_i32(),
        );
        assert_eq!(drain_sorted(&mut op), vec![2, 2]);
        assert!(op.next_batch().expect("eof").is_none());
    }

    #[test]
    fn row_key_hashes_supported_value_families_with_null_equal_semantics() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int16(1),
            Value::Int32(2),
            Value::Int64(3),
            Value::Money(4),
            Value::Oid(Oid::new(5)),
            Value::RegClass(Oid::new(6)),
            Value::RegType(Oid::new(7)),
            Value::PgLsn(Lsn::new(8)),
            Value::Float32(f32::from_bits(0x7fc0_0001)),
            Value::Float64(f64::from_bits(0x7ff8_0000_0000_0001)),
            Value::Text("text".into()),
            Value::Char("bpchar  ".into()),
            Value::Json(r#"{"a":1}"#.into()),
            Value::Jsonb(r#"{"a":1}"#.into()),
            Value::Xml("<r/>".into()),
            Value::Bytea(vec![0, 1, 255]),
            Value::Timestamp(9),
            Value::TimestampTz(10),
            Value::Time(11),
            Value::TimeTz {
                micros: 12,
                offset_seconds: 3_600,
            },
            Value::Date(13),
            Value::Uuid([14; 16]),
            Value::Decimal {
                value: 15,
                scale: 2,
            },
            Value::Interval {
                months: 1,
                days: 2,
                microseconds: 3,
            },
            Value::Range(RangeValue::parse(RangeType::Int4, "[1,3)").expect("range")),
            Value::Geometry(GeometryValue::parse(GeometryType::Box, "((0,0),(1,1))").expect("box")),
            Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(1), Value::Null],
            },
            Value::Vector(vec![1.0, f32::NAN]),
            Value::HalfVec(vec![2.0, f32::NAN]),
            Value::SparseVec(SparseVector {
                dims: 4,
                entries: vec![(2, 1.5)],
            }),
            Value::BitVec {
                dims: 9,
                bytes: vec![0b1010_0000, 0b1000_0000],
            },
            Value::BitString(BitString::parse("10101").expect("bits")),
            Value::Network(
                NetworkValue::parse_for_type(&DataType::Inet, "127.0.0.1").expect("inet"),
            ),
            Value::Record(vec![("x".into(), Value::Int32(1))]),
        ];

        let key = RowKey::from_row(&values);
        let clone = RowKey::from_row(&values);
        let mut seen = HashSet::new();
        assert!(seen.insert(key));
        assert!(!seen.insert(clone));

        assert_eq!(
            RowKey::from_row(&[Value::Null]),
            RowKey::from_row(&[Value::Null])
        );
        assert_eq!(
            RowKey::from_row(&[Value::Char("a".into())]),
            RowKey::from_row(&[Value::Char("a   ".into())])
        );
        assert_eq!(
            RowKey::from_row(&[Value::TimeTz {
                micros: 3_600_000_000,
                offset_seconds: 3_600,
            }]),
            RowKey::from_row(&[Value::TimeTz {
                micros: 0,
                offset_seconds: 0,
            }])
        );
    }
}
