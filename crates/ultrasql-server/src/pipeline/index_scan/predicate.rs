//! Predicate-shape matching and `i64` key-range arithmetic for the
//! index-scan lowerer.

use ultrasql_core::{DataType, Value};
use ultrasql_planner::{BinaryOp, ScalarExpr};

#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexKeyRange {
    /// Inclusive lower bound, or `None` for unbounded below.
    pub(crate) low: Option<i64>,
    /// Inclusive upper bound, or `None` for unbounded above.
    pub(crate) high: Option<i64>,
}

impl IndexKeyRange {
    /// Point probe: `key == k`.
    pub(crate) const fn point(k: i64) -> Self {
        Self {
            low: Some(k),
            high: Some(k),
        }
    }

    /// Empty key range.
    pub(crate) const fn empty() -> Self {
        Self {
            low: Some(1),
            high: Some(0),
        }
    }

    /// Whether this inclusive range cannot contain any key.
    pub(crate) const fn is_empty(self) -> bool {
        matches!((self.low, self.high), (Some(low), Some(high)) if low > high)
    }
}

/// Decode a `WHERE` predicate into an `(column_index, IndexKeyRange)`
/// pair when its shape is one the B-tree dispatcher can probe.
///
/// Recognised top-level shapes:
/// - `Binary(op, Column, Literal)` for `op ∈ {Eq, Lt, LtEq, Gt, GtEq}`
///   (or commuted operand order).
/// - `Binary(And, sub_left, sub_right)` where both subterms are
///   single-side comparisons on the same column — produces a bounded
///   range. This is the canonical post-binder shape for `BETWEEN`.
///
/// Returns `None` for anything else; the caller falls back to a
/// general filter.
pub(crate) fn match_indexable_predicate(predicate: &ScalarExpr) -> Option<(usize, IndexKeyRange)> {
    if let Some((col, range)) = match_simple_comparison(predicate) {
        return Some((col, range));
    }
    // Conjunction of two single-side comparisons on the same column.
    let ScalarExpr::Binary {
        op: BinaryOp::And,
        left,
        right,
        ..
    } = predicate
    else {
        return None;
    };
    let (left_col, left_range) = match_simple_comparison(left)?;
    let (right_col, right_range) = match_simple_comparison(right)?;
    if left_col != right_col {
        return None;
    }
    let combined = IndexKeyRange {
        low: max_lower_bound(left_range.low, right_range.low),
        high: min_upper_bound(left_range.high, right_range.high),
    };
    Some((left_col, combined))
}

/// Decode a single `Column op Literal` (or commuted) comparison into an
/// `(column_index, IndexKeyRange)`. Returns `None` when the operand type
/// is not one of the i64-mappable discrete domains
/// `column_idx_for_int_key` admits, the literal cannot be represented as
/// `i64`, or the operator is not a comparison.
///
/// Strict-bound operators are normalised to inclusive bounds via
/// `±1` adjustment (`x > 5` becomes `low = Some(6)`,
/// `x < 5` becomes `high = Some(4)`). Overflowing the adjustment
/// clamps to the sentinel; the resulting range is empty, which is
pub(crate) fn match_simple_comparison(expr: &ScalarExpr) -> Option<(usize, IndexKeyRange)> {
    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    // Decompose into (column_idx, literal_as_i64, op_with_col_on_left).
    // The column's `DataType` and the literal's `Value` must fall in the
    // same i64 unit-class (see `literal_in_same_unit_class_as_column`),
    // or the i64 key range would be mis-scaled — e.g. a `Date` column
    // (days) probed against a `Timestamp` literal (microseconds). A
    // mismatch yields `None` so the caller falls back to the safe
    // relation-wide lock / SeqScan.
    let (col_idx, raw_lit, op_normalised) = match (left.as_ref(), right.as_ref()) {
        (col @ ScalarExpr::Column { .. }, lit @ ScalarExpr::Literal { .. }) => {
            let idx = column_idx_for_int_key(col)?;
            let lit_val = literal_as_i64_for_column(col, lit)?;
            (idx, lit_val, *op)
        }
        (lit @ ScalarExpr::Literal { .. }, col @ ScalarExpr::Column { .. }) => {
            let idx = column_idx_for_int_key(col)?;
            let lit_val = literal_as_i64_for_column(col, lit)?;
            // Flip the operator so `lit op col` reads as `col flipped_op lit`.
            let flipped = match op {
                BinaryOp::Eq => BinaryOp::Eq,
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::LtEq => BinaryOp::GtEq,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::GtEq => BinaryOp::LtEq,
                _ => return None,
            };
            (idx, lit_val, flipped)
        }
        _ => return None,
    };
    let range = match op_normalised {
        BinaryOp::Eq => IndexKeyRange::point(raw_lit),
        BinaryOp::Lt => raw_lit
            .checked_sub(1)
            .map_or_else(IndexKeyRange::empty, |high| IndexKeyRange {
                low: None,
                high: Some(high),
            }),
        BinaryOp::LtEq => IndexKeyRange {
            low: None,
            high: Some(raw_lit),
        },
        BinaryOp::Gt => raw_lit
            .checked_add(1)
            .map_or_else(IndexKeyRange::empty, |low| IndexKeyRange {
                low: Some(low),
                high: None,
            }),
        BinaryOp::GtEq => IndexKeyRange {
            low: Some(raw_lit),
            high: None,
        },
        _ => return None,
    };
    Some((col_idx, range))
}

pub(super) fn match_hash_equality_predicate(expr: &ScalarExpr) -> Option<(usize, Value)> {
    let ScalarExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (ScalarExpr::Column { index, .. }, ScalarExpr::Literal { value, .. })
        | (ScalarExpr::Literal { value, .. }, ScalarExpr::Column { index, .. }) => {
            Some((*index, value.clone()))
        }
        _ => None,
    }
}

/// Read the column index from a [`ScalarExpr::Column`] whose data type
/// is represented directly in the index `i64` key space.
pub(super) const fn column_idx_for_int_key(expr: &ScalarExpr) -> Option<usize> {
    let ScalarExpr::Column {
        index, data_type, ..
    } = expr
    else {
        return None;
    };
    match data_type {
        DataType::Bool
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Timestamp
        | DataType::TimestampTz
        | DataType::Date
        | DataType::Time => Some(*index),
        _ => None,
    }
}

/// Lift an integer-typed literal to `i64`. `Int32` is sign-extended
/// via the lossless `i64::from(i32)` widening conversion. Returns
/// `None` for non-integer literals (text, float, NULL, …).
pub(crate) fn literal_as_i64(expr: &ScalarExpr) -> Option<i64> {
    let ScalarExpr::Literal { value, .. } = expr else {
        return None;
    };
    match value {
        Value::Bool(v) => Some(i64::from(*v)),
        Value::Int16(v) => Some(i64::from(*v)),
        Value::Int32(v) => Some(i64::from(*v)),
        Value::Int64(v) => Some(*v),
        Value::Timestamp(v) | Value::TimestampTz(v) => Some(*v),
        // `Date` is `i32` days since 2000-01-01; the lossless,
        // order-preserving `i64::from` widening matches the `Int32`
        // case. `Time` is already `i64` microseconds since midnight, so
        // it copies through like `Timestamp`. Both domains are discrete
        // (whole days / whole µs), so the `±1` strict-bound
        // normalisation in `match_simple_comparison` is integer-exact.
        Value::Date(v) => Some(i64::from(*v)),
        Value::Time(v) => Some(*v),
        _ => None,
    }
}

/// The i64 unit-class a column / literal maps into. A tight `i64`
/// key-range lock (or B-tree probe) is sound only when the column and the
/// literal share a unit-class; across classes the same logical instant
/// maps to *different* i64 values, so locking a column-scoped range on the
/// wrong scale would silently miss real rw-conflicts (non-serializable) or
/// probe the wrong key span.
///
/// - [`UnitClass::Int`]: `Bool`/`Int16`/`Int32`/`Int64` — raw integer
///   value. Width-crossing within the class is fine (the widening is
///   lossless and the binder already blocks int-vs-temporal comparisons),
///   so all integer widths collapse to one class.
/// - [`UnitClass::Timestamp`]: `Timestamp`/`TimestampTz` — microseconds
///   since the epoch. Interchangeable, so they share a class (preserving
///   the existing, sound Timestamp ↔ TimestampTz cross-compat).
/// - [`UnitClass::Date`]: `Date` — days since 2000-01-01.
/// - [`UnitClass::Time`]: `Time` — microseconds since midnight.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum UnitClass {
    Int,
    Timestamp,
    Date,
    Time,
}

/// The unit-class of a column's [`DataType`], or `None` for types that do
/// not take an i64 range lock at all (mirrors `column_idx_for_int_key`).
const fn column_unit_class(data_type: &DataType) -> Option<UnitClass> {
    match data_type {
        DataType::Bool | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Some(UnitClass::Int)
        }
        DataType::Timestamp | DataType::TimestampTz => Some(UnitClass::Timestamp),
        DataType::Date => Some(UnitClass::Date),
        DataType::Time => Some(UnitClass::Time),
        _ => None,
    }
}

/// The unit-class of a literal [`Value`], or `None` for values that have
/// no i64 mapping (mirrors `literal_as_i64`).
const fn value_unit_class(value: &Value) -> Option<UnitClass> {
    match value {
        Value::Bool(_) | Value::Int16(_) | Value::Int32(_) | Value::Int64(_) => {
            Some(UnitClass::Int)
        }
        Value::Timestamp(_) | Value::TimestampTz(_) => Some(UnitClass::Timestamp),
        Value::Date(_) => Some(UnitClass::Date),
        Value::Time(_) => Some(UnitClass::Time),
        _ => None,
    }
}

/// Whether the literal `value` is in the same i64 unit-class as a column
/// of `col_data_type` — the precondition for a sound tight range. Returns
/// `false` for any cross-class pair (e.g. `Date` column vs `Timestamp`
/// literal, or `Time` vs `Timestamp`) so the caller degrades to the safe
/// relation-wide lock instead of a mis-scaled column range.
pub(crate) fn literal_in_same_unit_class_as_column(
    col_data_type: &DataType,
    value: &Value,
) -> bool {
    matches!(
        (column_unit_class(col_data_type), value_unit_class(value)),
        (Some(col), Some(lit)) if col == lit
    )
}

/// Lift a literal to `i64` *only* when it shares the keyed column's i64
/// unit-class; otherwise `None`. This is the guarded form
/// `match_simple_comparison` uses so a cross-unit-class temporal pair
/// never produces a tight (and therefore mis-scaled) `i64` range.
fn literal_as_i64_for_column(col: &ScalarExpr, lit: &ScalarExpr) -> Option<i64> {
    let ScalarExpr::Column { data_type, .. } = col else {
        return None;
    };
    let ScalarExpr::Literal { value, .. } = lit else {
        return None;
    };
    if !literal_in_same_unit_class_as_column(data_type, value) {
        return None;
    }
    literal_as_i64(lit)
}

/// Pick the tighter (i.e., larger) lower bound from two candidates.
/// `None` means "no constraint"; any concrete bound wins over `None`.
const fn max_lower_bound(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(x), Some(y)) => Some(if x > y { x } else { y }),
    }
}

/// Pick the tighter (i.e., smaller) upper bound from two candidates.
const fn min_upper_bound(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(x), Some(y)) => Some(if x < y { x } else { y }),
    }
}

pub(super) fn key_value_for_expr(key: i64, expr: &ScalarExpr) -> Option<Value> {
    let ScalarExpr::Column { data_type, .. } = expr else {
        return None;
    };
    match data_type {
        DataType::Int32 => i32::try_from(key).ok().map(Value::Int32),
        DataType::Int64 => Some(Value::Int64(key)),
        _ => None,
    }
}
