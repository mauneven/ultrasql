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
/// `(column_index, IndexKeyRange)`. Returns `None` when the operand
/// types are not Int32 / Int64, the literal cannot be represented as
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
    let (col_idx, raw_lit, op_normalised) = match (left.as_ref(), right.as_ref()) {
        (col @ ScalarExpr::Column { .. }, lit @ ScalarExpr::Literal { .. }) => {
            let idx = column_idx_for_int_key(col)?;
            let lit_val = literal_as_i64(lit)?;
            (idx, lit_val, *op)
        }
        (lit @ ScalarExpr::Literal { .. }, col @ ScalarExpr::Column { .. }) => {
            let idx = column_idx_for_int_key(col)?;
            let lit_val = literal_as_i64(lit)?;
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
        | DataType::TimestampTz => Some(*index),
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
        _ => None,
    }
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
