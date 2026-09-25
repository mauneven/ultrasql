//! Vectorised fast-path support for [`Filter`](super::Filter).
//!
//! This module holds the predicate-shape matcher, the heuristic
//! selectivity estimator, and the SIMD comparison kernels that build a
//! `Bitmap` mask in one pass over the input columns. The matcher caches
//! a [`FastPredicate`] descriptor at construction so the shape-matching
//! cost is paid once.

use ultrasql_core::{DataType, Value};
use ultrasql_planner::{BinaryOp, ScalarExpr};
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{Column, NumericColumn, StringColumn};
use ultrasql_vec::dict::DictionaryColumn;
use ultrasql_vec::kernels::{CmpOp, cmp_i32_scalar, cmp_i64_scalar, compare_i32, compare_i64};

use super::FastPredicate;

pub(super) fn estimate_predicate_selectivity(expr: &ScalarExpr) -> f64 {
    let selectivity = match expr {
        ScalarExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => estimate_predicate_selectivity(left) * estimate_predicate_selectivity(right),
        ScalarExpr::Binary {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } => {
            let left_sel = estimate_predicate_selectivity(left);
            let right_sel = estimate_predicate_selectivity(right);
            left_sel + right_sel - (left_sel * right_sel)
        }
        ScalarExpr::Binary { op, .. } => match op {
            BinaryOp::Eq => 0.1,
            BinaryOp::NotEq => 0.9,
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => 0.33,
            _ => 0.5,
        },
        ScalarExpr::IsNull { negated, .. } => {
            if *negated {
                0.95
            } else {
                0.05
            }
        }
        ScalarExpr::Unary { .. } | ScalarExpr::FunctionCall { .. } => 0.5,
        ScalarExpr::Literal { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::Column { .. }
        | ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => 0.5,
    };
    selectivity.clamp(0.0, 1.0)
}

/// Match a simple comparison shape and produce a cached descriptor for
/// the vectorised path.
///
/// Returns `None` for any other predicate shape, including:
/// - Nested expressions (`col + 1 > 5`).
/// - Logical conjunctions (`a > 5 AND b < 10`).
/// - NULL literals — `WHERE col = NULL` always evaluates to NULL/false
///   in SQL but the existing scalar path already handles that.
pub(super) fn match_fast_predicate(expr: &ScalarExpr) -> Option<FastPredicate> {
    if let ScalarExpr::Binary {
        op: BinaryOp::And,
        left,
        right,
        ..
    } = expr
    {
        return Some(FastPredicate::And(
            Box::new(match_fast_predicate(left)?),
            Box::new(match_fast_predicate(right)?),
        ));
    }
    if let ScalarExpr::Binary {
        op: BinaryOp::Or,
        left,
        right,
        ..
    } = expr
    {
        return Some(FastPredicate::Or(
            Box::new(match_fast_predicate(left)?),
            Box::new(match_fast_predicate(right)?),
        ));
    }
    let ScalarExpr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    if let Some(like) = match_column_like(*op, left, right) {
        return Some(like);
    }
    let cmp = binary_op_to_cmp(*op)?;
    // Case 1: `column <op> literal`
    if let (ScalarExpr::Column { index, .. }, ScalarExpr::Literal { value, .. }) =
        (left.as_ref(), right.as_ref())
    {
        if matches!(value, Value::Null) {
            return None;
        }
        return Some(FastPredicate::ColumnLiteral {
            index: *index,
            op: cmp,
            literal: value.clone(),
        });
    }
    // Case 2: `literal <op> column` — flip the operator so the kernel
    // always sees the column on the left.
    if let (ScalarExpr::Literal { value, .. }, ScalarExpr::Column { index, .. }) =
        (left.as_ref(), right.as_ref())
    {
        if matches!(value, Value::Null) {
            return None;
        }
        return Some(FastPredicate::ColumnLiteral {
            index: *index,
            op: flip_cmp(cmp),
            literal: value.clone(),
        });
    }
    // Case 3: `left_column <op> right_column`.
    if let (
        ScalarExpr::Column {
            index: left_index, ..
        },
        ScalarExpr::Column {
            index: right_index, ..
        },
    ) = (left.as_ref(), right.as_ref())
    {
        return Some(FastPredicate::ColumnColumn {
            left_index: *left_index,
            right_index: *right_index,
            op: cmp,
        });
    }
    None
}

#[derive(Clone, Copy)]
pub(super) enum MaskCombine {
    And,
    Or,
}

pub(super) fn combine_masks(left: &Bitmap, right: &Bitmap, combine: MaskCombine) -> Bitmap {
    debug_assert_eq!(left.len(), right.len(), "mask lengths must align");
    let words = left
        .words()
        .iter()
        .zip(right.words().iter())
        .map(|(left_word, right_word)| match combine {
            MaskCombine::And => left_word & right_word,
            MaskCombine::Or => left_word | right_word,
        })
        .collect();
    Bitmap::from_words(words, left.len())
}

const fn binary_op_to_cmp(op: BinaryOp) -> Option<CmpOp> {
    match op {
        BinaryOp::Eq => Some(CmpOp::Eq),
        BinaryOp::NotEq => Some(CmpOp::Ne),
        BinaryOp::Lt => Some(CmpOp::Lt),
        BinaryOp::LtEq => Some(CmpOp::Le),
        BinaryOp::Gt => Some(CmpOp::Gt),
        BinaryOp::GtEq => Some(CmpOp::Ge),
        _ => None,
    }
}

/// Flip an ordering operator so that `lit <op> col` becomes the
/// equivalent `col <flipped_op> lit`. `Eq`/`Ne` are symmetric.
const fn flip_cmp(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Eq => CmpOp::Eq,
        CmpOp::Ne => CmpOp::Ne,
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Le => CmpOp::Ge,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::Ge => CmpOp::Le,
    }
}

/// Build a constant-valued mask for an `i32` column when the comparison
/// literal lies outside the `i32` range — every row gives the same
/// answer. NULL rows still get a 0 bit.
pub(super) fn const_mask_i32(column: &NumericColumn<i32>, literal_i64: i64, op: CmpOp) -> Bitmap {
    let high = literal_i64 > i64::from(i32::MAX);
    let constant_result = match op {
        CmpOp::Eq => false,
        CmpOp::Ne => true,
        // i32 values are all < literal when literal > MAX, and
        // all > literal when literal < MIN.
        CmpOp::Lt | CmpOp::Le => high,
        CmpOp::Gt | CmpOp::Ge => !high,
    };
    let n = column.len();
    if !constant_result {
        return Bitmap::new(n, false);
    }
    let mut bm = Bitmap::new(n, true);
    if let Some(nulls) = column.nulls() {
        let words = bm.words_mut();
        for (w, &v) in words.iter_mut().zip(nulls.words().iter()) {
            *w &= v;
        }
    }
    bm
}

/// Vectorised `left_column <op> right_column` comparison for the
/// physical integer families used by dates, timestamps, decimals, and
/// regular integer columns.
pub(super) fn cmp_columns_to_mask(
    left: &Column,
    right: &Column,
    left_type: &DataType,
    right_type: &DataType,
    op: CmpOp,
) -> Option<Bitmap> {
    if !raw_ordering_matches_logical_ordering(left_type, right_type) {
        return None;
    }
    match (left, right) {
        (Column::Int32(l), Column::Int32(r)) => {
            let validity = merge_numeric_validity(l, r);
            let cmp = compare_i32(l, r, validity.as_ref());
            Some(cmp_i32_scalar(&cmp, 0, op))
        }
        (Column::Int64(l), Column::Int64(r)) => {
            let validity = merge_numeric_validity(l, r);
            let cmp = compare_i64(l, r, validity.as_ref());
            Some(cmp_i64_scalar(&cmp, 0, op))
        }
        _ => None,
    }
}

fn raw_ordering_matches_logical_ordering(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::Int16, DataType::Int16)
        | (DataType::Int32, DataType::Int32)
        | (DataType::Int64, DataType::Int64)
        | (DataType::Money, DataType::Money)
        | (DataType::Oid, DataType::Oid)
        | (DataType::RegClass, DataType::RegClass)
        | (DataType::RegType, DataType::RegType)
        | (DataType::Date, DataType::Date)
        | (DataType::Time, DataType::Time)
        | (DataType::Timestamp, DataType::Timestamp)
        | (DataType::TimestampTz, DataType::TimestampTz)
        | (DataType::Timestamp, DataType::TimestampTz)
        | (DataType::TimestampTz, DataType::Timestamp) => true,
        // Decimal columns materialise as decimal text (not a raw i64
        // column), so they never reach the i64 fast path; they are
        // compared through the general decode/eval path which aligns
        // scales and handles the full i128 mantissa.
        _ => false,
    }
}

/// Merge two numeric validity masks. `None` means all rows valid.
fn merge_numeric_validity<T>(left: &NumericColumn<T>, right: &NumericColumn<T>) -> Option<Bitmap> {
    match (left.nulls(), right.nulls()) {
        (None, None) => None,
        (Some(l), None) => Some(l.clone()),
        (None, Some(r)) => Some(r.clone()),
        (Some(l), Some(r)) => {
            let mut merged = l.clone();
            for (word, &right_word) in merged.words_mut().iter_mut().zip(r.words().iter()) {
                *word &= right_word;
            }
            Some(merged)
        }
    }
}

/// Match `column [NOT] LIKE 'literal'` whose pattern uses only `%`.
fn match_column_like(op: BinaryOp, left: &ScalarExpr, right: &ScalarExpr) -> Option<FastPredicate> {
    let negated = match op {
        BinaryOp::Like => false,
        BinaryOp::NotLike => true,
        _ => return None,
    };
    let (ScalarExpr::Column { index, .. }, ScalarExpr::Literal { value, .. }) = (left, right)
    else {
        return None;
    };
    let Value::Text(pattern) = value else {
        return None;
    };
    Some(FastPredicate::ColumnLike {
        index: *index,
        pattern: PercentLikePattern::compile(pattern)?,
        negated,
    })
}

/// A case-sensitive `LIKE` pattern whose only wildcard is `%`, stored as
/// the literal runs between the wildcards. Backslash escapes the next
/// character, as in [`crate::eval::like`]. Patterns containing `_` are not
/// represented and keep using the general evaluator.
#[derive(Clone, Debug)]
pub(super) struct PercentLikePattern {
    /// Literal runs; `parts.len()` is one more than the number of `%`.
    parts: Vec<String>,
}

impl PercentLikePattern {
    pub(super) fn compile(pattern: &str) -> Option<Self> {
        let mut parts = vec![String::new()];
        let mut chars = pattern.chars();
        while let Some(ch) = chars.next() {
            match ch {
                '%' => parts.push(String::new()),
                '_' => return None,
                '\\' => parts.last_mut()?.push(chars.next().unwrap_or('\\')),
                other => parts.last_mut()?.push(other),
            }
        }
        Some(Self { parts })
    }

    pub(super) fn matches(&self, haystack: &str) -> bool {
        let (Some(first), Some(last)) = (self.parts.first(), self.parts.last()) else {
            return false;
        };
        if self.parts.len() == 1 {
            return haystack == first;
        }
        if haystack.len() < first.len() + last.len()
            || !haystack.starts_with(first.as_str())
            || !haystack.ends_with(last.as_str())
        {
            return false;
        }
        let mut pos = first.len();
        let end = haystack.len() - last.len();
        for middle in &self.parts[1..self.parts.len() - 1] {
            if middle.is_empty() {
                continue;
            }
            let Some(found) = haystack
                .get(pos..end)
                .and_then(|window| window.find(middle.as_str()))
            else {
                return false;
            };
            pos += found + middle.len();
        }
        true
    }
}

/// Evaluate `[NOT] LIKE` over a string column. NULL rows get a 0 bit, as
/// SQL's three-valued logic filters them out either way. Dictionary
/// columns evaluate the pattern once per distinct value.
pub(super) fn like_mask(
    column: &Column,
    pattern: &PercentLikePattern,
    negated: bool,
) -> Option<Bitmap> {
    match column {
        Column::Utf8(strings) => Some(like_mask_strings(strings, pattern, negated)),
        Column::DictionaryUtf8(dict) => Some(like_mask_dictionary(dict, pattern, negated)),
        _ => None,
    }
}

fn like_mask_strings(column: &StringColumn, pattern: &PercentLikePattern, negated: bool) -> Bitmap {
    let rows = column.len();
    let mut mask = Bitmap::new(rows, false);
    for row in 0..rows {
        if column.nulls().is_some_and(|nulls| !nulls.get(row)) {
            continue;
        }
        if let Some(value) = column.try_value(row)
            && pattern.matches(value) != negated
        {
            mask.set(row, true);
        }
    }
    mask
}

fn like_mask_dictionary(
    column: &DictionaryColumn,
    pattern: &PercentLikePattern,
    negated: bool,
) -> Bitmap {
    let passes: Vec<bool> = column
        .dict
        .iter()
        .map(|value| pattern.matches(value) != negated)
        .collect();
    let codes = column.codes.data();
    let mut mask = Bitmap::new(codes.len(), false);
    for (row, code) in codes.iter().enumerate() {
        if column.codes.nulls().is_some_and(|nulls| !nulls.get(row)) {
            continue;
        }
        let pass = usize::try_from(*code)
            .ok()
            .and_then(|code| passes.get(code))
            .copied()
            .unwrap_or(false);
        if pass {
            mask.set(row, true);
        }
    }
    mask
}

#[cfg(test)]
mod like_fast_path_tests {
    use super::PercentLikePattern;
    use crate::eval::like::like_match;

    fn strings_over(alphabet: &[char], max_len: usize) -> Vec<String> {
        let mut out = vec![String::new()];
        let mut frontier = vec![String::new()];
        for _ in 0..max_len {
            let mut next = Vec::new();
            for prefix in &frontier {
                for ch in alphabet {
                    let mut s = prefix.clone();
                    s.push(*ch);
                    next.push(s);
                }
            }
            out.extend(next.iter().cloned());
            frontier = next;
        }
        out
    }

    #[test]
    fn percent_patterns_agree_with_the_general_like_evaluator() {
        let patterns = strings_over(&['a', 'b', '%', '\\'], 5);
        let haystacks = strings_over(&['a', 'b', '%', 'é'], 4);
        let mut checked = 0_usize;
        for pattern in &patterns {
            let Some(compiled) = PercentLikePattern::compile(pattern) else {
                continue;
            };
            for haystack in &haystacks {
                assert_eq!(
                    compiled.matches(haystack),
                    like_match(haystack, pattern, false),
                    "pattern {pattern:?} haystack {haystack:?}"
                );
                checked += 1;
            }
        }
        assert!(checked > 100_000);
    }

    #[test]
    fn underscore_patterns_use_the_general_evaluator() {
        assert!(PercentLikePattern::compile("a_b").is_none());
        assert!(PercentLikePattern::compile("a\\_b").is_some());
    }
}
