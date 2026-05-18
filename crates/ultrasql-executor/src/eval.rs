//! Scalar expression interpreter.
//!
//! Evaluates a [`ScalarExpr`] against a single row (`&[Value]`) and
//! returns a [`Value`]. Operates one row at a time (the OLTP path); the
//! vectorized path is layered in `ultrasql-vec` and will arrive later.
//!
//! NULL handling: SQL three-valued logic. Any arithmetic with a NULL
//! operand returns NULL. Boolean operators follow Kleene logic
//! (NULL AND FALSE = FALSE, NULL AND TRUE = NULL, NULL OR TRUE = TRUE,
//! NULL OR FALSE = NULL).
//!
//! # Coverage
//!
//! - `Column { index, .. }` -- `row[index]` (clone)
//! - `Literal { value, .. }` -- `value.clone()`
//! - `Parameter { index, .. }` -- `params[index - 1].clone()`
//! - `Unary` -- `Neg` / `Pos` / `Not` with 3VL NULL propagation
//! - `Binary` -- arithmetic, comparison, boolean Kleene, `||` concat, LIKE/ILIKE
//! - `IsNull { expr, negated }` -- SQL `IS [NOT] NULL`
//! - Other variants (`Cast`, `Subquery`, `Exists`, ...) -- [`EvalError::Unsupported`]

use ultrasql_core::Value;
use ultrasql_planner::{BinaryOp, ScalarExpr, UnaryOp};

const MICROS_PER_DAY: i64 = 86_400_000_000;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors raised when [`Eval::eval`] cannot produce a result.
#[derive(Debug, thiserror::Error)]
pub enum EvalError {
    /// A runtime type did not match what the expression expected.
    #[error("type mismatch at runtime: {0}")]
    Type(String),

    /// An arithmetic operation produced a result outside the target type's
    /// representable range.
    #[error("integer overflow")]
    Overflow,

    /// Division or modulo by zero.
    #[error("division by zero")]
    DivByZero,

    /// A column reference addressed an index outside the row's length.
    #[error("column index out of range: {index} (row has {len} columns)")]
    ColumnIndex {
        /// The out-of-range index.
        index: usize,
        /// Number of columns in the row.
        len: usize,
    },

    /// A `$N` parameter reference addressed an index beyond the bound
    /// parameter list.
    #[error("parameter index out of range: ${index} (have {len} bound)")]
    ParameterIndex {
        /// The 1-based parameter index that was out of range.
        index: u32,
        /// Number of parameters bound to this evaluator.
        len: usize,
    },

    /// An expression variant not covered in v0.5 was encountered.
    #[error("unsupported expression in v0.5: {0}")]
    Unsupported(&'static str),
}

// ---------------------------------------------------------------------------
// Evaluator struct
// ---------------------------------------------------------------------------

/// Row-at-a-time scalar expression evaluator.
///
/// Evaluates a single [`ScalarExpr`] against one row represented as a
/// `&[Value]` slice. The evaluator is stateless apart from the expression
/// tree and the optional parameter vector; it can be reused across many
/// rows without re-allocation.
///
/// The public type is named `Eval` for brevity; the long-form `Evaluator`
/// alias points to the same struct.
///
/// # Example
///
/// ```ignore
/// let expr = ScalarExpr::Binary { op: BinaryOp::Add, ... };
/// let ev = Eval::new(expr);
/// let result = ev.eval_row(&[Value::Int32(3), Value::Int32(4)])?;
/// assert_eq!(result, Value::Int32(7));
/// ```
#[derive(Clone, Debug)]
pub struct Eval {
    expr: ScalarExpr,
    params: Vec<Value>,
}

impl Eval {
    /// Construct an evaluator with no bound parameters.
    #[must_use]
    pub const fn new(expr: ScalarExpr) -> Self {
        Self {
            expr,
            params: Vec::new(),
        }
    }

    /// Construct an evaluator with a pre-bound parameter list.
    ///
    /// `$1` maps to `params[0]`, `$2` to `params[1]`, and so on.
    #[must_use]
    pub const fn with_params(expr: ScalarExpr, params: Vec<Value>) -> Self {
        Self { expr, params }
    }

    /// Evaluate the expression against `row` and return the result.
    ///
    /// `row` must be at least as long as the highest column index
    /// referenced in the expression tree; otherwise
    /// [`EvalError::ColumnIndex`] is returned.
    ///
    /// # Errors
    ///
    /// Returns [`EvalError`] on type mismatches, overflow, division by
    /// zero, out-of-range column/parameter indices, or unsupported
    /// expression variants.
    pub fn eval(&self, row: &[Value]) -> Result<Value, EvalError> {
        eval_expr(&self.expr, row, &self.params)
    }
}

// ---------------------------------------------------------------------------
// Core recursive evaluator
// ---------------------------------------------------------------------------

pub(crate) fn eval_expr(
    expr: &ScalarExpr,
    row: &[Value],
    params: &[Value],
) -> Result<Value, EvalError> {
    match expr {
        ScalarExpr::Column { index, .. } => {
            row.get(*index).cloned().ok_or(EvalError::ColumnIndex {
                index: *index,
                len: row.len(),
            })
        }

        ScalarExpr::Literal { value, .. } => Ok(value.clone()),

        ScalarExpr::Parameter { index, .. } => {
            // Parameter indices are 1-based; convert to 0-based for slice access.
            let zero_idx = usize::try_from(index.saturating_sub(1))
                .expect("parameter index fits usize on all supported targets");
            params
                .get(zero_idx)
                .cloned()
                .ok_or(EvalError::ParameterIndex {
                    index: *index,
                    len: params.len(),
                })
        }

        ScalarExpr::Unary { op, expr, .. } => {
            let val = eval_expr(expr, row, params)?;
            apply_unary(*op, val)
        }

        ScalarExpr::Binary {
            op, left, right, ..
        } => {
            // For Kleene AND/OR we need short-circuit evaluation to handle
            // three-valued logic correctly without evaluating the right side
            // unnecessarily when a definitive answer is already available.
            match op {
                BinaryOp::And => eval_and(left, right, row, params),
                BinaryOp::Or => eval_or(left, right, row, params),
                _ => {
                    let lv = eval_expr(left, row, params)?;
                    let rv = eval_expr(right, row, params)?;
                    apply_binary(*op, lv, rv)
                }
            }
        }

        ScalarExpr::IsNull { expr, negated } => {
            let val = eval_expr(expr, row, params)?;
            let is_null = matches!(val, Value::Null);
            Ok(Value::Bool(is_null ^ negated))
        }

        // Subquery / outer-scope variants are produced by the binder but
        // are not yet runnable by the row-at-a-time interpreter. The
        // optimizer's subquery-decorrelation rule lowers them to joins
        // before the executor sees them; if any survive here it is a
        // bug upstream.
        ScalarExpr::OuterColumn { .. } => Err(EvalError::Unsupported(
            "outer-column reference reached the executor; decorrelation rule should have removed it",
        )),
        ScalarExpr::ScalarSubquery { .. } => Err(EvalError::Unsupported(
            "scalar subquery reached the executor; decorrelation rule should have removed it",
        )),
        ScalarExpr::Exists { .. } => Err(EvalError::Unsupported(
            "EXISTS subquery reached the executor; decorrelation rule should have removed it",
        )),
        ScalarExpr::InSubquery { .. } => Err(EvalError::Unsupported(
            "IN subquery reached the executor; decorrelation rule should have removed it",
        )),

        ScalarExpr::FunctionCall { name, args, .. } => {
            let evaluated: Result<Vec<Value>, EvalError> =
                args.iter().map(|a| eval_expr(a, row, params)).collect();
            let vals = evaluated?;
            eval_function_call(name, &vals)
        }
    }
}

// ---------------------------------------------------------------------------
// Built-in function dispatch
// ---------------------------------------------------------------------------

/// Dispatch the binder-resolved builtin scalar function calls.
///
/// Today's support set is the slice needed for TPC-H lift-off:
/// - `extract(unit, date)` — date-part extraction. Returns `i64`.
/// - `lower(text)` / `upper(text)` — case folding for expression indexes
///   and simple scalar projections.
/// - `pg_get_userbyid(oid)` — compatibility helper for psql catalog meta SQL.
/// - `substring(text, from[, for])` — 1-based string slicing.
///
/// Unknown function names return [`EvalError::Unsupported`] so the
/// binder upgrade lands ahead of executor coverage without crashing.
fn eval_function_call(name: &str, args: &[Value]) -> Result<Value, EvalError> {
    match name {
        "extract" => eval_extract(args),
        "lower" => eval_text_case(args, TextCase::Lower),
        "upper" => eval_text_case(args, TextCase::Upper),
        "pg_get_userbyid" => eval_pg_get_userbyid(args),
        "substring" => eval_substring(args),
        "coalesce" => Ok(args
            .iter()
            .find(|v| !matches!(v, Value::Null))
            .cloned()
            .unwrap_or(Value::Null)),
        "case_searched" => eval_case_searched(args),
        "case_simple" => eval_case_simple(args),
        other => Err(EvalError::Unsupported(Box::leak(
            format!("function `{other}` not implemented").into_boxed_str(),
        ))),
    }
}

#[derive(Clone, Copy)]
enum TextCase {
    Lower,
    Upper,
}

fn eval_text_case(args: &[Value], mode: TextCase) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "text case function: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Value::Text(s) = &args[0] else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "text case function: argument must be text, got {:?}",
                args[0].data_type()
            )))
        };
    };
    let out = match mode {
        TextCase::Lower => s.to_lowercase(),
        TextCase::Upper => s.to_uppercase(),
    };
    Ok(Value::Text(out))
}

fn eval_pg_get_userbyid(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_get_userbyid: expected 1 arg, got {}",
            args.len()
        )));
    }
    let oid = match &args[0] {
        Value::Int16(v) => i64::from(*v),
        Value::Int32(v) => i64::from(*v),
        Value::Int64(v) => *v,
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "pg_get_userbyid: oid must be integer, got {:?}",
                other.data_type()
            )));
        }
    };
    let name = if oid == 10 {
        "ultrasql".to_owned()
    } else {
        format!("unknown (OID={oid})")
    };
    Ok(Value::Text(name))
}

/// `CASE WHEN c1 THEN v1 … ELSE e END` — args layout:
/// `[c1, v1, c2, v2, …, else]`.
fn eval_case_searched(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Err(EvalError::Type(
            "case_searched: expected odd arg count (cond, then pairs + else)".into(),
        ));
    }
    let else_val = args.last().cloned().unwrap_or(Value::Null);
    let mut i = 0;
    while i + 1 < args.len() - 1 {
        match &args[i] {
            Value::Bool(true) => return Ok(args[i + 1].clone()),
            Value::Bool(false) | Value::Null => {}
            other => {
                return Err(EvalError::Type(format!(
                    "case_searched: WHEN clause must yield bool, got {:?}",
                    other.data_type()
                )));
            }
        }
        i += 2;
    }
    Ok(else_val)
}

/// `CASE op WHEN w1 THEN v1 … ELSE e END` — args layout:
/// `[op, w1, v1, w2, v2, …, else]`.
fn eval_case_simple(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() < 4 || args.len() % 2 != 0 {
        return Err(EvalError::Type(
            "case_simple: expected even arg count ≥ 4 (operand + pairs + else)".into(),
        ));
    }
    let op = &args[0];
    let else_val = args.last().cloned().unwrap_or(Value::Null);
    let mut i = 1;
    while i + 1 < args.len() - 1 {
        if values_equal_for_case(op, &args[i]) {
            return Ok(args[i + 1].clone());
        }
        i += 2;
    }
    Ok(else_val)
}

fn values_equal_for_case(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => false,
        _ => a == b,
    }
}

fn eval_extract(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "extract: expected 2 args, got {}",
            args.len()
        )));
    }
    let unit = match &args[0] {
        Value::Text(s) => s.as_str(),
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "extract: unit must be text, got {:?}",
                other.data_type()
            )));
        }
    };
    let (year, month, day) = match &args[1] {
        Value::Date(d) => civil_from_days(*d),
        Value::Timestamp(us) | Value::TimestampTz(us) => {
            let days = (*us).div_euclid(86_400_000_000);
            let days_i32 = i32::try_from(days).unwrap_or(i32::MAX);
            civil_from_days(days_i32)
        }
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "extract: source must be date/timestamp, got {:?}",
                other.data_type()
            )));
        }
    };
    let unit_norm = unit.to_ascii_lowercase();
    let out_i64 = match unit_norm.as_str() {
        "year" => i64::from(year),
        "month" => i64::from(month),
        "day" => i64::from(day),
        "quarter" => i64::from((month - 1) / 3 + 1),
        other => {
            return Err(EvalError::Type(format!(
                "extract: unit `{other}` not implemented"
            )));
        }
    };
    Ok(Value::Int64(out_i64))
}

/// Inverse of the Howard-Hinnant `days_from_civil` algorithm, rebased
/// on the 2000-01-01 epoch the engine uses. Returns `(year, month, day)`
/// in the standard 1-based calendar.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "civil-from-days arithmetic; doe / yoe fit in i32 by construction"
)]
fn civil_from_days(days_since_2000_01_01: i32) -> (i32, i32, i32) {
    let z = days_since_2000_01_01 + 10_957; // rebase to 1970-01-01
    let z = z + 719_468; // shift to year 0
    let era = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    let doe = (z - era * 146_097) as u32; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = (yoe as i32) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as i32; // [1, 31]
    let m = if mp < 10 {
        mp as i32 + 3
    } else {
        mp as i32 - 9
    }; // [1, 12]
    let final_y = if m <= 2 { y + 1 } else { y };
    (final_y, m, d)
}

fn eval_substring(args: &[Value]) -> Result<Value, EvalError> {
    if !(2..=3).contains(&args.len()) {
        return Err(EvalError::Type(format!(
            "substring: expected 2 or 3 args, got {}",
            args.len()
        )));
    }
    let s = match &args[0] {
        Value::Text(s) => s.clone(),
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "substring: source must be text, got {:?}",
                other.data_type()
            )));
        }
    };
    let from = match args[1].as_i64() {
        Some(v) => v,
        None if matches!(args[1], Value::Null) => return Ok(Value::Null),
        _ => {
            return Err(EvalError::Type("substring: `from` must be integer".into()));
        }
    };
    // SQL substring is 1-based and clamps to the string's character
    // range. We operate on bytes for the v0.6 milestone; ASCII-only
    // TPC-H comments / phone numbers / type strings make this safe.
    let bytes = s.as_bytes();
    let start_byte_signed = from.saturating_sub(1);
    let start = if start_byte_signed < 0 {
        0
    } else {
        usize::try_from(start_byte_signed)
            .unwrap_or(bytes.len())
            .min(bytes.len())
    };
    let end = if args.len() == 3 {
        let len = match args[2].as_i64() {
            Some(v) => v,
            None if matches!(args[2], Value::Null) => return Ok(Value::Null),
            _ => {
                return Err(EvalError::Type(
                    "substring: `for` length must be integer".into(),
                ));
            }
        };
        let len = len.max(0);
        let mut effective = usize::try_from(len).unwrap_or(0);
        if start_byte_signed < 0 {
            let abs_back = usize::try_from(start_byte_signed.unsigned_abs()).unwrap_or(usize::MAX);
            effective = effective.saturating_sub(abs_back);
        }
        (start + effective).min(bytes.len())
    } else {
        bytes.len()
    };
    let slice = &bytes[start..end];
    let out = std::str::from_utf8(slice)
        .map_err(|_| EvalError::Type("substring: utf-8 boundary mid-character".into()))?;
    Ok(Value::Text(out.to_owned()))
}

// ---------------------------------------------------------------------------
// Kleene short-circuit AND / OR
// ---------------------------------------------------------------------------

/// Kleene three-valued AND:
/// - `FALSE AND anything = FALSE`
/// - `TRUE AND x = x`
/// - `NULL AND FALSE = FALSE`, `NULL AND TRUE = NULL`
fn eval_and(
    left: &ScalarExpr,
    right: &ScalarExpr,
    row: &[Value],
    params: &[Value],
) -> Result<Value, EvalError> {
    let lv = eval_expr(left, row, params)?;
    // FALSE short-circuits regardless of the right operand.
    if matches!(lv, Value::Bool(false)) {
        return Ok(Value::Bool(false));
    }
    let rv = eval_expr(right, row, params)?;
    match (lv, rv) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Null, Value::Bool(true) | Value::Null) | (Value::Bool(true), Value::Null) => {
            Ok(Value::Null)
        }
        (l, r) => Err(EvalError::Type(format!(
            "AND requires boolean operands, got {l:?} AND {r:?}"
        ))),
    }
}

/// Kleene three-valued OR:
/// - `TRUE OR anything = TRUE`
/// - `FALSE OR x = x`
/// - `NULL OR TRUE = TRUE`, `NULL OR FALSE = NULL`
fn eval_or(
    left: &ScalarExpr,
    right: &ScalarExpr,
    row: &[Value],
    params: &[Value],
) -> Result<Value, EvalError> {
    let lv = eval_expr(left, row, params)?;
    // TRUE short-circuits regardless of the right operand.
    if matches!(lv, Value::Bool(true)) {
        return Ok(Value::Bool(true));
    }
    let rv = eval_expr(right, row, params)?;
    match (lv, rv) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Null, Value::Bool(false) | Value::Null) | (Value::Bool(false), Value::Null) => {
            Ok(Value::Null)
        }
        (l, r) => Err(EvalError::Type(format!(
            "OR requires boolean operands, got {l:?} OR {r:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Unary operators
// ---------------------------------------------------------------------------

fn apply_unary(op: UnaryOp, val: Value) -> Result<Value, EvalError> {
    match op {
        UnaryOp::Pos => {
            // `+x` is a no-op for all numeric types; propagates NULL.
            Ok(val)
        }

        UnaryOp::Neg => match val {
            Value::Null => Ok(Value::Null),
            Value::Int16(v) => v.checked_neg().map(Value::Int16).ok_or(EvalError::Overflow),
            Value::Int32(v) => v.checked_neg().map(Value::Int32).ok_or(EvalError::Overflow),
            Value::Int64(v) => v.checked_neg().map(Value::Int64).ok_or(EvalError::Overflow),
            Value::Float32(v) => Ok(Value::Float32(-v)),
            Value::Float64(v) => Ok(Value::Float64(-v)),
            other => Err(EvalError::Type(format!(
                "unary negation not defined for {other:?}"
            ))),
        },

        UnaryOp::Not => match val {
            Value::Null => Ok(Value::Null),
            Value::Bool(b) => Ok(Value::Bool(!b)),
            other => Err(EvalError::Type(format!(
                "NOT requires boolean operand, got {other:?}"
            ))),
        },

        UnaryOp::BitNot => match val {
            Value::Null => Ok(Value::Null),
            Value::Int16(v) => Ok(Value::Int16(!v)),
            Value::Int32(v) => Ok(Value::Int32(!v)),
            Value::Int64(v) => Ok(Value::Int64(!v)),
            other => Err(EvalError::Type(format!(
                "bitwise NOT (~) requires integer operand, got {other:?}"
            ))),
        },
    }
}

// ---------------------------------------------------------------------------
// Binary operators
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn apply_binary(op: BinaryOp, lv: Value, rv: Value) -> Result<Value, EvalError> {
    // NULL propagation for arithmetic and comparison ops.
    if matches!((&lv, &rv), (Value::Null, _) | (_, Value::Null)) {
        match op {
            BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::Pow
            | BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
            | BinaryOp::Concat
            | BinaryOp::Like
            | BinaryOp::NotLike
            | BinaryOp::Ilike
            | BinaryOp::NotIlike
            | BinaryOp::RegexMatch
            | BinaryOp::RegexIMatch
            | BinaryOp::RegexNotMatch
            | BinaryOp::RegexNotIMatch
            | BinaryOp::BitAnd
            | BinaryOp::BitOr
            | BinaryOp::BitXor
            | BinaryOp::ShiftLeft
            | BinaryOp::ShiftRight
            | BinaryOp::JsonGet
            | BinaryOp::JsonGetText
            | BinaryOp::JsonGetPath
            | BinaryOp::JsonGetPathText
            | BinaryOp::JsonContains
            | BinaryOp::JsonContained
            | BinaryOp::Overlap
            | BinaryOp::JsonHasKey
            | BinaryOp::JsonHasAnyKey
            | BinaryOp::JsonHasAllKeys => return Ok(Value::Null),
            // AND/OR handled in eval_and/eval_or and never reach here.
            BinaryOp::And | BinaryOp::Or => {
                unreachable!("AND/OR handled in short-circuit paths")
            }
        }
    }

    match op {
        // ------------------------------------------------------------------
        // Arithmetic
        // ------------------------------------------------------------------
        BinaryOp::Add => numeric_arith(lv, rv, ArithOp::Add),
        BinaryOp::Sub => numeric_arith(lv, rv, ArithOp::Sub),
        BinaryOp::Mul => numeric_arith(lv, rv, ArithOp::Mul),
        BinaryOp::Div => numeric_arith(lv, rv, ArithOp::Div),
        BinaryOp::Mod => numeric_arith(lv, rv, ArithOp::Mod),
        BinaryOp::Pow => numeric_arith(lv, rv, ArithOp::Pow),

        // ------------------------------------------------------------------
        // Comparison
        // ------------------------------------------------------------------
        BinaryOp::Eq => value_compare(&lv, &rv, |c| c == std::cmp::Ordering::Equal),
        BinaryOp::NotEq => value_compare(&lv, &rv, |c| c != std::cmp::Ordering::Equal),
        BinaryOp::Lt => value_compare(&lv, &rv, |c| c == std::cmp::Ordering::Less),
        BinaryOp::LtEq => value_compare(&lv, &rv, |c| {
            matches!(c, std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        }),
        BinaryOp::Gt => value_compare(&lv, &rv, |c| c == std::cmp::Ordering::Greater),
        BinaryOp::GtEq => value_compare(&lv, &rv, |c| {
            matches!(c, std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        }),

        // ------------------------------------------------------------------
        // String concatenation
        // ------------------------------------------------------------------
        BinaryOp::Concat => match (lv, rv) {
            (Value::Text(l), Value::Text(r)) => {
                let mut s = l;
                s.push_str(&r);
                Ok(Value::Text(s))
            }
            (l, r) => Err(EvalError::Type(format!(
                "|| requires Text operands, got {l:?} and {r:?}"
            ))),
        },

        // ------------------------------------------------------------------
        // LIKE / NOT LIKE / ILIKE / NOT ILIKE
        // ------------------------------------------------------------------
        BinaryOp::Like | BinaryOp::NotLike | BinaryOp::Ilike | BinaryOp::NotIlike => {
            let case_insensitive = matches!(op, BinaryOp::Ilike | BinaryOp::NotIlike);
            let negated = matches!(op, BinaryOp::NotLike | BinaryOp::NotIlike);
            match (lv, rv) {
                (Value::Text(haystack), Value::Text(pattern)) => {
                    let matched = like_match(&haystack, &pattern, case_insensitive);
                    Ok(Value::Bool(matched ^ negated))
                }
                (l, r) => Err(EvalError::Type(format!(
                    "LIKE requires Text operands, got {l:?} and {r:?}"
                ))),
            }
        }

        // ------------------------------------------------------------------
        // Bitwise integer operators
        // ------------------------------------------------------------------
        BinaryOp::BitAnd => integer_bitwise(lv, rv, |a, b| a & b),
        BinaryOp::BitOr => integer_bitwise(lv, rv, |a, b| a | b),
        BinaryOp::BitXor => integer_bitwise(lv, rv, |a, b| a ^ b),
        BinaryOp::ShiftLeft => integer_bitwise(lv, rv, |a, b| a << (b & 63)),
        BinaryOp::ShiftRight => integer_bitwise(lv, rv, |a, b| a >> (b & 63)),

        // ------------------------------------------------------------------
        // Unsupported operators (regex, JSON)
        // ------------------------------------------------------------------
        BinaryOp::RegexMatch
        | BinaryOp::RegexIMatch
        | BinaryOp::RegexNotMatch
        | BinaryOp::RegexNotIMatch => Err(EvalError::Unsupported("regex operators")),

        BinaryOp::JsonGet
        | BinaryOp::JsonGetText
        | BinaryOp::JsonGetPath
        | BinaryOp::JsonGetPathText
        | BinaryOp::JsonHasKey
        | BinaryOp::JsonHasAnyKey
        | BinaryOp::JsonHasAllKeys => Err(EvalError::Unsupported("JSON operators")),

        BinaryOp::JsonContains => contains_values(&lv, &rv)
            .map(Value::Bool)
            .ok_or(EvalError::Unsupported("JSON operators")),
        BinaryOp::JsonContained => contains_values(&rv, &lv)
            .map(Value::Bool)
            .ok_or(EvalError::Unsupported("JSON operators")),
        BinaryOp::Overlap => overlaps_values(&lv, &rv)
            .map(Value::Bool)
            .ok_or_else(|| EvalError::Type(format!("&& not defined for {lv:?} and {rv:?}"))),

        // AND / OR are handled above; unreachable here.
        BinaryOp::And | BinaryOp::Or => {
            unreachable!("AND/OR handled in short-circuit paths")
        }
    }
}

fn overlaps_values(left: &Value, right: &Value) -> Option<bool> {
    match (left, right) {
        (Value::Range(l), Value::Range(r)) => Some(l.overlaps(r)),
        (Value::Geometry(l), Value::Geometry(r)) => Some(l.overlaps(r)),
        _ => None,
    }
}

fn contains_values(left: &Value, right: &Value) -> Option<bool> {
    match (left, right) {
        (Value::Range(l), Value::Range(r)) => Some(l.contains_range(r)),
        (Value::Geometry(l), Value::Geometry(r)) => Some(l.contains_geometry(r)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Arithmetic helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
}

/// Evaluate an arithmetic binary operation.
///
/// Integer overflow returns [`EvalError::Overflow`]. Division by zero
/// returns [`EvalError::DivByZero`]. Type mismatches return
/// [`EvalError::Type`]. Floating-point overflow produces `f64::INFINITY`
/// (IEEE 754 semantics, consistent with PostgreSQL).
fn numeric_arith(lv: Value, rv: Value, op: ArithOp) -> Result<Value, EvalError> {
    if let Some((left, right)) = decimal_float_operands(&lv, &rv) {
        return float64_arith(left, right, op);
    }

    if matches!(
        (&lv, &rv),
        (Value::Decimal { .. }, _) | (_, Value::Decimal { .. })
    ) {
        let Some((left_value, left_scale)) = numeric_to_decimal(&lv)? else {
            return Err(EvalError::Type(format!(
                "arithmetic type mismatch: {lv:?} and {rv:?}"
            )));
        };
        let Some((right_value, right_scale)) = numeric_to_decimal(&rv)? else {
            return Err(EvalError::Type(format!(
                "arithmetic type mismatch: {lv:?} and {rv:?}"
            )));
        };
        return decimal_arith(left_value, left_scale, right_value, right_scale, op);
    }

    match (lv, rv) {
        (Value::Int16(l), Value::Int16(r)) => int16_arith(l, r, op),
        (Value::Int32(l), Value::Int32(r)) => int32_arith(l, r, op),
        (Value::Int64(l), Value::Int64(r)) => int64_arith(l, r, op),
        (Value::Int16(l), Value::Int32(r)) => int32_arith(i32::from(l), r, op),
        (Value::Int32(l), Value::Int16(r)) => int32_arith(l, i32::from(r), op),
        (Value::Int16(l), Value::Int64(r)) => int64_arith(i64::from(l), r, op),
        (Value::Int64(l), Value::Int16(r)) => int64_arith(l, i64::from(r), op),
        (Value::Int32(l), Value::Int64(r)) => int64_arith(i64::from(l), r, op),
        (Value::Int64(l), Value::Int32(r)) => int64_arith(l, i64::from(r), op),
        (Value::Float32(l), Value::Float32(r)) => float32_arith(l, r, op),
        (Value::Float64(l), Value::Float64(r)) => float64_arith(l, r, op),
        (Value::Float64(l), Value::Float32(r)) => float64_arith(l, f64::from(r), op),
        (Value::Float32(l), Value::Float64(r)) => float64_arith(f64::from(l), r, op),
        (Value::Float64(l), Value::Int16(r)) => float64_arith(l, f64::from(r), op),
        (Value::Int16(l), Value::Float64(r)) => float64_arith(f64::from(l), r, op),
        (Value::Float64(l), Value::Int32(r)) => float64_arith(l, f64::from(r), op),
        (Value::Int32(l), Value::Float64(r)) => float64_arith(f64::from(l), r, op),
        (l, r) => Err(EvalError::Type(format!(
            "arithmetic type mismatch: {l:?} and {r:?}"
        ))),
    }
}

fn decimal_float_operands(left: &Value, right: &Value) -> Option<(f64, f64)> {
    match (left, right) {
        (Value::Decimal { value, scale }, Value::Float32(r)) => {
            Some((decimal_value_to_f64(*value, *scale), f64::from(*r)))
        }
        (Value::Decimal { value, scale }, Value::Float64(r)) => {
            Some((decimal_value_to_f64(*value, *scale), *r))
        }
        (Value::Float32(l), Value::Decimal { value, scale }) => {
            Some((f64::from(*l), decimal_value_to_f64(*value, *scale)))
        }
        (Value::Float64(l), Value::Decimal { value, scale }) => {
            Some((*l, decimal_value_to_f64(*value, *scale)))
        }
        _ => None,
    }
}

fn decimal_value_to_f64(value: i64, scale: i32) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let raw = value as f64;
    raw / 10_f64.powi(scale)
}

fn numeric_to_decimal(value: &Value) -> Result<Option<(i64, i32)>, EvalError> {
    match value {
        Value::Decimal { value, scale } => Ok(Some((*value, *scale))),
        Value::Int16(v) => Ok(Some((i64::from(*v), 0))),
        Value::Int32(v) => Ok(Some((i64::from(*v), 0))),
        Value::Int64(v) => Ok(Some((*v, 0))),
        Value::Float32(v) => decimal_from_f64(f64::from(*v)).map(Some),
        Value::Float64(v) => decimal_from_f64(*v).map(Some),
        _ => Ok(None),
    }
}

fn decimal_from_f64(value: f64) -> Result<(i64, i32), EvalError> {
    if !value.is_finite() {
        return Err(EvalError::Type(
            "cannot coerce non-finite float to decimal".to_owned(),
        ));
    }
    let text = value.to_string();
    decimal_from_text(&text)
        .ok_or_else(|| EvalError::Type(format!("cannot coerce float literal `{text}` to decimal")))
}

fn decimal_from_text(text: &str) -> Option<(i64, i32)> {
    if text.contains('e') || text.contains('E') {
        return None;
    }
    let (negative, unsigned) = text
        .strip_prefix('-')
        .map_or((false, text), |stripped| (true, stripped));
    let (whole, frac) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let scale = i32::try_from(frac.len()).ok()?;
    let mut digits = String::with_capacity(whole.len() + frac.len());
    digits.push_str(if whole.is_empty() { "0" } else { whole });
    digits.push_str(frac);
    let mut value = digits.parse::<i64>().ok()?;
    if negative {
        value = value.checked_neg()?;
    }
    Some((value, scale))
}

fn int16_arith(l: i16, r: i16, op: ArithOp) -> Result<Value, EvalError> {
    let result = match op {
        ArithOp::Add => l.checked_add(r),
        ArithOp::Sub => l.checked_sub(r),
        ArithOp::Mul => l.checked_mul(r),
        ArithOp::Div => {
            if r == 0 {
                return Err(EvalError::DivByZero);
            }
            l.checked_div(r)
        }
        ArithOp::Mod => {
            if r == 0 {
                return Err(EvalError::DivByZero);
            }
            l.checked_rem(r)
        }
        ArithOp::Pow => {
            let base = i64::from(l);
            let exp = i64::from(r);
            if exp < 0 {
                return Err(EvalError::Type(
                    "negative exponent not supported for integer types".to_owned(),
                ));
            }
            let exp_u32 = u32::try_from(exp).map_err(|_| EvalError::Overflow)?;
            let result = base.checked_pow(exp_u32).ok_or(EvalError::Overflow)?;
            let result_i16 = i16::try_from(result).map_err(|_| EvalError::Overflow)?;
            return Ok(Value::Int16(result_i16));
        }
    };
    result.map(Value::Int16).ok_or(EvalError::Overflow)
}

fn int32_arith(l: i32, r: i32, op: ArithOp) -> Result<Value, EvalError> {
    let result = match op {
        ArithOp::Add => l.checked_add(r),
        ArithOp::Sub => l.checked_sub(r),
        ArithOp::Mul => l.checked_mul(r),
        ArithOp::Div => {
            if r == 0 {
                return Err(EvalError::DivByZero);
            }
            l.checked_div(r)
        }
        ArithOp::Mod => {
            if r == 0 {
                return Err(EvalError::DivByZero);
            }
            l.checked_rem(r)
        }
        ArithOp::Pow => {
            let base = i64::from(l);
            let exp = i64::from(r);
            if exp < 0 {
                return Err(EvalError::Type(
                    "negative exponent not supported for integer types".to_owned(),
                ));
            }
            let exp_u32 = u32::try_from(exp).map_err(|_| EvalError::Overflow)?;
            let result = base.checked_pow(exp_u32).ok_or(EvalError::Overflow)?;
            let result_i32 = i32::try_from(result).map_err(|_| EvalError::Overflow)?;
            return Ok(Value::Int32(result_i32));
        }
    };
    result.map(Value::Int32).ok_or(EvalError::Overflow)
}

fn int64_arith(l: i64, r: i64, op: ArithOp) -> Result<Value, EvalError> {
    let result = match op {
        ArithOp::Add => l.checked_add(r),
        ArithOp::Sub => l.checked_sub(r),
        ArithOp::Mul => l.checked_mul(r),
        ArithOp::Div => {
            if r == 0 {
                return Err(EvalError::DivByZero);
            }
            l.checked_div(r)
        }
        ArithOp::Mod => {
            if r == 0 {
                return Err(EvalError::DivByZero);
            }
            l.checked_rem(r)
        }
        ArithOp::Pow => {
            if r < 0 {
                return Err(EvalError::Type(
                    "negative exponent not supported for integer types".to_owned(),
                ));
            }
            let exp_u32 = u32::try_from(r).map_err(|_| EvalError::Overflow)?;
            return l
                .checked_pow(exp_u32)
                .map(Value::Int64)
                .ok_or(EvalError::Overflow);
        }
    };
    result.map(Value::Int64).ok_or(EvalError::Overflow)
}

fn decimal_arith(
    left_value: i64,
    left_scale: i32,
    right_value: i64,
    right_scale: i32,
    op: ArithOp,
) -> Result<Value, EvalError> {
    match op {
        ArithOp::Add | ArithOp::Sub | ArithOp::Mod => {
            let common_scale = left_scale.max(right_scale);
            let left = rescale_decimal_value(left_value, left_scale, common_scale)?;
            let right = rescale_decimal_value(right_value, right_scale, common_scale)?;
            let result = match op {
                ArithOp::Add => left.checked_add(right).ok_or(EvalError::Overflow)?,
                ArithOp::Sub => left.checked_sub(right).ok_or(EvalError::Overflow)?,
                ArithOp::Mod => {
                    if right == 0 {
                        return Err(EvalError::DivByZero);
                    }
                    left % right
                }
                _ => unreachable!(),
            };
            let value = i64::try_from(result).map_err(|_| EvalError::Overflow)?;
            Ok(Value::Decimal {
                value,
                scale: common_scale,
            })
        }
        ArithOp::Mul => {
            let scale = left_scale
                .checked_add(right_scale)
                .ok_or(EvalError::Overflow)?;
            let result = i128::from(left_value)
                .checked_mul(i128::from(right_value))
                .ok_or(EvalError::Overflow)?;
            let value = i64::try_from(result).map_err(|_| EvalError::Overflow)?;
            Ok(Value::Decimal { value, scale })
        }
        ArithOp::Div => {
            if right_value == 0 {
                return Err(EvalError::DivByZero);
            }
            let result_scale = left_scale.max(right_scale).max(6);
            let exponent = right_scale
                .checked_add(result_scale)
                .and_then(|v| v.checked_sub(left_scale))
                .ok_or(EvalError::Overflow)?;
            let factor = pow10_i128(u32::try_from(exponent).map_err(|_| EvalError::Overflow)?)
                .ok_or(EvalError::Overflow)?;
            let numerator = i128::from(left_value)
                .checked_mul(factor)
                .ok_or(EvalError::Overflow)?;
            let quotient = numerator / i128::from(right_value);
            let value = i64::try_from(quotient).map_err(|_| EvalError::Overflow)?;
            Ok(Value::Decimal {
                value,
                scale: result_scale,
            })
        }
        ArithOp::Pow => Err(EvalError::Type(
            "decimal exponentiation not supported".to_owned(),
        )),
    }
}

fn float32_arith(l: f32, r: f32, op: ArithOp) -> Result<Value, EvalError> {
    let result = match op {
        ArithOp::Add => l + r,
        ArithOp::Sub => l - r,
        ArithOp::Mul => l * r,
        ArithOp::Div => {
            if r == 0.0 {
                return Err(EvalError::DivByZero);
            }
            l / r
        }
        ArithOp::Mod => {
            if r == 0.0 {
                return Err(EvalError::DivByZero);
            }
            l % r
        }
        ArithOp::Pow => l.powf(r),
    };
    Ok(Value::Float32(result))
}

fn float64_arith(l: f64, r: f64, op: ArithOp) -> Result<Value, EvalError> {
    let result = match op {
        ArithOp::Add => l + r,
        ArithOp::Sub => l - r,
        ArithOp::Mul => l * r,
        ArithOp::Div => {
            if r == 0.0 {
                return Err(EvalError::DivByZero);
            }
            l / r
        }
        ArithOp::Mod => {
            if r == 0.0 {
                return Err(EvalError::DivByZero);
            }
            l % r
        }
        ArithOp::Pow => l.powf(r),
    };
    Ok(Value::Float64(result))
}

// ---------------------------------------------------------------------------
// Bitwise helpers
// ---------------------------------------------------------------------------

/// Evaluate a bitwise binary operation on integer operands.
///
/// Only `Int16`, `Int32`, and `Int64` value pairs are accepted. The
/// `op` closure receives `i64`-promoted operands so a single closure
/// form covers all widths; the result is narrowed back to the input
/// width.
fn integer_bitwise(lv: Value, rv: Value, op: impl Fn(i64, i64) -> i64) -> Result<Value, EvalError> {
    match (lv, rv) {
        (Value::Int16(l), Value::Int16(r)) => {
            let result = op(i64::from(l), i64::from(r));
            i16::try_from(result)
                .map(Value::Int16)
                .map_err(|_| EvalError::Overflow)
        }
        (Value::Int32(l), Value::Int32(r)) => {
            let result = op(i64::from(l), i64::from(r));
            i32::try_from(result)
                .map(Value::Int32)
                .map_err(|_| EvalError::Overflow)
        }
        (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(op(l, r))),
        (l, r) => Err(EvalError::Type(format!(
            "bitwise operation requires matching integer operands, got {l:?} and {r:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Comparison helpers
// ---------------------------------------------------------------------------

/// Compare two like-typed values and apply the `test` function to the
/// resulting `Ordering`.
fn value_compare(
    lv: &Value,
    rv: &Value,
    test: impl Fn(std::cmp::Ordering) -> bool,
) -> Result<Value, EvalError> {
    let ord = compare_values(lv, rv)?;
    Ok(Value::Bool(test(ord)))
}

/// Total ordering for Value pairs of the same type.
///
/// Only types that have a natural total order are supported. Mismatched
/// types return [`EvalError::Type`].
fn compare_values(lv: &Value, rv: &Value) -> Result<std::cmp::Ordering, EvalError> {
    if matches!(
        (lv, rv),
        (Value::Decimal { .. }, _) | (_, Value::Decimal { .. })
    ) {
        let Some((left_value, left_scale)) = numeric_to_decimal(lv)? else {
            return Err(EvalError::Type(format!(
                "comparison type mismatch: {lv:?} and {rv:?}"
            )));
        };
        let Some((right_value, right_scale)) = numeric_to_decimal(rv)? else {
            return Err(EvalError::Type(format!(
                "comparison type mismatch: {lv:?} and {rv:?}"
            )));
        };
        return compare_decimal_values(left_value, left_scale, right_value, right_scale);
    }

    match (lv, rv) {
        (Value::Int16(l), Value::Int16(r)) => Ok(l.cmp(r)),
        (Value::Int32(l), Value::Int32(r)) => Ok(l.cmp(r)),
        (Value::Int64(l), Value::Int64(r)) => Ok(l.cmp(r)),
        (Value::Float32(l), Value::Float32(r)) => l
            .partial_cmp(r)
            .ok_or_else(|| EvalError::Type("comparison of NaN is undefined".to_owned())),
        (Value::Float64(l), Value::Float64(r)) => l
            .partial_cmp(r)
            .ok_or_else(|| EvalError::Type("comparison of NaN is undefined".to_owned())),
        (Value::Text(l), Value::Text(r)) => Ok(l.cmp(r)),
        (Value::Bool(l), Value::Bool(r)) => Ok(l.cmp(r)),
        (Value::Range(l), Value::Range(r)) if l.range_type == r.range_type => {
            if l == r {
                Ok(std::cmp::Ordering::Equal)
            } else {
                Ok(l.to_string().cmp(&r.to_string()))
            }
        }
        (Value::Geometry(l), Value::Geometry(r)) if l.geometry_type == r.geometry_type => {
            if l == r {
                Ok(std::cmp::Ordering::Equal)
            } else {
                Ok(l.to_string().cmp(&r.to_string()))
            }
        }
        (
            Value::Decimal {
                value: lv,
                scale: ls,
            },
            Value::Decimal {
                value: rv,
                scale: rs,
            },
        ) => compare_decimal_values(*lv, *ls, *rv, *rs),
        (Value::Date(l), Value::Date(r)) => Ok(l.cmp(r)),
        (Value::Time(l), Value::Time(r)) => Ok(l.cmp(r)),
        (Value::Timestamp(l), Value::Timestamp(r))
        | (Value::TimestampTz(l), Value::TimestampTz(r))
        | (Value::Timestamp(l), Value::TimestampTz(r))
        | (Value::TimestampTz(l), Value::Timestamp(r)) => Ok(l.cmp(r)),
        (Value::Date(l), Value::Timestamp(r)) | (Value::Date(l), Value::TimestampTz(r)) => {
            Ok(date_as_timestamp(*l)?.cmp(r))
        }
        (Value::Timestamp(l), Value::Date(r)) | (Value::TimestampTz(l), Value::Date(r)) => {
            Ok(l.cmp(&date_as_timestamp(*r)?))
        }
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
        ) => Ok((lm, ld, lus).cmp(&(rm, rd, rus))),
        (l, r) => Err(EvalError::Type(format!(
            "comparison type mismatch: {l:?} and {r:?}"
        ))),
    }
}

fn date_as_timestamp(days_since_2000_01_01: i32) -> Result<i64, EvalError> {
    i64::from(days_since_2000_01_01)
        .checked_mul(MICROS_PER_DAY)
        .ok_or_else(|| EvalError::Type("date timestamp overflow".to_owned()))
}

fn rescale_decimal_value(
    value: i64,
    current_scale: i32,
    target_scale: i32,
) -> Result<i128, EvalError> {
    let scale_delta = target_scale - current_scale;
    if scale_delta < 0 {
        return Err(EvalError::Type("decimal rescale underflow".to_owned()));
    }
    let factor = pow10_i128(u32::try_from(scale_delta).map_err(|_| EvalError::Overflow)?)
        .ok_or(EvalError::Overflow)?;
    i128::from(value)
        .checked_mul(factor)
        .ok_or(EvalError::Overflow)
}

fn compare_decimal_values(
    left_value: i64,
    left_scale: i32,
    right_value: i64,
    right_scale: i32,
) -> Result<std::cmp::Ordering, EvalError> {
    if left_scale == right_scale {
        return Ok(left_value.cmp(&right_value));
    }
    let common_scale = left_scale.max(right_scale);
    let left = rescale_decimal_for_compare(left_value, left_scale, common_scale)?;
    let right = rescale_decimal_for_compare(right_value, right_scale, common_scale)?;
    Ok(left.cmp(&right))
}

fn rescale_decimal_for_compare(
    value: i64,
    current_scale: i32,
    target_scale: i32,
) -> Result<i128, EvalError> {
    let scale_delta = target_scale - current_scale;
    if scale_delta < 0 {
        return Err(EvalError::Type(
            "decimal comparison scale underflow".to_owned(),
        ));
    }
    let factor = pow10_i128(
        u32::try_from(scale_delta)
            .map_err(|_| EvalError::Type("decimal comparison scale overflow".to_owned()))?,
    )
    .ok_or_else(|| EvalError::Type("decimal comparison scale overflow".to_owned()))?;
    i128::from(value)
        .checked_mul(factor)
        .ok_or_else(|| EvalError::Type("decimal comparison overflow".to_owned()))
}

fn pow10_i128(exp: u32) -> Option<i128> {
    (0..exp).try_fold(1_i128, |acc, _| acc.checked_mul(10))
}

// ---------------------------------------------------------------------------
// LIKE / ILIKE pattern matching
// ---------------------------------------------------------------------------

/// Match `haystack` against a SQL LIKE/ILIKE `pattern`.
///
/// `%` matches any sequence of characters (including empty). `_`
/// matches exactly one character. All other characters are literal.
///
/// This is an O(n x m) recursive-descent implementation sufficient for
/// v0.5; a compiled-regex or NFA-based path is a perf TODO.
fn like_match(haystack: &str, pattern: &str, case_insensitive: bool) -> bool {
    // Collect to chars so we handle multi-byte UTF-8 correctly.
    let h: Vec<char> = if case_insensitive {
        haystack
            .chars()
            .map(|c| c.to_lowercase().next().unwrap_or(c))
            .collect()
    } else {
        haystack.chars().collect()
    };
    let p: Vec<char> = if case_insensitive {
        pattern
            .chars()
            .map(|c| c.to_lowercase().next().unwrap_or(c))
            .collect()
    } else {
        pattern.chars().collect()
    };
    like_match_chars(&h, &p)
}

fn like_match_chars(h: &[char], p: &[char]) -> bool {
    match p.first() {
        None => h.is_empty(),
        Some(&'%') => {
            // '%' matches zero or more characters: try all possible split points.
            for skip in 0..=h.len() {
                if like_match_chars(&h[skip..], &p[1..]) {
                    return true;
                }
            }
            false
        }
        Some(&'_') => {
            // '_' matches exactly one character.
            if h.is_empty() {
                false
            } else {
                like_match_chars(&h[1..], &p[1..])
            }
        }
        Some(pc) => {
            // Literal character: must match exactly.
            if h.first() == Some(pc) {
                like_match_chars(&h[1..], &p[1..])
            } else {
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use ultrasql_core::{DataType, Value};
    use ultrasql_planner::{BinaryOp, ScalarExpr, UnaryOp};

    use super::{Eval, EvalError};

    // -----------------------------------------------------------------------
    // Helper builders
    // -----------------------------------------------------------------------

    fn col(index: usize) -> ScalarExpr {
        ScalarExpr::Column {
            name: format!("col{index}"),
            index,
            data_type: DataType::Int32,
        }
    }

    fn lit_i32(v: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int32(v),
            data_type: DataType::Int32,
        }
    }

    fn lit_i64(v: i64) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Int64(v),
            data_type: DataType::Int64,
        }
    }

    fn lit_f64(v: f64) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Float64(v),
            data_type: DataType::Float64,
        }
    }

    fn lit_decimal(value: i64, scale: i32) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Decimal { value, scale },
            data_type: DataType::Decimal {
                precision: None,
                scale: Some(scale),
            },
        }
    }

    fn lit_text(s: &str) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Text(s.to_owned()),
            data_type: DataType::Text { max_len: None },
        }
    }

    fn lit_null() -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Null,
            data_type: DataType::Null,
        }
    }

    fn lit_bool(b: bool) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Bool(b),
            data_type: DataType::Bool,
        }
    }

    fn param(index: u32) -> ScalarExpr {
        ScalarExpr::Parameter {
            index,
            data_type: DataType::Int32,
        }
    }

    fn binop(op: BinaryOp, l: ScalarExpr, r: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Binary {
            op,
            left: Box::new(l),
            right: Box::new(r),
            data_type: DataType::Int32,
        }
    }

    fn unop(op: UnaryOp, e: ScalarExpr) -> ScalarExpr {
        ScalarExpr::Unary {
            op,
            expr: Box::new(e),
            data_type: DataType::Int32,
        }
    }

    // -----------------------------------------------------------------------
    // Column reference
    // -----------------------------------------------------------------------

    #[test]
    fn column_ref_returns_correct_value() {
        let ev = Eval::new(col(1));
        let row = [Value::Int32(10), Value::Int32(20)];
        assert_eq!(ev.eval(&row).unwrap(), Value::Int32(20));
    }

    #[test]
    fn column_ref_out_of_range_returns_error() {
        let ev = Eval::new(col(5));
        let err = ev.eval(&[Value::Int32(1)]).unwrap_err();
        assert!(
            matches!(err, EvalError::ColumnIndex { index: 5, len: 1 }),
            "unexpected: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Literal
    // -----------------------------------------------------------------------

    #[test]
    fn literal_returns_its_value() {
        let ev = Eval::new(lit_i32(42));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(42));
    }

    // -----------------------------------------------------------------------
    // Parameter substitution
    // -----------------------------------------------------------------------

    #[test]
    fn parameter_substitution_returns_bound_value() {
        let ev = Eval::with_params(param(1), vec![Value::Int32(99)]);
        assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(99));
    }

    #[test]
    fn parameter_out_of_range_returns_error() {
        let ev = Eval::with_params(param(3), vec![Value::Int32(1)]);
        let err = ev.eval(&[]).unwrap_err();
        assert!(
            matches!(err, EvalError::ParameterIndex { index: 3, len: 1 }),
            "unexpected: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Arithmetic: Int32
    // -----------------------------------------------------------------------

    #[test]
    fn int32_add() {
        let ev = Eval::new(binop(BinaryOp::Add, lit_i32(3), lit_i32(4)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(7));
    }

    #[test]
    fn int32_sub() {
        let ev = Eval::new(binop(BinaryOp::Sub, lit_i32(10), lit_i32(3)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(7));
    }

    #[test]
    fn int32_mul() {
        let ev = Eval::new(binop(BinaryOp::Mul, lit_i32(3), lit_i32(4)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(12));
    }

    #[test]
    fn int32_div() {
        let ev = Eval::new(binop(BinaryOp::Div, lit_i32(10), lit_i32(3)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(3));
    }

    #[test]
    fn int32_mod() {
        let ev = Eval::new(binop(BinaryOp::Mod, lit_i32(10), lit_i32(3)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(1));
    }

    #[test]
    fn int32_div_by_zero_returns_error() {
        let ev = Eval::new(binop(BinaryOp::Div, lit_i32(5), lit_i32(0)));
        assert!(matches!(ev.eval(&[]).unwrap_err(), EvalError::DivByZero));
    }

    #[test]
    fn int32_overflow_returns_error() {
        let ev = Eval::new(binop(BinaryOp::Add, lit_i32(i32::MAX), lit_i32(1)));
        assert!(matches!(ev.eval(&[]).unwrap_err(), EvalError::Overflow));
    }

    // -----------------------------------------------------------------------
    // Arithmetic: Float64
    // -----------------------------------------------------------------------

    #[test]
    fn float64_add() {
        let ev = Eval::new(binop(BinaryOp::Add, lit_f64(1.5), lit_f64(2.5)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(4.0));
    }

    #[test]
    fn float64_div_by_zero_returns_error() {
        let ev = Eval::new(binop(BinaryOp::Div, lit_f64(5.0), lit_f64(0.0)));
        assert!(matches!(ev.eval(&[]).unwrap_err(), EvalError::DivByZero));
    }

    #[test]
    fn decimal_multiplies_integer_literal() {
        let ev = Eval::new(binop(BinaryOp::Mul, lit_i32(100), lit_decimal(1234, 2)));
        assert_eq!(
            ev.eval(&[]).unwrap(),
            Value::Decimal {
                value: 123400,
                scale: 2
            }
        );
    }

    #[test]
    fn decimal_mixed_with_float_returns_float64() {
        let ev = Eval::new(binop(BinaryOp::Mul, lit_decimal(2, 1), lit_f64(18.0)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(3.6));
    }

    #[test]
    fn decimal_divides_float_literal() {
        let ev = Eval::new(binop(BinaryOp::Div, lit_decimal(12345, 2), lit_f64(7.0)));
        let Value::Float64(v) = ev.eval(&[]).unwrap() else {
            panic!("expected Float64");
        };
        assert!((v - 17.635_714_285_714_286).abs() < f64::EPSILON);
    }

    #[test]
    fn decimal_compares_float_literal() {
        let ev = Eval::new(binop(BinaryOp::Lt, lit_decimal(123, 2), lit_f64(2.0)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    // -----------------------------------------------------------------------
    // NULL propagation through arithmetic
    // -----------------------------------------------------------------------

    #[test]
    fn null_propagates_through_add() {
        let ev = Eval::new(binop(BinaryOp::Add, lit_null(), lit_i32(5)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
    }

    #[test]
    fn null_propagates_through_mul_right() {
        let ev = Eval::new(binop(BinaryOp::Mul, lit_i32(3), lit_null()));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
    }

    // -----------------------------------------------------------------------
    // Comparison: Int32
    // -----------------------------------------------------------------------

    #[test]
    fn int32_eq_true() {
        let ev = Eval::new(binop(BinaryOp::Eq, lit_i32(7), lit_i32(7)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn int32_eq_false() {
        let ev = Eval::new(binop(BinaryOp::Eq, lit_i32(7), lit_i32(8)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
    }

    #[test]
    fn int32_lt() {
        let ev = Eval::new(binop(BinaryOp::Lt, lit_i32(3), lit_i32(7)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    // -----------------------------------------------------------------------
    // Comparison: Text
    // -----------------------------------------------------------------------

    #[test]
    fn text_eq() {
        let ev = Eval::new(binop(BinaryOp::Eq, lit_text("hello"), lit_text("hello")));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn text_lt() {
        let ev = Eval::new(binop(BinaryOp::Lt, lit_text("abc"), lit_text("abd")));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    // -----------------------------------------------------------------------
    // NULL comparison returns NULL
    // -----------------------------------------------------------------------

    #[test]
    fn null_eq_null_returns_null() {
        let ev = Eval::new(binop(BinaryOp::Eq, lit_null(), lit_null()));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
    }

    // -----------------------------------------------------------------------
    // Kleene AND/OR
    // -----------------------------------------------------------------------

    #[test]
    fn kleene_null_and_false_is_false() {
        let ev = Eval::new(binop(BinaryOp::And, lit_null(), lit_bool(false)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
    }

    #[test]
    fn kleene_false_and_null_is_false() {
        let ev = Eval::new(binop(BinaryOp::And, lit_bool(false), lit_null()));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
    }

    #[test]
    fn kleene_null_and_true_is_null() {
        let ev = Eval::new(binop(BinaryOp::And, lit_null(), lit_bool(true)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
    }

    #[test]
    fn kleene_null_or_true_is_true() {
        let ev = Eval::new(binop(BinaryOp::Or, lit_null(), lit_bool(true)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn kleene_null_or_false_is_null() {
        let ev = Eval::new(binop(BinaryOp::Or, lit_null(), lit_bool(false)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
    }

    #[test]
    fn kleene_true_and_true_is_true() {
        let ev = Eval::new(binop(BinaryOp::And, lit_bool(true), lit_bool(true)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    // -----------------------------------------------------------------------
    // Concat
    // -----------------------------------------------------------------------

    #[test]
    fn concat_two_strings() {
        let ev = Eval::new(binop(BinaryOp::Concat, lit_text("foo"), lit_text("bar")));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Text("foobar".into()));
    }

    #[test]
    fn concat_null_propagation() {
        let ev = Eval::new(binop(BinaryOp::Concat, lit_null(), lit_text("bar")));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
    }

    // -----------------------------------------------------------------------
    // LIKE / ILIKE
    // -----------------------------------------------------------------------

    #[test]
    fn like_percent_matches_any_suffix() {
        let ev = Eval::new(binop(BinaryOp::Like, lit_text("foobar"), lit_text("foo%")));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn like_no_match() {
        let ev = Eval::new(binop(BinaryOp::Like, lit_text("foobar"), lit_text("baz%")));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
    }

    #[test]
    fn like_underscore_single_char() {
        let ev = Eval::new(binop(BinaryOp::Like, lit_text("foo"), lit_text("f_o")));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn not_like_positive() {
        let ev = Eval::new(binop(
            BinaryOp::NotLike,
            lit_text("foobar"),
            lit_text("baz%"),
        ));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn ilike_case_insensitive() {
        let ev = Eval::new(binop(BinaryOp::Ilike, lit_text("FooBar"), lit_text("foo%")));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn ilike_no_match() {
        let ev = Eval::new(binop(BinaryOp::Ilike, lit_text("foobar"), lit_text("baz%")));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
    }

    // -----------------------------------------------------------------------
    // IsNull
    // -----------------------------------------------------------------------

    #[test]
    fn is_null_true_for_null() {
        let ev = Eval::new(ScalarExpr::IsNull {
            expr: Box::new(lit_null()),
            negated: false,
        });
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn is_null_false_for_non_null() {
        let ev = Eval::new(ScalarExpr::IsNull {
            expr: Box::new(lit_i32(0)),
            negated: false,
        });
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
    }

    #[test]
    fn is_not_null_true_for_non_null() {
        let ev = Eval::new(ScalarExpr::IsNull {
            expr: Box::new(lit_i32(42)),
            negated: true,
        });
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    // -----------------------------------------------------------------------
    // Unary operators
    // -----------------------------------------------------------------------

    #[test]
    fn unary_neg_i32() {
        let ev = Eval::new(unop(UnaryOp::Neg, lit_i32(5)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(-5));
    }

    #[test]
    fn unary_neg_overflow() {
        let ev = Eval::new(unop(UnaryOp::Neg, lit_i32(i32::MIN)));
        assert!(matches!(ev.eval(&[]).unwrap_err(), EvalError::Overflow));
    }

    #[test]
    fn unary_pos_is_noop() {
        let ev = Eval::new(unop(UnaryOp::Pos, lit_i32(7)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(7));
    }

    #[test]
    fn unary_not_true() {
        let ev = Eval::new(unop(UnaryOp::Not, lit_bool(true)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(false));
    }

    #[test]
    fn unary_not_null_is_null() {
        let ev = Eval::new(unop(UnaryOp::Not, lit_null()));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Null);
    }

    // -----------------------------------------------------------------------
    // Property test: integer arithmetic matches i64::checked_*
    // -----------------------------------------------------------------------

    proptest! {
        #[test]
        fn prop_int32_add_matches_checked(a: i32, b: i32) {
            let ev = Eval::new(binop(BinaryOp::Add, lit_i32(a), lit_i32(b)));
            let result = ev.eval(&[]);
            match a.checked_add(b) {
                Some(expected) => prop_assert_eq!(result.unwrap(), Value::Int32(expected)),
                None => prop_assert!(matches!(result.unwrap_err(), EvalError::Overflow)),
            }
        }

        #[test]
        fn prop_int32_sub_matches_checked(a: i32, b: i32) {
            let ev = Eval::new(binop(BinaryOp::Sub, lit_i32(a), lit_i32(b)));
            let result = ev.eval(&[]);
            match a.checked_sub(b) {
                Some(expected) => prop_assert_eq!(result.unwrap(), Value::Int32(expected)),
                None => prop_assert!(matches!(result.unwrap_err(), EvalError::Overflow)),
            }
        }

        #[test]
        fn prop_int32_mul_matches_checked(a: i32, b: i32) {
            let ev = Eval::new(binop(BinaryOp::Mul, lit_i32(a), lit_i32(b)));
            let result = ev.eval(&[]);
            match a.checked_mul(b) {
                Some(expected) => prop_assert_eq!(result.unwrap(), Value::Int32(expected)),
                None => prop_assert!(matches!(result.unwrap_err(), EvalError::Overflow)),
            }
        }

        #[test]
        fn prop_int32_div_matches_checked(a: i32, b: i32) {
            let ev = Eval::new(binop(BinaryOp::Div, lit_i32(a), lit_i32(b)));
            let result = ev.eval(&[]);
            if b == 0 {
                prop_assert!(matches!(result.unwrap_err(), EvalError::DivByZero));
            } else {
                match a.checked_div(b) {
                    Some(expected) => prop_assert_eq!(result.unwrap(), Value::Int32(expected)),
                    None => prop_assert!(matches!(result.unwrap_err(), EvalError::Overflow)),
                }
            }
        }

        #[test]
        fn prop_int64_add_matches_checked(a: i64, b: i64) {
            let ev = Eval::new(binop(BinaryOp::Add, lit_i64(a), lit_i64(b)));
            let result = ev.eval(&[]);
            match a.checked_add(b) {
                Some(expected) => prop_assert_eq!(result.unwrap(), Value::Int64(expected)),
                None => prop_assert!(matches!(result.unwrap_err(), EvalError::Overflow)),
            }
        }
    }
}
