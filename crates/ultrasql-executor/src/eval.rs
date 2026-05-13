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

fn eval_expr(expr: &ScalarExpr, row: &[Value], params: &[Value]) -> Result<Value, EvalError> {
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
    }
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
        | BinaryOp::JsonContains
        | BinaryOp::JsonContained
        | BinaryOp::JsonHasKey
        | BinaryOp::JsonHasAnyKey
        | BinaryOp::JsonHasAllKeys => Err(EvalError::Unsupported("JSON operators")),

        // AND / OR are handled above; unreachable here.
        BinaryOp::And | BinaryOp::Or => {
            unreachable!("AND/OR handled in short-circuit paths")
        }
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
    match (lv, rv) {
        (Value::Int16(l), Value::Int16(r)) => int16_arith(l, r, op),
        (Value::Int32(l), Value::Int32(r)) => int32_arith(l, r, op),
        (Value::Int64(l), Value::Int64(r)) => int64_arith(l, r, op),
        (Value::Float32(l), Value::Float32(r)) => float32_arith(l, r, op),
        (Value::Float64(l), Value::Float64(r)) => float64_arith(l, r, op),
        (l, r) => Err(EvalError::Type(format!(
            "arithmetic type mismatch: {l:?} and {r:?}"
        ))),
    }
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
        (l, r) => Err(EvalError::Type(format!(
            "comparison type mismatch: {l:?} and {r:?}"
        ))),
    }
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
