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

#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use ultrasql_core::{DataType, SparseVector, Value};
use ultrasql_planner::{BinaryOp, ScalarExpr, UnaryOp};

const MICROS_PER_DAY: i64 = 86_400_000_000;
static UUID_FALLBACK_COUNTER: AtomicU64 = AtomicU64::new(1);

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
/// - `abs(int)` — absolute value. Returns `i64`.
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
        "abs" => eval_abs(args),
        "extract" => eval_extract(args),
        "lower" => eval_text_case(args, TextCase::Lower),
        "upper" => eval_text_case(args, TextCase::Upper),
        "pg_get_userbyid" => eval_pg_get_userbyid(args),
        "substring" => eval_substring(args),
        "gen_random_uuid" => eval_gen_random_uuid(args),
        "version" => eval_zero_arg_text(args, "UltraSQL 0.0.1"),
        "current_database" => eval_zero_arg_text(args, "ultrasql"),
        "current_user" | "session_user" => eval_zero_arg_text(args, "user"),
        "pg_typeof" => eval_pg_typeof(args),
        "pg_size_pretty" => eval_pg_size_pretty(args),
        "array_length" => eval_array_length(args),
        "array_to_string" => eval_array_to_string(args),
        "string_to_array" => eval_string_to_array(args),
        "array_cat" => eval_array_cat(args),
        "l2_distance" => eval_vector_metric(args, VectorDistanceOp::L2),
        "cosine_distance" => eval_vector_metric(args, VectorDistanceOp::Cosine),
        "inner_product" | "dot_product" => eval_vector_metric(args, VectorDistanceOp::InnerProduct),
        "l1_distance" => eval_vector_metric(args, VectorDistanceOp::L1),
        "vector_norm" | "l2_norm" => eval_vector_norm(args),
        "vector_dims" => eval_vector_dims(args),
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

fn eval_zero_arg_text(args: &[Value], value: &'static str) -> Result<Value, EvalError> {
    if !args.is_empty() {
        return Err(EvalError::Type(format!(
            "zero-argument system function: expected 0 args, got {}",
            args.len()
        )));
    }
    Ok(Value::Text(value.to_owned()))
}

fn eval_pg_typeof(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_typeof: expected 1 arg, got {}",
            args.len()
        )));
    }
    Ok(Value::Text(args[0].data_type().to_string()))
}

fn eval_pg_size_pretty(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_size_pretty: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(bytes) = args[0].as_i64() else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "pg_size_pretty: integer argument required, got {:?}",
                args[0].data_type()
            )))
        };
    };
    Ok(Value::Text(format_size_pretty(bytes)))
}

fn eval_array_length(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "array_length: expected 2 args, got {}",
            args.len()
        )));
    }
    let Value::Array { elements, .. } = &args[0] else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "array_length: array argument required, got {:?}",
                args[0].data_type()
            )))
        };
    };
    let Some(dim) = args[1].as_i64() else {
        return Err(EvalError::Type(format!(
            "array_length: integer dimension required, got {:?}",
            args[1].data_type()
        )));
    };
    if dim != 1 {
        return Ok(Value::Null);
    }
    let len = i32::try_from(elements.len())
        .map_err(|_| EvalError::Type("array_length overflow".to_owned()))?;
    Ok(Value::Int32(len))
}

fn eval_array_to_string(args: &[Value]) -> Result<Value, EvalError> {
    if !(2..=3).contains(&args.len()) {
        return Err(EvalError::Type(format!(
            "array_to_string: expected 2 or 3 args, got {}",
            args.len()
        )));
    }
    let Value::Array { elements, .. } = &args[0] else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "array_to_string: array argument required, got {:?}",
                args[0].data_type()
            )))
        };
    };
    let Value::Text(delimiter) = &args[1] else {
        return Err(EvalError::Type(format!(
            "array_to_string: delimiter must be text, got {:?}",
            args[1].data_type()
        )));
    };
    let null_text = match args.get(2) {
        Some(Value::Text(text)) => Some(text.as_str()),
        Some(Value::Null) | None => None,
        Some(other) => {
            return Err(EvalError::Type(format!(
                "array_to_string: null text must be text, got {:?}",
                other.data_type()
            )));
        }
    };
    let mut parts = Vec::with_capacity(elements.len());
    for element in elements {
        if element.is_null() {
            if let Some(text) = null_text {
                parts.push(text.to_owned());
            }
        } else {
            parts.push(element.to_string());
        }
    }
    Ok(Value::Text(parts.join(delimiter)))
}

fn eval_string_to_array(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "string_to_array: expected 2 args, got {}",
            args.len()
        )));
    }
    let (Value::Text(input), Value::Text(delimiter)) = (&args[0], &args[1]) else {
        return if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "string_to_array: text arguments required, got {:?} and {:?}",
                args[0].data_type(),
                args[1].data_type()
            )))
        };
    };
    let elements = if delimiter.is_empty() {
        input
            .chars()
            .map(|ch| Value::Text(ch.to_string()))
            .collect()
    } else {
        input
            .split(delimiter)
            .map(|part| Value::Text(part.to_owned()))
            .collect()
    };
    Ok(Value::Array {
        element_type: DataType::Text { max_len: None },
        elements,
    })
}

fn eval_array_cat(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "array_cat: expected 2 args, got {}",
            args.len()
        )));
    }
    match (&args[0], &args[1]) {
        (
            Value::Array {
                element_type: left_ty,
                elements: left,
            },
            Value::Array {
                element_type: right_ty,
                elements: right,
            },
        ) if left_ty == right_ty => {
            let mut elements = Vec::with_capacity(left.len() + right.len());
            elements.extend_from_slice(left);
            elements.extend_from_slice(right);
            Ok(Value::Array {
                element_type: left_ty.clone(),
                elements,
            })
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (left, right) => Err(EvalError::Type(format!(
            "array_cat: matching arrays required, got {:?} and {:?}",
            left.data_type(),
            right.data_type()
        ))),
    }
}

fn format_size_pretty(bytes: i64) -> String {
    let sign = if bytes < 0 { "-" } else { "" };
    let mut value = bytes.unsigned_abs();
    let units = ["bytes", "kB", "MB", "GB", "TB", "PB"];
    let mut unit_idx = 0_usize;
    while value >= 1024 && unit_idx + 1 < units.len() {
        value /= 1024;
        unit_idx += 1;
    }
    format!("{sign}{value} {}", units[unit_idx])
}

fn eval_gen_random_uuid(args: &[Value]) -> Result<Value, EvalError> {
    if !args.is_empty() {
        return Err(EvalError::Type(format!(
            "gen_random_uuid: expected 0 args, got {}",
            args.len()
        )));
    }
    let mut bytes = random_uuid_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Value::Uuid(bytes))
}

fn random_uuid_bytes() -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    #[cfg(unix)]
    {
        if let Ok(mut file) = File::open("/dev/urandom")
            && file.read_exact(&mut bytes).is_ok()
        {
            return bytes;
        }
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let low = u64::try_from(now & u128::from(u64::MAX)).unwrap_or(0);
    let high = u64::try_from(now >> 64).unwrap_or(0);
    let counter = UUID_FALLBACK_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut state = low ^ high.rotate_left(17) ^ counter.rotate_left(31);
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let word = state.to_le_bytes();
        chunk.copy_from_slice(&word[..chunk.len()]);
    }
    bytes
}

fn eval_abs(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "abs: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Int16(v) => Ok(Value::Int64(i64::from(*v).abs())),
        Value::Int32(v) => Ok(Value::Int64(i64::from(*v).abs())),
        Value::Int64(v) => v.checked_abs().map(Value::Int64).ok_or(EvalError::Overflow),
        Value::Null => Ok(Value::Null),
        other => Err(EvalError::Type(format!(
            "abs: integer argument required, got {:?}",
            other.data_type()
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
            | BinaryOp::VectorL2Distance
            | BinaryOp::VectorNegativeInnerProduct
            | BinaryOp::VectorCosineDistance
            | BinaryOp::VectorL1Distance
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
            | BinaryOp::JsonHasAllKeys
            | BinaryOp::TextSearchMatch => return Ok(Value::Null),
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
        // Vector distance operators
        // ------------------------------------------------------------------
        BinaryOp::VectorL2Distance => vector_distance(&lv, &rv, VectorDistanceOp::L2),
        BinaryOp::VectorNegativeInnerProduct => {
            vector_distance(&lv, &rv, VectorDistanceOp::NegativeInnerProduct)
        }
        BinaryOp::VectorCosineDistance => vector_distance(&lv, &rv, VectorDistanceOp::Cosine),
        BinaryOp::VectorL1Distance => vector_distance(&lv, &rv, VectorDistanceOp::L1),

        // ------------------------------------------------------------------
        // Unsupported operators (regex, JSON)
        // ------------------------------------------------------------------
        BinaryOp::RegexMatch
        | BinaryOp::RegexIMatch
        | BinaryOp::RegexNotMatch
        | BinaryOp::RegexNotIMatch => Err(EvalError::Unsupported("regex operators")),

        BinaryOp::JsonGet | BinaryOp::JsonGetPath => json_get(&lv, &rv, false),
        BinaryOp::JsonGetText | BinaryOp::JsonGetPathText => json_get(&lv, &rv, true),
        BinaryOp::JsonHasKey => json_has_key(&lv, &rv).map(Value::Bool),
        BinaryOp::JsonHasAnyKey => json_has_key_set(&lv, &rv, false).map(Value::Bool),
        BinaryOp::JsonHasAllKeys => json_has_key_set(&lv, &rv, true).map(Value::Bool),

        BinaryOp::JsonContains => contains_values(&lv, &rv)
            .map(Value::Bool)
            .ok_or_else(|| EvalError::Type(format!("@> not defined for {lv:?} and {rv:?}"))),
        BinaryOp::JsonContained => contains_values(&rv, &lv)
            .map(Value::Bool)
            .ok_or_else(|| EvalError::Type(format!("<@ not defined for {lv:?} and {rv:?}"))),
        BinaryOp::Overlap => overlaps_values(&lv, &rv)
            .map(Value::Bool)
            .ok_or_else(|| EvalError::Type(format!("&& not defined for {lv:?} and {rv:?}"))),
        BinaryOp::TextSearchMatch => text_search_match(&lv, &rv).map(Value::Bool),

        // AND / OR are handled above; unreachable here.
        BinaryOp::And | BinaryOp::Or => {
            unreachable!("AND/OR handled in short-circuit paths")
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum VectorDistanceOp {
    L2,
    InnerProduct,
    NegativeInnerProduct,
    Cosine,
    L1,
}

fn eval_vector_metric(args: &[Value], op: VectorDistanceOp) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "vector metric: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args, [Value::Null, _] | [_, Value::Null]) {
        return Ok(Value::Null);
    }
    vector_distance(&args[0], &args[1], op)
}

fn eval_vector_norm(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "vector_norm: expected 1 arg, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    vector_norm(&args[0]).map(Value::Float64)
}

fn eval_vector_dims(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "vector_dims: expected 1 arg, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let dims = vector_value_dims(&args[0]).ok_or_else(|| {
        EvalError::Type(format!(
            "vector_dims requires vector-family value, got {:?}",
            args[0].data_type()
        ))
    })?;
    let dims = i32::try_from(dims)
        .map_err(|_| EvalError::Type("vector_dims: dimension count exceeds int32".to_owned()))?;
    Ok(Value::Int32(dims))
}

fn vector_distance(left: &Value, right: &Value, op: VectorDistanceOp) -> Result<Value, EvalError> {
    if vector_metric_kind(left) != vector_metric_kind(right) || vector_metric_kind(left).is_none() {
        return Err(EvalError::Type(format!(
            "vector distance requires matching vector, halfvec, or sparsevec operands, got {:?} and {:?}",
            left.data_type(),
            right.data_type()
        )));
    }
    let left_dims = vector_value_dims(left).ok_or_else(|| {
        EvalError::Type(format!(
            "vector distance requires vector-family left operand, got {:?}",
            left.data_type()
        ))
    })?;
    let right_dims = vector_value_dims(right).ok_or_else(|| {
        EvalError::Type(format!(
            "vector distance requires vector-family right operand, got {:?}",
            right.data_type()
        ))
    })?;
    if left_dims != right_dims {
        return Err(EvalError::Type(format!(
            "vector dimension mismatch: {} and {}",
            left_dims, right_dims
        )));
    }
    if left_dims == 0 {
        return Err(EvalError::Type(
            "vector distance requires non-empty vectors".to_owned(),
        ));
    }

    let result = match (left, right) {
        (Value::Vector(left), Value::Vector(right))
        | (Value::HalfVec(left), Value::HalfVec(right)) => dense_vector_distance(left, right, op)?,
        (Value::SparseVec(left), Value::SparseVec(right)) => {
            sparse_vector_distance(left, right, op)?
        }
        _ => {
            return Err(EvalError::Type(format!(
                "vector distance requires matching vector, halfvec, or sparsevec operands, got {:?} and {:?}",
                left.data_type(),
                right.data_type()
            )));
        }
    };
    Ok(Value::Float64(result))
}

fn dense_vector_distance(
    left: &[f32],
    right: &[f32],
    op: VectorDistanceOp,
) -> Result<f64, EvalError> {
    if left
        .iter()
        .chain(right.iter())
        .any(|value| !value.is_finite())
    {
        return Err(EvalError::Type(
            "vector distance requires finite elements".to_owned(),
        ));
    }
    let result = match op {
        VectorDistanceOp::L2 => left
            .iter()
            .zip(right.iter())
            .map(|(l, r)| {
                let delta = f64::from(*l) - f64::from(*r);
                delta * delta
            })
            .sum::<f64>()
            .sqrt(),
        VectorDistanceOp::InnerProduct => left
            .iter()
            .zip(right.iter())
            .map(|(l, r)| f64::from(*l) * f64::from(*r))
            .sum::<f64>(),
        VectorDistanceOp::NegativeInnerProduct => -left
            .iter()
            .zip(right.iter())
            .map(|(l, r)| f64::from(*l) * f64::from(*r))
            .sum::<f64>(),
        VectorDistanceOp::Cosine => {
            let mut dot = 0.0_f64;
            let mut left_norm = 0.0_f64;
            let mut right_norm = 0.0_f64;
            for (l, r) in left.iter().zip(right.iter()) {
                let left = f64::from(*l);
                let right = f64::from(*r);
                dot += left * right;
                left_norm += left * left;
                right_norm += right * right;
            }
            if left_norm == 0.0 || right_norm == 0.0 {
                return Err(EvalError::Type(
                    "cosine distance requires non-zero vectors".to_owned(),
                ));
            }
            1.0 - (dot / (left_norm.sqrt() * right_norm.sqrt()))
        }
        VectorDistanceOp::L1 => left
            .iter()
            .zip(right.iter())
            .map(|(l, r)| (f64::from(*l) - f64::from(*r)).abs())
            .sum::<f64>(),
    };
    Ok(result)
}

fn sparse_vector_distance(
    left: &SparseVector,
    right: &SparseVector,
    op: VectorDistanceOp,
) -> Result<f64, EvalError> {
    let result = match op {
        VectorDistanceOp::L2 => sparse_l2_squared(left, right).sqrt(),
        VectorDistanceOp::InnerProduct => sparse_dot(left, right),
        VectorDistanceOp::NegativeInnerProduct => -sparse_dot(left, right),
        VectorDistanceOp::Cosine => {
            let left_norm = sparse_norm(left);
            let right_norm = sparse_norm(right);
            if left_norm == 0.0 || right_norm == 0.0 {
                return Err(EvalError::Type(
                    "cosine distance requires non-zero vectors".to_owned(),
                ));
            }
            1.0 - (sparse_dot(left, right) / (left_norm * right_norm))
        }
        VectorDistanceOp::L1 => sparse_l1(left, right),
    };
    Ok(result)
}

fn vector_norm(value: &Value) -> Result<f64, EvalError> {
    match value {
        Value::Vector(values) | Value::HalfVec(values) => {
            if values.iter().any(|value| !value.is_finite()) {
                return Err(EvalError::Type(
                    "vector_norm requires finite elements".to_owned(),
                ));
            }
            Ok(values
                .iter()
                .map(|value| {
                    let value = f64::from(*value);
                    value * value
                })
                .sum::<f64>()
                .sqrt())
        }
        Value::SparseVec(vector) => Ok(sparse_norm(vector)),
        other => Err(EvalError::Type(format!(
            "vector_norm requires vector, halfvec, or sparsevec, got {:?}",
            other.data_type()
        ))),
    }
}

fn vector_value_dims(value: &Value) -> Option<usize> {
    match value {
        Value::Vector(values) | Value::HalfVec(values) => Some(values.len()),
        Value::SparseVec(vector) => usize::try_from(vector.dims).ok(),
        Value::BitVec { dims, .. } => usize::try_from(*dims).ok(),
        _ => None,
    }
}

fn vector_metric_kind(value: &Value) -> Option<u8> {
    match value {
        Value::Vector(_) => Some(0),
        Value::HalfVec(_) => Some(1),
        Value::SparseVec(_) => Some(2),
        Value::BitVec { .. } => None,
        _ => None,
    }
}

fn sparse_dot(left: &SparseVector, right: &SparseVector) -> f64 {
    let mut left_idx = 0_usize;
    let mut right_idx = 0_usize;
    let mut dot = 0.0_f64;
    while left_idx < left.entries.len() && right_idx < right.entries.len() {
        let (left_pos, left_value) = left.entries[left_idx];
        let (right_pos, right_value) = right.entries[right_idx];
        match left_pos.cmp(&right_pos) {
            std::cmp::Ordering::Equal => {
                dot += f64::from(left_value) * f64::from(right_value);
                left_idx += 1;
                right_idx += 1;
            }
            std::cmp::Ordering::Less => left_idx += 1,
            std::cmp::Ordering::Greater => right_idx += 1,
        }
    }
    dot
}

fn sparse_norm(vector: &SparseVector) -> f64 {
    vector
        .entries
        .iter()
        .map(|(_, value)| {
            let value = f64::from(*value);
            value * value
        })
        .sum::<f64>()
        .sqrt()
}

fn sparse_l2_squared(left: &SparseVector, right: &SparseVector) -> f64 {
    sparse_union_fold(left, right, |left, right| {
        let delta = left - right;
        delta * delta
    })
}

fn sparse_l1(left: &SparseVector, right: &SparseVector) -> f64 {
    sparse_union_fold(left, right, |left, right| (left - right).abs())
}

fn sparse_union_fold(
    left: &SparseVector,
    right: &SparseVector,
    contribution: impl Fn(f64, f64) -> f64,
) -> f64 {
    let mut left_idx = 0_usize;
    let mut right_idx = 0_usize;
    let mut acc = 0.0_f64;
    while left_idx < left.entries.len() || right_idx < right.entries.len() {
        match (left.entries.get(left_idx), right.entries.get(right_idx)) {
            (Some(&(left_pos, left_value)), Some(&(right_pos, right_value))) => {
                match left_pos.cmp(&right_pos) {
                    std::cmp::Ordering::Equal => {
                        acc += contribution(f64::from(left_value), f64::from(right_value));
                        left_idx += 1;
                        right_idx += 1;
                    }
                    std::cmp::Ordering::Less => {
                        acc += contribution(f64::from(left_value), 0.0);
                        left_idx += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        acc += contribution(0.0, f64::from(right_value));
                        right_idx += 1;
                    }
                }
            }
            (Some(&(_, left_value)), None) => {
                acc += contribution(f64::from(left_value), 0.0);
                left_idx += 1;
            }
            (None, Some(&(_, right_value))) => {
                acc += contribution(0.0, f64::from(right_value));
                right_idx += 1;
            }
            (None, None) => break,
        }
    }
    acc
}

fn text_search_match(left: &Value, right: &Value) -> Result<bool, EvalError> {
    let Value::Text(document) = left else {
        return Err(EvalError::Type(format!(
            "@@ requires text-backed TSVECTOR, got {:?}",
            left.data_type()
        )));
    };
    let Value::Text(query) = right else {
        return Err(EvalError::Type(format!(
            "@@ requires text-backed TSQUERY, got {:?}",
            right.data_type()
        )));
    };
    let doc_terms = text_search_terms(document);
    let query_terms = text_search_terms(query);
    Ok(query_terms.iter().all(|term| doc_terms.contains(term)))
}

fn text_search_terms(text: &str) -> Vec<String> {
    text.split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn overlaps_values(left: &Value, right: &Value) -> Option<bool> {
    match (left, right) {
        (Value::Range(l), Value::Range(r)) => Some(l.overlaps(r)),
        (Value::Geometry(l), Value::Geometry(r)) => Some(l.overlaps(r)),
        (
            Value::Array {
                element_type: l_ty,
                elements: l_vals,
            },
            Value::Array {
                element_type: r_ty,
                elements: r_vals,
            },
        ) if l_ty == r_ty => Some(l_vals.iter().any(|v| r_vals.contains(v))),
        (Value::Jsonb(l), Value::Jsonb(r)) => {
            let left = text_collection_values(l);
            let right = text_collection_values(r);
            Some(left.iter().any(|v| right.contains(v)))
        }
        (Value::Text(l), Value::Text(r)) => {
            let left = text_collection_values(l);
            let right = text_collection_values(r);
            Some(left.iter().any(|v| right.contains(v)))
        }
        _ => None,
    }
}

fn contains_values(left: &Value, right: &Value) -> Option<bool> {
    match (left, right) {
        (Value::Range(l), Value::Range(r)) => Some(l.contains_range(r)),
        (Value::Geometry(l), Value::Geometry(r)) => Some(l.contains_geometry(r)),
        (
            Value::Array {
                element_type: l_ty,
                elements: l_vals,
            },
            Value::Array {
                element_type: r_ty,
                elements: r_vals,
            },
        ) if l_ty == r_ty => Some(r_vals.iter().all(|v| l_vals.contains(v))),
        (Value::Jsonb(l), Value::Jsonb(r)) => Some(text_contains(l, r)),
        (Value::Text(l), Value::Text(r)) => Some(text_contains(l, r)),
        _ => None,
    }
}

fn json_get(left: &Value, right: &Value, as_text: bool) -> Result<Value, EvalError> {
    let json = json_text(left).ok_or_else(|| {
        EvalError::Type(format!(
            "JSON access requires JSONB, got {:?}",
            left.data_type()
        ))
    })?;
    let key = json_key_text(right)?;
    let Some(value) = json_object_value(json, &key) else {
        return Ok(Value::Null);
    };
    if as_text {
        Ok(Value::Text(unquote_json_scalar(value).to_owned()))
    } else {
        Ok(Value::Jsonb(value.to_owned()))
    }
}

fn json_has_key(left: &Value, right: &Value) -> Result<bool, EvalError> {
    let json = json_text(left)
        .ok_or_else(|| EvalError::Type(format!("? requires JSONB, got {:?}", left.data_type())))?;
    let key = json_key_text(right)?;
    Ok(json_object_value(json, &key).is_some())
}

fn json_text(value: &Value) -> Option<&str> {
    match value {
        Value::Jsonb(text) | Value::Text(text) => Some(text.as_str()),
        _ => None,
    }
}

fn json_has_key_set(left: &Value, right: &Value, require_all: bool) -> Result<bool, EvalError> {
    let keys = match right {
        Value::Text(text) => text_collection_values(text),
        Value::Array { elements, .. } => elements
            .iter()
            .map(|value| match value {
                Value::Text(text) => Ok(text.clone()),
                other => Err(EvalError::Type(format!(
                    "?|/?& requires text array keys, got {:?}",
                    other.data_type()
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?,
        Value::Null => return Ok(false),
        other => {
            return Err(EvalError::Type(format!(
                "?|/?& requires text array keys, got {:?}",
                other.data_type()
            )));
        }
    };
    if require_all {
        for key in keys {
            if !json_has_key(left, &Value::Text(key))? {
                return Ok(false);
            }
        }
        Ok(true)
    } else {
        for key in keys {
            if json_has_key(left, &Value::Text(key))? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn json_key_text(value: &Value) -> Result<String, EvalError> {
    match value {
        Value::Text(s) => Ok(s.clone()),
        Value::Int16(v) => Ok(v.to_string()),
        Value::Int32(v) => Ok(v.to_string()),
        Value::Int64(v) => Ok(v.to_string()),
        Value::Null => Err(EvalError::Type("JSON key cannot be NULL".to_owned())),
        other => Err(EvalError::Type(format!(
            "JSON key must be text or integer, got {:?}",
            other.data_type()
        ))),
    }
}

fn text_contains(left: &str, right: &str) -> bool {
    if looks_like_json_object(left) && looks_like_json_object(right) {
        return json_object_pairs(right)
            .iter()
            .all(|(key, value)| json_object_value(left, key).is_some_and(|v| v == *value));
    }
    let left_values = text_collection_values(left);
    let right_values = text_collection_values(right);
    right_values.iter().all(|v| left_values.contains(v))
}

fn text_collection_values(text: &str) -> Vec<String> {
    let trimmed = text.trim();
    let inner = if (trimmed.starts_with('{') && trimmed.ends_with('}'))
        || (trimmed.starts_with('[') && trimmed.ends_with(']'))
    {
        &trimmed[1..trimmed.len().saturating_sub(1)]
    } else {
        trimmed
    };
    split_loose_list(inner)
        .into_iter()
        .map(|item| unquote_json_scalar(item.trim()).to_owned())
        .filter(|item| !item.is_empty())
        .collect()
}

fn looks_like_json_object(text: &str) -> bool {
    let trimmed = text.trim();
    trimmed.starts_with('{') && trimmed.ends_with('}') && trimmed.contains(':')
}

fn json_object_pairs(text: &str) -> Vec<(String, &str)> {
    let trimmed = text.trim();
    if !looks_like_json_object(trimmed) {
        return Vec::new();
    }
    let inner = &trimmed[1..trimmed.len().saturating_sub(1)];
    split_loose_list(inner)
        .into_iter()
        .filter_map(|pair| {
            let (key, value) = pair.split_once(':')?;
            Some((unquote_json_scalar(key.trim()).to_owned(), value.trim()))
        })
        .collect()
}

fn json_object_value<'a>(text: &'a str, wanted: &str) -> Option<&'a str> {
    json_object_pairs(text)
        .into_iter()
        .find_map(|(key, value)| if key == wanted { Some(value) } else { None })
}

fn split_loose_list(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut in_string = false;
    let mut escape = false;
    for (idx, ch) in text.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_string => escape = true,
            '"' => in_string = !in_string,
            ',' if !in_string => {
                out.push(&text[start..idx]);
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    out.push(&text[start..]);
    out
}

fn unquote_json_scalar(text: &str) -> &str {
    let trimmed = text.trim();
    trimmed
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(trimmed)
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

    fn lit_jsonb(s: &str) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Jsonb(s.to_owned()),
            data_type: DataType::Jsonb,
        }
    }

    fn lit_text_array(items: &[&str]) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: items
                    .iter()
                    .map(|item| Value::Text((*item).to_owned()))
                    .collect(),
            },
            data_type: DataType::Array(Box::new(DataType::Text { max_len: None })),
        }
    }

    fn call(name: &str, args: Vec<ScalarExpr>, data_type: DataType) -> ScalarExpr {
        ScalarExpr::FunctionCall {
            name: name.to_owned(),
            args,
            data_type,
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

    fn lit_vector(values: Vec<f32>) -> ScalarExpr {
        let dims = u32::try_from(values.len()).expect("test vector length fits u32");
        ScalarExpr::Literal {
            value: Value::Vector(values),
            data_type: DataType::Vector { dims: Some(dims) },
        }
    }

    fn lit_halfvec(values: Vec<f32>) -> ScalarExpr {
        let dims = u32::try_from(values.len()).expect("test halfvec length fits u32");
        ScalarExpr::Literal {
            value: Value::HalfVec(values),
            data_type: DataType::HalfVec { dims: Some(dims) },
        }
    }

    fn lit_sparsevec(dims: u32, entries: Vec<(u32, f32)>) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::SparseVec(ultrasql_core::SparseVector::new(dims, entries).unwrap()),
            data_type: DataType::SparseVec { dims: Some(dims) },
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

    #[test]
    fn jsonb_contains_and_key_ops_evaluate() {
        let doc = lit_text(r#"{"a":1,"b":"two"}"#);
        let contains = Eval::new(binop(
            BinaryOp::JsonContains,
            doc.clone(),
            lit_text(r#"{"a":1}"#),
        ));
        assert_eq!(contains.eval(&[]).unwrap(), Value::Bool(true));

        let has_key = Eval::new(binop(BinaryOp::JsonHasKey, doc.clone(), lit_text("b")));
        assert_eq!(has_key.eval(&[]).unwrap(), Value::Bool(true));

        let has_all = Eval::new(binop(
            BinaryOp::JsonHasAllKeys,
            doc,
            lit_text(r#"["a","b"]"#),
        ));
        assert_eq!(has_all.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn native_jsonb_access_contains_and_key_ops_evaluate() {
        let doc = lit_jsonb(r#"{"a":1,"b":"x"}"#);
        let get_text = Eval::new(binop(BinaryOp::JsonGetText, doc.clone(), lit_text("b")));
        assert_eq!(get_text.eval(&[]).unwrap(), Value::Text("x".into()));

        let contains = Eval::new(binop(
            BinaryOp::JsonContains,
            doc.clone(),
            lit_jsonb(r#"{"a":1}"#),
        ));
        assert_eq!(contains.eval(&[]).unwrap(), Value::Bool(true));

        let has_key = Eval::new(binop(BinaryOp::JsonHasKey, doc, lit_text("b")));
        assert_eq!(has_key.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn array_contains_and_overlap_evaluate() {
        let contains = Eval::new(binop(
            BinaryOp::JsonContains,
            lit_text("{red,green,blue}"),
            lit_text("{red,blue}"),
        ));
        assert_eq!(contains.eval(&[]).unwrap(), Value::Bool(true));

        let overlaps = Eval::new(binop(
            BinaryOp::Overlap,
            lit_text("{red,green}"),
            lit_text("{yellow,green}"),
        ));
        assert_eq!(overlaps.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn native_array_contains_and_overlap_evaluate() {
        let contains = Eval::new(binop(
            BinaryOp::JsonContains,
            lit_text_array(&["red", "green", "blue"]),
            lit_text_array(&["red", "blue"]),
        ));
        assert_eq!(contains.eval(&[]).unwrap(), Value::Bool(true));

        let overlaps = Eval::new(binop(
            BinaryOp::Overlap,
            lit_text_array(&["red", "green"]),
            lit_text_array(&["yellow", "green"]),
        ));
        assert_eq!(overlaps.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn array_scalar_functions_evaluate() {
        let string_to_array = call(
            "string_to_array",
            vec![lit_text("red,green,blue"), lit_text(",")],
            DataType::Array(Box::new(DataType::Text { max_len: None })),
        );
        let parsed = Eval::new(string_to_array.clone()).eval(&[]).unwrap();
        assert_eq!(
            parsed,
            Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: vec![
                    Value::Text("red".into()),
                    Value::Text("green".into()),
                    Value::Text("blue".into())
                ]
            }
        );

        let len = Eval::new(call(
            "array_length",
            vec![string_to_array.clone(), lit_i32(1)],
            DataType::Int32,
        ));
        assert_eq!(len.eval(&[]).unwrap(), Value::Int32(3));

        let joined = Eval::new(call(
            "array_to_string",
            vec![string_to_array, lit_text("|")],
            DataType::Text { max_len: None },
        ));
        assert_eq!(
            joined.eval(&[]).unwrap(),
            Value::Text("red|green|blue".into())
        );

        let cat = Eval::new(call(
            "array_cat",
            vec![lit_text_array(&["red"]), lit_text_array(&["green"])],
            DataType::Array(Box::new(DataType::Text { max_len: None })),
        ));
        assert_eq!(
            cat.eval(&[]).unwrap(),
            Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: vec![Value::Text("red".into()), Value::Text("green".into())]
            }
        );
    }

    #[test]
    fn tsvector_match_evaluates() {
        let ev = Eval::new(binop(
            BinaryOp::TextSearchMatch,
            lit_text("quick brown fox"),
            lit_text("quick & fox"),
        ));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
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

    #[test]
    fn vector_l2_distance_evaluates() {
        let ev = Eval::new(binop(
            BinaryOp::VectorL2Distance,
            lit_vector(vec![1.0, 2.0, 3.0]),
            lit_vector(vec![1.0, 2.0, 4.0]),
        ));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(1.0));
    }

    #[test]
    fn vector_negative_inner_product_evaluates() {
        let ev = Eval::new(binop(
            BinaryOp::VectorNegativeInnerProduct,
            lit_vector(vec![1.0, 2.0, 3.0]),
            lit_vector(vec![4.0, 5.0, 6.0]),
        ));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(-32.0));
    }

    #[test]
    fn vector_inner_product_functions_evaluate_positive_dot() {
        for name in ["inner_product", "dot_product"] {
            let ev = Eval::new(call(
                name,
                vec![
                    lit_vector(vec![1.0, 2.0, 3.0]),
                    lit_vector(vec![4.0, 5.0, 6.0]),
                ],
                DataType::Float64,
            ));
            assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(32.0), "{name}");
        }
    }

    #[test]
    fn vector_distance_functions_evaluate() {
        let l2 = Eval::new(call(
            "l2_distance",
            vec![
                lit_vector(vec![1.0, 2.0, 3.0]),
                lit_vector(vec![1.0, 2.0, 4.0]),
            ],
            DataType::Float64,
        ));
        assert_eq!(l2.eval(&[]).unwrap(), Value::Float64(1.0));

        let cosine = Eval::new(call(
            "cosine_distance",
            vec![lit_vector(vec![1.0, 0.0]), lit_vector(vec![0.0, 1.0])],
            DataType::Float64,
        ));
        assert_eq!(cosine.eval(&[]).unwrap(), Value::Float64(1.0));

        let l1 = Eval::new(call(
            "l1_distance",
            vec![
                lit_vector(vec![1.0, 2.0, 3.0]),
                lit_vector(vec![3.0, 2.0, -1.0]),
            ],
            DataType::Float64,
        ));
        assert_eq!(l1.eval(&[]).unwrap(), Value::Float64(6.0));
    }

    #[test]
    fn vector_norm_function_returns_euclidean_norm() {
        for (name, expr) in [
            ("vector_norm", lit_vector(vec![3.0, 4.0])),
            ("l2_norm", lit_halfvec(vec![3.0, 4.0])),
            ("sparse-l2_norm", lit_sparsevec(4, vec![(1, 3.0), (4, 4.0)])),
        ] {
            let func_name = if name == "sparse-l2_norm" {
                "l2_norm"
            } else {
                name
            };
            let ev = Eval::new(call(func_name, vec![expr], DataType::Float64));
            assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(5.0), "{name}");
        }
    }

    #[test]
    fn vector_dims_function_returns_dimension() {
        for expr in [
            lit_vector(vec![1.0, 2.0, 3.0]),
            lit_halfvec(vec![1.0, 2.0, 3.0]),
            lit_sparsevec(3, vec![(1, 1.0), (3, 3.0)]),
        ] {
            let ev = Eval::new(call("vector_dims", vec![expr], DataType::Int32));
            assert_eq!(ev.eval(&[]).unwrap(), Value::Int32(3));
        }
    }

    #[test]
    fn halfvec_distance_operators_evaluate() {
        let l2 = Eval::new(binop(
            BinaryOp::VectorL2Distance,
            lit_halfvec(vec![1.0, 2.0, 3.0]),
            lit_halfvec(vec![1.0, 2.0, 4.0]),
        ));
        assert_eq!(l2.eval(&[]).unwrap(), Value::Float64(1.0));

        let inner = Eval::new(binop(
            BinaryOp::VectorNegativeInnerProduct,
            lit_halfvec(vec![1.0, 2.0, 3.0]),
            lit_halfvec(vec![4.0, 5.0, 6.0]),
        ));
        assert_eq!(inner.eval(&[]).unwrap(), Value::Float64(-32.0));
    }

    #[test]
    fn sparsevec_distance_operators_evaluate_without_dense_expansion() {
        let left = lit_sparsevec(5, vec![(1, 1.0), (3, 2.0), (5, -1.0)]);
        let right = lit_sparsevec(5, vec![(1, 2.0), (4, 3.0), (5, 1.0)]);

        let l2 = Eval::new(binop(
            BinaryOp::VectorL2Distance,
            left.clone(),
            right.clone(),
        ));
        assert_eq!(l2.eval(&[]).unwrap(), Value::Float64(18.0_f64.sqrt()));

        let inner = Eval::new(binop(
            BinaryOp::VectorNegativeInnerProduct,
            left.clone(),
            right.clone(),
        ));
        assert_eq!(inner.eval(&[]).unwrap(), Value::Float64(-1.0));

        let l1 = Eval::new(binop(BinaryOp::VectorL1Distance, left, right));
        assert_eq!(l1.eval(&[]).unwrap(), Value::Float64(8.0));
    }

    #[test]
    fn vector_cosine_distance_evaluates() {
        let ev = Eval::new(binop(
            BinaryOp::VectorCosineDistance,
            lit_vector(vec![1.0, 0.0]),
            lit_vector(vec![0.0, 1.0]),
        ));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(1.0));
    }

    #[test]
    fn vector_l1_distance_evaluates() {
        let ev = Eval::new(binop(
            BinaryOp::VectorL1Distance,
            lit_vector(vec![1.0, 2.0, 3.0]),
            lit_vector(vec![3.0, 2.0, -1.0]),
        ));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Float64(6.0));
    }

    #[test]
    fn vector_distance_rejects_runtime_dimension_mismatch() {
        let ev = Eval::new(binop(
            BinaryOp::VectorL2Distance,
            lit_vector(vec![1.0, 2.0, 3.0]),
            lit_vector(vec![1.0, 2.0]),
        ));
        let err = ev.eval(&[]).unwrap_err();
        assert!(matches!(err, EvalError::Type(_)), "got {err:?}");
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
