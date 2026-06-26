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

use std::fmt::Write as _;
#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use num_traits::ToPrimitive;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use ultrasql_core::{
    DataType, Oid, SparseVector, Value, bpchar_semantic_text, parse_date_text, parse_decimal_text,
    parse_money_text, parse_time_text, parse_timestamp_text, parse_timestamptz_text,
    parse_timetz_text, timestamp_micros_at_timezone, timestamptz_display_in_timezone,
    timetz_at_timezone, timetz_utc_micros, xml_content_is_well_formed, xml_document_is_well_formed,
    xml_xpath_element_fragments_with_namespaces,
};
use ultrasql_planner::{BinaryOp, ScalarExpr, UnaryOp, catalog::builtin_type_oid};

use crate::json_path::{parse_json_path, select_json_path_with_vars};

const MICROS_PER_DAY: i64 = 86_400_000_000;
const UNIX_TO_ENGINE_EPOCH_DAYS: i64 = 10_957;
const UNIX_TO_ENGINE_EPOCH_MICROS: i64 = UNIX_TO_ENGINE_EPOCH_DAYS * MICROS_PER_DAY;
const MAX_EVAL_GENERATED_TEXT_CHARS: usize = 16 * 1024 * 1024;
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

    /// A numeric value exceeds declared precision.
    #[error("{0}")]
    NumericFieldOverflow(String),

    /// A textual value could not be parsed as the requested SQL type.
    #[error("{0}")]
    InvalidTextRepresentation(String),

    /// An XML document value is not well formed.
    #[error("{0}")]
    InvalidXmlDocument(String),

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
            let zero_idx = index
                .checked_sub(1)
                .and_then(|idx| usize::try_from(idx).ok())
                .ok_or(EvalError::ParameterIndex {
                    index: *index,
                    len: params.len(),
                })?;
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

        ScalarExpr::FunctionCall {
            name,
            args,
            data_type,
        } => {
            let evaluated: Result<Vec<Value>, EvalError> =
                args.iter().map(|a| eval_expr(a, row, params)).collect();
            let vals = evaluated?;
            eval_function_call(name, &vals, data_type)
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
/// - `pg_get_userbyid(oid)` — catalog helper for psql meta SQL.
/// - `substring(text, from[, for])` — 1-based string slicing.
///
/// Unknown function names return [`EvalError::Unsupported`] so the
/// binder upgrade lands ahead of executor coverage without crashing.
fn eval_function_call(
    name: &str,
    args: &[Value],
    return_type: &DataType,
) -> Result<Value, EvalError> {
    match name {
        "abs" => eval_abs(args),
        "extract" => eval_extract(args),
        "now" | "current_timestamp" => eval_now(args, return_type),
        "current_date" => eval_current_date(args),
        "age" => eval_age(args),
        "timezone" => eval_timezone(args),
        "date_trunc" => eval_date_trunc(args),
        "to_timestamp" => eval_to_timestamp(args),
        "make_date" => eval_make_date(args),
        "date_bin" => eval_date_bin(args),
        "ceil" => eval_round_family(args, "ceil", RoundMode::Ceil),
        "floor" => eval_round_family(args, "floor", RoundMode::Floor),
        "round" => eval_round_family(args, "round", RoundMode::Round),
        "trunc" => eval_round_family(args, "trunc", RoundMode::Trunc),
        "mod" => eval_mod(args),
        "power" => eval_numeric_binary(args, "power", f64::powf),
        "sqrt" => eval_numeric_unary(args, "sqrt", f64::sqrt),
        "exp" => eval_numeric_unary(args, "exp", f64::exp),
        "ln" => eval_numeric_unary(args, "ln", f64::ln),
        "log" => eval_numeric_unary(args, "log", f64::log10),
        "random" => eval_random(args),
        "sin" => eval_numeric_unary(args, "sin", f64::sin),
        "cos" => eval_numeric_unary(args, "cos", f64::cos),
        "tan" => eval_numeric_unary(args, "tan", f64::tan),
        "asin" => eval_numeric_unary(args, "asin", f64::asin),
        "acos" => eval_numeric_unary(args, "acos", f64::acos),
        "atan" => eval_numeric_unary(args, "atan", f64::atan),
        "pi" => eval_pi(args),
        "length" => eval_length(args),
        "bit_length" => eval_bit_length(args),
        "octet_length" => eval_octet_length(args),
        "bit_count" => eval_bit_count(args),
        "get_bit" => eval_get_bit(args),
        "set_bit" => eval_set_bit(args),
        "lower" => eval_text_case(args, TextCase::Lower),
        "upper" => eval_text_case(args, TextCase::Upper),
        "pg_get_userbyid" => eval_pg_get_userbyid(args),
        "trim" => eval_trim(args),
        "lpad" => eval_pad(args, PadSide::Left),
        "rpad" => eval_pad(args, PadSide::Right),
        "left" => eval_left(args),
        "right" => eval_right(args),
        "substr" | "substring" => eval_substring(args),
        "position" => eval_position(args),
        "replace" => eval_replace(args),
        "split_part" => eval_split_part(args),
        "concat" => eval_concat(args),
        "concat_ws" => eval_concat_ws(args),
        "repeat" => eval_repeat(args),
        "reverse" => eval_reverse(args),
        "md5" => eval_md5(args),
        "sha256" => eval_sha256(args),
        "quote_ident" => eval_quote_ident(args),
        "quote_literal" => eval_quote_literal(args),
        "format" => eval_format(args),
        "regexp_replace" => eval_regexp_replace(args),
        "to_tsvector" => eval_to_tsvector(args),
        "to_tsquery" | "plainto_tsquery" | "websearch_to_tsquery" | "phraseto_tsquery" => {
            eval_plain_tsquery(name, args)
        }
        "ts_rank" | "ts_rank_cd" => eval_ts_rank(name, args),
        "ts_headline" => eval_ts_headline(args),
        "numnode" => eval_numnode(args),
        "querytree" => eval_querytree(args),
        "ifnull" | "nvl" => eval_ifnull(args),
        "nullif" => eval_nullif(args),
        "is_distinct_from" => eval_is_distinct_from(args, false),
        "is_not_distinct_from" => eval_is_distinct_from(args, true),
        "is_true" => eval_is_boolean(args, BooleanTest::True),
        "is_not_true" => eval_is_boolean(args, BooleanTest::NotTrue),
        "is_false" => eval_is_boolean(args, BooleanTest::False),
        "is_not_false" => eval_is_boolean(args, BooleanTest::NotFalse),
        "least" => eval_extremum(args, "least", ExtremumKind::Least, NullPolicy::Ignore),
        "greatest" => eval_extremum(args, "greatest", ExtremumKind::Greatest, NullPolicy::Ignore),
        "min" => eval_extremum(args, "min", ExtremumKind::Least, NullPolicy::Propagate),
        "max" => eval_extremum(args, "max", ExtremumKind::Greatest, NullPolicy::Propagate),
        "row" => eval_row_constructor(args, return_type),
        "row_to_json" => eval_row_to_json(args),
        "json_build_object" => eval_json_build_object(args),
        "jsonb_set" => eval_jsonb_set(args),
        "jsonb_path_exists" => eval_jsonb_path_exists(args),
        "xmlparse" => eval_xmlparse(args),
        "xmlserialize" => eval_xmlserialize(args),
        "xml_is_well_formed" | "xml_is_well_formed_content" => {
            eval_xml_is_well_formed(args, XmlWellFormedMode::Content)
        }
        "xml_is_well_formed_document" => eval_xml_is_well_formed(args, XmlWellFormedMode::Document),
        "xpath_exists" => eval_xpath_exists(args),
        "xpath" => eval_xpath(args),
        "host" => eval_network_host(args),
        "family" => eval_network_family(args),
        "masklen" => eval_network_masklen(args),
        "pg_advisory_lock"
        | "pg_try_advisory_lock"
        | "pg_try_advisory_xact_lock"
        | "pg_advisory_unlock"
        | "pg_advisory_unlock_all" => Err(EvalError::Unsupported(
            "advisory lock functions require session context",
        )),
        "gen_random_uuid" => eval_gen_random_uuid(args),
        "version" => eval_zero_arg_text(
            args,
            concat!("PostgreSQL 14.0 (UltraSQL ", env!("CARGO_PKG_VERSION"), ")"),
        ),
        "current_catalog" => eval_zero_arg_text(args, "ultrasql"),
        "current_database" => eval_zero_arg_text(args, "ultrasql"),
        "current_schema" => eval_zero_arg_text(args, "public"),
        "current_schemas" => eval_current_schemas(args),
        "current_user" | "session_user" => eval_zero_arg_text(args, "user"),
        "pg_typeof" => eval_pg_typeof(args),
        "to_regtype" => eval_to_regtype(args),
        "pg_table_is_visible" => eval_pg_table_is_visible(args),
        "pg_is_other_temp_schema" => eval_pg_is_other_temp_schema(args),
        "pg_function_is_visible" => eval_pg_function_is_visible(args),
        "pg_relation_is_publishable" => eval_pg_relation_is_publishable(args),
        "set_config" => eval_set_config(args),
        "format_type" => eval_format_type(args),
        "pg_get_expr" => eval_pg_get_expr(args),
        "pg_get_indexdef" => eval_pg_get_indexdef(args),
        "pg_get_constraintdef" => eval_pg_get_constraintdef(args),
        "pg_get_statisticsobjdef_columns" => eval_pg_get_statisticsobjdef_columns(args),
        "pg_get_function_result" => eval_pg_get_function_result(args),
        "pg_get_function_arguments" => eval_pg_get_function_arguments(args),
        "pg_encoding_to_char" => eval_pg_encoding_to_char(args),
        "obj_description" | "shobj_description" => eval_obj_description(args),
        "col_description" => eval_col_description(args),
        "pg_get_serial_sequence" => eval_pg_get_serial_sequence(args),
        "pg_size_pretty" => eval_pg_size_pretty(args),
        "array_length" => eval_array_length(args),
        "array_ndims" => eval_array_ndims(args),
        "array_lower" => eval_array_bound(args, ArrayBound::Lower),
        "array_upper" => eval_array_bound(args, ArrayBound::Upper),
        "array_dims" => eval_array_dims(args),
        "cardinality" => eval_array_cardinality(args),
        "array_position" => eval_array_position(args),
        "array_to_string" => eval_array_to_string(args),
        "string_to_array" => eval_string_to_array(args),
        "array_cat" => eval_array_cat(args),
        "array_append" => eval_array_append(args),
        "array_prepend" => eval_array_prepend(args),
        "array_remove" => eval_array_remove(args),
        "array_replace" => eval_array_replace(args),
        "array_positions" => eval_array_positions(args),
        "trim_array" => eval_trim_array(args),
        "__ultrasql_array_subscript" => eval_array_subscript(args),
        "__ultrasql_array_slice" => eval_array_slice(args),
        "__ultrasql_eq_any_array" => eval_eq_any_array(args),
        "__ultrasql_cast_int2" => eval_cast_int16(args),
        "__ultrasql_cast_int4" => eval_cast_int32(args),
        "__ultrasql_cast_int8" => eval_cast_int64(args),
        "__ultrasql_cast_float4" => eval_cast_float32(args),
        "__ultrasql_cast_float8" => eval_cast_float64(args),
        "__ultrasql_cast_bool" => eval_cast_bool(args),
        "__ultrasql_cast_date" => eval_cast_date(args),
        "__ultrasql_cast_time" => eval_cast_time(args),
        "__ultrasql_cast_timestamp" => eval_cast_timestamp(args),
        "__ultrasql_cast_timestamptz" => eval_cast_timestamptz(args),
        "__ultrasql_cast_timetz" => eval_cast_timetz(args),
        "__ultrasql_cast_uuid" => eval_cast_uuid(args),
        "__ultrasql_cast_json" => eval_cast_json(args),
        "__ultrasql_cast_jsonb" => eval_cast_jsonb(args),
        "__ultrasql_cast_xml" => eval_cast_xml(args),
        "__ultrasql_cast_oid" => eval_cast_oid(args),
        "__ultrasql_cast_regclass" => eval_cast_regclass(args),
        "__ultrasql_cast_regtype" => eval_cast_regtype(args),
        "__ultrasql_cast_text" => eval_cast_text(args),
        "__ultrasql_cast_money" => eval_cast_money(args),
        "__ultrasql_cast_numeric" => eval_cast_numeric(args),
        "l2_distance" => eval_vector_metric(args, VectorDistanceOp::L2),
        "cosine_distance" => eval_vector_metric(args, VectorDistanceOp::Cosine),
        "inner_product" | "dot_product" => eval_vector_metric(args, VectorDistanceOp::InnerProduct),
        "l1_distance" => eval_vector_metric(args, VectorDistanceOp::L1),
        "hybrid_search" => Err(EvalError::Unsupported(
            "hybrid_search requires ORDER BY hybrid_search(...) DESC LIMIT k",
        )),
        "vector_norm" | "l2_norm" => eval_vector_norm(args),
        "vector_dims" => eval_vector_dims(args),
        "coalesce" => Ok(args
            .iter()
            .find(|v| !matches!(v, Value::Null))
            .cloned()
            .unwrap_or(Value::Null)),
        "case_searched" => eval_case_searched(args),
        "case_simple" => eval_case_simple(args),
        _other => Err(EvalError::Unsupported("function not implemented")),
    }
}

// ---------------------------------------------------------------------------
// Submodules (pure code motion from the original single-file `eval.rs`).
// ---------------------------------------------------------------------------
mod arithmetic;
mod bitwise;
mod compare;
pub mod eval_clock;
mod functions_array;
mod functions_array_ops;
mod functions_cast;
mod functions_datetime;
mod functions_json_xml;
mod functions_misc;
mod functions_pg;
mod functions_string;
mod functions_text;
mod like;
mod operators;
mod regex_cache;
mod textsearch;
mod vector;

use arithmetic::*;
use bitwise::*;
use compare::*;
use functions_array::*;
use functions_array_ops::*;
use functions_cast::*;
use functions_datetime::*;
use functions_json_xml::*;
use functions_misc::*;
use functions_pg::*;
use functions_string::*;
use functions_text::*;
use like::*;
use operators::*;
use textsearch::*;
use vector::*;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
