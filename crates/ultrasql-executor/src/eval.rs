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
    DataType, Oid, SparseVector, Value, bpchar_semantic_text, parse_money_text, timetz_utc_micros,
    xml_content_is_well_formed, xml_document_is_well_formed,
    xml_xpath_element_fragments_with_namespaces,
};
use ultrasql_planner::{BinaryOp, ScalarExpr, UnaryOp, catalog::builtin_type_oid};

use crate::json_path::{parse_json_path, select_json_path_with_vars};

const MICROS_PER_DAY: i64 = 86_400_000_000;
const UNIX_TO_ENGINE_EPOCH_DAYS: i64 = 10_957;
const UNIX_TO_ENGINE_EPOCH_MICROS: i64 = UNIX_TO_ENGINE_EPOCH_DAYS * MICROS_PER_DAY;
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
        "date_trunc" => eval_date_trunc(args),
        "to_timestamp" => eval_to_timestamp(args),
        "make_date" => eval_make_date(args),
        "date_bin" => eval_date_bin(args),
        "ceil" => eval_numeric_unary(args, "ceil", f64::ceil),
        "floor" => eval_numeric_unary(args, "floor", f64::floor),
        "round" => eval_numeric_unary(args, "round", f64::round),
        "trunc" => eval_numeric_unary(args, "trunc", f64::trunc),
        "mod" => eval_numeric_binary(args, "mod", |left, right| left % right),
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

fn eval_zero_arg_text(args: &[Value], value: &'static str) -> Result<Value, EvalError> {
    if !args.is_empty() {
        return Err(EvalError::Type(format!(
            "zero-argument system function: expected 0 args, got {}",
            args.len()
        )));
    }
    Ok(Value::Text(value.to_owned()))
}

#[derive(Clone, Copy)]
enum ExtremumKind {
    Least,
    Greatest,
}

#[derive(Clone, Copy)]
enum NullPolicy {
    Ignore,
    Propagate,
}

fn eval_ifnull(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "ifnull: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        Ok(args[1].clone())
    } else {
        Ok(args[0].clone())
    }
}

fn eval_nullif(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "nullif: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(args[0].clone());
    }
    if compare_values(&args[0], &args[1])? == std::cmp::Ordering::Equal {
        Ok(Value::Null)
    } else {
        Ok(args[0].clone())
    }
}

fn eval_extremum(
    args: &[Value],
    func_name: &str,
    kind: ExtremumKind,
    null_policy: NullPolicy,
) -> Result<Value, EvalError> {
    if args.is_empty() {
        return Err(EvalError::Type(format!(
            "{func_name}: expected at least 1 arg, got 0"
        )));
    }
    let mut best: Option<Value> = None;
    for value in args {
        if matches!(value, Value::Null) {
            if matches!(null_policy, NullPolicy::Propagate) {
                return Ok(Value::Null);
            }
            continue;
        }
        let replace = match &best {
            None => true,
            Some(current) => {
                let ordering = compare_values(value, current)?;
                matches!(
                    (kind, ordering),
                    (ExtremumKind::Least, std::cmp::Ordering::Less)
                        | (ExtremumKind::Greatest, std::cmp::Ordering::Greater)
                )
            }
        };
        if replace {
            best = Some(value.clone());
        }
    }
    Ok(best.unwrap_or(Value::Null))
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

fn eval_current_schemas(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "current_schemas: expected 1 arg, got {}",
            args.len()
        )));
    }
    let include_implicit = match args[0] {
        Value::Bool(value) => value,
        Value::Null => false,
        ref other => {
            return Err(EvalError::Type(format!(
                "current_schemas: boolean argument required, got {:?}",
                other.data_type()
            )));
        }
    };
    let mut elements = Vec::new();
    if include_implicit {
        elements.push(Value::Text("pg_catalog".to_owned()));
    }
    elements.push(Value::Text("public".to_owned()));
    Ok(Value::Array {
        element_type: DataType::Text { max_len: None },
        elements,
    })
}

fn eval_to_regtype(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "to_regtype: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::RegType(oid) => Ok(Value::RegType(*oid)),
        Value::Text(text) | Value::Char(text) => Ok(resolve_regtype_text(text)
            .map(Value::RegType)
            .unwrap_or(Value::Null)),
        other => Err(EvalError::Type(format!(
            "to_regtype: text argument required, got {:?}",
            other.data_type()
        ))),
    }
}

fn eval_pg_table_is_visible(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_table_is_visible: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Oid(_)
        | Value::RegClass(_)
        | Value::Int16(_)
        | Value::Int32(_)
        | Value::Int64(_) => Ok(Value::Bool(true)),
        other => Err(EvalError::Type(format!(
            "pg_table_is_visible: OID argument required, got {:?}",
            other.data_type()
        ))),
    }
}

fn eval_pg_is_other_temp_schema(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_is_other_temp_schema: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Oid(_)
        | Value::RegClass(_)
        | Value::RegType(_)
        | Value::Int16(_)
        | Value::Int32(_)
        | Value::Int64(_) => Ok(Value::Bool(false)),
        other => Err(EvalError::Type(format!(
            "pg_is_other_temp_schema: OID argument required, got {:?}",
            other.data_type()
        ))),
    }
}

fn eval_pg_function_is_visible(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_function_is_visible: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Oid(_)
        | Value::RegClass(_)
        | Value::RegType(_)
        | Value::Int16(_)
        | Value::Int32(_)
        | Value::Int64(_) => Ok(Value::Bool(true)),
        other => Err(EvalError::Type(format!(
            "pg_function_is_visible: OID argument required, got {:?}",
            other.data_type()
        ))),
    }
}

fn eval_pg_relation_is_publishable(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_relation_is_publishable: expected 1 arg, got {}",
            args.len()
        )));
    }
    Ok(Value::Bool(!matches!(args[0], Value::Null)))
}

fn eval_set_config(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "set_config: expected 3 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    if !matches!(args[0], Value::Text(_) | Value::Char(_)) {
        return Err(EvalError::Type(format!(
            "set_config: setting name must be text, got {:?}",
            args[0].data_type()
        )));
    }
    let value = match &args[1] {
        Value::Text(text) | Value::Char(text) => text.clone(),
        other => {
            return Err(EvalError::Type(format!(
                "set_config: setting value must be text, got {:?}",
                other.data_type()
            )));
        }
    };
    if !matches!(args[2], Value::Bool(_) | Value::Null) {
        return Err(EvalError::Type(format!(
            "set_config: local flag must be boolean, got {:?}",
            args[2].data_type()
        )));
    }
    Ok(Value::Text(value))
}

fn eval_format_type(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "format_type: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let Some(oid) = oid_or_integer_arg(&args[0]) else {
        return Err(EvalError::Type(format!(
            "format_type: oid argument required, got {:?}",
            args[0].data_type()
        )));
    };
    let name = builtin_type_display_name(oid).unwrap_or("text");
    Ok(Value::Text(name.to_owned()))
}

fn builtin_type_display_name(oid: u32) -> Option<&'static str> {
    match oid {
        16 => Some("boolean"),
        17 => Some("bytea"),
        20 => Some("bigint"),
        21 => Some("smallint"),
        23 => Some("integer"),
        25 => Some("text"),
        26 => Some("oid"),
        700 => Some("real"),
        701 => Some("double precision"),
        790 => Some("money"),
        114 => Some("json"),
        142 => Some("xml"),
        143 => Some("xml[]"),
        650 => Some("cidr"),
        829 => Some("macaddr"),
        869 => Some("inet"),
        1042 => Some("character"),
        1082 => Some("date"),
        1083 => Some("time without time zone"),
        1114 => Some("timestamp without time zone"),
        1184 => Some("timestamp with time zone"),
        1266 => Some("time with time zone"),
        1560 => Some("bit"),
        1562 => Some("bit varying"),
        1700 => Some("numeric"),
        2950 => Some("uuid"),
        3220 => Some("pg_lsn"),
        3614 => Some("tsvector"),
        3615 => Some("tsquery"),
        3802 => Some("jsonb"),
        2205 => Some("regclass"),
        2206 => Some("regtype"),
        _ => None,
    }
}

fn eval_pg_get_expr(args: &[Value]) -> Result<Value, EvalError> {
    if !(args.len() == 2 || args.len() == 3) {
        return Err(EvalError::Type(format!(
            "pg_get_expr: expected 2 or 3 args, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Text(text) | Value::Char(text) => Ok(Value::Text(text.clone())),
        other => Err(EvalError::Type(format!(
            "pg_get_expr: expression text required, got {:?}",
            other.data_type()
        ))),
    }
}

fn eval_pg_get_indexdef(args: &[Value]) -> Result<Value, EvalError> {
    if !(args.len() == 1 || args.len() == 2 || args.len() == 3) {
        return Err(EvalError::Type(format!(
            "pg_get_indexdef: expected 1 to 3 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let Some(oid) = oid_or_integer_arg(&args[0]) else {
        return Err(EvalError::Type(format!(
            "pg_get_indexdef: oid argument required, got {:?}",
            args[0].data_type()
        )));
    };
    Ok(Value::Text(format!("index {oid}")))
}

fn eval_pg_get_constraintdef(args: &[Value]) -> Result<Value, EvalError> {
    if !(args.len() == 1 || args.len() == 2) {
        return Err(EvalError::Type(format!(
            "pg_get_constraintdef: expected 1 or 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let Some(oid) = oid_or_integer_arg(&args[0]) else {
        return Err(EvalError::Type(format!(
            "pg_get_constraintdef: oid argument required, got {:?}",
            args[0].data_type()
        )));
    };
    if oid == 0 {
        return Ok(Value::Null);
    }
    Ok(Value::Text(format!("constraint {oid}")))
}

fn eval_pg_get_statisticsobjdef_columns(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_get_statisticsobjdef_columns: expected 1 arg, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    if oid_or_integer_arg(&args[0]).is_none() {
        return Err(EvalError::Type(format!(
            "pg_get_statisticsobjdef_columns: oid argument required, got {:?}",
            args[0].data_type()
        )));
    }
    Ok(Value::Text(String::new()))
}

fn eval_pg_get_function_result(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_get_function_result: expected 1 arg, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    if oid_or_integer_arg(&args[0]).is_none() {
        return Err(EvalError::Type(format!(
            "pg_get_function_result: oid argument required, got {:?}",
            args[0].data_type()
        )));
    }
    Ok(Value::Text(String::new()))
}

fn eval_pg_get_function_arguments(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_get_function_arguments: expected 1 arg, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    if oid_or_integer_arg(&args[0]).is_none() {
        return Err(EvalError::Type(format!(
            "pg_get_function_arguments: oid argument required, got {:?}",
            args[0].data_type()
        )));
    }
    Ok(Value::Text(String::new()))
}

fn eval_pg_encoding_to_char(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "pg_encoding_to_char: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(encoding) = args[0].as_i64() else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "pg_encoding_to_char: integer argument required, got {:?}",
                args[0].data_type()
            )))
        };
    };
    let name = if encoding == 6 { "UTF8" } else { "" };
    Ok(Value::Text(name.to_owned()))
}

fn eval_obj_description(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "obj_description: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    if oid_or_integer_arg(&args[0]).is_none() {
        return Err(EvalError::Type(format!(
            "obj_description: oid argument required, got {:?}",
            args[0].data_type()
        )));
    }
    match &args[1] {
        Value::Text(_) | Value::Char(_) => Ok(Value::Null),
        other => Err(EvalError::Type(format!(
            "obj_description: catalog name must be text, got {:?}",
            other.data_type()
        ))),
    }
}

fn eval_col_description(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "col_description: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    if oid_or_integer_arg(&args[0]).is_none() || integer_value_i128(&args[1]).is_none() {
        return Err(EvalError::Type(format!(
            "col_description: oid and integer arguments required, got {:?}, {:?}",
            args[0].data_type(),
            args[1].data_type()
        )));
    }
    Ok(Value::Null)
}

fn eval_pg_get_serial_sequence(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "pg_get_serial_sequence: expected 2 args, got {}",
            args.len()
        )));
    }
    for arg in args {
        if matches!(arg, Value::Null) {
            return Ok(Value::Null);
        }
        if !matches!(arg, Value::Text(_) | Value::Char(_)) {
            return Err(EvalError::Type(format!(
                "pg_get_serial_sequence: text arguments required, got {:?}",
                arg.data_type()
            )));
        }
    }
    Ok(Value::Null)
}

fn eval_cast_oid(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "oid cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Oid(oid) | Value::RegClass(oid) | Value::RegType(oid) => Ok(Value::Oid(*oid)),
        Value::Int16(v) => cast_i64_to_oid(i64::from(*v)).map(Value::Oid),
        Value::Int32(v) => cast_i64_to_oid(i64::from(*v)).map(Value::Oid),
        Value::Int64(v) => cast_i64_to_oid(*v).map(Value::Oid),
        other => Err(EvalError::Type(format!(
            "oid cast: OID argument required, got {:?}",
            other.data_type()
        ))),
    }
}

fn eval_cast_regclass(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "regclass cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Oid(oid) | Value::RegClass(oid) | Value::RegType(oid) => Ok(Value::RegClass(*oid)),
        Value::Int16(v) => cast_i64_to_oid(i64::from(*v)).map(Value::RegClass),
        Value::Int32(v) => cast_i64_to_oid(i64::from(*v)).map(Value::RegClass),
        Value::Int64(v) => cast_i64_to_oid(*v).map(Value::RegClass),
        other => Err(EvalError::Type(format!(
            "regclass cast: OID argument required, got {:?}",
            other.data_type()
        ))),
    }
}

fn eval_cast_regtype(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "regtype cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Oid(oid) | Value::RegClass(oid) | Value::RegType(oid) => Ok(Value::RegType(*oid)),
        Value::Int16(v) => cast_i64_to_oid(i64::from(*v)).map(Value::RegType),
        Value::Int32(v) => cast_i64_to_oid(i64::from(*v)).map(Value::RegType),
        Value::Int64(v) => cast_i64_to_oid(*v).map(Value::RegType),
        other => Err(EvalError::Type(format!(
            "regtype cast: OID argument required, got {:?}",
            other.data_type()
        ))),
    }
}

fn cast_i64_to_oid(raw: i64) -> Result<Oid, EvalError> {
    u32::try_from(raw)
        .map(Oid::new)
        .map_err(|_| EvalError::Type(format!("OID cast: value out of range: {raw}")))
}

fn eval_cast_text(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "text cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    if let Value::RegType(oid) = &args[0] {
        return Ok(Value::Text(
            builtin_type_display_name(oid.raw())
                .map_or_else(|| oid.raw().to_string(), str::to_owned),
        ));
    }
    Ok(Value::Text(args[0].to_string()))
}

fn eval_cast_money(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "money cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Money(cents) => Ok(Value::Money(*cents)),
        Value::Int16(v) => int_to_money(i64::from(*v)),
        Value::Int32(v) => int_to_money(i64::from(*v)),
        Value::Int64(v) => int_to_money(*v),
        Value::Decimal { .. } => parse_money_text(&args[0].to_string())
            .map_err(|err| EvalError::Type(format!("money cast: {err}"))),
        other => Err(EvalError::Type(format!(
            "money cast: numeric argument required, got {:?}",
            other.data_type()
        ))),
    }
}

fn int_to_money(value: i64) -> Result<Value, EvalError> {
    value
        .checked_mul(100)
        .map(Value::Money)
        .ok_or(EvalError::Overflow)
}

fn eval_cast_numeric(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "numeric cast: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Money(cents) => Ok(Value::Decimal {
            value: *cents,
            scale: 2,
        }),
        Value::Decimal { value, scale } => Ok(Value::Decimal {
            value: *value,
            scale: *scale,
        }),
        other => Err(EvalError::Type(format!(
            "numeric cast: money argument required, got {:?}",
            other.data_type()
        ))),
    }
}

fn resolve_regtype_text(text: &str) -> Option<Oid> {
    let trimmed = text.trim();
    let unqualified = trimmed
        .strip_prefix("pg_catalog.")
        .or_else(|| trimmed.strip_prefix("PG_CATALOG."))
        .unwrap_or(trimmed);
    Value::parse_oid_text(unqualified).or_else(|| builtin_type_oid(unqualified))
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
    let Value::Array { .. } = &args[0] else {
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
    if dim < 1 {
        return Ok(Value::Null);
    }
    let dimensions = args[0]
        .array_dimensions()
        .ok_or_else(|| EvalError::Type("array_length: ragged array value".to_owned()))?;
    let dimension_idx =
        usize::try_from(dim - 1).map_err(|_| EvalError::Type("array dimension overflow".into()))?;
    let Some(len) = dimensions.get(dimension_idx) else {
        return Ok(Value::Null);
    };
    let len =
        i32::try_from(*len).map_err(|_| EvalError::Type("array_length overflow".to_owned()))?;
    Ok(Value::Int32(len))
}

fn eval_array_ndims(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "array_ndims: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(dimensions) = array_dimensions_for_function("array_ndims", &args[0])? else {
        return Ok(Value::Null);
    };
    let ndims = i32::try_from(dimensions.len())
        .map_err(|_| EvalError::Type("array_ndims overflow".into()))?;
    Ok(Value::Int32(ndims))
}

#[derive(Debug, Clone, Copy)]
enum ArrayBound {
    Lower,
    Upper,
}

fn eval_array_bound(args: &[Value], bound: ArrayBound) -> Result<Value, EvalError> {
    let function_name = match bound {
        ArrayBound::Lower => "array_lower",
        ArrayBound::Upper => "array_upper",
    };
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "{function_name}: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(dimensions) = array_dimensions_for_function(function_name, &args[0])? else {
        return Ok(Value::Null);
    };
    let Some(dim) = args[1].as_i64() else {
        return if matches!(args[1], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "{function_name}: integer dimension required, got {:?}",
                args[1].data_type()
            )))
        };
    };
    if dim < 1 {
        return Ok(Value::Null);
    }
    let dimension_idx =
        usize::try_from(dim - 1).map_err(|_| EvalError::Type("array dimension overflow".into()))?;
    let Some(len) = dimensions.get(dimension_idx) else {
        return Ok(Value::Null);
    };
    if *len == 0 {
        return Ok(Value::Null);
    }
    let value = match bound {
        ArrayBound::Lower => 1,
        ArrayBound::Upper => {
            i32::try_from(*len).map_err(|_| EvalError::Type("array_upper overflow".to_owned()))?
        }
    };
    Ok(Value::Int32(value))
}

fn eval_array_dims(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "array_dims: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(dimensions) = array_dimensions_for_function("array_dims", &args[0])? else {
        return Ok(Value::Null);
    };
    if dimensions.contains(&0) {
        return Ok(Value::Null);
    }
    let mut output = String::new();
    for len in dimensions {
        output.push_str("[1:");
        output.push_str(&len.to_string());
        output.push(']');
    }
    Ok(Value::Text(output))
}

fn eval_array_cardinality(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "cardinality: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(dimensions) = array_dimensions_for_function("cardinality", &args[0])? else {
        return Ok(Value::Null);
    };
    let mut total = 1usize;
    for len in dimensions {
        total = total
            .checked_mul(len)
            .ok_or_else(|| EvalError::Type("cardinality overflow".to_owned()))?;
    }
    let total =
        i32::try_from(total).map_err(|_| EvalError::Type("cardinality overflow".to_owned()))?;
    Ok(Value::Int32(total))
}

fn array_dimensions_for_function(
    function_name: &str,
    value: &Value,
) -> Result<Option<Vec<usize>>, EvalError> {
    if matches!(value, Value::Null) {
        return Ok(None);
    }
    let Value::Array { .. } = value else {
        return Err(EvalError::Type(format!(
            "{function_name}: array argument required, got {:?}",
            value.data_type()
        )));
    };
    value
        .array_dimensions()
        .map(Some)
        .ok_or_else(|| EvalError::Type(format!("{function_name}: ragged array value")))
}

fn eval_array_subscript(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "array subscript: expected 2 args, got {}",
            args.len()
        )));
    }
    let Value::Array { elements, .. } = &args[0] else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "array subscript: array argument required, got {:?}",
                args[0].data_type()
            )))
        };
    };
    let Some(index) = args[1].as_i64() else {
        return if matches!(args[1], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "array subscript: integer index required, got {:?}",
                args[1].data_type()
            )))
        };
    };
    if index < 1 {
        return Ok(Value::Null);
    }
    let zero_idx =
        usize::try_from(index - 1).map_err(|_| EvalError::Type("array index overflow".into()))?;
    Ok(elements.get(zero_idx).cloned().unwrap_or(Value::Null))
}

fn eval_array_slice(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "array slice: expected 3 args, got {}",
            args.len()
        )));
    }
    let Value::Array {
        element_type,
        elements,
    } = &args[0]
    else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "array slice: array argument required, got {:?}",
                args[0].data_type()
            )))
        };
    };
    let lower = optional_array_slice_bound(&args[1], "lower")?;
    let upper = optional_array_slice_bound(&args[2], "upper")?;
    let len = i64::try_from(elements.len())
        .map_err(|_| EvalError::Type("array slice length overflow".to_owned()))?;
    let lower = lower.unwrap_or(1);
    let upper = upper.unwrap_or(len);
    if len == 0 || lower > upper || upper < 1 || lower > len {
        return Ok(Value::Array {
            element_type: element_type.clone(),
            elements: Vec::new(),
        });
    }
    let start = lower.max(1);
    let end = upper.min(len);
    let start_idx =
        usize::try_from(start - 1).map_err(|_| EvalError::Type("array slice overflow".into()))?;
    let end_exclusive =
        usize::try_from(end).map_err(|_| EvalError::Type("array slice overflow".into()))?;
    Ok(Value::Array {
        element_type: element_type.clone(),
        elements: elements[start_idx..end_exclusive].to_vec(),
    })
}

fn optional_array_slice_bound(value: &Value, name: &'static str) -> Result<Option<i64>, EvalError> {
    if matches!(value, Value::Null) {
        return Ok(None);
    }
    value.as_i64().map(Some).ok_or_else(|| {
        EvalError::Type(format!(
            "array slice: {name} bound must be integer, got {:?}",
            value.data_type()
        ))
    })
}

fn eval_eq_any_array(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "= ANY array: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let Value::Array { elements, .. } = &args[1] else {
        return if matches!(args[1], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "= ANY array: array argument required, got {:?}",
                args[1].data_type()
            )))
        };
    };
    let mut saw_null = false;
    for element in elements {
        if matches!(element, Value::Null) {
            saw_null = true;
            continue;
        }
        if compare_values(&args[0], element)? == std::cmp::Ordering::Equal {
            return Ok(Value::Bool(true));
        }
    }
    if saw_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Bool(false))
    }
}

fn eval_array_position(args: &[Value]) -> Result<Value, EvalError> {
    if !(2..=3).contains(&args.len()) {
        return Err(EvalError::Type(format!(
            "array_position: expected 2 or 3 args, got {}",
            args.len()
        )));
    }
    let Value::Array { elements, .. } = &args[0] else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "array_position: array argument required, got {:?}",
                args[0].data_type()
            )))
        };
    };
    if matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    let start_idx = match args.get(2) {
        Some(Value::Null) => return Ok(Value::Null),
        Some(value) => {
            let Some(start) = value.as_i64() else {
                return Err(EvalError::Type(format!(
                    "array_position: integer start required, got {:?}",
                    value.data_type()
                )));
            };
            if start < 1 {
                return Ok(Value::Null);
            }
            usize::try_from(start - 1)
                .map_err(|_| EvalError::Type("array_position start overflow".to_owned()))?
        }
        None => 0,
    };
    for (idx, element) in elements.iter().enumerate().skip(start_idx) {
        if matches!(element, Value::Null) {
            continue;
        }
        if compare_values(element, &args[1])? == std::cmp::Ordering::Equal {
            let pos = i32::try_from(idx + 1)
                .map_err(|_| EvalError::Type("array_position overflow".to_owned()))?;
            return Ok(Value::Int32(pos));
        }
    }
    Ok(Value::Null)
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
    append_array_to_string_parts(elements, null_text, &mut parts);
    Ok(Value::Text(parts.join(delimiter)))
}

fn append_array_to_string_parts(
    elements: &[Value],
    null_text: Option<&str>,
    parts: &mut Vec<String>,
) {
    for element in elements {
        match element {
            Value::Array { elements, .. } => {
                append_array_to_string_parts(elements, null_text, parts);
            }
            Value::Null => {
                if let Some(text) = null_text {
                    parts.push(text.to_owned());
                }
            }
            other => parts.push(other.to_string()),
        }
    }
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

fn eval_array_append(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "array_append: expected 2 args, got {}",
            args.len()
        )));
    }
    append_array_element("array_append", &args[0], &args[1], false)
}

fn eval_array_prepend(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "array_prepend: expected 2 args, got {}",
            args.len()
        )));
    }
    append_array_element("array_prepend", &args[1], &args[0], true)
}

fn append_array_element(
    function_name: &str,
    array_value: &Value,
    element: &Value,
    prepend: bool,
) -> Result<Value, EvalError> {
    let Value::Array {
        element_type,
        elements,
    } = array_value
    else {
        return if matches!(array_value, Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "{function_name}: array argument required, got {:?}",
                array_value.data_type()
            )))
        };
    };
    validate_array_element_value(function_name, element_type, element)?;
    let mut output = Vec::with_capacity(elements.len() + 1);
    if prepend {
        output.push(element.clone());
        output.extend_from_slice(elements);
    } else {
        output.extend_from_slice(elements);
        output.push(element.clone());
    }
    Ok(Value::Array {
        element_type: element_type.clone(),
        elements: output,
    })
}

fn eval_array_remove(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "array_remove: expected 2 args, got {}",
            args.len()
        )));
    }
    let Value::Array {
        element_type,
        elements,
    } = &args[0]
    else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "array_remove: array argument required, got {:?}",
                args[0].data_type()
            )))
        };
    };
    let needle = &args[1];
    validate_array_element_value("array_remove", element_type, needle)?;
    let mut output = Vec::with_capacity(elements.len());
    for element in elements {
        let should_remove = array_element_matches(element, needle)?;
        if !should_remove {
            output.push(element.clone());
        }
    }
    Ok(Value::Array {
        element_type: element_type.clone(),
        elements: output,
    })
}

fn eval_array_replace(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "array_replace: expected 3 args, got {}",
            args.len()
        )));
    }
    let Value::Array {
        element_type,
        elements,
    } = &args[0]
    else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "array_replace: array argument required, got {:?}",
                args[0].data_type()
            )))
        };
    };
    let from = &args[1];
    let to = &args[2];
    validate_array_element_value("array_replace", element_type, from)?;
    validate_array_element_value("array_replace", element_type, to)?;
    let mut output = Vec::with_capacity(elements.len());
    for element in elements {
        if array_element_matches(element, from)? {
            output.push(to.clone());
        } else {
            output.push(element.clone());
        }
    }
    Ok(Value::Array {
        element_type: element_type.clone(),
        elements: output,
    })
}

fn eval_array_positions(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "array_positions: expected 2 args, got {}",
            args.len()
        )));
    }
    let Value::Array {
        element_type,
        elements,
    } = &args[0]
    else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "array_positions: array argument required, got {:?}",
                args[0].data_type()
            )))
        };
    };
    let needle = &args[1];
    validate_array_element_value("array_positions", element_type, needle)?;
    let mut positions = Vec::new();
    for (idx, element) in elements.iter().enumerate() {
        if array_element_matches(element, needle)? {
            let position = i32::try_from(idx + 1)
                .map_err(|_| EvalError::Type("array_positions overflow".to_owned()))?;
            positions.push(Value::Int32(position));
        }
    }
    Ok(Value::Array {
        element_type: DataType::Int32,
        elements: positions,
    })
}

fn eval_trim_array(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "trim_array: expected 2 args, got {}",
            args.len()
        )));
    }
    let Value::Array {
        element_type,
        elements,
    } = &args[0]
    else {
        return if matches!(args[0], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "trim_array: array argument required, got {:?}",
                args[0].data_type()
            )))
        };
    };
    let Some(trim_count) = args[1].as_i64() else {
        return if matches!(args[1], Value::Null) {
            Ok(Value::Null)
        } else {
            Err(EvalError::Type(format!(
                "trim_array: integer trim count required, got {:?}",
                args[1].data_type()
            )))
        };
    };
    if trim_count < 0 {
        return Err(EvalError::Type(
            "trim_array: trim count must be non-negative".to_owned(),
        ));
    }
    let trim_count = usize::try_from(trim_count)
        .map_err(|_| EvalError::Type("trim_array trim count overflow".to_owned()))?;
    let keep = elements.len().saturating_sub(trim_count);
    Ok(Value::Array {
        element_type: element_type.clone(),
        elements: elements[..keep].to_vec(),
    })
}

fn validate_array_element_value(
    function_name: &str,
    element_type: &DataType,
    value: &Value,
) -> Result<(), EvalError> {
    if matches!(value, Value::Null) || value.data_type() == *element_type {
        Ok(())
    } else {
        Err(EvalError::Type(format!(
            "{function_name}: element type mismatch, expected {:?}, got {:?}",
            element_type,
            value.data_type()
        )))
    }
}

fn array_element_matches(element: &Value, needle: &Value) -> Result<bool, EvalError> {
    if matches!(needle, Value::Null) {
        Ok(matches!(element, Value::Null))
    } else if matches!(element, Value::Null) {
        Ok(false)
    } else {
        Ok(compare_values(element, needle)? == std::cmp::Ordering::Equal)
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
    let (Value::Text(s) | Value::Char(s)) = &args[0] else {
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

fn text_arg<'a>(func: &str, args: &'a [Value], idx: usize) -> Result<Option<&'a str>, EvalError> {
    match args.get(idx) {
        Some(Value::Text(text) | Value::Char(text)) => Ok(Some(text.as_str())),
        Some(Value::Null) => Ok(None),
        Some(other) => Err(EvalError::Type(format!(
            "{func}: argument {} must be text, got {:?}",
            idx + 1,
            other.data_type()
        ))),
        None => Err(EvalError::Type(format!(
            "{func}: missing argument {}",
            idx + 1
        ))),
    }
}

fn int_arg(func: &str, args: &[Value], idx: usize) -> Result<Option<i64>, EvalError> {
    match args.get(idx) {
        Some(value) => match value.as_i64() {
            Some(v) => Ok(Some(v)),
            None if matches!(value, Value::Null) => Ok(None),
            None => Err(EvalError::Type(format!(
                "{func}: argument {} must be integer, got {:?}",
                idx + 1,
                value.data_type()
            ))),
        },
        None => Err(EvalError::Type(format!(
            "{func}: missing argument {}",
            idx + 1
        ))),
    }
}

fn numeric_arg(func: &str, args: &[Value], idx: usize) -> Result<Option<f64>, EvalError> {
    match args.get(idx) {
        Some(Value::Float32(v)) => Ok(Some(f64::from(*v))),
        Some(Value::Float64(v)) => Ok(Some(*v)),
        Some(Value::Decimal { value, scale }) => {
            let base = value.to_f64().ok_or(EvalError::Overflow)?;
            Ok(Some(base / 10_f64.powi(*scale)))
        }
        Some(value) => match value.as_i64() {
            Some(v) => v.to_f64().map(Some).ok_or(EvalError::Overflow),
            None if matches!(value, Value::Null) => Ok(None),
            None => Err(EvalError::Type(format!(
                "{func}: argument {} must be numeric, got {:?}",
                idx + 1,
                value.data_type()
            ))),
        },
        None => Err(EvalError::Type(format!(
            "{func}: missing argument {}",
            idx + 1
        ))),
    }
}

fn eval_numeric_unary(
    args: &[Value],
    func: &str,
    op: impl FnOnce(f64) -> f64,
) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "{func}: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(value) = numeric_arg(func, args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Float64(op(value)))
}

fn eval_numeric_binary(
    args: &[Value],
    func: &str,
    op: impl FnOnce(f64, f64) -> f64,
) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "{func}: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(left) = numeric_arg(func, args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(right) = numeric_arg(func, args, 1)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Float64(op(left, right)))
}

fn eval_pi(args: &[Value]) -> Result<Value, EvalError> {
    if !args.is_empty() {
        return Err(EvalError::Type(format!(
            "pi: expected 0 args, got {}",
            args.len()
        )));
    }
    Ok(Value::Float64(std::f64::consts::PI))
}

fn eval_random(args: &[Value]) -> Result<Value, EvalError> {
    if !args.is_empty() {
        return Err(EvalError::Type(format!(
            "random: expected 0 args, got {}",
            args.len()
        )));
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let low = u64::try_from(now & u128::from(u64::MAX)).unwrap_or(0);
    let high = u64::try_from(now >> 64).unwrap_or(0);
    let mut state =
        low ^ high.rotate_left(11) ^ UUID_FALLBACK_COUNTER.fetch_add(1, Ordering::Relaxed);
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    let mantissa = state & ((1_u64 << 53) - 1);
    let numerator = mantissa.to_f64().ok_or(EvalError::Overflow)?;
    Ok(Value::Float64(numerator / 9_007_199_254_740_992.0))
}

fn eval_length(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "length: expected 1 arg, got {}",
            args.len()
        )));
    }
    let len = match &args[0] {
        Value::Text(text) => text.chars().count(),
        Value::Char(text) => bpchar_semantic_text(text).chars().count(),
        Value::BitString(bits) => usize::try_from(bits.len())
            .map_err(|_| EvalError::Type("length: result overflow".to_owned()))?,
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "length: argument 1 must be text or bit string, got {:?}",
                other.data_type()
            )));
        }
    };
    let len =
        i32::try_from(len).map_err(|_| EvalError::Type("length: result overflow".to_owned()))?;
    Ok(Value::Int32(len))
}

fn eval_bit_length(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "bit_length: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(bits) = bit_string_arg("bit_length", args, 0)? else {
        return Ok(Value::Null);
    };
    let len = i32::try_from(bits.len())
        .map_err(|_| EvalError::Type("bit_length: result overflow".to_owned()))?;
    Ok(Value::Int32(len))
}

fn eval_octet_length(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "octet_length: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(bits) = bit_string_arg("octet_length", args, 0)? else {
        return Ok(Value::Null);
    };
    let len = i32::try_from(bits.octet_len())
        .map_err(|_| EvalError::Type("octet_length: result overflow".to_owned()))?;
    Ok(Value::Int32(len))
}

fn eval_bit_count(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "bit_count: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(bits) = bit_string_arg("bit_count", args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Int64(i64::from(bits.bit_count())))
}

fn eval_get_bit(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "get_bit: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(bits) = bit_string_arg("get_bit", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(idx) = integer_arg_as_usize("get_bit", args, 1)? else {
        return Ok(Value::Null);
    };
    let bit = bits
        .bit(idx)
        .ok_or_else(|| EvalError::Type("get_bit: bit index out of range".to_owned()))?;
    Ok(Value::Int32(if bit { 1 } else { 0 }))
}

fn eval_set_bit(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "set_bit: expected 3 args, got {}",
            args.len()
        )));
    }
    let Some(bits) = bit_string_arg("set_bit", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(idx) = integer_arg_as_usize("set_bit", args, 1)? else {
        return Ok(Value::Null);
    };
    let Some(value) = integer_arg_as_usize("set_bit", args, 2)? else {
        return Ok(Value::Null);
    };
    if value > 1 {
        return Err(EvalError::Type(
            "set_bit: new value must be 0 or 1".to_owned(),
        ));
    }
    bits.set_bit(idx, value == 1)
        .map(Value::BitString)
        .ok_or_else(|| EvalError::Type("set_bit: bit index out of range".to_owned()))
}

fn eval_trim(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "trim: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("trim", args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(text.trim().to_owned()))
}

#[derive(Clone, Copy)]
enum PadSide {
    Left,
    Right,
}

fn eval_pad(args: &[Value], side: PadSide) -> Result<Value, EvalError> {
    let func = match side {
        PadSide::Left => "lpad",
        PadSide::Right => "rpad",
    };
    if !(2..=3).contains(&args.len()) {
        return Err(EvalError::Type(format!(
            "{func}: expected 2 or 3 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg(func, args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(target_len) = int_arg(func, args, 1)? else {
        return Ok(Value::Null);
    };
    let fill = if args.len() == 3 {
        let Some(fill) = text_arg(func, args, 2)? else {
            return Ok(Value::Null);
        };
        fill
    } else {
        " "
    };
    let target = usize::try_from(target_len.max(0)).unwrap_or(usize::MAX);
    let current = text.chars().count();
    if target <= current {
        return Ok(Value::Text(text.chars().take(target).collect()));
    }
    if fill.is_empty() {
        return Err(EvalError::Type(format!(
            "{func}: fill string cannot be empty"
        )));
    }
    let pad_needed = target - current;
    let mut padding = String::new();
    for ch in fill.chars().cycle().take(pad_needed) {
        padding.push(ch);
    }
    let out = match side {
        PadSide::Left => format!("{padding}{text}"),
        PadSide::Right => format!("{text}{padding}"),
    };
    Ok(Value::Text(out))
}

fn eval_left(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "left: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("left", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(count) = int_arg("left", args, 1)? else {
        return Ok(Value::Null);
    };
    let chars: Vec<char> = text.chars().collect();
    let keep = if count >= 0 {
        usize::try_from(count)
            .unwrap_or(usize::MAX)
            .min(chars.len())
    } else {
        chars.len().saturating_sub(i64_abs_to_usize(count))
    };
    Ok(Value::Text(chars.into_iter().take(keep).collect()))
}

fn eval_right(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "right: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("right", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(count) = int_arg("right", args, 1)? else {
        return Ok(Value::Null);
    };
    let chars: Vec<char> = text.chars().collect();
    let skip = if count >= 0 {
        chars
            .len()
            .saturating_sub(usize::try_from(count).unwrap_or(usize::MAX))
    } else {
        i64_abs_to_usize(count).min(chars.len())
    };
    Ok(Value::Text(chars.into_iter().skip(skip).collect()))
}

fn i64_abs_to_usize(value: i64) -> usize {
    usize::try_from(value.unsigned_abs()).unwrap_or(usize::MAX)
}

fn eval_position(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "position: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(needle) = text_arg("position", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(haystack) = text_arg("position", args, 1)? else {
        return Ok(Value::Null);
    };
    let pos = haystack.find(needle).map_or(0_i32, |byte_idx| {
        let chars_before = haystack[..byte_idx].chars().count();
        i32::try_from(chars_before.saturating_add(1)).unwrap_or(i32::MAX)
    });
    Ok(Value::Int32(pos))
}

fn eval_replace(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "replace: expected 3 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("replace", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(from) = text_arg("replace", args, 1)? else {
        return Ok(Value::Null);
    };
    let Some(to) = text_arg("replace", args, 2)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(text.replace(from, to)))
}

fn eval_split_part(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "split_part: expected 3 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("split_part", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(delimiter) = text_arg("split_part", args, 1)? else {
        return Ok(Value::Null);
    };
    let Some(field) = int_arg("split_part", args, 2)? else {
        return Ok(Value::Null);
    };
    if field <= 0 {
        return Err(EvalError::Type(
            "split_part: field position must be greater than zero".to_owned(),
        ));
    }
    let target = usize::try_from(field.saturating_sub(1)).unwrap_or(usize::MAX);
    Ok(Value::Text(
        text.split(delimiter).nth(target).unwrap_or("").to_owned(),
    ))
}

fn eval_concat(args: &[Value]) -> Result<Value, EvalError> {
    let mut out = String::new();
    for arg in args {
        if !matches!(arg, Value::Null) {
            out.push_str(&arg.to_string());
        }
    }
    Ok(Value::Text(out))
}

fn eval_concat_ws(args: &[Value]) -> Result<Value, EvalError> {
    if args.is_empty() {
        return Err(EvalError::Type(
            "concat_ws: expected at least 1 arg".to_owned(),
        ));
    }
    let Some(separator) = text_arg("concat_ws", args, 0)? else {
        return Ok(Value::Null);
    };
    let mut parts = Vec::new();
    for arg in &args[1..] {
        if !matches!(arg, Value::Null) {
            parts.push(arg.to_string());
        }
    }
    Ok(Value::Text(parts.join(separator)))
}

fn eval_repeat(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "repeat: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("repeat", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(count) = int_arg("repeat", args, 1)? else {
        return Ok(Value::Null);
    };
    let count = usize::try_from(count.max(0)).unwrap_or(usize::MAX);
    Ok(Value::Text(text.repeat(count)))
}

fn eval_reverse(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "reverse: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("reverse", args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(text.chars().rev().collect()))
}

fn eval_md5(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "md5: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("md5", args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(format!("{:x}", md5::compute(text.as_bytes()))))
}

fn eval_sha256(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "sha256: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("sha256", args, 0)? else {
        return Ok(Value::Null);
    };
    use sha2::Digest;
    let digest = sha2::Sha256::digest(text.as_bytes());
    let mut out = String::with_capacity(digest.len().saturating_mul(2));
    for byte in digest {
        write!(&mut out, "{byte:02x}")
            .map_err(|_| EvalError::Type("sha256: hex encoding failed".to_owned()))?;
    }
    Ok(Value::Text(out))
}

fn eval_quote_ident(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "quote_ident: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("quote_ident", args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(quote_identifier(text)))
}

fn eval_quote_literal(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "quote_literal: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("quote_literal", args, 0)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(quote_literal(text)))
}

fn eval_format(args: &[Value]) -> Result<Value, EvalError> {
    if args.is_empty() {
        return Err(EvalError::Type(
            "format: expected at least 1 arg".to_owned(),
        ));
    }
    let Some(template) = text_arg("format", args, 0)? else {
        return Ok(Value::Null);
    };
    let mut out = String::new();
    let mut chars = template.chars();
    let mut arg_idx = 1_usize;
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        let Some(spec) = chars.next() else {
            return Err(EvalError::Type(
                "format: unterminated format specifier".to_owned(),
            ));
        };
        if spec == '%' {
            out.push('%');
            continue;
        }
        let Some(value) = args.get(arg_idx) else {
            return Err(EvalError::Type("format: too few arguments".to_owned()));
        };
        arg_idx = arg_idx.saturating_add(1);
        match spec {
            's' => {
                if !matches!(value, Value::Null) {
                    out.push_str(&format_value_text(value));
                }
            }
            'I' => {
                if matches!(value, Value::Null) {
                    return Err(EvalError::Type("format: %I argument is null".to_owned()));
                }
                out.push_str(&quote_identifier(&format_value_text(value)));
            }
            'L' => {
                if matches!(value, Value::Null) {
                    out.push_str("NULL");
                } else {
                    out.push_str(&quote_literal(&format_value_text(value)));
                }
            }
            other => {
                return Err(EvalError::Type(format!(
                    "format: unsupported format specifier %{other}"
                )));
            }
        }
    }
    Ok(Value::Text(out))
}

fn eval_regexp_replace(args: &[Value]) -> Result<Value, EvalError> {
    if !(args.len() == 3 || args.len() == 4) {
        return Err(EvalError::Type(format!(
            "regexp_replace: expected 3 or 4 args, got {}",
            args.len()
        )));
    }
    let Some(text) = text_arg("regexp_replace", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(pattern) = text_arg("regexp_replace", args, 1)? else {
        return Ok(Value::Null);
    };
    let Some(replacement) = text_arg("regexp_replace", args, 2)? else {
        return Ok(Value::Null);
    };
    let flags = if args.len() == 4 {
        let Some(flags) = text_arg("regexp_replace", args, 3)? else {
            return Ok(Value::Null);
        };
        flags
    } else {
        ""
    };
    let regex = regex::Regex::new(pattern)
        .map_err(|err| EvalError::Type(format!("regexp_replace: invalid pattern: {err}")))?;
    let replaced = if flags.contains('g') {
        regex.replace_all(text, replacement)
    } else {
        regex.replace(text, replacement)
    };
    Ok(Value::Text(replaced.into_owned()))
}

fn format_value_text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.clone(),
        other => other.to_string(),
    }
}

fn eval_json_build_object(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() % 2 != 0 {
        return Err(EvalError::Type(format!(
            "json_build_object: expected even number of args, got {}",
            args.len()
        )));
    }

    let mut object = JsonMap::new();
    for pair in args.chunks_exact(2) {
        if matches!(pair[0], Value::Null) {
            return Err(EvalError::Type(
                "json_build_object: key must not be null".to_owned(),
            ));
        }
        object.insert(format_value_text(&pair[0]), sql_value_to_json(&pair[1]));
    }
    serde_json::to_string(&JsonValue::Object(object))
        .map(Value::Jsonb)
        .map_err(|err| EvalError::Type(format!("json_build_object: encode failed: {err}")))
}

fn eval_row_constructor(args: &[Value], return_type: &DataType) -> Result<Value, EvalError> {
    let field_names = match return_type {
        DataType::Record(fields) if fields.len() == args.len() => fields
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>(),
        _ => (0..args.len())
            .map(|idx| format!("f{}", idx + 1))
            .collect::<Vec<_>>(),
    };
    let fields = field_names
        .into_iter()
        .zip(args.iter())
        .map(|(name, value)| (name, value.clone()))
        .collect();
    Ok(Value::Record(fields))
}

fn eval_row_to_json(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "row_to_json: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Record(_) => json_value_to_jsonb(sql_value_to_json(&args[0]), "row_to_json"),
        Value::Json(text) | Value::Jsonb(text) | Value::Text(text) => {
            serde_json::from_str::<JsonValue>(text)
                .map_err(|err| EvalError::Type(format!("row_to_json: invalid json: {err}")))
                .and_then(|value| json_value_to_jsonb(value, "row_to_json"))
        }
        other => Err(EvalError::Type(format!(
            "row_to_json: expected record, json/jsonb, or text, got {:?}",
            other.data_type()
        ))),
    }
}

fn eval_jsonb_set(args: &[Value]) -> Result<Value, EvalError> {
    if !(args.len() == 3 || args.len() == 4) {
        return Err(EvalError::Type(format!(
            "jsonb_set: expected 3 or 4 args, got {}",
            args.len()
        )));
    }
    let Some(mut target) = json_document_arg("jsonb_set", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(path) = json_path_arg("jsonb_set", args, 1)? else {
        return Ok(Value::Null);
    };
    let Some(new_value) = json_document_arg("jsonb_set", args, 2)? else {
        return Ok(Value::Null);
    };
    let create_missing = match args.get(3) {
        Some(Value::Bool(v)) => *v,
        Some(Value::Null) => return Ok(Value::Null),
        Some(other) => {
            return Err(EvalError::Type(format!(
                "jsonb_set: create_missing must be boolean, got {:?}",
                other.data_type()
            )));
        }
        None => true,
    };

    let changed = set_json_path(&mut target, &path, new_value, create_missing);
    if !changed {
        return json_value_to_jsonb(target, "jsonb_set");
    }
    json_value_to_jsonb(target, "jsonb_set")
}

fn eval_jsonb_path_exists(args: &[Value]) -> Result<Value, EvalError> {
    if !(2..=3).contains(&args.len()) {
        return Err(EvalError::Type(format!(
            "jsonb_path_exists: expected 2 or 3 args, got {}",
            args.len()
        )));
    }
    let Some(document) = json_document_arg("jsonb_path_exists", args, 0)? else {
        return Ok(Value::Null);
    };
    let path = match &args[1] {
        Value::Text(text) | Value::Json(text) | Value::Jsonb(text) => text,
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "jsonb_path_exists: path must be text, got {:?}",
                other.data_type()
            )));
        }
    };
    let path = parse_json_path(path)
        .map_err(|err| EvalError::Type(format!("jsonb_path_exists: invalid jsonpath: {err}")))?;
    let vars = if args.len() == 3 {
        json_document_arg("jsonb_path_exists", args, 2)?
    } else {
        None
    };
    let selected = select_json_path_with_vars(&document, &path, vars.as_ref())
        .map_err(|err| EvalError::Type(format!("jsonb_path_exists: {err}")))?;
    Ok(Value::Bool(!selected.is_empty()))
}

#[derive(Clone, Copy)]
enum XmlWellFormedMode {
    Content,
    Document,
}

fn eval_xml_is_well_formed(args: &[Value], mode: XmlWellFormedMode) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "xml_is_well_formed: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(text) = xml_text_arg("xml_is_well_formed", args, 0)? else {
        return Ok(Value::Null);
    };
    let ok = match mode {
        XmlWellFormedMode::Content => xml_content_is_well_formed(text),
        XmlWellFormedMode::Document => xml_document_is_well_formed(text),
    };
    Ok(Value::Bool(ok))
}

fn eval_xmlparse(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "xmlparse: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(mode) = xml_mode_arg("xmlparse", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(text) = xml_text_arg("xmlparse", args, 1)? else {
        return Ok(Value::Null);
    };
    parse_xml_value("xmlparse", mode, text)
}

fn eval_xmlserialize(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "xmlserialize: expected 3 args, got {}",
            args.len()
        )));
    }
    let Some(mode) = xml_mode_arg("xmlserialize", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(text) = xml_text_arg("xmlserialize", args, 1)? else {
        return Ok(Value::Null);
    };
    let Some(target) = xml_text_arg("xmlserialize", args, 2)? else {
        return Ok(Value::Null);
    };
    if !target.eq_ignore_ascii_case("text") {
        return Err(EvalError::Type(format!(
            "xmlserialize: only AS TEXT is supported, got {target}"
        )));
    }
    let parsed = parse_xml_value("xmlserialize", mode, text)?;
    let Value::Xml(text) = parsed else {
        return Err(EvalError::Type(format!(
            "xmlserialize: expected XML parser output, got {:?}",
            parsed.data_type()
        )));
    };
    Ok(Value::Text(text))
}

fn parse_xml_value(
    function: &'static str,
    mode: XmlWellFormedMode,
    text: &str,
) -> Result<Value, EvalError> {
    let text = text.trim();
    if text.is_empty() {
        return Err(EvalError::Type(format!("{function}: empty XML input")));
    }
    let valid = match mode {
        XmlWellFormedMode::Content => xml_content_is_well_formed(text),
        XmlWellFormedMode::Document => xml_document_is_well_formed(text),
    };
    if valid {
        return Ok(Value::Xml(text.to_owned()));
    }
    let shape = match mode {
        XmlWellFormedMode::Content => "well-formed XML content",
        XmlWellFormedMode::Document => "well-formed XML document",
    };
    Err(EvalError::Type(format!("{function}: expected {shape}")))
}

const XPATH_SUPPORTED_SUBSET: &str = concat!(
    "supported subset is absolute element paths with optional @attr equality, ",
    "wildcards, text(), count(), string(), boolean(), name(), namespaces, ",
    "descendant paths, and basic child::, attribute::, descendant::, and ",
    "self::node() axes"
);

fn eval_xpath_exists(args: &[Value]) -> Result<Value, EvalError> {
    if !(2..=3).contains(&args.len()) {
        return Err(EvalError::Type(format!(
            "xpath_exists: expected 2 or 3 args, got {}",
            args.len()
        )));
    }
    let Some(path) = xml_text_arg("xpath_exists", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(document) = xml_text_arg("xpath_exists", args, 1)? else {
        return Ok(Value::Null);
    };
    let namespaces = xpath_namespace_arg("xpath_exists", args.get(2))?;
    let fragments = xml_xpath_element_fragments_with_namespaces(path, document, &namespaces)
        .ok_or_else(|| EvalError::Type(format!("xpath_exists: {XPATH_SUPPORTED_SUBSET}")))?;
    Ok(Value::Bool(!fragments.is_empty()))
}

fn eval_xpath(args: &[Value]) -> Result<Value, EvalError> {
    if !(2..=3).contains(&args.len()) {
        return Err(EvalError::Type(format!(
            "xpath: expected 2 or 3 args, got {}",
            args.len()
        )));
    }
    let Some(path) = xml_text_arg("xpath", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(document) = xml_text_arg("xpath", args, 1)? else {
        return Ok(Value::Null);
    };
    let namespaces = xpath_namespace_arg("xpath", args.get(2))?;
    let fragments = xml_xpath_element_fragments_with_namespaces(path, document, &namespaces)
        .ok_or_else(|| EvalError::Type(format!("xpath: {XPATH_SUPPORTED_SUBSET}")))?;
    Ok(Value::Array {
        element_type: DataType::Xml,
        elements: fragments.into_iter().map(Value::Xml).collect(),
    })
}

fn xpath_namespace_arg(
    function: &str,
    value: Option<&Value>,
) -> Result<Vec<(String, String)>, EvalError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if matches!(value, Value::Null) {
        return Ok(Vec::new());
    }
    let Value::Array { elements, .. } = value else {
        return Err(EvalError::Type(format!(
            "{function}: namespace argument must be text[][]"
        )));
    };
    elements
        .iter()
        .map(|row| {
            let Value::Array { elements, .. } = row else {
                return Err(EvalError::Type(format!(
                    "{function}: namespace rows must be text[2]"
                )));
            };
            let [prefix, uri] = elements.as_slice() else {
                return Err(EvalError::Type(format!(
                    "{function}: namespace rows must contain prefix and URI"
                )));
            };
            let Some(prefix) = xml_namespace_text(function, prefix)? else {
                return Err(EvalError::Type(format!(
                    "{function}: namespace prefix cannot be NULL"
                )));
            };
            let Some(uri) = xml_namespace_text(function, uri)? else {
                return Err(EvalError::Type(format!(
                    "{function}: namespace URI cannot be NULL"
                )));
            };
            if prefix.is_empty() || uri.is_empty() {
                return Err(EvalError::Type(format!(
                    "{function}: namespace prefix and URI cannot be empty"
                )));
            }
            Ok((prefix.to_owned(), uri.to_owned()))
        })
        .collect()
}

fn xml_namespace_text<'a>(function: &str, value: &'a Value) -> Result<Option<&'a str>, EvalError> {
    match value {
        Value::Text(text) => Ok(Some(text.as_str())),
        Value::Null => Ok(None),
        other => Err(EvalError::Type(format!(
            "{function}: namespace values must be text, got {:?}",
            other.data_type()
        ))),
    }
}

fn eval_network_host(args: &[Value]) -> Result<Value, EvalError> {
    let Some(addr) = network_inet_arg("host", args)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(addr.addr().to_string()))
}

fn eval_network_family(args: &[Value]) -> Result<Value, EvalError> {
    let Some(addr) = network_inet_arg("family", args)? else {
        return Ok(Value::Null);
    };
    let family = if addr.max_prefix() == 32 { 4 } else { 6 };
    Ok(Value::Int32(family))
}

fn eval_network_masklen(args: &[Value]) -> Result<Value, EvalError> {
    let Some(addr) = network_inet_arg("masklen", args)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Int32(i32::from(addr.prefix())))
}

fn network_inet_arg(
    function: &'static str,
    args: &[Value],
) -> Result<Option<ultrasql_core::InetAddr>, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "{function}: expected 1 arg, got {}",
            args.len()
        )));
    }
    match &args[0] {
        Value::Null => Ok(None),
        Value::Network(network) => network
            .inet_addr()
            .map(Some)
            .ok_or_else(|| EvalError::Type(format!("{function}: expected inet or cidr"))),
        other => Err(EvalError::Type(format!(
            "{function}: expected inet or cidr, got {:?}",
            other.data_type()
        ))),
    }
}

fn xml_text_arg<'a>(
    function: &'static str,
    args: &'a [Value],
    idx: usize,
) -> Result<Option<&'a str>, EvalError> {
    match args.get(idx) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Text(text) | Value::Char(text) | Value::Xml(text)) => Ok(Some(text)),
        Some(other) => Err(EvalError::Type(format!(
            "{function}: expected text or xml, got {:?}",
            other.data_type()
        ))),
    }
}

fn xml_mode_arg(
    function: &'static str,
    args: &[Value],
    idx: usize,
) -> Result<Option<XmlWellFormedMode>, EvalError> {
    let Some(mode) = xml_text_arg(function, args, idx)? else {
        return Ok(None);
    };
    if mode.eq_ignore_ascii_case("content") {
        Ok(Some(XmlWellFormedMode::Content))
    } else if mode.eq_ignore_ascii_case("document") {
        Ok(Some(XmlWellFormedMode::Document))
    } else {
        Err(EvalError::Type(format!(
            "{function}: mode must be DOCUMENT or CONTENT, got {mode}"
        )))
    }
}

fn json_document_arg(
    function: &'static str,
    args: &[Value],
    idx: usize,
) -> Result<Option<JsonValue>, EvalError> {
    match args.get(idx) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Json(text) | Value::Jsonb(text) | Value::Text(text)) => {
            serde_json::from_str(text)
                .map(Some)
                .map_err(|err| EvalError::Type(format!("{function}: invalid json/jsonb: {err}")))
        }
        Some(other) => Ok(Some(sql_value_to_json(other))),
    }
}

fn json_path_arg(
    function: &'static str,
    args: &[Value],
    idx: usize,
) -> Result<Option<Vec<String>>, EvalError> {
    match args.get(idx) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Array { elements, .. }) => elements
            .iter()
            .map(|value| match value {
                Value::Null => Err(EvalError::Type(format!("{function}: path contains null"))),
                other => Ok(format_value_text(other)),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        Some(Value::Text(text) | Value::Json(text) | Value::Jsonb(text)) => {
            Ok(Some(parse_json_path_text(text)))
        }
        Some(other) => Err(EvalError::Type(format!(
            "{function}: path must be text or text[], got {:?}",
            other.data_type()
        ))),
    }
}

fn parse_json_path_text(text: &str) -> Vec<String> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|v| v.strip_suffix('}'))
        .unwrap_or(trimmed);
    if inner.is_empty() {
        return Vec::new();
    }
    inner
        .split(',')
        .map(|part| part.trim().trim_matches('"').to_owned())
        .collect()
}

fn set_json_path(
    current: &mut JsonValue,
    path: &[String],
    new_value: JsonValue,
    create_missing: bool,
) -> bool {
    let Some((key, rest)) = path.split_first() else {
        *current = new_value;
        return true;
    };
    if rest.is_empty() {
        return set_json_leaf(current, key, new_value, create_missing);
    }
    match current {
        JsonValue::Object(map) => {
            if !map.contains_key(key) {
                if !create_missing {
                    return false;
                }
                map.insert(key.clone(), JsonValue::Object(JsonMap::new()));
            }
            let Some(child) = map.get_mut(key) else {
                return false;
            };
            set_json_path(child, rest, new_value, create_missing)
        }
        _ if create_missing => {
            *current = JsonValue::Object(JsonMap::new());
            set_json_path(current, path, new_value, create_missing)
        }
        _ => false,
    }
}

fn set_json_leaf(
    current: &mut JsonValue,
    key: &str,
    new_value: JsonValue,
    create_missing: bool,
) -> bool {
    match current {
        JsonValue::Object(map) if create_missing || map.contains_key(key) => {
            map.insert(key.to_owned(), new_value);
            true
        }
        JsonValue::Object(_) => false,
        _ if create_missing => {
            let mut map = JsonMap::new();
            map.insert(key.to_owned(), new_value);
            *current = JsonValue::Object(map);
            true
        }
        _ => false,
    }
}

fn json_value_to_jsonb(value: JsonValue, function: &'static str) -> Result<Value, EvalError> {
    serde_json::to_string(&value)
        .map(Value::Jsonb)
        .map_err(|err| EvalError::Type(format!("{function}: encode failed: {err}")))
}

fn sql_value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(v) => JsonValue::Bool(*v),
        Value::Int16(v) => JsonValue::Number(JsonNumber::from(i64::from(*v))),
        Value::Int32(v) => JsonValue::Number(JsonNumber::from(i64::from(*v))),
        Value::Int64(v) => JsonValue::Number(JsonNumber::from(*v)),
        Value::Float32(v) => {
            JsonNumber::from_f64(f64::from(*v)).map_or(JsonValue::Null, JsonValue::Number)
        }
        Value::Float64(v) => JsonNumber::from_f64(*v).map_or(JsonValue::Null, JsonValue::Number),
        Value::Text(v) | Value::Char(v) => JsonValue::String(v.clone()),
        Value::Json(v) | Value::Jsonb(v) => {
            serde_json::from_str(v).unwrap_or_else(|_| JsonValue::String(v.clone()))
        }
        Value::Vector(values) | Value::HalfVec(values) => JsonValue::Array(
            values
                .iter()
                .map(|v| {
                    JsonNumber::from_f64(f64::from(*v)).map_or(JsonValue::Null, JsonValue::Number)
                })
                .collect(),
        ),
        Value::Array { elements, .. } => {
            JsonValue::Array(elements.iter().map(sql_value_to_json).collect())
        }
        Value::Record(fields) => {
            let mut object = JsonMap::new();
            for (name, value) in fields {
                object.insert(name.clone(), sql_value_to_json(value));
            }
            JsonValue::Object(object)
        }
        other => JsonValue::String(other.to_string()),
    }
}

fn quote_identifier(identifier: &str) -> String {
    if is_unquoted_identifier(identifier) && !is_reserved_identifier(identifier) {
        return identifier.to_owned();
    }
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn is_unquoted_identifier(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_lowercase()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch == '$' || ch.is_ascii_lowercase() || ch.is_ascii_digit())
}

fn is_reserved_identifier(identifier: &str) -> bool {
    matches!(
        identifier,
        "all"
            | "and"
            | "as"
            | "by"
            | "case"
            | "create"
            | "delete"
            | "drop"
            | "false"
            | "format"
            | "from"
            | "group"
            | "insert"
            | "join"
            | "not"
            | "null"
            | "or"
            | "order"
            | "select"
            | "table"
            | "true"
            | "update"
            | "user"
            | "where"
    )
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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

fn eval_now(args: &[Value], return_type: &DataType) -> Result<Value, EvalError> {
    if !args.is_empty() {
        return Err(EvalError::Type(format!(
            "now: expected 0 args, got {}",
            args.len()
        )));
    }
    let micros = current_engine_timestamp_micros();
    if matches!(return_type, DataType::Timestamp) {
        Ok(Value::Timestamp(micros))
    } else {
        Ok(Value::TimestampTz(micros))
    }
}

fn eval_current_date(args: &[Value]) -> Result<Value, EvalError> {
    if !args.is_empty() {
        return Err(EvalError::Type(format!(
            "current_date: expected 0 args, got {}",
            args.len()
        )));
    }
    let days = current_engine_timestamp_micros().div_euclid(MICROS_PER_DAY);
    Ok(Value::Date(i32::try_from(days).unwrap_or(i32::MAX)))
}

fn eval_to_timestamp(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "to_timestamp: expected 1 arg, got {}",
            args.len()
        )));
    }
    let Some(seconds) = numeric_arg("to_timestamp", args, 0)? else {
        return Ok(Value::Null);
    };
    let unix_micros = (seconds * 1_000_000.0).round();
    if !unix_micros.is_finite() || unix_micros < i64::MIN as f64 || unix_micros > i64::MAX as f64 {
        return Err(EvalError::Type(
            "to_timestamp: timestamp overflow".to_owned(),
        ));
    }
    let unix_micros_text = format!("{unix_micros:.0}");
    let unix_micros = unix_micros_text
        .parse::<i64>()
        .map_err(|_| EvalError::Type("to_timestamp: timestamp overflow".to_owned()))?;
    Ok(Value::TimestampTz(
        unix_micros
            .checked_sub(UNIX_TO_ENGINE_EPOCH_MICROS)
            .ok_or_else(|| EvalError::Type("to_timestamp: timestamp overflow".to_owned()))?,
    ))
}

fn eval_make_date(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "make_date: expected 3 args, got {}",
            args.len()
        )));
    }
    let Some(year) = int_arg("make_date", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(month) = int_arg("make_date", args, 1)? else {
        return Ok(Value::Null);
    };
    let Some(day) = int_arg("make_date", args, 2)? else {
        return Ok(Value::Null);
    };
    let year = i32::try_from(year)
        .map_err(|_| EvalError::Type("make_date: year out of range".to_owned()))?;
    let month = u32::try_from(month)
        .map_err(|_| EvalError::Type("make_date: month out of range".to_owned()))?;
    let day = u32::try_from(day)
        .map_err(|_| EvalError::Type("make_date: day out of range".to_owned()))?;
    if !(1..=12).contains(&month) || !(1..=days_in_month(year, month)).contains(&day) {
        return Err(EvalError::Type("make_date: invalid date".to_owned()));
    }
    Ok(Value::Date(days_from_civil(year, month, day)?))
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
    if matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    let unit_norm = unit.to_ascii_lowercase();
    let out_i64 = extract_datetime_part(&unit_norm, &args[1])?;
    Ok(Value::Int64(out_i64))
}

fn extract_datetime_part(unit: &str, source: &Value) -> Result<i64, EvalError> {
    match source {
        Value::Date(days) => {
            let (year, month, day) = civil_from_days(*days);
            match unit {
                "year" => Ok(i64::from(year)),
                "month" => Ok(i64::from(month)),
                "day" => Ok(i64::from(day)),
                "quarter" => Ok(i64::from((month - 1) / 3 + 1)),
                "epoch" => Ok(date_as_timestamp(*days)?
                    .checked_add(UNIX_TO_ENGINE_EPOCH_MICROS)
                    .ok_or_else(|| EvalError::Type("extract: epoch overflow".to_owned()))?
                    / 1_000_000),
                other => Err(EvalError::Type(format!(
                    "extract: unit `{other}` not implemented"
                ))),
            }
        }
        Value::Timestamp(us) | Value::TimestampTz(us) => extract_timestamp_part(unit, *us),
        Value::Time(us) => extract_time_part(unit, *us),
        Value::TimeTz { micros, .. } => extract_time_part(unit, *micros),
        Value::Interval {
            months,
            days,
            microseconds,
        } => extract_interval_part(unit, *months, *days, *microseconds),
        Value::Null => Err(EvalError::Type("extract: null source".to_owned())),
        other => Err(EvalError::Type(format!(
            "extract: source must be date/time/timestamp/interval, got {:?}",
            other.data_type()
        ))),
    }
}

fn extract_timestamp_part(unit: &str, micros: i64) -> Result<i64, EvalError> {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let time = micros.rem_euclid(MICROS_PER_DAY);
    let days_i32 = i32::try_from(days).unwrap_or(i32::MAX);
    let (year, month, day) = civil_from_days(days_i32);
    match unit {
        "year" => Ok(i64::from(year)),
        "month" => Ok(i64::from(month)),
        "day" => Ok(i64::from(day)),
        "quarter" => Ok(i64::from((month - 1) / 3 + 1)),
        "hour" => Ok(time / 3_600_000_000),
        "minute" => Ok(time % 3_600_000_000 / 60_000_000),
        "second" => Ok(time % 60_000_000 / 1_000_000),
        "epoch" => Ok(micros
            .checked_add(UNIX_TO_ENGINE_EPOCH_MICROS)
            .ok_or_else(|| EvalError::Type("extract: epoch overflow".to_owned()))?
            / 1_000_000),
        other => Err(EvalError::Type(format!(
            "extract: unit `{other}` not implemented"
        ))),
    }
}

fn extract_time_part(unit: &str, micros: i64) -> Result<i64, EvalError> {
    let time = micros.rem_euclid(MICROS_PER_DAY);
    match unit {
        "hour" => Ok(time / 3_600_000_000),
        "minute" => Ok(time % 3_600_000_000 / 60_000_000),
        "second" => Ok(time % 60_000_000 / 1_000_000),
        other => Err(EvalError::Type(format!(
            "extract: unit `{other}` not implemented"
        ))),
    }
}

fn extract_interval_part(
    unit: &str,
    months: i32,
    days: i32,
    microseconds: i64,
) -> Result<i64, EvalError> {
    match unit {
        "year" => Ok(i64::from(months / 12)),
        "month" => Ok(i64::from(months % 12)),
        "day" => Ok(i64::from(days)),
        "hour" => Ok(microseconds / 3_600_000_000),
        "minute" => Ok(microseconds % 3_600_000_000 / 60_000_000),
        "second" => Ok(microseconds % 60_000_000 / 1_000_000),
        "epoch" => Ok(i64::from(days)
            .checked_mul(86_400)
            .and_then(|base| base.checked_add(microseconds / 1_000_000))
            .ok_or_else(|| EvalError::Type("extract: interval epoch overflow".to_owned()))?),
        other => Err(EvalError::Type(format!(
            "extract: unit `{other}` not implemented"
        ))),
    }
}

fn eval_date_trunc(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "date_trunc: expected 2 args, got {}",
            args.len()
        )));
    }
    let Some(unit) = text_arg("date_trunc", args, 0)? else {
        return Ok(Value::Null);
    };
    let Some(source) = timestamp_micros_arg("date_trunc", &args[1])? else {
        return Ok(Value::Null);
    };
    let unit = unit.to_ascii_lowercase();
    let truncated = truncate_timestamp(&unit, source)?;
    Ok(Value::TimestampTz(truncated))
}

fn eval_age(args: &[Value]) -> Result<Value, EvalError> {
    if !(args.len() == 1 || args.len() == 2) {
        return Err(EvalError::Type(format!(
            "age: expected 1 or 2 args, got {}",
            args.len()
        )));
    }
    let end = if args.len() == 2 {
        timestamp_micros_arg("age", &args[0])?
    } else {
        Some(current_engine_timestamp_micros())
    };
    let Some(end) = end else {
        return Ok(Value::Null);
    };
    let Some(start) = timestamp_micros_arg("age", &args[args.len() - 1])? else {
        return Ok(Value::Null);
    };
    let delta = end
        .checked_sub(start)
        .ok_or_else(|| EvalError::Type("age: interval overflow".to_owned()))?;
    Ok(Value::Interval {
        months: 0,
        days: i32::try_from(delta.div_euclid(MICROS_PER_DAY)).unwrap_or(i32::MAX),
        microseconds: delta.rem_euclid(MICROS_PER_DAY),
    })
}

fn eval_date_bin(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 3 {
        return Err(EvalError::Type(format!(
            "date_bin: expected 3 args, got {}",
            args.len()
        )));
    }
    let stride = match &args[0] {
        Value::Interval {
            months,
            days,
            microseconds,
        } => {
            if *months != 0 {
                return Err(EvalError::Type(
                    "date_bin: month stride is not supported".to_owned(),
                ));
            }
            i64::from(*days)
                .checked_mul(MICROS_PER_DAY)
                .and_then(|base| base.checked_add(*microseconds))
                .ok_or_else(|| EvalError::Type("date_bin: stride overflow".to_owned()))?
        }
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(EvalError::Type(format!(
                "date_bin: stride must be interval, got {:?}",
                other.data_type()
            )));
        }
    };
    if stride <= 0 {
        return Err(EvalError::Type(
            "date_bin: stride must be positive".to_owned(),
        ));
    }
    let Some(source) = timestamp_micros_arg("date_bin", &args[1])? else {
        return Ok(Value::Null);
    };
    let Some(origin) = timestamp_micros_arg("date_bin", &args[2])? else {
        return Ok(Value::Null);
    };
    let offset = source
        .checked_sub(origin)
        .ok_or_else(|| EvalError::Type("date_bin: timestamp overflow".to_owned()))?;
    let bucket_offset = offset
        .div_euclid(stride)
        .checked_mul(stride)
        .ok_or_else(|| EvalError::Type("date_bin: timestamp overflow".to_owned()))?;
    Ok(Value::TimestampTz(
        origin
            .checked_add(bucket_offset)
            .ok_or_else(|| EvalError::Type("date_bin: timestamp overflow".to_owned()))?,
    ))
}

fn timestamp_micros_arg(func: &str, value: &Value) -> Result<Option<i64>, EvalError> {
    match value {
        Value::Timestamp(us) | Value::TimestampTz(us) => Ok(Some(*us)),
        Value::Date(days) => date_as_timestamp(*days).map(Some),
        Value::Null => Ok(None),
        other => Err(EvalError::Type(format!(
            "{func}: argument must be date/timestamp, got {:?}",
            other.data_type()
        ))),
    }
}

fn truncate_timestamp(unit: &str, micros: i64) -> Result<i64, EvalError> {
    match unit {
        "second" => Ok(micros.div_euclid(1_000_000) * 1_000_000),
        "minute" => Ok(micros.div_euclid(60_000_000) * 60_000_000),
        "hour" => Ok(micros.div_euclid(3_600_000_000) * 3_600_000_000),
        "day" => Ok(micros.div_euclid(MICROS_PER_DAY) * MICROS_PER_DAY),
        "month" | "year" => {
            let days = micros.div_euclid(MICROS_PER_DAY);
            let days_i32 = i32::try_from(days).unwrap_or(i32::MAX);
            let (year, month, _) = civil_from_days(days_i32);
            let truncated_days = if unit == "year" {
                days_from_civil(year, 1, 1)?
            } else {
                days_from_civil(year, u32::try_from(month).unwrap_or(1), 1)?
            };
            date_as_timestamp(truncated_days)
        }
        other => Err(EvalError::Type(format!(
            "date_trunc: unit `{other}` not implemented"
        ))),
    }
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

fn current_engine_timestamp_micros() -> i64 {
    let unix_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros())
        .unwrap_or(0);
    let unix_micros = i64::try_from(unix_micros).unwrap_or(i64::MAX);
    unix_micros.saturating_sub(UNIX_TO_ENGINE_EPOCH_MICROS)
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    reason = "civil-to-days arithmetic follows Howard-Hinnant algorithm; casts stay within bounded intermediates"
)]
fn days_from_civil(year: i32, month: u32, day: u32) -> Result<i32, EvalError> {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let month_i32 =
        i32::try_from(month).map_err(|_| EvalError::Type("date conversion overflow".to_owned()))?;
    let mp = month_i32 + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5
        + i32::try_from(day).map_err(|_| EvalError::Type("date conversion overflow".to_owned()))?
        - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days_since_1970 = era * 146_097 + doe - 719_468;
    days_since_1970
        .checked_sub(i32::try_from(UNIX_TO_ENGINE_EPOCH_DAYS).unwrap_or(10_957))
        .ok_or_else(|| EvalError::Type("date conversion overflow".to_owned()))
}

fn eval_substring(args: &[Value]) -> Result<Value, EvalError> {
    if !(2..=3).contains(&args.len()) {
        return Err(EvalError::Type(format!(
            "substring: expected 2 or 3 args, got {}",
            args.len()
        )));
    }
    let Some(s) = text_arg("substring", args, 0)? else {
        return Ok(Value::Null);
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
            Value::Money(v) => v.checked_neg().map(Value::Money).ok_or(EvalError::Overflow),
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
            Value::BitString(bits) => Ok(Value::BitString(bits.bit_not())),
            Value::Network(network) => Ok(Value::Network(network.bit_not())),
            other => Err(EvalError::Type(format!(
                "bitwise NOT (~) requires integer, bit string, or network operand, got {other:?}"
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
            | BinaryOp::NetworkContainedEq
            | BinaryOp::NetworkContainsEq
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
        BinaryOp::Add => network_or_numeric_arith(lv, rv, ArithOp::Add),
        BinaryOp::Sub => network_or_numeric_arith(lv, rv, ArithOp::Sub),
        BinaryOp::Mul => numeric_arith(lv, rv, ArithOp::Mul),
        BinaryOp::Div => numeric_arith(lv, rv, ArithOp::Div),
        BinaryOp::Mod => numeric_arith(lv, rv, ArithOp::Mod),
        BinaryOp::Pow => numeric_arith(lv, rv, ArithOp::Pow),

        // ------------------------------------------------------------------
        // Comparison
        // ------------------------------------------------------------------
        BinaryOp::Eq => value_eq(&lv, &rv),
        BinaryOp::NotEq => value_not_eq(&lv, &rv),
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
            (Value::BitString(l), Value::BitString(r)) => l
                .concat(&r)
                .map(Value::BitString)
                .ok_or(EvalError::Overflow),
            (Value::Text(l) | Value::Char(l), Value::Text(r) | Value::Char(r)) => {
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
                (
                    Value::Text(haystack) | Value::Char(haystack),
                    Value::Text(pattern) | Value::Char(pattern),
                ) => {
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
        BinaryOp::BitAnd => bitwise_or_integer(lv, rv, BitStringOp::And, |a, b| a & b),
        BinaryOp::BitOr => bitwise_or_integer(lv, rv, BitStringOp::Or, |a, b| a | b),
        BinaryOp::BitXor => bitwise_or_integer(lv, rv, BitStringOp::Xor, |a, b| a ^ b),
        BinaryOp::ShiftLeft => shift_bit_string_or_integer(lv, rv, true),
        BinaryOp::ShiftRight => shift_bit_string_or_integer(lv, rv, false),
        BinaryOp::NetworkContainedEq => network_containment(lv, rv, false, true),
        BinaryOp::NetworkContainsEq => network_containment(lv, rv, true, true),

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
        // Regex operators
        // ------------------------------------------------------------------
        BinaryOp::RegexMatch
        | BinaryOp::RegexIMatch
        | BinaryOp::RegexNotMatch
        | BinaryOp::RegexNotIMatch => {
            let case_insensitive = matches!(op, BinaryOp::RegexIMatch | BinaryOp::RegexNotIMatch);
            let negated = matches!(op, BinaryOp::RegexNotMatch | BinaryOp::RegexNotIMatch);
            match (lv, rv) {
                (
                    Value::Text(haystack) | Value::Char(haystack),
                    Value::Text(pattern) | Value::Char(pattern),
                ) => regex_match(&haystack, &pattern, case_insensitive)
                    .map(|matched| Value::Bool(matched ^ negated)),
                (l, r) => Err(EvalError::Type(format!(
                    "regex operators require Text operands, got {l:?} and {r:?}"
                ))),
            }
        }

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
        VectorDistanceOp::L2 => {
            f64::from(ultrasql_vec::kernels::vector::l2_distance_f32(left, right))
        }
        VectorDistanceOp::InnerProduct => {
            f64::from(ultrasql_vec::kernels::vector::dot_f32(left, right))
        }
        VectorDistanceOp::NegativeInnerProduct => {
            -f64::from(ultrasql_vec::kernels::vector::dot_f32(left, right))
        }
        VectorDistanceOp::Cosine => f64::from(
            ultrasql_vec::kernels::vector::cosine_distance_f32(left, right).ok_or_else(|| {
                EvalError::Type("cosine distance requires non-zero vectors".to_owned())
            })?,
        ),
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

fn text_search_payload_arg<'a>(
    func_name: &str,
    args: &'a [Value],
) -> Result<Option<&'a str>, EvalError> {
    let payload = match args.len() {
        1 => &args[0],
        2 => &args[1],
        n => {
            return Err(EvalError::Type(format!(
                "{func_name}: expected 1 or 2 args, got {n}"
            )));
        }
    };
    match payload {
        Value::Null => Ok(None),
        Value::Text(text) | Value::Char(text) => Ok(Some(text.as_str())),
        other => Err(EvalError::Type(format!(
            "{func_name}: text argument required, got {:?}",
            other.data_type()
        ))),
    }
}

fn tsquery_payload_arg<'a>(
    func_name: &str,
    args: &'a [Value],
) -> Result<Option<&'a str>, EvalError> {
    let [payload] = args else {
        return Err(EvalError::Type(format!(
            "{func_name}: expected 1 arg, got {}",
            args.len()
        )));
    };
    match payload {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.as_str())),
        other => Err(EvalError::Type(format!(
            "{func_name}: text-backed TSQUERY required, got {:?}",
            other.data_type()
        ))),
    }
}

fn eval_to_tsvector(args: &[Value]) -> Result<Value, EvalError> {
    let Some(text) = text_search_payload_arg("to_tsvector", args)? else {
        return Ok(Value::Null);
    };
    let lexemes = text_search_terms(text)
        .into_iter()
        .enumerate()
        .map(|(idx, term)| format!("{term}:{}", idx + 1))
        .collect::<Vec<_>>();
    Ok(Value::Text(lexemes.join(" ")))
}

fn eval_plain_tsquery(func_name: &str, args: &[Value]) -> Result<Value, EvalError> {
    let Some(text) = text_search_payload_arg(func_name, args)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(text_search_terms(text).join(" & ")))
}

fn eval_ts_rank(func_name: &str, args: &[Value]) -> Result<Value, EvalError> {
    let (vector, query) = match args.len() {
        2 => (&args[0], &args[1]),
        n => {
            return Err(EvalError::Type(format!(
                "{func_name}: expected 2 args, got {n}"
            )));
        }
    };
    let (Value::Text(vector), Value::Text(query)) = (vector, query) else {
        if matches!(vector, Value::Null) || matches!(query, Value::Null) {
            return Ok(Value::Null);
        }
        return Err(EvalError::Type(format!(
            "{func_name}: text-backed TSVECTOR and TSQUERY required, got {:?} and {:?}",
            vector.data_type(),
            query.data_type()
        )));
    };
    let vector_terms = text_search_terms(vector);
    let query_terms = text_search_terms(query);
    if query_terms.is_empty() {
        return Ok(Value::Float64(0.0));
    }
    let matched = query_terms
        .iter()
        .filter(|term| vector_terms.contains(term))
        .count();
    let matched = u32::try_from(matched).map_or(f64::from(u32::MAX), f64::from);
    let total = u32::try_from(query_terms.len()).map_or(f64::from(u32::MAX), f64::from);
    Ok(Value::Float64(matched / total))
}

fn eval_ts_headline(args: &[Value]) -> Result<Value, EvalError> {
    let (document, query) = match args.len() {
        2 => (&args[0], &args[1]),
        3 => (&args[1], &args[2]),
        n => {
            return Err(EvalError::Type(format!(
                "ts_headline: expected 2 or 3 args, got {n}"
            )));
        }
    };
    let (Value::Text(document) | Value::Char(document), Value::Text(query)) = (document, query)
    else {
        if matches!(document, Value::Null) || matches!(query, Value::Null) {
            return Ok(Value::Null);
        }
        return Err(EvalError::Type(format!(
            "ts_headline: text document and text-backed TSQUERY required, got {:?} and {:?}",
            document.data_type(),
            query.data_type()
        )));
    };
    let terms = text_search_terms(query);
    Ok(Value::Text(highlight_text_search_terms(document, &terms)))
}

fn eval_numnode(args: &[Value]) -> Result<Value, EvalError> {
    let Some(query) = tsquery_payload_arg("numnode", args)? else {
        return Ok(Value::Null);
    };
    let node_count = i32::try_from(text_search_terms(query).len())
        .map_err(|_| EvalError::Type("numnode: query node count overflow".to_owned()))?;
    Ok(Value::Int32(node_count))
}

fn eval_querytree(args: &[Value]) -> Result<Value, EvalError> {
    let Some(query) = tsquery_payload_arg("querytree", args)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(text_search_terms(query).join(" & ")))
}

fn highlight_text_search_terms(document: &str, terms: &[String]) -> String {
    let mut output = String::with_capacity(document.len());
    let mut token_start = None;
    for (idx, ch) in document.char_indices() {
        if ch.is_alphanumeric() {
            token_start.get_or_insert(idx);
        } else if let Some(start) = token_start.take() {
            push_headline_token(&mut output, &document[start..idx], terms);
            output.push(ch);
        } else {
            output.push(ch);
        }
    }
    if let Some(start) = token_start {
        push_headline_token(&mut output, &document[start..], terms);
    }
    output
}

fn push_headline_token(output: &mut String, token: &str, terms: &[String]) {
    if terms
        .iter()
        .any(|term| term.as_str() == token.to_ascii_lowercase())
    {
        output.push_str("<b>");
        output.push_str(token);
        output.push_str("</b>");
    } else {
        output.push_str(token);
    }
}

fn overlaps_values(left: &Value, right: &Value) -> Option<bool> {
    match (left, right) {
        (Value::Range(l), Value::Range(r)) => Some(l.overlaps(r)),
        (Value::Geometry(l), Value::Geometry(r)) => Some(l.overlaps(r)),
        (Value::Network(l), Value::Network(r)) => Some(l.inet_addr()?.overlaps(r.inet_addr()?)),
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
            "JSON access requires JSON/JSONB, got {:?}",
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
    let json = json_text(left).ok_or_else(|| {
        EvalError::Type(format!("? requires JSON/JSONB, got {:?}", left.data_type()))
    })?;
    let key = json_key_text(right)?;
    Ok(json_object_value(json, &key).is_some())
}

fn json_text(value: &Value) -> Option<&str> {
    match value {
        Value::Json(text) | Value::Jsonb(text) | Value::Text(text) => Some(text.as_str()),
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

fn network_or_numeric_arith(lv: Value, rv: Value, op: ArithOp) -> Result<Value, EvalError> {
    match (lv, rv, op) {
        (Value::Network(network), value, ArithOp::Add) => {
            let delta = integer_delta(&value)?;
            let addr = network.inet_addr().ok_or_else(|| {
                EvalError::Type(format!(
                    "network arithmetic requires inet/cidr, got {network:?}"
                ))
            })?;
            addr.checked_add(delta)
                .map(ultrasql_core::NetworkValue::Inet)
                .map(Value::Network)
                .ok_or(EvalError::Overflow)
        }
        (value, Value::Network(network), ArithOp::Add) => {
            let delta = integer_delta(&value)?;
            let addr = network.inet_addr().ok_or_else(|| {
                EvalError::Type(format!(
                    "network arithmetic requires inet/cidr, got {network:?}"
                ))
            })?;
            addr.checked_add(delta)
                .map(ultrasql_core::NetworkValue::Inet)
                .map(Value::Network)
                .ok_or(EvalError::Overflow)
        }
        (Value::Network(left), Value::Network(right), ArithOp::Sub) => {
            let left = left.inet_addr().ok_or_else(|| {
                EvalError::Type(format!(
                    "network subtraction requires inet/cidr, got {left:?}"
                ))
            })?;
            let right = right.inet_addr().ok_or_else(|| {
                EvalError::Type(format!(
                    "network subtraction requires inet/cidr, got {right:?}"
                ))
            })?;
            left.checked_sub_addr(right)
                .map(Value::Int64)
                .ok_or(EvalError::Overflow)
        }
        (Value::Network(network), value, ArithOp::Sub) => {
            let delta = integer_delta(&value)?;
            let delta = delta.checked_neg().ok_or(EvalError::Overflow)?;
            let addr = network.inet_addr().ok_or_else(|| {
                EvalError::Type(format!(
                    "network arithmetic requires inet/cidr, got {network:?}"
                ))
            })?;
            addr.checked_add(delta)
                .map(ultrasql_core::NetworkValue::Inet)
                .map(Value::Network)
                .ok_or(EvalError::Overflow)
        }
        (left, right, op) => numeric_arith(left, right, op),
    }
}

fn integer_delta(value: &Value) -> Result<i64, EvalError> {
    match value {
        Value::Int16(v) => Ok(i64::from(*v)),
        Value::Int32(v) => Ok(i64::from(*v)),
        Value::Int64(v) => Ok(*v),
        other => Err(EvalError::Type(format!(
            "network arithmetic requires integer offset, got {other:?}"
        ))),
    }
}

/// Evaluate an arithmetic binary operation.
///
/// Integer overflow returns [`EvalError::Overflow`]. Division by zero
/// returns [`EvalError::DivByZero`]. Type mismatches return
/// [`EvalError::Type`]. Floating-point overflow produces `f64::INFINITY`
/// (IEEE 754 semantics, consistent with PostgreSQL).
fn numeric_arith(lv: Value, rv: Value, op: ArithOp) -> Result<Value, EvalError> {
    if let Some(value) = money_arith(&lv, &rv, op)? {
        return Ok(value);
    }

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

fn money_arith(left: &Value, right: &Value, op: ArithOp) -> Result<Option<Value>, EvalError> {
    match (left, right, op) {
        (Value::Money(l), Value::Money(r), ArithOp::Add) => l
            .checked_add(*r)
            .map(Value::Money)
            .map(Some)
            .ok_or(EvalError::Overflow),
        (Value::Money(l), Value::Money(r), ArithOp::Sub) => l
            .checked_sub(*r)
            .map(Value::Money)
            .map(Some)
            .ok_or(EvalError::Overflow),
        (Value::Money(l), Value::Money(r), ArithOp::Div) => money_ratio(*l, *r).map(Some),
        (Value::Money(cents), Value::Int16(divisor), ArithOp::Div) => {
            money_integer_div(*cents, i64::from(*divisor)).map(Some)
        }
        (Value::Money(cents), Value::Int32(divisor), ArithOp::Div) => {
            money_integer_div(*cents, i64::from(*divisor)).map(Some)
        }
        (Value::Money(cents), Value::Int64(divisor), ArithOp::Div) => {
            money_integer_div(*cents, *divisor).map(Some)
        }
        (Value::Money(cents), Value::Float32(divisor), ArithOp::Div) => {
            money_float_div(*cents, f64::from(*divisor)).map(Some)
        }
        (Value::Money(cents), Value::Float64(divisor), ArithOp::Div) => {
            money_float_div(*cents, *divisor).map(Some)
        }
        (Value::Money(cents), Value::Int16(multiplier), ArithOp::Mul) => {
            money_integer_mul(*cents, i64::from(*multiplier)).map(Some)
        }
        (Value::Money(cents), Value::Int32(multiplier), ArithOp::Mul) => {
            money_integer_mul(*cents, i64::from(*multiplier)).map(Some)
        }
        (Value::Money(cents), Value::Int64(multiplier), ArithOp::Mul) => {
            money_integer_mul(*cents, *multiplier).map(Some)
        }
        (Value::Int16(multiplier), Value::Money(cents), ArithOp::Mul) => {
            money_integer_mul(*cents, i64::from(*multiplier)).map(Some)
        }
        (Value::Int32(multiplier), Value::Money(cents), ArithOp::Mul) => {
            money_integer_mul(*cents, i64::from(*multiplier)).map(Some)
        }
        (Value::Int64(multiplier), Value::Money(cents), ArithOp::Mul) => {
            money_integer_mul(*cents, *multiplier).map(Some)
        }
        (Value::Money(cents), Value::Float32(multiplier), ArithOp::Mul) => {
            money_float_mul(*cents, f64::from(*multiplier)).map(Some)
        }
        (Value::Money(cents), Value::Float64(multiplier), ArithOp::Mul) => {
            money_float_mul(*cents, *multiplier).map(Some)
        }
        (Value::Float32(multiplier), Value::Money(cents), ArithOp::Mul) => {
            money_float_mul(*cents, f64::from(*multiplier)).map(Some)
        }
        (Value::Float64(multiplier), Value::Money(cents), ArithOp::Mul) => {
            money_float_mul(*cents, *multiplier).map(Some)
        }
        (Value::Money(_), Value::Money(_), _) => Err(EvalError::Type(
            "money arithmetic supports addition, subtraction, multiplication, and division"
                .to_owned(),
        )),
        _ => Ok(None),
    }
}

fn money_ratio(left_cents: i64, right_cents: i64) -> Result<Value, EvalError> {
    if right_cents == 0 {
        return Err(EvalError::DivByZero);
    }
    Ok(Value::Float64(
        cents_to_f64(left_cents) / cents_to_f64(right_cents),
    ))
}

fn money_integer_div(cents: i64, divisor: i64) -> Result<Value, EvalError> {
    if divisor == 0 {
        return Err(EvalError::DivByZero);
    }
    cents
        .checked_div(divisor)
        .map(Value::Money)
        .ok_or(EvalError::Overflow)
}

fn money_integer_mul(cents: i64, multiplier: i64) -> Result<Value, EvalError> {
    cents
        .checked_mul(multiplier)
        .map(Value::Money)
        .ok_or(EvalError::Overflow)
}

fn money_float_mul(cents: i64, multiplier: f64) -> Result<Value, EvalError> {
    rounded_money_from_f64(cents_to_f64(cents) * multiplier)
}

fn money_float_div(cents: i64, divisor: f64) -> Result<Value, EvalError> {
    if divisor == 0.0 {
        return Err(EvalError::DivByZero);
    }
    rounded_money_from_f64(cents_to_f64(cents) / divisor)
}

fn rounded_money_from_f64(cents: f64) -> Result<Value, EvalError> {
    cents
        .round()
        .to_i64()
        .map(Value::Money)
        .ok_or(EvalError::Overflow)
}

fn cents_to_f64(cents: i64) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let value = cents as f64;
    value
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
            let denominator = i128::from(right_value);
            let mut quotient = numerator / denominator;
            let remainder = numerator % denominator;
            if remainder != 0 {
                let twice_remainder = remainder
                    .checked_abs()
                    .and_then(|r| r.checked_mul(2))
                    .ok_or(EvalError::Overflow)?;
                let divisor = denominator.checked_abs().ok_or(EvalError::Overflow)?;
                if twice_remainder >= divisor {
                    let adjustment = if (numerator >= 0) == (denominator >= 0) {
                        1
                    } else {
                        -1
                    };
                    quotient = quotient
                        .checked_add(adjustment)
                        .ok_or(EvalError::Overflow)?;
                }
            }
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

#[derive(Clone, Copy)]
enum BitStringOp {
    And,
    Or,
    Xor,
}

fn bit_string_arg<'a>(
    function: &str,
    args: &'a [Value],
    idx: usize,
) -> Result<Option<&'a ultrasql_core::BitString>, EvalError> {
    match args.get(idx) {
        Some(Value::BitString(bits)) => Ok(Some(bits)),
        Some(Value::Null) => Ok(None),
        Some(other) => Err(EvalError::Type(format!(
            "{function}: argument {} must be bit string, got {:?}",
            idx + 1,
            other.data_type()
        ))),
        None => Err(EvalError::Type(format!(
            "{function}: missing argument {}",
            idx + 1
        ))),
    }
}

fn integer_arg_as_usize(
    function: &str,
    args: &[Value],
    idx: usize,
) -> Result<Option<usize>, EvalError> {
    let value = match args.get(idx) {
        Some(Value::Int16(v)) => i64::from(*v),
        Some(Value::Int32(v)) => i64::from(*v),
        Some(Value::Int64(v)) => *v,
        Some(Value::Null) => return Ok(None),
        Some(other) => {
            return Err(EvalError::Type(format!(
                "{function}: argument {} must be integer, got {:?}",
                idx + 1,
                other.data_type()
            )));
        }
        None => {
            return Err(EvalError::Type(format!(
                "{function}: missing argument {}",
                idx + 1
            )));
        }
    };
    if value < 0 {
        return Err(EvalError::Type(format!(
            "{function}: argument {} must be non-negative",
            idx + 1
        )));
    }
    usize::try_from(value)
        .map(Some)
        .map_err(|_| EvalError::Type(format!("{function}: integer argument out of range")))
}

fn bitwise_or_integer(
    lv: Value,
    rv: Value,
    bit_op: BitStringOp,
    int_op: impl Fn(i64, i64) -> i64,
) -> Result<Value, EvalError> {
    match (lv, rv) {
        (Value::BitString(left), Value::BitString(right)) => {
            let result = match bit_op {
                BitStringOp::And => left.bit_and(&right),
                BitStringOp::Or => left.bit_or(&right),
                BitStringOp::Xor => left.bit_xor(&right),
            };
            result.map(Value::BitString).ok_or_else(|| {
                EvalError::Type("bitwise operation requires equal-length bit strings".to_owned())
            })
        }
        (Value::Network(left), Value::Network(right)) => left
            .bitwise(right, |a, b| match bit_op {
                BitStringOp::And => a & b,
                BitStringOp::Or => a | b,
                BitStringOp::Xor => a ^ b,
            })
            .map(Value::Network)
            .ok_or_else(|| {
                EvalError::Type(
                    "network bitwise operation requires matching address families".to_owned(),
                )
            }),
        (left, right) => integer_bitwise(left, right, int_op),
    }
}

fn shift_bit_string_or_integer(lv: Value, rv: Value, left_shift: bool) -> Result<Value, EvalError> {
    match (&lv, &rv) {
        (Value::Network(_), Value::Network(_)) => network_containment(lv, rv, !left_shift, false),
        (Value::BitString(bits), _) => {
            let amount = shift_amount(&rv)?;
            if left_shift {
                bits.shift_left(amount)
            } else {
                bits.shift_right(amount)
            }
            .map(Value::BitString)
            .ok_or(EvalError::Overflow)
        }
        _ => {
            if left_shift {
                integer_bitwise(lv, rv, |a, b| a << (b & 63))
            } else {
                integer_bitwise(lv, rv, |a, b| a >> (b & 63))
            }
        }
    }
}

fn network_containment(
    lv: Value,
    rv: Value,
    left_contains_right: bool,
    allow_equal: bool,
) -> Result<Value, EvalError> {
    let (Value::Network(left), Value::Network(right)) = (lv, rv) else {
        return Err(EvalError::Type(
            "network containment requires inet/cidr operands".to_owned(),
        ));
    };
    let left = left
        .inet_addr()
        .ok_or_else(|| EvalError::Type("network containment requires inet/cidr".to_owned()))?;
    let right = right
        .inet_addr()
        .ok_or_else(|| EvalError::Type("network containment requires inet/cidr".to_owned()))?;
    let result = if left_contains_right {
        if allow_equal {
            left.contains_or_equal(right)
        } else {
            left.contains_strict(right)
        }
    } else if allow_equal {
        right.contains_or_equal(left)
    } else {
        right.contains_strict(left)
    };
    Ok(Value::Bool(result))
}

fn shift_amount(value: &Value) -> Result<usize, EvalError> {
    let raw = match value {
        Value::Int16(v) => i64::from(*v),
        Value::Int32(v) => i64::from(*v),
        Value::Int64(v) => *v,
        other => {
            return Err(EvalError::Type(format!(
                "bit shift requires integer shift count, got {other:?}"
            )));
        }
    };
    if raw < 0 {
        return Err(EvalError::Type(
            "bit shift requires non-negative shift count".to_owned(),
        ));
    }
    usize::try_from(raw).map_err(|_| EvalError::Overflow)
}

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

fn value_eq(lv: &Value, rv: &Value) -> Result<Value, EvalError> {
    sql_eq_3vl(lv, rv).map(bool_or_null)
}

fn value_not_eq(lv: &Value, rv: &Value) -> Result<Value, EvalError> {
    sql_eq_3vl(lv, rv).map(|result| bool_or_null(result.map(|eq| !eq)))
}

fn bool_or_null(value: Option<bool>) -> Value {
    value.map_or(Value::Null, Value::Bool)
}

fn sql_eq_3vl(lv: &Value, rv: &Value) -> Result<Option<bool>, EvalError> {
    if matches!((lv, rv), (Value::Null, _) | (_, Value::Null)) {
        return Ok(None);
    }
    match (lv, rv) {
        (Value::Record(left), Value::Record(right)) => record_eq_3vl(left, right),
        (Value::Record(_), _) | (_, Value::Record(_)) => Err(EvalError::Type(format!(
            "record comparison type mismatch: {lv:?} and {rv:?}"
        ))),
        _ => compare_values(lv, rv).map(|ordering| Some(ordering == std::cmp::Ordering::Equal)),
    }
}

fn record_eq_3vl(
    left: &[(String, Value)],
    right: &[(String, Value)],
) -> Result<Option<bool>, EvalError> {
    if left.len() != right.len() {
        return Err(EvalError::Type(format!(
            "record arity mismatch: {} and {}",
            left.len(),
            right.len()
        )));
    }

    let mut saw_unknown = false;
    for ((_, left_value), (_, right_value)) in left.iter().zip(right.iter()) {
        match sql_eq_3vl(left_value, right_value)? {
            Some(true) => {}
            Some(false) => return Ok(Some(false)),
            None => saw_unknown = true,
        }
    }

    Ok((!saw_unknown).then_some(true))
}

/// Total ordering for Value pairs of the same type.
///
/// Only types that have a natural total order are supported. Mismatched
/// types return [`EvalError::Type`].
fn compare_values(lv: &Value, rv: &Value) -> Result<std::cmp::Ordering, EvalError> {
    if let (Some(left), Some(right)) = (oid_alias_value(lv), oid_alias_value(rv)) {
        return Ok(left.cmp(&right));
    }
    if let Some(ordering) = compare_oid_alias_with_integer(lv, rv) {
        return Ok(ordering);
    }

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
        return Ok(compare_decimal_values(
            left_value,
            left_scale,
            right_value,
            right_scale,
        ));
    }

    match (lv, rv) {
        (Value::Int16(l), Value::Int16(r)) => Ok(l.cmp(r)),
        (Value::Int32(l), Value::Int32(r)) => Ok(l.cmp(r)),
        (Value::Int64(l), Value::Int64(r)) => Ok(l.cmp(r)),
        (Value::Oid(l), Value::Oid(r))
        | (Value::RegClass(l), Value::RegClass(r))
        | (Value::RegType(l), Value::RegType(r)) => Ok(l.cmp(r)),
        (Value::PgLsn(l), Value::PgLsn(r)) => Ok(l.cmp(r)),
        (Value::Float32(l), Value::Float32(r)) => l
            .partial_cmp(r)
            .ok_or_else(|| EvalError::Type("comparison of NaN is undefined".to_owned())),
        (Value::Float64(l), Value::Float64(r)) => l
            .partial_cmp(r)
            .ok_or_else(|| EvalError::Type("comparison of NaN is undefined".to_owned())),
        (Value::Text(l), Value::Text(r)) => Ok(l.cmp(r)),
        (Value::Char(l), Value::Char(r)) => {
            Ok(bpchar_semantic_text(l).cmp(bpchar_semantic_text(r)))
        }
        (Value::Char(l), Value::Text(r)) => Ok(bpchar_semantic_text(l).cmp(r)),
        (Value::Text(l), Value::Char(r)) => Ok(l.as_str().cmp(bpchar_semantic_text(r))),
        (Value::BitString(l), Value::BitString(r)) => Ok(l.to_bit_text().cmp(&r.to_bit_text())),
        (Value::Network(l), Value::Network(r)) => (*l)
            .cmp_network(*r)
            .ok_or_else(|| EvalError::Type("network comparison type mismatch".to_owned())),
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
        ) => Ok(compare_decimal_values(*lv, *ls, *rv, *rs)),
        (Value::Date(l), Value::Date(r)) => Ok(l.cmp(r)),
        (Value::Time(l), Value::Time(r)) => Ok(l.cmp(r)),
        (
            Value::TimeTz {
                micros: lm,
                offset_seconds: lo,
            },
            Value::TimeTz {
                micros: rm,
                offset_seconds: ro,
            },
        ) => Ok(timetz_utc_micros(*lm, *lo).cmp(&timetz_utc_micros(*rm, *ro))),
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

fn oid_alias_value(value: &Value) -> Option<Oid> {
    match value {
        Value::Oid(oid) | Value::RegClass(oid) | Value::RegType(oid) => Some(*oid),
        _ => None,
    }
}

fn compare_oid_alias_with_integer(lv: &Value, rv: &Value) -> Option<std::cmp::Ordering> {
    if let (Some(left), Some(right)) = (oid_alias_value(lv), integer_value_i128(rv)) {
        return Some(i128::from(left.raw()).cmp(&right));
    }
    if let (Some(left), Some(right)) = (integer_value_i128(lv), oid_alias_value(rv)) {
        return Some(left.cmp(&i128::from(right.raw())));
    }
    None
}

fn integer_value_i128(value: &Value) -> Option<i128> {
    match value {
        Value::Int16(v) => Some(i128::from(*v)),
        Value::Int32(v) => Some(i128::from(*v)),
        Value::Int64(v) => Some(i128::from(*v)),
        _ => None,
    }
}

fn oid_or_integer_arg(value: &Value) -> Option<u32> {
    if let Some(oid) = oid_alias_value(value) {
        return Some(oid.raw());
    }
    match value {
        Value::Int16(v) => u32::try_from(i64::from(*v)).ok(),
        Value::Int32(v) => u32::try_from(i64::from(*v)).ok(),
        Value::Int64(v) => u32::try_from(*v).ok(),
        _ => None,
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
) -> std::cmp::Ordering {
    match (left_value.cmp(&0), right_value.cmp(&0)) {
        (std::cmp::Ordering::Equal, std::cmp::Ordering::Equal) => {
            return std::cmp::Ordering::Equal;
        }
        (std::cmp::Ordering::Equal, std::cmp::Ordering::Less)
        | (std::cmp::Ordering::Greater, std::cmp::Ordering::Less) => {
            return std::cmp::Ordering::Greater;
        }
        (std::cmp::Ordering::Less, std::cmp::Ordering::Equal)
        | (std::cmp::Ordering::Less, std::cmp::Ordering::Greater) => {
            return std::cmp::Ordering::Less;
        }
        _ => {}
    }

    let left = DecimalMagnitude::new(left_value, left_scale);
    let right = DecimalMagnitude::new(right_value, right_scale);
    let magnitude_order = left.cmp_abs(&right);
    if left.negative {
        magnitude_order.reverse()
    } else {
        magnitude_order
    }
}

#[derive(Debug)]
struct DecimalMagnitude {
    negative: bool,
    digits: String,
    integer_digits: i64,
}

impl DecimalMagnitude {
    fn new(value: i64, scale: i32) -> Self {
        let mut magnitude = i128::from(value);
        let negative = magnitude < 0;
        if negative {
            magnitude = -magnitude;
        }
        let mut scale = i64::from(scale);
        while magnitude != 0 && magnitude % 10 == 0 {
            magnitude /= 10;
            scale = scale.saturating_sub(1);
        }
        let digits = magnitude.to_string();
        let digit_count = i64::try_from(digits.len()).unwrap_or(i64::MAX);
        Self {
            negative,
            digits,
            integer_digits: digit_count.saturating_sub(scale),
        }
    }

    fn cmp_abs(&self, other: &Self) -> std::cmp::Ordering {
        match self.integer_digits.cmp(&other.integer_digits) {
            std::cmp::Ordering::Equal => {}
            non_equal => return non_equal,
        }

        let max_len = self.digits.len().max(other.digits.len());
        let left = self.digits.as_bytes();
        let right = other.digits.as_bytes();
        for idx in 0..max_len {
            let l = left.get(idx).copied().unwrap_or(b'0');
            let r = right.get(idx).copied().unwrap_or(b'0');
            match l.cmp(&r) {
                std::cmp::Ordering::Equal => {}
                non_equal => return non_equal,
            }
        }
        std::cmp::Ordering::Equal
    }
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

fn regex_match(haystack: &str, pattern: &str, case_insensitive: bool) -> Result<bool, EvalError> {
    regex::RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .build()
        .map(|regex| regex.is_match(haystack))
        .map_err(|err| EvalError::Type(format!("regex operator: invalid pattern: {err}")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use ultrasql_core::{BitString, DataType, Field, NetworkValue, Oid, Schema, Value};
    use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr, UnaryOp};

    use super::{Eval, EvalError, eval_function_call};

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

    fn lit_money(cents: i64) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Money(cents),
            data_type: DataType::Money,
        }
    }

    fn lit_text(s: &str) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Text(s.to_owned()),
            data_type: DataType::Text { max_len: None },
        }
    }

    fn lit_char(s: &str, len: Option<u32>) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Char(s.to_owned()),
            data_type: DataType::Char { len },
        }
    }

    fn lit_jsonb(s: &str) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Jsonb(s.to_owned()),
            data_type: DataType::Jsonb,
        }
    }

    fn text_array_value(items: &[&str]) -> Value {
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements: items
                .iter()
                .map(|item| Value::Text((*item).to_owned()))
                .collect(),
        }
    }

    fn lit_text_array(items: &[&str]) -> ScalarExpr {
        ScalarExpr::Literal {
            value: text_array_value(items),
            data_type: DataType::Array(Box::new(DataType::Text { max_len: None })),
        }
    }

    fn lit_record(values: Vec<Value>) -> ScalarExpr {
        ScalarExpr::Literal {
            data_type: DataType::Record(
                values
                    .iter()
                    .enumerate()
                    .map(|(idx, value)| (format!("f{}", idx + 1), value.data_type()))
                    .collect(),
            ),
            value: Value::Record(
                values
                    .into_iter()
                    .enumerate()
                    .map(|(idx, value)| (format!("f{}", idx + 1), value))
                    .collect(),
            ),
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

    fn eval_fn(name: &str, args: Vec<Value>) -> Value {
        eval_function_call(name, &args, &DataType::Null).expect("function eval")
    }

    fn eval_fn_err(name: &str, args: Vec<Value>) -> String {
        eval_function_call(name, &args, &DataType::Null)
            .expect_err("function error")
            .to_string()
    }

    fn inet(text: &str) -> Value {
        Value::Network(NetworkValue::parse_for_type(&DataType::Inet, text).expect("inet"))
    }

    fn assert_float_close(value: Value, expected: f64) {
        let Value::Float64(actual) = value else {
            panic!("expected float64, got {value:?}");
        };
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    fn one_col_empty_plan() -> LogicalPlan {
        LogicalPlan::Empty {
            schema: Schema::new([Field::required("x", DataType::Int32)]).expect("schema"),
        }
    }

    #[test]
    fn subquery_and_outer_scope_guards_return_unsupported() {
        let subplan = one_col_empty_plan();
        let exprs = vec![
            ScalarExpr::OuterColumn {
                name: "x".into(),
                frame_depth: 1,
                column_index: 0,
                data_type: DataType::Int32,
            },
            ScalarExpr::ScalarSubquery {
                subplan: Box::new(subplan.clone()),
                correlated: false,
                data_type: DataType::Int32,
            },
            ScalarExpr::Exists {
                subplan: Box::new(subplan.clone()),
                negated: false,
                correlated: false,
            },
            ScalarExpr::InSubquery {
                expr: Box::new(lit_i32(1)),
                subplan: Box::new(subplan),
                negated: false,
                correlated: false,
                data_type: DataType::Int32,
            },
        ];

        for expr in exprs {
            let err = Eval::new(expr).eval(&[]).expect_err("guard must reject");
            assert!(matches!(err, EvalError::Unsupported(_)), "got {err}");
        }
    }

    #[test]
    fn null_helpers_extrema_xml_and_unknown_function_paths() {
        assert_eq!(
            eval_fn("coalesce", vec![Value::Null, Value::Text("x".into())]),
            Value::Text("x".into())
        );
        assert_eq!(eval_fn("coalesce", vec![Value::Null]), Value::Null);

        assert_eq!(
            eval_fn("ifnull", vec![Value::Null, Value::Int32(7)]),
            Value::Int32(7)
        );
        assert_eq!(
            eval_fn(
                "nvl",
                vec![Value::Text("a".into()), Value::Text("b".into())]
            ),
            Value::Text("a".into())
        );
        assert!(eval_fn_err("ifnull", vec![Value::Null]).contains("expected 2 args"));

        assert_eq!(
            eval_fn("nullif", vec![Value::Int32(7), Value::Int32(7)]),
            Value::Null
        );
        assert_eq!(
            eval_fn("nullif", vec![Value::Int32(7), Value::Int32(8)]),
            Value::Int32(7)
        );
        assert_eq!(
            eval_fn("nullif", vec![Value::Null, Value::Int32(8)]),
            Value::Null
        );
        assert!(eval_fn_err("nullif", vec![Value::Int32(1)]).contains("expected 2 args"));

        assert_eq!(
            eval_fn("least", vec![Value::Null, Value::Int32(8), Value::Int32(3)]),
            Value::Int32(3)
        );
        assert_eq!(
            eval_fn("greatest", vec![Value::Int32(8), Value::Int32(3)]),
            Value::Int32(8)
        );
        assert_eq!(eval_fn("least", vec![Value::Null]), Value::Null);
        assert_eq!(
            eval_fn("min", vec![Value::Int32(1), Value::Null]),
            Value::Null
        );
        assert!(eval_fn_err("greatest", vec![]).contains("expected at least 1 arg"));

        assert_eq!(
            eval_fn("xml_is_well_formed", vec![Value::Text("<a/><b/>".into())]),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn(
                "xml_is_well_formed_document",
                vec![Value::Text("<a/><b/>".into())]
            ),
            Value::Bool(false)
        );
        assert_eq!(
            eval_fn("xml_is_well_formed_content", vec![Value::Null]),
            Value::Null
        );
        assert!(eval_fn_err("xml_is_well_formed", vec![]).contains("expected 1 arg"));
        assert_eq!(
            eval_fn(
                "xmlparse",
                vec![
                    Value::Text("document".into()),
                    Value::Text("<root/>".into())
                ]
            ),
            Value::Xml("<root/>".into())
        );
        assert_eq!(
            eval_fn(
                "xmlparse",
                vec![
                    Value::Text("content".into()),
                    Value::Text("<a/><b/>".into())
                ]
            ),
            Value::Xml("<a/><b/>".into())
        );
        assert!(
            eval_fn_err(
                "xmlparse",
                vec![
                    Value::Text("document".into()),
                    Value::Text("<a/><b/>".into())
                ]
            )
            .contains("well-formed XML document")
        );
        assert_eq!(
            eval_fn(
                "xmlserialize",
                vec![
                    Value::Text("content".into()),
                    Value::Xml("<root/>".into()),
                    Value::Text("text".into())
                ]
            ),
            Value::Text("<root/>".into())
        );

        assert!(eval_fn_err("does_not_exist", vec![]).contains("function not implemented"));
    }

    #[test]
    fn catalog_edge_cases_cover_remaining_error_and_oid_paths() {
        for (oid, name) in [
            (21, "smallint"),
            (26, "oid"),
            (700, "real"),
            (701, "double precision"),
            (790, "money"),
            (114, "json"),
            (142, "xml"),
            (650, "cidr"),
            (829, "macaddr"),
            (869, "inet"),
            (1042, "character"),
            (1082, "date"),
            (1083, "time without time zone"),
            (1114, "timestamp without time zone"),
            (1184, "timestamp with time zone"),
            (1266, "time with time zone"),
            (1560, "bit"),
            (1562, "bit varying"),
            (3220, "pg_lsn"),
            (3614, "tsvector"),
            (3615, "tsquery"),
            (2205, "regclass"),
            (2206, "regtype"),
        ] {
            assert_eq!(
                eval_fn("format_type", vec![Value::Oid(Oid::new(oid)), Value::Null]),
                Value::Text(name.into())
            );
        }

        assert!(eval_fn_err("pg_typeof", vec![]).contains("expected 1 arg"));
        assert!(eval_fn_err("pg_get_indexdef", vec![]).contains("expected 1 to 3 args"));
        assert!(eval_fn_err("pg_get_constraintdef", vec![]).contains("expected 1 or 2 args"));
        assert_eq!(
            eval_fn("pg_get_constraintdef", vec![Value::Oid(Oid::new(0))]),
            Value::Null
        );
        assert!(eval_fn_err("pg_get_statisticsobjdef_columns", vec![]).contains("expected 1 arg"));
        assert!(eval_fn_err("pg_get_function_result", vec![]).contains("expected 1 arg"));
        assert!(eval_fn_err("pg_get_function_arguments", vec![]).contains("expected 1 arg"));
        assert!(eval_fn_err("pg_encoding_to_char", vec![]).contains("expected 1 arg"));
        assert_eq!(
            eval_fn("pg_encoding_to_char", vec![Value::Null]),
            Value::Null
        );
        assert!(
            eval_fn_err("pg_encoding_to_char", vec![Value::Text("UTF8".into())])
                .contains("integer argument")
        );

        assert!(eval_fn_err("obj_description", vec![]).contains("expected 2 args"));
        assert_eq!(
            eval_fn(
                "obj_description",
                vec![Value::Null, Value::Text("pg_class".into())]
            ),
            Value::Null
        );
        assert!(
            eval_fn_err(
                "obj_description",
                vec![Value::Text("bad".into()), Value::Text("pg_class".into())]
            )
            .contains("oid argument")
        );
        assert!(
            eval_fn_err(
                "obj_description",
                vec![Value::Oid(Oid::new(1)), Value::Int32(1)]
            )
            .contains("catalog name")
        );

        assert!(eval_fn_err("col_description", vec![]).contains("expected 2 args"));
        assert_eq!(
            eval_fn("col_description", vec![Value::Null, Value::Int32(1)]),
            Value::Null
        );
        assert!(
            eval_fn_err(
                "col_description",
                vec![Value::Text("bad".into()), Value::Text("bad".into())]
            )
            .contains("oid and integer")
        );

        assert!(eval_fn_err("pg_get_serial_sequence", vec![]).contains("expected 2 args"));
        assert_eq!(
            eval_fn(
                "pg_get_serial_sequence",
                vec![Value::Null, Value::Text("id".into())]
            ),
            Value::Null
        );
        assert!(
            eval_fn_err(
                "pg_get_serial_sequence",
                vec![Value::Int32(1), Value::Text("id".into())]
            )
            .contains("text arguments")
        );
    }

    #[test]
    fn cast_size_and_array_error_edges_cover_scalar_compat_paths() {
        assert!(eval_fn_err("__ultrasql_cast_oid", vec![]).contains("expected 1 arg"));
        assert_eq!(
            eval_fn("__ultrasql_cast_oid", vec![Value::Null]),
            Value::Null
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_oid", vec![Value::RegClass(Oid::new(42))]),
            Value::Oid(Oid::new(42))
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_oid", vec![Value::Int16(42)]),
            Value::Oid(Oid::new(42))
        );
        assert!(eval_fn_err("__ultrasql_cast_oid", vec![Value::Text("x".into())]).contains("OID"));

        assert!(eval_fn_err("__ultrasql_cast_regclass", vec![]).contains("expected 1 arg"));
        assert_eq!(
            eval_fn("__ultrasql_cast_regclass", vec![Value::Null]),
            Value::Null
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_regclass", vec![Value::Int16(7)]),
            Value::RegClass(Oid::new(7))
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_regclass", vec![Value::Int32(8)]),
            Value::RegClass(Oid::new(8))
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_regclass", vec![Value::Int64(9)]),
            Value::RegClass(Oid::new(9))
        );
        assert!(
            eval_fn_err("__ultrasql_cast_regclass", vec![Value::Text("x".into())]).contains("OID")
        );

        assert!(eval_fn_err("__ultrasql_cast_regtype", vec![]).contains("expected 1 arg"));
        assert_eq!(
            eval_fn("__ultrasql_cast_regtype", vec![Value::Null]),
            Value::Null
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_regtype", vec![Value::Int16(7)]),
            Value::RegType(Oid::new(7))
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_regtype", vec![Value::Int32(8)]),
            Value::RegType(Oid::new(8))
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_regtype", vec![Value::Int64(9)]),
            Value::RegType(Oid::new(9))
        );
        assert!(
            eval_fn_err("__ultrasql_cast_regtype", vec![Value::Text("x".into())]).contains("OID")
        );

        assert!(eval_fn_err("__ultrasql_cast_text", vec![]).contains("expected 1 arg"));
        assert_eq!(
            eval_fn("__ultrasql_cast_text", vec![Value::Null]),
            Value::Null
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_text", vec![Value::RegType(Oid::new(23))]),
            Value::Text("integer".into())
        );
        assert_eq!(
            eval_fn(
                "__ultrasql_cast_text",
                vec![Value::RegType(Oid::new(999_999))]
            ),
            Value::Text("999999".into())
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_numeric", vec![Value::Money(1234)]),
            Value::Decimal {
                value: 1234,
                scale: 2
            }
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_money", vec![Value::Int32(12)]),
            Value::Money(1200)
        );
        assert_eq!(
            eval_fn(
                "__ultrasql_cast_money",
                vec![Value::Decimal {
                    value: 12_345,
                    scale: 3
                }]
            ),
            Value::Money(1235)
        );
        assert!(eval_fn_err("pg_size_pretty", vec![]).contains("expected 1 arg"));
        assert_eq!(eval_fn("pg_size_pretty", vec![Value::Null]), Value::Null);
        assert!(eval_fn_err("pg_size_pretty", vec![Value::Text("x".into())]).contains("integer"));

        let array = Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(1)],
        };
        assert_eq!(
            eval_fn("array_length", vec![array.clone(), Value::Int32(0)]),
            Value::Null
        );
        assert!(eval_fn_err("__ultrasql_array_subscript", vec![]).contains("expected 2 args"));
        assert_eq!(
            eval_fn(
                "__ultrasql_array_subscript",
                vec![Value::Null, Value::Int32(1)]
            ),
            Value::Null
        );
        assert_eq!(
            eval_fn(
                "__ultrasql_array_subscript",
                vec![array.clone(), Value::Null]
            ),
            Value::Null
        );
        assert_eq!(
            eval_fn(
                "__ultrasql_array_subscript",
                vec![array.clone(), Value::Int32(0)]
            ),
            Value::Null
        );
        assert!(eval_fn_err("__ultrasql_eq_any_array", vec![]).contains("expected 2 args"));
        assert_eq!(
            eval_fn("__ultrasql_eq_any_array", vec![Value::Null, array.clone()]),
            Value::Null
        );
        assert_eq!(
            eval_fn("__ultrasql_eq_any_array", vec![Value::Int32(2), array]),
            Value::Bool(false)
        );
    }

    #[test]
    fn catalog_and_array_functions_cover_nulls_errors_and_fallbacks() {
        for name in [
            "version",
            "current_catalog",
            "current_database",
            "current_schema",
            "current_user",
        ] {
            assert!(eval_fn(name, vec![]).data_type() == DataType::Text { max_len: None });
            assert!(eval_fn_err(name, vec![Value::Int32(1)]).contains("expected 0 args"));
        }

        assert_eq!(
            eval_fn("current_schemas", vec![Value::Bool(true)]),
            Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: vec![
                    Value::Text("pg_catalog".into()),
                    Value::Text("public".into())
                ],
            }
        );
        assert_eq!(
            eval_fn("current_schemas", vec![Value::Bool(false)]),
            Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: vec![Value::Text("public".into())],
            }
        );
        assert_eq!(
            eval_fn("current_schemas", vec![Value::Null]),
            Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: vec![Value::Text("public".into())],
            }
        );
        assert!(eval_fn_err("current_schemas", vec![]).contains("expected 1 arg"));
        assert!(eval_fn_err("current_schemas", vec![Value::Int32(1)]).contains("boolean"));

        assert_eq!(eval_fn("to_regtype", vec![Value::Null]), Value::Null);
        assert_eq!(
            eval_fn("to_regtype", vec![Value::RegType(Oid::new(23))]),
            Value::RegType(Oid::new(23))
        );
        assert_eq!(
            eval_fn("to_regtype", vec![Value::Text("int4".into())]),
            Value::RegType(Oid::new(23))
        );
        assert!(eval_fn_err("to_regtype", vec![]).contains("expected 1 arg"));
        assert!(eval_fn_err("to_regtype", vec![Value::Int32(1)]).contains("text argument"));

        for name in [
            "pg_table_is_visible",
            "pg_is_other_temp_schema",
            "pg_function_is_visible",
            "pg_relation_is_publishable",
        ] {
            assert!(matches!(
                eval_fn(name, vec![Value::Null]),
                Value::Null | Value::Bool(false)
            ));
            assert!(matches!(
                eval_fn(name, vec![Value::Oid(Oid::new(1))]),
                Value::Bool(_)
            ));
            assert!(eval_fn_err(name, vec![]).contains("expected 1 arg"));
            if name != "pg_relation_is_publishable" {
                assert!(eval_fn_err(name, vec![Value::Text("bad".into())]).contains("OID"));
            }
        }

        assert_eq!(
            eval_fn(
                "set_config",
                vec![
                    Value::Text("work_mem".into()),
                    Value::Text("4MB".into()),
                    Value::Bool(true),
                ],
            ),
            Value::Text("4MB".into())
        );
        assert_eq!(
            eval_fn(
                "set_config",
                vec![Value::Null, Value::Text("x".into()), Value::Bool(false)]
            ),
            Value::Null
        );
        assert!(eval_fn_err("set_config", vec![]).contains("expected 3 args"));
        assert!(
            eval_fn_err(
                "set_config",
                vec![Value::Int32(1), Value::Text("x".into()), Value::Bool(false)]
            )
            .contains("setting name")
        );
        assert!(
            eval_fn_err(
                "set_config",
                vec![Value::Text("x".into()), Value::Int32(1), Value::Bool(false)]
            )
            .contains("setting value")
        );
        assert!(
            eval_fn_err(
                "set_config",
                vec![
                    Value::Text("x".into()),
                    Value::Text("y".into()),
                    Value::Int32(1)
                ]
            )
            .contains("local flag")
        );

        for (oid, name) in [
            (16, "boolean"),
            (17, "bytea"),
            (20, "bigint"),
            (23, "integer"),
            (25, "text"),
            (2950, "uuid"),
            (3802, "jsonb"),
            (999_999, "text"),
        ] {
            assert_eq!(
                eval_fn("format_type", vec![Value::Oid(Oid::new(oid)), Value::Null]),
                Value::Text(name.into())
            );
        }
        assert_eq!(
            eval_fn("format_type", vec![Value::Null, Value::Null]),
            Value::Null
        );
        assert!(eval_fn_err("format_type", vec![]).contains("expected 2 args"));
        assert!(
            eval_fn_err("format_type", vec![Value::Text("bad".into()), Value::Null])
                .contains("oid")
        );

        assert_eq!(
            eval_fn(
                "pg_get_expr",
                vec![Value::Text("x + 1".into()), Value::Oid(Oid::new(1))]
            ),
            Value::Text("x + 1".into())
        );
        assert_eq!(
            eval_fn(
                "pg_get_expr",
                vec![
                    Value::Text("x + 1".into()),
                    Value::Oid(Oid::new(1)),
                    Value::Bool(false)
                ]
            ),
            Value::Text("x + 1".into())
        );
        assert_eq!(
            eval_fn("pg_get_expr", vec![Value::Null, Value::Oid(Oid::new(1))]),
            Value::Null
        );
        assert!(eval_fn_err("pg_get_expr", vec![]).contains("expected 2 or 3 args"));
        assert!(
            eval_fn_err(
                "pg_get_expr",
                vec![Value::Int32(1), Value::Oid(Oid::new(1))]
            )
            .contains("expression text")
        );

        for name in [
            "pg_get_indexdef",
            "pg_get_constraintdef",
            "pg_get_statisticsobjdef_columns",
            "pg_get_function_result",
            "pg_get_function_arguments",
        ] {
            assert_eq!(eval_fn(name, vec![Value::Null]), Value::Null);
            assert!(eval_fn_err(name, vec![Value::Text("bad".into())]).contains("oid"));
        }
        assert_eq!(
            eval_fn("pg_encoding_to_char", vec![Value::Int32(6)]),
            Value::Text("UTF8".into())
        );
        assert_eq!(
            eval_fn(
                "obj_description",
                vec![Value::Oid(Oid::new(1)), Value::Text("pg_class".into())]
            ),
            Value::Null
        );
        assert_eq!(
            eval_fn(
                "col_description",
                vec![Value::Oid(Oid::new(1)), Value::Int32(1)]
            ),
            Value::Null
        );
        assert_eq!(
            eval_fn(
                "pg_get_serial_sequence",
                vec![Value::Text("public.t".into()), Value::Text("id".into())]
            ),
            Value::Null
        );

        let array = Value::Array {
            element_type: DataType::Int32,
            elements: vec![Value::Int32(10), Value::Null, Value::Int32(30)],
        };
        assert_eq!(
            eval_fn("array_length", vec![array.clone(), Value::Int32(1)]),
            Value::Int32(3)
        );
        assert_eq!(
            eval_fn("array_length", vec![Value::Null, Value::Int32(1)]),
            Value::Null
        );
        assert!(
            eval_fn_err("array_length", vec![Value::Int32(1), Value::Int32(1)]).contains("array")
        );
        assert!(
            eval_fn_err("array_length", vec![array.clone(), Value::Text("1".into())])
                .contains("dimension")
        );
        assert_eq!(
            eval_fn("array_position", vec![array.clone(), Value::Int32(30)]),
            Value::Int32(3)
        );
        assert_eq!(
            eval_fn(
                "array_position",
                vec![array.clone(), Value::Int32(30), Value::Int32(3)]
            ),
            Value::Int32(3)
        );
        assert_eq!(
            eval_fn("array_position", vec![array.clone(), Value::Int32(99)]),
            Value::Null
        );
        assert_eq!(
            eval_fn(
                "array_to_string",
                vec![
                    array.clone(),
                    Value::Text(",".into()),
                    Value::Text("NULL".into())
                ]
            ),
            Value::Text("10,NULL,30".into())
        );
        assert_eq!(
            eval_fn(
                "string_to_array",
                vec![Value::Text("a,b".into()), Value::Text(",".into())]
            ),
            Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: vec![Value::Text("a".into()), Value::Text("b".into())],
            }
        );
        assert_eq!(
            eval_fn(
                "__ultrasql_array_subscript",
                vec![array.clone(), Value::Int32(2)]
            ),
            Value::Null
        );
        assert_eq!(
            eval_fn(
                "__ultrasql_array_subscript",
                vec![array.clone(), Value::Int32(3)]
            ),
            Value::Int32(30)
        );
        assert_eq!(
            eval_fn(
                "__ultrasql_array_slice",
                vec![array.clone(), Value::Int32(1), Value::Int32(2)]
            ),
            Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(10), Value::Null],
            }
        );
        assert_eq!(
            eval_fn(
                "__ultrasql_array_slice",
                vec![array.clone(), Value::Int32(3), Value::Null]
            ),
            Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(30)],
            }
        );
        assert_eq!(
            eval_fn(
                "__ultrasql_eq_any_array",
                vec![Value::Int32(10), array.clone()]
            ),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn("__ultrasql_eq_any_array", vec![Value::Int32(20), array]),
            Value::Null
        );
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
    fn catalog_compatibility_functions_cover_visible_oid_and_description_paths() {
        assert_eq!(
            eval_fn("pg_typeof", vec![Value::Int32(1)]),
            Value::Text("integer".to_owned())
        );
        assert_eq!(
            eval_fn("current_schemas", vec![Value::Bool(true)]),
            Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: vec![
                    Value::Text("pg_catalog".to_owned()),
                    Value::Text("public".to_owned())
                ],
            }
        );
        assert_eq!(
            eval_fn(
                "to_regtype",
                vec![Value::Text("pg_catalog.int4".to_owned())]
            ),
            Value::RegType(Oid::new(23))
        );
        assert_eq!(
            eval_fn("pg_table_is_visible", vec![Value::RegClass(Oid::new(1259))]),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn("pg_is_other_temp_schema", vec![Value::Oid(Oid::new(11))]),
            Value::Bool(false)
        );
        assert_eq!(
            eval_fn("pg_function_is_visible", vec![Value::Int64(42)]),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn("pg_relation_is_publishable", vec![Value::Null]),
            Value::Bool(false)
        );
        assert_eq!(
            eval_fn(
                "set_config",
                vec![
                    Value::Text("search_path".to_owned()),
                    Value::Text("public".to_owned()),
                    Value::Bool(true),
                ],
            ),
            Value::Text("public".to_owned())
        );
        assert_eq!(
            eval_fn("format_type", vec![Value::Oid(Oid::new(1700)), Value::Null]),
            Value::Text("numeric".to_owned())
        );
        assert_eq!(
            eval_fn(
                "pg_get_expr",
                vec![Value::Text("a + b".to_owned()), Value::Oid(Oid::new(1))],
            ),
            Value::Text("a + b".to_owned())
        );
        assert_eq!(
            eval_fn("pg_get_indexdef", vec![Value::Oid(Oid::new(42))]),
            Value::Text("index 42".to_owned())
        );
        assert_eq!(
            eval_fn(
                "pg_get_constraintdef",
                vec![Value::Oid(Oid::new(7)), Value::Bool(true)],
            ),
            Value::Text("constraint 7".to_owned())
        );
        assert_eq!(
            eval_fn("pg_get_statisticsobjdef_columns", vec![Value::Int32(9)]),
            Value::Text(String::new())
        );
        assert_eq!(
            eval_fn("pg_get_function_result", vec![Value::RegType(Oid::new(10))]),
            Value::Text(String::new())
        );
        assert_eq!(
            eval_fn("pg_get_function_arguments", vec![Value::Int16(10)]),
            Value::Text(String::new())
        );
        assert_eq!(
            eval_fn("pg_encoding_to_char", vec![Value::Int32(6)]),
            Value::Text("UTF8".to_owned())
        );
        assert_eq!(
            eval_fn(
                "obj_description",
                vec![Value::Oid(Oid::new(1)), Value::Text("pg_class".to_owned())],
            ),
            Value::Null
        );
        assert_eq!(
            eval_fn(
                "col_description",
                vec![Value::Oid(Oid::new(1)), Value::Int32(2)]
            ),
            Value::Null
        );
        assert_eq!(
            eval_fn(
                "pg_get_serial_sequence",
                vec![Value::Text("t".to_owned()), Value::Text("id".to_owned()),],
            ),
            Value::Null
        );
    }

    #[test]
    fn cast_and_size_helpers_cover_oid_reg_and_text_surfaces() {
        assert_eq!(
            eval_fn("__ultrasql_cast_oid", vec![Value::Int64(42)]),
            Value::Oid(Oid::new(42))
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_regclass", vec![Value::Oid(Oid::new(43))]),
            Value::RegClass(Oid::new(43))
        );
        assert_eq!(
            eval_fn(
                "__ultrasql_cast_regtype",
                vec![Value::RegClass(Oid::new(44))]
            ),
            Value::RegType(Oid::new(44))
        );
        assert_eq!(
            eval_fn("__ultrasql_cast_text", vec![Value::Money(1234)]),
            Value::Text("$12.34".to_owned())
        );
        assert_eq!(
            eval_fn("pg_size_pretty", vec![Value::Int64(1536)]),
            Value::Text("1 kB".to_owned())
        );
        assert!(
            matches!(eval_fn("gen_random_uuid", vec![]), Value::Uuid(_)),
            "gen_random_uuid should emit uuid bytes"
        );
        assert!(
            eval_fn_err("__ultrasql_cast_oid", vec![Value::Int64(-1)])
                .contains("value out of range")
        );
    }

    #[test]
    fn text_math_regex_and_format_helpers_cover_common_scalar_paths() {
        assert_eq!(eval_fn("abs", vec![Value::Int32(-7)]), Value::Int64(7));
        assert_eq!(
            eval_fn("lower", vec![Value::Text("MiXeD".to_owned())]),
            Value::Text("mixed".to_owned())
        );
        assert_eq!(
            eval_fn("upper", vec![Value::Text("MiXeD".to_owned())]),
            Value::Text("MIXED".to_owned())
        );
        assert_eq!(eval_fn("pi", vec![]), Value::Float64(std::f64::consts::PI));
        assert!(matches!(eval_fn("random", vec![]), Value::Float64(v) if (0.0..1.0).contains(&v)));
        assert_eq!(
            eval_fn("length", vec![Value::Text("abc".to_owned())]),
            Value::Int32(3)
        );
        assert_eq!(
            eval_fn(
                "bit_length",
                vec![Value::BitString(BitString::parse("10101").expect("bits"))]
            ),
            Value::Int32(5)
        );
        assert_eq!(
            eval_fn(
                "octet_length",
                vec![Value::BitString(BitString::parse("10101").expect("bits"))]
            ),
            Value::Int32(1)
        );
        assert_eq!(
            eval_fn("trim", vec![Value::Text("  hi  ".to_owned())]),
            Value::Text("hi".to_owned())
        );
        assert_eq!(
            eval_fn(
                "lpad",
                vec![
                    Value::Text("7".to_owned()),
                    Value::Int32(3),
                    Value::Text("0".to_owned())
                ]
            ),
            Value::Text("007".to_owned())
        );
        assert_eq!(
            eval_fn(
                "rpad",
                vec![
                    Value::Text("7".to_owned()),
                    Value::Int32(3),
                    Value::Text("0".to_owned())
                ]
            ),
            Value::Text("700".to_owned())
        );
        assert_eq!(
            eval_fn(
                "left",
                vec![Value::Text("abcdef".to_owned()), Value::Int32(2)]
            ),
            Value::Text("ab".to_owned())
        );
        assert_eq!(
            eval_fn(
                "right",
                vec![Value::Text("abcdef".to_owned()), Value::Int32(2)]
            ),
            Value::Text("ef".to_owned())
        );
        assert_eq!(
            eval_fn(
                "position",
                vec![
                    Value::Text("cd".to_owned()),
                    Value::Text("abcdef".to_owned())
                ]
            ),
            Value::Int32(3)
        );
        assert_eq!(
            eval_fn(
                "replace",
                vec![
                    Value::Text("banana".to_owned()),
                    Value::Text("na".to_owned()),
                    Value::Text("NA".to_owned())
                ]
            ),
            Value::Text("baNANA".to_owned())
        );
        assert_eq!(
            eval_fn(
                "split_part",
                vec![
                    Value::Text("a,b,c".to_owned()),
                    Value::Text(",".to_owned()),
                    Value::Int32(2)
                ]
            ),
            Value::Text("b".to_owned())
        );
        assert_eq!(
            eval_fn(
                "concat",
                vec![Value::Text("a".to_owned()), Value::Null, Value::Int32(7)]
            ),
            Value::Text("a7".to_owned())
        );
        assert_eq!(
            eval_fn(
                "concat_ws",
                vec![
                    Value::Text("-".to_owned()),
                    Value::Text("a".to_owned()),
                    Value::Null,
                    Value::Text("b".to_owned())
                ]
            ),
            Value::Text("a-b".to_owned())
        );
        assert_eq!(
            eval_fn(
                "repeat",
                vec![Value::Text("ha".to_owned()), Value::Int32(3)]
            ),
            Value::Text("hahaha".to_owned())
        );
        assert_eq!(
            eval_fn("reverse", vec![Value::Text("abc".to_owned())]),
            Value::Text("cba".to_owned())
        );
        assert_eq!(
            eval_fn("quote_ident", vec![Value::Text("select".to_owned())]),
            Value::Text("\"select\"".to_owned())
        );
        assert_eq!(
            eval_fn("quote_literal", vec![Value::Text("a'b".to_owned())]),
            Value::Text("'a''b'".to_owned())
        );
        assert_eq!(
            eval_fn(
                "format",
                vec![
                    Value::Text("hello %s %I".to_owned()),
                    Value::Text("x".to_owned()),
                    Value::Text("select".to_owned())
                ]
            ),
            Value::Text("hello x \"select\"".to_owned())
        );
        assert_eq!(
            eval_fn(
                "regexp_replace",
                vec![
                    Value::Text("abc123".to_owned()),
                    Value::Text("[0-9]+".to_owned()),
                    Value::Text("!".to_owned())
                ]
            ),
            Value::Text("abc!".to_owned())
        );
    }

    #[test]
    fn date_json_xml_bit_and_network_helpers_cover_scalar_edges() {
        let date = eval_fn(
            "make_date",
            vec![Value::Int32(2024), Value::Int32(2), Value::Int32(29)],
        );
        assert_eq!(
            eval_fn(
                "extract",
                vec![Value::Text("year".to_owned()), date.clone()]
            ),
            Value::Int64(2024)
        );
        assert_eq!(
            eval_fn(
                "extract",
                vec![Value::Text("month".to_owned()), date.clone()]
            ),
            Value::Int64(2)
        );
        assert_eq!(
            eval_fn("extract", vec![Value::Text("day".to_owned()), date]),
            Value::Int64(29)
        );
        assert_eq!(
            eval_fn(
                "extract",
                vec![Value::Text("hour".to_owned()), Value::Time(3_661_000_000)]
            ),
            Value::Int64(1)
        );
        assert_eq!(
            eval_fn(
                "extract",
                vec![
                    Value::Text("minute".to_owned()),
                    Value::Interval {
                        months: 14,
                        days: 2,
                        microseconds: 7_200_000_000,
                    },
                ]
            ),
            Value::Int64(0)
        );
        assert_eq!(
            eval_fn(
                "date_trunc",
                vec![
                    Value::Text("minute".to_owned()),
                    Value::TimestampTz(123_456_789),
                ]
            ),
            Value::TimestampTz(120_000_000)
        );
        assert_eq!(
            eval_fn(
                "age",
                vec![
                    Value::Timestamp(2 * 86_400_000_000 + 1_000_000),
                    Value::Timestamp(0),
                ]
            ),
            Value::Interval {
                months: 0,
                days: 2,
                microseconds: 1_000_000,
            }
        );
        assert_eq!(
            eval_fn(
                "date_bin",
                vec![
                    Value::Interval {
                        months: 0,
                        days: 0,
                        microseconds: 15 * 60_000_000,
                    },
                    Value::TimestampTz(46 * 60_000_000),
                    Value::TimestampTz(0),
                ]
            ),
            Value::TimestampTz(45 * 60_000_000)
        );
        assert!(
            eval_fn_err(
                "make_date",
                vec![Value::Int32(2024), Value::Int32(2), Value::Int32(30)]
            )
            .contains("invalid date")
        );
        assert!(
            eval_fn_err(
                "date_bin",
                vec![
                    Value::Interval {
                        months: 1,
                        days: 0,
                        microseconds: 0,
                    },
                    Value::TimestampTz(0),
                    Value::TimestampTz(0),
                ],
            )
            .contains("month stride")
        );

        let bits = Value::BitString(BitString::parse("1010").expect("bits"));
        assert_eq!(eval_fn("bit_count", vec![bits.clone()]), Value::Int64(2));
        assert_eq!(
            eval_fn("get_bit", vec![bits.clone(), Value::Int32(2)]),
            Value::Int32(1)
        );
        assert_eq!(
            eval_fn("set_bit", vec![bits, Value::Int32(1), Value::Int32(1)]),
            Value::BitString(BitString::parse("1110").expect("bits"))
        );
        assert!(
            eval_fn_err(
                "set_bit",
                vec![
                    Value::BitString(BitString::parse("10").expect("bits")),
                    Value::Int32(0),
                    Value::Int32(2),
                ],
            )
            .contains("new value")
        );

        assert_eq!(
            eval_fn(
                "json_build_object",
                vec![
                    Value::Text("a".to_owned()),
                    Value::Int32(1),
                    Value::Text("b".to_owned()),
                    Value::Bool(true),
                ]
            ),
            Value::Jsonb(r#"{"a":1,"b":true}"#.to_owned())
        );
        assert_eq!(
            eval_fn(
                "jsonb_set",
                vec![
                    Value::Jsonb(r#"{"a":{"b":1}}"#.to_owned()),
                    Value::Array {
                        element_type: DataType::Text { max_len: None },
                        elements: vec![Value::Text("a".to_owned()), Value::Text("b".to_owned())],
                    },
                    Value::Int32(9),
                    Value::Bool(true),
                ]
            ),
            Value::Jsonb(r#"{"a":{"b":9}}"#.to_owned())
        );
        assert_eq!(
            eval_fn(
                "jsonb_path_exists",
                vec![
                    Value::Jsonb(r#"{"items":[{"score":12},{"score":25}]}"#.to_owned()),
                    Value::Text("$.items[*] ? (@.score >= 20)".to_owned()),
                ]
            ),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn(
                "row_to_json",
                vec![Value::Record(vec![
                    ("id".to_owned(), Value::Int32(1)),
                    ("name".to_owned(), Value::Text("a".to_owned())),
                ])]
            ),
            Value::Jsonb(r#"{"id":1,"name":"a"}"#.to_owned())
        );

        assert_eq!(
            eval_fn(
                "xml_is_well_formed_document",
                vec![Value::Text(
                    "<root><item id=\"2\">b</item></root>".to_owned()
                )]
            ),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn(
                "xpath_exists",
                vec![
                    Value::Text("/root/item[@id=\"2\"]".to_owned()),
                    Value::Xml("<root><item id=\"1\"/><item id=\"2\">b</item></root>".to_owned()),
                ]
            ),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn(
                "xpath",
                vec![
                    Value::Text("/root/item".to_owned()),
                    Value::Xml("<root><item>a</item><item>b</item></root>".to_owned()),
                ]
            ),
            Value::Array {
                element_type: DataType::Xml,
                elements: vec![
                    Value::Xml("<item>a</item>".to_owned()),
                    Value::Xml("<item>b</item>".to_owned()),
                ],
            }
        );

        let lit_inet = |text: &str| ScalarExpr::Literal {
            value: inet(text),
            data_type: DataType::Inet,
        };
        let network_add = ScalarExpr::Binary {
            op: BinaryOp::Add,
            left: Box::new(lit_inet("192.168.1.10")),
            right: Box::new(lit_i32(5)),
            data_type: DataType::Inet,
        };
        assert_eq!(
            Eval::new(network_add).eval(&[]).expect("network add"),
            inet("192.168.1.15")
        );
        let network_sub = ScalarExpr::Binary {
            op: BinaryOp::Sub,
            left: Box::new(lit_inet("192.168.1.15")),
            right: Box::new(lit_inet("192.168.1.10")),
            data_type: DataType::Int64,
        };
        assert_eq!(
            Eval::new(network_sub).eval(&[]).expect("network sub"),
            Value::Int64(5)
        );
    }

    #[test]
    fn numeric_and_case_function_dispatch_covers_common_edges() {
        assert_float_close(eval_fn("ceil", vec![Value::Float64(1.2)]), 2.0);
        assert_float_close(eval_fn("floor", vec![Value::Float64(1.8)]), 1.0);
        assert_float_close(eval_fn("round", vec![Value::Float64(1.5)]), 2.0);
        assert_float_close(eval_fn("trunc", vec![Value::Float64(1.9)]), 1.0);
        assert_float_close(
            eval_fn("mod", vec![Value::Float64(7.0), Value::Float64(4.0)]),
            3.0,
        );
        assert_float_close(
            eval_fn("power", vec![Value::Float64(2.0), Value::Float64(3.0)]),
            8.0,
        );
        assert_float_close(eval_fn("sqrt", vec![Value::Float64(9.0)]), 3.0);
        assert_float_close(eval_fn("exp", vec![Value::Float64(0.0)]), 1.0);
        assert_float_close(
            eval_fn("ln", vec![Value::Float64(std::f64::consts::E)]),
            1.0,
        );
        assert_float_close(eval_fn("log", vec![Value::Float64(100.0)]), 2.0);
        assert_float_close(eval_fn("sin", vec![Value::Float64(0.0)]), 0.0);
        assert_float_close(eval_fn("cos", vec![Value::Float64(0.0)]), 1.0);
        assert_float_close(eval_fn("tan", vec![Value::Float64(0.0)]), 0.0);
        assert_float_close(
            eval_fn("asin", vec![Value::Float64(1.0)]),
            std::f64::consts::FRAC_PI_2,
        );
        assert_float_close(eval_fn("acos", vec![Value::Float64(1.0)]), 0.0);
        assert_float_close(
            eval_fn("atan", vec![Value::Float64(1.0)]),
            std::f64::consts::FRAC_PI_4,
        );
        assert!(eval_fn_err("sqrt", vec![Value::Text("bad".to_owned())]).contains("numeric"));

        assert_eq!(
            eval_fn(
                "case_searched",
                vec![
                    Value::Bool(false),
                    Value::Text("no".to_owned()),
                    Value::Null,
                    Value::Text("skip".to_owned()),
                    Value::Bool(true),
                    Value::Text("yes".to_owned()),
                    Value::Text("else".to_owned()),
                ],
            ),
            Value::Text("yes".to_owned())
        );
        assert!(
            eval_fn_err(
                "case_searched",
                vec![Value::Int32(1), Value::Text("bad".to_owned()), Value::Null],
            )
            .contains("WHEN clause")
        );
        assert_eq!(
            eval_fn(
                "case_simple",
                vec![
                    Value::Int32(2),
                    Value::Int32(1),
                    Value::Text("one".to_owned()),
                    Value::Int32(2),
                    Value::Text("two".to_owned()),
                    Value::Text("else".to_owned()),
                ],
            ),
            Value::Text("two".to_owned())
        );
        assert_eq!(
            eval_fn(
                "case_simple",
                vec![
                    Value::Null,
                    Value::Null,
                    Value::Text("null".to_owned()),
                    Value::Text("else".to_owned()),
                ],
            ),
            Value::Text("else".to_owned())
        );
    }

    #[test]
    fn money_addition_and_subtraction_evaluate() {
        let add = Eval::new(ScalarExpr::Binary {
            op: BinaryOp::Add,
            left: Box::new(lit_money(125)),
            right: Box::new(lit_money(375)),
            data_type: DataType::Money,
        });
        assert_eq!(add.eval(&[]).expect("money add"), Value::Money(500));

        let sub = Eval::new(ScalarExpr::Binary {
            op: BinaryOp::Sub,
            left: Box::new(lit_money(500)),
            right: Box::new(lit_money(125)),
            data_type: DataType::Money,
        });
        assert_eq!(sub.eval(&[]).expect("money sub"), Value::Money(375));
    }

    #[test]
    fn money_division_matrix_evaluates() {
        let ratio = Eval::new(ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(lit_money(500)),
            right: Box::new(lit_money(200)),
            data_type: DataType::Float64,
        });
        assert_eq!(ratio.eval(&[]).expect("money ratio"), Value::Float64(2.5));

        let divided = Eval::new(ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(lit_money(501)),
            right: Box::new(lit_i32(2)),
            data_type: DataType::Money,
        });
        assert_eq!(divided.eval(&[]).expect("money int div"), Value::Money(250));

        let zero_money = Eval::new(ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(lit_money(500)),
            right: Box::new(lit_money(0)),
            data_type: DataType::Float64,
        });
        assert!(matches!(zero_money.eval(&[]), Err(EvalError::DivByZero)));

        let zero_int = Eval::new(ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(lit_money(500)),
            right: Box::new(lit_i32(0)),
            data_type: DataType::Money,
        });
        assert!(matches!(zero_int.eval(&[]), Err(EvalError::DivByZero)));

        let rounded = Eval::new(ScalarExpr::Binary {
            op: BinaryOp::Div,
            left: Box::new(lit_money(501)),
            right: Box::new(lit_f64(2.0)),
            data_type: DataType::Money,
        });
        assert_eq!(
            rounded.eval(&[]).expect("money float div"),
            Value::Money(251)
        );
    }

    #[test]
    fn money_scalar_multiplication_evaluates() {
        let money_int = Eval::new(ScalarExpr::Binary {
            op: BinaryOp::Mul,
            left: Box::new(lit_money(125)),
            right: Box::new(lit_i32(3)),
            data_type: DataType::Money,
        });
        assert_eq!(
            money_int.eval(&[]).expect("money int mul"),
            Value::Money(375)
        );

        let int_money = Eval::new(ScalarExpr::Binary {
            op: BinaryOp::Mul,
            left: Box::new(lit_i32(3)),
            right: Box::new(lit_money(125)),
            data_type: DataType::Money,
        });
        assert_eq!(
            int_money.eval(&[]).expect("int money mul"),
            Value::Money(375)
        );

        let money_float = Eval::new(ScalarExpr::Binary {
            op: BinaryOp::Mul,
            left: Box::new(lit_money(125)),
            right: Box::new(lit_f64(1.5)),
            data_type: DataType::Money,
        });
        assert_eq!(
            money_float.eval(&[]).expect("money float mul"),
            Value::Money(188)
        );
    }

    #[test]
    fn money_unary_signs_evaluate() {
        let neg = Eval::new(ScalarExpr::Unary {
            op: UnaryOp::Neg,
            expr: Box::new(lit_money(125)),
            data_type: DataType::Money,
        });
        assert_eq!(neg.eval(&[]).expect("money neg"), Value::Money(-125));

        let pos = Eval::new(ScalarExpr::Unary {
            op: UnaryOp::Pos,
            expr: Box::new(lit_money(125)),
            data_type: DataType::Money,
        });
        assert_eq!(pos.eval(&[]).expect("money pos"), Value::Money(125));
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
    fn substring_accepts_bpchar_source() {
        let ev = Eval::new(call(
            "substring",
            vec![
                lit_char("13-111-1111    ", Some(15)),
                lit_i32(1),
                lit_i32(2),
            ],
            DataType::Text { max_len: None },
        ));

        assert_eq!(ev.eval(&[]).unwrap(), Value::Text("13".to_owned()));
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

        assert_eq!(
            eval_fn(
                "array_append",
                vec![
                    text_array_value(&["red", "green"]),
                    Value::Text("blue".into())
                ]
            ),
            Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: vec![
                    Value::Text("red".into()),
                    Value::Text("green".into()),
                    Value::Text("blue".into())
                ]
            }
        );
        assert_eq!(
            eval_fn(
                "array_prepend",
                vec![Value::Text("red".into()), text_array_value(&["green"])]
            ),
            Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: vec![Value::Text("red".into()), Value::Text("green".into())]
            }
        );
        assert_eq!(
            eval_fn(
                "array_remove",
                vec![
                    text_array_value(&["red", "green", "red"]),
                    Value::Text("red".into())
                ]
            ),
            Value::Array {
                element_type: DataType::Text { max_len: None },
                elements: vec![Value::Text("green".into())]
            }
        );

        let matrix = Value::Array {
            element_type: DataType::Array(Box::new(DataType::Int32)),
            elements: vec![
                Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(1), Value::Int32(2)],
                },
                Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(3), Value::Int32(4)],
                },
            ],
        };
        assert_eq!(
            eval_fn("cardinality", vec![matrix.clone()]),
            Value::Int32(4)
        );
        assert_eq!(
            eval_fn("array_ndims", vec![matrix.clone()]),
            Value::Int32(2)
        );
        assert_eq!(
            eval_fn("array_lower", vec![matrix.clone(), Value::Int32(1)]),
            Value::Int32(1)
        );
        assert_eq!(
            eval_fn("array_upper", vec![matrix.clone(), Value::Int32(2)]),
            Value::Int32(2)
        );
        assert_eq!(
            eval_fn("array_dims", vec![matrix]),
            Value::Text("[1:2][1:2]".into())
        );

        assert_eq!(
            eval_fn(
                "array_replace",
                vec![
                    text_array_value(&["red", "green", "red"]),
                    Value::Text("red".into()),
                    Value::Text("blue".into())
                ]
            ),
            text_array_value(&["blue", "green", "blue"])
        );
        assert_eq!(
            eval_fn(
                "array_positions",
                vec![
                    text_array_value(&["red", "green", "red"]),
                    Value::Text("red".into())
                ]
            ),
            Value::Array {
                element_type: DataType::Int32,
                elements: vec![Value::Int32(1), Value::Int32(3)]
            }
        );
        assert_eq!(
            eval_fn(
                "trim_array",
                vec![text_array_value(&["red", "green", "blue"]), Value::Int32(1)]
            ),
            text_array_value(&["red", "green"])
        );
    }

    #[test]
    fn multidimensional_array_length_evaluates_dimensions() {
        let matrix_type = DataType::Array(Box::new(DataType::Array(Box::new(DataType::Int32))));
        let matrix = ScalarExpr::Literal {
            value: Value::Array {
                element_type: DataType::Array(Box::new(DataType::Int32)),
                elements: vec![
                    Value::Array {
                        element_type: DataType::Int32,
                        elements: vec![Value::Int32(1), Value::Int32(2)],
                    },
                    Value::Array {
                        element_type: DataType::Int32,
                        elements: vec![Value::Int32(3), Value::Int32(4)],
                    },
                ],
            },
            data_type: matrix_type,
        };

        let len_dim_1 = Eval::new(call(
            "array_length",
            vec![matrix.clone(), lit_i32(1)],
            DataType::Int32,
        ));
        assert_eq!(len_dim_1.eval(&[]).unwrap(), Value::Int32(2));

        let len_dim_2 = Eval::new(call(
            "array_length",
            vec![matrix.clone(), lit_i32(2)],
            DataType::Int32,
        ));
        assert_eq!(len_dim_2.eval(&[]).unwrap(), Value::Int32(2));

        let len_dim_3 = Eval::new(call(
            "array_length",
            vec![matrix, lit_i32(3)],
            DataType::Int32,
        ));
        assert_eq!(len_dim_3.eval(&[]).unwrap(), Value::Null);
    }

    #[test]
    fn multidimensional_array_to_string_flattens_elements() {
        let matrix = ScalarExpr::Literal {
            value: Value::Array {
                element_type: DataType::Array(Box::new(DataType::Int32)),
                elements: vec![
                    Value::Array {
                        element_type: DataType::Int32,
                        elements: vec![Value::Int32(1), Value::Int32(2)],
                    },
                    Value::Array {
                        element_type: DataType::Int32,
                        elements: vec![Value::Int32(3), Value::Int32(4)],
                    },
                ],
            },
            data_type: DataType::Array(Box::new(DataType::Array(Box::new(DataType::Int32)))),
        };
        let joined = Eval::new(call(
            "array_to_string",
            vec![matrix, lit_text(":")],
            DataType::Text { max_len: None },
        ));
        assert_eq!(joined.eval(&[]).unwrap(), Value::Text("1:2:3:4".into()));
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

    #[test]
    fn text_search_constructor_functions_evaluate() {
        let vector = Eval::new(call(
            "to_tsvector",
            vec![lit_text("The Quick brown fox")],
            DataType::Text { max_len: None },
        ))
        .eval(&[])
        .unwrap();
        assert_eq!(vector, Value::Text("the:1 quick:2 brown:3 fox:4".into()));

        let query = Eval::new(call(
            "to_tsquery",
            vec![lit_text("Quick fox")],
            DataType::Text { max_len: None },
        ))
        .eval(&[])
        .unwrap();
        assert_eq!(query, Value::Text("quick & fox".into()));

        let rank = Eval::new(call(
            "ts_rank_cd",
            vec![
                lit_text("the:1 quick:2 brown:3 fox:4"),
                lit_text("quick & missing"),
            ],
            DataType::Float64,
        ))
        .eval(&[])
        .unwrap();
        assert_eq!(rank, Value::Float64(0.5));

        let rank_extra_arg = Eval::new(call(
            "ts_rank_cd",
            vec![
                lit_text("ignored"),
                lit_text("the:1 quick:2 brown:3 fox:4"),
                lit_text("quick & missing"),
            ],
            DataType::Float64,
        ))
        .eval(&[])
        .unwrap_err()
        .to_string();
        assert!(rank_extra_arg.contains("expected 2 args"));

        let headline = Eval::new(call(
            "ts_headline",
            vec![lit_text("The Quick brown fox."), lit_text("quick & fox")],
            DataType::Text { max_len: None },
        ))
        .eval(&[])
        .unwrap();
        assert_eq!(
            headline,
            Value::Text("The <b>Quick</b> brown <b>fox</b>.".into())
        );

        let node_count = Eval::new(call(
            "numnode",
            vec![lit_text("quick & missing")],
            DataType::Int32,
        ))
        .eval(&[])
        .unwrap();
        assert_eq!(node_count, Value::Int32(2));

        let querytree = Eval::new(call(
            "querytree",
            vec![lit_text("Quick & missing")],
            DataType::Text { max_len: None },
        ))
        .eval(&[])
        .unwrap();
        assert_eq!(querytree, Value::Text("quick & missing".into()));
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

    #[test]
    fn zero_parameter_index_returns_error() {
        let ev = Eval::with_params(param(0), vec![Value::Int32(99)]);
        let err = ev.eval(&[]).unwrap_err();
        assert!(
            matches!(err, EvalError::ParameterIndex { index: 0, len: 1 }),
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
    fn decimal_division_rounds_to_result_scale() {
        let ev = Eval::new(binop(BinaryOp::Div, lit_decimal(1, 0), lit_decimal(6, 0)));
        assert_eq!(
            ev.eval(&[]).unwrap(),
            Value::Decimal {
                value: 166_667,
                scale: 6
            }
        );
    }

    #[test]
    fn decimal_compares_float_literal() {
        let ev = Eval::new(binop(BinaryOp::Lt, lit_decimal(123, 2), lit_f64(2.0)));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
    }

    #[test]
    fn decimal_compare_handles_large_scale_gap_without_overflow() {
        let ev = Eval::new(binop(BinaryOp::Gt, lit_decimal(1, 0), lit_decimal(2, 100)));
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

    #[test]
    fn record_eq_uses_three_valued_field_semantics() {
        let equal = Eval::new(binop(
            BinaryOp::Eq,
            lit_record(vec![Value::Int32(5), Value::Int32(10)]),
            lit_record(vec![Value::Int32(5), Value::Int32(10)]),
        ));
        assert_eq!(equal.eval(&[]).unwrap(), Value::Bool(true));

        let unknown = Eval::new(binop(
            BinaryOp::Eq,
            lit_record(vec![Value::Int32(5), Value::Null]),
            lit_record(vec![Value::Int32(5), Value::Int32(10)]),
        ));
        assert_eq!(unknown.eval(&[]).unwrap(), Value::Null);

        let different = Eval::new(binop(
            BinaryOp::Eq,
            lit_record(vec![Value::Int32(5), Value::Null]),
            lit_record(vec![Value::Int32(6), Value::Null]),
        ));
        assert_eq!(different.eval(&[]).unwrap(), Value::Bool(false));
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
    fn row_to_json_uses_record_field_names() {
        let record_type = DataType::Record(vec![
            ("id".to_owned(), DataType::Int32),
            ("name".to_owned(), DataType::Text { max_len: None }),
            ("meta".to_owned(), DataType::Jsonb),
        ]);
        let ev = Eval::new(call(
            "row_to_json",
            vec![call(
                "row",
                vec![
                    lit_i32(1),
                    lit_text("Ada"),
                    lit_jsonb("{\"kind\":\"guide\"}"),
                ],
                record_type,
            )],
            DataType::Jsonb,
        ));
        let Value::Jsonb(json) = ev.eval(&[]).unwrap() else {
            panic!("expected jsonb row object");
        };
        let got: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            got,
            serde_json::json!({
                "id": 1,
                "name": "Ada",
                "meta": {"kind": "guide"},
            })
        );
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

    #[test]
    fn regex_match_operator_matches_psql_meta_patterns() {
        let ev = Eval::new(binop(
            BinaryOp::RegexMatch,
            lit_text("psql_meta_table"),
            lit_text("^(psql_meta_table)$"),
        ));
        assert_eq!(ev.eval(&[]).unwrap(), Value::Bool(true));
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
