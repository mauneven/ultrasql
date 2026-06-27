//! Unit tests for the scalar evaluator (`super::eval`).
//!
//! Split out of the original single-file `eval.rs` test module; pure code
//! motion. Shared builder helpers live here and are visible to the topic
//! submodules via `use super::*;`.

mod tests_arith_compare;
mod tests_array_textsearch;
mod tests_cast_array_cov;
mod tests_catalog_array_cov;
mod tests_column_literal;
mod tests_coverage;
mod tests_logic_concat;
mod tests_money_json;
mod tests_pattern_unary;
mod tests_pg_builtin_bugfixes;
mod tests_scalar_cov;
mod tests_vector;

use proptest::prelude::*;
use ultrasql_core::{
    BitString, DataType, Field, NetworkValue, Oid, Schema, Value, parse_date_text, parse_time_text,
    parse_timestamp_text, parse_timestamptz_text, parse_timetz_text,
};
use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr, UnaryOp};

use super::{
    Eval, EvalError, MAX_EVAL_GENERATED_TEXT_CHARS, apply_binary, eval_function_call,
    generated_text_target_len,
};

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

fn lit_decimal(value: i128, scale: i32) -> ScalarExpr {
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

#[test]
fn generated_text_len_rejects_limit_overflow() {
    let err = generated_text_target_len(
        "repeat",
        i64::try_from(MAX_EVAL_GENERATED_TEXT_CHARS).unwrap() + 1,
    )
    .unwrap_err();
    assert!(matches!(err, EvalError::Type(message) if message.contains("output length")));
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
