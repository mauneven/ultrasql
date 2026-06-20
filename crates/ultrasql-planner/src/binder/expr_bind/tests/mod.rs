//! Unit tests for the expression binder. Shared fixtures live here;
//! the bulky matrix tests and the focused literal/parse tests live in
//! sibling submodules to keep each file under the line ceiling.

use std::sync::Arc;

use ultrasql_core::{DataType, GeometryType, Oid, RangeType, Value, composite_text_matches_arity};
use ultrasql_parser::Span;
use ultrasql_parser::ast::Literal;

use super::*;

mod literals;
mod matrices;
mod matrices2;

fn lit(value: Value) -> ScalarExpr {
    let data_type = value.data_type();
    ScalarExpr::Literal { value, data_type }
}

fn coerce(mut expr: ScalarExpr, target: &DataType) -> ScalarExpr {
    coerce_literal_to_type(&mut expr, target);
    expr
}

fn literal_type(expr: &ScalarExpr) -> DataType {
    let ScalarExpr::Literal { data_type, .. } = expr else {
        panic!("expected literal, got {expr:?}");
    };
    data_type.clone()
}

fn literal_value(expr: &ScalarExpr) -> Value {
    let ScalarExpr::Literal { value, .. } = expr else {
        panic!("expected literal, got {expr:?}");
    };
    value.clone()
}

fn typed(type_name: &str, value: &str, unit: Option<&str>) -> ScalarExpr {
    bind_literal(&Literal::Typed {
        type_name: type_name.to_owned(),
        value: value.to_owned(),
        unit: unit.map(str::to_owned),
        span: Span::default(),
    })
}

fn null_arg(data_type: DataType) -> ScalarExpr {
    ScalarExpr::Literal {
        value: Value::Null,
        data_type,
    }
}

#[test]
fn epoch_day_is_zero() {
    assert_eq!(parse_date_literal("2000-01-01"), Some(0));
}
