//! JSON-aggregate value serialisation for `json_agg` / `jsonb_agg`.
//!
//! Moved verbatim from `state.rs` to keep that module within the module
//! line budget; behaviour is unchanged.

use serde_json::{Number as JsonNumber, Value as JsonValue};
use ultrasql_core::Value;

pub(super) fn json_agg_text(items: &[Value]) -> String {
    let values = JsonValue::Array(items.iter().map(sql_value_to_json).collect());
    serde_json::to_string(&values).unwrap_or_else(|_| "[]".to_owned())
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
        other => JsonValue::String(other.to_string()),
    }
}
