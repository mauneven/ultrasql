//! Array builtins: length/bounds/dims and basic mutators.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

pub(crate) fn eval_pg_size_pretty(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_array_length(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_array_ndims(args: &[Value]) -> Result<Value, EvalError> {
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
pub(crate) enum ArrayBound {
    Lower,
    Upper,
}

pub(crate) fn eval_array_bound(args: &[Value], bound: ArrayBound) -> Result<Value, EvalError> {
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

pub(crate) fn eval_array_dims(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_array_cardinality(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn array_dimensions_for_function(
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

pub(crate) fn eval_array_subscript(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_array_slice(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn optional_array_slice_bound(
    value: &Value,
    name: &'static str,
) -> Result<Option<i64>, EvalError> {
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

pub(crate) fn eval_eq_any_array(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_array_position(args: &[Value]) -> Result<Value, EvalError> {
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
    // PostgreSQL compares with IS NOT DISTINCT FROM: a NULL search argument
    // locates the first NULL element (rather than short-circuiting to NULL),
    // matching `array_positions`/`array_replace`.
    for (idx, element) in elements.iter().enumerate().skip(start_idx) {
        if array_element_matches(element, &args[1])? {
            let pos = i32::try_from(idx + 1)
                .map_err(|_| EvalError::Type("array_position overflow".to_owned()))?;
            return Ok(Value::Int32(pos));
        }
    }
    Ok(Value::Null)
}

pub(crate) fn eval_array_to_string(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn append_array_to_string_parts(
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
            // Use the PostgreSQL output-function text so `boolean` elements
            // render as `t`/`f` (matching `array_to_string(ARRAY[true],',')`).
            other => parts.push(value_to_pg_output_text(other)),
        }
    }
}

pub(crate) fn eval_string_to_array(args: &[Value]) -> Result<Value, EvalError> {
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
