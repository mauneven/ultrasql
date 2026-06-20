//! Array concatenation, removal, search, and UUID/size helpers.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

pub(crate) fn eval_array_cat(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_array_append(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "array_append: expected 2 args, got {}",
            args.len()
        )));
    }
    append_array_element("array_append", &args[0], &args[1], false)
}

pub(crate) fn eval_array_prepend(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "array_prepend: expected 2 args, got {}",
            args.len()
        )));
    }
    append_array_element("array_prepend", &args[1], &args[0], true)
}

pub(crate) fn append_array_element(
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

pub(crate) fn eval_array_remove(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_array_replace(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_array_positions(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_trim_array(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn validate_array_element_value(
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

pub(crate) fn array_element_matches(element: &Value, needle: &Value) -> Result<bool, EvalError> {
    if matches!(needle, Value::Null) {
        Ok(matches!(element, Value::Null))
    } else if matches!(element, Value::Null) {
        Ok(false)
    } else {
        Ok(compare_values(element, needle)? == std::cmp::Ordering::Equal)
    }
}

pub(crate) fn format_size_pretty(bytes: i64) -> String {
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

pub(crate) fn eval_gen_random_uuid(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn random_uuid_bytes() -> [u8; 16] {
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
