//! Full-text search, JSON containment, and collection helpers.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

pub(crate) fn text_search_match(left: &Value, right: &Value) -> Result<bool, EvalError> {
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

pub(crate) fn text_search_terms(text: &str) -> Vec<String> {
    text.split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

pub(crate) fn text_search_payload_arg<'a>(
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

pub(crate) fn tsquery_payload_arg<'a>(
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

pub(crate) fn eval_to_tsvector(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_plain_tsquery(func_name: &str, args: &[Value]) -> Result<Value, EvalError> {
    let Some(text) = text_search_payload_arg(func_name, args)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(text_search_terms(text).join(" & ")))
}

pub(crate) fn eval_ts_rank(func_name: &str, args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_ts_headline(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_numnode(args: &[Value]) -> Result<Value, EvalError> {
    let Some(query) = tsquery_payload_arg("numnode", args)? else {
        return Ok(Value::Null);
    };
    let node_count = i32::try_from(text_search_terms(query).len())
        .map_err(|_| EvalError::Type("numnode: query node count overflow".to_owned()))?;
    Ok(Value::Int32(node_count))
}

pub(crate) fn eval_querytree(args: &[Value]) -> Result<Value, EvalError> {
    let Some(query) = tsquery_payload_arg("querytree", args)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(text_search_terms(query).join(" & ")))
}

pub(crate) fn highlight_text_search_terms(document: &str, terms: &[String]) -> String {
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

pub(crate) fn push_headline_token(output: &mut String, token: &str, terms: &[String]) {
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

pub(crate) fn overlaps_values(left: &Value, right: &Value) -> Option<bool> {
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

pub(crate) fn contains_values(left: &Value, right: &Value) -> Option<bool> {
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

pub(crate) fn json_get(left: &Value, right: &Value, as_text: bool) -> Result<Value, EvalError> {
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

pub(crate) fn json_has_key(left: &Value, right: &Value) -> Result<bool, EvalError> {
    let json = json_text(left).ok_or_else(|| {
        EvalError::Type(format!("? requires JSON/JSONB, got {:?}", left.data_type()))
    })?;
    let key = json_key_text(right)?;
    Ok(json_object_value(json, &key).is_some())
}

pub(crate) fn json_text(value: &Value) -> Option<&str> {
    match value {
        Value::Json(text) | Value::Jsonb(text) | Value::Text(text) => Some(text.as_str()),
        _ => None,
    }
}

pub(crate) fn json_has_key_set(left: &Value, right: &Value, require_all: bool) -> Result<bool, EvalError> {
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

pub(crate) fn json_key_text(value: &Value) -> Result<String, EvalError> {
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

pub(crate) fn text_contains(left: &str, right: &str) -> bool {
    if looks_like_json_object(left) && looks_like_json_object(right) {
        return json_object_pairs(right)
            .iter()
            .all(|(key, value)| json_object_value(left, key).is_some_and(|v| v == *value));
    }
    let left_values = text_collection_values(left);
    let right_values = text_collection_values(right);
    right_values.iter().all(|v| left_values.contains(v))
}

pub(crate) fn text_collection_values(text: &str) -> Vec<String> {
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

pub(crate) fn looks_like_json_object(text: &str) -> bool {
    let trimmed = text.trim();
    trimmed.starts_with('{') && trimmed.ends_with('}') && trimmed.contains(':')
}

pub(crate) fn json_object_pairs(text: &str) -> Vec<(String, &str)> {
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

pub(crate) fn json_object_value<'a>(text: &'a str, wanted: &str) -> Option<&'a str> {
    json_object_pairs(text)
        .into_iter()
        .find_map(|(key, value)| if key == wanted { Some(value) } else { None })
}

pub(crate) fn split_loose_list(text: &str) -> Vec<&str> {
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

pub(crate) fn unquote_json_scalar(text: &str) -> &str {
    let trimmed = text.trim();
    trimmed
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(trimmed)
}

