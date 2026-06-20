//! JSON, XML, and network scalar builtins.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

pub(crate) fn eval_json_build_object(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_row_constructor(
    args: &[Value],
    return_type: &DataType,
) -> Result<Value, EvalError> {
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

pub(crate) fn eval_row_to_json(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_jsonb_set(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_jsonb_path_exists(args: &[Value]) -> Result<Value, EvalError> {
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
pub(crate) enum XmlWellFormedMode {
    Content,
    Document,
}

pub(crate) fn eval_xml_is_well_formed(
    args: &[Value],
    mode: XmlWellFormedMode,
) -> Result<Value, EvalError> {
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

pub(crate) fn eval_xmlparse(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_xmlserialize(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn parse_xml_value(
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

pub(crate) const XPATH_SUPPORTED_SUBSET: &str = concat!(
    "supported subset is absolute element paths with optional @attr equality, ",
    "direct text()/child equality predicates, position predicates, wildcards, ",
    "text(), true(), false(), count(), string(), boolean(), not(), name(), ",
    "local-name(), normalize-space(), string-length(), contains(), ",
    "starts-with(), substring-before(), substring-after(), concat(), ",
    "number(), floor(), ceiling(), round(), sum(), namespaces, descendant ",
    "paths, and basic child::, attribute::, descendant::, and self::node() axes"
);

pub(crate) fn eval_xpath_exists(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_xpath(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn xpath_namespace_arg(
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

pub(crate) fn xml_namespace_text<'a>(
    function: &str,
    value: &'a Value,
) -> Result<Option<&'a str>, EvalError> {
    match value {
        Value::Text(text) => Ok(Some(text.as_str())),
        Value::Null => Ok(None),
        other => Err(EvalError::Type(format!(
            "{function}: namespace values must be text, got {:?}",
            other.data_type()
        ))),
    }
}

pub(crate) fn eval_network_host(args: &[Value]) -> Result<Value, EvalError> {
    let Some(addr) = network_inet_arg("host", args)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Text(addr.addr().to_string()))
}

pub(crate) fn eval_network_family(args: &[Value]) -> Result<Value, EvalError> {
    let Some(addr) = network_inet_arg("family", args)? else {
        return Ok(Value::Null);
    };
    let family = if addr.max_prefix() == 32 { 4 } else { 6 };
    Ok(Value::Int32(family))
}

pub(crate) fn eval_network_masklen(args: &[Value]) -> Result<Value, EvalError> {
    let Some(addr) = network_inet_arg("masklen", args)? else {
        return Ok(Value::Null);
    };
    Ok(Value::Int32(i32::from(addr.prefix())))
}

pub(crate) fn network_inet_arg(
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

pub(crate) fn xml_text_arg<'a>(
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

pub(crate) fn xml_mode_arg(
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

pub(crate) fn json_document_arg(
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

pub(crate) fn json_path_arg(
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

pub(crate) fn parse_json_path_text(text: &str) -> Vec<String> {
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

pub(crate) fn set_json_path(
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

pub(crate) fn set_json_leaf(
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

pub(crate) fn json_value_to_jsonb(
    value: JsonValue,
    function: &'static str,
) -> Result<Value, EvalError> {
    serde_json::to_string(&value)
        .map(Value::Jsonb)
        .map_err(|err| EvalError::Type(format!("{function}: encode failed: {err}")))
}

pub(crate) fn sql_value_to_json(value: &Value) -> JsonValue {
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

pub(crate) fn quote_identifier(identifier: &str) -> String {
    if is_unquoted_identifier(identifier) && !is_reserved_identifier(identifier) {
        return identifier.to_owned();
    }
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(crate) fn is_unquoted_identifier(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_lowercase()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch == '$' || ch.is_ascii_lowercase() || ch.is_ascii_digit())
}

pub(crate) fn is_reserved_identifier(identifier: &str) -> bool {
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

pub(crate) fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
