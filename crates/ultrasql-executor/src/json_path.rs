//! SQL/JSON path subset shared by scalar JSON functions and table functions.

use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

/// Parsed SQL/JSON path expression.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonPath {
    mode: JsonPathMode,
    steps: Vec<JsonPathStep>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum JsonPathMode {
    #[default]
    Lax,
    Strict,
}

impl JsonPathMode {
    const fn is_strict(self) -> bool {
        matches!(self, Self::Strict)
    }
}

#[derive(Clone, Debug, PartialEq)]
enum JsonPathStep {
    Key(String),
    Index(usize),
    Wildcard,
    Recursive,
    Filter(JsonPathPredicate),
    Method(JsonPathMethod),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JsonPathMethod {
    Abs,
    Bigint,
    Boolean,
    Ceiling,
    Double,
    Floor,
    Integer,
    KeyValue,
    Number,
    Size,
    String,
    Type,
}

#[derive(Clone, Debug, PartialEq)]
enum JsonPathPredicate {
    Path {
        path: Vec<JsonPathStep>,
        op: Option<JsonPathCompareOp>,
        literal: Option<JsonPathLiteral>,
    },
    And(Box<JsonPathPredicate>, Box<JsonPathPredicate>),
    Or(Box<JsonPathPredicate>, Box<JsonPathPredicate>),
    Not(Box<JsonPathPredicate>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JsonPathCompareOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Clone, Debug, PartialEq)]
enum JsonPathLiteral {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    Variable(String),
}

/// Error raised while parsing a SQL/JSON path.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct JsonPathError {
    message: String,
}

impl JsonPathError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Parse a SQL/JSON path subset.
///
/// Supported shape: root `$`, dot/bracket keys, array/object wildcards,
/// recursive descent `.**`, indexes, and filters of the form
/// `? (@.path <op> literal)`.
pub fn parse_json_path(path: &str) -> Result<JsonPath, JsonPathError> {
    let mut parser = JsonPathParser::new(path);
    parser.skip_ws();
    let mode = if parser.consume_keyword("lax") {
        parser.skip_ws();
        JsonPathMode::Lax
    } else if parser.consume_keyword("strict") {
        parser.skip_ws();
        JsonPathMode::Strict
    } else {
        JsonPathMode::Lax
    };
    parser.expect_byte(b'$', "path must start with $")?;
    let steps = parser.parse_steps(false)?;
    parser.skip_ws();
    if !parser.is_eof() {
        return Err(parser.err("unsupported path syntax"));
    }
    Ok(JsonPath { mode, steps })
}

/// Select every JSON value matched by `path`.
pub fn select_json_path(
    root: &JsonValue,
    path: &JsonPath,
) -> Result<Vec<JsonValue>, JsonPathError> {
    select_json_path_with_vars(root, path, None)
}

/// Select every JSON value matched by `path`, resolving `$name` literals
/// from `vars` when predicates compare against variables.
pub fn select_json_path_with_vars(
    root: &JsonValue,
    path: &JsonPath,
    vars: Option<&JsonValue>,
) -> Result<Vec<JsonValue>, JsonPathError> {
    select_steps(vec![root.clone()], &path.steps, vars, path.mode)
}

fn select_steps(
    mut current: Vec<JsonValue>,
    steps: &[JsonPathStep],
    vars: Option<&JsonValue>,
    mode: JsonPathMode,
) -> Result<Vec<JsonValue>, JsonPathError> {
    for step in steps {
        let mut next = Vec::new();
        for item in current {
            match step {
                JsonPathStep::Key(key) => {
                    if let JsonValue::Object(object) = &item {
                        if let Some(value) = object.get(key) {
                            next.push(value.clone());
                        } else if mode.is_strict() {
                            return Err(strict_structural_error(format!("missing key {key}")));
                        }
                    } else if mode.is_strict() {
                        return Err(strict_structural_error(format!(
                            "expected object for key {key}"
                        )));
                    }
                }
                JsonPathStep::Index(index) => {
                    if let JsonValue::Array(values) = &item {
                        if let Some(value) = values.get(*index) {
                            next.push(value.clone());
                        } else if mode.is_strict() {
                            return Err(strict_structural_error(format!(
                                "array index {index} out of bounds"
                            )));
                        }
                    } else if mode.is_strict() {
                        return Err(strict_structural_error(format!(
                            "expected array for index {index}"
                        )));
                    }
                }
                JsonPathStep::Wildcard => match &item {
                    JsonValue::Array(values) => {
                        next.extend(values.iter().cloned());
                    }
                    JsonValue::Object(object) => {
                        next.extend(object.values().cloned());
                    }
                    JsonValue::Null
                    | JsonValue::Bool(_)
                    | JsonValue::Number(_)
                    | JsonValue::String(_) => {
                        if mode.is_strict() {
                            return Err(strict_structural_error(
                                "expected object or array for wildcard",
                            ));
                        }
                    }
                },
                JsonPathStep::Recursive => {
                    collect_recursive(&item, &mut next);
                }
                JsonPathStep::Filter(predicate) => {
                    if predicate_matches(&item, predicate, vars, mode)? {
                        next.push(item);
                    }
                }
                JsonPathStep::Method(method) => {
                    next.extend(apply_json_path_method(*method, &item));
                }
            }
        }
        current = next;
    }
    Ok(current)
}

fn collect_recursive(value: &JsonValue, out: &mut Vec<JsonValue>) {
    out.push(value.clone());
    match value {
        JsonValue::Array(values) => {
            for child in values {
                collect_recursive(child, out);
            }
        }
        JsonValue::Object(object) => {
            for child in object.values() {
                collect_recursive(child, out);
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {}
    }
}

fn predicate_matches(
    value: &JsonValue,
    predicate: &JsonPathPredicate,
    vars: Option<&JsonValue>,
    mode: JsonPathMode,
) -> Result<bool, JsonPathError> {
    match predicate {
        JsonPathPredicate::Path { path, op, literal } => {
            let selected = select_steps(vec![value.clone()], path, vars, mode)?;
            let Some(op) = op else {
                return Ok(!selected.is_empty());
            };
            let Some(literal) = literal else {
                return Ok(false);
            };
            Ok(selected
                .iter()
                .any(|candidate| compare_json_path_literal(candidate, *op, literal, vars)))
        }
        JsonPathPredicate::And(left, right) => Ok(predicate_matches(value, left, vars, mode)?
            && predicate_matches(value, right, vars, mode)?),
        JsonPathPredicate::Or(left, right) => Ok(predicate_matches(value, left, vars, mode)?
            || predicate_matches(value, right, vars, mode)?),
        JsonPathPredicate::Not(inner) => Ok(!predicate_matches(value, inner, vars, mode)?),
    }
}

fn strict_structural_error(message: impl Into<String>) -> JsonPathError {
    JsonPathError::new(format!(
        "strict jsonpath structural error: {}",
        message.into()
    ))
}

fn compare_json_path_literal(
    value: &JsonValue,
    op: JsonPathCompareOp,
    literal: &JsonPathLiteral,
    vars: Option<&JsonValue>,
) -> bool {
    match (value, literal) {
        (JsonValue::String(left), JsonPathLiteral::String(right)) => compare_ord(left, op, right),
        (JsonValue::Number(left), JsonPathLiteral::Number(right)) => left
            .as_f64()
            .is_some_and(|left| compare_f64(left, op, *right)),
        (JsonValue::Bool(left), JsonPathLiteral::Bool(right)) => compare_ord(left, op, right),
        (JsonValue::Null, JsonPathLiteral::Null) => {
            matches!(
                op,
                JsonPathCompareOp::Eq | JsonPathCompareOp::LtEq | JsonPathCompareOp::GtEq
            )
        }
        (_, JsonPathLiteral::Variable(name)) => vars
            .and_then(|vars| vars.get(name))
            .is_some_and(|right| compare_json_value(value, op, right)),
        _ => false,
    }
}

fn compare_json_value(left: &JsonValue, op: JsonPathCompareOp, right: &JsonValue) -> bool {
    match (left, right) {
        (JsonValue::String(left), JsonValue::String(right)) => compare_ord(left, op, right),
        (JsonValue::Number(left), JsonValue::Number(right)) => left
            .as_f64()
            .zip(right.as_f64())
            .is_some_and(|(left, right)| compare_f64(left, op, right)),
        (JsonValue::Bool(left), JsonValue::Bool(right)) => compare_ord(left, op, right),
        (JsonValue::Null, JsonValue::Null) => {
            matches!(
                op,
                JsonPathCompareOp::Eq | JsonPathCompareOp::LtEq | JsonPathCompareOp::GtEq
            )
        }
        _ => false,
    }
}

fn apply_json_path_method(method: JsonPathMethod, value: &JsonValue) -> Vec<JsonValue> {
    match method {
        JsonPathMethod::Abs => vec![json_path_numeric_method(value, f64::abs)],
        JsonPathMethod::Bigint => vec![
            json_path_bigint(value)
                .map(JsonNumber::from)
                .map_or(JsonValue::Null, JsonValue::Number),
        ],
        JsonPathMethod::Boolean => {
            vec![json_path_boolean(value).map_or(JsonValue::Null, JsonValue::Bool)]
        }
        JsonPathMethod::Ceiling => vec![json_path_numeric_method(value, f64::ceil)],
        JsonPathMethod::Double => vec![json_path_double(value)],
        JsonPathMethod::Floor => vec![json_path_numeric_method(value, f64::floor)],
        JsonPathMethod::Integer => vec![
            json_path_integer(value)
                .map(JsonNumber::from)
                .map_or(JsonValue::Null, JsonValue::Number),
        ],
        JsonPathMethod::KeyValue => json_path_keyvalue(value),
        JsonPathMethod::Number => vec![json_path_number_from_f64(json_path_f64(value))],
        JsonPathMethod::Size => {
            let size = match value {
                JsonValue::Array(values) => values.len(),
                JsonValue::Object(object) => object.len(),
                JsonValue::Null
                | JsonValue::Bool(_)
                | JsonValue::Number(_)
                | JsonValue::String(_) => 1,
            };
            vec![JsonValue::Number(JsonNumber::from(
                u64::try_from(size).unwrap_or(u64::MAX),
            ))]
        }
        JsonPathMethod::String => {
            vec![json_path_string(value).map_or(JsonValue::Null, JsonValue::String)]
        }
        JsonPathMethod::Type => vec![JsonValue::String(json_path_type_name(value).to_owned())],
    }
}

fn json_path_numeric_method(value: &JsonValue, op: impl FnOnce(f64) -> f64) -> JsonValue {
    json_path_number_from_f64(json_path_f64(value).map(op))
}

fn json_path_double(value: &JsonValue) -> JsonValue {
    json_path_number_from_f64(json_path_f64(value))
}

fn json_path_boolean(value: &JsonValue) -> Option<bool> {
    match value {
        JsonValue::Bool(value) => Some(*value),
        JsonValue::Number(number) => json_number_to_bool(number),
        JsonValue::String(text) => json_string_to_bool(text),
        JsonValue::Null | JsonValue::Array(_) | JsonValue::Object(_) => None,
    }
}

fn json_path_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::Bool(value) => Some(value.to_string()),
        JsonValue::Number(number) => Some(number.to_string()),
        JsonValue::String(text) => Some(text.clone()),
        JsonValue::Null | JsonValue::Array(_) | JsonValue::Object(_) => None,
    }
}

fn json_path_integer(value: &JsonValue) -> Option<i32> {
    json_path_bigint(value).and_then(|value| i32::try_from(value).ok())
}

fn json_path_bigint(value: &JsonValue) -> Option<i64> {
    match value {
        JsonValue::Number(number) => json_number_to_i64(number),
        JsonValue::String(text) => text.trim().parse::<i64>().ok(),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Array(_) | JsonValue::Object(_) => None,
    }
}

fn json_path_keyvalue(value: &JsonValue) -> Vec<JsonValue> {
    let JsonValue::Object(object) = value else {
        return Vec::new();
    };
    object
        .iter()
        .map(|(key, value)| {
            let mut row = JsonMap::new();
            row.insert("id".to_owned(), JsonValue::Number(JsonNumber::from(0_i64)));
            row.insert("key".to_owned(), JsonValue::String(key.clone()));
            row.insert("value".to_owned(), value.clone());
            JsonValue::Object(row)
        })
        .collect()
}

fn json_path_f64(value: &JsonValue) -> Option<f64> {
    let parsed = match value {
        JsonValue::Number(number) => number.as_f64(),
        JsonValue::String(text) => text.parse::<f64>().ok(),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Array(_) | JsonValue::Object(_) => None,
    }?;
    parsed.is_finite().then_some(parsed)
}

fn json_path_number_from_f64(value: Option<f64>) -> JsonValue {
    value
        .and_then(JsonNumber::from_f64)
        .map_or(JsonValue::Null, JsonValue::Number)
}

fn json_number_to_bool(number: &JsonNumber) -> Option<bool> {
    if let Some(value) = number.as_i64() {
        return Some(value != 0);
    }
    if let Some(value) = number.as_u64() {
        return Some(value != 0);
    }
    let value = number.as_f64()?;
    value.is_finite().then_some(value != 0.0)
}

fn json_string_to_bool(text: &str) -> Option<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Some(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Some(false),
        _ => None,
    }
}

fn json_number_to_i64(number: &JsonNumber) -> Option<i64> {
    if let Some(value) = number.as_i64() {
        return Some(value);
    }
    if let Some(value) = number.as_u64() {
        return i64::try_from(value).ok();
    }
    number.to_string().parse::<i64>().ok()
}

fn json_path_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn compare_ord<T: Ord>(left: &T, op: JsonPathCompareOp, right: &T) -> bool {
    match op {
        JsonPathCompareOp::Eq => left == right,
        JsonPathCompareOp::NotEq => left != right,
        JsonPathCompareOp::Lt => left < right,
        JsonPathCompareOp::LtEq => left <= right,
        JsonPathCompareOp::Gt => left > right,
        JsonPathCompareOp::GtEq => left >= right,
    }
}

fn compare_f64(left: f64, op: JsonPathCompareOp, right: f64) -> bool {
    if left.is_nan() || right.is_nan() {
        return false;
    }
    match op {
        JsonPathCompareOp::Eq => left == right,
        JsonPathCompareOp::NotEq => left != right,
        JsonPathCompareOp::Lt => left < right,
        JsonPathCompareOp::LtEq => left <= right,
        JsonPathCompareOp::Gt => left > right,
        JsonPathCompareOp::GtEq => left >= right,
    }
}

struct JsonPathParser<'a> {
    text: &'a str,
    pos: usize,
}

impl<'a> JsonPathParser<'a> {
    fn new(text: &'a str) -> Self {
        Self { text, pos: 0 }
    }

    fn parse_steps(&mut self, relative: bool) -> Result<Vec<JsonPathStep>, JsonPathError> {
        let mut steps = Vec::new();
        loop {
            self.skip_ws();
            if self.is_eof()
                || (relative && (self.peek_byte() == Some(b')') || self.starts_compare_op()))
                || (relative && self.starts_boolean_op())
            {
                return Ok(steps);
            }
            match self.peek_byte() {
                Some(b'.') => {
                    self.pos += 1;
                    if self.consume_byte(b'*') {
                        if self.consume_byte(b'*') {
                            steps.push(JsonPathStep::Recursive);
                        } else {
                            steps.push(JsonPathStep::Wildcard);
                        }
                    } else if self.consume_byte(b'"') {
                        steps.push(JsonPathStep::Key(self.parse_quoted_string_body()?));
                    } else {
                        let identifier = self.parse_identifier()?;
                        if self.consume_byte(b'(') {
                            self.skip_ws();
                            self.expect_byte(b')', "expected ) after jsonpath method")?;
                            steps.push(JsonPathStep::Method(json_path_method(&identifier)?));
                        } else {
                            steps.push(JsonPathStep::Key(identifier));
                        }
                    }
                }
                Some(b'[') => steps.push(self.parse_bracket_step()?),
                Some(b'?') => steps.push(JsonPathStep::Filter(self.parse_filter()?)),
                _ => return Err(self.err("unsupported path syntax")),
            }
        }
    }

    fn parse_bracket_step(&mut self) -> Result<JsonPathStep, JsonPathError> {
        self.expect_byte(b'[', "expected [")?;
        self.skip_ws();
        if self.consume_byte(b'*') {
            self.skip_ws();
            self.expect_byte(b']', "expected ]")?;
            return Ok(JsonPathStep::Wildcard);
        }
        if self.consume_byte(b'"') {
            let key = self.parse_quoted_string_body()?;
            self.skip_ws();
            self.expect_byte(b']', "expected ]")?;
            return Ok(JsonPathStep::Key(key));
        }
        let start = self.pos;
        while self.peek_byte().is_some_and(|byte| byte.is_ascii_digit()) {
            self.pos += 1;
        }
        if start == self.pos {
            return Err(self.err("expected array index"));
        }
        let index = self.text[start..self.pos]
            .parse::<usize>()
            .map_err(|err| self.err(format!("bad array index: {err}")))?;
        self.skip_ws();
        self.expect_byte(b']', "expected ]")?;
        Ok(JsonPathStep::Index(index))
    }

    fn parse_filter(&mut self) -> Result<JsonPathPredicate, JsonPathError> {
        self.expect_byte(b'?', "expected ?")?;
        self.skip_ws();
        self.expect_byte(b'(', "expected ( after ?")?;
        self.skip_ws();
        let predicate = self.parse_predicate_expr()?;
        self.skip_ws();
        self.expect_byte(b')', "expected )")?;
        Ok(predicate)
    }

    fn parse_predicate_expr(&mut self) -> Result<JsonPathPredicate, JsonPathError> {
        self.parse_predicate_or()
    }

    fn parse_predicate_or(&mut self) -> Result<JsonPathPredicate, JsonPathError> {
        let mut predicate = self.parse_predicate_and()?;
        loop {
            self.skip_ws();
            if !self.consume_token("||") {
                return Ok(predicate);
            }
            let right = self.parse_predicate_and()?;
            predicate = JsonPathPredicate::Or(Box::new(predicate), Box::new(right));
        }
    }

    fn parse_predicate_and(&mut self) -> Result<JsonPathPredicate, JsonPathError> {
        let mut predicate = self.parse_predicate_not()?;
        loop {
            self.skip_ws();
            if !self.consume_token("&&") {
                return Ok(predicate);
            }
            let right = self.parse_predicate_not()?;
            predicate = JsonPathPredicate::And(Box::new(predicate), Box::new(right));
        }
    }

    fn parse_predicate_not(&mut self) -> Result<JsonPathPredicate, JsonPathError> {
        self.skip_ws();
        if self.consume_byte(b'!') {
            return self
                .parse_predicate_not()
                .map(|predicate| JsonPathPredicate::Not(Box::new(predicate)));
        }
        self.parse_predicate_atom()
    }

    fn parse_predicate_atom(&mut self) -> Result<JsonPathPredicate, JsonPathError> {
        self.skip_ws();
        if self.consume_byte(b'(') {
            let predicate = self.parse_predicate_expr()?;
            self.skip_ws();
            self.expect_byte(b')', "expected ) in predicate")?;
            return Ok(predicate);
        }
        self.expect_byte(b'@', "filter path must start with @")?;
        let path = self.parse_steps(true)?;
        self.skip_ws();
        let op = self.parse_compare_op();
        let literal = if op.is_some() {
            self.skip_ws();
            Some(self.parse_literal()?)
        } else {
            None
        };
        self.skip_ws();
        Ok(JsonPathPredicate::Path { path, op, literal })
    }

    fn parse_compare_op(&mut self) -> Option<JsonPathCompareOp> {
        for (token, op) in [
            ("==", JsonPathCompareOp::Eq),
            ("!=", JsonPathCompareOp::NotEq),
            (">=", JsonPathCompareOp::GtEq),
            ("<=", JsonPathCompareOp::LtEq),
            (">", JsonPathCompareOp::Gt),
            ("<", JsonPathCompareOp::Lt),
        ] {
            if self.text[self.pos..].starts_with(token) {
                self.pos += token.len();
                return Some(op);
            }
        }
        None
    }

    fn starts_compare_op(&self) -> bool {
        [">=", "<=", "==", "!=", ">", "<"]
            .iter()
            .any(|token| self.text[self.pos..].starts_with(token))
    }

    fn starts_boolean_op(&self) -> bool {
        self.text[self.pos..].starts_with("&&") || self.text[self.pos..].starts_with("||")
    }

    fn parse_literal(&mut self) -> Result<JsonPathLiteral, JsonPathError> {
        if self.consume_byte(b'"') {
            return self.parse_quoted_string_body().map(JsonPathLiteral::String);
        }
        if self.consume_byte(b'$') {
            return self.parse_identifier().map(JsonPathLiteral::Variable);
        }
        for (token, literal) in [
            ("true", JsonPathLiteral::Bool(true)),
            ("false", JsonPathLiteral::Bool(false)),
            ("null", JsonPathLiteral::Null),
        ] {
            if self.text[self.pos..].starts_with(token) {
                self.pos += token.len();
                return Ok(literal);
            }
        }
        let start = self.pos;
        if self.consume_byte(b'-') && !self.peek_byte().is_some_and(|byte| byte.is_ascii_digit()) {
            return Err(self.err("expected number after -"));
        }
        while self.peek_byte().is_some_and(|byte| byte.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.consume_byte(b'.') {
            while self.peek_byte().is_some_and(|byte| byte.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        if start == self.pos {
            return Err(self.err("expected literal"));
        }
        self.text[start..self.pos]
            .parse::<f64>()
            .map(JsonPathLiteral::Number)
            .map_err(|err| self.err(format!("bad number literal: {err}")))
    }

    fn parse_identifier(&mut self) -> Result<String, JsonPathError> {
        let start = self.pos;
        while self
            .peek_byte()
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            self.pos += 1;
        }
        if start == self.pos {
            return Err(self.err("empty object key"));
        }
        Ok(self.text[start..self.pos].to_owned())
    }

    fn parse_quoted_string_body(&mut self) -> Result<String, JsonPathError> {
        let mut out = String::new();
        while let Some(ch) = self.next_char() {
            match ch {
                '"' => return Ok(out),
                '\\' => {
                    let escaped = self
                        .next_char()
                        .ok_or_else(|| self.err("unterminated escape"))?;
                    match escaped {
                        '"' | '\\' | '/' => out.push(escaped),
                        'b' => out.push('\u{0008}'),
                        'f' => out.push('\u{000c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => out.push(self.parse_unicode_escape()?),
                        _ => return Err(self.err("unsupported string escape")),
                    }
                }
                _ => out.push(ch),
            }
        }
        Err(self.err("unterminated quoted string"))
    }

    fn parse_unicode_escape(&mut self) -> Result<char, JsonPathError> {
        let code = self.parse_hex4()?;
        if (0xD800..=0xDBFF).contains(&code) {
            if self.consume_byte(b'\\') && self.consume_byte(b'u') {
                let low = self.parse_hex4()?;
                if (0xDC00..=0xDFFF).contains(&low) {
                    let scalar = 0x10000 + (((code - 0xD800) << 10) | (low - 0xDC00));
                    return char::from_u32(scalar)
                        .ok_or_else(|| self.err("invalid unicode escape"));
                }
            }
            return Err(self.err("invalid unicode surrogate pair"));
        }
        if (0xDC00..=0xDFFF).contains(&code) {
            return Err(self.err("unexpected low unicode surrogate"));
        }
        char::from_u32(code).ok_or_else(|| self.err("invalid unicode escape"))
    }

    fn parse_hex4(&mut self) -> Result<u32, JsonPathError> {
        let bytes = self.text.as_bytes();
        if self.pos.saturating_add(4) > bytes.len() {
            return Err(self.err("incomplete unicode escape"));
        }
        let mut value = 0_u32;
        for idx in self.pos..self.pos + 4 {
            let digit = match bytes[idx] {
                b'0'..=b'9' => u32::from(bytes[idx] - b'0'),
                b'a'..=b'f' => u32::from(bytes[idx] - b'a' + 10),
                b'A'..=b'F' => u32::from(bytes[idx] - b'A' + 10),
                _ => return Err(self.err("invalid unicode escape digit")),
            };
            value = (value << 4) | digit;
        }
        self.pos += 4;
        Ok(value)
    }

    fn skip_ws(&mut self) {
        while self
            .peek_byte()
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            self.pos += 1;
        }
    }

    fn expect_byte(&mut self, expected: u8, message: &str) -> Result<(), JsonPathError> {
        if self.consume_byte(expected) {
            Ok(())
        } else {
            Err(self.err(message))
        }
    }

    fn consume_byte(&mut self, expected: u8) -> bool {
        if self.peek_byte() == Some(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn consume_token(&mut self, token: &str) -> bool {
        if self.text[self.pos..].starts_with(token) {
            self.pos += token.len();
            true
        } else {
            false
        }
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        let rest = &self.text[self.pos..];
        if !rest.starts_with(keyword) {
            return false;
        }
        let end = self.pos + keyword.len();
        if self
            .text
            .as_bytes()
            .get(end)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            return false;
        }
        self.pos = end;
        true
    }

    fn peek_byte(&self) -> Option<u8> {
        self.text.as_bytes().get(self.pos).copied()
    }

    fn next_char(&mut self) -> Option<char> {
        let ch = self.text[self.pos..].chars().next()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.text.len()
    }

    fn err(&self, message: impl Into<String>) -> JsonPathError {
        JsonPathError::new(format!("{} at byte {}", message.into(), self.pos))
    }
}

fn json_path_method(name: &str) -> Result<JsonPathMethod, JsonPathError> {
    match name {
        "abs" => Ok(JsonPathMethod::Abs),
        "bigint" => Ok(JsonPathMethod::Bigint),
        "boolean" => Ok(JsonPathMethod::Boolean),
        "ceiling" => Ok(JsonPathMethod::Ceiling),
        "double" => Ok(JsonPathMethod::Double),
        "floor" => Ok(JsonPathMethod::Floor),
        "integer" => Ok(JsonPathMethod::Integer),
        "keyvalue" => Ok(JsonPathMethod::KeyValue),
        "number" => Ok(JsonPathMethod::Number),
        "size" => Ok(JsonPathMethod::Size),
        "string" => Ok(JsonPathMethod::String),
        "type" => Ok(JsonPathMethod::Type),
        _ => Err(JsonPathError::new(format!(
            "unsupported jsonpath method {name}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn select(root: &JsonValue, path: &JsonPath) -> Vec<JsonValue> {
        select_json_path(root, path).expect("json path select")
    }

    #[test]
    fn path_supports_filters_quoted_keys_and_recursive_descent() {
        let document = serde_json::json!({
            "items": [
                {"id": 1, "score": 12, "meta": {"kind": "guide"}},
                {"id": 2, "score": 25, "meta": {"kind": "paper"}},
                {"id": 3, "score": 31, "meta": {"kind": "guide"}}
            ],
            "weird-key": {"id": 9},
            "snowman-\u{2603}": {"id": 10}
        });

        let path = parse_json_path(r#"$.items[*] ? (@.meta.kind == "guide").id"#).unwrap();
        let got = select(&document, &path);
        assert_eq!(got, vec![serde_json::json!(1), serde_json::json!(3)]);

        let quoted = parse_json_path(r#"$."weird-key".id"#).unwrap();
        assert_eq!(select(&document, &quoted), vec![serde_json::json!(9)]);

        let escaped = parse_json_path(r#"$."snowman-\u2603".id"#).unwrap();
        assert_eq!(select(&document, &escaped), vec![serde_json::json!(10)]);

        let recursive = parse_json_path("$.**.kind").unwrap();
        let mut got = select(&document, &recursive);
        got.sort_by_key(JsonValue::to_string);
        assert_eq!(
            got,
            vec![
                serde_json::json!("guide"),
                serde_json::json!("guide"),
                serde_json::json!("paper"),
            ]
        );
    }

    #[test]
    fn path_supports_basic_methods() {
        let document = serde_json::json!({
            "items": [1, 2, 3],
            "meta": {"ok": true},
            "scalar": 7
        });

        let size = parse_json_path("$.items.size()").unwrap();
        assert_eq!(select(&document, &size), vec![serde_json::json!(3)]);

        let object_type = parse_json_path("$.meta.type()").unwrap();
        assert_eq!(
            select(&document, &object_type),
            vec![serde_json::json!("object")]
        );

        let scalar_size = parse_json_path("$.scalar.size()").unwrap();
        assert_eq!(select(&document, &scalar_size), vec![serde_json::json!(1)]);
    }

    #[test]
    fn path_supports_numeric_methods() {
        let document = serde_json::json!({
            "negative": -7.5,
            "floor": 2.7,
            "ceiling": 2.2,
            "text": "3.5",
            "bad": "nan"
        });

        let abs = parse_json_path("$.negative.abs()").unwrap();
        assert_eq!(select(&document, &abs), vec![serde_json::json!(7.5)]);

        let floor = parse_json_path("$.floor.floor()").unwrap();
        assert_eq!(select(&document, &floor), vec![serde_json::json!(2.0)]);

        let ceiling = parse_json_path("$.ceiling.ceiling()").unwrap();
        assert_eq!(select(&document, &ceiling), vec![serde_json::json!(3.0)]);

        let double = parse_json_path("$.text.double()").unwrap();
        assert_eq!(select(&document, &double), vec![serde_json::json!(3.5)]);

        let bad = parse_json_path("$.bad.double()").unwrap();
        assert_eq!(select(&document, &bad), vec![serde_json::Value::Null]);
    }

    #[test]
    fn path_supports_conversion_methods() {
        let document = serde_json::json!({
            "truthy_number": 1,
            "falsey_number": 0,
            "truthy_text": "yes",
            "falsey_text": "off",
            "text": 12.5,
            "numeric_text": "123.45",
            "integer_text": "12345",
            "bigint_text": "9876543219",
            "bad": "maybe"
        });

        let truthy_number = parse_json_path("$.truthy_number.boolean()").unwrap();
        assert_eq!(
            select(&document, &truthy_number),
            vec![serde_json::json!(true)]
        );

        let falsey_number = parse_json_path("$.falsey_number.boolean()").unwrap();
        assert_eq!(
            select(&document, &falsey_number),
            vec![serde_json::json!(false)]
        );

        let truthy_text = parse_json_path("$.truthy_text.boolean()").unwrap();
        assert_eq!(
            select(&document, &truthy_text),
            vec![serde_json::json!(true)]
        );

        let falsey_text = parse_json_path("$.falsey_text.boolean()").unwrap();
        assert_eq!(
            select(&document, &falsey_text),
            vec![serde_json::json!(false)]
        );

        let string = parse_json_path("$.text.string()").unwrap();
        assert_eq!(select(&document, &string), vec![serde_json::json!("12.5")]);

        let number = parse_json_path("$.numeric_text.number()").unwrap();
        assert_eq!(select(&document, &number), vec![serde_json::json!(123.45)]);

        let integer = parse_json_path("$.integer_text.integer()").unwrap();
        assert_eq!(select(&document, &integer), vec![serde_json::json!(12345)]);

        let bigint = parse_json_path("$.bigint_text.bigint()").unwrap();
        assert_eq!(
            select(&document, &bigint),
            vec![serde_json::json!(9_876_543_219_i64)]
        );

        let bad_bool = parse_json_path("$.bad.boolean()").unwrap();
        assert_eq!(select(&document, &bad_bool), vec![serde_json::Value::Null]);
    }

    #[test]
    fn path_supports_keyvalue_method() {
        let document = serde_json::json!({
            "object": {"x": "20", "y": 32}
        });

        let keyvalue = parse_json_path("$.object.keyvalue()").unwrap();
        let mut got = select(&document, &keyvalue);
        got.sort_by_key(JsonValue::to_string);

        assert_eq!(
            got,
            vec![
                serde_json::json!({"id": 0, "key": "x", "value": "20"}),
                serde_json::json!({"id": 0, "key": "y", "value": 32}),
            ]
        );
    }

    #[test]
    fn path_supports_predicate_boolean_algebra() {
        let document = serde_json::json!({
            "items": [
                {"id": 1, "score": 12, "meta": {"kind": "guide"}},
                {"id": 2, "score": 25, "meta": {"kind": "paper"}},
                {"id": 3, "score": 31, "meta": {"kind": "guide"}}
            ]
        });

        let and_path =
            parse_json_path(r#"$.items[*] ? (@.score >= 20 && @.meta.kind == "guide").id"#)
                .unwrap();
        assert_eq!(select(&document, &and_path), vec![serde_json::json!(3)]);

        let or_path =
            parse_json_path(r#"$.items[*] ? (@.score < 15 || @.meta.kind == "paper").id"#).unwrap();
        assert_eq!(
            select(&document, &or_path),
            vec![serde_json::json!(1), serde_json::json!(2)]
        );

        let not_path = parse_json_path(r#"$.items[*] ? (!(@.meta.kind == "paper")).id"#).unwrap();
        assert_eq!(
            select(&document, &not_path),
            vec![serde_json::json!(1), serde_json::json!(3)]
        );
    }

    #[test]
    fn strict_mode_reports_structural_errors() {
        let document = serde_json::json!({"items": [{}]});

        let lax = parse_json_path("lax $.items[*].missing").unwrap();
        assert_eq!(select(&document, &lax), Vec::<JsonValue>::new());

        let strict = parse_json_path("strict $.items[*].missing").unwrap();
        let err = select_json_path(&document, &strict).expect_err("strict path errors");
        assert_eq!(
            err.to_string(),
            "strict jsonpath structural error: missing key missing"
        );
    }
}
