//! SQL/JSON path subset shared by scalar JSON functions and table functions.

use serde_json::Value as JsonValue;

/// Parsed SQL/JSON path expression.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonPath {
    steps: Vec<JsonPathStep>,
}

#[derive(Clone, Debug, PartialEq)]
enum JsonPathStep {
    Key(String),
    Index(usize),
    Wildcard,
    Recursive,
    Filter(JsonPathPredicate),
}

#[derive(Clone, Debug, PartialEq)]
struct JsonPathPredicate {
    path: Vec<JsonPathStep>,
    op: Option<JsonPathCompareOp>,
    literal: Option<JsonPathLiteral>,
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
    if parser.consume_keyword("lax") || parser.consume_keyword("strict") {
        parser.skip_ws();
    }
    parser.expect_byte(b'$', "path must start with $")?;
    let steps = parser.parse_steps(false)?;
    parser.skip_ws();
    if !parser.is_eof() {
        return Err(parser.err("unsupported path syntax"));
    }
    Ok(JsonPath { steps })
}

/// Select every JSON value matched by `path`.
#[must_use]
pub fn select_json_path<'a>(root: &'a JsonValue, path: &JsonPath) -> Vec<&'a JsonValue> {
    select_json_path_with_vars(root, path, None)
}

/// Select every JSON value matched by `path`, resolving `$name` literals
/// from `vars` when predicates compare against variables.
#[must_use]
pub fn select_json_path_with_vars<'a>(
    root: &'a JsonValue,
    path: &JsonPath,
    vars: Option<&JsonValue>,
) -> Vec<&'a JsonValue> {
    select_steps(vec![root], &path.steps, vars)
}

fn select_steps<'a>(
    mut current: Vec<&'a JsonValue>,
    steps: &[JsonPathStep],
    vars: Option<&JsonValue>,
) -> Vec<&'a JsonValue> {
    for step in steps {
        let mut next = Vec::new();
        for value in current {
            match (step, value) {
                (JsonPathStep::Key(key), JsonValue::Object(object)) => {
                    if let Some(value) = object.get(key) {
                        next.push(value);
                    }
                }
                (JsonPathStep::Index(index), JsonValue::Array(values)) => {
                    if let Some(value) = values.get(*index) {
                        next.push(value);
                    }
                }
                (JsonPathStep::Wildcard, JsonValue::Array(values)) => next.extend(values),
                (JsonPathStep::Wildcard, JsonValue::Object(object)) => {
                    next.extend(object.values());
                }
                (JsonPathStep::Recursive, _) => collect_recursive(value, &mut next),
                (JsonPathStep::Filter(predicate), _)
                    if predicate_matches(value, predicate, vars) =>
                {
                    next.push(value);
                }
                _ => {}
            }
        }
        current = next;
    }
    current
}

fn collect_recursive<'a>(value: &'a JsonValue, out: &mut Vec<&'a JsonValue>) {
    out.push(value);
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
) -> bool {
    let selected = select_steps(vec![value], &predicate.path, vars);
    let Some(op) = predicate.op else {
        return !selected.is_empty();
    };
    let Some(literal) = &predicate.literal else {
        return false;
    };
    selected
        .iter()
        .any(|candidate| compare_json_path_literal(candidate, op, literal, vars))
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
                        steps.push(JsonPathStep::Key(self.parse_identifier()?));
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
        self.expect_byte(b')', "expected )")?;
        Ok(JsonPathPredicate { path, op, literal })
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let got: Vec<_> = select_json_path(&document, &path)
            .into_iter()
            .cloned()
            .collect();
        assert_eq!(got, vec![serde_json::json!(1), serde_json::json!(3)]);

        let quoted = parse_json_path(r#"$."weird-key".id"#).unwrap();
        assert_eq!(
            select_json_path(&document, &quoted),
            vec![&serde_json::json!(9)]
        );

        let escaped = parse_json_path(r#"$."snowman-\u2603".id"#).unwrap();
        assert_eq!(
            select_json_path(&document, &escaped),
            vec![&serde_json::json!(10)]
        );

        let recursive = parse_json_path("$.**.kind").unwrap();
        let mut got: Vec<_> = select_json_path(&document, &recursive)
            .into_iter()
            .cloned()
            .collect();
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
}
