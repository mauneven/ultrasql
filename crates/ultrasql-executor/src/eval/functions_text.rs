//! `format`/`regexp_replace` text builtins.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

pub(crate) fn eval_format(args: &[Value]) -> Result<Value, EvalError> {
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

pub(crate) fn eval_regexp_replace(args: &[Value]) -> Result<Value, EvalError> {
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
    // PostgreSQL flag semantics: `i` => case-insensitive, `g` => replace all
    // (handled below — not a `regex` builder option), `m` => `^`/`$` match at
    // line boundaries. `g` must NOT be forwarded into the regex engine.
    let case_insensitive = flags.contains('i');
    let multi_line = flags.contains('m');
    let regex = super::regex_cache::cached_regex_with(pattern, case_insensitive, multi_line)
        .map_err(|err| EvalError::Type(format!("regexp_replace: invalid pattern: {err}")))?;
    let replaced = if flags.contains('g') {
        regex.replace_all(text, replacement)
    } else {
        regex.replace(text, replacement)
    };
    Ok(Value::Text(replaced.into_owned()))
}

pub(crate) fn format_value_text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.clone(),
        other => value_to_pg_output_text(other),
    }
}

/// Render a value the way PostgreSQL's *type output function* does, which is
/// what `concat`/`concat_ws`/`format`/`array_to_string` use for each element.
///
/// This differs from the explicit `::text` cast for `boolean`: `true::text`
/// is `'true'`, but `boolout` (the output function) yields `'t'`/`'f'`, so
/// `concat('x', true)` is `'xt'` and `format('%s', false)` is `'f'`. Every
/// other type's output function matches its `Display`, so we only special-case
/// `Value::Bool` and defer to `to_string()` otherwise.
pub(crate) fn value_to_pg_output_text(value: &Value) -> String {
    match value {
        Value::Bool(b) => if *b { "t" } else { "f" }.to_owned(),
        other => other.to_string(),
    }
}
