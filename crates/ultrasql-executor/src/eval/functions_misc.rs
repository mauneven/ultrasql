//! Misc scalar builtins: null helpers, extremum, boolean tests.
//!
//! Extracted verbatim from the original `eval.rs`; pure code motion.

use super::*;

pub(crate) fn eval_zero_arg_text(args: &[Value], value: &'static str) -> Result<Value, EvalError> {
    if !args.is_empty() {
        return Err(EvalError::Type(format!(
            "zero-argument system function: expected 0 args, got {}",
            args.len()
        )));
    }
    Ok(Value::Text(value.to_owned()))
}

#[derive(Clone, Copy)]
pub(crate) enum ExtremumKind {
    Least,
    Greatest,
}

#[derive(Clone, Copy)]
pub(crate) enum NullPolicy {
    Ignore,
    Propagate,
}

#[derive(Clone, Copy)]
pub(crate) enum BooleanTest {
    True,
    NotTrue,
    False,
    NotFalse,
}

pub(crate) fn eval_ifnull(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "ifnull: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) {
        Ok(args[1].clone())
    } else {
        Ok(args[0].clone())
    }
}

pub(crate) fn eval_nullif(args: &[Value]) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "nullif: expected 2 args, got {}",
            args.len()
        )));
    }
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(args[0].clone());
    }
    if compare_values(&args[0], &args[1])? == std::cmp::Ordering::Equal {
        Ok(Value::Null)
    } else {
        Ok(args[0].clone())
    }
}

/// Lazy `COALESCE(a1, a2, …)`: evaluate arguments left-to-right and return the
/// first non-NULL, leaving the remaining arguments unevaluated — matching
/// PostgreSQL. The eager [`super::eval_function_call`] path operates on
/// already-computed `Value`s, so a fallible later argument (e.g. `a/c` when an
/// earlier argument is non-NULL) would raise there; this walks the *unevaluated*
/// `ScalarExpr` arguments instead and stops at the first non-NULL.
pub(crate) fn eval_coalesce_lazy(
    args: &[ScalarExpr],
    row: &[Value],
    params: &[Value],
) -> Result<Value, EvalError> {
    for arg in args {
        let value = eval_expr(arg, row, params)?;
        if !matches!(value, Value::Null) {
            return Ok(value);
        }
    }
    Ok(Value::Null)
}

pub(crate) fn eval_is_distinct_from(args: &[Value], negated: bool) -> Result<Value, EvalError> {
    if args.len() != 2 {
        return Err(EvalError::Type(format!(
            "is_distinct_from: expected 2 args, got {}",
            args.len()
        )));
    }
    let distinct = sql_is_distinct_from(&args[0], &args[1])?;
    Ok(Value::Bool(if negated { !distinct } else { distinct }))
}

pub(crate) fn eval_is_boolean(args: &[Value], test: BooleanTest) -> Result<Value, EvalError> {
    if args.len() != 1 {
        return Err(EvalError::Type(format!(
            "IS boolean predicate: expected 1 arg, got {}",
            args.len()
        )));
    }
    let result = match (&args[0], test) {
        (Value::Bool(true), BooleanTest::True | BooleanTest::NotFalse) => true,
        (Value::Bool(false), BooleanTest::False | BooleanTest::NotTrue) => true,
        (Value::Null, BooleanTest::NotTrue | BooleanTest::NotFalse) => true,
        (Value::Bool(_) | Value::Null, _) => false,
        (other, _) => {
            return Err(EvalError::Type(format!(
                "IS boolean predicate expected boolean input, got {:?}",
                other.data_type()
            )));
        }
    };
    Ok(Value::Bool(result))
}

pub(crate) fn eval_extremum(
    args: &[Value],
    func_name: &str,
    kind: ExtremumKind,
    null_policy: NullPolicy,
) -> Result<Value, EvalError> {
    if args.is_empty() {
        return Err(EvalError::Type(format!(
            "{func_name}: expected at least 1 arg, got 0"
        )));
    }
    let mut best: Option<Value> = None;
    for value in args {
        if matches!(value, Value::Null) {
            if matches!(null_policy, NullPolicy::Propagate) {
                return Ok(Value::Null);
            }
            continue;
        }
        let replace = match &best {
            None => true,
            Some(current) => {
                let ordering = compare_values(value, current)?;
                matches!(
                    (kind, ordering),
                    (ExtremumKind::Least, std::cmp::Ordering::Less)
                        | (ExtremumKind::Greatest, std::cmp::Ordering::Greater)
                )
            }
        };
        if replace {
            best = Some(value.clone());
        }
    }
    Ok(best.unwrap_or(Value::Null))
}
