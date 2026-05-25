//! Extended-query message handlers — Parse, Bind, Describe (Statement/Portal),
//! and Close. Each handler is invoked by the per-session state machine in
//! [`crate::session`] after the wire codec decodes a frontend message.

use ultrasql_core::{DataType, Value};
use ultrasql_parser::Parser;
use ultrasql_planner::{PlanError, bind};
use ultrasql_protocol::{BackendMessage, DescribeKind};

use crate::error::ServerError;
use crate::workload::plan_hash_for_plan;

use super::codec::{
    DecodeError, decode_param, pg_type_oid, row_description_for_plan,
    row_description_for_plan_with_formats,
};
use super::params::{count_parameters_in_plan, infer_parameter_types};
use super::substitute::substitute_parameters_in_plan;
use super::{BoundPortal, ExtendedConnState, PreparedStatement};

// ---------------------------------------------------------------------------
// Parse: parse + bind + count parameters.
// ---------------------------------------------------------------------------

/// Handle a `Parse` message.
///
/// Parses the SQL, binds it against `bind_ctx`, counts the parameters
/// in the bound plan, and installs the [`PreparedStatement`] into
/// `state.statements` under `name`. Returns the single
/// [`BackendMessage::ParseComplete`] the caller should send.
///
/// # Errors
///
/// Propagates parser, binder, and planner errors. The caller is
/// expected to wrap them in `ErrorResponse` and call
/// [`ExtendedConnState::mark_failed`].
pub fn handle_parse(
    state: &mut ExtendedConnState,
    name: String,
    sql: String,
    param_type_oids: Vec<u32>,
    bind_ctx: &dyn ultrasql_planner::Catalog,
) -> Result<BackendMessage, ServerError> {
    let trimmed = sql.trim();
    let (plan, n_params, limit_offset_param_indexes) = if trimmed.is_empty() || trimmed == ";" {
        (None, 0, Vec::new())
    } else {
        let stmt = Parser::new(trimmed).parse_statement()?;
        match bind(&stmt, bind_ctx) {
            Ok(plan) => {
                let n = count_parameters_in_plan(&plan);
                (Some(plan), n, Vec::new())
            }
            Err(err) if is_non_literal_limit_offset(&err) => {
                let (shape_sql, limit_offset_params) =
                    rewrite_limit_offset_parameters(trimmed, |_| Ok("1".to_owned()))?.ok_or(err)?;
                let shape_stmt = Parser::new(&shape_sql).parse_statement()?;
                let shape_plan = bind(&shape_stmt, bind_ctx)?;
                let plan_params = count_parameters_in_plan(&shape_plan);
                let limit_params = limit_offset_params.iter().copied().max().unwrap_or(0);
                (
                    Some(shape_plan),
                    plan_params.max(limit_params),
                    limit_offset_params,
                )
            }
            Err(err) => return Err(err.into()),
        }
    };
    let plan_hash = plan.as_ref().map_or(0, plan_hash_for_plan);
    state.statements.insert(
        name,
        PreparedStatement {
            sql,
            plan,
            plan_hash,
            param_type_oids,
            n_params,
            limit_offset_param_indexes,
        },
    );
    Ok(BackendMessage::ParseComplete)
}

fn is_non_literal_limit_offset(err: &PlanError) -> bool {
    matches!(
        err,
        PlanError::NotSupported("non-literal LIMIT/OFFSET expressions")
    )
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn keyword_at(sql: &str, idx: usize, keyword: &str) -> bool {
    let bytes = sql.as_bytes();
    let kw = keyword.as_bytes();
    if idx > 0 && is_ident_byte(bytes[idx - 1]) {
        return false;
    }
    if idx + kw.len() > bytes.len() {
        return false;
    }
    if !bytes[idx..idx + kw.len()].eq_ignore_ascii_case(kw) {
        return false;
    }
    bytes
        .get(idx + kw.len())
        .is_none_or(|next| !is_ident_byte(*next))
}

fn copy_quoted(sql: &str, out: &mut String, start: usize, quote: u8) -> usize {
    let bytes = sql.as_bytes();
    let mut i = start;
    out.push(char::from(quote));
    i += 1;
    while i < bytes.len() {
        out.push(char::from(bytes[i]));
        if bytes[i] == quote {
            i += 1;
            if bytes.get(i) == Some(&quote) {
                out.push(char::from(quote));
                i += 1;
            } else {
                break;
            }
        } else {
            i += 1;
        }
    }
    i
}

fn rewrite_limit_offset_parameters<F>(
    sql: &str,
    mut literal_for: F,
) -> Result<Option<(String, Vec<u32>)>, ServerError>
where
    F: FnMut(u32) -> Result<String, ServerError>,
{
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut indexes = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            i = copy_quoted(sql, &mut out, i, bytes[i]);
            continue;
        }
        if bytes[i] == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() {
                out.push(char::from(bytes[i]));
                let was_newline = bytes[i] == b'\n';
                i += 1;
                if was_newline {
                    break;
                }
            }
            continue;
        }
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            out.push('/');
            out.push('*');
            i += 2;
            while i < bytes.len() {
                let done = bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/');
                out.push(char::from(bytes[i]));
                i += 1;
                if done {
                    out.push('/');
                    i += 1;
                    break;
                }
            }
            continue;
        }
        if keyword_at(sql, i, "limit") || keyword_at(sql, i, "offset") {
            let keyword_len = if keyword_at(sql, i, "limit") { 5 } else { 6 };
            out.push_str(&sql[i..i + keyword_len]);
            i += keyword_len;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                out.push(char::from(bytes[i]));
                i += 1;
            }
            if bytes.get(i) == Some(&b'$') {
                let mut j = i + 1;
                let mut index = 0_u32;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    index = index
                        .saturating_mul(10)
                        .saturating_add(u32::from(bytes[j] - b'0'));
                    j += 1;
                }
                if j > i + 1 && index > 0 {
                    out.push_str(&literal_for(index)?);
                    indexes.push(index);
                    i = j;
                    continue;
                }
            }
            continue;
        }
        out.push(char::from(bytes[i]));
        i += 1;
    }
    if indexes.is_empty() {
        return Ok(None);
    }
    indexes.sort_unstable();
    indexes.dedup();
    Ok(Some((out, indexes)))
}

fn limit_parameter_literal(value: &Value) -> Result<String, ServerError> {
    let parsed = match value {
        Value::Null => return Ok("NULL".to_owned()),
        Value::Int16(v) => i64::from(*v),
        Value::Int32(v) => i64::from(*v),
        Value::Int64(v) => *v,
        Value::Text(text) => text.parse::<i64>().map_err(|_| {
            ServerError::Unsupported("LIMIT/OFFSET parameter must be a non-negative integer")
        })?,
        _ => {
            return Err(ServerError::Unsupported(
                "LIMIT/OFFSET parameter must be a non-negative integer",
            ));
        }
    };
    if parsed < 0 {
        return Err(ServerError::Unsupported(
            "LIMIT/OFFSET parameter must be a non-negative integer",
        ));
    }
    Ok(parsed.to_string())
}

// ---------------------------------------------------------------------------
// Bind: decode parameter values + substitute + store portal.
// ---------------------------------------------------------------------------

/// Resolve a single parameter's wire format code per spec convention.
///
/// `param_formats` has one of three lengths (see the doc comment on
/// [`ultrasql_protocol::FrontendMessage::Bind::param_formats`]):
/// empty → all text; single → applies to every parameter; one-per →
/// element `i` governs `params[i]`.
pub(super) fn resolve_param_format(param_formats: &[i16], i: usize) -> i16 {
    match param_formats.len() {
        0 => 0,
        1 => param_formats[0],
        _ => param_formats.get(i).copied().unwrap_or(0),
    }
}

/// Handle a `Bind` message.
///
/// Look up the named statement, decode each parameter byte buffer into a
/// [`Value`] (per its format code and the statement's declared OID),
/// substitute the values into the plan, and install the resulting
/// [`BoundPortal`] in `state.portals`.
///
/// # Errors
///
/// Returns [`ServerError::Unsupported`] if the named statement does not
/// exist, or a parameter cannot be decoded. The caller wraps the error
/// in `ErrorResponse` and calls [`ExtendedConnState::mark_failed`].
pub fn handle_bind(
    state: &mut ExtendedConnState,
    portal_name: String,
    statement_name: &str,
    param_formats: &[i16],
    params: &[Option<Vec<u8>>],
    result_formats: Vec<i16>,
    catalog: Option<&dyn ultrasql_planner::Catalog>,
) -> Result<BackendMessage, ServerError> {
    // Pre-compute the not-found error so clippy's `unnecessary_closure`
    // pass is satisfied; the message branch depends on whether the
    // statement name was the unnamed one or a non-empty miss.
    let not_found_err = if statement_name.is_empty() {
        ServerError::Unsupported("Bind references the unnamed statement but no Parse has happened")
    } else {
        ServerError::Unsupported("Bind references an unknown statement name; Parse it first")
    };
    let stmt = state.statements.get(statement_name).ok_or(not_found_err)?;

    // Validate parameter count. The client is required to supply exactly
    // as many parameters as the plan references; if a Parse oversupplied
    // OIDs (longer than `n_params`), we trust the client and accept the
    // extra slots silently.
    let n_required = usize::try_from(stmt.n_params).unwrap_or(usize::MAX);
    if params.len() < n_required {
        return Err(ServerError::Unsupported(
            "Bind supplied fewer parameters than the prepared statement requires",
        ));
    }
    if result_formats.iter().any(|fmt| !matches!(*fmt, 0 | 1)) {
        return Err(ServerError::Unsupported(
            "Bind result format has unsupported format code",
        ));
    }

    // Resolve effective OIDs: client-declared (Parse) takes precedence;
    // unset slots fall back to inferred-from-plan OIDs so binary
    // parameters from drivers that omit OIDs in Parse still decode
    // correctly.
    let inferred: Vec<DataType> = stmt
        .plan
        .as_ref()
        .map(|p| infer_parameter_types(p, catalog))
        .unwrap_or_default();

    let mut values: Vec<Value> = Vec::with_capacity(params.len());
    for (i, raw) in params.iter().enumerate() {
        let fmt = resolve_param_format(param_formats, i);
        let declared = stmt.param_type_oids.get(i).copied().filter(|o| *o != 0);
        let one_based_index = u32::try_from(i).unwrap_or(u32::MAX).saturating_add(1);
        let oid = declared.or_else(|| {
            if stmt.limit_offset_param_indexes.contains(&one_based_index) {
                return Some(pg_type_oid(&DataType::Int32));
            }
            inferred.get(i).map(|t| {
                if matches!(t, DataType::Null) {
                    0
                } else {
                    pg_type_oid(t)
                }
            })
        });
        let oid = oid.filter(|o| *o != 0);
        let v = decode_param(raw.as_deref(), fmt, oid).map_err(|kind| match kind {
            DecodeError::BadFormat => {
                ServerError::Unsupported("Bind parameter has unsupported format code")
            }
            DecodeError::BadBytes => {
                ServerError::Unsupported("Bind parameter bytes do not match the declared type")
            }
        })?;
        values.push(v);
    }

    let bound_plan = if stmt.limit_offset_param_indexes.is_empty() {
        stmt.plan
            .as_ref()
            .map(|p| substitute_parameters_in_plan(p, &values))
    } else {
        let (rewritten_sql, _) = rewrite_limit_offset_parameters(&stmt.sql, |index| {
            let value_idx = usize::try_from(index.saturating_sub(1)).unwrap_or(usize::MAX);
            let value = values.get(value_idx).ok_or(ServerError::Unsupported(
                "LIMIT/OFFSET parameter index exceeds supplied Bind values",
            ))?;
            limit_parameter_literal(value)
        })?
        .ok_or(ServerError::Unsupported(
            "prepared statement lost LIMIT/OFFSET parameters",
        ))?;
        let parsed = Parser::new(&rewritten_sql).parse_statement()?;
        let plan = bind(
            &parsed,
            catalog.ok_or(ServerError::Unsupported(
                "Bind needs catalog to rebind LIMIT/OFFSET parameters",
            ))?,
        )?;
        Some(substitute_parameters_in_plan(&plan, &values))
    };

    let plan_hash = bound_plan
        .as_ref()
        .map_or(stmt.plan_hash, plan_hash_for_plan);

    state.portals.insert(
        portal_name,
        BoundPortal {
            plan: bound_plan,
            sql: stmt.sql.clone(),
            plan_hash,
            bind_param_count: u32::try_from(params.len()).unwrap_or(u32::MAX),
            bind_params_redacted: !params.is_empty(),
            result_formats,
        },
    );
    Ok(BackendMessage::BindComplete)
}

// ---------------------------------------------------------------------------
// Describe.
// ---------------------------------------------------------------------------

/// Handle a `Describe(Statement, name)` message.
///
/// Emits a `ParameterDescription` listing the parameter type OIDs the
/// server believes the prepared statement expects, followed by a
/// `RowDescription` for the plan's output (or `NoData` for plans that
/// produce no rows: DDL, INSERT/UPDATE/DELETE without RETURNING).
///
/// OID resolution prefers, in order:
///
/// 1. The client's declared OID from `Parse` (`param_type_oids[i]`).
/// 2. A type inferred from the plan via `infer_parameter_types`.
/// 3. `text` (OID `25`) as the libpq-compatible fallback.
///
/// Steps 1 and 2 matter for drivers like `tokio-postgres` that refuse
/// to send a binary-format parameter unless the server's announced
/// parameter type matches the Rust type being supplied.
pub fn handle_describe_statement(
    state: &ExtendedConnState,
    name: &str,
    catalog: Option<&dyn ultrasql_planner::Catalog>,
) -> Result<Vec<BackendMessage>, ServerError> {
    let stmt = state
        .statements
        .get(name)
        .ok_or(ServerError::Unsupported("Describe: statement not found"))?;
    let n = usize::try_from(stmt.n_params).unwrap_or(usize::MAX);
    let inferred: Vec<DataType> = stmt
        .plan
        .as_ref()
        .map(|p| infer_parameter_types(p, catalog))
        .unwrap_or_default();
    let mut oids = Vec::with_capacity(n);
    for i in 0..n {
        let oid = stmt
            .param_type_oids
            .get(i)
            .copied()
            .filter(|o| *o != 0)
            .unwrap_or_else(|| {
                let one_based_index = u32::try_from(i).unwrap_or(u32::MAX).saturating_add(1);
                if stmt.limit_offset_param_indexes.contains(&one_based_index) {
                    return pg_type_oid(&DataType::Int32);
                }
                let inferred_ty = inferred.get(i).cloned().unwrap_or(DataType::Null);
                pg_type_oid(&inferred_ty)
            });
        oids.push(oid);
    }
    let row_desc = stmt
        .plan
        .as_ref()
        .map_or(BackendMessage::NoData, row_description_for_plan);
    Ok(vec![
        BackendMessage::ParameterDescription { type_oids: oids },
        row_desc,
    ])
}

/// Handle a `Describe(Portal, name)` message.
pub fn handle_describe_portal(
    state: &ExtendedConnState,
    name: &str,
) -> Result<BackendMessage, ServerError> {
    let portal = state
        .portals
        .get(name)
        .ok_or(ServerError::Unsupported("Describe: portal not found"))?;
    Ok(portal.plan.as_ref().map_or(BackendMessage::NoData, |plan| {
        row_description_for_plan_with_formats(plan, &portal.result_formats)
    }))
}

// ---------------------------------------------------------------------------
// Close / Flush.
// ---------------------------------------------------------------------------

/// Handle a `Close(kind, name)` message.
///
/// Removes the named object if it exists; returns `CloseComplete`
/// unconditionally (spec: closing a non-existent object is not an error).
pub fn handle_close(
    state: &mut ExtendedConnState,
    kind: DescribeKind,
    name: &str,
) -> BackendMessage {
    match kind {
        DescribeKind::Statement => {
            state.statements.remove(name);
            // Per spec, closing a prepared statement also closes any
            // portals derived from it. We don't track that linkage in
            // v0.5; named portals are rare in real driver pipelines, so
            // we keep this conservative and document the gap.
        }
        DescribeKind::Portal => {
            state.portals.remove(name);
            state.suspended.remove(name);
        }
    }
    BackendMessage::CloseComplete
}
