//! Extended-query message handlers — Parse, Bind, Describe (Statement/Portal),
//! and Close. Each handler is invoked by the per-session state machine in
//! [`crate::session`] after the wire codec decodes a frontend message.

use ultrasql_core::{DataType, Value};
use ultrasql_parser::Parser;
use ultrasql_planner::bind;
use ultrasql_protocol::{BackendMessage, DescribeKind};

use crate::error::ServerError;
use crate::workload::plan_hash_for_plan;

use super::codec::{DecodeError, decode_param, pg_type_oid, row_description_for_plan};
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
    let (plan, n_params) = if trimmed.is_empty() || trimmed == ";" {
        (None, 0)
    } else {
        let stmt = Parser::new(trimmed).parse_statement()?;
        let plan = bind(&stmt, bind_ctx)?;
        let n = count_parameters_in_plan(&plan);
        (Some(plan), n)
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
        },
    );
    Ok(BackendMessage::ParseComplete)
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
        let oid = declared.or_else(|| {
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

    let bound_plan = stmt
        .plan
        .as_ref()
        .map(|p| substitute_parameters_in_plan(p, &values));

    state.portals.insert(
        portal_name,
        BoundPortal {
            plan: bound_plan,
            sql: stmt.sql.clone(),
            plan_hash: stmt.plan_hash,
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
    Ok(portal
        .plan
        .as_ref()
        .map_or(BackendMessage::NoData, row_description_for_plan))
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
