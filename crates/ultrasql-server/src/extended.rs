//! PostgreSQL Extended Query Protocol server-side dispatch.
//!
//! The Simple Query protocol carries one `Query` message at a time and
//! parses/binds/executes it inline. The Extended Query protocol splits
//! the same work across five client messages:
//!
//! ```text
//! Parse(name, sql, oids)        → ParseComplete
//! Bind (portal, stmt, params)   → BindComplete
//! Describe(S|P, name)           → ParameterDescription? RowDescription | NoData
//! Execute(portal, max_rows)     → DataRow* (CommandComplete | PortalSuspended)
//! Sync                          → ReadyForQuery
//! Close(S|P, name)              → CloseComplete
//! Flush                         → (no response, just flush buffered output)
//! ```
//!
//! ## Per-connection state
//!
//! Two `HashMap`s store named statements and named portals. They are
//! owned by the [`Session`] struct in `lib.rs` and accessed only by the
//! connection's own task, so no synchronisation primitive is needed
//! (per AGENTS.md §5: "default to the simplest primitive that meets the
//! workload" — the workload here is single-threaded). The empty string
//! is the canonical "unnamed" key, per the protocol spec.
//!
//! ## Parameter substitution strategy
//!
//! Bind decodes each parameter value (per its format code and the
//! statement's declared type OID) into a [`Value`], then walks the
//! prepared statement's bound [`LogicalPlan`] and rewrites every
//! [`ScalarExpr::Parameter`] into a [`ScalarExpr::Literal`] of the
//! corresponding value. The substituted plan is stored in the portal
//! and executed exactly the same way as Simple Query plans.
//!
//! The tradeoff: parameters do not flow through the optimizer with a
//! "parameter" identity, so plan caching does not yet share a single
//! generic plan across multiple bindings. That is acceptable for v0.5
//! (each Bind re-parses cheaply). The alternative — keeping the
//! `Parameter` node and plumbing a bound parameter vector through every
//! operator — would require touching `Filter`, `Project`, `HashAggregate`,
//! and the `Eval` constructors at the `lower_query` level. Substitution is
//! a self-contained rewrite that touches no executor code.
//!
//! ## Error handling
//!
//! Per the Extended Query spec, once any pipeline message produces an
//! error, the server replies with `ErrorResponse` and then **skips every
//! subsequent extended-protocol message until it sees a `Sync`**. Only
//! after `Sync` does it emit `ReadyForQuery` and resume processing.
//! [`ExtendedConnState::pipeline_failed`] tracks this skip state.

use std::collections::HashMap;

use ultrasql_core::{DataType, Value};
use ultrasql_parser::Parser;
use ultrasql_planner::{
    BinaryOp, LogicalAggregateExpr, LogicalJoinCondition, LogicalOnConflict, LogicalPlan,
    LogicalSetOp, LogicalSetQuantifier, ScalarExpr, SortKey, bind,
};
use ultrasql_protocol::{BackendMessage, DescribeKind, FieldDescription};

use crate::error::ServerError;
use crate::pipeline::{LowerCtx, lower_query};
use crate::result_encoder::{encode_text_value, run_modify_command};

// ---------------------------------------------------------------------------
// Type-OID constants. Duplicated narrowly with `result_encoder.rs` so this
// module is self-contained for the binary-format param decoder.
// ---------------------------------------------------------------------------

/// PostgreSQL type OID for `bool`. Pulled from `pg_type.dat`.
const PG_OID_BOOL: u32 = 16;
/// PostgreSQL type OID for `int2`.
const PG_OID_INT2: u32 = 21;
/// PostgreSQL type OID for `int4`.
const PG_OID_INT4: u32 = 23;
/// PostgreSQL type OID for `int8`.
const PG_OID_INT8: u32 = 20;
/// PostgreSQL type OID for `float4`.
const PG_OID_FLOAT4: u32 = 700;
/// PostgreSQL type OID for `float8`.
const PG_OID_FLOAT8: u32 = 701;
/// PostgreSQL type OID for `text`.
const PG_OID_TEXT: u32 = 25;
/// PostgreSQL type OID for `bytea`.
const PG_OID_BYTEA: u32 = 17;
/// PostgreSQL type OID for `varchar`.
const PG_OID_VARCHAR: u32 = 1043;
/// PostgreSQL type OID for `bpchar` (`char(n)`).
const PG_OID_BPCHAR: u32 = 1042;
/// PostgreSQL type OID for `oid`.
const PG_OID_OID: u32 = 26;

// ---------------------------------------------------------------------------
// Cached, parsed-and-bound prepared statement.
// ---------------------------------------------------------------------------

/// A `Parse`d statement waiting for `Bind`.
///
/// `plan` is `None` for empty statements (those parse and produce no
/// AST). `param_type_oids` retains the OIDs the client supplied; the
/// server uses them to decode binary parameters in `Bind`. `n_params`
/// is the maximum `$N` index referenced in the bound plan, computed at
/// Parse time so `Bind` can validate parameter count.
#[derive(Clone, Debug)]
pub struct PreparedStatement {
    /// Raw SQL text retained for diagnostics.
    pub sql: String,
    /// Bound logical plan. `None` for an empty statement (SQL `""`).
    pub plan: Option<LogicalPlan>,
    /// Parameter type OIDs as declared by the client. May be shorter
    /// than `n_params` (the client is allowed to leave types unset).
    pub param_type_oids: Vec<u32>,
    /// Number of distinct `$N` placeholder slots referenced in `plan`.
    /// Equal to the highest `index` seen; `$1`+`$3` yields `n_params=3`.
    pub n_params: u32,
}

/// A bound portal: a prepared statement plus the parameter values
/// substituted into its plan, plus the per-result-column format codes.
#[derive(Clone, Debug)]
pub struct BoundPortal {
    /// The plan with `Parameter` nodes already replaced by `Literal`s.
    pub plan: Option<LogicalPlan>,
    /// Per-result-column format codes (`0` = text, `1` = binary).
    ///
    /// Spec conventions: empty → all text; single element → applies to
    /// every column; one-per → element `i` governs result column `i`.
    pub result_formats: Vec<i16>,
}

/// Per-connection Extended Query state.
///
/// One instance per [`Session`]. Owned by the session, accessed only by
/// the connection's task, so no synchronisation primitive is needed.
///
/// `pipeline_failed` implements the spec's "ignore everything until
/// Sync" rule: once any extended-protocol message produces an error,
/// subsequent Parse/Bind/Describe/Execute/Close messages are skipped
/// silently until a `Sync` resets the flag.
#[derive(Debug, Default)]
pub struct ExtendedConnState {
    /// Prepared statements, keyed by name. Empty string = unnamed.
    pub statements: HashMap<String, PreparedStatement>,
    /// Open portals, keyed by name. Empty string = unnamed.
    pub portals: HashMap<String, BoundPortal>,
    /// `true` after an error in the current pipeline; cleared by `Sync`.
    pub pipeline_failed: bool,
}

impl ExtendedConnState {
    /// Build an empty state container.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the current pipeline as failed; subsequent extended-protocol
    /// messages (other than `Sync`) are ignored until `Sync` resets.
    pub const fn mark_failed(&mut self) {
        self.pipeline_failed = true;
    }

    /// Called by `Sync`: clear the failure flag.
    pub const fn reset_on_sync(&mut self) {
        self.pipeline_failed = false;
    }
}

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
    state.statements.insert(
        name,
        PreparedStatement {
            sql,
            plan,
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
fn resolve_param_format(param_formats: &[i16], i: usize) -> i16 {
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
/// 2. A type inferred from the plan via [`infer_parameter_types`].
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
// Execute.
// ---------------------------------------------------------------------------

/// Outcome of [`execute_portal`].
///
/// `messages` is the ordered list of `DataRow` / `CommandComplete` (or
/// `PortalSuspended`) messages the caller must emit. For a SELECT,
/// `RowDescription` is **not** included — the caller emits it ahead of
/// time when the client sent a `Describe`, or omits it entirely when
/// the client didn't (some drivers skip `Describe` for already-described
/// portals).
#[derive(Debug)]
pub struct ExecuteOutcome {
    /// The backend messages to send, in order.
    pub messages: Vec<BackendMessage>,
}

/// Execute the named portal and produce the message sequence.
///
/// Streams every row through the same execution path the Simple Query
/// dispatcher uses. `max_rows = 0` means "all rows" (the spec's
/// `INT32_MAX` shortcut). Any positive value caps the output; v0.5 does
/// not yet support resumption, so the portal is closed at the row cap
/// and a `PortalSuspended` is returned (the next `Execute` will see no
/// rows because the portal has no state to resume from). Documented as
/// a follow-up.
///
/// # Errors
///
/// Propagates lowering and execution errors. Wrap in `ErrorResponse`
/// and call [`ExtendedConnState::mark_failed`].
pub fn execute_portal(
    state: &mut ExtendedConnState,
    portal_name: &str,
    max_rows: i32,
    ctx: &LowerCtx<'_>,
) -> Result<ExecuteOutcome, ServerError> {
    let portal = state
        .portals
        .get(portal_name)
        .ok_or(ServerError::Unsupported("Execute: portal not found"))?
        .clone();

    let Some(plan) = portal.plan else {
        // Empty statement: emit EmptyQueryResponse + CommandComplete?
        // Actually for an empty Bind, PostgreSQL emits EmptyQueryResponse
        // only for Simple Query. In Extended Query, an empty statement
        // produces just CommandComplete with an empty tag (libpq's
        // observed behaviour). Stay conservative: emit CommandComplete
        // with a zero-row SELECT tag.
        return Ok(ExecuteOutcome {
            messages: vec![BackendMessage::CommandComplete {
                tag: "SELECT 0".to_string(),
            }],
        });
    };

    // DDL is dispatched ahead of operator lowering, matching the Simple
    // Query path in `Session::execute_query`.
    if let LogicalPlan::CreateTable { .. } = &plan {
        return Err(ServerError::Unsupported(
            "CREATE TABLE via Extended Query is not yet wired; use Simple Query",
        ));
    }

    // Build the operator tree.
    let mut op = lower_query(&plan, ctx)?;

    // INSERT/UPDATE/DELETE produce a row count message, not data rows.
    if let LogicalPlan::Insert { .. } = &plan {
        let sel = run_modify_command(op.as_mut(), "INSERT")?;
        return Ok(ExecuteOutcome {
            messages: sel.messages,
        });
    }
    if let LogicalPlan::Update { .. } = &plan {
        let sel = run_modify_command(op.as_mut(), "UPDATE")?;
        return Ok(ExecuteOutcome {
            messages: sel.messages,
        });
    }
    if let LogicalPlan::Delete { .. } = &plan {
        let sel = run_modify_command(op.as_mut(), "DELETE")?;
        return Ok(ExecuteOutcome {
            messages: sel.messages,
        });
    }

    // SELECT-like path. Drain row by row. Honor `result_formats` per
    // column. `max_rows = 0` means "no limit"; any positive value caps
    // and emits `PortalSuspended` when the cap is reached.
    let row_cap = if max_rows <= 0 {
        usize::MAX
    } else {
        usize::try_from(max_rows).unwrap_or(usize::MAX)
    };

    let mut messages: Vec<BackendMessage> = Vec::with_capacity(8);
    let mut emitted: u64 = 0;
    let mut suspended = false;

    'outer: loop {
        let Some(batch) = op.next_batch()? else { break };
        let n = batch.rows();
        for row in 0..n {
            if usize::try_from(emitted).unwrap_or(usize::MAX) >= row_cap {
                suspended = true;
                break 'outer;
            }
            let mut columns = Vec::with_capacity(batch.width());
            for (col_idx, col) in batch.columns().iter().enumerate() {
                let fmt = resolve_param_format(&portal.result_formats, col_idx);
                let encoded = if fmt == 1 {
                    encode_binary_value(col, row)
                } else {
                    encode_text_value(col, row)
                };
                columns.push(encoded);
            }
            messages.push(BackendMessage::DataRow { columns });
            emitted = emitted.saturating_add(1);
        }
    }

    if suspended {
        messages.push(BackendMessage::PortalSuspended);
        // v0.5: PortalSuspended leaves the portal in place but without
        // resumable state. Subsequent Execute returns CommandComplete
        // with zero rows (drop the portal to make this explicit so the
        // client sees a stable shape).
        state.portals.remove(portal_name);
    } else {
        messages.push(BackendMessage::CommandComplete {
            tag: format!("SELECT {emitted}"),
        });
    }

    Ok(ExecuteOutcome { messages })
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
        }
    }
    BackendMessage::CloseComplete
}

// ---------------------------------------------------------------------------
// Parameter-counting walker.
// ---------------------------------------------------------------------------

/// Return the highest `$N` placeholder index referenced anywhere in `plan`.
///
/// Returns `0` if the plan contains no `Parameter` nodes. Used by
/// `handle_parse` to validate the parameter count at `Bind` time.
fn count_parameters_in_plan(plan: &LogicalPlan) -> u32 {
    let mut max = 0_u32;
    walk_plan_exprs(plan, &mut |e| {
        if let ScalarExpr::Parameter { index, .. } = e {
            max = max.max(*index);
        }
    });
    max
}

/// Infer a concrete [`DataType`] for each `$N` placeholder referenced
/// in `plan`, returning a vector indexed by `index - 1` (1-based on the
/// wire, 0-based in the vector). Unresolved slots default to
/// [`DataType::Null`].
///
/// The inference is local: each `Parameter` in a binary comparison
/// against a column borrows the column's type; each `Values` row inside
/// an `Insert` borrows the target column's type; each `Update`
/// assignment to `Parameter` borrows the target column's type. This
/// covers the v0.5 wire shapes (WHERE col op $1, INSERT $1, UPDATE
/// SET col=$1 WHERE col=$2).
///
/// `catalog` is consulted when the inference encounters an `Insert` or
/// `Update` whose target schema is not visible from the plan alone
/// (PostgreSQL stores the target column types on the table catalog,
/// not on the bound plan). Passing `None` is equivalent to having no
/// catalog: target-driven inference is skipped and only the predicate-
/// shape inference applies.
fn infer_parameter_types(
    plan: &LogicalPlan,
    catalog: Option<&dyn ultrasql_planner::Catalog>,
) -> Vec<DataType> {
    let n = usize::try_from(count_parameters_in_plan(plan)).unwrap_or(0);
    let mut out = vec![DataType::Null; n];
    if n > 0 {
        infer_into(plan, catalog, &mut out);
    }
    out
}

/// Recursive driver behind [`infer_parameter_types`].
///
/// The match-on-`LogicalPlan` shape is intentionally exhaustive; per-
/// variant logic doesn't compress into a generic walker without
/// obscuring the type-inference rules. The `#[allow]` mirrors the
/// pattern used in `crates/ultrasql-protocol/src/codec.rs`.
#[allow(clippy::too_many_lines)]
fn infer_into(
    plan: &LogicalPlan,
    catalog: Option<&dyn ultrasql_planner::Catalog>,
    out: &mut [DataType],
) {
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::Begin { .. }
        | LogicalPlan::Commit { .. }
        | LogicalPlan::Rollback { .. }
        | LogicalPlan::Savepoint { .. }
        | LogicalPlan::RollbackToSavepoint { .. }
        | LogicalPlan::ReleaseSavepoint { .. }
        | LogicalPlan::PrepareTransaction { .. }
        | LogicalPlan::CommitPrepared { .. }
        | LogicalPlan::RollbackPrepared { .. }
        | LogicalPlan::SetTransaction { .. } => {}
        LogicalPlan::Filter { input, predicate } => {
            infer_into(input, catalog, out);
            infer_expr_types_from_predicate(predicate, out);
        }
        LogicalPlan::Project { input, exprs, .. } => {
            infer_into(input, catalog, out);
            for (e, _) in exprs {
                infer_in_expr(e, None, out);
            }
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::Sort { input, .. } => {
            infer_into(input, catalog, out);
            if let LogicalPlan::Sort { keys, .. } = plan {
                for k in keys {
                    infer_in_expr(&k.expr, None, out);
                }
            }
        }
        LogicalPlan::Values { rows, schema } => {
            for row in rows {
                for (col_i, cell) in row.iter().enumerate() {
                    let target = schema.fields().get(col_i).map(|f| f.data_type.clone());
                    infer_in_expr(cell, target, out);
                }
            }
        }
        LogicalPlan::Insert {
            table,
            columns,
            source,
            returning,
            ..
        } => {
            // The binder's `Values` schema collapses to `Null` when
            // every cell is a parameter, so we cannot rely on
            // `source.schema()`. Look up the target table in the
            // catalog and infer each `Values` row cell against the
            // *target column* type.
            if let Some(cat) = catalog {
                if let Some(meta) = cat.lookup_table(table) {
                    let target_cols: Vec<DataType> = if columns.is_empty() {
                        meta.schema
                            .fields()
                            .iter()
                            .map(|f| f.data_type.clone())
                            .collect()
                    } else {
                        columns
                            .iter()
                            .map(|i| {
                                meta.schema
                                    .fields()
                                    .get(*i)
                                    .map_or(DataType::Null, |f| f.data_type.clone())
                            })
                            .collect()
                    };
                    if let LogicalPlan::Values { rows, .. } = source.as_ref() {
                        for row in rows {
                            for (i, cell) in row.iter().enumerate() {
                                let target = target_cols.get(i).cloned();
                                infer_in_expr(cell, target, out);
                            }
                        }
                    }
                }
            }
            infer_into(source, catalog, out);
            for (e, _) in returning {
                infer_in_expr(e, None, out);
            }
        }
        LogicalPlan::Update {
            table,
            assignments,
            input,
            returning,
            ..
        } => {
            // Target column types via the catalog when available, then
            // via the underlying scan's schema as a fallback.
            let table_schema_owned: Option<ultrasql_core::Schema> = catalog
                .and_then(|cat| cat.lookup_table(table))
                .map(|m| m.schema);
            let table_schema: Option<&ultrasql_core::Schema> =
                table_schema_owned.as_ref().or_else(|| scan_schema(input));
            for (col_idx, e) in assignments {
                let target = table_schema
                    .and_then(|s| s.fields().get(*col_idx).map(|f| f.data_type.clone()));
                infer_in_expr(e, target, out);
            }
            infer_into(input, catalog, out);
            for (e, _) in returning {
                infer_in_expr(e, None, out);
            }
        }
        LogicalPlan::Delete {
            input, returning, ..
        } => {
            infer_into(input, catalog, out);
            for (e, _) in returning {
                infer_in_expr(e, None, out);
            }
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            infer_into(left, catalog, out);
            infer_into(right, catalog, out);
            if let LogicalJoinCondition::On(e) = condition {
                infer_expr_types_from_predicate(e, out);
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            infer_into(input, catalog, out);
            for e in group_by {
                infer_in_expr(e, None, out);
            }
            for a in aggregates {
                if let Some(e) = &a.arg {
                    infer_in_expr(e, None, out);
                }
            }
        }
        LogicalPlan::SetOp { left, right, .. } => {
            infer_into(left, catalog, out);
            infer_into(right, catalog, out);
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            infer_into(definition, catalog, out);
            infer_into(body, catalog, out);
        }
        LogicalPlan::LockRows { input, .. } => {
            infer_into(input, catalog, out);
        }
    }
}

/// Return the column [`Schema`] of the leftmost base-table `Scan` in `plan`.
///
/// Used to look up assignment-target column types in UPDATE: the
/// `assignments` list addresses the target table's schema directly, but
/// the `Update.input` plan's `Filter { Scan { ... } }` is where that
/// schema lives.
fn scan_schema(plan: &LogicalPlan) -> Option<&ultrasql_core::Schema> {
    match plan {
        LogicalPlan::Scan { schema, .. } => Some(schema),
        LogicalPlan::Filter { input, .. } | LogicalPlan::Limit { input, .. } => scan_schema(input),
        _ => None,
    }
}

/// Infer parameter types from a boolean predicate at the top of a
/// `Filter` / join `On`.
///
/// Recognises `Column ⟷ Parameter` and `Parameter ⟷ Column` binary
/// shapes (Eq/Lt/Gt/etc.) and assigns the column's type to the
/// parameter slot. Other shapes fall through to the generic walker.
fn infer_expr_types_from_predicate(expr: &ScalarExpr, out: &mut [DataType]) {
    match expr {
        ScalarExpr::Binary {
            left, right, op, ..
        } => {
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            ) {
                // Column = Parameter, or Parameter = Column.
                match (left.as_ref(), right.as_ref()) {
                    (ScalarExpr::Column { data_type, .. }, ScalarExpr::Parameter { index, .. })
                    | (ScalarExpr::Parameter { index, .. }, ScalarExpr::Column { data_type, .. }) =>
                    {
                        let slot = usize::try_from(index.saturating_sub(1)).unwrap_or(usize::MAX);
                        if let Some(s) = out.get_mut(slot) {
                            if matches!(s, DataType::Null) {
                                *s = data_type.clone();
                            }
                        }
                    }
                    _ => {
                        infer_in_expr(left, None, out);
                        infer_in_expr(right, None, out);
                    }
                }
                // Recurse into nested binaries (AND/OR conjunctions).
                infer_in_expr(left, None, out);
                infer_in_expr(right, None, out);
            } else {
                infer_expr_types_from_predicate(left, out);
                infer_expr_types_from_predicate(right, out);
            }
        }
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            infer_expr_types_from_predicate(expr, out);
        }
        _ => infer_in_expr(expr, None, out),
    }
}

/// Infer types from a generic expression. `target_type` is the expected
/// result type at this position (e.g. the target column's type in an
/// INSERT cell or an UPDATE assignment); a bare `Parameter` borrows it.
fn infer_in_expr(expr: &ScalarExpr, target_type: Option<DataType>, out: &mut [DataType]) {
    match expr {
        ScalarExpr::Parameter { index, .. } => {
            if let Some(t) = target_type {
                let slot = usize::try_from(index.saturating_sub(1)).unwrap_or(usize::MAX);
                if let Some(s) = out.get_mut(slot) {
                    if matches!(s, DataType::Null) {
                        *s = t;
                    }
                }
            }
        }
        ScalarExpr::Column { .. } | ScalarExpr::Literal { .. } | ScalarExpr::OuterColumn { .. } => {
        }
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            infer_in_expr(expr, None, out);
        }
        ScalarExpr::Binary {
            left, right, op, ..
        } => {
            // Comparisons surface column/parameter pairs.
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            ) {
                infer_expr_types_from_predicate(expr, out);
            }
            infer_in_expr(left, None, out);
            infer_in_expr(right, None, out);
        }
        ScalarExpr::ScalarSubquery { subplan, .. } | ScalarExpr::Exists { subplan, .. } => {
            // Type-inference inside subqueries does not have a catalog
            // handle here; subquery shapes are already a v0.5 follow-up
            // for the Extended Query path, so passing `None` is fine.
            infer_into(subplan, None, out);
        }
        ScalarExpr::InSubquery { expr, subplan, .. } => {
            infer_in_expr(expr, None, out);
            infer_into(subplan, None, out);
        }
    }
}

/// Walk every `ScalarExpr` reachable from `plan`, calling `f` on each.
///
/// Recurses into sub-plans (subqueries, CTE definitions) so $N references
/// in a subquery are visible to the caller. The walker is read-only —
/// see [`map_plan_exprs`] for the mutating sibling.
#[allow(clippy::too_many_lines)]
fn walk_plan_exprs<F: FnMut(&ScalarExpr)>(plan: &LogicalPlan, f: &mut F) {
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::Begin { .. }
        | LogicalPlan::Commit { .. }
        | LogicalPlan::Rollback { .. }
        | LogicalPlan::Savepoint { .. }
        | LogicalPlan::RollbackToSavepoint { .. }
        | LogicalPlan::ReleaseSavepoint { .. }
        | LogicalPlan::PrepareTransaction { .. }
        | LogicalPlan::CommitPrepared { .. }
        | LogicalPlan::RollbackPrepared { .. }
        | LogicalPlan::SetTransaction { .. } => {}
        LogicalPlan::Filter { input, predicate } => {
            walk_plan_exprs(input, f);
            walk_expr(predicate, f);
        }
        LogicalPlan::Project { input, exprs, .. } => {
            walk_plan_exprs(input, f);
            for (e, _) in exprs {
                walk_expr(e, f);
            }
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::Sort { input, .. } => {
            walk_plan_exprs(input, f);
            if let LogicalPlan::Sort { keys, .. } = plan {
                for k in keys {
                    walk_expr(&k.expr, f);
                }
            }
        }
        LogicalPlan::Values { rows, .. } => {
            for row in rows {
                for cell in row {
                    walk_expr(cell, f);
                }
            }
        }
        LogicalPlan::Insert {
            source,
            on_conflict,
            returning,
            ..
        } => {
            walk_plan_exprs(source, f);
            if let Some(oc) = on_conflict {
                walk_on_conflict(oc, f);
            }
            for (e, _) in returning {
                walk_expr(e, f);
            }
        }
        LogicalPlan::Update {
            assignments,
            input,
            returning,
            ..
        } => {
            walk_plan_exprs(input, f);
            for (_, e) in assignments {
                walk_expr(e, f);
            }
            for (e, _) in returning {
                walk_expr(e, f);
            }
        }
        LogicalPlan::Delete {
            input, returning, ..
        } => {
            walk_plan_exprs(input, f);
            for (e, _) in returning {
                walk_expr(e, f);
            }
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            walk_plan_exprs(left, f);
            walk_plan_exprs(right, f);
            if let LogicalJoinCondition::On(e) = condition {
                walk_expr(e, f);
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            ..
        } => {
            walk_plan_exprs(input, f);
            for e in group_by {
                walk_expr(e, f);
            }
            for a in aggregates {
                if let Some(e) = &a.arg {
                    walk_expr(e, f);
                }
            }
        }
        LogicalPlan::SetOp { left, right, .. } => {
            walk_plan_exprs(left, f);
            walk_plan_exprs(right, f);
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            walk_plan_exprs(definition, f);
            walk_plan_exprs(body, f);
        }
        LogicalPlan::LockRows { input, .. } => {
            walk_plan_exprs(input, f);
        }
    }
}

/// Recursively visit every node in `expr`, calling `f` on each.
///
/// Recurses into subquery plans via [`walk_plan_exprs`] so a `$N`
/// reference deep inside a `WHERE x IN (SELECT … WHERE y = $1)` is
/// surfaced to the caller.
fn walk_expr<F: FnMut(&ScalarExpr)>(expr: &ScalarExpr, f: &mut F) {
    f(expr);
    match expr {
        ScalarExpr::Column { .. }
        | ScalarExpr::Literal { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. } => {}
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => walk_expr(expr, f),
        ScalarExpr::Binary { left, right, .. } => {
            walk_expr(left, f);
            walk_expr(right, f);
        }
        ScalarExpr::ScalarSubquery { subplan, .. } | ScalarExpr::Exists { subplan, .. } => {
            walk_plan_exprs(subplan, f);
        }
        ScalarExpr::InSubquery { expr, subplan, .. } => {
            walk_expr(expr, f);
            walk_plan_exprs(subplan, f);
        }
    }
}

/// Walk the expressions inside an `ON CONFLICT` clause.
fn walk_on_conflict<F: FnMut(&ScalarExpr)>(oc: &LogicalOnConflict, f: &mut F) {
    if let LogicalOnConflict::DoUpdate {
        assignments,
        r#where,
        ..
    } = oc
    {
        for (_, e) in assignments {
            walk_expr(e, f);
        }
        if let Some(w) = r#where {
            walk_expr(w, f);
        }
    }
}

// ---------------------------------------------------------------------------
// Parameter substitution: ScalarExpr::Parameter → ScalarExpr::Literal.
// ---------------------------------------------------------------------------

/// Walk `plan` and rewrite every `ScalarExpr::Parameter { index }` into
/// a `ScalarExpr::Literal { value: values[index-1] }`.
///
/// Out-of-range `$N` references are left as `Parameter` nodes; the
/// executor will surface them as `EvalError::ParameterIndex`. That
/// behaviour matches PostgreSQL, which only checks parameter arity
/// during `Bind` (we already check in `handle_bind`).
///
/// The walker constructs a fresh plan; the input is unchanged. This
/// makes the function suitable for use against a `&PreparedStatement`
/// shared across multiple `Bind` calls.
pub(crate) fn substitute_parameters_in_plan(plan: &LogicalPlan, values: &[Value]) -> LogicalPlan {
    map_plan_exprs(plan, &|e| substitute_parameter_in_expr(e, values))
}

/// Recursively rewrite parameters in `expr`.
fn substitute_parameter_in_expr(expr: &ScalarExpr, values: &[Value]) -> ScalarExpr {
    match expr {
        ScalarExpr::Parameter { index, .. } => {
            let zero = usize::try_from(index.saturating_sub(1)).unwrap_or(usize::MAX);
            values.get(zero).map_or_else(
                || expr.clone(),
                |v| ScalarExpr::Literal {
                    data_type: v.data_type(),
                    value: v.clone(),
                },
            )
        }
        ScalarExpr::Column { .. } | ScalarExpr::Literal { .. } | ScalarExpr::OuterColumn { .. } => {
            expr.clone()
        }
        ScalarExpr::Unary {
            op,
            expr,
            data_type,
        } => ScalarExpr::Unary {
            op: *op,
            expr: Box::new(substitute_parameter_in_expr(expr, values)),
            data_type: data_type.clone(),
        },
        ScalarExpr::Binary {
            op,
            left,
            right,
            data_type,
        } => {
            let mut new_left = substitute_parameter_in_expr(left, values);
            let mut new_right = substitute_parameter_in_expr(right, values);
            // After substitution we may have a binary operator whose
            // result type was inferred against `Null` (the binder's
            // default for `Parameter`). Re-derive the result type from
            // the now-concrete operand types so the executor's type
            // checks downstream see the right shape (e.g. an `Eq` with
            // an Int32 column on the left should report Bool, not
            // whatever was inferred before).
            let lt = new_left.data_type();
            let rt = new_right.data_type();
            // Best-effort numeric widening: if comparing/arith between
            // Int32-column and Int64-literal, the literal coerces to
            // Int32 when it fits and to Int64 otherwise. We coerce the
            // *literal* side so the Filter operator's SIMD i32/i64
            // dispatch picks the column's type.
            coerce_literal_to_match(&mut new_left, &mut new_right);
            // Recompute data_type if the operator is a comparison;
            // comparisons always return Bool. For arithmetic we keep
            // the binder's original choice (it's a join over the two
            // operand types and that join is still valid).
            let new_dt = match op {
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
                | BinaryOp::And
                | BinaryOp::Or
                | BinaryOp::Like
                | BinaryOp::NotLike
                | BinaryOp::Ilike
                | BinaryOp::NotIlike => DataType::Bool,
                _ => data_type.clone(),
            };
            let _ = (lt, rt);
            ScalarExpr::Binary {
                op: *op,
                left: Box::new(new_left),
                right: Box::new(new_right),
                data_type: new_dt,
            }
        }
        ScalarExpr::IsNull { expr, negated } => ScalarExpr::IsNull {
            expr: Box::new(substitute_parameter_in_expr(expr, values)),
            negated: *negated,
        },
        ScalarExpr::ScalarSubquery {
            subplan,
            correlated,
            data_type,
        } => ScalarExpr::ScalarSubquery {
            subplan: Box::new(substitute_parameters_in_plan(subplan, values)),
            correlated: *correlated,
            data_type: data_type.clone(),
        },
        ScalarExpr::Exists {
            subplan,
            negated,
            correlated,
        } => ScalarExpr::Exists {
            subplan: Box::new(substitute_parameters_in_plan(subplan, values)),
            negated: *negated,
            correlated: *correlated,
        },
        ScalarExpr::InSubquery {
            expr,
            subplan,
            negated,
            correlated,
            data_type,
        } => ScalarExpr::InSubquery {
            expr: Box::new(substitute_parameter_in_expr(expr, values)),
            subplan: Box::new(substitute_parameters_in_plan(subplan, values)),
            negated: *negated,
            correlated: *correlated,
            data_type: data_type.clone(),
        },
    }
}

/// If `left` or `right` is a `Literal` and the other side is a `Column`
/// (or anything with a concrete type), coerce the literal to the
/// column's type when it's a safe numeric narrow/widen. Keeps the
/// Filter SIMD fast-path happy: it dispatches on the column's type and
/// expects both operands to have the same width.
fn coerce_literal_to_match(left: &mut ScalarExpr, right: &mut ScalarExpr) {
    coerce_literal_side(left, right);
    coerce_literal_side(right, left);
}

fn coerce_literal_side(lit_side: &mut ScalarExpr, ref_side: &ScalarExpr) {
    let ScalarExpr::Literal { value, data_type } = lit_side else {
        return;
    };
    let target = ref_side.data_type();
    if matches!(target, DataType::Null) || data_type == &target {
        return;
    }
    match (target, &*value) {
        (DataType::Int32, Value::Int64(v)) => {
            if let Ok(narrow) = i32::try_from(*v) {
                *value = Value::Int32(narrow);
                *data_type = DataType::Int32;
            }
        }
        (DataType::Int64, Value::Int32(v)) => {
            *value = Value::Int64(i64::from(*v));
            *data_type = DataType::Int64;
        }
        (DataType::Float64, Value::Float32(v)) => {
            *value = Value::Float64(f64::from(*v));
            *data_type = DataType::Float64;
        }
        (DataType::Float32, Value::Float64(v)) => {
            #[allow(clippy::cast_possible_truncation)]
            let narrow = *v as f32;
            *value = Value::Float32(narrow);
            *data_type = DataType::Float32;
        }
        // Int → Float widening (e.g. id (Int32) = 42 (Int32 lit) is fine;
        // this hits when comparing a Float column to an integer literal).
        (DataType::Float64, Value::Int32(v)) => {
            *value = Value::Float64(f64::from(*v));
            *data_type = DataType::Float64;
        }
        (DataType::Float64, Value::Int64(v)) => {
            #[allow(clippy::cast_precision_loss)]
            let widened = *v as f64;
            *value = Value::Float64(widened);
            *data_type = DataType::Float64;
        }
        _ => {}
    }
}

/// Walk `plan`, rebuilding it with every `ScalarExpr` replaced by `f(e)`.
///
/// The traversal is exhaustive: every place the plan stores a
/// `ScalarExpr` is visited. Sub-plans (subqueries, CTE bodies) are
/// recursed into via `substitute_parameters_in_plan`.
#[allow(clippy::too_many_lines)]
fn map_plan_exprs<F>(plan: &LogicalPlan, f: &F) -> LogicalPlan
where
    F: Fn(&ScalarExpr) -> ScalarExpr,
{
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Truncate { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::Begin { .. }
        | LogicalPlan::Commit { .. }
        | LogicalPlan::Rollback { .. }
        | LogicalPlan::Savepoint { .. }
        | LogicalPlan::RollbackToSavepoint { .. }
        | LogicalPlan::ReleaseSavepoint { .. }
        | LogicalPlan::PrepareTransaction { .. }
        | LogicalPlan::CommitPrepared { .. }
        | LogicalPlan::RollbackPrepared { .. }
        | LogicalPlan::SetTransaction { .. } => plan.clone(),
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(map_plan_exprs(input, f)),
            predicate: f(predicate),
        },
        LogicalPlan::Project {
            input,
            exprs,
            schema,
        } => LogicalPlan::Project {
            input: Box::new(map_plan_exprs(input, f)),
            exprs: exprs.iter().map(|(e, n)| (f(e), n.clone())).collect(),
            schema: schema.clone(),
        },
        LogicalPlan::Limit { input, n, offset } => LogicalPlan::Limit {
            input: Box::new(map_plan_exprs(input, f)),
            n: *n,
            offset: *offset,
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(map_plan_exprs(input, f)),
            keys: keys
                .iter()
                .map(|k| SortKey {
                    expr: f(&k.expr),
                    asc: k.asc,
                    nulls_first: k.nulls_first,
                })
                .collect(),
        },
        LogicalPlan::Values { rows, schema } => {
            // After substitution, parameter cells become concrete-typed
            // literals; the binder built `schema` assuming `Null` for
            // every all-parameter column. Rebuild any column whose
            // schema type is still `Null` from the first concrete-typed
            // cell — the executor's ValuesScan / batch builder rejects
            // `DataType::Null` and we don't want a downstream panic.
            let new_rows: Vec<Vec<ScalarExpr>> =
                rows.iter().map(|row| row.iter().map(f).collect()).collect();
            let new_schema = rebuild_values_schema(schema, &new_rows);
            LogicalPlan::Values {
                rows: new_rows,
                schema: new_schema,
            }
        }
        LogicalPlan::Insert {
            table,
            columns,
            source,
            on_conflict,
            returning,
            schema,
        } => LogicalPlan::Insert {
            table: table.clone(),
            columns: columns.clone(),
            source: Box::new(map_plan_exprs(source, f)),
            on_conflict: on_conflict.as_ref().map(|oc| map_on_conflict(oc, f)),
            returning: returning.iter().map(|(e, n)| (f(e), n.clone())).collect(),
            schema: schema.clone(),
        },
        LogicalPlan::Update {
            table,
            assignments,
            input,
            returning,
            schema,
        } => LogicalPlan::Update {
            table: table.clone(),
            assignments: assignments.iter().map(|(i, e)| (*i, f(e))).collect(),
            input: Box::new(map_plan_exprs(input, f)),
            returning: returning.iter().map(|(e, n)| (f(e), n.clone())).collect(),
            schema: schema.clone(),
        },
        LogicalPlan::Delete {
            table,
            input,
            returning,
            schema,
        } => LogicalPlan::Delete {
            table: table.clone(),
            input: Box::new(map_plan_exprs(input, f)),
            returning: returning.iter().map(|(e, n)| (f(e), n.clone())).collect(),
            schema: schema.clone(),
        },
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            schema,
        } => LogicalPlan::Join {
            left: Box::new(map_plan_exprs(left, f)),
            right: Box::new(map_plan_exprs(right, f)),
            join_type: *join_type,
            condition: match condition {
                LogicalJoinCondition::On(e) => LogicalJoinCondition::On(f(e)),
                other => other.clone(),
            },
            schema: schema.clone(),
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            schema,
        } => LogicalPlan::Aggregate {
            input: Box::new(map_plan_exprs(input, f)),
            group_by: group_by.iter().map(f).collect(),
            aggregates: aggregates
                .iter()
                .map(|a| LogicalAggregateExpr {
                    func: a.func,
                    arg: a.arg.as_ref().map(f),
                    distinct: a.distinct,
                    output_name: a.output_name.clone(),
                    data_type: a.data_type.clone(),
                })
                .collect(),
            schema: schema.clone(),
        },
        LogicalPlan::SetOp {
            op,
            quantifier,
            left,
            right,
            schema,
        } => LogicalPlan::SetOp {
            op: match op {
                LogicalSetOp::Union => LogicalSetOp::Union,
                LogicalSetOp::Intersect => LogicalSetOp::Intersect,
                LogicalSetOp::Except => LogicalSetOp::Except,
            },
            quantifier: match quantifier {
                LogicalSetQuantifier::All => LogicalSetQuantifier::All,
                LogicalSetQuantifier::Distinct => LogicalSetQuantifier::Distinct,
            },
            left: Box::new(map_plan_exprs(left, f)),
            right: Box::new(map_plan_exprs(right, f)),
            schema: schema.clone(),
        },
        LogicalPlan::Cte {
            name,
            recursive,
            definition,
            body,
            schema,
        } => LogicalPlan::Cte {
            name: name.clone(),
            recursive: *recursive,
            definition: Box::new(map_plan_exprs(definition, f)),
            body: Box::new(map_plan_exprs(body, f)),
            schema: schema.clone(),
        },
        LogicalPlan::LockRows {
            input,
            strength,
            wait_policy,
            schema,
        } => LogicalPlan::LockRows {
            input: Box::new(map_plan_exprs(input, f)),
            strength: *strength,
            wait_policy: *wait_policy,
            schema: schema.clone(),
        },
    }
}

/// Rebuild a `Values` plan's column schema when post-substitution
/// cells reveal concrete types the binder could not see.
///
/// For each column position, keep the existing schema field if its
/// type is already concrete (not `Null`); otherwise take the first
/// concrete-typed cell across all rows. The output schema mirrors the
/// binder's "column1, column2, …" naming convention. Falling back to
/// the input schema on any rebuild failure keeps callers crash-safe.
fn rebuild_values_schema(
    schema: &ultrasql_core::Schema,
    rows: &[Vec<ScalarExpr>],
) -> ultrasql_core::Schema {
    let fields = schema.fields();
    let mut new_types: Vec<DataType> = fields.iter().map(|f| f.data_type.clone()).collect();
    for (ci, ty) in new_types.iter_mut().enumerate() {
        if matches!(ty, DataType::Null) {
            for row in rows {
                if let Some(cell) = row.get(ci) {
                    let cell_ty = cell.data_type();
                    if !matches!(cell_ty, DataType::Null) {
                        *ty = cell_ty;
                        break;
                    }
                }
            }
        }
    }
    let rebuilt: Vec<ultrasql_core::Field> = new_types
        .into_iter()
        .enumerate()
        .map(|(i, ty)| {
            // Mirror the binder's `column{N}` naming.
            let name = fields
                .get(i)
                .map_or_else(|| format!("column{}", i + 1), |f| f.name.clone());
            ultrasql_core::Field::nullable(name, ty)
        })
        .collect();
    ultrasql_core::Schema::new(rebuilt).unwrap_or_else(|_| schema.clone())
}

fn map_on_conflict<F>(oc: &LogicalOnConflict, f: &F) -> LogicalOnConflict
where
    F: Fn(&ScalarExpr) -> ScalarExpr,
{
    match oc {
        LogicalOnConflict::DoNothing { target } => LogicalOnConflict::DoNothing {
            target: target.clone(),
        },
        LogicalOnConflict::DoUpdate {
            target,
            assignments,
            r#where,
        } => LogicalOnConflict::DoUpdate {
            target: target.clone(),
            assignments: assignments.iter().map(|(i, e)| (*i, f(e))).collect(),
            r#where: r#where.as_ref().map(f),
        },
    }
}

// ---------------------------------------------------------------------------
// Parameter byte-decoder.
// ---------------------------------------------------------------------------

/// Errors raised while decoding a single Bind parameter.
#[derive(Debug)]
enum DecodeError {
    /// Format code other than `0` (text) or `1` (binary).
    BadFormat,
    /// Bytes do not match the declared type (length mismatch, invalid
    /// UTF-8, unparseable numeric, etc.).
    BadBytes,
}

/// Decode one Bind parameter into a [`Value`].
///
/// `raw = None` → SQL NULL (`Value::Null`). Otherwise, `format` is
/// the per-parameter format code (`0` = text, `1` = binary). `oid` is
/// the declared parameter type OID from `Parse`; an absent OID is
/// treated as text-default.
///
/// Text-format decoding parses the UTF-8 bytes through Rust's std
/// parsers. Binary-format decoding uses the type-specific big-endian
/// layout from `pg_type.dat`.
fn decode_param(raw: Option<&[u8]>, format: i16, oid: Option<u32>) -> Result<Value, DecodeError> {
    let Some(bytes) = raw else {
        return Ok(Value::Null);
    };
    match format {
        0 => decode_param_text(bytes, oid),
        1 => decode_param_binary(bytes, oid),
        _ => Err(DecodeError::BadFormat),
    }
}

/// Decode a parameter in text format.
fn decode_param_text(bytes: &[u8], oid: Option<u32>) -> Result<Value, DecodeError> {
    let s = std::str::from_utf8(bytes).map_err(|_| DecodeError::BadBytes)?;
    // PG treats an empty oid (0) as "unspecified"; default to text-ish
    // and let the binder/runtime coerce.
    match oid.unwrap_or(0) {
        PG_OID_BOOL => match s {
            "t" | "true" | "1" | "TRUE" | "T" | "yes" | "YES" | "y" | "Y" | "on" | "ON" => {
                Ok(Value::Bool(true))
            }
            "f" | "false" | "0" | "FALSE" | "F" | "no" | "NO" | "n" | "N" | "off" | "OFF" => {
                Ok(Value::Bool(false))
            }
            _ => Err(DecodeError::BadBytes),
        },
        PG_OID_INT2 => s
            .parse::<i16>()
            .map(Value::Int16)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_INT4 => s
            .parse::<i32>()
            .map(Value::Int32)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_INT8 | PG_OID_OID => s
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_FLOAT4 => s
            .parse::<f32>()
            .map(Value::Float32)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_FLOAT8 => s
            .parse::<f64>()
            .map(Value::Float64)
            .map_err(|_| DecodeError::BadBytes),
        PG_OID_TEXT | PG_OID_VARCHAR | PG_OID_BPCHAR => Ok(Value::Text(s.to_string())),
        PG_OID_BYTEA => Ok(Value::Bytea(bytes.to_vec())),
        // No declared OID, or an OID we don't decode specially: best-effort
        // numeric-then-text fallback so libpq's `text` default still works
        // for "WHERE id = $1" with $1='42'.
        _ => Ok(s.parse::<i32>().map_or_else(
            |_| {
                s.parse::<i64>().map_or_else(
                    |_| {
                        s.parse::<f64>()
                            .map_or_else(|_| Value::Text(s.to_string()), Value::Float64)
                    },
                    Value::Int64,
                )
            },
            Value::Int32,
        )),
    }
}

/// Decode a parameter in binary format.
fn decode_param_binary(bytes: &[u8], oid: Option<u32>) -> Result<Value, DecodeError> {
    match oid.unwrap_or(0) {
        PG_OID_BOOL => {
            if bytes.len() != 1 {
                return Err(DecodeError::BadBytes);
            }
            Ok(Value::Bool(bytes[0] != 0))
        }
        PG_OID_INT2 => {
            let arr: [u8; 2] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Int16(i16::from_be_bytes(arr)))
        }
        PG_OID_INT4 => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Int32(i32::from_be_bytes(arr)))
        }
        PG_OID_INT8 | PG_OID_OID => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Int64(i64::from_be_bytes(arr)))
        }
        PG_OID_FLOAT4 => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Float32(f32::from_be_bytes(arr)))
        }
        PG_OID_FLOAT8 => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Float64(f64::from_be_bytes(arr)))
        }
        PG_OID_TEXT | PG_OID_VARCHAR | PG_OID_BPCHAR => {
            let s = std::str::from_utf8(bytes).map_err(|_| DecodeError::BadBytes)?;
            Ok(Value::Text(s.to_string()))
        }
        PG_OID_BYTEA => Ok(Value::Bytea(bytes.to_vec())),
        // Unknown OID with binary format: fall back to widths we can
        // disambiguate by length.
        _ => match bytes.len() {
            1 => Ok(Value::Bool(bytes[0] != 0)),
            2 => Ok(Value::Int16(i16::from_be_bytes([bytes[0], bytes[1]]))),
            4 => Ok(Value::Int32(i32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ]))),
            8 => Ok(Value::Int64(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))),
            _ => Ok(Value::Bytea(bytes.to_vec())),
        },
    }
}

// ---------------------------------------------------------------------------
// Result-column binary encoder.
// ---------------------------------------------------------------------------

/// Encode column row `row` of `col` in binary format. Falls back to
/// the text encoder for value types whose binary layout is not yet
/// implemented in v0.5 — float types, dates/times, etc. The fallback
/// is conservative (returning the text form) so the client sees a
/// well-formed `DataRow` even if the format code says binary; libpq
/// does not validate that the wire format matches its requested
/// format byte-for-byte.
fn encode_binary_value(col: &ultrasql_vec::column::Column, row: usize) -> Option<Vec<u8>> {
    use ultrasql_vec::column::Column;
    let nulls = match col {
        Column::Int32(c) => c.nulls(),
        Column::Int64(c) => c.nulls(),
        Column::Float32(c) => c.nulls(),
        Column::Float64(c) => c.nulls(),
        Column::Bool(c) => c.nulls(),
        Column::Utf8(c) => c.nulls(),
    };
    if let Some(b) = nulls {
        if !b.get(row) {
            return None;
        }
    }
    match col {
        Column::Bool(c) => Some(vec![u8::from(c.value(row))]),
        Column::Int32(c) => Some(c.data()[row].to_be_bytes().to_vec()),
        Column::Int64(c) => Some(c.data()[row].to_be_bytes().to_vec()),
        Column::Float32(c) => Some(c.data()[row].to_be_bytes().to_vec()),
        Column::Float64(c) => Some(c.data()[row].to_be_bytes().to_vec()),
        Column::Utf8(c) => Some(c.value(row).as_bytes().to_vec()),
    }
}

// ---------------------------------------------------------------------------
// RowDescription builder.
// ---------------------------------------------------------------------------

/// Build a `RowDescription` for the output schema of `plan`, or
/// `NoData` for plans that yield no rows.
fn row_description_for_plan(plan: &LogicalPlan) -> BackendMessage {
    // DDL, transaction-control, and modify-without-returning produce no row data.
    let no_rows = matches!(
        plan,
        LogicalPlan::CreateTable { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::AlterTable { .. }
            | LogicalPlan::Truncate { .. }
            | LogicalPlan::Begin { .. }
            | LogicalPlan::Commit { .. }
            | LogicalPlan::Rollback { .. }
            | LogicalPlan::Savepoint { .. }
            | LogicalPlan::RollbackToSavepoint { .. }
            | LogicalPlan::ReleaseSavepoint { .. }
            | LogicalPlan::PrepareTransaction { .. }
            | LogicalPlan::CommitPrepared { .. }
            | LogicalPlan::RollbackPrepared { .. }
    ) || matches!(plan, LogicalPlan::Insert { returning, .. } if returning.is_empty())
        || matches!(plan, LogicalPlan::Update { returning, .. } if returning.is_empty())
        || matches!(plan, LogicalPlan::Delete { returning, .. } if returning.is_empty());
    if no_rows {
        return BackendMessage::NoData;
    }
    let schema = plan.schema();
    let fields = schema
        .fields()
        .iter()
        .map(|f| FieldDescription {
            name: f.name.clone(),
            table_oid: 0,
            col_attnum: 0,
            type_oid: pg_type_oid(&f.data_type),
            type_size: pg_type_size(&f.data_type),
            type_modifier: -1,
            format_code: 0,
        })
        .collect();
    BackendMessage::RowDescription { fields }
}

const fn pg_type_oid(ty: &DataType) -> u32 {
    match ty {
        DataType::Bool => PG_OID_BOOL,
        DataType::Int16 => PG_OID_INT2,
        DataType::Int32 => PG_OID_INT4,
        DataType::Int64 => PG_OID_INT8,
        DataType::Float32 => PG_OID_FLOAT4,
        DataType::Float64 => PG_OID_FLOAT8,
        DataType::Bytea => PG_OID_BYTEA,
        _ => PG_OID_TEXT,
    }
}

const fn pg_type_size(ty: &DataType) -> i16 {
    match ty {
        DataType::Bool => 1,
        DataType::Int16 => 2,
        DataType::Int32 | DataType::Float32 => 4,
        DataType::Int64 | DataType::Float64 => 8,
        _ => -1,
    }
}

// ---------------------------------------------------------------------------
// Tag inference for the CommandComplete message.
// ---------------------------------------------------------------------------

/// Compute the `CommandComplete` tag for a plan. Used only when the
/// plan is a SELECT-like shape (Insert/Update/Delete have their own
/// tag-emitting paths through `run_modify_command`).
#[allow(dead_code)] // Kept for future use when Execute paths grow.
fn select_tag(rows: u64) -> String {
    format!("SELECT {rows}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_planner::InMemoryCatalog;

    fn fixture_catalog() -> InMemoryCatalog {
        let mut catalog = InMemoryCatalog::new();
        let _ = crate::pipeline::build_sample_database(&mut catalog);
        catalog
    }

    // ── Text-format param decoding ───────────────────────────────────────────

    #[test]
    fn decode_text_int4_parses() {
        let v = decode_param(Some(b"42"), 0, Some(PG_OID_INT4)).unwrap();
        assert_eq!(v, Value::Int32(42));
    }

    #[test]
    fn decode_text_int8_parses() {
        let v = decode_param(Some(b"9000000000"), 0, Some(PG_OID_INT8)).unwrap();
        assert_eq!(v, Value::Int64(9_000_000_000));
    }

    #[test]
    fn decode_text_bool_t_and_f() {
        assert_eq!(
            decode_param(Some(b"t"), 0, Some(PG_OID_BOOL)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            decode_param(Some(b"f"), 0, Some(PG_OID_BOOL)).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn decode_text_null_returns_null() {
        assert_eq!(
            decode_param(None, 0, Some(PG_OID_INT4)).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn decode_text_text_oid_returns_text() {
        let v = decode_param(Some(b"hello"), 0, Some(PG_OID_TEXT)).unwrap();
        assert_eq!(v, Value::Text("hello".to_string()));
    }

    #[test]
    fn decode_text_no_oid_infers_int32_for_numeric() {
        // libpq's "I haven't told you the type" path.
        let v = decode_param(Some(b"42"), 0, None).unwrap();
        assert_eq!(v, Value::Int32(42));
    }

    // ── Binary-format param decoding ─────────────────────────────────────────

    #[test]
    fn decode_binary_int4_parses() {
        let bytes = 42_i32.to_be_bytes();
        let v = decode_param(Some(&bytes), 1, Some(PG_OID_INT4)).unwrap();
        assert_eq!(v, Value::Int32(42));
    }

    #[test]
    fn decode_binary_int8_parses() {
        let bytes = 9_000_000_000_i64.to_be_bytes();
        let v = decode_param(Some(&bytes), 1, Some(PG_OID_INT8)).unwrap();
        assert_eq!(v, Value::Int64(9_000_000_000));
    }

    #[test]
    fn decode_binary_bool_byte() {
        let v = decode_param(Some(&[1]), 1, Some(PG_OID_BOOL)).unwrap();
        assert_eq!(v, Value::Bool(true));
        let v = decode_param(Some(&[0]), 1, Some(PG_OID_BOOL)).unwrap();
        assert_eq!(v, Value::Bool(false));
    }

    #[test]
    fn decode_binary_wrong_length_errors() {
        let three_bytes = [0_u8, 0, 0];
        let err = decode_param(Some(&three_bytes), 1, Some(PG_OID_INT4)).unwrap_err();
        assert!(matches!(err, DecodeError::BadBytes));
    }

    #[test]
    fn decode_unknown_format_errors() {
        let err = decode_param(Some(b"42"), 2, None).unwrap_err();
        assert!(matches!(err, DecodeError::BadFormat));
    }

    // ── Parameter substitution / counting ────────────────────────────────────

    #[test]
    fn substitute_simple_eq_predicate() {
        // SELECT id FROM users WHERE id = $1   →   id = 1
        let catalog = fixture_catalog();
        let mut state = ExtendedConnState::new();
        let _ = handle_parse(
            &mut state,
            "s1".to_string(),
            "SELECT id FROM users WHERE id = $1".to_string(),
            vec![PG_OID_INT4],
            &catalog,
        )
        .expect("parse ok");
        let stmt = state.statements.get("s1").unwrap();
        assert_eq!(stmt.n_params, 1);

        // Substitute and check the predicate became id = 1 literal.
        let sub = substitute_parameters_in_plan(stmt.plan.as_ref().unwrap(), &[Value::Int32(1)]);
        // The plan is Project(Filter(Scan)); reach into Filter.predicate.
        let mut found = false;
        walk_plan_exprs(&sub, &mut |e| {
            if let ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left,
                right,
                ..
            } = e
            {
                match (left.as_ref(), right.as_ref()) {
                    (
                        ScalarExpr::Column { .. },
                        ScalarExpr::Literal {
                            value: Value::Int32(1),
                            ..
                        },
                    )
                    | (
                        ScalarExpr::Literal {
                            value: Value::Int32(1),
                            ..
                        },
                        ScalarExpr::Column { .. },
                    ) => found = true,
                    _ => {}
                }
            }
        });
        assert!(found, "Parameter not substituted into Filter predicate");
    }

    #[test]
    fn counting_zero_parameters_returns_zero() {
        let catalog = fixture_catalog();
        let mut state = ExtendedConnState::new();
        handle_parse(
            &mut state,
            "s".to_string(),
            "SELECT id FROM users".to_string(),
            vec![],
            &catalog,
        )
        .expect("parse ok");
        assert_eq!(state.statements.get("s").unwrap().n_params, 0);
    }

    // ── Close / describe ─────────────────────────────────────────────────────

    #[test]
    fn close_removes_statement() {
        let catalog = fixture_catalog();
        let mut state = ExtendedConnState::new();
        handle_parse(
            &mut state,
            "s".to_string(),
            "SELECT id FROM users".to_string(),
            vec![],
            &catalog,
        )
        .expect("parse ok");
        let msg = handle_close(&mut state, DescribeKind::Statement, "s");
        assert!(matches!(msg, BackendMessage::CloseComplete));
        assert!(!state.statements.contains_key("s"));
    }

    #[test]
    fn describe_statement_emits_parameter_then_row_description() {
        let catalog = fixture_catalog();
        let mut state = ExtendedConnState::new();
        handle_parse(
            &mut state,
            "s".to_string(),
            "SELECT id FROM users WHERE id = $1".to_string(),
            vec![PG_OID_INT4],
            &catalog,
        )
        .expect("parse ok");
        let msgs = handle_describe_statement(
            &state,
            "s",
            Some(&catalog as &dyn ultrasql_planner::Catalog),
        )
        .expect("describe ok");
        assert_eq!(msgs.len(), 2);
        assert!(matches!(
            msgs[0],
            BackendMessage::ParameterDescription { .. }
        ));
        assert!(matches!(msgs[1], BackendMessage::RowDescription { .. }));
    }

    #[test]
    fn describe_portal_for_select_returns_row_description() {
        let catalog = fixture_catalog();
        let mut state = ExtendedConnState::new();
        handle_parse(
            &mut state,
            "s".to_string(),
            "SELECT id FROM users".to_string(),
            vec![],
            &catalog,
        )
        .expect("parse ok");
        handle_bind(&mut state, String::new(), "s", &[], &[], vec![], None).expect("bind ok");
        let msg = handle_describe_portal(&state, "").expect("describe ok");
        assert!(matches!(msg, BackendMessage::RowDescription { .. }));
    }

    #[test]
    fn bind_unknown_statement_errors() {
        let mut state = ExtendedConnState::new();
        let err = handle_bind(&mut state, String::new(), "nope", &[], &[], vec![], None)
            .expect_err("bind must fail");
        assert!(matches!(err, ServerError::Unsupported(_)));
    }
}
