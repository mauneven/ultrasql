//! Portal execution path: [`execute_portal`] runs an Execute message against
//! a previously bound portal; [`resume_suspended_portal`] resumes a portal
//! that was suspended by a prior `max_rows` cap.

use ultrasql_executor::Operator;
use ultrasql_planner::LogicalPlan;
use ultrasql_protocol::BackendMessage;

use crate::error::ServerError;
use crate::pipeline::{LowerCtx, lower_query};
use crate::result_encoder::{encode_text_value_typed, run_modify_command};

use super::codec::encode_binary_value_typed;
use super::handlers::resolve_param_format;
use super::{ExecuteOutcome, ExtendedConnState, SuspendedPortal};

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
    // Resume a previously-suspended portal if one exists under this
    // name. The suspended state owns the live operator + the partially
    // consumed batch, so the next `Execute` can pick up where the
    // previous one stopped.
    if let Some(sus) = state.suspended.remove(portal_name) {
        return resume_suspended_portal(state, portal_name, max_rows, sus);
    }

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
    if matches!(
        &plan,
        LogicalPlan::CreateTable { .. }
            | LogicalPlan::CreateMaterializedView { .. }
            | LogicalPlan::CreateTypeEnum { .. }
            | LogicalPlan::CreateTypeComposite { .. }
            | LogicalPlan::CreateDomain { .. }
            | LogicalPlan::CreateOperator { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::CreateRole { .. }
            | LogicalPlan::AlterRole { .. }
            | LogicalPlan::DropRole { .. }
            | LogicalPlan::CreateSequence { .. }
            | LogicalPlan::AlterSequence { .. }
            | LogicalPlan::DropSequence { .. }
            | LogicalPlan::Comment { .. }
    ) {
        return Err(ServerError::Unsupported(
            "DDL via Extended Query is not yet wired; use Simple Query",
        ));
    }

    // Build the operator tree. Repeated safe GROUP BY summaries can be
    // replayed from a version-scoped physical projection cache while keeping
    // Extended Query row-format handling in this path.
    let mut op: Box<dyn Operator> = if let Some(scan) =
        crate::projection_summary::try_build_cached_grouped_projection_scan(
            &plan,
            &ctx.catalog_snapshot,
            ctx.heap.as_ref(),
        ) {
        Box::new(scan)
    } else {
        lower_query(&plan, ctx)?
    };

    // INSERT/UPDATE/DELETE either produce a row count tag or, when
    // `RETURNING` is present, a row stream plus the DML-specific
    // `CommandComplete` tag. Like the pre-existing DML path, we drain
    // the full operator here rather than supporting portal suspension
    // for mutation statements.
    if let LogicalPlan::Insert { returning, .. } = &plan {
        if returning.is_empty() {
            let sel = run_modify_command(op.as_mut(), "INSERT")?;
            return Ok(ExecuteOutcome {
                messages: sel.messages,
            });
        }
        return execute_modify_returning(op.as_mut(), &portal.result_formats, "INSERT");
    }
    if let LogicalPlan::Update { returning, .. } = &plan {
        if returning.is_empty() {
            let sel = run_modify_command(op.as_mut(), "UPDATE")?;
            return Ok(ExecuteOutcome {
                messages: sel.messages,
            });
        }
        return execute_modify_returning(op.as_mut(), &portal.result_formats, "UPDATE");
    }
    if let LogicalPlan::Delete { returning, .. } = &plan {
        if returning.is_empty() {
            let sel = run_modify_command(op.as_mut(), "DELETE")?;
            return Ok(ExecuteOutcome {
                messages: sel.messages,
            });
        }
        return execute_modify_returning(op.as_mut(), &portal.result_formats, "DELETE");
    }

    // SELECT-like path. Drain row by row. Honor `result_formats` per
    // column. `max_rows = 0` means "no limit"; any positive value caps
    // and emits `PortalSuspended` when the cap is reached.
    let row_cap = if max_rows <= 0 {
        usize::MAX
    } else {
        usize::try_from(max_rows).unwrap_or(usize::MAX)
    };

    let output_schema = op.schema().clone();
    let mut messages: Vec<BackendMessage> = Vec::with_capacity(8);
    let mut emitted: u64 = 0;
    let mut suspended = false;

    let mut leftover: Option<(ultrasql_vec::Batch, usize)> = None;
    'outer: loop {
        let Some(batch) = op.next_batch()? else { break };
        let n = batch.rows();
        for row in 0..n {
            if usize::try_from(emitted).unwrap_or(usize::MAX) >= row_cap {
                // Save the remaining slice of this batch so the next
                // resumed Execute starts at row `row` of the same
                // batch, not at the next batch.
                leftover = Some((batch, row));
                suspended = true;
                break 'outer;
            }
            let mut columns = Vec::with_capacity(batch.width());
            for (col_idx, col) in batch.columns().iter().enumerate() {
                let fmt = resolve_param_format(&portal.result_formats, col_idx);
                let logical_type = &output_schema.field_at(col_idx).data_type;
                let encoded = encode_result_value(col, row, logical_type, fmt);
                columns.push(encoded);
            }
            messages.push(BackendMessage::DataRow { columns });
            emitted = emitted.saturating_add(1);
        }
    }

    if suspended {
        messages.push(BackendMessage::PortalSuspended);
        // Retain the in-flight operator so the next `Execute` against
        // this portal name resumes from the same row position rather
        // than restarting from scratch. The `Close` message (or session
        // drop) clears the entry.
        state.suspended.insert(
            portal_name.to_string(),
            SuspendedPortal {
                op,
                leftover,
                emitted,
                result_formats: portal.result_formats.clone(),
            },
        );
    } else {
        messages.push(BackendMessage::CommandComplete {
            tag: format!("SELECT {emitted}"),
        });
    }

    Ok(ExecuteOutcome { messages })
}

fn execute_modify_returning(
    op: &mut dyn Operator,
    result_formats: &[i16],
    command: &str,
) -> Result<ExecuteOutcome, ServerError> {
    let output_schema = op.schema().clone();
    let mut messages: Vec<BackendMessage> = Vec::with_capacity(8);
    let mut emitted: u64 = 0;
    loop {
        let Some(batch) = op.next_batch()? else { break };
        let n = batch.rows();
        for row in 0..n {
            let mut columns = Vec::with_capacity(batch.width());
            for (col_idx, col) in batch.columns().iter().enumerate() {
                let fmt = resolve_param_format(result_formats, col_idx);
                let logical_type = &output_schema.field_at(col_idx).data_type;
                let encoded = encode_result_value(col, row, logical_type, fmt);
                columns.push(encoded);
            }
            messages.push(BackendMessage::DataRow { columns });
            emitted = emitted.saturating_add(1);
        }
    }
    messages.push(BackendMessage::CommandComplete {
        tag: modify_command_tag(command, emitted),
    });
    Ok(ExecuteOutcome { messages })
}

fn encode_result_value(
    col: &ultrasql_vec::column::Column,
    row: usize,
    logical_type: &ultrasql_core::DataType,
    format: i16,
) -> Option<Vec<u8>> {
    if format == 1 {
        encode_binary_value_typed(col, row, logical_type)
    } else {
        encode_text_value_typed(col, row, logical_type)
    }
}

fn modify_command_tag(command: &str, affected: u64) -> String {
    if command.eq_ignore_ascii_case("INSERT") {
        format!("INSERT 0 {affected}")
    } else {
        format!("{} {affected}", command.to_uppercase())
    }
}

/// Resume an `Execute` on a portal that previously emitted
/// `PortalSuspended`. Drives the retained operator forward; on
/// re-suspension, the portal is re-inserted into the suspension map.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn resume_suspended_portal(
    state: &mut ExtendedConnState,
    portal_name: &str,
    max_rows: i32,
    mut sus: SuspendedPortal,
) -> Result<ExecuteOutcome, ServerError> {
    let row_cap = if max_rows <= 0 {
        usize::MAX
    } else {
        usize::try_from(max_rows).unwrap_or(usize::MAX)
    };

    let output_schema = sus.op.schema().clone();
    let mut messages: Vec<BackendMessage> = Vec::with_capacity(8);
    let mut emitted_this_call: usize = 0;
    let mut suspended = false;

    let mut current = sus.leftover.take();

    'outer: loop {
        // Pull a fresh batch only when the leftover is exhausted.
        let (batch, start_row) = match current.take() {
            Some(pair) => pair,
            None => match sus.op.next_batch()? {
                Some(b) => (b, 0),
                None => break 'outer,
            },
        };
        let n = batch.rows();
        for row in start_row..n {
            if emitted_this_call >= row_cap {
                // Hit the cap mid-batch. Save the remaining slice so
                // the next resumption picks up at this row.
                current = Some((batch, row));
                suspended = true;
                break 'outer;
            }
            let mut columns = Vec::with_capacity(batch.width());
            for (col_idx, col) in batch.columns().iter().enumerate() {
                let fmt = resolve_param_format(&sus.result_formats, col_idx);
                let logical_type = &output_schema.field_at(col_idx).data_type;
                let encoded = encode_result_value(col, row, logical_type, fmt);
                columns.push(encoded);
            }
            messages.push(BackendMessage::DataRow { columns });
            emitted_this_call = emitted_this_call.saturating_add(1);
        }
    }

    sus.emitted = sus
        .emitted
        .saturating_add(u64::try_from(emitted_this_call).unwrap_or(u64::MAX));

    if suspended {
        sus.leftover = current;
        messages.push(BackendMessage::PortalSuspended);
        state.suspended.insert(portal_name.to_string(), sus);
    } else {
        messages.push(BackendMessage::CommandComplete {
            tag: format!("SELECT {emitted}", emitted = sus.emitted),
        });
    }

    Ok(ExecuteOutcome { messages })
}
