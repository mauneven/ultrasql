//! Portal execution path: [`execute_portal`] runs an Execute message against
//! a previously bound portal; [`resume_suspended_portal`] resumes a portal
//! that was suspended by a prior `max_rows` cap.

use ultrasql_executor::Operator;
use ultrasql_planner::LogicalPlan;
use ultrasql_protocol::BackendMessage;

use crate::error::ServerError;
use crate::pipeline::{LowerCtx, lower_query};
use crate::result_encoder::{
    TextEncodingOptions, encode_text_value_typed_with_options, run_modify_command,
};

use super::codec::encode_binary_value_typed;
use super::handlers::resolve_param_format;
use super::{ExecuteOutcome, ExtendedConnState, SuspendedPortal};

/// Execute the named portal and produce the message sequence.
///
/// Streams every row through the same execution path the Simple Query
/// dispatcher uses. `max_rows = 0` means "all rows" (the spec's
/// `INT32_MAX` shortcut). Any positive value caps the output and returns
/// `PortalSuspended`; the in-flight operator plus partial batch are kept
/// in session state so a later `Execute` on the same portal resumes at
/// the first un-emitted row.
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
            | LogicalPlan::CreateSchema { .. }
            | LogicalPlan::DropSchema { .. }
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
    let text_options = TextEncodingOptions::from_session_settings(ctx.session_settings.as_ref());

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
        return execute_modify_returning(
            op.as_mut(),
            &portal.result_formats,
            "INSERT",
            &text_options,
        );
    }
    if let LogicalPlan::Update { returning, .. } = &plan {
        if returning.is_empty() {
            let sel = run_modify_command(op.as_mut(), "UPDATE")?;
            return Ok(ExecuteOutcome {
                messages: sel.messages,
            });
        }
        return execute_modify_returning(
            op.as_mut(),
            &portal.result_formats,
            "UPDATE",
            &text_options,
        );
    }
    if let LogicalPlan::Delete { returning, .. } = &plan {
        if returning.is_empty() {
            let sel = run_modify_command(op.as_mut(), "DELETE")?;
            return Ok(ExecuteOutcome {
                messages: sel.messages,
            });
        }
        return execute_modify_returning(
            op.as_mut(),
            &portal.result_formats,
            "DELETE",
            &text_options,
        );
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
                let encoded = encode_result_value(col, row, logical_type, fmt, &text_options);
                columns.push(encoded);
            }
            messages.push(BackendMessage::DataRow { columns });
            increment_u64_row_count(&mut emitted)?;
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
                text_options,
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
    text_options: &TextEncodingOptions,
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
                let encoded = encode_result_value(col, row, logical_type, fmt, text_options);
                columns.push(encoded);
            }
            messages.push(BackendMessage::DataRow { columns });
            increment_u64_row_count(&mut emitted)?;
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
    text_options: &TextEncodingOptions,
) -> Option<Vec<u8>> {
    if format == 1 {
        encode_binary_value_typed(col, row, logical_type)
    } else {
        encode_text_value_typed_with_options(col, row, logical_type, text_options)
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
                let encoded = encode_result_value(col, row, logical_type, fmt, &sus.text_options);
                columns.push(encoded);
            }
            messages.push(BackendMessage::DataRow { columns });
            emitted_this_call =
                emitted_this_call
                    .checked_add(1)
                    .ok_or(ServerError::Unsupported(
                        "extended query row count overflow",
                    ))?;
        }
    }

    sus.emitted = add_emitted_rows(sus.emitted, emitted_this_call)?;

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

fn increment_u64_row_count(count: &mut u64) -> Result<(), ServerError> {
    *count = count.checked_add(1).ok_or(ServerError::Unsupported(
        "extended query row count overflow",
    ))?;
    Ok(())
}

fn add_emitted_rows(total: u64, delta: usize) -> Result<u64, ServerError> {
    let delta = u64::try_from(delta)
        .map_err(|_| ServerError::Unsupported("extended query row count exceeds protocol limit"))?;
    total.checked_add(delta).ok_or(ServerError::Unsupported(
        "extended query row count overflow",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_executor::ExecError;
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    #[derive(Debug)]
    struct TestOperator {
        schema: Schema,
    }

    impl Operator for TestOperator {
        fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
            Ok(None)
        }

        fn schema(&self) -> &Schema {
            &self.schema
        }
    }

    #[test]
    fn resume_rejects_emitted_counter_overflow() {
        let schema = Schema::new([Field::nullable("v", DataType::Int32)]).expect("schema");
        let leftover =
            Batch::new([Column::Int32(NumericColumn::from_data(vec![1]))]).expect("batch");
        let sus = SuspendedPortal {
            op: Box::new(TestOperator {
                schema: schema.clone(),
            }),
            leftover: Some((leftover, 0)),
            emitted: u64::MAX,
            result_formats: Vec::new(),
            text_options: TextEncodingOptions::default(),
        };
        let mut state = ExtendedConnState::new();

        let err = resume_suspended_portal(&mut state, "p", 0, sus)
            .expect_err("resumed row counter must not saturate");

        assert!(
            matches!(err, ServerError::Unsupported(message) if message.contains("row count overflow")),
            "unexpected error: {err:?}"
        );
        assert!(!state.suspended.contains_key("p"));
    }
}
