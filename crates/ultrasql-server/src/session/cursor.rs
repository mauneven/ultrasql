//! Server-side cursors: `DECLARE` / `FETCH` / `MOVE` / `CLOSE`.
//!
//! Scope (PostgreSQL-observable behavior first):
//!
//! - Forward-only, `WITHOUT HOLD` cursors inside an explicit
//!   transaction block. `DECLARE` outside a block is rejected with
//!   SQLSTATE `25P01` (`no_active_sql_transaction`), matching
//!   PostgreSQL's "DECLARE CURSOR can only be used in transaction
//!   blocks".
//! - The cursor's `SELECT` is executed and **materialized at `DECLARE`
//!   time** inside the transaction's snapshot; `FETCH` windows over the
//!   buffered rows. Row-visibility semantics match PostgreSQL's
//!   insensitive cursors; the trade-off is memory proportional to the
//!   result set rather than pipelined execution (documented in
//!   `docs/known-limitations.md`).
//! - `WITH HOLD`, `SCROLL`, backward/absolute `FETCH` directions,
//!   `MOVE`, and `BINARY` cursors are parsed but rejected with SQLSTATE
//!   `0A000` (`feature_not_supported`) and a hint.
//! - `COMMIT` / `ROLLBACK` / `PREPARE TRANSACTION` close every cursor
//!   (they are all `WITHOUT HOLD`); `FETCH` / `CLOSE` on a missing name
//!   is `34000` (`invalid_cursor_name`); a duplicate `DECLARE` is
//!   `42P03` (`duplicate_cursor`).
//!
//! Cursor statements are session-scoped meta statements (like
//! `PREPARE` / `EXECUTE`): they are dispatched before the binder and
//! never reach the planner.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::CatalogSnapshot;
use ultrasql_parser::ast::{CloseStmt, DeclareCursorStmt, FetchDirection, FetchStmt, Statement};
use ultrasql_planner::bind;
use ultrasql_protocol::BackendMessage;

use super::Session;
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};
use crate::{CombinedCatalog, TxnState};

/// A materialized server-side cursor.
///
/// Holds the `SELECT`'s wire-ready `RowDescription` plus the remaining
/// `DataRow` messages; `FETCH` drains from the front.
pub(crate) struct SessionCursor {
    /// The `RowDescription` replayed at the head of every `FETCH` reply.
    row_description: BackendMessage,
    /// Remaining rows, drained from the front by `FETCH`.
    rows: std::collections::VecDeque<BackendMessage>,
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Dispatch a cursor statement (`DECLARE` / `FETCH` / `MOVE` /
    /// `CLOSE`). Returns `Ok(None)` for every other statement so the
    /// caller continues to the binder.
    pub(crate) fn try_dispatch_cursor_statement(
        &mut self,
        stmt: &Statement,
        sql: &str,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<Option<SelectResult>, ServerError> {
        // A failed transaction block rejects every statement except
        // COMMIT/ROLLBACK with 25P02 — cursors included.
        match stmt {
            Statement::DeclareCursor(_) | Statement::Fetch(_) | Statement::Close(_) => {
                if matches!(self.txn_state, TxnState::Failed(_)) {
                    return Err(ServerError::TransactionAborted);
                }
            }
            _ => return Ok(None),
        }
        match stmt {
            Statement::DeclareCursor(declare) => self
                .execute_declare_cursor(declare, sql, catalog_snapshot)
                .map(Some),
            Statement::Fetch(fetch) => self.execute_fetch(fetch).map(Some),
            Statement::Close(close) => self.execute_close(close).map(Some),
            _ => Ok(None),
        }
    }

    /// `DECLARE name CURSOR FOR select` — run the SELECT to completion
    /// inside the open transaction and buffer its reply for `FETCH`.
    fn execute_declare_cursor(
        &mut self,
        declare: &DeclareCursorStmt,
        sql: &str,
        catalog_snapshot: &Arc<CatalogSnapshot>,
    ) -> Result<SelectResult, ServerError> {
        if declare.hold {
            return Err(self.fail_if_in_transaction(ServerError::unsupported(
                "DECLARE CURSOR WITH HOLD is not supported\nHINT:  declare the cursor WITHOUT \
                 HOLD (the default); holdable cursors that outlive their transaction are not \
                 yet implemented",
            )));
        }
        if declare.scroll {
            return Err(self.fail_if_in_transaction(ServerError::unsupported(
                "DECLARE SCROLL CURSOR is not supported\nHINT:  declare the cursor without \
                 SCROLL (or with NO SCROLL); only forward-only cursors are implemented",
            )));
        }
        if declare.binary {
            return Err(self.fail_if_in_transaction(ServerError::unsupported(
                "DECLARE BINARY CURSOR is not supported\nHINT:  declare the cursor without \
                 BINARY; cursor rows are returned in text format",
            )));
        }
        // PostgreSQL: a WITHOUT HOLD cursor cannot outlive its
        // transaction, so DECLARE requires an explicit block.
        if !matches!(self.txn_state, TxnState::InTransaction(_)) {
            return Err(ServerError::Savepoint(
                "DECLARE CURSOR can only be used in transaction blocks",
            ));
        }
        let name = declare.name.value.clone();
        if self.cursors.contains_key(&name) {
            return Err(self.fail_if_in_transaction(ServerError::DuplicateCursor(name)));
        }

        // Bind + execute the embedded SELECT exactly like a top-level
        // one (same privilege/RLS enforcement, same txn snapshot), with
        // streaming disabled so the reply is fully buffered.
        let select_stmt = Statement::Select(declare.select.clone());
        let combined = CombinedCatalog {
            snapshot: catalog_snapshot,
            fallback: &self.state.catalog,
            search_path: self.session_settings.get("search_path").map(String::as_str),
        };
        let plan = match bind(&select_stmt, &combined) {
            Ok(p) => p,
            Err(e) => return Err(self.fail_if_in_transaction(e.into())),
        };
        let result = self.execute_bound_plan(plan, sql, Arc::clone(catalog_snapshot), false)?;
        let (row_description, rows) = split_select_reply(result)?;
        self.cursors.insert(
            name,
            SessionCursor {
                row_description,
                rows,
            },
        );
        Ok(run_ddl_command("DECLARE CURSOR"))
    }

    /// `FETCH [direction] [FROM|IN] cursor` — return up to `count`
    /// buffered rows with the SELECT's `RowDescription` and a
    /// `FETCH n` tag. `MOVE` and scroll-only directions are rejected.
    fn execute_fetch(&mut self, fetch: &FetchStmt) -> Result<SelectResult, ServerError> {
        if fetch.is_move {
            return Err(self.fail_if_in_transaction(ServerError::unsupported(
                "MOVE is not supported\nHINT:  use FETCH to consume rows; repositioning a \
                 cursor without returning rows is not yet implemented",
            )));
        }
        let count = match fetch.direction {
            FetchDirection::Forward { count } => count,
            FetchDirection::Scrollable => {
                return Err(self.fail_if_in_transaction(ServerError::unsupported(
                    "backward or absolute FETCH is not supported\nHINT:  cursors are \
                     forward-only (NO SCROLL); fetch forward with FETCH [FORWARD] count or \
                     FETCH ALL",
                )));
            }
        };
        let name = &fetch.cursor.value;
        let Some(cursor) = self.cursors.get_mut(name) else {
            return Err(self.fail_if_in_transaction(ServerError::InvalidCursorName(name.clone())));
        };
        let take = match count {
            // `FETCH 0` is valid PostgreSQL: no rows, tag `FETCH 0`.
            Some(n) => usize::try_from(n).unwrap_or(0).min(cursor.rows.len()),
            None => cursor.rows.len(),
        };
        let mut messages = Vec::with_capacity(take + 2);
        messages.push(cursor.row_description.clone());
        messages.extend(cursor.rows.drain(..take));
        messages.push(BackendMessage::CommandComplete {
            tag: format!("FETCH {take}"),
        });
        Ok(SelectResult {
            messages,
            streamed_body: None,
            shared_streamed_body: None,
            streaming: None,
            rows: u64::try_from(take).unwrap_or(u64::MAX),
        })
    }

    /// `CLOSE { name | ALL }`.
    fn execute_close(&mut self, close: &CloseStmt) -> Result<SelectResult, ServerError> {
        match &close.cursor {
            Some(name) => {
                if self.cursors.remove(&name.value).is_none() {
                    return Err(self.fail_if_in_transaction(ServerError::InvalidCursorName(
                        name.value.clone(),
                    )));
                }
                Ok(run_ddl_command("CLOSE CURSOR"))
            }
            None => {
                self.cursors.clear();
                Ok(run_ddl_command("CLOSE CURSOR ALL"))
            }
        }
    }

    /// Drop every open cursor. Called when the transaction block ends
    /// (`COMMIT` / `ROLLBACK` / `PREPARE TRANSACTION`): all cursors are
    /// `WITHOUT HOLD`, so none survives its transaction.
    pub(crate) fn close_all_cursors(&mut self) {
        self.cursors.clear();
    }
}

/// Split a buffered SELECT reply into its `RowDescription` and
/// `DataRow` messages, dropping the trailing `CommandComplete`.
///
/// The pre-encoded fast paths (column-cache / projection-summary hits)
/// return the reply as encoded wire bytes instead of typed messages;
/// those bodies are decoded back into messages here so a cache hit at
/// `DECLARE` time behaves identically to a cold run.
fn split_select_reply(
    mut result: SelectResult,
) -> Result<(BackendMessage, std::collections::VecDeque<BackendMessage>), ServerError> {
    let messages = if result.messages.is_empty() {
        let body: &[u8] = if let Some(body) = &result.streamed_body {
            body.as_ref()
        } else if let Some(body) = &result.shared_streamed_body {
            body.as_ref()
        } else {
            &[]
        };
        decode_reply_body(body)?
    } else {
        std::mem::take(&mut result.messages)
    };

    let mut row_description = None;
    let mut rows = std::collections::VecDeque::new();
    for msg in messages {
        match msg {
            BackendMessage::RowDescription { .. } => row_description = Some(msg),
            BackendMessage::DataRow { .. } => rows.push_back(msg),
            _ => {}
        }
    }
    let row_description = row_description.ok_or(ServerError::Unsupported(
        "DECLARE CURSOR requires a row-returning SELECT",
    ))?;
    Ok((row_description, rows))
}

/// Decode a pre-encoded SELECT wire body back into typed messages.
fn decode_reply_body(body: &[u8]) -> Result<Vec<BackendMessage>, ServerError> {
    let mut buf = bytes::BytesMut::from(body);
    let mut messages = Vec::new();
    while let Some(msg) = ultrasql_protocol::decode_backend(&mut buf)? {
        messages.push(msg);
        if buf.is_empty() {
            break;
        }
    }
    Ok(messages)
}
