//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

#![allow(unused_imports)]

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, info, warn};
use ultrasql_catalog::{
    CatalogSnapshot, IndexEntry, MutableCatalog, PersistentCatalog, TableEntry,
};
use ultrasql_core::{DataType, PageId, RelationId, Value};
use ultrasql_optimizer::{NoStats, PlanCache, PlanCacheConfig, PlanCacheKey, StatsSource};
use ultrasql_parser::Parser;
use ultrasql_planner::{
    Catalog as PlannerCatalog, InMemoryCatalog, LogicalAlterTableAction, LogicalPlan, TableMeta,
    bind,
};
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_frontend, encode_backend};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, UpdateOptions};
use ultrasql_storage::page::Page;
use ultrasql_txn::{IsolationLevel, Transaction, TransactionManager};

use crate::error::ServerError;
use crate::extended;
use crate::pipeline::{self, LowerCtx, SampleTables};
use crate::result_encoder::{
    self, SelectResult, run_ddl_command, run_modify_command, run_select, run_select_streamed,
};
use crate::{
    BlankPageLoader, CombinedCatalog, Server, TxnState, notice_warning, run_plan_in_txn,
    decode_key_column,
};
use super::Session;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Main per-query loop. Returns on clean termination.
    ///
    /// Two message families are dispatched here:
    ///
    /// - Simple Query (`'Q'`) — parsed, bound, lowered, and executed
    ///   end-to-end in [`Self::handle_query`].
    /// - Extended Query (`Parse`/`Bind`/`Describe`/`Execute`/`Sync`/
    ///   `Close`/`Flush`) — routed to [`Self::handle_extended`]. The
    ///   spec defines a pipelined contract: errors silence subsequent
    ///   extended messages until a `Sync` resets the flag and the
    ///   server emits `ReadyForQuery`.
    pub(crate) async fn run(&mut self) -> Result<(), ServerError> {
        loop {
            let msg = match self.read_frontend().await {
                Ok(m) => m,
                Err(ServerError::UnexpectedEof) => return Ok(()),
                Err(other) => return Err(other),
            };
            match msg {
                FrontendMessage::Query { sql } => {
                    self.handle_query(&sql).await?;
                }
                FrontendMessage::Terminate => return Ok(()),
                FrontendMessage::Parse {
                    name,
                    sql,
                    param_types,
                } => {
                    self.handle_parse(name, sql, param_types).await?;
                }
                FrontendMessage::Bind {
                    portal_name,
                    statement_name,
                    param_formats,
                    params,
                    result_formats,
                } => {
                    self.handle_bind(
                        portal_name,
                        statement_name,
                        param_formats,
                        params,
                        result_formats,
                    )
                    .await?;
                }
                FrontendMessage::Describe { kind, name } => {
                    self.handle_describe(kind, &name).await?;
                }
                FrontendMessage::Execute { portal, max_rows } => {
                    self.handle_execute(&portal, max_rows).await?;
                }
                FrontendMessage::Sync => {
                    self.handle_sync().await?;
                }
                FrontendMessage::Close { kind, name } => {
                    self.handle_extended_close(kind, &name).await?;
                }
                FrontendMessage::Flush => {
                    self.handle_flush().await?;
                }
                FrontendMessage::Password { .. } => {
                    // Auth is not yet a state in the loop; if a client
                    // sends a Password out of nowhere we treat it as
                    // a query-scoped error.
                    self.send_error("password message outside auth flow", "08P01")
                        .await?;
                    self.send(&BackendMessage::ReadyForQuery {
                        status: self.txn_state.ready_for_query_status(),
                    })
                    .await?;
                }
                FrontendMessage::StartupMessage { .. } => {
                    // A second StartupMessage is a protocol violation.
                    return Err(ServerError::UnexpectedEof);
                }
                // The protocol enum is `#[non_exhaustive]`; future
                // additions trigger this arm and are reported as
                // query-scoped feature-not-supported.
                _ => {
                    self.send_error("unsupported frontend message", "0A000")
                        .await?;
                    self.send(&BackendMessage::ReadyForQuery {
                        status: self.txn_state.ready_for_query_status(),
                    })
                    .await?;
                }
            }
        }
    }

    /// Execute a simple `'Q'` query end-to-end and write the response.
    ///
    /// The trailing `ReadyForQuery`'s status byte reflects the
    /// session's [`TxnState`] *after* the statement has run: `'I'` for
    /// `Idle`, `'T'` for `InTransaction`, `'E'` for `Failed`. Drivers
    /// like tokio-postgres rely on this byte to decide whether to send
    /// a `ROLLBACK` on pool return.
    #[inline]
    pub(crate) async fn handle_query(&mut self, sql: &str) -> Result<(), ServerError> {
        let trimmed = sql.trim();
        if trimmed.is_empty() || trimmed == ";" {
            // Coalesce `EmptyQueryResponse` + `ReadyForQuery` into one
            // `write_all` so the empty-query reply stays a single
            // syscall round-trip.
            self.write_buf.clear();
            encode_backend(&BackendMessage::EmptyQueryResponse, &mut self.write_buf);
            encode_backend(
                &BackendMessage::ReadyForQuery {
                    status: self.txn_state.ready_for_query_status(),
                },
                &mut self.write_buf,
            );
            self.io.write_all(&self.write_buf).await?;
            self.io.flush().await?;
            return Ok(());
        }

        match self.execute_query(trimmed) {
            Ok(result) => {
                // Append the trailing `ReadyForQuery` to the same
                // wire-buffer the query result writes so the whole
                // response (CommandComplete / DataRow stream +
                // ReadyForQuery) ships in one `write_all` + `flush`.
                // Saves a per-statement syscall round-trip on the
                // simple-query path; cumulative impact is visible on
                // the cross_compare_sql bench shapes that issue one
                // statement per wire roundtrip (UPDATE / DELETE /
                // INSERT / mixed-oltp).
                self.send_query_result_with_ready(result).await?;
            }
            Err(err) => {
                if !err.is_query_scoped() {
                    return Err(err);
                }
                self.send_error_with_ready(&err.to_string(), err.sqlstate()).await?;
            }
        }
        Ok(())
    }

    /// Send the query result and the trailing `ReadyForQuery` in one
    /// `write_all`. See `handle_query` for motivation.
    #[inline]
    pub(crate) async fn send_query_result_with_ready(
        &mut self,
        mut result: SelectResult,
    ) -> Result<(), ServerError> {
        let ready = BackendMessage::ReadyForQuery {
            status: self.txn_state.ready_for_query_status(),
        };
        // Streamed-body path: append `ReadyForQuery` directly to the
        // result's existing `BytesMut` and write it out without an
        // extra round through `self.write_buf`. For a 10 000-row
        // `select_scan_10k` response that streamed body is ~250 KB;
        // copying it into a second buffer used to add a memcpy of
        // the whole response on every query. Appending `ready` (5
        // bytes) to the tail keeps the wire reply on a single
        // `write_all` + `flush` and saves the per-byte copy.
        if let Some(body) = result.streamed_body.as_mut() {
            encode_backend(&ready, body);
            self.io.write_all(body).await?;
            self.io.flush().await?;
            return Ok(());
        }
        self.write_buf.clear();
        for msg in &result.messages {
            encode_backend(msg, &mut self.write_buf);
        }
        encode_backend(&ready, &mut self.write_buf);
        self.io.write_all(&self.write_buf).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Send an `ErrorResponse` immediately followed by `ReadyForQuery`
    /// in one `write_all`.
    pub(crate) async fn send_error_with_ready(
        &mut self,
        message: &str,
        sqlstate: &str,
    ) -> Result<(), ServerError> {
        let err = BackendMessage::ErrorResponse {
            fields: vec![
                (b'S', "ERROR".to_string()),
                (b'C', sqlstate.to_string()),
                (b'M', message.to_string()),
            ],
        };
        let ready = BackendMessage::ReadyForQuery {
            status: self.txn_state.ready_for_query_status(),
        };
        self.write_buf.clear();
        encode_backend(&err, &mut self.write_buf);
        encode_backend(&ready, &mut self.write_buf);
        self.io.write_all(&self.write_buf).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Dispatch a [`SelectResult`] over the wire in a single
    /// `write_all` + `flush`.
    ///
    /// For the SELECT-streaming case the result carries a
    /// `streamed_body` blob of pre-encoded `RowDescription` /
    /// `DataRow` / `CommandComplete` bytes that we hand to the socket
    /// verbatim. Otherwise we fall back to the legacy
    /// `Vec<BackendMessage>` shape and coalesce its encoded form into
    /// one syscall.
    pub(crate) async fn send_query_result(&mut self, result: SelectResult) -> Result<(), ServerError> {
        if let Some(body) = result.streamed_body.as_ref() {
            self.send_raw(body).await
        } else {
            self.send_messages_coalesced(&result.messages).await
        }
    }

}
