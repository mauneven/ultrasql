//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

use std::io::{Error as IoError, ErrorKind, IoSlice};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::debug;
use ultrasql_parser::Parser;
use ultrasql_protocol::{BackendMessage, FrontendMessage, encode_backend};

use super::AutocommitAbortGuard;
use super::Session;
use super::notify::ReadOrNotify;
use super::timeout::StatementTimeoutGuard;
use crate::TxnState;
use crate::error::ServerError;
use crate::result_encoder::SelectResult;
use crate::workload::WorkloadQueryRecordRef;

const SHARED_STREAM_COPY_LIMIT_BYTES: usize = 16 * 1024 * 1024;

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
            // Race the socket read against the notification receiver so
            // an idle session can push a `NotificationResponse` the
            // moment it arrives, rather than waiting for the next
            // client-initiated `Sync`. We poll the two halves
            // explicitly via a manually-constructed future so neither
            // branch needs to share a borrow of `self` with the other.
            //
            // Cancel-safety:
            // - `tokio::io::AsyncReadExt::read_buf` is cancel-safe; the
            //   bytes already accumulated in `self.read_buf` survive a
            //   dropped read future.
            // - `mpsc::UnboundedReceiver::poll_recv` only consumes a
            //   record when it returns `Poll::Ready(Some(_))`.
            // Idle-read timeout. Matching PostgreSQL, the two idle timeouts are
            // mutually exclusive by transaction state: inside an open
            // transaction `idle_in_transaction_session_timeout` applies (an
            // abandoned txn holds its row/relation locks and pins vacuum, so on
            // expiry we roll it back and disconnect with SQLSTATE 25P03);
            // outside a transaction the server-level `idle_session_timeout`
            // applies (just close). Either being 0 means no bound.
            let (idle_timeout_ms, terminate_idle_in_txn) =
                if matches!(self.txn_state, TxnState::Idle) {
                    (self.state.idle_session_timeout_ms(), false)
                } else {
                    (self.idle_in_transaction_session_timeout_ms, true)
                };
            let read_outcome = if idle_timeout_ms == 0 {
                self.read_frontend_or_notify().await?
            } else {
                match tokio::time::timeout(
                    Duration::from_millis(idle_timeout_ms),
                    self.read_frontend_or_notify(),
                )
                .await
                {
                    Ok(outcome) => outcome?,
                    Err(_) => {
                        if terminate_idle_in_txn {
                            debug!(
                                target: "ultrasqld",
                                pid = self.pid,
                                idle_timeout_ms,
                                "idle-in-transaction timeout; rolling back and closing connection"
                            );
                            // Roll back the abandoned transaction so its locks
                            // are released, then tell the client why.
                            let _ = self.execute_rollback();
                            self.send(&BackendMessage::ErrorResponse {
                                fields: vec![
                                    (b'S', "FATAL".to_owned()),
                                    (b'V', "FATAL".to_owned()),
                                    (b'C', "25P03".to_owned()),
                                    (
                                        b'M',
                                        "terminating connection due to idle-in-transaction timeout"
                                            .to_owned(),
                                    ),
                                ],
                            })
                            .await?;
                        } else {
                            debug!(
                                target: "ultrasqld",
                                pid = self.pid,
                                idle_timeout_ms,
                                "idle session timeout; closing connection"
                            );
                        }
                        return Ok(());
                    }
                }
            };
            let msg = match read_outcome {
                ReadOrNotify::Frontend(m) => m,
                ReadOrNotify::Eof => return Ok(()),
                ReadOrNotify::Notification(record) => {
                    // Idle-path delivery. Encode and flush immediately —
                    // there is no in-flight pipeline to wait on.
                    let process_id = i32::from_le_bytes(record.notifier_pid.to_le_bytes());
                    self.send(&BackendMessage::NotificationResponse {
                        process_id,
                        channel: record.channel,
                        payload: record.payload,
                    })
                    .await?;
                    continue;
                }
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

    /// Run the synchronous executor entry point for one statement under a
    /// `catch_unwind` guard so a panic anywhere on the execution path
    /// (hundreds of `unwrap`/`expect`/`panic!`/index/arithmetic sites in the
    /// planner / optimizer / executor / storage layers) tears down only this
    /// statement, never the connection task and never — now that the release
    /// profile unwinds rather than aborts (`Cargo.toml`) — the whole process.
    ///
    /// On a caught panic this:
    /// - logs the panic payload + location server-side at `error` level (full
    ///   detail for operators), and
    /// - transitions an open explicit transaction block to `Failed` via
    ///   [`Self::fail_if_in_transaction`] so the trailing `ReadyForQuery`
    ///   reports `'E'` and every subsequent statement in the block is rejected
    ///   with `25P02` until the client issues `ROLLBACK`/`COMMIT` — exactly the
    ///   abort behaviour a normal in-block error produces. (An autocommit
    ///   statement's own `Transaction` was a local inside `execute_query`. It
    ///   is dropped during the unwind, but `Transaction` has no `Drop`, so the
    ///   drop alone does NOT release its row locks — an
    ///   [`AutocommitAbortGuard`](super::AutocommitAbortGuard) armed around the
    ///   autocommit txn (in `run_dml_or_select`, and around the streaming drive
    ///   loop) aborts the XID on the unwind, releasing every lock and marking
    ///   the CLOG entry `Aborted`. No durable effects survive.)
    ///
    /// It returns [`ServerError::Internal`] (query-scoped → SQLSTATE `XX000`,
    /// generic "internal error" message). The caller surfaces it through the
    /// normal query-scoped error path, so the panic string is logged but never
    /// sent to the client.
    ///
    /// `AssertUnwindSafe` is sound here: on a caught panic we do not resume
    /// using any executor-internal state that the panic might have left torn —
    /// we only touch `self.txn_state` (to mark the block failed) and return an
    /// error. The shared cross-connection state lives behind `parking_lot` /
    /// `DashMap` / `ArcSwap` (no lock poisoning) or has explicit
    /// poison-recovery, so a sibling connection is unaffected.
    pub(crate) fn execute_query_catching_panics(
        &mut self,
        sql: &str,
        allow_streaming: bool,
    ) -> Result<SelectResult, ServerError> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.execute_query(sql, allow_streaming)
        })) {
            Ok(outcome) => outcome,
            Err(payload) => Err(self.on_caught_statement_panic(payload, sql)),
        }
    }

    /// Shared handling for a panic caught by a per-statement guard
    /// (`'Q'` and Extended-Query paths). Logs full detail, aborts an open
    /// explicit block, and returns the generic [`ServerError::Internal`].
    pub(crate) fn on_caught_statement_panic(
        &mut self,
        payload: Box<dyn std::any::Any + Send>,
        sql: &str,
    ) -> ServerError {
        let detail = panic_payload_message(&payload);
        // Server-side log carries the full payload + offending statement; the
        // client never sees either (it gets only a generic `XX000`).
        tracing::error!(
            target: "ultrasqld",
            pid = self.pid,
            panic = %detail,
            statement = %sql,
            "statement execution panicked; isolating to this connection \
             (server and other connections keep running)"
        );
        // Abort an open explicit transaction block so its uncommitted effects
        // are discarded and the block reports `'E'` / rejects further
        // statements with 25P02, mirroring a normal in-block error.
        self.fail_if_in_transaction(ServerError::Internal)
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
        if simple_query_is_empty(trimmed) {
            // Coalesce `EmptyQueryResponse` + any pending notifications
            // + `ReadyForQuery` into one `write_all` so the empty-query
            // reply stays a single syscall round-trip.
            let mut scratch = std::mem::take(&mut self.write_buf);
            scratch.clear();
            encode_backend(&BackendMessage::EmptyQueryResponse, &mut scratch);
            self.drain_pending_notifications_into(&mut scratch);
            encode_backend(
                &BackendMessage::ReadyForQuery {
                    status: self.txn_state.ready_for_query_status(),
                },
                &mut scratch,
            );
            let res = self.io.write_all(&scratch).await;
            scratch.clear();
            self.write_buf = scratch;
            res?;
            self.io.flush().await?;
            return Ok(());
        }

        if simple_query_needs_batch_parse(trimmed) {
            let cached_batch = self.simple_batch_cache.borrow().get(trimmed).cloned();
            if let Some(statements) = cached_batch {
                return self.handle_query_batch_strings(&statements).await;
            }
            if let Ok(statement_slices) = Parser::new(trimmed).parse_statement_slices()
                && statement_slices.len() > 1
            {
                let statements = Arc::new(
                    statement_slices
                        .iter()
                        .map(|statement| statement.trim().to_owned())
                        .filter(|statement| !simple_query_is_empty(statement))
                        .collect::<Vec<_>>(),
                );
                if statements.len() > 1 {
                    self.simple_batch_cache
                        .borrow_mut()
                        .insert(trimmed.to_owned(), Arc::clone(&statements));
                    return self.handle_query_batch_strings(&statements).await;
                }
            }
        }

        // COPY needs the async wire flow.
        match self.try_bind_copy_plan(trimmed) {
            Ok(Some(plan)) => return self.handle_copy_statement(&plan).await,
            Ok(None) => {}
            Err(err) => {
                if !err.is_query_scoped() {
                    return Err(err);
                }
                return self
                    .send_error_with_ready(&err.to_string(), err.sqlstate())
                    .await;
            }
        }

        let started = Instant::now();
        let timeout_guard =
            StatementTimeoutGuard::arm(self.statement_timeout_ms, self.cancel_flag.clone());
        self.state
            .workload_recorder
            .set_session_active(self.pid, trimmed.to_string());
        // Single-statement Simple-Query path: this is the only consumer that
        // drives a streaming handle (`send_query_result_with_ready` →
        // `drive_streaming_select`), so it is the only caller that may
        // request streaming. Wrapped in `catch_unwind` so an executor-path
        // panic is isolated to this statement instead of unwinding the
        // connection task (and, under the old panic=abort, the whole server).
        let outcome = self.execute_query_catching_panics(trimmed, true);
        self.state.workload_recorder.set_session_idle(self.pid);
        let elapsed = started.elapsed();
        let rows = outcome.as_ref().map_or(0, |result| result.rows);
        let error = outcome.as_ref().err().map(ToString::to_string);
        self.log_completed_statement(trimmed, elapsed, rows, error.as_deref());
        self.state
            .workload_recorder
            .record_ref(WorkloadQueryRecordRef {
                query: trimmed,
                plan_hash: 0,
                elapsed,
                rows,
                error: error.as_deref(),
                bind_param_count: 0,
                bind_params_redacted: false,
            });

        // Keep the statement-timeout guard ARMED across the response send,
        // not just the execute. A large SELECT streams its windows during
        // `send_query_result_with_ready` → `drive_streaming_select`; the
        // operator polls the cancel flag between batches, so the guard must
        // stay live for the whole windowed drain. Dropping it before the
        // drain (as the pre-streaming code did) left every window after
        // window 0 running with no statement_timeout — a regression vs. the
        // buffered path, which ran entirely under the guard. On timeout the
        // flag trips, the next `encode_window` returns `Cancelled`, and the
        // inline mid-stream error path emits `57014` + `ReadyForQuery` and
        // aborts the txn. The guard is dropped once the send completes
        // (below), so post-response maintenance runs unguarded as before.
        let (send_result, ran_query) = match outcome {
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
                (self.send_query_result_with_ready(result).await, true)
            }
            Err(err) => {
                if !err.is_query_scoped() {
                    // The guard drops here on the function-fatal error path,
                    // resetting the cancel flag (mirrors the pre-streaming
                    // ordering for a non-query-scoped error).
                    drop(timeout_guard);
                    return Err(err);
                }
                (
                    self.send_error_with_ready(&err.to_string(), err.sqlstate())
                        .await,
                    false,
                )
            }
        };
        // Drain (or error report) finished: disarm the timer and reset the
        // cancel flag before any post-response maintenance.
        drop(timeout_guard);
        send_result?;
        // Post-response maintenance runs only after a successful query
        // result (not after a query-scoped error), exactly as before.
        if ran_query && matches!(self.txn_state, TxnState::Idle) {
            self.run_post_response_maintenance();
        }
        Ok(())
    }

    async fn handle_query_batch(&mut self, statements: &[&str]) -> Result<(), ServerError> {
        let mut scratch = std::mem::take(&mut self.write_buf);
        scratch.clear();

        for statement in statements {
            let trimmed = statement.trim();
            if simple_query_is_empty(trimmed) {
                continue;
            }
            match self.try_bind_copy_plan(trimmed) {
                Ok(Some(_)) => {
                    let err = ServerError::Unsupported(
                        "COPY is not supported inside a multi-statement Simple Query batch",
                    );
                    Self::encode_error_response(&mut scratch, &err.to_string(), err.sqlstate());
                    break;
                }
                Ok(None) => {}
                Err(err) => {
                    if !err.is_query_scoped() {
                        scratch.clear();
                        self.write_buf = scratch;
                        return Err(err);
                    }
                    Self::encode_error_response(&mut scratch, &err.to_string(), err.sqlstate());
                    break;
                }
            }

            let started = Instant::now();
            let timeout_guard =
                StatementTimeoutGuard::arm(self.statement_timeout_ms, self.cancel_flag.clone());
            self.state
                .workload_recorder
                .set_session_active(self.pid, trimmed.to_string());
            // Multi-statement batch: `encode_query_result_body` cannot drive
            // a streaming handle, so the batch path must never request one.
            // A streamed SELECT here would ship only window 0 (no
            // CommandComplete → wire corruption) and leak the XID held by
            // the dropped handle. Force the whole-buffer path with `false`.
            // Caught-panic isolated per statement, as on the single path.
            let outcome = self.execute_query_catching_panics(trimmed, false);
            self.state.workload_recorder.set_session_idle(self.pid);
            drop(timeout_guard);
            let elapsed = started.elapsed();
            let rows = outcome.as_ref().map_or(0, |result| result.rows);
            let error = outcome.as_ref().err().map(ToString::to_string);
            self.log_completed_statement(trimmed, elapsed, rows, error.as_deref());
            self.state
                .workload_recorder
                .record_ref(WorkloadQueryRecordRef {
                    query: trimmed,
                    plan_hash: 0,
                    elapsed,
                    rows,
                    error: error.as_deref(),
                    bind_param_count: 0,
                    bind_params_redacted: false,
                });

            match outcome {
                Ok(result) => Self::encode_query_result_body(&mut scratch, result),
                Err(err) => {
                    if !err.is_query_scoped() {
                        scratch.clear();
                        self.write_buf = scratch;
                        return Err(err);
                    }
                    Self::encode_error_response(&mut scratch, &err.to_string(), err.sqlstate());
                    break;
                }
            }
        }

        self.drain_pending_notifications_into(&mut scratch);
        encode_backend(
            &BackendMessage::ReadyForQuery {
                status: self.txn_state.ready_for_query_status(),
            },
            &mut scratch,
        );
        let res = self.io.write_all(&scratch).await;
        scratch.clear();
        self.write_buf = scratch;
        res?;
        self.io.flush().await?;
        if matches!(self.txn_state, TxnState::Idle) {
            self.run_post_response_maintenance();
        }
        Ok(())
    }

    async fn handle_query_batch_strings(
        &mut self,
        statements: &[String],
    ) -> Result<(), ServerError> {
        let statement_refs = statements.iter().map(String::as_str).collect::<Vec<_>>();
        self.handle_query_batch(&statement_refs).await
    }

    fn encode_query_result_body(scratch: &mut BytesMut, mut result: SelectResult) {
        // The batch path cannot drive a streaming handle. The dispatch
        // context passes `allow_streaming: false` for every batch statement,
        // so the SELECT arm can never have produced one; assert it to catch
        // any future regression that re-enables streaming on this path.
        debug_assert!(
            result.streaming.is_none(),
            "encode_query_result_body received a streaming SelectResult; \
             the batch path cannot drive it (allow_streaming must be false)"
        );
        if let Some(body) = result.streamed_body.take() {
            scratch.extend_from_slice(&body);
            return;
        }
        if let Some(body) = result.shared_streamed_body.take() {
            scratch.extend_from_slice(body.as_ref());
            return;
        }
        for msg in &result.messages {
            encode_backend(msg, scratch);
        }
    }

    fn encode_error_response(scratch: &mut BytesMut, message: &str, sqlstate: &str) {
        encode_backend(
            &BackendMessage::ErrorResponse {
                fields: vec![
                    (b'S', "ERROR".to_string()),
                    (b'C', sqlstate.to_string()),
                    (b'M', message.to_string()),
                ],
            },
            scratch,
        );
    }

    pub(in crate::session) fn log_completed_statement(
        &self,
        sql: &str,
        elapsed: Duration,
        rows: u64,
        error: Option<&str>,
    ) {
        let config = self.state.logging_config();
        let class = statement_log_class(sql);
        let duration_match = if config.log_min_duration_statement_ms >= 0 {
            let threshold_ms =
                u64::try_from(config.log_min_duration_statement_ms).unwrap_or(u64::MAX);
            elapsed >= Duration::from_millis(threshold_ms)
        } else {
            false
        };
        let class_match = match config.log_statement {
            crate::LogStatementMode::None => false,
            crate::LogStatementMode::Ddl => class == "ddl",
            crate::LogStatementMode::Mod => class == "ddl" || class == "mod",
            crate::LogStatementMode::All => true,
        };
        if !duration_match && !class_match {
            return;
        }

        let elapsed_us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        tracing::info!(
            target: "ultrasqld::statement",
            statement = %sql,
            statement_class = class,
            elapsed_us,
            rows,
            error = error.unwrap_or(""),
            "statement completed"
        );
    }

    /// Send the query result and the trailing `ReadyForQuery` in one
    /// `write_all`. See `handle_query` for motivation.
    ///
    /// Any pending `LISTEN` notifications are appended *between* the
    /// result body and `ReadyForQuery` so libpq routes them via the
    /// async-message callback before the next query begins. The drain
    /// is non-blocking (`try_recv`) — only records the hub has already
    /// delivered participate.
    #[inline]
    pub(crate) async fn send_query_result_with_ready(
        &mut self,
        mut result: SelectResult,
    ) -> Result<(), ServerError> {
        let ready = BackendMessage::ReadyForQuery {
            status: self.txn_state.ready_for_query_status(),
        };
        // Windowed-streaming path (highest precedence): a large top-level
        // Simple-Query SELECT whose body exceeded the window high-water
        // mark. `streamed_body` carries window 0 (RowDescription + the
        // first window of DataRows); `streaming` carries the still-live
        // operator. Ship window 0, then drive `encode_window` →
        // `write_all().await` → clear in a loop so peak memory stays
        // bounded by one window regardless of result size, and a slow
        // client throttles the operator pull (the only await is the
        // socket write; no lock is held across it).
        if let Some(handle) = result.streaming.take() {
            let window0 = result.streamed_body.take().unwrap_or_default();
            return self.drive_streaming_select(handle, window0, ready).await;
        }
        // Streamed-body path: append notifications + `ReadyForQuery`
        // directly to the result's existing `BytesMut` and write it out
        // without an extra round through `self.write_buf`. For a
        // 10 000-row `select_scan_10k` response that streamed body is
        // ~250 KB; copying it into a second buffer used to add a memcpy
        // of the whole response on every query. Appending the trailer
        // bytes to the tail keeps the wire reply on a single
        // `write_all` + `flush` and saves the per-byte copy.
        //
        // After the write completes the buffer is parked back in the
        // per-thread `result_encoder` pool so the next SELECT in this
        // task reuses the same `BytesMut` allocation. Without the
        // park, every iteration of `cross_compare_sql --workload
        // select-scan` paid a fresh allocator round for the ~250 KiB
        // reply buffer.
        if let Some(mut body) = result.streamed_body.take() {
            self.drain_pending_notifications_into(&mut body);
            encode_backend(&ready, &mut body);
            let res = self.io.write_all(&body).await;
            body.clear();
            self.write_buf = body;
            res?;
            self.io.flush().await?;
            return Ok(());
        }
        if let Some(body) = result.shared_streamed_body.take() {
            // Same-host scan responses under this cap are faster as one
            // contiguous `write_all` than as repeated partial `writev`
            // progress over a shared body plus tiny trailer. The cap keeps
            // very large result sets on the zero-copy shared-body path.
            if body.len() <= SHARED_STREAM_COPY_LIMIT_BYTES {
                let mut scratch = std::mem::take(&mut self.write_buf);
                scratch.clear();
                scratch.extend_from_slice(body.as_ref());
                self.drain_pending_notifications_into(&mut scratch);
                encode_backend(&ready, &mut scratch);
                let res = self.io.write_all(&scratch).await;
                scratch.clear();
                self.write_buf = scratch;
                res?;
                self.io.flush().await?;
                return Ok(());
            }
            let mut scratch = std::mem::take(&mut self.write_buf);
            scratch.clear();
            self.drain_pending_notifications_into(&mut scratch);
            encode_backend(&ready, &mut scratch);
            let res = self.write_all_vectored_pair(body.as_ref(), &scratch).await;
            scratch.clear();
            self.write_buf = scratch;
            res?;
            self.io.flush().await?;
            return Ok(());
        }
        let mut scratch = std::mem::take(&mut self.write_buf);
        scratch.clear();
        for msg in &result.messages {
            encode_backend(msg, &mut scratch);
        }
        self.drain_pending_notifications_into(&mut scratch);
        encode_backend(&ready, &mut scratch);
        let res = self.io.write_all(&scratch).await;
        scratch.clear();
        self.write_buf = scratch;
        res?;
        self.io.flush().await?;
        Ok(())
    }

    /// Drive a large streaming SELECT to completion: ship window 0, then
    /// loop `encode_window` → `write_all().await` → clear until EOF,
    /// appending notifications + `ReadyForQuery` to the final window.
    ///
    /// Backpressure: the only await is `write_all`; the operator pull
    /// (`encode_window`) runs synchronously to the high-water mark and
    /// returns *before* the await, so no operator borrow or lock is held
    /// across it. A slow client stalls the await, which suspends the loop
    /// and throttles the next pull — peak memory stays bounded by one
    /// window plus one overshooting batch.
    ///
    /// Transaction ordering (cursor semantics): the autocommit txn carried
    /// in the handle is committed only *after* a clean drain. On an
    /// operator error mid-stream, ≥1 window (window 0) has already been
    /// flushed, so per the protocol we emit `…DataRow · ErrorResponse ·
    /// ReadyForQuery` inline (no `CommandComplete`) and abort the txn;
    /// returning `Ok(())` keeps `handle_query` from double-reporting the
    /// error. A socket write error tears the connection down and
    /// propagates as today.
    async fn drive_streaming_select(
        &mut self,
        mut handle: Box<crate::result_encoder::StreamingSelect>,
        mut window0: BytesMut,
        ready: BackendMessage,
    ) -> Result<(), ServerError> {
        // Commit/abort the autocommit txn (if any) after the drain. Held
        // in an `Option` and `take`n exactly once at the terminal arm so
        // the per-window loop never moves it.
        let mut commit_txn = handle.take_commit_txn();

        // Unwind guard for the autocommit XID. The panic-prone synchronous
        // `encode_window` pull now runs under a local `catch_unwind` in the loop
        // below — a mid-drain panic is converted to `ServerError::Internal` and
        // reported inline (which aborts the txn), so the connection survives with
        // a clean error instead of unwinding the future and dropping the socket.
        // This guard remains as a backstop for any unwind that escapes elsewhere
        // in the loop (e.g. the async write path), aborting the XID so its row
        // locks are never leaked — the buffered path's old failure mode. It is
        // disarmed before every normal `commit_txn.take()` (each of which itself
        // commits or aborts the handle). `None` `commit_txn` is an explicit-txn
        // streaming SELECT whose handle lives in `txn_state` — no autocommit XID.
        let mut abort_guard = commit_txn
            .as_ref()
            .map(|txn| AutocommitAbortGuard::arm(Arc::clone(&self.state.txn_manager), txn.xid));
        // Disarm the guard for a normal `commit_txn.take()` site. After this the
        // taken handle's own commit/abort owns the XID's fate.
        macro_rules! disarm_abort_guard {
            () => {
                if let Some(guard) = abort_guard.as_mut() {
                    guard.disarm();
                }
            };
        }

        // Window 0 is already encoded (RowDescription + first DataRows);
        // ship it first. If even this fails, the connection is dead.
        if let Err(e) = self.io.write_all(&window0).await {
            window0.clear();
            self.write_buf = window0;
            // No data acknowledged on our side; abort to release CLOG state.
            if let Some(txn) = commit_txn.take() {
                disarm_abort_guard!();
                let _ = self
                    .state
                    .abort_transaction(txn, false, "streaming SELECT write error");
            }
            return Err(e.into());
        }
        let mut buf = window0;

        loop {
            buf.clear();
            // Catch a panic in the synchronous operator pull (executor-grade
            // code over user data) and route it into the same inline mid-stream
            // error path an operator `Err` uses, so a mid-drain panic yields a
            // clean ErrorResponse + ReadyForQuery and the connection survives.
            // Only the sync pull is wrapped — `catch_unwind` cannot span the
            // `write_all().await`. `encode_window` borrows only `handle`+`buf`,
            // so `self` stays out of the closure.
            let window = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::result_encoder::encode_window(
                    &mut handle,
                    &mut buf,
                    crate::result_encoder::STREAM_WINDOW_HIGH_WATER_BYTES,
                )
            })) {
                Ok(result) => result,
                Err(payload) => {
                    // Log full detail server-side; the client gets a generic
                    // XX000 (panic strings never reach the wire). The `Err` arm
                    // below performs the txn abort via report_streaming_error_inline.
                    tracing::error!(
                        target: "ultrasqld",
                        pid = self.pid,
                        panic = %panic_payload_message(&payload),
                        "streaming SELECT drain panicked mid-drive; reporting inline"
                    );
                    Err(ServerError::Internal)
                }
            };
            match window {
                Ok(more) => {
                    if !more {
                        // Final window: append trailer (notifications +
                        // ReadyForQuery) so the tail order matches the
                        // buffered path exactly.
                        self.drain_pending_notifications_into(&mut buf);
                        encode_backend(&ready, &mut buf);
                    }
                    if let Err(e) = self.io.write_all(&buf).await {
                        buf.clear();
                        self.write_buf = buf;
                        if let Some(txn) = commit_txn.take() {
                            disarm_abort_guard!();
                            let _ = self.state.abort_transaction(
                                txn,
                                false,
                                "streaming SELECT write error",
                            );
                        }
                        return Err(e.into());
                    }
                    if !more {
                        // Drain complete. The final window — including
                        // `ReadyForQuery` — has already been written to the
                        // socket above. Commit the autocommit txn now (cursor
                        // semantics: rows were read under the snapshot, the
                        // commit finalises once exhausted).
                        //
                        // Defensive: this commit runs *after* `ReadyForQuery`
                        // is on the wire, so we must NOT surface a commit
                        // failure as a second `ErrorResponse`/`ReadyForQuery`
                        // (the client already saw a clean completion). For a
                        // read-only Read-Committed SELECT the commit cannot
                        // meaningfully fail; if it somehow does, log it and
                        // keep the client's view a clean success. The CLOG
                        // entry is left in whatever state `commit_transaction`
                        // reached — a read-only txn writes no heap, so there
                        // is nothing to roll back.
                        if let Some(txn) = commit_txn.take() {
                            disarm_abort_guard!();
                            match self.state.commit_transaction(
                                txn,
                                false,
                                "streaming SELECT autocommit",
                            ) {
                                Ok(()) => self.pending_post_commit_maintenance = true,
                                Err(e) => tracing::error!(
                                    target: "ultrasqld",
                                    pid = self.pid,
                                    error = %e,
                                    "post-drain autocommit commit failed after \
                                     ReadyForQuery was flushed; not re-reporting \
                                     to the client"
                                ),
                            }
                        }
                        buf.clear();
                        self.write_buf = buf;
                        self.io.flush().await?;
                        return Ok(());
                    }
                }
                Err(e) => {
                    // Operator error after ≥1 window flushed: report inline
                    // (no CommandComplete) and abort the txn. We own error
                    // reporting from here, so return Ok to avoid a second
                    // ErrorResponse from `handle_query`. This is a normal
                    // `Err` return from `encode_window`, not an unwind, so
                    // `report_streaming_error_inline` owns the abort — disarm.
                    let txn = commit_txn.take();
                    disarm_abort_guard!();
                    return self.report_streaming_error_inline(buf, txn, e).await;
                }
            }
        }
    }

    /// Emit `ErrorResponse · ReadyForQuery` after partial DataRows have
    /// already reached the client, abort the streaming txn, and park the
    /// buffer back. Used only once ≥1 window has been flushed.
    async fn report_streaming_error_inline(
        &mut self,
        mut buf: BytesMut,
        commit_txn: Option<ultrasql_txn::Transaction>,
        err: ServerError,
    ) -> Result<(), ServerError> {
        // Abort the autocommit txn (read-only, so no durable marker). For
        // an explicit-transaction streaming SELECT (`commit_txn` is None)
        // mark the in-flight block Failed so the trailing ReadyForQuery and
        // subsequent statements observe the aborted state, mirroring the
        // non-streaming `InTransaction → Failed` transition.
        if let Some(txn) = commit_txn {
            let _ = self
                .state
                .abort_transaction(txn, false, "streaming SELECT operator error");
        } else if let TxnState::InTransaction(txn) =
            std::mem::replace(&mut self.txn_state, TxnState::Idle)
        {
            self.txn_state = TxnState::Failed(txn);
        }

        let status = self.txn_state.ready_for_query_status();
        buf.clear();
        Self::encode_error_response(&mut buf, &err.to_string(), err.sqlstate());
        self.drain_pending_notifications_into(&mut buf);
        encode_backend(&BackendMessage::ReadyForQuery { status }, &mut buf);
        let res = self.io.write_all(&buf).await;
        buf.clear();
        self.write_buf = buf;
        res?;
        self.io.flush().await?;
        Ok(())
    }

    async fn write_all_vectored_pair(
        &mut self,
        mut first: &[u8],
        mut second: &[u8],
    ) -> std::io::Result<()> {
        while !first.is_empty() || !second.is_empty() {
            let written = if first.is_empty() {
                self.io.write_vectored(&[IoSlice::new(second)]).await?
            } else if second.is_empty() {
                self.io.write_vectored(&[IoSlice::new(first)]).await?
            } else {
                self.io
                    .write_vectored(&[IoSlice::new(first), IoSlice::new(second)])
                    .await?
            };
            if written == 0 {
                return Err(IoError::new(
                    ErrorKind::WriteZero,
                    "failed to write query response",
                ));
            }
            if written < first.len() {
                first = &first[written..];
            } else {
                let second_written = written.saturating_sub(first.len()).min(second.len());
                first = &[];
                second = &second[second_written..];
            }
        }
        Ok(())
    }

    /// Send an `ErrorResponse` immediately followed by any pending
    /// notifications and `ReadyForQuery` in one `write_all`.
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
        let mut scratch = std::mem::take(&mut self.write_buf);
        scratch.clear();
        encode_backend(&err, &mut scratch);
        self.drain_pending_notifications_into(&mut scratch);
        encode_backend(&ready, &mut scratch);
        let res = self.io.write_all(&scratch).await;
        scratch.clear();
        self.write_buf = scratch;
        res?;
        self.io.flush().await?;
        Ok(())
    }
}

/// Extract a human-readable message from a caught panic payload for
/// server-side logging. Panics carry either a `&'static str` (from
/// `panic!("literal")`) or a `String` (from `panic!("{}", x)` /
/// `unwrap`/`expect`); anything else is reported opaquely.
pub(crate) fn panic_payload_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

fn simple_query_is_empty(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b' ' | b'\t' | b'\n' | b'\r' | b';' => idx += 1,
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                idx += 2;
                while idx < bytes.len() && bytes[idx] != b'\n' {
                    idx += 1;
                }
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                idx += 2;
                while idx + 1 < bytes.len() && !(bytes[idx] == b'*' && bytes[idx + 1] == b'/') {
                    idx += 1;
                }
                if idx + 1 >= bytes.len() {
                    return true;
                }
                idx += 2;
            }
            _ => return false,
        }
    }
    true
}

fn simple_query_needs_batch_parse(sql: &str) -> bool {
    sql.as_bytes().contains(&b';')
}

fn statement_log_class(sql: &str) -> &'static str {
    let head = sql.split_whitespace().next().unwrap_or_default();
    if head.eq_ignore_ascii_case("create")
        || head.eq_ignore_ascii_case("alter")
        || head.eq_ignore_ascii_case("drop")
        || head.eq_ignore_ascii_case("truncate")
        || head.eq_ignore_ascii_case("comment")
        || head.eq_ignore_ascii_case("grant")
        || head.eq_ignore_ascii_case("revoke")
    {
        "ddl"
    } else if head.eq_ignore_ascii_case("insert")
        || head.eq_ignore_ascii_case("update")
        || head.eq_ignore_ascii_case("delete")
        || head.eq_ignore_ascii_case("copy")
        || head.eq_ignore_ascii_case("merge")
    {
        "mod"
    } else {
        "other"
    }
}

#[cfg(test)]
mod tests {
    use super::simple_query_needs_batch_parse;

    #[test]
    fn batch_parse_gate_skips_plain_single_statement_without_semicolon() {
        assert!(!simple_query_needs_batch_parse(
            "SELECT SUM(x) FROM bench_sum_shared"
        ));
        assert!(!simple_query_needs_batch_parse(
            "  SELECT AVG(x) FROM bench_avg_shared"
        ));
    }

    #[test]
    fn batch_parse_gate_keeps_any_semicolon_on_parser_path() {
        assert!(simple_query_needs_batch_parse("SELECT 1; SELECT 2"));
        assert!(simple_query_needs_batch_parse("SELECT 1;"));
        assert!(simple_query_needs_batch_parse("SELECT ';'"));
    }
}
