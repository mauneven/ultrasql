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
            let idle_timeout_ms = self.state.idle_session_timeout_ms();
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
                        debug!(
                            target: "ultrasqld",
                            pid = self.pid,
                            idle_timeout_ms,
                            "idle session timeout; closing connection"
                        );
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
        let outcome = self.execute_query(trimmed);
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
                if matches!(self.txn_state, TxnState::Idle) {
                    self.run_post_response_maintenance();
                }
            }
            Err(err) => {
                if !err.is_query_scoped() {
                    return Err(err);
                }
                self.send_error_with_ready(&err.to_string(), err.sqlstate())
                    .await?;
            }
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
            let outcome = self.execute_query(trimmed);
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
