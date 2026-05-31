//! Session-level dispatch for the `COPY` statement.
//!
//! The synchronous `execute_query` path cannot drive `COPY`'s wire flow
//! because every `CopyData` frame is an async read/write. This module
//! reopens the `impl<RW> Session<RW>` block and handles `COPY` end-to-end
//! against the async I/O surface.
//!
//! ## Protocol
//!
//! ### `COPY t TO STDOUT`
//!
//! ```text
//! Server: CopyOutResponse { overall_format: 0, column_formats: [0; n] }
//! Server: CopyData(row_bytes)  ×N
//! Server: CopyDone
//! Server: CommandComplete { tag: "COPY N" }
//! Server: ReadyForQuery
//! ```
//!
//! ### `COPY t FROM STDIN`
//!
//! ```text
//! Server: CopyInResponse { overall_format: 0, column_formats: [0; n] }
//! Client: CopyData(chunk)  ×N
//! Client: CopyDone   -or-   CopyFail
//! Server: CommandComplete { tag: "COPY N" }    (on CopyDone)
//! Server: ErrorResponse                         (on CopyFail or row error)
//! Server: ReadyForQuery
//! ```

#![allow(unused_imports)]

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::warn;
use ultrasql_catalog::{CatalogSnapshot, TableEntry};
use ultrasql_core::csv::sniff_csv_text;
use ultrasql_core::{
    BitString, DataType, NetworkValue, RelationId, Schema, Value, coerce_bpchar_text,
    decode_pg_money_binary, decode_pg_numeric_binary, encode_pg_money_binary,
    encode_pg_numeric_binary, parse_decimal_text, parse_money_text, parse_time_text,
    parse_timetz_text,
};
use ultrasql_executor::RowCodec;
use ultrasql_parser::Parser;
use ultrasql_planner::{
    CopyDirection, CopyFormat as PlanCopyFormat, CopySource, LogicalPlan, bind,
};
use ultrasql_protocol::{BackendMessage, FrontendMessage, encode_backend};
use ultrasql_storage::heap::InsertOptions;
use ultrasql_txn::{IsolationLevel, Transaction};

use super::Session;
use super::jsonb_ingest::{JsonbShapeCache, encode_pg_binary_jsonb, parse_json_text};
use crate::CombinedCatalog;
use crate::copy::{
    CopyFormat as ServerCopyFormat, CopyOptions, copy_in_response_with_format,
    copy_out_response_with_format, encode_csv_row, encode_text_row, parse_csv_row, parse_text_row,
    parse_unquoted_csv_row_slices,
};
use crate::error::ServerError;

const COPY_INSERT_BATCH_ROWS: usize = 4096;
const COPY_AUTODETECT_SAMPLE_BYTES: usize = 64 * 1024;
const DEFAULT_COPY_BINARY_FILE_LIMIT_BYTES: u64 = 128 * 1024 * 1024;
const MICROS_PER_DAY: i64 = 86_400_000_000;

struct CopyRejectTarget {
    entry: TableEntry,
    codec: RowCodec,
    payload_batch: Vec<Vec<u8>>,
    rows: u64,
}

struct CopyRejectState {
    max_errors: u64,
    bad_rows: u64,
    target: Option<CopyRejectTarget>,
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn finalise_copy_from_commit(
        &self,
        txn: Transaction,
        rows_changed: u64,
        context: &str,
    ) -> Result<(), ServerError> {
        self.state
            .commit_transaction(txn, rows_changed > 0, context)
    }

    /// Best-effort parse + bind that returns `Some(plan)` only when `sql`
    /// is a single `COPY` statement.
    ///
    /// The prefix probe stays alloc-free: every non-COPY query (i.e.
    /// every `SELECT` / DML on the `select_scan_10k` hot path) lands
    /// here once per call, and a `String`-collecting lowercase round
    /// trip would dominate the per-query budget on a small relation.
    /// `sql.trim_start().as_bytes().get(..4)` returns the first four
    /// bytes (or `None` for shorter inputs); `eq_ignore_ascii_case`
    /// against the literal `b"COPY"` runs as a 4-byte compare without
    /// touching the heap.
    pub(crate) fn try_bind_copy_plan(
        &mut self,
        sql: &str,
    ) -> Result<Option<LogicalPlan>, ServerError> {
        let trimmed = sql.trim_start();
        let head = trimmed.as_bytes().get(..4).unwrap_or_default();
        if !head.eq_ignore_ascii_case(b"COPY") {
            return Ok(None);
        }

        let stmt = match Parser::new(sql).parse_statement() {
            Ok(s) => s,
            Err(e) => return Err(self.fail_if_in_transaction(e.into())),
        };
        if !matches!(stmt, ultrasql_parser::ast::Statement::Copy(_)) {
            return Ok(None);
        }
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.state.catalog,
        };
        let plan = match bind(&stmt, &combined) {
            Ok(p) => p,
            Err(e) => return Err(self.fail_if_in_transaction(e.into())),
        };
        Ok(Some(plan))
    }

    /// Dispatch a bound [`LogicalPlan::Copy`] end-to-end on the wire from
    /// the Simple Query path. Emits a trailing `ReadyForQuery` after the
    /// COPY completes (success or query-scoped error).
    pub(crate) async fn handle_copy_statement(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<(), ServerError> {
        self.handle_copy_statement_inner(plan, true).await
    }

    /// Dispatch a bound [`LogicalPlan::Copy`] from the Extended Query
    /// path. The Extended Query state machine emits its own
    /// `ReadyForQuery` from `handle_sync`; sending one here would
    /// duplicate it and confuse libpq clients that depend on a single
    /// RFQ per Sync.
    pub(crate) async fn handle_copy_statement_extended(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<(), ServerError> {
        self.handle_copy_statement_inner(plan, false).await
    }

    async fn handle_copy_statement_inner(
        &mut self,
        plan: &LogicalPlan,
        emit_ready_for_query: bool,
    ) -> Result<(), ServerError> {
        let outcome = self.run_copy_inner(plan, emit_ready_for_query).await;
        match outcome {
            Ok(()) => Ok(()),
            Err(err) => {
                if !err.is_query_scoped() {
                    return Err(err);
                }
                let err = self.fail_if_in_transaction(err);
                if emit_ready_for_query {
                    self.send_error_with_ready(&err.to_string(), err.sqlstate())
                        .await
                } else {
                    self.send_error(&err.to_string(), err.sqlstate()).await
                }
            }
        }
    }

    async fn run_copy_inner(
        &mut self,
        plan: &LogicalPlan,
        emit_ready_for_query: bool,
    ) -> Result<(), ServerError> {
        let LogicalPlan::Copy {
            relation,
            input,
            columns,
            direction,
            source,
            format,
            delimiter,
            null_str,
            header,
            auto_detect,
            ignore_errors,
            max_errors,
            reject_table,
            schema,
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "handle_copy_statement called with non-Copy plan",
            ));
        };

        let opts = CopyOptions {
            format: match format {
                PlanCopyFormat::Text => ServerCopyFormat::Text,
                PlanCopyFormat::Csv => ServerCopyFormat::Csv,
                PlanCopyFormat::Binary => ServerCopyFormat::Binary,
                PlanCopyFormat::Parquet => ServerCopyFormat::Parquet,
            },
            delimiter: *delimiter,
            null_str: null_str.clone(),
            header: *header,
            auto_detect: *auto_detect,
            ignore_errors: *ignore_errors,
            max_errors: if *ignore_errors && *max_errors == 0 {
                1000
            } else {
                *max_errors
            },
            reject_table: reject_table.clone(),
        };

        if let Some(input) = input {
            return self
                .copy_query_to_destination(input, source, schema, &opts, emit_ready_for_query)
                .await;
        }

        let relation = relation
            .as_ref()
            .ok_or(ServerError::Unsupported("COPY table target missing"))?;
        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        let entry = catalog_snapshot
            .tables
            .get(relation)
            .ok_or_else(|| {
                ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(relation.clone()))
            })?
            .clone();

        match direction {
            CopyDirection::To => match source {
                CopySource::Stdout => {
                    self.copy_to_stdout(&entry, columns, schema, &opts, emit_ready_for_query)
                        .await
                }
                CopySource::File(path) => {
                    let rows = self.copy_to_file(&entry, columns, schema, &opts, path)?;
                    self.send_copy_complete(rows, emit_ready_for_query).await
                }
                CopySource::Stdin => Err(ServerError::Unsupported("COPY TO STDIN is invalid")),
            },
            CopyDirection::From => match source {
                CopySource::Stdin => {
                    self.copy_from_stdin(&entry, columns, schema, &opts, emit_ready_for_query)
                        .await
                }
                CopySource::File(path) => {
                    self.copy_from_file(&entry, columns, schema, &opts, path, emit_ready_for_query)
                        .await
                }
                CopySource::Stdout => Err(ServerError::Unsupported("COPY FROM STDOUT is invalid")),
            },
        }
    }

    async fn copy_to_stdout(
        &mut self,
        entry: &TableEntry,
        columns: &[usize],
        schema: &Schema,
        opts: &CopyOptions,
        emit_ready_for_query: bool,
    ) -> Result<(), ServerError> {
        if opts.format == ServerCopyFormat::Parquet {
            return Err(ServerError::Unsupported(
                "parquet COPY TO requires a server-side file path",
            ));
        }
        let n_columns = schema.len();
        self.write_buf.clear();
        let format_code = copy_format_code(opts.format);
        encode_backend(
            &copy_out_response_with_format(n_columns, format_code),
            &mut self.write_buf,
        );
        self.io.write_all(&self.write_buf).await?;
        self.io.flush().await?;

        if opts.format == ServerCopyFormat::Binary {
            let (payload, rows_sent) = self.encode_table_binary_copy(entry, columns, schema)?;
            let mut wire_buf = BytesMut::with_capacity(payload.len() + 128);
            encode_backend(&BackendMessage::CopyData(payload), &mut wire_buf);
            encode_backend(&BackendMessage::CopyDone, &mut wire_buf);
            encode_backend(
                &BackendMessage::CommandComplete {
                    tag: format!("COPY {rows_sent}"),
                },
                &mut wire_buf,
            );
            if emit_ready_for_query {
                encode_backend(
                    &BackendMessage::ReadyForQuery {
                        status: self.txn_state.ready_for_query_status(),
                    },
                    &mut wire_buf,
                );
            }
            self.io.write_all(&wire_buf).await?;
            self.io.flush().await?;
            return Ok(());
        }

        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let codec = RowCodec::new(entry.schema.clone());

        let mut rows_sent: u64 = 0;
        let mut wire_buf = BytesMut::with_capacity(8 * 1024);

        if opts.header {
            let header_cells: Vec<Option<Vec<u8>>> = schema
                .fields()
                .iter()
                .map(|f| Some(f.name.as_bytes().to_vec()))
                .collect();
            let bytes = match opts.format {
                ServerCopyFormat::Text => encode_text_row(&header_cells, opts),
                ServerCopyFormat::Csv => encode_csv_row(&header_cells, opts),
                ServerCopyFormat::Binary | ServerCopyFormat::Parquet => Vec::new(),
            };
            encode_backend(&BackendMessage::CopyData(bytes), &mut wire_buf);
        }

        let scan_result: Result<(), ServerError> = {
            let scan = self.state.heap.scan_visible(
                rel,
                block_count,
                &txn.snapshot,
                self.state.txn_manager.as_ref(),
            );
            let mut iter_err: Option<ServerError> = None;
            for result in scan {
                let tup = match result {
                    Ok(t) => t,
                    Err(e) => {
                        iter_err = Some(ServerError::ddl(format!("COPY TO heap scan: {e}")));
                        break;
                    }
                };
                let row = match codec.decode(&tup.data) {
                    Ok(r) => r,
                    Err(e) => {
                        iter_err =
                            Some(ServerError::CopyFormat(format!("COPY TO row decode: {e}")));
                        break;
                    }
                };
                let cells: Vec<Option<Vec<u8>>> = if columns.is_empty() {
                    row.iter()
                        .zip(entry.schema.fields())
                        .map(|(value, field)| value_to_copy_cell(value, &field.data_type))
                        .collect()
                } else {
                    columns
                        .iter()
                        .map(|&i| {
                            let field = entry.schema.field_at(i);
                            row.get(i)
                                .and_then(|value| value_to_copy_cell(value, &field.data_type))
                        })
                        .collect()
                };
                let bytes = match opts.format {
                    ServerCopyFormat::Text => encode_text_row(&cells, opts),
                    ServerCopyFormat::Csv => encode_csv_row(&cells, opts),
                    ServerCopyFormat::Binary | ServerCopyFormat::Parquet => Vec::new(),
                };
                encode_backend(&BackendMessage::CopyData(bytes), &mut wire_buf);
                rows_sent = rows_sent.saturating_add(1);
            }
            if let Some(e) = iter_err {
                Err(e)
            } else {
                Ok(())
            }
        };

        if let Err(e) = scan_result {
            if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                warn!(error = %abort_err, "COPY TO autocommit abort failed");
            }
            return Err(e);
        }
        if let Err(e) = self.state.txn_manager.commit(txn) {
            warn!(error = %e, "COPY TO autocommit commit failed to finalise");
        }

        encode_backend(&BackendMessage::CopyDone, &mut wire_buf);
        encode_backend(
            &BackendMessage::CommandComplete {
                tag: format!("COPY {rows_sent}"),
            },
            &mut wire_buf,
        );
        if emit_ready_for_query {
            encode_backend(
                &BackendMessage::ReadyForQuery {
                    status: self.txn_state.ready_for_query_status(),
                },
                &mut wire_buf,
            );
        }
        self.io.write_all(&wire_buf).await?;
        self.io.flush().await?;
        Ok(())
    }

    async fn copy_from_stdin(
        &mut self,
        entry: &TableEntry,
        columns: &[usize],
        schema: &Schema,
        opts: &CopyOptions,
        emit_ready_for_query: bool,
    ) -> Result<(), ServerError> {
        if opts.format == ServerCopyFormat::Parquet {
            return Err(ServerError::Unsupported(
                "parquet COPY FROM requires a server-side file path",
            ));
        }
        let n_columns = schema.len();
        let format_code = copy_format_code(opts.format);
        self.send(&copy_in_response_with_format(n_columns, format_code))
            .await?;

        if opts.format == ServerCopyFormat::Binary {
            let bytes = self.collect_copy_stdin_bytes().await?;
            return self
                .copy_binary_bytes_into_table(entry, columns, schema, &bytes, emit_ready_for_query)
                .await;
        }

        let mut buffer: Vec<u8> = Vec::new();
        let mut payload_batch: Vec<Vec<u8>> = Vec::with_capacity(COPY_INSERT_BATCH_ROWS);
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let codec = RowCodec::new(entry.schema.clone());

        let mut rows_inserted: u64 = 0;
        let mut header_skipped = !opts.header;
        let mut received_done = false;
        let mut client_fail_message: Option<String> = None;

        loop {
            let msg = self.read_frontend().await?;
            match msg {
                FrontendMessage::CopyData(chunk) => {
                    buffer.extend_from_slice(&chunk);
                    let mut start = 0;
                    while let Some(rel_nl) = buffer[start..].iter().position(|&b| b == b'\n') {
                        let end = start + rel_nl + 1;
                        if !header_skipped {
                            header_skipped = true;
                            start = end;
                            continue;
                        }
                        let decoded = {
                            let mut jsonb_shape_cache = self.jsonb_shape_cache.borrow_mut();
                            decode_one_copy_row(
                                &buffer[start..end],
                                entry,
                                columns,
                                schema,
                                &codec,
                                opts,
                                &mut jsonb_shape_cache,
                            )
                        };
                        start = end;
                        match decoded {
                            Ok(payload) => payload_batch.push(payload),
                            Err(e) => {
                                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                                    warn!(error = %abort_err, "COPY FROM autocommit abort failed");
                                }
                                self.drain_copy_remainder().await?;
                                return Err(e);
                            }
                        }
                        if payload_batch.len() == COPY_INSERT_BATCH_ROWS {
                            let batch_len = u64::try_from(payload_batch.len()).unwrap_or(u64::MAX);
                            if let Err(e) =
                                self.flush_copy_insert_batch(entry, &payload_batch, &txn)
                            {
                                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                                    warn!(error = %abort_err, "COPY FROM autocommit abort failed");
                                }
                                self.drain_copy_remainder().await?;
                                return Err(e);
                            }
                            rows_inserted = rows_inserted.saturating_add(batch_len);
                            payload_batch.clear();
                        }
                    }
                    if start > 0 {
                        buffer.drain(..start);
                    }
                }
                FrontendMessage::CopyDone => {
                    received_done = true;
                    break;
                }
                FrontendMessage::CopyFail(reason) => {
                    client_fail_message = Some(reason);
                    break;
                }
                // tokio-postgres pipelines `Bind+Execute+Sync+Flush` ahead
                // of the COPY data stream, so a `Sync` (or `Flush`) frame
                // can arrive *before* the first `CopyData`. The PG protocol
                // allows these as no-ops during a COPY-in phase — the
                // server simply waits for the next `CopyData` / `CopyDone`
                // / `CopyFail`. Ignoring them keeps the libpq pipeline
                // semantics intact.
                FrontendMessage::Sync | FrontendMessage::Flush => continue,
                FrontendMessage::Terminate => {
                    if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                        warn!(error = %abort_err, "COPY FROM abort on terminate failed");
                    }
                    return Err(ServerError::UnexpectedEof);
                }
                other => {
                    if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                        warn!(error = %abort_err, "COPY FROM abort on protocol error failed");
                    }
                    return Err(ServerError::CopyFormat(format!(
                        "unexpected frontend message during COPY FROM: {other:?}"
                    )));
                }
            }
        }

        if received_done && !buffer.is_empty() {
            if header_skipped {
                let line = std::mem::take(&mut buffer);
                let decoded = {
                    let mut jsonb_shape_cache = self.jsonb_shape_cache.borrow_mut();
                    decode_one_copy_row(
                        &line,
                        entry,
                        columns,
                        schema,
                        &codec,
                        opts,
                        &mut jsonb_shape_cache,
                    )
                };
                match decoded {
                    Ok(payload) => payload_batch.push(payload),
                    Err(e) => {
                        if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                            warn!(error = %abort_err, "COPY FROM autocommit abort failed");
                        }
                        return Err(e);
                    }
                }
            } else {
                buffer.clear();
            }
        }

        if let Some(reason) = client_fail_message {
            if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                warn!(error = %abort_err, "COPY FROM abort on CopyFail failed");
            }
            return Err(ServerError::CopyAborted(reason));
        }

        if !payload_batch.is_empty() {
            let batch_len = u64::try_from(payload_batch.len()).unwrap_or(u64::MAX);
            if let Err(e) = self.flush_copy_insert_batch(entry, &payload_batch, &txn) {
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    warn!(error = %abort_err, "COPY FROM autocommit abort failed");
                }
                return Err(e);
            }
            rows_inserted = rows_inserted.saturating_add(batch_len);
        }

        if rows_inserted > 0 {
            if let Err(e) = self.state.validate_deferred_foreign_keys(&txn) {
                let xid = txn.xid;
                if let Err(rollback_err) = self.state.heap.rollback_in_place_updates(xid) {
                    warn!(
                        error = %rollback_err,
                        "COPY FROM rollback of in-place updates failed after deferred FK violation",
                    );
                }
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    warn!(
                        error = %abort_err,
                        "COPY FROM abort failed after deferred FK violation",
                    );
                }
                return Err(e);
            }
        }
        self.finalise_copy_from_commit(txn, rows_inserted, "COPY FROM autocommit")?;
        self.state.note_commit_for_gc();
        self.state
            .note_table_modifications(&entry.name, rows_inserted);
        self.plan_cache_invalidate();

        let mut wire_buf = BytesMut::with_capacity(64);
        encode_backend(
            &BackendMessage::CommandComplete {
                tag: format!("COPY {rows_inserted}"),
            },
            &mut wire_buf,
        );
        if emit_ready_for_query {
            encode_backend(
                &BackendMessage::ReadyForQuery {
                    status: self.txn_state.ready_for_query_status(),
                },
                &mut wire_buf,
            );
        }
        self.io.write_all(&wire_buf).await?;
        self.io.flush().await?;
        Ok(())
    }

    async fn collect_copy_stdin_bytes(&mut self) -> Result<Vec<u8>, ServerError> {
        let mut bytes = Vec::new();
        loop {
            match self.read_frontend().await? {
                FrontendMessage::CopyData(chunk) => bytes.extend_from_slice(&chunk),
                FrontendMessage::CopyDone => return Ok(bytes),
                FrontendMessage::CopyFail(reason) => return Err(ServerError::CopyAborted(reason)),
                FrontendMessage::Sync | FrontendMessage::Flush => continue,
                FrontendMessage::Terminate => return Err(ServerError::UnexpectedEof),
                other => {
                    return Err(ServerError::CopyFormat(format!(
                        "unexpected frontend message during binary COPY FROM: {other:?}"
                    )));
                }
            }
        }
    }

    async fn copy_from_file(
        &mut self,
        entry: &TableEntry,
        columns: &[usize],
        schema: &Schema,
        opts: &CopyOptions,
        path: &str,
        emit_ready_for_query: bool,
    ) -> Result<(), ServerError> {
        if opts.format == ServerCopyFormat::Parquet {
            let rows = self.copy_from_parquet_file(entry, columns, schema, path)?;
            self.state.note_commit_for_gc();
            self.state.note_table_modifications(&entry.name, rows);
            self.plan_cache_invalidate();
            return self.send_copy_complete(rows, emit_ready_for_query).await;
        }
        if opts.format == ServerCopyFormat::Binary {
            let bytes = read_copy_input_file(path)?;
            return self
                .copy_binary_bytes_into_table(entry, columns, schema, &bytes, emit_ready_for_query)
                .await;
        }

        let effective_opts = self.effective_copy_file_options(path, opts)?;
        let file = open_copy_input_file(path)?;
        let mut reader = BufReader::new(file);
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let codec = RowCodec::new(entry.schema.clone());
        let mut payload_batch: Vec<Vec<u8>> = Vec::with_capacity(COPY_INSERT_BATCH_ROWS);
        let mut reject_state = self.copy_reject_state(&effective_opts)?;

        let stream_result = self.copy_text_file_stream_into_table(
            entry,
            columns,
            schema,
            &effective_opts,
            &codec,
            &txn,
            &mut reader,
            &mut payload_batch,
            reject_state.as_mut(),
            path,
        );
        let rows = match stream_result {
            Ok(rows) => rows,
            Err(err) => {
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    warn!(error = %abort_err, "COPY FROM file abort failed");
                }
                return Err(err);
            }
        };
        let reject_rows = reject_state
            .as_ref()
            .and_then(|state| state.target.as_ref())
            .map_or(0, |target| target.rows);
        self.finalise_copy_from_commit(txn, rows.saturating_add(reject_rows), "COPY FROM file")?;
        self.state.note_commit_for_gc();
        self.state.note_table_modifications(&entry.name, rows);
        if let Some(reject_target) = reject_state.and_then(|state| state.target) {
            if reject_target.rows > 0 {
                self.state
                    .note_table_modifications(&reject_target.entry.name, reject_target.rows);
            }
        }
        self.send_copy_complete(rows, emit_ready_for_query).await
    }

    fn effective_copy_file_options(
        &self,
        path: &str,
        opts: &CopyOptions,
    ) -> Result<CopyOptions, ServerError> {
        if opts.format != ServerCopyFormat::Csv || !opts.auto_detect {
            return Ok(opts.clone());
        }
        let sample = read_copy_file_sample(path)?;
        let sniff = sniff_csv_text(path, &sample)
            .map_err(|err| ServerError::CopyFormat(format!("COPY AUTO_DETECT {path}: {err}")))?;
        let mut detected = opts.clone();
        detected.delimiter = sniff.delimiter;
        detected.header = opts.header || sniff.has_header;
        Ok(detected)
    }

    fn copy_reject_state(
        &self,
        opts: &CopyOptions,
    ) -> Result<Option<CopyRejectState>, ServerError> {
        if !opts.ignore_errors {
            return Ok(None);
        }
        let target = if let Some(table_name) = &opts.reject_table {
            let catalog_snapshot = self.state.catalog_snapshot();
            let entry = catalog_snapshot
                .tables
                .get(&table_name.to_ascii_lowercase())
                .ok_or_else(|| {
                    ServerError::CopyFormat(format!("COPY reject_table not found: {table_name}"))
                })?
                .clone();
            validate_copy_reject_table(&entry)?;
            Some(CopyRejectTarget {
                codec: RowCodec::new(entry.schema.clone()),
                entry,
                payload_batch: Vec::with_capacity(COPY_INSERT_BATCH_ROWS),
                rows: 0,
            })
        } else {
            None
        };
        Ok(Some(CopyRejectState {
            max_errors: opts.max_errors.max(1),
            bad_rows: 0,
            target,
        }))
    }

    fn record_copy_reject(
        &self,
        state: &mut CopyRejectState,
        path: &str,
        line_number: u64,
        raw_record: &[u8],
        err: &ServerError,
        txn: &Transaction,
    ) -> Result<(), ServerError> {
        let next_bad_rows = state.bad_rows.saturating_add(1);
        if next_bad_rows > state.max_errors {
            return Err(ServerError::CopyFormat(format!(
                "COPY max_errors exceeded: {next_bad_rows} bad rows (limit {})",
                state.max_errors
            )));
        }
        state.bad_rows = next_bad_rows;
        let Some(target) = state.target.as_mut() else {
            return Ok(());
        };
        let line_number = i64::try_from(line_number)
            .map_err(|_| ServerError::CopyFormat("COPY reject line_number overflow".to_string()))?;
        let row = vec![
            Value::Text(path.to_owned()),
            Value::Int64(line_number),
            Value::Text(String::from_utf8_lossy(raw_record).into_owned()),
            Value::Text(err.to_string()),
        ];
        let payload = target
            .codec
            .encode(&row)
            .map_err(|e| ServerError::CopyFormat(format!("COPY reject row encode: {e}")))?;
        target.payload_batch.push(payload);
        target.rows = target.rows.saturating_add(1);
        if target.payload_batch.len() == COPY_INSERT_BATCH_ROWS {
            self.flush_copy_reject_batch(target, txn)?;
        }
        Ok(())
    }

    fn flush_copy_reject_batch(
        &self,
        target: &mut CopyRejectTarget,
        txn: &Transaction,
    ) -> Result<(), ServerError> {
        if target.payload_batch.is_empty() {
            return Ok(());
        }
        self.flush_copy_insert_batch(&target.entry, &target.payload_batch, txn)?;
        target.payload_batch.clear();
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_text_file_stream_into_table(
        &self,
        entry: &TableEntry,
        columns: &[usize],
        schema: &Schema,
        opts: &CopyOptions,
        codec: &RowCodec,
        txn: &Transaction,
        reader: &mut dyn BufRead,
        payload_batch: &mut Vec<Vec<u8>>,
        mut reject_state: Option<&mut CopyRejectState>,
        path: &str,
    ) -> Result<u64, ServerError> {
        let mut rows_inserted = 0_u64;
        let mut header_skipped = !opts.header;
        let mut record = Vec::new();
        let mut line = Vec::new();
        let mut physical_line_number = 0_u64;
        let mut record_start_line = 1_u64;

        loop {
            line.clear();
            let bytes_read = reader
                .read_until(b'\n', &mut line)
                .map_err(|e| ServerError::Io(std::io::Error::other(format!("COPY FROM: {e}"))))?;
            if bytes_read == 0 {
                break;
            }
            if record.is_empty() {
                record_start_line = physical_line_number.saturating_add(1);
            }
            physical_line_number = physical_line_number.saturating_add(1);
            record.extend_from_slice(&line);
            if opts.format == ServerCopyFormat::Csv && !csv_record_complete(&record, opts)? {
                continue;
            }
            if !header_skipped {
                header_skipped = true;
                record.clear();
                continue;
            }
            if record.is_empty() {
                continue;
            }
            let decoded = {
                let mut jsonb_shape_cache = self.jsonb_shape_cache.borrow_mut();
                decode_one_copy_row(
                    &record,
                    entry,
                    columns,
                    schema,
                    codec,
                    opts,
                    &mut jsonb_shape_cache,
                )
            };
            let payload = match decoded {
                Ok(payload) => payload,
                Err(err) => {
                    if let Some(state) = reject_state.as_deref_mut() {
                        self.record_copy_reject(
                            state,
                            path,
                            record_start_line,
                            &record,
                            &err,
                            txn,
                        )?;
                        record.clear();
                        continue;
                    }
                    return Err(err);
                }
            };
            record.clear();
            payload_batch.push(payload);
            if payload_batch.len() == COPY_INSERT_BATCH_ROWS {
                let batch_len = u64::try_from(payload_batch.len()).unwrap_or(u64::MAX);
                self.flush_copy_insert_batch(entry, payload_batch, txn)?;
                rows_inserted = rows_inserted.saturating_add(batch_len);
                payload_batch.clear();
            }
        }

        if !record.is_empty() {
            if opts.format == ServerCopyFormat::Csv && !csv_record_complete(&record, opts)? {
                let err =
                    ServerError::CopyFormat("unterminated quoted field in CSV input".to_string());
                if let Some(state) = reject_state.as_deref_mut() {
                    self.record_copy_reject(state, path, record_start_line, &record, &err, txn)?;
                    record.clear();
                } else {
                    return Err(err);
                }
            }
            if header_skipped && !record.is_empty() {
                let decoded = {
                    let mut jsonb_shape_cache = self.jsonb_shape_cache.borrow_mut();
                    decode_one_copy_row(
                        &record,
                        entry,
                        columns,
                        schema,
                        codec,
                        opts,
                        &mut jsonb_shape_cache,
                    )
                };
                let payload = match decoded {
                    Ok(payload) => payload,
                    Err(err) => {
                        if let Some(state) = reject_state.as_deref_mut() {
                            self.record_copy_reject(
                                state,
                                path,
                                record_start_line,
                                &record,
                                &err,
                                txn,
                            )?;
                            record.clear();
                            return self.finish_copy_stream_batches(
                                entry,
                                payload_batch,
                                txn,
                                rows_inserted,
                                reject_state,
                            );
                        }
                        return Err(err);
                    }
                };
                payload_batch.push(payload);
            }
        }

        self.finish_copy_stream_batches(entry, payload_batch, txn, rows_inserted, reject_state)
    }

    fn finish_copy_stream_batches(
        &self,
        entry: &TableEntry,
        payload_batch: &mut Vec<Vec<u8>>,
        txn: &Transaction,
        mut rows_inserted: u64,
        reject_state: Option<&mut CopyRejectState>,
    ) -> Result<u64, ServerError> {
        if !payload_batch.is_empty() {
            let batch_len = u64::try_from(payload_batch.len()).unwrap_or(u64::MAX);
            self.flush_copy_insert_batch(entry, payload_batch, txn)?;
            rows_inserted = rows_inserted.saturating_add(batch_len);
            payload_batch.clear();
        }
        if let Some(state) = reject_state {
            if let Some(target) = state.target.as_mut() {
                self.flush_copy_reject_batch(target, txn)?;
            }
        }
        Ok(rows_inserted)
    }

    async fn copy_binary_bytes_into_table(
        &mut self,
        entry: &TableEntry,
        columns: &[usize],
        schema: &Schema,
        bytes: &[u8],
        emit_ready_for_query: bool,
    ) -> Result<(), ServerError> {
        let codec = RowCodec::new(entry.schema.clone());
        let payloads = {
            let mut jsonb_shape_cache = self.jsonb_shape_cache.borrow_mut();
            decode_binary_copy_payload(
                bytes,
                entry,
                columns,
                schema,
                &codec,
                &mut jsonb_shape_cache,
            )?
        };
        let rows = u64::try_from(payloads.len()).unwrap_or(u64::MAX);
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        self.flush_copy_insert_batch(entry, &payloads, &txn)?;
        self.finalise_copy_from_commit(txn, rows, "binary COPY FROM")?;
        self.state.note_commit_for_gc();
        self.state.note_table_modifications(&entry.name, rows);
        self.send_copy_complete(rows, emit_ready_for_query).await
    }

    async fn send_copy_complete(
        &mut self,
        rows: u64,
        emit_ready_for_query: bool,
    ) -> Result<(), ServerError> {
        let mut wire_buf = BytesMut::with_capacity(64);
        encode_backend(
            &BackendMessage::CommandComplete {
                tag: format!("COPY {rows}"),
            },
            &mut wire_buf,
        );
        if emit_ready_for_query {
            encode_backend(
                &BackendMessage::ReadyForQuery {
                    status: self.txn_state.ready_for_query_status(),
                },
                &mut wire_buf,
            );
        }
        self.io.write_all(&wire_buf).await?;
        self.io.flush().await?;
        Ok(())
    }

    fn copy_to_file(
        &mut self,
        entry: &TableEntry,
        columns: &[usize],
        schema: &Schema,
        opts: &CopyOptions,
        path: &str,
    ) -> Result<u64, ServerError> {
        if opts.format == ServerCopyFormat::Parquet {
            return self.copy_to_parquet_file(entry, columns, schema, path);
        }
        let (bytes, rows) = if opts.format == ServerCopyFormat::Binary {
            self.encode_table_binary_copy(entry, columns, schema)?
        } else {
            self.encode_table_textual_copy(entry, columns, opts)?
        };
        write_copy_output_file(path, &bytes)?;
        Ok(rows)
    }

    async fn copy_query_to_destination(
        &mut self,
        input: &LogicalPlan,
        source: &CopySource,
        schema: &Schema,
        opts: &CopyOptions,
        emit_ready_for_query: bool,
    ) -> Result<(), ServerError> {
        if opts.format == ServerCopyFormat::Parquet {
            return Err(ServerError::Unsupported(
                "parquet COPY for query targets is not yet supported",
            ));
        }
        if opts.format == ServerCopyFormat::Binary {
            return Err(ServerError::Unsupported(
                "binary COPY for query targets is not yet supported",
            ));
        }
        let snapshot = self.state.catalog_snapshot();
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let ctx = crate::pipeline::LowerCtx {
            tables: &self.state.tables,
            catalog_snapshot: Arc::clone(&snapshot),
            table_constraints: Arc::clone(&self.state.table_constraints),
            sequences: Arc::clone(&self.state.sequences),
            role_catalog: Arc::clone(&self.state.role_catalog),
            privilege_catalog: Arc::clone(&self.state.privilege_catalog),
            row_security: Arc::clone(&self.state.row_security),
            session_settings: Arc::new(self.session_settings.clone()),
            current_user: self.current_user.clone(),
            session_user: self.auth_user.clone(),
            persistent_catalog: Arc::clone(&self.state.persistent_catalog),
            time_partitions: Arc::clone(&self.state.time_partitions),
            workload_recorder: Arc::clone(&self.state.workload_recorder),
            autovacuum_config: self.state.autovacuum_config(),
            logging_config: self.state.logging_config(),
            wal_archive_config: self.state.wal_archive_config(),
            data_dir: self.state.data_dir.clone(),
            logical_replication: Arc::clone(&self.state.logical_replication),
            sequence_state: Some(self.sequence_state.clone()),
            advisory_state: Some(self.advisory_state.clone()),
            heap: Arc::clone(&self.state.heap),
            vm: Arc::clone(&self.state.vm),
            snapshot: txn.snapshot.clone(),
            isolation: txn.isolation,
            oracle: Arc::clone(&self.state.txn_manager),
            xid: txn.current_xid(),
            command_id: txn.current_command,
            cte_buffers: std::collections::HashMap::new(),
            jit: self.jit_config(),
            cancel_flag: Some(self.cancel_flag.clone()),
            work_mem: Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(u64::MAX)),
            profile_operators: false,
        };
        let result = match crate::pipeline::lower_query(input, &ctx)
            .and_then(|mut op| crate::result_encoder::run_select(op.as_mut()))
        {
            Ok(result) => result,
            Err(e) => {
                if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                    warn!(error = %abort_err, "COPY query transaction abort failed");
                }
                return Err(e);
            }
        };
        if let Err(e) = self.state.txn_manager.commit(txn) {
            warn!(error = %e, "COPY query transaction commit failed");
        }
        let (payload, rows) = copy_rows_from_select_result(&result, schema, opts)?;
        match source {
            CopySource::Stdout => {
                self.write_buf.clear();
                encode_backend(
                    &copy_out_response_with_format(schema.len(), copy_format_code(opts.format)),
                    &mut self.write_buf,
                );
                encode_backend(&BackendMessage::CopyData(payload), &mut self.write_buf);
                encode_backend(&BackendMessage::CopyDone, &mut self.write_buf);
                encode_backend(
                    &BackendMessage::CommandComplete {
                        tag: format!("COPY {rows}"),
                    },
                    &mut self.write_buf,
                );
                if emit_ready_for_query {
                    encode_backend(
                        &BackendMessage::ReadyForQuery {
                            status: self.txn_state.ready_for_query_status(),
                        },
                        &mut self.write_buf,
                    );
                }
                self.io.write_all(&self.write_buf).await?;
                self.io.flush().await?;
                Ok(())
            }
            CopySource::File(path) => {
                write_copy_output_file(path, &payload)?;
                self.send_copy_complete(rows, emit_ready_for_query).await
            }
            CopySource::Stdin => Err(ServerError::Unsupported(
                "COPY query target cannot use STDIN",
            )),
        }
    }

    fn encode_table_textual_copy(
        &self,
        entry: &TableEntry,
        columns: &[usize],
        opts: &CopyOptions,
    ) -> Result<(Vec<u8>, u64), ServerError> {
        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let codec = RowCodec::new(entry.schema.clone());
        let mut out = Vec::new();
        let stream_schema = projected_schema(entry, columns)?;
        if opts.header {
            let header_cells: Vec<Option<Vec<u8>>> = stream_schema
                .fields()
                .iter()
                .map(|f| Some(f.name.as_bytes().to_vec()))
                .collect();
            match opts.format {
                ServerCopyFormat::Text => {
                    out.extend_from_slice(&encode_text_row(&header_cells, opts))
                }
                ServerCopyFormat::Csv => {
                    out.extend_from_slice(&encode_csv_row(&header_cells, opts))
                }
                ServerCopyFormat::Binary | ServerCopyFormat::Parquet => {}
            }
        }
        let mut rows = 0_u64;
        let scan = self.state.heap.scan_visible(
            rel,
            block_count,
            &txn.snapshot,
            self.state.txn_manager.as_ref(),
        );
        for tuple in scan {
            let tuple =
                tuple.map_err(|e| ServerError::ddl(format!("COPY TO file heap scan: {e}")))?;
            let row = codec
                .decode(&tuple.data)
                .map_err(|e| ServerError::CopyFormat(format!("COPY TO file row decode: {e}")))?;
            let cells = copy_cells_from_row(&row, &entry.schema, columns);
            match opts.format {
                ServerCopyFormat::Text => out.extend_from_slice(&encode_text_row(&cells, opts)),
                ServerCopyFormat::Csv => out.extend_from_slice(&encode_csv_row(&cells, opts)),
                ServerCopyFormat::Binary | ServerCopyFormat::Parquet => {}
            }
            rows = rows.saturating_add(1);
        }
        if let Err(e) = self.state.txn_manager.commit(txn) {
            warn!(error = %e, "COPY TO file scan commit failed");
        }
        Ok((out, rows))
    }

    fn encode_table_binary_copy(
        &self,
        entry: &TableEntry,
        columns: &[usize],
        schema: &Schema,
    ) -> Result<(Vec<u8>, u64), ServerError> {
        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let codec = RowCodec::new(entry.schema.clone());
        let mut out = Vec::new();
        append_binary_copy_header(&mut out);
        let mut rows = 0_u64;
        let scan = self.state.heap.scan_visible(
            rel,
            block_count,
            &txn.snapshot,
            self.state.txn_manager.as_ref(),
        );
        for tuple in scan {
            let tuple =
                tuple.map_err(|e| ServerError::ddl(format!("binary COPY heap scan: {e}")))?;
            let row = codec
                .decode(&tuple.data)
                .map_err(|e| ServerError::CopyFormat(format!("binary COPY row decode: {e}")))?;
            append_binary_copy_row(&mut out, &row, &entry.schema, columns, schema)?;
            rows = rows.saturating_add(1);
        }
        append_i16_be(&mut out, -1);
        if let Err(e) = self.state.txn_manager.commit(txn) {
            warn!(error = %e, "binary COPY scan commit failed");
        }
        Ok((out, rows))
    }

    pub(super) fn flush_copy_insert_batch(
        &self,
        entry: &TableEntry,
        payloads: &[Vec<u8>],
        txn: &Transaction,
    ) -> Result<(), ServerError> {
        if payloads.is_empty() {
            return Ok(());
        }
        let payload_refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let wal = self.state.heap.wal_sink().map(Arc::as_ref);
        let insert_opts = InsertOptions {
            xmin: txn.current_xid(),
            command_id: txn.current_command,
            wal,
            fsm: None,
            vm: Some(self.state.vm.as_ref()),
        };
        self.state
            .heap
            .insert_batch(RelationId(entry.oid), &payload_refs, insert_opts)
            .map_err(|e| ServerError::ddl(format!("COPY FROM heap insert batch: {e}")))?;
        self.state.flush_dirty_heap_pages_if_needed()?;
        Ok(())
    }

    async fn drain_copy_remainder(&mut self) -> Result<(), ServerError> {
        loop {
            match self.read_frontend().await? {
                FrontendMessage::CopyData(_) => continue,
                FrontendMessage::CopyDone | FrontendMessage::CopyFail(_) => return Ok(()),
                FrontendMessage::Terminate => return Err(ServerError::UnexpectedEof),
                other => {
                    return Err(ServerError::CopyFormat(format!(
                        "unexpected frontend message while draining COPY FROM: {other:?}"
                    )));
                }
            }
        }
    }
}

fn validate_copy_reject_table(entry: &TableEntry) -> Result<(), ServerError> {
    let fields = entry.schema.fields();
    if fields.len() != 4 {
        return Err(ServerError::CopyFormat(format!(
            "COPY reject_table {} must have columns filename TEXT, line_number BIGINT, raw_row TEXT, error TEXT",
            entry.name
        )));
    }
    let expected = [
        ("filename", RejectColumnType::Text),
        ("line_number", RejectColumnType::Int64),
        ("raw_row", RejectColumnType::Text),
        ("error", RejectColumnType::Text),
    ];
    for (field, (name, ty)) in fields.iter().zip(expected) {
        if !field.name.eq_ignore_ascii_case(name)
            || !reject_column_type_matches(&field.data_type, ty)
        {
            return Err(ServerError::CopyFormat(format!(
                "COPY reject_table {} must have columns filename TEXT, line_number BIGINT, raw_row TEXT, error TEXT",
                entry.name
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum RejectColumnType {
    Text,
    Int64,
}

fn reject_column_type_matches(data_type: &DataType, expected: RejectColumnType) -> bool {
    match expected {
        RejectColumnType::Text => {
            matches!(data_type, DataType::Text { .. } | DataType::Char { .. })
        }
        RejectColumnType::Int64 => *data_type == DataType::Int64,
    }
}

fn read_copy_file_sample(path: &str) -> Result<String, ServerError> {
    let file = open_copy_input_file(path)?;
    let mut reader = BufReader::new(file);
    let mut sample = Vec::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        let bytes_read = reader
            .read_until(b'\n', &mut line)
            .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
        if bytes_read == 0 {
            break;
        }
        sample.extend_from_slice(&line);
        if sample.len() >= COPY_AUTODETECT_SAMPLE_BYTES && csv_sample_record_complete(&sample) {
            break;
        }
    }
    String::from_utf8(sample).map_err(|e| {
        ServerError::CopyFormat(format!(
            "COPY AUTO_DETECT {path}: invalid UTF-8 sample: {e}"
        ))
    })
}

fn open_copy_input_file(path: &str) -> Result<File, ServerError> {
    ensure_regular_copy_input(path)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    options
        .open(path)
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))
}

fn read_copy_input_file(path: &str) -> Result<Vec<u8>, ServerError> {
    let file = open_copy_input_file(path)?;
    let limit = copy_binary_file_limit_bytes();
    let len = file
        .metadata()
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?
        .len();
    if len > limit {
        return Err(ServerError::CopyFormat(format!(
            "COPY binary file exceeds limit: {path} size={len} limit={limit}"
        )));
    }
    let mut bytes = Vec::new();
    let mut limited = file.take(limit.saturating_add(1));
    limited
        .read_to_end(&mut bytes)
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
    let read_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if read_len > limit {
        return Err(ServerError::CopyFormat(format!(
            "COPY binary file exceeds limit: {path} size={read_len} limit={limit}"
        )));
    }
    Ok(bytes)
}

fn copy_binary_file_limit_bytes() -> u64 {
    std::env::var("ULTRASQL_COPY_BINARY_FILE_LIMIT_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_COPY_BINARY_FILE_LIMIT_BYTES)
}

fn ensure_regular_copy_input(path: &str) -> Result<(), ServerError> {
    let metadata = fs::symlink_metadata(Path::new(path))
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        Err(ServerError::CopyFormat(format!(
            "COPY file is not a regular file: {path}"
        )))
    }
}

fn write_copy_output_file(path: &str, bytes: &[u8]) -> Result<(), ServerError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
    file.write_all(bytes)
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))
}

fn csv_record_complete(record: &[u8], opts: &CopyOptions) -> Result<bool, ServerError> {
    let delimiter = single_byte_delimiter(opts.delimiter)?;
    let mut in_quotes = false;
    let mut at_field_start = true;
    let mut i = 0;
    while i < record.len() {
        let b = record[i];
        if in_quotes {
            if b == b'"' {
                if i + 1 < record.len() && record[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_quotes = false;
            }
            i += 1;
            continue;
        }
        if b == b'"' && at_field_start {
            in_quotes = true;
            at_field_start = false;
        } else {
            at_field_start = b == delimiter || b == b'\n' || b == b'\r';
        }
        i += 1;
    }
    Ok(!in_quotes)
}

fn csv_sample_record_complete(sample: &[u8]) -> bool {
    let mut in_quotes = false;
    let mut i = 0;
    while i < sample.len() {
        if sample[i] == b'"' {
            if in_quotes && i + 1 < sample.len() && sample[i + 1] == b'"' {
                i += 2;
                continue;
            }
            in_quotes = !in_quotes;
        }
        i += 1;
    }
    !in_quotes
}

fn single_byte_delimiter(delimiter: char) -> Result<u8, ServerError> {
    let mut bytes = [0_u8; 4];
    let encoded = delimiter.encode_utf8(&mut bytes).as_bytes();
    if encoded.len() != 1 {
        return Err(ServerError::CopyFormat(
            "COPY delimiter must be one byte for streaming CSV".to_string(),
        ));
    }
    Ok(encoded[0])
}

fn copy_format_code(format: ServerCopyFormat) -> u8 {
    match format {
        ServerCopyFormat::Text | ServerCopyFormat::Csv => 0,
        ServerCopyFormat::Binary => 1,
        ServerCopyFormat::Parquet => 0,
    }
}

fn projected_schema(entry: &TableEntry, columns: &[usize]) -> Result<Schema, ServerError> {
    if columns.is_empty() {
        return Ok(entry.schema.clone());
    }
    let fields = columns
        .iter()
        .map(|&i| entry.schema.fields()[i].clone())
        .collect::<Vec<_>>();
    Schema::new(fields).map_err(|e| ServerError::CopyFormat(format!("COPY schema: {e}")))
}

fn copy_cells_from_row(row: &[Value], schema: &Schema, columns: &[usize]) -> Vec<Option<Vec<u8>>> {
    if columns.is_empty() {
        row.iter()
            .zip(schema.fields())
            .map(|(value, field)| value_to_copy_cell(value, &field.data_type))
            .collect()
    } else {
        columns
            .iter()
            .map(|&i| {
                let field = schema.field_at(i);
                row.get(i)
                    .and_then(|value| value_to_copy_cell(value, &field.data_type))
            })
            .collect()
    }
}

fn copy_rows_from_select_result(
    result: &crate::result_encoder::SelectResult,
    schema: &Schema,
    opts: &CopyOptions,
) -> Result<(Vec<u8>, u64), ServerError> {
    let mut out = Vec::new();
    if opts.header {
        let header_cells: Vec<Option<Vec<u8>>> = schema
            .fields()
            .iter()
            .map(|f| Some(f.name.as_bytes().to_vec()))
            .collect();
        match opts.format {
            ServerCopyFormat::Text => out.extend_from_slice(&encode_text_row(&header_cells, opts)),
            ServerCopyFormat::Csv => out.extend_from_slice(&encode_csv_row(&header_cells, opts)),
            ServerCopyFormat::Binary | ServerCopyFormat::Parquet => {}
        }
    }
    let mut rows = 0_u64;
    for msg in &result.messages {
        if let BackendMessage::DataRow { columns } = msg {
            match opts.format {
                ServerCopyFormat::Text => out.extend_from_slice(&encode_text_row(columns, opts)),
                ServerCopyFormat::Csv => out.extend_from_slice(&encode_csv_row(columns, opts)),
                ServerCopyFormat::Binary => {
                    return Err(ServerError::Unsupported(
                        "binary COPY for query targets is not yet supported",
                    ));
                }
                ServerCopyFormat::Parquet => {
                    return Err(ServerError::Unsupported(
                        "parquet COPY for query targets is not yet supported",
                    ));
                }
            }
            rows = rows.saturating_add(1);
        }
    }
    Ok((out, rows))
}

fn append_binary_copy_header(out: &mut Vec<u8>) {
    out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    out.extend_from_slice(&0_i32.to_be_bytes());
    out.extend_from_slice(&0_i32.to_be_bytes());
}

fn append_binary_copy_row(
    out: &mut Vec<u8>,
    row: &[Value],
    table_schema: &Schema,
    columns: &[usize],
    stream_schema: &Schema,
) -> Result<(), ServerError> {
    append_i16_be(
        out,
        i16::try_from(stream_schema.len())
            .map_err(|_| ServerError::CopyFormat("too many COPY columns".to_string()))?,
    );
    if columns.is_empty() {
        for (idx, value) in row.iter().enumerate() {
            append_binary_copy_cell(out, value, &table_schema.field_at(idx).data_type)?;
        }
    } else {
        for &idx in columns {
            let value = row.get(idx).unwrap_or(&Value::Null);
            append_binary_copy_cell(out, value, &table_schema.field_at(idx).data_type)?;
        }
    }
    Ok(())
}

fn append_binary_copy_cell(
    out: &mut Vec<u8>,
    value: &Value,
    dtype: &DataType,
) -> Result<(), ServerError> {
    if matches!(value, Value::Null) {
        out.extend_from_slice(&(-1_i32).to_be_bytes());
        return Ok(());
    }
    let bytes = binary_copy_cell_bytes(value, dtype)?;
    out.extend_from_slice(
        &i32::try_from(bytes.len())
            .map_err(|_| ServerError::CopyFormat("binary COPY cell too large".to_string()))?
            .to_be_bytes(),
    );
    out.extend_from_slice(&bytes);
    Ok(())
}

fn binary_copy_cell_bytes(value: &Value, dtype: &DataType) -> Result<Vec<u8>, ServerError> {
    let bytes = match (dtype, value) {
        (DataType::Bool, Value::Bool(v)) => vec![u8::from(*v)],
        (DataType::Int16, Value::Int16(v)) => v.to_be_bytes().to_vec(),
        (DataType::Int32, Value::Int32(v)) => v.to_be_bytes().to_vec(),
        (DataType::Int64, Value::Int64(v)) => v.to_be_bytes().to_vec(),
        (DataType::Money, Value::Money(v) | Value::Int64(v)) => encode_pg_money_binary(*v).to_vec(),
        (DataType::Float32, Value::Float32(v)) => v.to_bits().to_be_bytes().to_vec(),
        (DataType::Float64, Value::Float64(v)) => v.to_bits().to_be_bytes().to_vec(),
        (DataType::Date, Value::Date(v) | Value::Int32(v)) => v.to_be_bytes().to_vec(),
        (DataType::Time, Value::Time(v) | Value::Int64(v))
        | (DataType::Timestamp, Value::Timestamp(v) | Value::Int64(v))
        | (DataType::TimestampTz, Value::TimestampTz(v) | Value::Int64(v)) => {
            v.to_be_bytes().to_vec()
        }
        (
            DataType::TimeTz,
            Value::TimeTz {
                micros,
                offset_seconds,
            },
        ) => {
            let mut out = Vec::with_capacity(12);
            out.extend_from_slice(&micros.to_be_bytes());
            out.extend_from_slice(&offset_seconds.to_be_bytes());
            out
        }
        (DataType::Decimal { .. }, Value::Decimal { value, scale }) => {
            encode_pg_numeric_binary(*value, *scale)
                .map_err(|err| ServerError::CopyFormat(format!("binary COPY numeric: {err}")))?
        }
        (DataType::Decimal { scale, .. }, Value::Int64(v)) => {
            encode_pg_numeric_binary(*v, scale.unwrap_or(0))
                .map_err(|err| ServerError::CopyFormat(format!("binary COPY numeric: {err}")))?
        }
        (DataType::Text { .. } | DataType::TsVector | DataType::TsQuery, Value::Text(v))
        | (DataType::Char { .. }, Value::Char(v)) => v.as_bytes().to_vec(),
        (DataType::Bit { .. } | DataType::VarBit { .. }, Value::BitString(bits)) => {
            bits.to_pg_binary()
        }
        (
            DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8,
            Value::Network(network),
        ) if network.data_type() == dtype.clone() => network.to_pg_binary(),
        (DataType::Json, Value::Json(v)) => v.as_bytes().to_vec(),
        (DataType::Jsonb, Value::Jsonb(v)) => encode_pg_binary_jsonb(v),
        (DataType::Xml, Value::Xml(v)) => v.as_bytes().to_vec(),
        (DataType::Bytea, Value::Bytea(v)) => v.clone(),
        (DataType::Uuid, Value::Uuid(v)) => v.to_vec(),
        (_, other) => other.to_string().into_bytes(),
    };
    Ok(bytes)
}

fn append_i16_be(out: &mut Vec<u8>, v: i16) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn decode_binary_copy_payload(
    bytes: &[u8],
    entry: &TableEntry,
    columns: &[usize],
    schema: &Schema,
    codec: &RowCodec,
    jsonb_shape_cache: &mut JsonbShapeCache,
) -> Result<Vec<Vec<u8>>, ServerError> {
    const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";
    if bytes.len() < MAGIC.len() + 8 || &bytes[..MAGIC.len()] != MAGIC {
        return Err(ServerError::CopyFormat(
            "invalid binary COPY header".to_string(),
        ));
    }
    let mut pos = MAGIC.len();
    let _flags = read_i32_be(bytes, &mut pos)?;
    let ext_len = read_i32_be(bytes, &mut pos)?;
    if ext_len < 0 {
        return Err(ServerError::CopyFormat(
            "invalid binary COPY extension length".to_string(),
        ));
    }
    let ext_len = usize::try_from(ext_len)
        .map_err(|_| ServerError::CopyFormat("invalid binary COPY extension".to_string()))?;
    pos = pos.saturating_add(ext_len);
    if pos > bytes.len() {
        return Err(ServerError::CopyFormat(
            "truncated binary COPY extension".to_string(),
        ));
    }

    let mut payloads = Vec::new();
    loop {
        let field_count = read_i16_be(bytes, &mut pos)?;
        if field_count == -1 {
            break;
        }
        let expected = i16::try_from(schema.len())
            .map_err(|_| ServerError::CopyFormat("too many COPY columns".to_string()))?;
        if field_count != expected {
            return Err(ServerError::CopyFormat(format!(
                "binary COPY expected {expected} columns, got {field_count}"
            )));
        }
        let mut row = vec![Value::Null; entry.schema.len()];
        for stream_idx in 0..usize::try_from(field_count).unwrap_or(0) {
            let len = read_i32_be(bytes, &mut pos)?;
            let value = if len == -1 {
                Value::Null
            } else {
                if len < 0 {
                    return Err(ServerError::CopyFormat(
                        "invalid binary COPY field length".to_string(),
                    ));
                }
                let len = usize::try_from(len).map_err(|_| {
                    ServerError::CopyFormat("invalid binary COPY field length".to_string())
                })?;
                let end = pos.saturating_add(len);
                if end > bytes.len() {
                    return Err(ServerError::CopyFormat(
                        "truncated binary COPY field".to_string(),
                    ));
                }
                let target_idx = columns.get(stream_idx).copied().unwrap_or(stream_idx);
                let dtype = &entry.schema.field_at(target_idx).data_type;
                let value = decode_binary_copy_cell(
                    &bytes[pos..end],
                    dtype,
                    stream_idx,
                    jsonb_shape_cache,
                )?;
                pos = end;
                value
            };
            let target_idx = columns.get(stream_idx).copied().unwrap_or(stream_idx);
            row[target_idx] = value;
        }
        payloads.push(
            codec
                .encode(&row)
                .map_err(|e| ServerError::CopyFormat(format!("binary COPY row encode: {e}")))?,
        );
    }
    Ok(payloads)
}

fn read_i16_be(bytes: &[u8], pos: &mut usize) -> Result<i16, ServerError> {
    let end = pos.saturating_add(2);
    if end > bytes.len() {
        return Err(ServerError::CopyFormat("truncated binary COPY".to_string()));
    }
    let out = i16::from_be_bytes([bytes[*pos], bytes[*pos + 1]]);
    *pos = end;
    Ok(out)
}

fn read_i32_be(bytes: &[u8], pos: &mut usize) -> Result<i32, ServerError> {
    let end = pos.saturating_add(4);
    if end > bytes.len() {
        return Err(ServerError::CopyFormat("truncated binary COPY".to_string()));
    }
    let out = i32::from_be_bytes([
        bytes[*pos],
        bytes[*pos + 1],
        bytes[*pos + 2],
        bytes[*pos + 3],
    ]);
    *pos = end;
    Ok(out)
}

fn decode_binary_copy_cell(
    bytes: &[u8],
    dtype: &DataType,
    column_idx: usize,
    jsonb_shape_cache: &mut JsonbShapeCache,
) -> Result<Value, ServerError> {
    let exact = |n: usize| {
        if bytes.len() == n {
            Ok(())
        } else {
            Err(ServerError::CopyFormat(format!(
                "column {column_idx}: binary length {}, expected {n}",
                bytes.len()
            )))
        }
    };
    match dtype {
        DataType::Bool => {
            exact(1)?;
            Ok(Value::Bool(bytes[0] != 0))
        }
        DataType::Int16 => {
            exact(2)?;
            Ok(Value::Int16(i16::from_be_bytes([bytes[0], bytes[1]])))
        }
        DataType::Int32 => {
            exact(4)?;
            Ok(Value::Int32(i32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ])))
        }
        DataType::Int64 => {
            exact(8)?;
            Ok(Value::Int64(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])))
        }
        DataType::Float32 => {
            exact(4)?;
            Ok(Value::Float32(f32::from_bits(u32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ]))))
        }
        DataType::Float64 => {
            exact(8)?;
            Ok(Value::Float64(f64::from_bits(u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))))
        }
        DataType::Date => {
            exact(4)?;
            Ok(Value::Date(i32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ])))
        }
        DataType::Time => {
            exact(8)?;
            Ok(Value::Time(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])))
        }
        DataType::Timestamp => {
            exact(8)?;
            Ok(Value::Timestamp(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])))
        }
        DataType::TimestampTz => {
            exact(8)?;
            Ok(Value::TimestampTz(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])))
        }
        DataType::TimeTz => {
            exact(12)?;
            Ok(Value::TimeTz {
                micros: i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]),
                offset_seconds: i32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            })
        }
        DataType::Decimal { .. } => decode_pg_numeric_binary(bytes)
            .map_err(|err| ServerError::CopyFormat(format!("column {column_idx}: {err}"))),
        DataType::Money => decode_pg_money_binary(bytes)
            .map_err(|err| ServerError::CopyFormat(format!("column {column_idx}: {err}"))),
        DataType::Bit { .. } | DataType::VarBit { .. } => BitString::from_pg_binary(bytes)
            .and_then(|bits| bits.coerce_to(dtype, false))
            .map(Value::BitString)
            .ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} binary"))
            }),
        DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8 => {
            NetworkValue::from_pg_binary(dtype, bytes)
                .map(Value::Network)
                .ok_or_else(|| {
                    ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} binary"))
                })
        }
        DataType::Text { .. } | DataType::TsVector | DataType::TsQuery => {
            std::str::from_utf8(bytes)
                .map(|s| Value::Text(s.to_string()))
                .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}")))
        }
        DataType::Char { len } => std::str::from_utf8(bytes)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}")))
            .and_then(|s| {
                coerce_bpchar_text(s, *len, false)
                    .map(Value::Char)
                    .map_err(|err| ServerError::CopyFormat(format!("column {column_idx}: {err}")))
            }),
        DataType::Json => parse_json_text(bytes, column_idx).map(Value::Json),
        DataType::Jsonb => jsonb_shape_cache
            .parse_pg_binary(bytes, column_idx)
            .map(Value::Jsonb),
        DataType::Xml => parse_xml_text(bytes, column_idx).map(Value::Xml),
        DataType::Bytea => Ok(Value::Bytea(bytes.to_vec())),
        DataType::Uuid => {
            exact(16)?;
            let raw: [u8; 16] = bytes.try_into().map_err(|_| {
                ServerError::CopyFormat(format!("column {column_idx}: binary UUID length invalid"))
            })?;
            Ok(Value::Uuid(raw))
        }
        other => decode_copy_cell(Some(bytes), other, column_idx, jsonb_shape_cache),
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_one_copy_row(
    line: &[u8],
    entry: &TableEntry,
    columns: &[usize],
    schema: &Schema,
    codec: &RowCodec,
    opts: &CopyOptions,
    jsonb_shape_cache: &mut JsonbShapeCache,
) -> Result<Vec<u8>, ServerError> {
    match opts.format {
        ServerCopyFormat::Csv if !line.contains(&b'"') => {
            let raw_cells = parse_unquoted_csv_row_slices(line, opts)?;
            return decode_copy_cells_to_payload(
                &raw_cells,
                entry,
                columns,
                schema,
                codec,
                jsonb_shape_cache,
            );
        }
        ServerCopyFormat::Text | ServerCopyFormat::Csv => {}
        ServerCopyFormat::Binary | ServerCopyFormat::Parquet => {
            return Err(ServerError::CopyFormat(
                "binary COPY rows are decoded by binary parser".to_string(),
            ));
        }
    };
    let owned_cells = match opts.format {
        ServerCopyFormat::Text => parse_text_row(line, opts)?,
        ServerCopyFormat::Csv => parse_csv_row(line, opts)?,
        ServerCopyFormat::Binary | ServerCopyFormat::Parquet => unreachable!(),
    };
    let raw_cells = owned_cells.iter().map(Option::as_deref).collect::<Vec<_>>();
    decode_copy_cells_to_payload(&raw_cells, entry, columns, schema, codec, jsonb_shape_cache)
}

#[allow(clippy::too_many_arguments)]
fn decode_copy_cells_to_payload(
    raw_cells: &[Option<&[u8]>],
    entry: &TableEntry,
    columns: &[usize],
    schema: &Schema,
    codec: &RowCodec,
    jsonb_shape_cache: &mut JsonbShapeCache,
) -> Result<Vec<u8>, ServerError> {
    if raw_cells.len() != schema.len() {
        return Err(ServerError::CopyFormat(format!(
            "COPY FROM expected {} columns, got {}",
            schema.len(),
            raw_cells.len()
        )));
    }

    let mut row = vec![Value::Null; entry.schema.len()];
    if columns.is_empty() {
        for (col_idx, raw) in raw_cells.iter().enumerate() {
            let field = entry.schema.field_at(col_idx);
            row[col_idx] = decode_copy_cell(*raw, &field.data_type, col_idx, jsonb_shape_cache)?;
        }
    } else {
        for (stream_idx, (table_col_idx, raw)) in columns.iter().zip(raw_cells.iter()).enumerate() {
            let field = entry.schema.field_at(*table_col_idx);
            row[*table_col_idx] =
                decode_copy_cell(*raw, &field.data_type, stream_idx, jsonb_shape_cache)?;
        }
    }

    for (col_idx, (value, field)) in row.iter().zip(entry.schema.fields()).enumerate() {
        if matches!(value, Value::Null) && !field.nullable {
            return Err(ServerError::CopyFormat(format!(
                "column {col_idx}: NULL violates not-null constraint"
            )));
        }
    }

    codec
        .encode(&row)
        .map_err(|e| ServerError::CopyFormat(format!("COPY FROM row encode: {e}")))
}

/// Encode a runtime [`Value`] as a `CopyData` cell (`None` is SQL NULL).
fn value_to_copy_cell(value: &Value, dtype: &DataType) -> Option<Vec<u8>> {
    match (dtype, value) {
        (_, Value::Null) => None,
        (DataType::Date, Value::Int32(v) | Value::Date(v)) => {
            Some(Value::Date(*v).to_string().into_bytes())
        }
        (DataType::Decimal { scale, .. }, Value::Int64(v)) => Some(
            Value::Decimal {
                value: *v,
                scale: scale.unwrap_or(0),
            }
            .to_string()
            .into_bytes(),
        ),
        (DataType::Money, Value::Int64(v) | Value::Money(v)) => {
            Some(Value::Money(*v).to_string().into_bytes())
        }
        (DataType::Time, Value::Int64(v) | Value::Time(v)) => {
            Some(Value::Time(*v).to_string().into_bytes())
        }
        (DataType::Timestamp, Value::Int64(v) | Value::Timestamp(v)) => {
            Some(Value::Timestamp(*v).to_string().into_bytes())
        }
        (DataType::TimestampTz, Value::Int64(v) | Value::TimestampTz(v)) => {
            Some(Value::TimestampTz(*v).to_string().into_bytes())
        }
        (
            DataType::TimeTz,
            Value::TimeTz {
                micros,
                offset_seconds,
            },
        ) => Some(
            Value::TimeTz {
                micros: *micros,
                offset_seconds: *offset_seconds,
            }
            .to_string()
            .into_bytes(),
        ),
        (_, value) => value_to_copy_cell_by_value(value),
    }
}

fn value_to_copy_cell_by_value(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(if *b { b"t".to_vec() } else { b"f".to_vec() }),
        Value::Int16(v) => Some(v.to_string().into_bytes()),
        Value::Int32(v) => Some(v.to_string().into_bytes()),
        Value::Int64(v) => Some(v.to_string().into_bytes()),
        Value::Oid(v) | Value::RegClass(v) | Value::RegType(v) => {
            Some(v.raw().to_string().into_bytes())
        }
        Value::PgLsn(v) => Some(v.to_string().into_bytes()),
        Value::Float32(v) => Some(format_float_f32(*v)),
        Value::Float64(v) => Some(format_float_f64(*v)),
        Value::Text(s) | Value::Char(s) => Some(s.as_bytes().to_vec()),
        Value::Bytea(b) => Some(b.clone()),
        Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => {
            Some(v.to_string().into_bytes())
        }
        Value::TimeTz { .. } => Some(value.to_string().into_bytes()),
        Value::Date(v) => Some(v.to_string().into_bytes()),
        Value::Uuid(bytes) => Some(Value::Uuid(*bytes).to_string().into_bytes()),
        Value::Decimal { .. }
        | Value::Money(_)
        | Value::BitString(_)
        | Value::Network(_)
        | Value::Interval { .. }
        | Value::Range(_)
        | Value::Geometry(_)
        | Value::Json(_)
        | Value::Jsonb(_)
        | Value::Xml(_)
        | Value::Vector(_)
        | Value::HalfVec(_)
        | Value::SparseVec(_)
        | Value::BitVec { .. }
        | Value::Array { .. }
        | Value::Record(_) => Some(value.to_string().into_bytes()),
    }
}

fn parse_xml_text(bytes: &[u8], column_idx: usize) -> Result<String, ServerError> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid UTF-8 in xml"))
    })?;
    Value::validate_xml_text(text)
        .ok_or_else(|| ServerError::CopyFormat(format!("column {column_idx}: invalid xml")))
}

/// Decode a single COPY cell into a typed [`Value`] consistent with the
/// target column's [`DataType`].
fn decode_copy_cell(
    raw: Option<&[u8]>,
    dtype: &DataType,
    column_idx: usize,
    jsonb_shape_cache: &mut JsonbShapeCache,
) -> Result<Value, ServerError> {
    let Some(bytes) = raw else {
        return Ok(Value::Null);
    };
    let s = std::str::from_utf8(bytes).map_err(|_| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid UTF-8 in COPY input"))
    })?;
    match dtype {
        DataType::Bool => parse_copy_bool(s, column_idx).map(Value::Bool),
        DataType::Int16 => s
            .parse::<i16>()
            .map(Value::Int16)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Int32 => s
            .parse::<i32>()
            .map(Value::Int32)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Int64 => s
            .parse::<i64>()
            .map(Value::Int64)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Oid | DataType::RegClass | DataType::RegType => {
            let oid = Value::parse_oid_text(s).ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
            })?;
            Ok(match dtype {
                DataType::Oid => Value::Oid(oid),
                DataType::RegClass => Value::RegClass(oid),
                DataType::RegType => Value::RegType(oid),
                _ => unreachable!(),
            })
        }
        DataType::PgLsn => Value::parse_pg_lsn_text(s)
            .map(Value::PgLsn)
            .ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
            }),
        DataType::Float32 => s
            .parse::<f32>()
            .map(Value::Float32)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Float64 => s
            .parse::<f64>()
            .map(Value::Float64)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Decimal { scale, .. } => parse_copy_decimal(s, *scale, column_idx),
        DataType::Money => parse_money_text(s)
            .map_err(|err| ServerError::CopyFormat(format!("column {column_idx}: {err}"))),
        DataType::Date => parse_copy_date(s, column_idx).map(Value::Date),
        DataType::Time => parse_copy_time(s, column_idx).map(Value::Time),
        DataType::TimeTz => parse_copy_timetz(s, column_idx),
        DataType::Timestamp => parse_copy_timestamp(s, column_idx).map(Value::Timestamp),
        DataType::TimestampTz => parse_copy_timestamptz(s, column_idx).map(Value::TimestampTz),
        DataType::Text { .. } | DataType::TsVector | DataType::TsQuery => {
            Ok(Value::Text(s.to_string()))
        }
        DataType::Char { len } => coerce_bpchar_text(s, *len, false)
            .map(Value::Char)
            .map_err(|err| ServerError::CopyFormat(format!("column {column_idx}: {err}"))),
        DataType::Bit { .. } | DataType::VarBit { .. } => {
            parse_copy_bit_string(s, dtype, column_idx)
        }
        DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8 => {
            Value::parse_network(dtype, s).ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
            })
        }
        DataType::Json => parse_json_text(bytes, column_idx).map(Value::Json),
        DataType::Jsonb => jsonb_shape_cache
            .parse_text(bytes, column_idx)
            .map(Value::Jsonb),
        DataType::Xml => parse_xml_text(bytes, column_idx).map(Value::Xml),
        DataType::Bytea => {
            if s.starts_with("\\x") {
                Value::parse_bytea(s).map(Value::Bytea).ok_or_else(|| {
                    ServerError::CopyFormat(format!("column {column_idx}: invalid bytea literal"))
                })
            } else {
                Ok(Value::Bytea(bytes.to_vec()))
            }
        }
        DataType::Uuid => Value::parse_uuid(s).map(Value::Uuid).ok_or_else(|| {
            ServerError::CopyFormat(format!("column {column_idx}: invalid uuid literal"))
        }),
        DataType::Vector { dims } => match Value::parse_vector(s) {
            Some(Value::Vector(values))
                if dims.is_none() || u32::try_from(values.len()).ok() == *dims =>
            {
                Ok(Value::Vector(values))
            }
            _ => Err(ServerError::CopyFormat(format!(
                "column {column_idx}: invalid {dtype} literal"
            ))),
        },
        DataType::HalfVec { dims } => {
            parse_copy_vector_family(Value::parse_halfvec(s), *dims, dtype, column_idx)
        }
        DataType::SparseVec { dims } => {
            parse_copy_vector_family(Value::parse_sparsevec(s), *dims, dtype, column_idx)
        }
        DataType::BitVec { dims } => {
            parse_copy_vector_family(Value::parse_bitvec(s), *dims, dtype, column_idx)
        }
        DataType::Range(range_type) => ultrasql_core::RangeValue::parse(*range_type, s)
            .map(Value::Range)
            .ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
            }),
        DataType::Geometry(geometry_type) => ultrasql_core::GeometryValue::parse(*geometry_type, s)
            .map(Value::Geometry)
            .ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
            }),
        other => Err(ServerError::CopyFormat(format!(
            "column {column_idx}: unsupported COPY target type {other}"
        ))),
    }
}

fn parse_copy_vector_family(
    parsed: Option<Value>,
    expected_dims: Option<u32>,
    dtype: &DataType,
    column_idx: usize,
) -> Result<Value, ServerError> {
    let value = parsed.ok_or_else(|| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
    })?;
    let actual_dims = value.data_type().vector_dims().flatten();
    if actual_dims.is_some_and(|dims| expected_dims.is_none_or(|expected| expected == dims)) {
        Ok(value)
    } else {
        Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid {dtype} literal"
        )))
    }
}

fn parse_copy_bit_string(
    text: &str,
    dtype: &DataType,
    column_idx: usize,
) -> Result<Value, ServerError> {
    let bits = Value::parse_bit_string(text).ok_or_else(|| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid {dtype} literal"))
    })?;
    match bits {
        Value::BitString(bit_string) if bit_string.matches_type(dtype) => {
            Ok(Value::BitString(bit_string))
        }
        Value::BitString(_) => Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid {dtype} length"
        ))),
        _ => Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid {dtype} literal"
        ))),
    }
}

/// PostgreSQL-style boolean accept rules used by COPY text input.
fn parse_copy_bool(s: &str, column_idx: usize) -> Result<bool, ServerError> {
    match s {
        "t" | "true" | "TRUE" | "T" | "1" | "y" | "Y" | "yes" | "YES" => Ok(true),
        "f" | "false" | "FALSE" | "F" | "0" | "n" | "N" | "no" | "NO" => Ok(false),
        other => Err(ServerError::CopyFormat(format!(
            "column {column_idx}: not a boolean ({other:?})"
        ))),
    }
}

fn parse_copy_decimal(
    s: &str,
    scale: Option<i32>,
    column_idx: usize,
) -> Result<Value, ServerError> {
    parse_decimal_text(s, scale)
        .map_err(|err| ServerError::CopyFormat(format!("column {column_idx}: {err}")))
}

fn parse_copy_date(s: &str, column_idx: usize) -> Result<i32, ServerError> {
    let raw = s.trim();
    if raw.len() != 10 {
        return Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid date literal {raw:?}"
        )));
    }
    let bytes = raw.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid date literal {raw:?}"
        )));
    }
    let year = raw[..4].parse::<i32>().map_err(|e| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid date year: {e}"))
    })?;
    let month = raw[5..7].parse::<u32>().map_err(|e| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid date month: {e}"))
    })?;
    let day = raw[8..10].parse::<u32>().map_err(|e| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid date day: {e}"))
    })?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid date literal {raw:?}"
        )));
    }
    Ok(days_since_epoch(year, month, day))
}

fn parse_copy_timestamp(s: &str, column_idx: usize) -> Result<i64, ServerError> {
    let raw = s.trim();
    let split = raw.find(' ').or_else(|| raw.find('T')).ok_or_else(|| {
        ServerError::CopyFormat(format!(
            "column {column_idx}: invalid timestamp literal {raw:?}"
        ))
    })?;
    let date_micros = i64::from(parse_copy_date(&raw[..split], column_idx)?)
        .checked_mul(MICROS_PER_DAY)
        .ok_or_else(|| {
            ServerError::CopyFormat(format!("column {column_idx}: timestamp overflow"))
        })?;
    let time_micros = parse_copy_time(&raw[split + 1..], column_idx)?;
    date_micros
        .checked_add(time_micros)
        .ok_or_else(|| ServerError::CopyFormat(format!("column {column_idx}: timestamp overflow")))
}

fn parse_copy_timestamptz(s: &str, column_idx: usize) -> Result<i64, ServerError> {
    let raw = s.trim();
    let split = raw.find(' ').or_else(|| raw.find('T')).ok_or_else(|| {
        ServerError::CopyFormat(format!(
            "column {column_idx}: invalid timestamptz literal {raw:?}"
        ))
    })?;
    let date_micros = i64::from(parse_copy_date(&raw[..split], column_idx)?)
        .checked_mul(MICROS_PER_DAY)
        .ok_or_else(|| {
            ServerError::CopyFormat(format!("column {column_idx}: timestamptz overflow"))
        })?;
    let (time_micros, offset_seconds) = parse_timetz_text(&raw[split + 1..]).ok_or_else(|| {
        ServerError::CopyFormat(format!(
            "column {column_idx}: invalid timestamptz literal {raw:?}"
        ))
    })?;
    date_micros
        .checked_add(time_micros)
        .and_then(|v| v.checked_sub(i64::from(offset_seconds).checked_mul(1_000_000)?))
        .ok_or_else(|| {
            ServerError::CopyFormat(format!("column {column_idx}: timestamptz overflow"))
        })
}

fn parse_copy_time(s: &str, column_idx: usize) -> Result<i64, ServerError> {
    parse_time_text(s).ok_or_else(|| {
        ServerError::CopyFormat(format!(
            "column {column_idx}: invalid time literal {:?}",
            s.trim()
        ))
    })
}

fn parse_copy_timetz(s: &str, column_idx: usize) -> Result<Value, ServerError> {
    parse_timetz_text(s)
        .map(|(micros, offset_seconds)| Value::TimeTz {
            micros,
            offset_seconds,
        })
        .ok_or_else(|| {
            ServerError::CopyFormat(format!(
                "column {column_idx}: invalid timetz literal {:?}",
                s.trim()
            ))
        })
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Howard Hinnant days_from_civil algorithm bounds yoe/doe before casts"
)]
fn days_since_epoch(year: i32, month: u32, day: u32) -> i32 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days_from_1970_03_01 = era * 146_097 + doe as i32 - 719_468;
    days_from_1970_03_01 - 10_957
}

fn format_float_f32(v: f32) -> Vec<u8> {
    if v.is_nan() {
        b"NaN".to_vec()
    } else if v.is_infinite() {
        if v > 0.0 {
            b"Infinity".to_vec()
        } else {
            b"-Infinity".to_vec()
        }
    } else {
        format!("{v}").into_bytes()
    }
}

fn format_float_f64(v: f64) -> Vec<u8> {
    if v.is_nan() {
        b"NaN".to_vec()
    } else if v.is_infinite() {
        if v > 0.0 {
            b"Infinity".to_vec()
        } else {
            b"-Infinity".to_vec()
        }
    } else {
        format!("{v}").into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result_encoder::SelectResult;
    use ultrasql_core::{Field, GeometryType, Oid, RangeType};

    fn copy_opts(format: ServerCopyFormat) -> CopyOptions {
        CopyOptions {
            format,
            delimiter: ',',
            null_str: "\\N".to_owned(),
            header: false,
            auto_detect: false,
            ignore_errors: false,
            max_errors: 0,
            reject_table: None,
        }
    }

    fn schema(fields: impl IntoIterator<Item = Field>) -> Schema {
        Schema::new(fields).expect("test schema")
    }

    fn entry_with_schema(schema: Schema) -> TableEntry {
        TableEntry::new(Oid::new(42), "copy_t", "public", schema)
    }

    #[test]
    fn copy_reject_table_validation_and_textual_helpers_cover_edges() {
        let valid = entry_with_schema(schema([
            Field::required("filename", DataType::Text { max_len: None }),
            Field::required("line_number", DataType::Int64),
            Field::required("raw_row", DataType::Char { len: Some(64) }),
            Field::required("error", DataType::Text { max_len: None }),
        ]));
        validate_copy_reject_table(&valid).expect("valid reject table");

        let wrong_len = entry_with_schema(schema([Field::required(
            "filename",
            DataType::Text { max_len: None },
        )]));
        assert!(validate_copy_reject_table(&wrong_len).is_err());
        let wrong_name = entry_with_schema(schema([
            Field::required("path", DataType::Text { max_len: None }),
            Field::required("line_number", DataType::Int64),
            Field::required("raw_row", DataType::Text { max_len: None }),
            Field::required("error", DataType::Text { max_len: None }),
        ]));
        assert!(validate_copy_reject_table(&wrong_name).is_err());
        assert!(reject_column_type_matches(
            &DataType::Char { len: Some(8) },
            RejectColumnType::Text,
        ));
        assert!(!reject_column_type_matches(
            &DataType::Int32,
            RejectColumnType::Int64,
        ));

        let opts = copy_opts(ServerCopyFormat::Csv);
        assert!(
            csv_record_complete(
                br#""a","b
c""#,
                &opts
            )
            .expect("record check")
        );
        assert!(!csv_record_complete(br#""a","b"#, &opts).expect("record check"));
        assert!(csv_sample_record_complete(
            br#""a
b""#
        ));
        assert!(!csv_sample_record_complete(
            br#""a
b"#
        ));
        assert_eq!(single_byte_delimiter('|').expect("delimiter"), b'|');
        assert!(single_byte_delimiter('¿').is_err());
        assert_eq!(copy_format_code(ServerCopyFormat::Text), 0);
        assert_eq!(copy_format_code(ServerCopyFormat::Csv), 0);
        assert_eq!(copy_format_code(ServerCopyFormat::Binary), 1);
        assert_eq!(copy_format_code(ServerCopyFormat::Parquet), 0);

        let file = tempfile::NamedTempFile::new().expect("sample file");
        std::fs::write(file.path(), b"col1,col2\n\"multi\nline\",2\n").expect("write sample");
        let sample =
            read_copy_file_sample(file.path().to_str().expect("utf8 path")).expect("copy sample");
        assert!(sample.contains("multi"));

        let _env_guard = copy_env_test_lock();
        // SAFETY: copy_env_test_lock serializes process-env mutation in this
        // module's tests.
        unsafe {
            std::env::set_var("ULTRASQL_COPY_BINARY_FILE_LIMIT_BYTES", "3");
        }
        let oversized = tempfile::NamedTempFile::new().expect("oversized file");
        std::fs::write(oversized.path(), b"abcd").expect("write oversized");
        let err = read_copy_input_file(oversized.path().to_str().expect("utf8 oversized"))
            .expect_err("oversized binary COPY input rejected");
        assert!(err.to_string().contains("COPY binary file exceeds limit"));
        // SAFETY: copy_env_test_lock serializes process-env mutation in this
        // module's tests.
        unsafe {
            std::env::remove_var("ULTRASQL_COPY_BINARY_FILE_LIMIT_BYTES");
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let dir = tempfile::TempDir::new().expect("copy symlink dir");
            let link = dir.path().join("sample.csv");
            symlink(file.path(), &link).expect("symlink sample");
            assert!(read_copy_file_sample(link.to_str().expect("utf8 link")).is_err());

            let target = dir.path().join("target.out");
            let output_link = dir.path().join("output.csv");
            std::fs::write(&target, b"keep").expect("write target");
            symlink(&target, &output_link).expect("symlink output");
            assert!(
                write_copy_output_file(output_link.to_str().expect("utf8 output"), b"new").is_err()
            );
            assert_eq!(std::fs::read(&target).expect("read target"), b"keep");
        }

        let table_schema = schema([
            Field::required("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
            Field::required("created", DataType::Date),
            Field::required(
                "amount",
                DataType::Decimal {
                    precision: Some(12),
                    scale: Some(2),
                },
            ),
            Field::required("paid", DataType::Money),
        ]);
        let entry = entry_with_schema(table_schema.clone());
        let projected = projected_schema(&entry, &[1, 3]).expect("projected schema");
        assert_eq!(projected.fields()[0].name, "name");
        assert_eq!(projected.fields()[1].name, "amount");

        let row = vec![
            Value::Int32(7),
            Value::Text("ada".to_owned()),
            Value::Date(0),
            Value::Int64(12_34),
            Value::Money(56_78),
        ];
        let cells = copy_cells_from_row(&row, &table_schema, &[0, 2, 3, 4]);
        assert_eq!(cells[0].as_deref(), Some(&b"7"[..]));
        assert_eq!(cells[1].as_deref(), Some(&b"2000-01-01"[..]));
        assert_eq!(cells[2].as_deref(), Some(&b"12.34"[..]));
        assert_eq!(cells[3].as_deref(), Some(&b"$56.78"[..]));

        let select = SelectResult {
            messages: vec![
                BackendMessage::RowDescription { fields: Vec::new() },
                BackendMessage::DataRow {
                    columns: vec![Some(b"1".to_vec()), Some(b"ada".to_vec())],
                },
                BackendMessage::CommandComplete {
                    tag: "SELECT 1".to_owned(),
                },
            ],
            streamed_body: None,
            shared_streamed_body: None,
            rows: 1,
        };
        let stream_schema = schema([
            Field::required("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
        ]);
        let mut text_opts = copy_opts(ServerCopyFormat::Text);
        text_opts.header = true;
        let (payload, rows) =
            copy_rows_from_select_result(&select, &stream_schema, &text_opts).expect("copy rows");
        assert_eq!(rows, 1);
        assert!(
            String::from_utf8(payload)
                .expect("utf8")
                .starts_with("id,name\n")
        );
        assert!(
            copy_rows_from_select_result(
                &select,
                &stream_schema,
                &copy_opts(ServerCopyFormat::Binary),
            )
            .is_err()
        );
    }

    #[test]
    fn binary_copy_round_trips_rows_and_rejects_malformed_payloads() {
        let table_schema = schema([
            Field::required("b", DataType::Bool),
            Field::required("i2", DataType::Int16),
            Field::required("i4", DataType::Int32),
            Field::required("i8", DataType::Int64),
            Field::required("f4", DataType::Float32),
            Field::required("f8", DataType::Float64),
            Field::required("d", DataType::Date),
            Field::required("t", DataType::Time),
            Field::required("ts", DataType::Timestamp),
            Field::required("tstz", DataType::TimestampTz),
            Field::required("ttz", DataType::TimeTz),
            Field::required(
                "n",
                DataType::Decimal {
                    precision: Some(10),
                    scale: Some(2),
                },
            ),
            Field::required("m", DataType::Money),
            Field::required("txt", DataType::Text { max_len: None }),
            Field::required("ch", DataType::Char { len: Some(4) }),
            Field::required("bits", DataType::Bit { len: Some(4) }),
            Field::required("inet", DataType::Inet),
            Field::required("json", DataType::Json),
            Field::required("jsonb", DataType::Jsonb),
            Field::required("xml", DataType::Xml),
            Field::required("bytea", DataType::Bytea),
            Field::required("uuid", DataType::Uuid),
        ]);
        let entry = entry_with_schema(table_schema.clone());
        let row = vec![
            Value::Bool(true),
            Value::Int16(-2),
            Value::Int32(32),
            Value::Int64(64),
            Value::Float32(1.25),
            Value::Float64(-2.5),
            Value::Date(0),
            Value::Time(1_000),
            Value::Timestamp(2_000),
            Value::TimestampTz(3_000),
            Value::TimeTz {
                micros: 4_000,
                offset_seconds: -18_000,
            },
            Value::Decimal {
                value: 12_34,
                scale: 2,
            },
            Value::Money(56_78),
            Value::Text("hello".to_owned()),
            Value::Char("xy  ".to_owned()),
            Value::parse_bit_string("1010").expect("bit string"),
            Value::parse_network(&DataType::Inet, "127.0.0.1").expect("inet"),
            Value::Json("{\"a\":1}".to_owned()),
            Value::Jsonb("{\"a\":1}".to_owned()),
            Value::Xml("<root/>".to_owned()),
            Value::Bytea(vec![1, 2, 3]),
            Value::Uuid([7; 16]),
        ];

        let mut encoded = Vec::new();
        append_binary_copy_header(&mut encoded);
        append_binary_copy_row(&mut encoded, &row, &table_schema, &[], &table_schema)
            .expect("append row");
        append_i16_be(&mut encoded, -1);

        let codec = RowCodec::new(table_schema.clone());
        let mut cache = JsonbShapeCache::default();
        let payloads =
            decode_binary_copy_payload(&encoded, &entry, &[], &table_schema, &codec, &mut cache)
                .expect("decode binary copy");
        assert_eq!(payloads.len(), 1);
        assert_eq!(codec.decode(&payloads[0]).expect("row decode"), row);

        assert!(
            decode_binary_copy_payload(b"bad", &entry, &[], &table_schema, &codec, &mut cache)
                .is_err()
        );

        let mut negative_ext = Vec::new();
        negative_ext.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        negative_ext.extend_from_slice(&0_i32.to_be_bytes());
        negative_ext.extend_from_slice(&(-1_i32).to_be_bytes());
        assert!(
            decode_binary_copy_payload(
                &negative_ext,
                &entry,
                &[],
                &table_schema,
                &codec,
                &mut cache
            )
            .is_err()
        );

        let mut wrong_count = Vec::new();
        append_binary_copy_header(&mut wrong_count);
        append_i16_be(&mut wrong_count, 1);
        wrong_count.extend_from_slice(&(-1_i32).to_be_bytes());
        assert!(
            decode_binary_copy_payload(
                &wrong_count,
                &entry,
                &[],
                &table_schema,
                &codec,
                &mut cache
            )
            .is_err()
        );

        let mut bad_len = Vec::new();
        append_binary_copy_header(&mut bad_len);
        append_i16_be(
            &mut bad_len,
            i16::try_from(table_schema.len()).expect("column count"),
        );
        bad_len.extend_from_slice(&(-2_i32).to_be_bytes());
        assert!(
            decode_binary_copy_payload(&bad_len, &entry, &[], &table_schema, &codec, &mut cache)
                .is_err()
        );
    }

    #[test]
    fn copy_text_cell_decoding_covers_types_and_errors() {
        let mut cache = JsonbShapeCache::default();
        assert_eq!(
            decode_copy_cell(Some(b"yes"), &DataType::Bool, 0, &mut cache).expect("bool"),
            Value::Bool(true)
        );
        assert_eq!(
            decode_copy_cell(Some(b"N"), &DataType::Bool, 0, &mut cache).expect("bool"),
            Value::Bool(false)
        );
        assert!(decode_copy_cell(Some(b"maybe"), &DataType::Bool, 0, &mut cache).is_err());
        assert_eq!(
            decode_copy_cell(Some(b"123"), &DataType::Oid, 0, &mut cache).expect("oid"),
            Value::Oid(Oid::new(123))
        );
        assert_eq!(
            decode_copy_cell(Some(b"124"), &DataType::RegClass, 0, &mut cache).expect("regclass"),
            Value::RegClass(Oid::new(124))
        );
        assert_eq!(
            decode_copy_cell(Some(b"125"), &DataType::RegType, 0, &mut cache).expect("regtype"),
            Value::RegType(Oid::new(125))
        );
        assert!(decode_copy_cell(Some(b"bad"), &DataType::PgLsn, 0, &mut cache).is_err());
        assert_eq!(
            decode_copy_cell(Some(b"1.25"), &DataType::Float32, 0, &mut cache).expect("float4"),
            Value::Float32(1.25)
        );
        assert_eq!(
            decode_copy_cell(Some(b"-2.5"), &DataType::Float64, 0, &mut cache).expect("float8"),
            Value::Float64(-2.5)
        );
        assert_eq!(
            decode_copy_cell(
                Some(b"12.345"),
                &DataType::Decimal {
                    precision: Some(8),
                    scale: Some(2),
                },
                0,
                &mut cache,
            )
            .expect("decimal"),
            Value::Decimal {
                value: 1235,
                scale: 2,
            }
        );
        assert_eq!(
            decode_copy_cell(Some(b"$1.25"), &DataType::Money, 0, &mut cache).expect("money"),
            Value::Money(125)
        );
        assert_eq!(
            decode_copy_cell(Some(b"1970-01-02"), &DataType::Date, 0, &mut cache).expect("date"),
            Value::Date(-10_956)
        );
        assert!(decode_copy_cell(Some(b"2024-02-30"), &DataType::Date, 0, &mut cache).is_err());
        assert_eq!(
            decode_copy_cell(Some(b"00:00:01"), &DataType::Time, 0, &mut cache).expect("time"),
            Value::Time(1_000_000)
        );
        assert_eq!(
            decode_copy_cell(Some(b"00:00:01+05"), &DataType::TimeTz, 0, &mut cache)
                .expect("timetz"),
            Value::TimeTz {
                micros: 1_000_000,
                offset_seconds: 18_000,
            }
        );
        assert_eq!(
            decode_copy_cell(
                Some(b"1970-01-01 00:00:01"),
                &DataType::Timestamp,
                0,
                &mut cache,
            )
            .expect("timestamp"),
            Value::Timestamp(-946_684_799_000_000)
        );
        assert_eq!(
            decode_copy_cell(
                Some(b"1970-01-01 00:00:01+00"),
                &DataType::TimestampTz,
                0,
                &mut cache,
            )
            .expect("timestamptz"),
            Value::TimestampTz(-946_684_799_000_000)
        );
        assert_eq!(
            decode_copy_cell(Some(b"xy"), &DataType::Char { len: Some(4) }, 0, &mut cache,)
                .expect("char"),
            Value::Char("xy  ".to_owned())
        );
        assert_eq!(
            decode_copy_cell(
                Some(b"1010"),
                &DataType::Bit { len: Some(4) },
                0,
                &mut cache,
            )
            .expect("bit"),
            Value::parse_bit_string("1010").expect("bit string")
        );
        assert!(
            decode_copy_cell(Some(b"101"), &DataType::Bit { len: Some(4) }, 0, &mut cache,)
                .is_err()
        );
        assert!(decode_copy_cell(Some(b"{"), &DataType::Json, 0, &mut cache).is_err());
        assert_eq!(
            decode_copy_cell(Some(b"<root/>"), &DataType::Xml, 0, &mut cache).expect("xml"),
            Value::Xml("<root/>".to_owned())
        );
        assert!(decode_copy_cell(Some(b"<root>"), &DataType::Xml, 0, &mut cache).is_err());
        assert_eq!(
            decode_copy_cell(
                Some(b"00000000-0000-0000-0000-000000000007"),
                &DataType::Uuid,
                0,
                &mut cache,
            )
            .expect("uuid"),
            Value::Uuid([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7])
        );
        assert!(
            decode_copy_cell(
                Some(b"[1,2]"),
                &DataType::Vector { dims: Some(3) },
                0,
                &mut cache,
            )
            .is_err()
        );
        assert_eq!(
            decode_copy_cell(
                Some(b"[1,2]"),
                &DataType::HalfVec { dims: Some(2) },
                0,
                &mut cache,
            )
            .expect("halfvec"),
            Value::parse_halfvec("[1,2]").expect("halfvec")
        );
        assert_eq!(
            decode_copy_cell(
                Some(b"{1:1,3:2}/3"),
                &DataType::SparseVec { dims: Some(3) },
                0,
                &mut cache,
            )
            .expect("sparsevec"),
            Value::parse_sparsevec("{1:1,3:2}/3").expect("sparsevec")
        );
        assert_eq!(
            decode_copy_cell(
                Some(b"101"),
                &DataType::BitVec { dims: Some(3) },
                0,
                &mut cache,
            )
            .expect("bitvec"),
            Value::parse_bitvec("101").expect("bitvec")
        );
        assert!(
            decode_copy_cell(
                Some(b"101"),
                &DataType::BitVec { dims: Some(4) },
                0,
                &mut cache,
            )
            .is_err()
        );
        assert_eq!(
            decode_copy_cell(
                Some(b"[1,10)"),
                &DataType::Range(RangeType::Int4),
                0,
                &mut cache,
            )
            .expect("range"),
            Value::Range(
                ultrasql_core::RangeValue::parse(RangeType::Int4, "[1,10)").expect("range")
            )
        );
        assert!(
            decode_copy_cell(
                Some(b"bad"),
                &DataType::Range(RangeType::Int4),
                0,
                &mut cache,
            )
            .is_err()
        );
        assert_eq!(
            decode_copy_cell(
                Some(b"(1,2)"),
                &DataType::Geometry(GeometryType::Point),
                0,
                &mut cache,
            )
            .expect("point"),
            Value::Geometry(
                ultrasql_core::GeometryValue::parse(GeometryType::Point, "(1,2)").expect("point")
            )
        );
        assert!(
            decode_copy_cell(
                Some(b"(1)"),
                &DataType::Geometry(GeometryType::Point),
                0,
                &mut cache,
            )
            .is_err()
        );
        assert_eq!(
            decode_copy_cell(Some(b"\\x0a0b"), &DataType::Bytea, 0, &mut cache).expect("bytea"),
            Value::Bytea(vec![10, 11])
        );
        assert_eq!(
            decode_copy_cell(Some(b"deadbeef"), &DataType::Bytea, 0, &mut cache)
                .expect("raw bytea"),
            Value::Bytea(b"deadbeef".to_vec())
        );
        assert!(decode_copy_cell(Some(b"\\xabc"), &DataType::Bytea, 0, &mut cache).is_err());
        assert!(
            decode_copy_cell(
                Some(&[0xff]),
                &DataType::Text { max_len: None },
                0,
                &mut cache
            )
            .is_err()
        );
        assert_eq!(
            decode_copy_cell(None, &DataType::Int32, 0, &mut cache).expect("null"),
            Value::Null
        );
        assert!(decode_copy_cell(Some(b"x"), &DataType::Null, 0, &mut cache).is_err());
    }

    #[test]
    fn copy_row_and_binary_cell_helpers_cover_projection_and_errors() {
        let table_schema = schema([
            Field::required("id", DataType::Int32),
            Field::required("name", DataType::Text { max_len: None }),
            Field::nullable("optional", DataType::Int64),
        ]);
        let stream_schema = schema([
            Field::required("name", DataType::Text { max_len: None }),
            Field::required("id", DataType::Int32),
        ]);
        let entry = entry_with_schema(table_schema.clone());
        let codec = RowCodec::new(table_schema.clone());
        let mut cache = JsonbShapeCache::default();
        let payload = decode_copy_cells_to_payload(
            &[Some(b"ada"), Some(b"7")],
            &entry,
            &[1, 0],
            &stream_schema,
            &codec,
            &mut cache,
        )
        .expect("decode projected payload");
        let decoded = codec.decode(&payload).expect("payload row");
        assert_eq!(
            decoded,
            vec![Value::Int32(7), Value::Text("ada".to_owned()), Value::Null]
        );
        assert!(
            decode_copy_cells_to_payload(
                &[Some(b"ada")],
                &entry,
                &[1, 0],
                &stream_schema,
                &codec,
                &mut cache,
            )
            .is_err()
        );

        let opts = copy_opts(ServerCopyFormat::Csv);
        let payload = decode_one_copy_row(
            b"ada,7\n",
            &entry,
            &[1, 0],
            &stream_schema,
            &codec,
            &opts,
            &mut cache,
        )
        .expect("fast csv decode");
        assert_eq!(
            codec.decode(&payload).expect("fast row")[0],
            Value::Int32(7)
        );
        assert!(
            decode_one_copy_row(
                b"ada,7\n",
                &entry,
                &[1, 0],
                &stream_schema,
                &codec,
                &copy_opts(ServerCopyFormat::Binary),
                &mut cache,
            )
            .is_err()
        );

        assert!(read_i16_be(&[1], &mut 0).is_err());
        assert!(read_i32_be(&[1, 2, 3], &mut 0).is_err());
        assert_eq!(
            binary_copy_cell_bytes(&Value::Null, &DataType::Text { max_len: None })
                .expect("null fallback"),
            b"NULL".to_vec()
        );
        assert!(decode_binary_copy_cell(&[1, 2], &DataType::Int32, 0, &mut cache).is_err());
        assert!(decode_binary_copy_cell(b"bad", &DataType::Jsonb, 0, &mut cache).is_err());
        assert!(decode_binary_copy_cell(b"<bad>", &DataType::Xml, 0, &mut cache).is_err());
        assert_eq!(
            decode_binary_copy_cell(&[9; 16], &DataType::Uuid, 0, &mut cache).expect("uuid"),
            Value::Uuid([9; 16])
        );

        assert_eq!(parse_copy_date("2000-02-29", 0).expect("leap"), 59);
        assert!(parse_copy_date("20000229", 0).is_err());
        assert!(parse_copy_date("year-02-29", 0).is_err());
        assert!(parse_copy_date("2024-mm-29", 0).is_err());
        assert!(parse_copy_date("2024-02-dd", 0).is_err());
        assert!(parse_copy_timestamp("1970-01-01", 0).is_err());
        assert!(parse_copy_timestamptz("1970-01-01 bad", 0).is_err());
        assert!(parse_copy_time("bad", 0).is_err());
        assert!(parse_copy_timetz("bad", 0).is_err());
        assert_eq!(days_in_month(2024, 2), 29);
        assert_eq!(days_in_month(2023, 2), 28);
        assert_eq!(days_in_month(2023, 13), 0);
        assert_eq!(format_float_f32(f32::INFINITY), b"Infinity".to_vec());
        assert_eq!(format_float_f64(f64::NEG_INFINITY), b"-Infinity".to_vec());
        assert_eq!(format_float_f64(f64::NAN), b"NaN".to_vec());
    }

    fn copy_env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .expect("copy env test lock")
    }
}
