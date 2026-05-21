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

use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::warn;
use ultrasql_catalog::{CatalogSnapshot, TableEntry};
use ultrasql_core::csv::sniff_csv_text;
use ultrasql_core::{DataType, RelationId, Schema, Value};
use ultrasql_executor::RowCodec;
use ultrasql_parser::Parser;
use ultrasql_planner::{
    CopyDirection, CopyFormat as PlanCopyFormat, CopySource, LogicalPlan, bind,
};
use ultrasql_protocol::{BackendMessage, FrontendMessage, encode_backend};
use ultrasql_storage::heap::InsertOptions;
use ultrasql_txn::{IsolationLevel, Transaction};

use super::Session;
use super::jsonb_ingest::{JsonbShapeCache, encode_pg_binary_jsonb};
use crate::CombinedCatalog;
use crate::copy::{
    CopyFormat as ServerCopyFormat, CopyOptions, copy_in_response_with_format,
    copy_out_response_with_format, encode_csv_row, encode_text_row, parse_csv_row, parse_text_row,
    parse_unquoted_csv_row_slices,
};
use crate::error::ServerError;

const COPY_INSERT_BATCH_ROWS: usize = 4096;
const COPY_AUTODETECT_SAMPLE_BYTES: usize = 64 * 1024;
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
        if let Err(e) = self.state.txn_manager.commit(txn) {
            warn!(error = %e, "COPY FROM autocommit commit failed");
        }
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
            let bytes = fs::read(path)
                .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
            return self
                .copy_binary_bytes_into_table(entry, columns, schema, &bytes, emit_ready_for_query)
                .await;
        }

        let effective_opts = self.effective_copy_file_options(path, opts)?;
        let file = File::open(path)
            .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
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
        if let Err(e) = self.state.txn_manager.commit(txn) {
            warn!(error = %e, "COPY FROM file commit failed");
        }
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
        if let Err(e) = self.state.txn_manager.commit(txn) {
            warn!(error = %e, "binary COPY FROM commit failed");
        }
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
        fs::write(path, bytes)
            .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
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
            persistent_catalog: Arc::clone(&self.state.persistent_catalog),
            time_partitions: Arc::clone(&self.state.time_partitions),
            workload_recorder: Arc::clone(&self.state.workload_recorder),
            sequence_state: Some(self.sequence_state.clone()),
            heap: Arc::clone(&self.state.heap),
            vm: Arc::clone(&self.state.vm),
            snapshot: txn.snapshot.clone(),
            oracle: Arc::clone(&self.state.txn_manager),
            xid: txn.current_xid(),
            command_id: txn.current_command,
            cte_buffers: std::collections::HashMap::new(),
            jit: self.jit_config(),
            cancel_flag: Some(self.cancel_flag.clone()),
            work_mem: Arc::new(ultrasql_executor::work_mem::WorkMemBudget::new(u64::MAX)),
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
                fs::write(path, payload)
                    .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
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
        let insert_opts = InsertOptions {
            xmin: txn.current_xid(),
            command_id: txn.current_command,
            wal: None,
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
        RejectColumnType::Text => matches!(data_type, DataType::Text { .. }),
        RejectColumnType::Int64 => *data_type == DataType::Int64,
    }
}

fn read_copy_file_sample(path: &str) -> Result<String, ServerError> {
    let file = File::open(path)
        .map_err(|e| ServerError::Io(std::io::Error::other(format!("{path}: {e}"))))?;
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
        (DataType::Float32, Value::Float32(v)) => v.to_bits().to_be_bytes().to_vec(),
        (DataType::Float64, Value::Float64(v)) => v.to_bits().to_be_bytes().to_vec(),
        (DataType::Date, Value::Date(v) | Value::Int32(v)) => v.to_be_bytes().to_vec(),
        (DataType::Text { .. }, Value::Text(v)) => v.as_bytes().to_vec(),
        (DataType::Jsonb, Value::Jsonb(v)) => encode_pg_binary_jsonb(v),
        (DataType::Bytea, Value::Bytea(v)) => v.clone(),
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
        DataType::Text { .. } => std::str::from_utf8(bytes)
            .map(|s| Value::Text(s.to_string()))
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Jsonb => jsonb_shape_cache
            .parse_pg_binary(bytes, column_idx)
            .map(Value::Jsonb),
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
        Value::Float32(v) => Some(format_float_f32(*v)),
        Value::Float64(v) => Some(format_float_f64(*v)),
        Value::Text(s) => Some(s.as_bytes().to_vec()),
        Value::Bytea(b) => Some(b.clone()),
        Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => {
            Some(v.to_string().into_bytes())
        }
        Value::Date(v) => Some(v.to_string().into_bytes()),
        Value::Uuid(bytes) => Some(Value::Uuid(*bytes).to_string().into_bytes()),
        Value::Decimal { .. }
        | Value::Interval { .. }
        | Value::Range(_)
        | Value::Geometry(_)
        | Value::Jsonb(_)
        | Value::Vector(_)
        | Value::HalfVec(_)
        | Value::SparseVec(_)
        | Value::BitVec { .. }
        | Value::Array { .. } => Some(value.to_string().into_bytes()),
    }
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
        DataType::Float32 => s
            .parse::<f32>()
            .map(Value::Float32)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Float64 => s
            .parse::<f64>()
            .map(Value::Float64)
            .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}"))),
        DataType::Decimal { scale, .. } => parse_copy_decimal(s, scale.unwrap_or(0), column_idx),
        DataType::Date => parse_copy_date(s, column_idx).map(Value::Date),
        DataType::Time => parse_copy_time(s, column_idx).map(Value::Time),
        DataType::Timestamp => parse_copy_timestamp(s, column_idx).map(Value::Timestamp),
        DataType::TimestampTz => parse_copy_timestamp(s, column_idx).map(Value::TimestampTz),
        DataType::Text { .. } => Ok(Value::Text(s.to_string())),
        DataType::Jsonb => jsonb_shape_cache
            .parse_text(bytes, column_idx)
            .map(Value::Jsonb),
        DataType::Bytea => Ok(Value::Bytea(bytes.to_vec())),
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

fn parse_copy_decimal(s: &str, scale: i32, column_idx: usize) -> Result<Value, ServerError> {
    let raw = s.trim();
    let scale_usize = usize::try_from(scale).map_err(|_| {
        ServerError::CopyFormat(format!(
            "column {column_idx}: negative decimal scale {scale} not supported by COPY"
        ))
    })?;
    let (negative, digits) = match raw.as_bytes().first() {
        Some(b'-') => (true, &raw[1..]),
        Some(b'+') => (false, &raw[1..]),
        _ => (false, raw),
    };
    let mut parts = digits.split('.');
    let whole = parts.next().unwrap_or_default();
    let frac = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || (whole.is_empty() && frac.is_empty())
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid decimal literal {raw:?}"
        )));
    }
    if frac.len() > scale_usize && frac.as_bytes()[scale_usize..].iter().any(|&b| b != b'0') {
        return Err(ServerError::CopyFormat(format!(
            "column {column_idx}: decimal literal {raw:?} has scale greater than {scale}"
        )));
    }

    let mut value: i128 = 0;
    for digit in whole.bytes() {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(digit - b'0')))
            .ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: decimal overflow"))
            })?;
    }
    for digit in frac.bytes().take(scale_usize) {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(digit - b'0')))
            .ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: decimal overflow"))
            })?;
    }
    let missing_frac_digits = scale_usize.saturating_sub(frac.len().min(scale_usize));
    for _ in 0..missing_frac_digits {
        value = value.checked_mul(10).ok_or_else(|| {
            ServerError::CopyFormat(format!("column {column_idx}: decimal overflow"))
        })?;
    }
    if negative {
        value = value.checked_neg().ok_or_else(|| {
            ServerError::CopyFormat(format!("column {column_idx}: decimal overflow"))
        })?;
    }
    let value = i64::try_from(value)
        .map_err(|_| ServerError::CopyFormat(format!("column {column_idx}: decimal overflow")))?;
    Ok(Value::Decimal { value, scale })
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

fn parse_copy_time(s: &str, column_idx: usize) -> Result<i64, ServerError> {
    let raw = s.trim();
    let mut parts = raw.splitn(3, ':');
    let hour = parts
        .next()
        .ok_or_else(|| {
            ServerError::CopyFormat(format!("column {column_idx}: invalid time literal {raw:?}"))
        })?
        .parse::<i64>()
        .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}")))?;
    let minute = parts
        .next()
        .ok_or_else(|| {
            ServerError::CopyFormat(format!("column {column_idx}: invalid time literal {raw:?}"))
        })?
        .parse::<i64>()
        .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}")))?;
    let second_text = parts.next().ok_or_else(|| {
        ServerError::CopyFormat(format!("column {column_idx}: invalid time literal {raw:?}"))
    })?;
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) {
        return Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid time literal {raw:?}"
        )));
    }
    let (second_part, frac_part) = second_text
        .split_once('.')
        .map_or((second_text, ""), |(sec, frac)| (sec, frac));
    let second = second_part
        .parse::<i64>()
        .map_err(|e| ServerError::CopyFormat(format!("column {column_idx}: {e}")))?;
    if !(0..=59).contains(&second) {
        return Err(ServerError::CopyFormat(format!(
            "column {column_idx}: invalid time literal {raw:?}"
        )));
    }

    let mut frac_micros = 0_i64;
    let mut scale = 100_000_i64;
    for ch in frac_part.chars().take(6) {
        let digit = i64::from(ch.to_digit(10).ok_or_else(|| {
            ServerError::CopyFormat(format!("column {column_idx}: invalid time literal {raw:?}"))
        })?);
        frac_micros = frac_micros
            .checked_add(digit.checked_mul(scale).ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: time overflow"))
            })?)
            .ok_or_else(|| {
                ServerError::CopyFormat(format!("column {column_idx}: time overflow"))
            })?;
        scale /= 10;
    }

    hour.checked_mul(3_600_000_000)
        .and_then(|v| v.checked_add(minute.checked_mul(60_000_000)?))
        .and_then(|v| v.checked_add(second.checked_mul(1_000_000)?))
        .and_then(|v| v.checked_add(frac_micros))
        .ok_or_else(|| ServerError::CopyFormat(format!("column {column_idx}: time overflow")))
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
