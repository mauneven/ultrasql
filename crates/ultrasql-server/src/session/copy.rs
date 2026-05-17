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

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::warn;
use ultrasql_catalog::{CatalogSnapshot, TableEntry};
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
use crate::CombinedCatalog;
use crate::copy::{
    CopyFormat as ServerCopyFormat, CopyOptions, copy_in_response, copy_out_response,
    encode_csv_row, encode_text_row, parse_csv_row, parse_text_row,
};
use crate::error::ServerError;

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
            columns,
            direction,
            source,
            format,
            delimiter,
            null_str,
            header,
            schema,
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "handle_copy_statement called with non-Copy plan",
            ));
        };

        match (direction, source) {
            (CopyDirection::From, CopySource::Stdin) | (CopyDirection::To, CopySource::Stdout) => {}
            _ => {
                return Err(ServerError::Unsupported(
                    "COPY direction/source mismatch (only FROM STDIN / TO STDOUT supported)",
                ));
            }
        }

        let catalog_snapshot: Arc<CatalogSnapshot> = self.state.catalog_snapshot();
        let entry = catalog_snapshot
            .tables
            .get(relation)
            .ok_or_else(|| {
                ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(relation.clone()))
            })?
            .clone();

        let opts = CopyOptions {
            format: match format {
                PlanCopyFormat::Text => ServerCopyFormat::Text,
                PlanCopyFormat::Csv => ServerCopyFormat::Csv,
            },
            delimiter: *delimiter,
            null_str: null_str.clone(),
            header: *header,
        };

        match direction {
            CopyDirection::To => {
                self.copy_to_stdout(&entry, columns, schema, &opts, emit_ready_for_query)
                    .await
            }
            CopyDirection::From => {
                self.copy_from_stdin(&entry, columns, schema, &opts, emit_ready_for_query)
                    .await
            }
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
        let n_columns = schema.len();
        self.write_buf.clear();
        encode_backend(&copy_out_response(n_columns), &mut self.write_buf);
        self.io.write_all(&self.write_buf).await?;
        self.io.flush().await?;

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
                    row.iter().map(value_to_copy_cell).collect()
                } else {
                    columns
                        .iter()
                        .map(|&i| row.get(i).and_then(value_to_copy_cell))
                        .collect()
                };
                let bytes = match opts.format {
                    ServerCopyFormat::Text => encode_text_row(&cells, opts),
                    ServerCopyFormat::Csv => encode_csv_row(&cells, opts),
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
        let n_columns = schema.len();
        self.send(&copy_in_response(n_columns)).await?;

        let mut buffer: Vec<u8> = Vec::new();
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
                    while let Some(line) = take_line(&mut buffer) {
                        if !header_skipped {
                            header_skipped = true;
                            continue;
                        }
                        if let Err(e) = self
                            .insert_one_copy_row(&line, entry, columns, schema, &codec, opts, &txn)
                        {
                            if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                                warn!(error = %abort_err, "COPY FROM autocommit abort failed");
                            }
                            self.drain_copy_remainder().await?;
                            return Err(e);
                        }
                        rows_inserted = rows_inserted.saturating_add(1);
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
                if let Err(e) =
                    self.insert_one_copy_row(&line, entry, columns, schema, &codec, opts, &txn)
                {
                    if let Err(abort_err) = self.state.txn_manager.abort(txn) {
                        warn!(error = %abort_err, "COPY FROM autocommit abort failed");
                    }
                    return Err(e);
                }
                rows_inserted = rows_inserted.saturating_add(1);
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

    /// Decode one CopyData line into a `Vec<Value>` and write it to
    /// the heap. The argument list is wide because every step needs a
    /// piece of state the dispatcher already has on hand — packing them
    /// into a struct would just push the indirection through every call
    /// site without changing the cost. The local `#[allow]` keeps clippy
    /// quiet without raising the workspace-wide threshold.
    #[allow(clippy::too_many_arguments)]
    fn insert_one_copy_row(
        &self,
        line: &[u8],
        entry: &TableEntry,
        columns: &[usize],
        schema: &Schema,
        codec: &RowCodec,
        opts: &CopyOptions,
        txn: &Transaction,
    ) -> Result<(), ServerError> {
        let raw_cells = match opts.format {
            ServerCopyFormat::Text => parse_text_row(line, opts)?,
            ServerCopyFormat::Csv => parse_csv_row(line, opts)?,
        };
        if raw_cells.len() != schema.len() {
            return Err(ServerError::CopyFormat(format!(
                "COPY row has {} columns; expected {}",
                raw_cells.len(),
                schema.len()
            )));
        }

        let table_arity = entry.schema.len();
        let mut row: Vec<Value> = vec![Value::Null; table_arity];
        if columns.is_empty() {
            for (i, cell) in raw_cells.iter().enumerate() {
                let dtype = &entry.schema.fields()[i].data_type;
                row[i] = decode_copy_cell(cell.as_deref(), dtype, i)?;
            }
        } else {
            for (cell_idx, cell) in raw_cells.iter().enumerate() {
                let table_idx = columns[cell_idx];
                let dtype = &entry.schema.fields()[table_idx].data_type;
                row[table_idx] = decode_copy_cell(cell.as_deref(), dtype, table_idx)?;
            }
        }
        for (idx, (value, field)) in row.iter().zip(entry.schema.fields().iter()).enumerate() {
            if matches!(value, Value::Null) && !field.nullable {
                let name = if field.name.is_empty() {
                    format!("#{idx}")
                } else {
                    field.name.clone()
                };
                return Err(ServerError::CopyFormat(format!(
                    "NOT NULL constraint violated for column {name} in COPY row",
                )));
            }
        }
        let payload = codec
            .encode(&row)
            .map_err(|e| ServerError::CopyFormat(format!("COPY encode: {e}")))?;
        let insert_opts = InsertOptions {
            xmin: txn.current_xid(),
            command_id: txn.current_command,
            wal: None,
            fsm: None,
            vm: None,
        };
        self.state
            .heap
            .insert(RelationId(entry.oid), &payload, insert_opts)
            .map_err(|e| ServerError::ddl(format!("COPY FROM heap insert: {e}")))?;
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

/// Encode a runtime [`Value`] as a `CopyData` cell (`None` is SQL NULL).
fn value_to_copy_cell(value: &Value) -> Option<Vec<u8>> {
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
        Value::Uuid(bytes) => Some(format!("{bytes:x?}").into_bytes()),
        Value::Decimal { .. } | Value::Interval { .. } => {
            Some(value.to_string().into_bytes())
        }
    }
}

/// Decode a single COPY cell into a typed [`Value`] consistent with the
/// target column's [`DataType`].
fn decode_copy_cell(
    raw: Option<&[u8]>,
    dtype: &DataType,
    column_idx: usize,
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
        DataType::Text { .. } => Ok(Value::Text(s.to_string())),
        DataType::Bytea => Ok(Value::Bytea(bytes.to_vec())),
        other => Err(ServerError::CopyFormat(format!(
            "column {column_idx}: unsupported COPY target type {other}"
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

/// Take the next `\n`-terminated line out of `buffer`, returning it as
/// a fresh `Vec<u8>` (with the newline included) and leaving the
/// remainder in `buffer`. Returns `None` when no full line is available.
fn take_line(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    let nl = buffer.iter().position(|&b| b == b'\n')?;
    let mut line = buffer.split_off(nl + 1);
    std::mem::swap(buffer, &mut line);
    Some(line)
}
