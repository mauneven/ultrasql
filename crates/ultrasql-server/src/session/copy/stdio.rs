//! `COPY ... TO/FROM STDOUT/STDIN` streaming over the client connection.
//!
//! Drives the COPY wire flow against the async socket: emitting
//! `CopyOutResponse`/`CopyData`/`CopyDone` for `TO STDOUT`, and consuming the
//! client's `CopyData`/`CopyDone`/`CopyFail` frames for `FROM STDIN`
//! (text, CSV, and binary), plus the shared batch-insert and completion paths.

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use ultrasql_catalog::TableEntry;
use ultrasql_core::{RelationId, Schema};
use ultrasql_executor::RowCodec;
use ultrasql_protocol::{BackendMessage, FrontendMessage, encode_backend};
use ultrasql_storage::heap::InsertOptions;
use ultrasql_txn::{IsolationLevel, Transaction};

use super::super::Session;
use super::binary::decode_binary_copy_payload;
use super::decode::{decode_one_copy_row, value_to_copy_cell_with_options};
use super::fs_io::{check_copy_stdin_within_limit, copy_binary_file_limit_bytes, copy_format_code};
use super::{
    COPY_INSERT_BATCH_ROWS, CopyOptions, CopyRowDecodeContext, ServerCopyFormat, ServerError,
    add_copy_batch_rows, copy_in_response_with_format, copy_out_response_with_format,
    copy_rows_from_usize, copy_table_key, encode_csv_row, encode_text_row, increment_copy_rows,
};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(super) async fn copy_to_stdout(
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
            // `COPY ... TO STDOUT` is a read: in an explicit transaction it
            // scans the session txn's (command-advanced) snapshot and never
            // commits/aborts it; in autocommit it runs today's implicit read
            // txn. The encode is synchronous, so the snapshot borrow never
            // crosses the `.await` below.
            let (payload, rows_sent) = self.with_copy_read_snapshot(
                "binary COPY scan commit",
                "binary COPY rollback after scan error",
                |session, snapshot| {
                    session.encode_table_binary_copy(entry, columns, schema, snapshot)
                },
            )?;
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

        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let codec = RowCodec::new(entry.schema.clone());
        let text_options = crate::result_encoder::TextEncodingOptions::from_session_settings(
            &self.session_settings,
        );

        // Build the full CopyData body synchronously under the read snapshot.
        // `with_copy_read_snapshot` finalises the implicit read txn (autocommit)
        // or leaves the session txn open + intact (explicit), and never aborts
        // the session txn on a scan error.
        let (wire_buf, rows_sent) = self.with_copy_read_snapshot(
            "COPY TO autocommit commit",
            "COPY TO autocommit rollback after scan error",
            |session, snapshot| {
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
                let scan = session.state.heap.scan_visible(
                    rel,
                    block_count,
                    snapshot,
                    session.state.txn_manager.as_ref(),
                );
                for result in scan {
                    let tup =
                        result.map_err(|e| ServerError::ddl(format!("COPY TO heap scan: {e}")))?;
                    let row = codec
                        .decode(&tup.data)
                        .map_err(|e| ServerError::CopyFormat(format!("COPY TO row decode: {e}")))?;
                    let cells: Vec<Option<Vec<u8>>> = if columns.is_empty() {
                        row.iter()
                            .zip(entry.schema.fields())
                            .map(|(value, field)| {
                                value_to_copy_cell_with_options(
                                    value,
                                    &field.data_type,
                                    &text_options,
                                )
                            })
                            .collect()
                    } else {
                        columns
                            .iter()
                            .map(|&i| {
                                let field = entry.schema.field_at(i);
                                row.get(i).and_then(|value| {
                                    value_to_copy_cell_with_options(
                                        value,
                                        &field.data_type,
                                        &text_options,
                                    )
                                })
                            })
                            .collect()
                    };
                    let bytes = match opts.format {
                        ServerCopyFormat::Text => encode_text_row(&cells, opts),
                        ServerCopyFormat::Csv => encode_csv_row(&cells, opts),
                        ServerCopyFormat::Binary | ServerCopyFormat::Parquet => Vec::new(),
                    };
                    encode_backend(&BackendMessage::CopyData(bytes), &mut wire_buf);
                    increment_copy_rows(&mut rows_sent, "COPY TO STDOUT")?;
                }
                Ok((wire_buf, rows_sent))
            },
        )?;
        let mut wire_buf = wire_buf;

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

    pub(super) async fn copy_from_stdin(
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
        // Shape A: in an explicit transaction block, COPY rides the SESSION
        // txn so a later ROLLBACK discards its rows and a COMMIT makes them
        // durable atomically. We `take` the txn out of `self.txn_state` and
        // hold it as an owned local so the synchronous insert calls can borrow
        // `&Transaction` while the async wire I/O between them still borrows
        // `&mut self` — no borrow ever crosses an `.await`. The snapshot is
        // refreshed ONCE up front (advancing `current_command`) so the COPY
        // sees prior in-txn writes and stamps rows with the session xid. In
        // autocommit mode we open today's implicit ReadCommitted txn.
        let session_mode = self.copy_in_session_txn();
        let txn = if session_mode {
            match std::mem::replace(&mut self.txn_state, crate::TxnState::Idle) {
                crate::TxnState::InTransaction(mut txn) => {
                    self.state.txn_manager.refresh_snapshot(&mut txn);
                    txn
                }
                // `copy_in_session_txn` already gated InTransaction; the other
                // arms are unreachable, but restore + bail rather than panic.
                other => {
                    self.txn_state = other;
                    return Err(ServerError::Unsupported(
                        "COPY FROM session txn vanished mid-dispatch",
                    ));
                }
            }
        } else {
            self.state.txn_manager.begin(IsolationLevel::ReadCommitted)
        };
        let codec = RowCodec::new(entry.schema.clone());

        let mut rows_inserted: u64 = 0;
        let mut header_skipped = !opts.header;
        let mut received_done = false;
        let mut client_fail_message: Option<String> = None;

        loop {
            // The session txn is held as an owned local here (taken out of
            // `self.txn_state` above). A bare `?` on this wire read would drop
            // that local on a socket/EOF error and leave `self.txn_state =
            // Idle`, silently losing the session's transaction. Park it back as
            // `Failed(txn)` (via `fail_or_rollback_copy_from`) before
            // propagating so the take-and-park contract holds on the dying-
            // connection edge too.
            let msg = match self.read_frontend().await {
                Ok(msg) => msg,
                Err(e) => {
                    return Err(self.fail_or_rollback_copy_from(
                        session_mode,
                        txn,
                        e,
                        "COPY FROM autocommit rollback after wire read error",
                    ));
                }
            };
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
                                opts,
                                CopyRowDecodeContext {
                                    entry,
                                    columns,
                                    schema,
                                    codec: &codec,
                                    jsonb_shape_cache: &mut jsonb_shape_cache,
                                },
                            )
                        };
                        start = end;
                        match decoded {
                            Ok(payload) => payload_batch.push(payload),
                            Err(e) => {
                                let err = self.fail_or_rollback_copy_from(
                                    session_mode,
                                    txn,
                                    e,
                                    "COPY FROM autocommit rollback after row decode error",
                                );
                                self.drain_copy_remainder().await?;
                                return Err(err);
                            }
                        }
                        if payload_batch.len() == COPY_INSERT_BATCH_ROWS {
                            if let Err(e) = add_copy_batch_rows(
                                &mut rows_inserted,
                                payload_batch.len(),
                                "COPY FROM STDIN",
                            ) {
                                let err = self.fail_or_rollback_copy_from(
                                    session_mode,
                                    txn,
                                    e,
                                    "COPY FROM autocommit rollback after row count overflow",
                                );
                                self.drain_copy_remainder().await?;
                                return Err(err);
                            }
                            if let Err(e) = self.flush_copy_insert_batch(
                                entry,
                                &payload_batch,
                                &txn,
                                !session_mode,
                            ) {
                                let err = self.fail_or_rollback_copy_from(
                                    session_mode,
                                    txn,
                                    e,
                                    "COPY FROM autocommit rollback after insert batch error",
                                );
                                self.drain_copy_remainder().await?;
                                return Err(err);
                            }
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
                    return Err(self.fail_or_rollback_copy_from(
                        session_mode,
                        txn,
                        ServerError::UnexpectedEof,
                        "COPY FROM rollback after terminate",
                    ));
                }
                other => {
                    return Err(self.fail_or_rollback_copy_from(
                        session_mode,
                        txn,
                        ServerError::CopyFormat(format!(
                            "unexpected frontend message during COPY FROM: {other:?}"
                        )),
                        "COPY FROM rollback after protocol error",
                    ));
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
                        opts,
                        CopyRowDecodeContext {
                            entry,
                            columns,
                            schema,
                            codec: &codec,
                            jsonb_shape_cache: &mut jsonb_shape_cache,
                        },
                    )
                };
                match decoded {
                    Ok(payload) => payload_batch.push(payload),
                    Err(e) => {
                        return Err(self.fail_or_rollback_copy_from(
                            session_mode,
                            txn,
                            e,
                            "COPY FROM autocommit rollback after final row decode error",
                        ));
                    }
                }
            } else {
                buffer.clear();
            }
        }

        if let Some(reason) = client_fail_message {
            return Err(self.fail_or_rollback_copy_from(
                session_mode,
                txn,
                ServerError::CopyAborted(reason),
                "COPY FROM rollback after CopyFail",
            ));
        }

        if !payload_batch.is_empty() {
            if let Err(e) =
                add_copy_batch_rows(&mut rows_inserted, payload_batch.len(), "COPY FROM STDIN")
            {
                return Err(self.fail_or_rollback_copy_from(
                    session_mode,
                    txn,
                    e,
                    "COPY FROM autocommit rollback after row count overflow",
                ));
            }
            if let Err(e) = self.flush_copy_insert_batch(entry, &payload_batch, &txn, !session_mode)
            {
                return Err(self.fail_or_rollback_copy_from(
                    session_mode,
                    txn,
                    e,
                    "COPY FROM autocommit rollback after insert batch error",
                ));
            }
        }

        if session_mode {
            // Do NOT commit and do NOT validate deferred FKs here — COMMIT's
            // `execute_commit` validates via `pending_table_modifications` and
            // rebuilds the column cache for the COPY-touched table. Note the
            // table so that machinery covers it.
            //
            // PARK FIRST, then note: `note_copy_in_session` mutates only
            // `self.pending_table_modifications` (not the txn), so it is valid
            // after the park. Parking first guarantees the take-and-park
            // contract — if `note_copy_in_session` errors (row-count overflow),
            // the txn is already back in `self.txn_state` as `InTransaction`, so
            // `fail_if_in_transaction` transitions the block to `Failed(txn)`
            // instead of silently dropping the txn and leaving the session
            // `Idle`. Recording the table after the park does not change COMMIT's
            // deferred-FK + column-cache behaviour: the rows are already inserted
            // under `txn.xid`; the note just records the table for COMMIT.
            self.txn_state = crate::TxnState::InTransaction(txn);
            self.note_copy_in_session(&copy_table_key(entry), rows_inserted)
                .map_err(|e| self.fail_if_in_transaction(e))?;
        } else {
            self.finalise_copy_from_autocommit(
                txn,
                &copy_table_key(entry),
                rows_inserted,
                "COPY FROM autocommit",
            )?;
        }

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
        // Bound the cumulative stream size. Without this an authenticated
        // client (and under the default Trust policy, any client) can stream
        // `CopyData` frames indefinitely via `COPY t FROM STDIN WITH (FORMAT
        // binary)`, growing this Vec until the process is OOM-killed and every
        // session dies — a classic unbounded-buffer DoS. The binary *file*
        // path already enforces this 128 MiB ceiling; mirror it for STDIN.
        let limit = copy_binary_file_limit_bytes();
        let mut bytes = Vec::new();
        loop {
            match self.read_frontend().await? {
                FrontendMessage::CopyData(chunk) => {
                    check_copy_stdin_within_limit(bytes.len(), chunk.len(), limit)?;
                    bytes.extend_from_slice(&chunk);
                }
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

    pub(super) async fn copy_binary_bytes_into_table(
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
        let rows = copy_rows_from_usize(payloads.len(), "binary COPY FROM")?;
        // Shape A: ride the session txn in an explicit block, else autocommit.
        // The whole binary payload is decoded synchronously above (the wire
        // bytes were already collected), so no `.await` interleaves the insert.
        let session_mode = self.copy_in_session_txn();
        let txn = if session_mode {
            match std::mem::replace(&mut self.txn_state, crate::TxnState::Idle) {
                crate::TxnState::InTransaction(mut txn) => {
                    self.state.txn_manager.refresh_snapshot(&mut txn);
                    txn
                }
                other => {
                    self.txn_state = other;
                    return Err(ServerError::Unsupported(
                        "binary COPY FROM session txn vanished mid-dispatch",
                    ));
                }
            }
        } else {
            self.state.txn_manager.begin(IsolationLevel::ReadCommitted)
        };
        if let Err(e) = self.flush_copy_insert_batch(entry, &payloads, &txn, !session_mode) {
            return Err(self.fail_or_rollback_copy_from(
                session_mode,
                txn,
                e,
                "binary COPY FROM rollback after insert batch error",
            ));
        }
        if session_mode {
            // Park the session txn back FIRST, then note the table (see the text
            // STDIN path for the full rationale): `note_copy_in_session` touches
            // only `self.pending_table_modifications`, so it is valid after the
            // park, and an overflow error then routes through
            // `fail_if_in_transaction` (a real `Failed(txn)` transition) rather
            // than dropping the owned txn and leaving the session `Idle`.
            self.txn_state = crate::TxnState::InTransaction(txn);
            self.note_copy_in_session(&copy_table_key(entry), rows)
                .map_err(|e| self.fail_if_in_transaction(e))?;
        } else {
            // Autocommit binary COPY keeps its prior finalisation byte-for-byte
            // (no deferred-FK pre-validation — the text path differs, but
            // changing binary's behaviour is out of scope here).
            self.finalise_copy_from_commit(txn, rows, "binary COPY FROM")?;
            self.state.note_commit_for_gc();
            self.state
                .note_table_modifications(&copy_table_key(entry), rows);
        }
        self.send_copy_complete(rows, emit_ready_for_query).await
    }

    pub(super) async fn send_copy_complete(
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

    /// Insert one COPY batch under `txn`.
    ///
    /// `mark_all_visible` controls the visibility-map bulk-load optimisation:
    ///
    /// - Autocommit COPY (`true`): the implicit txn commits the moment the COPY
    ///   finishes, so freshly bulk-filled pages can be stamped all-visible — the
    ///   historical COPY fast path, preserved byte-for-byte.
    /// - In-session COPY (`false`): the rows are written under the still-open
    ///   session xid (InProgress). Marking the page all-visible would let a scan
    ///   skip the MVCC visibility check and SEE the uncommitted rows — and, worse,
    ///   keep seeing them after a ROLLBACK aborts the xid. So we pass `vm: None`:
    ///   the rows stay subject to normal MVCC visibility (own-write visible to
    ///   this txn, invisible to others, gone on ROLLBACK). COMMIT does not need
    ///   the all-visible bit; it is an optimisation a later VACUUM re-establishes.
    pub(in crate::session) fn flush_copy_insert_batch(
        &self,
        entry: &TableEntry,
        payloads: &[Vec<u8>],
        txn: &Transaction,
        mark_all_visible: bool,
    ) -> Result<(), ServerError> {
        if payloads.is_empty() {
            return Ok(());
        }
        let payload_refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let wal = self.state.heap.wal_sink().map(Arc::as_ref);
        let n_atts = u16::try_from(entry.schema.len())
            .map_err(|_| ServerError::ddl("COPY FROM schema column count exceeds u16"))?;
        let insert_opts = InsertOptions {
            xmin: txn.current_xid(),
            command_id: txn.current_command,
            n_atts,
            wal,
            fsm: None,
            vm: mark_all_visible.then(|| self.state.vm.as_ref()),
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
