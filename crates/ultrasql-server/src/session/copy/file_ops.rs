//! Server-side file `COPY` and `COPY (query) TO ...` execution on the
//! [`Session`].
//!
//! Handles `COPY ... FROM/TO '<path>'` for text, CSV, and binary formats:
//! the streaming text-file importer with reject-row handling, the table
//! encoders for file output, and lowering a `COPY (SELECT ...)` source into a
//! result set that is then encoded to STDOUT or a file.

use std::io::BufReader;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use ultrasql_catalog::{TableEntry, table_lookup_key};
use ultrasql_core::csv::sniff_csv_text;
use ultrasql_core::{RelationId, Schema, Value};
use ultrasql_executor::RowCodec;
use ultrasql_planner::{CopySource, LogicalPlan};
use ultrasql_protocol::{BackendMessage, encode_backend};
use ultrasql_txn::IsolationLevel;

use super::super::Session;
use super::binary::{append_binary_copy_header, append_binary_copy_row, append_i16_be};
use super::decode::{
    copy_cells_from_row_with_options, copy_rows_from_select_result, decode_one_copy_row,
};
use super::fs_io::{
    copy_format_code, csv_record_complete, open_copy_input_file, projected_schema,
    read_copy_file_sample, read_copy_input_file, validate_copy_reject_table,
    write_copy_output_file,
};
use super::{
    COPY_INSERT_BATCH_ROWS, CopyInsertTxn, CopyOptions, CopyRejectState, CopyRejectTarget,
    CopyRowDecodeContext, CopyTextFileStreamArgs, ServerCopyFormat, ServerError,
    add_copy_batch_rows, copy_add_row_counts, copy_out_response_with_format, copy_table_key,
    encode_csv_row, encode_text_row, increment_copy_rows,
};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(super) async fn copy_from_file(
        &mut self,
        entry: &TableEntry,
        columns: &[usize],
        schema: &Schema,
        opts: &CopyOptions,
        path: &str,
        emit_ready_for_query: bool,
    ) -> Result<(), ServerError> {
        if opts.format == ServerCopyFormat::Parquet {
            // `copy_from_parquet_file` owns the session-vs-autocommit decision
            // (it rides the session txn inside an explicit block). In autocommit
            // it commits the implicit txn, so the post-commit GC + table-mod +
            // plan-cache notes belong here. In session mode the rows are still
            // uncommitted (tracked via `pending_table_modifications`); COMMIT
            // emits those effects, so skip them now.
            let session_mode = self.copy_in_session_txn();
            let rows = self.copy_from_parquet_file(entry, columns, schema, path)?;
            if !session_mode {
                self.state.note_commit_for_gc();
                self.state
                    .note_table_modifications(&copy_table_key(entry), rows);
                self.plan_cache_invalidate();
            }
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
        let codec = RowCodec::new(entry.schema.clone());
        let mut payload_batch: Vec<Vec<u8>> = Vec::with_capacity(COPY_INSERT_BATCH_ROWS);
        // Resolve the reject state BEFORE taking the session txn out. This call
        // is fallible (a missing or invalid REJECT_TABLE errors), and it does
        // not depend on the txn — building it here keeps the only fallible work
        // between the take-out and the park-back inside the
        // `fail_or_rollback_copy_from`-wrapped stream call below, so no bare `?`
        // can drop the owned txn and leave `self.txn_state = Idle`.
        let mut reject_state = self.copy_reject_state(&effective_opts)?;
        // Shape A: ride the session txn in an explicit block, else autocommit.
        // The whole file is read + inserted synchronously below (no `.await`
        // between the borrow of `&txn` and the inserts), so the session txn
        // taken out here never has a borrow cross an await.
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
                        "COPY FROM file session txn vanished mid-dispatch",
                    ));
                }
            }
        } else {
            self.state.txn_manager.begin(IsolationLevel::ReadCommitted)
        };

        let stream_result = self.copy_text_file_stream_into_table(CopyTextFileStreamArgs {
            entry,
            columns,
            schema,
            opts: &effective_opts,
            codec: &codec,
            txn: &txn,
            reader: &mut reader,
            payload_batch: &mut payload_batch,
            reject_state: reject_state.as_mut(),
            path,
            mark_all_visible: !session_mode,
        });
        let rows = match stream_result {
            Ok(rows) => rows,
            Err(err) => {
                return Err(self.fail_or_rollback_copy_from(
                    session_mode,
                    txn,
                    err,
                    "COPY FROM file rollback after import error",
                ));
            }
        };
        let reject_rows = reject_state
            .as_ref()
            .and_then(|state| state.target.as_ref())
            .map_or(0, |target| target.rows);
        if session_mode {
            // The COPY target and any REJECT_TABLE target both rode `&txn`, so
            // both are part of the session transaction. Route each through
            // `pending_table_modifications` so COMMIT's deferred-FK validation
            // + column-cache invalidation cover them.
            //
            // PARK FIRST, then note both tables: `note_copy_in_session` mutates
            // only `self.pending_table_modifications` (valid after the park),
            // and `copy_add_row_counts`/`rows_changed` is consumed solely by the
            // autocommit branch, so it stays there. Parking before the notes
            // honours the take-and-park contract — if either note errors
            // (row-count overflow), the txn is already back in `self.txn_state`,
            // so `fail_if_in_transaction` transitions the block to `Failed(txn)`
            // rather than dropping the owned txn and leaving the session `Idle`.
            self.txn_state = crate::TxnState::InTransaction(txn);
            self.note_copy_in_session(&copy_table_key(entry), rows)
                .map_err(|e| self.fail_if_in_transaction(e))?;
            if let Some(reject_target) = reject_state
                .as_ref()
                .and_then(|state| state.target.as_ref())
                && reject_target.rows > 0
            {
                self.note_copy_in_session(
                    &copy_table_key(&reject_target.entry),
                    reject_target.rows,
                )
                .map_err(|e| self.fail_if_in_transaction(e))?;
            }
        } else {
            // Autocommit file COPY keeps its prior finalisation byte-for-byte:
            // commit on `rows_changed` (main + reject), but note only the main
            // table's `rows` and the reject target's rows separately, with no
            // deferred-FK pre-validation (the file path never had one).
            let rows_changed = copy_add_row_counts(rows, reject_rows, "COPY FROM file")?;
            self.finalise_copy_from_commit(txn, rows_changed, "COPY FROM file")?;
            self.state.note_commit_for_gc();
            self.state
                .note_table_modifications(&copy_table_key(entry), rows);
            if let Some(reject_target) = reject_state.and_then(|state| state.target) {
                if reject_target.rows > 0 {
                    self.state.note_table_modifications(
                        &copy_table_key(&reject_target.entry),
                        reject_target.rows,
                    );
                }
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
            let entry = self.copy_reject_table_entry(table_name)?;
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

    fn copy_reject_table_entry(&self, table_name: &str) -> Result<TableEntry, ServerError> {
        // Overlay-aware so a REJECT_TABLE target this session created earlier in
        // its open transaction resolves the same way the COPY target does.
        let catalog_snapshot = self.effective_catalog_snapshot();
        let folded = table_name.to_ascii_lowercase();
        match crate::parse_pg_identifier_path(&folded).as_deref() {
            Some([relation_name]) => {
                for namespace in crate::search_path_schema_names(
                    self.session_settings.get("search_path").map(String::as_str),
                ) {
                    let key = table_lookup_key(&namespace, relation_name);
                    if let Some(entry) = catalog_snapshot.tables.get(&key) {
                        return Ok(entry.clone());
                    }
                }
            }
            Some([namespace, relation_name]) => {
                let key = table_lookup_key(namespace, relation_name);
                if let Some(entry) = catalog_snapshot.tables.get(&key) {
                    return Ok(entry.clone());
                }
            }
            _ => {}
        }

        Err(ServerError::CopyFormat(format!(
            "COPY reject_table not found: {table_name}"
        )))
    }

    fn record_copy_reject(
        &self,
        state: &mut CopyRejectState,
        path: &str,
        line_number: u64,
        raw_record: &[u8],
        err: &ServerError,
        insert: CopyInsertTxn<'_>,
    ) -> Result<(), ServerError> {
        let next_bad_rows = copy_add_row_counts(state.bad_rows, 1, "COPY reject rows")?;
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
        increment_copy_rows(&mut target.rows, "COPY reject target")?;
        if target.payload_batch.len() == COPY_INSERT_BATCH_ROWS {
            self.flush_copy_reject_batch(target, insert)?;
        }
        Ok(())
    }

    fn flush_copy_reject_batch(
        &self,
        target: &mut CopyRejectTarget,
        insert: CopyInsertTxn<'_>,
    ) -> Result<(), ServerError> {
        if target.payload_batch.is_empty() {
            return Ok(());
        }
        self.flush_copy_insert_batch(
            &target.entry,
            &target.payload_batch,
            insert.txn,
            insert.mark_all_visible,
        )?;
        target.payload_batch.clear();
        Ok(())
    }

    fn copy_text_file_stream_into_table(
        &self,
        args: CopyTextFileStreamArgs<'_>,
    ) -> Result<u64, ServerError> {
        let CopyTextFileStreamArgs {
            entry,
            columns,
            schema,
            opts,
            codec,
            txn,
            reader,
            payload_batch,
            mut reject_state,
            path,
            mark_all_visible,
        } = args;
        let insert = CopyInsertTxn {
            txn,
            mark_all_visible,
        };
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
                    opts,
                    CopyRowDecodeContext {
                        entry,
                        columns,
                        schema,
                        codec,
                        jsonb_shape_cache: &mut jsonb_shape_cache,
                    },
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
                            insert,
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
                add_copy_batch_rows(&mut rows_inserted, payload_batch.len(), "COPY FROM file")?;
                self.flush_copy_insert_batch(entry, payload_batch, txn, mark_all_visible)?;
                payload_batch.clear();
            }
        }

        if !record.is_empty() {
            if opts.format == ServerCopyFormat::Csv && !csv_record_complete(&record, opts)? {
                let err =
                    ServerError::CopyFormat("unterminated quoted field in CSV input".to_string());
                if let Some(state) = reject_state.as_deref_mut() {
                    self.record_copy_reject(state, path, record_start_line, &record, &err, insert)?;
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
                        opts,
                        CopyRowDecodeContext {
                            entry,
                            columns,
                            schema,
                            codec,
                            jsonb_shape_cache: &mut jsonb_shape_cache,
                        },
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
                                insert,
                            )?;
                            record.clear();
                            return self.finish_copy_stream_batches(
                                entry,
                                payload_batch,
                                rows_inserted,
                                reject_state,
                                insert,
                            );
                        }
                        return Err(err);
                    }
                };
                payload_batch.push(payload);
            }
        }

        self.finish_copy_stream_batches(entry, payload_batch, rows_inserted, reject_state, insert)
    }

    fn finish_copy_stream_batches(
        &self,
        entry: &TableEntry,
        payload_batch: &mut Vec<Vec<u8>>,
        mut rows_inserted: u64,
        reject_state: Option<&mut CopyRejectState>,
        insert: CopyInsertTxn<'_>,
    ) -> Result<u64, ServerError> {
        if !payload_batch.is_empty() {
            add_copy_batch_rows(&mut rows_inserted, payload_batch.len(), "COPY FROM file")?;
            self.flush_copy_insert_batch(
                entry,
                payload_batch,
                insert.txn,
                insert.mark_all_visible,
            )?;
            payload_batch.clear();
        }
        if let Some(state) = reject_state {
            if let Some(target) = state.target.as_mut() {
                self.flush_copy_reject_batch(target, insert)?;
            }
        }
        Ok(rows_inserted)
    }

    pub(super) fn copy_to_file(
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
        // `COPY ... TO '<path>'` is a read: in an explicit transaction it scans
        // the session txn's (command-advanced) snapshot so it sees this
        // session's own uncommitted writes, without begin/commit; in autocommit
        // it runs today's implicit read txn. Borrow is synchronous (no await).
        let (bytes, rows) = self.with_copy_read_snapshot(
            "COPY TO file scan commit",
            "COPY TO file rollback after scan error",
            |session, snapshot| {
                if opts.format == ServerCopyFormat::Binary {
                    session.encode_table_binary_copy(entry, columns, schema, snapshot)
                } else {
                    session.encode_table_textual_copy(entry, columns, opts, snapshot)
                }
            },
        )?;
        write_copy_output_file(path, &bytes)?;
        Ok(rows)
    }

    pub(super) async fn copy_query_to_destination(
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
        // Resolve through the per-txn catalog overlay so a `COPY (SELECT …
        // FROM t) TO …` over an in-txn-created (or in-txn-altered) relation `t`
        // sees the issuing session's own uncommitted schema rather than 42P01.
        // Autocommit sessions (no overlay) get the committed snapshot
        // unchanged.
        let snapshot = self.effective_catalog_snapshot();
        // Shape A: in an explicit transaction block the query reads against the
        // SESSION txn (so it sees this session's own in-txn writes) and never
        // commits/aborts it; in autocommit it opens today's implicit read txn.
        // The whole lower+execute is synchronous below, so the session-txn
        // borrow never crosses the trailing `.await` wire writes.
        let session_mode = matches!(self.txn_state, crate::TxnState::InTransaction(_));
        if session_mode && let crate::TxnState::InTransaction(txn) = &mut self.txn_state {
            self.state.txn_manager.refresh_snapshot(txn);
        }
        let read_txn = if session_mode {
            None
        } else {
            Some(self.state.txn_manager.begin(IsolationLevel::ReadCommitted))
        };
        // Borrow the read parameters (snapshot / xid / command / isolation) from
        // whichever txn governs this read.
        let (mvcc_snapshot, read_isolation, read_xid, read_command) = match &read_txn {
            Some(txn) => (
                txn.snapshot.clone(),
                txn.isolation,
                txn.current_xid(),
                txn.current_command,
            ),
            None => {
                let crate::TxnState::InTransaction(txn) = &self.txn_state else {
                    return Err(ServerError::Unsupported(
                        "COPY query session txn vanished mid-dispatch",
                    ));
                };
                (
                    txn.snapshot.clone(),
                    txn.isolation,
                    txn.current_xid(),
                    txn.current_command,
                )
            }
        };
        let ctx = crate::pipeline::LowerCtx {
            tables: &self.state.tables,
            catalog_snapshot: Arc::clone(&snapshot),
            table_constraints: Arc::clone(&self.state.table_constraints),
            sequences: Arc::clone(&self.state.sequences),
            sequence_owners: Arc::clone(&self.state.sequence_owners),
            sequence_namespaces: Arc::clone(&self.state.sequence_namespaces),
            schemas: Arc::clone(&self.state.schemas),
            operators: Arc::clone(&self.state.operators),
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
            snapshot: mvcc_snapshot,
            isolation: read_isolation,
            oracle: Arc::clone(&self.state.txn_manager),
            xid: read_xid,
            lock_xid: read_xid,
            command_id: read_command,
            cte_buffers: std::collections::HashMap::new(),
            jit: self.jit_config(),
            cancel_flag: Some(self.cancel_flag.clone()),
            work_mem: self.work_mem_budget(),
            profile_operators: false,
            allow_server_files: self.current_role_is_superuser(),
        };
        let text_options = crate::result_encoder::TextEncodingOptions::from_session_settings(
            ctx.session_settings.as_ref(),
        );
        let result = match crate::pipeline::lower_query(input, &ctx).and_then(|mut op| {
            crate::result_encoder::run_select_with_options(op.as_mut(), &text_options)
        }) {
            Ok(result) => result,
            Err(e) => {
                // Session mode: a read error must NOT abort the session txn —
                // propagate, and the dispatcher transitions the block to Failed
                // via `fail_if_in_transaction`. Autocommit: roll back the
                // implicit read txn as before.
                return match read_txn {
                    Some(txn) => Err(self.rollback_copy_transaction_after_error(
                        txn,
                        e,
                        "COPY query rollback after execution error",
                    )),
                    None => Err(e),
                };
            }
        };
        // Commit only the implicit autocommit read txn; the session txn stays
        // open and untouched.
        if let Some(txn) = read_txn {
            self.finalise_read_transaction(txn, "COPY query transaction commit")?;
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
                self.ensure_copy_server_file_access()?;
                write_copy_output_file(path, &payload)?;
                self.send_copy_complete(rows, emit_ready_for_query).await
            }
            CopySource::Stdin => Err(ServerError::Unsupported(
                "COPY query target cannot use STDIN",
            )),
        }
    }

    /// Encode the table to text/CSV bytes by scanning under `snapshot`.
    ///
    /// The transaction lifecycle (session-vs-autocommit begin/commit) is owned
    /// by the caller via [`Session::with_copy_read_snapshot`]; this function is
    /// a pure synchronous read against the supplied MVCC snapshot.
    fn encode_table_textual_copy(
        &self,
        entry: &TableEntry,
        columns: &[usize],
        opts: &CopyOptions,
        snapshot: &ultrasql_mvcc::Snapshot,
    ) -> Result<(Vec<u8>, u64), ServerError> {
        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let codec = RowCodec::new(entry.schema.clone());
        let mut out = Vec::new();
        let stream_schema = projected_schema(entry, columns)?;
        let text_options = crate::result_encoder::TextEncodingOptions::from_session_settings(
            &self.session_settings,
        );
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
            snapshot,
            self.state.txn_manager.as_ref(),
        );
        for tuple in scan {
            let tuple =
                tuple.map_err(|e| ServerError::ddl(format!("COPY TO file heap scan: {e}")))?;
            let row = codec
                .decode(&tuple.data)
                .map_err(|e| ServerError::CopyFormat(format!("COPY TO file row decode: {e}")))?;
            let cells =
                copy_cells_from_row_with_options(&row, &entry.schema, columns, &text_options);
            match opts.format {
                ServerCopyFormat::Text => out.extend_from_slice(&encode_text_row(&cells, opts)),
                ServerCopyFormat::Csv => out.extend_from_slice(&encode_csv_row(&cells, opts)),
                ServerCopyFormat::Binary | ServerCopyFormat::Parquet => {}
            }
            increment_copy_rows(&mut rows, "COPY TO file")?;
        }
        Ok((out, rows))
    }

    /// Encode the table to the binary `PGCOPY` wire format by scanning under
    /// `snapshot`. Like the textual encoder, the txn lifecycle is the caller's.
    pub(super) fn encode_table_binary_copy(
        &self,
        entry: &TableEntry,
        columns: &[usize],
        schema: &Schema,
        snapshot: &ultrasql_mvcc::Snapshot,
    ) -> Result<(Vec<u8>, u64), ServerError> {
        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let codec = RowCodec::new(entry.schema.clone());
        let mut out = Vec::new();
        append_binary_copy_header(&mut out);
        let mut rows = 0_u64;
        let scan = self.state.heap.scan_visible(
            rel,
            block_count,
            snapshot,
            self.state.txn_manager.as_ref(),
        );
        for tuple in scan {
            let tuple =
                tuple.map_err(|e| ServerError::ddl(format!("binary COPY heap scan: {e}")))?;
            let row = codec
                .decode(&tuple.data)
                .map_err(|e| ServerError::CopyFormat(format!("binary COPY row decode: {e}")))?;
            append_binary_copy_row(&mut out, &row, &entry.schema, columns, schema)?;
            increment_copy_rows(&mut rows, "binary COPY TO")?;
        }
        append_i16_be(&mut out, -1);
        Ok((out, rows))
    }
}
