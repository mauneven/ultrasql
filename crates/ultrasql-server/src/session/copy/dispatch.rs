//! Top-level `COPY` dispatch on the [`Session`] async I/O surface.
//!
//! Parses and binds the `COPY` statement, routes the bound plan to the
//! direction/source-specific handlers, and owns the autocommit transaction
//! finalisation and superuser gate for server-side file access.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::CatalogSnapshot;
use ultrasql_parser::Parser;
use ultrasql_planner::{
    CopyDirection, CopyFormat as PlanCopyFormat, CopySource, LogicalPlan, bind,
};
use ultrasql_txn::Transaction;

use super::super::Session;
use super::{CopyOptions, ServerCopyFormat, ServerError};
use crate::{CombinedCatalog, TxnState};

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

    /// Whether the COPY handler should run inside the open session
    /// transaction (Shape A) rather than its own autocommit txn.
    ///
    /// `true` only when `self.txn_state` is `InTransaction`. `Failed` is
    /// guarded earlier (25P02) and `Idle` is the autocommit path.
    pub(in crate::session) fn copy_in_session_txn(&self) -> bool {
        matches!(self.txn_state, TxnState::InTransaction(_))
    }

    /// Finalise a `COPY ... FROM` after a successful import.
    ///
    /// - Autocommit (`Idle`): today's path — validate deferred FKs against
    ///   the implicit txn, commit it, then note GC + table modifications so
    ///   autovacuum/columnar shadows stay in sync.
    /// - Session (`InTransaction`): do **not** commit and do **not** validate
    ///   deferred FKs here. COMMIT's `execute_commit` already validates the
    ///   deferred FKs (via `pending_table_modifications`) and rebuilds the
    ///   column cache for the COPY-touched table; route the table through
    ///   `pending_table_modifications` so that machinery covers it, and leave
    ///   the session txn intact in `self.txn_state`.
    ///
    /// `txn` is consumed by value in the autocommit branch; in the session
    /// branch the caller keeps ownership of the session txn, so this takes
    /// only the table key + row count.
    pub(in crate::session) fn finalise_copy_from_autocommit(
        &mut self,
        txn: Transaction,
        table_key: &str,
        rows_changed: u64,
        context: &'static str,
    ) -> Result<(), ServerError> {
        if rows_changed > 0
            && let Err(e) = self.state.validate_deferred_foreign_keys(&txn)
        {
            return Err(self.rollback_copy_transaction_after_error(
                txn,
                e,
                "COPY FROM autocommit rollback after deferred FK violation",
            ));
        }
        self.finalise_copy_from_commit(txn, rows_changed, context)?;
        self.state.note_commit_for_gc();
        self.state.note_table_modifications(table_key, rows_changed);
        self.plan_cache_invalidate();
        Ok(())
    }

    /// Run a `COPY ... TO` read body against the correct MVCC snapshot and
    /// finalise the read transaction per mode.
    ///
    /// - Session mode (`InTransaction`): refresh the session txn's snapshot
    ///   (advancing `current_command`, so the read sees prior in-txn writes
    ///   such as a just-issued `INSERT`), then scan against that snapshot. No
    ///   `begin`/`commit` — the session txn stays open and untouched. On a scan
    ///   error this function does NOT abort the session txn and does NOT itself
    ///   transition the block to `Failed`; it simply propagates the error to its
    ///   caller with the txn left in place. For the STDOUT path the
    ///   `CopyOutResponse` has already been written to the wire by the time the
    ///   body runs, so a mid-COPY-out error cannot be cleanly turned into an
    ///   in-band ErrorResponse + RFQ: it bubbles up out of `handle_copy_statement`
    ///   and `run()`, terminating the connection (the session txn then dies with
    ///   the connection — there is no surviving session to leave in a `Failed`
    ///   block). This is unlike the COPY-FROM write paths, which DO park the txn
    ///   back as `Failed` on error.
    /// - Autocommit mode (`Idle`): today's path — open an implicit
    ///   ReadCommitted read txn, scan against its snapshot, and commit it on
    ///   success or roll it back on error.
    ///
    /// `body` is purely synchronous (heap scan + encode), so the borrow of the
    /// session txn never crosses an `.await`.
    pub(in crate::session) fn with_copy_read_snapshot<T>(
        &mut self,
        commit_context: &'static str,
        rollback_context: &'static str,
        body: impl FnOnce(&Self, &ultrasql_mvcc::Snapshot) -> Result<T, ServerError>,
    ) -> Result<T, ServerError> {
        if matches!(self.txn_state, TxnState::InTransaction(_)) {
            // Refresh in place so `current_command` advances and the read sees
            // this session's own uncommitted writes.
            let snapshot = if let TxnState::InTransaction(txn) = &mut self.txn_state {
                self.state.txn_manager.refresh_snapshot(txn);
                txn.snapshot.clone()
            } else {
                unreachable!("guarded by matches! above")
            };
            // The error path must NOT abort the session txn and does NOT
            // transition the block to `Failed` here — it propagates out (the
            // CopyOut subprotocol is already on the wire, so the error bubbles
            // up out of `run()` and terminates the connection; see the doc
            // comment above). The session txn is left in place; it dies with the
            // connection.
            body(self, &snapshot)
        } else {
            let txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            let snapshot = txn.snapshot.clone();
            match body(self, &snapshot) {
                Ok(value) => {
                    self.finalise_read_transaction(txn, commit_context)?;
                    Ok(value)
                }
                Err(err) => {
                    Err(self.rollback_copy_transaction_after_error(txn, err, rollback_context))
                }
            }
        }
    }

    /// Record a successful in-session `COPY ... FROM` so the eventual
    /// COMMIT's deferred-FK validation and column-cache invalidation cover
    /// the COPY-touched table (R2). Mirrors the bookkeeping
    /// `note_dml_effect` performs for an in-txn INSERT, but COPY has no
    /// `LogicalPlan` here so the table key is threaded directly.
    pub(in crate::session) fn note_copy_in_session(
        &mut self,
        table_key: &str,
        rows_changed: u64,
    ) -> Result<(), ServerError> {
        if rows_changed == 0 {
            return Ok(());
        }
        let table = table_key.to_ascii_lowercase();
        let current = self
            .pending_table_modifications
            .get(&table)
            .copied()
            .unwrap_or(0);
        let total = current.checked_add(rows_changed).ok_or_else(|| {
            ServerError::Execute(ultrasql_executor::ExecError::NumericFieldOverflow(
                "COPY pending DML row count overflow".to_owned(),
            ))
        })?;
        self.pending_table_modifications.insert(table, total);
        Ok(())
    }

    pub(in crate::session) fn rollback_copy_transaction_after_error(
        &self,
        txn: Transaction,
        original: ServerError,
        context: &'static str,
    ) -> ServerError {
        self.rollback_transaction_after_error(txn, original, context)
    }

    /// Error finaliser for a `COPY ... FROM` import.
    ///
    /// - Session mode (`InTransaction`, txn taken out into a local): transition
    ///   the block to `Failed` so the next statement gets 25P02 and the user's
    ///   ROLLBACK discards every COPY row. Crucially we do **not** abort the
    ///   session txn here — the rows stay InProgress under the live session xid
    ///   and are discarded only when the user issues ROLLBACK (or COMMIT, which
    ///   a failed block treats as ROLLBACK). Returns the original error
    ///   verbatim; the caller still drains the wire and reports it.
    /// - Autocommit mode (`Idle`): today's path — roll back and abort the
    ///   single implicit txn so a mid-stream error leaves zero rows.
    ///
    /// `txn` is the owned transaction handle the caller has been threading.
    pub(in crate::session) fn fail_or_rollback_copy_from(
        &mut self,
        session_mode: bool,
        txn: Transaction,
        original: ServerError,
        autocommit_context: &'static str,
    ) -> ServerError {
        if session_mode {
            // The txn was taken out of `self.txn_state`; park it back as the
            // failed block. Equivalent to `fail_if_in_transaction` but the txn
            // is owned here rather than living in `self.txn_state`.
            self.txn_state = TxnState::Failed(txn);
            original
        } else {
            self.rollback_copy_transaction_after_error(txn, original, autocommit_context)
        }
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
        let catalog_snapshot: Arc<CatalogSnapshot> = self.effective_catalog_snapshot();
        let combined = CombinedCatalog {
            snapshot: &catalog_snapshot,
            fallback: &self.state.catalog,
            search_path: self.session_settings.get("search_path").map(String::as_str),
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

        // Failed-block guard (SQLSTATE 25P02). A statement issued while the
        // surrounding explicit transaction is aborted must be rejected before
        // it runs — for COPY this matters doubly because, pre-fix, COPY opened
        // its OWN autocommit txn and so durably committed rows *inside* an
        // aborted block. Reject here, before any wire negotiation or txn work,
        // exactly like the DML/SELECT path does in `execute_query`.
        if matches!(self.txn_state, TxnState::Failed(_)) {
            return Err(ServerError::TransactionAborted);
        }

        // Read-only transaction enforcement (SQLSTATE 25006): `COPY ... FROM`
        // writes rows into a table. `COPY ... TO` is a read and is allowed.
        // The error routes through `fail_if_in_transaction`, which aborts the
        // surrounding block like any other in-transaction failure.
        if matches!(direction, CopyDirection::From)
            && let TxnState::InTransaction(txn) = &self.txn_state
            && txn.read_only
        {
            return Err(ServerError::ReadOnlyTransaction("COPY"));
        }

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
        // Route through the overlay-aware snapshot so a COPY into a table this
        // session created earlier in the same open transaction resolves it
        // (self-yes); other sessions still read the unmodified committed
        // snapshot (others-no).
        let catalog_snapshot: Arc<CatalogSnapshot> = self.effective_catalog_snapshot();
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
                    self.ensure_copy_server_file_access()?;
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
                    self.ensure_copy_server_file_access()?;
                    self.copy_from_file(&entry, columns, schema, &opts, path, emit_ready_for_query)
                        .await
                }
                CopySource::Stdout => Err(ServerError::Unsupported("COPY FROM STDOUT is invalid")),
            },
        }
    }

    /// Gate for server-side file COPY (`COPY ... TO/FROM '<path>'`). These
    /// variants read or write files on the database host using the server
    /// process's own filesystem privileges, so — like PostgreSQL — they are
    /// restricted to superusers. Without this gate any role able to run COPY on
    /// a table could read arbitrary server-readable files (e.g. `COPY t FROM
    /// '/etc/passwd'`) or write attacker-controlled bytes into new files on the
    /// host. The STDIN/STDOUT variants stream over the client connection and are
    /// unaffected — they need only the table-level privileges enforced by the
    /// planner.
    pub(super) fn ensure_copy_server_file_access(&self) -> Result<(), ServerError> {
        if self.current_role_is_superuser() {
            Ok(())
        } else {
            Err(ServerError::InsufficientPrivilege(
                "permission denied for server-side file COPY: must be superuser \
                 (use COPY ... FROM/TO STDIN/STDOUT for client-side transfer)"
                    .to_owned(),
            ))
        }
    }
}
