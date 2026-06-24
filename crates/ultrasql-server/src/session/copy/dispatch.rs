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

    pub(in crate::session) fn rollback_copy_transaction_after_error(
        &self,
        txn: Transaction,
        original: ServerError,
        context: &'static str,
    ) -> ServerError {
        self.rollback_transaction_after_error(txn, original, context)
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
