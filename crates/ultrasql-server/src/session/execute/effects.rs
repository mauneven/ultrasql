//! DML effect tracking, command-tag parsing, logical-replication DDL, and backup/admin helpers.

use super::*;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn note_dml_effect(
        &mut self,
        plan: &LogicalPlan,
        rows: u64,
    ) -> Result<(), ServerError> {
        if rows == 0 {
            return Ok(());
        }
        let Some(table) = Self::dml_target_table(plan) else {
            return Ok(());
        };
        let table = table.to_ascii_lowercase();
        let current = self
            .pending_table_modifications
            .get(&table)
            .copied()
            .unwrap_or(0);
        let total = current.checked_add(rows).ok_or_else(|| {
            ServerError::Execute(ultrasql_executor::ExecError::NumericFieldOverflow(
                "pending DML row count overflow".to_owned(),
            ))
        })?;
        if self.state.logical_replication.has_publications()
            && let Some(kind) = Self::dml_change_kind(plan)
        {
            self.pending_logical_changes.push(PendingLogicalChange {
                table: table.clone(),
                kind,
                rows_affected: rows,
            });
        }
        self.pending_table_modifications.insert(table, total);
        Ok(())
    }

    pub(crate) fn flush_dirty_heap_pages_after_dml_if_needed(
        &self,
        plan: &LogicalPlan,
        rows: u64,
    ) -> Result<(), ServerError> {
        if rows > 0 && matches!(plan, LogicalPlan::Insert { .. }) {
            self.state.flush_dirty_heap_pages_if_needed()?;
        }
        Ok(())
    }

    pub(crate) fn parse_affected_rows_tag(messages: &[BackendMessage]) -> u64 {
        let Some(BackendMessage::CommandComplete { tag }) = messages
            .iter()
            .find(|m| matches!(m, BackendMessage::CommandComplete { .. }))
        else {
            return 0;
        };
        let mut parts = tag.split_whitespace();
        let Some(cmd) = parts.next() else {
            return 0;
        };
        if !matches!(cmd, "INSERT" | "UPDATE" | "DELETE" | "MERGE") {
            return 0;
        }
        // INSERT tag shape is `INSERT 0 <rows>`, UPDATE/DELETE/MERGE is
        // `<CMD> <rows>`.
        let last = parts.next_back().unwrap_or_default();
        last.parse::<u64>().unwrap_or(0)
    }

    pub(crate) fn parse_command_rows_tag(messages: &[BackendMessage]) -> u64 {
        let Some(BackendMessage::CommandComplete { tag }) = messages
            .iter()
            .find(|m| matches!(m, BackendMessage::CommandComplete { .. }))
        else {
            return 0;
        };
        tag.split_whitespace()
            .next_back()
            .and_then(|rows| rows.parse::<u64>().ok())
            .unwrap_or(0)
    }

    pub(crate) fn note_committed_dml_effect(
        &self,
        plan: &LogicalPlan,
        rows: u64,
    ) -> Result<(), ServerError> {
        if rows == 0 {
            return Ok(());
        }
        let Some(table) = Self::dml_target_table(plan) else {
            return Ok(());
        };
        let logical_result = if let Some(kind) = Self::dml_change_kind(plan) {
            self.state
                .logical_replication
                .record_committed_dml(table, kind, rows)
        } else {
            Ok(())
        };
        self.state.note_table_modifications(table, rows);
        logical_result
    }

    pub(crate) fn flush_pending_dml_effects(&mut self) -> Result<(), ServerError> {
        let logical = std::mem::take(&mut self.pending_logical_changes);
        let mut first_error = None;
        for change in logical {
            if let Err(err) = self.state.logical_replication.record_committed_dml(
                &change.table,
                change.kind,
                change.rows_affected,
            ) && first_error.is_none()
            {
                first_error = Some(err);
            }
        }
        let drained = std::mem::take(&mut self.pending_table_modifications);
        for (table, rows) in drained {
            self.state.note_table_modifications(&table, rows);
        }
        if let Some(err) = first_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    pub(crate) fn flush_pending_materialized_view_rows(&mut self) -> Result<(), ServerError> {
        let drained = std::mem::take(&mut self.pending_materialized_view_rows);
        for (view, rows) in drained {
            if rows == 0 {
                continue;
            }
            let previous = view
                .materialized_rows
                .fetch_update(
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                    |current| current.checked_add(rows),
                )
                .map_err(|_| materialized_view_row_count_overflow())?;
            let total = checked_materialized_view_row_add(previous, rows)?;
            if let Err(err) = self
                .state
                .persist_materialized_view_runtime_metadata(&view, total)
            {
                tracing::warn!(
                    error = %err,
                    view = %view.view_table,
                    "persist materialized-view runtime metadata failed",
                );
            }
            self.state.note_table_modifications(&view.view_table, rows);
        }
        Ok(())
    }

    pub(crate) fn run_post_response_maintenance(&mut self) {
        if self.pending_post_commit_maintenance {
            self.pending_post_commit_maintenance = false;
            self.state.note_commit_for_gc();
        }
        if let Err(err) = self.flush_pending_dml_effects() {
            tracing::warn!(
                error = %err,
                "post-response DML effect finalization failed",
            );
        }
    }

    pub(crate) fn clear_pending_dml_effects(&mut self) {
        self.pending_table_modifications.clear();
        self.pending_logical_changes.clear();
        self.pending_materialized_view_rows.clear();
    }

    pub(crate) fn dml_target_table(plan: &LogicalPlan) -> Option<&str> {
        match plan {
            LogicalPlan::Insert { table, .. }
            | LogicalPlan::Update { table, .. }
            | LogicalPlan::Delete { table, .. }
            | LogicalPlan::Merge { target: table, .. } => Some(table.as_str()),
            _ => None,
        }
    }

    pub(crate) fn dml_change_kind(plan: &LogicalPlan) -> Option<LogicalChangeKind> {
        match plan {
            LogicalPlan::Insert { .. } => Some(LogicalChangeKind::Insert),
            LogicalPlan::Update { .. } => Some(LogicalChangeKind::Update),
            LogicalPlan::Delete { .. } => Some(LogicalChangeKind::Delete),
            _ => None,
        }
    }

    /// If the session is currently `InTransaction`, transition to
    /// `Failed` so subsequent statements get the `25P02` rejection
    /// until COMMIT/ROLLBACK. This mirrors PostgreSQL: any failure
    /// inside a transaction block — including parser errors, bind
    /// errors, executor errors, and DDL-inside-tx rejections —
    /// aborts the block.
    ///
    /// Statements outside a transaction (Idle) and statements while
    /// already in a Failed block leave the state unchanged.
    ///
    /// Returns the original error verbatim so callers can `return`
    /// with a single line.
    pub(crate) fn fail_if_in_transaction(&mut self, err: ServerError) -> ServerError {
        if matches!(self.txn_state, TxnState::InTransaction(_)) {
            // Replace+match avoids needing to clone the Transaction
            // handle out of the variant.
            let prev = std::mem::replace(&mut self.txn_state, TxnState::Idle);
            if let TxnState::InTransaction(txn) = prev {
                self.txn_state = TxnState::Failed(txn);
            }
        }
        err
    }

    pub(crate) fn try_execute_logical_replication_ddl(
        &self,
        trimmed_sql: &str,
    ) -> Result<Option<SelectResult>, ServerError> {
        let Some(ddl) = Self::try_parse_logical_replication_ddl(trimmed_sql)? else {
            return Ok(None);
        };
        match ddl {
            LogicalReplicationDdl::CreatePublication { name, tables } => {
                self.state
                    .logical_replication
                    .create_publication(&name, tables)?;
                Ok(Some(run_ddl_command("CREATE PUBLICATION")))
            }
            LogicalReplicationDdl::DropPublication { name, if_exists } => {
                let dropped = self.state.logical_replication.drop_publication(&name)?;
                if !dropped && !if_exists {
                    return Err(ServerError::ddl(format!(
                        "publication \"{}\" does not exist",
                        name.to_ascii_lowercase()
                    )));
                }
                Ok(Some(run_ddl_command("DROP PUBLICATION")))
            }
            LogicalReplicationDdl::CreateSubscription {
                name,
                conninfo,
                publications,
                slot_name,
            } => {
                self.state.logical_replication.create_subscription(
                    &name,
                    &conninfo,
                    publications,
                    slot_name,
                )?;
                Ok(Some(run_ddl_command("CREATE SUBSCRIPTION")))
            }
            LogicalReplicationDdl::DropSubscription { name, if_exists } => {
                let dropped = self.state.logical_replication.drop_subscription(&name)?;
                if !dropped && !if_exists {
                    return Err(ServerError::ddl(format!(
                        "subscription \"{}\" does not exist",
                        name.to_ascii_lowercase()
                    )));
                }
                Ok(Some(run_ddl_command("DROP SUBSCRIPTION")))
            }
        }
    }

    pub(crate) fn try_parse_logical_replication_ddl(
        trimmed_sql: &str,
    ) -> Result<Option<LogicalReplicationDdl>, ServerError> {
        let sql = trimmed_sql.trim().trim_end_matches(';').trim();
        if starts_with_keyword_pair(sql, "CREATE", "PUBLICATION") {
            let rest = sql["CREATE PUBLICATION".len()..].trim();
            let (name, after_name) = split_first_token(rest)?;
            let tables = parse_publication_tables(after_name)?;
            return Ok(Some(LogicalReplicationDdl::CreatePublication {
                name: name.to_string(),
                tables,
            }));
        }
        if starts_with_keyword_pair(sql, "DROP", "PUBLICATION") {
            let rest = sql["DROP PUBLICATION".len()..].trim();
            let (if_exists, rest) = if rest
                .get(.."IF EXISTS".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("IF EXISTS"))
            {
                (true, rest["IF EXISTS".len()..].trim())
            } else {
                (false, rest)
            };
            let (name, _) = split_first_token(rest)?;
            return Ok(Some(LogicalReplicationDdl::DropPublication {
                name: name.to_string(),
                if_exists,
            }));
        }
        if starts_with_keyword_pair(sql, "CREATE", "SUBSCRIPTION") {
            let rest = sql["CREATE SUBSCRIPTION".len()..].trim();
            return Ok(Some(parse_create_subscription(rest)?));
        }
        if starts_with_keyword_pair(sql, "DROP", "SUBSCRIPTION") {
            let rest = sql["DROP SUBSCRIPTION".len()..].trim();
            let (if_exists, rest) = if rest
                .get(.."IF EXISTS".len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("IF EXISTS"))
            {
                (true, rest["IF EXISTS".len()..].trim())
            } else {
                (false, rest)
            };
            let (name, _) = split_first_token(rest)?;
            return Ok(Some(LogicalReplicationDdl::DropSubscription {
                name: name.to_string(),
                if_exists,
            }));
        }
        Ok(None)
    }

    pub(crate) fn try_parse_backup_function(trimmed_sql: &str) -> Option<&'static str> {
        let normalized = trimmed_sql
            .trim_end_matches(';')
            .trim()
            .to_ascii_lowercase();
        if normalized.starts_with("select pg_start_backup(")
            || normalized.starts_with("select pg_backup_start(")
        {
            return Some("pg_start_backup");
        }
        if normalized == "select pg_stop_backup()"
            || normalized == "select pg_backup_stop()"
            || normalized.starts_with("select pg_stop_backup(")
            || normalized.starts_with("select pg_backup_stop(")
        {
            return Some("pg_stop_backup");
        }
        None
    }

    pub(crate) fn execute_backup_function(
        &self,
        function_name: &'static str,
    ) -> Result<SelectResult, ServerError> {
        let lsn = self.state.record_backup_marker(function_name)?;
        Ok(Self::single_text_select(function_name, &lsn))
    }

    pub(crate) fn single_text_select(name: &str, value: &str) -> SelectResult {
        SelectResult {
            messages: vec![
                BackendMessage::RowDescription {
                    fields: vec![FieldDescription {
                        name: name.to_owned(),
                        table_oid: 0,
                        col_attnum: 0,
                        type_oid: 25,
                        type_size: -1,
                        type_modifier: -1,
                        format_code: 0,
                    }],
                },
                BackendMessage::DataRow {
                    columns: vec![Some(value.as_bytes().to_vec())],
                },
                BackendMessage::CommandComplete {
                    tag: "SELECT 1".to_owned(),
                },
            ],
            streamed_body: None,
            shared_streamed_body: None,
            streaming: None,
            rows: 1,
        }
    }

    pub(crate) fn hot_standby_allows(trimmed_sql: &str) -> bool {
        let normalized = trimmed_sql.trim();
        if normalized.is_empty() {
            return true;
        }
        let upper = normalized.to_ascii_uppercase();
        upper.starts_with("SELECT")
            || upper.starts_with("SHOW")
            || upper.starts_with("EXPLAIN")
            || upper.starts_with("WITH")
            || upper.starts_with("VALUES")
            || (upper.starts_with("COPY") && upper.contains(" TO "))
    }
}
