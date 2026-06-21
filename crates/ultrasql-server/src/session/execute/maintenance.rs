//! ANALYZE / VACUUM / CREATE STATISTICS parsing and execution.

use super::*;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn try_parse_analyze_target(&self, trimmed_sql: &str) -> Option<Option<String>> {
        if trimmed_sql.len() < "analyze".len() || !trimmed_sql[..7].eq_ignore_ascii_case("analyze")
        {
            return None;
        }
        let rest = trimmed_sql[7..].trim();
        if rest.is_empty() || rest == ";" {
            return Some(None);
        }
        let ident = rest.trim_end_matches(';').trim();
        if ident.is_empty() {
            return Some(None);
        }
        // v0.6: support `ANALYZE` and `ANALYZE table_name`.
        if ident.split_whitespace().count() == 1 {
            return Some(Some(ident.trim_matches('"').to_ascii_lowercase()));
        }
        None
    }

    pub(crate) fn try_parse_vacuum_target(&self, trimmed_sql: &str) -> Option<Option<String>> {
        if trimmed_sql.len() < "vacuum".len() || !trimmed_sql[..6].eq_ignore_ascii_case("vacuum") {
            return None;
        }
        let rest = trimmed_sql[6..].trim();
        if rest.is_empty() || rest == ";" {
            return Some(None);
        }
        let ident = rest.trim_end_matches(';').trim();
        if ident.is_empty() {
            return Some(None);
        }
        if ident.split_whitespace().count() == 1 {
            return Some(Some(ident.trim_matches('"').to_ascii_lowercase()));
        }
        None
    }

    pub(crate) fn try_parse_create_statistics(
        trimmed_sql: &str,
    ) -> Result<Option<CreateStatisticsSpec>, ServerError> {
        let head = "create statistics";
        if trimmed_sql.len() < head.len() || !trimmed_sql[..head.len()].eq_ignore_ascii_case(head) {
            return Ok(None);
        }
        let rest = trimmed_sql[head.len()..].trim();
        let rest = rest.strip_suffix(';').unwrap_or(rest).trim();
        if rest.is_empty() {
            return Err(ServerError::ddl("malformed CREATE STATISTICS"));
        }
        let normalized = rest.replace(',', " , ");
        let tokens: Vec<&str> = normalized.split_whitespace().collect();
        if tokens.len() < 5 || !tokens[1].eq_ignore_ascii_case("on") {
            return Err(ServerError::ddl("malformed CREATE STATISTICS"));
        }
        let mut columns = Vec::new();
        let mut idx = 2;
        while idx < tokens.len() && !tokens[idx].eq_ignore_ascii_case("from") {
            if tokens[idx] != "," {
                columns.push(Self::fold_statistics_identifier(tokens[idx]));
            }
            idx += 1;
        }
        if columns.is_empty()
            || idx + 2 != tokens.len()
            || !tokens[idx].eq_ignore_ascii_case("from")
        {
            return Err(ServerError::ddl("malformed CREATE STATISTICS"));
        }
        Ok(Some(CreateStatisticsSpec {
            name: Self::fold_statistics_identifier(tokens[0]),
            table: Self::fold_statistics_identifier(tokens[idx + 1]),
            columns,
        }))
    }

    fn fold_statistics_identifier(ident: &str) -> String {
        ident.trim_matches('"').to_ascii_lowercase()
    }

    pub(crate) fn execute_create_statistics(
        &mut self,
        snapshot: &CatalogSnapshot,
        spec: CreateStatisticsSpec,
    ) -> Result<SelectResult, ServerError> {
        let table = snapshot.tables.get(&spec.table).ok_or_else(|| {
            self.fail_if_in_transaction(ServerError::Plan(
                ultrasql_planner::PlanError::TableNotFound(spec.table.clone()),
            ))
        })?;
        let mut stxkeys = Vec::with_capacity(spec.columns.len());
        for column in &spec.columns {
            let position = table
                .schema
                .fields()
                .iter()
                .position(|field| field.name.eq_ignore_ascii_case(column))
                .ok_or_else(|| {
                    self.fail_if_in_transaction(ServerError::Plan(
                        ultrasql_planner::PlanError::ColumnNotFound(column.clone()),
                    ))
                })?;
            stxkeys.push(
                i16::try_from(position.saturating_add(1)).map_err(|_| {
                    ServerError::ddl("CREATE STATISTICS table has too many columns")
                })?,
            );
        }
        let row = StatisticExtRow {
            oid: self.state.persistent_catalog.next_oid(),
            stxname: spec.name,
            stxrelid: table.oid,
            stxkeys,
            stxkind: vec!['d', 'f', 'm'],
        };
        self.state
            .persistent_catalog
            .create_statistic_ext(row.clone())
            .map_err(ServerError::Catalog)?;
        let catalog_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        if let Err(e) = self.state.persistent_catalog.persist_statistic_ext_row(
            &row,
            self.state.heap.as_ref(),
            catalog_txn.xid,
            catalog_txn.current_command,
        ) {
            return Err(self.rollback_catalog_transaction_after_error(
                catalog_txn,
                ServerError::Catalog(e),
                "CREATE STATISTICS catalog rollback after persist error",
            ));
        }
        self.state.commit_transaction(
            catalog_txn,
            true,
            "CREATE STATISTICS catalog transaction",
        )?;
        self.plan_cache_invalidate();
        Ok(result_encoder::SelectResult {
            messages: vec![BackendMessage::CommandComplete {
                tag: "CREATE STATISTICS".to_string(),
            }],
            streamed_body: None,
            shared_streamed_body: None,
            streaming: None,
            rows: 0,
        })
    }

    pub(crate) fn execute_vacuum(
        &mut self,
        table: Option<&str>,
    ) -> Result<SelectResult, ServerError> {
        let snapshot = self.state.catalog_snapshot();
        let tables: Vec<TableEntry> = match table {
            Some(name) => vec![snapshot.tables.get(name).cloned().ok_or_else(|| {
                self.fail_if_in_transaction(ServerError::Plan(
                    ultrasql_planner::PlanError::TableNotFound(name.to_string()),
                ))
            })?],
            None => snapshot.tables.values().cloned().collect(),
        };
        let oldest = self.state.txn_manager.oldest_in_progress();
        for entry in tables {
            let rel = RelationId(entry.oid);
            let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
            self.state
                .workload_recorder
                .begin_vacuum(self.pid, entry.oid.raw(), block_count);
            let result = (|| -> Result<(), ServerError> {
                self.state
                    .workload_recorder
                    .update_vacuum(self.pid, "vacuuming indexes", 0, 0);
                self.vacuum_one_table_indexes(&snapshot, &entry, oldest)?;
                self.state.workload_recorder.update_vacuum(
                    self.pid,
                    "vacuuming heap",
                    block_count,
                    0,
                );
                self.state
                    .heap
                    .vacuum_heap(rel, oldest, self.state.txn_manager.as_ref())
                    .map_err(|e| ServerError::ddl(format!("VACUUM heap: {e}")))?;
                self.state.workload_recorder.update_vacuum(
                    self.pid,
                    "performing final cleanup",
                    block_count,
                    block_count,
                );
                self.state.vacuum_mark_visible_pages(oldest);
                self.resummarize_brin_indexes(&snapshot, &entry)?;
                self.maintain_aggregating_indexes_for_tables_after_commit(std::slice::from_ref(
                    &entry.name,
                ))?;
                Ok(())
            })();
            self.state.workload_recorder.finish_vacuum(self.pid);
            result?;
            self.state
                .workload_recorder
                .record_table_vacuum(entry.oid.raw());
        }
        Ok(result_encoder::SelectResult {
            messages: vec![BackendMessage::CommandComplete {
                tag: "VACUUM".to_string(),
            }],
            streamed_body: None,
            shared_streamed_body: None,
            streaming: None,
            rows: 0,
        })
    }

    fn vacuum_one_table_indexes(
        &self,
        snapshot: &CatalogSnapshot,
        entry: &TableEntry,
        oldest: Xid,
    ) -> Result<(), ServerError> {
        let Some(indexes) = snapshot.indexes_by_table.get(&entry.oid) else {
            return Ok(());
        };
        for index in indexes {
            if let Some(hnsw) =
                self.state
                    .table_constraints
                    .get(&entry.oid)
                    .and_then(|constraints| {
                        let metadata = constraints.indexes.get(&index.oid)?;
                        (metadata.method == ultrasql_planner::LogicalIndexMethod::Hnsw)
                            .then(|| metadata.hnsw.clone())
                            .flatten()
                    })
            {
                hnsw.vacuum_deleted_logged(oldest, self.state.heap.wal_sink().map(Arc::as_ref))
                    .map_err(|e| ServerError::ddl(format!("VACUUM HNSW {}: {e}", index.name)))?;
                continue;
            }
            if let Some(ivfflat) =
                self.state
                    .table_constraints
                    .get(&entry.oid)
                    .and_then(|constraints| {
                        let metadata = constraints.indexes.get(&index.oid)?;
                        (metadata.method == ultrasql_planner::LogicalIndexMethod::IvfFlat)
                            .then(|| metadata.ivfflat.clone())
                            .flatten()
                    })
            {
                ivfflat
                    .compact_deleted_logged(oldest, self.state.heap.wal_sink().map(Arc::as_ref))
                    .map_err(|e| ServerError::ddl(format!("VACUUM IVFFlat {}: {e}", index.name)))?;
                continue;
            }
            if index.root_block == BlockNumber::INVALID {
                continue;
            }
            let btree = BTree::open(
                Arc::clone(self.state.heap.buffer_pool()),
                RelationId::new(index.oid.raw()),
                index.root_block,
            );
            btree
                .vacuum(|tid| {
                    let Ok(tuple) = self.state.heap.fetch(tid) else {
                        return true;
                    };
                    let xmax = tuple.header.xmax;
                    !xmax.is_invalid() && xmax < oldest && self.state.txn_manager.is_committed(xmax)
                })
                .map_err(|e| ServerError::ddl(format!("VACUUM index {}: {e}", index.name)))?;
        }
        Ok(())
    }

    fn resummarize_brin_indexes(
        &self,
        snapshot: &CatalogSnapshot,
        entry: &TableEntry,
    ) -> Result<(), ServerError> {
        let Some(indexes) = snapshot.indexes_by_table.get(&entry.oid) else {
            return Ok(());
        };
        let Some(constraints) = self.state.table_constraints.get(&entry.oid) else {
            return Ok(());
        };
        let brin_indexes: Vec<_> = indexes
            .iter()
            .filter_map(|index| {
                let metadata = constraints.indexes.get(&index.oid)?;
                if metadata.method != ultrasql_planner::LogicalIndexMethod::Brin {
                    return None;
                }
                let brin = metadata.brin.clone()?;
                Some((index.clone(), metadata.clone(), brin))
            })
            .collect();
        drop(constraints);
        if brin_indexes.is_empty() {
            return Ok(());
        }

        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        if block_count == 0 {
            for (_, _, brin) in brin_indexes {
                brin.clear_summaries();
            }
            return Ok(());
        }

        let txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let result = (|| -> Result<(), ServerError> {
            for (index, metadata, brin) in &brin_indexes {
                let columns: Vec<usize> = index
                    .columns
                    .iter()
                    .map(|attnum| usize::from(*attnum))
                    .collect();
                let encoding = if metadata.key_exprs.is_empty() {
                    crate::index_key::IndexKeyEncoding::for_columns(&entry.schema, &columns)?
                } else {
                    let [expr] = metadata.key_exprs.as_slice() else {
                        return Err(ServerError::Unsupported(
                            "CREATE INDEX: expression indexes support exactly one key in this wave",
                        ));
                    };
                    crate::index_key::IndexKeyEncoding::for_data_type(&expr.data_type())?
                };
                brin.clear_summaries();
                let scan = self.state.heap.scan_visible(
                    rel,
                    block_count,
                    &txn.snapshot,
                    self.state.txn_manager.as_ref(),
                );
                for tuple in scan {
                    let tuple = tuple
                        .map_err(|e| ServerError::ddl(format!("VACUUM BRIN heap scan: {e}")))?;
                    let key = crate::decode_key_column(
                        &tuple.data,
                        &entry.schema,
                        columns.first().copied(),
                        &metadata.key_exprs,
                        metadata.predicate.as_ref(),
                        metadata.method,
                        &encoding,
                    )?;
                    if let Some(key) = key {
                        let brin_key = BrinIndex::encode_i64_key(key);
                        brin.insert(&brin_key, tuple.tid).map_err(|e| {
                            ServerError::ddl(format!("VACUUM BRIN summarize {}: {e}", index.name))
                        })?;
                    }
                }
            }
            Ok(())
        })();
        self.finalise_read_maintenance_transaction(
            txn,
            result,
            "VACUUM BRIN summarize commit",
            "VACUUM BRIN summarize rollback after rebuild error",
        )
    }

    pub(crate) fn execute_analyze(
        &mut self,
        table: Option<&str>,
    ) -> Result<SelectResult, ServerError> {
        match table {
            Some(t) => {
                if !self.state.analyze_table_with_pid(t, self.pid)? {
                    return Err(self.fail_if_in_transaction(ServerError::Plan(
                        ultrasql_planner::PlanError::TableNotFound(t.to_string()),
                    )));
                }
            }
            None => {
                let snapshot = self.state.catalog_snapshot();
                let tables: Vec<String> = snapshot.tables.keys().map(|k| k.to_string()).collect();
                for name in tables {
                    let _ = self.state.analyze_table_with_pid(&name, self.pid);
                }
            }
        }
        Ok(result_encoder::SelectResult {
            messages: vec![BackendMessage::CommandComplete {
                tag: "ANALYZE".to_string(),
            }],
            streamed_body: None,
            shared_streamed_body: None,
            streaming: None,
            rows: 0,
        })
    }
}
