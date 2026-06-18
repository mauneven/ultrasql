//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{
    Catalog, CatalogError, CatalogSnapshot, MutableCatalog, TableEntry, table_lookup_key,
};
use ultrasql_core::{BlockNumber, Field, RelationId, Value};
use ultrasql_planner::{LogicalAlterTableAction, LogicalPlan, ScalarExpr};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::heap::{DeleteOptions, UpdateOptions};
use ultrasql_txn::IsolationLevel;

use super::Session;
use crate::auth::PrivilegeObjectKind;
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};

struct AlterRewriteIndexUpdate<'a> {
    old_row: &'a [Value],
    new_row: &'a [Value],
    old_tid: ultrasql_core::TupleId,
    new_tid: ultrasql_core::TupleId,
    xid: ultrasql_core::Xid,
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Apply an `ALTER TABLE` action.
    ///
    /// The only supported action in this wave is `ADD COLUMN`. For
    /// `ADD COLUMN` we
    ///
    /// 1. take a per-statement MVCC snapshot,
    /// 2. scan every visible tuple under the *old* schema and reject
    ///    non-nullable appended columns without a default for non-empty
    ///    tables, otherwise rewrite each tuple through
    ///    `HeapAccess::update` with a payload encoded against the *new*
    ///    schema (the appended column carries its bound DEFAULT, or
    ///    [`Value::Null`] when no default exists),
    /// 3. swap the catalog entry to the new schema via
    ///    [`MutableCatalog::alter_table_add_column`].
    ///
    /// Steps 2 and 3 are wrapped in a single autocommit transaction so
    /// the rewrite and the catalog swap commit (or abort) together;
    /// concurrent readers either see the old schema with old tuples or
    /// the new schema with rewritten tuples — never a torn state.
    ///
    /// # Sub-shape gaps documented for reviewers
    ///
    /// - `DROP COLUMN`, `RENAME COLUMN`, `RENAME TO`, and
    ///   `ADD/DROP CONSTRAINT` are not yet bindable in
    ///   [`ultrasql_planner::bind`]; the binder returns
    ///   `NotSupported` for them so they never reach this arm.
    /// - The rewrite is not online-safe today: there is no per-relation
    ///   exclusive lock taken across steps 2 and 3, so a concurrent
    ///   INSERT during the rewrite may produce a tuple that scans see
    ///   under the new schema but was encoded against the old one. We
    ///   ship this anyway because v0.5 dispatches Simple Query
    ///   statements serially per connection and the README workload
    ///   does not concurrently mutate the relation under test. A
    ///   follow-up will route DDL through the lock manager
    ///   (`AccessExclusiveLock`).
    pub(crate) fn execute_alter_table(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::AlterTable {
            table_name, action, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_alter_table called with non-AlterTable plan",
            ));
        };
        let entry = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        self.ensure_table_owner_or_superuser(entry.oid, table_name)?;
        match action {
            LogicalAlterTableAction::AddColumn { column, default } => {
                self.execute_alter_add_column(table_name, column.clone(), default.clone(), snapshot)
            }
            LogicalAlterTableAction::DropColumn {
                column_index,
                column_name,
            } => self.execute_alter_drop_column(table_name, *column_index, column_name, snapshot),
            LogicalAlterTableAction::RenameColumn {
                column_index,
                new_name,
                ..
            } => self.execute_alter_rename_column(table_name, *column_index, new_name, snapshot),
            LogicalAlterTableAction::RenameTable { new_name } => {
                self.execute_alter_rename_table(table_name, new_name)
            }
            LogicalAlterTableAction::EnableRowLevelSecurity => {
                self.execute_alter_enable_row_security(table_name, snapshot)
            }
            LogicalAlterTableAction::SetOptions { options } => {
                self.execute_alter_set_options(table_name, options, snapshot)
            }
            LogicalAlterTableAction::AddUniqueConstraint { constraint } => {
                self.execute_alter_add_unique_constraint(table_name, constraint, snapshot)
            }
        }
    }

    fn execute_alter_add_unique_constraint(
        &self,
        table_name: &str,
        constraint: &ultrasql_planner::LogicalUniqueConstraint,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let table = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        if constraint.primary_key {
            for &column in &constraint.columns {
                let field = table.schema.field(column).ok_or_else(|| {
                    ServerError::ddl(format!(
                        "ALTER TABLE ADD PRIMARY KEY: column index {column} missing"
                    ))
                })?;
                if field.nullable {
                    return Err(ServerError::Unsupported(
                        "ALTER TABLE ADD PRIMARY KEY currently requires NOT NULL columns",
                    ));
                }
            }
        }
        let create_index = LogicalPlan::CreateIndex {
            index_name: constraint.name.clone(),
            index_namespace: table.schema_name.clone(),
            table_name: table_name.to_owned(),
            columns: constraint.columns.clone(),
            key_exprs: Vec::new(),
            opclasses: vec![None; constraint.columns.len()],
            index_options: Vec::new(),
            include_columns: Vec::new(),
            predicate: None,
            method: ultrasql_planner::LogicalIndexMethod::Btree,
            aggregating: None,
            unique: true,
            primary_key: constraint.primary_key,
            concurrently: false,
            if_not_exists: false,
            schema: ultrasql_core::Schema::empty(),
        };
        let _ = self.execute_create_index(&create_index, snapshot)?;
        let constraint_row = ultrasql_catalog::persistent::ConstraintRow {
            oid: self.state.persistent_catalog.next_oid(),
            conname: constraint.name.clone(),
            conrelid: table.oid,
            contype: if constraint.primary_key {
                ultrasql_catalog::persistent::ConType::PrimaryKey
            } else {
                ultrasql_catalog::persistent::ConType::Unique
            },
            condeferrable: false,
            condeferred: false,
            conkey: alter_constraint_attnums(&constraint.columns, &constraint.name)?,
            confrelid: ultrasql_core::Oid::INVALID,
            confkey: Vec::new(),
        };
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        if let Err(e) = self.state.persistent_catalog.persist_constraint_row(
            &constraint_row,
            self.state.heap.as_ref(),
            ddl_txn.xid,
            ddl_txn.current_command,
        ) {
            let _ = self
                .state
                .persistent_catalog
                .drop_index(&ultrasql_catalog::index_lookup_key(
                    &table.schema_name,
                    &constraint.name,
                ));
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e.into(),
                "ALTER TABLE ADD CONSTRAINT catalog rollback after persist error",
            ));
        }
        self.state.commit_transaction(
            ddl_txn,
            true,
            "ALTER TABLE ADD CONSTRAINT catalog transaction",
        )?;
        self.state
            .persistent_catalog
            .install_constraint_rows([constraint_row]);
        self.plan_cache_invalidate();
        Ok(run_ddl_command("ALTER TABLE"))
    }

    fn execute_alter_set_options(
        &self,
        table_name: &str,
        options: &[ultrasql_planner::LogicalTableOption],
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let entry = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        let mut pairs = options
            .iter()
            .map(|option| (option.name.clone(), option.value.clone()))
            .collect::<Vec<_>>();
        crate::validate_autovacuum_reloptions(&pairs)?;
        pairs.extend(
            entry
                .options
                .iter()
                .filter(|(name, _)| name.starts_with("ultrasql."))
                .cloned(),
        );
        let updated_entry = self
            .state
            .persistent_catalog
            .alter_table_options(table_name, pairs)
            .map_err(ServerError::Catalog)?;
        let txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        if let Err(e) = self.state.persistent_catalog.persist_table_rows(
            &updated_entry,
            self.state.heap.as_ref(),
            txn.xid,
            txn.current_command,
        ) {
            return Err(self.rollback_catalog_transaction_after_error(
                txn,
                e.into(),
                "ALTER TABLE SET catalog rollback after persist error",
            ));
        }
        self.state
            .commit_transaction(txn, true, "ALTER TABLE SET catalog transaction")?;
        self.state.plan_cache.invalidate_all();
        Ok(run_ddl_command(&format!("ALTER TABLE {}", entry.name)))
    }

    pub(crate) fn execute_alter_enable_row_security(
        &self,
        table_name: &str,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let entry = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        let previous = self
            .state
            .row_security
            .get(&entry.oid)
            .map(|guard| guard.clone());
        let mut runtime = previous
            .as_ref()
            .map(|existing| existing.as_ref().clone())
            .unwrap_or_default();
        if runtime.owner_role.is_empty() {
            runtime.owner_role = self.current_user.to_ascii_lowercase();
        }
        runtime.enabled = true;
        self.state.row_security.insert(entry.oid, Arc::new(runtime));
        if let Err(e) = self.state.persist_row_security_metadata() {
            if let Some(previous) = previous {
                self.state.row_security.insert(entry.oid, previous);
            } else {
                self.state.row_security.remove(&entry.oid);
            }
            return Err(e);
        }
        self.plan_cache_invalidate();
        Ok(run_ddl_command("ALTER TABLE"))
    }

    /// Execute `ALTER TABLE t DROP COLUMN c`: rewrite every visible
    /// tuple without that slot, then publish the narrower schema.
    pub(crate) fn execute_alter_drop_column(
        &self,
        table_name: &str,
        column_index: usize,
        column_name: &str,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let entry = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        let mut new_fields: Vec<ultrasql_core::Field> = entry.schema.fields().to_vec();
        if column_index >= new_fields.len() {
            return Err(ServerError::ddl(format!(
                "ALTER TABLE DROP COLUMN: index {column_index} out of bounds for {table_name}"
            )));
        }
        new_fields.remove(column_index);
        let new_schema = ultrasql_core::Schema::new(new_fields).map_err(|e| {
            ServerError::Catalog(ultrasql_catalog::CatalogError::schema_conflict(format!(
                "ALTER TABLE DROP COLUMN: {e}"
            )))
        })?;
        let table_key = ultrasql_catalog::table_lookup_key(&entry.schema_name, &entry.name);
        let partition_chunks = if let Some(runtime) = self.state.time_partitions.get(&table_key) {
            if runtime.partition_column_index == column_index {
                return Err(ServerError::Unsupported(
                    "ALTER TABLE DROP COLUMN cannot drop a time partition key",
                ));
            }
            runtime
                .chunks
                .iter()
                .filter_map(|chunk| snapshot.tables.get(&chunk.table_name).cloned())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let old_codec = ultrasql_executor::RowCodec::new(entry.schema.clone());
        let new_codec = ultrasql_executor::RowCodec::new(new_schema.clone());

        let rewrite_result: Result<(), ServerError> = (|| {
            let mut to_rewrite: Vec<(ultrasql_core::TupleId, Vec<Value>)> = Vec::new();
            {
                let scan = self.state.heap.scan_visible(
                    rel,
                    block_count,
                    &txn.snapshot,
                    self.state.txn_manager.as_ref(),
                );
                for result in scan {
                    let tup = result.map_err(|e| {
                        ServerError::ddl(format!("ALTER TABLE DROP COLUMN scan: {e}"))
                    })?;
                    let row = old_codec.decode(&tup.data).map_err(|e| {
                        ServerError::ddl(format!("ALTER TABLE DROP COLUMN decode: {e}"))
                    })?;
                    to_rewrite.push((tup.tid, row));
                }
            }
            let mut chunk_rewrites = Vec::with_capacity(partition_chunks.len());
            for chunk_entry in &partition_chunks {
                let chunk_rel = RelationId(chunk_entry.oid);
                let chunk_block_count = self
                    .state
                    .heap
                    .block_count(chunk_rel)
                    .max(chunk_entry.n_blocks);
                let chunk_codec = ultrasql_executor::RowCodec::new(chunk_entry.schema.clone());
                let mut chunk_rows = Vec::new();
                {
                    let scan = self.state.heap.scan_visible(
                        chunk_rel,
                        chunk_block_count,
                        &txn.snapshot,
                        self.state.txn_manager.as_ref(),
                    );
                    for result in scan {
                        let tup = result.map_err(|e| {
                            ServerError::ddl(format!(
                                "ALTER TABLE DROP COLUMN partition chunk scan: {e}"
                            ))
                        })?;
                        let row = chunk_codec.decode(&tup.data).map_err(|e| {
                            ServerError::ddl(format!(
                                "ALTER TABLE DROP COLUMN partition chunk decode: {e}"
                            ))
                        })?;
                        chunk_rows.push((tup.tid, row));
                    }
                }
                chunk_rewrites.push(chunk_rows);
            }
            for (tid, mut old_row) in to_rewrite {
                old_row.remove(column_index);
                let new_payload = new_codec.encode(&old_row).map_err(|e| {
                    ServerError::ddl(format!("ALTER TABLE DROP COLUMN encode: {e}"))
                })?;
                self.state
                    .heap
                    .update(
                        tid,
                        &new_payload,
                        UpdateOptions {
                            xid: txn.xid,
                            command_id: ultrasql_core::CommandId::FIRST,
                            wal: self.state.heap.wal_sink().map(|sink| sink.as_ref()),
                            vm: Some(self.state.vm.as_ref()),
                            hot_eligible: true,
                        },
                    )
                    .map_err(|e| {
                        ServerError::ddl(format!("ALTER TABLE DROP COLUMN heap update: {e}"))
                    })?;
            }
            for rows in chunk_rewrites {
                for (tid, mut old_row) in rows {
                    old_row.remove(column_index);
                    let new_payload = new_codec.encode(&old_row).map_err(|e| {
                        ServerError::ddl(format!(
                            "ALTER TABLE DROP COLUMN partition chunk encode: {e}"
                        ))
                    })?;
                    self.state
                        .heap
                        .update(
                            tid,
                            &new_payload,
                            UpdateOptions {
                                xid: txn.xid,
                                command_id: ultrasql_core::CommandId::FIRST,
                                wal: self.state.heap.wal_sink().map(|sink| sink.as_ref()),
                                vm: Some(self.state.vm.as_ref()),
                                hot_eligible: true,
                            },
                        )
                        .map_err(|e| {
                            ServerError::ddl(format!(
                                "ALTER TABLE DROP COLUMN partition chunk update: {e}"
                            ))
                        })?;
                }
            }
            Ok(())
        })();

        match rewrite_result {
            Ok(()) => {
                let updated_entry = self
                    .state
                    .persistent_catalog
                    .alter_table_replace_schema(table_name, new_schema.clone())
                    .map_err(ServerError::Catalog)?;
                if let Err(e) = self
                    .state
                    .persistent_catalog
                    .persist_table_schema_replacement(
                        entry,
                        &updated_entry,
                        self.state.heap.as_ref(),
                        txn.xid,
                        txn.current_command,
                    )
                {
                    return Err(self.rollback_catalog_transaction_after_error(
                        txn,
                        e.into(),
                        "ALTER TABLE DROP COLUMN catalog rollback after persist error",
                    ));
                }
                for chunk_entry in &partition_chunks {
                    let chunk_key = ultrasql_catalog::table_lookup_key(
                        &chunk_entry.schema_name,
                        &chunk_entry.name,
                    );
                    let updated_chunk = self
                        .state
                        .persistent_catalog
                        .alter_table_replace_schema(&chunk_key, new_schema.clone())
                        .map_err(ServerError::Catalog)?;
                    if let Err(e) = self
                        .state
                        .persistent_catalog
                        .persist_table_schema_replacement(
                            chunk_entry,
                            &updated_chunk,
                            self.state.heap.as_ref(),
                            txn.xid,
                            txn.current_command,
                        )
                    {
                        return Err(self.rollback_catalog_transaction_after_error(
                            txn,
                            e.into(),
                            "ALTER TABLE DROP COLUMN partition chunk catalog rollback after persist error",
                        ));
                    }
                }
                self.state
                    .commit_transaction(txn, true, "ALTER TABLE DROP COLUMN")?;
                if let Some((_, partition)) = self.state.time_partitions.remove(&table_key) {
                    let partition_column_index = if column_index < partition.partition_column_index
                    {
                        partition.partition_column_index.saturating_sub(1)
                    } else {
                        partition.partition_column_index
                    };
                    self.state.time_partitions.insert(
                        table_key,
                        Arc::new(partition.as_ref().with_parent_metadata(
                            updated_entry.schema_name.clone(),
                            updated_entry.name.clone(),
                            updated_entry.schema.clone(),
                            partition.partition_column.clone(),
                            partition_column_index,
                        )),
                    );
                }
                self.state.plan_cache.invalidate_all();
                Ok(run_ddl_command(&format!(
                    "ALTER TABLE DROP COLUMN {column_name}"
                )))
            }
            Err(e) => Err(self.rollback_transaction_after_error(
                txn,
                e,
                "ALTER TABLE DROP COLUMN rollback after rewrite error",
            )),
        }
    }

    /// Execute `ALTER TABLE t RENAME COLUMN old TO new`: catalog-only
    /// (the heap's row codec is positional so no rewrite is needed).
    pub(crate) fn execute_alter_rename_column(
        &self,
        table_name: &str,
        column_index: usize,
        new_name: &str,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let entry = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        let mut new_fields: Vec<ultrasql_core::Field> = entry.schema.fields().to_vec();
        if column_index >= new_fields.len() {
            return Err(ServerError::ddl(format!(
                "ALTER TABLE RENAME COLUMN: index {column_index} out of bounds for {table_name}"
            )));
        }
        let renamed = ultrasql_core::Field {
            name: new_name.to_string(),
            ..new_fields[column_index].clone()
        };
        new_fields[column_index] = renamed;
        let new_schema = ultrasql_core::Schema::new(new_fields).map_err(|e| {
            ServerError::Catalog(ultrasql_catalog::CatalogError::schema_conflict(format!(
                "ALTER TABLE RENAME COLUMN: {e}"
            )))
        })?;
        let table_key = ultrasql_catalog::table_lookup_key(&entry.schema_name, &entry.name);
        let partition_update = self.state.time_partitions.get(&table_key).map(|runtime| {
            let partition_column = if runtime.partition_column_index == column_index {
                new_name.to_string()
            } else {
                runtime.partition_column.clone()
            };
            let chunks = runtime
                .chunks
                .iter()
                .filter_map(|chunk| snapshot.tables.get(&chunk.table_name).cloned())
                .collect::<Vec<_>>();
            (partition_column, runtime.partition_column_index, chunks)
        });
        let attr_has_defaults = if let Some(runtime) = self.state.table_constraints.get(&entry.oid)
        {
            alter_attr_has_defaults(Some(runtime.value().as_ref()), new_schema.len())
        } else {
            alter_attr_has_defaults(None, new_schema.len())
        };
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let mut updated_entry = self
            .state
            .persistent_catalog
            .alter_table_replace_schema(table_name, new_schema)
            .map_err(ServerError::Catalog)?;
        if let Some((partition_column, _, _)) = partition_update.as_ref() {
            let options = crate::time_partition::parent_catalog_options_with_column(
                &updated_entry.options,
                partition_column,
            );
            updated_entry = self
                .state
                .persistent_catalog
                .alter_table_options(table_name, options)
                .map_err(ServerError::Catalog)?;
        }
        if let Err(e) = self
            .state
            .persistent_catalog
            .persist_table_schema_replacement_with_defaults(
                entry,
                &updated_entry,
                &attr_has_defaults,
                self.state.heap.as_ref(),
                txn.xid,
                txn.current_command,
            )
        {
            return Err(self.rollback_catalog_transaction_after_error(
                txn,
                e.into(),
                "ALTER TABLE RENAME COLUMN catalog rollback after persist error",
            ));
        }
        if let Some((_, _, chunks)) = partition_update.as_ref() {
            for chunk_entry in chunks {
                let mut chunk_fields: Vec<ultrasql_core::Field> =
                    chunk_entry.schema.fields().to_vec();
                if column_index >= chunk_fields.len() {
                    return Err(self.rollback_catalog_transaction_after_error(
                        txn,
                        ServerError::ddl(format!(
                            "ALTER TABLE RENAME COLUMN partition chunk index {column_index} out of bounds"
                        )),
                        "ALTER TABLE RENAME COLUMN partition chunk catalog rollback after schema error",
                    ));
                }
                chunk_fields[column_index] = ultrasql_core::Field {
                    name: new_name.to_string(),
                    ..chunk_fields[column_index].clone()
                };
                let chunk_schema = ultrasql_core::Schema::new(chunk_fields).map_err(|e| {
                    ServerError::Catalog(ultrasql_catalog::CatalogError::schema_conflict(format!(
                        "ALTER TABLE RENAME COLUMN partition chunk: {e}"
                    )))
                })?;
                let chunk_key =
                    ultrasql_catalog::table_lookup_key(&chunk_entry.schema_name, &chunk_entry.name);
                let updated_chunk = self
                    .state
                    .persistent_catalog
                    .alter_table_replace_schema(&chunk_key, chunk_schema)
                    .map_err(ServerError::Catalog)?;
                if let Err(e) = self
                    .state
                    .persistent_catalog
                    .persist_table_schema_replacement(
                        chunk_entry,
                        &updated_chunk,
                        self.state.heap.as_ref(),
                        txn.xid,
                        txn.current_command,
                    )
                {
                    return Err(self.rollback_catalog_transaction_after_error(
                        txn,
                        e.into(),
                        "ALTER TABLE RENAME COLUMN partition chunk catalog rollback after persist error",
                    ));
                }
            }
        }
        self.state
            .commit_transaction(txn, true, "ALTER TABLE RENAME COLUMN")?;
        if let Some((partition_column, partition_column_index, _)) = partition_update
            && let Some((_, partition)) = self.state.time_partitions.remove(&table_key)
        {
            self.state.time_partitions.insert(
                table_key,
                Arc::new(partition.as_ref().with_parent_metadata(
                    updated_entry.schema_name.clone(),
                    updated_entry.name.clone(),
                    updated_entry.schema.clone(),
                    partition_column,
                    partition_column_index,
                )),
            );
        }
        self.state.plan_cache.invalidate_all();
        Ok(run_ddl_command(&format!(
            "ALTER TABLE RENAME COLUMN TO {new_name}"
        )))
    }

    /// Execute `ALTER TABLE t RENAME TO new`: catalog-only (relations
    /// are OID-addressed; the rename only updates the by-name index).
    pub(crate) fn execute_alter_rename_table(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<SelectResult, ServerError> {
        self.state
            .ensure_create_relation_metadata_slots_persistable()?;
        let old_entry = self
            .state
            .persistent_catalog
            .lookup_table(old_name)
            .ok_or_else(|| ServerError::Catalog(CatalogError::not_found(old_name.to_owned())))?;
        let old_table_key = table_lookup_key(&old_entry.schema_name, &old_entry.name);
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        let updated_entry = self
            .state
            .persistent_catalog
            .alter_table_rename(old_name, new_name)
            .map_err(ServerError::Catalog)?;
        let privilege_metadata_changed = self.state.privilege_catalog.rename_object_grants(
            PrivilegeObjectKind::Table,
            old_name,
            new_name,
        );
        let new_table_key = table_lookup_key(&updated_entry.schema_name, &updated_entry.name);
        if let Some((_, runtime)) = self.state.time_partitions.remove(&old_table_key) {
            self.state.time_partitions.insert(
                new_table_key,
                Arc::new(runtime.as_ref().renamed(
                    updated_entry.schema_name.clone(),
                    updated_entry.name.clone(),
                )),
            );
        }
        let attr_has_defaults =
            if let Some(runtime) = self.state.table_constraints.get(&updated_entry.oid) {
                alter_attr_has_defaults(Some(runtime.value().as_ref()), updated_entry.schema.len())
            } else {
                alter_attr_has_defaults(None, updated_entry.schema.len())
            };
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        if let Err(e) = self
            .state
            .persistent_catalog
            .persist_table_rows_with_defaults(
                &updated_entry,
                &attr_has_defaults,
                self.state.heap.as_ref(),
                txn.xid,
                txn.current_command,
            )
        {
            return Err(self.rollback_catalog_transaction_after_error(
                txn,
                e.into(),
                "ALTER TABLE RENAME catalog rollback after persist error",
            ));
        }
        self.state
            .commit_transaction(txn, true, "ALTER TABLE RENAME")?;
        self.state.persist_row_security_metadata()?;
        if privilege_metadata_changed && let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        self.state.plan_cache.invalidate_all();
        Ok(run_ddl_command(&format!(
            "ALTER TABLE RENAME TO {new_name}"
        )))
    }

    /// Execute the
    /// `ALTER TABLE t ADD COLUMN c TYPE [DEFAULT expr] [NULL | NOT NULL]`
    /// path.
    ///
    /// Decoded from the dispatch arm so `execute_alter_table` stays
    /// a thin shape-match. See [`Self::execute_alter_table`] for the
    /// design notes that apply to the rewrite ordering, MVCC, and the
    /// known online-DDL gap.
    pub(crate) fn execute_alter_add_column(
        &self,
        table_name: &str,
        column: Field,
        default: Option<ScalarExpr>,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        // 1. Resolve the existing entry and build the new schema.
        let entry = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        let mut new_fields: Vec<ultrasql_core::Field> = entry.schema.fields().to_vec();
        new_fields.push(column.clone());
        let new_schema = ultrasql_core::Schema::new(new_fields).map_err(|e| {
            ServerError::Catalog(ultrasql_catalog::CatalogError::schema_conflict(format!(
                "ALTER TABLE ADD COLUMN: {e}"
            )))
        })?;
        let new_width = new_schema.len();
        let runtime_after =
            self.runtime_column_metadata_after_add_column(entry.oid, new_width, default.clone());
        let table_key = ultrasql_catalog::table_lookup_key(&entry.schema_name, &entry.name);
        if let Some(runtime) = runtime_after.as_ref() {
            self.state
                .ensure_table_runtime_constraints_metadata_persistable(&table_key, runtime)?;
            self.state
                .ensure_table_runtime_constraints_metadata_slots_persistable()?;
        }
        let partition_chunks = self
            .state
            .time_partitions
            .get(&table_key)
            .map(|runtime| {
                runtime
                    .chunks
                    .iter()
                    .filter_map(|chunk| snapshot.tables.get(&chunk.table_name).cloned())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // 2. Rewrite existing tuples — outside the catalog swap so
        //    the snapshot scan still observes the old schema.
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let old_codec = ultrasql_executor::RowCodec::new(entry.schema.clone());
        let new_codec = ultrasql_executor::RowCodec::new(new_schema);

        let rewrite_result: Result<(), ServerError> = (|| {
            // Collect the visible tuples up front so the heap iterator
            // is fully drained before any update lands — otherwise the
            // iterator could revisit a row that the update has just
            // copied into a new slot. The relations we ALTER in v0.5
            // fit comfortably in memory.
            let mut to_rewrite: Vec<(ultrasql_core::TupleId, Vec<Value>)> = Vec::new();
            {
                let scan = self.state.heap.scan_visible(
                    rel,
                    block_count,
                    &txn.snapshot,
                    self.state.txn_manager.as_ref(),
                );
                for result in scan {
                    let tup = result
                        .map_err(|e| ServerError::ddl(format!("ALTER TABLE heap scan: {e}")))?;
                    let row = old_codec
                        .decode(&tup.data)
                        .map_err(|e| ServerError::ddl(format!("ALTER TABLE row decode: {e}")))?;
                    to_rewrite.push((tup.tid, row));
                }
            }
            let mut chunk_rewrites = Vec::with_capacity(partition_chunks.len());
            for chunk_entry in &partition_chunks {
                let chunk_rel = RelationId(chunk_entry.oid);
                let chunk_block_count = self
                    .state
                    .heap
                    .block_count(chunk_rel)
                    .max(chunk_entry.n_blocks);
                let chunk_codec = ultrasql_executor::RowCodec::new(chunk_entry.schema.clone());
                let mut chunk_rows = Vec::new();
                {
                    let scan = self.state.heap.scan_visible(
                        chunk_rel,
                        chunk_block_count,
                        &txn.snapshot,
                        self.state.txn_manager.as_ref(),
                    );
                    for result in scan {
                        let tup = result.map_err(|e| {
                            ServerError::ddl(format!("ALTER TABLE partition chunk scan: {e}"))
                        })?;
                        let row = chunk_codec.decode(&tup.data).map_err(|e| {
                            ServerError::ddl(format!("ALTER TABLE partition chunk decode: {e}"))
                        })?;
                        chunk_rows.push((tup.tid, row));
                    }
                }
                chunk_rewrites.push((chunk_entry.clone(), chunk_rows));
            }

            if default.is_none()
                && !column.nullable
                && (!to_rewrite.is_empty()
                    || chunk_rewrites.iter().any(|(_, rows)| !rows.is_empty()))
            {
                return Err(ServerError::Execute(
                    ultrasql_executor::ExecError::NotNullViolation(column.name.clone()),
                ));
            }

            // Now perform the updates.
            for (tid, old_row) in to_rewrite {
                let mut new_row = old_row.clone();
                new_row.push(alter_add_column_default_value(default.as_ref(), &column)?);
                let new_payload = new_codec
                    .encode(&new_row)
                    .map_err(|e| ServerError::ddl(format!("ALTER TABLE row encode: {e}")))?;
                let outcome = self
                    .state
                    .heap
                    .update(
                        tid,
                        &new_payload,
                        UpdateOptions {
                            xid: txn.xid,
                            command_id: ultrasql_core::CommandId::FIRST,
                            wal: self.state.heap.wal_sink().map(|sink| sink.as_ref()),
                            vm: Some(self.state.vm.as_ref()),
                            hot_eligible: true,
                        },
                    )
                    .map_err(|e| ServerError::ddl(format!("ALTER TABLE heap update: {e}")))?;
                self.maintain_indexes_for_alter_rewrite(
                    entry,
                    snapshot,
                    AlterRewriteIndexUpdate {
                        old_row: &old_row,
                        new_row: &new_row,
                        old_tid: tid,
                        new_tid: outcome.new_tid,
                        xid: txn.xid,
                    },
                )?;
            }
            for (chunk_entry, rows) in chunk_rewrites {
                for (tid, old_row) in rows {
                    let mut new_row = old_row;
                    new_row.push(alter_add_column_default_value(default.as_ref(), &column)?);
                    let new_payload = new_codec.encode(&new_row).map_err(|e| {
                        ServerError::ddl(format!("ALTER TABLE partition chunk encode: {e}"))
                    })?;
                    self.state
                        .heap
                        .update(
                            tid,
                            &new_payload,
                            UpdateOptions {
                                xid: txn.xid,
                                command_id: ultrasql_core::CommandId::FIRST,
                                wal: self.state.heap.wal_sink().map(|sink| sink.as_ref()),
                                vm: Some(self.state.vm.as_ref()),
                                hot_eligible: true,
                            },
                        )
                        .map_err(|e| {
                            ServerError::ddl(format!("ALTER TABLE partition chunk update: {e}"))
                        })?;
                }
                let _ = chunk_entry;
            }
            Ok(())
        })();

        // 3. Swap the catalog entry only if the rewrite succeeded;
        //    otherwise abort the transaction so the half-rewritten
        //    tuples become dead (their xmin matches our xid, which we
        //    will mark aborted on rollback).
        match rewrite_result {
            Ok(()) => {
                let updated_entry = self
                    .state
                    .persistent_catalog
                    .alter_table_add_column(table_name, column.clone())?;
                let attr_has_defaults =
                    alter_attr_has_defaults(runtime_after.as_ref(), updated_entry.schema.len());
                if let Err(e) = self
                    .state
                    .persistent_catalog
                    .persist_table_schema_replacement_with_defaults(
                        entry,
                        &updated_entry,
                        &attr_has_defaults,
                        self.state.heap.as_ref(),
                        txn.xid,
                        txn.current_command,
                    )
                {
                    return Err(self.rollback_catalog_transaction_after_error(
                        txn,
                        e.into(),
                        "ALTER TABLE ADD COLUMN catalog rollback after persist error",
                    ));
                }
                for chunk_entry in &partition_chunks {
                    let chunk_key = ultrasql_catalog::table_lookup_key(
                        &chunk_entry.schema_name,
                        &chunk_entry.name,
                    );
                    let updated_chunk = self
                        .state
                        .persistent_catalog
                        .alter_table_add_column(&chunk_key, column.clone())
                        .map_err(ServerError::Catalog)?;
                    if let Err(e) = self
                        .state
                        .persistent_catalog
                        .persist_table_schema_replacement(
                            chunk_entry,
                            &updated_chunk,
                            self.state.heap.as_ref(),
                            txn.xid,
                            txn.current_command,
                        )
                    {
                        return Err(self.rollback_catalog_transaction_after_error(
                            txn,
                            e.into(),
                            "ALTER TABLE ADD COLUMN partition chunk catalog rollback after persist error",
                        ));
                    }
                }
                self.state
                    .commit_transaction(txn, true, "ALTER TABLE ADD COLUMN")?;
                if let Some(runtime) = runtime_after {
                    self.state
                        .table_constraints
                        .insert(entry.oid, Arc::new(runtime));
                    self.state.persist_table_runtime_constraints_metadata()?;
                }
                if let Some((_, partition)) = self.state.time_partitions.remove(&table_key) {
                    self.state.time_partitions.insert(
                        table_key,
                        Arc::new(partition.as_ref().with_parent_metadata(
                            updated_entry.schema_name.clone(),
                            updated_entry.name.clone(),
                            updated_entry.schema.clone(),
                            partition.partition_column.clone(),
                            partition.partition_column_index,
                        )),
                    );
                }
                // A schema change can invalidate any cached projection-
                // pushdown / predicate-pushdown decision; clear all.
                self.plan_cache_invalidate();
                Ok(run_ddl_command("ALTER TABLE"))
            }
            Err(e) => Err(self.rollback_transaction_after_error(
                txn,
                e,
                "ALTER TABLE ADD COLUMN rollback after rewrite error",
            )),
        }
    }

    fn runtime_column_metadata_after_add_column(
        &self,
        table_oid: ultrasql_core::Oid,
        width: usize,
        default: Option<ScalarExpr>,
    ) -> Option<crate::TableRuntimeConstraints> {
        let (had_existing, mut constraints) =
            if let Some(existing) = self.state.table_constraints.get(&table_oid) {
                (true, existing.value().as_ref().clone())
            } else {
                (false, crate::TableRuntimeConstraints::default())
            };
        constraints.defaults.resize(width, None);
        constraints.sequence_defaults.resize(width, None);
        constraints.identity_always.resize(width, false);
        constraints.generated_stored.resize(width, None);
        if let Some(default) = default {
            constraints.defaults[width - 1] = Some(default);
        }
        if had_existing || table_runtime_constraints_have_metadata(&constraints) {
            Some(constraints)
        } else {
            None
        }
    }

    fn maintain_indexes_for_alter_rewrite(
        &self,
        table: &TableEntry,
        snapshot: &CatalogSnapshot,
        update: AlterRewriteIndexUpdate<'_>,
    ) -> Result<(), ServerError> {
        let Some(indexes) = snapshot.indexes_by_table.get(&table.oid) else {
            return Ok(());
        };
        let wal = self.state.heap.wal_sink().map(|sink| sink.as_ref());
        for index in indexes {
            if index.root_block == BlockNumber::INVALID {
                continue;
            }
            let columns = index
                .columns
                .iter()
                .map(|column| usize::from(*column))
                .collect::<Vec<_>>();
            let encoding =
                crate::index_key::IndexKeyEncoding::for_columns(&table.schema, &columns)?;
            let old_key = alter_encode_index_key(&encoding, &columns, update.old_row, &index.name)?;
            let new_key = alter_encode_index_key(&encoding, &columns, update.new_row, &index.name)?;
            if old_key == new_key {
                if let Some(key) = old_key {
                    let mut tree = BTree::open(
                        Arc::clone(self.state.heap.buffer_pool()),
                        RelationId::new(index.oid.raw()),
                        index.root_block,
                    );
                    let _ = tree
                        .delete_logged::<i64>(key, update.old_tid, update.xid, wal)
                        .map_err(|e| {
                            ServerError::ddl(format!(
                                "ALTER TABLE index delete {}: {e}",
                                index.name
                            ))
                        })?;
                    let result = if index.is_unique {
                        tree.insert::<i64>(key, update.new_tid, update.xid, wal)
                    } else {
                        tree.insert_non_unique::<i64>(key, update.new_tid, update.xid, wal)
                    };
                    result.map_err(|e| match e {
                        ultrasql_storage::btree::BTreeError::DuplicateKey => ServerError::Execute(
                            ultrasql_executor::ExecError::UniqueViolation(index.name.clone()),
                        ),
                        other => ServerError::ddl(format!(
                            "ALTER TABLE index insert {}: {other}",
                            index.name
                        )),
                    })?;
                }
                continue;
            }
            return Err(ServerError::Unsupported(
                "ALTER TABLE rewrite changed an index key unexpectedly",
            ));
        }
        Ok(())
    }

    /// Empty every relation named in the `TRUNCATE` statement.
    ///
    /// PostgreSQL's `TRUNCATE` takes `ACCESS EXCLUSIVE` and reclaims the
    /// relfilenode in a single fast-path: drop the segment files, then
    /// allocate a fresh empty heap on commit. UltraSQL's v0.5 in-memory
    /// runtime has no segment manager wired into the server's
    /// `BufferPool<BlankPageLoader>`, so the fast-path "swap the
    /// relfilenode" hook does not yet exist on this path. Instead, we
    /// open an autocommit MVCC transaction and stamp `xmax` on every
    /// row visible to the txn's own snapshot by calling
    /// [`HeapAccess::delete`] for each visible TID.
    ///
    /// Correctness notes:
    ///
    /// - The result is MVCC-correct under our snapshot model: a
    ///   concurrent snapshot that pre-dates the truncate's commit
    ///   continues to see every row (its `xmax` is committed-after
    ///   from the older snapshot's POV); a snapshot taken after the
    ///   commit sees the relation as empty.
    /// - Dead-tuple pages stay on the heap. A subsequent `INSERT` will
    ///   reuse free space inside them as it would after any DELETE,
    ///   and `n_blocks` stays unchanged so future scans still cover
    ///   the dead-tuple block range (necessary because a row inserted
    ///   into one of those reused slots must still be discovered).
    /// - The path is `O(rows visible to txn)` rather than O(1). For
    ///   the wire-completion gate this is acceptable: TRUNCATE is no
    ///   longer rejected, and a future segment-manager wiring can
    ///   replace this body with the proper fast-path without touching
    ///   any caller.
    ///
    /// `CASCADE` walks the runtime foreign-key graph built by `CREATE TABLE`
    /// and rebuilt from durable table-runtime metadata on WAL-backed restart.
    /// Referencing child tables are added recursively; omitting `CASCADE`
    /// raises `2BP01` when such dependencies exist.
    ///
    /// Multi-table `TRUNCATE` truncates every table inside a single
    /// autocommit transaction so the operation is atomic — either all
    /// listed relations become empty in the next snapshot or none do.
    pub(crate) fn execute_truncate(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::Truncate {
            tables,
            restart_identity,
            cascade,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_truncate called with non-Truncate plan",
            ));
        };
        let tables = self.collect_truncate_tables(tables, *cascade, snapshot)?;
        for name in &tables {
            let entry = snapshot.tables.get(name).ok_or_else(|| {
                ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(name.clone()))
            })?;
            self.ensure_table_owner_or_privilege_or_superuser(
                entry.oid,
                name,
                crate::auth::PrivilegeKind::Truncate,
                "truncate",
            )?;
        }
        let truncate_tables = self.expand_time_partition_truncate_tables(&tables);

        // Single autocommit txn so the multi-table case is atomic. A
        // partial failure aborts the txn and every delete it stamped
        // becomes invisible to subsequent snapshots.
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);

        let truncate_result: Result<(), ServerError> = (|| {
            let mut owned_sequences_to_restart = Vec::new();
            let mut seen_owned_sequences = std::collections::HashSet::new();
            for name in &truncate_tables {
                let entry = snapshot.tables.get(name).ok_or_else(|| {
                    ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(name.clone()))
                })?;
                if *restart_identity {
                    if let Some(constraints) = self.state.table_constraints.get(&entry.oid) {
                        for seq_name in constraints.sequence_defaults.iter().flatten() {
                            if !seen_owned_sequences.insert(seq_name.clone()) {
                                continue;
                            }
                            let seq = self
                                .state
                                .sequences
                                .get(seq_name)
                                .map(|entry| entry.clone())
                                .ok_or_else(|| {
                                    ServerError::ddl(format!(
                                        "TRUNCATE RESTART IDENTITY: missing owned sequence {seq_name}",
                                    ))
                                })?;
                            owned_sequences_to_restart.push((seq_name.clone(), seq));
                        }
                    }
                }
                let rel = RelationId(entry.oid);
                // The heap's resident block count is the source of
                // truth for "how many blocks must I scan." We OR with
                // the catalog's hint so a relation extended on a
                // previous connection still gets a complete scan.
                let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);

                // Snapshot every visible TID up front, then issue the
                // deletes in a second pass. Holding the heap iterator
                // open across delete calls would let the iterator
                // revisit a tuple whose xmax we just stamped; flushing
                // to a vector first avoids that race.
                let mut tids: Vec<ultrasql_core::TupleId> = Vec::new();
                {
                    let scan = self.state.heap.scan_visible(
                        rel,
                        block_count,
                        &txn.snapshot,
                        self.state.txn_manager.as_ref(),
                    );
                    for result in scan {
                        let tup = result
                            .map_err(|e| ServerError::ddl(format!("TRUNCATE heap scan: {e}")))?;
                        tids.push(tup.tid);
                    }
                }

                for tid in tids {
                    self.state
                        .heap
                        .delete(
                            tid,
                            DeleteOptions {
                                xmax: txn.xid,
                                cmax: ultrasql_core::CommandId::FIRST,
                                wal: None,
                                fsm: None,
                                vm: Some(self.state.vm.as_ref()),
                            },
                        )
                        .map_err(|e| ServerError::ddl(format!("TRUNCATE heap delete: {e}")))?;
                }
            }
            let sequence_wal = self.state.heap.wal_sink().cloned();
            for (seq_name, seq) in owned_sequences_to_restart {
                let options = seq.options_snapshot();
                seq.alter_options_logged(
                    options,
                    Some(options.start),
                    &seq_name,
                    RelationId::INVALID,
                    txn.xid,
                    sequence_wal.as_deref(),
                )
                .map_err(|e| ServerError::ddl(format!("TRUNCATE RESTART IDENTITY: {e}")))?;
            }
            Ok(())
        })();

        match truncate_result {
            Ok(()) => {
                self.state.commit_transaction(txn, true, "TRUNCATE")?;
                for name in &truncate_tables {
                    self.state.columnar_storage.mark_dirty(name);
                }
                // Row counts changed beyond recognition; clear the cache
                // so any cardinality-aware plan re-runs.
                self.plan_cache_invalidate();
                Ok(run_ddl_command("TRUNCATE TABLE"))
            }
            Err(e) => Err(self.rollback_transaction_after_error(
                txn,
                e,
                "TRUNCATE rollback after execution error",
            )),
        }
    }

    fn expand_time_partition_truncate_tables(&self, tables: &[String]) -> Vec<String> {
        let mut expanded = tables.to_vec();
        let mut seen = tables
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<std::collections::HashSet<_>>();
        for name in tables {
            let Some(runtime) = self.state.time_partitions.get(name) else {
                continue;
            };
            for chunk in runtime.chunks.iter() {
                let chunk_name = chunk.value().table_name.clone();
                if seen.insert(chunk_name.to_ascii_lowercase()) {
                    expanded.push(chunk_name);
                }
            }
        }
        expanded
    }

    fn collect_truncate_tables(
        &self,
        requested_tables: &[String],
        cascade: bool,
        snapshot: &CatalogSnapshot,
    ) -> Result<Vec<String>, ServerError> {
        let mut truncate_tables = requested_tables.to_vec();
        let mut truncate_set: std::collections::HashSet<String> =
            truncate_tables.iter().cloned().collect();

        loop {
            let target_oids: std::collections::HashSet<ultrasql_core::Oid> = truncate_tables
                .iter()
                .filter_map(|name| snapshot.tables.get(name).map(|entry| entry.oid))
                .collect();
            let mut dependent_constraints = Vec::new();
            let mut dependent_tables = Vec::new();

            for item in self.state.table_constraints.iter() {
                let table_oid = *item.key();
                let Some(table) = snapshot.tables_by_oid.get(&table_oid) else {
                    continue;
                };
                let table_name =
                    ultrasql_catalog::table_lookup_key(&table.schema_name, &table.name);
                if truncate_set.contains(&table_name) {
                    continue;
                }
                for fk in &item.value().foreign_keys {
                    if !target_oids.contains(&fk.target_oid) {
                        continue;
                    }
                    if cascade {
                        dependent_tables.push(table_name.clone());
                    } else {
                        dependent_constraints.push(format!("{}.{}", table.name, fk.name));
                    }
                }
            }

            if !dependent_constraints.is_empty() {
                dependent_constraints.sort();
                dependent_constraints.dedup();
                return Err(ServerError::DependentObjectsStillExist(format!(
                    "cannot truncate table because other objects depend on it: {}",
                    dependent_constraints.join(", ")
                )));
            }

            dependent_tables.sort();
            dependent_tables.dedup();
            let mut changed = false;
            for table_name in dependent_tables {
                if truncate_set.insert(table_name.clone()) {
                    truncate_tables.push(table_name);
                    changed = true;
                }
            }
            if !changed {
                return Ok(truncate_tables);
            }
        }
    }
}

fn alter_add_column_default_value(
    default: Option<&ScalarExpr>,
    column: &Field,
) -> Result<Value, ServerError> {
    let value = if let Some(expr) = default {
        ultrasql_executor::Eval::new(expr.clone())
            .eval(&[])
            .map_err(ultrasql_executor::eval_error_to_exec_error)
            .map_err(ServerError::Execute)?
    } else {
        Value::Null
    };
    if !column.nullable && matches!(value, Value::Null) {
        return Err(ServerError::Execute(
            ultrasql_executor::ExecError::NotNullViolation(column.name.clone()),
        ));
    }
    Ok(value)
}

fn alter_attr_has_defaults(
    runtime: Option<&crate::TableRuntimeConstraints>,
    width: usize,
) -> Vec<bool> {
    let Some(runtime) = runtime else {
        return vec![false; width];
    };
    (0..width)
        .map(|idx| {
            runtime.defaults.get(idx).is_some_and(Option::is_some)
                || runtime
                    .sequence_defaults
                    .get(idx)
                    .is_some_and(Option::is_some)
                || runtime.identity_always.get(idx).copied().unwrap_or(false)
                || runtime
                    .generated_stored
                    .get(idx)
                    .is_some_and(Option::is_some)
        })
        .collect()
}

fn table_runtime_constraints_have_metadata(constraints: &crate::TableRuntimeConstraints) -> bool {
    constraints.defaults.iter().any(Option::is_some)
        || constraints.sequence_defaults.iter().any(Option::is_some)
        || constraints.identity_always.iter().any(|flag| *flag)
        || constraints.generated_stored.iter().any(Option::is_some)
        || !constraints.checks.is_empty()
        || !constraints.foreign_keys.is_empty()
        || !constraints.exclusion_constraints.is_empty()
        || !constraints.indexes.is_empty()
}

fn alter_constraint_attnums(columns: &[usize], name: &str) -> Result<Vec<i16>, ServerError> {
    columns
        .iter()
        .map(|col| {
            let attnum = col.checked_add(1).ok_or(ServerError::Unsupported(
                "ALTER TABLE: constraint attnum overflow",
            ))?;
            i16::try_from(attnum).map_err(|_| {
                ServerError::ddl(format!(
                    "ALTER TABLE: constraint {name} column position {attnum} does not fit i16"
                ))
            })
        })
        .collect()
}

fn alter_encode_index_key(
    encoding: &crate::index_key::IndexKeyEncoding,
    columns: &[usize],
    row: &[Value],
    index_name: &str,
) -> Result<Option<i64>, ServerError> {
    match columns {
        [col] => {
            let value = row.get(*col).ok_or_else(|| {
                ServerError::ddl(format!("index {index_name}: row missing key column {col}"))
            })?;
            encoding.encode_value(value)
        }
        _ => encoding.encode_row(row),
    }
}
