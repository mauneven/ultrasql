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

/// The catalog adjustments `ALTER TABLE DROP COLUMN` must apply to the
/// table's dependent indexes and position-referencing constraints so the
/// physical schema compaction (which shifts every column after the dropped
/// one down by one slot) does not leave index `columns` / constraint
/// `conkey` pointing at the wrong — or out-of-bounds — positions.
///
/// PostgreSQL drops an index outright if any of its key columns is the
/// dropped column, and re-points the surviving ones; it likewise drops a
/// UNIQUE / PRIMARY KEY whose key includes the dropped column (together with
/// its backing index) and a CHECK that references the dropped column, while
/// shifting the survivors. UltraSQL compacts attnums (rather than keeping a
/// `pg.dropped.N` tombstone column like PostgreSQL), so the survivors are
/// re-pointed by decrementing every position greater than the dropped one.
#[derive(Default)]
struct DropColumnDependents {
    /// Indexes that must be dropped entirely (a key column was dropped).
    indexes_to_drop: Vec<ultrasql_catalog::IndexEntry>,
    /// Surviving indexes with their `columns` already shifted to the new
    /// post-compaction positions.
    indexes_to_shift: Vec<ultrasql_catalog::IndexEntry>,
    /// Constraints (UNIQUE / PRIMARY KEY / CHECK) that must be dropped
    /// because their key / referenced columns included the dropped column.
    constraints_to_drop: Vec<ultrasql_catalog::persistent::ConstraintRow>,
    /// Surviving constraints with their `conkey` already shifted.
    constraints_to_shift: Vec<ultrasql_catalog::persistent::ConstraintRow>,
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
        &mut self,
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
        // Transactional-DDL milestone 4: when an explicit transaction is open
        // and this is one of the catalog-only sub-actions the gate
        // (`alter_table_is_txn_safe`) admitted, stage the effects in the
        // session overlay bound to the user xid rather than mutating the global
        // catalog. `None` here means autocommit — the legacy self-committing
        // dispatch below runs byte-for-byte unchanged.
        //
        // An ALTER issued while a SAVEPOINT is active is out of scope for the
        // same reason `CREATE TABLE` rejects it (the durable rows ride the
        // parent xid; `ROLLBACK TO SAVEPOINT` could not undo them) → `0A000`.
        //
        // `Failed` cannot reach here (the dispatcher rejects statements in a
        // failed block before execution); autocommit / idle fall through to the
        // legacy self-committing dispatch below.
        if let crate::TxnState::InTransaction(txn) = &self.txn_state {
            if txn.subtxn_stack.depth() > 0 {
                return Err(self.fail_if_in_transaction(ServerError::DdlInTransaction));
            }
            let user_xid = txn.xid;
            return self.execute_alter_table_in_txn(table_name, action, snapshot, user_xid);
        }
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
            LogicalAlterTableAction::AddCheckConstraint { constraint } => {
                self.execute_alter_add_check_constraint(table_name, constraint, snapshot)
            }
            LogicalAlterTableAction::DropConstraint {
                name,
                if_exists,
                cascade,
            } => {
                self.execute_alter_drop_constraint(table_name, name, *if_exists, *cascade, snapshot)
            }
            LogicalAlterTableAction::AlterColumnSetNotNull {
                column_index,
                column_name,
            } => self.execute_alter_column_set_not_null(
                table_name,
                *column_index,
                column_name,
                snapshot,
            ),
            LogicalAlterTableAction::AlterColumnDropNotNull {
                column_index,
                column_name,
            } => self.execute_alter_column_drop_not_null(
                table_name,
                *column_index,
                column_name,
                snapshot,
            ),
            LogicalAlterTableAction::AlterColumnSetDefault {
                column_index,
                column_name,
                default,
            } => self.execute_alter_column_set_default(
                table_name,
                *column_index,
                column_name,
                default.clone(),
                snapshot,
            ),
            LogicalAlterTableAction::AlterColumnDropDefault {
                column_index,
                column_name,
            } => self.execute_alter_column_drop_default(
                table_name,
                *column_index,
                column_name,
                snapshot,
            ),
        }
    }

    /// Execute `ALTER TABLE t ALTER [COLUMN] c SET NOT NULL`.
    ///
    /// 1. Scan every visible row; the first NULL in the target column
    ///    aborts with `23502` (`not_null_violation`) naming the column,
    ///    so the flag is never set against data that already violates it.
    /// 2. Rebuild the schema with the column's `nullable` flag cleared
    ///    and publish it through [`MutableCatalog::alter_table_replace_schema`],
    ///    persisting the new `pg_attribute` rows so the `NOT NULL` flag
    ///    survives restart. Subsequent INSERT/UPDATE enforcement is the
    ///    existing schema-driven `check_not_null_violations` path.
    fn execute_alter_column_set_not_null(
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
        let field = entry.schema.field(column_index).ok_or_else(|| {
            ServerError::ddl(format!(
                "ALTER TABLE SET NOT NULL: column index {column_index} out of bounds for {table_name}"
            ))
        })?;
        // Already NOT NULL — nothing to validate or persist.
        if !field.nullable {
            return Ok(run_ddl_command("ALTER TABLE"));
        }

        // Validate existing data: no visible row may carry NULL.
        let validate_txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let validate_result = (|| -> Result<(), ServerError> {
            let rel = RelationId(entry.oid);
            let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
            let codec = ultrasql_executor::RowCodec::new(entry.schema.clone());
            let scan = self.state.heap.scan_visible(
                rel,
                block_count,
                &validate_txn.snapshot,
                self.state.txn_manager.as_ref(),
            );
            for result in scan {
                let tup = result
                    .map_err(|e| ServerError::ddl(format!("ALTER TABLE SET NOT NULL scan: {e}")))?;
                let row = codec.decode(&tup.data).map_err(|e| {
                    ServerError::ddl(format!("ALTER TABLE SET NOT NULL decode: {e}"))
                })?;
                if matches!(row.get(column_index), Some(Value::Null)) {
                    return Err(ServerError::Execute(
                        ultrasql_executor::ExecError::NotNullViolation(column_name.to_owned()),
                    ));
                }
            }
            Ok(())
        })();
        if let Err(e) = validate_result {
            return Err(self.rollback_transaction_after_error(
                validate_txn,
                e,
                "ALTER TABLE SET NOT NULL rollback after existing-row validation",
            ));
        }
        self.state
            .commit_transaction(validate_txn, true, "ALTER TABLE SET NOT NULL validation")?;

        self.replace_column_nullability(table_name, column_index, false, snapshot)
    }

    /// Execute `ALTER TABLE t ALTER [COLUMN] c DROP NOT NULL`.
    ///
    /// Rejects the change when the column participates in a PRIMARY KEY
    /// (`42P16`, `invalid_table_definition`), matching PostgreSQL.
    /// Otherwise sets the column's `nullable` flag and persists the new
    /// schema so NULLs are allowed afterward and across restart.
    fn execute_alter_column_drop_not_null(
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
        let field = entry.schema.field(column_index).ok_or_else(|| {
            ServerError::ddl(format!(
                "ALTER TABLE DROP NOT NULL: column index {column_index} out of bounds for {table_name}"
            ))
        })?;
        if column_in_primary_key(snapshot, entry.oid, column_index) {
            return Err(ServerError::InvalidTableDefinition(format!(
                "column \"{column_name}\" is in a primary key"
            )));
        }
        // Already nullable — nothing to persist.
        if field.nullable {
            return Ok(run_ddl_command("ALTER TABLE"));
        }
        self.replace_column_nullability(table_name, column_index, true, snapshot)
    }

    /// Rebuild a table's schema with a single column's nullability flag
    /// changed, publish it, and persist the new `pg_attribute` rows so
    /// the change survives restart.
    fn replace_column_nullability(
        &self,
        table_name: &str,
        column_index: usize,
        nullable: bool,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let entry = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        let mut new_fields: Vec<ultrasql_core::Field> = entry.schema.fields().to_vec();
        let target = new_fields.get_mut(column_index).ok_or_else(|| {
            ServerError::ddl(format!(
                "ALTER TABLE ALTER COLUMN: column index {column_index} out of bounds for {table_name}"
            ))
        })?;
        target.nullable = nullable;
        let new_schema = ultrasql_core::Schema::new(new_fields).map_err(|e| {
            ServerError::Catalog(ultrasql_catalog::CatalogError::schema_conflict(format!(
                "ALTER TABLE ALTER COLUMN: {e}"
            )))
        })?;
        // Preserve `pg_attribute.atthasdef` so a column's stored default
        // is not silently forgotten when the schema is re-persisted.
        let attr_has_defaults = if let Some(runtime) = self.state.table_constraints.get(&entry.oid)
        {
            alter_attr_has_defaults(Some(runtime.value().as_ref()), new_schema.len())
        } else {
            alter_attr_has_defaults(None, new_schema.len())
        };
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let updated_entry = self
            .state
            .persistent_catalog
            .alter_table_replace_schema(table_name, new_schema)
            .map_err(ServerError::Catalog)?;
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
                "ALTER TABLE ALTER COLUMN catalog rollback after persist error",
            ));
        }
        self.state
            .commit_transaction(txn, true, "ALTER TABLE ALTER COLUMN nullability")?;
        self.state.plan_cache.invalidate_all();
        Ok(run_ddl_command("ALTER TABLE"))
    }

    /// Execute `ALTER TABLE t ALTER [COLUMN] c SET DEFAULT <expr>`.
    ///
    /// Stores the bound default on the table's runtime constraints side
    /// map (indexed by column position) and flushes the runtime metadata
    /// so future INSERTs that omit the column use it — and the default
    /// survives restart. Existing rows are not changed (PostgreSQL
    /// semantics).
    fn execute_alter_column_set_default(
        &self,
        table_name: &str,
        column_index: usize,
        column_name: &str,
        default: ScalarExpr,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        self.update_column_default(
            table_name,
            column_index,
            column_name,
            Some(default),
            snapshot,
        )
    }

    /// Execute `ALTER TABLE t ALTER [COLUMN] c DROP DEFAULT`.
    ///
    /// Clears the column's stored default. Future INSERTs that omit the
    /// column get NULL (or fail `23502` if the column is `NOT NULL`).
    fn execute_alter_column_drop_default(
        &self,
        table_name: &str,
        column_index: usize,
        column_name: &str,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        self.update_column_default(table_name, column_index, column_name, None, snapshot)
    }

    /// Set or clear a column's runtime default and persist the change.
    ///
    /// Both `pg_attribute.atthasdef` (re-persisted via a schema
    /// replacement) and the runtime constraints side map are kept in
    /// sync, so catalog views and DML lowering agree and the change
    /// reloads correctly on restart.
    fn update_column_default(
        &self,
        table_name: &str,
        column_index: usize,
        column_name: &str,
        default: Option<ScalarExpr>,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let entry = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        let width = entry.schema.len();
        if column_index >= width {
            return Err(ServerError::ddl(format!(
                "ALTER TABLE ALTER COLUMN DEFAULT: column index {column_index} out of bounds for {table_name}"
            )));
        }
        let _ = column_name;

        // Stage the new runtime side-map entry and make sure it is
        // serializable before any durable write.
        let table_key = ultrasql_catalog::table_lookup_key(&entry.schema_name, &entry.name);
        let previous = self
            .state
            .table_constraints
            .get(&entry.oid)
            .map(|guard| guard.clone());
        let mut runtime = previous
            .as_ref()
            .map(|existing| existing.as_ref().clone())
            .unwrap_or_default();
        // A column-position-indexed vec must cover the target column.
        if runtime.defaults.len() < width {
            runtime.defaults.resize(width, None);
        }
        runtime.defaults[column_index] = default;
        self.state
            .ensure_table_runtime_constraints_metadata_persistable(&table_key, &runtime)?;
        self.state
            .ensure_table_runtime_constraints_metadata_slots_persistable()?;

        // Re-persist the schema so `pg_attribute.atthasdef` matches the
        // new default state. The schema itself is unchanged.
        let attr_has_defaults = alter_attr_has_defaults(Some(&runtime), width);
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        if let Err(e) = self
            .state
            .persistent_catalog
            .persist_table_rows_with_defaults(
                entry,
                &attr_has_defaults,
                self.state.heap.as_ref(),
                txn.xid,
                txn.current_command,
            )
        {
            return Err(self.rollback_catalog_transaction_after_error(
                txn,
                e.into(),
                "ALTER TABLE ALTER COLUMN DEFAULT catalog rollback after persist error",
            ));
        }
        self.state
            .commit_transaction(txn, true, "ALTER TABLE ALTER COLUMN DEFAULT")?;

        // Publish the runtime default, then flush runtime metadata so the
        // default is applied (and survives restart). Roll the in-memory
        // side map back if the metadata flush fails.
        self.state
            .table_constraints
            .insert(entry.oid, Arc::new(runtime));
        if let Err(e) = self.state.persist_table_runtime_constraints_metadata() {
            match previous {
                Some(previous) => {
                    self.state.table_constraints.insert(entry.oid, previous);
                }
                None => {
                    self.state.table_constraints.remove(&entry.oid);
                }
            }
            return Err(e);
        }
        self.plan_cache_invalidate();
        Ok(run_ddl_command("ALTER TABLE"))
    }

    fn execute_alter_add_unique_constraint(
        &mut self,
        table_name: &str,
        constraint: &ultrasql_planner::LogicalUniqueConstraint,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let table = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        self.reject_duplicate_constraint_name(table.oid, &constraint.name)?;
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
        // Building the unique index scans every existing row; a
        // pre-existing duplicate aborts the build. Surface that as a
        // PostgreSQL-shaped 23505 naming the constraint rather than the
        // generic DDL error the index builder raises.
        self.execute_create_index(&create_index, snapshot)
            .map_err(|e| map_index_build_duplicate(e, &constraint.name))?;
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

    /// Reject an `ADD CONSTRAINT` whose name is already taken on the
    /// table with SQLSTATE `42710` (`duplicate_object`), matching
    /// PostgreSQL.
    fn reject_duplicate_constraint_name(
        &self,
        table_oid: ultrasql_core::Oid,
        constraint_name: &str,
    ) -> Result<(), ServerError> {
        if self
            .state
            .persistent_catalog
            .lookup_constraint_by_name(table_oid, constraint_name)
            .is_some()
        {
            return Err(ServerError::DuplicateObject(format!(
                "constraint \"{constraint_name}\" for relation already exists"
            )));
        }
        Ok(())
    }

    /// Execute `ALTER TABLE t ADD CONSTRAINT name CHECK (expr)`.
    ///
    /// 1. Reject a duplicate constraint name (`42710`).
    /// 2. Take a per-statement MVCC snapshot and evaluate the bound
    ///    predicate against every visible row; the first row that does
    ///    not satisfy it aborts with `23514` (`check_violation`) naming
    ///    the constraint, so the constraint is never added against data
    ///    that already violates it.
    /// 3. Persist the `pg_constraint` row, append the bound predicate to
    ///    the table's runtime constraint side map, and flush the runtime
    ///    metadata so the CHECK survives restart and is enforced by all
    ///    subsequent DML.
    fn execute_alter_add_check_constraint(
        &self,
        table_name: &str,
        constraint: &ultrasql_planner::LogicalCheckConstraint,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let table = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        self.reject_duplicate_constraint_name(table.oid, &constraint.name)?;

        // Validate existing data: every visible row must satisfy the
        // predicate before the constraint is allowed to exist.
        let validate_txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let validate_result = (|| -> Result<(), ServerError> {
            let rel = RelationId(table.oid);
            let block_count = self.state.heap.block_count(rel).max(table.n_blocks);
            let codec = ultrasql_executor::RowCodec::new(table.schema.clone());
            let evaluator = ultrasql_executor::Eval::new(constraint.expr.clone());
            let scan = self.state.heap.scan_visible(
                rel,
                block_count,
                &validate_txn.snapshot,
                self.state.txn_manager.as_ref(),
            );
            for result in scan {
                let tup = result
                    .map_err(|e| ServerError::ddl(format!("ALTER TABLE ADD CHECK scan: {e}")))?;
                let row = codec
                    .decode(&tup.data)
                    .map_err(|e| ServerError::ddl(format!("ALTER TABLE ADD CHECK decode: {e}")))?;
                match evaluator
                    .eval(&row)
                    .map_err(ultrasql_executor::eval_error_to_exec_error)
                    .map_err(ServerError::Execute)?
                {
                    Value::Bool(true) | Value::Null => {}
                    Value::Bool(false) => {
                        return Err(ServerError::Execute(
                            ultrasql_executor::ExecError::CheckViolation(constraint.name.clone()),
                        ));
                    }
                    other => {
                        return Err(ServerError::ddl(format!(
                            "ALTER TABLE ADD CHECK '{}' evaluated to non-boolean {other:?}",
                            constraint.name
                        )));
                    }
                }
            }
            Ok(())
        })();
        if let Err(e) = validate_result {
            return Err(self.rollback_transaction_after_error(
                validate_txn,
                e,
                "ALTER TABLE ADD CHECK rollback after existing-row validation",
            ));
        }
        // The validation scan only read rows; commit so the XID does not
        // leak as in-progress.
        self.state
            .commit_transaction(validate_txn, true, "ALTER TABLE ADD CHECK validation")?;

        // Stage the new runtime side-map entry and make sure it is
        // serializable before any durable write.
        let table_key = ultrasql_catalog::table_lookup_key(&table.schema_name, &table.name);
        let previous = self
            .state
            .table_constraints
            .get(&table.oid)
            .map(|guard| guard.clone());
        let mut runtime = previous
            .as_ref()
            .map(|existing| existing.as_ref().clone())
            .unwrap_or_default();
        runtime.checks.push(crate::RuntimeCheckConstraint {
            name: constraint.name.clone(),
            expr: constraint.expr.clone(),
        });
        self.state
            .ensure_table_runtime_constraints_metadata_persistable(&table_key, &runtime)?;
        self.state
            .ensure_table_runtime_constraints_metadata_slots_persistable()?;

        let constraint_row = ultrasql_catalog::persistent::ConstraintRow {
            oid: self.state.persistent_catalog.next_oid(),
            conname: constraint.name.clone(),
            conrelid: table.oid,
            contype: ultrasql_catalog::persistent::ConType::Check,
            condeferrable: false,
            condeferred: false,
            conkey: Vec::new(),
            confrelid: ultrasql_core::Oid::INVALID,
            confkey: Vec::new(),
        };
        let ddl_txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        if let Err(e) = self.state.persistent_catalog.persist_constraint_row(
            &constraint_row,
            self.state.heap.as_ref(),
            ddl_txn.xid,
            ddl_txn.current_command,
        ) {
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e.into(),
                "ALTER TABLE ADD CHECK catalog rollback after persist error",
            ));
        }
        self.state.commit_transaction(
            ddl_txn,
            true,
            "ALTER TABLE ADD CHECK catalog transaction",
        )?;

        // Publish the catalog row and the runtime predicate, then flush
        // runtime metadata so the CHECK is enforced after restart.
        self.state
            .persistent_catalog
            .install_constraint_rows([constraint_row]);
        self.state
            .table_constraints
            .insert(table.oid, Arc::new(runtime));
        if let Err(e) = self.state.persist_table_runtime_constraints_metadata() {
            // Roll the in-memory side map back to its prior state so a
            // failed metadata flush does not leave an unpersisted CHECK.
            match previous {
                Some(previous) => {
                    self.state.table_constraints.insert(table.oid, previous);
                }
                None => {
                    self.state.table_constraints.remove(&table.oid);
                }
            }
            return Err(e);
        }
        self.plan_cache_invalidate();
        Ok(run_ddl_command("ALTER TABLE"))
    }

    /// Execute `ALTER TABLE t DROP CONSTRAINT [IF EXISTS] name`.
    ///
    /// Resolves the constraint by name on the table. A missing
    /// constraint is a no-op under `IF EXISTS`, otherwise it raises
    /// `42704` (`undefined_object`). On success the constraint is
    /// removed from `pg_constraint` (with a durable tombstone so the
    /// drop survives restart), its bound CHECK predicate is dropped from
    /// the runtime side map, and any backing unique index is dropped so
    /// enforcement stops on subsequent DML.
    ///
    /// `CASCADE` / `RESTRICT` are parsed and accepted; a constraint drop
    /// here has no dependent objects beyond its own backing index (which
    /// is always removed), so both behave identically.
    fn execute_alter_drop_constraint(
        &self,
        table_name: &str,
        constraint_name: &str,
        if_exists: bool,
        cascade: bool,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let _ = cascade;
        let table = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;
        let Some(row) = self
            .state
            .persistent_catalog
            .lookup_constraint_by_name(table.oid, constraint_name)
        else {
            if if_exists {
                return Ok(run_ddl_command("ALTER TABLE"));
            }
            return Err(ServerError::UndefinedObject(format!(
                "constraint \"{constraint_name}\" of relation \"{}\" does not exist",
                table.name
            )));
        };

        // A PRIMARY KEY / UNIQUE / EXCLUSION constraint is backed by an
        // index that uses the constraint name; it must be dropped in the
        // same catalog transaction so unique enforcement stops. The
        // backing index lives in the table's schema namespace under the
        // constraint name.
        let backing_index = matches!(
            row.contype,
            ultrasql_catalog::persistent::ConType::PrimaryKey
                | ultrasql_catalog::persistent::ConType::Unique
                | ultrasql_catalog::persistent::ConType::Exclusion
        )
        .then(|| {
            self.state
                .persistent_catalog
                .lookup_index(&ultrasql_catalog::index_lookup_key(
                    &table.schema_name,
                    &row.conname,
                ))
        })
        .flatten();

        let ddl_txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let persist_result = (|| -> Result<(), ServerError> {
            self.state
                .persistent_catalog
                .persist_constraint_drop_tombstone(
                    row.oid,
                    table.oid,
                    &row.conname,
                    self.state.heap.as_ref(),
                    ddl_txn.xid,
                    ddl_txn.current_command,
                )?;
            if let Some(index) = &backing_index {
                self.state.persistent_catalog.persist_index_drop_tombstone(
                    index,
                    self.state.heap.as_ref(),
                    ddl_txn.xid,
                    ddl_txn.current_command,
                )?;
            }
            Ok(())
        })();
        if let Err(e) = persist_result {
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e,
                "ALTER TABLE DROP CONSTRAINT catalog rollback after tombstone error",
            ));
        }
        self.state.commit_transaction(
            ddl_txn,
            true,
            "ALTER TABLE DROP CONSTRAINT catalog transaction",
        )?;
        self.state.persistent_catalog.remove_constraint(row.oid);
        if let Some(index) = &backing_index {
            self.state
                .persistent_catalog
                .clear_descriptions_for_object(index.oid);
            self.state
                .persistent_catalog
                .drop_index(&ultrasql_catalog::index_lookup_key(
                    &index.schema_name,
                    &index.name,
                ))?;
        }

        // Drop a bound CHECK predicate from the runtime side map and
        // flush the change so enforcement stops after restart too.
        if matches!(row.contype, ultrasql_catalog::persistent::ConType::Check) {
            let previous = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|guard| guard.clone());
            if let Some(previous) = previous {
                let mut runtime = previous.as_ref().clone();
                let before = runtime.checks.len();
                let folded = constraint_name.to_ascii_lowercase();
                runtime
                    .checks
                    .retain(|check| check.name.to_ascii_lowercase() != folded);
                if runtime.checks.len() != before {
                    self.state
                        .table_constraints
                        .insert(table.oid, Arc::new(runtime));
                    if let Err(e) = self.state.persist_table_runtime_constraints_metadata() {
                        self.state.table_constraints.insert(table.oid, previous);
                        return Err(e);
                    }
                }
            }
        }

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

    /// Classify the table's dependent indexes and position-referencing
    /// constraints into the ones that must be dropped (a key column equals the
    /// dropped column) and the ones that survive with shifted positions.
    ///
    /// `column_index` is the 0-based position being dropped; index `columns`
    /// are 0-based attnums and constraint `conkey` entries are 1-based attnums.
    /// A surviving entry decrements every position strictly greater than the
    /// dropped one, matching the physical heap compaction.
    fn plan_drop_column_dependents(
        &self,
        table_oid: ultrasql_core::Oid,
        column_index: usize,
        snapshot: &CatalogSnapshot,
    ) -> DropColumnDependents {
        let dropped_attnum_0 = u16::try_from(column_index).unwrap_or(u16::MAX);
        let dropped_attnum_1 = i16::try_from(column_index.saturating_add(1)).unwrap_or(i16::MAX);
        let mut plan = DropColumnDependents::default();

        if let Some(indexes) = snapshot.indexes_by_table.get(&table_oid) {
            for index in indexes {
                if index.columns.contains(&dropped_attnum_0) {
                    plan.indexes_to_drop.push(index.clone());
                } else if index.columns.iter().any(|&c| c > dropped_attnum_0) {
                    let mut shifted = index.clone();
                    for col in &mut shifted.columns {
                        if *col > dropped_attnum_0 {
                            *col -= 1;
                        }
                    }
                    plan.indexes_to_shift.push(shifted);
                }
            }
        }

        // CHECK predicates carry no `conkey` (it is persisted empty); their
        // column dependency lives in the bound runtime expression. Collect the
        // (case-folded) names of CHECKs that reference the dropped column so the
        // matching `pg_constraint` rows are tombstoned (PostgreSQL drops a CHECK
        // that references a dropped column).
        let dropped_check_names: std::collections::HashSet<String> = self
            .state
            .table_constraints
            .get(&table_oid)
            .map(|guard| {
                guard
                    .checks
                    .iter()
                    .filter(|check| scalar_expr_references_column(&check.expr, column_index))
                    .map(|check| check.name.to_ascii_lowercase())
                    .collect()
            })
            .unwrap_or_default();

        for row in snapshot.constraints.values() {
            if row.conrelid != table_oid {
                continue;
            }
            // Only key-referencing constraint kinds carry positional `conkey`
            // entries that the compaction can invalidate. FK `conkey` on the
            // referencing side is out of scope (documented for the gate).
            if !matches!(
                row.contype,
                ultrasql_catalog::persistent::ConType::PrimaryKey
                    | ultrasql_catalog::persistent::ConType::Unique
                    | ultrasql_catalog::persistent::ConType::Check
                    | ultrasql_catalog::persistent::ConType::Exclusion
            ) {
                continue;
            }
            // A CHECK referencing the dropped column (detected from its bound
            // expression, since `conkey` is empty) is dropped outright.
            if matches!(row.contype, ultrasql_catalog::persistent::ConType::Check)
                && dropped_check_names.contains(&row.conname.to_ascii_lowercase())
            {
                plan.constraints_to_drop.push(row.clone());
                continue;
            }
            if row.conkey.contains(&dropped_attnum_1) {
                plan.constraints_to_drop.push(row.clone());
            } else if row.conkey.iter().any(|&k| k > dropped_attnum_1) {
                let mut shifted = row.clone();
                for key in &mut shifted.conkey {
                    if *key > dropped_attnum_1 {
                        *key -= 1;
                    }
                }
                plan.constraints_to_shift.push(shifted);
            }
        }

        plan
    }

    /// Persist the durable catalog effects of [`Self::plan_drop_column_dependents`]
    /// into the in-flight DROP COLUMN transaction: a `pg_class` tombstone for
    /// each dropped index, a re-persisted `pg_index` row carrying the shifted
    /// key for each survivor, and `pg_constraint` tombstone / re-persist rows.
    ///
    /// Riding the same `xid` / `command_id` as the schema replacement makes the
    /// whole adjustment atomic with the column drop: a COMMIT makes it durable,
    /// a ROLLBACK (or restart before commit) reconstructs the pre-drop state.
    fn persist_drop_column_dependents(
        &self,
        plan: &DropColumnDependents,
        xid: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), ServerError> {
        let heap = self.state.heap.as_ref();
        for index in &plan.indexes_to_drop {
            self.state
                .persistent_catalog
                .persist_index_drop_tombstone(index, heap, xid, command_id)
                .map_err(ServerError::Catalog)?;
        }
        for index in &plan.indexes_to_shift {
            // `pg_index` is append-only and bootstrap keeps the latest row per
            // index OID, so a fresh row with the shifted `indkey` re-points the
            // survivor durably without disturbing its built `root_block`.
            self.state
                .persistent_catalog
                .persist_index_rows(index, heap, xid, command_id)
                .map_err(ServerError::Catalog)?;
        }
        for row in &plan.constraints_to_drop {
            self.state
                .persistent_catalog
                .persist_constraint_drop_tombstone(
                    row.oid,
                    row.conrelid,
                    &row.conname,
                    heap,
                    xid,
                    command_id,
                )
                .map_err(ServerError::Catalog)?;
        }
        for row in &plan.constraints_to_shift {
            self.state
                .persistent_catalog
                .persist_constraint_row(row, heap, xid, command_id)
                .map_err(ServerError::Catalog)?;
        }
        Ok(())
    }

    /// Apply the in-memory catalog half of [`Self::plan_drop_column_dependents`]
    /// after the DROP COLUMN transaction has durably committed: drop the dead
    /// indexes, re-point the survivors, and reconcile `pg_constraint`. Mirrors
    /// the post-commit catalog teardown in `execute_alter_drop_constraint`.
    fn apply_drop_column_dependents(&self, plan: &DropColumnDependents) {
        let catalog = &self.state.persistent_catalog;
        for index in &plan.indexes_to_drop {
            catalog.clear_descriptions_for_object(index.oid);
            let key = ultrasql_catalog::index_lookup_key(&index.schema_name, &index.name);
            if let Err(e) = catalog.drop_index(&key) {
                tracing::error!(
                    error = %e,
                    index = %index.name,
                    "ALTER TABLE DROP COLUMN: removing dependent index from catalog failed; \
                     the durable tombstone is authoritative and a restart rebuilds the same state"
                );
            }
        }
        for index in &plan.indexes_to_shift {
            // No in-place index mutator exists; drop the stale by-name/by-table
            // entries then re-register the shifted clone (same OID/root_block).
            let key = ultrasql_catalog::index_lookup_key(&index.schema_name, &index.name);
            let _ = catalog.drop_index(&key);
            if let Err(e) = catalog.create_index(index.clone()) {
                tracing::error!(
                    error = %e,
                    index = %index.name,
                    "ALTER TABLE DROP COLUMN: re-pointing dependent index failed; \
                     the durable pg_index row is authoritative and a restart rebuilds it"
                );
            }
        }
        for row in &plan.constraints_to_drop {
            catalog.remove_constraint(row.oid);
        }
        if !plan.constraints_to_shift.is_empty() {
            catalog.install_constraint_rows(plan.constraints_to_shift.iter().cloned());
        }
    }

    /// Re-index the per-column runtime metadata (defaults, generated-column
    /// expressions, SERIAL sequence bindings, identity flags) to the shifted
    /// positions, and drop any bound CHECK predicate whose constraint was
    /// dropped because it referenced the removed column.
    ///
    /// The persistent compaction (`persist_table_schema_replacement`) already
    /// narrows `pg_attribute`, but the position-keyed runtime side map is a
    /// separate artifact the existing compaction never touched — leaving it
    /// misaligned would mis-apply defaults / generated expressions to the wrong
    /// column on the next INSERT.
    fn compact_runtime_constraints_after_drop_column(
        &self,
        table_oid: ultrasql_core::Oid,
        table_name: &str,
        column_index: usize,
        plan: &DropColumnDependents,
    ) {
        let Some(previous) = self
            .state
            .table_constraints
            .get(&table_oid)
            .map(|guard| guard.clone())
        else {
            return;
        };
        let mut runtime = previous.as_ref().clone();
        let mut changed = false;
        // Drop the dropped column's slot from every per-column vector.
        changed |= remove_at(&mut runtime.defaults, column_index);
        changed |= remove_at(&mut runtime.sequence_defaults, column_index);
        changed |= remove_at(&mut runtime.identity_always, column_index);
        changed |= remove_at(&mut runtime.generated_stored, column_index);
        // Re-point any surviving bound DEFAULT / generated-column expression at
        // the compacted positions (they reference other columns by index).
        for default in runtime.defaults.iter_mut().flatten() {
            scalar_expr_shift_columns(default, column_index);
            changed = true;
        }
        for generated in runtime.generated_stored.iter_mut().flatten() {
            scalar_expr_shift_columns(generated, column_index);
            changed = true;
        }
        // Drop bound CHECK predicates whose constraint was dropped (its
        // expression references the removed column by old position).
        let dropped_checks: std::collections::HashSet<String> = plan
            .constraints_to_drop
            .iter()
            .filter(|row| matches!(row.contype, ultrasql_catalog::persistent::ConType::Check))
            .map(|row| row.conname.to_ascii_lowercase())
            .collect();
        if !dropped_checks.is_empty() {
            let before = runtime.checks.len();
            runtime
                .checks
                .retain(|check| !dropped_checks.contains(&check.name.to_ascii_lowercase()));
            changed |= runtime.checks.len() != before;
        }
        // Re-point the SURVIVING CHECK predicates at the compacted positions.
        for check in &mut runtime.checks {
            scalar_expr_shift_columns(&mut check.expr, column_index);
            changed = true;
        }
        if !changed {
            return;
        }
        self.state
            .table_constraints
            .insert(table_oid, Arc::new(runtime));
        if let Err(e) = self.state.persist_table_runtime_constraints_metadata() {
            // The schema is already committed narrower; restore the prior side
            // map so DML keeps using consistent (if slightly stale) metadata and
            // surface the flush failure rather than silently corrupting it.
            self.state.table_constraints.insert(table_oid, previous);
            tracing::error!(
                error = %e,
                table = %table_name,
                "ALTER TABLE DROP COLUMN: flushing re-indexed runtime constraints failed"
            );
        }
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

        // Classify dependent indexes / constraints against the OLD positions
        // (the `snapshot` still describes the pre-drop schema) BEFORE any
        // catalog mutation, so we know which to drop and which to re-point.
        let dependents = self.plan_drop_column_dependents(entry.oid, column_index, snapshot);

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
                // Drop / re-point dependent indexes and constraints durably in
                // the SAME transaction so the adjustment is atomic with the
                // column drop (commits together, rolls back together).
                if let Err(e) =
                    self.persist_drop_column_dependents(&dependents, txn.xid, txn.current_command)
                {
                    return Err(self.rollback_catalog_transaction_after_error(
                        txn,
                        e,
                        "ALTER TABLE DROP COLUMN catalog rollback after dependent index/constraint \
                         persist error",
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
                // The durable side committed; reconcile the in-memory catalog
                // (drop dead indexes, re-point survivors, prune constraints)
                // and re-index the per-column runtime metadata to the shift.
                self.apply_drop_column_dependents(&dependents);
                self.compact_runtime_constraints_after_drop_column(
                    updated_entry.oid,
                    &updated_entry.name,
                    column_index,
                    &dependents,
                );
                // The heap rewrite re-stamped every surviving row as a new
                // tuple version, orphaning the existing index leaves on the dead
                // pre-images. Repopulate the survivors' btree/hash pages from the
                // committed heap so UNIQUE / PRIMARY KEY enforcement resumes at
                // once — otherwise the next duplicate INSERT slips past the 23505
                // check and later aborts the restart's index rebuild, leaving the
                // server unable to boot.
                self.state
                    .repopulate_table_btree_indexes_after_drop_column(&updated_entry);
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

/// Return whether the 0-based `column_index` participates in a live
/// PRIMARY KEY constraint on the table.
///
/// `pg_constraint.conkey` stores 1-based attribute numbers, so the
/// target column matches `column_index + 1`. Used to reject
/// `DROP NOT NULL` on a primary-key column, as PostgreSQL does.
pub(super) fn column_in_primary_key(
    snapshot: &CatalogSnapshot,
    table_oid: ultrasql_core::Oid,
    column_index: usize,
) -> bool {
    let Ok(attnum) = i16::try_from(column_index + 1) else {
        return false;
    };
    snapshot.constraints.values().any(|row| {
        row.conrelid == table_oid
            && matches!(
                row.contype,
                ultrasql_catalog::persistent::ConType::PrimaryKey
            )
            && row.conkey.contains(&attnum)
    })
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

pub(super) fn alter_attr_has_defaults(
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

/// Translate the generic DDL error the unique-index builder raises on a
/// pre-existing duplicate into a PostgreSQL-shaped `UniqueViolation`
/// (SQLSTATE `23505`) naming the constraint. Other errors pass through.
fn map_index_build_duplicate(err: ServerError, constraint_name: &str) -> ServerError {
    if let ServerError::Ddl(message) = &err
        && (message.contains("duplicate key") || message.contains("DuplicateKey"))
    {
        return ServerError::Execute(ultrasql_executor::ExecError::UniqueViolation(
            constraint_name.to_owned(),
        ));
    }
    err
}

/// Whether a bound scalar expression references the table column at 0-based
/// `column_index` through any [`ScalarExpr::Column`]. CHECK predicates bind to
/// the table row schema, so a positive answer means the CHECK depends on the
/// column being dropped (PostgreSQL drops such a CHECK).
fn scalar_expr_references_column(expr: &ScalarExpr, column_index: usize) -> bool {
    match expr {
        ScalarExpr::Column { index, .. } => *index == column_index,
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            scalar_expr_references_column(expr, column_index)
        }
        ScalarExpr::Binary { left, right, .. } => {
            scalar_expr_references_column(left, column_index)
                || scalar_expr_references_column(right, column_index)
        }
        ScalarExpr::FunctionCall { args, .. } => args
            .iter()
            .any(|arg| scalar_expr_references_column(arg, column_index)),
        // A table CHECK predicate cannot contain subqueries / parameters /
        // outer columns; those variants carry no table-column reference to
        // shift, so they are inert here.
        ScalarExpr::Literal { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => false,
    }
}

/// Decrement every [`ScalarExpr::Column`] index strictly greater than
/// `dropped_index` by one, re-pointing a surviving CHECK predicate at the
/// compacted schema. Assumes the expression does not itself reference the
/// dropped column (callers drop those CHECKs instead of shifting them).
fn scalar_expr_shift_columns(expr: &mut ScalarExpr, dropped_index: usize) {
    match expr {
        ScalarExpr::Column { index, .. } => {
            if *index > dropped_index {
                *index -= 1;
            }
        }
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            scalar_expr_shift_columns(expr, dropped_index);
        }
        ScalarExpr::Binary { left, right, .. } => {
            scalar_expr_shift_columns(left, dropped_index);
            scalar_expr_shift_columns(right, dropped_index);
        }
        ScalarExpr::FunctionCall { args, .. } => {
            for arg in args {
                scalar_expr_shift_columns(arg, dropped_index);
            }
        }
        ScalarExpr::Literal { .. }
        | ScalarExpr::Parameter { .. }
        | ScalarExpr::OuterColumn { .. }
        | ScalarExpr::ScalarSubquery { .. }
        | ScalarExpr::Exists { .. }
        | ScalarExpr::InSubquery { .. } => {}
    }
}

/// Remove the element at `index` from a per-column runtime vector, returning
/// whether a removal happened. An out-of-range index (the vector is shorter
/// than the schema, e.g. trailing-default elision) is a no-op.
fn remove_at<T>(vec: &mut Vec<T>, index: usize) -> bool {
    if index < vec.len() {
        vec.remove(index);
        true
    } else {
        false
    }
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
