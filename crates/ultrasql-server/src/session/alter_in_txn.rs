//! In-transaction `ALTER TABLE` for the catalog-only sub-action subset
//! (transactional-DDL milestone 4).
//!
//! When one of the seven catalog-only `ALTER TABLE` sub-actions
//! (`RENAME TO` / `RENAME COLUMN` / `ALTER COLUMN SET|DROP DEFAULT` /
//! `ALTER COLUMN SET|DROP NOT NULL` / `SET (...)`) runs inside an explicit
//! `BEGIN … COMMIT` block, the global catalog must NOT be mutated mid-statement
//! (a concurrent session would observe an uncommitted schema edit, and a
//! `ROLLBACK` could not undo it). Instead the post-ALTER `TableEntry` is staged
//! in the session overlay (folded to OVERRIDE the committed entry), the durable
//! `pg_class` / `pg_attribute` rows are written under the USER xid (NOT
//! committed — the user's COMMIT/ROLLBACK and the visibility-filtered bootstrap
//! decide their fate via MVCC), and the in-memory runtime side maps (defaults /
//! privilege grants) are mutated in place for self-visibility with their
//! pre-ALTER values captured for the rollback revert.
//!
//! On COMMIT the staged op is replayed against the global catalog with the same
//! mutator the autocommit path uses (`commit_pending_catalog_ddl`); on ROLLBACK
//! the staged before-images are restored (`revert_staged_catalog_ddl_side_effects`)
//! and the durable user-xid rows ride the aborted xid (invisible + bootstrap
//! hidden), so the committed pre-ALTER row wins on restart.
//!
//! Out-of-scope shapes (`ADD`/`DROP COLUMN`, `ADD`/`DROP CONSTRAINT`,
//! `ENABLE RLS`, …) keep the gate's `0A000` in-transaction and are unaffected
//! here; a time-partitioned target table is rejected too (its non-MVCC chunk
//! sidecar the overlay cannot transactionally rekey).

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{CatalogSnapshot, TableEntry, table_lookup_key};
use ultrasql_core::{Value, Xid};
use ultrasql_planner::{LogicalAlterTableAction, ScalarExpr};

use super::Session;
use super::alter::{alter_attr_has_defaults, column_in_primary_key};
use super::catalog_overlay::{AlterTableOp, AlteredSideEffects, CatalogOverlay};
use crate::auth::PrivilegeObjectKind;
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Dispatch one catalog-only `ALTER TABLE` sub-action onto the
    /// in-transaction staging path. Only the seven sub-actions admitted by
    /// [`Session::alter_table_is_txn_safe`] reach here (the dispatcher routes
    /// everything else to the `0A000` reject before execution).
    pub(crate) fn execute_alter_table_in_txn(
        &mut self,
        table_name: &str,
        action: &LogicalAlterTableAction,
        snapshot: &CatalogSnapshot,
        user_xid: Xid,
    ) -> Result<SelectResult, ServerError> {
        // Resolve the target against the EFFECTIVE snapshot so an ALTER of a
        // table created earlier in this same transaction (which lives only in
        // the overlay, not the committed snapshot) resolves to its overlaid
        // entry. `snapshot` (the per-statement snapshot) already is the
        // effective one for this statement, but a prior in-txn ALTER of the
        // SAME table staged an `altered_tables` override that a re-fetch picks
        // up — so resolve through `effective_catalog_snapshot()`.
        let effective = self.effective_catalog_snapshot();
        let entry = effective.tables.get(table_name).cloned().ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.to_owned(),
            ))
        })?;

        // A time-partitioned parent installs a non-MVCC chunk sidecar the
        // whole-transaction overlay cannot transactionally rekey (RENAME) or
        // re-shape (RENAME COLUMN / SET NOT NULL). Reject in-txn with `0A000`;
        // the autocommit path handles it. Cheap key probe.
        let table_key = table_lookup_key(&entry.schema_name, &entry.name);
        if self.state.time_partitions.contains_key(&table_key) {
            return Err(self.fail_if_in_transaction(ServerError::UnsupportedOwned(
                "ALTER TABLE on a time-partitioned table inside an explicit transaction is not \
                 yet supported\nHINT:  run it in autocommit"
                    .to_string(),
            )));
        }

        match action {
            LogicalAlterTableAction::RenameTable { new_name } => {
                self.alter_in_txn_rename_table(&entry, new_name, user_xid)
            }
            LogicalAlterTableAction::RenameColumn {
                column_index,
                new_name,
                ..
            } => self.alter_in_txn_rename_column(&entry, *column_index, new_name, user_xid),
            LogicalAlterTableAction::AlterColumnSetNotNull {
                column_index,
                column_name,
            } => self.alter_in_txn_set_not_null(
                &entry,
                *column_index,
                column_name,
                snapshot,
                user_xid,
            ),
            LogicalAlterTableAction::AlterColumnDropNotNull {
                column_index,
                column_name,
            } => self.alter_in_txn_drop_not_null(&entry, *column_index, column_name, user_xid),
            LogicalAlterTableAction::AlterColumnSetDefault {
                column_index,
                default,
                ..
            } => self.alter_in_txn_set_default(
                &entry,
                *column_index,
                Some(default.clone()),
                user_xid,
            ),
            LogicalAlterTableAction::AlterColumnDropDefault { column_index, .. } => {
                self.alter_in_txn_set_default(&entry, *column_index, None, user_xid)
            }
            LogicalAlterTableAction::SetOptions { options } => {
                self.alter_in_txn_set_options(&entry, options, user_xid)
            }
            // Unreachable: the gate admits only the seven sub-actions above.
            _ => Err(self.fail_if_in_transaction(ServerError::DdlInTransaction)),
        }
    }

    /// Take AccessExclusive on `table_key`, keyed on the user xid (released by
    /// `release_all` at the user COMMIT/ROLLBACK). Non-blocking `try_acquire`
    /// (the engine's lock discipline; parking a tokio worker on a
    /// cross-transaction lock would stall the runtime) — the loser fails with
    /// `40001` so two transactions altering the same relation serialize.
    fn alter_in_txn_lock(&mut self, table_key: &str, user_xid: Xid) -> Result<(), ServerError> {
        let tag = super::ddl::create_table::create_table_name_lock_tag(table_key);
        let acquired = self
            .state
            .txn_manager
            .lock_manager
            .try_acquire(ultrasql_txn::LockRequest {
                xid: user_xid,
                tag,
                mode: ultrasql_txn::LockMode::AccessExclusive,
            })
            .map_err(|e| ServerError::ddl(format!("ALTER TABLE relation lock: {e}")))?;
        if !acquired {
            return Err(
                self.fail_if_in_transaction(ServerError::SerializationFailure(format!(
                    "could not obtain lock on relation \"{table_key}\": another transaction is \
                     altering it concurrently"
                ))),
            );
        }
        Ok(())
    }

    /// The current command id of the active in-transaction. The caller is only
    /// reached from an `InTransaction` state.
    fn alter_in_txn_command_id(&self) -> ultrasql_core::CommandId {
        match &self.txn_state {
            crate::TxnState::InTransaction(txn) => txn.current_command,
            _ => ultrasql_core::CommandId::FIRST,
        }
    }

    /// Append an ALTER's staged effects to the (possibly pre-existing) overlay
    /// and mark the table modified so a durable commit marker is written for the
    /// user xid (making the user-xid catalog rows visible after restart).
    fn alter_in_txn_stage(
        &mut self,
        user_xid: Xid,
        updated_entry: TableEntry,
        staged: AlteredSideEffects,
    ) {
        let table_modify_key = table_lookup_key(&updated_entry.schema_name, &updated_entry.name);
        let oid = updated_entry.oid;
        let overlay = self
            .pending_catalog_ddl
            .get_or_insert_with(|| CatalogOverlay {
                xid: user_xid,
                created_tables: Vec::new(),
                indexes: Vec::new(),
                constraints: Vec::new(),
                extra_indexes: Vec::new(),
                extra_index_constraints: Vec::new(),
                staged: Vec::new(),
                altered_tables: Vec::new(),
                altered_staged: Vec::new(),
            });
        debug_assert_eq!(overlay.xid, user_xid);
        // Deliberately DO NOT mutate `created_tables` for a same-txn-created
        // target: `created_tables` carries the PURE-CREATE state, and the COMMIT
        // path publishes those entries first and THEN replays each ALTER op on
        // the global catalog (mirroring autocommit's create-then-alter order).
        // Mutating `created_tables` here would make the publish file the table
        // under its post-ALTER name, leaving the `Rename` replay's `old_name`
        // un-findable. Self-visibility is unaffected: the overlay fold drops the
        // `created_tables` entry by OID and re-inserts this `altered_tables`
        // override (post-ALTER name/schema), so the issuing session resolves the
        // altered shape regardless.
        //
        // The latest post-ALTER entry for an OID wins the overlay fold; drop any
        // prior staged override for this OID so the fold inserts exactly one.
        overlay.altered_tables.retain(|t| t.oid != oid);
        overlay.altered_tables.push(updated_entry);
        overlay.altered_staged.push(staged);
        self.pending_table_modifications
            .entry(table_modify_key)
            .or_insert(0);
        // A staged schema/name edit can shadow a cached plan bound against the
        // pre-ALTER snapshot; clear the session bind cache so the next statement
        // re-resolves through the overlay-folded snapshot.
        self.plan_cache_invalidate();
    }

    /// `ALTER TABLE … RENAME TO new_name` staged in-txn.
    fn alter_in_txn_rename_table(
        &mut self,
        entry: &TableEntry,
        new_name: &str,
        user_xid: Xid,
    ) -> Result<SelectResult, ServerError> {
        let old_key = table_lookup_key(&entry.schema_name, &entry.name);
        let new_key = table_lookup_key(&entry.schema_name, new_name);
        // Reject a name collision against the effective (overlay-folded) view so
        // a same-txn-created sibling is honoured.
        if self
            .effective_catalog_snapshot()
            .tables
            .contains_key(&new_key)
        {
            return Err(self.fail_if_in_transaction(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(new_name.to_owned()),
            )));
        }
        // Lock BOTH the old and the new qualified name so a concurrent
        // CREATE/ALTER of either serializes against this rename.
        self.alter_in_txn_lock(&old_key, user_xid)?;
        self.alter_in_txn_lock(&new_key, user_xid)?;
        self.state
            .ensure_create_relation_metadata_slots_persistable()?;

        let mut updated_entry = entry.clone();
        updated_entry.name = new_name.to_string();

        // Persist the renamed pg_class / pg_attribute rows under the user xid
        // (NOT committed). `relname` carries the new name; bootstrap keeps the
        // latest row per OID so this supersedes the old-name row once committed.
        let attr_has_defaults = self.alter_in_txn_attr_has_defaults(entry.oid, &updated_entry);
        self.state
            .persistent_catalog
            .persist_table_rows_with_defaults(
                &updated_entry,
                &attr_has_defaults,
                self.state.heap.as_ref(),
                user_xid,
                self.alter_in_txn_command_id(),
            )
            .map_err(|e| self.fail_if_in_transaction(e.into()))?;

        // Rename the privilege grants in-memory (self-visible); capture the
        // pre-ALTER snapshot for the rollback revert. The durable metadata file
        // write is DEFERRED to COMMIT (writing a grant for a renamed table that
        // never commits would dangle after a crash).
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        let privileges_changed = self.state.privilege_catalog.rename_object_grants(
            PrivilegeObjectKind::Table,
            &old_key,
            &new_key,
        );

        let staged = AlteredSideEffects {
            oid: entry.oid,
            op: AlterTableOp::Rename {
                old_name: old_key,
                new_name: new_name.to_string(),
            },
            runtime_constraints_before: None,
            runtime_constraints_changed: false,
            time_partition_before: None,
            time_partition_key_before: String::new(),
            privilege_grants_before: before_grants,
            privilege_default_grants_before: before_default_grants,
            privileges_changed,
        };
        self.alter_in_txn_stage(user_xid, updated_entry, staged);
        Ok(run_ddl_command(&format!(
            "ALTER TABLE RENAME TO {new_name}"
        )))
    }

    /// `ALTER TABLE … RENAME COLUMN old TO new` staged in-txn (catalog-only —
    /// the row codec is positional, no heap rewrite).
    fn alter_in_txn_rename_column(
        &mut self,
        entry: &TableEntry,
        column_index: usize,
        new_name: &str,
        user_xid: Xid,
    ) -> Result<SelectResult, ServerError> {
        let table_key = table_lookup_key(&entry.schema_name, &entry.name);
        self.alter_in_txn_lock(&table_key, user_xid)?;

        let mut new_fields: Vec<ultrasql_core::Field> = entry.schema.fields().to_vec();
        if column_index >= new_fields.len() {
            return Err(self.fail_if_in_transaction(ServerError::ddl(format!(
                "ALTER TABLE RENAME COLUMN: index {column_index} out of bounds for {}",
                entry.name
            ))));
        }
        new_fields[column_index] = ultrasql_core::Field {
            name: new_name.to_string(),
            ..new_fields[column_index].clone()
        };
        let new_schema = ultrasql_core::Schema::new(new_fields).map_err(|e| {
            self.fail_if_in_transaction(ServerError::Catalog(
                ultrasql_catalog::CatalogError::schema_conflict(format!(
                    "ALTER TABLE RENAME COLUMN: {e}"
                )),
            ))
        })?;
        self.alter_in_txn_replace_schema(entry, new_schema, user_xid)?;
        Ok(run_ddl_command(&format!(
            "ALTER TABLE RENAME COLUMN TO {new_name}"
        )))
    }

    /// `ALTER TABLE … ALTER COLUMN c SET NOT NULL` staged in-txn. Validates that
    /// no visible row carries NULL in the column under the USER snapshot — with
    /// `current_command` advanced past the transaction's prior writes so a
    /// same-txn INSERT is self-visible (the DDL dispatch does not refresh the
    /// snapshot for an ALTER, so the raw `txn.snapshot` would hide it) — then
    /// stages the cleared-nullable schema.
    fn alter_in_txn_set_not_null(
        &mut self,
        entry: &TableEntry,
        column_index: usize,
        column_name: &str,
        _snapshot: &CatalogSnapshot,
        user_xid: Xid,
    ) -> Result<SelectResult, ServerError> {
        let field = entry.schema.field(column_index).ok_or_else(|| {
            ServerError::ddl(format!(
                "ALTER TABLE SET NOT NULL: column index {column_index} out of bounds for {}",
                entry.name
            ))
        })?;
        if !field.nullable {
            return Ok(run_ddl_command("ALTER TABLE"));
        }
        let table_key = table_lookup_key(&entry.schema_name, &entry.name);
        self.alter_in_txn_lock(&table_key, user_xid)?;

        // Validate existing + in-txn rows under the user transaction snapshot,
        // with NO inner transaction: the user's own inserts are self-visible and
        // nothing committed concurrently can be (the scan runs before COMMIT).
        //
        // CORRECTNESS (corruption fix): the DDL dispatch does NOT call
        // `refresh_snapshot` for an ALTER, so `txn.snapshot.current_command` is
        // still the PRIOR statement's command id. A row inserted by the
        // immediately-preceding in-txn INSERT carries that SAME command id, and
        // the MVCC self-visibility rule (`cmin >= current_command ⇒ Invisible`)
        // would hide it — the scan would see 0 rows and a same-txn NULL would
        // pass, durably persisting an inconsistent NOT NULL column. Validate
        // instead against the FROZEN MVCC view (same xmin/xmax/xip/own-subxids,
        // so previously-COMMITTED NULLs are still caught and a concurrent
        // session's UNCOMMITTED rows stay invisible) but with `current_command`
        // advanced past the prior writes so our OWN prior in-txn rows become
        // visible — exactly the bump `refresh_snapshot` applies, without
        // mutating the live txn snapshot the rest of this statement + COMMIT
        // depend on.
        let txn_snapshot = match &self.txn_state {
            crate::TxnState::InTransaction(txn) => {
                let mut snap = txn.snapshot.clone();
                snap.current_command = txn.current_command.next();
                snap
            }
            _ => {
                return Err(self.fail_if_in_transaction(ServerError::Unsupported(
                    "ALTER TABLE SET NOT NULL in-txn reached without an active transaction",
                )));
            }
        };
        let validate = (|| -> Result<(), ServerError> {
            let rel = ultrasql_core::RelationId(entry.oid);
            let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
            let codec = ultrasql_executor::RowCodec::new(entry.schema.clone());
            let scan = self.state.heap.scan_visible(
                rel,
                block_count,
                &txn_snapshot,
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
        if let Err(e) = validate {
            return Err(self.fail_if_in_transaction(e));
        }
        let new_schema = self.alter_in_txn_schema_with_nullability(entry, column_index, false)?;
        self.alter_in_txn_replace_schema(entry, new_schema, user_xid)?;
        Ok(run_ddl_command("ALTER TABLE"))
    }

    /// `ALTER TABLE … ALTER COLUMN c DROP NOT NULL` staged in-txn. Rejects a
    /// primary-key column (`42P16`), matching the autocommit path and PostgreSQL.
    fn alter_in_txn_drop_not_null(
        &mut self,
        entry: &TableEntry,
        column_index: usize,
        column_name: &str,
        user_xid: Xid,
    ) -> Result<SelectResult, ServerError> {
        let field = entry.schema.field(column_index).ok_or_else(|| {
            ServerError::ddl(format!(
                "ALTER TABLE DROP NOT NULL: column index {column_index} out of bounds for {}",
                entry.name
            ))
        })?;
        // A PK column can only come from a committed constraint (in-txn
        // PRIMARY KEY rows ride the overlay, but `column_in_primary_key` reads
        // the committed snapshot — the only place a same-txn PK row would be is
        // the overlay's `constraints`, and an ALTER of a same-txn-created PK
        // column is an exotic shape; the effective snapshot's `constraints`
        // carries the overlay rows, so check there).
        if column_in_primary_key(&self.effective_catalog_snapshot(), entry.oid, column_index) {
            return Err(
                self.fail_if_in_transaction(ServerError::InvalidTableDefinition(format!(
                    "column \"{column_name}\" is in a primary key"
                ))),
            );
        }
        if field.nullable {
            return Ok(run_ddl_command("ALTER TABLE"));
        }
        let table_key = table_lookup_key(&entry.schema_name, &entry.name);
        self.alter_in_txn_lock(&table_key, user_xid)?;
        let new_schema = self.alter_in_txn_schema_with_nullability(entry, column_index, true)?;
        self.alter_in_txn_replace_schema(entry, new_schema, user_xid)?;
        Ok(run_ddl_command("ALTER TABLE"))
    }

    /// `ALTER TABLE … ALTER COLUMN c SET|DROP DEFAULT` staged in-txn. The schema
    /// is unchanged; the default lives in the runtime side map and
    /// `pg_attribute.atthasdef` is re-persisted under the user xid.
    fn alter_in_txn_set_default(
        &mut self,
        entry: &TableEntry,
        column_index: usize,
        default: Option<ScalarExpr>,
        user_xid: Xid,
    ) -> Result<SelectResult, ServerError> {
        let width = entry.schema.len();
        if column_index >= width {
            return Err(self.fail_if_in_transaction(ServerError::ddl(format!(
                "ALTER TABLE ALTER COLUMN DEFAULT: column index {column_index} out of bounds for {}",
                entry.name
            ))));
        }
        let table_key = table_lookup_key(&entry.schema_name, &entry.name);
        self.alter_in_txn_lock(&table_key, user_xid)?;

        let runtime_constraints_before = self
            .state
            .table_constraints
            .get(&entry.oid)
            .map(|guard| guard.clone());
        let mut runtime = runtime_constraints_before
            .as_ref()
            .map(|existing| existing.as_ref().clone())
            .unwrap_or_default();
        if runtime.defaults.len() < width {
            runtime.defaults.resize(width, None);
        }
        runtime.defaults[column_index] = default;
        self.state
            .ensure_table_runtime_constraints_metadata_persistable(&table_key, &runtime)?;
        self.state
            .ensure_table_runtime_constraints_metadata_slots_persistable()?;

        // Re-persist pg_class / pg_attribute under the user xid so `atthasdef`
        // matches the new default state on restart (the schema is unchanged).
        let attr_has_defaults = alter_attr_has_defaults(Some(&runtime), width);
        self.state
            .persistent_catalog
            .persist_table_rows_with_defaults(
                entry,
                &attr_has_defaults,
                self.state.heap.as_ref(),
                user_xid,
                self.alter_in_txn_command_id(),
            )
            .map_err(|e| self.fail_if_in_transaction(e.into()))?;

        // Publish the runtime default in-memory (self-visible for in-txn
        // INSERTs); the durable metadata-file flush is deferred to COMMIT.
        self.state
            .table_constraints
            .insert(entry.oid, Arc::new(runtime));

        let staged = AlteredSideEffects {
            oid: entry.oid,
            op: AlterTableOp::DefaultOnly,
            runtime_constraints_before,
            runtime_constraints_changed: true,
            time_partition_before: None,
            time_partition_key_before: String::new(),
            privilege_grants_before: Vec::new(),
            privilege_default_grants_before: Vec::new(),
            privileges_changed: false,
        };
        // The entry is unchanged by a default edit, but staging it keeps the
        // overlay fold + commit-marker bookkeeping uniform across sub-actions.
        self.alter_in_txn_stage(user_xid, entry.clone(), staged);
        Ok(run_ddl_command("ALTER TABLE"))
    }

    /// `ALTER TABLE … SET (...)` staged in-txn.
    fn alter_in_txn_set_options(
        &mut self,
        entry: &TableEntry,
        options: &[ultrasql_planner::LogicalTableOption],
        user_xid: Xid,
    ) -> Result<SelectResult, ServerError> {
        let table_key = table_lookup_key(&entry.schema_name, &entry.name);
        self.alter_in_txn_lock(&table_key, user_xid)?;

        let mut pairs = options
            .iter()
            .map(|option| (option.name.clone(), option.value.clone()))
            .collect::<Vec<_>>();
        crate::validate_autovacuum_reloptions(&pairs)?;
        // Preserve UltraSQL-internal options (e.g. column collations) exactly as
        // the autocommit `execute_alter_set_options` does.
        pairs.extend(
            entry
                .options
                .iter()
                .filter(|(name, _)| name.starts_with("ultrasql."))
                .cloned(),
        );

        let mut updated_entry = entry.clone();
        updated_entry.options = pairs.clone();

        let attr_has_defaults = self.alter_in_txn_attr_has_defaults(entry.oid, &updated_entry);
        self.state
            .persistent_catalog
            .persist_table_rows_with_defaults(
                &updated_entry,
                &attr_has_defaults,
                self.state.heap.as_ref(),
                user_xid,
                self.alter_in_txn_command_id(),
            )
            .map_err(|e| self.fail_if_in_transaction(e.into()))?;

        let staged = AlteredSideEffects {
            oid: entry.oid,
            op: AlterTableOp::Options {
                name: table_key,
                opts: pairs,
            },
            runtime_constraints_before: None,
            runtime_constraints_changed: false,
            time_partition_before: None,
            time_partition_key_before: String::new(),
            privilege_grants_before: Vec::new(),
            privilege_default_grants_before: Vec::new(),
            privileges_changed: false,
        };
        let display_name = updated_entry.name.clone();
        self.alter_in_txn_stage(user_xid, updated_entry, staged);
        Ok(run_ddl_command(&format!("ALTER TABLE {display_name}")))
    }

    // ----- shared helpers -------------------------------------------------

    /// Persist a replaced-schema ALTER (`RENAME COLUMN` / `SET|DROP NOT NULL`)
    /// under the user xid and stage it. The schema is replaced; runtime defaults
    /// are unchanged (their `atthasdef` is preserved via the side map).
    fn alter_in_txn_replace_schema(
        &mut self,
        entry: &TableEntry,
        new_schema: ultrasql_core::Schema,
        user_xid: Xid,
    ) -> Result<(), ServerError> {
        let mut updated_entry = entry.clone();
        updated_entry.schema = new_schema;
        let table_key = table_lookup_key(&entry.schema_name, &entry.name);

        let attr_has_defaults = self.alter_in_txn_attr_has_defaults(entry.oid, &updated_entry);
        // Append dropped markers for old attnums + the new compacted attributes
        // under the user xid (NOT committed); bootstrap keeps the latest row per
        // (attrelid, attnum) so the new schema wins once committed.
        self.state
            .persistent_catalog
            .persist_table_schema_replacement_with_defaults(
                entry,
                &updated_entry,
                &attr_has_defaults,
                self.state.heap.as_ref(),
                user_xid,
                self.alter_in_txn_command_id(),
            )
            .map_err(|e| self.fail_if_in_transaction(e.into()))?;

        let staged = AlteredSideEffects {
            oid: entry.oid,
            op: AlterTableOp::ReplaceSchema {
                name: table_key,
                schema: updated_entry.schema.clone(),
            },
            runtime_constraints_before: None,
            runtime_constraints_changed: false,
            time_partition_before: None,
            time_partition_key_before: String::new(),
            privilege_grants_before: Vec::new(),
            privilege_default_grants_before: Vec::new(),
            privileges_changed: false,
        };
        self.alter_in_txn_stage(user_xid, updated_entry, staged);
        Ok(())
    }

    /// Build a new schema with one column's `nullable` flag set.
    fn alter_in_txn_schema_with_nullability(
        &mut self,
        entry: &TableEntry,
        column_index: usize,
        nullable: bool,
    ) -> Result<ultrasql_core::Schema, ServerError> {
        let mut new_fields: Vec<ultrasql_core::Field> = entry.schema.fields().to_vec();
        let target = new_fields.get_mut(column_index).ok_or_else(|| {
            self.fail_if_in_transaction(ServerError::ddl(format!(
                "ALTER TABLE ALTER COLUMN: column index {column_index} out of bounds for {}",
                entry.name
            )))
        })?;
        target.nullable = nullable;
        ultrasql_core::Schema::new(new_fields).map_err(|e| {
            self.fail_if_in_transaction(ServerError::Catalog(
                ultrasql_catalog::CatalogError::schema_conflict(format!(
                    "ALTER TABLE ALTER COLUMN: {e}"
                )),
            ))
        })
    }

    /// Compute `pg_attribute.atthasdef` for `new_entry` from the table's current
    /// runtime defaults, mirroring the autocommit paths so a re-persist does not
    /// silently forget a stored default.
    fn alter_in_txn_attr_has_defaults(
        &self,
        table_oid: ultrasql_core::Oid,
        new_entry: &TableEntry,
    ) -> Vec<bool> {
        if let Some(runtime) = self.state.table_constraints.get(&table_oid) {
            alter_attr_has_defaults(Some(runtime.value().as_ref()), new_entry.schema.len())
        } else {
            alter_attr_has_defaults(None, new_entry.schema.len())
        }
    }
}
