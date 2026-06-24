//! `CREATE TABLE` DDL handler and its unique-index helper. Part of the
//! `session::ddl` module split; reopens the `impl<RW> Session<RW>` block
//! defined in `session/mod.rs`.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{CatalogSnapshot, IndexEntry, MutableCatalog, TableEntry};
use ultrasql_core::RelationId;
use ultrasql_planner::{Catalog as PlannerCatalog, LogicalPlan};
use ultrasql_storage::btree::BTree;
use ultrasql_wal::payload::SequenceOpKind;

use super::super::Session;
use super::{log_failed_ddl_rollback, table_entry_lookup_key};
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};

const COLUMN_COLLATION_OPTION_PREFIX: &str = "ultrasql.attcollation.";

const PG_OID_INT8: u32 = 20;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Persist a `CREATE TABLE` into the catalog.
    ///
    /// Honors `IF NOT EXISTS` by short-circuiting when the relation
    /// already exists in either the persistent snapshot or the
    /// in-memory sample catalog. The resolved column [`Schema`] from
    /// the binder is stored verbatim, so a subsequent statement that
    /// captures a fresh snapshot will see the new relation.
    ///
    /// Currently a metadata-only operation: the segment file and the
    /// `pg_class.relfilenode` block are allocated lazily on the first
    /// `INSERT`. This matches PostgreSQL's `RelationSetNewRelfilenode`
    /// timing closely enough that subsequent `INSERT` wiring (in a
    /// follow-up commit) can stamp the right block number then.
    pub(crate) fn execute_create_table(
        &mut self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateTable {
            table_name,
            namespace,
            columns,
            column_collations,
            defaults,
            sequence_defaults,
            sequence_options,
            identity_always,
            generated_stored,
            checks,
            unique_constraints,
            foreign_keys,
            exclusion_constraints,
            partition,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_table called with non-CreateTable plan",
            ));
        };
        // Transactional-DDL milestone 1: when an explicit transaction is
        // open this `CREATE TABLE` must stage its catalog effects in a
        // session-local overlay bound to the user xid rather than mutating
        // the global catalog. `None` here means autocommit — the legacy
        // self-committing path runs byte-for-byte unchanged.
        //
        // A `CREATE TABLE` issued while a SAVEPOINT is active is out of scope
        // for milestone 1: the durable catalog rows ride the parent xid (not
        // the subtransaction xid) and the overlay is whole-transaction-scoped,
        // so a later `ROLLBACK TO SAVEPOINT` could NOT undo the table — a
        // rolled-back-to-savepoint relation that survives is the same
        // corruption class we are guarding against. Reject it with the gate's
        // `0A000` until subtransaction-scoped catalog DDL lands.
        let in_txn_xid = match &self.txn_state {
            crate::TxnState::InTransaction(txn) => {
                if txn.subtxn_stack.depth() > 0 {
                    return Err(self.fail_if_in_transaction(ServerError::DdlInTransaction));
                }
                Some(txn.xid)
            }
            _ => None,
        };
        self.ensure_schema_exists(namespace)?;
        self.ensure_schema_create_privilege(namespace)?;
        let table_key = ultrasql_catalog::table_lookup_key(namespace, table_name);
        // Transactional CREATE TABLE takes AccessExclusive BEFORE the
        // existence check, keyed on the *qualified name* (not the
        // yet-to-be-allocated OID, which differs per session and would not
        // serialize a same-name race). Held for the rest of the user
        // transaction and auto-released by `release_all(xid)` at
        // COMMIT/ROLLBACK. This is the authoritative serialization point for
        // two transactions racing to create the same relation: a non-blocking
        // `try_acquire` (the engine's established lock discipline — blocking a
        // tokio worker on a cross-transaction lock would stall the runtime;
        // see `txn_exec::lock_tuple_ids`) so the loser fails immediately with
        // a serialization error rather than parking a worker. Either way, two
        // same-name `pg_class` rows can never both reach durable commit (which
        // would be a duplicate-name corruption on restart).
        // Both paths take this name lock so an autocommit and an in-transaction
        // `CREATE TABLE` of the same name serialize against each other (an
        // autocommit creator cannot see the in-txn table — it lives only in the
        // other session's overlay — so without a shared lock both could persist
        // a `pg_class` row for the same name → duplicate-name corruption on
        // restart). The in-txn path keys the grant on the user xid (released by
        // `release_all` at the user COMMIT/ROLLBACK); the autocommit path keys
        // it on a dedicated lock transaction held in `autocommit_name_lock`
        // (released when that guard is committed at the end of this method, or
        // aborted on any early-return error path via its `Drop`).
        let name_lock_tag = create_table_name_lock_tag(&table_key);
        let mut autocommit_name_lock: Option<AutocommitNameLock> = None;
        if let Some(user_xid) = in_txn_xid {
            let acquired = self
                .state
                .txn_manager
                .lock_manager
                .try_acquire(ultrasql_txn::LockRequest {
                    xid: user_xid,
                    tag: name_lock_tag,
                    mode: ultrasql_txn::LockMode::AccessExclusive,
                })
                .map_err(|e| ServerError::ddl(format!("CREATE TABLE relation lock: {e}")))?;
            if !acquired {
                return Err(
                    self.fail_if_in_transaction(ServerError::SerializationFailure(format!(
                        "could not obtain lock on relation \"{table_name}\": another transaction \
                         is creating it concurrently"
                    ))),
                );
            }
        } else {
            match AutocommitNameLock::acquire(&self.state.txn_manager, name_lock_tag)? {
                Some(guard) => autocommit_name_lock = Some(guard),
                None => {
                    return Err(ServerError::SerializationFailure(format!(
                        "could not obtain lock on relation \"{table_name}\": another transaction \
                         is creating it concurrently"
                    )));
                }
            }
        }
        // Existence check. Now that the name lock is held (on BOTH paths),
        // consult the CURRENT global snapshot rather than the per-statement
        // `snapshot` captured before the lock: a racing creator (autocommit or
        // in-txn) may have committed the same name while we waited on the lock,
        // and the passed-in snapshot would not reflect it — re-reading under the
        // lock closes that window. `effective_catalog_snapshot()` is the live
        // committed snapshot for the autocommit path (no overlay) and the
        // overlay-folded snapshot for the in-txn path, so a second
        // `CREATE TABLE same` within one transaction is rejected too.
        let live_snapshot = self.effective_catalog_snapshot();
        let exists_persistent = live_snapshot.tables.contains_key(&table_key);
        let exists_fallback = self
            .state
            .catalog
            .lookup_table_in_schema(namespace, table_name)
            .is_some();
        if exists_persistent || exists_fallback {
            if *if_not_exists {
                // IF NOT EXISTS short-circuit: nothing is created, so no durable
                // side effect can leak even in-txn — accept regardless of the
                // milestone-1 feature gate below.
                if let Some(guard) = autocommit_name_lock.take() {
                    guard.commit();
                }
                return Ok(run_ddl_command("CREATE TABLE"));
            }
            return Err(self.fail_if_in_transaction(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(table_name.clone()),
            )));
        }
        // Transactional-DDL milestone 1 only stages catalog metadata that the
        // session overlay can publish at COMMIT or discard at ROLLBACK via MVCC.
        // Any `CREATE TABLE` variant that produces a SEPARATE durable artifact
        // the overlay cannot transactionally undo is rejected in-txn with the
        // gate's `0A000` (the block goes Failed); the same statement is fully
        // supported on the autocommit path, which self-commits the artifact.
        //
        // Rejected in-txn:
        // - PRIMARY KEY / UNIQUE: builds a durable B-tree segment at create time
        //   (`create_table_unique_indexes`) that a ROLLBACK would orphan, and on
        //   restart the index OID can be reused onto the stale segment.
        // - FOREIGN KEY: validates against and references another relation; its
        //   semantics span tables the whole-transaction overlay does not model.
        // - PARTITION BY: a partitioned parent installs a non-MVCC time-partition
        //   runtime sidecar and chunk machinery the overlay cannot roll back.
        // - DEFAULT … nextval(…): ties the column to a sequence whose advance is
        //   non-transactional.
        // - serial / IDENTITY: rejected just below (sequence WAL is replayed
        //   unconditionally on restart) — kept at its original site.
        //
        // ALLOWED in-txn: plain columns, NOT NULL, DEFAULT with a constant /
        // immutable (non-nextval) expression, and CHECK constraints (which
        // persist as pure `pg_constraint` MVCC rows under the user xid — no
        // separate durable artifact).
        if in_txn_xid.is_some() {
            if !unique_constraints.is_empty() {
                return Err(self.fail_if_in_transaction(ServerError::UnsupportedOwned(
                    "PRIMARY KEY / UNIQUE on CREATE TABLE inside an explicit transaction is not \
                     yet supported\nHINT:  it builds a durable index segment that cannot be \
                     transactionally rolled back yet; create the table in autocommit"
                        .to_string(),
                )));
            }
            if !foreign_keys.is_empty() {
                return Err(self.fail_if_in_transaction(ServerError::UnsupportedOwned(
                    "FOREIGN KEY on CREATE TABLE inside an explicit transaction is not yet \
                     supported\nHINT:  it references another relation; create the table in \
                     autocommit"
                        .to_string(),
                )));
            }
            if partition.is_some() {
                return Err(self.fail_if_in_transaction(ServerError::UnsupportedOwned(
                    "PARTITION BY on CREATE TABLE inside an explicit transaction is not yet \
                     supported\nHINT:  create the partitioned table in autocommit"
                        .to_string(),
                )));
            }
            if defaults.iter().flatten().any(default_calls_nextval) {
                return Err(self.fail_if_in_transaction(ServerError::UnsupportedOwned(
                    "a column DEFAULT calling nextval() on CREATE TABLE inside an explicit \
                     transaction is not yet supported\nHINT:  sequence advances are \
                     non-transactional; create the table in autocommit"
                        .to_string(),
                )));
            }
        }
        let oid = self.state.persistent_catalog.next_oid();
        let mut table_options = column_collation_options(column_collations);
        if let Some(partition) = partition {
            table_options.extend(crate::time_partition::parent_catalog_options(
                &partition.column,
                crate::time_partition::DEFAULT_TIME_CHUNK_INTERVAL_US,
            ));
        }
        let entry = TableEntry::new(oid, table_name.clone(), namespace.clone(), columns.clone())
            .with_options(table_options);
        for unique in unique_constraints {
            crate::index_key::IndexKeyEncoding::for_columns(&entry.schema, &unique.columns)?;
        }
        let sequence_namespace = namespace.to_ascii_lowercase();
        let runtime_sequence_defaults = sequence_defaults
            .iter()
            .map(|name| {
                name.as_ref()
                    .map(|name| crate::sequence_lookup_key(&sequence_namespace, name))
            })
            .collect::<Vec<_>>();
        let serial_sequences: Vec<(String, String, ultrasql_planner::LogicalSequenceOptions)> =
            sequence_defaults
                .iter()
                .zip(sequence_options)
                .filter_map(|(name, options)| {
                    name.as_ref().map(|name| {
                        (
                            name.clone(),
                            crate::sequence_lookup_key(&sequence_namespace, name),
                            options.unwrap_or_default(),
                        )
                    })
                })
                .collect();
        for (_, seq_key, _) in &serial_sequences {
            if self.state.sequences.contains_key(seq_key) {
                return Err(ServerError::Catalog(
                    ultrasql_catalog::CatalogError::already_exists(seq_key.clone()),
                ));
            }
        }
        // A serial / sequence-backed column inside an explicit transaction is
        // out of scope for milestone 1: the sequence-create WAL record is
        // emitted with `Xid::INVALID` and is replayed unconditionally on
        // restart (`recovery_target::apply_sequence_op`), so a rolled-back
        // (or crash-before-commit) sequence would resurrect — a non-MVCC
        // sidecar that cannot be transactionally undone. Reject it with the
        // same `0A000` the gate uses for the other out-of-scope DDL, and
        // fail the block. The autocommit path is unaffected.
        if in_txn_xid.is_some() && !serial_sequences.is_empty() {
            return Err(self.fail_if_in_transaction(ServerError::DdlInTransaction));
        }
        let runtime_foreign_keys = foreign_keys
            .iter()
            .map(|fk| {
                let target = snapshot.tables.get(&fk.target_table).ok_or_else(|| {
                    ServerError::Catalog(ultrasql_catalog::CatalogError::not_found(
                        fk.target_table.clone(),
                    ))
                })?;
                Ok(crate::RuntimeForeignKeyConstraint {
                    name: fk.name.clone(),
                    columns: fk.columns.clone(),
                    target_table: fk.target_table.clone(),
                    target_oid: target.oid,
                    target_columns: fk.target_columns.clone(),
                    on_delete: fk.on_delete,
                    on_update: fk.on_update,
                    deferrable: fk.deferrable,
                    initially_deferred: fk.initially_deferred,
                })
            })
            .collect::<Result<Vec<_>, ServerError>>()?;
        let runtime_exclusion_constraints = exclusion_constraints
            .iter()
            .map(|constraint| crate::RuntimeExclusionConstraint {
                name: constraint.name.clone(),
                method: constraint.method,
                elements: constraint
                    .elements
                    .iter()
                    .map(|element| crate::RuntimeExclusionElement {
                        column: element.column,
                        op: element.op,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        let mut runtime_checks = checks
            .iter()
            .map(|check| crate::RuntimeCheckConstraint {
                name: check.name.clone(),
                expr: check.expr.clone(),
            })
            .collect::<Vec<_>>();
        runtime_checks.extend(self.domain_checks_for_columns(columns)?);
        let runtime_constraints = if defaults.iter().any(Option::is_some)
            || sequence_defaults.iter().any(Option::is_some)
            || identity_always.iter().any(|v| *v)
            || generated_stored.iter().any(Option::is_some)
            || !runtime_checks.is_empty()
            || !runtime_foreign_keys.is_empty()
            || !runtime_exclusion_constraints.is_empty()
        {
            let runtime = Arc::new(crate::TableRuntimeConstraints {
                defaults: defaults.clone(),
                sequence_defaults: runtime_sequence_defaults.clone(),
                identity_always: identity_always.clone(),
                generated_stored: generated_stored.clone(),
                checks: runtime_checks.clone(),
                foreign_keys: runtime_foreign_keys.clone(),
                exclusion_constraints: runtime_exclusion_constraints.clone(),
                indexes: std::collections::HashMap::new(),
            });
            self.state
                .ensure_table_runtime_constraints_metadata_persistable(
                    &table_entry_lookup_key(&entry),
                    runtime.as_ref(),
                )?;
            Some(runtime)
        } else {
            None
        };
        self.state
            .ensure_create_table_runtime_metadata_slots_persistable(!serial_sequences.is_empty())?;
        // Autocommit publishes the table into the global catalog immediately;
        // the in-transaction path keeps it session-local in the overlay until
        // COMMIT, so a concurrent session never observes an uncommitted table
        // and a ROLLBACK has nothing to undo globally.
        if in_txn_xid.is_none() {
            self.state.persistent_catalog.create_table(entry.clone())?;
        }
        let mut serial_sequence_rows = Vec::with_capacity(serial_sequences.len());
        let sequence_owner = self.current_user.to_ascii_lowercase();
        for (seq_name, seq_key, options) in &serial_sequences {
            let seq = ultrasql_storage::sequence::Sequence::new(
                super::super::sequence::to_storage_options(*options),
            )
            .map_err(|e| ServerError::ddl(format!("CREATE TABLE serial sequence: {e}")))?;
            let seq_oid = self.state.persistent_catalog.next_oid();
            let seq_rel = RelationId::new(seq_oid.raw());
            let seq_opts = seq.options_snapshot();
            serial_sequence_rows.push((
                seq_name.clone(),
                seq_key.clone(),
                ultrasql_catalog::persistent::SequenceRow {
                    seqrelid: seq_oid,
                    seqtypid: PG_OID_INT8,
                    seqstart: seq_opts.start,
                    seqincrement: seq_opts.increment,
                    seqmax: seq_opts.max.unwrap_or(i64::MAX),
                    seqmin: seq_opts.min.unwrap_or(1),
                    seqcache: i64::from(seq_opts.cache),
                    seqcycle: seq_opts.cycle,
                },
            ));
            seq.emit_wal(
                SequenceOpKind::Create,
                seq_key,
                seq_rel,
                ultrasql_core::Xid::INVALID,
                self.state.heap.wal_sink().map(|sink| sink.as_ref()),
            )
            .map_err(|e| ServerError::ddl(format!("CREATE TABLE serial sequence WAL: {e}")))?;
            self.state.sequences.insert(seq_key.clone(), Arc::new(seq));
            self.state
                .sequence_owners
                .insert(seq_key.clone(), sequence_owner.clone());
            self.state
                .sequence_namespaces
                .insert(seq_key.clone(), sequence_namespace.clone());
        }
        let mut persistent_constraint_rows = Vec::with_capacity(
            unique_constraints.len()
                + runtime_checks.len()
                + runtime_foreign_keys.len()
                + runtime_exclusion_constraints.len(),
        );
        for unique in unique_constraints {
            persistent_constraint_rows.push(ultrasql_catalog::persistent::ConstraintRow {
                oid: self.state.persistent_catalog.next_oid(),
                conname: unique.name.clone(),
                conrelid: oid,
                contype: if unique.primary_key {
                    ultrasql_catalog::persistent::ConType::PrimaryKey
                } else {
                    ultrasql_catalog::persistent::ConType::Unique
                },
                condeferrable: false,
                condeferred: false,
                conkey: constraint_attnums(&unique.columns, &unique.name)?,
                confrelid: ultrasql_core::Oid::INVALID,
                confkey: Vec::new(),
            });
        }
        for check in &runtime_checks {
            persistent_constraint_rows.push(ultrasql_catalog::persistent::ConstraintRow {
                oid: self.state.persistent_catalog.next_oid(),
                conname: check.name.clone(),
                conrelid: oid,
                contype: ultrasql_catalog::persistent::ConType::Check,
                condeferrable: false,
                condeferred: false,
                conkey: Vec::new(),
                confrelid: ultrasql_core::Oid::INVALID,
                confkey: Vec::new(),
            });
        }
        for fk in &runtime_foreign_keys {
            persistent_constraint_rows.push(ultrasql_catalog::persistent::ConstraintRow {
                oid: self.state.persistent_catalog.next_oid(),
                conname: fk.name.clone(),
                conrelid: oid,
                contype: ultrasql_catalog::persistent::ConType::ForeignKey,
                condeferrable: fk.deferrable,
                condeferred: fk.initially_deferred,
                conkey: constraint_attnums(&fk.columns, &fk.name)?,
                confrelid: fk.target_oid,
                confkey: constraint_attnums(&fk.target_columns, &fk.name)?,
            });
        }
        for exclusion in &runtime_exclusion_constraints {
            let columns = exclusion
                .elements
                .iter()
                .map(|element| element.column)
                .collect::<Vec<_>>();
            persistent_constraint_rows.push(ultrasql_catalog::persistent::ConstraintRow {
                oid: self.state.persistent_catalog.next_oid(),
                conname: exclusion.name.clone(),
                conrelid: oid,
                contype: ultrasql_catalog::persistent::ConType::Exclusion,
                condeferrable: false,
                condeferred: false,
                conkey: constraint_attnums(&columns, &exclusion.name)?,
                confrelid: ultrasql_core::Oid::INVALID,
                confkey: Vec::new(),
            });
        }
        if let Some(runtime) = runtime_constraints.clone() {
            self.state.table_constraints.insert(oid, runtime);
        }
        let created_unique_indexes = match self.create_table_unique_indexes(
            &entry,
            unique_constraints,
            in_txn_xid.is_none(),
        ) {
            Ok(indexes) => indexes,
            Err(e) => {
                // Undo the in-memory side maps mutated so far. The global
                // `drop_table` only applies to the autocommit path, where
                // the table was already published; the in-txn path never
                // published it.
                if in_txn_xid.is_none() {
                    log_failed_ddl_rollback(
                        self.state.persistent_catalog.drop_table(table_name),
                        "drop table",
                    );
                }
                self.state.table_constraints.remove(&oid);
                for (_, seq_key, _) in &serial_sequences {
                    self.state.sequences.remove(seq_key);
                    self.state.sequence_owners.remove(seq_key);
                    self.state.sequence_namespaces.remove(seq_key);
                }
                return Err(e);
            }
        };
        let attr_has_defaults: Vec<bool> = (0..columns.len())
            .map(|idx| {
                defaults.get(idx).is_some_and(Option::is_some)
                    || sequence_defaults.get(idx).is_some_and(Option::is_some)
                    || identity_always.get(idx).copied().unwrap_or(false)
                    || generated_stored.get(idx).is_some_and(Option::is_some)
            })
            .collect();
        // Persist the typed pg_class + pg_attribute rows so a restart can
        // rebuild this `TableEntry` via `PersistentCatalog::bootstrap_from_heap`.
        //
        // Autocommit: open a private, self-committing `ddl_txn` and stamp the
        // rows with its xid (legacy path, byte-for-byte unchanged).
        //
        // In-transaction: stamp the rows with the USER xid and do NOT commit
        // them — the user's COMMIT/ROLLBACK decides their fate via MVCC, and a
        // crash before COMMIT leaves them MVCC-invisible (and hidden by the
        // visibility-filtered catalog bootstrap on restart).
        if let Some(user_xid) = in_txn_xid {
            let command_id = match &self.txn_state {
                crate::TxnState::InTransaction(txn) => txn.current_command,
                // Unreachable: `in_txn_xid` is `Some` iff `InTransaction`.
                _ => ultrasql_core::CommandId::FIRST,
            };
            self.persist_create_table_rows_under_xid(
                &entry,
                &attr_has_defaults,
                &created_unique_indexes,
                &persistent_constraint_rows,
                user_xid,
                command_id,
            )?;
        } else {
            let ddl_txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            let ddl_xid = ddl_txn.xid;
            let ddl_command_id = ddl_txn.current_command;
            let persist_result = (|| -> Result<(), ultrasql_catalog::CatalogError> {
                self.state
                    .persistent_catalog
                    .persist_table_rows_with_defaults(
                        &entry,
                        &attr_has_defaults,
                        self.state.heap.as_ref(),
                        ddl_xid,
                        ddl_command_id,
                    )?;
                for index in &created_unique_indexes {
                    self.state.persistent_catalog.persist_index_rows(
                        index,
                        self.state.heap.as_ref(),
                        ddl_xid,
                        ddl_command_id,
                    )?;
                }
                for row in &persistent_constraint_rows {
                    self.state.persistent_catalog.persist_constraint_row(
                        row,
                        self.state.heap.as_ref(),
                        ddl_xid,
                        ddl_command_id,
                    )?;
                }
                for (seq_name, _seq_key, row) in &serial_sequence_rows {
                    self.state.persistent_catalog.persist_sequence_rows(
                        seq_name,
                        &sequence_namespace,
                        row,
                        self.state.heap.as_ref(),
                        ddl_xid,
                        ddl_command_id,
                    )?;
                }
                Ok(())
            })();
            if let Err(e) = persist_result {
                log_failed_ddl_rollback(
                    self.state.persistent_catalog.drop_table(table_name),
                    "drop table",
                );
                self.state.table_constraints.remove(&oid);
                for (_, seq_key, _) in &serial_sequences {
                    self.state.sequences.remove(seq_key);
                    self.state.sequence_owners.remove(seq_key);
                    self.state.sequence_namespaces.remove(seq_key);
                }
                return Err(self.rollback_catalog_transaction_after_error(
                    ddl_txn,
                    e.into(),
                    "CREATE TABLE catalog rollback after persist error",
                ));
            }
            self.state.commit_transaction(
                ddl_txn,
                true,
                "CREATE TABLE catalog-write transaction",
            )?;
        }
        // Autocommit publishes the constraint rows into the live catalog map
        // (one `rebuild_snapshot`); the in-txn path defers this to the COMMIT
        // merge — the overlay already carries the constraints for self-reads.
        if in_txn_xid.is_none() {
            self.state
                .persistent_catalog
                .install_constraint_rows(persistent_constraint_rows.clone());
        }
        let row_security_before = self.state.row_security.get(&oid).map(|g| g.value().clone());
        let mut row_security = row_security_before
            .as_ref()
            .map(|g| g.as_ref().clone())
            .unwrap_or_default();
        if row_security.owner_role.is_empty() {
            row_security.owner_role = self.current_user.to_ascii_lowercase();
        }
        self.state.row_security.insert(oid, Arc::new(row_security));
        // The runtime-constraints / RLS / sequence-owner metadata files are
        // derived from `(in-memory side map) ∩ (global catalog tables)`, so a
        // persist while the table is still session-local (in-txn) is a no-op
        // for the new table — defer these to the COMMIT merge, where the table
        // is published into the global snapshot first.
        if in_txn_xid.is_none() {
            self.state.persist_table_runtime_constraints_metadata()?;
            self.state.persist_row_security_metadata()?;
            if !serial_sequences.is_empty() {
                self.state.persist_sequence_owner_metadata()?;
            }
        }
        let before_grants = self.state.privilege_catalog.list_grants();
        let before_default_grants = self.state.privilege_catalog.list_default_grants();
        self.state.privilege_catalog.apply_default_privileges(
            &self.current_user,
            namespace,
            crate::auth::PrivilegeObjectKind::Table,
            table_name,
        );
        for (seq_name, _, _) in &serial_sequences {
            self.state.privilege_catalog.apply_default_privileges(
                &self.current_user,
                namespace,
                crate::auth::PrivilegeObjectKind::Sequence,
                seq_name,
            );
        }
        let grants_changed = before_grants != self.state.privilege_catalog.list_grants()
            || before_default_grants != self.state.privilege_catalog.list_default_grants();
        // Autocommit persists the new grants to the durable metadata file
        // immediately. In-transaction defers the durable write to COMMIT: the
        // grants are applied in-memory (so privilege checks inside the txn see
        // them) but writing them to disk before COMMIT would leave a dangling
        // grant for an absent table after a crash-before-commit. ROLLBACK
        // reverts the in-memory grants via `install_snapshot`, never having
        // touched the file.
        if in_txn_xid.is_none()
            && grants_changed
            && let Err(err) = self.state.persist_privilege_metadata()
        {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        let time_partition_inserted = partition.is_some();
        if let Some(partition) = partition {
            self.state.time_partitions.insert(
                table_key.clone(),
                Arc::new(crate::time_partition::TimePartitionRuntime::daily(
                    namespace.clone(),
                    table_name.clone(),
                    oid,
                    columns.clone(),
                    partition.column.clone(),
                    partition.column_index,
                )),
            );
        }
        // A new relation can shadow names a cached plan rewrote against the
        // previous snapshot; clear the cache so the next statement re-plans.
        // For the in-txn path this also forces the session's bind cache to
        // re-resolve the table through the overlay-aware snapshot.
        self.plan_cache_invalidate();
        // In-transaction: stage the overlay (self-visible immediately, merged
        // at COMMIT, discarded at ROLLBACK) and record the table in the
        // pending-modification set so `execute_commit` writes a durable commit
        // marker for the user xid carrying these catalog rows.
        if let Some(user_xid) = in_txn_xid {
            let staged = super::super::catalog_overlay::StagedSideEffects {
                oid,
                table_key: table_key.clone(),
                runtime_constraints: runtime_constraints.clone(),
                row_security_before,
                time_partition_inserted,
                privilege_grants_before: before_grants,
                privilege_default_grants_before: before_default_grants,
                privileges_changed: grants_changed,
            };
            self.pending_catalog_ddl = Some(super::super::catalog_overlay::CatalogOverlay {
                xid: user_xid,
                table: entry.clone(),
                indexes: created_unique_indexes,
                constraints: persistent_constraint_rows,
                staged,
            });
            // Mark the new table modified so `commit_transaction`'s
            // `modified_tables` is non-empty → a durable commit marker is
            // written, making the user-xid catalog rows visible after restart.
            self.pending_table_modifications
                .entry(table_entry_lookup_key(&entry))
                .or_insert(0);
        }
        // Autocommit success: the relation's durable rows are committed, so
        // release the per-name lock by committing the dedicated lock
        // transaction. (`Drop` would also release it — via abort — on any
        // early-return path above; releasing it here keeps the lock held no
        // longer than necessary.) No-op for the in-txn path, whose lock rides
        // the user xid until the user's COMMIT/ROLLBACK.
        if let Some(guard) = autocommit_name_lock.take() {
            guard.commit();
        }
        Ok(run_ddl_command("CREATE TABLE"))
    }

    /// Persist the durable catalog heap rows for an in-transaction
    /// `CREATE TABLE`, stamped with the user `xid` / `command_id`, WITHOUT
    /// committing them. On any persist failure the in-memory side maps that
    /// were already mutated are reverted and the error is returned; the
    /// caller (still inside the user transaction) propagates it, transitioning
    /// the block to `Failed`. The durable rows that did land carry the
    /// uncommitted user xid and are MVCC-invisible until COMMIT — and hidden
    /// by the visibility-filtered bootstrap on restart — so a partial write is
    /// not a corruption hazard.
    fn persist_create_table_rows_under_xid(
        &mut self,
        entry: &TableEntry,
        attr_has_defaults: &[bool],
        indexes: &[IndexEntry],
        constraints: &[ultrasql_catalog::persistent::ConstraintRow],
        xid: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), ServerError> {
        let persist = (|| -> Result<(), ultrasql_catalog::CatalogError> {
            self.state
                .persistent_catalog
                .persist_table_rows_with_defaults(
                    entry,
                    attr_has_defaults,
                    self.state.heap.as_ref(),
                    xid,
                    command_id,
                )?;
            for index in indexes {
                self.state.persistent_catalog.persist_index_rows(
                    index,
                    self.state.heap.as_ref(),
                    xid,
                    command_id,
                )?;
            }
            for row in constraints {
                self.state.persistent_catalog.persist_constraint_row(
                    row,
                    self.state.heap.as_ref(),
                    xid,
                    command_id,
                )?;
            }
            Ok(())
        })();
        if let Err(e) = persist {
            // Nothing was published into the global catalog and no overlay was
            // installed yet, so only the runtime-constraints side map needs
            // reverting here. The durable rows under the uncommitted user xid
            // stay invisible.
            self.state.table_constraints.remove(&entry.oid);
            return Err(self.fail_if_in_transaction(e.into()));
        }
        Ok(())
    }

    /// Build the implicit unique/PK constraint indexes for a new table.
    ///
    /// `install_global` controls whether each built [`IndexEntry`] is
    /// published into the global catalog via
    /// [`MutableCatalog::create_index`]: `true` for the autocommit path,
    /// `false` for the in-transaction path where the parent table is not in
    /// the global catalog (it lives in the session overlay) so a global
    /// `create_index` would fail the parent-OID lookup. In the in-txn case
    /// the entries are returned for the overlay; the empty btree pages were
    /// already allocated here (a rolled-back transaction leaks those pages,
    /// a bounded cosmetic cost mirroring the OID leak — PG-tolerable).
    fn create_table_unique_indexes(
        &self,
        table: &TableEntry,
        unique_constraints: &[ultrasql_planner::LogicalUniqueConstraint],
        install_global: bool,
    ) -> Result<Vec<IndexEntry>, ServerError> {
        let mut created = Vec::with_capacity(unique_constraints.len());
        for unique in unique_constraints {
            crate::index_key::IndexKeyEncoding::for_columns(&table.schema, &unique.columns)?;
            let index_oid = self.state.persistent_catalog.next_oid();
            let index_rel = RelationId::new(index_oid.raw());
            let btree = BTree::create(Arc::clone(self.state.heap.buffer_pool()), index_rel)
                .map_err(|e| {
                    ServerError::ddl(format!("CREATE TABLE constraint index create: {e}"))
                })?;
            let root_block = btree.root_block();
            let mut attnums = Vec::with_capacity(unique.columns.len());
            for &col in &unique.columns {
                let attnum = u16::try_from(col).map_err(|_| {
                    ServerError::Unsupported(
                        "CREATE TABLE: constraint column index does not fit u16",
                    )
                })?;
                attnums.push(attnum);
            }
            let mut entry =
                IndexEntry::new(index_oid, unique.name.clone(), table.oid, attnums, true)
                    .with_schema_name(table.schema_name.clone())
                    .with_primary(unique.primary_key);
            entry.root_block = root_block;
            // Empty table, so there are no existing heap rows to populate.
            if install_global {
                self.state.persistent_catalog.create_index(entry.clone())?;
            }
            created.push(entry);
        }
        Ok(created)
    }
}

/// `classid` distinguishing the CREATE-TABLE name-lock space from other
/// advisory locks. Arbitrary but stable; combined with a hash of the
/// qualified table name it yields a deterministic `LockTag` two sessions
/// creating the same relation will both contend on.
const CREATE_TABLE_NAME_LOCK_CLASSID: u32 = 0xDD11_C7AB;

/// Build the AccessExclusive name-lock tag for an in-transaction
/// `CREATE TABLE`, keyed on the case-folded qualified table key so two
/// transactions racing to create the same relation serialize.
fn create_table_name_lock_tag(table_key: &str) -> ultrasql_txn::LockTag {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    table_key.hash(&mut hasher);
    // XOR-fold the 64-bit hash into the 32-bit `objid` slot. Collisions only
    // cost a spurious (correct-but-unnecessary) serialization between two
    // unrelated names; they never cause a missed conflict.
    let digest = hasher.finish();
    let objid =
        u32::try_from(digest >> 32).unwrap_or(0) ^ u32::try_from(digest & 0xFFFF_FFFF).unwrap_or(0);
    ultrasql_txn::LockTag::Advisory {
        classid: CREATE_TABLE_NAME_LOCK_CLASSID,
        objid,
    }
}

/// RAII holder for the per-name `AccessExclusive` lock taken on the AUTOCOMMIT
/// `CREATE TABLE` path.
///
/// The in-transaction path keys this lock on the user xid and lets
/// `release_all(xid)` reclaim it at the user's COMMIT/ROLLBACK. The autocommit
/// path has no long-lived user transaction, so it begins a dedicated,
/// short-lived "name-lock" transaction whose xid owns the grant. This guard
/// owns that transaction and the lock together so that EVERY exit from the
/// autocommit body — the success path or any of its `?`/early-return error
/// paths — reclaims the lock and terminates the lock transaction. Without the
/// guard a mid-statement error after the lock was taken would strand the grant
/// (blocking every future creator of that name) and leak the lock transaction
/// as a perpetually-in-progress xid (polluting snapshots and stalling GC).
///
/// The lock transaction never writes WAL and is committed/aborted purely to
/// release its lock and clear its CLOG/in-progress state; it carries no durable
/// effect of its own (the real catalog rows ride the separate `ddl_txn`).
struct AutocommitNameLock {
    txn_manager: Arc<ultrasql_txn::TransactionManager>,
    /// `Some` until the guard is finalised; `None` after commit/abort so the
    /// `Drop` glue does not double-release.
    txn: Option<ultrasql_txn::Transaction>,
}

impl AutocommitNameLock {
    /// Acquire the per-name `AccessExclusive` lock for the autocommit path
    /// under a fresh dedicated transaction.
    ///
    /// Returns `Ok(None)` on contention (the loser of a same-name race) so the
    /// caller can surface the engine's standard 40001 — matching the in-txn
    /// path's non-blocking `try_acquire` discipline and avoiding parking a
    /// tokio worker on a cross-transaction lock. On success the lock transaction
    /// is held inside the returned guard until [`Self::commit`] or `Drop`.
    fn acquire(
        txn_manager: &Arc<ultrasql_txn::TransactionManager>,
        tag: ultrasql_txn::LockTag,
    ) -> Result<Option<Self>, ServerError> {
        let txn = txn_manager.begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let acquired = txn_manager
            .lock_manager
            .try_acquire(ultrasql_txn::LockRequest {
                xid: txn.xid,
                tag,
                mode: ultrasql_txn::LockMode::AccessExclusive,
            })
            .map_err(|e| ServerError::ddl(format!("CREATE TABLE relation lock: {e}")));
        match acquired {
            Ok(true) => Ok(Some(Self {
                txn_manager: Arc::clone(txn_manager),
                txn: Some(txn),
            })),
            Ok(false) => {
                // Lost the race: terminate the lock transaction (no grant to
                // release) and report contention.
                let _ = txn_manager.abort(txn);
                Ok(None)
            }
            Err(e) => {
                let _ = txn_manager.abort(txn);
                Err(e)
            }
        }
    }

    /// Release the lock by committing the lock transaction (`release_all`).
    /// Logged, not propagated: the caller's relation is already durably
    /// committed via its own `ddl_txn`, and the lock transaction has no durable
    /// effect of its own, so a finalisation hiccup is cosmetic.
    fn commit(mut self) {
        if let Some(txn) = self.txn.take()
            && let Err(e) = self.txn_manager.commit(txn)
        {
            tracing::warn!(error = %e, "CREATE TABLE autocommit name-lock release (commit) failed");
        }
    }
}

impl Drop for AutocommitNameLock {
    fn drop(&mut self) {
        if let Some(txn) = self.txn.take()
            && let Err(e) = self.txn_manager.abort(txn)
        {
            tracing::warn!(error = %e, "CREATE TABLE autocommit name-lock release (abort) failed");
        }
    }
}

/// Whether a column DEFAULT expression invokes `nextval()` anywhere in its
/// tree.
///
/// A `DEFAULT nextval('seq')` (distinct from a SERIAL/IDENTITY default, which
/// is carried separately in `sequence_defaults`) couples the column to a
/// sequence whose advance is non-transactional, so an in-transaction
/// `CREATE TABLE` carrying one is rejected for milestone 1. The scan is
/// recursive so a wrapped form (e.g. `COALESCE(nextval('s'), 0)`) is caught
/// too.
fn default_calls_nextval(expr: &ultrasql_planner::ScalarExpr) -> bool {
    use ultrasql_planner::ScalarExpr;
    match expr {
        ScalarExpr::FunctionCall { name, args, .. } => {
            name.eq_ignore_ascii_case("nextval") || args.iter().any(default_calls_nextval)
        }
        ScalarExpr::Unary { expr, .. } | ScalarExpr::IsNull { expr, .. } => {
            default_calls_nextval(expr)
        }
        ScalarExpr::Binary { left, right, .. } => {
            default_calls_nextval(left) || default_calls_nextval(right)
        }
        _ => false,
    }
}

fn column_collation_options(collations: &[Option<u32>]) -> Vec<(String, String)> {
    collations
        .iter()
        .enumerate()
        .filter_map(|(idx, collation)| {
            collation.map(|oid| {
                (
                    format!("{COLUMN_COLLATION_OPTION_PREFIX}{idx}"),
                    oid.to_string(),
                )
            })
        })
        .collect()
}

fn constraint_attnums(columns: &[usize], name: &str) -> Result<Vec<i16>, ServerError> {
    columns
        .iter()
        .map(|col| {
            let attnum = col.checked_add(1).ok_or(ServerError::Unsupported(
                "CREATE TABLE: constraint attnum overflow",
            ))?;
            i16::try_from(attnum).map_err(|_| {
                ServerError::ddl(format!(
                    "CREATE TABLE: constraint {name} column position {attnum} does not fit i16"
                ))
            })
        })
        .collect()
}
