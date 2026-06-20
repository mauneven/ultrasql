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
        &self,
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
        self.ensure_schema_exists(namespace)?;
        self.ensure_schema_create_privilege(namespace)?;
        let table_key = ultrasql_catalog::table_lookup_key(namespace, table_name);
        let exists_persistent = snapshot.tables.contains_key(&table_key);
        let exists_fallback = self
            .state
            .catalog
            .lookup_table_in_schema(namespace, table_name)
            .is_some();
        if exists_persistent || exists_fallback {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE TABLE"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(table_name.clone()),
            ));
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
        self.state.persistent_catalog.create_table(entry.clone())?;
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
        let created_unique_indexes =
            match self.create_table_unique_indexes(&entry, unique_constraints) {
                Ok(indexes) => indexes,
                Err(e) => {
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
        // Persist the typed pg_class + pg_attribute rows so a restart
        // can rebuild this `TableEntry` via
        // `PersistentCatalog::bootstrap_from_heap`. The DDL runs in an
        // autocommit transaction allocated on the spot; the rows are
        // stamped with that xid so MVCC visibility lines up with the
        // user-table relations created in the same statement.
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
        self.state
            .commit_transaction(ddl_txn, true, "CREATE TABLE catalog-write transaction")?;
        self.state
            .persistent_catalog
            .install_constraint_rows(persistent_constraint_rows);
        let mut row_security = self
            .state
            .row_security
            .get(&oid)
            .map(|guard| guard.as_ref().clone())
            .unwrap_or_default();
        if row_security.owner_role.is_empty() {
            row_security.owner_role = self.current_user.to_ascii_lowercase();
        }
        self.state.row_security.insert(oid, Arc::new(row_security));
        self.state.persist_table_runtime_constraints_metadata()?;
        self.state.persist_row_security_metadata()?;
        if !serial_sequences.is_empty() {
            self.state.persist_sequence_owner_metadata()?;
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
        if grants_changed && let Err(err) = self.state.persist_privilege_metadata() {
            self.state
                .privilege_catalog
                .install_snapshot(before_grants, before_default_grants);
            return Err(err);
        }
        if let Some(partition) = partition {
            self.state.time_partitions.insert(
                table_key,
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
        // A new relation can shadow names a cached plan rewrote against
        // the previous snapshot; clear the cache so the next statement
        // re-plans.
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE TABLE"))
    }

    fn create_table_unique_indexes(
        &self,
        table: &TableEntry,
        unique_constraints: &[ultrasql_planner::LogicalUniqueConstraint],
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
            self.state.persistent_catalog.create_index(entry.clone())?;
            created.push(entry);
        }
        Ok(created)
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
