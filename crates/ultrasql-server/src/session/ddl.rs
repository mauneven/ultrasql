//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

#![allow(unused_imports)]

use std::collections::HashSet;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error, info, warn};
use ultrasql_catalog::{
    Catalog, CatalogSnapshot, IndexEntry, MutableCatalog, PersistentCatalog, TableEntry,
};
use ultrasql_core::{DataType, PageId, RelationId, Value};
use ultrasql_optimizer::{NoStats, PlanCache, PlanCacheConfig, PlanCacheKey, StatsSource};
use ultrasql_parser::Parser;
use ultrasql_planner::{
    Catalog as PlannerCatalog, InMemoryCatalog, LogicalAlterTableAction, LogicalCommentTarget,
    LogicalIndexMethod, LogicalIndexOption, LogicalPlan, TableMeta, bind,
};
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_frontend, encode_backend};
use ultrasql_storage::access_method::{
    AccessMethod, AnnPayloadKind, BrinIndex, HnswMetric, PageBackedHnswIndex,
    PageBackedIvfFlatIndex,
};
use ultrasql_storage::btree::BTree;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, UpdateOptions};
use ultrasql_storage::page::Page;
use ultrasql_txn::{IsolationLevel, Transaction, TransactionManager};
use ultrasql_wal::payload::SequenceOpKind;

use super::Session;
use crate::error::ServerError;
use crate::extended;
use crate::pipeline::{self, LowerCtx, SampleTables};
use crate::result_encoder::{
    self, SelectResult, run_ddl_command, run_modify_command, run_select, run_select_streamed,
};
use crate::{
    BlankPageLoader, CombinedCatalog, Server, TxnState, decode_key_column, notice_warning,
    run_plan_in_txn,
};

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
        let exists_persistent = snapshot.tables.contains_key(table_name);
        let exists_fallback = self.state.catalog.lookup_table(table_name).is_some();
        if exists_persistent || exists_fallback {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE TABLE"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(table_name.clone()),
            ));
        }
        let oid = self.state.persistent_catalog.next_oid();
        let entry = TableEntry::new(oid, table_name.clone(), namespace.clone(), columns.clone());
        for unique in unique_constraints {
            crate::index_key::IndexKeyEncoding::for_columns(&entry.schema, &unique.columns)?;
        }
        let serial_sequences: Vec<(String, ultrasql_planner::LogicalSequenceOptions)> =
            sequence_defaults
                .iter()
                .zip(sequence_options)
                .filter_map(|(name, options)| {
                    name.as_ref()
                        .map(|name| (name.clone(), options.unwrap_or_default()))
                })
                .collect();
        for (seq_name, _) in &serial_sequences {
            if self.state.sequences.contains_key(seq_name) {
                return Err(ServerError::Catalog(
                    ultrasql_catalog::CatalogError::already_exists(seq_name.clone()),
                ));
            }
        }
        self.state.persistent_catalog.create_table(entry.clone())?;
        let mut serial_sequence_rows = Vec::with_capacity(serial_sequences.len());
        for (seq_name, options) in &serial_sequences {
            let seq = ultrasql_storage::sequence::Sequence::new(
                super::sequence::to_storage_options(*options),
            )
            .map_err(|e| ServerError::ddl(format!("CREATE TABLE serial sequence: {e}")))?;
            let seq_oid = self.state.persistent_catalog.next_oid();
            let seq_rel = RelationId::new(seq_oid.raw());
            let seq_opts = seq.options_snapshot();
            serial_sequence_rows.push((
                seq_name.clone(),
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
                seq_name,
                seq_rel,
                ultrasql_core::Xid::INVALID,
                self.state.heap.wal_sink().map(|sink| sink.as_ref()),
            )
            .map_err(|e| ServerError::ddl(format!("CREATE TABLE serial sequence WAL: {e}")))?;
            self.state.sequences.insert(seq_name.clone(), Arc::new(seq));
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
        let mut persistent_constraint_rows = Vec::with_capacity(
            unique_constraints.len()
                + checks.len()
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
        for check in checks {
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
        if defaults.iter().any(Option::is_some)
            || sequence_defaults.iter().any(Option::is_some)
            || identity_always.iter().any(|v| *v)
            || generated_stored.iter().any(Option::is_some)
            || !checks.is_empty()
            || !runtime_foreign_keys.is_empty()
            || !runtime_exclusion_constraints.is_empty()
        {
            self.state.table_constraints.insert(
                oid,
                Arc::new(crate::TableRuntimeConstraints {
                    defaults: defaults.clone(),
                    sequence_defaults: sequence_defaults.clone(),
                    identity_always: identity_always.clone(),
                    generated_stored: generated_stored.clone(),
                    checks: checks
                        .iter()
                        .map(|check| crate::RuntimeCheckConstraint {
                            name: check.name.clone(),
                            expr: check.expr.clone(),
                        })
                        .collect(),
                    foreign_keys: runtime_foreign_keys.clone(),
                    exclusion_constraints: runtime_exclusion_constraints.clone(),
                    indexes: std::collections::HashMap::new(),
                }),
            );
        }
        let created_unique_indexes =
            match self.create_table_unique_indexes(&entry, unique_constraints) {
                Ok(indexes) => indexes,
                Err(e) => {
                    let _ = self.state.persistent_catalog.drop_table(table_name);
                    self.state.table_constraints.remove(&oid);
                    for (seq_name, _) in &serial_sequences {
                        self.state.sequences.remove(seq_name);
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
            for (seq_name, row) in &serial_sequence_rows {
                self.state.persistent_catalog.persist_sequence_rows(
                    seq_name,
                    row,
                    self.state.heap.as_ref(),
                    ddl_xid,
                    ddl_command_id,
                )?;
            }
            Ok(())
        })();
        if let Err(e) = persist_result {
            // Abort the catalog-write txn before surfacing the error so
            // the CLOG entry is closed and the rollback path cleans
            // any partial in-place undo entries (there are none for
            // pg_class inserts, but symmetry matters for future
            // expansion).
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of catalog-write txn failed after persist_table_rows error",
                );
            }
            let _ = self.state.persistent_catalog.drop_table(table_name);
            self.state.table_constraints.remove(&oid);
            for (seq_name, _) in &serial_sequences {
                self.state.sequences.remove(seq_name);
            }
            return Err(e.into());
        }
        if let Err(commit_err) = self.state.txn_manager.commit(ddl_txn) {
            tracing::warn!(
                error = %commit_err,
                "catalog-write txn failed to commit; restart visibility may differ",
            );
        }
        if let Some(partition) = partition {
            self.state.time_partitions.insert(
                table_name.clone(),
                Arc::new(crate::time_partition::TimePartitionRuntime::daily(
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

    /// Persist and populate an append-only materialized view.
    pub(crate) fn execute_create_materialized_view(
        &mut self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateMaterializedView {
            table_name,
            namespace,
            columns,
            source,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_materialized_view called with wrong plan",
            ));
        };
        let exists_persistent = snapshot.tables.contains_key(table_name);
        let exists_fallback = self.state.catalog.lookup_table(table_name).is_some();
        if exists_persistent || exists_fallback {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE MATERIALIZED VIEW"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(table_name.clone()),
            ));
        }
        let Some(source_table) = crate::append_only_materialized_source_table(source) else {
            return Err(ServerError::Unsupported(
                "CREATE MATERIALIZED VIEW supports append-only SELECT/FILTER/PROJECT over one table",
            ));
        };

        let oid = self.state.persistent_catalog.next_oid();
        let entry = TableEntry::new(oid, table_name.clone(), namespace.clone(), columns.clone());
        self.state.persistent_catalog.create_table(entry.clone())?;

        let runtime = Arc::new(crate::MaterializedViewRuntime {
            view_table: table_name.clone(),
            source_table: source_table.to_ascii_lowercase(),
            source: source.as_ref().clone(),
            materialized_rows: std::sync::atomic::AtomicU64::new(0),
        });
        let attr_has_defaults = vec![false; columns.len()];
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        let ddl_xid = ddl_txn.xid;
        let ddl_command_id = ddl_txn.current_command;
        let materialized_rows = (|| -> Result<u64, ServerError> {
            self.state
                .persistent_catalog
                .persist_relation_rows_with_defaults(
                    &entry,
                    ultrasql_catalog::persistent::RelKind::MaterializedView,
                    &attr_has_defaults,
                    self.state.heap.as_ref(),
                    ddl_xid,
                    ddl_command_id,
                )?;
            self.materialize_view_delta(&runtime, &ddl_txn)
        })();
        let materialized_rows = match materialized_rows {
            Ok(rows) => rows,
            Err(e) => {
                if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "abort of materialized-view DDL txn failed",
                    );
                }
                let _ = self.state.persistent_catalog.drop_table(table_name);
                return Err(e);
            }
        };
        if let Err(commit_err) = self.state.txn_manager.commit(ddl_txn) {
            tracing::warn!(
                error = %commit_err,
                "materialized-view DDL txn failed to commit",
            );
        }
        runtime
            .materialized_rows
            .store(materialized_rows, std::sync::atomic::Ordering::Release);
        self.state
            .note_table_modifications(table_name, materialized_rows);
        self.state
            .materialized_views
            .insert(table_name.clone(), runtime);
        self.plan_cache_invalidate();
        Ok(run_ddl_command("CREATE MATERIALIZED VIEW"))
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
                IndexEntry::new(index_oid, unique.name.clone(), table.oid, attnums, true);
            entry.root_block = root_block;
            // Empty table, so there are no existing heap rows to populate.
            self.state.persistent_catalog.create_index(entry.clone())?;
            created.push(entry);
        }
        Ok(created)
    }

    /// Build a B+ tree index over the supplied table and register it
    /// in `pg_index`.
    ///
    /// The kernel work is split into four steps:
    ///
    /// 1. Validate the request against the current catalog snapshot —
    ///    `IF NOT EXISTS`, presence of the parent table, and key-column
    ///    type compatibility with the B-tree (the v0.5 tree stores
    ///    fixed-size 8-byte keys, so every supported column type is
    ///    mapped into an `i64` by the
    ///    [`crate::index_key::IndexKeyEncoding`] this method picks).
    /// 2. Allocate a fresh OID for the index and instantiate a new
    ///    [`BTree`] over a relation id derived from that OID. The
    ///    buffer pool's blank-page loader hands out empty heap pages
    ///    which `BTree::create` then initialises as B-tree leaves.
    /// 3. Scan every visible row of the parent table under an
    ///    autocommit snapshot, decode the key column(s), and call
    ///    [`BTree::insert`] with the row's [`ultrasql_core::TupleId`].
    /// 4. Build an [`IndexEntry`] carrying the root block plus the
    ///    requested attnums, register it with the persistent catalog,
    ///    and let the catalog's snapshot rotation publish the entry to
    ///    subsequent statements.
    ///
    /// # Supported key shapes
    ///
    /// - Single column of `Int16`, `Int32`, `Int64`, `Bool`,
    ///   `Timestamp`, `TimestampTz`, `Float32`, `Float64`, or `Text`.
    ///   See [`crate::index_key::IndexKeyEncoding`] for the per-type
    ///   mapping. `Text` columns are truncated to their first 8 UTF-8
    ///   bytes; collisions are resolved by a heap-side recheck during
    ///   index probes.
    /// - Two columns of `Bool` / `Int16` / `Int32` packed into a single
    ///   `i64` (`hi << 32 | lo`). Composite probes are recheck-filtered
    ///   to drop bit-pattern collisions.
    /// - Indexes over three or more columns, over wider integer halves,
    ///   and over float / text composites still return
    ///   [`ServerError::Unsupported`] — they require a `Vec<u8>`-keyed
    ///   B-tree, scheduled for the v0.7 wave.
    ///
    /// # Other gaps
    ///
    /// - `UNIQUE` is honoured at the catalog level — the
    ///   [`IndexEntry::is_unique`] flag is propagated — but the
    ///   B-tree's existing duplicate-key rejection is the only
    ///   enforcement. Non-unique indexes that happen to have unique
    ///   data still build correctly; non-unique indexes with
    ///   duplicates would error here, which is a known limitation we
    ///   accept until the B-tree gains a non-unique mode.
    pub(crate) fn execute_create_index(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateIndex {
            index_name,
            table_name,
            columns,
            key_exprs,
            opclasses,
            index_options,
            include_columns,
            predicate,
            method,
            aggregating,
            unique,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_index called with non-CreateIndex plan",
            ));
        };

        // 1a. IF NOT EXISTS short-circuit.
        if snapshot.indexes.contains_key(index_name) {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE INDEX"));
            }
            return Err(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(index_name.clone()),
            ));
        }

        // 1b. Resolve the parent table.
        let table = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.clone(),
            ))
        })?;

        if *method == LogicalIndexMethod::Aggregating {
            if *unique {
                return Err(ServerError::Unsupported(
                    "CREATE UNIQUE AGGREGATING INDEX is not supported",
                ));
            }
            let Some(spec) = aggregating.clone() else {
                return Err(ServerError::ddl(
                    "CREATE AGGREGATING INDEX missing aggregating metadata",
                ));
            };
            let index_oid = self.state.persistent_catalog.next_oid();
            let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
            let build_result = crate::aggregating_index::build_aggregating_index_rows(
                table,
                &spec,
                self.state.heap.as_ref(),
                &txn.snapshot,
                self.state.txn_manager.as_ref(),
            );
            if let Err(e) = self.state.txn_manager.commit(txn) {
                tracing::warn!(error = %e, "autocommit (CREATE AGGREGATING INDEX) failed to finalise");
            }
            let rows = build_result?;
            let attnums = columns
                .iter()
                .map(|col| {
                    u16::try_from(*col).map_err(|_| {
                        ServerError::Unsupported(
                            "CREATE AGGREGATING INDEX: column index does not fit in u16 attnum field",
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let entry = IndexEntry::new(index_oid, index_name.clone(), table.oid, attnums, false);
            self.state.persistent_catalog.create_index(entry.clone())?;
            let ddl_txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            if let Err(e) = self.state.persistent_catalog.persist_index_rows(
                &entry,
                self.state.heap.as_ref(),
                ddl_txn.xid,
                ddl_txn.current_command,
            ) {
                if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "abort of catalog-write txn failed after persist_index_rows error",
                    );
                }
                let _ = self.state.persistent_catalog.drop_index(index_name);
                return Err(e.into());
            }
            if let Err(commit_err) = self.state.txn_manager.commit(ddl_txn) {
                tracing::warn!(
                    error = %commit_err,
                    "catalog-write txn failed to commit; restart visibility may differ",
                );
            }
            let mut constraints = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            constraints.indexes.insert(
                index_oid,
                crate::RuntimeIndexMetadata {
                    key_exprs: key_exprs.clone(),
                    predicate: None,
                    include_columns: Vec::new(),
                    method: *method,
                    brin: None,
                    hnsw: None,
                    ivfflat: None,
                    aggregating: Some(Arc::new(crate::RuntimeAggregatingIndex::new(spec, rows))),
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
            self.plan_cache_invalidate();

            return Ok(run_ddl_command("CREATE INDEX"));
        }

        if *method == LogicalIndexMethod::IvfFlat {
            if *unique {
                return Err(ServerError::Unsupported(
                    "CREATE UNIQUE INDEX USING ivfflat: ivfflat indexes do not enforce uniqueness",
                ));
            }
            if columns.len() != 1 || key_exprs.len() != 1 || !include_columns.is_empty() {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX USING ivfflat: exactly one vector column key is supported",
                ));
            }
            if predicate.is_some() {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX USING ivfflat: partial indexes are not supported in this wave",
                ));
            }
            let vector_col = columns[0];
            let field = table.schema.field(vector_col).ok_or_else(|| {
                ServerError::ddl(format!(
                    "CREATE INDEX USING ivfflat: key column {vector_col} missing"
                ))
            })?;
            let (dims, default_payload) =
                ann_dims_and_default_payload("CREATE INDEX USING ivfflat", &field.data_type)?;
            let metric = hnsw_metric_for_opclass(opclasses.first().and_then(Option::as_deref))?;
            let (lists, probes, payload) = ivfflat_options(index_options)?;
            let payload = payload.unwrap_or(default_payload);
            let index_oid = self.state.persistent_catalog.next_oid();
            let ivfflat = Arc::new(
                PageBackedIvfFlatIndex::new_with_payload_kind(
                    RelationId::new(index_oid.raw()),
                    dims,
                    metric,
                    lists,
                    probes,
                    payload,
                )
                .map_err(|e| ServerError::ddl(format!("CREATE INDEX ivfflat init: {e}")))?,
            );
            let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
            let table_rel = RelationId(table.oid);
            let block_count = self.state.heap.block_count(table_rel).max(table.n_blocks);
            let codec = ultrasql_executor::RowCodec::new(table.schema.clone());
            let scan = self.state.heap.scan_visible(
                table_rel,
                block_count,
                &txn.snapshot,
                self.state.txn_manager.as_ref(),
            );
            let build_result = (|| -> Result<(), ServerError> {
                let mut rows = Vec::new();
                for result in scan {
                    let tuple = result.map_err(|e| {
                        ServerError::ddl(format!("CREATE INDEX ivfflat heap scan: {e}"))
                    })?;
                    let row = codec.decode(&tuple.data).map_err(|e| {
                        ServerError::ddl(format!("CREATE INDEX ivfflat decode: {e}"))
                    })?;
                    let vector = match row.get(vector_col) {
                        Some(Value::Vector(vector) | Value::HalfVec(vector)) => vector.clone(),
                        Some(Value::Null) => continue,
                        _ => {
                            return Err(ServerError::ddl(
                                "CREATE INDEX ivfflat: key column did not decode as vector or halfvec",
                            ));
                        }
                    };
                    rows.push((vector, tuple.tid));
                }
                ivfflat
                    .bulk_load_logged(rows, txn.xid, self.state.heap.wal_sink().map(Arc::as_ref))
                    .map_err(|e| ServerError::ddl(format!("CREATE INDEX ivfflat bulk load: {e}")))
            })();
            if let Err(e) = self.state.txn_manager.commit(txn) {
                tracing::warn!(error = %e, "autocommit (CREATE INDEX ivfflat) failed to finalise");
            }
            build_result?;
            let attnum = u16::try_from(vector_col).map_err(|_| {
                ServerError::Unsupported(
                    "CREATE INDEX: column index does not fit in u16 attnum field",
                )
            })?;
            let entry = IndexEntry::new(
                index_oid,
                index_name.clone(),
                table.oid,
                vec![attnum],
                false,
            )
            .with_access_method("ivfflat", opclasses.clone())
            .with_options(index_options_as_pairs(index_options));
            self.state.persistent_catalog.create_index(entry.clone())?;
            let ddl_txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            if let Err(e) = self.state.persistent_catalog.persist_index_rows(
                &entry,
                self.state.heap.as_ref(),
                ddl_txn.xid,
                ddl_txn.current_command,
            ) {
                if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "abort of catalog-write txn failed after persist_index_rows error",
                    );
                }
                let _ = self.state.persistent_catalog.drop_index(index_name);
                return Err(e.into());
            }
            if let Err(commit_err) = self.state.txn_manager.commit(ddl_txn) {
                tracing::warn!(
                    error = %commit_err,
                    "catalog-write txn failed to commit; restart visibility may differ",
                );
            }
            let mut constraints = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            constraints.indexes.insert(
                index_oid,
                crate::RuntimeIndexMetadata {
                    key_exprs: Vec::new(),
                    predicate: None,
                    include_columns: Vec::new(),
                    method: *method,
                    brin: None,
                    hnsw: None,
                    ivfflat: Some(ivfflat),
                    aggregating: None,
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
            self.plan_cache_invalidate();

            return Ok(run_ddl_command("CREATE INDEX"));
        }

        if *method == LogicalIndexMethod::Hnsw {
            if *unique {
                return Err(ServerError::Unsupported(
                    "CREATE UNIQUE INDEX USING hnsw: hnsw indexes do not enforce uniqueness",
                ));
            }
            if columns.len() != 1 || key_exprs.len() != 1 || !include_columns.is_empty() {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX USING hnsw: exactly one vector column key is supported",
                ));
            }
            if predicate.is_some() {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX USING hnsw: partial indexes are not supported in this wave",
                ));
            }
            let vector_col = columns[0];
            let field = table.schema.field(vector_col).ok_or_else(|| {
                ServerError::ddl(format!(
                    "CREATE INDEX USING hnsw: key column {vector_col} missing"
                ))
            })?;
            let (dims, default_payload) =
                ann_dims_and_default_payload("CREATE INDEX USING hnsw", &field.data_type)?;

            let metric = hnsw_metric_for_opclass(opclasses.first().and_then(Option::as_deref))?;
            let payload = hnsw_payload_option(index_options)?.unwrap_or(default_payload);
            let index_oid = self.state.persistent_catalog.next_oid();
            let index_rel = RelationId::new(index_oid.raw());
            let hnsw = Arc::new(
                PageBackedHnswIndex::new_with_payload_kind(
                    index_rel, dims, metric, 16, 64, payload,
                )
                .map_err(|e| ServerError::ddl(format!("CREATE INDEX hnsw init: {e}")))?,
            );
            let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
            let table_rel = RelationId(table.oid);
            let block_count = self.state.heap.block_count(table_rel).max(table.n_blocks);
            let codec = ultrasql_executor::RowCodec::new(table.schema.clone());
            let scan = self.state.heap.scan_visible(
                table_rel,
                block_count,
                &txn.snapshot,
                self.state.txn_manager.as_ref(),
            );
            let build_result = (|| -> Result<(), ServerError> {
                for result in scan {
                    let tuple = result.map_err(|e| {
                        ServerError::ddl(format!("CREATE INDEX hnsw heap scan: {e}"))
                    })?;
                    let row = codec
                        .decode(&tuple.data)
                        .map_err(|e| ServerError::ddl(format!("CREATE INDEX hnsw decode: {e}")))?;
                    let vector = match row.get(vector_col) {
                        Some(Value::Vector(vector) | Value::HalfVec(vector)) => vector,
                        Some(Value::Null) => continue,
                        _ => {
                            return Err(ServerError::ddl(
                                "CREATE INDEX hnsw: key column did not decode as vector or halfvec",
                            ));
                        }
                    };
                    hnsw.insert_vector_logged(
                        vector,
                        tuple.tid,
                        txn.xid,
                        self.state.heap.wal_sink().map(Arc::as_ref),
                    )
                    .map_err(|e| ServerError::ddl(format!("CREATE INDEX hnsw insert: {e}")))?;
                }
                Ok(())
            })();
            if let Err(e) = self.state.txn_manager.commit(txn) {
                tracing::warn!(error = %e, "autocommit (CREATE INDEX hnsw) failed to finalise");
            }
            build_result?;
            let attnum = u16::try_from(vector_col).map_err(|_| {
                ServerError::Unsupported(
                    "CREATE INDEX: column index does not fit in u16 attnum field",
                )
            })?;
            let entry = IndexEntry::new(
                index_oid,
                index_name.clone(),
                table.oid,
                vec![attnum],
                false,
            )
            .with_access_method("hnsw", opclasses.clone())
            .with_options(index_options_as_pairs(index_options));
            self.state.persistent_catalog.create_index(entry.clone())?;
            let ddl_txn = self
                .state
                .txn_manager
                .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
            if let Err(e) = self.state.persistent_catalog.persist_index_rows(
                &entry,
                self.state.heap.as_ref(),
                ddl_txn.xid,
                ddl_txn.current_command,
            ) {
                if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                    tracing::warn!(
                        error = %abort_err,
                        "abort of catalog-write txn failed after persist_index_rows error",
                    );
                }
                let _ = self.state.persistent_catalog.drop_index(index_name);
                return Err(e.into());
            }
            if let Err(commit_err) = self.state.txn_manager.commit(ddl_txn) {
                tracing::warn!(
                    error = %commit_err,
                    "catalog-write txn failed to commit; restart visibility may differ",
                );
            }
            let mut constraints = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            constraints.indexes.insert(
                index_oid,
                crate::RuntimeIndexMetadata {
                    key_exprs: Vec::new(),
                    predicate: None,
                    include_columns: Vec::new(),
                    method: *method,
                    brin: None,
                    hnsw: Some(hnsw),
                    ivfflat: None,
                    aggregating: None,
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
            self.plan_cache_invalidate();

            return Ok(run_ddl_command("CREATE INDEX"));
        }

        // 1c. Pick an i64 encoding for the requested key shape. The
        //     encoding is shared with the IndexScan probe path via
        //     `pipeline::key_encoding_for_btree` — keep the two
        //     resolutions consistent or a freshly built index will be
        //     unprobe-able.
        let expression_key_exprs = if columns.is_empty() {
            let [expr] = key_exprs.as_slice() else {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX: expression indexes support exactly one key in this wave",
                ));
            };
            let _ = expr;
            key_exprs.clone()
        } else {
            Vec::new()
        };
        let encoding = if *method == ultrasql_planner::LogicalIndexMethod::Hash {
            crate::index_key::IndexKeyEncoding::Int64
        } else if expression_key_exprs.is_empty() {
            crate::index_key::IndexKeyEncoding::for_columns(&table.schema, columns)?
        } else {
            crate::index_key::IndexKeyEncoding::for_data_type(&expression_key_exprs[0].data_type())?
        };
        let key_col_idx = columns.first().copied();

        // 2. Allocate an OID and instantiate the B-tree.
        let index_oid = self.state.persistent_catalog.next_oid();
        let index_rel = RelationId::new(index_oid.raw());
        let pool = self.state.heap.buffer_pool();
        let mut btree = BTree::create(Arc::clone(pool), index_rel)
            .map_err(|e| ServerError::ddl(format!("BTree::create failed: {e}")))?;
        let root_block = btree.root_block();
        let brin_summary = if *method == ultrasql_planner::LogicalIndexMethod::Brin {
            Some(Arc::new(BrinIndex::new(128)))
        } else {
            None
        };

        // 3. Scan the heap and populate the tree.
        let mut attnums: Vec<u16> = Vec::with_capacity(columns.len());
        for &col in columns {
            let attnum = u16::try_from(col).map_err(|_| {
                ServerError::Unsupported(
                    "CREATE INDEX: column index does not fit in u16 attnum field",
                )
            })?;
            attnums.push(attnum);
        }
        let txn = self.state.txn_manager.begin(IsolationLevel::ReadCommitted);
        let table_rel = RelationId(table.oid);
        let block_count = self.state.heap.block_count(table_rel).max(table.n_blocks);
        let scan = self.state.heap.scan_visible(
            table_rel,
            block_count,
            &txn.snapshot,
            self.state.txn_manager.as_ref(),
        );
        let insert_result = (|| -> Result<u64, ServerError> {
            let mut inserted: u64 = 0;
            for result in scan {
                let tup =
                    result.map_err(|e| ServerError::ddl(format!("CREATE INDEX heap scan: {e}")))?;
                let row = decode_key_column(
                    &tup.data,
                    &table.schema,
                    key_col_idx,
                    &expression_key_exprs,
                    predicate.as_ref(),
                    *method,
                    &encoding,
                )?;
                if let Some(key) = row {
                    if *unique {
                        btree.insert(key, tup.tid, txn.xid, None).map_err(|e| {
                            ServerError::ddl(format!("CREATE INDEX btree insert: {e}"))
                        })?;
                    } else {
                        btree
                            .insert_non_unique(key, tup.tid, txn.xid, None)
                            .map_err(|e| {
                                ServerError::ddl(format!("CREATE INDEX btree insert: {e}"))
                            })?;
                    }
                    if let Some(brin) = &brin_summary {
                        let brin_key = BrinIndex::encode_i64_key(key);
                        brin.insert(&brin_key, tup.tid).map_err(|e| {
                            ServerError::ddl(format!("CREATE INDEX brin summarize: {e}"))
                        })?;
                    }
                    inserted += 1;
                }
                // NULL key — skip; PostgreSQL's btree omits NULL keys
                // from the index unless `INCLUDE` adds them, and our
                // BTree::insert lacks a NULL marker.
            }
            Ok(inserted)
        })();

        // Commit the txn regardless of build outcome so the XID does
        // not leak as in-progress forever.
        if let Err(e) = self.state.txn_manager.commit(txn) {
            tracing::warn!(error = %e, "autocommit (CREATE INDEX) failed to finalise");
        }
        let _ = insert_result?;

        // 4. Register the index entry. The columns vector uses the
        //    1-based attnum convention shared with `pg_attribute`; the
        //    `IndexEntry` stores 0-based positions internally, so the
        //    cast is direct. We override `root_block` to match the
        //    freshly built tree.
        let mut entry = IndexEntry::new(index_oid, index_name.clone(), table.oid, attnums, *unique)
            .with_access_method(logical_index_method_name(*method), opclasses.clone())
            .with_options(index_options_as_pairs(index_options));
        entry.root_block = root_block;
        self.state.persistent_catalog.create_index(entry.clone())?;
        let ddl_txn = self
            .state
            .txn_manager
            .begin(ultrasql_txn::IsolationLevel::ReadCommitted);
        if let Err(e) = self.state.persistent_catalog.persist_index_rows(
            &entry,
            self.state.heap.as_ref(),
            ddl_txn.xid,
            ddl_txn.current_command,
        ) {
            if let Err(abort_err) = self.state.txn_manager.abort(ddl_txn) {
                tracing::warn!(
                    error = %abort_err,
                    "abort of catalog-write txn failed after persist_index_rows error",
                );
            }
            let _ = self.state.persistent_catalog.drop_index(index_name);
            return Err(e.into());
        }
        if let Err(commit_err) = self.state.txn_manager.commit(ddl_txn) {
            tracing::warn!(
                error = %commit_err,
                "catalog-write txn failed to commit; restart visibility may differ",
            );
        }
        if !expression_key_exprs.is_empty()
            || predicate.is_some()
            || !include_columns.is_empty()
            || *method != ultrasql_planner::LogicalIndexMethod::Btree
        {
            let mut constraints = self
                .state
                .table_constraints
                .get(&table.oid)
                .map(|entry| entry.value().as_ref().clone())
                .unwrap_or_default();
            constraints.indexes.insert(
                index_oid,
                crate::RuntimeIndexMetadata {
                    key_exprs: expression_key_exprs,
                    predicate: predicate.clone(),
                    include_columns: include_columns.clone(),
                    method: *method,
                    brin: brin_summary.clone(),
                    hnsw: None,
                    ivfflat: None,
                    aggregating: None,
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
        }
        // A new index can flip an existing cached plan from
        // `Filter(SeqScan)` to `IndexScan`; clear the cache so the next
        // statement re-plans against the post-CREATE INDEX catalog.
        self.plan_cache_invalidate();

        Ok(run_ddl_command("CREATE INDEX"))
    }

    /// Drop one or more tables.
    ///
    /// The binder has already filtered names through the catalog —
    /// see [`ultrasql_planner::bind`] — so the only failure surface
    /// here is `CatalogError::NotFound`, which can fire only when a
    /// concurrent DDL deleted the relation between the binder and the
    /// dispatcher. Associated indexes are removed by
    /// [`MutableCatalog::drop_table`] in a single atomic snapshot
    /// rotation.
    ///
    /// Heap pages backing the dropped relation are *not* reclaimed in
    /// this wave: the in-memory buffer pool grows on demand and the
    /// segment manager has not yet landed. The dropped name becomes
    /// available immediately for reuse via `CREATE TABLE` — subsequent
    /// inserts will reuse the relation-id space without colliding
    /// because OIDs are monotonic.
    pub(crate) fn execute_drop_table(
        &self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::DropTable {
            tables, cascade, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_drop_table called with non-DropTable plan",
            ));
        };
        let drop_set: HashSet<String> = tables
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect();
        for name in tables {
            let Some(entry) = self.state.persistent_catalog.lookup_table(name) else {
                continue;
            };
            let dependents = self.foreign_key_dependents(entry.oid, &drop_set);
            if !dependents.is_empty() && !*cascade {
                return Err(ServerError::DependentObjectsStillExist(format!(
                    "cannot drop table {name} because other objects depend on it: {}",
                    dependents.join(", ")
                )));
            }
        }
        for name in tables {
            if let Some(entry) = self.state.persistent_catalog.lookup_table(name) {
                if *cascade {
                    self.drop_foreign_key_dependencies(entry.oid, &drop_set);
                }
                if let Some((_, runtime)) = self.state.time_partitions.remove(name) {
                    for chunk in runtime.chunks.iter() {
                        let _ = self.state.persistent_catalog.drop_table(&chunk.table_name);
                    }
                }
                self.state.columnar_storage.remove(name);
                if let Some((_, constraints)) = self.state.table_constraints.remove(&entry.oid) {
                    for seq_name in constraints.sequence_defaults.iter().flatten() {
                        self.state.sequences.remove(seq_name);
                    }
                }
                self.state
                    .persistent_catalog
                    .clear_descriptions_for_object(entry.oid);
            }
            self.state.persistent_catalog.drop_table(name)?;
        }
        // Any cached plan that referenced this name is now invalid;
        // clear the cache so subsequent statements re-plan.
        self.plan_cache_invalidate();
        Ok(run_ddl_command("DROP TABLE"))
    }

    fn foreign_key_dependents(
        &self,
        target_oid: ultrasql_core::Oid,
        drop_set: &HashSet<String>,
    ) -> Vec<String> {
        let snapshot = self.state.catalog_snapshot();
        let mut out = Vec::new();
        for item in self.state.table_constraints.iter() {
            let table_oid = *item.key();
            let Some(table) = snapshot.tables_by_oid.get(&table_oid) else {
                continue;
            };
            if drop_set.contains(&table.name.to_ascii_lowercase()) {
                continue;
            }
            for fk in &item.value().foreign_keys {
                if fk.target_oid == target_oid {
                    out.push(format!("{}.{}", table.name, fk.name));
                }
            }
        }
        out.sort();
        out
    }

    fn drop_foreign_key_dependencies(
        &self,
        target_oid: ultrasql_core::Oid,
        drop_set: &HashSet<String>,
    ) {
        let snapshot = self.state.catalog_snapshot();
        let mut updates = Vec::new();
        for item in self.state.table_constraints.iter() {
            let table_oid = *item.key();
            let Some(table) = snapshot.tables_by_oid.get(&table_oid) else {
                continue;
            };
            if drop_set.contains(&table.name.to_ascii_lowercase()) {
                continue;
            }
            if item
                .value()
                .foreign_keys
                .iter()
                .any(|fk| fk.target_oid == target_oid)
            {
                let mut next = item.value().as_ref().clone();
                next.foreign_keys.retain(|fk| fk.target_oid != target_oid);
                updates.push((table_oid, next));
            }
        }
        for (table_oid, constraints) in updates {
            self.state
                .table_constraints
                .insert(table_oid, Arc::new(constraints));
        }
    }

    pub(crate) fn execute_comment(
        &self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::Comment {
            target, comment, ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_comment called with non-Comment plan",
            ));
        };
        let (objoid, objsubid) = match target {
            LogicalCommentTarget::Table { table } => {
                let entry = snapshot
                    .tables
                    .get(table)
                    .ok_or_else(|| ultrasql_catalog::CatalogError::not_found(table.clone()))?;
                (entry.oid, 0)
            }
            LogicalCommentTarget::Index { index } => {
                let entry = snapshot
                    .indexes
                    .get(index)
                    .ok_or_else(|| ultrasql_catalog::CatalogError::not_found(index.clone()))?;
                (entry.oid, 0)
            }
            LogicalCommentTarget::Column { table, attnum, .. } => {
                let entry = snapshot
                    .tables
                    .get(table)
                    .ok_or_else(|| ultrasql_catalog::CatalogError::not_found(table.clone()))?;
                (entry.oid, *attnum)
            }
        };
        self.state.persistent_catalog.set_description(
            objoid,
            ultrasql_core::Oid::new(ultrasql_catalog::bootstrap::PG_CLASS_OID),
            objsubid,
            comment.clone(),
        );
        self.plan_cache_invalidate();
        Ok(run_ddl_command("COMMENT"))
    }
}

fn hnsw_metric_for_opclass(opclass: Option<&str>) -> Result<HnswMetric, ServerError> {
    match opclass.unwrap_or("vector_l2_ops") {
        "vector_l2_ops" => Ok(HnswMetric::L2),
        "vector_cosine_ops" => Ok(HnswMetric::Cosine),
        "vector_ip_ops" => Ok(HnswMetric::NegativeInnerProduct),
        "vector_l1_ops" => Ok(HnswMetric::L1),
        other => Err(ServerError::ddl(format!(
            "CREATE INDEX USING hnsw: unsupported vector opclass {other}"
        ))),
    }
}

fn logical_index_method_name(method: LogicalIndexMethod) -> &'static str {
    match method {
        LogicalIndexMethod::Btree => "btree",
        LogicalIndexMethod::Hash => "hash",
        LogicalIndexMethod::Gin => "gin",
        LogicalIndexMethod::Gist => "gist",
        LogicalIndexMethod::Brin => "brin",
        LogicalIndexMethod::Hnsw => "hnsw",
        LogicalIndexMethod::IvfFlat => "ivfflat",
        LogicalIndexMethod::Aggregating => "aggregating",
    }
}

fn index_options_as_pairs(options: &[LogicalIndexOption]) -> Vec<(String, String)> {
    options
        .iter()
        .map(|option| (option.name.clone(), option.value.clone()))
        .collect()
}

fn ann_dims_and_default_payload(
    context: &str,
    data_type: &DataType,
) -> Result<(u32, AnnPayloadKind), ServerError> {
    match data_type {
        DataType::Vector { dims: Some(dims) } => Ok((*dims, AnnPayloadKind::F32)),
        DataType::HalfVec { dims: Some(dims) } => Ok((*dims, AnnPayloadKind::Bf16)),
        other => Err(ServerError::ddl(format!(
            "{context} requires vector(n) or halfvec(n), got {other}"
        ))),
    }
}

fn hnsw_payload_option(
    options: &[LogicalIndexOption],
) -> Result<Option<AnnPayloadKind>, ServerError> {
    let mut payload = None;
    for option in options {
        if option.name != "payload" {
            return Err(ServerError::ddl(format!(
                "CREATE INDEX USING hnsw: unsupported option {}",
                option.name
            )));
        }
        payload = Some(ann_payload_kind_from_value(
            "CREATE INDEX USING hnsw",
            &option.value,
        )?);
    }
    Ok(payload)
}

fn ann_payload_kind_from_value(context: &str, value: &str) -> Result<AnnPayloadKind, ServerError> {
    match value.to_ascii_lowercase().as_str() {
        "f32" | "float32" => Ok(AnnPayloadKind::F32),
        "bf16" | "bfloat16" => Ok(AnnPayloadKind::Bf16),
        "int8" | "i8" => Ok(AnnPayloadKind::Int8),
        other => Err(ServerError::ddl(format!(
            "{context}: unsupported payload {other}; expected f32, bf16, or int8"
        ))),
    }
}

fn ivfflat_options(
    options: &[LogicalIndexOption],
) -> Result<(usize, usize, Option<AnnPayloadKind>), ServerError> {
    let mut lists = 100_usize;
    let mut probes = 1_usize;
    let mut payload = None;
    for option in options {
        match option.name.as_str() {
            "lists" => lists = parse_positive_ivfflat_option(option)?,
            "probes" => probes = parse_positive_ivfflat_option(option)?,
            "payload" => {
                payload = Some(ann_payload_kind_from_value(
                    "CREATE INDEX USING ivfflat",
                    &option.value,
                )?);
            }
            other => {
                return Err(ServerError::ddl(format!(
                    "CREATE INDEX USING ivfflat: unsupported option {other}"
                )));
            }
        }
    }
    Ok((lists, probes, payload))
}

fn parse_positive_ivfflat_option(option: &LogicalIndexOption) -> Result<usize, ServerError> {
    let parsed = option.value.parse::<usize>().map_err(|_| {
        ServerError::ddl(format!(
            "CREATE INDEX USING ivfflat: option {} must be a positive integer",
            option.name
        ))
    })?;
    if parsed == 0 {
        return Err(ServerError::ddl(format!(
            "CREATE INDEX USING ivfflat: option {} must be greater than zero",
            option.name
        )));
    }
    Ok(parsed)
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
