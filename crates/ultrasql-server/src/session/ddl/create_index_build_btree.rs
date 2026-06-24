//! B-tree (and BRIN / Hash) index builder for `CREATE INDEX`. Part of
//! the `session::ddl` module split; reopens the `impl<RW> Session<RW>`
//! block defined in `session/mod.rs`.
//!
//! Split out from `create_index_build.rs` to keep each file under the
//! 700-line ceiling. Logic is moved verbatim; see
//! [`Session::execute_create_index`] for the supported key shapes and
//! gaps.

use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{IndexEntry, MutableCatalog, TableEntry};
use ultrasql_core::RelationId;
use ultrasql_planner::{LogicalIndexMethod, LogicalIndexOption, ScalarExpr};
use ultrasql_storage::access_method::{AccessMethod, BrinIndex};
use ultrasql_storage::btree::BTree;
use ultrasql_txn::IsolationLevel;

use super::super::Session;
use super::index_options::{index_options_as_pairs, logical_index_method_name};
use super::{CreateIndexProgressGuard, log_failed_ddl_rollback};
use crate::decode_key_column;
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Build a B+ tree index (optionally also a BRIN summary, or under
    /// the Hash access method) and register it in `pg_index`. See the
    /// module-level docs on [`Session::execute_create_index`] for the
    /// supported key shapes and gaps.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_btree_index(
        &self,
        table: &TableEntry,
        index_name: &str,
        index_namespace: &str,
        columns: &[usize],
        key_exprs: &[ScalarExpr],
        opclasses: &[Option<String>],
        index_options: &[LogicalIndexOption],
        include_columns: &[usize],
        predicate: &Option<ScalarExpr>,
        method: LogicalIndexMethod,
        unique: bool,
        primary_key: bool,
        index_key: &str,
    ) -> Result<SelectResult, ServerError> {
        // 1c. Pick an i64 encoding for the requested key shape. The
        //     encoding is shared with the IndexScan probe path via
        //     `pipeline::key_encoding_for_btree` — keep the two
        //     resolutions consistent or a freshly built index will be
        //     unprobe-able.
        let expression_key_exprs = if columns.is_empty() {
            let [expr] = key_exprs else {
                return Err(ServerError::Unsupported(
                    "CREATE INDEX: expression indexes support exactly one key in this wave",
                ));
            };
            let _ = expr;
            key_exprs.to_vec()
        } else {
            Vec::new()
        };
        let encoding = if method == ultrasql_planner::LogicalIndexMethod::Hash {
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
        let brin_summary = if method == ultrasql_planner::LogicalIndexMethod::Brin {
            Some(Arc::new(BrinIndex::new(128)))
        } else {
            None
        };
        let writes_runtime_index_metadata = !expression_key_exprs.is_empty()
            || predicate.is_some()
            || !include_columns.is_empty()
            || method != ultrasql_planner::LogicalIndexMethod::Btree;
        if writes_runtime_index_metadata {
            self.state
                .ensure_table_runtime_constraints_metadata_slots_persistable()?;
        }

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
        let progress = CreateIndexProgressGuard::new(
            self.state.workload_recorder.as_ref(),
            self.pid,
            table.oid.raw(),
            index_oid.raw(),
            block_count,
        );
        progress.update("building index", 0);
        let scan = self.state.heap.scan_visible(
            table_rel,
            block_count,
            &txn.snapshot,
            self.state.txn_manager.as_ref(),
        );
        let insert_result = (|| -> Result<u64, ServerError> {
            let mut inserted: u64 = 0;
            let mut last_progress_block = 0;
            for result in scan {
                let tup =
                    result.map_err(|e| ServerError::ddl(format!("CREATE INDEX heap scan: {e}")))?;
                let blocks_done = tup.tid.page.block.raw().saturating_add(1).min(block_count);
                if blocks_done != last_progress_block {
                    progress.update("building index", blocks_done);
                    last_progress_block = blocks_done;
                }
                let row = decode_key_column(
                    &tup.data,
                    &table.schema,
                    key_col_idx,
                    &expression_key_exprs,
                    predicate.as_ref(),
                    method,
                    &encoding,
                )?;
                if let Some(key) = row {
                    if unique {
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
        self.state
            .commit_transaction(txn, true, "CREATE INDEX build")?;
        let _ = insert_result?;
        progress.update("writing catalog", block_count);

        // 4. Register the index entry. The columns vector uses the
        //    1-based attnum convention shared with `pg_attribute`; the
        //    `IndexEntry` stores 0-based positions internally, so the
        //    cast is direct. We override `root_block` to match the
        //    freshly built tree.
        let mut entry = IndexEntry::new(
            index_oid,
            index_name.to_string(),
            table.oid,
            attnums,
            unique,
        )
        .with_schema_name(index_namespace.to_string())
        .with_primary(primary_key)
        .with_access_method(logical_index_method_name(method), opclasses.to_vec())
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
            log_failed_ddl_rollback(
                self.state.persistent_catalog.drop_index(index_key),
                "drop index",
            );
            return Err(self.rollback_catalog_transaction_after_error(
                ddl_txn,
                e.into(),
                "CREATE INDEX catalog rollback after persist error",
            ));
        }
        self.state
            .commit_transaction(ddl_txn, true, "CREATE INDEX catalog transaction")?;
        if writes_runtime_index_metadata {
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
                    include_columns: include_columns.to_vec(),
                    method,
                    brin: brin_summary.clone(),
                    hnsw: None,
                    ivfflat: None,
                    aggregating: None,
                },
            );
            self.state
                .table_constraints
                .insert(table.oid, Arc::new(constraints));
            self.state.persist_table_runtime_constraints_metadata()?;
        }
        // A new index can flip an existing cached plan from
        // `Filter(SeqScan)` to `IndexScan`; clear the cache so the next
        // statement re-plans against the post-CREATE INDEX catalog.
        self.plan_cache_invalidate();

        Ok(run_ddl_command("CREATE INDEX"))
    }

    /// Transactional `CREATE INDEX` on an EXISTING table (milestone 3).
    ///
    /// Mirrors the autocommit `build_btree_index` split but DEFERS the durable
    /// B-tree segment to COMMIT, exactly as milestone 2 does for implicit
    /// constraint indexes:
    ///
    /// - reject the out-of-scope shapes (expression / partial / `INCLUDE` /
    ///   `CONCURRENTLY` / non-B-tree) with `0A000` (defensive — the gate
    ///   already filtered them, but this is the authoritative reject);
    /// - reject the same-txn-created-table scope boundary (the target table
    ///   lives in this session's overlay) cleanly with `0A000`;
    /// - take AccessExclusive on the TARGET table (keyed on the user xid,
    ///   released by `release_all` at COMMIT/ROLLBACK) so two in-txn
    ///   `CREATE INDEX` on the same table serialize (the loser gets `40001`);
    /// - allocate the index OID, build the `IndexEntry` with
    ///   `root_block == INVALID`, persist its `pg_index` rows under the USER
    ///   xid (UNBUILT — no `BTree::create`, no `create_index` publish), and
    ///   stash the entry into the overlay's `extra_indexes`;
    /// - mark `pending_table_modifications` for the target table so COMMIT
    ///   writes a durable marker.
    ///
    /// The deferred tree is built — over the existing table's rows under the
    /// user snapshot — at COMMIT by
    /// [`Session::build_pending_catalog_ddl_indexes`]; a duplicate on a
    /// `CREATE UNIQUE INDEX` aborts the whole transaction with `23505`. A
    /// ROLLBACK (or crash before COMMIT) leaks no durable segment — none was
    /// built — and the UNBUILT `pg_index` rows ride the aborted user xid
    /// (MVCC-invisible, bootstrap-hidden).
    pub(super) fn execute_create_index_in_txn(
        &mut self,
        plan: &ultrasql_planner::LogicalPlan,
        table: &TableEntry,
        user_xid: ultrasql_core::Xid,
    ) -> Result<SelectResult, ServerError> {
        let ultrasql_planner::LogicalPlan::CreateIndex {
            index_name,
            index_namespace,
            columns,
            opclasses,
            index_options,
            method,
            unique,
            primary_key,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_index_in_txn called with non-CreateIndex plan",
            ));
        };

        // Out-of-scope shape (defensive). A non-txn-safe `CREATE INDEX` reaching
        // here would mean the gate let it through; reject with `0A000` and fail
        // the block rather than stage an index whose sidecar the overlay cannot
        // roll back.
        if !Self::create_index_is_txn_safe(plan) {
            return Err(self.fail_if_in_transaction(ServerError::UnsupportedOwned(
                "this CREATE INDEX form inside an explicit transaction is not yet supported\n\
                 HINT:  only a plain B-tree index (no expression / partial / INCLUDE / \
                 CONCURRENTLY) can be created in a transaction; create it in autocommit"
                    .to_string(),
            )));
        }

        // Scope boundary: a `CREATE INDEX` on a table CREATED EARLIER IN THE
        // SAME TRANSACTION is deferred to a follow-up. The created table lives
        // in this session's overlay (not the global catalog); building an index
        // over it would have to thread the overlay's unbuilt table through the
        // deferred-build path. Reject it cleanly for now with `0A000`.
        if let Some(overlay) = self.pending_catalog_ddl.as_ref()
            && overlay
                .table
                .as_ref()
                .is_some_and(|created| created.oid == table.oid)
        {
            return Err(self.fail_if_in_transaction(ServerError::UnsupportedOwned(
                "CREATE INDEX on a table created earlier in the same transaction is not yet \
                 supported\nHINT:  commit the CREATE TABLE first, or create the index in autocommit"
                    .to_string(),
            )));
        }

        // Overlay-clobber guard (milestones 1–3): the session catalog overlay
        // (`pending_catalog_ddl`) holds ONE schema-changing statement. If an
        // overlay already exists here, a PRIOR in-txn DDL statement (e.g. a
        // CREATE TABLE on a different relation, or an earlier CREATE INDEX) has
        // already staged its rows. Appending this index's `extra_indexes` —
        // after persisting its pg_index rows — silently grows a SECOND producer
        // of the single overlay, which the COMMIT-time deferred build does not
        // robustly reconcile (the symmetric `execute_create_table` path would
        // overwrite it). Reject the second statement with `0A000` BEFORE any
        // durable persist (no pg_index rows) and BEFORE the overlay is touched,
        // so the first statement's overlay survives for a later ROLLBACK/COMMIT.
        // (The same-table-created combo is the more specific reject above.)
        if self.pending_catalog_ddl.is_some() {
            return Err(self.fail_if_in_transaction(ServerError::UnsupportedOwned(
                "a second schema-changing statement inside an explicit transaction is not yet \
                 supported\nHINT:  only one schema-changing statement is supported per \
                 transaction so far; commit and start a new transaction"
                    .to_string(),
            )));
        }

        // Validate the key encoding up front (same resolution the IndexScan
        // probe path uses) so an unsupported key shape fails before any durable
        // write — and before the name lock is taken.
        let _ = crate::index_key::IndexKeyEncoding::for_columns(&table.schema, columns)?;

        // Take AccessExclusive on the TARGET table, keyed on the user xid, with
        // a non-blocking `try_acquire` (the engine's lock discipline — parking a
        // tokio worker on a cross-transaction lock would stall the runtime). The
        // grant rides the user xid and is auto-released by `release_all` at the
        // user COMMIT/ROLLBACK. Two in-txn `CREATE INDEX` on the same table thus
        // serialize: the loser fails immediately with `40001`.
        let table_key = super::table_entry_lookup_key(table);
        let name_lock_tag = super::create_table::create_table_name_lock_tag(&table_key);
        let acquired = self
            .state
            .txn_manager
            .lock_manager
            .try_acquire(ultrasql_txn::LockRequest {
                xid: user_xid,
                tag: name_lock_tag,
                mode: ultrasql_txn::LockMode::AccessExclusive,
            })
            .map_err(|e| ServerError::ddl(format!("CREATE INDEX relation lock: {e}")))?;
        if !acquired {
            return Err(
                self.fail_if_in_transaction(ServerError::SerializationFailure(format!(
                    "could not obtain lock on relation \"{}\": another transaction is creating an \
                     index on it concurrently",
                    table.name
                ))),
            );
        }

        // Allocate the OID and build the IndexEntry UNBUILT (root_block stays
        // INVALID). The B-tree is NOT created here — the deferred build at
        // COMMIT allocates the segment over the table's rows. No global
        // `create_index` publish either: the entry lives only in the overlay
        // until COMMIT.
        let index_oid = self.state.persistent_catalog.next_oid();
        let mut attnums: Vec<u16> = Vec::with_capacity(columns.len());
        for &col in columns {
            let attnum = u16::try_from(col).map_err(|_| {
                ServerError::Unsupported(
                    "CREATE INDEX: column index does not fit in u16 attnum field",
                )
            })?;
            attnums.push(attnum);
        }
        let entry = IndexEntry::new(index_oid, index_name.clone(), table.oid, attnums, *unique)
            .with_schema_name(index_namespace.clone())
            .with_primary(*primary_key)
            .with_access_method(logical_index_method_name(*method), opclasses.to_vec())
            .with_options(index_options_as_pairs(index_options));
        debug_assert_eq!(entry.root_block, ultrasql_core::BlockNumber::INVALID);

        // Persist the pg_index / pg_class rows UNBUILT under the user xid +
        // command id. They are MVCC-invisible until COMMIT and hidden by the
        // visibility-filtered bootstrap after a crash; the COMMIT build
        // re-persists them with the real root_block. On a persist failure the
        // user xid stays uncommitted, so the partial rows are harmless; fail the
        // block.
        let command_id = match &self.txn_state {
            crate::TxnState::InTransaction(txn) => txn.current_command,
            // Unreachable: `user_xid` came from an `InTransaction` state.
            _ => ultrasql_core::CommandId::FIRST,
        };
        if let Err(e) = self.state.persistent_catalog.persist_index_rows(
            &entry,
            self.state.heap.as_ref(),
            user_xid,
            command_id,
        ) {
            return Err(self.fail_if_in_transaction(e.into()));
        }

        // Stage the entry in a freshly created overlay. The overlay-clobber
        // guard above guarantees `pending_catalog_ddl` is `None` here — at most
        // one schema-changing statement is staged per transaction — so this
        // CREATE INDEX is the sole producer of the overlay. A pure `CREATE
        // INDEX` overlay carries `table == None`.
        debug_assert!(self.pending_catalog_ddl.is_none());
        self.pending_catalog_ddl = Some(super::super::catalog_overlay::CatalogOverlay {
            xid: user_xid,
            table: None,
            indexes: Vec::new(),
            constraints: Vec::new(),
            extra_indexes: vec![entry],
            extra_index_constraints: Vec::new(),
            staged: None,
        });

        // Mark the target table modified so `commit_transaction`'s
        // `modified_tables` is non-empty → a durable commit marker is written,
        // making the user-xid pg_index rows visible after restart.
        self.pending_table_modifications
            .entry(table_key)
            .or_insert(0);

        // The new (pending) index can flip a cached plan to `IndexScan` for the
        // issuing session; clear the cache so the next statement re-plans
        // against the overlay-folded snapshot.
        self.plan_cache_invalidate();

        Ok(run_ddl_command("CREATE INDEX"))
    }
}
