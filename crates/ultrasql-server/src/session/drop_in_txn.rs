//! In-transaction `DROP TABLE` via a negative-mask catalog overlay
//! (transactional-DDL milestone 5).
//!
//! When a plain (`RESTRICT`, non-`CASCADE`) `DROP TABLE` runs inside an explicit
//! `BEGIN … COMMIT` block, the global catalog must NOT be mutated mid-statement:
//! a concurrent session would observe the table vanish before the transaction is
//! durable, and a `ROLLBACK` could not resurrect it — a rolled-back DROP that
//! stays gone is silent loss of a committed table WITH its data + indexes. The
//! whole point of M5 is to avoid that.
//!
//! THE INVARIANT: the in-txn DROP handler mutates NOTHING in the global catalog
//! and emits NO `SequenceOp::Drop` WAL. For a table committed BEFORE this
//! transaction it stages a durable `pg_class` `RelKind::Dropped` tombstone under
//! the USER xid (not a self-committing inner txn — that is the autocommit
//! pattern that would make the drop durable immediately and un-rollback-able)
//! plus a session-local negative mask (`overlay.dropped_oids`) that hides the
//! table, its indexes, and its constraints from the issuing session's effective
//! snapshot. COMMIT applies the real global `drop_table` + the in-memory
//! teardown once the user xid is durably committed; ROLLBACK/crash discard the
//! overlay (free — nothing global was touched) and the tombstone rides the
//! aborted user xid (MVCC-invisible + bootstrap-hidden), so the committed table
//! reappears on restart.
//!
//! A table CREATED earlier in THIS transaction is un-staged from
//! `created_tables` / `indexes` / `constraints` / etc. (so the COMMIT publish
//! never creates it and the deferred index build never targets it) and masked.
//! It STILL gets a durable tombstone under the user xid: its `pg_class` CREATE
//! rows are already on disk under that xid, so without a later Dropped row for
//! the same OID a restart's latest-row-per-OID bootstrap would resurrect it
//! after the txn commits. The tombstone's later command id sorts it after the
//! CREATE row, so latest-per-OID yields Dropped (net-absent); on ROLLBACK both
//! rows ride the aborted xid and are bootstrap-hidden.
//!
//! Every shape with a non-MVCC sidecar the negative mask cannot transactionally
//! revert — an owned sequence (whose `SequenceOp::Drop` WAL is replayed
//! unconditionally), RLS, a dependent view/matview, a time-partition
//! parent/chunk, an inbound/outbound FK, a columnar shadow / custom stats /
//! comments, or a system table — is rejected in-txn with `0A000` + a HINT and
//! stays fully supported in autocommit.

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{TableEntry, table_lookup_key};
use ultrasql_core::Xid;

use super::Session;
use super::catalog_overlay::{CatalogOverlay, DroppedTableState};
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Stage one or more in-transaction `DROP TABLE` targets as a negative mask
    /// (milestone 5). Reached only from [`Session::execute_drop_table`] when an
    /// explicit transaction is open and the gate
    /// ([`Session::drop_table_is_txn_safe`]) admitted the (non-CASCADE) plan.
    ///
    /// `tables` are the lowercase target names in user order; `if_exists`
    /// suppresses the `42P01` on an absent target. Each target is resolved
    /// through the EFFECTIVE (overlay-folded) snapshot so a same-txn-created /
    /// altered / already-in-txn-dropped target is honoured — that is what makes
    /// `DROP t; DROP t` (bare → `42P01`, IF EXISTS → no-op) and `ALTER t; DROP t`
    /// compose correctly.
    pub(crate) fn execute_drop_table_in_txn(
        &mut self,
        tables: &[String],
        if_exists: bool,
        user_xid: Xid,
    ) -> Result<SelectResult, ServerError> {
        for name in tables {
            self.drop_one_table_in_txn(name, if_exists, user_xid)?;
        }
        Ok(run_ddl_command("DROP TABLE"))
    }

    /// Stage a single in-txn `DROP TABLE` target.
    fn drop_one_table_in_txn(
        &mut self,
        name: &str,
        if_exists: bool,
        user_xid: Xid,
    ) -> Result<(), ServerError> {
        // (a) Resolve through the EFFECTIVE snapshot. An absent target with
        // IF EXISTS is a no-op success; a bare DROP of an absent target is
        // `42P01` (CatalogError::NotFound). Resolving through the effective
        // snapshot honours a same-txn CREATE/ALTER and an earlier in-txn DROP
        // (whose mask already hid the table).
        let effective = self.effective_catalog_snapshot();
        let Some(entry) = effective.tables.get(name).cloned() else {
            if if_exists {
                return Ok(());
            }
            return Err(self.fail_if_in_transaction(ServerError::Catalog(
                ultrasql_catalog::CatalogError::not_found(name.to_owned()),
            )));
        };
        self.ensure_table_owner_or_superuser(entry.oid, name)?;

        // Is this OID a table created earlier in THIS transaction? (Probe the
        // overlay's `created_tables` BEFORE the lock so the same-txn fast path
        // and the committed-before path take the same lock.)
        let was_same_txn_created = self
            .pending_catalog_ddl
            .as_ref()
            .is_some_and(|overlay| overlay.created_tables.iter().any(|t| t.oid == entry.oid));

        // The COMMIT-time global `drop_table` and the durable-marker key must use
        // the key the table is filed under in the GLOBAL catalog — which, for a
        // committed-before table ALTERed (RENAMEd) earlier in this same
        // transaction, is the PRE-ALTER name, NOT the post-ALTER name carried by
        // the effective entry (the ALTER op only stages a replay, it does not
        // mutate the global catalog). A same-txn-created table is not in the
        // global catalog at all, so the effective key (its create-time name) is
        // the right key for the mask. Resolve the global key by OID.
        let table_key = if was_same_txn_created {
            table_lookup_key(&entry.schema_name, &entry.name)
        } else {
            self.state
                .catalog_snapshot()
                .tables_by_oid
                .get(&entry.oid)
                .map_or_else(
                    || table_lookup_key(&entry.schema_name, &entry.name),
                    |global| table_lookup_key(&global.schema_name, &global.name),
                )
        };

        // (b) AccessExclusive name-lock on the TARGET, keyed on the user xid and
        // taken via the non-blocking `try_acquire` the rest of transactional DDL
        // uses (re-entrant for the same xid, so DROP-then-recreate of the same
        // name in one txn does not self-deadlock; the loser of a cross-txn race
        // gets `40001`). DROP today takes NO lock — this ADDS the serialization
        // point so two sessions cannot both stage a conflicting drop/create of
        // one name and both reach durable commit.
        self.drop_in_txn_lock(&table_key, user_xid)?;

        // (c) The full STATE-dependent reject predicate, run BEFORE any durable
        // write or WAL emit. Each blocker is a non-MVCC sidecar the negative
        // mask cannot transactionally revert.
        self.reject_unsafe_in_txn_drop(&entry, &table_key, was_same_txn_created)?;

        // (f) Pre-flight the post-commit sidecar metadata write slots so a
        // COMMIT-time flush cannot fail on a full disk after the user xid is
        // already durably committed.
        self.state
            .ensure_drop_table_runtime_metadata_slots_persistable(std::slice::from_ref(
                &table_key,
            ))?;

        // (d) SAME-TXN-CREATED FAST PATH (un-stage): strip the OID from every
        // additive/override staging vector so the COMMIT publish never creates
        // it and the deferred index build (`build_pending_catalog_ddl_indexes`)
        // never scans a masked heap. The COMMIT then publishes/drops NOTHING
        // globally for this OID (it was never in the global catalog).
        if was_same_txn_created {
            self.unstage_same_txn_created(&entry);
        }

        // (e) ALWAYS stage the durable `RelKind::Dropped` tombstone under the
        // USER xid (NOT a self-committing inner txn) — for a committed-before
        // table AND a same-txn-created one.
        //
        // CORRUPTION FIX (durability): a same-txn-created table's `pg_class` /
        // `pg_attribute` CREATE rows are ALREADY on disk under the user xid
        // (`persist_create_table_rows_under_xid`). If the txn COMMITS, the commit
        // marker makes them visible, and bootstrap's latest-row-per-OID rule
        // would RESURRECT the table on restart unless a Dropped row for the same
        // OID sorts AFTER the CREATE row. Un-staging from `created_tables` only
        // prevents the in-memory re-publish; it does NOT touch the durable heap.
        // So we ALWAYS append the tombstone (its later command id sorts it after
        // the CREATE row → latest-per-OID yields Dropped → absent). On ROLLBACK
        // both the CREATE and the Dropped rows ride the aborted xid and are
        // bootstrap-hidden. Do NOT call the global `drop_table` and do NOT run
        // the sidecar fan here — a committed-before drop defers both to COMMIT;
        // a same-txn-created drop has nothing global to tear down.
        let command_id = self.drop_in_txn_command_id();
        self.state
            .persistent_catalog
            .persist_table_drop_tombstone(&entry, self.state.heap.as_ref(), user_xid, command_id)
            .map_err(|e| self.fail_if_in_transaction(e.into()))?;

        // Insert the mask + the COMMIT/ROLLBACK record into the overlay.
        self.stage_drop(user_xid, entry.oid, table_key.clone(), was_same_txn_created);

        // (g) Mark the table key modified so `commit_transaction` writes a
        // durable commit marker for the user xid (making the user-xid tombstone
        // rows visible after restart).
        self.pending_table_modifications
            .entry(table_key)
            .or_insert(0);

        // (h) A staged drop can shadow a cached plan bound against the pre-drop
        // snapshot; clear the bind cache so the next statement re-resolves
        // through the overlay-folded (masked) snapshot.
        self.plan_cache_invalidate();
        Ok(())
    }

    /// Take AccessExclusive on `table_key`, keyed on the user xid (released by
    /// `release_all` at COMMIT/ROLLBACK). Non-blocking `try_acquire`; the loser
    /// fails `40001`. Re-entrant for the same xid, so a DROP then a same-name
    /// recreate in one transaction both hold the grant without self-deadlock.
    fn drop_in_txn_lock(&mut self, table_key: &str, user_xid: Xid) -> Result<(), ServerError> {
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
            .map_err(|e| ServerError::ddl(format!("DROP TABLE relation lock: {e}")))?;
        if !acquired {
            return Err(
                self.fail_if_in_transaction(ServerError::SerializationFailure(format!(
                    "could not obtain lock on relation \"{table_key}\": another transaction is \
                     dropping or creating it concurrently"
                ))),
            );
        }
        Ok(())
    }

    /// The current command id of the active in-transaction.
    fn drop_in_txn_command_id(&self) -> ultrasql_core::CommandId {
        match &self.txn_state {
            crate::TxnState::InTransaction(txn) => txn.current_command,
            _ => ultrasql_core::CommandId::FIRST,
        }
    }

    /// The full reject predicate (§1) against a resolved DROP target. Any
    /// blocker that the negative-mask overlay cannot transactionally revert
    /// fails with `0A000` + a HINT. A `was_same_txn_created` target is clean by
    /// construction (serial/partition/FK CREATE are all rejected in-txn, so it
    /// can carry none of these sidecars) — its checks are skipped.
    fn reject_unsafe_in_txn_drop(
        &mut self,
        entry: &TableEntry,
        table_key: &str,
        was_same_txn_created: bool,
    ) -> Result<(), ServerError> {
        // (g) System / catalog table — never tombstone a bootstrapped relation.
        if entry.schema_name.eq_ignore_ascii_case("pg_catalog")
            || entry.schema_name.eq_ignore_ascii_case("information_schema")
        {
            return Err(self.reject_drop(
                "a system catalog table cannot be dropped inside an explicit transaction",
            ));
        }

        // A same-txn-created table cannot own any of the sidecars below (its
        // CREATE rejected serial/partition/FK in-txn), so skip the rest. A
        // same-txn-created table that was also ALTERed is handled by the
        // un-stage fast path (which strips its `altered_*` staging), so the
        // ALTER-pending reject below targets the committed-before case only.
        if was_same_txn_created {
            return Ok(());
        }

        // A committed-before table that was ALTERed earlier in THIS transaction
        // carries a pending `altered_staged` op (with in-memory side-map edits —
        // e.g. a RENAME renamed its privilege grants in place). Dropping it would
        // have to BOTH discard the ALTER replay AND clean up those renamed
        // in-memory maps at COMMIT / restore them at ROLLBACK — a revert surface
        // the minimal negative-mask scope does not model. Reject `0A000`; the
        // ALTER then the DROP each work fine in autocommit.
        let has_pending_alter = self
            .pending_catalog_ddl
            .as_ref()
            .is_some_and(|overlay| overlay.altered_staged.iter().any(|s| s.oid == entry.oid));
        if has_pending_alter {
            return Err(self.reject_drop(
                "the table was altered earlier in this transaction; drop it in autocommit",
            ));
        }

        // (a) Owns a sequence (SERIAL / IDENTITY) — THE hard blocker. The
        // autocommit fan emits `SequenceOp::Drop` WAL with `Xid::INVALID`, which
        // is replayed UNCONDITIONALLY on recovery: a rolled-back in-txn DROP
        // would still vaporize a still-referenced sequence. Reject BEFORE any
        // WAL emit. (e, outbound) A table carrying its own FOREIGN KEY
        // constraints is a non-MVCC runtime sidecar the mask cannot revert
        // per-table. Probe both from one runtime-constraints read so the DashMap
        // guard drops before the mutable `reject_drop`.
        let (owns_sequence, has_outbound_fk) = self
            .state
            .table_constraints
            .get(&entry.oid)
            .map(|runtime| {
                (
                    runtime.sequence_defaults.iter().flatten().next().is_some(),
                    !runtime.foreign_keys.is_empty(),
                )
            })
            .unwrap_or((false, false));
        if owns_sequence {
            return Err(self.reject_drop(
                "the table owns a sequence whose drop is replayed unconditionally on restart; \
                 drop it in autocommit",
            ));
        }
        if has_outbound_fk {
            return Err(self.reject_drop(
                "the table has outbound FOREIGN KEY constraints; drop it in autocommit",
            ));
        }

        // (b) RLS policies — reversible in-memory but adds a metadata-file revert
        // surface; defer to a later milestone.
        let has_rls = self
            .state
            .row_security
            .get(&entry.oid)
            .is_some_and(|rls| rls.enabled || !rls.policies.is_empty());
        if has_rls {
            return Err(self
                .reject_drop("the table has row-level-security policies; drop it in autocommit"));
        }

        // (d) Time-partition parent OR is a chunk — fans out into N chunk
        // tombstones + removes the non-MVCC `time_partitions` runtime.
        if self.state.time_partitions.contains_key(table_key) {
            return Err(
                self.reject_drop("the table is a time-partition parent; drop it in autocommit")
            );
        }
        if self.is_time_partition_chunk(table_key) {
            return Err(self.reject_drop(
                "the table is a time-partition chunk; drop the parent in autocommit",
            ));
        }

        // (e, inbound) Another table references this one by FOREIGN KEY — a
        // non-CASCADE drop already errors in autocommit; in-txn the cross-table
        // FK edge the mask cannot model means reject.
        if self.in_txn_drop_has_inbound_fk(entry.oid) {
            return Err(self.reject_drop(
                "another table references this one by FOREIGN KEY; drop it in autocommit",
            ));
        }

        // (c) Dependent view / materialized view — non-MVCC view runtime sidecars
        // the mask cannot tombstone.
        if self.in_txn_drop_has_view_dependents(&entry.name) {
            return Err(self.reject_drop(
                "a view or materialized view depends on this table; drop it in autocommit",
            ));
        }

        // (f) Columnar shadow / custom statistics / comments — each is an
        // immediate in-memory delete NOT stamped with the user xid, so the MVCC
        // tombstone alone does not revert it.
        if self.state.columnar_storage.stats(&entry.name).is_some() {
            return Err(self.reject_drop("the table has a columnar shadow; drop it in autocommit"));
        }
        let base = self.state.catalog_snapshot();
        if base
            .statistics
            .keys()
            .any(|(starelid, _)| *starelid == entry.oid)
            || base
                .statistic_ext
                .values()
                .any(|row| row.stxrelid == entry.oid)
        {
            return Err(self.reject_drop("the table has custom statistics; drop it in autocommit"));
        }
        if base
            .descriptions
            .keys()
            .any(|(objoid, _, _)| *objoid == entry.oid)
        {
            return Err(self.reject_drop("the table has comments; drop it in autocommit"));
        }

        Ok(())
    }

    /// Build the `0A000` feature-not-supported rejection (transitions the block
    /// to Failed) with a stable HINT.
    fn reject_drop(&mut self, why: &str) -> ServerError {
        self.fail_if_in_transaction(ServerError::UnsupportedOwned(format!(
            "DROP TABLE inside an explicit transaction is not supported here: {why}\nHINT:  run it \
             in autocommit"
        )))
    }

    /// Whether `table_key` is a chunk of any time-partition parent.
    fn is_time_partition_chunk(&self, table_key: &str) -> bool {
        self.state.time_partitions.iter().any(|parent| {
            parent
                .value()
                .chunks
                .iter()
                .any(|chunk| chunk.value().table_name.eq_ignore_ascii_case(table_key))
        })
    }

    /// Whether any OTHER table references `target_oid` by FOREIGN KEY.
    fn in_txn_drop_has_inbound_fk(&self, target_oid: ultrasql_core::Oid) -> bool {
        self.state.table_constraints.iter().any(|item| {
            *item.key() != target_oid
                && item
                    .value()
                    .foreign_keys
                    .iter()
                    .any(|fk| fk.target_oid == target_oid)
        })
    }

    /// Whether any regular view or materialized view depends on `target_table`.
    fn in_txn_drop_has_view_dependents(&self, target_table: &str) -> bool {
        let target = target_table.to_ascii_lowercase();
        let mv_dep = self
            .state
            .materialized_views
            .iter()
            .any(|item| item.value().source_table.eq_ignore_ascii_case(&target));
        if mv_dep {
            return true;
        }
        let drop_set = std::collections::HashSet::new();
        !self.regular_view_dependents(&target, &drop_set).is_empty()
    }

    /// Un-stage a same-txn-created OID from every overlay staging vector so the
    /// COMMIT publish never creates it and the deferred index build never scans
    /// a masked heap. The mask (`stage_drop`) hides any residue immediately.
    fn unstage_same_txn_created(&mut self, entry: &TableEntry) {
        let oid = entry.oid;
        let Some(overlay) = self.pending_catalog_ddl.as_mut() else {
            return;
        };
        overlay.created_tables.retain(|t| t.oid != oid);
        overlay.indexes.retain(|ix| ix.table_oid != oid);
        overlay.extra_indexes.retain(|ix| ix.table_oid != oid);
        overlay.constraints.retain(|row| row.conrelid != oid);
        overlay
            .extra_index_constraints
            .retain(|row| row.conrelid != oid);
        overlay.altered_tables.retain(|t| t.oid != oid);
        overlay.altered_staged.retain(|s| s.oid != oid);
        overlay.staged.retain(|s| s.oid != oid);
    }

    /// Insert the negative mask + the COMMIT/ROLLBACK record into the
    /// (possibly pre-existing) overlay.
    fn stage_drop(
        &mut self,
        user_xid: Xid,
        oid: ultrasql_core::Oid,
        table_key: String,
        was_same_txn_created: bool,
    ) {
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
                dropped_oids: std::collections::HashSet::new(),
                dropped: Vec::new(),
            });
        debug_assert_eq!(overlay.xid, user_xid);
        overlay.dropped_oids.insert(oid);
        overlay.dropped.push(DroppedTableState {
            oid,
            table_key,
            was_same_txn_created,
        });
    }
}
