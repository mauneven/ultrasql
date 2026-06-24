//! `CREATE INDEX` DDL handler. Part of the `session::ddl` module split;
//! reopens the `impl<RW> Session<RW>` block defined in `session/mod.rs`.
//!
//! `execute_create_index` validates the request against the catalog
//! snapshot and dispatches to the per-access-method builders in
//! `create_index_build.rs`.

use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::CatalogSnapshot;
use ultrasql_planner::{LogicalIndexMethod, LogicalPlan};

use super::super::Session;
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
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
    ///    [`BTree`](ultrasql_storage::btree::BTree) over a relation id
    ///    derived from that OID. The buffer pool's blank-page loader
    ///    hands out empty heap pages which `BTree::create` then
    ///    initialises as B-tree leaves.
    /// 3. Scan every visible row of the parent table under an
    ///    autocommit snapshot, decode the key column(s), and call
    ///    [`BTree::insert`](ultrasql_storage::btree::BTree::insert) with
    ///    the row's [`ultrasql_core::TupleId`].
    /// 4. Build an [`IndexEntry`](ultrasql_catalog::IndexEntry) carrying
    ///    the root block plus the requested attnums, register it with the
    ///    persistent catalog, and let the catalog's snapshot rotation
    ///    publish the entry to subsequent statements.
    ///
    /// The kernel for each access method lives in a dedicated builder in
    /// `create_index_build.rs`; this method only validates the request
    /// and dispatches.
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
    ///   [`IndexEntry::is_unique`](ultrasql_catalog::IndexEntry::is_unique)
    ///   flag is propagated — but the B-tree's existing duplicate-key
    ///   rejection is the only enforcement. Non-unique indexes that
    ///   happen to have unique data still build correctly; non-unique
    ///   indexes with duplicates would error here, which is a known
    ///   limitation we accept until the B-tree gains a non-unique mode.
    pub(crate) fn execute_create_index(
        &mut self,
        plan: &LogicalPlan,
        snapshot: &CatalogSnapshot,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::CreateIndex {
            index_name,
            index_namespace,
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
            primary_key,
            if_not_exists,
            ..
        } = plan
        else {
            return Err(ServerError::Unsupported(
                "execute_create_index called with non-CreateIndex plan",
            ));
        };

        // Transactional-DDL milestone 3: when an explicit transaction is open
        // this `CREATE INDEX` must defer its B-tree build to COMMIT and stage
        // the `IndexEntry` in the session overlay rather than mutating the
        // global catalog. `None` here means autocommit — the legacy
        // self-committing builder path runs byte-for-byte unchanged.
        //
        // A `CREATE INDEX` issued while a SAVEPOINT is active is out of scope:
        // the durable `pg_index` rows ride the parent xid (not the subtxn xid)
        // and the overlay is whole-transaction-scoped, so a `ROLLBACK TO
        // SAVEPOINT` could NOT undo the index. Reject with the gate's `0A000`
        // (mirrors the milestone-1 `CREATE TABLE` reject).
        let in_txn_xid = match &self.txn_state {
            crate::TxnState::InTransaction(txn) => {
                if txn.subtxn_stack.depth() > 0 {
                    return Err(self.fail_if_in_transaction(ServerError::DdlInTransaction));
                }
                Some(txn.xid)
            }
            _ => None,
        };

        // 1a. IF NOT EXISTS short-circuit.
        let index_key = ultrasql_catalog::index_lookup_key(index_namespace, index_name);
        if snapshot.indexes.contains_key(&index_key) {
            if *if_not_exists {
                return Ok(run_ddl_command("CREATE INDEX"));
            }
            return Err(self.fail_if_in_transaction(ServerError::Catalog(
                ultrasql_catalog::CatalogError::already_exists(index_key),
            )));
        }

        // 1b. Resolve the parent table.
        let table = snapshot.tables.get(table_name).ok_or_else(|| {
            ServerError::Plan(ultrasql_planner::PlanError::TableNotFound(
                table_name.clone(),
            ))
        })?;
        self.ensure_table_owner_or_superuser(table.oid, table_name)?;

        // Transactional-DDL milestone 3: route the in-transaction case to the
        // deferred builder. It rejects the out-of-scope shapes (`0A000`) and
        // the same-txn-created-table scope boundary, takes AccessExclusive on
        // the target table, persists the `pg_index` rows UNBUILT under the user
        // xid, and stages the entry in the overlay (built at COMMIT). The
        // borrow on `snapshot.tables` is dropped before the `&mut self` call.
        if let Some(user_xid) = in_txn_xid {
            let table = table.clone();
            return self.execute_create_index_in_txn(plan, &table, user_xid);
        }

        // 1c. Dispatch to the per-access-method builder. Each builder
        //     owns the index build plus `pg_index` registration; logic is
        //     identical to the pre-split single-method handler.
        match *method {
            LogicalIndexMethod::Aggregating => self.build_aggregating_index(
                table,
                index_name,
                index_namespace,
                columns,
                key_exprs,
                *method,
                aggregating,
                *unique,
                &index_key,
            ),
            LogicalIndexMethod::IvfFlat => self.build_ivfflat_index(
                table,
                index_name,
                index_namespace,
                columns,
                key_exprs,
                opclasses,
                index_options,
                include_columns,
                predicate,
                *method,
                *unique,
                &index_key,
            ),
            LogicalIndexMethod::Hnsw => self.build_hnsw_index(
                table,
                index_name,
                index_namespace,
                columns,
                key_exprs,
                opclasses,
                index_options,
                include_columns,
                predicate,
                *method,
                *unique,
                &index_key,
            ),
            _ => self.build_btree_index(
                table,
                index_name,
                index_namespace,
                columns,
                key_exprs,
                opclasses,
                index_options,
                include_columns,
                predicate,
                *method,
                *unique,
                *primary_key,
                &index_key,
            ),
        }
    }
}
