//! INSERT/UPDATE/DELETE lowering plus the fused-kernel fast paths.

use std::sync::Arc;

use ultrasql_catalog::CatalogSnapshot;
use ultrasql_core::{Oid, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_storage::heap::HeapAccess;
use ultrasql_txn::TransactionManager;

use crate::BlankPageLoader;
use crate::TableRuntimeConstraints;
use crate::pipeline::LowerCtx;

mod constraints;
mod delete;
mod indexes;
mod insert;
mod lowering;
mod merge;
mod referential;
mod update;

pub(super) use delete::lower_real_delete;
pub(super) use insert::lower_real_insert;
pub(super) use lowering::lower_project_columns;
pub(super) use merge::lower_real_merge;
pub(super) use update::lower_real_update;

pub(crate) use constraints::{
    build_exclusion_insert_checks_from_deps, build_foreign_key_checks_from_deps,
};
pub(crate) use indexes::{
    build_insert_index_maintainers_from_deps, build_vector_index_maintainers_from_deps,
};

/// Explicit dependency view for the secondary-index maintainer builders.
///
/// The INSERT-lowering path constructs this from a [`LowerCtx`]; `COPY FROM`
/// assembles the same fields off the [`Session`](crate::Session) and the
/// governing transaction. Sharing one view lets both paths build identical
/// index maintainers (encoder, partial predicate, BRIN, vector method) from a
/// single body — the COPY reuse seam — instead of reimplementing maintenance.
pub(crate) struct IndexMaintainerDeps<'a> {
    pub catalog_snapshot: &'a CatalogSnapshot,
    pub table_constraints: &'a dashmap::DashMap<Oid, Arc<TableRuntimeConstraints>>,
    pub heap: &'a HeapAccess<BlankPageLoader>,
    pub xid: Xid,
}

impl<'a> IndexMaintainerDeps<'a> {
    fn from_lower_ctx(ctx: &'a LowerCtx<'_>) -> Self {
        Self {
            catalog_snapshot: &ctx.catalog_snapshot,
            table_constraints: &ctx.table_constraints,
            heap: &ctx.heap,
            xid: ctx.xid,
        }
    }
}

/// Explicit dependency view for the FK / EXCLUDE row-check builders.
///
/// The check closures heap-recheck visible rows under an MVCC snapshot +
/// oracle, exactly as INSERT does; this view lets `COPY FROM` reuse those
/// builders with the COPY transaction's snapshot so a uniqueness/FK/EXCLUDE
/// recheck during COPY sees the same committed-and-own-write image an INSERT
/// would.
pub(crate) struct ConstraintCheckDeps<'a> {
    pub catalog_snapshot: &'a CatalogSnapshot,
    pub heap: &'a Arc<HeapAccess<BlankPageLoader>>,
    pub snapshot: &'a Snapshot,
    pub oracle: &'a Arc<TransactionManager>,
}

impl<'a> ConstraintCheckDeps<'a> {
    fn from_lower_ctx(ctx: &'a LowerCtx<'_>) -> Self {
        Self {
            catalog_snapshot: &ctx.catalog_snapshot,
            heap: &ctx.heap,
            snapshot: &ctx.snapshot,
            oracle: &ctx.oracle,
        }
    }
}
