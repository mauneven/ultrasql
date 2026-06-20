//! Catalog-snapshot lookups for single-column indexes and key-type
//! eligibility used by the index-scan lowerer.

use std::sync::Arc;

use ultrasql_catalog::{CatalogSnapshot, IndexEntry, TableEntry};
use ultrasql_core::{BlockNumber, DataType};
use ultrasql_planner::LogicalIndexMethod;
use ultrasql_storage::access_method::BrinIndex;

use super::LowerCtx;

/// Return the [`IndexEntry`] that covers exactly the single column
/// `col_idx` of `table_entry`, if any. Composite indexes whose first
/// key is `col_idx` are *not* returned today: the on-disk B-tree only
/// supports 8-byte keys, so a composite index could not be probed
/// through the existing API.
pub(crate) fn find_single_column_index<'a>(
    snapshot: &'a CatalogSnapshot,
    table_entry: &TableEntry,
    col_idx: usize,
    ctx: &LowerCtx<'_>,
) -> Option<&'a IndexEntry> {
    let attnum = u16::try_from(col_idx).ok()?;
    let indexes = snapshot.indexes_by_table.get(&table_entry.oid)?;
    indexes.iter().find(|e| {
        e.columns.len() == 1
            && e.columns[0] == attnum
            && e.root_block != BlockNumber::INVALID
            && index_method(ctx, table_entry.oid, e.oid) == LogicalIndexMethod::Btree
    })
}

pub(super) fn find_single_column_hash_index<'a>(
    snapshot: &'a CatalogSnapshot,
    table_entry: &TableEntry,
    col_idx: usize,
    ctx: &LowerCtx<'_>,
) -> Option<&'a IndexEntry> {
    let attnum = u16::try_from(col_idx).ok()?;
    let indexes = snapshot.indexes_by_table.get(&table_entry.oid)?;
    indexes.iter().find(|e| {
        e.columns.len() == 1
            && e.columns[0] == attnum
            && e.root_block != BlockNumber::INVALID
            && index_method(ctx, table_entry.oid, e.oid) == LogicalIndexMethod::Hash
    })
}

pub(super) fn find_single_column_brin_index<'a>(
    snapshot: &'a CatalogSnapshot,
    table_entry: &TableEntry,
    col_idx: usize,
    ctx: &LowerCtx<'_>,
) -> Option<&'a IndexEntry> {
    let attnum = u16::try_from(col_idx).ok()?;
    let indexes = snapshot.indexes_by_table.get(&table_entry.oid)?;
    indexes.iter().find(|e| {
        e.columns.len() == 1
            && e.columns[0] == attnum
            && e.root_block != BlockNumber::INVALID
            && index_method(ctx, table_entry.oid, e.oid) == LogicalIndexMethod::Brin
    })
}

pub(super) fn brin_summary(
    ctx: &LowerCtx<'_>,
    table_oid: ultrasql_core::Oid,
    index_oid: ultrasql_core::Oid,
) -> Option<Arc<BrinIndex>> {
    let constraints = ctx.table_constraints.get(&table_oid)?;
    constraints.indexes.get(&index_oid)?.brin.clone()
}

fn index_method(
    ctx: &LowerCtx<'_>,
    table_oid: ultrasql_core::Oid,
    index_oid: ultrasql_core::Oid,
) -> LogicalIndexMethod {
    ctx.table_constraints
        .get(&table_oid)
        .map_or(LogicalIndexMethod::Btree, |constraints| {
            constraints
                .indexes
                .get(&index_oid)
                .map_or(LogicalIndexMethod::Btree, |metadata| metadata.method)
        })
}

/// Confirm the keyed column has a type stored directly in the `i64`
/// key space. Returns `None` for types whose index encoding needs a
/// transform not represented by [`super::literal_as_i64`].
///
/// Mirrors the check in `Server::execute_create_index` — keep the two
/// in sync, or a `CREATE INDEX` that succeeds will produce an index a
pub(crate) fn key_type_for_btree(table_entry: &TableEntry, col_idx: usize) -> Option<bool> {
    let field = table_entry.schema.field(col_idx)?;
    match field.data_type {
        DataType::Bool | DataType::Int16 | DataType::Timestamp | DataType::TimestampTz => {
            Some(true)
        }
        DataType::Int32 => Some(true),
        DataType::Int64 => Some(false),
        _ => None,
    }
}
