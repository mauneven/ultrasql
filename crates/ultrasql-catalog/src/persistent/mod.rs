//! Persistent (heap-backed) system catalog tables.
//!
//! This module provides `PersistentCatalog`, a thin wrapper that stores
//! catalog metadata in named in-memory maps mirroring the PostgreSQL
//! system catalog tables: `pg_namespace`, `pg_class`, `pg_attribute`,
//! `pg_index`, `pg_constraint`, `pg_sequence`, `pg_depend`,
//! `pg_description`, `pg_statistic`, and `pg_statistic_ext`.
//!
//! # Architecture
//!
//! `PersistentCatalog` satisfies the [`Catalog`] and [`MutableCatalog`]
//! traits via an arc-swap snapshot cache that gives wait-free reads on
//! the hot path.
//!
//! ```text
//!  PersistentCatalog
//!   └── ArcSwap<CatalogSnapshot>   ← wait-free reads
//!        └── DashMap<name, row>    ← shard-locked writes
//! ```
//!
//! Writes take a Mutex to build a new snapshot and swap it in atomically.
//! The calling thread sees the new state immediately; background readers
//! in flight see the old snapshot until they re-acquire.
//!
//! # Bootstrap lifecycle
//!
//! On a fresh database the heap files for the system catalog tables are
//! empty. [`PersistentCatalog::bootstrap_from_heap`] detects this
//! condition and installs the [`crate::bootstrap::initial_snapshot`],
//! which contains the three well-known namespaces and the system
//! relations the server needs to query its own catalog.
//!
//! On a warm restart the heap is non-empty.
//! [`PersistentCatalog::bootstrap_from_heap`] scans the `pg_class`,
//! `pg_attribute`, and `pg_index` heap pages, decodes each user row via
//! [`ClassRow`] / [`crate::encoding::decode_attribute_row`] /
//! [`crate::encoding::decode_index_row`] and
//! [`crate::encoding::schema_from_attributes`], decodes durable
//! `pg_statistic` / `pg_statistic_ext` rows, then overlays the decoded user
//! `TableEntry` / `IndexEntry` lists and statistics on top of the initial
//! system snapshot.
//! The result is an MVCC-consistent snapshot that combines the durable
//! system schema with the durable user schema. System relations always
//! come from [`crate::bootstrap::initial_snapshot`]; only user rows are
//! decoded from heap. See `persistent.rs` (the call site around the heap
//! scan) and the round-trip test at the bottom of this file.

use std::sync::atomic::{AtomicU32, Ordering};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::sync::Arc;
use ultrasql_core::{DataType, Field, Oid, RelationId, Schema};
use ultrasql_storage::buffer_pool::PageLoader;
use ultrasql_storage::heap::HeapAccess;

use crate::bootstrap::{self, initial_snapshot};
use crate::encoding::{
    ATTRIBUTE_ROW_N_ATTS, CLASS_ROW_N_ATTS, CONSTRAINT_ROW_N_ATTS, DESCRIPTION_ROW_N_ATTS,
    ENUM_ROW_N_ATTS, INDEX_ROW_N_ATTS, SEQUENCE_ROW_N_ATTS, STATISTIC_EXT_ROW_N_ATTS,
    STATISTIC_ROW_N_ATTS, TYPE_ROW_N_ATTS,
};
use crate::entry::{
    CompositeTypeEntry, DomainTypeEntry, EnumLabelEntry, EnumTypeEntry, IndexEntry, TableEntry,
    index_lookup_key, table_lookup_key, type_lookup_key,
};
use crate::error::CatalogError;
use crate::traits::{Catalog, MutableCatalog};

mod bootstrap_heap;
mod core;
mod index_constraint;
mod mutations;
mod persist_tables;
mod rows;
#[cfg(test)]
mod tests;
mod traits_impl;
mod types_ddl;

pub use rows::{
    AttributeRow, CatalogSnapshot, CatalogStats, ClassRow, ConType, ConstraintRow, DependRow,
    DescriptionRow, EnumRow, IndexRow, NamespaceRow, RelKind, SequenceRow, StatisticExtRow,
    StatisticRow, TypeRow,
};

// ---------------------------------------------------------------------------
// PersistentCatalog
// ---------------------------------------------------------------------------

/// Heap-backed system catalog.
///
/// Reads are served from a wait-free `ArcSwap<CatalogSnapshot>`. Writes
/// update the underlying `DashMap`s, rebuild the snapshot, and swap it in
/// atomically.
///
/// # Persistent anchor
///
/// `TODO(catalog-persistent-heap)`: replace the `DashMap` backing with
/// buffer-pool pages for each system catalog table. The column layouts
/// of [`ClassRow`], [`AttributeRow`], etc., already match PostgreSQL's
/// on-disk format for the corresponding tables.
#[derive(Debug)]
pub struct PersistentCatalog {
    // --- raw system table rows ---
    pub pg_namespace: DashMap<Oid, NamespaceRow>,
    pub pg_class: DashMap<Oid, ClassRow>,
    pub pg_attribute: DashMap<(Oid, i16), AttributeRow>,
    pub pg_type: DashMap<Oid, TypeRow>,
    pub pg_enum: DashMap<(Oid, u32), EnumRow>,
    pub pg_index: DashMap<Oid, IndexRow>,
    pub pg_constraint: DashMap<Oid, ConstraintRow>,
    pub pg_sequence: DashMap<Oid, SequenceRow>,
    pub pg_depend: parking_lot::Mutex<Vec<DependRow>>,
    pub pg_description: DashMap<(Oid, Oid, i32), DescriptionRow>,
    pub pg_statistic: DashMap<(Oid, i16), StatisticRow>,
    pub pg_statistic_ext: DashMap<Oid, StatisticExtRow>,

    // --- planner-facing view (for Catalog trait) ---
    tables_by_name: DashMap<String, TableEntry>,
    tables_by_oid: DashMap<Oid, TableEntry>,
    indexes_by_name: DashMap<String, IndexEntry>,
    indexes_by_table: DashMap<Oid, Vec<IndexEntry>>,
    enum_types_by_name: DashMap<String, EnumTypeEntry>,
    enum_types_by_oid: DashMap<Oid, EnumTypeEntry>,
    composite_types_by_name: DashMap<String, CompositeTypeEntry>,
    composite_types_by_oid: DashMap<Oid, CompositeTypeEntry>,
    domain_types_by_name: DashMap<String, DomainTypeEntry>,
    domain_types_by_oid: DashMap<Oid, DomainTypeEntry>,

    /// Wait-free snapshot for the binder.
    snapshot: ArcSwap<CatalogSnapshot>,
    /// Serializes snapshot rebuilds.
    write_lock: Mutex<()>,
    /// OID allocator.
    next_oid: AtomicU32,
}

fn attnum_for_index(idx: usize, object: &str) -> Result<i16, CatalogError> {
    let one_based = idx.checked_add(1).ok_or_else(|| {
        CatalogError::schema_conflict(format!("{object} has too many attributes"))
    })?;
    i16::try_from(one_based).map_err(|_| {
        CatalogError::schema_conflict(format!(
            "{object} has too many attributes: attribute number {one_based} exceeds i16::MAX"
        ))
    })
}

fn track_next_oid(highest_oid: &mut u32, oid: Oid, source: &str) -> Result<(), CatalogError> {
    let next = oid.raw().checked_add(1).ok_or_else(|| {
        CatalogError::schema_conflict(format!(
            "catalog OID space exhausted after {source} oid {}",
            oid.raw()
        ))
    })?;
    *highest_oid = (*highest_oid).max(next);
    Ok(())
}

impl Default for PersistentCatalog {
    fn default() -> Self {
        Self::new()
    }
}

fn normalized_opclasses(entry: &IndexEntry) -> Vec<Option<String>> {
    if entry.opclasses.is_empty() {
        vec![None; entry.columns.len()]
    } else {
        entry.opclasses.clone()
    }
}

fn type_row_from_enum(entry: &EnumTypeEntry) -> TypeRow {
    TypeRow {
        oid: entry.oid,
        typname: entry.name.clone(),
        typnamespace: namespace_oid_for_schema(&entry.schema_name),
        typtype: 'e',
        typcategory: 'E',
        typlen: -1,
        typelem: 0,
    }
}

fn type_row_from_composite(entry: &CompositeTypeEntry) -> TypeRow {
    TypeRow {
        oid: entry.oid,
        typname: entry.name.clone(),
        typnamespace: namespace_oid_for_schema(&entry.schema_name),
        typtype: 'c',
        typcategory: 'C',
        typlen: -1,
        typelem: 0,
    }
}

fn type_row_from_domain(entry: &DomainTypeEntry) -> TypeRow {
    TypeRow {
        oid: entry.oid,
        typname: entry.name.clone(),
        typnamespace: namespace_oid_for_schema(&entry.schema_name),
        typtype: 'd',
        typcategory: type_category_for(&entry.base_type),
        typlen: entry
            .base_type
            .fixed_size()
            .and_then(|len| i16::try_from(len).ok())
            .unwrap_or(-1),
        typelem: 0,
    }
}

fn type_category_for(ty: &DataType) -> char {
    match ty {
        DataType::Bool => 'B',
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal { .. }
        | DataType::Money => 'N',
        DataType::Text { .. } | DataType::Char { .. } => 'S',
        DataType::Bit { .. } | DataType::VarBit { .. } => 'V',
        DataType::Date
        | DataType::Time
        | DataType::TimeTz
        | DataType::Timestamp
        | DataType::TimestampTz
        | DataType::Interval => 'D',
        DataType::Array(_) => 'A',
        DataType::Enum { .. } => 'E',
        DataType::Composite { .. } | DataType::Record(_) => 'C',
        DataType::Domain { base_type, .. } => type_category_for(base_type),
        _ => 'U',
    }
}

fn class_row_from_composite(entry: &CompositeTypeEntry) -> ClassRow {
    ClassRow {
        oid: entry.oid,
        relname: entry.name.clone(),
        relnamespace: namespace_oid_for_schema(&entry.schema_name),
        relkind: RelKind::CompositeType,
        relpages: 0,
        reltuples: 0.0,
        relfilenode: 0,
        relhasindex: false,
        reloptions: Vec::new(),
    }
}

fn enum_row_from_label(enumtypid: Oid, label: &EnumLabelEntry) -> EnumRow {
    EnumRow {
        oid: label.oid,
        enumtypid,
        enumsortorder: label.sort_order,
        enumlabel: label.label.clone(),
    }
}

fn namespace_oid_for_schema(schema_name: &str) -> Oid {
    match schema_name {
        "pg_catalog" => Oid::new(bootstrap::PG_CATALOG_OID),
        "information_schema" => Oid::new(bootstrap::INFORMATION_SCHEMA_OID),
        "public" => Oid::new(bootstrap::PUBLIC_OID),
        other => Oid::new(runtime_schema_oid(other)),
    }
}

fn runtime_schema_oid(name: &str) -> u32 {
    const USER_SCHEMA_OID_BASE: u32 = 70_000;
    const USER_SCHEMA_OID_SPACE: u32 = 1_000_000;
    let hash = name.as_bytes().iter().fold(0x811c_9dc5_u32, |acc, byte| {
        (acc ^ u32::from(*byte)).wrapping_mul(0x0100_0193)
    });
    USER_SCHEMA_OID_BASE + (hash % USER_SCHEMA_OID_SPACE)
}

fn fold_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn table_entry_key(entry: &TableEntry) -> String {
    table_lookup_key(&entry.schema_name, &entry.name)
}

fn index_entry_key(entry: &IndexEntry) -> String {
    index_lookup_key(&entry.schema_name, &entry.name)
}

trait TypeEntryKey {
    fn type_schema_name(&self) -> &str;
    fn type_name(&self) -> &str;
}

impl TypeEntryKey for EnumTypeEntry {
    fn type_schema_name(&self) -> &str {
        &self.schema_name
    }

    fn type_name(&self) -> &str {
        &self.name
    }
}

impl TypeEntryKey for CompositeTypeEntry {
    fn type_schema_name(&self) -> &str {
        &self.schema_name
    }

    fn type_name(&self) -> &str {
        &self.name
    }
}

impl TypeEntryKey for DomainTypeEntry {
    fn type_schema_name(&self) -> &str {
        &self.schema_name
    }

    fn type_name(&self) -> &str {
        &self.name
    }
}

fn type_entry_key(entry: &impl TypeEntryKey) -> String {
    type_lookup_key(entry.type_schema_name(), entry.type_name())
}
