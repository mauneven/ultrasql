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

// ---------------------------------------------------------------------------
// System catalog row types
// ---------------------------------------------------------------------------

/// A row in `pg_namespace`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceRow {
    /// `oid` column.
    pub oid: Oid,
    /// `nspname` — namespace name.
    pub nspname: String,
    /// `nspowner` — OID of the owner role.
    pub nspowner: Oid,
}

/// Relation kind: matches `pg_class.relkind`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RelKind {
    /// Ordinary table (`'r'`).
    Table,
    /// Index (`'i'`).
    Index,
    /// Sequence (`'S'`).
    Sequence,
    /// View (`'v'`).
    View,
    /// Materialized view (`'m'`).
    MaterializedView,
    /// Composite type (`'c'`).
    CompositeType,
    /// TOAST table (`'t'`).
    Toast,
    /// Foreign table (`'f'`).
    ForeignTable,
    /// Append-only tombstone for a dropped relation.
    Dropped,
}

/// A row in `pg_class`.
#[derive(Clone, Debug, PartialEq)]
pub struct ClassRow {
    /// `oid`.
    pub oid: Oid,
    /// `relname`.
    pub relname: String,
    /// `relnamespace` — OID of the containing namespace.
    pub relnamespace: Oid,
    /// `relkind`.
    pub relkind: RelKind,
    /// `relpages` — estimated number of disk pages.
    pub relpages: u32,
    /// `reltuples` — estimated number of live tuples.
    pub reltuples: f64,
    /// `relfilenode` — block number of the first page (relation root).
    pub relfilenode: u32,
    /// `relhasindex` — true when at least one index exists.
    pub relhasindex: bool,
    /// Internal relation storage options captured from `ALTER TABLE ... SET`.
    pub reloptions: Vec<(String, String)>,
}

/// A row in `pg_attribute`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttributeRow {
    /// `attrelid` — OID of the parent table.
    pub attrelid: Oid,
    /// `attname` — column name.
    pub attname: String,
    /// `atttypid` — OID of the data type (simplified: 0 = unknown).
    pub atttypid: u32,
    /// `attnum` — 1-based column position.
    pub attnum: i16,
    /// `attnotnull` — NOT NULL constraint.
    pub attnotnull: bool,
    /// `atthasdef` — column has a default expression.
    pub atthasdef: bool,
    /// `attisdropped` — column has been dropped.
    pub attisdropped: bool,
}

/// A row in `pg_type`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeRow {
    /// `oid`.
    pub oid: Oid,
    /// `typname`.
    pub typname: String,
    /// `typnamespace` — OID of the containing namespace.
    pub typnamespace: Oid,
    /// `typtype` (`'b'` built-in/base, `'e'` enum, etc.).
    pub typtype: char,
    /// `typcategory` (`'E'` for enum, `'S'` for string, etc.).
    pub typcategory: char,
    /// `typlen`; `-1` means varlena.
    pub typlen: i16,
    /// Element type OID for arrays, or 0.
    pub typelem: u32,
}

/// A row in `pg_enum`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumRow {
    /// `oid`.
    pub oid: Oid,
    /// `enumtypid` — owning enum type OID.
    pub enumtypid: Oid,
    /// `enumsortorder` in declaration order.
    pub enumsortorder: u32,
    /// `enumlabel`.
    pub enumlabel: String,
}

/// A row in `pg_index`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexRow {
    /// `indexrelid` — OID of the index itself (in `pg_class`).
    pub indexrelid: Oid,
    /// `indrelid` — OID of the indexed table.
    pub indrelid: Oid,
    /// `indnatts` — number of columns.
    pub indnatts: u16,
    /// `indisunique`.
    pub indisunique: bool,
    /// `indisprimary`.
    pub indisprimary: bool,
    /// `indisvalid` — false while a CONCURRENT build is in progress.
    pub indisvalid: bool,
    /// `indkey` — 0-based column positions matching [`IndexEntry::columns`].
    ///
    /// PostgreSQL exposes 1-based attnums here. UltraSQL stores the
    /// planner-facing internal form durably and adds 1 only at SQL view
    /// boundaries that need PostgreSQL parity.
    pub indkey: Vec<i16>,
    /// Internal access method name captured from `CREATE INDEX ... USING`.
    pub indmethod: String,
    /// Internal opclass names, one per key where present.
    pub indopclasses: Vec<Option<String>>,
    /// Internal storage options captured from `WITH (...)`.
    pub indoptions: Vec<(String, String)>,
}

/// Constraint type, mirroring `pg_constraint.contype`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConType {
    /// Check constraint (`'c'`).
    Check,
    /// Foreign key (`'f'`).
    ForeignKey,
    /// Primary key (`'p'`).
    PrimaryKey,
    /// Unique (`'u'`).
    Unique,
    /// Trigger (`'t'`).
    Trigger,
    /// Exclusion (`'x'`).
    Exclusion,
}

/// A row in `pg_constraint`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConstraintRow {
    /// `oid`.
    pub oid: Oid,
    /// `conname`.
    pub conname: String,
    /// `conrelid` — OID of the constrained table.
    pub conrelid: Oid,
    /// `contype`.
    pub contype: ConType,
    /// `condeferrable`.
    pub condeferrable: bool,
    /// `condeferred`.
    pub condeferred: bool,
    /// `conkey` — column numbers the constraint covers.
    pub conkey: Vec<i16>,
    /// `confrelid` — referenced table OID (FK only).
    pub confrelid: Oid,
    /// `confkey` — referenced column numbers (FK only).
    pub confkey: Vec<i16>,
}

/// A row in `pg_sequence`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceRow {
    /// `seqrelid` — OID of the sequence's `pg_class` entry.
    pub seqrelid: Oid,
    /// `seqtypid` — OID of the sequence's data type.
    pub seqtypid: u32,
    /// `seqstart`.
    pub seqstart: i64,
    /// `seqincrement`.
    pub seqincrement: i64,
    /// `seqmax`.
    pub seqmax: i64,
    /// `seqmin`.
    pub seqmin: i64,
    /// `seqcache`.
    pub seqcache: i64,
    /// `seqcycle`.
    pub seqcycle: bool,
}

/// A row in `pg_depend`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DependRow {
    /// `classid` — OID of the system catalog that contains the dependent object.
    pub classid: Oid,
    /// `objid` — OID of the dependent object.
    pub objid: Oid,
    /// `refclassid` — OID of the system catalog of the referenced object.
    pub refclassid: Oid,
    /// `refobjid` — OID of the referenced object.
    pub refobjid: Oid,
    /// `deptype` — dependency type character.
    pub deptype: char,
}

/// A row in `pg_description`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescriptionRow {
    /// `objoid` — OID of the described object.
    pub objoid: Oid,
    /// `classoid` — OID of the system catalog.
    pub classoid: Oid,
    /// `objsubid` — column number for column comments.
    pub objsubid: i32,
    /// `description` — comment text.
    pub description: String,
}

/// A row in `pg_statistic` (simplified).
#[derive(Clone, Debug, PartialEq)]
pub struct StatisticRow {
    /// `starelid`.
    pub starelid: Oid,
    /// `staattnum`.
    pub staattnum: i16,
    /// `stanullfrac` — fraction of entries that are NULL.
    pub stanullfrac: f32,
    /// `stadistinct` — number of distinct values (negative = fraction).
    pub stadistinct: f32,
}

/// A row in `pg_statistic_ext`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatisticExtRow {
    /// `oid`.
    pub oid: Oid,
    /// `stxname`.
    pub stxname: String,
    /// `stxrelid`.
    pub stxrelid: Oid,
    /// `stxkeys` — column attnums covered.
    pub stxkeys: Vec<i16>,
    /// `stxkind` — statistic kinds enabled (`'d'` = ndistinct, `'f'` = dependencies, `'m'` = MCV).
    pub stxkind: Vec<char>,
}

// ---------------------------------------------------------------------------
// Catalog bootstrap statistics
// ---------------------------------------------------------------------------

/// Summary counts produced by [`PersistentCatalog::bootstrap_from_heap`].
///
/// Returned on both a successful heap-based bootstrap and a fresh-database
/// bootstrap so callers can log the startup summary without branching.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CatalogStats {
    /// Number of namespaces loaded.
    pub namespaces: u32,
    /// Number of relations loaded.
    pub relations: u32,
    /// Number of attributes loaded.
    pub attributes: u32,
    /// Number of indexes loaded.
    pub indexes: u32,
    /// Number of constraints loaded.
    pub constraints: u32,
    /// Number of `pg_description` rows loaded.
    pub descriptions: u32,
    /// Number of `pg_statistic` rows loaded.
    pub statistics: u32,
    /// Number of `pg_statistic_ext` rows loaded.
    pub statistic_ext: u32,
}

impl CatalogStats {
    /// Stats for a fresh-database initial snapshot: 3 namespaces, 13 relations,
    /// no attributes, indexes, or constraints yet decoded from the heap.
    ///
    /// Used when `bootstrap_from_heap` detects an empty heap and installs the
    /// hard-coded initial snapshot.
    #[must_use]
    pub const fn initial() -> Self {
        Self {
            namespaces: 3,
            relations: 13,
            attributes: 0,
            indexes: 0,
            constraints: 0,
            descriptions: 0,
            statistics: 0,
            statistic_ext: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Catalog snapshot
// ---------------------------------------------------------------------------

/// An immutable snapshot of the catalog, used for wait-free reads.
///
/// The binder acquires one snapshot at the start of planning and uses it
/// for the duration. This mirrors PostgreSQL's `CatalogSnapshot`.
#[derive(Clone, Debug)]
pub struct CatalogSnapshot {
    /// Tables keyed by folded name.
    pub tables: std::collections::HashMap<String, TableEntry>,
    /// Tables keyed by OID.
    pub tables_by_oid: std::collections::HashMap<Oid, TableEntry>,
    /// Indexes keyed by folded name.
    pub indexes: std::collections::HashMap<String, IndexEntry>,
    /// Indexes keyed by table OID.
    pub indexes_by_table: std::collections::HashMap<Oid, Vec<IndexEntry>>,
    /// User-defined enum types keyed by folded name.
    pub enum_types: std::collections::HashMap<String, EnumTypeEntry>,
    /// User-defined enum types keyed by `pg_type.oid`.
    pub enum_types_by_oid: std::collections::HashMap<Oid, EnumTypeEntry>,
    /// User-defined composite types keyed by folded name.
    pub composite_types: std::collections::HashMap<String, CompositeTypeEntry>,
    /// User-defined composite types keyed by `pg_type.oid`.
    pub composite_types_by_oid: std::collections::HashMap<Oid, CompositeTypeEntry>,
    /// User-defined domain types keyed by folded name.
    pub domain_types: std::collections::HashMap<String, DomainTypeEntry>,
    /// User-defined domain types keyed by `pg_type.oid`.
    pub domain_types_by_oid: std::collections::HashMap<Oid, DomainTypeEntry>,
    /// Constraints keyed by `pg_constraint.oid`.
    pub constraints: std::collections::HashMap<Oid, ConstraintRow>,
    /// Comments keyed by `(objoid, classoid, objsubid)`.
    pub descriptions: std::collections::HashMap<(Oid, Oid, i32), DescriptionRow>,
    /// `pg_statistic` rows keyed by `(starelid, staattnum)`.
    pub statistics: std::collections::HashMap<(Oid, i16), StatisticRow>,
    /// `pg_statistic_ext` rows keyed by statistic object OID.
    pub statistic_ext: std::collections::HashMap<Oid, StatisticExtRow>,
}

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

impl PersistentCatalog {
    /// Construct an empty persistent catalog.
    #[must_use]
    pub fn new() -> Self {
        let empty = Arc::new(CatalogSnapshot {
            tables: std::collections::HashMap::new(),
            tables_by_oid: std::collections::HashMap::new(),
            indexes: std::collections::HashMap::new(),
            indexes_by_table: std::collections::HashMap::new(),
            enum_types: std::collections::HashMap::new(),
            enum_types_by_oid: std::collections::HashMap::new(),
            composite_types: std::collections::HashMap::new(),
            composite_types_by_oid: std::collections::HashMap::new(),
            domain_types: std::collections::HashMap::new(),
            domain_types_by_oid: std::collections::HashMap::new(),
            constraints: std::collections::HashMap::new(),
            descriptions: std::collections::HashMap::new(),
            statistics: std::collections::HashMap::new(),
            statistic_ext: std::collections::HashMap::new(),
        });
        Self {
            pg_namespace: DashMap::new(),
            pg_class: DashMap::new(),
            pg_attribute: DashMap::new(),
            pg_type: DashMap::new(),
            pg_enum: DashMap::new(),
            pg_index: DashMap::new(),
            pg_constraint: DashMap::new(),
            pg_sequence: DashMap::new(),
            pg_depend: Mutex::new(Vec::new()),
            pg_description: DashMap::new(),
            pg_statistic: DashMap::new(),
            pg_statistic_ext: DashMap::new(),
            tables_by_name: DashMap::new(),
            tables_by_oid: DashMap::new(),
            indexes_by_name: DashMap::new(),
            indexes_by_table: DashMap::new(),
            enum_types_by_name: DashMap::new(),
            enum_types_by_oid: DashMap::new(),
            composite_types_by_name: DashMap::new(),
            composite_types_by_oid: DashMap::new(),
            domain_types_by_name: DashMap::new(),
            domain_types_by_oid: DashMap::new(),
            snapshot: ArcSwap::new(empty),
            write_lock: Mutex::new(()),
            next_oid: AtomicU32::new(crate::memory::FIRST_USER_OID),
        }
    }

    /// Allocate a fresh OID.
    pub fn next_oid(&self) -> Oid {
        Oid::new(self.next_oid.fetch_add(1, Ordering::Relaxed))
    }

    /// Acquire the current catalog snapshot for statement-level reads.
    ///
    /// The returned `Arc<CatalogSnapshot>` is stable for the caller's
    /// lifetime; background writes atomically swap in a new pointer
    /// without invalidating existing references.
    pub fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.snapshot.load_full()
    }

    fn table_lookup_key_for_unqualified(&self, name: &str) -> String {
        let folded = fold_name(name);
        if self.tables_by_name.contains_key(&folded) {
            return folded;
        }
        let public_key = table_lookup_key("public", name);
        if public_key == folded {
            folded
        } else {
            public_key
        }
    }

    fn index_lookup_key_for_unqualified(&self, name: &str) -> String {
        let folded = fold_name(name);
        if self.indexes_by_name.contains_key(&folded) {
            return folded;
        }
        let public_key = index_lookup_key("public", name);
        if public_key == folded {
            folded
        } else {
            public_key
        }
    }

    /// Atomically replace the in-memory snapshot with `snap`.
    ///
    /// The caller is responsible for also updating the `DashMap` backing
    /// stores when appropriate. This method is the low-level primitive
    /// used by [`Self::bootstrap_from_heap`] and by tests that need to
    /// inject a known snapshot.
    ///
    /// Callers that update the backing maps and then call this method
    /// should hold `write_lock` across both operations so concurrent
    /// readers either see the old snapshot or the new one — never a
    /// partially-updated state.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::SchemaConflict`] if a composite type has
    /// more attributes than `pg_attribute.attnum` can represent.
    pub fn install_snapshot(&self, snap: CatalogSnapshot) -> Result<(), CatalogError> {
        for entry in snap.composite_types.values() {
            let attr_context = format!("composite type {}", entry.name);
            for (idx, _) in entry.schema.fields().iter().enumerate() {
                attnum_for_index(idx, &attr_context)?;
            }
        }

        let _guard = self.write_lock.lock();
        // Re-populate the backing DashMaps from the snapshot so that
        // subsequent MutableCatalog operations (create_table, etc.) have
        // a consistent starting point.
        self.tables_by_name.clear();
        self.tables_by_oid.clear();
        self.indexes_by_name.clear();
        self.indexes_by_table.clear();
        self.enum_types_by_name.clear();
        self.enum_types_by_oid.clear();
        self.composite_types_by_name.clear();
        self.composite_types_by_oid.clear();
        self.domain_types_by_name.clear();
        self.domain_types_by_oid.clear();
        self.pg_type.clear();
        self.pg_enum.clear();
        self.pg_description.clear();
        self.pg_constraint.clear();
        self.pg_sequence.clear();
        self.pg_statistic.clear();
        self.pg_statistic_ext.clear();

        for entry in snap.tables_by_oid.values() {
            self.tables_by_name
                .insert(table_entry_key(entry), entry.clone());
            self.tables_by_oid.insert(entry.oid, entry.clone());
        }
        for entry in snap.indexes.values() {
            self.indexes_by_name
                .insert(index_entry_key(entry), entry.clone());
        }
        for (oid, entries) in &snap.indexes_by_table {
            self.indexes_by_table.insert(*oid, entries.clone());
        }
        for entry in snap.enum_types.values() {
            self.enum_types_by_name
                .insert(type_entry_key(entry), entry.clone());
            self.enum_types_by_oid.insert(entry.oid, entry.clone());
            self.pg_type.insert(entry.oid, type_row_from_enum(entry));
            for label in &entry.labels {
                self.pg_enum.insert(
                    (entry.oid, label.sort_order),
                    enum_row_from_label(entry.oid, label),
                );
            }
        }
        for entry in snap.composite_types.values() {
            self.composite_types_by_name
                .insert(type_entry_key(entry), entry.clone());
            self.composite_types_by_oid.insert(entry.oid, entry.clone());
            self.pg_type
                .insert(entry.oid, type_row_from_composite(entry));
            self.pg_class
                .insert(entry.oid, class_row_from_composite(entry));
            for (idx, field) in entry.schema.fields().iter().enumerate() {
                let attnum = attnum_for_index(idx, &format!("composite type {}", entry.name))?;
                let attr = AttributeRow {
                    attrelid: entry.oid,
                    attname: field.name.clone(),
                    atttypid: 0,
                    attnum,
                    attnotnull: !field.nullable,
                    atthasdef: false,
                    attisdropped: false,
                };
                self.pg_attribute.insert((entry.oid, attnum), attr);
            }
        }
        for entry in snap.domain_types.values() {
            self.domain_types_by_name
                .insert(type_entry_key(entry), entry.clone());
            self.domain_types_by_oid.insert(entry.oid, entry.clone());
            self.pg_type.insert(entry.oid, type_row_from_domain(entry));
        }
        for (key, row) in &snap.descriptions {
            self.pg_description.insert(*key, row.clone());
        }
        for (key, row) in &snap.statistics {
            self.pg_statistic.insert(*key, row.clone());
        }
        for (oid, row) in &snap.statistic_ext {
            self.pg_statistic_ext.insert(*oid, row.clone());
        }
        self.snapshot.store(Arc::new(snap));
        Ok(())
    }

    /// Bootstrap the catalog from on-disk system catalog heap pages.
    ///
    /// Reads `pg_namespace`, `pg_class`, `pg_attribute`, `pg_index`,
    /// `pg_constraint`, `pg_sequence`, `pg_depend`, `pg_description`,
    /// `pg_statistic`, and `pg_statistic_ext` from heap pages via the supplied
    /// [`HeapAccess`]. Builds a
    /// [`CatalogSnapshot`] and atomically swaps it into the in-memory
    /// `ArcSwap` cache.
    ///
    /// # Fresh database
    ///
    /// When all system catalog heap pages are empty (i.e. the database was
    /// just initialized) this method detects the empty heap and installs the
    /// hard-coded [`initial_snapshot`] that contains the three well-known
    /// namespaces and the eleven system relations.  The returned
    /// [`CatalogStats`] in this case reflects the initial snapshot counts.
    ///
    /// # Idempotent
    ///
    /// Subsequent calls re-read the heap and rebuild the snapshot.  This is
    /// intentional: the server calls this after DDL that modifies the system
    /// catalog to refresh the in-memory state.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::SchemaConflict`] if the heap contains
    /// entries that violate catalog invariants (e.g. duplicate OIDs).
    pub fn bootstrap_from_heap<L: PageLoader>(
        &self,
        heap: &HeapAccess<L>,
    ) -> Result<CatalogStats, CatalogError> {
        use crate::encoding::{
            decode_attribute_row, decode_constraint_row, decode_description_row, decode_enum_row,
            decode_index_row, decode_sequence_row, decode_statistic_ext_row, decode_statistic_row,
            decode_type_row, schema_from_attributes,
        };

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_attribute_rel = RelationId::new(bootstrap::PG_ATTRIBUTE_OID);
        let pg_type_rel = RelationId::new(bootstrap::PG_TYPE_OID);
        let pg_enum_rel = RelationId::new(bootstrap::PG_ENUM_OID);
        let pg_index_rel = RelationId::new(bootstrap::PG_INDEX_OID);
        let pg_constraint_rel = RelationId::new(bootstrap::PG_CONSTRAINT_OID);
        let pg_sequence_rel = RelationId::new(bootstrap::PG_SEQUENCE_OID);
        let pg_description_rel = RelationId::new(bootstrap::PG_DESCRIPTION_OID);
        let pg_statistic_rel = RelationId::new(bootstrap::PG_STATISTIC_OID);
        let pg_statistic_ext_rel = RelationId::new(bootstrap::PG_STATISTIC_EXT_OID);
        let class_blocks = heap.block_count(pg_class_rel);
        let type_blocks = heap.block_count(pg_type_rel);
        let enum_blocks = heap.block_count(pg_enum_rel);

        if class_blocks == 0 && type_blocks == 0 && enum_blocks == 0 {
            // Fresh database — install the initial hard-coded snapshot.
            let snap = initial_snapshot();
            let stats = CatalogStats::initial();
            self.install_snapshot(snap)?;
            tracing::debug!(
                ?stats,
                "catalog bootstrapped from initial snapshot (empty heap)"
            );
            return Ok(stats);
        }

        // Warm restart. Start from the initial snapshot (which carries
        // every system relation), then overlay any user-defined tables
        // we find in pg_class.
        let initial = initial_snapshot();
        let mut tables: std::collections::HashMap<String, TableEntry> = initial.tables.clone();
        let mut tables_by_oid: std::collections::HashMap<Oid, TableEntry> =
            initial.tables_by_oid.clone();
        let mut indexes: std::collections::HashMap<String, IndexEntry> = initial.indexes.clone();
        let mut indexes_by_table: std::collections::HashMap<Oid, Vec<IndexEntry>> =
            initial.indexes_by_table.clone();
        let mut enum_types: std::collections::HashMap<String, EnumTypeEntry> =
            initial.enum_types.clone();
        let mut enum_types_by_oid: std::collections::HashMap<Oid, EnumTypeEntry> =
            initial.enum_types_by_oid.clone();
        let mut composite_types: std::collections::HashMap<String, CompositeTypeEntry> =
            initial.composite_types.clone();
        let mut composite_types_by_oid: std::collections::HashMap<Oid, CompositeTypeEntry> =
            initial.composite_types_by_oid.clone();
        let domain_types: std::collections::HashMap<String, DomainTypeEntry> =
            initial.domain_types.clone();
        let domain_types_by_oid: std::collections::HashMap<Oid, DomainTypeEntry> =
            initial.domain_types_by_oid.clone();
        let mut highest_oid: u32 = self.next_oid.load(Ordering::Acquire);

        let mut type_rows_by_oid: std::collections::HashMap<Oid, TypeRow> =
            std::collections::HashMap::new();
        if type_blocks > 0 {
            let type_scan = heap.scan(pg_type_rel, type_blocks);
            for result in type_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_type: {e}"))
                })?;
                let row = decode_type_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_type row: {e}"))
                })?;
                track_next_oid(&mut highest_oid, row.oid, "pg_type")?;
                if row.oid.raw() >= crate::memory::FIRST_USER_OID {
                    type_rows_by_oid.insert(row.oid, row);
                }
            }
        }

        let mut enum_rows_by_type: std::collections::HashMap<Oid, Vec<EnumRow>> =
            std::collections::HashMap::new();
        if enum_blocks > 0 {
            let enum_scan = heap.scan(pg_enum_rel, enum_blocks);
            for result in enum_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_enum: {e}"))
                })?;
                let row = decode_enum_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_enum row: {e}"))
                })?;
                track_next_oid(&mut highest_oid, row.oid, "pg_enum")?;
                if row.enumtypid.raw() >= crate::memory::FIRST_USER_OID {
                    enum_rows_by_type
                        .entry(row.enumtypid)
                        .or_default()
                        .push(row);
                }
            }
        }

        for (type_oid, type_row) in &type_rows_by_oid {
            if type_row.typtype != 'e' {
                continue;
            }
            let mut enum_rows = enum_rows_by_type.remove(type_oid).ok_or_else(|| {
                CatalogError::schema_conflict(format!(
                    "enum type '{}' has no pg_enum labels",
                    type_row.typname
                ))
            })?;
            enum_rows.sort_by_key(|row| row.enumsortorder);
            let labels = enum_rows
                .into_iter()
                .map(|row| EnumLabelEntry {
                    oid: row.oid,
                    label: row.enumlabel,
                    sort_order: row.enumsortorder,
                })
                .collect::<Vec<_>>();
            let schema_name = if type_row.typnamespace.raw() == bootstrap::PG_CATALOG_OID {
                "pg_catalog".to_owned()
            } else {
                "public".to_owned()
            };
            let entry = EnumTypeEntry {
                oid: *type_oid,
                name: type_row.typname.clone(),
                schema_name,
                labels,
            };
            enum_types.insert(type_entry_key(&entry), entry.clone());
            enum_types_by_oid.insert(entry.oid, entry);
        }

        // Keep the latest attribute row per `(attrelid, attnum)`, then group
        // by relation so append-only ALTER TABLE catalog rows replace older
        // schema history during bootstrap.
        let attribute_blocks = heap.block_count(pg_attribute_rel);
        let mut latest_attrs_by_key: std::collections::HashMap<
            (Oid, i16),
            (
                crate::persistent::AttributeRow,
                ultrasql_core::DataType,
                bool,
            ),
        > = std::collections::HashMap::new();
        let mut attribute_rows: std::collections::HashMap<(Oid, i16), AttributeRow> =
            std::collections::HashMap::new();
        let mut total_attrs: u32 = 0;
        if attribute_blocks > 0 {
            let attr_scan = heap.scan(pg_attribute_rel, attribute_blocks);
            for result in attr_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_attribute: {e}"))
                })?;
                let (row, dt, nullable) = decode_attribute_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_attribute row: {e}"))
                })?;
                let key = (row.attrelid, row.attnum);
                attribute_rows.insert(key, row.clone());
                latest_attrs_by_key.insert(key, (row, dt, nullable));
                total_attrs = total_attrs.saturating_add(1);
            }
        }
        let mut attrs_by_relation: std::collections::HashMap<
            Oid,
            Vec<(
                crate::persistent::AttributeRow,
                ultrasql_core::DataType,
                bool,
            )>,
        > = std::collections::HashMap::new();
        for (_, (row, dt, nullable)) in latest_attrs_by_key {
            attrs_by_relation
                .entry(row.attrelid)
                .or_default()
                .push((row, dt, nullable));
        }

        let index_blocks = heap.block_count(pg_index_rel);
        let mut index_rows_by_oid: std::collections::HashMap<Oid, IndexRow> =
            std::collections::HashMap::new();
        let mut total_index_rows: u32 = 0;
        if index_blocks > 0 {
            let index_scan = heap.scan(pg_index_rel, index_blocks);
            for result in index_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_index: {e}"))
                })?;
                let row = decode_index_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_index row: {e}"))
                })?;
                if row.indexrelid.raw() >= crate::memory::FIRST_USER_OID {
                    index_rows_by_oid.insert(row.indexrelid, row);
                }
                total_index_rows = total_index_rows.saturating_add(1);
            }
        }

        let constraint_blocks = heap.block_count(pg_constraint_rel);
        let mut constraint_rows: std::collections::HashMap<Oid, ConstraintRow> =
            std::collections::HashMap::new();
        let mut total_constraint_rows: u32 = 0;
        if constraint_blocks > 0 {
            let constraint_scan = heap.scan(pg_constraint_rel, constraint_blocks);
            for result in constraint_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_constraint: {e}"))
                })?;
                let row = decode_constraint_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_constraint row: {e}"))
                })?;
                constraint_rows.insert(row.oid, row);
                total_constraint_rows = total_constraint_rows.saturating_add(1);
            }
        }

        let sequence_blocks = heap.block_count(pg_sequence_rel);
        let mut sequence_rows: std::collections::HashMap<Oid, SequenceRow> =
            std::collections::HashMap::new();
        if sequence_blocks > 0 {
            let sequence_scan = heap.scan(pg_sequence_rel, sequence_blocks);
            for result in sequence_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_sequence: {e}"))
                })?;
                let row = decode_sequence_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_sequence row: {e}"))
                })?;
                sequence_rows.insert(row.seqrelid, row);
            }
        }

        // Decode pg_class rows. The catalog heap is append-only, so keep the
        // latest row per OID before rebuilding tables. This lets ALTER TABLE
        // replacement rows override CREATE-time rows without consuming the
        // attribute set twice.
        let class_scan = heap.scan(pg_class_rel, class_blocks);
        let mut latest_class_by_oid: std::collections::HashMap<Oid, ClassRow> =
            std::collections::HashMap::new();
        for result in class_scan {
            let tuple = result.map_err(|e| {
                CatalogError::schema_conflict(format!("heap scan error on pg_class: {e}"))
            })?;
            let class_row = ClassRow::decode(&tuple.data)
                .map_err(|e| CatalogError::schema_conflict(format!("decode pg_class row: {e}")))?;
            // Skip system relations — they live in the initial snapshot.
            if class_row.oid.raw() < crate::memory::FIRST_USER_OID {
                continue;
            }
            track_next_oid(&mut highest_oid, class_row.oid, "pg_class")?;
            latest_class_by_oid.insert(class_row.oid, class_row);
        }

        let class_rows_by_oid = latest_class_by_oid.clone();
        let mut user_relations: u32 = 0;
        let mut user_index_classes: Vec<ClassRow> = Vec::new();
        for (_, class_row) in latest_class_by_oid {
            match class_row.relkind {
                RelKind::Table | RelKind::MaterializedView => {
                    user_relations = user_relations.saturating_add(1);
                    let attrs = attrs_by_relation.remove(&class_row.oid).unwrap_or_default();
                    let schema = schema_from_attributes(attrs).map_err(|e| {
                        CatalogError::schema_conflict(format!(
                            "rebuild schema for oid {}: {e}",
                            class_row.oid.raw(),
                        ))
                    })?;
                    let schema_name = if class_row.relnamespace.raw() == bootstrap::PG_CATALOG_OID {
                        "pg_catalog".to_owned()
                    } else {
                        "public".to_owned()
                    };
                    let entry = TableEntry {
                        oid: class_row.oid,
                        name: class_row.relname.clone(),
                        schema_name,
                        schema,
                        created_at_lsn: ultrasql_core::Lsn::ZERO,
                        n_blocks: class_row.relpages,
                        root_block: ultrasql_core::BlockNumber::new(class_row.relfilenode),
                        options: class_row.reloptions.clone(),
                    };
                    tables.insert(table_entry_key(&entry), entry.clone());
                    tables_by_oid.insert(entry.oid, entry);
                }
                RelKind::Index => {
                    user_relations = user_relations.saturating_add(1);
                    user_index_classes.push(class_row);
                }
                RelKind::CompositeType => {
                    user_relations = user_relations.saturating_add(1);
                    let Some(type_row) = type_rows_by_oid.get(&class_row.oid) else {
                        tracing::warn!(
                            oid = class_row.oid.raw(),
                            relname = %class_row.relname,
                            "skipping composite pg_class row without pg_type metadata"
                        );
                        continue;
                    };
                    if type_row.typtype != 'c' {
                        tracing::warn!(
                            oid = class_row.oid.raw(),
                            typtype = %type_row.typtype,
                            "skipping composite pg_class row whose pg_type row is not composite"
                        );
                        continue;
                    }
                    let attrs = attrs_by_relation.remove(&class_row.oid).unwrap_or_default();
                    let schema = schema_from_attributes(attrs).map_err(|e| {
                        CatalogError::schema_conflict(format!(
                            "rebuild composite schema for oid {}: {e}",
                            class_row.oid.raw(),
                        ))
                    })?;
                    let schema_name = if class_row.relnamespace.raw() == bootstrap::PG_CATALOG_OID {
                        "pg_catalog".to_owned()
                    } else {
                        "public".to_owned()
                    };
                    let entry = CompositeTypeEntry {
                        oid: class_row.oid,
                        name: class_row.relname.clone(),
                        schema_name,
                        schema,
                    };
                    composite_types.insert(type_entry_key(&entry), entry.clone());
                    composite_types_by_oid.insert(entry.oid, entry);
                }
                _ => {}
            }
        }
        for oid in constraint_rows.keys() {
            track_next_oid(&mut highest_oid, *oid, "pg_constraint")?;
        }
        for oid in sequence_rows.keys() {
            track_next_oid(&mut highest_oid, *oid, "pg_sequence")?;
        }

        let mut loaded_indexes: u32 = 0;
        for class_row in user_index_classes {
            let Some(index_row) = index_rows_by_oid.get(&class_row.oid) else {
                tracing::warn!(
                    index = %class_row.relname,
                    oid = class_row.oid.raw(),
                    "skipping orphaned pg_class index row without pg_index metadata"
                );
                continue;
            };
            if !index_row.indisvalid {
                continue;
            }
            if usize::from(index_row.indnatts) != index_row.indkey.len() {
                tracing::warn!(
                    index_oid = index_row.indexrelid.raw(),
                    indnatts = index_row.indnatts,
                    indkey_len = index_row.indkey.len(),
                    "skipping malformed pg_index row with mismatched key count"
                );
                continue;
            }
            if !tables_by_oid.contains_key(&index_row.indrelid) {
                tracing::warn!(
                    index_oid = index_row.indexrelid.raw(),
                    table_oid = index_row.indrelid.raw(),
                    "skipping pg_index row referencing unknown table"
                );
                continue;
            }
            let mut columns = Vec::with_capacity(index_row.indkey.len());
            let mut invalid_column = None;
            for &attnum in &index_row.indkey {
                match u16::try_from(attnum) {
                    Ok(column) => columns.push(column),
                    Err(_) => {
                        invalid_column = Some(attnum);
                        break;
                    }
                }
            }
            if let Some(attnum) = invalid_column {
                tracing::warn!(
                    index_oid = index_row.indexrelid.raw(),
                    attnum,
                    "skipping pg_index row with invalid column position"
                );
                continue;
            }
            let mut entry = IndexEntry::new(
                class_row.oid,
                class_row.relname.clone(),
                index_row.indrelid,
                columns,
                index_row.indisunique,
            );
            entry.schema_name = if class_row.relnamespace.raw() == bootstrap::PG_CATALOG_OID {
                "pg_catalog".to_owned()
            } else {
                "public".to_owned()
            };
            entry.root_block = ultrasql_core::BlockNumber::new(class_row.relfilenode);
            entry.access_method = index_row.indmethod.clone();
            entry.opclasses = index_row.indopclasses.clone();
            entry.options = index_row.indoptions.clone();
            indexes.insert(index_entry_key(&entry), entry.clone());
            indexes_by_table
                .entry(index_row.indrelid)
                .or_default()
                .push(entry);
            loaded_indexes = loaded_indexes.saturating_add(1);
        }
        // Bump the OID allocator past every observed OID so a
        // subsequent `next_oid` call cannot collide with a restored
        // relation.
        self.next_oid.store(highest_oid, Ordering::Release);

        let statistic_blocks = heap.block_count(pg_statistic_rel);
        let mut statistics = initial.statistics;
        let mut total_statistics: u32 = 0;
        if statistic_blocks > 0 {
            let statistic_scan = heap.scan(pg_statistic_rel, statistic_blocks);
            for result in statistic_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_statistic: {e}"))
                })?;
                let row = decode_statistic_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_statistic row: {e}"))
                })?;
                statistics.insert((row.starelid, row.staattnum), row);
                total_statistics = total_statistics.saturating_add(1);
            }
        }
        statistics.retain(|(starelid, _), _| {
            starelid.raw() < crate::memory::FIRST_USER_OID || tables_by_oid.contains_key(starelid)
        });
        total_statistics = u32::try_from(statistics.len()).unwrap_or(u32::MAX);

        let statistic_ext_blocks = heap.block_count(pg_statistic_ext_rel);
        let mut statistic_ext = initial.statistic_ext;
        let mut total_statistic_ext: u32 = 0;
        if statistic_ext_blocks > 0 {
            let statistic_ext_scan = heap.scan(pg_statistic_ext_rel, statistic_ext_blocks);
            for result in statistic_ext_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!(
                        "heap scan error on pg_statistic_ext: {e}"
                    ))
                })?;
                let row = decode_statistic_ext_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_statistic_ext row: {e}"))
                })?;
                statistic_ext.insert(row.oid, row);
                total_statistic_ext = total_statistic_ext.saturating_add(1);
            }
        }
        statistic_ext.retain(|_, row| {
            row.stxrelid.raw() < crate::memory::FIRST_USER_OID
                || tables_by_oid.contains_key(&row.stxrelid)
        });
        total_statistic_ext = u32::try_from(statistic_ext.len()).unwrap_or(u32::MAX);

        let description_blocks = heap.block_count(pg_description_rel);
        let mut descriptions = initial.descriptions;
        let mut total_description_rows: u32 = 0;
        if description_blocks > 0 {
            let description_scan = heap.scan(pg_description_rel, description_blocks);
            for result in description_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_description: {e}"))
                })?;
                let (row, deleted) = decode_description_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_description row: {e}"))
                })?;
                let key = (row.objoid, row.classoid, row.objsubid);
                if deleted {
                    descriptions.remove(&key);
                } else {
                    descriptions.insert(key, row);
                }
                total_description_rows = total_description_rows.saturating_add(1);
            }
        }
        let mut live_description_oids: std::collections::HashSet<Oid> =
            tables_by_oid.keys().copied().collect();
        for index in indexes.values() {
            live_description_oids.insert(index.oid);
        }
        descriptions.retain(|(objoid, _, _), _| live_description_oids.contains(objoid));

        let snap = CatalogSnapshot {
            tables,
            tables_by_oid,
            indexes,
            indexes_by_table,
            enum_types,
            enum_types_by_oid,
            composite_types,
            composite_types_by_oid,
            domain_types,
            domain_types_by_oid,
            constraints: constraint_rows.clone(),
            descriptions,
            statistics,
            statistic_ext,
        };
        let stats = CatalogStats {
            namespaces: CatalogStats::initial().namespaces,
            relations: CatalogStats::initial().relations + user_relations,
            attributes: total_attrs,
            indexes: loaded_indexes.max(total_index_rows),
            constraints: total_constraint_rows,
            descriptions: total_description_rows,
            statistics: total_statistics,
            statistic_ext: total_statistic_ext,
        };
        self.install_snapshot(snap)?;
        self.pg_class.clear();
        for (oid, row) in class_rows_by_oid {
            self.pg_class.insert(oid, row);
        }
        self.pg_type.clear();
        for (oid, row) in type_rows_by_oid {
            self.pg_type.insert(oid, row);
        }
        self.pg_attribute.clear();
        for (key, row) in attribute_rows {
            self.pg_attribute.insert(key, row);
        }
        self.pg_constraint.clear();
        for (oid, row) in constraint_rows {
            self.pg_constraint.insert(oid, row);
        }
        self.pg_sequence.clear();
        for (oid, row) in sequence_rows {
            self.pg_sequence.insert(oid, row);
        }
        tracing::debug!(?stats, "catalog bootstrapped from heap");
        Ok(stats)
    }

    /// Register a user-defined enum type in the in-memory catalog snapshot.
    ///
    /// `entry.labels` must be non-empty and label text must be unique inside
    /// the type. The durable heap rows are written separately by
    /// [`Self::persist_enum_type_rows`] so DDL can coordinate catalog writes
    /// with its transaction metadata.
    pub fn create_enum_type(&self, entry: EnumTypeEntry) -> Result<(), CatalogError> {
        if entry.oid.is_invalid() {
            return Err(CatalogError::schema_conflict(
                "cannot register enum type with INVALID oid",
            ));
        }
        if entry.labels.is_empty() {
            return Err(CatalogError::schema_conflict(format!(
                "enum type '{}' must have at least one label",
                entry.name
            )));
        }
        let mut seen = std::collections::HashSet::with_capacity(entry.labels.len());
        for label in &entry.labels {
            if label.oid.is_invalid() {
                return Err(CatalogError::schema_conflict(format!(
                    "enum type '{}' has label '{}' with INVALID oid",
                    entry.name, label.label
                )));
            }
            if !seen.insert(label.label.clone()) {
                return Err(CatalogError::schema_conflict(format!(
                    "enum type '{}' repeats label '{}'",
                    entry.name, label.label
                )));
            }
        }
        let key = type_entry_key(&entry);
        let relation_key = table_lookup_key(&entry.schema_name, &entry.name);
        let _guard = self.write_lock.lock();
        if self.enum_types_by_name.contains_key(&key)
            || self.composite_types_by_name.contains_key(&key)
            || self.domain_types_by_name.contains_key(&key)
            || self.tables_by_name.contains_key(&relation_key)
        {
            return Err(CatalogError::already_exists(entry.name));
        }
        if self.enum_types_by_oid.contains_key(&entry.oid)
            || self.composite_types_by_oid.contains_key(&entry.oid)
            || self.domain_types_by_oid.contains_key(&entry.oid)
            || self.tables_by_oid.contains_key(&entry.oid)
        {
            return Err(CatalogError::already_exists(format!(
                "oid {}",
                entry.oid.raw()
            )));
        }
        self.pg_type.insert(entry.oid, type_row_from_enum(&entry));
        for label in &entry.labels {
            self.pg_enum.insert(
                (entry.oid, label.sort_order),
                enum_row_from_label(entry.oid, label),
            );
        }
        self.enum_types_by_name.insert(key, entry.clone());
        self.enum_types_by_oid.insert(entry.oid, entry);
        self.rebuild_snapshot();
        Ok(())
    }

    /// Remove an enum type from the in-memory catalog snapshot.
    ///
    /// Used by DDL rollback paths when durable catalog-row writes fail after
    /// the type has been published to the current process.
    pub fn drop_enum_type(&self, name: &str) -> Result<(), CatalogError> {
        let key = fold_name(name);
        let _guard = self.write_lock.lock();
        let removed = self
            .enum_types_by_name
            .remove(&key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .1;
        self.enum_types_by_oid.remove(&removed.oid);
        self.pg_type.remove(&removed.oid);
        let enum_keys = self
            .pg_enum
            .iter()
            .filter(|row| row.key().0 == removed.oid)
            .map(|row| *row.key())
            .collect::<Vec<_>>();
        for enum_key in enum_keys {
            self.pg_enum.remove(&enum_key);
        }
        self.rebuild_snapshot();
        Ok(())
    }

    /// Append durable `pg_type` / `pg_enum` rows for one user enum type.
    pub fn persist_enum_type_rows<L: PageLoader>(
        &self,
        entry: &EnumTypeEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::{encode_enum_row, encode_type_row};
        use ultrasql_storage::heap::InsertOptions;

        let pg_type_rel = RelationId::new(bootstrap::PG_TYPE_OID);
        let pg_enum_rel = RelationId::new(bootstrap::PG_ENUM_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());

        let type_row = type_row_from_enum(entry);
        let type_bytes = encode_type_row(&type_row)
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_type: {e}")))?;
        heap.insert(
            pg_type_rel,
            &type_bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: TYPE_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_type insert: {e}")))?;

        for label in &entry.labels {
            let enum_row = enum_row_from_label(entry.oid, label);
            let enum_bytes = encode_enum_row(&enum_row)
                .map_err(|e| CatalogError::schema_conflict(format!("encode pg_enum: {e}")))?;
            heap.insert(
                pg_enum_rel,
                &enum_bytes,
                InsertOptions {
                    xmin,
                    command_id,
                    n_atts: ENUM_ROW_N_ATTS,
                    wal,
                    fsm: None,
                    vm: None,
                },
            )
            .map_err(|e| CatalogError::schema_conflict(format!("pg_enum insert: {e}")))?;
        }
        Ok(())
    }

    /// Register a user-defined composite type in the in-memory catalog
    /// snapshot.
    pub fn create_composite_type(&self, entry: CompositeTypeEntry) -> Result<(), CatalogError> {
        if entry.oid.is_invalid() {
            return Err(CatalogError::schema_conflict(
                "cannot register composite type with INVALID oid",
            ));
        }
        if entry.schema.fields().is_empty() {
            return Err(CatalogError::schema_conflict(format!(
                "composite type '{}' must have at least one attribute",
                entry.name
            )));
        }
        let key = type_entry_key(&entry);
        let relation_key = table_lookup_key(&entry.schema_name, &entry.name);
        let _guard = self.write_lock.lock();
        if self.composite_types_by_name.contains_key(&key)
            || self.enum_types_by_name.contains_key(&key)
            || self.domain_types_by_name.contains_key(&key)
            || self.tables_by_name.contains_key(&relation_key)
        {
            return Err(CatalogError::already_exists(entry.name));
        }
        if self.composite_types_by_oid.contains_key(&entry.oid)
            || self.enum_types_by_oid.contains_key(&entry.oid)
            || self.domain_types_by_oid.contains_key(&entry.oid)
            || self.tables_by_oid.contains_key(&entry.oid)
        {
            return Err(CatalogError::already_exists(format!(
                "oid {}",
                entry.oid.raw()
            )));
        }
        self.pg_type
            .insert(entry.oid, type_row_from_composite(&entry));
        self.pg_class
            .insert(entry.oid, class_row_from_composite(&entry));
        let attr_context = format!("composite type {}", entry.name);
        for (idx, field) in entry.schema.fields().iter().enumerate() {
            let attnum = attnum_for_index(idx, &attr_context)?;
            self.pg_attribute.insert(
                (entry.oid, attnum),
                AttributeRow {
                    attrelid: entry.oid,
                    attname: field.name.clone(),
                    atttypid: 0,
                    attnum,
                    attnotnull: !field.nullable,
                    atthasdef: false,
                    attisdropped: false,
                },
            );
        }
        self.composite_types_by_name.insert(key, entry.clone());
        self.composite_types_by_oid.insert(entry.oid, entry);
        self.rebuild_snapshot();
        Ok(())
    }

    /// Remove a composite type from the in-memory catalog snapshot.
    pub fn drop_composite_type(&self, name: &str) -> Result<(), CatalogError> {
        let key = fold_name(name);
        let _guard = self.write_lock.lock();
        let removed = self
            .composite_types_by_name
            .remove(&key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .1;
        self.composite_types_by_oid.remove(&removed.oid);
        self.pg_type.remove(&removed.oid);
        self.pg_class.remove(&removed.oid);
        let attr_keys = self
            .pg_attribute
            .iter()
            .filter(|row| row.key().0 == removed.oid)
            .map(|row| *row.key())
            .collect::<Vec<_>>();
        for attr_key in attr_keys {
            self.pg_attribute.remove(&attr_key);
        }
        self.rebuild_snapshot();
        Ok(())
    }

    /// Append durable `pg_type` / `pg_class` / `pg_attribute` rows for one
    /// user composite type.
    pub fn persist_composite_type_rows<L: PageLoader>(
        &self,
        entry: &CompositeTypeEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::{encode_attribute_row, encode_type_row};
        use ultrasql_storage::heap::InsertOptions;

        let pg_type_rel = RelationId::new(bootstrap::PG_TYPE_OID);
        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_attribute_rel = RelationId::new(bootstrap::PG_ATTRIBUTE_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());

        let type_row = type_row_from_composite(entry);
        let type_bytes = encode_type_row(&type_row)
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_type: {e}")))?;
        heap.insert(
            pg_type_rel,
            &type_bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: TYPE_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_type insert: {e}")))?;

        let class_bytes = class_row_from_composite(entry)
            .encode()
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_class: {e}")))?;
        heap.insert(
            pg_class_rel,
            &class_bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: CLASS_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_class insert: {e}")))?;

        let attr_context = format!("composite type {}", entry.name);
        for (idx, field) in entry.schema.fields().iter().enumerate() {
            let attnum = attnum_for_index(idx, &attr_context)?;
            let attr_row = AttributeRow {
                attrelid: entry.oid,
                attname: field.name.clone(),
                atttypid: 0,
                attnum,
                attnotnull: !field.nullable,
                atthasdef: false,
                attisdropped: false,
            };
            let bytes = encode_attribute_row(&attr_row, &field.data_type, field.nullable)
                .map_err(|e| CatalogError::schema_conflict(format!("encode pg_attribute: {e}")))?;
            heap.insert(
                pg_attribute_rel,
                &bytes,
                InsertOptions {
                    xmin,
                    command_id,
                    n_atts: ATTRIBUTE_ROW_N_ATTS,
                    wal,
                    fsm: None,
                    vm: None,
                },
            )
            .map_err(|e| CatalogError::schema_conflict(format!("pg_attribute insert: {e}")))?;
        }
        Ok(())
    }

    /// Register a user-defined domain type in the in-memory catalog snapshot.
    pub fn create_domain_type(&self, entry: DomainTypeEntry) -> Result<(), CatalogError> {
        if entry.oid.is_invalid() {
            return Err(CatalogError::schema_conflict(
                "cannot register domain type with INVALID oid",
            ));
        }
        if matches!(entry.base_type, DataType::Null) {
            return Err(CatalogError::schema_conflict(format!(
                "domain type '{}' must have a concrete base type",
                entry.name
            )));
        }
        let key = type_entry_key(&entry);
        let relation_key = table_lookup_key(&entry.schema_name, &entry.name);
        let _guard = self.write_lock.lock();
        if self.domain_types_by_name.contains_key(&key)
            || self.enum_types_by_name.contains_key(&key)
            || self.composite_types_by_name.contains_key(&key)
            || self.tables_by_name.contains_key(&relation_key)
        {
            return Err(CatalogError::already_exists(entry.name));
        }
        if self.domain_types_by_oid.contains_key(&entry.oid)
            || self.enum_types_by_oid.contains_key(&entry.oid)
            || self.composite_types_by_oid.contains_key(&entry.oid)
            || self.tables_by_oid.contains_key(&entry.oid)
        {
            return Err(CatalogError::already_exists(format!(
                "oid {}",
                entry.oid.raw()
            )));
        }
        self.pg_type.insert(entry.oid, type_row_from_domain(&entry));
        self.domain_types_by_name.insert(key, entry.clone());
        self.domain_types_by_oid.insert(entry.oid, entry);
        self.rebuild_snapshot();
        Ok(())
    }

    /// Remove a domain type from the in-memory catalog snapshot.
    pub fn drop_domain_type(&self, name: &str) -> Result<(), CatalogError> {
        let key = fold_name(name);
        let _guard = self.write_lock.lock();
        let removed = self
            .domain_types_by_name
            .remove(&key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .1;
        self.domain_types_by_oid.remove(&removed.oid);
        self.pg_type.remove(&removed.oid);
        self.rebuild_snapshot();
        Ok(())
    }

    /// Append durable `pg_type` rows for one user domain type.
    pub fn persist_domain_type_rows<L: PageLoader>(
        &self,
        entry: &DomainTypeEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_type_row;
        use ultrasql_storage::heap::InsertOptions;

        let pg_type_rel = RelationId::new(bootstrap::PG_TYPE_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let type_bytes = encode_type_row(&type_row_from_domain(entry))
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_type: {e}")))?;
        heap.insert(
            pg_type_rel,
            &type_bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: TYPE_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_type insert: {e}")))?;
        Ok(())
    }

    /// Encode and write `entry` into persistent `pg_class` / `pg_index` rows.
    ///
    /// This is the durable counterpart to [`Self::create_index`], which only
    /// publishes the in-memory catalog snapshot. DDL callers invoke both so a
    /// warm restart can rebuild index metadata and keep choosing `IndexScan`
    /// plans.
    pub fn persist_index_rows<L: PageLoader>(
        &self,
        entry: &IndexEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_index_row;
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_index_rel = RelationId::new(bootstrap::PG_INDEX_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());

        let class_row = ClassRow {
            oid: entry.oid,
            relname: entry.name.clone(),
            relnamespace: namespace_oid_for_schema(&entry.schema_name),
            relkind: RelKind::Index,
            relpages: 0,
            reltuples: 0.0,
            relfilenode: entry.root_block.raw(),
            relhasindex: false,
            reloptions: Vec::new(),
        };
        let class_bytes = class_row
            .encode()
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_class: {e}")))?;
        heap.insert(
            pg_class_rel,
            &class_bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: CLASS_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_class index insert: {e}")))?;

        let mut indkey = Vec::with_capacity(entry.columns.len());
        for &column in &entry.columns {
            indkey.push(i16::try_from(column).map_err(|_| {
                CatalogError::schema_conflict(format!(
                    "index '{}' column position {} does not fit i16",
                    entry.name, column
                ))
            })?);
        }
        let index_row = IndexRow {
            indexrelid: entry.oid,
            indrelid: entry.table_oid,
            indnatts: u16::try_from(entry.columns.len()).map_err(|_| {
                CatalogError::schema_conflict(format!(
                    "index '{}' has too many key columns",
                    entry.name
                ))
            })?,
            indisunique: entry.is_unique,
            indisprimary: entry.name.ends_with("_pkey"),
            indisvalid: true,
            indkey,
            indmethod: entry.access_method.clone(),
            indopclasses: normalized_opclasses(entry),
            indoptions: entry.options.clone(),
        };
        let bytes = encode_index_row(&index_row)
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_index: {e}")))?;
        heap.insert(
            pg_index_rel,
            &bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: INDEX_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_index insert: {e}")))?;
        Ok(())
    }

    /// Append a durable `pg_class` tombstone for a dropped index.
    ///
    /// `pg_index` rows are append-only today. Bootstrap only rebuilds indexes
    /// whose latest `pg_class` row is `RelKind::Index`, so a dropped marker on
    /// the index relation suppresses older index metadata after restart.
    pub fn persist_index_drop_tombstone<L: PageLoader>(
        &self,
        entry: &IndexEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let class_row = ClassRow {
            oid: entry.oid,
            relname: entry.name.clone(),
            relnamespace: namespace_oid_for_schema(&entry.schema_name),
            relkind: RelKind::Dropped,
            relpages: 0,
            reltuples: 0.0,
            relfilenode: entry.root_block.raw(),
            relhasindex: false,
            reloptions: Vec::new(),
        };
        let class_bytes = class_row
            .encode()
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_class: {e}")))?;
        heap.insert(
            pg_class_rel,
            &class_bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: CLASS_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| {
            CatalogError::schema_conflict(format!("pg_class index tombstone insert: {e}"))
        })?;
        Ok(())
    }

    /// Append one `pg_constraint` row to the persistent catalog heap.
    pub fn persist_constraint_row<L: PageLoader>(
        &self,
        row: &ConstraintRow,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_constraint_row;
        use ultrasql_storage::heap::InsertOptions;

        let pg_constraint_rel = RelationId::new(bootstrap::PG_CONSTRAINT_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let bytes = encode_constraint_row(row)
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_constraint: {e}")))?;
        heap.insert(
            pg_constraint_rel,
            &bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: CONSTRAINT_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_constraint insert: {e}")))?;
        Ok(())
    }

    /// Publish committed `pg_constraint` rows to the live catalog side map.
    ///
    /// DDL callers invoke this only after the catalog-write transaction commits.
    /// Bootstrap installs the same rows from heap during startup.
    pub fn install_constraint_rows<I>(&self, rows: I)
    where
        I: IntoIterator<Item = ConstraintRow>,
    {
        for row in rows {
            self.pg_constraint.insert(row.oid, row);
        }
        self.rebuild_snapshot();
    }

    /// Remove live `pg_constraint` rows owned by one dropped table.
    pub fn remove_constraints_for_table(&self, table_oid: Oid) {
        let stale = self
            .pg_constraint
            .iter()
            .filter_map(|row| (row.value().conrelid == table_oid).then_some(*row.key()))
            .collect::<Vec<_>>();
        for oid in stale {
            self.pg_constraint.remove(&oid);
        }
    }

    /// Return a constraint that depends on an index name for one table.
    ///
    /// Constraint-created indexes use the constraint name as the index name in
    /// the current catalog. Dropping those indexes directly would leave
    /// `pg_constraint` rows that claim enforcement still exists.
    #[must_use]
    pub fn constraint_dependency_for_index(
        &self,
        table_oid: Oid,
        index_name: &str,
    ) -> Option<ConstraintRow> {
        let key = fold_name(index_name);
        self.pg_constraint
            .iter()
            .find(|row| {
                let row = row.value();
                row.conrelid == table_oid
                    && fold_name(&row.conname) == key
                    && matches!(
                        row.contype,
                        ConType::PrimaryKey | ConType::Unique | ConType::Exclusion
                    )
            })
            .map(|row| row.value().clone())
    }

    /// Append `pg_class` / `pg_sequence` rows for one sequence.
    pub fn persist_sequence_rows<L: PageLoader>(
        &self,
        sequence_name: &str,
        schema_name: &str,
        row: &SequenceRow,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_sequence_row;
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_sequence_rel = RelationId::new(bootstrap::PG_SEQUENCE_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let class_row = ClassRow {
            oid: row.seqrelid,
            relname: sequence_name.to_owned(),
            relnamespace: namespace_oid_for_schema(schema_name),
            relkind: RelKind::Sequence,
            relpages: 0,
            reltuples: 0.0,
            relfilenode: 0,
            relhasindex: false,
            reloptions: Vec::new(),
        };
        let class_bytes = class_row
            .encode()
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_class: {e}")))?;
        heap.insert(
            pg_class_rel,
            &class_bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: CLASS_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_class sequence insert: {e}")))?;

        let bytes = encode_sequence_row(row);
        heap.insert(
            pg_sequence_rel,
            &bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: SEQUENCE_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_sequence insert: {e}")))?;
        Ok(())
    }

    /// Encode and write `entry` into the persistent `pg_class` /
    /// `pg_attribute` heaps so a subsequent
    /// [`Self::bootstrap_from_heap`] call can rebuild this
    /// `TableEntry` after restart.
    ///
    /// This is the durable counterpart to [`Self::create_table`]
    /// (which only updates the in-memory `DashMap`s). DDL callers
    /// invoke both: first `create_table` so the planner sees the new
    /// relation, then `persist_table_rows` so the next restart finds
    /// it on disk. Heap I/O happens through the same `xmin`/
    /// `command_id` the DDL transaction owns so MVCC visibility
    /// rules apply uniformly.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::SchemaConflict`] when the column's
    /// [`DataType`] is outside the catalog-
    /// persistable set (e.g. `Array`, `Record`), or when a heap I/O
    /// failure prevents either pg_class or pg_attribute from
    /// accepting the row.
    pub fn persist_table_rows<L: PageLoader>(
        &self,
        entry: &TableEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        self.persist_table_rows_with_defaults(entry, &[], heap, xmin, command_id)
    }

    /// Append catalog rows for a table schema replacement.
    ///
    /// `pg_attribute` is append-only. To replace a compacted UltraSQL schema
    /// after `ALTER TABLE`, write dropped markers for every old attnum first,
    /// then write the new compacted attributes. Bootstrap keeps the latest row
    /// per `(attrelid, attnum)`, so reused attnums resolve to the new schema
    /// and old surplus attnums resolve to dropped columns.
    pub fn persist_table_schema_replacement<L: PageLoader>(
        &self,
        old_entry: &TableEntry,
        new_entry: &TableEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        self.persist_table_schema_replacement_with_defaults(
            old_entry,
            new_entry,
            &[],
            heap,
            xmin,
            command_id,
        )
    }

    /// Append catalog rows for a table schema replacement with
    /// caller-supplied `pg_attribute.atthasdef` metadata.
    ///
    /// `attr_has_defaults` is indexed by zero-based column position in
    /// `new_entry.schema`. Missing entries default to `false`.
    pub fn persist_table_schema_replacement_with_defaults<L: PageLoader>(
        &self,
        old_entry: &TableEntry,
        new_entry: &TableEntry,
        attr_has_defaults: &[bool],
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_attribute_row;
        use crate::persistent::{AttributeRow, ClassRow};
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_attribute_rel = RelationId::new(bootstrap::PG_ATTRIBUTE_OID);
        let namespace_oid = namespace_oid_for_schema(&new_entry.schema_name);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let class_row = ClassRow {
            oid: new_entry.oid,
            relname: new_entry.name.clone(),
            relnamespace: namespace_oid,
            relkind: RelKind::Table,
            relpages: new_entry.n_blocks,
            reltuples: 0.0,
            relfilenode: new_entry.root_block.raw(),
            relhasindex: false,
            reloptions: new_entry.options.clone(),
        };
        let class_bytes = class_row
            .encode()
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_class: {e}")))?;
        heap.insert(
            pg_class_rel,
            &class_bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: CLASS_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_class insert: {e}")))?;

        let old_attr_context = format!("old table {}", old_entry.name);
        for (i, field) in old_entry.schema.fields().iter().enumerate() {
            let attnum = attnum_for_index(i, &old_attr_context)?;
            let attr_row = AttributeRow {
                attrelid: new_entry.oid,
                attname: field.name.clone(),
                atttypid: 0,
                attnum,
                attnotnull: !field.nullable,
                atthasdef: false,
                attisdropped: true,
            };
            let bytes = encode_attribute_row(&attr_row, &field.data_type, field.nullable)
                .map_err(|e| CatalogError::schema_conflict(format!("encode pg_attribute: {e}")))?;
            heap.insert(
                pg_attribute_rel,
                &bytes,
                InsertOptions {
                    xmin,
                    command_id,
                    n_atts: ATTRIBUTE_ROW_N_ATTS,
                    wal,
                    fsm: None,
                    vm: None,
                },
            )
            .map_err(|e| CatalogError::schema_conflict(format!("pg_attribute insert: {e}")))?;
        }

        let new_attr_context = format!("table {}", new_entry.name);
        for (i, field) in new_entry.schema.fields().iter().enumerate() {
            let attnum = attnum_for_index(i, &new_attr_context)?;
            let attr_row = AttributeRow {
                attrelid: new_entry.oid,
                attname: field.name.clone(),
                atttypid: 0,
                attnum,
                attnotnull: !field.nullable,
                atthasdef: attr_has_defaults.get(i).copied().unwrap_or(false),
                attisdropped: false,
            };
            let bytes = encode_attribute_row(&attr_row, &field.data_type, field.nullable)
                .map_err(|e| CatalogError::schema_conflict(format!("encode pg_attribute: {e}")))?;
            heap.insert(
                pg_attribute_rel,
                &bytes,
                InsertOptions {
                    xmin,
                    command_id,
                    n_atts: ATTRIBUTE_ROW_N_ATTS,
                    wal,
                    fsm: None,
                    vm: None,
                },
            )
            .map_err(|e| CatalogError::schema_conflict(format!("pg_attribute insert: {e}")))?;
        }
        Ok(())
    }

    /// Append a durable `pg_class` tombstone for a dropped table.
    ///
    /// Catalog heaps are append-only today. Bootstrap keeps the newest
    /// `pg_class` row per OID, so a `RelKind::Dropped` marker suppresses
    /// older CREATE/ALTER rows after restart without needing heap delete
    /// support first.
    pub fn persist_table_drop_tombstone<L: PageLoader>(
        &self,
        entry: &TableEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_attribute_row;
        use crate::persistent::{AttributeRow, ClassRow};
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_attribute_rel = RelationId::new(bootstrap::PG_ATTRIBUTE_OID);
        let namespace_oid = namespace_oid_for_schema(&entry.schema_name);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let class_row = ClassRow {
            oid: entry.oid,
            relname: entry.name.clone(),
            relnamespace: namespace_oid,
            relkind: RelKind::Dropped,
            relpages: entry.n_blocks,
            reltuples: 0.0,
            relfilenode: entry.root_block.raw(),
            relhasindex: false,
            reloptions: entry.options.clone(),
        };
        let class_bytes = class_row
            .encode()
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_class: {e}")))?;
        heap.insert(
            pg_class_rel,
            &class_bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: CLASS_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_class tombstone insert: {e}")))?;

        let attr_context = format!("table {}", entry.name);
        for (i, field) in entry.schema.fields().iter().enumerate() {
            let attnum = attnum_for_index(i, &attr_context)?;
            let attr_row = AttributeRow {
                attrelid: entry.oid,
                attname: field.name.clone(),
                atttypid: 0,
                attnum,
                attnotnull: !field.nullable,
                atthasdef: false,
                attisdropped: true,
            };
            let bytes = encode_attribute_row(&attr_row, &field.data_type, field.nullable)
                .map_err(|e| CatalogError::schema_conflict(format!("encode pg_attribute: {e}")))?;
            heap.insert(
                pg_attribute_rel,
                &bytes,
                InsertOptions {
                    xmin,
                    command_id,
                    n_atts: ATTRIBUTE_ROW_N_ATTS,
                    wal,
                    fsm: None,
                    vm: None,
                },
            )
            .map_err(|e| {
                CatalogError::schema_conflict(format!("pg_attribute tombstone insert: {e}"))
            })?;
        }
        Ok(())
    }

    /// Append `pg_class` / `pg_attribute` rows for one user table with
    /// caller-supplied `atthasdef` metadata.
    ///
    /// `attr_has_defaults` is indexed by zero-based column position. Missing
    /// entries are treated as `false`, preserving the legacy behavior for
    /// callers that have no default-expression metadata.
    pub fn persist_table_rows_with_defaults<L: PageLoader>(
        &self,
        entry: &TableEntry,
        attr_has_defaults: &[bool],
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        self.persist_relation_rows_with_defaults(
            entry,
            RelKind::Table,
            attr_has_defaults,
            heap,
            xmin,
            command_id,
        )
    }

    /// Insert `pg_class` + `pg_attribute` rows for a heap-backed relation kind.
    pub fn persist_relation_rows_with_defaults<L: PageLoader>(
        &self,
        entry: &TableEntry,
        relkind: RelKind,
        attr_has_defaults: &[bool],
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_attribute_row;
        use crate::persistent::{AttributeRow, ClassRow};
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_attribute_rel = RelationId::new(bootstrap::PG_ATTRIBUTE_OID);

        let namespace_oid = namespace_oid_for_schema(&entry.schema_name);

        let class_row = ClassRow {
            oid: entry.oid,
            relname: entry.name.clone(),
            relnamespace: namespace_oid,
            relkind,
            relpages: entry.n_blocks,
            reltuples: 0.0,
            relfilenode: entry.root_block.raw(),
            relhasindex: false,
            reloptions: entry.options.clone(),
        };
        let class_bytes = class_row
            .encode()
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_class: {e}")))?;
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let class_opts = InsertOptions {
            xmin,
            command_id,
            n_atts: CLASS_ROW_N_ATTS,
            wal,
            fsm: None,
            vm: None,
        };
        heap.insert(pg_class_rel, &class_bytes, class_opts)
            .map_err(|e| CatalogError::schema_conflict(format!("pg_class insert: {e}")))?;

        let attr_context = format!("table {}", entry.name);
        for (i, field) in entry.schema.fields().iter().enumerate() {
            let attnum = attnum_for_index(i, &attr_context)?;
            let attr_row = AttributeRow {
                attrelid: entry.oid,
                attname: field.name.clone(),
                atttypid: 0,
                attnum,
                attnotnull: !field.nullable,
                atthasdef: attr_has_defaults.get(i).copied().unwrap_or(false),
                attisdropped: false,
            };
            let bytes = encode_attribute_row(&attr_row, &field.data_type, field.nullable)
                .map_err(|e| CatalogError::schema_conflict(format!("encode pg_attribute: {e}")))?;
            let attr_opts = InsertOptions {
                xmin,
                command_id,
                n_atts: ATTRIBUTE_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            };
            heap.insert(pg_attribute_rel, &bytes, attr_opts)
                .map_err(|e| CatalogError::schema_conflict(format!("pg_attribute insert: {e}")))?;
        }
        Ok(())
    }

    /// Append `pg_statistic` rows to the persistent catalog heap.
    ///
    /// `replace_statistics` updates the wait-free in-memory snapshot. This
    /// method writes the durable row stream consumed by
    /// [`Self::bootstrap_from_heap`]. Rows are append-only; bootstrap keeps the
    /// last row for each `(starelid, staattnum)` key.
    pub fn persist_statistic_rows<L: PageLoader>(
        &self,
        rows: &[StatisticRow],
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_statistic_row;
        use ultrasql_storage::heap::InsertOptions;

        let pg_statistic_rel = RelationId::new(bootstrap::PG_STATISTIC_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        for row in rows {
            let bytes = encode_statistic_row(row);
            let opts = InsertOptions {
                xmin,
                command_id,
                n_atts: STATISTIC_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            };
            heap.insert(pg_statistic_rel, &bytes, opts)
                .map_err(|e| CatalogError::schema_conflict(format!("pg_statistic insert: {e}")))?;
        }
        Ok(())
    }

    /// Append one `pg_statistic_ext` row to the persistent catalog heap.
    pub fn persist_statistic_ext_row<L: PageLoader>(
        &self,
        row: &StatisticExtRow,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_statistic_ext_row;
        use ultrasql_storage::heap::InsertOptions;

        let pg_statistic_ext_rel = RelationId::new(bootstrap::PG_STATISTIC_EXT_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let bytes = encode_statistic_ext_row(row)
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_statistic_ext: {e}")))?;
        heap.insert(
            pg_statistic_ext_rel,
            &bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: STATISTIC_EXT_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_statistic_ext insert: {e}")))?;
        Ok(())
    }

    /// Rebuild and swap in a new snapshot.
    ///
    /// Must hold `write_lock` when calling.
    fn rebuild_snapshot(&self) {
        let tables: std::collections::HashMap<String, TableEntry> = self
            .tables_by_name
            .iter()
            .map(|r| {
                let entry = r.value().clone();
                (table_entry_key(&entry), entry)
            })
            .collect();
        let tables_by_oid: std::collections::HashMap<Oid, TableEntry> = self
            .tables_by_oid
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        let indexes: std::collections::HashMap<String, IndexEntry> = self
            .indexes_by_name
            .iter()
            .map(|r| {
                let entry = r.value().clone();
                (index_entry_key(&entry), entry)
            })
            .collect();
        let indexes_by_table: std::collections::HashMap<Oid, Vec<IndexEntry>> = self
            .indexes_by_table
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        let enum_types: std::collections::HashMap<String, EnumTypeEntry> = self
            .enum_types_by_name
            .iter()
            .map(|r| {
                let entry = r.value().clone();
                (type_entry_key(&entry), entry)
            })
            .collect();
        let enum_types_by_oid: std::collections::HashMap<Oid, EnumTypeEntry> = self
            .enum_types_by_oid
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        let composite_types: std::collections::HashMap<String, CompositeTypeEntry> = self
            .composite_types_by_name
            .iter()
            .map(|r| {
                let entry = r.value().clone();
                (type_entry_key(&entry), entry)
            })
            .collect();
        let composite_types_by_oid: std::collections::HashMap<Oid, CompositeTypeEntry> = self
            .composite_types_by_oid
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        let domain_types: std::collections::HashMap<String, DomainTypeEntry> = self
            .domain_types_by_name
            .iter()
            .map(|r| {
                let entry = r.value().clone();
                (type_entry_key(&entry), entry)
            })
            .collect();
        let domain_types_by_oid: std::collections::HashMap<Oid, DomainTypeEntry> = self
            .domain_types_by_oid
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        let descriptions: std::collections::HashMap<(Oid, Oid, i32), DescriptionRow> = self
            .pg_description
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        let constraints: std::collections::HashMap<Oid, ConstraintRow> = self
            .pg_constraint
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        let statistics: std::collections::HashMap<(Oid, i16), StatisticRow> = self
            .pg_statistic
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        let statistic_ext: std::collections::HashMap<Oid, StatisticExtRow> = self
            .pg_statistic_ext
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        let snap = Arc::new(CatalogSnapshot {
            tables,
            tables_by_oid,
            indexes,
            indexes_by_table,
            enum_types,
            enum_types_by_oid,
            composite_types,
            composite_types_by_oid,
            domain_types,
            domain_types_by_oid,
            constraints,
            descriptions,
            statistics,
            statistic_ext,
        });
        self.snapshot.store(snap);
    }

    /// Set or clear an object comment in `pg_description`.
    pub fn set_description(
        &self,
        objoid: Oid,
        classoid: Oid,
        objsubid: i32,
        description: Option<String>,
    ) {
        let _guard = self.write_lock.lock();
        let key = (objoid, classoid, objsubid);
        if let Some(description) = description {
            self.pg_description.insert(
                key,
                DescriptionRow {
                    objoid,
                    classoid,
                    objsubid,
                    description,
                },
            );
        } else {
            self.pg_description.remove(&key);
        }
        self.rebuild_snapshot();
    }

    /// Append a durable `pg_description` row or deletion tombstone.
    pub fn persist_description_row<L: PageLoader>(
        &self,
        row: &DescriptionRow,
        deleted: bool,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_description_row;
        use ultrasql_storage::heap::InsertOptions;

        let pg_description_rel = RelationId::new(bootstrap::PG_DESCRIPTION_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let bytes = encode_description_row(row, deleted)
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_description: {e}")))?;
        heap.insert(
            pg_description_rel,
            &bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: DESCRIPTION_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_description insert: {e}")))?;
        Ok(())
    }

    /// Clear every comment attached to one object OID.
    pub fn clear_descriptions_for_object(&self, objoid: Oid) {
        let _guard = self.write_lock.lock();
        let keys: Vec<_> = self
            .pg_description
            .iter()
            .filter(|entry| entry.key().0 == objoid)
            .map(|entry| *entry.key())
            .collect();
        for key in keys {
            self.pg_description.remove(&key);
        }
        self.rebuild_snapshot();
    }

    /// Replace every `pg_statistic` row for one relation.
    pub fn replace_statistics(&self, starelid: Oid, rows: impl IntoIterator<Item = StatisticRow>) {
        let _guard = self.write_lock.lock();
        let keys: Vec<_> = self
            .pg_statistic
            .iter()
            .filter(|entry| entry.key().0 == starelid)
            .map(|entry| *entry.key())
            .collect();
        for key in keys {
            self.pg_statistic.remove(&key);
        }
        for row in rows {
            self.pg_statistic.insert((row.starelid, row.staattnum), row);
        }
        self.rebuild_snapshot();
    }

    /// Remove every extended-statistics row attached to one relation.
    pub fn remove_statistic_ext_for_relation(&self, stxrelid: Oid) -> usize {
        let _guard = self.write_lock.lock();
        let keys: Vec<_> = self
            .pg_statistic_ext
            .iter()
            .filter(|entry| entry.value().stxrelid == stxrelid)
            .map(|entry| *entry.key())
            .collect();
        let removed = keys.len();
        for key in keys {
            self.pg_statistic_ext.remove(&key);
        }
        if removed != 0 {
            self.rebuild_snapshot();
        }
        removed
    }

    /// Insert one `pg_statistic_ext` row and publish a new snapshot.
    pub fn create_statistic_ext(&self, row: StatisticExtRow) -> Result<(), CatalogError> {
        let _guard = self.write_lock.lock();
        if self.pg_statistic_ext.contains_key(&row.oid) {
            return Err(CatalogError::already_exists(format!(
                "oid {}",
                row.oid.raw()
            )));
        }
        if self
            .pg_statistic_ext
            .iter()
            .any(|entry| entry.value().stxname.eq_ignore_ascii_case(&row.stxname))
        {
            return Err(CatalogError::already_exists(row.stxname));
        }
        self.pg_statistic_ext.insert(row.oid, row);
        self.rebuild_snapshot();
        Ok(())
    }

    /// Refresh user object schema names after runtime schema metadata loads.
    ///
    /// Heap bootstrap runs before the server has loaded runtime schema
    /// sidecars, so custom namespace OIDs cannot be named on the first pass.
    /// This method translates those OIDs back into schema names and publishes
    /// a fresh catalog snapshot before planning resumes.
    pub fn refresh_runtime_schema_names(
        &self,
        namespace_names: &std::collections::HashMap<Oid, String>,
    ) {
        if namespace_names.is_empty() {
            return;
        }
        let _guard = self.write_lock.lock();
        for mut item in self.tables_by_oid.iter_mut() {
            if let Some(class_row) = self.pg_class.get(&item.oid)
                && let Some(schema_name) = namespace_names.get(&class_row.relnamespace)
            {
                item.schema_name = schema_name.clone();
            }
        }
        let table_entries: Vec<TableEntry> = self
            .tables_by_oid
            .iter()
            .map(|item| item.value().clone())
            .collect();
        self.tables_by_name.clear();
        for entry in table_entries {
            self.tables_by_name.insert(table_entry_key(&entry), entry);
        }
        let mut index_entries = self
            .indexes_by_table
            .iter()
            .flat_map(|item| item.value().clone())
            .collect::<Vec<_>>();
        for entry in &mut index_entries {
            if let Some(class_row) = self.pg_class.get(&entry.oid)
                && let Some(schema_name) = namespace_names.get(&class_row.relnamespace)
            {
                entry.schema_name = schema_name.clone();
            }
        }
        self.indexes_by_name.clear();
        self.indexes_by_table.clear();
        for entry in index_entries {
            self.indexes_by_name
                .insert(index_entry_key(&entry), entry.clone());
            self.indexes_by_table
                .entry(entry.table_oid)
                .or_default()
                .push(entry);
        }
        for mut item in self.enum_types_by_oid.iter_mut() {
            if let Some(type_row) = self.pg_type.get(&item.oid)
                && let Some(schema_name) = namespace_names.get(&type_row.typnamespace)
            {
                item.schema_name = schema_name.clone();
            }
        }
        for mut item in self.composite_types_by_oid.iter_mut() {
            if let Some(type_row) = self.pg_type.get(&item.oid)
                && let Some(schema_name) = namespace_names.get(&type_row.typnamespace)
            {
                item.schema_name = schema_name.clone();
            }
        }
        for mut item in self.domain_types_by_oid.iter_mut() {
            if let Some(type_row) = self.pg_type.get(&item.oid)
                && let Some(schema_name) = namespace_names.get(&type_row.typnamespace)
            {
                item.schema_name = schema_name.clone();
            }
        }
        let enum_entries = self
            .enum_types_by_oid
            .iter()
            .map(|item| item.value().clone())
            .collect::<Vec<_>>();
        self.enum_types_by_name.clear();
        for entry in enum_entries {
            self.enum_types_by_name
                .insert(type_entry_key(&entry), entry);
        }
        let composite_entries = self
            .composite_types_by_oid
            .iter()
            .map(|item| item.value().clone())
            .collect::<Vec<_>>();
        self.composite_types_by_name.clear();
        for entry in composite_entries {
            self.composite_types_by_name
                .insert(type_entry_key(&entry), entry);
        }
        let domain_entries = self
            .domain_types_by_oid
            .iter()
            .map(|item| item.value().clone())
            .collect::<Vec<_>>();
        self.domain_types_by_name.clear();
        for entry in domain_entries {
            self.domain_types_by_name
                .insert(type_entry_key(&entry), entry);
        }
        self.rebuild_snapshot();
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

impl Catalog for PersistentCatalog {
    fn lookup_table(&self, name: &str) -> Option<TableEntry> {
        let snap = self.snapshot.load();
        let folded = fold_name(name);
        snap.tables.get(&folded).cloned().or_else(|| {
            let public_key = table_lookup_key("public", name);
            (public_key != folded)
                .then(|| snap.tables.get(&public_key).cloned())
                .flatten()
        })
    }

    fn lookup_table_in_schema(&self, schema_name: &str, name: &str) -> Option<TableEntry> {
        let snap = self.snapshot.load();
        snap.tables
            .get(&table_lookup_key(schema_name, name))
            .cloned()
    }

    fn lookup_table_by_oid(&self, oid: Oid) -> Option<TableEntry> {
        let snap = self.snapshot.load();
        snap.tables_by_oid.get(&oid).cloned()
    }

    fn list_tables(&self) -> Vec<TableEntry> {
        let snap = self.snapshot.load();
        snap.tables.values().cloned().collect()
    }

    fn lookup_index(&self, name: &str) -> Option<IndexEntry> {
        let snap = self.snapshot.load();
        let folded = fold_name(name);
        snap.indexes.get(&folded).cloned().or_else(|| {
            let public_key = index_lookup_key("public", name);
            (public_key != folded)
                .then(|| snap.indexes.get(&public_key).cloned())
                .flatten()
        })
    }

    fn lookup_index_in_schema(&self, schema_name: &str, name: &str) -> Option<IndexEntry> {
        let snap = self.snapshot.load();
        snap.indexes
            .get(&index_lookup_key(schema_name, name))
            .cloned()
    }

    fn list_indexes_for_table(&self, table_oid: Oid) -> Vec<IndexEntry> {
        let snap = self.snapshot.load();
        snap.indexes_by_table
            .get(&table_oid)
            .cloned()
            .unwrap_or_default()
    }
}

impl MutableCatalog for PersistentCatalog {
    fn create_table(&self, entry: TableEntry) -> Result<(), CatalogError> {
        if entry.oid.is_invalid() {
            return Err(CatalogError::schema_conflict(
                "cannot register table with INVALID oid",
            ));
        }
        let key = table_entry_key(&entry);
        let _guard = self.write_lock.lock();
        if self.tables_by_name.contains_key(&key) {
            return Err(CatalogError::already_exists(entry.name));
        }
        if self.tables_by_oid.contains_key(&entry.oid) {
            return Err(CatalogError::already_exists(format!(
                "oid {}",
                entry.oid.raw()
            )));
        }
        self.tables_by_name.insert(key, entry.clone());
        self.tables_by_oid.insert(entry.oid, entry);
        self.rebuild_snapshot();
        Ok(())
    }

    fn drop_table(&self, name: &str) -> Result<(), CatalogError> {
        let key = self.table_lookup_key_for_unqualified(name);
        let _guard = self.write_lock.lock();
        let removed = self
            .tables_by_name
            .remove(&key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .1;
        self.tables_by_oid.remove(&removed.oid);
        if let Some((_, indexes)) = self.indexes_by_table.remove(&removed.oid) {
            for idx in indexes {
                self.indexes_by_name.remove(&index_entry_key(&idx));
            }
        }
        self.remove_constraints_for_table(removed.oid);
        self.rebuild_snapshot();
        Ok(())
    }

    fn create_index(&self, entry: IndexEntry) -> Result<(), CatalogError> {
        if entry.oid.is_invalid() {
            return Err(CatalogError::schema_conflict(
                "cannot register index with INVALID oid",
            ));
        }
        let _guard = self.write_lock.lock();
        let parent = self
            .tables_by_oid
            .get(&entry.table_oid)
            .ok_or_else(|| {
                CatalogError::schema_conflict(format!(
                    "index '{}' references unknown table oid {}",
                    entry.name,
                    entry.table_oid.raw()
                ))
            })?
            .value()
            .clone();
        if !entry.schema_name.eq_ignore_ascii_case(&parent.schema_name) {
            return Err(CatalogError::schema_conflict(format!(
                "index '{}' schema '{}' does not match table '{}' schema '{}'",
                entry.name, entry.schema_name, parent.name, parent.schema_name
            )));
        }
        let key = index_entry_key(&entry);
        if self.indexes_by_name.contains_key(&key) {
            return Err(CatalogError::already_exists(entry.name));
        }
        self.indexes_by_name.insert(key, entry.clone());
        self.indexes_by_table
            .entry(entry.table_oid)
            .or_default()
            .push(entry);
        self.rebuild_snapshot();
        Ok(())
    }

    fn drop_index(&self, name: &str) -> Result<(), CatalogError> {
        let key = self.index_lookup_key_for_unqualified(name);
        let _guard = self.write_lock.lock();
        let removed = self
            .indexes_by_name
            .remove(&key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .1;
        if let Some(mut list) = self.indexes_by_table.get_mut(&removed.table_oid) {
            list.retain(|i| i.oid != removed.oid);
        }
        self.rebuild_snapshot();
        Ok(())
    }

    fn update_table_size(&self, oid: Oid, n_blocks: u32) -> Result<(), CatalogError> {
        let _guard = self.write_lock.lock();
        let folded = {
            let mut entry = self
                .tables_by_oid
                .get_mut(&oid)
                .ok_or_else(|| CatalogError::not_found(format!("oid {}", oid.raw())))?;
            entry.n_blocks = n_blocks;
            table_entry_key(&entry)
        };
        if let Some(mut by_name) = self.tables_by_name.get_mut(&folded) {
            by_name.n_blocks = n_blocks;
        }
        self.rebuild_snapshot();
        Ok(())
    }

    fn alter_table_add_column(
        &self,
        name: &str,
        column: Field,
    ) -> Result<TableEntry, CatalogError> {
        let key = self.table_lookup_key_for_unqualified(name);
        let _guard = self.write_lock.lock();
        // Snapshot the existing entry under the write lock so the
        // schema rebuild observes a stable input even when concurrent
        // readers race a snapshot acquisition.
        let existing = self
            .tables_by_name
            .get(&key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .value()
            .clone();
        let mut fields: Vec<Field> = existing.schema.fields().to_vec();
        fields.push(column);
        let new_schema = Schema::new(fields)
            .map_err(|e| CatalogError::schema_conflict(format!("ALTER TABLE ADD COLUMN: {e}")))?;
        let mut updated = existing.clone();
        updated.schema = new_schema;
        if let Some(mut entry) = self.tables_by_name.get_mut(&key) {
            *entry = updated.clone();
        }
        if let Some(mut entry) = self.tables_by_oid.get_mut(&existing.oid) {
            *entry = updated.clone();
        }
        self.rebuild_snapshot();
        Ok(updated)
    }

    fn alter_table_replace_schema(
        &self,
        name: &str,
        new_schema: Schema,
    ) -> Result<TableEntry, CatalogError> {
        let key = self.table_lookup_key_for_unqualified(name);
        let _guard = self.write_lock.lock();
        let existing = self
            .tables_by_name
            .get(&key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .value()
            .clone();
        let mut updated = existing.clone();
        updated.schema = new_schema;
        if let Some(mut entry) = self.tables_by_name.get_mut(&key) {
            *entry = updated.clone();
        }
        if let Some(mut entry) = self.tables_by_oid.get_mut(&existing.oid) {
            *entry = updated.clone();
        }
        self.rebuild_snapshot();
        Ok(updated)
    }

    fn alter_table_options(
        &self,
        name: &str,
        options: Vec<(String, String)>,
    ) -> Result<TableEntry, CatalogError> {
        let key = self.table_lookup_key_for_unqualified(name);
        let _guard = self.write_lock.lock();
        let existing = self
            .tables_by_name
            .get(&key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .value()
            .clone();
        let mut updated = existing.clone();
        updated.options = options;
        if let Some(mut entry) = self.tables_by_name.get_mut(&key) {
            *entry = updated.clone();
        }
        if let Some(mut entry) = self.tables_by_oid.get_mut(&existing.oid) {
            *entry = updated.clone();
        }
        self.rebuild_snapshot();
        Ok(updated)
    }

    fn alter_table_rename(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<TableEntry, CatalogError> {
        let old_key = self.table_lookup_key_for_unqualified(old_name);
        let _guard = self.write_lock.lock();
        let existing = self
            .tables_by_name
            .get(&old_key)
            .ok_or_else(|| CatalogError::not_found(old_name.to_owned()))?
            .value()
            .clone();
        let new_key = table_lookup_key(&existing.schema_name, new_name);
        if self.tables_by_name.contains_key(&new_key) {
            return Err(CatalogError::already_exists(new_name.to_owned()));
        }
        let existing = self
            .tables_by_name
            .remove(&old_key)
            .ok_or_else(|| CatalogError::not_found(old_name.to_owned()))?
            .1;
        let mut updated = existing.clone();
        updated.name = new_name.to_string();
        self.tables_by_name.insert(new_key, updated.clone());
        if let Some(mut entry) = self.tables_by_oid.get_mut(&existing.oid) {
            *entry = updated.clone();
        }
        self.rebuild_snapshot();
        Ok(updated)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ultrasql_core::{BlockNumber, DataType, Field, Lsn, Oid, Schema};

    use super::*;
    use crate::entry::{CompositeTypeEntry, IndexEntry, TableEntry};
    use crate::traits::{Catalog, MutableCatalog};

    fn sample_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int64),
            Field::nullable("name", DataType::Text { max_len: None }),
        ])
        .expect("schema invariants hold")
    }

    fn make_table(cat: &PersistentCatalog, name: &str) -> TableEntry {
        TableEntry {
            oid: cat.next_oid(),
            name: name.to_owned(),
            schema_name: "public".to_owned(),
            schema: sample_schema(),
            created_at_lsn: Lsn::ZERO,
            n_blocks: 0,
            root_block: BlockNumber::INVALID,
            options: Vec::new(),
        }
    }

    // --- Create / lookup round-trip via snapshot ---

    #[test]
    fn create_and_lookup_via_snapshot() {
        let cat = PersistentCatalog::new();
        let entry = make_table(&cat, "orders");
        let oid = entry.oid;
        cat.create_table(entry.clone()).expect("create");

        let snap = cat.snapshot();
        assert!(snap.tables.contains_key("orders"));
        assert_eq!(snap.tables_by_oid[&oid], entry);
    }

    #[test]
    fn drop_removes_from_snapshot() {
        let cat = PersistentCatalog::new();
        cat.create_table(make_table(&cat, "users")).expect("create");
        cat.drop_table("users").expect("drop");
        let snap = cat.snapshot();
        assert!(!snap.tables.contains_key("users"));
    }

    // --- Catalog trait delegation ---

    #[test]
    fn catalog_trait_lookup_table_by_name() {
        let cat = PersistentCatalog::new();
        let entry = make_table(&cat, "products");
        let oid = entry.oid;
        cat.create_table(entry).expect("create");
        assert!(cat.lookup_table("products").is_some());
        assert!(cat.lookup_table_by_oid(oid).is_some());
    }

    #[test]
    fn catalog_trait_list_tables() {
        let cat = PersistentCatalog::new();
        cat.create_table(make_table(&cat, "a")).expect("a");
        cat.create_table(make_table(&cat, "b")).expect("b");
        assert_eq!(cat.list_tables().len(), 2);
    }

    // --- Index management ---

    #[test]
    fn index_create_and_list() {
        let cat = PersistentCatalog::new();
        let tbl = make_table(&cat, "items");
        let toid = tbl.oid;
        cat.create_table(tbl).expect("create");
        let idx = IndexEntry::new(cat.next_oid(), "items_pk", toid, vec![0], true);
        cat.create_index(idx).expect("idx create");
        let snap = cat.snapshot();
        assert!(snap.indexes.contains_key("items_pk"));
        assert!(!snap.indexes_by_table[&toid].is_empty());
    }

    // --- pg_class insert ---

    #[test]
    fn pg_class_row_can_be_inserted() {
        let cat = PersistentCatalog::new();
        let oid = cat.next_oid();
        cat.pg_class.insert(
            oid,
            ClassRow {
                oid,
                relname: "widgets".into(),
                relnamespace: Oid::new(2200),
                relkind: RelKind::Table,
                relpages: 0,
                reltuples: 0.0,
                relfilenode: 0,
                relhasindex: false,
                reloptions: Vec::new(),
            },
        );
        assert!(cat.pg_class.contains_key(&oid));
        assert_eq!(cat.pg_class.get(&oid).unwrap().relname, "widgets");
    }

    // --- Update table size ---

    #[test]
    fn update_table_size_reflects_in_snapshot() {
        let cat = PersistentCatalog::new();
        let entry = make_table(&cat, "logs");
        let oid = entry.oid;
        cat.create_table(entry).expect("create");
        cat.update_table_size(oid, 42).expect("update");
        let snap = cat.snapshot();
        assert_eq!(snap.tables_by_oid[&oid].n_blocks, 42);
    }

    #[test]
    fn attnum_overflow_returns_catalog_error() {
        let overflowing_index =
            usize::try_from(i64::from(i16::MAX)).expect("usize stores i16::MAX");
        let err =
            attnum_for_index(overflowing_index, "composite type c").expect_err("attnum overflow");
        assert!(
            matches!(err, CatalogError::SchemaConflict(message) if message.contains("too many attributes"))
        );
    }

    #[test]
    fn install_snapshot_attnum_overflow_preserves_existing_snapshot() {
        let cat = PersistentCatalog::new();
        let heap = blank_heap();
        cat.bootstrap_from_heap(&heap).expect("bootstrap");
        let before = cat.snapshot();
        let mut snap = (*before).clone();
        let field_count =
            usize::try_from(i64::from(i16::MAX) + 1).expect("usize stores overflow field count");
        let fields = (0..field_count)
            .map(|idx| Field::required(format!("c{idx}"), DataType::Int32))
            .collect::<Vec<_>>();
        let schema = Schema::new(fields).expect("many unique fields");
        let entry = CompositeTypeEntry {
            oid: cat.next_oid(),
            name: "too_wide".to_owned(),
            schema_name: "public".to_owned(),
            schema,
        };
        snap.composite_types
            .insert(type_entry_key(&entry), entry.clone());
        snap.composite_types_by_oid.insert(entry.oid, entry.clone());

        let err = cat
            .install_snapshot(snap)
            .expect_err("attnum overflow rejects snapshot");
        assert!(
            matches!(err, CatalogError::SchemaConflict(message) if message.contains("too many attributes"))
        );
        let after = cat.snapshot();
        assert_eq!(after.tables.len(), before.tables.len());
        assert!(!after.composite_types.contains_key(&type_entry_key(&entry)));
    }

    // -----------------------------------------------------------------------
    // Bootstrap tests (E)
    // -----------------------------------------------------------------------

    /// A blank-page loader: every miss returns a fresh empty heap page.
    /// Used to build a `HeapAccess` whose all relations have zero blocks.
    fn blank_heap() -> HeapAccess<impl PageLoader> {
        use std::sync::Arc;
        use ultrasql_core::PageId;
        use ultrasql_storage::buffer_pool::BufferPool;
        use ultrasql_storage::page::Page;
        let pool = Arc::new(BufferPool::new(16, |_: PageId| Ok(Page::new_heap())));
        HeapAccess::new(pool)
    }

    /// `bootstrap_from_heap` on a fresh database (empty heap) installs the
    /// initial snapshot that contains the 13 system relations.
    #[test]
    fn bootstrap_from_empty_heap_installs_initial_snapshot() {
        let cat = PersistentCatalog::new();
        let heap = blank_heap();
        let stats = cat
            .bootstrap_from_heap(&heap)
            .expect("bootstrap must not fail on empty heap");

        // Stats reflect the initial snapshot counts.
        assert_eq!(stats.namespaces, 3);
        assert_eq!(stats.relations, 13);

        // The snapshot contains all 13 system relations.
        let snap = cat.snapshot();
        assert_eq!(snap.tables.len(), 13);
        assert!(snap.tables.contains_key("pg_class"));
        assert!(snap.tables.contains_key("pg_attribute"));
        assert!(snap.tables.contains_key("pg_attrdef"));
        assert!(snap.tables.contains_key("pg_type"));
        assert!(snap.tables.contains_key("pg_enum"));
        assert!(snap.tables.contains_key("pg_namespace"));
    }

    /// `snapshot()` returns an `Arc<CatalogSnapshot>` via `arc_swap` `load_full`
    /// — a wait-free operation. We verify the Arc is stable across a
    /// concurrent write.
    #[test]
    fn snapshot_returns_wait_free_arc_load() {
        let cat = PersistentCatalog::new();
        let heap = blank_heap();
        cat.bootstrap_from_heap(&heap).expect("bootstrap");

        // Capture snapshot before any mutation.
        let snap_before = cat.snapshot();
        assert_eq!(snap_before.tables.len(), 13);

        // Add a table — this swaps in a new snapshot.
        cat.create_table(make_table(&cat, "user_orders"))
            .expect("create");

        // The old snapshot reference is still valid and unchanged.
        assert_eq!(snap_before.tables.len(), 13);

        // A fresh snapshot call reflects the new state.
        let snap_after = cat.snapshot();
        assert_eq!(snap_after.tables.len(), 14);
    }

    /// N threads each take a snapshot concurrently; all must see the same
    /// data and none must deadlock or panic.
    #[test]
    fn multiple_concurrent_snapshots_consistent() {
        use std::thread;
        const THREADS: usize = 16;

        let cat = std::sync::Arc::new(PersistentCatalog::new());
        let heap = blank_heap();
        cat.bootstrap_from_heap(&heap).expect("bootstrap");

        let counts: Vec<usize> = (0..THREADS)
            .map(|_| {
                let cat = std::sync::Arc::clone(&cat);
                thread::spawn(move || {
                    let snap = cat.snapshot();
                    snap.tables.len()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().expect("thread panicked"))
            .collect();

        // Every thread must see the same count.
        let first = counts[0];
        assert!(counts.iter().all(|&c| c == first));
        assert_eq!(first, 13);
    }

    /// After installing a new snapshot via `install_snapshot`, the very next
    /// `snapshot()` call must return the new state.
    #[test]
    fn install_snapshot_after_ddl_is_observable_on_next_snapshot() {
        let cat = PersistentCatalog::new();
        let heap = blank_heap();
        cat.bootstrap_from_heap(&heap).expect("bootstrap");

        // Snapshot A: 13 system tables.
        let snap_a = cat.snapshot();
        assert_eq!(snap_a.tables.len(), 13);

        // Build a richer snapshot with an additional table.
        let mut tables = snap_a.tables.clone();
        let mut tables_by_oid = snap_a.tables_by_oid.clone();
        let entry = make_table(&cat, "extra_table");
        tables.insert("extra_table".to_owned(), entry.clone());
        tables_by_oid.insert(entry.oid, entry);
        let snap_b = CatalogSnapshot {
            tables,
            tables_by_oid,
            indexes: snap_a.indexes.clone(),
            indexes_by_table: snap_a.indexes_by_table.clone(),
            enum_types: snap_a.enum_types.clone(),
            enum_types_by_oid: snap_a.enum_types_by_oid.clone(),
            composite_types: snap_a.composite_types.clone(),
            composite_types_by_oid: snap_a.composite_types_by_oid.clone(),
            domain_types: snap_a.domain_types.clone(),
            domain_types_by_oid: snap_a.domain_types_by_oid.clone(),
            constraints: snap_a.constraints.clone(),
            descriptions: snap_a.descriptions.clone(),
            statistics: snap_a.statistics.clone(),
            statistic_ext: snap_a.statistic_ext.clone(),
        };
        cat.install_snapshot(snap_b).expect("install snapshot");

        // Snapshot B must be visible immediately.
        let snap_after = cat.snapshot();
        assert_eq!(snap_after.tables.len(), 14);
        assert!(snap_after.tables.contains_key("extra_table"));
    }

    #[test]
    fn set_description_updates_snapshot_and_clear_removes_rows() {
        let cat = PersistentCatalog::new();
        let heap = blank_heap();
        cat.bootstrap_from_heap(&heap).expect("bootstrap");

        let objoid = Oid::new(42_000);
        let classoid = Oid::new(crate::bootstrap::PG_CLASS_OID);
        cat.set_description(objoid, classoid, 0, Some("table docs".to_owned()));
        let snap = cat.snapshot();
        let row = snap
            .descriptions
            .get(&(objoid, classoid, 0))
            .expect("description row present");
        assert_eq!(row.description, "table docs");

        cat.set_description(objoid, classoid, 1, Some("column docs".to_owned()));
        assert_eq!(cat.snapshot().descriptions.len(), 2);

        cat.clear_descriptions_for_object(objoid);
        assert!(cat.snapshot().descriptions.is_empty());
    }

    #[test]
    fn statistics_updates_publish_snapshot_rows() {
        let cat = PersistentCatalog::new();
        let table_oid = Oid::new(42_001);
        cat.replace_statistics(
            table_oid,
            [
                StatisticRow {
                    starelid: table_oid,
                    staattnum: 1,
                    stanullfrac: 0.25,
                    stadistinct: -0.75,
                },
                StatisticRow {
                    starelid: table_oid,
                    staattnum: 2,
                    stanullfrac: 0.0,
                    stadistinct: 3.0,
                },
            ],
        );
        assert_eq!(cat.snapshot().statistics.len(), 2);
        cat.replace_statistics(
            table_oid,
            [StatisticRow {
                starelid: table_oid,
                staattnum: 1,
                stanullfrac: 0.0,
                stadistinct: 1.0,
            }],
        );
        let snap = cat.snapshot();
        assert_eq!(snap.statistics.len(), 1);
        assert_eq!(
            snap.statistics
                .get(&(table_oid, 1))
                .expect("stat row")
                .stadistinct,
            1.0
        );
    }

    #[test]
    fn statistic_ext_create_publishes_snapshot_row() {
        let cat = PersistentCatalog::new();
        let oid = Oid::new(42_002);
        cat.create_statistic_ext(StatisticExtRow {
            oid,
            stxname: "s_ab".to_owned(),
            stxrelid: Oid::new(42_001),
            stxkeys: vec![1, 2],
            stxkind: vec!['d', 'f', 'm'],
        })
        .expect("create statistic ext");
        let snap = cat.snapshot();
        let row = snap.statistic_ext.get(&oid).expect("statistic ext row");
        assert_eq!(row.stxname, "s_ab");
        assert_eq!(row.stxkeys, vec![1, 2]);
    }

    #[test]
    fn statistic_ext_remove_by_relation_updates_snapshot() {
        let cat = PersistentCatalog::new();
        let table_oid = Oid::new(42_001);
        let keep_oid = Oid::new(42_099);
        for (oid, name, stxrelid) in [
            (Oid::new(42_002), "s_ab", table_oid),
            (Oid::new(42_003), "s_bc", table_oid),
            (keep_oid, "s_keep", Oid::new(42_098)),
        ] {
            cat.create_statistic_ext(StatisticExtRow {
                oid,
                stxname: name.to_owned(),
                stxrelid,
                stxkeys: vec![1, 2],
                stxkind: vec!['d'],
            })
            .expect("create statistic ext");
        }

        assert_eq!(cat.remove_statistic_ext_for_relation(table_oid), 2);
        let snap = cat.snapshot();
        assert_eq!(snap.statistic_ext.len(), 1);
        assert!(snap.statistic_ext.contains_key(&keep_oid));
        assert_eq!(cat.remove_statistic_ext_for_relation(table_oid), 0);
    }

    /// `alter_table_add_column` on the persistent catalog extends the
    /// schema, preserves the OID, and the new entry is reflected in the
    /// next snapshot taken via `ArcSwap`.
    #[test]
    fn alter_table_add_column_persistent_updates_snapshot() {
        use ultrasql_core::{DataType, Field};

        let cat = PersistentCatalog::new();
        let entry = make_table(&cat, "items");
        let oid = entry.oid;
        cat.create_table(entry).expect("create");

        let new_col = Field::nullable("note", DataType::Text { max_len: None });
        let updated = cat
            .alter_table_add_column("items", new_col.clone())
            .expect("ALTER ADD COLUMN");
        assert_eq!(updated.oid, oid);
        assert_eq!(updated.schema.len(), 3);
        assert_eq!(updated.schema.field_at(2), &new_col);

        // Fresh snapshot reflects the wider schema.
        let snap = cat.snapshot();
        let snap_entry = snap.tables.get("items").expect("present");
        assert_eq!(snap_entry.schema.len(), 3);
        assert_eq!(snap_entry.oid, oid);
    }

    /// Round-trip a user-defined table through `persist_table_rows`
    /// → `bootstrap_from_heap`. The relation must survive the round-
    /// trip with its full schema (column names, types, nullability).
    #[test]
    fn bootstrap_round_trip_preserves_known_relation() {
        use std::sync::Arc;
        use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
        use ultrasql_storage::buffer_pool::BufferPool;
        use ultrasql_storage::heap::HeapAccess;
        use ultrasql_storage::page::Page;

        let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
        let heap = HeapAccess::new(pool);

        // Build a representative user table and persist its rows.
        let cat = PersistentCatalog::new();
        let oid = cat.next_oid();
        let entry = TableEntry::new(
            oid,
            "orders".to_owned(),
            "public".to_owned(),
            Schema::new(vec![
                Field {
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                Field {
                    name: "amount".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
            ])
            .expect("schema"),
        );
        cat.create_table(entry.clone()).expect("create_table");
        cat.persist_table_rows(&entry, &heap, Xid::new(1), CommandId::new(0))
            .expect("persist_table_rows");

        // Reset to a clean catalog and bootstrap from the heap pages
        // that the previous step wrote.
        let cat2 = PersistentCatalog::new();
        let stats = cat2
            .bootstrap_from_heap(&heap)
            .expect("bootstrap must succeed");
        // Initial system relations plus the one user table.
        assert!(stats.relations >= 11);
        assert_eq!(stats.attributes, 2);

        let snap = cat2.snapshot();
        let restored = snap
            .tables
            .get("orders")
            .expect("user relation present after bootstrap");
        assert_eq!(restored.oid, oid);
        assert_eq!(restored.schema.fields().len(), 2);
        assert_eq!(restored.schema.fields()[0].name, "id");
        assert_eq!(restored.schema.fields()[0].data_type, DataType::Int32);
        assert!(!restored.schema.fields()[0].nullable);
        assert_eq!(restored.schema.fields()[1].name, "amount");
        assert_eq!(restored.schema.fields()[1].data_type, DataType::Int64);
        assert!(restored.schema.fields()[1].nullable);
    }

    #[test]
    fn bootstrap_rejects_max_oid_relation_without_successor() {
        use ultrasql_core::{CommandId, Xid};

        let heap = blank_heap();
        let cat = PersistentCatalog::new();
        let entry = TableEntry::new(
            Oid::new(u32::MAX),
            "max_oid_table".to_owned(),
            "public".to_owned(),
            sample_schema(),
        );
        cat.create_table(entry.clone()).expect("create table");
        cat.persist_table_rows(&entry, &heap, Xid::new(1), CommandId::new(0))
            .expect("persist table rows");

        let cat2 = PersistentCatalog::new();
        let err = cat2
            .bootstrap_from_heap(&heap)
            .expect_err("max oid row should reject restart");

        assert!(
            matches!(err, CatalogError::SchemaConflict(message) if message.contains("catalog OID space exhausted"))
        );
    }

    #[test]
    fn bootstrap_round_trip_preserves_atthasdef_metadata() {
        use std::sync::Arc;
        use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
        use ultrasql_storage::buffer_pool::BufferPool;
        use ultrasql_storage::heap::HeapAccess;
        use ultrasql_storage::page::Page;

        let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
        let heap = HeapAccess::new(pool);
        let cat = PersistentCatalog::new();
        let oid = cat.next_oid();
        let entry = TableEntry::new(
            oid,
            "defaults_demo".to_owned(),
            "public".to_owned(),
            Schema::new(vec![
                Field::required("id", DataType::Int64),
                Field::nullable("note", DataType::Text { max_len: None }),
            ])
            .expect("schema"),
        );

        cat.persist_table_rows_with_defaults(
            &entry,
            &[true, false],
            &heap,
            Xid::new(1),
            CommandId::new(0),
        )
        .expect("persist table rows with defaults");

        let cat2 = PersistentCatalog::new();
        cat2.bootstrap_from_heap(&heap).expect("bootstrap");

        assert!(cat2.pg_attribute.get(&(oid, 1)).expect("id attr").atthasdef);
        assert!(
            !cat2
                .pg_attribute
                .get(&(oid, 2))
                .expect("note attr")
                .atthasdef
        );
    }

    #[test]
    fn bootstrap_round_trip_preserves_index_entry() {
        use std::sync::Arc;
        use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
        use ultrasql_storage::buffer_pool::BufferPool;
        use ultrasql_storage::heap::HeapAccess;
        use ultrasql_storage::page::Page;

        let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
        let heap = HeapAccess::new(pool);

        let cat = PersistentCatalog::new();
        let table_oid = cat.next_oid();
        let table = TableEntry::new(
            table_oid,
            "orders".to_owned(),
            "public".to_owned(),
            Schema::new(vec![
                Field::required("id", DataType::Int64),
                Field::nullable("note", DataType::Text { max_len: None }),
            ])
            .expect("schema"),
        );
        cat.persist_table_rows(&table, &heap, Xid::new(1), CommandId::new(0))
            .expect("persist table");

        let mut index = IndexEntry::new(cat.next_oid(), "orders_id_idx", table_oid, vec![0], false);
        index.root_block = BlockNumber::new(7);
        cat.persist_index_rows(&index, &heap, Xid::new(2), CommandId::new(0))
            .expect("persist index");

        let cat2 = PersistentCatalog::new();
        let stats = cat2.bootstrap_from_heap(&heap).expect("bootstrap");
        assert_eq!(stats.indexes, 1);

        let snap = cat2.snapshot();
        let restored = snap.indexes.get("orders_id_idx").expect("index restored");
        assert_eq!(restored.oid, index.oid);
        assert_eq!(restored.table_oid, table_oid);
        assert_eq!(restored.columns, vec![0]);
        assert_eq!(restored.root_block, BlockNumber::new(7));
        assert!(!restored.is_unique);
        assert_eq!(snap.indexes_by_table[&table_oid], vec![restored.clone()]);
    }

    #[test]
    fn bootstrap_round_trip_preserves_index_method_opclass_and_options() {
        use std::sync::Arc;
        use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
        use ultrasql_storage::buffer_pool::BufferPool;
        use ultrasql_storage::heap::HeapAccess;
        use ultrasql_storage::page::Page;

        let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
        let heap = HeapAccess::new(pool);

        let cat = PersistentCatalog::new();
        let table_oid = cat.next_oid();
        let table = TableEntry::new(
            table_oid,
            "embeddings".to_owned(),
            "public".to_owned(),
            Schema::new(vec![
                Field::required("id", DataType::Int64),
                Field::required("embedding", DataType::Vector { dims: Some(3) }),
            ])
            .expect("schema"),
        );
        cat.persist_table_rows(&table, &heap, Xid::new(1), CommandId::new(0))
            .expect("persist table");

        let mut index = IndexEntry::new(
            cat.next_oid(),
            "embeddings_hnsw_idx",
            table_oid,
            vec![1],
            false,
        );
        index.access_method = "hnsw".to_owned();
        index.opclasses = vec![Some("vector_l2_ops".to_owned())];
        index.options = vec![
            ("m".to_owned(), "16".to_owned()),
            ("ef_search".to_owned(), "64".to_owned()),
        ];
        cat.persist_index_rows(&index, &heap, Xid::new(2), CommandId::new(0))
            .expect("persist index");

        let cat2 = PersistentCatalog::new();
        cat2.bootstrap_from_heap(&heap).expect("bootstrap");

        let snap = cat2.snapshot();
        let restored = snap
            .indexes
            .get("embeddings_hnsw_idx")
            .expect("index restored");
        assert_eq!(restored.access_method, "hnsw");
        assert_eq!(restored.opclasses, vec![Some("vector_l2_ops".to_owned())]);
        assert_eq!(
            restored.options,
            vec![
                ("m".to_owned(), "16".to_owned()),
                ("ef_search".to_owned(), "64".to_owned()),
            ]
        );
    }

    #[test]
    fn bootstrap_round_trip_preserves_pg_statistic_rows() {
        use std::sync::Arc;
        use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
        use ultrasql_storage::buffer_pool::BufferPool;
        use ultrasql_storage::heap::HeapAccess;
        use ultrasql_storage::page::Page;

        let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
        let heap = HeapAccess::new(pool);

        let cat = PersistentCatalog::new();
        let oid = cat.next_oid();
        let entry = TableEntry::new(
            oid,
            "orders".to_owned(),
            "public".to_owned(),
            Schema::new(vec![
                Field::required("id", DataType::Int32),
                Field::nullable("note", DataType::Text { max_len: None }),
            ])
            .expect("schema"),
        );
        cat.persist_table_rows(&entry, &heap, Xid::new(1), CommandId::new(0))
            .expect("persist table");
        cat.persist_statistic_rows(
            &[
                StatisticRow {
                    starelid: oid,
                    staattnum: 1,
                    stanullfrac: 0.5,
                    stadistinct: -0.25,
                },
                StatisticRow {
                    starelid: oid,
                    staattnum: 1,
                    stanullfrac: 0.0,
                    stadistinct: 10.0,
                },
                StatisticRow {
                    starelid: oid,
                    staattnum: 2,
                    stanullfrac: 0.75,
                    stadistinct: 2.0,
                },
                StatisticRow {
                    starelid: Oid::new(999_999),
                    staattnum: 1,
                    stanullfrac: 0.0,
                    stadistinct: 1.0,
                },
            ],
            &heap,
            Xid::new(2),
            CommandId::new(0),
        )
        .expect("persist statistics");

        let cat2 = PersistentCatalog::new();
        let stats = cat2.bootstrap_from_heap(&heap).expect("bootstrap");
        assert_eq!(stats.statistics, 2);

        let snap = cat2.snapshot();
        assert_eq!(snap.statistics.len(), 2);
        assert_eq!(
            snap.statistics
                .get(&(oid, 1))
                .expect("latest att1 row")
                .stadistinct,
            10.0
        );
        assert_eq!(
            snap.statistics
                .get(&(oid, 2))
                .expect("att2 row")
                .stanullfrac,
            0.75
        );
    }

    #[test]
    fn bootstrap_round_trip_preserves_pg_statistic_ext_rows() {
        use std::sync::Arc;
        use ultrasql_core::{CommandId, DataType, Field, PageId, Schema, Xid};
        use ultrasql_storage::buffer_pool::BufferPool;
        use ultrasql_storage::heap::HeapAccess;
        use ultrasql_storage::page::Page;

        let pool = Arc::new(BufferPool::new(64, |_: PageId| Ok(Page::new_heap())));
        let heap = HeapAccess::new(pool);

        let cat = PersistentCatalog::new();
        let table_oid = cat.next_oid();
        let entry = TableEntry::new(
            table_oid,
            "orders".to_owned(),
            "public".to_owned(),
            Schema::new(vec![
                Field::required("id", DataType::Int32),
                Field::required("region", DataType::Int32),
            ])
            .expect("schema"),
        );
        cat.persist_table_rows(&entry, &heap, Xid::new(1), CommandId::new(0))
            .expect("persist table");

        let row = StatisticExtRow {
            oid: cat.next_oid(),
            stxname: "orders_stats".to_owned(),
            stxrelid: table_oid,
            stxkeys: vec![1, 2],
            stxkind: vec!['d', 'f', 'm'],
        };
        cat.persist_statistic_ext_row(&row, &heap, Xid::new(2), CommandId::new(0))
            .expect("persist statistic ext");

        let cat2 = PersistentCatalog::new();
        let stats = cat2.bootstrap_from_heap(&heap).expect("bootstrap");
        assert_eq!(stats.statistic_ext, 1);
        assert_eq!(
            cat2.snapshot()
                .statistic_ext
                .get(&row.oid)
                .expect("statistic ext row"),
            &row
        );
    }
}
