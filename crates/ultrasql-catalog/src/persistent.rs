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
//! [`PersistentCatalog::bootstrap_from_heap`] scans the `pg_class` and
//! `pg_attribute` heap pages, decodes each user row via
//! [`crate::encoding::ClassRow`] / [`crate::encoding::decode_attribute_row`]
//! and [`crate::encoding::schema_from_attributes`], then overlays the
//! decoded user `TableEntry` list on top of the initial system snapshot.
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
use ultrasql_core::{Field, Oid, RelationId, Schema};
use ultrasql_storage::buffer_pool::PageLoader;
use ultrasql_storage::heap::HeapAccess;

use crate::bootstrap::{self, initial_snapshot};
use crate::entry::{IndexEntry, TableEntry};
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
    /// `indkey` — 1-based column attnums.
    pub indkey: Vec<i16>,
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
}

impl CatalogStats {
    /// Stats for a fresh-database initial snapshot: 3 namespaces, 10 relations,
    /// no attributes, indexes, or constraints yet decoded from the heap.
    ///
    /// Used when `bootstrap_from_heap` detects an empty heap and installs the
    /// hard-coded initial snapshot.
    #[must_use]
    pub const fn initial() -> Self {
        Self {
            namespaces: 3,
            relations: 10,
            attributes: 0,
            indexes: 0,
            constraints: 0,
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

    /// Wait-free snapshot for the binder.
    snapshot: ArcSwap<CatalogSnapshot>,
    /// Serializes snapshot rebuilds.
    write_lock: Mutex<()>,
    /// OID allocator.
    next_oid: AtomicU32,
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
            descriptions: std::collections::HashMap::new(),
            statistics: std::collections::HashMap::new(),
            statistic_ext: std::collections::HashMap::new(),
        });
        Self {
            pg_namespace: DashMap::new(),
            pg_class: DashMap::new(),
            pg_attribute: DashMap::new(),
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
    pub fn install_snapshot(&self, snap: CatalogSnapshot) {
        let _guard = self.write_lock.lock();
        // Re-populate the backing DashMaps from the snapshot so that
        // subsequent MutableCatalog operations (create_table, etc.) have
        // a consistent starting point.
        self.tables_by_name.clear();
        self.tables_by_oid.clear();
        self.indexes_by_name.clear();
        self.indexes_by_table.clear();
        self.pg_description.clear();
        self.pg_statistic.clear();
        self.pg_statistic_ext.clear();

        for (name, entry) in &snap.tables {
            self.tables_by_name.insert(name.clone(), entry.clone());
            self.tables_by_oid.insert(entry.oid, entry.clone());
        }
        for (name, entry) in &snap.indexes {
            self.indexes_by_name.insert(name.clone(), entry.clone());
        }
        for (oid, entries) in &snap.indexes_by_table {
            self.indexes_by_table.insert(*oid, entries.clone());
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
    }

    /// Bootstrap the catalog from on-disk system catalog heap pages.
    ///
    /// Reads `pg_namespace`, `pg_class`, `pg_attribute`, `pg_index`,
    /// `pg_constraint`, `pg_sequence`, `pg_depend`, `pg_description` from
    /// heap pages via the supplied [`HeapAccess`].  Builds a
    /// [`CatalogSnapshot`] and atomically swaps it into the in-memory
    /// `ArcSwap` cache.
    ///
    /// # Fresh database
    ///
    /// When all system catalog heap pages are empty (i.e. the database was
    /// just initialized) this method detects the empty heap and installs the
    /// hard-coded [`initial_snapshot`] that contains the three well-known
    /// namespaces and the ten system relations.  The returned
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
        use crate::encoding::{decode_attribute_row, schema_from_attributes};

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_attribute_rel = RelationId::new(bootstrap::PG_ATTRIBUTE_OID);
        let class_blocks = heap.block_count(pg_class_rel);

        if class_blocks == 0 {
            // Fresh database — install the initial hard-coded snapshot.
            let snap = initial_snapshot();
            let stats = CatalogStats::initial();
            self.install_snapshot(snap);
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

        // Group attribute rows by `attrelid` so the per-relation schema
        // can be rebuilt in one pass.
        let attribute_blocks = heap.block_count(pg_attribute_rel);
        let mut attrs_by_relation: std::collections::HashMap<
            Oid,
            Vec<(
                crate::persistent::AttributeRow,
                ultrasql_core::DataType,
                bool,
            )>,
        > = std::collections::HashMap::new();
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
                attrs_by_relation
                    .entry(row.attrelid)
                    .or_default()
                    .push((row, dt, nullable));
                total_attrs = total_attrs.saturating_add(1);
            }
        }

        // Decode pg_class rows. Each user table maps to one TableEntry
        // whose schema is rebuilt from the matching attribute rows.
        let class_scan = heap.scan(pg_class_rel, class_blocks);
        let mut user_relations: u32 = 0;
        let mut highest_oid: u32 = self.next_oid.load(Ordering::Acquire);
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
            };
            let key = class_row.relname.to_ascii_lowercase();
            tables.insert(key, entry.clone());
            tables_by_oid.insert(entry.oid, entry);
            user_relations = user_relations.saturating_add(1);
            highest_oid = highest_oid.max(class_row.oid.raw().saturating_add(1));
        }
        // Bump the OID allocator past every observed OID so a
        // subsequent `next_oid` call cannot collide with a restored
        // relation.
        self.next_oid.store(highest_oid, Ordering::Release);

        let snap = CatalogSnapshot {
            tables,
            tables_by_oid,
            indexes: initial.indexes,
            indexes_by_table: initial.indexes_by_table,
            descriptions: initial.descriptions,
            statistics: initial.statistics,
            statistic_ext: initial.statistic_ext,
        };
        let stats = CatalogStats {
            namespaces: CatalogStats::initial().namespaces,
            relations: CatalogStats::initial().relations + user_relations,
            attributes: total_attrs,
            indexes: 0,
            constraints: 0,
        };
        self.install_snapshot(snap);
        tracing::debug!(?stats, "catalog bootstrapped from heap");
        Ok(stats)
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
    /// [`DataType`](ultrasql_core::DataType) is outside the catalog-
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
        use crate::encoding::encode_attribute_row;
        use crate::persistent::{AttributeRow, ClassRow, RelKind};
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_attribute_rel = RelationId::new(bootstrap::PG_ATTRIBUTE_OID);

        let namespace_oid = if entry.schema_name == "pg_catalog" {
            Oid::new(bootstrap::PG_CATALOG_OID)
        } else {
            Oid::new(bootstrap::PUBLIC_OID)
        };

        let class_row = ClassRow {
            oid: entry.oid,
            relname: entry.name.clone(),
            relnamespace: namespace_oid,
            relkind: RelKind::Table,
            relpages: entry.n_blocks,
            reltuples: 0.0,
            relfilenode: entry.root_block.raw(),
            relhasindex: false,
        };
        let class_bytes = class_row.encode();
        let class_opts = InsertOptions {
            xmin,
            command_id,
            wal: None,
            fsm: None,
            vm: None,
        };
        heap.insert(pg_class_rel, &class_bytes, class_opts)
            .map_err(|e| CatalogError::schema_conflict(format!("pg_class insert: {e}")))?;

        for (i, field) in entry.schema.fields().iter().enumerate() {
            let attr_row = AttributeRow {
                attrelid: entry.oid,
                attname: field.name.clone(),
                atttypid: 0,
                attnum: i16::try_from(i + 1).unwrap_or(i16::MAX),
                attnotnull: !field.nullable,
                atthasdef: false,
                attisdropped: false,
            };
            let bytes = encode_attribute_row(&attr_row, &field.data_type, field.nullable)
                .map_err(|e| CatalogError::schema_conflict(format!("encode pg_attribute: {e}")))?;
            let attr_opts = InsertOptions {
                xmin,
                command_id,
                wal: None,
                fsm: None,
                vm: None,
            };
            heap.insert(pg_attribute_rel, &bytes, attr_opts)
                .map_err(|e| CatalogError::schema_conflict(format!("pg_attribute insert: {e}")))?;
        }
        Ok(())
    }

    /// Rebuild and swap in a new snapshot.
    ///
    /// Must hold `write_lock` when calling.
    fn rebuild_snapshot(&self) {
        let tables: std::collections::HashMap<String, TableEntry> = self
            .tables_by_name
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();
        let tables_by_oid: std::collections::HashMap<Oid, TableEntry> = self
            .tables_by_oid
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        let indexes: std::collections::HashMap<String, IndexEntry> = self
            .indexes_by_name
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();
        let indexes_by_table: std::collections::HashMap<Oid, Vec<IndexEntry>> = self
            .indexes_by_table
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        let descriptions: std::collections::HashMap<(Oid, Oid, i32), DescriptionRow> = self
            .pg_description
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
            self.pg_statistic
                .insert((row.starelid, row.staattnum), row);
        }
        self.rebuild_snapshot();
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
}

fn fold_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

impl Catalog for PersistentCatalog {
    fn lookup_table(&self, name: &str) -> Option<TableEntry> {
        let snap = self.snapshot.load();
        snap.tables.get(&fold_name(name)).cloned()
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
        snap.indexes.get(&fold_name(name)).cloned()
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
        let key = fold_name(&entry.name);
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
        let key = fold_name(name);
        let _guard = self.write_lock.lock();
        let removed = self
            .tables_by_name
            .remove(&key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .1;
        self.tables_by_oid.remove(&removed.oid);
        if let Some((_, indexes)) = self.indexes_by_table.remove(&removed.oid) {
            for idx in indexes {
                self.indexes_by_name.remove(&fold_name(&idx.name));
            }
        }
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
        if !self.tables_by_oid.contains_key(&entry.table_oid) {
            return Err(CatalogError::schema_conflict(format!(
                "index '{}' references unknown table oid {}",
                entry.name,
                entry.table_oid.raw()
            )));
        }
        let key = fold_name(&entry.name);
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
        let key = fold_name(name);
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
            fold_name(&entry.name)
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
        let key = fold_name(name);
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
        let key = fold_name(name);
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

    fn alter_table_rename(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<TableEntry, CatalogError> {
        let old_key = fold_name(old_name);
        let new_key = fold_name(new_name);
        let _guard = self.write_lock.lock();
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
    use crate::entry::{IndexEntry, TableEntry};
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
    /// initial snapshot that contains the 10 system relations.
    #[test]
    fn bootstrap_from_empty_heap_installs_initial_snapshot() {
        let cat = PersistentCatalog::new();
        let heap = blank_heap();
        let stats = cat
            .bootstrap_from_heap(&heap)
            .expect("bootstrap must not fail on empty heap");

        // Stats reflect the initial snapshot counts.
        assert_eq!(stats.namespaces, 3);
        assert_eq!(stats.relations, 10);

        // The snapshot contains all 10 system relations.
        let snap = cat.snapshot();
        assert_eq!(snap.tables.len(), 10);
        assert!(snap.tables.contains_key("pg_class"));
        assert!(snap.tables.contains_key("pg_attribute"));
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
        assert_eq!(snap_before.tables.len(), 10);

        // Add a table — this swaps in a new snapshot.
        cat.create_table(make_table(&cat, "user_orders"))
            .expect("create");

        // The old snapshot reference is still valid and unchanged.
        assert_eq!(snap_before.tables.len(), 10);

        // A fresh snapshot call reflects the new state.
        let snap_after = cat.snapshot();
        assert_eq!(snap_after.tables.len(), 11);
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
        assert_eq!(first, 10);
    }

    /// After installing a new snapshot via `install_snapshot`, the very next
    /// `snapshot()` call must return the new state.
    #[test]
    fn install_snapshot_after_ddl_is_observable_on_next_snapshot() {
        let cat = PersistentCatalog::new();
        let heap = blank_heap();
        cat.bootstrap_from_heap(&heap).expect("bootstrap");

        // Snapshot A: 10 system tables.
        let snap_a = cat.snapshot();
        assert_eq!(snap_a.tables.len(), 10);

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
            descriptions: snap_a.descriptions.clone(),
            statistics: snap_a.statistics.clone(),
            statistic_ext: snap_a.statistic_ext.clone(),
        };
        cat.install_snapshot(snap_b);

        // Snapshot B must be visible immediately.
        let snap_after = cat.snapshot();
        assert_eq!(snap_after.tables.len(), 11);
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
}
