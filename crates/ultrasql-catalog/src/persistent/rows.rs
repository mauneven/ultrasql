//! System catalog row types, `CatalogStats`, and the wait-free
//! `CatalogSnapshot`.
//!
//! Extracted verbatim from the original `persistent.rs`; see [`super`].

use super::*;

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
    /// Tombstone for a dropped constraint (`'D'`).
    ///
    /// The `pg_constraint` heap is append-only, so `ALTER TABLE
    /// DROP CONSTRAINT` persists a row reusing the original constraint
    /// OID with this type. Bootstrap keeps the latest row per OID and
    /// skips tombstones, suppressing the dropped constraint after
    /// restart. Not a real PostgreSQL `contype`.
    Dropped,
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

impl CatalogSnapshot {
    /// Return a clone of this snapshot with the supplied in-transaction-DDL
    /// entries overlaid as if already committed.
    ///
    /// This is the read-side primitive for transactional DDL: the issuing
    /// session resolves the in-transaction-created relation / index through a
    /// snapshot built by this method, while every other session keeps
    /// reading the unmodified committed snapshot. Keys are computed with the
    /// same `table_lookup_key` / `index_lookup_key` helpers the live
    /// DashMaps use, so a name resolved through the overlay matches the one
    /// resolved after the change commits.
    ///
    /// `tables` are the in-txn `CREATE TABLE` entries (each with its implicit
    /// constraint `indexes` and `constraints`); the slice is empty for a pure
    /// `CREATE INDEX` overlay where the target table is already committed.
    /// `extra_indexes` / `extra_index_constraints` (milestone 3) overlay an
    /// in-txn `CREATE INDEX` on an EXISTING table or one created earlier in the
    /// same transaction.
    ///
    /// The entries are inserted, never removed: the supported transactional
    /// DDL (`CREATE TABLE`, `CREATE INDEX`) is purely additive.
    #[must_use]
    pub fn with_overlay(
        &self,
        tables: &[TableEntry],
        indexes: &[IndexEntry],
        constraints: &[ConstraintRow],
        extra_indexes: &[IndexEntry],
        extra_index_constraints: &[ConstraintRow],
    ) -> Self {
        let mut snap = self.clone();
        for table in tables {
            snap.tables.insert(
                table_lookup_key(&table.schema_name, &table.name),
                table.clone(),
            );
            snap.tables_by_oid.insert(table.oid, table.clone());
        }
        for index in indexes.iter().chain(extra_indexes) {
            snap.indexes.insert(
                index_lookup_key(&index.schema_name, &index.name),
                index.clone(),
            );
            snap.indexes_by_table
                .entry(index.table_oid)
                .or_default()
                .push(index.clone());
        }
        for row in constraints.iter().chain(extra_index_constraints) {
            snap.constraints.insert(row.oid, row.clone());
        }
        snap
    }
}
