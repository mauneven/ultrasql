//! Bootstrap catalog snapshot for a fresh database.
//!
//! A fresh UltraSQL instance has no heap pages yet — the system catalog
//! tables are empty. This module produces the initial
//! [`CatalogSnapshot`] that describes the catalog's own relations so
//! the server can query the catalog about its own catalog tables, and
//! so the binder can resolve system namespaces without a heap scan.
//!
//! # Namespaces baked in
//!
//! | OID  | name               |
//! |------|--------------------|
//! | 11   | `pg_catalog`       |
//! | 12   | `information_schema` |
//! | 2200 | `public`           |
//!
//! # System relations baked in
//!
//! The thirteen core system tables that exist in every UltraSQL
//! database.  Their OIDs are well-known constants used throughout the
//! rest of the system.
//!
//! | OID  | relname             |
//! |------|---------------------|
//! | 1259 | `pg_class`          |
//! | 1249 | `pg_attribute`      |
//! | 2604 | `pg_attrdef`        |
//! | 1247 | `pg_type`           |
//! | 3501 | `pg_enum`           |
//! | 2615 | `pg_namespace`      |
//! | 2610 | `pg_index`          |
//! | 2606 | `pg_constraint`     |
//! | 1505 | `pg_sequence`       |
//! | 2608 | `pg_depend`         |
//! | 2609 | `pg_description`    |
//! | 2619 | `pg_statistic`      |
//! | 3381 | `pg_statistic_ext`  |
//!
//! The attribute lists for each relation are deliberately minimal — only
//! the columns modelled by the v0.8 row types in `persistent.rs` are
//! present. A future RFC will expand them to full PostgreSQL parity.

use std::collections::HashMap;

use ultrasql_core::{BlockNumber, DataType, Field, Lsn, Oid, Schema};

use crate::entry::TableEntry;
use crate::persistent::CatalogSnapshot;

// ---------------------------------------------------------------------------
// Well-known namespace OIDs (matches PostgreSQL)
// ---------------------------------------------------------------------------

/// OID of the `pg_catalog` namespace.
pub const PG_CATALOG_OID: u32 = 11;
/// OID of the `information_schema` namespace.
pub const INFORMATION_SCHEMA_OID: u32 = 12;
/// OID of the `public` namespace.
pub const PUBLIC_OID: u32 = 2200;

// ---------------------------------------------------------------------------
// Well-known system relation OIDs (matches PostgreSQL)
// ---------------------------------------------------------------------------

/// OID of `pg_class`.
pub const PG_CLASS_OID: u32 = 1259;
/// OID of `pg_attribute`.
pub const PG_ATTRIBUTE_OID: u32 = 1249;
/// OID of `pg_attrdef`.
pub const PG_ATTRDEF_OID: u32 = 2604;
/// OID of `pg_type`.
pub const PG_TYPE_OID: u32 = 1247;
/// OID of `pg_enum`.
pub const PG_ENUM_OID: u32 = 3501;
/// OID of `pg_namespace`.
pub const PG_NAMESPACE_OID: u32 = 2615;
/// OID of `pg_index`.
pub const PG_INDEX_OID: u32 = 2610;
/// OID of `pg_constraint`.
pub const PG_CONSTRAINT_OID: u32 = 2606;
/// OID of `pg_sequence`.
pub const PG_SEQUENCE_OID: u32 = 1505;
/// OID of `pg_depend`.
pub const PG_DEPEND_OID: u32 = 2608;
/// OID of `pg_description`.
pub const PG_DESCRIPTION_OID: u32 = 2609;
/// OID of `pg_statistic`.
pub const PG_STATISTIC_OID: u32 = 2619;
/// OID of `pg_statistic_ext`.
pub const PG_STATISTIC_EXT_OID: u32 = 3381;

// ---------------------------------------------------------------------------
// Schema builders
// ---------------------------------------------------------------------------

#[allow(
    clippy::expect_used,
    reason = "bootstrap system schemas are static field lists; duplicate-name failure is a source invariant"
)]
fn static_schema<const N: usize>(fields: [Field; N], invariant: &str) -> Schema {
    Schema::new(fields).expect(invariant)
}

/// Schema for `pg_namespace` (abridged to v0.8 column set).
fn pg_namespace_schema() -> Schema {
    static_schema(
        [
            Field::required("oid", DataType::Int64),
            Field::required("nspname", DataType::Text { max_len: None }),
            Field::required("nspowner", DataType::Int64),
        ],
        "pg_namespace schema invariants hold",
    )
}

/// Schema for `pg_class` (abridged to v0.8 column set).
fn pg_class_schema() -> Schema {
    static_schema(
        [
            Field::required("oid", DataType::Int64),
            Field::required("relname", DataType::Text { max_len: None }),
            Field::required("relnamespace", DataType::Int64),
            Field::required("relkind", DataType::Text { max_len: Some(1) }),
            Field::required("relpages", DataType::Int32),
            Field::required("reltuples", DataType::Float64),
            Field::required("relfilenode", DataType::Int32),
            Field::required("relhasindex", DataType::Bool),
        ],
        "pg_class schema invariants hold",
    )
}

/// Schema for `pg_attribute` (abridged to v0.8 column set).
fn pg_attribute_schema() -> Schema {
    static_schema(
        [
            Field::required("attrelid", DataType::Int64),
            Field::required("attname", DataType::Text { max_len: None }),
            Field::required("atttypid", DataType::Int32),
            Field::required("attnum", DataType::Int16),
            Field::required("attnotnull", DataType::Bool),
            Field::required("atthasdef", DataType::Bool),
            Field::required("attisdropped", DataType::Bool),
        ],
        "pg_attribute schema invariants hold",
    )
}

/// Schema for `pg_attrdef` (abridged to v0.9 column set).
fn pg_attrdef_schema() -> Schema {
    static_schema(
        [
            Field::required("oid", DataType::Int64),
            Field::required("adrelid", DataType::Int64),
            Field::required("adnum", DataType::Int16),
            Field::required("adbin", DataType::Text { max_len: None }),
        ],
        "pg_attrdef schema invariants hold",
    )
}

/// Schema for `pg_type` (abridged to enum-compatible v1 column set).
fn pg_type_schema() -> Schema {
    static_schema(
        [
            Field::required("oid", DataType::Int64),
            Field::required("typname", DataType::Text { max_len: None }),
            Field::required("typnamespace", DataType::Int64),
            Field::required("typtype", DataType::Text { max_len: Some(1) }),
            Field::required("typcategory", DataType::Text { max_len: Some(1) }),
            Field::required("typlen", DataType::Int16),
            Field::required("typelem", DataType::Int32),
        ],
        "pg_type schema invariants hold",
    )
}

/// Schema for `pg_enum` (abridged to enum-compatible v1 column set).
fn pg_enum_schema() -> Schema {
    static_schema(
        [
            Field::required("oid", DataType::Int64),
            Field::required("enumtypid", DataType::Int64),
            Field::required("enumsortorder", DataType::Float32),
            Field::required("enumlabel", DataType::Text { max_len: None }),
        ],
        "pg_enum schema invariants hold",
    )
}

/// Schema for `pg_index` (abridged to v0.8 column set).
fn pg_index_schema() -> Schema {
    static_schema(
        [
            Field::required("indexrelid", DataType::Int64),
            Field::required("indrelid", DataType::Int64),
            Field::required("indnatts", DataType::Int16),
            Field::required("indisunique", DataType::Bool),
            Field::required("indisprimary", DataType::Bool),
            Field::required("indisvalid", DataType::Bool),
        ],
        "pg_index schema invariants hold",
    )
}

/// Schema for `pg_constraint` (abridged to v0.8 column set).
fn pg_constraint_schema() -> Schema {
    static_schema(
        [
            Field::required("oid", DataType::Int64),
            Field::required("conname", DataType::Text { max_len: None }),
            Field::required("conrelid", DataType::Int64),
            Field::required("contype", DataType::Text { max_len: Some(1) }),
            Field::required("condeferrable", DataType::Bool),
            Field::required("condeferred", DataType::Bool),
            Field::required("confrelid", DataType::Int64),
        ],
        "pg_constraint schema invariants hold",
    )
}

/// Schema for `pg_sequence` (abridged to v0.8 column set).
fn pg_sequence_schema() -> Schema {
    static_schema(
        [
            Field::required("seqrelid", DataType::Int64),
            Field::required("seqtypid", DataType::Int32),
            Field::required("seqstart", DataType::Int64),
            Field::required("seqincrement", DataType::Int64),
            Field::required("seqmax", DataType::Int64),
            Field::required("seqmin", DataType::Int64),
            Field::required("seqcache", DataType::Int64),
            Field::required("seqcycle", DataType::Bool),
        ],
        "pg_sequence schema invariants hold",
    )
}

/// Schema for `pg_depend` (abridged to v0.8 column set).
fn pg_depend_schema() -> Schema {
    static_schema(
        [
            Field::required("classid", DataType::Int64),
            Field::required("objid", DataType::Int64),
            Field::required("refclassid", DataType::Int64),
            Field::required("refobjid", DataType::Int64),
            Field::required("deptype", DataType::Text { max_len: Some(1) }),
        ],
        "pg_depend schema invariants hold",
    )
}

/// Schema for `pg_description` (abridged to v0.8 column set).
fn pg_description_schema() -> Schema {
    static_schema(
        [
            Field::required("objoid", DataType::Int64),
            Field::required("classoid", DataType::Int64),
            Field::required("objsubid", DataType::Int32),
            Field::required("description", DataType::Text { max_len: None }),
        ],
        "pg_description schema invariants hold",
    )
}

/// Schema for `pg_statistic` (abridged to v0.8 column set).
fn pg_statistic_schema() -> Schema {
    static_schema(
        [
            Field::required("starelid", DataType::Int64),
            Field::required("staattnum", DataType::Int16),
            Field::required("stanullfrac", DataType::Float32),
            Field::required("stadistinct", DataType::Float32),
        ],
        "pg_statistic schema invariants hold",
    )
}

/// Schema for `pg_statistic_ext` (abridged to v0.8 column set).
fn pg_statistic_ext_schema() -> Schema {
    static_schema(
        [
            Field::required("oid", DataType::Int64),
            Field::required("stxname", DataType::Text { max_len: None }),
            Field::required("stxrelid", DataType::Int64),
            Field::required("stxkeys", DataType::Text { max_len: None }),
            Field::required("stxkind", DataType::Text { max_len: None }),
        ],
        "pg_statistic_ext schema invariants hold",
    )
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Produce the initial [`CatalogSnapshot`] for a fresh database.
///
/// Pre-populates:
/// - 3 namespaces: `pg_catalog` (OID 11), `information_schema` (OID 12),
///   `public` (OID 2200).
/// - 13 system relations in `pg_catalog`: `pg_namespace`, `pg_class`,
///   `pg_attribute`, `pg_attrdef`, `pg_type`, `pg_enum`, `pg_index`,
///   `pg_constraint`, `pg_sequence`, `pg_depend`, `pg_description`.
///
/// This snapshot is installed by [`crate::PersistentCatalog::bootstrap_from_heap`]
/// when the on-disk heap is empty (fresh `initdb`-like boot), and is
/// what enables the server to query the catalog about its own catalog tables.
#[must_use]
pub fn initial_snapshot() -> CatalogSnapshot {
    let entries = system_table_entries();

    let mut tables: HashMap<String, TableEntry> = HashMap::with_capacity(entries.len());
    let mut tables_by_oid: HashMap<Oid, TableEntry> = HashMap::with_capacity(entries.len());

    for entry in entries {
        tables.insert(entry.name.to_ascii_lowercase(), entry.clone());
        tables_by_oid.insert(entry.oid, entry);
    }

    CatalogSnapshot {
        tables,
        tables_by_oid,
        indexes: HashMap::new(),
        indexes_by_table: HashMap::new(),
        enum_types: HashMap::new(),
        enum_types_by_oid: HashMap::new(),
        composite_types: HashMap::new(),
        composite_types_by_oid: HashMap::new(),
        domain_types: HashMap::new(),
        domain_types_by_oid: HashMap::new(),
        descriptions: HashMap::new(),
        statistics: HashMap::new(),
        statistic_ext: HashMap::new(),
    }
}

/// Enumerate the thirteen system [`TableEntry`] values that exist in every
/// fresh database.
///
/// Used by both [`initial_snapshot`] and
/// [`crate::PersistentCatalog::bootstrap_from_heap`] to seed the catalog
/// when no heap pages are present.
pub fn system_table_entries() -> Vec<TableEntry> {
    let ns = "pg_catalog";

    macro_rules! sys_table {
        ($oid:expr, $name:expr, $schema_fn:expr) => {
            TableEntry {
                oid: Oid::new($oid),
                name: $name.to_owned(),
                schema_name: ns.to_owned(),
                schema: $schema_fn(),
                created_at_lsn: Lsn::ZERO,
                n_blocks: 0,
                root_block: BlockNumber::INVALID,
                options: Vec::new(),
            }
        };
    }

    vec![
        sys_table!(PG_NAMESPACE_OID, "pg_namespace", pg_namespace_schema),
        sys_table!(PG_CLASS_OID, "pg_class", pg_class_schema),
        sys_table!(PG_ATTRIBUTE_OID, "pg_attribute", pg_attribute_schema),
        sys_table!(PG_ATTRDEF_OID, "pg_attrdef", pg_attrdef_schema),
        sys_table!(PG_TYPE_OID, "pg_type", pg_type_schema),
        sys_table!(PG_ENUM_OID, "pg_enum", pg_enum_schema),
        sys_table!(PG_INDEX_OID, "pg_index", pg_index_schema),
        sys_table!(PG_CONSTRAINT_OID, "pg_constraint", pg_constraint_schema),
        sys_table!(PG_SEQUENCE_OID, "pg_sequence", pg_sequence_schema),
        sys_table!(PG_DEPEND_OID, "pg_depend", pg_depend_schema),
        sys_table!(PG_DESCRIPTION_OID, "pg_description", pg_description_schema),
        sys_table!(PG_STATISTIC_OID, "pg_statistic", pg_statistic_schema),
        sys_table!(
            PG_STATISTIC_EXT_OID,
            "pg_statistic_ext",
            pg_statistic_ext_schema
        ),
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_snapshot_has_three_namespaces_worth_of_relations() {
        // All thirteen system tables live in pg_catalog.
        let snap = initial_snapshot();
        assert_eq!(
            snap.tables.len(),
            13,
            "expected 13 system tables, got {}",
            snap.tables.len()
        );
    }

    #[test]
    fn initial_snapshot_contains_known_relations() {
        let snap = initial_snapshot();
        let names = [
            "pg_namespace",
            "pg_class",
            "pg_attribute",
            "pg_type",
            "pg_enum",
            "pg_index",
            "pg_constraint",
            "pg_sequence",
            "pg_depend",
            "pg_description",
            "pg_statistic",
            "pg_statistic_ext",
        ];
        for name in names {
            assert!(
                snap.tables.contains_key(name),
                "missing system table: {name}"
            );
        }
    }

    #[test]
    fn initial_snapshot_oids_are_well_known() {
        let snap = initial_snapshot();
        assert!(snap.tables_by_oid.contains_key(&Oid::new(PG_CLASS_OID)));
        assert!(snap.tables_by_oid.contains_key(&Oid::new(PG_ATTRIBUTE_OID)));
        assert!(snap.tables_by_oid.contains_key(&Oid::new(PG_TYPE_OID)));
        assert!(snap.tables_by_oid.contains_key(&Oid::new(PG_ENUM_OID)));
        assert!(snap.tables_by_oid.contains_key(&Oid::new(PG_NAMESPACE_OID)));
        assert!(snap.tables_by_oid.contains_key(&Oid::new(PG_INDEX_OID)));
        assert!(
            snap.tables_by_oid
                .contains_key(&Oid::new(PG_CONSTRAINT_OID))
        );
        assert!(snap.tables_by_oid.contains_key(&Oid::new(PG_SEQUENCE_OID)));
        assert!(snap.tables_by_oid.contains_key(&Oid::new(PG_DEPEND_OID)));
        assert!(
            snap.tables_by_oid
                .contains_key(&Oid::new(PG_DESCRIPTION_OID))
        );
        assert!(snap.tables_by_oid.contains_key(&Oid::new(PG_STATISTIC_OID)));
        assert!(
            snap.tables_by_oid
                .contains_key(&Oid::new(PG_STATISTIC_EXT_OID))
        );
    }

    #[test]
    fn initial_snapshot_pg_class_has_correct_schema_columns() {
        let snap = initial_snapshot();
        let pg_class = snap.tables.get("pg_class").expect("pg_class present");
        // The v0.8 subset has 8 columns.
        assert_eq!(pg_class.schema.len(), 8);
    }

    #[test]
    fn initial_snapshot_has_no_indexes() {
        let snap = initial_snapshot();
        assert!(snap.indexes.is_empty());
        assert!(snap.indexes_by_table.is_empty());
    }

    #[test]
    fn system_table_entries_are_all_pg_catalog() {
        for entry in system_table_entries() {
            assert_eq!(
                entry.schema_name, "pg_catalog",
                "system table '{}' not in pg_catalog",
                entry.name
            );
        }
    }
}
