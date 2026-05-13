//! Catalog row types — `TableEntry` and `IndexEntry`.
//!
//! These are the canonical in-memory descriptions of a relation and an
//! index. They are kept deliberately small and `Clone`-friendly so the
//! catalog can hand callers an owned snapshot without lifetimes leaking
//! across thread boundaries.
//!
//! # On-disk parity
//!
//! Each field maps to a column on the future system catalog tables:
//!
//! | Field                 | Future column                |
//! |-----------------------|------------------------------|
//! | `oid`                 | `pg_class.oid`               |
//! | `name`                | `pg_class.relname`           |
//! | `schema_name`         | `pg_namespace.nspname`       |
//! | `schema`              | derived from `pg_attribute`  |
//! | `created_at_lsn`      | `pg_class.relfilelsn` (new)  |
//! | `n_blocks`            | `pg_class.relpages`          |
//! | `root_block`          | `pg_class.relfilenode` (new) |
//!
//! For [`IndexEntry`] the parity is with `pg_index`. The mapping is
//! noted here so the persistent implementation can be slotted in by a
//! follow-up RFC without renaming fields.

use ultrasql_core::{BlockNumber, Lsn, Oid, Schema};

/// A table (relation) entry in the catalog.
///
/// The owning catalog hands out cloned `TableEntry` values rather than
/// borrowed references. This keeps the API uniform between the in-memory
/// implementation (where a clone is cheap) and the future persistent
/// implementation (where the entry is materialized from a heap page and
/// the borrow would tie up a buffer pin).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableEntry {
    /// Catalog-wide object identifier. Stable for the life of the table.
    pub oid: Oid,
    /// Bare relation name (without schema qualifier).
    pub name: String,
    /// Schema (namespace) the table lives in. `"public"` by default.
    pub schema_name: String,
    /// Ordered column metadata.
    pub schema: Schema,
    /// LSN at which the CREATE TABLE record was committed. Useful for
    /// crash recovery and time-travel queries; ignored by the in-memory
    /// implementation today.
    pub created_at_lsn: Lsn,
    /// Estimated number of heap blocks. The optimizer uses this as a
    /// size hint for sequential-scan costing. Update via
    /// [`crate::MutableCatalog::update_table_size`] when ANALYZE or a
    /// bulk load completes.
    pub n_blocks: u32,
    /// First heap page of this table. `BlockNumber::INVALID` for tables
    /// that have not been materialized yet (CREATE TABLE without any
    /// inserts).
    pub root_block: BlockNumber,
}

impl TableEntry {
    /// Construct a `TableEntry` with default size statistics.
    ///
    /// Defaults: `created_at_lsn = Lsn::ZERO`, `n_blocks = 0`,
    /// `root_block = BlockNumber::INVALID`. Callers that need exact
    /// values should build the struct literally.
    #[must_use]
    pub fn new<N: Into<String>>(oid: Oid, name: N, schema_name: N, schema: Schema) -> Self {
        Self {
            oid,
            name: name.into(),
            schema_name: schema_name.into(),
            schema,
            created_at_lsn: Lsn::ZERO,
            n_blocks: 0,
            root_block: BlockNumber::INVALID,
        }
    }
}

/// An index entry in the catalog.
///
/// Mirrors the fields of `pg_index` that the planner needs: which table
/// the index covers, which columns (by attnum) it indexes, where its
/// root page lives, and whether duplicates are forbidden.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    /// Catalog-wide object identifier for the index itself.
    pub oid: Oid,
    /// Bare index name.
    pub name: String,
    /// OID of the underlying table.
    pub table_oid: Oid,
    /// Column attnums (0-based positions into the table's schema) the
    /// index covers, in declaration order. Composite indexes carry
    /// multiple entries.
    pub columns: Vec<u16>,
    /// Root page of the index B+ tree.
    pub root_block: BlockNumber,
    /// Whether this index enforces uniqueness.
    pub is_unique: bool,
}

impl IndexEntry {
    /// Construct an `IndexEntry` with `root_block = BlockNumber::INVALID`.
    ///
    /// The root block becomes meaningful once the index is materialized
    /// (the executor allocates the first leaf and rewrites the entry via
    /// a follow-up update path, parallel to PostgreSQL's
    /// `RelationSetNewRelfilenode`).
    #[must_use]
    pub fn new<N: Into<String>>(
        oid: Oid,
        name: N,
        table_oid: Oid,
        columns: Vec<u16>,
        is_unique: bool,
    ) -> Self {
        Self {
            oid,
            name: name.into(),
            table_oid,
            columns,
            root_block: BlockNumber::INVALID,
            is_unique,
        }
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field};

    use super::*;

    fn sample_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int64),
            Field::nullable("name", DataType::Text { max_len: None }),
        ])
        .expect("schema invariants hold for test fixture")
    }

    #[test]
    fn table_entry_defaults_are_safe() {
        let entry = TableEntry::new(Oid::new(16384), "users", "public", sample_schema());
        assert_eq!(entry.n_blocks, 0);
        assert_eq!(entry.root_block, BlockNumber::INVALID);
        assert_eq!(entry.created_at_lsn, Lsn::ZERO);
        assert_eq!(entry.name, "users");
        assert_eq!(entry.schema_name, "public");
    }

    #[test]
    fn index_entry_defaults_are_safe() {
        let entry = IndexEntry::new(Oid::new(16385), "users_pk", Oid::new(16384), vec![0], true);
        assert_eq!(entry.root_block, BlockNumber::INVALID);
        assert!(entry.is_unique);
        assert_eq!(entry.columns, vec![0]);
        assert_eq!(entry.table_oid, Oid::new(16384));
    }
}
