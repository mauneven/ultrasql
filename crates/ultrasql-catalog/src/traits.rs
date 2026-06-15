//! Catalog traits.
//!
//! Two layered traits:
//!
//! - [`Catalog`] — read-only lookups. Cheap; the binder and the
//!   optimizer call it on every statement.
//! - [`MutableCatalog`] — DDL operations. Distinct trait so a caller
//!   that only reads (the binder) cannot accidentally mutate.
//!
//! Implementations must be `Send + Sync` so a single catalog handle can
//! be shared across worker threads. Read paths must not deadlock with
//! write paths: the in-memory implementation uses sharded maps to
//! guarantee that.

use ultrasql_core::{Field, Oid};

use crate::entry::{IndexEntry, TableEntry, index_lookup_key};
use crate::error::CatalogError;

/// Read-only catalog interface.
///
/// All methods are case-insensitive in their name arguments — SQL
/// identifiers compare case-insensitively unless quoted, and the
/// catalog stores its lookup key folded to ASCII lowercase. Callers
/// receive owned [`TableEntry`] / [`IndexEntry`] values; cloning is
/// cheap (the heavy field, [`ultrasql_core::Schema`], is internally
/// `Arc`-shared).
pub trait Catalog: Send + Sync {
    /// Resolve a table by its bare relation name.
    ///
    /// Returns `None` when no table by that case-folded name is
    /// registered. The name comparison ignores ASCII case.
    fn lookup_table(&self, name: &str) -> Option<TableEntry>;

    /// Resolve a table by schema and bare relation name.
    ///
    /// Returns `None` when no table with that `(schema, name)` pair is
    /// registered. Implementations that do not model schemas may fall back to
    /// [`Self::lookup_table`] and filter by [`TableEntry::schema_name`].
    fn lookup_table_in_schema(&self, schema_name: &str, name: &str) -> Option<TableEntry> {
        self.lookup_table(name)
            .filter(|entry| entry.schema_name.eq_ignore_ascii_case(schema_name))
    }

    /// Resolve a table by its OID.
    ///
    /// Returns `None` when no live table holds that OID. Catalog
    /// dropped-table tombstones are *not* returned through this path;
    /// tombstones are an internal concern of the persistent
    /// implementation.
    fn lookup_table_by_oid(&self, oid: Oid) -> Option<TableEntry>;

    /// Enumerate all live tables.
    ///
    /// The returned slice has no defined order. Callers that need a
    /// deterministic listing should sort by `oid` or by `name` at the
    /// call site; sorting inside the catalog would impose a cost on
    /// every consumer.
    fn list_tables(&self) -> Vec<TableEntry>;

    /// Resolve an index by its name.
    ///
    /// Returns `None` when no live index by that case-folded name is
    /// registered.
    fn lookup_index(&self, name: &str) -> Option<IndexEntry>;

    /// Resolve an index by schema and bare index name.
    ///
    /// Returns `None` when no live index with that `(schema, name)` pair is
    /// registered.
    fn lookup_index_in_schema(&self, schema_name: &str, name: &str) -> Option<IndexEntry> {
        self.lookup_index(&index_lookup_key(schema_name, name))
            .filter(|entry| entry.schema_name.eq_ignore_ascii_case(schema_name))
    }

    /// Enumerate every index whose `table_oid` matches the supplied
    /// argument. The returned list is empty if there are no indexes,
    /// or if `table_oid` does not name a live table.
    fn list_indexes_for_table(&self, table_oid: Oid) -> Vec<IndexEntry>;
}

/// DDL-capable catalog.
///
/// Implementations are required to be `Send + Sync` (inherited via the
/// [`Catalog`] supertrait). Mutating methods are called from DDL
/// statement execution; reads can run concurrently with writes, but the
/// implementation must guarantee that a successful create is visible to
/// every subsequent read on the same thread (the in-memory
/// implementation publishes through `DashMap`, which gives this for
/// free).
pub trait MutableCatalog: Catalog {
    /// Register a new table.
    ///
    /// # Errors
    /// - [`CatalogError::AlreadyExists`] when another live table shares
    ///   the case-folded name or the OID.
    fn create_table(&self, entry: TableEntry) -> Result<(), CatalogError>;

    /// Remove a table by name. The associated indexes are dropped too;
    /// callers do *not* have to drop indexes individually first.
    ///
    /// # Errors
    /// - [`CatalogError::NotFound`] when no table by that name is
    ///   registered.
    fn drop_table(&self, name: &str) -> Result<(), CatalogError>;

    /// Register a new index.
    ///
    /// # Errors
    /// - [`CatalogError::AlreadyExists`] when another index by that
    ///   name (or that OID) exists.
    /// - [`CatalogError::SchemaConflict`] when `table_oid` does not
    ///   name a registered table, or when any column attnum in
    ///   `columns` is out of range for that table's schema.
    fn create_index(&self, entry: IndexEntry) -> Result<(), CatalogError>;

    /// Remove an index by name.
    ///
    /// # Errors
    /// - [`CatalogError::NotFound`] when no index by that name is
    ///   registered.
    fn drop_index(&self, name: &str) -> Result<(), CatalogError>;

    /// Update the `n_blocks` size hint stored on a table entry.
    /// Called by ANALYZE, by bulk loaders, and by the heap when it
    /// extends a relation.
    ///
    /// # Errors
    /// - [`CatalogError::NotFound`] when no table holds that OID.
    fn update_table_size(&self, oid: Oid, n_blocks: u32) -> Result<(), CatalogError>;

    /// Append `column` to the named table's schema and atomically
    /// publish the new entry.
    ///
    /// The table's [`Oid`] is preserved; only `schema` is rebuilt
    /// with the additional [`Field`] appended at the end.
    ///
    /// # Errors
    /// - [`CatalogError::NotFound`] when no table by `name` is
    ///   registered.
    /// - [`CatalogError::SchemaConflict`] when appending the field
    ///   would violate a [`ultrasql_core::Schema`] invariant (e.g. a
    ///   duplicate column name after case-folding).
    fn alter_table_add_column(&self, name: &str, column: Field)
    -> Result<TableEntry, CatalogError>;

    /// Replace the schema on the named table with `new_schema`.
    ///
    /// Preserves the table's [`Oid`]; only the schema and the dependent
    /// `n_blocks` / dependent-index columns are rebuilt. Used by
    /// `ALTER TABLE DROP COLUMN` and `ALTER TABLE RENAME COLUMN` —
    /// both of which produce a schema of the same arity (drop)
    /// or the same shape with a renamed field (rename) and never
    /// touch tuples whose codec layout is positional rather than
    /// name-addressed.
    ///
    /// # Errors
    /// - [`CatalogError::NotFound`] when no table by `name` is
    ///   registered.
    /// - [`CatalogError::SchemaConflict`] when `new_schema` violates a
    ///   [`ultrasql_core::Schema`] invariant.
    fn alter_table_replace_schema(
        &self,
        name: &str,
        new_schema: ultrasql_core::Schema,
    ) -> Result<TableEntry, CatalogError>;

    /// Replace relation storage options on the named table.
    ///
    /// # Errors
    /// - [`CatalogError::NotFound`] when no table by `name` is
    ///   registered.
    fn alter_table_options(
        &self,
        name: &str,
        options: Vec<(String, String)>,
    ) -> Result<TableEntry, CatalogError>;

    /// Rename a table.
    ///
    /// The [`Oid`] is preserved. Dependent indexes keep their `table_oid`
    /// pointer so they survive the rename without rebuilding.
    ///
    /// # Errors
    /// - [`CatalogError::NotFound`] when no table by `old_name` is
    ///   registered.
    /// - [`CatalogError::AlreadyExists`] when `new_name` collides with
    ///   another live table.
    fn alter_table_rename(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<TableEntry, CatalogError>;

    /// Move a table-like relation into another schema.
    ///
    /// The [`Oid`] and bare relation name are preserved. Only the
    /// namespace component of the by-name key changes.
    ///
    /// # Errors
    /// - [`CatalogError::NotFound`] when no relation by `name` exists.
    /// - [`CatalogError::AlreadyExists`] when `new_schema.name` collides
    ///   with another live relation.
    fn alter_table_set_schema(
        &self,
        name: &str,
        new_schema: &str,
    ) -> Result<TableEntry, CatalogError>;
}
