//! In-memory implementation of [`Catalog`] and [`MutableCatalog`].
//!
//! Backed by [`dashmap::DashMap`] for sharded concurrent access — reads
//! and writes hit independent shards keyed by hash, so a write to one
//! relation never blocks a read on another. OIDs are allocated by an
//! [`AtomicU32`] starting at `FIRST_USER_OID` (16 384), mirroring
//! PostgreSQL's reservation of the first 16 384 OIDs for system objects.
//!
//! # Persistent migration anchor
//!
//! The replacement plan is to swap each `DashMap` for a typed view over
//! the corresponding system catalog heap table. The fields read off
//! [`TableEntry`] / [`IndexEntry`] are intentionally identical to the
//! column set of those heap rows; the migration is therefore a thin
//! adapter that decodes a tuple into the same struct. A `TODO(catalog-
//! persistent)` marker is placed at each integration point so a follow-
//! up RFC can find them with `git grep`.

use std::sync::atomic::{AtomicU32, Ordering};

use dashmap::DashMap;
use ultrasql_core::Oid;

use crate::entry::{IndexEntry, TableEntry};
use crate::error::CatalogError;
use crate::traits::{Catalog, MutableCatalog};

/// First OID handed out to user objects. Matches PostgreSQL's
/// `FirstNormalObjectId` (`src/include/access/transam.h`). The lower
/// range is reserved for bootstrap-allocated system catalog rows.
pub const FIRST_USER_OID: u32 = 16_384;

/// Folds a SQL identifier to the catalog's storage key. SQL identifiers
/// compare case-insensitively unless quoted; the catalog matches that
/// by lowercasing on the way in.
#[inline]
fn fold_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// In-memory catalog.
///
/// Concurrency model:
/// - `tables_by_name` and `tables_by_oid` are two views of the same
///   set. They are kept consistent by holding the destination shard
///   write-lock on both maps for the duration of a create/drop.
/// - `indexes_by_name` and `indexes_by_table` are likewise kept in
///   sync.
/// - `next_oid` is an `AtomicU32`; allocation is wait-free.
///
/// A future persistent implementation will replace the `DashMap`s with
/// MVCC reads against the catalog heap; the public surface (this
/// struct's `impl Catalog`) is the integration point.
#[derive(Debug)]
pub struct InMemoryCatalog {
    tables_by_name: DashMap<String, TableEntry>,
    tables_by_oid: DashMap<Oid, TableEntry>,
    indexes_by_name: DashMap<String, IndexEntry>,
    indexes_by_table: DashMap<Oid, Vec<IndexEntry>>,
    next_oid: AtomicU32,
}

impl Default for InMemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryCatalog {
    /// Construct an empty catalog.
    ///
    /// OID allocation starts at [`FIRST_USER_OID`]. The map shards are
    /// sized by `DashMap`'s defaults; we do not pre-size because the
    /// expected workload is small (hundreds of tables, not millions).
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables_by_name: DashMap::new(),
            tables_by_oid: DashMap::new(),
            indexes_by_name: DashMap::new(),
            indexes_by_table: DashMap::new(),
            next_oid: AtomicU32::new(FIRST_USER_OID),
        }
    }

    /// Allocate a fresh OID. Wait-free; never returns the invalid
    /// sentinel.
    ///
    /// Wrap-around lands the allocator back at zero. We assert in
    /// debug that we never reach `Oid::INVALID` (zero) — if we do,
    /// the catalog has allocated more than 4 billion entries and is
    /// unrecoverable.
    pub fn next_oid(&self) -> Oid {
        let raw = self.next_oid.fetch_add(1, Ordering::Relaxed);
        debug_assert!(raw != 0, "OID allocator wrapped to INVALID");
        Oid::new(raw)
    }

    /// Internal: install a table into both indexes. Returns
    /// `AlreadyExists` if either is already taken.
    ///
    /// TODO(catalog-persistent): replace with a heap insert against
    /// `pg_class` plus a unique-key check on `(relname, relnamespace)`.
    fn install_table(&self, entry: TableEntry) -> Result<(), CatalogError> {
        let key = fold_name(&entry.name);
        if self.tables_by_name.contains_key(&key) {
            return Err(CatalogError::already_exists(entry.name));
        }
        if self.tables_by_oid.contains_key(&entry.oid) {
            return Err(CatalogError::already_exists(format!("oid {}", entry.oid)));
        }
        // Race-window note: between the two reads above and the writes
        // below, another thread may insert the same key. We close that
        // window with the `entry` API on the by-name map, which is
        // shard-locked.
        match self.tables_by_name.entry(key) {
            dashmap::Entry::Occupied(occ) => Err(CatalogError::already_exists(occ.key().clone())),
            dashmap::Entry::Vacant(vac) => {
                // Reserve the OID slot before publishing the name slot,
                // so a reader that finds the name always also finds the
                // OID-keyed entry.
                if self.tables_by_oid.contains_key(&entry.oid) {
                    return Err(CatalogError::already_exists(format!("oid {}", entry.oid)));
                }
                self.tables_by_oid.insert(entry.oid, entry.clone());
                vac.insert(entry);
                Ok(())
            }
        }
    }

    /// Internal: remove every index that points at `table_oid`.
    fn drop_indexes_for_table(&self, table_oid: Oid) {
        if let Some((_, indexes)) = self.indexes_by_table.remove(&table_oid) {
            for idx in indexes {
                self.indexes_by_name.remove(&fold_name(&idx.name));
            }
        }
    }
}

impl Catalog for InMemoryCatalog {
    fn lookup_table(&self, name: &str) -> Option<TableEntry> {
        self.tables_by_name
            .get(&fold_name(name))
            .map(|r| r.value().clone())
    }

    fn lookup_table_by_oid(&self, oid: Oid) -> Option<TableEntry> {
        self.tables_by_oid.get(&oid).map(|r| r.value().clone())
    }

    fn list_tables(&self) -> Vec<TableEntry> {
        self.tables_by_name
            .iter()
            .map(|r| r.value().clone())
            .collect()
    }

    fn lookup_index(&self, name: &str) -> Option<IndexEntry> {
        self.indexes_by_name
            .get(&fold_name(name))
            .map(|r| r.value().clone())
    }

    fn list_indexes_for_table(&self, table_oid: Oid) -> Vec<IndexEntry> {
        self.indexes_by_table
            .get(&table_oid)
            .map_or_else(Vec::new, |r| r.value().clone())
    }
}

impl MutableCatalog for InMemoryCatalog {
    fn create_table(&self, entry: TableEntry) -> Result<(), CatalogError> {
        if entry.oid.is_invalid() {
            return Err(CatalogError::schema_conflict(
                "cannot register table with INVALID oid",
            ));
        }
        self.install_table(entry)
    }

    fn drop_table(&self, name: &str) -> Result<(), CatalogError> {
        let key = fold_name(name);
        // TODO(catalog-persistent): replace with a heap delete + index
        // cascade against `pg_class`, `pg_attribute`, `pg_index`.
        let removed = self
            .tables_by_name
            .remove(&key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .1;
        self.tables_by_oid.remove(&removed.oid);
        self.drop_indexes_for_table(removed.oid);
        Ok(())
    }

    fn create_index(&self, entry: IndexEntry) -> Result<(), CatalogError> {
        if entry.oid.is_invalid() {
            return Err(CatalogError::schema_conflict(
                "cannot register index with INVALID oid",
            ));
        }
        // Validate against the parent table.
        let parent = self
            .tables_by_oid
            .get(&entry.table_oid)
            .ok_or_else(|| {
                CatalogError::schema_conflict(format!(
                    "index '{}' references unknown table oid {}",
                    entry.name, entry.table_oid
                ))
            })?
            .value()
            .clone();
        let width = parent.schema.len();
        for col in &entry.columns {
            if usize::from(*col) >= width {
                return Err(CatalogError::schema_conflict(format!(
                    "index '{}' column attnum {} out of range for table '{}' (width {})",
                    entry.name, col, parent.name, width
                )));
            }
        }
        let key = fold_name(&entry.name);
        // TODO(catalog-persistent): replace with a heap insert against
        // `pg_index` and a unique-key check on `(relname, relnamespace)`.
        match self.indexes_by_name.entry(key) {
            dashmap::Entry::Occupied(occ) => Err(CatalogError::already_exists(occ.key().clone())),
            dashmap::Entry::Vacant(vac) => {
                self.indexes_by_table
                    .entry(entry.table_oid)
                    .or_default()
                    .push(entry.clone());
                vac.insert(entry);
                Ok(())
            }
        }
    }

    fn drop_index(&self, name: &str) -> Result<(), CatalogError> {
        let key = fold_name(name);
        let removed = self
            .indexes_by_name
            .remove(&key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .1;
        if let Some(mut list) = self.indexes_by_table.get_mut(&removed.table_oid) {
            list.retain(|i| i.oid != removed.oid);
        }
        Ok(())
    }

    fn update_table_size(&self, oid: Oid, n_blocks: u32) -> Result<(), CatalogError> {
        let folded = {
            let mut entry = self
                .tables_by_oid
                .get_mut(&oid)
                .ok_or_else(|| CatalogError::not_found(format!("oid {oid}")))?;
            entry.n_blocks = n_blocks;
            fold_name(&entry.name)
        };
        // Drop the by-oid write-guard before reacquiring on the by-name
        // shard: holding both at once would risk a cross-shard wait
        // graph under contention.
        if let Some(mut by_name) = self.tables_by_name.get_mut(&folded) {
            by_name.n_blocks = n_blocks;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use ultrasql_core::{BlockNumber, DataType, Field, Lsn, Schema};

    use super::*;

    fn users_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int64),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema invariants hold for test fixture")
    }

    fn make_table(cat: &InMemoryCatalog, name: &str) -> TableEntry {
        TableEntry {
            oid: cat.next_oid(),
            name: name.to_owned(),
            schema_name: "public".to_owned(),
            schema: users_schema(),
            created_at_lsn: Lsn::ZERO,
            n_blocks: 0,
            root_block: BlockNumber::INVALID,
        }
    }

    #[test]
    fn create_and_lookup_round_trip() {
        let cat = InMemoryCatalog::new();
        let entry = make_table(&cat, "users");
        let oid = entry.oid;
        cat.create_table(entry.clone())
            .expect("create succeeds on empty catalog");
        let by_name = cat
            .lookup_table("users")
            .expect("table is reachable by name");
        assert_eq!(by_name, entry);
        let by_oid = cat.lookup_table_by_oid(oid).expect("reachable by oid");
        assert_eq!(by_oid, entry);
    }

    #[test]
    fn list_tables_returns_all_created() {
        let cat = InMemoryCatalog::new();
        let names = ["users", "orders", "products"];
        for n in names {
            cat.create_table(make_table(&cat, n))
                .expect("create succeeds");
        }
        let listed: Vec<String> = {
            let mut v: Vec<String> = cat.list_tables().into_iter().map(|t| t.name).collect();
            v.sort();
            v
        };
        let mut want: Vec<String> = names.iter().map(|s| (*s).to_owned()).collect();
        want.sort();
        assert_eq!(listed, want);
    }

    #[test]
    fn duplicate_name_create_returns_already_exists() {
        let cat = InMemoryCatalog::new();
        cat.create_table(make_table(&cat, "users"))
            .expect("first create succeeds");
        let err = cat
            .create_table(make_table(&cat, "users"))
            .expect_err("duplicate must fail");
        assert!(matches!(err, CatalogError::AlreadyExists(_)));
    }

    #[test]
    fn duplicate_name_create_is_case_insensitive() {
        let cat = InMemoryCatalog::new();
        cat.create_table(make_table(&cat, "Users"))
            .expect("first create succeeds");
        let err = cat
            .create_table(make_table(&cat, "USERS"))
            .expect_err("case-folded duplicate must fail");
        assert!(matches!(err, CatalogError::AlreadyExists(_)));
    }

    #[test]
    fn drop_then_lookup_returns_none() {
        let cat = InMemoryCatalog::new();
        cat.create_table(make_table(&cat, "users"))
            .expect("create succeeds");
        cat.drop_table("users").expect("drop succeeds");
        assert!(cat.lookup_table("users").is_none());
    }

    #[test]
    fn drop_nonexistent_returns_not_found() {
        let cat = InMemoryCatalog::new();
        let err = cat.drop_table("missing").expect_err("drop must fail");
        assert!(matches!(err, CatalogError::NotFound(_)));
    }

    #[test]
    fn auto_oids_are_unique_and_above_floor() {
        let cat = InMemoryCatalog::new();
        let n = 64_usize;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..n {
            let oid = cat.next_oid();
            assert!(oid.raw() >= FIRST_USER_OID);
            assert!(seen.insert(oid), "duplicate oid {oid}");
        }
        assert_eq!(seen.len(), n);
    }

    #[test]
    fn index_create_and_list_for_table() {
        let cat = InMemoryCatalog::new();
        let table = make_table(&cat, "users");
        let table_oid = table.oid;
        cat.create_table(table).expect("create succeeds");

        let idx1 = IndexEntry::new(cat.next_oid(), "users_pkey", table_oid, vec![0], true);
        let idx2 = IndexEntry::new(cat.next_oid(), "users_name_idx", table_oid, vec![1], false);
        cat.create_index(idx1.clone()).expect("idx1 create");
        cat.create_index(idx2.clone()).expect("idx2 create");

        let mut listed = cat.list_indexes_for_table(table_oid);
        listed.sort_by_key(|i| i.oid.raw());
        let mut want = vec![idx1.clone(), idx2];
        want.sort_by_key(|i| i.oid.raw());
        assert_eq!(listed, want);

        let by_name = cat
            .lookup_index("USERS_PKEY")
            .expect("case-insensitive index lookup");
        assert_eq!(by_name, idx1);
    }

    #[test]
    fn index_create_rejects_out_of_range_attnum() {
        let cat = InMemoryCatalog::new();
        let table = make_table(&cat, "users");
        let table_oid = table.oid;
        cat.create_table(table).expect("create succeeds");

        let idx = IndexEntry::new(
            cat.next_oid(),
            "bad_idx",
            table_oid,
            vec![42], // schema only has 3 columns
            false,
        );
        let err = cat.create_index(idx).expect_err("attnum out of range");
        assert!(matches!(err, CatalogError::SchemaConflict(_)));
    }

    #[test]
    fn index_create_rejects_unknown_table_oid() {
        let cat = InMemoryCatalog::new();
        let idx = IndexEntry::new(
            cat.next_oid(),
            "orphan_idx",
            Oid::new(99_999),
            vec![0],
            false,
        );
        let err = cat
            .create_index(idx)
            .expect_err("orphan index must be rejected");
        assert!(matches!(err, CatalogError::SchemaConflict(_)));
    }

    #[test]
    fn drop_index_removes_from_table_list() {
        let cat = InMemoryCatalog::new();
        let table = make_table(&cat, "users");
        let table_oid = table.oid;
        cat.create_table(table).expect("create succeeds");
        let idx = IndexEntry::new(cat.next_oid(), "users_pkey", table_oid, vec![0], true);
        cat.create_index(idx).expect("idx create");

        cat.drop_index("users_pkey").expect("idx drop");
        assert!(cat.lookup_index("users_pkey").is_none());
        assert!(cat.list_indexes_for_table(table_oid).is_empty());
    }

    #[test]
    fn drop_table_cascades_to_indexes() {
        let cat = InMemoryCatalog::new();
        let table = make_table(&cat, "users");
        let table_oid = table.oid;
        cat.create_table(table).expect("create succeeds");
        let idx = IndexEntry::new(cat.next_oid(), "users_pkey", table_oid, vec![0], true);
        cat.create_index(idx).expect("idx create");

        cat.drop_table("users").expect("drop succeeds");
        assert!(cat.lookup_index("users_pkey").is_none());
        assert!(cat.list_indexes_for_table(table_oid).is_empty());
    }

    #[test]
    fn case_insensitive_table_lookup() {
        let cat = InMemoryCatalog::new();
        cat.create_table(make_table(&cat, "Users"))
            .expect("create succeeds");
        assert!(cat.lookup_table("users").is_some());
        assert!(cat.lookup_table("USERS").is_some());
        assert!(cat.lookup_table("UsErS").is_some());
        assert!(cat.lookup_table("nope").is_none());
    }

    #[test]
    fn update_table_size_propagates_to_both_views() {
        let cat = InMemoryCatalog::new();
        let entry = make_table(&cat, "users");
        let oid = entry.oid;
        cat.create_table(entry).expect("create succeeds");

        cat.update_table_size(oid, 7).expect("update succeeds");
        assert_eq!(cat.lookup_table("users").unwrap().n_blocks, 7);
        assert_eq!(cat.lookup_table_by_oid(oid).unwrap().n_blocks, 7);
    }

    #[test]
    fn update_table_size_unknown_oid_is_not_found() {
        let cat = InMemoryCatalog::new();
        let err = cat
            .update_table_size(Oid::new(123_456), 9)
            .expect_err("must fail");
        assert!(matches!(err, CatalogError::NotFound(_)));
    }

    #[test]
    fn create_table_rejects_invalid_oid() {
        let cat = InMemoryCatalog::new();
        let mut entry = make_table(&cat, "users");
        entry.oid = Oid::INVALID;
        let err = cat.create_table(entry).expect_err("must reject");
        assert!(matches!(err, CatalogError::SchemaConflict(_)));
    }

    #[test]
    fn concurrent_creates_lose_no_entries() {
        // N threads each create M distinct tables. Every table either
        // installs cleanly or returns AlreadyExists; in aggregate the
        // catalog must hold exactly N*M unique entries.
        const THREADS: usize = 8;
        const PER_THREAD: usize = 32;
        let cat = Arc::new(InMemoryCatalog::new());
        let mut handles = Vec::with_capacity(THREADS);
        for t in 0..THREADS {
            let cat = Arc::clone(&cat);
            handles.push(thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let name = format!("tbl_{t}_{i}");
                    let entry =
                        TableEntry::new(cat.next_oid(), name, "public".to_owned(), users_schema());
                    cat.create_table(entry)
                        .expect("disjoint names never collide");
                }
            }));
        }
        for h in handles {
            h.join().expect("worker panic-free");
        }
        assert_eq!(cat.list_tables().len(), THREADS * PER_THREAD);
    }

    #[test]
    fn concurrent_duplicate_creates_serialize() {
        // Many threads race to install the same name. Exactly one
        // succeeds; the rest get AlreadyExists. The catalog holds
        // exactly one entry afterwards.
        const RACERS: usize = 16;
        let cat = Arc::new(InMemoryCatalog::new());
        let oid = cat.next_oid();
        let mut handles = Vec::with_capacity(RACERS);
        let success = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for _ in 0..RACERS {
            let cat = Arc::clone(&cat);
            let success = Arc::clone(&success);
            handles.push(thread::spawn(move || {
                let entry = TableEntry::new(
                    Oid::new(oid.raw()),
                    "shared".to_owned(),
                    "public".to_owned(),
                    users_schema(),
                );
                if cat.create_table(entry).is_ok() {
                    success.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().expect("worker panic-free");
        }
        assert_eq!(success.load(Ordering::Relaxed), 1);
        assert_eq!(cat.list_tables().len(), 1);
    }
}
