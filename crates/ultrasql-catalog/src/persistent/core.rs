//! Construction, OID allocation, snapshot access, and snapshot install.
//!
//! Extracted verbatim from the original `persistent.rs`; see [`super`].

use super::*;

impl PersistentCatalog {
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

    pub(crate) fn table_lookup_key_for_unqualified(&self, name: &str) -> String {
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

    pub(crate) fn index_lookup_key_for_unqualified(&self, name: &str) -> String {
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

}
