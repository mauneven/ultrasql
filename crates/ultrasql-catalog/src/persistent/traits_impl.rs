//! Trait implementations and schema-move helper for [`PersistentCatalog`].
//!
//! Extracted verbatim from the original `persistent.rs`; see [`super`].

use super::*;

impl PersistentCatalog {
    /// Move a relation without indexes to another schema, preserving its OID.
    ///
    /// This is currently used by `ALTER VIEW ... SET SCHEMA`. Ordinary
    /// table moves need additional index namespace handling and should use a
    /// broader catalog API when that SQL surface lands.
    pub fn alter_relation_set_schema(
        &self,
        name: &str,
        new_schema: &str,
    ) -> Result<TableEntry, CatalogError> {
        let old_key = self.table_lookup_key_for_unqualified(name);
        let new_schema = fold_name(new_schema);
        let _guard = self.write_lock.lock();
        let existing = self
            .tables_by_name
            .get(&old_key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .value()
            .clone();
        if self.indexes_by_table.contains_key(&existing.oid) {
            return Err(CatalogError::schema_conflict(format!(
                "relation '{}' has indexes and cannot be moved by alter_relation_set_schema",
                existing.name
            )));
        }
        let new_key = table_lookup_key(&new_schema, &existing.name);
        if new_key != old_key && self.tables_by_name.contains_key(&new_key) {
            return Err(CatalogError::already_exists(existing.name.clone()));
        }
        let existing = self
            .tables_by_name
            .remove(&old_key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .1;
        let mut updated = existing.clone();
        updated.schema_name = new_schema;
        self.tables_by_name.insert(new_key, updated.clone());
        if let Some(mut entry) = self.tables_by_oid.get_mut(&existing.oid) {
            *entry = updated.clone();
        }
        self.rebuild_snapshot();
        Ok(updated)
    }
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

    fn alter_table_set_schema(
        &self,
        name: &str,
        new_schema: &str,
    ) -> Result<TableEntry, CatalogError> {
        let new_schema = new_schema.to_ascii_lowercase();
        let old_key = self.table_lookup_key_for_unqualified(name);
        let _guard = self.write_lock.lock();
        let existing = self
            .tables_by_name
            .get(&old_key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .value()
            .clone();
        let new_key = table_lookup_key(&new_schema, &existing.name);
        if new_key != old_key && self.tables_by_name.contains_key(&new_key) {
            return Err(CatalogError::already_exists(new_key));
        }
        let existing = self
            .tables_by_name
            .remove(&old_key)
            .ok_or_else(|| CatalogError::not_found(name.to_owned()))?
            .1;
        let mut updated = existing.clone();
        updated.schema_name = new_schema;
        self.tables_by_name.insert(new_key, updated.clone());
        if let Some(mut entry) = self.tables_by_oid.get_mut(&existing.oid) {
            *entry = updated.clone();
        }
        self.rebuild_snapshot();
        Ok(updated)
    }
}
