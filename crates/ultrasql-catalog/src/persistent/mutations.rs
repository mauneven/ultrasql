//! Snapshot rebuild, descriptions, and statistics mutations.
//!
//! Extracted verbatim from the original `persistent.rs`; see [`super`].

use super::*;

impl PersistentCatalog {
    /// Rebuild and swap in a new snapshot.
    ///
    /// Must hold `write_lock` when calling.
    pub(crate) fn rebuild_snapshot(&self) {
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
