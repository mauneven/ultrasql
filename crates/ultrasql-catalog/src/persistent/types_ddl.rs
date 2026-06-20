//! User-defined type DDL: enum, composite, and domain types.
//!
//! Extracted verbatim from the original `persistent.rs`; see [`super`].

use super::*;

impl PersistentCatalog {
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
}
