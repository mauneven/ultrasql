//! Heap-based catalog bootstrap (warm restart and fresh database).
//!
//! Extracted verbatim from the original `persistent.rs`; see [`super`].

use super::*;

impl PersistentCatalog {
    /// Bootstrap the catalog from on-disk system catalog heap pages.
    ///
    /// Reads `pg_namespace`, `pg_class`, `pg_attribute`, `pg_index`,
    /// `pg_constraint`, `pg_sequence`, `pg_depend`, `pg_description`,
    /// `pg_statistic`, and `pg_statistic_ext` from heap pages via the supplied
    /// [`HeapAccess`]. Builds a
    /// [`CatalogSnapshot`] and atomically swaps it into the in-memory
    /// `ArcSwap` cache.
    ///
    /// # Fresh database
    ///
    /// When all system catalog heap pages are empty (i.e. the database was
    /// just initialized) this method detects the empty heap and installs the
    /// hard-coded [`initial_snapshot`] that contains the three well-known
    /// namespaces and the eleven system relations.  The returned
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
        use crate::encoding::{
            decode_attribute_row, decode_constraint_row, decode_description_row, decode_enum_row,
            decode_index_row, decode_sequence_row, decode_statistic_ext_row, decode_statistic_row,
            decode_type_row, schema_from_attributes,
        };

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_attribute_rel = RelationId::new(bootstrap::PG_ATTRIBUTE_OID);
        let pg_type_rel = RelationId::new(bootstrap::PG_TYPE_OID);
        let pg_enum_rel = RelationId::new(bootstrap::PG_ENUM_OID);
        let pg_index_rel = RelationId::new(bootstrap::PG_INDEX_OID);
        let pg_constraint_rel = RelationId::new(bootstrap::PG_CONSTRAINT_OID);
        let pg_sequence_rel = RelationId::new(bootstrap::PG_SEQUENCE_OID);
        let pg_description_rel = RelationId::new(bootstrap::PG_DESCRIPTION_OID);
        let pg_statistic_rel = RelationId::new(bootstrap::PG_STATISTIC_OID);
        let pg_statistic_ext_rel = RelationId::new(bootstrap::PG_STATISTIC_EXT_OID);
        let class_blocks = heap.block_count(pg_class_rel);
        let type_blocks = heap.block_count(pg_type_rel);
        let enum_blocks = heap.block_count(pg_enum_rel);

        if class_blocks == 0 && type_blocks == 0 && enum_blocks == 0 {
            // Fresh database — install the initial hard-coded snapshot.
            let snap = initial_snapshot();
            let stats = CatalogStats::initial();
            self.install_snapshot(snap)?;
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
        let mut indexes: std::collections::HashMap<String, IndexEntry> = initial.indexes.clone();
        let mut indexes_by_table: std::collections::HashMap<Oid, Vec<IndexEntry>> =
            initial.indexes_by_table.clone();
        let mut enum_types: std::collections::HashMap<String, EnumTypeEntry> =
            initial.enum_types.clone();
        let mut enum_types_by_oid: std::collections::HashMap<Oid, EnumTypeEntry> =
            initial.enum_types_by_oid.clone();
        let mut composite_types: std::collections::HashMap<String, CompositeTypeEntry> =
            initial.composite_types.clone();
        let mut composite_types_by_oid: std::collections::HashMap<Oid, CompositeTypeEntry> =
            initial.composite_types_by_oid.clone();
        let domain_types: std::collections::HashMap<String, DomainTypeEntry> =
            initial.domain_types.clone();
        let domain_types_by_oid: std::collections::HashMap<Oid, DomainTypeEntry> =
            initial.domain_types_by_oid.clone();
        let mut highest_oid: u32 = self.next_oid.load(Ordering::Acquire);

        let mut type_rows_by_oid: std::collections::HashMap<Oid, TypeRow> =
            std::collections::HashMap::new();
        if type_blocks > 0 {
            let type_scan = heap.scan(pg_type_rel, type_blocks);
            for result in type_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_type: {e}"))
                })?;
                let row = decode_type_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_type row: {e}"))
                })?;
                track_next_oid(&mut highest_oid, row.oid, "pg_type")?;
                if row.oid.raw() >= crate::memory::FIRST_USER_OID {
                    type_rows_by_oid.insert(row.oid, row);
                }
            }
        }

        let mut enum_rows_by_type: std::collections::HashMap<Oid, Vec<EnumRow>> =
            std::collections::HashMap::new();
        if enum_blocks > 0 {
            let enum_scan = heap.scan(pg_enum_rel, enum_blocks);
            for result in enum_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_enum: {e}"))
                })?;
                let row = decode_enum_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_enum row: {e}"))
                })?;
                track_next_oid(&mut highest_oid, row.oid, "pg_enum")?;
                if row.enumtypid.raw() >= crate::memory::FIRST_USER_OID {
                    enum_rows_by_type
                        .entry(row.enumtypid)
                        .or_default()
                        .push(row);
                }
            }
        }

        for (type_oid, type_row) in &type_rows_by_oid {
            if type_row.typtype != 'e' {
                continue;
            }
            let mut enum_rows = enum_rows_by_type.remove(type_oid).ok_or_else(|| {
                CatalogError::schema_conflict(format!(
                    "enum type '{}' has no pg_enum labels",
                    type_row.typname
                ))
            })?;
            enum_rows.sort_by_key(|row| row.enumsortorder);
            let labels = enum_rows
                .into_iter()
                .map(|row| EnumLabelEntry {
                    oid: row.oid,
                    label: row.enumlabel,
                    sort_order: row.enumsortorder,
                })
                .collect::<Vec<_>>();
            let schema_name = if type_row.typnamespace.raw() == bootstrap::PG_CATALOG_OID {
                "pg_catalog".to_owned()
            } else {
                "public".to_owned()
            };
            let entry = EnumTypeEntry {
                oid: *type_oid,
                name: type_row.typname.clone(),
                schema_name,
                labels,
            };
            enum_types.insert(type_entry_key(&entry), entry.clone());
            enum_types_by_oid.insert(entry.oid, entry);
        }

        // Keep the latest attribute row per `(attrelid, attnum)`, then group
        // by relation so append-only ALTER TABLE catalog rows replace older
        // schema history during bootstrap.
        let attribute_blocks = heap.block_count(pg_attribute_rel);
        let mut latest_attrs_by_key: std::collections::HashMap<
            (Oid, i16),
            (
                crate::persistent::AttributeRow,
                ultrasql_core::DataType,
                bool,
            ),
        > = std::collections::HashMap::new();
        let mut attribute_rows: std::collections::HashMap<(Oid, i16), AttributeRow> =
            std::collections::HashMap::new();
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
                let key = (row.attrelid, row.attnum);
                attribute_rows.insert(key, row.clone());
                latest_attrs_by_key.insert(key, (row, dt, nullable));
                total_attrs = total_attrs.saturating_add(1);
            }
        }
        let mut attrs_by_relation: std::collections::HashMap<
            Oid,
            Vec<(
                crate::persistent::AttributeRow,
                ultrasql_core::DataType,
                bool,
            )>,
        > = std::collections::HashMap::new();
        for (_, (row, dt, nullable)) in latest_attrs_by_key {
            attrs_by_relation
                .entry(row.attrelid)
                .or_default()
                .push((row, dt, nullable));
        }

        let index_blocks = heap.block_count(pg_index_rel);
        let mut index_rows_by_oid: std::collections::HashMap<Oid, IndexRow> =
            std::collections::HashMap::new();
        let mut total_index_rows: u32 = 0;
        if index_blocks > 0 {
            let index_scan = heap.scan(pg_index_rel, index_blocks);
            for result in index_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_index: {e}"))
                })?;
                let row = decode_index_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_index row: {e}"))
                })?;
                if row.indexrelid.raw() >= crate::memory::FIRST_USER_OID {
                    index_rows_by_oid.insert(row.indexrelid, row);
                }
                total_index_rows = total_index_rows.saturating_add(1);
            }
        }

        let constraint_blocks = heap.block_count(pg_constraint_rel);
        let mut constraint_rows: std::collections::HashMap<Oid, ConstraintRow> =
            std::collections::HashMap::new();
        let mut total_constraint_rows: u32 = 0;
        if constraint_blocks > 0 {
            let constraint_scan = heap.scan(pg_constraint_rel, constraint_blocks);
            for result in constraint_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_constraint: {e}"))
                })?;
                let row = decode_constraint_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_constraint row: {e}"))
                })?;
                constraint_rows.insert(row.oid, row);
                total_constraint_rows = total_constraint_rows.saturating_add(1);
            }
        }

        let sequence_blocks = heap.block_count(pg_sequence_rel);
        let mut sequence_rows: std::collections::HashMap<Oid, SequenceRow> =
            std::collections::HashMap::new();
        if sequence_blocks > 0 {
            let sequence_scan = heap.scan(pg_sequence_rel, sequence_blocks);
            for result in sequence_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_sequence: {e}"))
                })?;
                let row = decode_sequence_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_sequence row: {e}"))
                })?;
                sequence_rows.insert(row.seqrelid, row);
            }
        }

        // Decode pg_class rows. The catalog heap is append-only, so keep the
        // latest row per OID before rebuilding tables. This lets ALTER TABLE
        // replacement rows override CREATE-time rows without consuming the
        // attribute set twice.
        let class_scan = heap.scan(pg_class_rel, class_blocks);
        let mut latest_class_by_oid: std::collections::HashMap<Oid, ClassRow> =
            std::collections::HashMap::new();
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
            track_next_oid(&mut highest_oid, class_row.oid, "pg_class")?;
            latest_class_by_oid.insert(class_row.oid, class_row);
        }

        let class_rows_by_oid = latest_class_by_oid.clone();
        let mut user_relations: u32 = 0;
        let mut user_index_classes: Vec<ClassRow> = Vec::new();
        for (_, class_row) in latest_class_by_oid {
            match class_row.relkind {
                RelKind::Table | RelKind::View | RelKind::MaterializedView => {
                    user_relations = user_relations.saturating_add(1);
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
                        options: class_row.reloptions.clone(),
                    };
                    tables.insert(table_entry_key(&entry), entry.clone());
                    tables_by_oid.insert(entry.oid, entry);
                }
                RelKind::Index => {
                    user_relations = user_relations.saturating_add(1);
                    user_index_classes.push(class_row);
                }
                RelKind::CompositeType => {
                    user_relations = user_relations.saturating_add(1);
                    let Some(type_row) = type_rows_by_oid.get(&class_row.oid) else {
                        tracing::warn!(
                            oid = class_row.oid.raw(),
                            relname = %class_row.relname,
                            "skipping composite pg_class row without pg_type metadata"
                        );
                        continue;
                    };
                    if type_row.typtype != 'c' {
                        tracing::warn!(
                            oid = class_row.oid.raw(),
                            typtype = %type_row.typtype,
                            "skipping composite pg_class row whose pg_type row is not composite"
                        );
                        continue;
                    }
                    let attrs = attrs_by_relation.remove(&class_row.oid).unwrap_or_default();
                    let schema = schema_from_attributes(attrs).map_err(|e| {
                        CatalogError::schema_conflict(format!(
                            "rebuild composite schema for oid {}: {e}",
                            class_row.oid.raw(),
                        ))
                    })?;
                    let schema_name = if class_row.relnamespace.raw() == bootstrap::PG_CATALOG_OID {
                        "pg_catalog".to_owned()
                    } else {
                        "public".to_owned()
                    };
                    let entry = CompositeTypeEntry {
                        oid: class_row.oid,
                        name: class_row.relname.clone(),
                        schema_name,
                        schema,
                    };
                    composite_types.insert(type_entry_key(&entry), entry.clone());
                    composite_types_by_oid.insert(entry.oid, entry);
                }
                _ => {}
            }
        }
        for oid in constraint_rows.keys() {
            track_next_oid(&mut highest_oid, *oid, "pg_constraint")?;
        }
        for oid in sequence_rows.keys() {
            track_next_oid(&mut highest_oid, *oid, "pg_sequence")?;
        }

        let mut loaded_indexes: u32 = 0;
        for class_row in user_index_classes {
            let Some(index_row) = index_rows_by_oid.get(&class_row.oid) else {
                tracing::warn!(
                    index = %class_row.relname,
                    oid = class_row.oid.raw(),
                    "skipping orphaned pg_class index row without pg_index metadata"
                );
                continue;
            };
            if !index_row.indisvalid {
                continue;
            }
            if usize::from(index_row.indnatts) != index_row.indkey.len() {
                tracing::warn!(
                    index_oid = index_row.indexrelid.raw(),
                    indnatts = index_row.indnatts,
                    indkey_len = index_row.indkey.len(),
                    "skipping malformed pg_index row with mismatched key count"
                );
                continue;
            }
            if !tables_by_oid.contains_key(&index_row.indrelid) {
                tracing::warn!(
                    index_oid = index_row.indexrelid.raw(),
                    table_oid = index_row.indrelid.raw(),
                    "skipping pg_index row referencing unknown table"
                );
                continue;
            }
            let mut columns = Vec::with_capacity(index_row.indkey.len());
            let mut invalid_column = None;
            for &attnum in &index_row.indkey {
                match u16::try_from(attnum) {
                    Ok(column) => columns.push(column),
                    Err(_) => {
                        invalid_column = Some(attnum);
                        break;
                    }
                }
            }
            if let Some(attnum) = invalid_column {
                tracing::warn!(
                    index_oid = index_row.indexrelid.raw(),
                    attnum,
                    "skipping pg_index row with invalid column position"
                );
                continue;
            }
            let mut entry = IndexEntry::new(
                class_row.oid,
                class_row.relname.clone(),
                index_row.indrelid,
                columns,
                index_row.indisunique,
            );
            entry.schema_name = if class_row.relnamespace.raw() == bootstrap::PG_CATALOG_OID {
                "pg_catalog".to_owned()
            } else {
                "public".to_owned()
            };
            entry.root_block = ultrasql_core::BlockNumber::new(class_row.relfilenode);
            entry.is_primary = index_row.indisprimary;
            entry.access_method = index_row.indmethod.clone();
            entry.opclasses = index_row.indopclasses.clone();
            entry.options = index_row.indoptions.clone();
            indexes.insert(index_entry_key(&entry), entry.clone());
            indexes_by_table
                .entry(index_row.indrelid)
                .or_default()
                .push(entry);
            loaded_indexes = loaded_indexes.saturating_add(1);
        }
        // Bump the OID allocator past every observed OID so a
        // subsequent `next_oid` call cannot collide with a restored
        // relation.
        self.next_oid.store(highest_oid, Ordering::Release);

        let statistic_blocks = heap.block_count(pg_statistic_rel);
        let mut statistics = initial.statistics;
        let mut total_statistics: u32 = 0;
        if statistic_blocks > 0 {
            let statistic_scan = heap.scan(pg_statistic_rel, statistic_blocks);
            for result in statistic_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_statistic: {e}"))
                })?;
                let row = decode_statistic_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_statistic row: {e}"))
                })?;
                statistics.insert((row.starelid, row.staattnum), row);
                total_statistics = total_statistics.saturating_add(1);
            }
        }
        statistics.retain(|(starelid, _), _| {
            starelid.raw() < crate::memory::FIRST_USER_OID || tables_by_oid.contains_key(starelid)
        });
        total_statistics = u32::try_from(statistics.len()).unwrap_or(u32::MAX);

        let statistic_ext_blocks = heap.block_count(pg_statistic_ext_rel);
        let mut statistic_ext = initial.statistic_ext;
        let mut total_statistic_ext: u32 = 0;
        if statistic_ext_blocks > 0 {
            let statistic_ext_scan = heap.scan(pg_statistic_ext_rel, statistic_ext_blocks);
            for result in statistic_ext_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!(
                        "heap scan error on pg_statistic_ext: {e}"
                    ))
                })?;
                let row = decode_statistic_ext_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_statistic_ext row: {e}"))
                })?;
                statistic_ext.insert(row.oid, row);
                total_statistic_ext = total_statistic_ext.saturating_add(1);
            }
        }
        statistic_ext.retain(|_, row| {
            row.stxrelid.raw() < crate::memory::FIRST_USER_OID
                || tables_by_oid.contains_key(&row.stxrelid)
        });
        total_statistic_ext = u32::try_from(statistic_ext.len()).unwrap_or(u32::MAX);

        let description_blocks = heap.block_count(pg_description_rel);
        let mut descriptions = initial.descriptions;
        let mut total_description_rows: u32 = 0;
        if description_blocks > 0 {
            let description_scan = heap.scan(pg_description_rel, description_blocks);
            for result in description_scan {
                let tuple = result.map_err(|e| {
                    CatalogError::schema_conflict(format!("heap scan error on pg_description: {e}"))
                })?;
                let (row, deleted) = decode_description_row(&tuple.data).map_err(|e| {
                    CatalogError::schema_conflict(format!("decode pg_description row: {e}"))
                })?;
                let key = (row.objoid, row.classoid, row.objsubid);
                if deleted {
                    descriptions.remove(&key);
                } else {
                    descriptions.insert(key, row);
                }
                total_description_rows = total_description_rows.saturating_add(1);
            }
        }
        let mut live_description_oids: std::collections::HashSet<Oid> =
            tables_by_oid.keys().copied().collect();
        for index in indexes.values() {
            live_description_oids.insert(index.oid);
        }
        descriptions.retain(|(objoid, _, _), _| live_description_oids.contains(objoid));

        let snap = CatalogSnapshot {
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
            constraints: constraint_rows.clone(),
            descriptions,
            statistics,
            statistic_ext,
        };
        let stats = CatalogStats {
            namespaces: CatalogStats::initial().namespaces,
            relations: CatalogStats::initial().relations + user_relations,
            attributes: total_attrs,
            indexes: loaded_indexes.max(total_index_rows),
            constraints: total_constraint_rows,
            descriptions: total_description_rows,
            statistics: total_statistics,
            statistic_ext: total_statistic_ext,
        };
        self.install_snapshot(snap)?;
        self.pg_class.clear();
        for (oid, row) in class_rows_by_oid {
            self.pg_class.insert(oid, row);
        }
        self.pg_type.clear();
        for (oid, row) in type_rows_by_oid {
            self.pg_type.insert(oid, row);
        }
        self.pg_attribute.clear();
        for (key, row) in attribute_rows {
            self.pg_attribute.insert(key, row);
        }
        self.pg_constraint.clear();
        for (oid, row) in constraint_rows {
            self.pg_constraint.insert(oid, row);
        }
        self.pg_sequence.clear();
        for (oid, row) in sequence_rows {
            self.pg_sequence.insert(oid, row);
        }
        tracing::debug!(?stats, "catalog bootstrapped from heap");
        Ok(stats)
    }

}
