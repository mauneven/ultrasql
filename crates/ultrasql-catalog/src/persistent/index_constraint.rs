//! Index and constraint persistence.
//!
//! Extracted verbatim from the original `persistent.rs`; see [`super`].

use super::*;

impl PersistentCatalog {
    /// Encode and write `entry` into persistent `pg_class` / `pg_index` rows.
    ///
    /// This is the durable counterpart to [`Self::create_index`], which only
    /// publishes the in-memory catalog snapshot. DDL callers invoke both so a
    /// warm restart can rebuild index metadata and keep choosing `IndexScan`
    /// plans.
    pub fn persist_index_rows<L: PageLoader>(
        &self,
        entry: &IndexEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_index_row;
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_index_rel = RelationId::new(bootstrap::PG_INDEX_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());

        let class_row = ClassRow {
            oid: entry.oid,
            relname: entry.name.clone(),
            relnamespace: namespace_oid_for_schema(&entry.schema_name),
            relkind: RelKind::Index,
            relpages: 0,
            reltuples: 0.0,
            relfilenode: entry.root_block.raw(),
            relhasindex: false,
            reloptions: Vec::new(),
        };
        let class_bytes = class_row
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
        .map_err(|e| CatalogError::schema_conflict(format!("pg_class index insert: {e}")))?;

        let mut indkey = Vec::with_capacity(entry.columns.len());
        for &column in &entry.columns {
            indkey.push(i16::try_from(column).map_err(|_| {
                CatalogError::schema_conflict(format!(
                    "index '{}' column position {} does not fit i16",
                    entry.name, column
                ))
            })?);
        }
        let index_row = IndexRow {
            indexrelid: entry.oid,
            indrelid: entry.table_oid,
            indnatts: u16::try_from(entry.columns.len()).map_err(|_| {
                CatalogError::schema_conflict(format!(
                    "index '{}' has too many key columns",
                    entry.name
                ))
            })?,
            indisunique: entry.is_unique,
            indisprimary: entry.is_primary,
            indisvalid: true,
            indkey,
            indmethod: entry.access_method.clone(),
            indopclasses: normalized_opclasses(entry),
            indoptions: entry.options.clone(),
        };
        let bytes = encode_index_row(&index_row)
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_index: {e}")))?;
        heap.insert(
            pg_index_rel,
            &bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: INDEX_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_index insert: {e}")))?;
        Ok(())
    }

    /// Append a durable `pg_class` tombstone for a dropped index.
    ///
    /// `pg_index` rows are append-only today. Bootstrap only rebuilds indexes
    /// whose latest `pg_class` row is `RelKind::Index`, so a dropped marker on
    /// the index relation suppresses older index metadata after restart.
    pub fn persist_index_drop_tombstone<L: PageLoader>(
        &self,
        entry: &IndexEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let class_row = ClassRow {
            oid: entry.oid,
            relname: entry.name.clone(),
            relnamespace: namespace_oid_for_schema(&entry.schema_name),
            relkind: RelKind::Dropped,
            relpages: 0,
            reltuples: 0.0,
            relfilenode: entry.root_block.raw(),
            relhasindex: false,
            reloptions: Vec::new(),
        };
        let class_bytes = class_row
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
        .map_err(|e| {
            CatalogError::schema_conflict(format!("pg_class index tombstone insert: {e}"))
        })?;
        Ok(())
    }

    /// Append one `pg_constraint` row to the persistent catalog heap.
    pub fn persist_constraint_row<L: PageLoader>(
        &self,
        row: &ConstraintRow,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_constraint_row;
        use ultrasql_storage::heap::InsertOptions;

        let pg_constraint_rel = RelationId::new(bootstrap::PG_CONSTRAINT_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let bytes = encode_constraint_row(row)
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_constraint: {e}")))?;
        heap.insert(
            pg_constraint_rel,
            &bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: CONSTRAINT_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_constraint insert: {e}")))?;
        Ok(())
    }

    /// Publish committed `pg_constraint` rows to the live catalog side map.
    ///
    /// DDL callers invoke this only after the catalog-write transaction commits.
    /// Bootstrap installs the same rows from heap during startup.
    pub fn install_constraint_rows<I>(&self, rows: I)
    where
        I: IntoIterator<Item = ConstraintRow>,
    {
        for row in rows {
            self.pg_constraint.insert(row.oid, row);
        }
        self.rebuild_snapshot();
    }

    /// Remove live `pg_constraint` rows owned by one dropped table.
    pub fn remove_constraints_for_table(&self, table_oid: Oid) {
        let stale = self
            .pg_constraint
            .iter()
            .filter_map(|row| (row.value().conrelid == table_oid).then_some(*row.key()))
            .collect::<Vec<_>>();
        for oid in stale {
            self.pg_constraint.remove(&oid);
        }
    }

    /// Return a constraint that depends on an index name for one table.
    ///
    /// Constraint-created indexes use the constraint name as the index name in
    /// the current catalog. Dropping those indexes directly would leave
    /// `pg_constraint` rows that claim enforcement still exists.
    #[must_use]
    pub fn constraint_dependency_for_index(
        &self,
        table_oid: Oid,
        index_name: &str,
    ) -> Option<ConstraintRow> {
        let key = fold_name(index_name);
        self.pg_constraint
            .iter()
            .find(|row| {
                let row = row.value();
                row.conrelid == table_oid
                    && fold_name(&row.conname) == key
                    && matches!(
                        row.contype,
                        ConType::PrimaryKey | ConType::Unique | ConType::Exclusion
                    )
            })
            .map(|row| row.value().clone())
    }
}
