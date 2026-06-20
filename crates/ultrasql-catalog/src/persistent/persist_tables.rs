//! Sequence, table, relation, and statistics persistence.
//!
//! Extracted verbatim from the original `persistent.rs`; see [`super`].

use super::*;

impl PersistentCatalog {
    /// Append `pg_class` / `pg_sequence` rows for one sequence.
    pub fn persist_sequence_rows<L: PageLoader>(
        &self,
        sequence_name: &str,
        schema_name: &str,
        row: &SequenceRow,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_sequence_row;
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_sequence_rel = RelationId::new(bootstrap::PG_SEQUENCE_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let class_row = ClassRow {
            oid: row.seqrelid,
            relname: sequence_name.to_owned(),
            relnamespace: namespace_oid_for_schema(schema_name),
            relkind: RelKind::Sequence,
            relpages: 0,
            reltuples: 0.0,
            relfilenode: 0,
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
        .map_err(|e| CatalogError::schema_conflict(format!("pg_class sequence insert: {e}")))?;

        let bytes = encode_sequence_row(row);
        heap.insert(
            pg_sequence_rel,
            &bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: SEQUENCE_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_sequence insert: {e}")))?;
        Ok(())
    }

    /// Encode and write `entry` into the persistent `pg_class` /
    /// `pg_attribute` heaps so a subsequent
    /// [`Self::bootstrap_from_heap`] call can rebuild this
    /// `TableEntry` after restart.
    ///
    /// This is the durable counterpart to [`Self::create_table`]
    /// (which only updates the in-memory `DashMap`s). DDL callers
    /// invoke both: first `create_table` so the planner sees the new
    /// relation, then `persist_table_rows` so the next restart finds
    /// it on disk. Heap I/O happens through the same `xmin`/
    /// `command_id` the DDL transaction owns so MVCC visibility
    /// rules apply uniformly.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::SchemaConflict`] when the column's
    /// [`DataType`] is outside the catalog-
    /// persistable set (e.g. `Array`, `Record`), or when a heap I/O
    /// failure prevents either pg_class or pg_attribute from
    /// accepting the row.
    pub fn persist_table_rows<L: PageLoader>(
        &self,
        entry: &TableEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        self.persist_table_rows_with_defaults(entry, &[], heap, xmin, command_id)
    }

    /// Append catalog rows for a table schema replacement.
    ///
    /// `pg_attribute` is append-only. To replace a compacted UltraSQL schema
    /// after `ALTER TABLE`, write dropped markers for every old attnum first,
    /// then write the new compacted attributes. Bootstrap keeps the latest row
    /// per `(attrelid, attnum)`, so reused attnums resolve to the new schema
    /// and old surplus attnums resolve to dropped columns.
    pub fn persist_table_schema_replacement<L: PageLoader>(
        &self,
        old_entry: &TableEntry,
        new_entry: &TableEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        self.persist_table_schema_replacement_with_defaults(
            old_entry,
            new_entry,
            &[],
            heap,
            xmin,
            command_id,
        )
    }

    /// Append catalog rows for a table schema replacement with
    /// caller-supplied `pg_attribute.atthasdef` metadata.
    ///
    /// `attr_has_defaults` is indexed by zero-based column position in
    /// `new_entry.schema`. Missing entries default to `false`.
    pub fn persist_table_schema_replacement_with_defaults<L: PageLoader>(
        &self,
        old_entry: &TableEntry,
        new_entry: &TableEntry,
        attr_has_defaults: &[bool],
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_attribute_row;
        use crate::persistent::{AttributeRow, ClassRow};
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_attribute_rel = RelationId::new(bootstrap::PG_ATTRIBUTE_OID);
        let namespace_oid = namespace_oid_for_schema(&new_entry.schema_name);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let class_row = ClassRow {
            oid: new_entry.oid,
            relname: new_entry.name.clone(),
            relnamespace: namespace_oid,
            relkind: RelKind::Table,
            relpages: new_entry.n_blocks,
            reltuples: 0.0,
            relfilenode: new_entry.root_block.raw(),
            relhasindex: false,
            reloptions: new_entry.options.clone(),
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
        .map_err(|e| CatalogError::schema_conflict(format!("pg_class insert: {e}")))?;

        let old_attr_context = format!("old table {}", old_entry.name);
        for (i, field) in old_entry.schema.fields().iter().enumerate() {
            let attnum = attnum_for_index(i, &old_attr_context)?;
            let attr_row = AttributeRow {
                attrelid: new_entry.oid,
                attname: field.name.clone(),
                atttypid: 0,
                attnum,
                attnotnull: !field.nullable,
                atthasdef: false,
                attisdropped: true,
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

        let new_attr_context = format!("table {}", new_entry.name);
        for (i, field) in new_entry.schema.fields().iter().enumerate() {
            let attnum = attnum_for_index(i, &new_attr_context)?;
            let attr_row = AttributeRow {
                attrelid: new_entry.oid,
                attname: field.name.clone(),
                atttypid: 0,
                attnum,
                attnotnull: !field.nullable,
                atthasdef: attr_has_defaults.get(i).copied().unwrap_or(false),
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

    /// Append a durable `pg_class` tombstone for a dropped table.
    ///
    /// Catalog heaps are append-only today. Bootstrap keeps the newest
    /// `pg_class` row per OID, so a `RelKind::Dropped` marker suppresses
    /// older CREATE/ALTER rows after restart without needing heap delete
    /// support first.
    pub fn persist_table_drop_tombstone<L: PageLoader>(
        &self,
        entry: &TableEntry,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_attribute_row;
        use crate::persistent::{AttributeRow, ClassRow};
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_attribute_rel = RelationId::new(bootstrap::PG_ATTRIBUTE_OID);
        let namespace_oid = namespace_oid_for_schema(&entry.schema_name);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let class_row = ClassRow {
            oid: entry.oid,
            relname: entry.name.clone(),
            relnamespace: namespace_oid,
            relkind: RelKind::Dropped,
            relpages: entry.n_blocks,
            reltuples: 0.0,
            relfilenode: entry.root_block.raw(),
            relhasindex: false,
            reloptions: entry.options.clone(),
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
        .map_err(|e| CatalogError::schema_conflict(format!("pg_class tombstone insert: {e}")))?;

        let attr_context = format!("table {}", entry.name);
        for (i, field) in entry.schema.fields().iter().enumerate() {
            let attnum = attnum_for_index(i, &attr_context)?;
            let attr_row = AttributeRow {
                attrelid: entry.oid,
                attname: field.name.clone(),
                atttypid: 0,
                attnum,
                attnotnull: !field.nullable,
                atthasdef: false,
                attisdropped: true,
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
            .map_err(|e| {
                CatalogError::schema_conflict(format!("pg_attribute tombstone insert: {e}"))
            })?;
        }
        Ok(())
    }

    /// Append `pg_class` / `pg_attribute` rows for one user table with
    /// caller-supplied `atthasdef` metadata.
    ///
    /// `attr_has_defaults` is indexed by zero-based column position. Missing
    /// entries are treated as `false`, preserving the legacy behavior for
    /// callers that have no default-expression metadata.
    pub fn persist_table_rows_with_defaults<L: PageLoader>(
        &self,
        entry: &TableEntry,
        attr_has_defaults: &[bool],
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        self.persist_relation_rows_with_defaults(
            entry,
            RelKind::Table,
            attr_has_defaults,
            heap,
            xmin,
            command_id,
        )
    }

    /// Insert `pg_class` + `pg_attribute` rows for a heap-backed relation kind.
    pub fn persist_relation_rows_with_defaults<L: PageLoader>(
        &self,
        entry: &TableEntry,
        relkind: RelKind,
        attr_has_defaults: &[bool],
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_attribute_row;
        use crate::persistent::{AttributeRow, ClassRow};
        use ultrasql_storage::heap::InsertOptions;

        let pg_class_rel = RelationId::new(bootstrap::PG_CLASS_OID);
        let pg_attribute_rel = RelationId::new(bootstrap::PG_ATTRIBUTE_OID);

        let namespace_oid = namespace_oid_for_schema(&entry.schema_name);

        let class_row = ClassRow {
            oid: entry.oid,
            relname: entry.name.clone(),
            relnamespace: namespace_oid,
            relkind,
            relpages: entry.n_blocks,
            reltuples: 0.0,
            relfilenode: entry.root_block.raw(),
            relhasindex: false,
            reloptions: entry.options.clone(),
        };
        let class_bytes = class_row
            .encode()
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_class: {e}")))?;
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let class_opts = InsertOptions {
            xmin,
            command_id,
            n_atts: CLASS_ROW_N_ATTS,
            wal,
            fsm: None,
            vm: None,
        };
        heap.insert(pg_class_rel, &class_bytes, class_opts)
            .map_err(|e| CatalogError::schema_conflict(format!("pg_class insert: {e}")))?;

        let attr_context = format!("table {}", entry.name);
        for (i, field) in entry.schema.fields().iter().enumerate() {
            let attnum = attnum_for_index(i, &attr_context)?;
            let attr_row = AttributeRow {
                attrelid: entry.oid,
                attname: field.name.clone(),
                atttypid: 0,
                attnum,
                attnotnull: !field.nullable,
                atthasdef: attr_has_defaults.get(i).copied().unwrap_or(false),
                attisdropped: false,
            };
            let bytes = encode_attribute_row(&attr_row, &field.data_type, field.nullable)
                .map_err(|e| CatalogError::schema_conflict(format!("encode pg_attribute: {e}")))?;
            let attr_opts = InsertOptions {
                xmin,
                command_id,
                n_atts: ATTRIBUTE_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            };
            heap.insert(pg_attribute_rel, &bytes, attr_opts)
                .map_err(|e| CatalogError::schema_conflict(format!("pg_attribute insert: {e}")))?;
        }
        Ok(())
    }

    /// Append `pg_statistic` rows to the persistent catalog heap.
    ///
    /// `replace_statistics` updates the wait-free in-memory snapshot. This
    /// method writes the durable row stream consumed by
    /// [`Self::bootstrap_from_heap`]. Rows are append-only; bootstrap keeps the
    /// last row for each `(starelid, staattnum)` key.
    pub fn persist_statistic_rows<L: PageLoader>(
        &self,
        rows: &[StatisticRow],
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_statistic_row;
        use ultrasql_storage::heap::InsertOptions;

        let pg_statistic_rel = RelationId::new(bootstrap::PG_STATISTIC_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        for row in rows {
            let bytes = encode_statistic_row(row);
            let opts = InsertOptions {
                xmin,
                command_id,
                n_atts: STATISTIC_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            };
            heap.insert(pg_statistic_rel, &bytes, opts)
                .map_err(|e| CatalogError::schema_conflict(format!("pg_statistic insert: {e}")))?;
        }
        Ok(())
    }

    /// Append one `pg_statistic_ext` row to the persistent catalog heap.
    pub fn persist_statistic_ext_row<L: PageLoader>(
        &self,
        row: &StatisticExtRow,
        heap: &HeapAccess<L>,
        xmin: ultrasql_core::Xid,
        command_id: ultrasql_core::CommandId,
    ) -> Result<(), CatalogError> {
        use crate::encoding::encode_statistic_ext_row;
        use ultrasql_storage::heap::InsertOptions;

        let pg_statistic_ext_rel = RelationId::new(bootstrap::PG_STATISTIC_EXT_OID);
        let wal = heap.wal_sink().map(|sink| sink.as_ref());
        let bytes = encode_statistic_ext_row(row)
            .map_err(|e| CatalogError::schema_conflict(format!("encode pg_statistic_ext: {e}")))?;
        heap.insert(
            pg_statistic_ext_rel,
            &bytes,
            InsertOptions {
                xmin,
                command_id,
                n_atts: STATISTIC_EXT_ROW_N_ATTS,
                wal,
                fsm: None,
                vm: None,
            },
        )
        .map_err(|e| CatalogError::schema_conflict(format!("pg_statistic_ext insert: {e}")))?;
        Ok(())
    }

}
