//! Runtime support for native time-range partitioned tables.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use parking_lot::Mutex;
use ultrasql_catalog::persistent::PersistentCatalog;
use ultrasql_catalog::{Catalog, MutableCatalog, TableEntry, table_lookup_key};
use ultrasql_core::{CommandId, DataType, Field, Oid, RelationId, Schema, Value, Xid};
use ultrasql_executor::{ExecError, Operator, RowCodec, batch_to_rows, build_batch};
use ultrasql_storage::heap::{HeapAccess, InsertOptions};
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_storage::wal_sink::WalSink;
use ultrasql_vec::Batch;

use crate::BlankPageLoader;

/// Default chunk width for `PARTITION BY RANGE(timestamp)`: one day.
pub const DEFAULT_TIME_CHUNK_INTERVAL_US: i64 = 86_400_000_000;
const TIME_PARTITION_KIND_OPTION: &str = "ultrasql.time_partition.kind";
const TIME_PARTITION_PARENT_KIND: &str = "parent";
const TIME_PARTITION_CHUNK_KIND: &str = "chunk";
const TIME_PARTITION_COLUMN_OPTION: &str = "ultrasql.time_partition.column";
const TIME_PARTITION_INTERVAL_US_OPTION: &str = "ultrasql.time_partition.interval_us";
const TIME_PARTITION_PARENT_OID_OPTION: &str = "ultrasql.time_partition.parent_oid";
const TIME_PARTITION_START_US_OPTION: &str = "ultrasql.time_partition.start_us";
const TIME_PARTITION_END_US_OPTION: &str = "ultrasql.time_partition.end_us";

/// Durable parent-table time partition metadata decoded from catalog options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TimePartitionParentOptions {
    /// Partition key column name.
    pub column: String,
    /// Chunk interval in microseconds.
    pub interval_us: i64,
}

/// Durable chunk-table time partition metadata decoded from catalog options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TimePartitionChunkOptions {
    /// Parent table OID.
    pub parent_oid: Oid,
    /// Inclusive lower bound in microseconds since 2000-01-01.
    pub start_us: i64,
    /// Exclusive upper bound in microseconds since 2000-01-01.
    pub end_us: i64,
}

/// Return catalog options for a time-partitioned parent relation.
#[must_use]
pub(crate) fn parent_catalog_options(
    partition_column: &str,
    interval_us: i64,
) -> Vec<(String, String)> {
    vec![
        (
            TIME_PARTITION_KIND_OPTION.to_owned(),
            TIME_PARTITION_PARENT_KIND.to_owned(),
        ),
        (
            TIME_PARTITION_COLUMN_OPTION.to_owned(),
            partition_column.to_owned(),
        ),
        (
            TIME_PARTITION_INTERVAL_US_OPTION.to_owned(),
            interval_us.to_string(),
        ),
    ]
}

/// Return relation options with the parent partition key column renamed.
#[must_use]
pub(crate) fn parent_catalog_options_with_column(
    options: &[(String, String)],
    partition_column: &str,
) -> Vec<(String, String)> {
    options
        .iter()
        .map(|(name, value)| {
            if name == TIME_PARTITION_COLUMN_OPTION {
                (name.clone(), partition_column.to_owned())
            } else {
                (name.clone(), value.clone())
            }
        })
        .collect()
}

/// Return catalog options for an auto-created time-partition chunk relation.
#[must_use]
pub(crate) fn chunk_catalog_options(
    parent_oid: Oid,
    start_us: i64,
    end_us: i64,
) -> Vec<(String, String)> {
    vec![
        (
            TIME_PARTITION_KIND_OPTION.to_owned(),
            TIME_PARTITION_CHUNK_KIND.to_owned(),
        ),
        (
            TIME_PARTITION_PARENT_OID_OPTION.to_owned(),
            parent_oid.raw().to_string(),
        ),
        (
            TIME_PARTITION_START_US_OPTION.to_owned(),
            start_us.to_string(),
        ),
        (TIME_PARTITION_END_US_OPTION.to_owned(), end_us.to_string()),
    ]
}

/// Decode parent time-partition metadata from a table entry.
pub(crate) fn parent_options_from_entry(
    entry: &TableEntry,
) -> Result<Option<TimePartitionParentOptions>, String> {
    match option_value(entry, TIME_PARTITION_KIND_OPTION) {
        None | Some(TIME_PARTITION_CHUNK_KIND) => Ok(None),
        Some(TIME_PARTITION_PARENT_KIND) => {
            let column = option_value(entry, TIME_PARTITION_COLUMN_OPTION)
                .ok_or_else(|| format!("table '{}' missing time partition column", entry.name))?
                .to_owned();
            let interval_us = parse_i64_option(entry, TIME_PARTITION_INTERVAL_US_OPTION)?;
            if interval_us <= 0 {
                return Err(format!(
                    "table '{}' has non-positive time partition interval {interval_us}",
                    entry.name
                ));
            }
            Ok(Some(TimePartitionParentOptions {
                column,
                interval_us,
            }))
        }
        Some(kind) => Err(format!(
            "table '{}' has unknown time partition kind '{}'",
            entry.name, kind
        )),
    }
}

/// Decode chunk time-partition metadata from a table entry.
pub(crate) fn chunk_options_from_entry(
    entry: &TableEntry,
) -> Result<Option<TimePartitionChunkOptions>, String> {
    match option_value(entry, TIME_PARTITION_KIND_OPTION) {
        None | Some(TIME_PARTITION_PARENT_KIND) => Ok(None),
        Some(TIME_PARTITION_CHUNK_KIND) => {
            let parent_oid_raw = parse_u32_option(entry, TIME_PARTITION_PARENT_OID_OPTION)?;
            let start_us = parse_i64_option(entry, TIME_PARTITION_START_US_OPTION)?;
            let end_us = parse_i64_option(entry, TIME_PARTITION_END_US_OPTION)?;
            if end_us <= start_us {
                return Err(format!(
                    "table '{}' has invalid time partition chunk bounds {start_us}..{end_us}",
                    entry.name
                ));
            }
            Ok(Some(TimePartitionChunkOptions {
                parent_oid: Oid::new(parent_oid_raw),
                start_us,
                end_us,
            }))
        }
        Some(kind) => Err(format!(
            "table '{}' has unknown time partition kind '{}'",
            entry.name, kind
        )),
    }
}

fn option_value<'a>(entry: &'a TableEntry, name: &str) -> Option<&'a str> {
    entry
        .options
        .iter()
        .find_map(|(key, value)| (key == name).then_some(value.as_str()))
}

fn parse_i64_option(entry: &TableEntry, name: &str) -> Result<i64, String> {
    option_value(entry, name)
        .ok_or_else(|| {
            format!(
                "table '{}' missing time partition option {name}",
                entry.name
            )
        })?
        .parse::<i64>()
        .map_err(|_| {
            format!(
                "table '{}' has invalid time partition option {name}",
                entry.name
            )
        })
}

fn parse_u32_option(entry: &TableEntry, name: &str) -> Result<u32, String> {
    option_value(entry, name)
        .ok_or_else(|| {
            format!(
                "table '{}' missing time partition option {name}",
                entry.name
            )
        })?
        .parse::<u32>()
        .map_err(|_| {
            format!(
                "table '{}' has invalid time partition option {name}",
                entry.name
            )
        })
}

/// Runtime metadata for one time-range partitioned parent table.
#[derive(Debug)]
pub struct TimePartitionRuntime {
    /// Canonical parent table lookup key.
    pub parent_table: String,
    /// Parent table relation name without a schema qualifier.
    pub parent_relname: String,
    /// Parent table schema name.
    pub parent_schema_name: String,
    /// Parent table OID.
    pub parent_oid: Oid,
    /// Parent table schema.
    pub schema: Schema,
    /// Partition key column name.
    pub partition_column: String,
    /// Partition key column index.
    pub partition_column_index: usize,
    /// Chunk width in microseconds.
    pub chunk_interval_us: i64,
    /// Auto-created chunks keyed by bucket start timestamp.
    pub chunks: DashMap<i64, TimeChunkRuntime>,
    /// Total chunks considered by the most recent parent scan.
    pub last_scan_total_chunks: AtomicUsize,
    /// Chunks selected by pruning for the most recent parent scan.
    pub last_scan_selected_chunks: AtomicUsize,
    create_lock: Mutex<()>,
}

impl TimePartitionRuntime {
    /// Build a one-day range partition runtime descriptor.
    #[must_use]
    pub fn daily(
        parent_schema_name: String,
        parent_relname: String,
        parent_oid: Oid,
        schema: Schema,
        partition_column: String,
        partition_column_index: usize,
    ) -> Self {
        let parent_table = table_lookup_key(&parent_schema_name, &parent_relname);
        Self {
            parent_table,
            parent_relname,
            parent_schema_name,
            parent_oid,
            schema,
            partition_column,
            partition_column_index,
            chunk_interval_us: DEFAULT_TIME_CHUNK_INTERVAL_US,
            chunks: DashMap::new(),
            last_scan_total_chunks: AtomicUsize::new(0),
            last_scan_selected_chunks: AtomicUsize::new(0),
            create_lock: Mutex::new(()),
        }
    }

    /// Build a copy of this runtime for a renamed parent table.
    #[must_use]
    pub fn renamed(&self, parent_schema_name: String, parent_relname: String) -> Self {
        self.with_parent_metadata(
            parent_schema_name,
            parent_relname,
            self.schema.clone(),
            self.partition_column.clone(),
            self.partition_column_index,
        )
    }

    /// Build a copy of this runtime with updated parent metadata.
    #[must_use]
    pub fn with_parent_metadata(
        &self,
        parent_schema_name: String,
        parent_relname: String,
        schema: Schema,
        partition_column: String,
        partition_column_index: usize,
    ) -> Self {
        let mut renamed = Self::daily(
            parent_schema_name,
            parent_relname,
            self.parent_oid,
            schema,
            partition_column,
            partition_column_index,
        );
        for chunk in &self.chunks {
            renamed.chunks.insert(*chunk.key(), chunk.value().clone());
        }
        renamed.chunk_interval_us = self.chunk_interval_us;
        renamed.last_scan_total_chunks.store(
            self.last_scan_total_chunks.load(Ordering::Acquire),
            Ordering::Release,
        );
        renamed.last_scan_selected_chunks.store(
            self.last_scan_selected_chunks.load(Ordering::Acquire),
            Ordering::Release,
        );
        renamed
    }
}

/// Runtime metadata for one physical chunk table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeChunkRuntime {
    /// Inclusive lower bound in microseconds since 2000-01-01.
    pub start_us: i64,
    /// Exclusive upper bound in microseconds since 2000-01-01.
    pub end_us: i64,
    /// Physical chunk table name.
    pub table_name: String,
    /// Physical chunk table OID.
    pub oid: Oid,
}

/// Pull operator that routes inserted rows into auto-created time chunks.
pub(crate) struct TimePartitionInsert {
    runtime: Arc<TimePartitionRuntime>,
    persistent_catalog: Arc<PersistentCatalog>,
    heap: Arc<HeapAccess<BlankPageLoader>>,
    vm: Arc<VisibilityMap>,
    child: Box<dyn Operator>,
    child_schema: Schema,
    codec: RowCodec,
    xid: Xid,
    command_id: CommandId,
    wal: Option<Arc<dyn WalSink>>,
    schema: Schema,
    done: bool,
}

impl std::fmt::Debug for TimePartitionInsert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimePartitionInsert")
            .field("parent_table", &self.runtime.parent_table)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl TimePartitionInsert {
    /// Construct a partitioned INSERT router.
    #[must_use]
    pub(crate) fn new(
        runtime: Arc<TimePartitionRuntime>,
        persistent_catalog: Arc<PersistentCatalog>,
        heap: Arc<HeapAccess<BlankPageLoader>>,
        vm: Arc<VisibilityMap>,
        child: Box<dyn Operator>,
        xid: Xid,
        command_id: CommandId,
    ) -> Self {
        let child_schema = child.schema().clone();
        let codec = RowCodec::new(runtime.schema.clone());
        Self {
            runtime,
            persistent_catalog,
            heap,
            vm,
            child,
            child_schema,
            codec,
            xid,
            command_id,
            wal: None,
            schema: affected_rows_schema(),
            done: false,
        }
    }

    /// Attach a WAL sink used for chunk heap writes.
    #[must_use]
    pub(crate) fn with_wal(mut self, wal: Option<Arc<dyn WalSink>>) -> Self {
        self.wal = wal;
        self
    }

    fn ensure_chunk(&self, key_us: i64) -> Result<TimeChunkRuntime, ExecError> {
        let start_us = bucket_start(key_us, self.runtime.chunk_interval_us);
        if let Some(chunk) = self.runtime.chunks.get(&start_us) {
            return Ok(chunk.value().clone());
        }

        let _guard = self.runtime.create_lock.lock();
        if let Some(chunk) = self.runtime.chunks.get(&start_us) {
            return Ok(chunk.value().clone());
        }

        let chunk_relname = chunk_table_name(&self.runtime.parent_relname, start_us);
        let table_name = table_lookup_key(&self.runtime.parent_schema_name, &chunk_relname);
        let end_us = start_us.saturating_add(self.runtime.chunk_interval_us);
        let oid = if let Some(existing) = self.persistent_catalog.lookup_table(&table_name) {
            existing.oid
        } else {
            let oid = self.persistent_catalog.next_oid();
            let entry = TableEntry::new(
                oid,
                chunk_relname,
                self.runtime.parent_schema_name.clone(),
                self.runtime.schema.clone(),
            )
            .with_options(chunk_catalog_options(
                self.runtime.parent_oid,
                start_us,
                end_us,
            ));
            self.persistent_catalog
                .create_table(entry.clone())
                .map_err(|e| ExecError::TypeMismatch(format!("create time chunk: {e}")))?;
            if let Err(err) = self.persistent_catalog.persist_table_rows(
                &entry,
                self.heap.as_ref(),
                self.xid,
                self.command_id,
            ) {
                let _ = self.persistent_catalog.drop_table(&table_name);
                return Err(ExecError::TypeMismatch(format!(
                    "persist time chunk catalog rows: {err}"
                )));
            }
            oid
        };
        let chunk = TimeChunkRuntime {
            start_us,
            end_us,
            table_name,
            oid,
        };
        self.runtime.chunks.insert(start_us, chunk.clone());
        Ok(chunk)
    }
}

impl Operator for TimePartitionInsert {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let mut by_chunk: BTreeMap<i64, Vec<Vec<Value>>> = BTreeMap::new();
        let mut affected: u64 = 0;
        while let Some(batch) = self.child.next_batch()? {
            let rows = batch_to_rows(&batch, &self.child_schema)?;
            for row in rows {
                check_not_null_violations(&row, &self.runtime.schema)?;
                let key = partition_key_us(&row, self.runtime.partition_column_index)?;
                let start = bucket_start(key, self.runtime.chunk_interval_us);
                by_chunk.entry(start).or_default().push(row);
                increment_affected_rows(&mut affected)?;
            }
        }

        for (start_us, rows) in by_chunk {
            let chunk = self.ensure_chunk(start_us)?;
            let n_atts = u16::try_from(self.runtime.schema.len())
                .map_err(|_| ExecError::Internal("partition schema column count exceeds u16"))?;
            let payloads = rows
                .iter()
                .map(|row| {
                    self.codec
                        .encode(row)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let payload_refs = payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
            self.heap
                .insert_batch(
                    RelationId(chunk.oid),
                    &payload_refs,
                    InsertOptions {
                        xmin: self.xid,
                        command_id: self.command_id,
                        n_atts,
                        wal: self.wal.as_deref(),
                        fsm: None,
                        vm: Some(self.vm.as_ref()),
                    },
                )
                .map_err(|e| ExecError::TypeMismatch(e.to_string()))?;
        }

        let rows = vec![vec![affected_rows_value(affected)?]];
        build_batch(&rows, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Concatenate already-lowered chunk scans.
pub(crate) struct AppendScan {
    children: VecDeque<Box<dyn Operator>>,
    schema: Schema,
}

impl std::fmt::Debug for AppendScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendScan")
            .field("children", &self.children.len())
            .finish_non_exhaustive()
    }
}

impl AppendScan {
    /// Build an append scan over chunk children.
    #[must_use]
    pub(crate) fn new(children: Vec<Box<dyn Operator>>, schema: Schema) -> Self {
        Self {
            children: children.into(),
            schema,
        }
    }
}

impl Operator for AppendScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        while let Some(child) = self.children.front_mut() {
            if let Some(batch) = child.next_batch()? {
                return Ok(Some(batch));
            }
            self.children.pop_front();
        }
        Ok(None)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

pub(crate) fn chunk_table_name(parent: &str, start_us: i64) -> String {
    let suffix = if start_us < 0 {
        format!("m{}", start_us.unsigned_abs())
    } else {
        start_us.to_string()
    };
    format!("__ultrasql_{parent}_chunk_{suffix}")
}

pub(crate) fn bucket_start(value_us: i64, interval_us: i64) -> i64 {
    value_us.div_euclid(interval_us).saturating_mul(interval_us)
}

fn partition_key_us(row: &[Value], idx: usize) -> Result<i64, ExecError> {
    match row.get(idx) {
        Some(Value::Timestamp(v) | Value::TimestampTz(v)) => Ok(*v),
        Some(Value::Null) => Err(ExecError::NotNullViolation("partition key".to_owned())),
        Some(other) => Err(ExecError::TypeMismatch(format!(
            "partition key must be timestamp, got {:?}",
            other.data_type()
        ))),
        None => Err(ExecError::TypeMismatch(
            "partition key column out of range".to_owned(),
        )),
    }
}

fn affected_rows_schema() -> Schema {
    match Schema::new([Field::required("affected_rows", DataType::Int64)]) {
        Ok(schema) => schema,
        Err(err) => {
            tracing::error!(error = %err, "time-partition affected_rows schema failed");
            Schema::empty()
        }
    }
}

fn increment_affected_rows(affected: &mut u64) -> Result<(), ExecError> {
    *affected = affected.checked_add(1).ok_or_else(|| {
        ExecError::TypeMismatch("time partition affected row overflow".to_owned())
    })?;
    Ok(())
}

fn affected_rows_value(affected: u64) -> Result<Value, ExecError> {
    i64::try_from(affected)
        .map(Value::Int64)
        .map_err(|_| ExecError::TypeMismatch("time partition affected rows exceed i64".to_owned()))
}

fn check_not_null_violations(row: &[Value], schema: &Schema) -> Result<(), ExecError> {
    for (value, field) in row.iter().zip(schema.fields()) {
        if !field.nullable && matches!(value, Value::Null) {
            return Err(ExecError::NotNullViolation(field.name.clone()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{affected_rows_value, chunk_table_name};

    #[test]
    fn chunk_table_name_handles_min_timestamp_without_wrapping_minus() {
        assert_eq!(
            chunk_table_name("metrics", i64::MIN),
            "__ultrasql_metrics_chunk_m9223372036854775808"
        );
    }

    #[test]
    fn affected_rows_value_rejects_i64_overflow() {
        let too_many = u64::try_from(i64::MAX).unwrap() + 1;
        let err = affected_rows_value(too_many).unwrap_err();
        assert!(matches!(err, ultrasql_executor::ExecError::TypeMismatch(_)));
    }
}
