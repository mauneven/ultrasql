//! Runtime support for native time-range partitioned tables.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use dashmap::DashMap;
use parking_lot::Mutex;
use ultrasql_catalog::persistent::PersistentCatalog;
use ultrasql_catalog::{Catalog, MutableCatalog, TableEntry};
use ultrasql_core::{CommandId, DataType, Field, Oid, RelationId, Schema, Value, Xid};
use ultrasql_executor::{ExecError, Operator, RowCodec, batch_to_rows, build_batch};
use ultrasql_storage::heap::{HeapAccess, InsertOptions};
use ultrasql_storage::vm::VisibilityMap;
use ultrasql_storage::wal_sink::WalSink;
use ultrasql_vec::Batch;

use crate::BlankPageLoader;

/// Default chunk width for `PARTITION BY RANGE(timestamp)`: one day.
pub const DEFAULT_TIME_CHUNK_INTERVAL_US: i64 = 86_400_000_000;

/// Runtime metadata for one time-range partitioned parent table.
#[derive(Debug)]
pub struct TimePartitionRuntime {
    /// Parent table name.
    pub parent_table: String,
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
        parent_table: String,
        parent_oid: Oid,
        schema: Schema,
        partition_column: String,
        partition_column_index: usize,
    ) -> Self {
        Self {
            parent_table,
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

        let table_name = chunk_table_name(&self.runtime.parent_table, start_us);
        let oid = if let Some(existing) = self.persistent_catalog.lookup_table(&table_name) {
            existing.oid
        } else {
            let oid = self.persistent_catalog.next_oid();
            let entry = TableEntry::new(
                oid,
                table_name.clone(),
                "public".to_owned(),
                self.runtime.schema.clone(),
            );
            self.persistent_catalog
                .create_table(entry)
                .map_err(|e| ExecError::TypeMismatch(format!("create time chunk: {e}")))?;
            oid
        };
        let chunk = TimeChunkRuntime {
            start_us,
            end_us: start_us.saturating_add(self.runtime.chunk_interval_us),
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
