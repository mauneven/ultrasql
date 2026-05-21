//! Lightweight runtime profiling wrapper for pull-mode operators.
//!
//! The wrapper is opt-in: normal query execution keeps the direct operator
//! tree, while `EXPLAIN ANALYZE` can wrap each lowered physical node and
//! read the counters after the root is drained.

use std::time::Instant;

use ultrasql_core::Schema;
use ultrasql_vec::Batch;
use ultrasql_vec::column::Column;

use crate::{ExecError, Operator};

/// Runtime counters for one physical operator plus profiled children.
#[derive(Clone, Debug, Default)]
pub struct OperatorRuntimeProfile {
    /// Stable operator label chosen by the server lowerer.
    pub operator: String,
    /// Rows consumed from profiled child operators.
    pub rows_in: u64,
    /// Rows emitted by this operator.
    pub rows_out: u64,
    /// Batches emitted by this operator.
    pub batches: u64,
    /// Wall-clock time spent inside this operator's `next_batch` calls.
    pub time_us: u64,
    /// Peak emitted batch memory in bytes.
    pub memory_bytes: u64,
    /// Spill events reported by the operator.
    pub spills: u64,
    /// Bytes written to spill files when known.
    pub spill_bytes: u64,
    /// Operator-owned IO bytes when known.
    pub io_bytes: u64,
    /// Pruning notes such as row groups skipped or index candidates dropped.
    pub pruning: Vec<String>,
    /// Profiled child operators in execution-tree order.
    pub children: Vec<OperatorRuntimeProfile>,
}

/// Spill counters surfaced by operators with a disk-backed fallback.
#[derive(Clone, Copy, Debug, Default)]
pub struct OperatorSpillProfile {
    /// Number of spill files, partitions, or fallback spill events.
    pub spills: u64,
    /// Bytes written to spill files when the operator can account them.
    pub bytes: u64,
}

/// Opt-in profiling wrapper around a physical [`Operator`].
#[derive(Debug)]
pub struct ProfiledOperator {
    operator: String,
    inner: Box<dyn Operator>,
    rows_out: u64,
    batches: u64,
    time_us: u64,
    memory_bytes: u64,
}

impl ProfiledOperator {
    /// Wrap `inner` with the operator label used in `EXPLAIN ANALYZE`.
    #[must_use]
    pub fn new(operator: impl Into<String>, inner: Box<dyn Operator>) -> Self {
        Self {
            operator: operator.into(),
            inner,
            rows_out: 0,
            batches: 0,
            time_us: 0,
            memory_bytes: 0,
        }
    }
}

impl Operator for ProfiledOperator {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        let started = Instant::now();
        let result = self.inner.next_batch();
        let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.time_us = self.time_us.saturating_add(elapsed);
        if let Ok(Some(batch)) = &result {
            self.rows_out = self
                .rows_out
                .saturating_add(u64::try_from(batch.rows()).unwrap_or(u64::MAX));
            self.batches = self.batches.saturating_add(1);
            self.memory_bytes = self
                .memory_bytes
                .max(u64::try_from(estimate_batch_memory(batch)).unwrap_or(u64::MAX));
        }
        result
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn estimated_row_count(&self) -> Option<usize> {
        self.inner.estimated_row_count()
    }

    fn runtime_profile(&self) -> Option<OperatorRuntimeProfile> {
        let children: Vec<OperatorRuntimeProfile> = self
            .inner
            .profile_children()
            .into_iter()
            .filter_map(Operator::runtime_profile)
            .collect();
        let rows_in = children
            .iter()
            .fold(0_u64, |acc, child| acc.saturating_add(child.rows_out));
        let spill = self.inner.spill_profile();
        Some(OperatorRuntimeProfile {
            operator: self.operator.clone(),
            rows_in,
            rows_out: self.rows_out,
            batches: self.batches,
            time_us: self.time_us,
            memory_bytes: self.memory_bytes,
            spills: spill.spills,
            spill_bytes: spill.bytes,
            io_bytes: self.inner.io_bytes(),
            pruning: self.inner.pruning_stats(),
            children,
        })
    }
}

fn estimate_batch_memory(batch: &Batch) -> usize {
    batch.columns().iter().map(estimate_column_memory).sum()
}

fn estimate_column_memory(column: &Column) -> usize {
    match column {
        Column::Int32(c) => std::mem::size_of_val(c.data()) + bitmap_bytes(c.nulls()),
        Column::Int64(c) => std::mem::size_of_val(c.data()) + bitmap_bytes(c.nulls()),
        Column::Float32(c) => std::mem::size_of_val(c.data()) + bitmap_bytes(c.nulls()),
        Column::Float64(c) => std::mem::size_of_val(c.data()) + bitmap_bytes(c.nulls()),
        Column::Bool(c) => c.data().len() + bitmap_bytes(c.nulls()),
        Column::Utf8(c) => {
            c.values().len() + std::mem::size_of_val(c.offsets()) + bitmap_bytes(c.nulls())
        }
        Column::DictionaryUtf8(c) => {
            std::mem::size_of_val(c.codes.data())
                + c.dict.iter().map(String::len).sum::<usize>()
                + bitmap_bytes(c.codes.nulls())
        }
    }
}

fn bitmap_bytes(bitmap: Option<&ultrasql_vec::Bitmap>) -> usize {
    bitmap.map_or(0, |bits| bits.len().div_ceil(8))
}
