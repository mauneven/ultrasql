//! Column-cache fast path for [`SeqScan`].
//!
//! When a relation's columnar projection is already materialised in
//! `HeapAccess::column_cache`, the scan replays it directly with
//! [`SeqScan::next_batch_from_cache`] instead of walking the heap.
//! Scans over cache-eligible relations also *populate* the cache as a
//! side effect, finalised by [`SeqScan::finalise_cache_build`] on EOF.

use ultrasql_core::{DataType, Schema};
use ultrasql_mvcc::XidStatusOracle;
use ultrasql_storage::PageLoader;
use ultrasql_vec::Batch;
use ultrasql_vec::column::Column;

use super::SeqScan;
use crate::ExecError;
use crate::row_codec::{ColumnBuilder, RowCodec};

/// Cache-read state: a snapshot of cached columns for a relation,
/// plus the row cursor we are streaming from.
pub(super) struct CacheReadState {
    pub(super) columns: std::sync::Arc<ultrasql_storage::column_cache::CachedColumns>,
    pub(super) cursor: usize,
}

impl std::fmt::Debug for CacheReadState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheReadState")
            .field("cursor", &self.cursor)
            .field("rows", &self.columns.columns.first().map(Column::len))
            .finish()
    }
}

/// Cache-build state: parallel column builders that accumulate
/// **every** decoded row across the whole scan (in contrast to the
/// per-batch `builders` field which is swapped out on every batch
/// emit). Finalised and stored in the column cache on EOF.
pub(super) struct CacheBuildState {
    pub(super) builders: Vec<ColumnBuilder>,
    /// Version of the relation when the build started, captured
    /// from `HeapAccess::column_cache.relation_version`. Re-checked
    /// at `put` time so a writer-during-build race drops the entry
    /// on the floor instead of resurrecting stale columns.
    pub(super) target_version: u64,
}

impl std::fmt::Debug for CacheBuildState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheBuildState")
            .field("target_version", &self.target_version)
            .finish()
    }
}

impl<L, O> SeqScan<L, O>
where
    L: PageLoader + Send + Sync + std::fmt::Debug + 'static,
    O: XidStatusOracle + Send + Sync + std::fmt::Debug + 'static,
{
    /// Slice the next batch out of the cached columnar projection.
    ///
    /// Replaces the heap walk + decode loop entirely when the
    /// relation's `ColumnCache` entry is live. Emits wider batches
    /// than the heap-walk path
    /// (`CACHE_REPLAY_BATCH_ROWS`) because the cache already holds
    /// the full columnar projection in memory: larger batches
    /// amortise per-batch operator overhead (filter / select_column
    /// / aggregate). Downstream operators handle variable batch
    /// sizes — the 4096-cap is a soft target for the streaming
    /// heap-scan path, not a hard invariant of the executor.
    pub(super) fn next_batch_from_cache(&mut self) -> Result<Option<Batch>, ExecError> {
        const CACHE_REPLAY_BATCH_ROWS: usize = 1_048_576;

        let Some(state) = self.cache_read.as_mut() else {
            // Should not happen: caller checked `is_some` before
            // calling, but stay defensive.
            return Ok(None);
        };
        let total_rows = state.columns.columns.first().map(Column::len).unwrap_or(0);
        if state.cursor >= total_rows {
            self.eof = true;
            self.cache_read = None;
            return Ok(None);
        }
        let end = (state.cursor + CACHE_REPLAY_BATCH_ROWS).min(total_rows);
        let mut batch_cols: Vec<Column> = Vec::with_capacity(state.columns.columns.len());
        for col in &state.columns.columns {
            batch_cols.push(slice_column(col, state.cursor, end)?);
        }
        state.cursor = end;
        let batch = Batch::new(batch_cols).map_err(ExecError::from)?;
        Ok(Some(batch))
    }

    /// Move the accumulator builders into the relation's
    /// [`ColumnCache`] entry. No-op when `cache_build` is `None`
    /// (TID-prefixed scan or a scan that already started from a
    /// live cache entry).
    pub(super) fn finalise_cache_build(&mut self) {
        let Some(build) = self.cache_build.take() else {
            return;
        };
        // Build the final `Vec<Column>` from the accumulator
        // builders. Any per-builder finish error means the cache
        // is unbuildable for this scan — drop the build silently;
        // the next scan over the same relation will retry.
        let finished_batch = match RowCodec::finish_batch(build.builders) {
            Ok(b) => b,
            Err(_) => return,
        };
        let columns: Vec<Column> = finished_batch.columns().to_vec();
        let entry = ultrasql_storage::column_cache::CachedColumns::new(
            build.target_version,
            self.codec.schema().clone(),
            columns,
        );
        self.heap.column_cache.put(self.relation, entry);
    }
}

/// Slice rows `[start, end)` out of `col` into an owned [`Column`].
///
/// This is a zero-conditional clone of the underlying typed
/// `Vec<T>` for fixed-width columns and an offsets+values clone
/// for the variable-width `Utf8` arm. Used by the column-cache
/// fast path to materialise a batch from cached data without
/// re-decoding from heap bytes.
pub(super) fn slice_column(col: &Column, start: usize, end: usize) -> Result<Column, ExecError> {
    use ultrasql_vec::bitmap::Bitmap;
    use ultrasql_vec::column::NumericColumn;

    fn slice_nulls(nulls: Option<&Bitmap>, start: usize, end: usize) -> Option<Bitmap> {
        nulls.map(|b| {
            let mut out = Bitmap::new(end - start, false);
            for (i, src) in (start..end).enumerate() {
                out.set(i, b.get(src));
            }
            out
        })
    }

    fn nullable_numeric<T>(
        data: Vec<T>,
        nulls: Option<Bitmap>,
        wrap: impl FnOnce(NumericColumn<T>) -> Column,
    ) -> Result<Column, ExecError> {
        match nulls {
            Some(n) => NumericColumn::with_nulls(data, n)
                .map(wrap)
                .map_err(|_| ExecError::Internal("cached column null bitmap length mismatch")),
            None => Ok(wrap(NumericColumn::from_data(data))),
        }
    }

    match col {
        Column::Int32(c) => {
            let data = c.data()[start..end].to_vec();
            let nulls = slice_nulls(c.nulls(), start, end);
            nullable_numeric(data, nulls, Column::Int32)
        }
        Column::Int64(c) => {
            let data = c.data()[start..end].to_vec();
            let nulls = slice_nulls(c.nulls(), start, end);
            nullable_numeric(data, nulls, Column::Int64)
        }
        Column::Float32(c) => {
            let data = c.data()[start..end].to_vec();
            let nulls = slice_nulls(c.nulls(), start, end);
            nullable_numeric(data, nulls, Column::Float32)
        }
        Column::Float64(c) => {
            let data = c.data()[start..end].to_vec();
            let nulls = slice_nulls(c.nulls(), start, end);
            nullable_numeric(data, nulls, Column::Float64)
        }
        // Bool / Utf8 cache slicing is intentionally not
        // implemented: `schema_all_fixed_numeric` keeps these out of
        // the cache-eligible set, so this arm is unreachable in
        // practice. Surfacing it as a panic catches a future
        // regression where the eligibility check is loosened
        // without finishing the slice paths.
        Column::Bool(_) | Column::Utf8(_) | Column::DictionaryUtf8(_) => {
            Err(ExecError::TypeMismatch(
                "column cache supports only fixed-width numeric columns".to_owned(),
            ))
        }
    }
}

/// `true` iff every column in `schema` is a fixed-width numeric
/// type. Used to gate column-cache eligibility — the slice path
/// only supports `Int16` / `Int32` / `Int64` / `Float32` / `Float64`
/// at the moment.
pub(super) fn schema_all_fixed_numeric(schema: &Schema) -> bool {
    schema.fields().iter().all(|f| {
        matches!(
            f.data_type.storage_type(),
            DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Oid
                | DataType::RegClass
                | DataType::RegType
                | DataType::Float32
                | DataType::Float64
        )
    })
}
