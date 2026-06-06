//! Bitmap heap scan operator (v0.6 closeout).
//!
//! [`BitmapIndexScan`] emits a TID bitmap (a `Vec<u64>` of 64-bit row IDs)
//! from an in-memory index. [`BitmapHeapScan`] consumes that bitmap and
//! returns the matching rows as batches.
//!
//! ## Design
//!
//! This is a scaffold implementation for v0.6.  The bitmap is represented as a
//! `Vec<u64>` of dense 64-row-per-word bitmap pages.  Each set bit at word `w`
//! and position `b` corresponds to the row at index `w * 64 + b` in the
//! underlying table.
//!
//! In production the heap would be fetched via `HeapAccess`; here we operate
//! over an in-memory row store for testing purposes.  The indexing API matches
//! what the optimizer's physical-selection stage produces.
//!
//! ## Multi-index AND/OR
//!
//! When multiple bitmap index scans are combined with AND or OR, the bitmaps
//! are merged word-by-word before the heap fetch, which is more cache-friendly
//! than sorting and merging TID lists.

#![allow(clippy::type_complexity)]
#![allow(clippy::needless_collect)]

use std::fmt;

use ultrasql_core::{Schema, Value};
use ultrasql_vec::Batch;

use crate::{ExecError, Operator};

// ============================================================================
// TidBitmap
// ============================================================================

/// Compact row-selection bitmap.
///
/// Word `i` covers rows `[i*64 .. (i+1)*64)`. A set bit means the row at
/// that position passed the index scan predicate.
#[derive(Clone, Debug, Default)]
pub struct TidBitmap {
    words: Vec<u64>,
    /// Total number of rows the bitmap was built over.
    capacity: usize,
}

impl TidBitmap {
    /// Allocate an all-zero bitmap for `capacity` rows.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            words: vec![0_u64; capacity.div_ceil(64)],
            capacity,
        }
    }

    /// Set the bit for row `i`.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::TypeMismatch`] if `i >= capacity`.
    pub fn set(&mut self, i: usize) -> Result<(), ExecError> {
        if i >= self.capacity {
            return Err(ExecError::TypeMismatch(format!(
                "TidBitmap: row index {i} out of range for capacity {}",
                self.capacity
            )));
        }
        self.words[i / 64] |= 1_u64 << (i % 64);
        Ok(())
    }

    /// Test bit `i`.
    ///
    /// Returns `false` when `i >= capacity`.
    #[must_use]
    pub fn get(&self, i: usize) -> bool {
        if i >= self.capacity {
            return false;
        }
        (self.words[i / 64] >> (i % 64)) & 1 == 1
    }

    /// Merge with `other` using bitwise OR (union of matching rows).
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::TypeMismatch`] when the two bitmaps have different
    /// capacities.
    pub fn or_merge(&mut self, other: &Self) -> Result<(), ExecError> {
        ensure_same_capacity("TidBitmap::or_merge", self.capacity, other.capacity)?;
        for (w, &v) in self.words.iter_mut().zip(other.words.iter()) {
            *w |= v;
        }
        Ok(())
    }

    /// Merge with `other` using bitwise AND (intersection of matching rows).
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::TypeMismatch`] when the two bitmaps have different
    /// capacities.
    pub fn and_merge(&mut self, other: &Self) -> Result<(), ExecError> {
        ensure_same_capacity("TidBitmap::and_merge", self.capacity, other.capacity)?;
        for (w, &v) in self.words.iter_mut().zip(other.words.iter()) {
            *w &= v;
        }
        Ok(())
    }

    /// Iterate over set row indices in ascending order.
    pub fn iter_ones(&self) -> impl Iterator<Item = usize> + '_ {
        TidBitmapIter {
            words: &self.words,
            capacity: self.capacity,
            word_idx: 0,
            current: self.words.first().copied().unwrap_or(0),
        }
    }
}

fn ensure_same_capacity(
    operation: &'static str,
    left: usize,
    right: usize,
) -> Result<(), ExecError> {
    if left == right {
        return Ok(());
    }
    Err(ExecError::TypeMismatch(format!(
        "{operation}: capacity mismatch ({left} != {right})"
    )))
}

fn trailing_bit_index(word: u64) -> usize {
    debug_assert_ne!(word, 0);
    match usize::try_from(word.trailing_zeros()) {
        Ok(bit) => bit,
        Err(_) => unreachable!("u64 trailing-zero count must fit usize"),
    }
}

struct TidBitmapIter<'a> {
    words: &'a [u64],
    capacity: usize,
    word_idx: usize,
    current: u64,
}

impl Iterator for TidBitmapIter<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        loop {
            if self.current != 0 {
                let bit = trailing_bit_index(self.current);
                let i = self.word_idx * 64 + bit;
                self.current &= self.current - 1;
                if i < self.capacity {
                    return Some(i);
                }
                return None;
            }
            self.word_idx += 1;
            if self.word_idx >= self.words.len() {
                return None;
            }
            self.current = self.words[self.word_idx];
        }
    }
}

// ============================================================================
// BitmapIndexScan
// ============================================================================

/// A mock bitmap index scan that filters rows using a predicate closure.
///
/// In production this would query a B-tree index via `HeapAccess`. For v0.6
/// the implementation applies `predicate` to a row store and sets bits for
/// matching rows.
pub struct BitmapIndexScan {
    rows: Vec<Vec<Value>>,
    predicate: Box<dyn Fn(&[Value]) -> bool + Send>,
}

impl fmt::Debug for BitmapIndexScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BitmapIndexScan")
            .field("rows", &self.rows.len())
            .finish_non_exhaustive()
    }
}

impl BitmapIndexScan {
    /// Construct a bitmap index scan.
    ///
    /// - `rows`      — the in-memory row store.
    /// - `predicate` — returns `true` for rows that the index scan would match.
    pub fn new(rows: Vec<Vec<Value>>, predicate: Box<dyn Fn(&[Value]) -> bool + Send>) -> Self {
        Self { rows, predicate }
    }

    /// Run the index scan and return a `TidBitmap` of matching rows.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::TypeMismatch`] if bitmap construction observes an
    /// impossible row index outside the bitmap capacity.
    pub fn scan(&self) -> Result<TidBitmap, ExecError> {
        let n = self.rows.len();
        let mut bm = TidBitmap::new(n);
        for (i, row) in self.rows.iter().enumerate() {
            if (self.predicate)(row) {
                bm.set(i)?;
            }
        }
        Ok(bm)
    }
}

// ============================================================================
// BitmapHeapScan
// ============================================================================

/// Bitmap heap scan operator.
///
/// Given a [`TidBitmap`] (from one or more combined [`BitmapIndexScan`]s),
/// fetches the matching rows from the heap and emits them as batches.
///
/// The visibility map bit is not checked in this v0.6 implementation; every
/// row in the bitmap is fetched from the heap.
pub struct BitmapHeapScan {
    rows: Vec<Vec<Value>>,
    bitmap: TidBitmap,
    schema: Schema,
    /// Iterator position across the bitmap's set bits.
    iter_pos: Option<Box<dyn Iterator<Item = usize> + Send>>,
    eof: bool,
}

impl fmt::Debug for BitmapHeapScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BitmapHeapScan")
            .field("rows", &self.rows.len())
            .field("eof", &self.eof)
            .finish_non_exhaustive()
    }
}

impl BitmapHeapScan {
    /// Construct a bitmap heap scan.
    ///
    /// - `rows`   — the in-memory row store (all rows, including non-matching ones).
    /// - `bitmap` — the TID bitmap from the upstream [`BitmapIndexScan`].
    /// - `schema` — schema of the output rows.
    #[must_use]
    pub fn new(rows: Vec<Vec<Value>>, bitmap: TidBitmap, schema: Schema) -> Self {
        Self {
            rows,
            bitmap,
            schema,
            iter_pos: None,
            eof: false,
        }
    }
}

const BATCH_TARGET_ROWS: usize = 4096;

impl Operator for BitmapHeapScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        // Materialise the set-bits iterator on the first call.
        // The collect is intentional: the iterator borrows self.bitmap so it
        // cannot be stored directly in self.iter_pos without a self-referential
        // struct.
        if self.iter_pos.is_none() {
            let indices: Vec<usize> = self.bitmap.iter_ones().collect();
            self.iter_pos = Some(Box::new(indices.into_iter()));
        }

        let iter = self.iter_pos.as_mut().ok_or(ExecError::Internal(
            "bitmap heap scan iterator missing after initialization",
        ))?;
        let mut chunk: Vec<Vec<Value>> = Vec::with_capacity(BATCH_TARGET_ROWS);
        for idx in iter.by_ref().take(BATCH_TARGET_ROWS) {
            if idx < self.rows.len() {
                chunk.push(self.rows[idx].clone());
            }
        }

        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }

        crate::seq_scan::build_batch(&chunk, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ============================================================================
// IndexOnlyScan
// ============================================================================

/// Index-only scan operator.
///
/// When the visibility map (VM) indicates that all tuples on a page are
/// visible (all-visible bit = true), the heap fetch can be skipped and the
/// answer can be returned from the index entry alone.
///
/// In this v0.6 implementation the VM is represented as a `Vec<bool>` where
/// element `i` is `true` if row `i` is all-visible. When the VM bit is set,
/// the row is returned directly from the index entry without a heap fetch.
/// When the VM bit is clear, the row is fetched from the heap store.
pub struct IndexOnlyScan {
    /// Index entries: (key, `row_id`, projected columns).
    index_entries: Vec<Vec<Value>>,
    /// Visibility map: `true` means row is all-visible (skip heap fetch).
    vm: Vec<bool>,
    /// Heap rows (only needed for non-all-visible rows).
    heap_rows: Vec<Vec<Value>>,
    schema: Schema,
    pos: usize,
    eof: bool,
}

impl fmt::Debug for IndexOnlyScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexOnlyScan")
            .field("entries", &self.index_entries.len())
            .field("eof", &self.eof)
            .finish_non_exhaustive()
    }
}

impl IndexOnlyScan {
    /// Construct an index-only scan.
    ///
    /// - `index_entries` — pre-projected rows from the index (covers all
    ///   requested columns; aligned with `vm` and `heap_rows` by row id).
    /// - `vm`            — per-row visibility-map bits.
    /// - `heap_rows`     — fallback heap rows when the VM bit is clear.
    /// - `schema`        — output schema.
    #[must_use]
    pub const fn new(
        index_entries: Vec<Vec<Value>>,
        vm: Vec<bool>,
        heap_rows: Vec<Vec<Value>>,
        schema: Schema,
    ) -> Self {
        Self {
            index_entries,
            vm,
            heap_rows,
            schema,
            pos: 0,
            eof: false,
        }
    }
}

impl Operator for IndexOnlyScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        let n = self.index_entries.len();
        let mut chunk: Vec<Vec<Value>> = Vec::with_capacity(BATCH_TARGET_ROWS);

        while self.pos < n && chunk.len() < BATCH_TARGET_ROWS {
            let i = self.pos;
            self.pos += 1;
            // If VM bit is set, return the index entry directly.
            let row = if *self.vm.get(i).unwrap_or(&false) {
                self.index_entries[i].clone()
            } else {
                // Heap fetch required.
                if i < self.heap_rows.len() {
                    self.heap_rows[i].clone()
                } else {
                    self.index_entries[i].clone()
                }
            };
            chunk.push(row);
        }

        if chunk.is_empty() {
            self.eof = true;
            return Ok(None);
        }

        crate::seq_scan::build_batch(&chunk, &self.schema).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_vec::column::Column;

    use super::*;
    use crate::Operator;

    fn schema_id_val() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::required("val", DataType::Int64),
        ])
        .expect("schema ok")
    }

    fn rows(n: usize) -> Vec<Vec<Value>> {
        (0..n)
            .map(|i| {
                vec![
                    Value::Int32(i32::try_from(i).unwrap_or(0)),
                    Value::Int64(i64::try_from(i).unwrap_or(0) * 10),
                ]
            })
            .collect()
    }

    // ---- TidBitmap ----

    #[test]
    fn tid_bitmap_set_and_get() {
        let mut bm = TidBitmap::new(128);
        bm.set(0).expect("set row 0");
        bm.set(63).expect("set row 63");
        bm.set(64).expect("set row 64");
        bm.set(127).expect("set row 127");
        assert!(bm.get(0));
        assert!(bm.get(63));
        assert!(bm.get(64));
        assert!(bm.get(127));
        assert!(!bm.get(1));
        assert!(!bm.get(126));
    }

    #[test]
    fn tid_bitmap_iter_ones_ascending() {
        let mut bm = TidBitmap::new(200);
        for i in [5_usize, 13, 64, 100, 199] {
            bm.set(i).expect("set row");
        }
        let got: Vec<usize> = bm.iter_ones().collect();
        assert_eq!(got, vec![5, 13, 64, 100, 199]);
    }

    #[test]
    fn tid_bitmap_rejects_out_of_range_set_without_panic() {
        let mut bm = TidBitmap::new(2);
        let err = bm.set(2).expect_err("out of range row must error");
        assert!(
            err.to_string().contains("row index 2 out of range"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn tid_bitmap_or_merge() {
        let mut a = TidBitmap::new(64);
        let mut b = TidBitmap::new(64);
        a.set(1).expect("set a");
        b.set(3).expect("set b");
        a.or_merge(&b).expect("or merge");
        assert!(a.get(1));
        assert!(a.get(3));
    }

    #[test]
    fn tid_bitmap_and_merge() {
        let mut a = TidBitmap::new(64);
        let mut b = TidBitmap::new(64);
        a.set(1).expect("set a1");
        a.set(2).expect("set a2");
        b.set(2).expect("set b2");
        b.set(3).expect("set b3");
        a.and_merge(&b).expect("and merge");
        assert!(!a.get(1));
        assert!(a.get(2));
        assert!(!a.get(3));
    }

    #[test]
    fn tid_bitmap_rejects_capacity_mismatch_merge_without_panic() {
        let mut a = TidBitmap::new(64);
        let b = TidBitmap::new(65);

        let or_err = a.or_merge(&b).expect_err("or capacity mismatch must error");
        assert!(
            or_err.to_string().contains("capacity mismatch"),
            "unexpected error: {or_err}"
        );
        let and_err = a
            .and_merge(&b)
            .expect_err("and capacity mismatch must error");
        assert!(
            and_err.to_string().contains("capacity mismatch"),
            "unexpected error: {and_err}"
        );
    }

    // ---- BitmapIndexScan + BitmapHeapScan ----

    #[test]
    fn bitmap_heap_scan_over_10k_rows_two_indexes() {
        let all_rows = rows(10_000);
        // Index 1: rows where id % 3 == 0
        let scan1 = BitmapIndexScan::new(
            all_rows.clone(),
            Box::new(|row| matches!(&row[0], Value::Int32(v) if v % 3 == 0)),
        );
        // Index 2: rows where id % 5 == 0
        let scan2 = BitmapIndexScan::new(
            all_rows.clone(),
            Box::new(|row| matches!(&row[0], Value::Int32(v) if v % 5 == 0)),
        );
        let mut bm1 = scan1.scan().expect("scan 1");
        let bm2 = scan2.scan().expect("scan 2");
        // Combine with OR (rows divisible by 3 OR by 5).
        bm1.or_merge(&bm2).expect("or merge");

        let mut heap_scan = BitmapHeapScan::new(all_rows, bm1, schema_id_val());

        let mut total = 0_usize;
        let mut all_ids: Vec<i32> = Vec::new();
        while let Some(batch) = heap_scan.next_batch().unwrap() {
            total += batch.rows();
            match &batch.columns()[0] {
                Column::Int32(c) => all_ids.extend_from_slice(c.data()),
                other => panic!("unexpected {other:?}"),
            }
        }

        // Expected: union of (0..10000 divisible by 3) and (0..10000 divisible by 5)
        let expected_count = (0..10_000_i32).filter(|i| i % 3 == 0 || i % 5 == 0).count();
        assert_eq!(total, expected_count, "total rows mismatch");
        // Verify all returned IDs satisfy the predicate
        for id in &all_ids {
            assert!(id % 3 == 0 || id % 5 == 0, "id {id} fails predicate");
        }
    }

    #[test]
    fn bitmap_heap_scan_emits_in_batches_of_4096() {
        let all_rows = rows(5_000);
        let scan = BitmapIndexScan::new(
            all_rows.clone(),
            Box::new(|_| true), // all rows
        );
        let bm = scan.scan().expect("scan");
        let mut heap_scan = BitmapHeapScan::new(all_rows, bm, schema_id_val());
        let mut batch_sizes = Vec::new();
        while let Some(b) = heap_scan.next_batch().unwrap() {
            batch_sizes.push(b.rows());
        }
        assert!(batch_sizes.contains(&4096));
        assert_eq!(batch_sizes.iter().sum::<usize>(), 5_000);
    }

    // ---- IndexOnlyScan ----

    #[test]
    fn index_only_scan_skips_heap_when_vm_set() {
        let n = 10;
        let index_entries = rows(n);
        let heap_rows = rows(n); // different values not needed; vm=true short-circuits
        // All rows visible.
        let vm = vec![true; n];
        let schema = schema_id_val();
        let mut scan = IndexOnlyScan::new(index_entries, vm, heap_rows, schema);

        let mut total = 0;
        while let Some(b) = scan.next_batch().unwrap() {
            total += b.rows();
        }
        assert_eq!(total, n);
    }

    #[test]
    fn index_only_scan_fetches_heap_when_vm_clear() {
        // Row 5 has VM=false; we override the heap row to have val=9999.
        let n = 10;
        let mut heap_rows = rows(n);
        heap_rows[5] = vec![Value::Int32(5), Value::Int64(9999)];
        let index_entries = rows(n); // index says val=50 for row 5
        let mut vm = vec![true; n];
        vm[5] = false; // row 5 must be fetched from heap

        let schema = schema_id_val();
        let mut scan = IndexOnlyScan::new(index_entries, vm, heap_rows, schema);

        let mut all_vals: Vec<i64> = Vec::new();
        while let Some(b) = scan.next_batch().unwrap() {
            match &b.columns()[1] {
                Column::Int64(c) => all_vals.extend_from_slice(c.data()),
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(all_vals[5], 9999, "row 5 should come from the heap");
    }
}
