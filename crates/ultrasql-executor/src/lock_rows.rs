//! `LockRows` pass-through operator.
//!
//! [`LockRows`] wraps a child operator and calls a row-lock callback for
//! every row it emits. This implements the `SELECT FOR UPDATE` /
//! `SELECT FOR SHARE` locking semantics at the executor level without
//! modifying the batch data itself.
//!
//! # Pass-through semantics
//!
//! `LockRows` does not alter, filter, or reorder rows. Every batch
//! emitted by the child is forwarded to the caller unchanged after the
//! lock callback has been invoked for each row in the batch.
//!
//! # Callback contract
//!
//! The `lock_row` closure receives a row index (0-based within the batch)
//! and may return `Err(ExecError)` to abort the scan (e.g. when the lock
//! manager detects a deadlock). On `Ok(())` the row is emitted normally.
//!
//! # v0.5 note
//!
//! For v0.5 the lock callback is user-supplied. In production the server
//! will inject a closure backed by `ultrasql_txn::LockManager::lock_tuple`.
//! Tests pass a no-op closure.

#![allow(clippy::type_complexity)]

use ultrasql_core::Schema;
use ultrasql_vec::Batch;

use crate::{ExecError, Operator};

/// Pass-through operator that acquires a row lock per emitted row.
///
/// The `lock_row` callback is called with the batch and the row index for
/// every row before that row is included in the output batch. If the
/// callback returns `Err`, the entire `next_batch` call propagates the
/// error and the stream is terminated.
///
/// # Send
///
/// `LockRows` is `Send` because `Box<dyn Operator>` and `Box<dyn Fn>`
/// (with `Send` bound) are both `Send`.
pub struct LockRows {
    child: Box<dyn Operator>,
    schema: Schema,
    /// Per-row lock callback. `Ok(())` = lock acquired; `Err` = abort.
    lock_fn: Box<dyn FnMut(&Batch, usize) -> Result<(), ExecError> + Send>,
    eof: bool,
}

impl std::fmt::Debug for LockRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockRows")
            .field("schema", &self.schema)
            .field("eof", &self.eof)
            .finish_non_exhaustive()
    }
}

impl LockRows {
    /// Construct a lock-rows operator.
    ///
    /// - `child` — the source operator.
    /// - `lock_fn` — called for every row before it is emitted. Must be
    ///   `Send` so the operator can be moved between threads.
    #[must_use]
    pub fn new(
        child: Box<dyn Operator>,
        lock_fn: Box<dyn FnMut(&Batch, usize) -> Result<(), ExecError> + Send>,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            schema,
            lock_fn,
            eof: false,
        }
    }
}

impl Operator for LockRows {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        let Some(batch) = self.child.next_batch()? else {
            self.eof = true;
            return Ok(None);
        };

        // Call the lock callback for each row in the batch.
        for row_idx in 0..batch.rows() {
            (self.lock_fn)(&batch, row_idx)?;
        }

        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::Batch;
    use ultrasql_vec::column::{Column, NumericColumn};

    use super::LockRows;
    use crate::mem_table_scan::MemTableScan;
    use crate::{ExecError, Operator};

    fn schema_i32() -> Schema {
        Schema::new([Field::required("v", DataType::Int32)]).expect("schema ok")
    }

    fn i32_batch(vals: &[i32]) -> Batch {
        Batch::new([Column::Int32(NumericColumn::from_data(vals.to_vec()))]).expect("batch ok")
    }

    #[test]
    fn lock_rows_calls_callback_per_row() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c2 = Arc::clone(&counter);
        let scan = MemTableScan::new(
            schema_i32(),
            vec![i32_batch(&[1, 2, 3]), i32_batch(&[4, 5])],
        );
        let mut op = LockRows::new(
            Box::new(scan),
            Box::new(move |_batch, _idx| {
                c2.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }),
        );
        while op.next_batch().expect("ok").is_some() {}
        assert_eq!(counter.load(Ordering::Relaxed), 5, "5 rows = 5 lock calls");
    }

    #[test]
    fn lock_rows_passes_batches_through_unchanged() {
        let scan = MemTableScan::new(schema_i32(), vec![i32_batch(&[10, 20])]);
        let mut op = LockRows::new(Box::new(scan), Box::new(|_b, _i| Ok(())));
        let batch = op.next_batch().expect("ok").expect("batch");
        assert_eq!(batch.rows(), 2);
    }

    #[test]
    fn lock_rows_callback_error_propagates() {
        let scan = MemTableScan::new(schema_i32(), vec![i32_batch(&[1, 2])]);
        let mut op = LockRows::new(
            Box::new(scan),
            Box::new(|_b, _i| Err(ExecError::Unsupported("lock conflict"))),
        );
        let err = op.next_batch().expect_err("callback error must propagate");
        assert!(matches!(err, ExecError::Unsupported(_)));
    }
}
