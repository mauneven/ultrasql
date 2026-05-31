//! Per-query work memory budget.
//!
//! [`WorkMemBudget`] tracks how many bytes of scratch memory a query is
//! allowed to allocate inside memory-heavy operators (Sort, `TopK`,
//! `HashAggregate`, `HashJoin`). When a reservation request would push usage
//! above the configured limit, the caller receives [`ExecError::Unsupported`]
//! (or triggers spill, depending on the operator).
//!
//! # Design
//!
//! The budget is backed by a single `AtomicU64` so all clones of the
//! surrounding `Arc<WorkMemBudget>` share the same byte counter across
//! operator threads. Reservations are returned via [`WorkMemReservation`],
//! which releases the bytes in its `Drop` implementation — ensuring
//! that temporary allocations are always returned even when an operator
//! errors early.
//!
//! # `temp_file_limit`
//!
//! The separate `temp_file_limit` constant caps how many bytes one spill
//! writer may put into its temp file. This is an advisory limit checked at
//! spill time, not enforced with atomics.
//! The limit is a constant today; a configurable GUC arrives in a later
//! storage configuration slice.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::ExecError;

/// Advisory limit on spill temp files per query (256 MiB).
///
/// Callers check this before opening a new temp file run. A future GUC
/// will make this per-session-configurable.
pub const TEMP_FILE_LIMIT_BYTES: u64 = 256 * 1024 * 1024;

/// Returns the current `temp_file_limit` in bytes.
///
/// Always returns [`TEMP_FILE_LIMIT_BYTES`] in v0.5.
#[must_use]
pub const fn temp_file_limit() -> u64 {
    TEMP_FILE_LIMIT_BYTES
}

/// Per-query work memory budget.
///
/// Shared via `Arc<WorkMemBudget>` across operators that need scratch
/// memory (Sort, `TopK`, `HashAggregate`, `HashJoin`). The budget is enforced
/// cooperatively: each operator must call `reserve` before allocating or use
/// the limit to choose a spill path.
///
/// # Send + Sync
///
/// `WorkMemBudget` is `Send + Sync` because all mutable state goes through
/// the `AtomicU64` counter; no non-atomic interior mutability exists.
#[derive(Debug)]
pub struct WorkMemBudget {
    limit_bytes: u64,
    used: AtomicU64,
}

impl WorkMemBudget {
    /// Create a new budget capped at `limit_bytes`.
    ///
    /// A limit of `0` means no memory is ever available. Pass `u64::MAX`
    /// for effectively unlimited budgets in tests.
    #[must_use]
    pub const fn new(limit_bytes: u64) -> Self {
        Self {
            limit_bytes,
            used: AtomicU64::new(0),
        }
    }

    /// Attempt to reserve `bytes` from the budget.
    ///
    /// On success returns a [`WorkMemReservation`] whose `Drop` releases
    /// the bytes back to the budget.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::Unsupported`] when the reservation would
    /// exceed the configured limit. Callers should treat this as a signal
    /// to spill to disk rather than a fatal error.
    pub fn reserve(&self, bytes: u64) -> Result<WorkMemReservation<'_>, ExecError> {
        // Compare-and-swap loop: only succeeds if the post-add total stays
        // within the limit.
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            let Some(new_total) = current.checked_add(bytes) else {
                return Err(ExecError::Unsupported(
                    "work_mem budget exceeded; operator must spill",
                ));
            };
            if new_total > self.limit_bytes {
                return Err(ExecError::Unsupported(
                    "work_mem budget exceeded; operator must spill",
                ));
            }
            match self.used.compare_exchange_weak(
                current,
                new_total,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(WorkMemReservation {
                        budget: self,
                        bytes,
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Current bytes in use across all live reservations.
    #[must_use]
    pub fn used_bytes(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    /// Maximum bytes available to this budget.
    #[must_use]
    pub const fn limit_bytes(&self) -> u64 {
        self.limit_bytes
    }
}

/// RAII guard returned by [`WorkMemBudget::reserve`].
///
/// Dropping this releases the reserved bytes back to the budget.
///
/// # Send + Sync
///
/// The reservation borrows the budget by shared reference and uses
/// atomic operations, so it is `Send` and `Sync`.
pub struct WorkMemReservation<'a> {
    budget: &'a WorkMemBudget,
    bytes: u64,
}

impl std::fmt::Debug for WorkMemReservation<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkMemReservation")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for WorkMemReservation<'_> {
    fn drop(&mut self) {
        // Saturating sub is safe: the reservation was added atomically so
        // used >= self.bytes at this point under normal operation.
        let _ = self
            .budget
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |prev| {
                Some(prev.saturating_sub(self.bytes))
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_within_limit_succeeds() {
        let budget = WorkMemBudget::new(1024);
        let r = budget.reserve(512).expect("should succeed");
        assert_eq!(budget.used_bytes(), 512);
        drop(r);
        assert_eq!(budget.used_bytes(), 0, "reservation released on drop");
    }

    #[test]
    fn reserve_exceeding_limit_returns_error() {
        let budget = WorkMemBudget::new(1024);
        let err = budget.reserve(1025).expect_err("must fail");
        assert!(matches!(err, ExecError::Unsupported(_)));
    }

    #[test]
    fn reserve_rejects_counter_overflow_even_with_max_limit() {
        let budget = WorkMemBudget::new(u64::MAX);
        let first = budget.reserve(u64::MAX).expect("max reservation fits");

        let err = budget
            .reserve(1)
            .expect_err("counter overflow must not saturate");
        assert!(matches!(err, ExecError::Unsupported(_)));
        assert_eq!(budget.used_bytes(), u64::MAX);

        drop(first);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn multiple_reservations_accumulate() {
        let budget = WorkMemBudget::new(1000);
        let r1 = budget.reserve(400).expect("first ok");
        let r2 = budget.reserve(400).expect("second ok");
        assert_eq!(budget.used_bytes(), 800);
        let err = budget.reserve(201).expect_err("third must fail");
        assert!(matches!(err, ExecError::Unsupported(_)));
        drop(r1);
        drop(r2);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn temp_file_limit_returns_constant() {
        assert_eq!(temp_file_limit(), TEMP_FILE_LIMIT_BYTES);
    }
}
