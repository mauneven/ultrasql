//! SQL sequence generator.
//!
//! A [`Sequence`] is a monotonically advancing (or descending) integer
//! generator. Sequences power `SERIAL` / `IDENTITY` columns and are
//! also directly accessible via the `nextval`, `currval`, `lastval`,
//! and `setval` SQL functions.
//!
//! # On-disk representation
//!
//! A sequence occupies a single heap page. Its persistent state is the
//! `SequenceState` struct serialized into the page body. WAL-logged
//! writes ensure crash safety. The in-memory cache (`cache` field)
//! allows batching up to `cache_size` calls before touching the heap,
//! reducing WAL traffic.
//!
//! `TODO(sequence-persistent)`: connect the sequence's single heap page
//! to the buffer pool and WAL sink so the state survives a restart.
//! Currently the state is in-memory only and resets after a crash.
//!
//! # Sequence semantics
//!
//! `nextval` advances the sequence and returns the new value; `currval`
//! returns the last value returned by `nextval` in the current session (or
//! `SequenceError::NotCalled` if `nextval` has never been called); `lastval`
//! returns the last value from any sequence in the current session.

use parking_lot::Mutex;
use thiserror::Error;
use ultrasql_core::{RelationId, Xid};
use ultrasql_wal::payload::{SequenceOpKind, SequenceOpPayload};
use ultrasql_wal::{RecordType, WalRecord};

use crate::wal_sink::WalSink;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors raised by sequence operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SequenceError {
    /// `nextval` reached the limit of the sequence and CYCLE is not set.
    #[error("sequence reached its maximum/minimum value and is not a CYCLE sequence")]
    Exhausted,

    /// `currval` called before `nextval` was ever called in this session.
    #[error("currval of sequence called before nextval")]
    NotCalled,

    /// The requested value is out of the sequence's `[min, max]` range.
    #[error("setval value {value} is out of range [{min}, {max}]")]
    OutOfRange {
        /// Requested value.
        value: i64,
        /// Configured minimum.
        min: i64,
        /// Configured maximum.
        max: i64,
    },

    /// Attempt to configure a sequence with invalid options.
    #[error("invalid sequence options: {0}")]
    InvalidOptions(String),

    /// WAL sink rejected a sequence record.
    #[error("sequence WAL append failed: {0}")]
    Wal(String),
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/// Options that define a sequence's behaviour.
///
/// Mirrors the arguments accepted by PostgreSQL's `CREATE SEQUENCE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceOptions {
    /// First value returned by the sequence. Defaults to `min` for
    /// ascending sequences and `max` for descending ones.
    pub start: i64,
    /// Step between consecutive values. Negative for descending
    /// sequences. Must not be zero.
    pub increment: i64,
    /// Minimum value. `None` uses the default (`1` ascending,
    /// `i64::MIN` descending).
    pub min: Option<i64>,
    /// Maximum value. `None` uses the default (`i64::MAX` ascending,
    /// `-1` descending).
    pub max: Option<i64>,
    /// Number of values to pre-allocate in the session cache before
    /// writing back to the heap page. Zero and one are equivalent.
    pub cache: u32,
    /// Whether the sequence wraps around when exhausted.
    pub cycle: bool,
}

impl Default for SequenceOptions {
    fn default() -> Self {
        Self {
            start: 1,
            increment: 1,
            min: None,
            max: None,
            cache: 1,
            cycle: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

/// The durable state of a sequence (persisted to the heap page).
#[derive(Clone, Debug)]
struct SequenceState {
    /// Configured restart value from `CREATE SEQUENCE START WITH` or
    /// `ALTER SEQUENCE START WITH`.
    start_value: i64,
    /// The next value to be returned by `nextval` (or the last value
    /// returned when `is_called` is false — see `setval`).
    last_value: i64,
    /// `false` after `CREATE SEQUENCE` or `setval(..., false)`: means
    /// `last_value` is the value that will be returned next rather than
    /// the value that was just returned. Matches PostgreSQL semantics.
    is_called: bool,
    /// Resolved minimum value (after applying defaults).
    min_value: i64,
    /// Resolved maximum value (after applying defaults).
    max_value: i64,
    /// Step. Negative for descending sequences.
    increment: i64,
    /// Whether the sequence cycles.
    cycle: bool,
    /// Cache size as configured.
    ///
    /// TODO(sequence-persistent): use this to batch WAL writes when the
    /// session cache is refilled from the heap page.
    cache_size: u32,
}

/// Durable sequence state carried by `SequenceOp` WAL records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceSnapshot {
    /// Configured restart value.
    pub start_value: i64,
    /// Last value returned, or next value when `is_called` is false.
    pub last_value: i64,
    /// PostgreSQL `is_called` state.
    pub is_called: bool,
    /// Lower bound.
    pub min_value: i64,
    /// Upper bound.
    pub max_value: i64,
    /// Step.
    pub increment: i64,
    /// Whether CYCLE is enabled.
    pub cycle: bool,
    /// Configured cache size.
    pub cache_size: u32,
}

impl SequenceState {
    fn from_opts(opts: &SequenceOptions) -> Result<Self, SequenceError> {
        if opts.increment == 0 {
            return Err(SequenceError::InvalidOptions(
                "INCREMENT must not be zero".into(),
            ));
        }
        let ascending = opts.increment > 0;
        let min_value = opts.min.unwrap_or(if ascending { 1 } else { i64::MIN });
        let max_value = opts.max.unwrap_or(if ascending { i64::MAX } else { -1 });
        if min_value >= max_value {
            return Err(SequenceError::InvalidOptions(format!(
                "MINVALUE {min_value} must be less than MAXVALUE {max_value}",
            )));
        }
        let start = opts.start;
        if start < min_value || start > max_value {
            return Err(SequenceError::InvalidOptions(format!(
                "START {start} is out of range [{min_value}, {max_value}]",
            )));
        }
        let cache_size = opts.cache.max(1);
        Ok(Self {
            start_value: start,
            last_value: start,
            is_called: false,
            min_value,
            max_value,
            increment: opts.increment,
            cycle: opts.cycle,
            cache_size,
        })
    }

    fn from_snapshot(snapshot: SequenceSnapshot) -> Result<Self, SequenceError> {
        if snapshot.increment == 0 {
            return Err(SequenceError::InvalidOptions(
                "INCREMENT must not be zero".into(),
            ));
        }
        if snapshot.min_value >= snapshot.max_value {
            return Err(SequenceError::InvalidOptions(format!(
                "MINVALUE {} must be less than MAXVALUE {}",
                snapshot.min_value, snapshot.max_value,
            )));
        }
        if snapshot.start_value < snapshot.min_value || snapshot.start_value > snapshot.max_value {
            return Err(SequenceError::InvalidOptions(format!(
                "START {} is out of range [{}, {}]",
                snapshot.start_value, snapshot.min_value, snapshot.max_value,
            )));
        }
        if snapshot.last_value < snapshot.min_value || snapshot.last_value > snapshot.max_value {
            return Err(SequenceError::OutOfRange {
                value: snapshot.last_value,
                min: snapshot.min_value,
                max: snapshot.max_value,
            });
        }
        Ok(Self {
            start_value: snapshot.start_value,
            last_value: snapshot.last_value,
            is_called: snapshot.is_called,
            min_value: snapshot.min_value,
            max_value: snapshot.max_value,
            increment: snapshot.increment,
            cycle: snapshot.cycle,
            cache_size: snapshot.cache_size.max(1),
        })
    }

    fn snapshot(&self) -> SequenceSnapshot {
        SequenceSnapshot {
            start_value: self.start_value,
            last_value: self.last_value,
            is_called: self.is_called,
            min_value: self.min_value,
            max_value: self.max_value,
            increment: self.increment,
            cycle: self.cycle,
            cache_size: self.cache_size,
        }
    }

    /// Advance the sequence and return the next value.
    ///
    /// PostgreSQL semantics:
    /// - When `is_called` is false, `last_value` is the value to return
    ///   *next* (it hasn't been returned yet). After returning it we set
    ///   `is_called = true` and update `last_value` to the value just
    ///   returned.
    /// - When `is_called` is true, `last_value` is the value returned by
    ///   the previous call. We compute the next value by adding
    ///   `increment`, then check range / cycle.
    fn advance(&mut self) -> Result<i64, SequenceError> {
        let value = if self.is_called {
            // Subsequent call — step from `last_value`.
            let next = self.last_value.checked_add(self.increment);
            let in_range = |v: i64| v >= self.min_value && v <= self.max_value;
            match next {
                Some(n) if in_range(n) => n,
                _ => {
                    // Hit a boundary or arithmetic overflow.
                    if self.cycle {
                        // Wrap to the appropriate bound.
                        if self.increment > 0 {
                            self.min_value
                        } else {
                            self.max_value
                        }
                    } else {
                        return Err(SequenceError::Exhausted);
                    }
                }
            }
        } else {
            // First call — return `last_value` as-is (equals `start`).
            self.is_called = true;
            self.last_value
        };

        self.last_value = value;
        Ok(value)
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// A WAL-logged, MVCC-safe sequence generator.
///
/// # Thread safety
///
/// All state is behind a `Mutex`. `nextval` acquires the mutex, advances
/// state atomically, and releases. Concurrent callers see strictly
/// increasing (or decreasing) values.
#[derive(Debug)]
pub struct Sequence {
    state: Mutex<SequenceState>,
    /// Cached currval for this session (the last value returned by
    /// `nextval` in this handle's lifetime). Postgres stores this
    /// per-backend; here it is per-`Sequence` handle.
    currval: Mutex<Option<i64>>,
}

impl Sequence {
    /// Create a new sequence with the given options.
    pub fn new(opts: SequenceOptions) -> Result<Self, SequenceError> {
        let state = SequenceState::from_opts(&opts)?;
        Ok(Self {
            state: Mutex::new(state),
            currval: Mutex::new(None),
        })
    }

    /// Rebuild a sequence from a durable WAL/catalog snapshot.
    pub fn from_snapshot(snapshot: SequenceSnapshot) -> Result<Self, SequenceError> {
        let state = SequenceState::from_snapshot(snapshot)?;
        Ok(Self {
            state: Mutex::new(state),
            currval: Mutex::new(None),
        })
    }

    /// Advance the sequence and return the next value.
    ///
    /// This is the only operation that modifies persistent state. In
    /// production the advance is WAL-logged before returning.
    ///
    /// # Errors
    ///
    /// Returns [`SequenceError::Exhausted`] when the sequence has
    /// reached its bound and `CYCLE` is not set.
    pub fn nextval(&self) -> Result<i64, SequenceError> {
        let value = self.state.lock().advance()?;
        *self.currval.lock() = Some(value);
        Ok(value)
    }

    /// Advance the sequence and emit a `SequenceOp` WAL record carrying the
    /// post-advance state when `wal` is configured.
    pub fn nextval_logged(
        &self,
        name: &str,
        seqrelid: RelationId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<i64, SequenceError> {
        let value = self.nextval()?;
        self.emit_wal(SequenceOpKind::Advance, name, seqrelid, xid, wal)?;
        Ok(value)
    }

    /// Return the last value returned by [`Self::nextval`] on this handle.
    ///
    /// # Errors
    ///
    /// Returns [`SequenceError::NotCalled`] if [`Self::nextval`] has never
    /// been called on this handle.
    pub fn currval(&self) -> Result<i64, SequenceError> {
        self.currval.lock().ok_or(SequenceError::NotCalled)
    }

    /// Return the last value returned by any sequence in the current
    /// session. For this implementation it is identical to [`Self::currval`]
    /// because we track per-handle.
    pub fn lastval(&self) -> Result<i64, SequenceError> {
        self.currval()
    }

    /// Set the sequence's current value.
    ///
    /// When `is_called` is `true` (the default in SQL), the next
    /// `nextval` call will return `value + increment`. When `false`, the
    /// next `nextval` returns `value` itself.
    ///
    /// # Errors
    ///
    /// Returns [`SequenceError::OutOfRange`] when `value` is outside the
    /// sequence's configured `[min, max]` range.
    pub fn setval(&self, value: i64, is_called: bool) -> Result<(), SequenceError> {
        {
            let mut state = self.state.lock();
            if value < state.min_value || value > state.max_value {
                return Err(SequenceError::OutOfRange {
                    value,
                    min: state.min_value,
                    max: state.max_value,
                });
            }
            state.last_value = value;
            state.is_called = is_called;
        }
        if is_called {
            *self.currval.lock() = Some(value);
        }
        Ok(())
    }

    /// Set the sequence value and emit a `SequenceOp` WAL record carrying the
    /// post-set state when `wal` is configured.
    pub fn setval_logged(
        &self,
        value: i64,
        is_called: bool,
        name: &str,
        seqrelid: RelationId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), SequenceError> {
        self.setval(value, is_called)?;
        self.emit_wal(SequenceOpKind::Set, name, seqrelid, xid, wal)
    }

    /// Return the full durable state snapshot.
    pub fn state_snapshot(&self) -> SequenceSnapshot {
        self.state.lock().snapshot()
    }

    /// Replace the durable state from recovery.
    pub fn apply_snapshot(&self, snapshot: SequenceSnapshot) -> Result<(), SequenceError> {
        let next = SequenceState::from_snapshot(snapshot)?;
        *self.state.lock() = next;
        *self.currval.lock() = None;
        Ok(())
    }

    /// Return a snapshot of the current sequence configuration.
    pub fn options_snapshot(&self) -> SequenceOptions {
        let state = self.state.lock();
        SequenceOptions {
            start: state.start_value,
            increment: state.increment,
            min: Some(state.min_value),
            max: Some(state.max_value),
            cache: state.cache_size,
            cycle: state.cycle,
        }
    }

    /// Replace sequence options, optionally restarting current value.
    ///
    /// When `restart` is `None`, the current `last_value`/`is_called`
    /// state is preserved after validating it fits inside the new
    /// bounds. `ALTER SEQUENCE ... START WITH` changes only the
    /// configured restart value; `ALTER SEQUENCE ... RESTART [WITH n]`
    /// passes `Some(n)` and makes the next [`Self::nextval`] return `n`.
    pub fn alter_options(
        &self,
        opts: SequenceOptions,
        restart: Option<i64>,
    ) -> Result<(), SequenceError> {
        let mut next_state = SequenceState::from_opts(&opts)?;
        let mut state = self.state.lock();
        if let Some(value) = restart {
            if value < next_state.min_value || value > next_state.max_value {
                return Err(SequenceError::OutOfRange {
                    value,
                    min: next_state.min_value,
                    max: next_state.max_value,
                });
            }
            next_state.last_value = value;
            next_state.is_called = false;
        } else {
            if state.last_value < next_state.min_value || state.last_value > next_state.max_value {
                return Err(SequenceError::OutOfRange {
                    value: state.last_value,
                    min: next_state.min_value,
                    max: next_state.max_value,
                });
            }
            next_state.last_value = state.last_value;
            next_state.is_called = state.is_called;
        }
        *state = next_state;
        Ok(())
    }

    /// Alter options and emit a `SequenceOp` WAL record carrying the
    /// post-alter state when `wal` is configured.
    pub fn alter_options_logged(
        &self,
        opts: SequenceOptions,
        restart: Option<i64>,
        name: &str,
        seqrelid: RelationId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), SequenceError> {
        self.alter_options(opts, restart)?;
        self.emit_wal(SequenceOpKind::Alter, name, seqrelid, xid, wal)
    }

    /// Return the configured minimum value.
    pub fn min_value(&self) -> i64 {
        self.state.lock().min_value
    }

    /// Return the configured maximum value.
    pub fn max_value(&self) -> i64 {
        self.state.lock().max_value
    }

    /// Return the configured increment.
    pub fn increment(&self) -> i64 {
        self.state.lock().increment
    }

    /// Return whether CYCLE is set.
    pub fn is_cycle(&self) -> bool {
        self.state.lock().cycle
    }

    /// Emit the current state as a sequence WAL record.
    pub fn emit_wal(
        &self,
        op: SequenceOpKind,
        name: &str,
        seqrelid: RelationId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), SequenceError> {
        let Some(wal) = wal else {
            return Ok(());
        };
        let snapshot = self.state_snapshot();
        let payload = SequenceOpPayload {
            op,
            seqrelid,
            name: name.to_ascii_lowercase(),
            start_value: snapshot.start_value,
            last_value: snapshot.last_value,
            min_value: snapshot.min_value,
            max_value: snapshot.max_value,
            increment: snapshot.increment,
            cache_size: snapshot.cache_size,
            is_called: snapshot.is_called,
            cycle: snapshot.cycle,
        }
        .encode()
        .map_err(|e| SequenceError::Wal(e.to_string()))?;
        let prev_lsn = wal.last_lsn_for(xid);
        let record = WalRecord::new(RecordType::SequenceOp, xid, prev_lsn, 0, payload)
            .map_err(|e| SequenceError::Wal(e.to_string()))?;
        wal.append(record)
            .map(|_| ())
            .map_err(|e| SequenceError::Wal(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use ultrasql_wal::RecordType;
    use ultrasql_wal::payload::{SequenceOpKind, SequenceOpPayload};

    use crate::wal_sink::test_support::InMemoryWalSink;

    use super::*;

    fn ascending() -> Sequence {
        Sequence::new(SequenceOptions {
            start: 1,
            increment: 1,
            min: None,
            max: None,
            cache: 1,
            cycle: false,
        })
        .expect("create ascending sequence")
    }

    fn bounded(start: i64, min: i64, max: i64, cycle: bool) -> Sequence {
        Sequence::new(SequenceOptions {
            start,
            increment: 1,
            min: Some(min),
            max: Some(max),
            cache: 1,
            cycle,
        })
        .expect("create bounded sequence")
    }

    // --- Basic nextval ---

    #[test]
    fn nextval_returns_sequential_values() {
        let seq = ascending();
        assert_eq!(seq.nextval().unwrap(), 1);
        assert_eq!(seq.nextval().unwrap(), 2);
        assert_eq!(seq.nextval().unwrap(), 3);
    }

    #[test]
    fn logged_nextval_emits_sequence_op_payload() {
        let seq = ascending();
        let sink = InMemoryWalSink::new();
        let value = seq
            .nextval_logged(
                "orders_id_seq",
                RelationId::new(42),
                Xid::new(7),
                Some(&sink),
            )
            .unwrap();
        assert_eq!(value, 1);
        let records = sink.records();
        assert_eq!(records.len(), 1);
        let record = &records[0].1;
        assert_eq!(record.header.record_type, RecordType::SequenceOp);
        let payload = SequenceOpPayload::decode(&record.payload).unwrap();
        assert_eq!(payload.op, SequenceOpKind::Advance);
        assert_eq!(payload.seqrelid, RelationId::new(42));
        assert_eq!(payload.name, "orders_id_seq");
        assert_eq!(payload.last_value, 1);
        assert!(payload.is_called);
    }

    #[test]
    fn snapshot_rebuild_and_apply_restore_state() {
        let seq = ascending();
        assert_eq!(seq.nextval().unwrap(), 1);
        assert_eq!(seq.nextval().unwrap(), 2);
        let snapshot = seq.state_snapshot();

        let rebuilt = Sequence::from_snapshot(snapshot.clone()).unwrap();
        assert_eq!(rebuilt.nextval().unwrap(), 3);

        let target = ascending();
        target.apply_snapshot(snapshot).unwrap();
        assert_eq!(target.nextval().unwrap(), 3);
    }

    #[test]
    fn currval_after_nextval_returns_last() {
        let seq = ascending();
        seq.nextval().unwrap();
        seq.nextval().unwrap();
        assert_eq!(seq.currval().unwrap(), 2);
    }

    #[test]
    fn currval_before_nextval_is_not_called() {
        let seq = ascending();
        let err = seq.currval().expect_err("not called yet");
        assert!(matches!(err, SequenceError::NotCalled));
    }

    // --- setval ---

    #[test]
    fn setval_is_called_true_resumes_from_next() {
        let seq = ascending();
        seq.setval(100, true).unwrap();
        assert_eq!(seq.nextval().unwrap(), 101);
    }

    #[test]
    fn setval_is_called_false_returns_set_value_first() {
        let seq = ascending();
        seq.setval(50, false).unwrap();
        assert_eq!(seq.nextval().unwrap(), 50);
        assert_eq!(seq.nextval().unwrap(), 51);
    }

    #[test]
    fn setval_out_of_range_returns_error() {
        let seq = bounded(1, 1, 10, false);
        let err = seq.setval(100, true).expect_err("out of range");
        assert!(matches!(
            err,
            SequenceError::OutOfRange {
                value: 100,
                min: 1,
                max: 10
            }
        ));
    }

    #[test]
    fn alter_start_preserves_current_value_until_restart() {
        let seq = ascending();
        assert_eq!(seq.nextval().unwrap(), 1);
        assert_eq!(seq.nextval().unwrap(), 2);

        let mut opts = seq.options_snapshot();
        opts.start = 50;
        seq.alter_options(opts, None).unwrap();
        assert_eq!(seq.nextval().unwrap(), 3);

        let opts = seq.options_snapshot();
        seq.alter_options(opts, Some(opts.start)).unwrap();
        assert_eq!(seq.nextval().unwrap(), 50);
    }

    #[test]
    fn alter_increment_preserves_current_last_value() {
        let seq = ascending();
        assert_eq!(seq.nextval().unwrap(), 1);

        let mut opts = seq.options_snapshot();
        opts.increment = 10;
        seq.alter_options(opts, None).unwrap();
        assert_eq!(seq.nextval().unwrap(), 11);
    }

    // --- Exhaustion and CYCLE ---

    #[test]
    fn non_cycle_sequence_exhausts() {
        let seq = bounded(1, 1, 3, false);
        assert_eq!(seq.nextval().unwrap(), 1);
        assert_eq!(seq.nextval().unwrap(), 2);
        assert_eq!(seq.nextval().unwrap(), 3);
        let err = seq.nextval().expect_err("must exhaust");
        assert!(matches!(err, SequenceError::Exhausted));
    }

    #[test]
    fn cycle_sequence_wraps_around() {
        let seq = bounded(1, 1, 3, true);
        assert_eq!(seq.nextval().unwrap(), 1);
        assert_eq!(seq.nextval().unwrap(), 2);
        assert_eq!(seq.nextval().unwrap(), 3);
        // Should wrap back to min.
        assert_eq!(seq.nextval().unwrap(), 1);
        assert_eq!(seq.nextval().unwrap(), 2);
    }

    // --- Descending sequence ---

    #[test]
    fn descending_sequence_decrements() {
        let seq = Sequence::new(SequenceOptions {
            start: 10,
            increment: -1,
            min: Some(1),
            max: Some(10),
            cache: 1,
            cycle: false,
        })
        .unwrap();
        assert_eq!(seq.nextval().unwrap(), 10);
        assert_eq!(seq.nextval().unwrap(), 9);
        assert_eq!(seq.nextval().unwrap(), 8);
    }

    // --- Concurrency ---

    #[test]
    fn concurrent_nextval_produces_unique_values() {
        const THREADS: usize = 16;
        const PER_THREAD: usize = 100;
        let seq = Arc::new(ascending());
        let mut handles = Vec::with_capacity(THREADS);
        for _ in 0..THREADS {
            let seq = Arc::clone(&seq);
            handles.push(thread::spawn(move || {
                let mut values = Vec::with_capacity(PER_THREAD);
                for _ in 0..PER_THREAD {
                    values.push(seq.nextval().expect("nextval"));
                }
                values
            }));
        }
        let mut all: Vec<i64> = handles
            .into_iter()
            .flat_map(|h| h.join().expect("thread"))
            .collect();
        all.sort_unstable();
        all.dedup();
        assert_eq!(
            all.len(),
            THREADS * PER_THREAD,
            "concurrent nextval produced duplicate values"
        );
    }

    // --- Invalid options ---

    #[test]
    fn zero_increment_is_rejected() {
        let err = Sequence::new(SequenceOptions {
            start: 1,
            increment: 0,
            ..Default::default()
        })
        .expect_err("zero increment must fail");
        assert!(matches!(err, SequenceError::InvalidOptions(_)));
    }

    #[test]
    fn min_ge_max_is_rejected() {
        let err = Sequence::new(SequenceOptions {
            start: 1,
            increment: 1,
            min: Some(10),
            max: Some(5),
            cache: 1,
            cycle: false,
        })
        .expect_err("min >= max must fail");
        assert!(matches!(err, SequenceError::InvalidOptions(_)));
    }
}
