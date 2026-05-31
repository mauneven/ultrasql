//! Background WAL writer thread with group-commit fsync batching.
//!
//! [`WalWriter::open`] spawns a dedicated OS thread that repeatedly
//! drains the shared [`WalBuffer`] into the current segment file. Bytes
//! are written eagerly; the expensive `fsync` is deferred and amortized
//! across one of three triggers:
//!
//! 1. The group-commit window (`fsync_window_us`) has elapsed since the
//!    last fsync.
//! 2. The number of unflushed bytes has reached `fsync_batch_bytes`.
//! 3. A shutdown signal arrived.
//!
//! Upon a successful fsync the writer publishes the new durable LSN
//! through [`WalBuffer::publish_durable_lsn`], unblocking committers
//! that are polling `durable_lsn`.
//!
//! Segment rollover is handled inline: when the active segment's size
//! reaches `segment_size_bytes`, the writer fsyncs the segment, closes
//! it, and opens the next one. Drains happen in whole-record granularity
//! so rollover always happens on a record boundary.
//!
//! Platform fsync semantics
//! ------------------------
//!
//! Durability uses Rust's safe `File::sync_all`, which maps to the
//! platform's strongest generally available equivalent of `fsync(2)` for
//! the current handle.

use std::fs::{File, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use tracing::{debug, error, info, warn};
use ultrasql_core::Lsn;

use crate::buffer::WalBuffer;
use crate::record::WalRecordError;
use crate::segment::{list_segments, segment_path};

/// Errors that can arise opening, running, or shutting down the writer.
#[derive(Debug, thiserror::Error)]
pub enum WalWriterError {
    /// An I/O error from the segment-file path.
    #[error("wal writer io error: {0}")]
    Io(#[from] std::io::Error),

    /// A record from the buffer failed to encode or decode.
    #[error("wal writer record error: {0}")]
    Encode(#[from] WalRecordError),

    /// The writer thread has already been shut down or terminated
    /// abnormally.
    #[error("wal writer is already shut down")]
    Shutdown,

    /// No segment index remains after `u32::MAX`.
    #[error("wal segment index exhausted")]
    SegmentIndexExhausted,

    /// A writer-maintained byte counter overflowed.
    #[error("wal writer counter overflow: {counter}")]
    CounterOverflow {
        /// Counter that overflowed.
        counter: &'static str,
    },
}

/// Tuning knobs for the writer thread.
#[derive(Clone, Copy, Debug)]
pub struct WalWriterConfig {
    /// Maximum size of a single segment file in bytes. The writer rolls
    /// to the next segment after this threshold. Default: 16 MiB.
    pub segment_size_bytes: u64,
    /// Group-commit window in microseconds. The writer fsyncs at
    /// least once per window even if no batch threshold is reached.
    /// Default: 200 microseconds.
    pub fsync_window_us: u64,
    /// Number of unflushed bytes that triggers an immediate fsync,
    /// regardless of the time window. Default: 256 KiB.
    pub fsync_batch_bytes: usize,
}

/// Live WAL writer counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalWriterStats {
    /// Number of completed file flush + fsync calls.
    pub fsync_count: u64,
    /// Sum of completed fsync latencies in microseconds.
    pub fsync_total_us: u64,
    /// Maximum completed fsync latency in microseconds.
    pub fsync_max_us: u64,
    /// Most recent completed fsync latency in microseconds.
    pub fsync_last_us: u64,
}

impl Default for WalWriterConfig {
    fn default() -> Self {
        Self {
            segment_size_bytes: 16 * 1024 * 1024,
            fsync_window_us: 200,
            fsync_batch_bytes: 256 * 1024,
        }
    }
}

/// Wake-up state shared between the writer thread and external
/// signallers (appenders, the shutdown call).
#[derive(Debug, Default)]
struct WakeState {
    /// `true` once shutdown has been requested. The writer thread
    /// observes this, drains, fsyncs, and exits.
    stopping: bool,
    /// Bumped on every appender notification. The condvar wait keys
    /// off the value, not the boolean, so a notification that arrives
    /// between the lock release and the wait does not get lost.
    epoch: u64,
}

/// State the writer thread owns and the public handle borrows
/// transparently via `Arc`.
#[derive(Debug)]
struct Shared {
    wake_mutex: Mutex<WakeState>,
    wake_cv: Condvar,
    /// Most recently published durable LSN, mirrored from the buffer
    /// so [`WalWriter::flushed_lsn`] is a lock-free atomic read.
    durable_lsn: AtomicU64,
    /// Completed fsync count.
    fsync_count: AtomicU64,
    /// Sum of fsync latencies in microseconds.
    fsync_total_us: AtomicU64,
    /// Maximum observed fsync latency in microseconds.
    fsync_max_us: AtomicU64,
    /// Most recent observed fsync latency in microseconds.
    fsync_last_us: AtomicU64,
}

impl Shared {
    fn record_fsync_latency(&self, elapsed_us: u64) {
        self.fsync_count.fetch_add(1, Ordering::Relaxed);
        self.fsync_total_us.fetch_add(elapsed_us, Ordering::Relaxed);
        self.fsync_last_us.store(elapsed_us, Ordering::Relaxed);

        let mut current = self.fsync_max_us.load(Ordering::Relaxed);
        while elapsed_us > current {
            match self.fsync_max_us.compare_exchange_weak(
                current,
                elapsed_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    fn stats(&self) -> WalWriterStats {
        WalWriterStats {
            fsync_count: self.fsync_count.load(Ordering::Relaxed),
            fsync_total_us: self.fsync_total_us.load(Ordering::Relaxed),
            fsync_max_us: self.fsync_max_us.load(Ordering::Relaxed),
            fsync_last_us: self.fsync_last_us.load(Ordering::Relaxed),
        }
    }
}

/// Owning handle to the background WAL writer thread.
#[derive(Debug)]
pub struct WalWriter {
    shared: Arc<Shared>,
    handle: Option<JoinHandle<Result<(), WalWriterError>>>,
}

impl WalWriter {
    /// Open (or reopen) the segmented WAL at `wal_dir` and spawn the
    /// background writer thread. If the directory contains existing
    /// segment files, the writer resumes by writing to the next free
    /// segment index; previously-closed segments remain immutable.
    ///
    /// `buffer` is taken by value so the caller can pass `Arc::clone`
    /// of their own handle and not retain a reference; the writer
    /// thread keeps its own clone for the lifetime of the writer.
    #[allow(clippy::needless_pass_by_value)]
    pub fn open(
        wal_dir: impl Into<PathBuf>,
        buffer: Arc<WalBuffer>,
        config: WalWriterConfig,
    ) -> Result<Self, WalWriterError> {
        let dir = wal_dir.into();
        std::fs::create_dir_all(&dir)?;

        let next_index = next_segment_index(&dir)?;
        let initial_durable = buffer.durable_lsn().raw();
        let shared = Arc::new(Shared {
            wake_mutex: Mutex::new(WakeState::default()),
            wake_cv: Condvar::new(),
            durable_lsn: AtomicU64::new(initial_durable),
            fsync_count: AtomicU64::new(0),
            fsync_total_us: AtomicU64::new(0),
            fsync_max_us: AtomicU64::new(0),
            fsync_last_us: AtomicU64::new(0),
        });

        let thread_shared = Arc::clone(&shared);
        let thread_buffer = Arc::clone(&buffer);
        let thread_dir = dir.clone();
        let handle = thread::Builder::new()
            .name(String::from("ultrasql-wal-writer"))
            .spawn(move || {
                let mut driver =
                    WriterDriver::new(thread_dir, thread_buffer, thread_shared, config, next_index);
                let result = driver.run();
                if let Err(ref e) = result {
                    error!(error = %e, "wal writer thread terminated with error");
                }
                result
            })?;

        info!(?dir, next_segment = next_index, "wal writer started");
        Ok(Self {
            shared,
            handle: Some(handle),
        })
    }

    /// Wake the writer thread immediately. Optional; the writer also
    /// polls on its `fsync_window_us` cadence.
    pub fn notify(&self) {
        {
            let mut state = self.shared.wake_mutex.lock();
            state.epoch = state.epoch.wrapping_add(1);
        }
        self.shared.wake_cv.notify_one();
    }

    /// LSN through which the writer thread has fsynced.
    pub fn flushed_lsn(&self) -> Lsn {
        Lsn::new(self.shared.durable_lsn.load(Ordering::Acquire))
    }

    /// Return live WAL writer counters.
    #[must_use]
    pub fn stats(&self) -> WalWriterStats {
        self.shared.stats()
    }

    /// Signal the writer thread to stop, wait for it to drain
    /// remaining bytes, fsync, and exit. Consumes the handle.
    pub fn shutdown(mut self) -> Result<(), WalWriterError> {
        self.signal_stop();
        let Some(handle) = self.handle.take() else {
            return Err(WalWriterError::Shutdown);
        };
        handle.join().unwrap_or_else(|_| {
            error!("wal writer thread panicked");
            Err(WalWriterError::Shutdown)
        })
    }

    fn signal_stop(&self) {
        {
            let mut state = self.shared.wake_mutex.lock();
            state.stopping = true;
            state.epoch = state.epoch.wrapping_add(1);
        }
        self.shared.wake_cv.notify_all();
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            warn!("WalWriter dropped without shutdown(); signalling thread to stop");
            self.signal_stop();
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => error!(error = %e, "wal writer thread errored during Drop"),
                Err(_) => error!("wal writer thread panicked during Drop"),
            }
        }
    }
}

#[cfg(test)]
mod stats_tests {
    use super::*;

    #[test]
    fn wal_writer_stats_snapshot_starts_empty() {
        let shared = Arc::new(Shared {
            wake_mutex: Mutex::new(WakeState::default()),
            wake_cv: Condvar::new(),
            durable_lsn: AtomicU64::new(0),
            fsync_count: AtomicU64::new(0),
            fsync_total_us: AtomicU64::new(0),
            fsync_max_us: AtomicU64::new(0),
            fsync_last_us: AtomicU64::new(0),
        });
        let writer = WalWriter {
            shared,
            handle: None,
        };

        assert_eq!(writer.stats(), WalWriterStats::default());
    }
}

/// Internal driver: owns the open segment file and the loop logic.
#[derive(Debug)]
struct WriterDriver {
    dir: PathBuf,
    buffer: Arc<WalBuffer>,
    shared: Arc<Shared>,
    config: WalWriterConfig,
    /// Index of the segment file currently being written.
    current_index: u32,
    /// Open handle to that segment file.
    current_file: Option<File>,
    /// Number of bytes already written to `current_file`.
    current_size: u64,
    /// Bytes written but not yet fsynced.
    unflushed_bytes: usize,
    /// The largest LSN whose bytes have been written-but-not-yet-fsynced.
    pending_lsn: Lsn,
    /// The largest LSN that has been fsynced. Initialized from the
    /// buffer's `durable_lsn` on entry.
    durable_lsn: Lsn,
}

impl WriterDriver {
    fn new(
        dir: PathBuf,
        buffer: Arc<WalBuffer>,
        shared: Arc<Shared>,
        config: WalWriterConfig,
        first_index: u32,
    ) -> Self {
        let initial = buffer.durable_lsn();
        Self {
            dir,
            buffer,
            shared,
            config,
            current_index: first_index,
            current_file: None,
            current_size: 0,
            unflushed_bytes: 0,
            pending_lsn: initial,
            durable_lsn: initial,
        }
    }

    fn run(&mut self) -> Result<(), WalWriterError> {
        let window = Duration::from_micros(self.config.fsync_window_us.max(1));
        let mut last_fsync = Instant::now();
        let mut epoch_seen: u64 = 0;

        loop {
            // Drain whatever is pending in the buffer first.
            let drained = self.buffer.drain();
            if !drained.bytes.is_empty() {
                self.write_drained(&drained.bytes, drained.end_lsn)?;
            }

            // Decide whether to fsync this iteration.
            let elapsed = last_fsync.elapsed();
            let stopping = {
                let state = self.shared.wake_mutex.lock();
                state.stopping
            };
            let need_fsync = self.unflushed_bytes > 0
                && (stopping
                    || self.unflushed_bytes >= self.config.fsync_batch_bytes
                    || elapsed >= window);

            if need_fsync {
                self.flush_current()?;
                last_fsync = Instant::now();
            }

            if stopping {
                // Drain one last time in case appenders snuck in
                // between the drain above and observing `stopping`.
                let trailing = self.buffer.drain();
                if !trailing.bytes.is_empty() {
                    self.write_drained(&trailing.bytes, trailing.end_lsn)?;
                }
                if self.unflushed_bytes > 0 {
                    self.flush_current()?;
                }
                debug!(durable = %self.durable_lsn, "wal writer exiting cleanly");
                return Ok(());
            }

            // Sleep on the condvar with a deadline so we wake at least
            // once per window even if no appender notifies us.
            let mut state = self.shared.wake_mutex.lock();
            if state.stopping {
                continue;
            }
            if state.epoch != epoch_seen {
                // A notification arrived between our last drain and
                // taking the lock. Don't sleep; loop and drain again.
                epoch_seen = state.epoch;
                continue;
            }
            let _result = self.shared.wake_cv.wait_for(&mut state, window);
            epoch_seen = state.epoch;
        }
    }

    /// Append `bytes` (a serialized run of complete WAL records, in
    /// LSN order) to the current segment, rotating on record
    /// boundaries when the segment fills up.
    ///
    /// We walk `bytes` record-by-record using each record header's
    /// `total_length` field, then rotate before any record that would
    /// straddle a segment boundary. If a single record is larger than
    /// `segment_size_bytes` the writer puts it alone in its own
    /// (oversized) segment, since splitting a record is not allowed.
    fn write_drained(&mut self, bytes: &[u8], end_lsn: Lsn) -> Result<(), WalWriterError> {
        let mut cursor = 0;
        while cursor < bytes.len() {
            let record_len = peek_record_length(&bytes[cursor..])?;
            self.ensure_segment_open()?;
            let remaining_capacity = self
                .config
                .segment_size_bytes
                .saturating_sub(self.current_size);
            // If this record doesn't fit *and* the segment isn't fresh
            // (current_size > 0), rotate first. A fresh segment that's
            // still too small simply gets the oversized record alone.
            if remaining_capacity < record_len && self.current_size > 0 {
                self.rotate_segment()?;
                continue;
            }
            let record_len_usize = usize::try_from(record_len).map_err(|_| {
                WalWriterError::Io(std::io::Error::other("record length exceeds usize"))
            })?;
            let next_cursor = checked_writer_usize_add(cursor, record_len_usize, "drain cursor")?;
            let chunk = bytes.get(cursor..next_cursor).ok_or_else(|| {
                WalWriterError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "wal drain ended before record length",
                ))
            })?;
            let next_current_size =
                checked_writer_u64_add(self.current_size, record_len, "segment size")?;
            let next_unflushed_bytes =
                checked_writer_usize_add(self.unflushed_bytes, chunk.len(), "unflushed bytes")?;
            let file = self.current_file.as_mut().ok_or_else(|| {
                WalWriterError::Io(std::io::Error::other("segment file unexpectedly closed"))
            })?;
            file.write_all(chunk)?;
            self.current_size = next_current_size;
            self.unflushed_bytes = next_unflushed_bytes;
            cursor = next_cursor;
        }
        self.pending_lsn = end_lsn;
        Ok(())
    }

    fn ensure_segment_open(&mut self) -> Result<(), WalWriterError> {
        if self.current_file.is_some() {
            return Ok(());
        }
        let path = segment_path(&self.dir, self.current_index);
        let file = open_segment_file(&path)?;
        let size = file.metadata()?.len();
        debug!(?path, current_size = size, "wal writer opened segment");
        self.current_file = Some(file);
        self.current_size = size;
        Ok(())
    }

    fn rotate_segment(&mut self) -> Result<(), WalWriterError> {
        // Make sure everything written to the current segment is on
        // stable storage *before* we open the next one. Otherwise a
        // crash between the close and the rotation could leave a hole.
        if self.unflushed_bytes > 0 {
            self.flush_current()?;
        }
        if let Some(file) = self.current_file.take() {
            // Explicit drop closes the OS handle.
            drop(file);
        }
        let prev = self.current_index;
        self.current_index = self
            .current_index
            .checked_add(1)
            .ok_or(WalWriterError::SegmentIndexExhausted)?;
        self.current_size = 0;
        debug!(
            prev_index = prev,
            new_index = self.current_index,
            "wal writer rotated segment"
        );
        Ok(())
    }

    fn flush_current(&mut self) -> Result<(), WalWriterError> {
        if let Some(file) = self.current_file.as_mut() {
            let started = Instant::now();
            file.flush()?;
            full_fsync(file)?;
            let elapsed_us = duration_as_micros_saturated(started.elapsed());
            self.shared.record_fsync_latency(elapsed_us);
        }
        self.unflushed_bytes = 0;
        self.durable_lsn = self.pending_lsn;
        let raw = self.durable_lsn.raw();
        self.shared.durable_lsn.store(raw, Ordering::Release);
        self.buffer.publish_durable_lsn(self.durable_lsn);
        Ok(())
    }
}

fn duration_as_micros_saturated(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn checked_writer_u64_add(
    current: u64,
    delta: u64,
    counter: &'static str,
) -> Result<u64, WalWriterError> {
    current
        .checked_add(delta)
        .ok_or(WalWriterError::CounterOverflow { counter })
}

fn checked_writer_usize_add(
    current: usize,
    delta: usize,
    counter: &'static str,
) -> Result<usize, WalWriterError> {
    current
        .checked_add(delta)
        .ok_or(WalWriterError::CounterOverflow { counter })
}

/// Read the `total_length` u32 from the front of an encoded WAL
/// record without otherwise validating the record. Used to advance
/// cursor by exact record-byte counts and to perform record-aligned
/// segment rotation. Returns the length in bytes.
fn peek_record_length(bytes: &[u8]) -> Result<u64, WalWriterError> {
    if bytes.len() < 4 {
        return Err(WalWriterError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "wal drain produced fewer than 4 bytes; corrupt buffer state",
        )));
    }
    let total = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if total < crate::record::RECORD_HEADER_SIZE_U32 {
        return Err(WalWriterError::Encode(WalRecordError::Malformed(
            "total_length too small",
        )));
    }
    Ok(u64::from(total))
}

/// Open (or create) a WAL segment file for append.
///
/// On Unix the file is opened with `O_NOFOLLOW` so that a hostile actor
/// who plants `wal_dir/segment_*` as a symlink to a sensitive file
/// (`/etc/passwd`, another database's data file, etc.) cannot trick the
/// writer into appending WAL records into the symlinked target. The
/// `open(2)` call fails with `ELOOP` in that case and the writer
/// surfaces the error to its caller, who tears the writer thread down
/// rather than silently overwriting unrelated files.
///
/// On non-Unix targets the flag is unavailable and the open is plain.
#[cfg_attr(not(unix), allow(unused_variables))]
fn open_segment_file(path: &Path) -> std::io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.create(true).append(true).read(false);
    #[cfg(unix)]
    opts.custom_flags(libc::O_NOFOLLOW);
    opts.open(path)
}

/// Determine the index to use for the next segment to be written to a
/// directory. Returns `0` if no segments exist, otherwise
/// `max(existing) + 1` so we never reopen and append to a previously
/// closed segment. This keeps each segment's contents immutable after
/// the writer rotates past it, which simplifies recovery's torn-write
/// reasoning.
fn next_segment_index(dir: &Path) -> Result<u32, WalWriterError> {
    let segs = list_segments(dir)?;
    let Some(&(idx, _)) = segs.last() else {
        return Ok(0);
    };
    idx.checked_add(1)
        .ok_or(WalWriterError::SegmentIndexExhausted)
}

/// Force the file's contents and metadata to stable storage.
fn full_fsync(file: &File) -> std::io::Result<()> {
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;
    use ultrasql_core::{Lsn, Xid};

    use super::*;
    use crate::buffer::WalBuffer;
    use crate::record::{RecordType, WalRecord};

    fn rec(payload: &[u8]) -> WalRecord {
        WalRecord::new(
            RecordType::HeapInsert,
            Xid::new(1),
            Lsn::ZERO,
            0,
            payload.to_vec(),
        )
        .expect("test WAL record should fit size limits")
    }

    #[test]
    fn next_segment_index_handles_empty_and_nonempty() {
        let dir = TempDir::new().unwrap();
        assert_eq!(next_segment_index(dir.path()).unwrap(), 0);
        // Create a couple of empty segment files manually.
        std::fs::File::create(segment_path(dir.path(), 0)).unwrap();
        std::fs::File::create(segment_path(dir.path(), 3)).unwrap();
        assert_eq!(next_segment_index(dir.path()).unwrap(), 4);
    }

    #[test]
    fn next_segment_index_rejects_exhaustion() {
        let dir = TempDir::new().unwrap();
        std::fs::File::create(segment_path(dir.path(), u32::MAX)).unwrap();

        let err = next_segment_index(dir.path()).unwrap_err();
        assert!(matches!(err, WalWriterError::SegmentIndexExhausted));
    }

    #[test]
    fn writer_counter_add_rejects_overflow() {
        let err = checked_writer_u64_add(u64::MAX, 1, "segment size").unwrap_err();
        assert!(matches!(err, WalWriterError::CounterOverflow { .. }));
    }

    #[test]
    fn writer_writes_and_fsyncs_a_single_record() {
        let dir = TempDir::new().unwrap();
        let buffer = Arc::new(WalBuffer::new(64 * 1024, Lsn::ZERO));
        let writer = WalWriter::open(
            dir.path(),
            Arc::clone(&buffer),
            WalWriterConfig {
                segment_size_bytes: 1024,
                fsync_window_us: 100,
                fsync_batch_bytes: 1,
            },
        )
        .unwrap();
        buffer.append(&rec(b"hi")).unwrap();
        writer.notify();
        let next_lsn = buffer.next_lsn();
        writer.shutdown().unwrap();
        assert!(buffer.durable_lsn() >= next_lsn);
    }

    /// Hostile-fs scenario: an attacker has staged a symlink at
    /// `wal_dir/segment_0000000000` pointing at a sensitive file. The
    /// writer must refuse to follow the link rather than appending WAL
    /// bytes into the linked target. We assert the open returns an
    /// `ELOOP`-class error.
    #[cfg(unix)]
    #[test]
    fn segment_open_refuses_to_follow_symlink() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new().unwrap();
        let target_dir = TempDir::new().unwrap();
        let target = target_dir.path().join("victim");
        std::fs::write(&target, b"original").unwrap();
        let link = segment_path(dir.path(), 0);
        symlink(&target, &link).unwrap();

        let err = open_segment_file(&link).expect_err("open must refuse symlink");
        // POSIX returns ELOOP; some platforms surface it as
        // `FilesystemLoop`, others as `Other`. Either way it must NOT
        // be Ok.
        let raw = err.raw_os_error();
        assert!(
            raw == Some(libc::ELOOP) || err.kind() == std::io::ErrorKind::Other,
            "expected ELOOP, got {err:?}"
        );

        // The target file's contents must be unchanged.
        let after = std::fs::read(&target).unwrap();
        assert_eq!(after, b"original");
    }
}
