//! Standby-side WAL landing — durably write received WAL bytes into local
//! segment files matching the primary's layout.
//!
//! The streaming-replication walreceiver (see
//! `docs/streaming-replication-design.md`, Phase 2) receives WAL as a contiguous
//! byte stream of complete records. The walsender may split a large record
//! across several `XLogData` frames, so a received chunk can end mid-record;
//! [`WalReceiver::land`] therefore buffers a trailing partial record and only
//! writes a record once it has arrived in full.
//!
//! Records are appended to local segments using the *same* rotation rule as the
//! primary [`crate::writer::WalWriter`] (`write_drained`): rotate before a record
//! that would straddle a segment boundary; a record larger than a segment gets
//! its own oversized segment. Given the same segment size and the same record
//! stream, the standby's segment files are therefore byte-identical to the
//! primary's — the Phase 2 gate. This landing path is deliberately kept in
//! lock-step with `write_drained`; a divergence is caught by the byte-identical
//! round-trip test below.

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use ultrasql_core::Lsn;
use ultrasql_core::fsync::durability_sync;

use crate::record::{MAX_RECORD_BYTES, RECORD_HEADER_SIZE_U32, WalRecordError};
use crate::segment::{list_segments, segment_path};
use crate::writer::{
    WalWriterError, open_segment_file, open_segment_file_append, peek_record_length,
};

/// Writes received WAL into a standby's local segment directory.
///
/// Construct with [`WalReceiver::create`] over an empty directory; feed received
/// frames to [`WalReceiver::land`]; call [`WalReceiver::flush`] to make landed
/// records durable; report [`WalReceiver::written_lsn`] (after a flush) as the
/// standby flush position.
#[derive(Debug)]
pub struct WalReceiver {
    dir: PathBuf,
    segment_size_bytes: u64,
    current_index: u32,
    current_file: Option<File>,
    current_size: u64,
    /// LSN just past the last record written to a segment file. These bytes are
    /// in the OS page cache but not necessarily fsynced — this is the standby
    /// *write* position, not the durable one.
    written_lsn: u64,
    /// LSN through which written records have been fsynced — the standby *flush*
    /// position. Advanced only after a segment is fsynced (on rotation or an
    /// explicit [`Self::flush`]). Never exceeds `written_lsn`.
    flushed_lsn: u64,
    /// Received bytes not yet forming a complete record — the tail of a record
    /// split across frames. Bounded: a record larger than [`MAX_RECORD_BYTES`]
    /// is rejected, so at most one in-flight record (< the cap) is ever buffered.
    pending: Vec<u8>,
}

impl WalReceiver {
    /// Create a receiver that lands into `dir` (created if absent), starting at
    /// segment 0 / LSN 0. The directory must not already contain segments.
    ///
    /// # Errors
    /// Returns [`WalWriterError::Io`] if the directory cannot be created.
    pub fn create(
        dir: impl Into<PathBuf>,
        segment_size_bytes: u64,
    ) -> Result<Self, WalWriterError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            segment_size_bytes,
            current_index: 0,
            current_file: None,
            current_size: 0,
            written_lsn: 0,
            flushed_lsn: 0,
            pending: Vec::new(),
        })
    }

    /// Resume landing into an *existing* standby WAL directory, continuing to
    /// append to the last segment so the stream stays byte-identical to the
    /// primary's across a reconnect/restart. The resume position
    /// ([`Self::received_lsn`]) is the total of the present segments' lengths —
    /// segments are contiguous from LSN 0 with no padding — which is the LSN to
    /// pass as the `START_REPLICATION` start.
    ///
    /// Assumes a graceful prior shutdown (the last segment ends on a complete,
    /// fsynced record); recovering a crash-torn tail before resuming is a
    /// follow-up. An empty/absent directory resumes as a fresh receiver.
    ///
    /// # Errors
    /// [`WalWriterError::Io`] on a directory/segment read or open failure;
    /// [`WalWriterError::CounterOverflow`] if the summed length overflows.
    pub fn resume(
        dir: impl Into<PathBuf>,
        segment_size_bytes: u64,
    ) -> Result<Self, WalWriterError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let segments = list_segments(&dir)?;
        let Some((last_index, last_path)) = segments.last().cloned() else {
            // Nothing landed yet — behaves exactly like a fresh receiver.
            return Self::create(dir, segment_size_bytes);
        };
        // Total bytes already landed = the resume LSN (contiguous, unpadded).
        let mut written_lsn: u64 = 0;
        for (_index, path) in &segments {
            let len = std::fs::metadata(path)?.len();
            written_lsn = written_lsn
                .checked_add(len)
                .ok_or(WalWriterError::CounterOverflow {
                    counter: "receiver resume lsn",
                })?;
        }
        let current_size = std::fs::metadata(&last_path)?.len();
        let current_file = open_segment_file_append(&last_path)?;
        Ok(Self {
            dir,
            segment_size_bytes,
            current_index: last_index,
            current_file: Some(current_file),
            current_size,
            written_lsn,
            // Prior landed records were fsynced before the graceful shutdown.
            flushed_lsn: written_lsn,
            pending: Vec::new(),
        })
    }

    /// Standby *write* LSN: bytes written to segment files (OS page cache),
    /// which may not yet be durable. Use [`Self::flushed_lsn`] for the position
    /// safe to acknowledge to the primary.
    #[must_use]
    pub fn written_lsn(&self) -> Lsn {
        Lsn::new(self.written_lsn)
    }

    /// Standby *flush* LSN: the position through which landed WAL has been
    /// fsynced and is durable. This is the position to acknowledge upstream.
    #[must_use]
    pub fn flushed_lsn(&self) -> Lsn {
        Lsn::new(self.flushed_lsn)
    }

    /// Total received byte position — written records plus any buffered partial
    /// record (the standby *receive* position).
    #[must_use]
    pub fn received_lsn(&self) -> Lsn {
        Lsn::new(self.written_lsn.saturating_add(self.pending.len() as u64))
    }

    /// Land a chunk of received WAL bytes beginning at `start_lsn`. The received
    /// stream must be contiguous: `start_lsn` must equal the current
    /// [`Self::received_lsn`]. Complete records are written immediately; a
    /// trailing partial record is buffered until the rest arrives.
    ///
    /// # Errors
    /// - [`WalWriterError::Io`] on a gap/overlap in the stream or a write failure.
    /// - [`WalWriterError::Encode`] if a record header is malformed.
    pub fn land(&mut self, start_lsn: Lsn, bytes: &[u8]) -> Result<(), WalWriterError> {
        let expected = self.received_lsn().raw();
        if start_lsn.raw() != expected {
            return Err(WalWriterError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "non-contiguous WAL stream: expected lsn {expected}, received {}",
                    start_lsn.raw()
                ),
            )));
        }
        // Defense-in-depth: a single landed chunk larger than the maximum record
        // is invalid (a real walsender chunks well below this), so refuse it
        // before it can enlarge the pending buffer — keeping the bound on
        // `pending` independent of any caller or upstream framing limit.
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(WalWriterError::Encode(WalRecordError::Malformed(
                "received WAL chunk exceeds the maximum record size",
            )));
        }
        self.pending.extend_from_slice(bytes);
        self.drain_complete_records()?;
        // After draining, only a single incomplete record may remain, which is
        // smaller than its claimed length (already capped at MAX_RECORD_BYTES).
        // Anything larger means a corrupt or hostile stream — refuse rather than
        // keep buffering. This makes the buffer bound explicit defense-in-depth.
        if self.pending.len() > MAX_RECORD_BYTES {
            return Err(WalWriterError::Encode(WalRecordError::Malformed(
                "buffered partial WAL record exceeds the maximum",
            )));
        }
        Ok(())
    }

    /// fsync the current segment so every landed record is durable.
    ///
    /// # Errors
    /// Returns [`WalWriterError::Io`] if the fsync fails.
    pub fn flush(&mut self) -> Result<(), WalWriterError> {
        if let Some(file) = self.current_file.as_mut() {
            file.flush()?;
            durability_sync(file)?;
        }
        // Every written record is now durable: the current segment was just
        // fsynced, and any earlier segments were fsynced on rotation.
        self.flushed_lsn = self.written_lsn;
        Ok(())
    }

    /// Write every complete record currently buffered, applying the primary's
    /// record-aligned rotation rule, and retain any trailing partial record.
    fn drain_complete_records(&mut self) -> Result<(), WalWriterError> {
        let mut cursor = 0;
        loop {
            // Need a full header before the record length can be read.
            if self.pending.len().saturating_sub(cursor) < RECORD_HEADER_SIZE_U32 as usize {
                break;
            }
            let record_len = peek_record_length(&self.pending[cursor..])?;
            // The stream is untrusted: refuse a header claiming a record larger
            // than the engine's own cap, so a corrupt/hostile length cannot make
            // us buffer unboundedly waiting for bytes that will never arrive.
            if record_len > MAX_RECORD_BYTES as u64 {
                return Err(WalWriterError::Encode(WalRecordError::Malformed(
                    "received WAL record length exceeds the maximum",
                )));
            }
            let record_len_usize = usize::try_from(record_len).map_err(|_| {
                WalWriterError::Io(std::io::Error::other("record length exceeds usize"))
            })?;
            // The whole record has not been received yet — keep it buffered.
            if self.pending.len().saturating_sub(cursor) < record_len_usize {
                break;
            }
            self.ensure_segment_open()?;
            let remaining = self.segment_size_bytes.saturating_sub(self.current_size);
            // Rotate before a record that would straddle the boundary, unless the
            // segment is fresh (an oversized record then gets its own segment).
            if remaining < record_len && self.current_size > 0 {
                self.rotate_segment()?;
                continue; // re-open the next segment and re-evaluate this record
            }
            // Checked range + counters, mirroring WalWriter::write_drained.
            let next_cursor = cursor.checked_add(record_len_usize).ok_or_else(|| {
                WalWriterError::Io(std::io::Error::other("WAL drain cursor overflow"))
            })?;
            let chunk = self.pending.get(cursor..next_cursor).ok_or_else(|| {
                WalWriterError::Io(std::io::Error::other(
                    "WAL drain ended before record length",
                ))
            })?;
            let next_current_size = self.current_size.checked_add(record_len).ok_or(
                WalWriterError::CounterOverflow {
                    counter: "receiver segment size",
                },
            )?;
            let next_written_lsn = self.written_lsn.checked_add(record_len).ok_or(
                WalWriterError::CounterOverflow {
                    counter: "receiver written lsn",
                },
            )?;
            let file = self.current_file.as_mut().ok_or_else(|| {
                WalWriterError::Io(std::io::Error::other("segment file unexpectedly closed"))
            })?;
            file.write_all(chunk)?;
            self.current_size = next_current_size;
            self.written_lsn = next_written_lsn;
            cursor = next_cursor;
        }
        if cursor > 0 {
            self.pending.drain(0..cursor);
        }
        Ok(())
    }

    fn ensure_segment_open(&mut self) -> Result<(), WalWriterError> {
        if self.current_file.is_none() {
            let path = segment_path(&self.dir, self.current_index);
            self.current_file = Some(open_segment_file(&path)?);
            self.current_size = 0;
        }
        Ok(())
    }

    fn rotate_segment(&mut self) -> Result<(), WalWriterError> {
        // fsync the finished segment before opening the next so a crash between
        // them cannot leave a hole (mirrors WalWriter::rotate_segment). All
        // records in the finished segment are now durable.
        if let Some(file) = self.current_file.as_mut() {
            file.flush()?;
            durability_sync(file)?;
            self.flushed_lsn = self.written_lsn;
        }
        self.current_file = None;
        self.current_index = self
            .current_index
            .checked_add(1)
            .ok_or(WalWriterError::SegmentIndexExhausted)?;
        self.current_size = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;
    use ultrasql_core::{Lsn, Xid};

    use super::*;
    use crate::buffer::WalBuffer;
    use crate::reader::read_wal_range;
    use crate::record::{RecordType, WalRecord};
    use crate::segment::list_segments;
    use crate::writer::{WalWriter, WalWriterConfig};

    fn rec(n: u8) -> WalRecord {
        WalRecord::new(
            RecordType::HeapInsert,
            Xid::new(u64::from(n) + 1),
            Lsn::ZERO,
            0,
            vec![n; usize::from(n) + 1],
        )
        .expect("test WAL record fits size limits")
    }

    /// Write a stream of records through the real primary writer (small segments
    /// to force several rotations) and return the dir + the durable LSN.
    fn write_primary(records: &[WalRecord], segment_size_bytes: u64) -> (TempDir, Lsn) {
        let dir = TempDir::new().unwrap();
        let buffer = Arc::new(WalBuffer::new(64 * 1024, Lsn::ZERO));
        let writer = WalWriter::open(
            dir.path(),
            Arc::clone(&buffer),
            WalWriterConfig {
                segment_size_bytes,
                fsync_window_us: 100,
                fsync_batch_bytes: 1,
            },
        )
        .unwrap();
        for r in records {
            buffer.append(r).unwrap();
        }
        writer.notify();
        writer.shutdown().unwrap();
        (dir, buffer.durable_lsn())
    }

    fn read_all_segment_bytes(dir: &std::path::Path) -> Vec<(u32, Vec<u8>)> {
        list_segments(dir)
            .unwrap()
            .into_iter()
            .map(|(idx, path)| (idx, std::fs::read(path).unwrap()))
            .collect()
    }

    #[test]
    fn landed_segments_are_byte_identical_to_primary() {
        const SEG: u64 = 256;
        let written: Vec<WalRecord> = (0..16).map(rec).collect();
        let (primary_dir, durable) = write_primary(&written, SEG);

        // Read the whole primary WAL byte stream back as records.
        let stream = read_wal_range(primary_dir.path(), Lsn::ZERO, durable).expect("read");
        let mut wal_bytes = Vec::new();
        for r in &stream.records {
            wal_bytes.extend_from_slice(&r.bytes);
        }
        assert_eq!(wal_bytes.len() as u64, durable.raw());

        // Land it into a standby dir in tiny, deliberately record-UNALIGNED
        // chunks, to exercise the partial-record reassembly and prove rotation
        // happens on record boundaries regardless of frame boundaries.
        let standby_dir = TempDir::new().unwrap();
        let mut receiver = WalReceiver::create(standby_dir.path(), SEG).unwrap();
        let mut offset = 0;
        while offset < wal_bytes.len() {
            let end = (offset + 7).min(wal_bytes.len());
            receiver
                .land(Lsn::new(offset as u64), &wal_bytes[offset..end])
                .expect("land chunk");
            offset = end;
        }
        receiver.flush().expect("flush");

        assert_eq!(receiver.written_lsn(), durable, "all records landed");
        assert_eq!(
            receiver.flushed_lsn(),
            durable,
            "all records durable after flush"
        );
        assert_eq!(
            receiver.received_lsn(),
            durable,
            "no partial record left over"
        );

        // The standby's segment files match the primary's byte-for-byte.
        let primary_segments = read_all_segment_bytes(primary_dir.path());
        let standby_segments = read_all_segment_bytes(standby_dir.path());
        assert!(
            primary_segments.len() > 1,
            "test should span several segments"
        );
        assert_eq!(
            standby_segments, primary_segments,
            "standby segments are byte-identical to the primary"
        );

        // And the standby WAL recovers into the same records.
        let landed = read_wal_range(standby_dir.path(), Lsn::ZERO, durable).expect("read standby");
        assert_eq!(landed.records.len(), written.len());
        for (got, original) in landed.records.iter().zip(&written) {
            let (decoded, _) = WalRecord::decode(&got.bytes).expect("decode landed record");
            assert_eq!(decoded.payload, original.payload);
        }
    }

    #[test]
    fn land_rejects_a_non_contiguous_stream() {
        let standby_dir = TempDir::new().unwrap();
        let mut receiver = WalReceiver::create(standby_dir.path(), 1024).unwrap();
        let r = rec(3);
        let bytes = r.encode();
        // A start LSN that does not match the expected position is rejected.
        let err = receiver.land(Lsn::new(10), &bytes).unwrap_err();
        assert!(matches!(err, WalWriterError::Io(_)));
        // From the correct position it lands fine.
        receiver.land(Lsn::ZERO, &bytes).expect("contiguous land");
        assert_eq!(receiver.received_lsn(), Lsn::new(bytes.len() as u64));
    }

    #[test]
    fn land_rejects_a_record_larger_than_the_maximum() {
        let standby_dir = TempDir::new().unwrap();
        let mut receiver = WalReceiver::create(standby_dir.path(), 1024).unwrap();
        // A header (untrusted input) claiming a record one byte past the cap is
        // rejected before it can drive unbounded buffering.
        let bogus_len = u32::try_from(MAX_RECORD_BYTES).expect("cap fits u32") + 1;
        let mut bytes = bogus_len.to_le_bytes().to_vec();
        bytes.resize(RECORD_HEADER_SIZE_U32 as usize, 0);
        let err = receiver.land(Lsn::ZERO, &bytes).unwrap_err();
        assert!(matches!(err, WalWriterError::Encode(_)), "got {err:?}");
    }

    #[test]
    fn flushed_lsn_lags_written_lsn_until_flush() {
        // A single small record into a fresh, un-rotated segment: it is written
        // (in the OS cache) but not durable until flush(), so flushed_lsn must
        // not advance prematurely (the durability contract for status replies).
        let standby_dir = TempDir::new().unwrap();
        let mut receiver = WalReceiver::create(standby_dir.path(), 1 << 20).unwrap();
        let bytes = rec(5).encode();
        receiver.land(Lsn::ZERO, &bytes).expect("land");

        assert_eq!(
            receiver.written_lsn(),
            Lsn::new(bytes.len() as u64),
            "written advances"
        );
        assert_eq!(
            receiver.flushed_lsn(),
            Lsn::ZERO,
            "not durable before flush"
        );

        receiver.flush().expect("flush");
        assert_eq!(
            receiver.flushed_lsn(),
            receiver.written_lsn(),
            "flush makes the written position durable"
        );
    }

    #[test]
    fn resume_continues_landing_byte_identically() {
        const SEG: u64 = 256;
        let written: Vec<WalRecord> = (0..16).map(rec).collect();
        let (primary_dir, durable) = write_primary(&written, SEG);
        let stream = read_wal_range(primary_dir.path(), Lsn::ZERO, durable).expect("read");
        assert_eq!(stream.records.len(), written.len());

        // Split the record stream at a record boundary (after the first 8).
        let split = 8;
        let first_half: Vec<u8> = stream.records[..split]
            .iter()
            .flat_map(|r| r.bytes.clone())
            .collect();
        let second_half: Vec<u8> = stream.records[split..]
            .iter()
            .flat_map(|r| r.bytes.clone())
            .collect();
        let split_lsn = stream.records[split].lsn;
        assert_eq!(split_lsn.raw(), first_half.len() as u64);

        let standby_dir = TempDir::new().unwrap();
        // Land the first half, flush, and drop (a graceful shutdown).
        {
            let mut first = WalReceiver::create(standby_dir.path(), SEG).unwrap();
            first.land(Lsn::ZERO, &first_half).unwrap();
            first.flush().unwrap();
            assert_eq!(first.written_lsn(), split_lsn);
        }
        // Resume and land the rest.
        let mut resumed = WalReceiver::resume(standby_dir.path(), SEG).unwrap();
        assert_eq!(
            resumed.received_lsn(),
            split_lsn,
            "resumes at prior position"
        );
        assert_eq!(resumed.flushed_lsn(), split_lsn);
        resumed.land(split_lsn, &second_half).unwrap();
        resumed.flush().unwrap();
        assert_eq!(resumed.written_lsn(), durable);

        // The resumed standby's segments are byte-identical to the primary's —
        // resume continued the same segment layout across the restart.
        let primary_segs = read_all_segment_bytes(primary_dir.path());
        let standby_segs = read_all_segment_bytes(standby_dir.path());
        assert!(primary_segs.len() > 1, "test spans several segments");
        assert_eq!(
            standby_segs, primary_segs,
            "resumed landing is byte-identical to the primary"
        );
    }
}
