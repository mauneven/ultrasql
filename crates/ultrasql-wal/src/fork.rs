//! Durable WAL timeline forks after point-in-time recovery.
//!
//! A writable server that stops replay at an older record boundary must not
//! leave the later physical WAL tail in place: new records would reuse those
//! logical LSNs while recovery still enumerated the old bytes. [`fork_wal_at`]
//! removes that tail before a writer is opened.
//!
//! Crash safety depends on the operation order:
//!
//! 1. remove every segment after the boundary segment and fsync the directory;
//! 2. truncate and fsync the boundary segment;
//! 3. let the caller durably consume its recovery-target marker.
//!
//! If the process stops during steps 1 or 2, the marker still exists, so the
//! next startup replays to the same boundary and retries the fork. Removing
//! later segments before shortening the boundary segment also prevents a
//! truncated segment from being followed by old bytes whose logical LSNs
//! would otherwise shift backwards.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use ultrasql_core::Lsn;

use crate::manifest::read_floor;
use crate::record::WalRecordError;
use crate::recovery::RecoveryError;
use crate::segment::{list_segments, segment_path};

/// Result of durably forking a WAL stream at a record boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalForkOutcome {
    /// Exclusive end boundary retained by the fork.
    pub boundary_lsn: Lsn,
    /// Segment kept as the last segment of the old timeline.
    pub boundary_segment: u32,
    /// Byte length retained in [`Self::boundary_segment`].
    pub boundary_segment_len: u64,
    /// Later segment indices removed from disk, ascending.
    pub removed_segments: Vec<u32>,
}

fn malformed(message: &'static str) -> RecoveryError {
    RecoveryError::Record(WalRecordError::Malformed(message))
}

/// Durably discard every WAL byte strictly after `boundary_lsn`.
///
/// `boundary_lsn` must be the exclusive end LSN returned by recovery and must
/// not precede the current recovery floor. The caller must ensure no WAL writer
/// is open. When the boundary is exactly the start of a segment, that segment
/// is retained with length zero; this preserves the manifest's segment-index
/// continuity, including a fork at a recycled floor or at LSN zero.
///
/// This operation is idempotent. Its crash-safety protocol requires the caller
/// to keep the recovery-target marker durable until this function succeeds.
pub fn fork_wal_at(
    wal_dir: impl AsRef<Path>,
    boundary_lsn: Lsn,
) -> Result<WalForkOutcome, RecoveryError> {
    let dir = wal_dir.as_ref();
    std::fs::create_dir_all(dir)?;

    let floor = read_floor(dir)?;
    if boundary_lsn.raw() < floor.floor_lsn.raw() {
        return Err(malformed("wal fork boundary precedes recovery floor"));
    }
    let verified_boundary = crate::recovery::recover_with_target(
        dir,
        crate::recovery::RecoveryTarget::up_to_lsn(boundary_lsn),
        |_| Ok(()),
    )?;
    if verified_boundary != boundary_lsn {
        return Err(malformed(
            "wal fork target is not a retained record boundary",
        ));
    }

    let segments: Vec<_> = list_segments(dir)?
        .into_iter()
        .filter(|(index, _)| *index >= floor.segment_index)
        .collect();
    if segments.is_empty() {
        if boundary_lsn != floor.floor_lsn {
            return Err(malformed("wal fork boundary exceeds physical stream"));
        }
        let path = segment_path(dir, floor.segment_index);
        let file = open_boundary_segment(&path, true)?;
        file.set_len(0)?;
        ultrasql_core::fsync::durability_sync(&file)?;
        sync_dir(dir)?;
        return Ok(WalForkOutcome {
            boundary_lsn,
            boundary_segment: floor.segment_index,
            boundary_segment_len: 0,
            removed_segments: Vec::new(),
        });
    }

    let boundary = locate_boundary(
        &segments,
        floor.segment_index,
        floor.floor_lsn,
        boundary_lsn,
    )?;

    // Delete the later timeline first. Until the boundary segment is shortened,
    // an interrupted operation still has a self-consistent prefix to which the
    // durable recovery target can be applied on the next startup.
    let mut removed_segments = Vec::new();
    for (index, path) in &segments {
        if *index <= boundary.index {
            continue;
        }
        match std::fs::remove_file(path) {
            Ok(()) => removed_segments.push(*index),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(RecoveryError::Io(error)),
        }
    }
    sync_dir(dir)?;

    let file = open_boundary_segment(&boundary.path, false)?;
    file.set_len(boundary.keep_len)?;
    ultrasql_core::fsync::durability_sync(&file)?;

    Ok(WalForkOutcome {
        boundary_lsn,
        boundary_segment: boundary.index,
        boundary_segment_len: boundary.keep_len,
        removed_segments,
    })
}

#[derive(Debug)]
struct BoundarySegment {
    index: u32,
    path: PathBuf,
    keep_len: u64,
}

fn locate_boundary(
    segments: &[(u32, PathBuf)],
    floor_index: u32,
    floor_lsn: Lsn,
    boundary_lsn: Lsn,
) -> Result<BoundarySegment, RecoveryError> {
    let mut segment_start = floor_lsn.raw();
    let mut expected_index = floor_index;

    for (position, (index, path)) in segments.iter().enumerate() {
        if *index != expected_index {
            return Err(malformed("wal segment gap before fork boundary"));
        }
        let len = std::fs::metadata(path)?.len();

        // Prefer a segment whose start is the boundary. In particular, an LSN
        // at a rollover boundary keeps the following segment empty instead of
        // treating the preceding full segment as the fork tip.
        if boundary_lsn.raw() == segment_start {
            return Ok(BoundarySegment {
                index: *index,
                path: path.clone(),
                keep_len: 0,
            });
        }

        let segment_end = segment_start
            .checked_add(len)
            .ok_or_else(|| malformed("wal segment lsn overflow during fork"))?;
        if boundary_lsn.raw() < segment_end {
            return Ok(BoundarySegment {
                index: *index,
                path: path.clone(),
                keep_len: boundary_lsn.raw() - segment_start,
            });
        }
        if boundary_lsn.raw() == segment_end && position + 1 == segments.len() {
            return Ok(BoundarySegment {
                index: *index,
                path: path.clone(),
                keep_len: len,
            });
        }

        segment_start = segment_end;
        expected_index = expected_index
            .checked_add(1)
            .ok_or_else(|| malformed("wal segment index overflow during fork"))?;
    }

    Err(malformed("wal fork boundary exceeds physical stream"))
}

fn open_boundary_segment(path: &Path, create: bool) -> Result<File, RecoveryError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(create);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    options.open(path).map_err(RecoveryError::Io)
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<(), RecoveryError> {
    let dir = File::open(path)?;
    match dir.sync_all() {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => Ok(()),
        Err(error) => Err(RecoveryError::Io(error)),
    }
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> Result<(), RecoveryError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tempfile::TempDir;
    use ultrasql_core::Xid;

    use super::*;
    use crate::buffer::WalBuffer;
    use crate::manifest::{WalFloor, write_floor};
    use crate::record::{RecordType, WalRecord};
    use crate::recovery::recover;
    use crate::writer::{WalDurabilityWait, WalWriter, WalWriterConfig};

    fn record(xid: u64, payload_len: usize) -> WalRecord {
        WalRecord::new(
            RecordType::Nop,
            Xid::new(xid),
            Lsn::ZERO,
            0,
            vec![u8::try_from(xid).unwrap_or(0); payload_len],
        )
        .unwrap()
    }

    fn write_segment(dir: &Path, index: u32, records: &[WalRecord]) -> u64 {
        let mut bytes = Vec::new();
        for record in records {
            bytes.extend_from_slice(&record.encode());
        }
        std::fs::write(segment_path(dir, index), &bytes).unwrap();
        u64::try_from(bytes.len()).unwrap()
    }

    fn test_writer_config() -> WalWriterConfig {
        WalWriterConfig {
            segment_size_bytes: 1024,
            fsync_window_us: 100,
            fsync_batch_bytes: 1,
        }
    }

    #[test]
    fn fork_inside_segment_truncates_at_record_boundary() {
        let dir = TempDir::new().unwrap();
        let first = record(1, 7);
        let second = record(2, 11);
        let third = record(3, 13);
        let first_end = u64::try_from(first.encode().len()).unwrap();
        write_segment(dir.path(), 0, &[first.clone(), second, third]);

        let outcome = fork_wal_at(dir.path(), Lsn::new(first_end)).unwrap();

        assert_eq!(outcome.boundary_segment, 0);
        assert_eq!(outcome.boundary_segment_len, first_end);
        assert_eq!(
            std::fs::metadata(segment_path(dir.path(), 0))
                .unwrap()
                .len(),
            first_end
        );
        let mut seen = Vec::new();
        assert_eq!(
            recover(dir.path(), |record| {
                seen.push(record.header.xid);
                Ok(())
            })
            .unwrap(),
            Lsn::new(first_end)
        );
        assert_eq!(seen, vec![Xid::new(1)]);
    }

    #[test]
    fn fork_rejects_mid_record_target_without_modifying_wal() {
        let dir = TempDir::new().unwrap();
        let record = record(1, 17);
        let original = record.encode();
        std::fs::write(segment_path(dir.path(), 0), &original).unwrap();

        let error = fork_wal_at(dir.path(), Lsn::new(1)).unwrap_err();

        assert!(error.to_string().contains("not a retained record boundary"));
        assert_eq!(
            std::fs::read(segment_path(dir.path(), 0)).unwrap(),
            original
        );
    }

    #[test]
    fn fork_at_rollover_boundary_keeps_following_segment_empty() {
        let dir = TempDir::new().unwrap();
        let first = record(1, 5);
        let boundary = write_segment(dir.path(), 0, &[first]);
        write_segment(dir.path(), 1, &[record(2, 5)]);
        write_segment(dir.path(), 2, &[record(3, 5)]);

        let outcome = fork_wal_at(dir.path(), Lsn::new(boundary)).unwrap();

        assert_eq!(outcome.boundary_segment, 1);
        assert_eq!(outcome.boundary_segment_len, 0);
        assert_eq!(outcome.removed_segments, vec![2]);
        assert_eq!(
            std::fs::metadata(segment_path(dir.path(), 1))
                .unwrap()
                .len(),
            0
        );
        assert!(!segment_path(dir.path(), 2).exists());
    }

    #[test]
    fn fork_at_zero_keeps_origin_segment_empty() {
        let dir = TempDir::new().unwrap();
        write_segment(dir.path(), 0, &[record(1, 5)]);
        write_segment(dir.path(), 1, &[record(2, 5)]);

        let outcome = fork_wal_at(dir.path(), Lsn::ZERO).unwrap();

        assert_eq!(outcome.boundary_segment, 0);
        assert_eq!(outcome.boundary_segment_len, 0);
        assert_eq!(outcome.removed_segments, vec![1]);
        assert_eq!(
            std::fs::metadata(segment_path(dir.path(), 0))
                .unwrap()
                .len(),
            0
        );
        assert_eq!(recover(dir.path(), |_| Ok(())).unwrap(), Lsn::ZERO);
    }

    #[test]
    fn fork_respects_recycled_floor_and_rejects_older_target() {
        let dir = TempDir::new().unwrap();
        let floor = WalFloor {
            segment_index: 7,
            floor_lsn: Lsn::new(10_000),
        };
        write_floor(dir.path(), floor).unwrap();
        let first_len = write_segment(dir.path(), 7, &[record(7, 9)]);
        write_segment(dir.path(), 8, &[record(8, 9)]);

        let error = fork_wal_at(dir.path(), Lsn::new(9_999)).unwrap_err();
        assert!(error.to_string().contains("precedes recovery floor"));

        let boundary = Lsn::new(floor.floor_lsn.raw() + first_len);
        let outcome = fork_wal_at(dir.path(), boundary).unwrap();
        assert_eq!(outcome.boundary_segment, 8);
        assert_eq!(outcome.boundary_segment_len, 0);
        assert_eq!(recover(dir.path(), |_| Ok(())).unwrap(), boundary);
    }

    #[test]
    fn writer_restart_after_fork_continues_without_duplicate_lsn() {
        let dir = TempDir::new().unwrap();
        let retained = record(1, 5);
        let retained_end = write_segment(dir.path(), 0, std::slice::from_ref(&retained));
        write_segment(dir.path(), 1, &[record(2, 5)]);
        fork_wal_at(dir.path(), Lsn::new(retained_end)).unwrap();

        let buffer = Arc::new(WalBuffer::new(4096, Lsn::new(retained_end)));
        let writer = WalWriter::open(
            dir.path().to_path_buf(),
            Arc::clone(&buffer),
            test_writer_config(),
        )
        .unwrap();
        let replacement = record(3, 7);
        let replacement_start = buffer.append(&replacement).unwrap();
        assert_eq!(replacement_start, Lsn::new(retained_end));
        assert_eq!(
            writer.wait_for_record_durable(replacement_start, Duration::from_secs(5)),
            WalDurabilityWait::Durable
        );
        writer.shutdown().unwrap();

        let mut seen = Vec::new();
        let end = recover(dir.path(), |record| {
            seen.push(record.header.xid);
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![Xid::new(1), Xid::new(3)]);
        assert_eq!(
            end.raw(),
            retained_end + u64::try_from(replacement.encode().len()).unwrap()
        );
    }
}
