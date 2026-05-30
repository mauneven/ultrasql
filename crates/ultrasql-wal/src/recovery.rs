//! Crash-recovery replay for WAL segment directories.
//!
//! [`recover`] enumerates `segment_*` files in `wal_dir` in numeric
//! order, decodes each record, and hands it to a caller-supplied
//! applier. The applier is responsible for translating record bytes
//! into in-memory state changes (heap inserts, B+ tree page rewrites,
//! commit-record processing, etc.).
//!
//! Torn writes
//! -----------
//!
//! WAL records can be truncated by a crash mid-write. When the decoder
//! encounters a CRC mismatch or a "needs more bytes than available"
//! error past the start of a record, recovery treats the rest of the
//! file as torn-write residue: it logs a warning, stops scanning, and
//! returns the LSN of the last record that decoded cleanly. Earlier
//! segments are guaranteed durable because we fsync before rotating.

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use tracing::{debug, info, warn};
use ultrasql_core::{Lsn, Xid};

use crate::RecordType;
use crate::payload::CommitPayload;
use crate::record::{WalRecord, WalRecordError};
use crate::segment::list_segments;

const DEFAULT_RECOVERY_SEGMENT_READ_LIMIT_BYTES: u64 = 128 * 1024 * 1024;
const RECOVERY_SEGMENT_LIMIT_ENV: &str = "ULTRASQL_WAL_RECOVERY_SEGMENT_LIMIT_BYTES";

/// Optional point-in-time recovery target.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveryTarget {
    /// Stop before applying records whose end LSN is greater than this value.
    pub target_lsn: Option<Lsn>,
    /// Stop after applying the commit record for this transaction ID.
    pub target_xid: Option<Xid>,
    /// Stop before applying the first commit newer than this Unix timestamp.
    pub target_time_micros: Option<u64>,
}

impl RecoveryTarget {
    /// Recover every valid WAL record.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            target_lsn: None,
            target_xid: None,
            target_time_micros: None,
        }
    }

    /// Recover only records whose end LSN is less than or equal to `target`.
    #[must_use]
    pub const fn up_to_lsn(target: Lsn) -> Self {
        Self {
            target_lsn: Some(target),
            target_xid: None,
            target_time_micros: None,
        }
    }

    /// Recover through the commit record for `target`.
    #[must_use]
    pub const fn up_to_xid(target: Xid) -> Self {
        Self {
            target_lsn: None,
            target_xid: Some(target),
            target_time_micros: None,
        }
    }

    /// Recover commits whose timestamps are less than or equal to `target`.
    #[must_use]
    pub const fn up_to_time_micros(target: u64) -> Self {
        Self {
            target_lsn: None,
            target_xid: None,
            target_time_micros: Some(target),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplayDecision {
    Continue,
    StopBeforeRecord,
    StopAfterRecord,
}

fn replay_decision(
    record: &WalRecord,
    target: RecoveryTarget,
) -> Result<ReplayDecision, RecoveryError> {
    if record.header.record_type != RecordType::Commit {
        return Ok(ReplayDecision::Continue);
    }

    if let Some(target_time_micros) = target.target_time_micros {
        let payload = CommitPayload::decode(&record.payload)
            .map_err(|err| RecoveryError::Applier(format!("commit payload decode: {err}")))?;
        if payload.commit_timestamp_micros > target_time_micros {
            return Ok(ReplayDecision::StopBeforeRecord);
        }
    }

    if target.target_xid == Some(record.header.xid) {
        return Ok(ReplayDecision::StopAfterRecord);
    }

    Ok(ReplayDecision::Continue)
}

/// Errors that can arise during crash recovery.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    /// I/O error reading the WAL directory or a segment file.
    #[error("recovery io error: {0}")]
    Io(#[from] std::io::Error),

    /// A WAL record decoded successfully into a header but produced a
    /// fatal record-format error that is *not* a torn-write signal
    /// (e.g. an unknown record type, which suggests the WAL was
    /// written by a version of the software the current binary does
    /// not understand).
    #[error("recovery record error: {0}")]
    Record(#[from] WalRecordError),

    /// The applier returned an error. The string carries the
    /// applier's `Display` rendering so this enum stays generic over
    /// applier error types.
    #[error("recovery applier error: {0}")]
    Applier(String),
}

/// Replay every record in `wal_dir` to a caller-supplied applier.
///
/// Returns the byte length of the recovered prefix, expressed as an
/// `Lsn` (i.e. the LSN where the next record would have been written,
/// measured from the start of the WAL stream). An empty WAL directory
/// returns [`Lsn::ZERO`].
///
/// Stops scanning at the first torn-write or CRC failure, treating it
/// as the tail of the log. The applier is not called for any record
/// past that point.
pub fn recover(
    wal_dir: impl AsRef<Path>,
    mut apply: impl FnMut(&WalRecord) -> Result<(), RecoveryError>,
) -> Result<Lsn, RecoveryError> {
    recover_with_target(wal_dir, RecoveryTarget::none(), |record| apply(record))
}

/// Replay records in `wal_dir` up to an optional recovery target.
///
/// The LSN target is a physical prefix target: records ending after the target
/// are not applied, and the returned LSN is the end of the last applied record.
pub fn recover_with_target(
    wal_dir: impl AsRef<Path>,
    target: RecoveryTarget,
    mut apply: impl FnMut(&WalRecord) -> Result<(), RecoveryError>,
) -> Result<Lsn, RecoveryError> {
    let dir = wal_dir.as_ref();
    let segments = list_segments(dir)?;
    if segments.is_empty() {
        debug!(?dir, "wal recovery: no segments found");
        return Ok(Lsn::ZERO);
    }

    let mut stream_pos: u64 = 0;
    let mut record_count: u64 = 0;
    let mut last_good_pos: u64 = 0;

    for (index, path) in segments {
        let buf = read_segment_bytes(&path)?;
        debug!(
            ?path,
            segment = index,
            bytes = buf.len(),
            "wal recovery: scanning segment"
        );

        let mut offset = 0;
        while offset < buf.len() {
            match WalRecord::decode(&buf[offset..]) {
                Ok((record, used)) => {
                    let used_u64 = u64::try_from(used).unwrap_or(u64::MAX);
                    let record_end = stream_pos.saturating_add(used_u64);
                    if let Some(target_lsn) = target.target_lsn
                        && record_end > target_lsn.raw()
                    {
                        info!(
                            target_lsn = target_lsn.raw(),
                            last_lsn = last_good_pos,
                            "wal recovery: reached target lsn"
                        );
                        return Ok(Lsn::new(last_good_pos));
                    }
                    let decision = replay_decision(&record, target)?;
                    if decision == ReplayDecision::StopBeforeRecord {
                        info!(
                            last_lsn = last_good_pos,
                            xid = record.header.xid.raw(),
                            "wal recovery: reached target time"
                        );
                        return Ok(Lsn::new(last_good_pos));
                    }
                    apply(&record).map_err(|e| match e {
                        RecoveryError::Applier(s) => RecoveryError::Applier(s),
                        other => RecoveryError::Applier(other.to_string()),
                    })?;
                    offset += used;
                    stream_pos = record_end;
                    last_good_pos = stream_pos;
                    record_count = record_count.saturating_add(1);
                    if decision == ReplayDecision::StopAfterRecord {
                        info!(
                            last_lsn = last_good_pos,
                            xid = record.header.xid.raw(),
                            "wal recovery: reached target xid"
                        );
                        return Ok(Lsn::new(last_good_pos));
                    }
                }
                Err(WalRecordError::Truncated { needed, have }) => {
                    warn!(
                        ?path,
                        needed, have, "wal recovery: torn record at tail; stopping cleanly"
                    );
                    return Ok(Lsn::new(last_good_pos));
                }
                Err(WalRecordError::CrcMismatch { expected, actual }) => {
                    warn!(
                        ?path,
                        expected = format!("{expected:08x}"),
                        actual = format!("{actual:08x}"),
                        "wal recovery: crc mismatch at tail; stopping cleanly"
                    );
                    return Ok(Lsn::new(last_good_pos));
                }
                Err(other) => return Err(RecoveryError::Record(other)),
            }
        }
    }

    info!(
        records = record_count,
        last_lsn = last_good_pos,
        "wal recovery complete"
    );
    Ok(Lsn::new(last_good_pos))
}

fn read_segment_bytes(path: &Path) -> Result<Vec<u8>, RecoveryError> {
    read_segment_bytes_with_limit(path, recovery_segment_read_limit_bytes())
}

fn read_segment_bytes_with_limit(path: &Path, limit: u64) -> Result<Vec<u8>, RecoveryError> {
    let file = open_recovery_segment(path)?;
    let file_len = file.metadata()?.len();
    if file_len > limit {
        return Err(recovery_segment_limit_error(path, file_len, limit));
    }

    let max_read = limit.saturating_add(1);
    let mut reader = file.take(max_read);
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    let bytes_read = u64::try_from(buf.len()).unwrap_or(u64::MAX);
    if bytes_read > limit {
        return Err(recovery_segment_limit_error(path, bytes_read, limit));
    }
    Ok(buf)
}

fn recovery_segment_read_limit_bytes() -> u64 {
    std::env::var(RECOVERY_SEGMENT_LIMIT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|&limit| limit > 0)
        .unwrap_or(DEFAULT_RECOVERY_SEGMENT_READ_LIMIT_BYTES)
}

fn recovery_segment_limit_error(path: &Path, bytes: u64, limit: u64) -> RecoveryError {
    RecoveryError::Io(std::io::Error::new(
        ErrorKind::InvalidData,
        format!(
            "WAL segment exceeds recovery read limit: path={} bytes={} limit={} env={}",
            path.display(),
            bytes,
            limit,
            RECOVERY_SEGMENT_LIMIT_ENV
        ),
    ))
}

#[cfg_attr(not(unix), allow(unused_variables))]
fn open_recovery_segment(path: &Path) -> std::io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.read(true);
    #[cfg(unix)]
    opts.custom_flags(libc::O_NOFOLLOW);
    opts.open(path)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use ultrasql_core::Xid;

    use crate::payload::CommitPayload;
    use crate::{RecordType, WalRecord};

    use super::*;

    #[test]
    fn empty_dir_returns_zero() {
        let dir = TempDir::new().unwrap();
        let lsn = recover(dir.path(), |_| Ok(())).unwrap();
        assert_eq!(lsn, Lsn::ZERO);
    }

    #[test]
    fn missing_dir_returns_zero() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("does-not-exist");
        let lsn = recover(&missing, |_| Ok(())).unwrap();
        assert_eq!(lsn, Lsn::ZERO);
    }

    #[test]
    fn recover_with_lsn_target_stops_before_later_records() {
        let dir = TempDir::new().unwrap();
        let first = WalRecord::new(RecordType::Nop, Xid::new(10), Lsn::ZERO, 0, Vec::new())
            .expect("test WAL record should fit size limits");
        let first_bytes = first.encode();
        let second = WalRecord::new(RecordType::Nop, Xid::new(11), Lsn::ZERO, 0, Vec::new())
            .expect("test WAL record should fit size limits");
        let target = Lsn::new(u64::try_from(first_bytes.len()).unwrap());
        let mut segment = first_bytes;
        segment.extend_from_slice(&second.encode());
        std::fs::write(dir.path().join("segment_0000000000"), segment).unwrap();

        let mut seen = Vec::new();
        let recovered =
            recover_with_target(dir.path(), RecoveryTarget::up_to_lsn(target), |record| {
                seen.push(record.header.xid.raw());
                Ok(())
            })
            .unwrap();

        assert_eq!(seen, vec![10]);
        assert_eq!(recovered, target);
    }

    #[test]
    fn recover_with_xid_target_stops_after_target_commit() {
        let dir = TempDir::new().unwrap();
        let first = commit_record(Xid::new(10), 1_000);
        let first_bytes = first.encode();
        let second = commit_record(Xid::new(11), 2_000);
        let mut segment = first_bytes.clone();
        segment.extend_from_slice(&second.encode());
        std::fs::write(dir.path().join("segment_0000000000"), segment).unwrap();

        let mut seen = Vec::new();
        let recovered = recover_with_target(
            dir.path(),
            RecoveryTarget::up_to_xid(Xid::new(10)),
            |record| {
                seen.push(record.header.xid.raw());
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(seen, vec![10]);
        assert_eq!(
            recovered,
            Lsn::new(u64::try_from(first_bytes.len()).unwrap())
        );
    }

    #[test]
    fn recover_with_time_target_stops_before_later_commit() {
        let dir = TempDir::new().unwrap();
        let first = commit_record(Xid::new(10), 1_000);
        let first_bytes = first.encode();
        let second = commit_record(Xid::new(11), 2_000);
        let mut segment = first_bytes.clone();
        segment.extend_from_slice(&second.encode());
        std::fs::write(dir.path().join("segment_0000000000"), segment).unwrap();

        let mut seen = Vec::new();
        let recovered = recover_with_target(
            dir.path(),
            RecoveryTarget::up_to_time_micros(1_500),
            |record| {
                seen.push(record.header.xid.raw());
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(seen, vec![10]);
        assert_eq!(
            recovered,
            Lsn::new(u64::try_from(first_bytes.len()).unwrap())
        );
    }

    #[test]
    fn recovery_rejects_configured_oversized_segment() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("segment_0000000000");
        std::fs::write(&path, [0_u8; 4]).unwrap();

        let err = read_segment_bytes_with_limit(&path, 3).unwrap_err();
        assert!(
            err.to_string()
                .contains("WAL segment exceeds recovery read limit"),
            "{err}"
        );
    }

    fn commit_record(xid: Xid, commit_timestamp_micros: u64) -> WalRecord {
        let payload = CommitPayload {
            commit_lsn: Lsn::ZERO,
            commit_timestamp_micros,
        };
        WalRecord::new(RecordType::Commit, xid, Lsn::ZERO, 0, payload.encode())
            .expect("test WAL record should fit size limits")
    }
}
