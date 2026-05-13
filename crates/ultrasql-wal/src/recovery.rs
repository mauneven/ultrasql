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

use std::io::Read;
use std::path::Path;

use tracing::{debug, info, warn};
use ultrasql_core::Lsn;

use crate::record::{WalRecord, WalRecordError};
use crate::segment::list_segments;

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
        let mut file = std::fs::File::open(&path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
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
                    apply(&record).map_err(|e| match e {
                        RecoveryError::Applier(s) => RecoveryError::Applier(s),
                        other => RecoveryError::Applier(other.to_string()),
                    })?;
                    offset += used;
                    stream_pos = stream_pos.saturating_add(used as u64);
                    last_good_pos = stream_pos;
                    record_count = record_count.saturating_add(1);
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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

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
}
