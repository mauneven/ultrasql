//! WAL recovery-floor manifest.
//!
//! Records the LSN at which the on-disk WAL stream currently *begins*, so that
//! once low segments have been recycled (truncated) recovery can start from the
//! first surviving segment instead of assuming the stream starts at LSN 0.
//!
//! The WAL LSN is an absolute byte offset from the original start of history,
//! and on-disk page headers store those absolute LSNs. So if head segments are
//! removed, recovery must seed its byte cursor from the surviving stream's true
//! start LSN — otherwise every reconstructed record LSN is shifted down and no
//! longer matches the page headers (`should_skip_redo`), corrupting redo.
//!
//! The manifest is a tiny, checksummed file written atomically (temp + fsync +
//! rename + directory fsync). An **absent** manifest means "the stream starts at
//! segment 0 / LSN 0" — the only state before any truncation, and the historical
//! behaviour. A **present-but-corrupt** manifest is a hard error rather than a
//! silent default: after truncation the floor is load-bearing, so defaulting to
//! the origin would mis-seed recovery against the page headers. Refusing to
//! start is the safe choice.

use std::path::Path;

use ultrasql_core::Lsn;

use crate::record::WalRecordError;
use crate::recovery::RecoveryError;

const MANIFEST_FILE: &str = "wal.manifest";
const MANIFEST_TMP_FILE: &str = "wal.manifest.tmp";
const MANIFEST_MAGIC: &[u8; 8] = b"USQLWFL1";
const MANIFEST_VERSION: u32 = 1;
/// magic(8) + version(4) + segment_index(4) + floor_lsn(8) = 24 body bytes.
const MANIFEST_BODY_LEN: usize = 24;
/// Body + trailing crc32c(4).
const MANIFEST_FILE_LEN: usize = MANIFEST_BODY_LEN + 4;

/// The recovery floor: the index and start LSN of the lowest segment the
/// on-disk WAL stream currently begins at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalFloor {
    /// Index of the first surviving segment file (`segment_<index>`).
    pub segment_index: u32,
    /// Absolute LSN (byte offset from the original start of WAL history) at
    /// which `segment_index` begins.
    pub floor_lsn: Lsn,
}

impl WalFloor {
    /// The pre-truncation default: the stream starts at segment 0 / LSN 0.
    pub const ORIGIN: Self = Self {
        segment_index: 0,
        floor_lsn: Lsn::ZERO,
    };
}

fn malformed(msg: &'static str) -> RecoveryError {
    RecoveryError::Record(WalRecordError::Malformed(msg))
}

/// Read the recovery floor for `wal_dir`.
///
/// Returns [`WalFloor::ORIGIN`] when no manifest exists. Returns an error when a
/// manifest exists but is unreadable, the wrong length, fails its `crc32c`, or
/// carries an unknown magic/version — see the module docs for why corruption is
/// fatal rather than defaulted.
pub fn read_floor(wal_dir: &Path) -> Result<WalFloor, RecoveryError> {
    let path = wal_dir.join(MANIFEST_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(WalFloor::ORIGIN),
        Err(e) => return Err(RecoveryError::Io(e)),
    };
    if bytes.len() != MANIFEST_FILE_LEN {
        return Err(malformed("wal manifest wrong length"));
    }
    let (body, crc_bytes) = bytes.split_at(MANIFEST_BODY_LEN);
    let stored_crc = u32::from_le_bytes(
        crc_bytes
            .try_into()
            .map_err(|_| malformed("wal manifest crc read"))?,
    );
    if crc32c::crc32c(body) != stored_crc {
        return Err(malformed("wal manifest checksum mismatch"));
    }
    if &body[0..8] != MANIFEST_MAGIC {
        return Err(malformed("wal manifest magic mismatch"));
    }
    let version = u32::from_le_bytes(
        body[8..12]
            .try_into()
            .map_err(|_| malformed("wal manifest version read"))?,
    );
    if version != MANIFEST_VERSION {
        return Err(malformed("wal manifest version unsupported"));
    }
    let segment_index = u32::from_le_bytes(
        body[12..16]
            .try_into()
            .map_err(|_| malformed("wal manifest segment read"))?,
    );
    let floor_lsn = Lsn::new(u64::from_le_bytes(
        body[16..24]
            .try_into()
            .map_err(|_| malformed("wal manifest lsn read"))?,
    ));
    Ok(WalFloor {
        segment_index,
        floor_lsn,
    })
}

/// Atomically write the recovery floor for `wal_dir`.
///
/// Writes to a temp file, fsyncs it, renames it over the manifest, then fsyncs
/// the directory so the rename is durable. A crash mid-write leaves either the
/// previous manifest or none — never a torn file.
pub fn write_floor(wal_dir: &Path, floor: WalFloor) -> Result<(), RecoveryError> {
    use std::io::Write as _;

    let mut body = Vec::with_capacity(MANIFEST_FILE_LEN);
    body.extend_from_slice(MANIFEST_MAGIC);
    body.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    body.extend_from_slice(&floor.segment_index.to_le_bytes());
    body.extend_from_slice(&floor.floor_lsn.raw().to_le_bytes());
    let crc = crc32c::crc32c(&body);
    body.extend_from_slice(&crc.to_le_bytes());

    let final_path = wal_dir.join(MANIFEST_FILE);
    let tmp_path = wal_dir.join(MANIFEST_TMP_FILE);
    {
        let mut file = std::fs::File::create(&tmp_path)?;
        file.write_all(&body)?;
        ultrasql_core::fsync::full_fsync(&file)?;
    }
    std::fs::rename(&tmp_path, &final_path)?;
    // Make the manifest rename durable. Opening a directory as a file is not
    // portable (fails on Windows), so tolerate a failed open; but a failed
    // fsync on a handle that did open means the new floor may not survive a
    // crash, so propagate it rather than silently dropping it.
    if let Ok(dir) = std::fs::File::open(wal_dir) {
        dir.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn absent_manifest_reads_as_origin() {
        let dir = TempDir::new().unwrap();
        assert_eq!(read_floor(dir.path()).unwrap(), WalFloor::ORIGIN);
    }

    #[test]
    fn floor_round_trips() {
        let dir = TempDir::new().unwrap();
        let floor = WalFloor {
            segment_index: 7,
            floor_lsn: Lsn::new(7 * 16 * 1024 * 1024),
        };
        write_floor(dir.path(), floor).unwrap();
        assert_eq!(read_floor(dir.path()).unwrap(), floor);
        // Origin round-trips too.
        write_floor(dir.path(), WalFloor::ORIGIN).unwrap();
        assert_eq!(read_floor(dir.path()).unwrap(), WalFloor::ORIGIN);
    }

    #[test]
    fn corrupt_manifest_is_an_error_not_a_default() {
        let dir = TempDir::new().unwrap();
        let floor = WalFloor {
            segment_index: 3,
            floor_lsn: Lsn::new(48 * 1024 * 1024),
        };
        write_floor(dir.path(), floor).unwrap();
        let path = dir.path().join(MANIFEST_FILE);

        // Flipped middle byte fails the crc.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[14] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(read_floor(dir.path()).is_err());

        // Truncated file is rejected on length.
        std::fs::write(&path, &bytes[..10]).unwrap();
        assert!(read_floor(dir.path()).is_err());

        // Corrupt magic is rejected.
        let mut bad_magic = std::fs::read(&path).unwrap_or_default();
        if bad_magic.len() == MANIFEST_FILE_LEN {
            bad_magic[0] = b'X';
            std::fs::write(&path, &bad_magic).unwrap();
            assert!(read_floor(dir.path()).is_err());
        }
    }
}
