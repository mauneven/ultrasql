//! Vector-index and CLOG snapshot encode/decode, data-directory hardening,
//! and recovery-target parsing.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

pub(crate) fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(crate) fn pages_to_bytes_saturated(pages: usize) -> u64 {
    usize_to_u64_saturated(pages).saturating_mul(usize_to_u64_saturated(PAGE_SIZE))
}

/// Directory holding per-index vector-index snapshots under a data dir.
pub(crate) fn vector_snapshot_dir(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("vecsnap")
}

/// Fold one index's snapshot LSN into a running minimum, ignoring `Lsn::ZERO`.
///
/// The minimum over a vector-index family bounds the WAL recycling floor: the
/// floor must stay at or below every index's snapshot LSN so the WAL above it
/// (which the index replays on restart) survives. A `ZERO` snapshot LSN means
/// the index has had no logged mutation — and therefore has no WAL records of
/// its own — so it imposes no floor and must be excluded, otherwise an empty or
/// never-written index would pin the floor at 0 and block all recycling.
pub(crate) fn fold_min_nonzero_lsn(acc: Option<Lsn>, candidate: Lsn) -> Option<Lsn> {
    if candidate == Lsn::ZERO {
        return acc;
    }
    Some(acc.map_or(candidate, |current| {
        if current.raw() <= candidate.raw() {
            current
        } else {
            candidate
        }
    }))
}

/// Read a vector-index snapshot if one exists. Returns `None` on absence or any
/// IO error — the caller then falls back to full WAL replay, so a missing or
/// unreadable snapshot only costs a slower restart, never correctness.
pub(crate) fn read_vector_snapshot(data_dir: &Path, oid: ultrasql_core::Oid) -> Option<Vec<u8>> {
    std::fs::read(vector_snapshot_dir(data_dir).join(format!("{}.snap", oid.raw()))).ok()
}

/// Durably write a vector-index snapshot via temp-file + atomic rename so a
/// crash mid-write never leaves a torn `<oid>.snap` (the previous snapshot, or
/// none, survives and a corrupt one is rejected by `from_snapshot_bytes`).
/// Best-effort by contract: the WAL remains the source of truth, so a failed
/// snapshot only slows the next restart.
pub(crate) fn write_vector_snapshot(
    data_dir: &Path,
    oid: ultrasql_core::Oid,
    bytes: &[u8],
) -> std::io::Result<()> {
    use std::io::Write as _;
    let dir = vector_snapshot_dir(data_dir);
    std::fs::create_dir_all(&dir)?;
    let final_path = dir.join(format!("{}.snap", oid.raw()));
    let tmp_path = dir.join(format!("{}.snap.tmp", oid.raw()));
    {
        let mut file = std::fs::File::create(&tmp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp_path, &final_path)?;
    // fsync the directory so the rename entry itself is durable. Opening a
    // directory as a file is not portable (it fails on Windows), so tolerate a
    // failed open; but if the handle opens, a failed fsync means the rename may
    // not be durable, so surface it instead of silently dropping it.
    if let Ok(dir_file) = std::fs::File::open(&dir) {
        dir_file.sync_all()?;
    }
    Ok(())
}

pub(crate) const CLOG_SNAPSHOT_MAGIC: &[u8; 8] = b"USQLCLG1";
pub(crate) const CLOG_SNAPSHOT_VERSION: u32 = 1;
/// magic(8) + version(4) + snapshot_lsn(8) + next_xid(8) + count(4).
pub(crate) const CLOG_SNAPSHOT_HEADER_LEN: usize = 32;
/// xid(8) + status(1) per entry.
pub(crate) const CLOG_SNAPSHOT_ENTRY_LEN: usize = 9;

/// A decoded commit-log snapshot: its WAL LSN, the allocator next-XID, and the
/// terminal `(xid, status)` entries.
pub(crate) type DecodedClogSnapshot = (Lsn, u64, Vec<(Xid, ultrasql_mvcc::XidStatus)>);

pub(crate) fn clog_status_to_u8(status: ultrasql_mvcc::XidStatus) -> u8 {
    match status {
        ultrasql_mvcc::XidStatus::InProgress => 0,
        ultrasql_mvcc::XidStatus::Committed => 1,
        ultrasql_mvcc::XidStatus::Aborted => 2,
        ultrasql_mvcc::XidStatus::Frozen => 3,
    }
}

pub(crate) fn clog_status_from_u8(byte: u8) -> Result<ultrasql_mvcc::XidStatus, ServerError> {
    match byte {
        0 => Ok(ultrasql_mvcc::XidStatus::InProgress),
        1 => Ok(ultrasql_mvcc::XidStatus::Committed),
        2 => Ok(ultrasql_mvcc::XidStatus::Aborted),
        3 => Ok(ultrasql_mvcc::XidStatus::Frozen),
        other => Err(ServerError::ddl(format!(
            "clog snapshot invalid status tag {other}"
        ))),
    }
}

/// Serialize the commit log to a versioned, crc32c-checksummed byte buffer.
pub(crate) fn encode_clog_snapshot(
    snapshot_lsn: Lsn,
    next_xid: u64,
    entries: &[(Xid, ultrasql_mvcc::XidStatus)],
) -> Vec<u8> {
    let count = u32::try_from(entries.len()).unwrap_or(u32::MAX);
    let mut out =
        Vec::with_capacity(CLOG_SNAPSHOT_HEADER_LEN + entries.len() * CLOG_SNAPSHOT_ENTRY_LEN + 4);
    out.extend_from_slice(CLOG_SNAPSHOT_MAGIC);
    out.extend_from_slice(&CLOG_SNAPSHOT_VERSION.to_le_bytes());
    out.extend_from_slice(&snapshot_lsn.raw().to_le_bytes());
    out.extend_from_slice(&next_xid.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    for (xid, status) in entries {
        out.extend_from_slice(&xid.raw().to_le_bytes());
        out.push(clog_status_to_u8(*status));
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Strictly decode a commit-log snapshot, rejecting any corruption (so a caller
/// falls back to full WAL commit-status rebuild). Returns the snapshot LSN, the
/// allocator next-XID, and the terminal `(xid, status)` entries.
pub(crate) fn decode_clog_snapshot(bytes: &[u8]) -> Result<DecodedClogSnapshot, ServerError> {
    let body_len = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| ServerError::ddl("clog snapshot too short".to_owned()))?;
    let (body, crc_bytes) = bytes.split_at(body_len);
    let stored = u32::from_le_bytes(
        crc_bytes
            .try_into()
            .map_err(|_| ServerError::ddl("clog snapshot crc read".to_owned()))?,
    );
    if crc32c::crc32c(body) != stored {
        return Err(ServerError::ddl(
            "clog snapshot checksum mismatch".to_owned(),
        ));
    }
    if body.len() < CLOG_SNAPSHOT_HEADER_LEN || &body[0..8] != CLOG_SNAPSHOT_MAGIC {
        return Err(ServerError::ddl("clog snapshot header invalid".to_owned()));
    }
    let read_u32 = |o: usize| -> Result<u32, ServerError> {
        body.get(o..o + 4)
            .and_then(|s| s.try_into().ok())
            .map(u32::from_le_bytes)
            .ok_or_else(|| ServerError::ddl("clog snapshot truncated".to_owned()))
    };
    let read_u64 = |o: usize| -> Result<u64, ServerError> {
        body.get(o..o + 8)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
            .ok_or_else(|| ServerError::ddl("clog snapshot truncated".to_owned()))
    };
    let version = read_u32(8)?;
    if version != CLOG_SNAPSHOT_VERSION {
        return Err(ServerError::ddl(format!(
            "clog snapshot version {version} unsupported"
        )));
    }
    let snapshot_lsn = Lsn::new(read_u64(12)?);
    let next_xid = read_u64(20)?;
    let count = usize::try_from(read_u32(28)?)
        .map_err(|_| ServerError::ddl("clog snapshot count overflow".to_owned()))?;
    let entries_len = count
        .checked_mul(CLOG_SNAPSHOT_ENTRY_LEN)
        .and_then(|n| n.checked_add(CLOG_SNAPSHOT_HEADER_LEN))
        .ok_or_else(|| ServerError::ddl("clog snapshot size overflow".to_owned()))?;
    if body.len() != entries_len {
        return Err(ServerError::ddl("clog snapshot length mismatch".to_owned()));
    }
    let mut entries = Vec::with_capacity(count.min(1 << 20));
    let mut off = CLOG_SNAPSHOT_HEADER_LEN;
    for _ in 0..count {
        let xid = Xid::new(read_u64(off)?);
        let status = clog_status_from_u8(
            *body
                .get(off + 8)
                .ok_or_else(|| ServerError::ddl("clog snapshot truncated".to_owned()))?,
        )?;
        entries.push((xid, status));
        off += CLOG_SNAPSHOT_ENTRY_LEN;
    }
    Ok((snapshot_lsn, next_xid, entries))
}

pub(crate) fn clog_snapshot_path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("clog.snapshot")
}

pub(crate) fn read_clog_snapshot(data_dir: &Path) -> Option<Vec<u8>> {
    std::fs::read(clog_snapshot_path(data_dir)).ok()
}

/// Durably write the commit-log snapshot via temp-file + atomic rename + fsync.
pub(crate) fn write_clog_snapshot(data_dir: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let final_path = clog_snapshot_path(data_dir);
    let tmp_path = data_dir.join("clog.snapshot.tmp");
    {
        let mut file = std::fs::File::create(&tmp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp_path, &final_path)?;
    // Make the rename durable. Tolerate a non-portable directory-open failure
    // (Windows), but propagate a real fsync failure on platforms where the
    // directory handle opens.
    if let Ok(dir) = std::fs::File::open(data_dir) {
        dir.sync_all()?;
    }
    Ok(())
}

pub(crate) fn recovery_replay_target_from_data_dir(
    data_dir: &Path,
) -> Result<ultrasql_wal::RecoveryTarget, ServerError> {
    let path = data_dir.join("recovery.targets");
    let Some(text) = read_capped_regular_text_file(
        &path,
        "recovery targets file",
        RECOVERY_TARGETS_FILE_LIMIT_BYTES,
    )?
    else {
        return Ok(ultrasql_wal::RecoveryTarget::none());
    };
    let mut target = ultrasql_wal::RecoveryTarget::none();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim().trim_matches('\'').trim_matches('"');
        if key.eq_ignore_ascii_case("recovery_target_lsn") {
            target.target_lsn = Some(parse_recovery_lsn(value)?);
        } else if key.eq_ignore_ascii_case("recovery_target_time") {
            target.target_time_micros = Some(parse_recovery_time_micros(value)?);
        } else if key.eq_ignore_ascii_case("recovery_target_xid") {
            target.target_xid = Some(parse_recovery_xid(value)?);
        }
    }
    Ok(target)
}

pub(crate) fn prepare_secure_data_dir(data_dir: &Path) -> Result<PathBuf, ServerError> {
    reject_data_dir_symlink(data_dir)?;
    let existed = data_dir.try_exists().map_err(ServerError::Io)?;
    std::fs::create_dir_all(data_dir).map_err(ServerError::Io)?;
    reject_data_dir_symlink(data_dir)?;
    let canonical = data_dir.canonicalize().map_err(ServerError::Io)?;
    validate_data_dir_ownership(&canonical)?;
    validate_data_dir_permissions(&canonical, existed)?;
    Ok(canonical)
}

pub(crate) fn reject_data_dir_symlink(data_dir: &Path) -> Result<(), ServerError> {
    let metadata = match std::fs::symlink_metadata(data_dir) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(ServerError::Io(err)),
    };
    if metadata.file_type().is_symlink() {
        return Err(ServerError::ddl(format!(
            "data directory {} is a symlink; use a canonical non-symlink path",
            data_dir.display()
        )));
    }
    Ok(())
}

pub(crate) fn validate_data_dir_ownership(data_dir: &Path) -> Result<(), ServerError> {
    #[cfg(unix)]
    {
        validate_data_dir_owner(data_dir, effective_uid())
    }
    #[cfg(not(unix))]
    {
        let _ = data_dir;
        Ok(())
    }
}

pub(crate) fn validate_data_dir_permissions(
    data_dir: &Path,
    existed: bool,
) -> Result<(), ServerError> {
    #[cfg(unix)]
    {
        validate_data_dir_mode(data_dir, existed)
    }
    #[cfg(not(unix))]
    {
        let _ = (data_dir, existed);
        Ok(())
    }
}

#[cfg(unix)]
pub(crate) fn validate_data_dir_owner(
    data_dir: &Path,
    expected_uid: u32,
) -> Result<(), ServerError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::metadata(data_dir).map_err(ServerError::Io)?;
    if !metadata.is_dir() {
        return Err(ServerError::ddl(format!(
            "data directory {} is not a directory",
            data_dir.display()
        )));
    }
    let actual_uid = metadata.uid();
    if actual_uid != expected_uid {
        return Err(ServerError::ddl(format!(
            "data directory {} is owned by uid {actual_uid}, expected effective uid {expected_uid}",
            data_dir.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn validate_data_dir_mode(data_dir: &Path, existed: bool) -> Result<(), ServerError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    const PRIVATE_DIR_MODE: u32 = 0o700;
    const GROUP_OR_WORLD_BITS: u32 = 0o077;

    let metadata = std::fs::metadata(data_dir).map_err(ServerError::Io)?;
    let mode = metadata.mode() & 0o777;
    if mode & GROUP_OR_WORLD_BITS == 0 {
        return Ok(());
    }
    if !existed || data_dir_is_empty(data_dir)? {
        std::fs::set_permissions(data_dir, std::fs::Permissions::from_mode(PRIVATE_DIR_MODE))
            .map_err(ServerError::Io)?;
        let tightened = std::fs::metadata(data_dir).map_err(ServerError::Io)?.mode() & 0o777;
        if tightened & GROUP_OR_WORLD_BITS == 0 {
            return Ok(());
        }
    }
    Err(ServerError::ddl(format!(
        "data directory {} has group/world permissions {:o}; chmod 700 before startup",
        data_dir.display(),
        mode
    )))
}

#[cfg(unix)]
pub(crate) fn data_dir_is_empty(data_dir: &Path) -> Result<bool, ServerError> {
    let mut entries = std::fs::read_dir(data_dir).map_err(ServerError::Io)?;
    entries
        .next()
        .transpose()
        .map_err(ServerError::Io)
        .map(|entry| entry.is_none())
}

#[cfg(unix)]
pub(crate) fn effective_uid() -> u32 {
    // SAFETY: `geteuid` has no preconditions and only reads process credentials.
    unsafe { libc::geteuid() }
}

pub(crate) fn parse_recovery_lsn(value: &str) -> Result<Lsn, ServerError> {
    let value = value.trim();
    if let Some((high, low)) = value.split_once('/') {
        let high = u64::from_str_radix(high, 16)
            .map_err(|_| ServerError::ddl("invalid recovery_target_lsn high half"))?;
        let low = u64::from_str_radix(low, 16)
            .map_err(|_| ServerError::ddl("invalid recovery_target_lsn low half"))?;
        if high > u64::from(u32::MAX) || low > u64::from(u32::MAX) {
            return Err(ServerError::ddl("recovery_target_lsn half out of range"));
        }
        return Ok(Lsn::new((high << 32) | low));
    }
    value
        .parse::<u64>()
        .map(Lsn::new)
        .map_err(|_| ServerError::ddl("invalid recovery_target_lsn"))
}

pub(crate) fn parse_recovery_time_micros(value: &str) -> Result<u64, ServerError> {
    let value = value.trim();
    let normalized = if value.contains(' ') && !value.contains('T') {
        value.replacen(' ', "T", 1)
    } else {
        value.to_owned()
    };
    let parsed = chrono::DateTime::parse_from_rfc3339(&normalized)
        .map_err(|_| ServerError::ddl("invalid recovery_target_time"))?;
    u64::try_from(parsed.timestamp_micros())
        .map_err(|_| ServerError::ddl("recovery_target_time before Unix epoch"))
}

pub(crate) fn parse_recovery_xid(value: &str) -> Result<Xid, ServerError> {
    let raw = value
        .trim()
        .parse::<u64>()
        .map_err(|_| ServerError::ddl("invalid recovery_target_xid"))?;
    if raw == 0 {
        return Err(ServerError::ddl("recovery_target_xid must be nonzero"));
    }
    Ok(Xid::new(raw))
}
