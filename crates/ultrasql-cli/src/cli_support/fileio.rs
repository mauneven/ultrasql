//! Regular-file read/write helpers (symlink-hardened) plus hex and checksum
//! utilities shared by the dump, backup, and WAL subcommands.

use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

pub(crate) const DEFAULT_SQL_SCRIPT_FILE_LIMIT_BYTES: u64 = 128 * 1024 * 1024;

pub(crate) fn read_regular_text_file(path: &Path, context: &str) -> Result<String> {
    let mut bytes = Vec::new();
    let mut file = open_regular_source_file(path, context)?;
    std::io::Read::read_to_end(&mut file, &mut bytes)
        .with_context(|| format!("cannot read {context}: {}", path.display()))?;
    String::from_utf8(bytes).with_context(|| format!("{context} is not UTF-8: {}", path.display()))
}

pub(crate) fn read_sql_script_file(path: &Path) -> Result<String> {
    read_regular_text_file_capped(path, "SQL script", sql_script_file_limit_bytes())
}

fn sql_script_file_limit_bytes() -> u64 {
    std::env::var("ULTRASQL_SQL_SCRIPT_FILE_LIMIT_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SQL_SCRIPT_FILE_LIMIT_BYTES)
}

fn read_regular_text_file_capped(path: &Path, context: &str, limit: u64) -> Result<String> {
    let bytes = read_regular_file_capped(path, context, limit)?;
    String::from_utf8(bytes).with_context(|| format!("{context} is not UTF-8: {}", path.display()))
}

pub(crate) fn read_regular_file(path: &Path, context: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut file = open_regular_source_file(path, context)?;
    std::io::Read::read_to_end(&mut file, &mut bytes)
        .with_context(|| format!("cannot read {context}: {}", path.display()))?;
    Ok(bytes)
}

pub(crate) fn read_regular_file_capped(path: &Path, context: &str, limit: u64) -> Result<Vec<u8>> {
    let file = open_regular_source_file(path, context)?;
    let len = file
        .metadata()
        .with_context(|| format!("cannot inspect {context}: {}", path.display()))?
        .len();
    if len > limit {
        anyhow::bail!(
            "{context} exceeds read limit: {} size={} limit={}",
            path.display(),
            len,
            limit
        );
    }
    let mut bytes = Vec::new();
    let mut limited = std::io::Read::take(file, limit.saturating_add(1));
    std::io::Read::read_to_end(&mut limited, &mut bytes)
        .with_context(|| format!("cannot read {context}: {}", path.display()))?;
    let read_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if read_len > limit {
        anyhow::bail!(
            "{context} exceeds read limit: {} size={} limit={}",
            path.display(),
            read_len,
            limit
        );
    }
    Ok(bytes)
}

fn open_regular_source_file(path: &Path, context: &str) -> Result<fs::File> {
    ensure_regular_source_file(path, context)?;
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    options
        .open(path)
        .with_context(|| format!("cannot read {context}: {}", path.display()))
}

pub(crate) fn copy_regular_file(source: &Path, dest: &Path, context: &str) -> Result<()> {
    ensure_regular_source_file(source, context)?;
    ensure_regular_destination_file(dest, context)?;
    fs::copy(source, dest).with_context(|| {
        format!(
            "cannot copy {context}: {} to {}",
            source.display(),
            dest.display()
        )
    })?;
    Ok(())
}

pub(crate) fn write_regular_file(dest: &Path, bytes: &[u8], context: &str) -> Result<()> {
    ensure_regular_destination_file(dest, context)?;
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options
        .open(dest)
        .with_context(|| format!("cannot create {context}: {}", dest.display()))?;
    std::io::Write::write_all(&mut file, bytes)
        .with_context(|| format!("cannot write {context}: {}", dest.display()))
}

fn ensure_regular_source_file(path: &Path, context: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("cannot inspect {context}: {}", path.display()))?;
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        anyhow::bail!("{context} is not a regular file: {}", path.display());
    }
}

fn ensure_regular_destination_file(path: &Path, context: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => anyhow::bail!("{context} target is not a regular file: {}", path.display()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => {
            Err(err).with_context(|| format!("cannot inspect {context}: {}", path.display()))
        }
    }
}

pub(crate) fn path_exists_or_symlink(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err).with_context(|| format!("cannot inspect path: {}", path.display())),
    }
}

pub(crate) fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

pub(crate) fn decode_hex(hex: &str) -> Result<Vec<u8>> {
    if hex.len() % 2 != 0 {
        anyhow::bail!("hex payload has odd length");
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for idx in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[idx..idx + 2], 16)
            .with_context(|| format!("invalid hex payload at byte offset {idx}"))?;
        out.push(byte);
    }
    Ok(out)
}

pub(crate) fn checksum_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(bytes);
    hex_bytes(&digest)
}
