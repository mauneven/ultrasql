//! Data-directory catalog-version guard.
//!
//! UltraSQL v1.0 data directories carry a small `catalog.version` marker at
//! the root. Startup accepts the current version, initializes missing markers
//! for pre-v1 development directories, and refuses markers written by a newer
//! binary so an older server cannot silently corrupt a newer catalog layout.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use crate::ServerError;

/// Current on-disk catalog layout version for v1.0.
pub const CURRENT_CATALOG_VERSION: u32 = 1;

/// Root-relative marker filename in a WAL-backed data directory.
pub const CATALOG_VERSION_FILE: &str = "catalog.version";

const CATALOG_VERSION_MARKER_LIMIT_BYTES: u64 = 64;

/// Result of checking the data-directory catalog-version marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogVersionStatus {
    /// Version observed after the check. Missing markers are initialized to
    /// [`CURRENT_CATALOG_VERSION`].
    pub observed_version: u32,
    /// `true` when this call created the marker.
    pub created: bool,
}

/// Ensure `data_dir` can be opened by this binary's catalog layout.
///
/// Missing markers are initialized to v1 because pre-v1 development data
/// directories did not have a durable catalog-version file. Markers newer than
/// [`CURRENT_CATALOG_VERSION`] are refused; operators must start with a newer
/// UltraSQL binary or run an explicit offline migration documented in
/// `docs/catalog-upgrades.md`.
///
/// # Errors
///
/// Returns [`ServerError::Io`] for filesystem failures and
/// [`ServerError::Ddl`] for malformed or newer-than-supported markers.
pub fn ensure_catalog_version(data_dir: &Path) -> Result<CatalogVersionStatus, ServerError> {
    std::fs::create_dir_all(data_dir).map_err(ServerError::Io)?;
    let path = data_dir.join(CATALOG_VERSION_FILE);
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            let raw = read_catalog_version_marker(&path)?;
            let observed_version = raw.trim().parse::<u32>().map_err(|err| {
                ServerError::Ddl(format!(
                    "catalog version marker {} is not a u32: {err}",
                    path.display()
                ))
            })?;
            if observed_version > CURRENT_CATALOG_VERSION {
                return Err(ServerError::Ddl(format!(
                    "catalog version {observed_version} is newer than this UltraSQL binary supports ({CURRENT_CATALOG_VERSION}); start with a newer binary or run the documented offline catalog migration"
                )));
            }
            Ok(CatalogVersionStatus {
                observed_version,
                created: false,
            })
        }
        Ok(_) => Err(ServerError::Ddl(format!(
            "catalog version marker {} is not a regular file",
            path.display()
        ))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.custom_flags(libc::O_NOFOLLOW);
            let mut file = options.open(&path).map_err(ServerError::Io)?;
            file.write_all(format!("{CURRENT_CATALOG_VERSION}\n").as_bytes())
                .map_err(ServerError::Io)?;
            Ok(CatalogVersionStatus {
                observed_version: CURRENT_CATALOG_VERSION,
                created: true,
            })
        }
        Err(err) => Err(ServerError::Io(err)),
    }
}

fn read_catalog_version_marker(path: &Path) -> Result<String, ServerError> {
    let file = open_catalog_version_marker(path)?;
    let metadata = file.metadata().map_err(ServerError::Io)?;
    if !metadata.file_type().is_file() {
        return Err(ServerError::Ddl(format!(
            "catalog version marker {} is not a regular file",
            path.display()
        )));
    }
    if metadata.len() > CATALOG_VERSION_MARKER_LIMIT_BYTES {
        return Err(ServerError::Ddl(format!(
            "catalog version marker {} exceeds read limit: bytes={} limit={}",
            path.display(),
            metadata.len(),
            CATALOG_VERSION_MARKER_LIMIT_BYTES
        )));
    }

    let mut raw = String::new();
    let mut limited = file.take(catalog_marker_take_limit(
        CATALOG_VERSION_MARKER_LIMIT_BYTES,
    )?);
    limited.read_to_string(&mut raw).map_err(ServerError::Io)?;
    let bytes_read = catalog_marker_bytes_read_len(raw.len())?;
    if bytes_read > CATALOG_VERSION_MARKER_LIMIT_BYTES {
        return Err(ServerError::Ddl(format!(
            "catalog version marker {} exceeds read limit: bytes={} limit={}",
            path.display(),
            bytes_read,
            CATALOG_VERSION_MARKER_LIMIT_BYTES
        )));
    }
    Ok(raw)
}

fn catalog_marker_take_limit(limit: u64) -> Result<u64, ServerError> {
    limit.checked_add(1).ok_or_else(|| {
        ServerError::Ddl(format!(
            "catalog version marker read limit is too large: limit={limit}"
        ))
    })
}

fn catalog_marker_bytes_read_len(len: usize) -> Result<u64, ServerError> {
    u64::try_from(len).map_err(|_| {
        ServerError::Ddl(format!(
            "catalog version marker byte count exceeds u64: bytes={len}"
        ))
    })
}

#[cfg_attr(not(unix), allow(unused_variables))]
fn open_catalog_version_marker(path: &Path) -> Result<File, ServerError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    options.open(path).map_err(ServerError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_marker_take_limit_rejects_overflow() {
        let err = catalog_marker_take_limit(u64::MAX).unwrap_err();
        assert!(err.to_string().contains("read limit is too large"));
    }
}
