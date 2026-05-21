//! Data-directory catalog-version guard.
//!
//! UltraSQL v1.0 data directories carry a small `catalog.version` marker at
//! the root. Startup accepts the current version, initializes missing markers
//! for pre-v1 development directories, and refuses markers written by a newer
//! binary so an older server cannot silently corrupt a newer catalog layout.

use std::path::Path;

use crate::ServerError;

/// Current on-disk catalog layout version for v1.0.
pub const CURRENT_CATALOG_VERSION: u32 = 1;

/// Root-relative marker filename in a WAL-backed data directory.
pub const CATALOG_VERSION_FILE: &str = "catalog.version";

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
    match std::fs::read_to_string(&path) {
        Ok(raw) => {
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
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            std::fs::write(&path, format!("{CURRENT_CATALOG_VERSION}\n"))
                .map_err(ServerError::Io)?;
            Ok(CatalogVersionStatus {
                observed_version: CURRENT_CATALOG_VERSION,
                created: true,
            })
        }
        Err(err) => Err(ServerError::Io(err)),
    }
}
