//! Durable file-data flush with a configurable sync method.
//!
//! The WAL, the data-page segment store, catalog snapshots, and runtime
//! metadata all share one durability barrier: [`durability_sync`]. Which
//! primitive it issues is process-global and set once at server startup via
//! [`set_wal_sync_method`], mirroring PostgreSQL's `wal_sync_method`:
//!
//! - [`WalSyncMethod::Fsync`] (default): `fsync(2)`, issued directly (NOT
//!   [`File::sync_all`], which on macOS upgrades itself to `F_FULLFSYNC`).
//!   This is the durability class PostgreSQL (`fsync = on` with its default
//!   `wal_sync_method`) and SQLite (`synchronous=FULL` with its default
//!   `fullfsync` off) provide on every platform. On Linux `fsync` forces the
//!   device write cache through the block layer, so a completed flush
//!   survives power loss. On macOS `fsync` pushes data to the drive but does
//!   NOT force the drive to flush its own cache — the same posture as
//!   PostgreSQL's and SQLite's macOS defaults: commits survive an OS crash,
//!   but sudden power loss can lose or reorder whatever is still in the
//!   drive's volatile cache (unordered destage can persist a later barrier
//!   while an earlier cached write is lost — the exposure PostgreSQL
//!   documents for its own macOS default).
//! - [`WalSyncMethod::FsyncWritethrough`]: additionally forces the drive's
//!   write cache to stable media. On macOS this issues `fcntl(F_FULLFSYNC)`
//!   (what PostgreSQL calls `wal_sync_method = fsync_writethrough` and SQLite
//!   `PRAGMA fullfsync = ON`); on other platforms it is identical to
//!   [`WalSyncMethod::Fsync`].
//!
//! Directory fsyncs persist a name/rename (not drive-cache data) and keep
//! using [`File::sync_all`] regardless of the configured method.

use std::fs::File;
use std::sync::atomic::{AtomicU8, Ordering};

/// Which primitive [`durability_sync`] issues. Process-global; see the
/// module docs for the durability semantics of each value.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WalSyncMethod {
    /// `fsync(2)`, issued directly — PostgreSQL's and SQLite's default
    /// durability class on every platform.
    #[default]
    Fsync,
    /// `fsync` plus a forced drive-cache flush (`fcntl(F_FULLFSYNC)` on
    /// macOS; identical to [`Self::Fsync`] elsewhere). PostgreSQL calls this
    /// `fsync_writethrough`.
    FsyncWritethrough,
}

impl WalSyncMethod {
    /// Canonical configuration spelling (`fsync` / `fsync_writethrough`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fsync => "fsync",
            Self::FsyncWritethrough => "fsync_writethrough",
        }
    }
}

impl std::str::FromStr for WalSyncMethod {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "fsync" => Ok(Self::Fsync),
            "fsync_writethrough" => Ok(Self::FsyncWritethrough),
            other => Err(format!(
                "unknown wal_sync_method '{other}' (expected 'fsync' or 'fsync_writethrough')"
            )),
        }
    }
}

impl std::fmt::Display for WalSyncMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

const METHOD_FSYNC: u8 = 0;
const METHOD_FSYNC_WRITETHROUGH: u8 = 1;

static WAL_SYNC_METHOD: AtomicU8 = AtomicU8::new(METHOD_FSYNC);

/// Set the process-global sync method. Called once at server startup from
/// the `--wal-sync-method` flag before any storage or WAL file is opened;
/// changing it while flushes are in flight is safe (each flush reads the
/// current value) but only startup configuration is supported.
pub fn set_wal_sync_method(method: WalSyncMethod) {
    let raw = match method {
        WalSyncMethod::Fsync => METHOD_FSYNC,
        WalSyncMethod::FsyncWritethrough => METHOD_FSYNC_WRITETHROUGH,
    };
    WAL_SYNC_METHOD.store(raw, Ordering::Release);
}

/// The currently configured process-global sync method.
#[must_use]
pub fn wal_sync_method() -> WalSyncMethod {
    match WAL_SYNC_METHOD.load(Ordering::Acquire) {
        METHOD_FSYNC_WRITETHROUGH => WalSyncMethod::FsyncWritethrough,
        _ => WalSyncMethod::Fsync,
    }
}

/// Flush the file's contents and metadata with the configured
/// [`WalSyncMethod`] — the shared durability barrier for every FILE-DATA
/// flush (WAL, data segments, snapshots, runtime metadata).
///
/// With the default [`WalSyncMethod::Fsync`] this is a direct `fsync(2)`
/// everywhere. With [`WalSyncMethod::FsyncWritethrough`] on macOS it issues
/// `fcntl(F_FULLFSYNC)` to force the drive's own write cache to stable
/// media; if the filesystem/device cannot honor that (some network
/// filesystems return `ENOTSUP`/`EOPNOTSUPP`/`EINVAL`), it falls back to
/// `sync_all`. Any other error is propagated so a genuine I/O failure is
/// never downgraded to a weaker flush.
pub fn durability_sync(file: &File) -> std::io::Result<()> {
    match wal_sync_method() {
        WalSyncMethod::Fsync => plain_fsync(file),
        WalSyncMethod::FsyncWritethrough => full_fsync_writethrough(file),
    }
}

/// `fsync(2)`, exactly — what PostgreSQL's `wal_sync_method = fsync` issues.
///
/// NOT [`File::sync_all`]: on macOS Rust's `sync_all` itself upgrades to
/// `fcntl(F_FULLFSYNC)` (~60x slower on Apple SSDs), which would silently
/// turn the `fsync` method back into `fsync_writethrough`.
fn plain_fsync(file: &File) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;

        // SAFETY: `fsync` takes only the descriptor; `as_raw_fd()` stays valid
        // for the duration of the call because `file` is borrowed throughout.
        if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
    #[cfg(not(unix))]
    {
        file.sync_all()
    }
}

/// The strongest flush available: force file data *and* the drive's write
/// cache to stable media, regardless of the configured [`WalSyncMethod`].
fn full_fsync_writethrough(file: &File) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;

        // SAFETY: `F_FULLFSYNC` is a non-variadic `fcntl` command that takes no
        // pointer argument; `as_raw_fd()` yields a descriptor that stays valid
        // for the duration of the call because `file` is borrowed throughout.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } != -1 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        match err.raw_os_error() {
            // Device/filesystem without F_FULLFSYNC support: `fsync` is the
            // strongest durability primitive available here.
            Some(libc::ENOTSUP) | Some(libc::EOPNOTSUPP) | Some(libc::EINVAL) => file.sync_all(),
            _ => Err(err),
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        file.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn method_parses_canonical_spellings_and_rejects_unknown() {
        assert_eq!("fsync".parse::<WalSyncMethod>(), Ok(WalSyncMethod::Fsync));
        assert_eq!(
            "fsync_writethrough".parse::<WalSyncMethod>(),
            Ok(WalSyncMethod::FsyncWritethrough)
        );
        assert_eq!(WalSyncMethod::Fsync.as_str(), "fsync");
        assert_eq!(
            WalSyncMethod::FsyncWritethrough.as_str(),
            "fsync_writethrough"
        );
        assert!("open_datasync".parse::<WalSyncMethod>().is_err());
        assert!("".parse::<WalSyncMethod>().is_err());
    }

    #[test]
    fn default_method_is_fsync() {
        assert_eq!(WalSyncMethod::default(), WalSyncMethod::Fsync);
    }

    #[test]
    fn durability_sync_flushes_under_both_methods() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut file = File::create(dir.path().join("sync_probe")).expect("create");
        file.write_all(b"durability probe").expect("write");

        let before = wal_sync_method();
        set_wal_sync_method(WalSyncMethod::Fsync);
        durability_sync(&file).expect("fsync method flushes");
        set_wal_sync_method(WalSyncMethod::FsyncWritethrough);
        durability_sync(&file).expect("fsync_writethrough method flushes");
        set_wal_sync_method(before);
    }
}
