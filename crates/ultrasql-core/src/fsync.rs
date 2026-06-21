//! Durable file-data flush that truly reaches stable storage.
//!
//! The WAL, the data-page segment store, catalog snapshots, and runtime
//! metadata all share one durability requirement: once a flush returns, the
//! bytes must survive a power loss. On macOS, plain `fsync` / [`File::sync_all`]
//! does NOT satisfy that — it pushes data to the drive but does not force the
//! drive to flush its own write cache — so a committed record can still be lost
//! on power failure. [`full_fsync`] issues `fcntl(F_FULLFSYNC)` there (the
//! behaviour PostgreSQL and SQLite rely on for real durability on Apple
//! hardware) and only falls back to `sync_all` when the filesystem/device
//! cannot honor it.
//!
//! Use [`full_fsync`] for every FILE-DATA durability barrier. Directory
//! fsyncs persist a name/rename (not drive-cache data) and should keep using
//! [`File::sync_all`].

use std::fs::File;

/// Force the file's contents and metadata to truly stable storage.
///
/// On macOS this issues `fcntl(fd, F_FULLFSYNC)`, which — unlike `fsync` /
/// [`File::sync_all`] — forces the drive to flush its own write cache, so a
/// record reported durable cannot be silently lost on power failure. If the
/// filesystem/device cannot honor it (some network filesystems return
/// `ENOTSUP`/`EOPNOTSUPP`/`EINVAL`), fall back to `sync_all`; any other error is
/// propagated so a genuine I/O failure is never downgraded to a weaker flush.
/// On every other platform this is exactly `File::sync_all`.
pub fn full_fsync(file: &File) -> std::io::Result<()> {
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
