//! WAL archive and restore orchestration for `ultrasqld`.
//!
//! Drives the background archiver loop and the startup restore probe:
//! renders the operator-supplied `archive_command` / `restore_command`
//! templates, runs them under a kill-on-timeout shell wrapper, and
//! tracks per-segment `.done` / `.failed` / `.missing` status markers.
//! All filesystem entry points reject symlinks and shell-unsafe WAL
//! names so a hostile data directory cannot escape the WAL tree.

// Panic hardening: production (non-test) server-binary code must not
// `.unwrap()`, `.expect()`, or `panic!`. Fallible sites propagate errors;
// proven invariants carry a per-site `#[allow]` with an `// INVARIANT:`
// justification.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info};

pub(crate) const DEFAULT_WAL_COMMAND_TIMEOUT: Duration = Duration::from_secs(60);
const WAL_COMMAND_TIMEOUT_POLL_INTERVAL: Duration = Duration::from_millis(10);

pub(crate) async fn run_wal_archiver_loop(
    data_dir: PathBuf,
    archive_command: String,
    interval_ms: u64,
    timeout: Option<Duration>,
) {
    let interval_ms = interval_ms.max(1);
    let data_dir = Arc::new(data_dir);
    let archive_command = Arc::new(archive_command);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
    loop {
        ticker.tick().await;
        // `archive_wal_once_with_timeout` blocks the thread: it walks the
        // WAL directory, polls a subprocess with `std::thread::sleep`, and
        // waits on synchronous filesystem IO. Run it off the async reactor
        // (mirroring the checkpoint task) so a slow/hung archive command
        // never starves connection handling for up to the command timeout.
        let data_dir = Arc::clone(&data_dir);
        let archive_command = Arc::clone(&archive_command);
        let result = tokio::task::spawn_blocking(move || {
            archive_wal_once_with_timeout(&data_dir, &archive_command, timeout)
        })
        .await;
        match result {
            Ok(Ok(archived)) if archived > 0 => {
                info!(target: "ultrasqld", archived, "WAL archiver completed batch");
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                error!(target: "ultrasqld", error = %e, "WAL archiver failed");
            }
            Err(e) => {
                error!(target: "ultrasqld", error = %e, "WAL archiver task panicked");
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn archive_wal_once(data_dir: &Path, archive_command: &str) -> Result<usize, String> {
    archive_wal_once_with_timeout(data_dir, archive_command, Some(DEFAULT_WAL_COMMAND_TIMEOUT))
}

pub(crate) fn archive_wal_once_with_timeout(
    data_dir: &Path,
    archive_command: &str,
    timeout: Option<Duration>,
) -> Result<usize, String> {
    let wal_dir = data_dir.join("pg_wal");
    let status_dir = wal_dir.join("archive_status");
    ensure_directory(&wal_dir, "WAL directory")?;
    ensure_directory(&status_dir, "archive status directory")?;

    let mut files = Vec::new();
    for entry in fs::read_dir(&wal_dir).map_err(|e| format!("read {}: {e}", wal_dir.display()))? {
        let entry = entry.map_err(|e| format!("read {} entry: {e}", wal_dir.display()))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if matches!(name, "archive_status" | "restore_status") {
            continue;
        }
        let file_type = entry
            .file_type()
            .map_err(|e| format!("inspect {}: {e}", path.display()))?;
        if file_type.is_file() {
            if !is_safe_wal_archive_filename(name) {
                return Err(format!("unsafe WAL filename: {name}"));
            }
            files.push(path);
        } else if is_safe_wal_archive_filename(name) {
            return Err(format!("not a regular WAL file: {name}"));
        }
    }
    files.sort();

    // Conservative cut: skip newest segment candidate, because it is likely the
    // currently-open WAL file. It will be archived after a later segment appears.
    if !files.is_empty() {
        files.pop();
    }

    let mut archived = 0_usize;
    for path in files {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let done = status_dir.join(format!("{name}.done"));
        if status_marker_exists(&done)? {
            continue;
        }
        let rendered = render_archive_command(archive_command, &path, name);
        match run_archive_shell_command(&rendered, timeout) {
            Ok(status) if status.success() => {}
            Ok(status) => {
                let failed = status_dir.join(format!("{name}.failed"));
                write_status_marker(&failed, rendered.as_bytes())?;
                return Err(format!(
                    "archive command failed for {name} with status {status}"
                ));
            }
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => {
                let failed = status_dir.join(format!("{name}.failed"));
                write_status_marker(&failed, rendered.as_bytes())?;
                return Err(format!(
                    "archive command timed out for {name} after {} ms",
                    timeout_label_ms(timeout)
                ));
            }
            Err(err) => return Err(format!("archive command spawn failed for {name}: {err}")),
        }
        write_status_marker(&done, rendered.as_bytes())?;
        archived = archived.saturating_add(1);
    }
    Ok(archived)
}

#[cfg(test)]
pub(crate) fn restore_wal_once(
    data_dir: &Path,
    restore_command: &str,
    max_segments: u32,
) -> Result<usize, String> {
    restore_wal_once_with_timeout(
        data_dir,
        restore_command,
        max_segments,
        Some(DEFAULT_WAL_COMMAND_TIMEOUT),
    )
}

pub(crate) fn restore_wal_once_with_timeout(
    data_dir: &Path,
    restore_command: &str,
    max_segments: u32,
    timeout: Option<Duration>,
) -> Result<usize, String> {
    if restore_command.trim().is_empty() || max_segments == 0 {
        return Ok(0);
    }

    let wal_dir = data_dir.join("pg_wal");
    let status_dir = wal_dir.join("restore_status");
    ensure_directory(&wal_dir, "WAL directory")?;
    ensure_directory(&status_dir, "restore status directory")?;

    let mut restored = 0_usize;
    for index in 0..max_segments {
        let name = wal_segment_filename(index);
        let path = wal_dir.join(&name);
        if wal_file_exists(&path, &name)? {
            continue;
        }

        let rendered = render_restore_command(restore_command, &path, &name);
        match run_restore_shell_command(&rendered, timeout) {
            Ok(status) if status.success() => {}
            Ok(_) => {
                let missing = status_dir.join(format!("{name}.missing"));
                write_status_marker(&missing, rendered.as_bytes())?;
                break;
            }
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => {
                let failed = status_dir.join(format!("{name}.failed"));
                write_status_marker(&failed, rendered.as_bytes())?;
                return Err(format!(
                    "restore command timed out for {name} after {} ms",
                    timeout_label_ms(timeout)
                ));
            }
            Err(err) => return Err(format!("restore command spawn failed for {name}: {err}")),
        }
        if !wal_file_exists(&path, &name)? {
            let missing = status_dir.join(format!("{name}.missing"));
            write_status_marker(&missing, rendered.as_bytes())?;
            break;
        }

        let done = status_dir.join(format!("{name}.done"));
        write_status_marker(&done, rendered.as_bytes())?;
        restored = restored.saturating_add(1);
    }
    Ok(restored)
}

fn wal_file_exists(path: &Path, name: &str) -> Result<bool, String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(format!("not a regular WAL file: {name}")),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(format!("inspect {}: {err}", path.display())),
    }
}

fn ensure_directory(path: &Path, label: &str) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(format!("{label} is not a directory: {}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|err| format!("create {}: {err}", path.display()))?;
            match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
                Ok(_) => Err(format!("{label} is not a directory: {}", path.display())),
                Err(err) => Err(format!("inspect {}: {err}", path.display())),
            }
        }
        Err(err) => Err(format!("inspect {}: {err}", path.display())),
    }
}

fn status_marker_exists(path: &Path) -> Result<bool, String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(format!(
            "status marker is not a regular file: {}",
            path.display()
        )),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(format!("inspect {}: {err}", path.display())),
    }
}

fn write_status_marker(path: &Path, bytes: &[u8]) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => {
            return Err(format!(
                "status marker is not a regular file: {}",
                path.display()
            ));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(format!("inspect {}: {err}", path.display())),
    }
    write_regular_status_marker(path, bytes).map_err(|e| format!("write {}: {e}", path.display()))
}

#[cfg(unix)]
fn write_regular_status_marker(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(bytes)?;
    file.flush()
}

#[cfg(not(unix))]
fn write_regular_status_marker(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    fs::write(path, bytes)
}

fn is_safe_wal_archive_filename(name: &str) -> bool {
    let ultrasql_segment = name
        .strip_prefix("segment_")
        .is_some_and(|suffix| suffix.len() == 10 && suffix.bytes().all(|b| b.is_ascii_digit()));
    let pg_segment = name.len() == 24 && name.bytes().all(|b| b.is_ascii_hexdigit());
    ultrasql_segment || pg_segment
}

pub(crate) fn wal_segment_filename(index: u32) -> String {
    format!("segment_{index:010}")
}

pub(crate) fn render_archive_command(template: &str, path: &Path, filename: &str) -> String {
    template
        .replace("%p", &path.to_string_lossy())
        .replace("%f", filename)
}

pub(crate) fn render_restore_command(template: &str, path: &Path, filename: &str) -> String {
    template
        .replace("%p", &path.to_string_lossy())
        .replace("%f", filename)
}

fn run_archive_shell_command(
    command: &str,
    timeout: Option<Duration>,
) -> std::io::Result<std::process::ExitStatus> {
    run_shell_command_with_timeout(command, timeout)
}

fn run_restore_shell_command(
    command: &str,
    timeout: Option<Duration>,
) -> std::io::Result<std::process::ExitStatus> {
    run_shell_command_with_timeout(command, timeout)
}

pub(crate) fn run_shell_command_with_timeout(
    command: &str,
    timeout: Option<Duration>,
) -> std::io::Result<std::process::ExitStatus> {
    let Some(timeout) = timeout else {
        return spawn_shell_command(command)?.wait();
    };
    let mut child = spawn_shell_command(command)?;
    let started = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if started.elapsed() >= timeout {
            terminate_shell_child(&mut child)?;
            let _status = child.wait()?;
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("shell command timed out after {} ms", timeout.as_millis()),
            ));
        }
        let remaining = timeout
            .checked_sub(started.elapsed())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            continue;
        }
        std::thread::sleep(remaining.min(WAL_COMMAND_TIMEOUT_POLL_INTERVAL));
    }
}

fn spawn_shell_command(command: &str) -> std::io::Result<std::process::Child> {
    #[cfg(windows)]
    {
        Command::new("cmd").args(["/C", command]).spawn()
    }
    #[cfg(not(windows))]
    {
        use std::os::unix::process::CommandExt;

        let mut shell = Command::new("sh");
        shell.args(["-c", command]);
        // SAFETY: The closure only calls async-signal-safe `setpgid` in the
        // child after fork and before exec. It does not touch shared Rust state.
        unsafe {
            shell.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
        shell.spawn()
    }
}

fn terminate_shell_child(child: &mut std::process::Child) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        if let Err(err) = child.kill()
            && err.kind() != std::io::ErrorKind::InvalidInput
        {
            return Err(err);
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let pid = libc::pid_t::try_from(child.id()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "child process id does not fit platform pid_t",
            )
        })?;
        // SAFETY: `spawn_shell_command` puts the shell in a new process group
        // whose pgid is the child pid. Negative pid targets that process group.
        if unsafe { libc::kill(-pid, libc::SIGKILL) } == -1 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::ESRCH) {
                return Err(err);
            }
        }
        Ok(())
    }
}

pub(crate) fn command_timeout(timeout_ms: u64) -> Option<Duration> {
    (timeout_ms > 0).then(|| Duration::from_millis(timeout_ms))
}

fn timeout_label_ms(timeout: Option<Duration>) -> u128 {
    timeout.unwrap_or(DEFAULT_WAL_COMMAND_TIMEOUT).as_millis()
}
