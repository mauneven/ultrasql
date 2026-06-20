//! Tests for WAL archive/restore orchestration ([`crate::wal_archive`]).

use std::path::Path;
use std::time::Duration;

use crate::wal_archive::{
    archive_wal_once, archive_wal_once_with_timeout, render_archive_command,
    render_restore_command, restore_wal_once, restore_wal_once_with_timeout,
    run_shell_command_with_timeout, wal_segment_filename,
};

#[test]
fn archive_command_renderer_expands_path_and_filename() {
    let rendered = render_archive_command(
        "copy %p archive/%f",
        Path::new("/data/pg_wal/000000010000000000000001"),
        "000000010000000000000001",
    );

    assert_eq!(
        rendered,
        "copy /data/pg_wal/000000010000000000000001 archive/000000010000000000000001"
    );
}

#[test]
fn archive_wal_once_marks_completed_files_done() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let wal_dir = dir.path().join("pg_wal");
    std::fs::create_dir_all(&wal_dir).expect("pg_wal");
    std::fs::write(wal_dir.join("000000010000000000000001"), b"wal-a").expect("wal a");
    std::fs::write(wal_dir.join("000000010000000000000002"), b"wal-b").expect("wal b");

    let command = successful_archive_command();
    assert_eq!(
        archive_wal_once(dir.path(), command).expect("first archive"),
        1
    );
    assert!(
        wal_dir
            .join("archive_status/000000010000000000000001.done")
            .exists()
    );
    assert_eq!(
        archive_wal_once(dir.path(), command).expect("second archive"),
        0
    );

    std::fs::write(wal_dir.join("000000010000000000000003"), b"wal-c").expect("wal c");
    assert_eq!(
        archive_wal_once(dir.path(), command).expect("third archive"),
        1
    );
    assert!(
        wal_dir
            .join("archive_status/000000010000000000000002.done")
            .exists()
    );
}

#[test]
fn archive_wal_once_reports_missing_dir_and_failed_status() {
    let bad_dir = tempfile::TempDir::new().expect("bad temp dir");
    std::fs::write(bad_dir.path().join("pg_wal"), b"not a directory").expect("pg_wal file");
    let err = archive_wal_once(bad_dir.path(), successful_archive_command())
        .expect_err("pg_wal file should fail");
    assert!(err.contains("WAL directory is not a directory"));

    let dir = tempfile::TempDir::new().expect("temp dir");
    let wal_dir = dir.path().join("pg_wal");
    std::fs::create_dir_all(&wal_dir).expect("pg_wal");
    std::fs::write(wal_dir.join("000000010000000000000001"), b"wal-a").expect("wal a");
    std::fs::write(wal_dir.join("000000010000000000000002"), b"wal-b").expect("wal b");

    let err =
        archive_wal_once(dir.path(), failing_shell_command()).expect_err("failed archive command");
    assert!(err.contains("archive command failed"));
    assert!(
        wal_dir
            .join("archive_status/000000010000000000000001.failed")
            .exists()
    );
}

#[cfg(not(windows))]
#[test]
fn archive_wal_once_rejects_shell_unsafe_filenames() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let wal_dir = dir.path().join("pg_wal");
    std::fs::create_dir_all(&wal_dir).expect("pg_wal");
    std::fs::write(wal_dir.join("segment_0000000000;touch wal_pwned"), b"wal-a")
        .expect("malicious wal");
    std::fs::write(wal_dir.join("segment_0000000001"), b"wal-b").expect("newest wal");

    let command = format!("cd {} && true %p", sh_single_quoted_path(dir.path()));
    let err = archive_wal_once(dir.path(), &command).expect_err("unsafe WAL name");

    assert!(err.contains("unsafe WAL filename"));
    assert!(!dir.path().join("wal_pwned").exists());
}

#[cfg(unix)]
#[test]
fn archive_wal_once_rejects_symlinked_wal_files() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::TempDir::new().expect("temp dir");
    let wal_dir = dir.path().join("pg_wal");
    std::fs::create_dir_all(&wal_dir).expect("pg_wal");
    let outside = dir.path().join("outside");
    std::fs::write(&outside, b"secret").expect("outside");
    symlink(&outside, wal_dir.join("000000010000000000000001")).expect("wal symlink");
    std::fs::write(wal_dir.join("000000010000000000000002"), b"newest").expect("newest wal");

    let err = archive_wal_once(dir.path(), successful_archive_command())
        .expect_err("symlinked WAL rejected");

    assert!(err.contains("not a regular WAL file"));
    assert!(
        !wal_dir
            .join("archive_status/000000010000000000000001.done")
            .exists()
    );
}

#[cfg(unix)]
#[test]
fn archive_wal_once_rejects_symlinked_wal_directory() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::TempDir::new().expect("temp dir");
    let outside = tempfile::TempDir::new().expect("outside dir");
    std::fs::write(outside.path().join("000000010000000000000001"), b"wal-a").expect("wal a");
    std::fs::write(outside.path().join("000000010000000000000002"), b"wal-b").expect("wal b");
    symlink(outside.path(), dir.path().join("pg_wal")).expect("pg_wal symlink");

    let err = archive_wal_once(dir.path(), successful_archive_command())
        .expect_err("symlinked WAL directory rejected");

    assert!(err.contains("WAL directory is not a directory"));
    assert!(
        !outside
            .path()
            .join("archive_status/000000010000000000000001.done")
            .exists()
    );
}

#[cfg(unix)]
#[test]
fn archive_wal_once_rejects_symlinked_status_directory() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::TempDir::new().expect("temp dir");
    let outside = tempfile::TempDir::new().expect("outside dir");
    let wal_dir = dir.path().join("pg_wal");
    std::fs::create_dir_all(&wal_dir).expect("pg_wal");
    std::fs::write(wal_dir.join("000000010000000000000001"), b"wal-a").expect("wal a");
    std::fs::write(wal_dir.join("000000010000000000000002"), b"wal-b").expect("wal b");
    symlink(outside.path(), wal_dir.join("archive_status")).expect("archive_status symlink");

    let err = archive_wal_once(dir.path(), successful_archive_command())
        .expect_err("symlinked archive status rejected");

    assert!(err.contains("archive status directory is not a directory"));
    assert!(
        !outside
            .path()
            .join("000000010000000000000001.done")
            .exists()
    );
}

#[cfg(unix)]
#[test]
fn archive_wal_once_rejects_symlinked_status_markers() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::TempDir::new().expect("temp dir");
    let wal_dir = dir.path().join("pg_wal");
    let status_dir = wal_dir.join("archive_status");
    std::fs::create_dir_all(&status_dir).expect("archive_status");
    std::fs::write(wal_dir.join("000000010000000000000001"), b"wal-a").expect("wal a");
    std::fs::write(wal_dir.join("000000010000000000000002"), b"wal-b").expect("wal b");
    let outside = dir.path().join("outside.done");
    symlink(&outside, status_dir.join("000000010000000000000001.done")).expect("done symlink");

    let err = archive_wal_once(dir.path(), successful_archive_command())
        .expect_err("symlinked status rejected");

    assert!(err.contains("status marker"));
    assert!(!outside.exists());
}

#[cfg(windows)]
fn successful_archive_command() -> &'static str {
    "exit /B 0"
}

#[cfg(not(windows))]
fn successful_archive_command() -> &'static str {
    "true"
}

#[cfg(windows)]
fn failing_shell_command() -> &'static str {
    "exit /B 7"
}

#[cfg(not(windows))]
fn failing_shell_command() -> &'static str {
    "exit 7"
}

#[test]
fn shell_command_timeout_stops_hung_commands() {
    let started = std::time::Instant::now();
    let err =
        run_shell_command_with_timeout(hanging_shell_command(), Some(Duration::from_millis(25)))
            .expect_err("hung shell command should time out");

    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "timeout should stop command promptly"
    );
}

#[cfg(not(windows))]
#[test]
fn shell_command_timeout_kills_spawned_children() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let pid_file = dir.path().join("sleep.pid");
    let command = format!(
        "sleep 5 & echo $! > {}; wait",
        sh_single_quoted_path(&pid_file)
    );

    let err = run_shell_command_with_timeout(&command, Some(Duration::from_millis(100)))
        .expect_err("spawned child should time out");

    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    let pid = std::fs::read_to_string(&pid_file)
        .expect("pid file")
        .trim()
        .parse::<libc::pid_t>()
        .expect("pid");
    let child_running = process_running_after_wait(pid, Duration::from_secs(1));
    if child_running {
        kill_process(pid);
    }
    assert!(!child_running, "timed-out shell child should be killed");
}

#[test]
fn archive_wal_once_marks_timed_out_command_failed() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let wal_dir = dir.path().join("pg_wal");
    std::fs::create_dir_all(&wal_dir).expect("pg_wal");
    std::fs::write(wal_dir.join("000000010000000000000001"), b"wal-a").expect("wal a");
    std::fs::write(wal_dir.join("000000010000000000000002"), b"wal-b").expect("wal b");

    let err = archive_wal_once_with_timeout(
        dir.path(),
        hanging_shell_command(),
        Some(Duration::from_millis(25)),
    )
    .expect_err("hung archive command should fail");

    assert!(err.contains("archive command timed out"));
    assert!(
        wal_dir
            .join("archive_status/000000010000000000000001.failed")
            .exists()
    );
}

#[test]
fn restore_wal_once_errors_on_timed_out_command() {
    let dir = tempfile::TempDir::new().expect("temp dir");

    let err = restore_wal_once_with_timeout(
        dir.path(),
        hanging_shell_command(),
        1,
        Some(Duration::from_millis(25)),
    )
    .expect_err("hung restore command should fail");

    assert!(err.contains("restore command timed out"));
    assert!(
        dir.path()
            .join("pg_wal/restore_status/segment_0000000000.failed")
            .exists()
    );
}

#[cfg(windows)]
fn hanging_shell_command() -> &'static str {
    "powershell -NoProfile -NonInteractive -Command Start-Sleep -Seconds 5"
}

#[cfg(not(windows))]
fn hanging_shell_command() -> &'static str {
    "sleep 5"
}

#[cfg(not(windows))]
fn sh_single_quoted_path(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

#[cfg(not(windows))]
fn process_running_after_wait(pid: libc::pid_t, timeout: Duration) -> bool {
    let started = std::time::Instant::now();
    loop {
        if !process_running(pid) {
            return false;
        }
        if started.elapsed() >= timeout {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(all(not(windows), target_os = "linux"))]
fn process_running(pid: libc::pid_t) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => !linux_proc_state_is_dead(&stat),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => process_exists(pid),
    }
}

#[cfg(all(not(windows), target_os = "linux"))]
fn linux_proc_state_is_dead(stat: &str) -> bool {
    stat.rsplit_once(") ")
        .and_then(|(_, rest)| rest.as_bytes().first().copied())
        .is_some_and(|state| matches!(state, b'Z' | b'X' | b'x'))
}

#[cfg(all(not(windows), not(target_os = "linux")))]
fn process_running(pid: libc::pid_t) -> bool {
    process_exists(pid)
}

#[cfg(not(windows))]
fn process_exists(pid: libc::pid_t) -> bool {
    // SAFETY: `kill(pid, 0)` does not send a signal; it probes whether the
    // process exists and whether this process can signal it.
    unsafe { libc::kill(pid, 0) == 0 }
}

#[cfg(not(windows))]
fn kill_process(pid: libc::pid_t) {
    // SAFETY: Best-effort test cleanup for a PID created by this test.
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
}

#[test]
fn restore_command_renderer_expands_destination_and_filename() {
    let rendered = render_restore_command(
        "copy archive/%f %p",
        Path::new("/data/pg_wal/segment_0000000007"),
        "segment_0000000007",
    );

    assert_eq!(
        rendered,
        "copy archive/segment_0000000007 /data/pg_wal/segment_0000000007"
    );
}

#[test]
fn restore_wal_once_restores_until_first_missing_segment() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let archive = tempfile::TempDir::new().expect("archive dir");
    let wal_dir = dir.path().join("pg_wal");
    std::fs::create_dir_all(&wal_dir).expect("pg_wal");
    std::fs::write(archive.path().join("segment_0000000000"), b"wal-a").expect("wal a");
    std::fs::write(archive.path().join("segment_0000000001"), b"wal-b").expect("wal b");

    let command = copy_restore_command(archive.path());
    assert_eq!(
        restore_wal_once(dir.path(), &command, 3).expect("restore wal"),
        2
    );
    assert_eq!(
        std::fs::read(wal_dir.join("segment_0000000000")).expect("restored 0"),
        b"wal-a"
    );
    assert_eq!(
        std::fs::read(wal_dir.join("segment_0000000001")).expect("restored 1"),
        b"wal-b"
    );
    assert!(
        wal_dir
            .join("restore_status/segment_0000000000.done")
            .exists()
    );
    assert!(
        wal_dir
            .join("restore_status/segment_0000000001.done")
            .exists()
    );
    assert!(
        wal_dir
            .join("restore_status/segment_0000000002.missing")
            .exists()
    );
}

#[test]
fn restore_wal_once_handles_disabled_existing_and_no_output_paths() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let wal_dir = dir.path().join("pg_wal");
    std::fs::create_dir_all(&wal_dir).expect("pg_wal");
    std::fs::write(wal_dir.join("segment_0000000000"), b"existing").expect("existing wal");

    assert_eq!(
        restore_wal_once(dir.path(), "", 2).expect("empty restore command"),
        0
    );
    assert_eq!(
        restore_wal_once(dir.path(), successful_archive_command(), 0).expect("disabled"),
        0
    );

    let restored = restore_wal_once(dir.path(), successful_archive_command(), 2)
        .expect("successful command without output stops as missing");
    assert_eq!(restored, 0);
    assert!(
        wal_dir
            .join("restore_status/segment_0000000001.missing")
            .exists()
    );
    assert_eq!(wal_segment_filename(7), "segment_0000000007");
}

#[cfg(unix)]
#[test]
fn restore_wal_once_rejects_symlinked_output_paths() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::TempDir::new().expect("temp dir");
    let archive = tempfile::TempDir::new().expect("archive dir");
    let wal_dir = dir.path().join("pg_wal");
    std::fs::create_dir_all(&wal_dir).expect("pg_wal");
    std::fs::write(archive.path().join("segment_0000000000"), b"wal-a").expect("wal a");
    let outside = dir.path().join("outside");
    symlink(&outside, wal_dir.join("segment_0000000000")).expect("wal output symlink");

    let err = restore_wal_once(dir.path(), &copy_restore_command(archive.path()), 1)
        .expect_err("symlinked output rejected");

    assert!(err.contains("not a regular WAL file"));
    assert!(!outside.exists());
}

#[cfg(unix)]
#[test]
fn restore_wal_once_rejects_symlinked_wal_directory() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::TempDir::new().expect("temp dir");
    let outside = tempfile::TempDir::new().expect("outside dir");
    symlink(outside.path(), dir.path().join("pg_wal")).expect("pg_wal symlink");

    let err = restore_wal_once(dir.path(), successful_archive_command(), 1)
        .expect_err("symlinked WAL directory rejected");

    assert!(err.contains("WAL directory is not a directory"));
    assert!(
        !outside
            .path()
            .join("restore_status/segment_0000000000.missing")
            .exists()
    );
}

#[cfg(unix)]
#[test]
fn restore_wal_once_rejects_symlinked_status_directory() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::TempDir::new().expect("temp dir");
    let outside = tempfile::TempDir::new().expect("outside dir");
    let wal_dir = dir.path().join("pg_wal");
    std::fs::create_dir_all(&wal_dir).expect("pg_wal");
    symlink(outside.path(), wal_dir.join("restore_status")).expect("restore_status symlink");

    let err = restore_wal_once(dir.path(), successful_archive_command(), 1)
        .expect_err("symlinked restore status rejected");

    assert!(err.contains("restore status directory is not a directory"));
    assert!(!outside.path().join("segment_0000000000.missing").exists());
}

#[cfg(windows)]
fn copy_restore_command(archive_dir: &Path) -> String {
    let source = powershell_single_quoted_path(&archive_dir.join("%f"));
    format!(
        "powershell -NoProfile -NonInteractive -Command Copy-Item -LiteralPath {source} -Destination '%p' -Force -ErrorAction Stop"
    )
}

#[cfg(windows)]
fn powershell_single_quoted_path(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "''"))
}

#[cfg(not(windows))]
fn copy_restore_command(archive_dir: &Path) -> String {
    format!("cp '{}/%f' '%p' 2>/dev/null", archive_dir.display())
}
