//! Unit tests for the `ultrasql` client binary, grouped by feature area.
//!
//! Shared fixtures live here; topic-specific assertions live in the child
//! modules.

use std::fs;
use std::path::{Path, PathBuf};

use super::cli_args::DumpFormat;

mod backup;
mod conn;
mod format;
mod ops;
mod waldump;

/// A fully-defaulted `Cli` used by tests that only need a few fields set.
pub(super) fn test_cli() -> super::cli_args::Cli {
    super::cli_args::Cli {
        host: None,
        port: None,
        dbname: None,
        username: None,
        password: None,
        url: None,
        command: None,
        file: None,
        isready: false,
        ops_endpoint: None,
        waldump: None,
        ctl: None,
        basebackup: None,
        pg_dump: None,
        dump_format: DumpFormat::Custom,
        pg_restore: None,
        archive_wal: None,
        restore_wal: None,
        wal_send_once: None,
        wal_send_interval_ms: 0,
        wal_receive_once: None,
        wal_receive_interval_ms: 0,
        wal_receive_cascade_archive: None,
        replication_slot: "standby".to_owned(),
        archive_dir: PathBuf::from("archive"),
        restore_output: None,
        recovery_target_time: None,
        recovery_target_lsn: None,
        recovery_target_xid: None,
        data_dir: PathBuf::from("data"),
        subcommand: None,
        positional_url: None,
    }
}

pub(super) fn write_pgpass(path: &Path, content: &str) {
    fs::write(path, content).expect("write pgpass");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod pgpass");
    }
}

pub(super) fn cli_env_test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().expect("cli env test lock")
}

pub(super) async fn spawn_one_shot_http(response: &'static str) -> std::net::SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test http listener");
    let addr = listener.local_addr().expect("listener addr");
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept test HTTP");
        let mut request = [0_u8; 512];
        let _ = socket.read(&mut request).await;
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write test HTTP response");
    });
    addr
}

pub(super) async fn spawn_recording_http(
    responses: Vec<String>,
) -> (
    std::net::SocketAddr,
    tokio::sync::mpsc::UnboundedReceiver<String>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test http listener");
    let addr = listener.local_addr().expect("listener addr");
    let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        for response in responses {
            let (mut socket, _) = listener.accept().await.expect("accept test HTTP");
            let mut request = Vec::new();
            let mut buf = [0_u8; 512];
            loop {
                let read = socket.read(&mut buf).await.expect("read request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            request_tx
                .send(String::from_utf8_lossy(&request).into_owned())
                .expect("record request");
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write test HTTP response");
        }
    });
    (addr, request_rx)
}
