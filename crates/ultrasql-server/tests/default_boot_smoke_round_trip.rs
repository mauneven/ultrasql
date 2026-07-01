//! Boot smoke test: the SHIPPED default entry points must actually start the
//! real `ultrasqld` binary and accept a client connection.
//!
//! The ~140 in-process `*_round_trip.rs` tests drive an `ultrasql_server::Server`
//! directly and never exec the compiled binary, so clap parsing, `Cli::parse`,
//! `require_auth_or_refuse`, and the CLI->config translation are otherwise
//! untested end-to-end — the exact regression class that once shipped a
//! `docker run` that failed to boot. These tests close that gap by booting the
//! real binary (Test 1: bare loopback default; Test 2: the real container
//! entrypoint's env->flags + SCRAM handshake).

use std::process::{Child, Command};
use std::time::{Duration, Instant};

use tokio_postgres::NoTls;

/// Reserve a free loopback TCP port, then release it for the child to bind.
fn free_loopback_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    port
}

/// Kill (and reap) the child on scope exit so a failed assertion never leaks a
/// running server. When the entrypoint `exec`s `ultrasqld`, the child pid IS the
/// server, so killing the handle stops it.
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Poll until `127.0.0.1:port` accepts a TCP connection. Fails fast (rather than
/// hanging the CI job) if the child exits first or the deadline passes.
fn wait_for_tcp(child: &mut Child, port: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("try_wait on ultrasqld") {
            panic!("ultrasqld exited before accepting connections (status: {status})");
        }
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!(
                "ultrasqld did not accept a TCP connection on 127.0.0.1:{port} within {timeout:?}"
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Test 1 — the real binary with the loopback default (the bare `ultrasqld` /
/// systemd path): no `--data-dir` (in-memory sample DB) and no `--auth`
/// (trust, which `require_auth_or_refuse` permits on loopback). Proves clap
/// parsing + boot + a client handshake + serving the sample DB all line up.
#[tokio::test]
async fn shipped_binary_default_loopback_boots_and_serves() {
    let port = free_loopback_port();
    let child = Command::new(env!("CARGO_BIN_EXE_ultrasqld"))
        .args([
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--log-level",
            "warn",
        ])
        .spawn()
        .expect("spawn ultrasqld");
    let mut guard = ChildGuard(child);

    wait_for_tcp(&mut guard.0, port, Duration::from_secs(30));

    let conn_str = format!("host=127.0.0.1 port={port} user=ultrasql dbname=ultrasql");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("client connects to the default-booted server");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    let rows = client
        .query("SELECT id, name FROM users ORDER BY id", &[])
        .await
        .expect("query the sample users table");
    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(1)).collect();
    assert_eq!(rows.len(), 3, "sample DB has 3 users, got {names:?}");
    assert!(
        names.iter().any(|n| n == "Ada"),
        "sample users served over the wire: {names:?}"
    );

    drop(client);
    let _ = conn_handle.await;
    // guard drops here -> kills the server
}

/// Test 2 — the REAL container entrypoint's default env->flags path plus SCRAM.
/// This is the single test that proves the entrypoint flag injection, real clap
/// parsing, SCRAM-SHA-256 verifier derivation, and a client handshake all agree.
/// POSIX-only (the entrypoint is `sh`); the `test` CI matrix includes Windows,
/// where this is skipped.
#[cfg(unix)]
#[tokio::test]
async fn shipped_entrypoint_default_scram_boots_and_serves() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::TempDir::new().expect("temp dir");
    // The entrypoint `exec`s `ultrasqld` by BARE NAME, so place the real binary
    // as literally `ultrasqld` on a controlled PATH.
    let bin = tmp.path().join("ultrasqld");
    std::fs::copy(env!("CARGO_BIN_EXE_ultrasqld"), &bin).expect("copy ultrasqld");
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).expect("chmod bin");

    // The entrypoint injects `--data-dir` but does not create it; a real image
    // ships /var/lib/ultrasql at 0700 and `Server::init` requires 0700.
    let data_dir = tmp.path().join("data");
    std::fs::create_dir(&data_dir).expect("mkdir data dir");
    std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o700))
        .expect("chmod data dir 0700");

    let entrypoint = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packaging/docker/docker-entrypoint.sh");
    assert!(
        entrypoint.exists(),
        "entrypoint at {}",
        entrypoint.display()
    );

    let port = free_loopback_port();
    let path_env = format!(
        "{}:{}",
        tmp.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let child = Command::new("sh")
        .arg(&entrypoint)
        .env("PATH", path_env)
        .env("ULTRASQL_PASSWORD", "beta-smoke-pw-123") // >=12 bytes, no whitespace
        .env("ULTRASQL_LISTEN", format!("127.0.0.1:{port}"))
        .env("ULTRASQL_DATA_DIR", &data_dir)
        .env("ULTRASQL_OPS_LISTEN", "127.0.0.1:0") // avoid the 9100 default colliding in CI
        .spawn()
        .expect("spawn docker-entrypoint.sh");
    let mut guard = ChildGuard(child);

    wait_for_tcp(&mut guard.0, port, Duration::from_secs(30));

    // NoTls performs SCRAM-SHA-256 automatically against the CLI-derived verifier.
    let conn_str = format!(
        "host=127.0.0.1 port={port} user=ultrasql password=beta-smoke-pw-123 dbname=ultrasql"
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("SCRAM client connects via the entrypoint-configured default");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    // Data-dir mode boots WAL-backed storage (not the in-memory sample DB), so
    // assert a trivial query rather than the sample `users` table.
    let rows = client.query("SELECT 1", &[]).await.expect("SELECT 1");
    assert_eq!(rows.len(), 1);
    let one: i32 = rows[0].get(0);
    assert_eq!(one, 1);

    drop(client);
    let _ = conn_handle.await;
    // guard drops here -> kills the server
}
