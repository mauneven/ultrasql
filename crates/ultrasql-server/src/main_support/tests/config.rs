//! Tests for CLI-to-config validation ([`crate::config`]).

use std::path::{Path, PathBuf};

use ultrasql_server::{LogStatementMode, Server};

use crate::cli::{Cli, CliLogStatementMode, LogFormat};
use crate::config::{
    apply_auth_config, apply_startup_signal_files, auth_config_from_cli,
    autovacuum_config_from_cli, listen_security_from_cli, logging_config_from_cli,
    ops_token_from_cli, require_auth_or_refuse, wal_sync_method_from_cli,
};

#[test]
fn wal_sync_method_from_cli_maps_both_methods_and_rejects_unknown() {
    use ultrasql_core::fsync::WalSyncMethod;

    let mut cli = cli_with_auth_password_file(PathBuf::from("/unused"));
    assert_eq!(
        wal_sync_method_from_cli(&cli),
        Ok(WalSyncMethod::Fsync),
        "default CLI value must map to the fsync method"
    );

    cli.wal_sync_method = "fsync_writethrough".to_owned();
    assert_eq!(
        wal_sync_method_from_cli(&cli),
        Ok(WalSyncMethod::FsyncWritethrough)
    );

    cli.wal_sync_method = "open_datasync".to_owned();
    assert!(
        wal_sync_method_from_cli(&cli).is_err(),
        "unsupported methods must be rejected, not silently downgraded"
    );
}

#[test]
fn autovacuum_config_from_cli_converts_scale_factors() {
    let cli = Cli {
        listen: "127.0.0.1:5433".parse().expect("listen addr"),
        allow_insecure_listen: false,
        data_dir: None,
        ops_listen: None,
        ops_token: None,
        auth_user: None,
        auth_password_file: None,
        auth_method: "scram".to_owned(),
        hba_file: None,
        tls_cert: None,
        tls_key: None,
        log_level: "info".to_owned(),
        log_format: LogFormat::Text,
        log_connections: false,
        log_min_duration_statement_ms: -1,
        log_statement: CliLogStatementMode::None,
        idle_session_timeout_ms: 0,
        shutdown_drain_timeout_ms: 5000,
        autovacuum_interval_ms: 1000,
        checkpoint_interval_ms: 0,
        wal_segment_size_bytes: 0,
        autovacuum_vacuum_threshold: 7,
        autovacuum_vacuum_scale_factor: 0.25,
        autovacuum_analyze_threshold: 11,
        autovacuum_analyze_scale_factor: 0.125,
        archive_command: None,
        restore_command: None,
        restore_max_segments: 0,
        archive_interval_ms: 1000,
        archive_command_timeout_ms: 60_000,
        restore_command_timeout_ms: 60_000,
        wal_sync_method: "fsync".to_owned(),
    };

    let config = autovacuum_config_from_cli(&cli).expect("valid autovacuum config");

    assert_eq!(config.vacuum_threshold, 7);
    assert_eq!(config.vacuum_scale_factor_ppm, 250_000);
    assert_eq!(config.analyze_threshold, 11);
    assert_eq!(config.analyze_scale_factor_ppm, 125_000);
}

#[test]
fn autovacuum_config_from_cli_rejects_invalid_scale_factor() {
    let cli = Cli {
        listen: "127.0.0.1:5433".parse().expect("listen addr"),
        allow_insecure_listen: false,
        data_dir: None,
        ops_listen: None,
        ops_token: None,
        auth_user: None,
        auth_password_file: None,
        auth_method: "scram".to_owned(),
        hba_file: None,
        tls_cert: None,
        tls_key: None,
        log_level: "info".to_owned(),
        log_format: LogFormat::Text,
        log_connections: false,
        log_min_duration_statement_ms: -1,
        log_statement: CliLogStatementMode::None,
        idle_session_timeout_ms: 0,
        shutdown_drain_timeout_ms: 5000,
        autovacuum_interval_ms: 1000,
        checkpoint_interval_ms: 0,
        wal_segment_size_bytes: 0,
        autovacuum_vacuum_threshold: 50,
        autovacuum_vacuum_scale_factor: f64::NAN,
        autovacuum_analyze_threshold: 50,
        autovacuum_analyze_scale_factor: 0.1,
        archive_command: None,
        restore_command: None,
        restore_max_segments: 0,
        archive_interval_ms: 1000,
        archive_command_timeout_ms: 60_000,
        restore_command_timeout_ms: 60_000,
        wal_sync_method: "fsync".to_owned(),
    };

    assert!(autovacuum_config_from_cli(&cli).is_err());
}

#[test]
fn logging_config_from_cli_rejects_invalid_duration() {
    let cli = Cli {
        listen: "127.0.0.1:5433".parse().expect("listen addr"),
        allow_insecure_listen: false,
        data_dir: None,
        ops_listen: None,
        ops_token: None,
        auth_user: None,
        auth_password_file: None,
        auth_method: "scram".to_owned(),
        hba_file: None,
        tls_cert: None,
        tls_key: None,
        log_level: "info".to_owned(),
        log_format: LogFormat::Text,
        log_connections: false,
        log_min_duration_statement_ms: -2,
        log_statement: CliLogStatementMode::Mod,
        idle_session_timeout_ms: 0,
        shutdown_drain_timeout_ms: 5000,
        autovacuum_interval_ms: 1000,
        checkpoint_interval_ms: 0,
        wal_segment_size_bytes: 0,
        autovacuum_vacuum_threshold: 50,
        autovacuum_vacuum_scale_factor: 0.2,
        autovacuum_analyze_threshold: 50,
        autovacuum_analyze_scale_factor: 0.1,
        archive_command: None,
        restore_command: None,
        restore_max_segments: 0,
        archive_interval_ms: 1000,
        archive_command_timeout_ms: 60_000,
        restore_command_timeout_ms: 60_000,
        wal_sync_method: "fsync".to_owned(),
    };

    assert!(logging_config_from_cli(&cli).is_err());
}

#[test]
fn logging_config_from_cli_accepts_duration_and_statement_mode() {
    let cli = Cli {
        listen: "127.0.0.1:5433".parse().expect("listen addr"),
        allow_insecure_listen: false,
        data_dir: None,
        ops_listen: None,
        ops_token: None,
        auth_user: None,
        auth_password_file: None,
        auth_method: "scram".to_owned(),
        hba_file: None,
        tls_cert: None,
        tls_key: None,
        log_level: "info".to_owned(),
        log_format: LogFormat::Json,
        log_connections: true,
        log_min_duration_statement_ms: 25,
        log_statement: CliLogStatementMode::All,
        idle_session_timeout_ms: 0,
        shutdown_drain_timeout_ms: 5000,
        autovacuum_interval_ms: 1000,
        checkpoint_interval_ms: 0,
        wal_segment_size_bytes: 0,
        autovacuum_vacuum_threshold: 50,
        autovacuum_vacuum_scale_factor: 0.2,
        autovacuum_analyze_threshold: 50,
        autovacuum_analyze_scale_factor: 0.1,
        archive_command: None,
        restore_command: None,
        restore_max_segments: 0,
        archive_interval_ms: 1000,
        archive_command_timeout_ms: 60_000,
        restore_command_timeout_ms: 60_000,
        wal_sync_method: "fsync".to_owned(),
    };

    let config = logging_config_from_cli(&cli).expect("valid logging config");

    assert!(config.log_connections);
    assert_eq!(config.log_min_duration_statement_ms, 25);
    assert_eq!(config.log_statement, LogStatementMode::All);
}

#[test]
fn listen_security_from_cli_rejects_wildcard_without_override() {
    let mut cli = Cli {
        listen: "0.0.0.0:5433".parse().expect("listen addr"),
        allow_insecure_listen: false,
        data_dir: None,
        ops_listen: None,
        ops_token: None,
        auth_user: None,
        auth_password_file: None,
        auth_method: "scram".to_owned(),
        hba_file: None,
        tls_cert: None,
        tls_key: None,
        log_level: "info".to_owned(),
        log_format: LogFormat::Text,
        log_connections: false,
        log_min_duration_statement_ms: -1,
        log_statement: CliLogStatementMode::None,
        idle_session_timeout_ms: 0,
        shutdown_drain_timeout_ms: 5000,
        autovacuum_interval_ms: 1000,
        checkpoint_interval_ms: 0,
        wal_segment_size_bytes: 0,
        autovacuum_vacuum_threshold: 50,
        autovacuum_vacuum_scale_factor: 0.2,
        autovacuum_analyze_threshold: 50,
        autovacuum_analyze_scale_factor: 0.1,
        archive_command: None,
        restore_command: None,
        restore_max_segments: 0,
        archive_interval_ms: 1000,
        archive_command_timeout_ms: 60_000,
        restore_command_timeout_ms: 60_000,
        wal_sync_method: "fsync".to_owned(),
    };

    let err = listen_security_from_cli(&cli).expect_err("wildcard trust must be rejected");
    assert!(
        err.contains("non-loopback"),
        "expected non-loopback rejection, got {err}"
    );

    cli.listen = "127.0.0.1:5433".parse().expect("loopback listen");
    assert!(listen_security_from_cli(&cli).is_ok());

    cli.listen = "0.0.0.0:5433".parse().expect("wildcard listen");
    cli.allow_insecure_listen = true;
    assert!(listen_security_from_cli(&cli).is_ok());
}

#[test]
fn require_auth_or_refuse_rejects_public_bind_without_auth_or_flag() {
    let public: std::net::SocketAddr = "0.0.0.0:5433".parse().expect("addr");
    let err = require_auth_or_refuse(&public, false, false)
        .expect_err("public + no auth + no flag must refuse to start");
    assert!(
        err.contains("non-loopback") && err.contains("--insecure-no-auth"),
        "error must name the cause and the opt-in flag, got {err}"
    );
}

#[test]
fn require_auth_or_refuse_allows_public_bind_with_insecure_flag() {
    let public: std::net::SocketAddr = "0.0.0.0:5433".parse().expect("addr");
    assert!(
        require_auth_or_refuse(&public, false, true).is_ok(),
        "explicit --insecure-no-auth must permit trust on a public bind"
    );
}

#[test]
fn require_auth_or_refuse_allows_public_bind_with_explicit_auth() {
    let public: std::net::SocketAddr = "0.0.0.0:5433".parse().expect("addr");
    assert!(
        require_auth_or_refuse(&public, true, false).is_ok(),
        "explicit auth must permit a public bind without the insecure flag"
    );
}

#[test]
fn require_auth_or_refuse_allows_loopback_without_auth() {
    for addr in ["127.0.0.1:5433", "127.5.6.7:5433", "[::1]:5433"] {
        let bind: std::net::SocketAddr = addr.parse().expect("addr");
        assert!(
            require_auth_or_refuse(&bind, false, false).is_ok(),
            "loopback bind {addr} (no auth, no flag) must be allowed"
        );
    }
}

#[test]
fn require_auth_or_refuse_treats_wildcard_as_public() {
    // 0.0.0.0 / [::] are not loopback: they must require auth or the flag.
    for addr in ["0.0.0.0:5433", "[::]:5433"] {
        let bind: std::net::SocketAddr = addr.parse().expect("addr");
        assert!(
            require_auth_or_refuse(&bind, false, false).is_err(),
            "wildcard bind {addr} must be treated as public"
        );
    }
}

#[test]
fn require_auth_or_refuse_treats_routable_address_as_public() {
    let bind: std::net::SocketAddr = "203.0.113.5:5433".parse().expect("addr");
    assert!(
        require_auth_or_refuse(&bind, false, false).is_err(),
        "a routable public IP must require auth or the insecure flag"
    );
}

#[test]
fn listen_security_from_cli_allows_public_bind_with_hba_file() {
    // A configured pg_hba.conf-style rules file is explicit auth
    // configuration: the public-bind guard must accept it without the
    // insecure flag.
    let mut cli = wildcard_trust_cli();
    cli.hba_file = Some(PathBuf::from("/etc/ultrasql/pg_hba.conf"));
    assert!(
        listen_security_from_cli(&cli).is_ok(),
        "explicit --hba-file auth must satisfy the public-bind guard"
    );
}

#[test]
fn md5_auth_from_cli_reads_password_file_and_secures_wildcard_listener() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let password_file = dir.path().join("password");
    write_private_password_file(&password_file, "very-secret-password\n");
    let cli = Cli {
        listen: "0.0.0.0:5433".parse().expect("listen addr"),
        allow_insecure_listen: false,
        data_dir: None,
        ops_listen: None,
        ops_token: None,
        auth_user: Some("alice".to_owned()),
        auth_password_file: Some(password_file),
        auth_method: "scram".to_owned(),
        hba_file: None,
        tls_cert: None,
        tls_key: None,
        log_level: "info".to_owned(),
        log_format: LogFormat::Text,
        log_connections: false,
        log_min_duration_statement_ms: -1,
        log_statement: CliLogStatementMode::None,
        idle_session_timeout_ms: 0,
        shutdown_drain_timeout_ms: 5000,
        autovacuum_interval_ms: 1000,
        checkpoint_interval_ms: 0,
        wal_segment_size_bytes: 0,
        autovacuum_vacuum_threshold: 50,
        autovacuum_vacuum_scale_factor: 0.2,
        autovacuum_analyze_threshold: 50,
        autovacuum_analyze_scale_factor: 0.1,
        archive_command: None,
        restore_command: None,
        restore_max_segments: 0,
        archive_interval_ms: 1000,
        archive_command_timeout_ms: 60_000,
        restore_command_timeout_ms: 60_000,
        wal_sync_method: "fsync".to_owned(),
    };

    let auth = auth_config_from_cli(&cli).expect("password file auth config");

    // The default `--auth-method` is SCRAM-SHA-256.
    match auth {
        Some(ultrasql_server::AuthConfig::Scram { username, .. }) => {
            assert_eq!(username, "alice");
        }
        other => panic!("expected SCRAM auth, got {other:?}"),
    }
    assert!(listen_security_from_cli(&cli).is_ok());
}

#[test]
fn auth_config_from_cli_md5_method_selects_md5() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let password_file = dir.path().join("password");
    write_private_password_file(&password_file, "very-secret-password\n");
    let mut cli = cli_with_auth_password_file(password_file);
    cli.auth_user = Some("alice".to_owned());
    cli.auth_method = "md5".to_owned();

    match auth_config_from_cli(&cli).expect("md5 auth config") {
        Some(ultrasql_server::AuthConfig::Md5 { username, password }) => {
            assert_eq!(username, "alice");
            assert_eq!(password, "very-secret-password");
        }
        other => panic!("expected MD5 auth, got {other:?}"),
    }
}

#[test]
fn auth_config_from_cli_hba_file_selects_hba() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let hba_path = dir.path().join("pg_hba.conf");
    std::fs::write(&hba_path, "host all all 127.0.0.1/32 scram-sha-256\n").expect("write hba");
    let mut cli = cli_with_auth_password_file(dir.path().join("password"));
    cli.auth_user = None;
    cli.auth_password_file = None;
    cli.hba_file = Some(hba_path);

    match auth_config_from_cli(&cli).expect("hba auth config") {
        Some(ultrasql_server::AuthConfig::Hba(_)) => {}
        other => panic!("expected Hba auth, got {other:?}"),
    }
}

#[test]
fn auth_config_from_cli_rejects_hba_with_auth_user() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let hba_path = dir.path().join("pg_hba.conf");
    std::fs::write(&hba_path, "host all all 127.0.0.1/32 trust\n").expect("write hba");
    // The helper sets auth_user + auth_password_file; adding --hba-file is
    // mutually exclusive and must error.
    let mut cli = cli_with_auth_password_file(dir.path().join("password"));
    cli.hba_file = Some(hba_path);
    auth_config_from_cli(&cli).expect_err("--hba-file and --auth-user are mutually exclusive");
}

#[test]
fn apply_auth_config_enables_md5_password_auth() {
    let server = apply_auth_config(
        Server::with_sample_database(),
        &Some(ultrasql_server::AuthConfig::Md5 {
            username: "alice".to_owned(),
            password: "very-secret-password".to_owned(),
        }),
    );

    match &server.auth {
        ultrasql_server::AuthConfig::Md5 { username, password } => {
            assert_eq!(username, "alice");
            assert_eq!(password, "very-secret-password");
        }
        other => panic!("expected MD5 auth, got {other:?}"),
    }
}

#[test]
fn md5_auth_from_cli_rejects_partial_or_dirty_password_config() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let password_file = dir.path().join("password");
    write_private_password_file(&password_file, "short\n");
    let mut cli = Cli {
        listen: "127.0.0.1:5433".parse().expect("listen addr"),
        allow_insecure_listen: false,
        data_dir: None,
        ops_listen: None,
        ops_token: None,
        auth_user: Some("alice".to_owned()),
        auth_password_file: None,
        auth_method: "scram".to_owned(),
        hba_file: None,
        tls_cert: None,
        tls_key: None,
        log_level: "info".to_owned(),
        log_format: LogFormat::Text,
        log_connections: false,
        log_min_duration_statement_ms: -1,
        log_statement: CliLogStatementMode::None,
        idle_session_timeout_ms: 0,
        shutdown_drain_timeout_ms: 5000,
        autovacuum_interval_ms: 1000,
        checkpoint_interval_ms: 0,
        wal_segment_size_bytes: 0,
        autovacuum_vacuum_threshold: 50,
        autovacuum_vacuum_scale_factor: 0.2,
        autovacuum_analyze_threshold: 50,
        autovacuum_analyze_scale_factor: 0.1,
        archive_command: None,
        restore_command: None,
        restore_max_segments: 0,
        archive_interval_ms: 1000,
        archive_command_timeout_ms: 60_000,
        restore_command_timeout_ms: 60_000,
        wal_sync_method: "fsync".to_owned(),
    };

    let err = auth_config_from_cli(&cli).expect_err("partial auth config rejected");
    assert!(
        err.contains("auth_password_file"),
        "expected missing password-file rejection, got {err}"
    );

    cli.auth_password_file = Some(password_file);
    let err = auth_config_from_cli(&cli).expect_err("weak password rejected");
    assert!(
        err.contains("at least 12 bytes"),
        "expected weak password rejection, got {err}"
    );

    let dirty_file = dir.path().join("dirty-password");
    write_private_password_file(&dirty_file, "valid-password\r\n");
    cli.auth_password_file = Some(dirty_file);
    let err = auth_config_from_cli(&cli).expect_err("dirty password rejected");
    assert!(
        err.contains("control bytes"),
        "expected control-byte rejection, got {err}"
    );
}

#[test]
fn md5_auth_from_cli_rejects_unsafe_password_files() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let oversized = dir.path().join("oversized-password");
    write_private_password_file(&oversized, &"a".repeat(2048));
    let cli = cli_with_auth_password_file(oversized);

    let err = auth_config_from_cli(&cli).expect_err("oversized password file rejected");
    assert!(
        err.contains("at most"),
        "expected oversized password-file rejection, got {err}"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut cli = cli;
        let public_file = dir.path().join("public-password");
        write_private_password_file(&public_file, "very-secret-password\n");
        std::fs::set_permissions(&public_file, std::fs::Permissions::from_mode(0o644))
            .expect("make password file public");
        cli.auth_password_file = Some(public_file);
        let err = auth_config_from_cli(&cli).expect_err("public password file rejected");
        assert!(
            err.contains("group- or world-accessible"),
            "expected password-file mode rejection, got {err}"
        );

        let target_file = dir.path().join("target-password");
        write_private_password_file(&target_file, "very-secret-password\n");
        let symlink_file = dir.path().join("symlink-password");
        std::os::unix::fs::symlink(&target_file, &symlink_file).expect("create password symlink");
        cli.auth_password_file = Some(symlink_file);
        let err = auth_config_from_cli(&cli).expect_err("password symlink rejected");
        assert!(
            err.contains("symlink"),
            "expected password-file symlink rejection, got {err}"
        );
    }
}

fn write_private_password_file(path: &Path, contents: &str) {
    std::fs::write(path, contents).expect("write password");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("chmod private password file");
    }
}

/// A non-loopback, no-auth, no-flag `Cli` — the insecure-by-default
/// shape the public-bind guard must reject unless something is opted in.
fn wildcard_trust_cli() -> Cli {
    let mut cli = cli_with_auth_password_file(PathBuf::from("/unused"));
    cli.listen = "0.0.0.0:5433".parse().expect("wildcard listen");
    cli.auth_user = None;
    cli.auth_password_file = None;
    cli
}

fn cli_with_auth_password_file(password_file: PathBuf) -> Cli {
    Cli {
        listen: "127.0.0.1:5433".parse().expect("listen addr"),
        allow_insecure_listen: false,
        data_dir: None,
        ops_listen: None,
        ops_token: None,
        auth_user: Some("alice".to_owned()),
        auth_password_file: Some(password_file),
        auth_method: "scram".to_owned(),
        hba_file: None,
        tls_cert: None,
        tls_key: None,
        log_level: "info".to_owned(),
        log_format: LogFormat::Text,
        log_connections: false,
        log_min_duration_statement_ms: -1,
        log_statement: CliLogStatementMode::None,
        idle_session_timeout_ms: 0,
        shutdown_drain_timeout_ms: 5000,
        autovacuum_interval_ms: 1000,
        checkpoint_interval_ms: 0,
        wal_segment_size_bytes: 0,
        autovacuum_vacuum_threshold: 50,
        autovacuum_vacuum_scale_factor: 0.2,
        autovacuum_analyze_threshold: 50,
        autovacuum_analyze_scale_factor: 0.1,
        archive_command: None,
        restore_command: None,
        restore_max_segments: 0,
        archive_interval_ms: 1000,
        archive_command_timeout_ms: 60_000,
        restore_command_timeout_ms: 60_000,
        wal_sync_method: "fsync".to_owned(),
    }
}

#[test]
fn ops_token_from_cli_rejects_weak_tokens() {
    let mut cli = Cli {
        listen: "127.0.0.1:5433".parse().expect("listen addr"),
        allow_insecure_listen: false,
        data_dir: None,
        ops_listen: None,
        ops_token: None,
        auth_user: None,
        auth_password_file: None,
        auth_method: "scram".to_owned(),
        hba_file: None,
        tls_cert: None,
        tls_key: None,
        log_level: "info".to_owned(),
        log_format: LogFormat::Text,
        log_connections: false,
        log_min_duration_statement_ms: -1,
        log_statement: CliLogStatementMode::None,
        idle_session_timeout_ms: 0,
        shutdown_drain_timeout_ms: 5000,
        autovacuum_interval_ms: 1000,
        checkpoint_interval_ms: 0,
        wal_segment_size_bytes: 0,
        autovacuum_vacuum_threshold: 50,
        autovacuum_vacuum_scale_factor: 0.2,
        autovacuum_analyze_threshold: 50,
        autovacuum_analyze_scale_factor: 0.1,
        archive_command: None,
        restore_command: None,
        restore_max_segments: 0,
        archive_interval_ms: 1000,
        archive_command_timeout_ms: 60_000,
        restore_command_timeout_ms: 60_000,
        wal_sync_method: "fsync".to_owned(),
    };

    assert!(
        ops_token_from_cli(&cli)
            .expect("missing token ok")
            .is_none()
    );

    cli.ops_token = Some("short".to_owned());
    assert!(ops_token_from_cli(&cli).is_err());

    cli.ops_token = Some("0123456789abcde ".to_owned());
    assert!(ops_token_from_cli(&cli).is_err());

    cli.ops_token = Some("0123456789abcdef".to_owned());
    assert_eq!(
        ops_token_from_cli(&cli).expect("valid token").as_deref(),
        Some("0123456789abcdef")
    );
}

#[test]
fn startup_signal_files_enable_standby_mode() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let server = Server::with_sample_database();

    assert!(!apply_startup_signal_files(&server, dir.path()));
    assert!(!server.is_standby_mode());

    std::fs::write(dir.path().join("standby.signal"), b"standby\n").expect("write signal");
    assert!(apply_startup_signal_files(&server, dir.path()));
    assert!(server.is_standby_mode());
}

#[cfg(unix)]
#[test]
fn startup_signal_files_ignore_symlinked_markers() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::TempDir::new().expect("temp dir");
    let server = Server::with_sample_database();
    let outside = dir.path().join("outside-signal");
    std::fs::write(&outside, b"standby\n").expect("outside signal");
    symlink(&outside, dir.path().join("standby.signal")).expect("standby symlink");

    assert!(!apply_startup_signal_files(&server, dir.path()));
    assert!(!server.is_standby_mode());
}
