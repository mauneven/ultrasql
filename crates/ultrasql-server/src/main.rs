//! `ultrasqld` — UltraSQL database server.
//!
//! Binary entry point. Parses CLI arguments, initializes structured
//! logging, builds a Tokio runtime, optionally boots a WAL-backed data
//! directory, and runs the connection accept loop until shutdown.
//!
//! The actual session logic lives in the [`ultrasql_server`] library
//! crate so it can be exercised by unit tests against an in-memory
//! duplex stream as well as by integration tests over a real TCP
//! socket.

// Panic hardening: production (non-test) server-binary code must not
// `.unwrap()`, `.expect()`, or `panic!`. Fallible sites propagate errors;
// proven invariants carry a per-site `#[allow]` with an `// INVARIANT:`
// justification.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

use std::fs;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use ultrasql_server::{
    AutovacuumConfig, LogStatementMode, LoggingConfig, Server, WalArchiveConfig, run_server,
};

const OPS_REQUEST_HEAD_LIMIT_BYTES: usize = 8 * 1024;
const OPS_REQUEST_HEAD_HARD_LIMIT_BYTES: usize = 64 * 1024;
const OPS_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_WAL_COMMAND_TIMEOUT: Duration = Duration::from_secs(60);
const WAL_COMMAND_TIMEOUT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const AUTH_PASSWORD_FILE_MAX_BYTES: u64 = 1024;

/// `ultrasqld` v0.5: SQL server with an
/// in-memory sample database.
///
/// On startup the server registers a single table:
///
/// ```text
///     users(id INT, name TEXT, score DOUBLE PRECISION)
/// ```
///
/// pre-populated with three rows (Ada, Grace, Linus). Connect with
/// any PostgreSQL v3 client and run:
///
/// ```text
///     SELECT id FROM users;
///     SELECT id FROM users WHERE id = 2;
///     SELECT id FROM users LIMIT 1;
/// ```
#[derive(Debug, Parser)]
#[command(
    name = "ultrasqld",
    version,
    about = "UltraSQL database server",
    long_about = LONG_ABOUT
)]
struct Cli {
    /// Address to bind the PostgreSQL-wire listener on.
    #[arg(long, default_value = "127.0.0.1:5433")]
    listen: SocketAddr,

    /// Permit trust-auth PostgreSQL-wire listener on non-loopback addresses.
    #[arg(long, default_value_t = false)]
    allow_insecure_listen: bool,

    /// Optional data directory. When set, server boots WAL-backed storage.
    #[arg(long, env = "ULTRASQL_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Optional HTTP operations endpoint for `/health`, `/ready`, `/metrics`,
    /// and token-protected backup fencing.
    #[arg(long, env = "ULTRASQL_OPS_LISTEN")]
    ops_listen: Option<SocketAddr>,

    /// Bearer token required for mutating ops routes such as backup fencing.
    #[arg(long, env = "ULTRASQL_OPS_TOKEN")]
    ops_token: Option<String>,

    /// PostgreSQL startup user that must authenticate with MD5.
    #[arg(long, env = "ULTRASQL_AUTH_USER")]
    auth_user: Option<String>,

    /// File containing the MD5 authentication password for `--auth-user`.
    #[arg(long, env = "ULTRASQL_AUTH_PASSWORD_FILE")]
    auth_password_file: Option<PathBuf>,

    /// Tracing level filter, e.g. `info`, `debug`, `ultrasqld=trace`.
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Log output format.
    #[arg(long, value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,

    /// Log each successful connection after authentication.
    #[arg(long, default_value_t = false)]
    log_connections: bool,

    /// Minimum statement duration to log in milliseconds; -1 disables.
    #[arg(long, default_value_t = -1)]
    log_min_duration_statement_ms: i64,

    /// Statement classes logged regardless of duration.
    #[arg(long, value_enum, default_value_t = CliLogStatementMode::None)]
    log_statement: CliLogStatementMode,

    /// Close idle sessions after this many milliseconds; 0 disables.
    #[arg(long, default_value_t = 0)]
    idle_session_timeout_ms: u64,

    /// Background autovacuum/analyze maintenance interval in milliseconds.
    #[arg(long, default_value_t = 1000)]
    autovacuum_interval_ms: u64,

    /// Minimum tuple changes before autovacuum considers VACUUM work.
    #[arg(long, default_value_t = 50)]
    autovacuum_vacuum_threshold: u64,

    /// Fraction of estimated table rows added to the VACUUM threshold.
    #[arg(long, default_value_t = 0.2)]
    autovacuum_vacuum_scale_factor: f64,

    /// Minimum tuple changes before autovacuum considers ANALYZE work.
    #[arg(long, default_value_t = 50)]
    autovacuum_analyze_threshold: u64,

    /// Fraction of estimated table rows added to the ANALYZE threshold.
    #[arg(long, default_value_t = 0.1)]
    autovacuum_analyze_scale_factor: f64,

    /// Shell command used to archive completed WAL files. `%p` expands to the
    /// source path and `%f` expands to the WAL filename.
    #[arg(long, env = "ULTRASQL_ARCHIVE_COMMAND")]
    archive_command: Option<String>,

    /// Shell command used to restore archived WAL files before startup
    /// recovery. `%p` expands to the destination path and `%f` expands to the
    /// WAL filename.
    #[arg(long, env = "ULTRASQL_RESTORE_COMMAND")]
    restore_command: Option<String>,

    /// Maximum number of WAL segment names to probe with `restore_command`.
    /// Zero disables server-side startup restore.
    #[arg(long, default_value_t = 0)]
    restore_max_segments: u32,

    /// Background WAL archive scan interval in milliseconds.
    #[arg(long, default_value_t = 1000)]
    archive_interval_ms: u64,

    /// Kill `archive_command` after this many milliseconds; 0 disables.
    #[arg(
        long,
        env = "ULTRASQL_ARCHIVE_COMMAND_TIMEOUT_MS",
        default_value_t = 60_000
    )]
    archive_command_timeout_ms: u64,

    /// Kill `restore_command` after this many milliseconds; 0 disables.
    #[arg(
        long,
        env = "ULTRASQL_RESTORE_COMMAND_TIMEOUT_MS",
        default_value_t = 60_000
    )]
    restore_command_timeout_ms: u64,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliLogStatementMode {
    None,
    Ddl,
    Mod,
    All,
}

impl From<CliLogStatementMode> for LogStatementMode {
    fn from(value: CliLogStatementMode) -> Self {
        match value {
            CliLogStatementMode::None => Self::None,
            CliLogStatementMode::Ddl => Self::Ddl,
            CliLogStatementMode::Mod => Self::Mod,
            CliLogStatementMode::All => Self::All,
        }
    }
}

/// Long description shown by `--help`. Kept as a separate constant so
/// rustfmt does not split it across lines that mangle the indentation.
const LONG_ABOUT: &str = "UltraSQL database server (v0.5).

Speaks the PostgreSQL wire protocol v3. Without --data-dir it serves an
in-memory sample database pre-populated with:

    users(id INT, name TEXT, score DOUBLE PRECISION)
    -- 3 rows: Ada/Grace/Linus

Connect with any libpq-style client. Example session:

    psql -h 127.0.0.1 -p 5433 -d ultrasql -c 'SELECT id FROM users;'

Supported query shapes in v0.5:
  - SELECT col [, col]* FROM users
  - SELECT col FROM users WHERE int_col = literal
  - ... LIMIT n

Production-oriented v0.9 flags:
  - --data-dir DIR      boot WAL-backed storage
  - --allow-insecure-listen  permit trust-auth listener outside loopback
  - --auth-user USER    require MD5 password auth for this PostgreSQL user
  - --auth-password-file PATH  read MD5 auth password from a private local secret file
  - --ops-listen ADDR   serve /health, /ready, /metrics, and backup routes
  - --ops-token TOKEN   require bearer token for /backup/start and /backup/stop
  - --log-format json   emit structured logs
  - --log-min-duration-statement-ms N
  - --log-statement none|ddl|mod|all
  - --idle-session-timeout-ms N
  - --archive-command CMD  archive completed WAL files; %p=path, %f=name
  - --restore-command CMD  restore archived WAL before recovery; %p=path, %f=name
  - --archive-command-timeout-ms N  kill hung archive commands; 0 disables
  - --restore-command-timeout-ms N  kill hung restore commands; 0 disables
";

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    if let Err(e) = init_tracing(&cli.log_level, cli.log_format) {
        eprintln!("ultrasqld: failed to initialise tracing: {e}");
        return std::process::ExitCode::from(1);
    }
    let autovacuum_config = match autovacuum_config_from_cli(&cli) {
        Ok(config) => config,
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "invalid autovacuum configuration");
            return std::process::ExitCode::from(1);
        }
    };
    let logging_config = match logging_config_from_cli(&cli) {
        Ok(config) => config,
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "invalid logging configuration");
            return std::process::ExitCode::from(1);
        }
    };
    let auth_config = match auth_config_from_cli(&cli) {
        Ok(config) => config,
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "invalid auth configuration");
            return std::process::ExitCode::from(1);
        }
    };
    if let Err(e) = listen_security_from_cli(&cli) {
        error!(target: "ultrasqld", error = %e, "invalid listener security configuration");
        return std::process::ExitCode::from(1);
    }
    let ops_token = match ops_token_from_cli(&cli) {
        Ok(token) => token,
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "invalid ops token configuration");
            return std::process::ExitCode::from(1);
        }
    };
    let wal_archive_config = WalArchiveConfig {
        archive_command: cli.archive_command.clone().unwrap_or_default(),
        restore_command: cli.restore_command.clone().unwrap_or_default(),
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "failed to build tokio runtime");
            return std::process::ExitCode::from(1);
        }
    };

    let state = match &cli.data_dir {
        Some(path) => {
            if let Some(command) = cli
                .restore_command
                .as_deref()
                .filter(|command| !command.trim().is_empty())
            {
                let timeout = command_timeout(cli.restore_command_timeout_ms);
                match restore_wal_once_with_timeout(
                    path,
                    command,
                    cli.restore_max_segments,
                    timeout,
                ) {
                    Ok(restored) if restored > 0 => {
                        info!(target: "ultrasqld", restored, data_dir = %path.display(), "restored archived WAL before startup recovery");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        error!(target: "ultrasqld", error = %e, data_dir = %path.display(), "WAL restore failed");
                        return std::process::ExitCode::from(1);
                    }
                }
            }
            match Server::init(path) {
                Ok(mut server) => {
                    server.set_autovacuum_config(autovacuum_config);
                    server.set_logging_config(logging_config);
                    server.set_idle_session_timeout_ms(cli.idle_session_timeout_ms);
                    server.set_wal_archive_config(wal_archive_config.clone());
                    server = apply_auth_config(server, &auth_config);
                    Arc::new(server)
                }
                Err(e) => {
                    error!(target: "ultrasqld", error = %e, data_dir = %path.display(), "server init failed");
                    return std::process::ExitCode::from(1);
                }
            }
        }
        None => {
            let mut server = Server::with_sample_database();
            server.set_autovacuum_config(autovacuum_config);
            server.set_logging_config(logging_config);
            server.set_idle_session_timeout_ms(cli.idle_session_timeout_ms);
            server.set_wal_archive_config(wal_archive_config);
            server = apply_auth_config(server, &auth_config);
            Arc::new(server)
        }
    };
    if let Some(path) = &cli.data_dir {
        if apply_startup_signal_files(state.as_ref(), path) {
            info!(target: "ultrasqld", data_dir = %path.display(), "hot standby read-only mode enabled");
        }
    }
    let outcome = runtime.block_on(async move {
        if let Some(ops_addr) = cli.ops_listen {
            let pg_addr = cli.listen;
            let ops_state = Arc::clone(&state);
            let ops_token = ops_token.clone();
            tokio::spawn(async move {
                if let Err(e) = run_ops_endpoint(ops_addr, pg_addr, ops_state, ops_token).await {
                    error!(target: "ultrasqld", error = %e, "ops endpoint terminated");
                }
            });
        }
        if cli.autovacuum_interval_ms > 0 {
            let autovacuum_state = Arc::clone(&state);
            let interval_ms = cli.autovacuum_interval_ms;
            tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_millis(interval_ms));
                loop {
                    ticker.tick().await;
                    autovacuum_state.run_autovacuum_cycle();
                }
            });
        }
        if let (Some(data_dir), Some(command)) = (
            cli.data_dir.clone(),
            cli.archive_command
                .clone()
                .filter(|command| !command.trim().is_empty()),
        ) {
            let interval_ms = cli.archive_interval_ms;
            let timeout = command_timeout(cli.archive_command_timeout_ms);
            tokio::spawn(async move {
                run_wal_archiver_loop(data_dir, command, interval_ms, timeout).await;
            });
        }
        run_server(cli.listen, state).await
    });
    match outcome {
        Ok(()) => std::process::ExitCode::from(0),
        Err(e) => {
            error!(target: "ultrasqld", error = %e, "server terminated with error");
            std::process::ExitCode::from(1)
        }
    }
}

fn init_tracing(
    filter: &str,
    format: LogFormat,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let env_filter = EnvFilter::try_new(filter)?;
    match format {
        LogFormat::Text => tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(true)
            .try_init()?,
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .with_target(true)
            .try_init()?,
    }
    Ok(())
}

fn autovacuum_config_from_cli(cli: &Cli) -> Result<AutovacuumConfig, String> {
    Ok(AutovacuumConfig {
        vacuum_threshold: cli.autovacuum_vacuum_threshold,
        vacuum_scale_factor_ppm: AutovacuumConfig::scale_factor_to_ppm(
            "autovacuum_vacuum_scale_factor",
            cli.autovacuum_vacuum_scale_factor,
        )?,
        analyze_threshold: cli.autovacuum_analyze_threshold,
        analyze_scale_factor_ppm: AutovacuumConfig::scale_factor_to_ppm(
            "autovacuum_analyze_scale_factor",
            cli.autovacuum_analyze_scale_factor,
        )?,
    })
}

async fn run_wal_archiver_loop(
    data_dir: PathBuf,
    archive_command: String,
    interval_ms: u64,
    timeout: Option<Duration>,
) {
    let interval_ms = interval_ms.max(1);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
    loop {
        ticker.tick().await;
        match archive_wal_once_with_timeout(&data_dir, &archive_command, timeout) {
            Ok(archived) if archived > 0 => {
                info!(target: "ultrasqld", archived, "WAL archiver completed batch");
            }
            Ok(_) => {}
            Err(e) => {
                error!(target: "ultrasqld", error = %e, "WAL archiver failed");
            }
        }
    }
}

#[cfg(test)]
fn archive_wal_once(data_dir: &Path, archive_command: &str) -> Result<usize, String> {
    archive_wal_once_with_timeout(data_dir, archive_command, Some(DEFAULT_WAL_COMMAND_TIMEOUT))
}

fn archive_wal_once_with_timeout(
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
fn restore_wal_once(
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

fn restore_wal_once_with_timeout(
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

fn wal_segment_filename(index: u32) -> String {
    format!("segment_{index:010}")
}

fn render_archive_command(template: &str, path: &Path, filename: &str) -> String {
    template
        .replace("%p", &path.to_string_lossy())
        .replace("%f", filename)
}

fn render_restore_command(template: &str, path: &Path, filename: &str) -> String {
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

fn run_shell_command_with_timeout(
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

fn command_timeout(timeout_ms: u64) -> Option<Duration> {
    (timeout_ms > 0).then(|| Duration::from_millis(timeout_ms))
}

fn timeout_label_ms(timeout: Option<Duration>) -> u128 {
    timeout.unwrap_or(DEFAULT_WAL_COMMAND_TIMEOUT).as_millis()
}

fn ops_token_from_cli(cli: &Cli) -> Result<Option<Arc<str>>, String> {
    let Some(token) = cli.ops_token.as_deref() else {
        return Ok(None);
    };
    if token.len() < 16 {
        return Err("ops_token must be at least 16 bytes".to_string());
    }
    if token
        .bytes()
        .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
    {
        return Err("ops_token must not contain whitespace or control bytes".to_string());
    }
    Ok(Some(Arc::<str>::from(token)))
}

fn auth_config_from_cli(cli: &Cli) -> Result<Option<(String, String)>, String> {
    match (cli.auth_user.as_deref(), cli.auth_password_file.as_deref()) {
        (None, None) => Ok(None),
        (Some(_), None) => Err("auth_password_file is required when auth_user is set".to_string()),
        (None, Some(_)) => Err("auth_user is required when auth_password_file is set".to_string()),
        (Some(user), Some(password_file)) => {
            validate_auth_user(user)?;
            let raw = read_auth_password_file(password_file)?;
            let password = raw.strip_suffix('\n').unwrap_or(raw.as_str());
            validate_auth_password(password)?;
            Ok(Some((user.to_owned(), password.to_owned())))
        }
    }
}

fn read_auth_password_file(password_file: &Path) -> Result<String, String> {
    reject_auth_password_file_symlink(password_file)?;
    let file = open_auth_password_file(password_file)?;
    let metadata = file.metadata().map_err(|err| {
        format!(
            "inspect auth_password_file {}: {err}",
            password_file.display()
        )
    })?;
    let file_type = metadata.file_type();
    if !file_type.is_file() {
        return Err(format!(
            "auth_password_file {} must be a regular file",
            password_file.display()
        ));
    }
    validate_auth_password_file_permissions(password_file, &metadata)?;
    if metadata.len() > AUTH_PASSWORD_FILE_MAX_BYTES {
        return Err(format!(
            "auth_password_file {} must be at most {AUTH_PASSWORD_FILE_MAX_BYTES} bytes",
            password_file.display()
        ));
    }
    let mut raw = String::new();
    let mut limited = file.take(AUTH_PASSWORD_FILE_MAX_BYTES.saturating_add(1));
    limited
        .read_to_string(&mut raw)
        .map_err(|err| format!("read auth_password_file {}: {err}", password_file.display()))?;
    if u64::try_from(raw.len()).unwrap_or(u64::MAX) > AUTH_PASSWORD_FILE_MAX_BYTES {
        return Err(format!(
            "auth_password_file {} must be at most {AUTH_PASSWORD_FILE_MAX_BYTES} bytes",
            password_file.display()
        ));
    }
    Ok(raw)
}

fn reject_auth_password_file_symlink(password_file: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(password_file).map_err(|err| {
        format!(
            "inspect auth_password_file {}: {err}",
            password_file.display()
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "auth_password_file {} must not be a symlink",
            password_file.display()
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn open_auth_password_file(password_file: &Path) -> Result<fs::File, String> {
    use std::os::unix::fs::OpenOptionsExt;

    fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(password_file)
        .map_err(|err| {
            if err.raw_os_error() == Some(libc::ELOOP) {
                format!(
                    "auth_password_file {} must not be a symlink",
                    password_file.display()
                )
            } else {
                format!("open auth_password_file {}: {err}", password_file.display())
            }
        })
}

#[cfg(not(unix))]
fn open_auth_password_file(password_file: &Path) -> Result<fs::File, String> {
    fs::File::open(password_file)
        .map_err(|err| format!("open auth_password_file {}: {err}", password_file.display()))
}

#[cfg(unix)]
fn validate_auth_password_file_permissions(
    password_file: &Path,
    metadata: &fs::Metadata,
) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(format!(
            "auth_password_file {} must not be group- or world-accessible",
            password_file.display()
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_auth_password_file_permissions(
    _password_file: &Path,
    _metadata: &fs::Metadata,
) -> Result<(), String> {
    Ok(())
}

fn validate_auth_user(user: &str) -> Result<(), String> {
    if user.is_empty() {
        return Err("auth_user must not be empty".to_string());
    }
    if user
        .bytes()
        .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
    {
        return Err("auth_user must not contain whitespace or control bytes".to_string());
    }
    Ok(())
}

fn validate_auth_password(password: &str) -> Result<(), String> {
    if password.len() < 12 {
        return Err("auth password must be at least 12 bytes".to_string());
    }
    if password
        .bytes()
        .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
    {
        return Err("auth password must not contain whitespace or control bytes".to_string());
    }
    Ok(())
}

fn apply_auth_config(mut server: Server, auth_config: &Option<(String, String)>) -> Server {
    if let Some((username, password)) = auth_config {
        server = server.require_md5_password(username.clone(), password.clone());
    }
    server
}

fn logging_config_from_cli(cli: &Cli) -> Result<LoggingConfig, String> {
    if cli.log_min_duration_statement_ms < -1 {
        return Err("log_min_duration_statement_ms must be -1 or greater".to_string());
    }
    Ok(LoggingConfig {
        log_connections: cli.log_connections,
        log_min_duration_statement_ms: cli.log_min_duration_statement_ms,
        log_statement: cli.log_statement.into(),
    })
}

fn listen_security_from_cli(cli: &Cli) -> Result<(), String> {
    if cli.listen.ip().is_loopback() || cli.allow_insecure_listen || cli_has_password_auth(cli) {
        return Ok(());
    }
    Err(format!(
        "PostgreSQL listener {} uses trust authentication on a non-loopback address; bind to 127.0.0.1/::1 or pass --allow-insecure-listen for isolated test networks",
        cli.listen
    ))
}

fn cli_has_password_auth(cli: &Cli) -> bool {
    cli.auth_user.is_some() && cli.auth_password_file.is_some()
}

fn apply_startup_signal_files(state: &Server, data_dir: &Path) -> bool {
    let enabled = startup_signal_file_present(&data_dir.join("standby.signal"))
        || startup_signal_file_present(&data_dir.join("recovery.signal"));
    if enabled {
        state.set_standby_mode(true);
    }
    enabled
}

fn startup_signal_file_present(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file())
}

fn start_backup_fence(state: &Server) -> Result<String, String> {
    state.set_standby_mode(true);
    let flushed_pages = match state.flush_dirty_heap_pages() {
        Ok(flushed) => flushed,
        Err(e) => {
            state.set_standby_mode(false);
            return Err(format!(
                "{{\"status\":\"backup_start_failed\",\"error\":\"{}\"}}\n",
                json_escape(&e.to_string())
            ));
        }
    };
    let flushed_lsn = state
        .runtime_wal_flushed_lsn()
        .map_or_else(|| "null".to_string(), |lsn| lsn.raw().to_string());
    Ok(format!(
        "{{\"status\":\"backup_started\",\"read_only\":true,\"flushed_pages\":{},\"flushed_lsn\":{flushed_lsn}}}\n",
        usize_to_u64_saturated(flushed_pages)
    ))
}

fn stop_backup_fence(state: &Server) -> String {
    state.set_standby_mode(false);
    "{\"status\":\"backup_stopped\",\"read_only\":false}\n".to_string()
}

async fn run_ops_endpoint(
    addr: SocketAddr,
    pg_addr: SocketAddr,
    state: Arc<Server>,
    ops_token: Option<Arc<str>>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(handle_ops_request(
            stream,
            pg_addr,
            Arc::clone(&state),
            ops_token.clone(),
        ));
    }
}

async fn handle_ops_request(
    mut stream: TcpStream,
    pg_addr: SocketAddr,
    state: Arc<Server>,
    ops_token: Option<Arc<str>>,
) {
    let buf = match read_ops_request_head(&mut stream).await {
        OpsRequestHead::Complete(buf) => buf,
        OpsRequestHead::TooLarge => {
            write_ops_response(
                &mut stream,
                "431 Request Header Fields Too Large",
                "application/json",
                "{\"error\":\"request header too large\"}\n",
            )
            .await;
            return;
        }
        OpsRequestHead::Timeout | OpsRequestHead::Io => return,
    };
    let req = String::from_utf8_lossy(&buf);
    let request_line = req.lines().next().unwrap_or_default();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default();
    let path = request_parts.next().unwrap_or("/");

    let (status, content_type, body) = match path {
        "/health" => (
            "200 OK",
            "application/json",
            format!(
                "{{\"status\":\"ok\",\"server\":\"ultrasqld\",\"version\":\"{}\"}}\n",
                env!("CARGO_PKG_VERSION")
            ),
        ),
        "/ready" => {
            let ready = TcpStream::connect(pg_addr).await.is_ok();
            if ready {
                (
                    "200 OK",
                    "application/json",
                    format!(
                        "{{\"status\":\"ready\",\"postgres_listener\":\"{}\"}}\n",
                        pg_addr
                    ),
                )
            } else {
                (
                    "503 Service Unavailable",
                    "application/json",
                    format!(
                        "{{\"status\":\"not_ready\",\"postgres_listener\":\"{}\"}}\n",
                        pg_addr
                    ),
                )
            }
        }
        "/metrics" => ("200 OK", "text/plain; version=0.0.4", metrics_body(&state)),
        "/backup/start" if method == "POST" => {
            match ops_control_auth_response(&req, ops_token.as_deref()) {
                Some(auth_response) => auth_response,
                None => match start_backup_fence(&state) {
                    Ok(body) => ("200 OK", "application/json", body),
                    Err(body) => ("500 Internal Server Error", "application/json", body),
                },
            }
        }
        "/backup/stop" if method == "POST" => {
            match ops_control_auth_response(&req, ops_token.as_deref()) {
                Some(auth_response) => auth_response,
                None => ("200 OK", "application/json", stop_backup_fence(&state)),
            }
        }
        "/backup/start" | "/backup/stop" => (
            "405 Method Not Allowed",
            "application/json",
            "{\"error\":\"method not allowed\"}\n".to_string(),
        ),
        _ => (
            "404 Not Found",
            "application/json",
            "{\"error\":\"not found\"}\n".to_string(),
        ),
    };

    write_ops_response(&mut stream, status, content_type, &body).await;
}

fn ops_control_auth_response(
    request: &str,
    ops_token: Option<&str>,
) -> Option<(&'static str, &'static str, String)> {
    let Some(expected) = ops_token else {
        return Some((
            "403 Forbidden",
            "application/json",
            "{\"error\":\"ops token required\"}\n".to_string(),
        ));
    };
    let Some(actual) = ops_authorization_bearer(request) else {
        return Some((
            "401 Unauthorized",
            "application/json",
            "{\"error\":\"unauthorized\"}\n".to_string(),
        ));
    };
    if constant_time_eq(expected.as_bytes(), actual.as_bytes()) {
        None
    } else {
        Some((
            "401 Unauthorized",
            "application/json",
            "{\"error\":\"unauthorized\"}\n".to_string(),
        ))
    }
}

fn ops_authorization_bearer(request: &str) -> Option<&str> {
    for line in request.lines().skip(1) {
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("authorization") {
            return value.trim().strip_prefix("Bearer ");
        }
    }
    None
}

fn constant_time_eq(expected: &[u8], supplied: &[u8]) -> bool {
    let mut diff = expected.len() ^ supplied.len();
    for (idx, expected_byte) in expected.iter().copied().enumerate() {
        let supplied_byte = supplied.get(idx).copied().unwrap_or(0);
        diff |= usize::from(expected_byte ^ supplied_byte);
    }
    diff == 0
}

enum OpsRequestHead {
    Complete(Vec<u8>),
    TooLarge,
    Timeout,
    Io,
}

async fn read_ops_request_head(stream: &mut TcpStream) -> OpsRequestHead {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    let mut too_large = false;
    loop {
        let read =
            match tokio::time::timeout(OPS_REQUEST_READ_TIMEOUT, stream.read(&mut chunk)).await {
                Ok(Ok(read)) => read,
                Ok(Err(_)) => return OpsRequestHead::Io,
                Err(_) => return OpsRequestHead::Timeout,
            };
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
        if request.len() > OPS_REQUEST_HEAD_LIMIT_BYTES {
            too_large = true;
        }
        if request.len() > OPS_REQUEST_HEAD_HARD_LIMIT_BYTES {
            return OpsRequestHead::TooLarge;
        }
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    if too_large {
        return OpsRequestHead::TooLarge;
    }
    OpsRequestHead::Complete(request)
}

async fn write_ops_response(stream: &mut TcpStream, status: &str, content_type: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

fn metrics_body(state: &Server) -> String {
    let buffer = state.heap.buffer_pool().stats();
    let wal_sink = state
        .heap
        .wal_sink()
        .map(|sink| sink.stats())
        .unwrap_or_default();
    let wal = state.wal_writer_stats().unwrap_or_default();
    let object = ultrasql_objectstore::object_range_cache_metrics();
    let ann = state.ann_system_metrics();
    let latency = state.workload_recorder.latency_histogram();

    let mut body = String::new();
    body.push_str(
        "# HELP ultrasql_up Whether ultrasqld process is running.\n\
         # TYPE ultrasql_up gauge\n\
         ultrasql_up 1\n\
         # HELP ultrasql_build_info Build metadata.\n\
         # TYPE ultrasql_build_info gauge\n",
    );
    body.push_str(&format!(
        "ultrasql_build_info{{version=\"{}\"}} 1\n",
        env!("CARGO_PKG_VERSION")
    ));
    body.push_str(
        "# HELP ultrasql_buffer_pool_hits_total Buffer-pool page hits.\n\
         # TYPE ultrasql_buffer_pool_hits_total counter\n",
    );
    push_metric(&mut body, "ultrasql_buffer_pool_hits_total", buffer.hits);
    body.push_str(
        "# HELP ultrasql_buffer_pool_misses_total Buffer-pool page misses.\n\
         # TYPE ultrasql_buffer_pool_misses_total counter\n",
    );
    push_metric(
        &mut body,
        "ultrasql_buffer_pool_misses_total",
        buffer.misses,
    );
    push_metric(&mut body, "ultrasql_buffer_pool_gets_total", buffer.gets);
    push_metric(
        &mut body,
        "ultrasql_buffer_pool_evictions_total",
        buffer.evictions,
    );
    push_metric(
        &mut body,
        "ultrasql_buffer_pool_resident_pages",
        usize_to_u64_saturated(buffer.resident),
    );
    push_metric(
        &mut body,
        "ultrasql_buffer_pool_pinned_pages",
        usize_to_u64_saturated(buffer.pinned),
    );
    push_metric(
        &mut body,
        "ultrasql_buffer_pool_dirty_pages",
        usize_to_u64_saturated(buffer.dirty),
    );

    body.push_str(
        "# HELP ultrasql_wal_fsync_latency_us WAL fsync latency in microseconds.\n\
         # TYPE ultrasql_wal_fsync_latency_us summary\n",
    );
    push_metric(
        &mut body,
        "ultrasql_wal_fsync_latency_us_count",
        wal.fsync_count,
    );
    push_metric(
        &mut body,
        "ultrasql_wal_fsync_latency_us_sum",
        wal.fsync_total_us,
    );
    push_metric(
        &mut body,
        "ultrasql_wal_fsync_latency_us_max",
        wal.fsync_max_us,
    );
    push_metric(
        &mut body,
        "ultrasql_wal_fsync_latency_us_last",
        wal.fsync_last_us,
    );
    body.push_str(
        "# HELP ultrasql_wal_records_total WAL records appended.\n\
         # TYPE ultrasql_wal_records_total counter\n",
    );
    push_metric(
        &mut body,
        "ultrasql_wal_records_total",
        wal_sink.wal_records,
    );
    push_metric(&mut body, "ultrasql_wal_fpi_total", wal_sink.wal_fpi);
    push_metric(&mut body, "ultrasql_wal_bytes_total", wal_sink.wal_bytes);
    push_metric(&mut body, "ultrasql_wal_write_total", wal_sink.wal_write);

    body.push_str(
        "# HELP ultrasql_object_store_remote_bytes_total Object-store bytes fetched remotely.\n\
         # TYPE ultrasql_object_store_remote_bytes_total counter\n",
    );
    push_metric(
        &mut body,
        "ultrasql_object_store_remote_bytes_total",
        object.remote_bytes,
    );
    push_metric(
        &mut body,
        "ultrasql_object_store_range_requests_total",
        object.range_requests,
    );
    push_metric(
        &mut body,
        "ultrasql_object_store_cache_hits_total",
        object.cache_hits,
    );
    push_metric(
        &mut body,
        "ultrasql_object_store_cache_misses_total",
        object.cache_misses,
    );

    body.push_str(
        "# HELP ultrasql_ann_candidates ANN candidates available in runtime vector indexes.\n\
         # TYPE ultrasql_ann_candidates gauge\n",
    );
    push_metric(&mut body, "ultrasql_ann_candidates", ann.candidates);
    push_metric(&mut body, "ultrasql_ann_tombstones", ann.tombstones);
    push_metric(&mut body, "ultrasql_ann_hnsw_indexes", ann.hnsw_indexes);
    push_metric(
        &mut body,
        "ultrasql_ann_ivfflat_indexes",
        ann.ivfflat_indexes,
    );
    push_metric(
        &mut body,
        "ultrasql_vector_index_memory_bytes",
        ann.vector_index_memory_bytes,
    );

    body.push_str(
        "# HELP ultrasql_query_latency_us Query latency histogram in microseconds.\n\
         # TYPE ultrasql_query_latency_us histogram\n",
    );
    for bucket in latency.buckets {
        let le = if bucket.le_us == u64::MAX {
            "+Inf".to_string()
        } else {
            bucket.le_us.to_string()
        };
        body.push_str(&format!(
            "ultrasql_query_latency_us_bucket{{le=\"{le}\"}} {}\n",
            bucket.count
        ));
    }
    push_metric(&mut body, "ultrasql_query_latency_us_count", latency.count);
    push_metric(&mut body, "ultrasql_query_latency_us_sum", latency.sum_us);
    body
}

fn push_metric(body: &mut String, name: &str, value: u64) {
    body.push_str(&format!("{name} {value}\n"));
}

fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_body_reports_system_counters() {
        let state = Server::with_sample_database();
        let body = metrics_body(&state);

        for metric in [
            "ultrasql_buffer_pool_hits_total",
            "ultrasql_buffer_pool_misses_total",
            "ultrasql_wal_fsync_latency_us_count",
            "ultrasql_wal_fsync_latency_us_sum",
            "ultrasql_wal_records_total",
            "ultrasql_wal_bytes_total",
            "ultrasql_object_store_remote_bytes_total",
            "ultrasql_ann_candidates",
            "ultrasql_vector_index_memory_bytes",
            "ultrasql_query_latency_us_bucket",
            "ultrasql_query_latency_us_count",
            "ultrasql_query_latency_us_sum",
        ] {
            assert!(body.contains(metric), "missing metric {metric}");
        }
    }

    #[tokio::test]
    async fn ops_endpoint_paths_return_expected_http_shapes() {
        let state = Arc::new(Server::with_sample_database());
        let pg_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind pg probe");
        let ready_addr = pg_listener.local_addr().expect("pg addr");

        let health = request_ops_path("/health", ready_addr, Arc::clone(&state)).await;
        assert!(health.starts_with("HTTP/1.1 200 OK"));
        assert!(health.contains("\"status\":\"ok\""));

        let ready = request_ops_path("/ready", ready_addr, Arc::clone(&state)).await;
        assert!(ready.starts_with("HTTP/1.1 200 OK"));
        assert!(ready.contains("\"status\":\"ready\""));
        drop(pg_listener);

        let missing_pg: SocketAddr = "127.0.0.1:0".parse().expect("missing pg addr");
        let not_ready = request_ops_path("/ready", missing_pg, Arc::clone(&state)).await;
        assert!(not_ready.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(not_ready.contains("\"status\":\"not_ready\""));

        let metrics = request_ops_path("/metrics", missing_pg, Arc::clone(&state)).await;
        assert!(metrics.starts_with("HTTP/1.1 200 OK"));
        assert!(metrics.contains("content-type: text/plain; version=0.0.4"));
        assert!(metrics.contains("ultrasql_up 1"));

        let not_found = request_ops_path("/nope", missing_pg, state).await;
        assert!(not_found.starts_with("HTTP/1.1 404 Not Found"));
        assert!(not_found.contains("\"error\":\"not found\""));
    }

    #[tokio::test]
    async fn ops_endpoint_backup_routes_reject_get_requests() {
        let state = Arc::new(Server::with_sample_database());
        let missing_pg: SocketAddr = "127.0.0.1:0".parse().expect("missing pg addr");

        let backup_start = request_ops_path("/backup/start", missing_pg, Arc::clone(&state)).await;
        assert!(backup_start.starts_with("HTTP/1.1 405 Method Not Allowed"));
        assert!(!state.is_standby_mode());

        state.set_standby_mode(true);
        let backup_stop = request_ops_path("/backup/stop", missing_pg, Arc::clone(&state)).await;
        assert!(backup_stop.starts_with("HTTP/1.1 405 Method Not Allowed"));
        assert!(state.is_standby_mode());
    }

    #[tokio::test]
    async fn ops_endpoint_backup_routes_require_bearer_token() {
        let state = Arc::new(Server::with_sample_database());
        let missing_pg: SocketAddr = "127.0.0.1:0".parse().expect("missing pg addr");

        let backup_start =
            request_ops_method("POST", "/backup/start", missing_pg, Arc::clone(&state)).await;

        assert!(backup_start.starts_with("HTTP/1.1 403 Forbidden"));
        assert!(backup_start.contains("\"error\":\"ops token required\""));
        assert!(!state.is_standby_mode());

        let token = Arc::<str>::from("0123456789abcdef");
        let missing_auth = request_ops_method_with_auth(
            "POST",
            "/backup/start",
            missing_pg,
            Arc::clone(&state),
            Some(Arc::clone(&token)),
            None,
        )
        .await;
        assert!(missing_auth.starts_with("HTTP/1.1 401 Unauthorized"));
        assert!(!state.is_standby_mode());

        let wrong_auth = request_ops_method_with_auth(
            "POST",
            "/backup/start",
            missing_pg,
            Arc::clone(&state),
            Some(Arc::clone(&token)),
            Some("Bearer fedcba9876543210"),
        )
        .await;
        assert!(wrong_auth.starts_with("HTTP/1.1 401 Unauthorized"));
        assert!(!state.is_standby_mode());

        let backup_start = request_ops_method_with_auth(
            "POST",
            "/backup/start",
            missing_pg,
            Arc::clone(&state),
            Some(Arc::clone(&token)),
            Some("Bearer 0123456789abcdef"),
        )
        .await;
        assert!(backup_start.starts_with("HTTP/1.1 200 OK"));
        assert!(backup_start.contains("\"backup_started\""));
        assert!(state.is_standby_mode());

        let backup_stop = request_ops_method_with_auth(
            "POST",
            "/backup/stop",
            missing_pg,
            Arc::clone(&state),
            Some(token),
            Some("Bearer 0123456789abcdef"),
        )
        .await;
        assert!(backup_stop.starts_with("HTTP/1.1 200 OK"));
        assert!(backup_stop.contains("\"backup_stopped\""));
        assert!(!state.is_standby_mode());
    }

    #[tokio::test]
    async fn ops_endpoint_rejects_oversized_request_headers() {
        let state = Arc::new(Server::with_sample_database());
        let missing_pg: SocketAddr = "127.0.0.1:0".parse().expect("missing pg addr");
        let path = format!("/ready{}", "x".repeat(9 * 1024));

        let response = request_ops_path(&path, missing_pg, state).await;

        assert!(
            response.starts_with("HTTP/1.1 431 Request Header Fields Too Large"),
            "{response}"
        );
    }

    async fn request_ops_path(path: &str, pg_addr: SocketAddr, state: Arc<Server>) -> String {
        request_ops_method("GET", path, pg_addr, state).await
    }

    async fn request_ops_method(
        method: &str,
        path: &str,
        pg_addr: SocketAddr,
        state: Arc<Server>,
    ) -> String {
        request_ops_method_with_auth(method, path, pg_addr, state, None, None).await
    }

    async fn request_ops_method_with_auth(
        method: &str,
        path: &str,
        pg_addr: SocketAddr,
        state: Arc<Server>,
        ops_token: Option<Arc<str>>,
        authorization: Option<&str>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ops probe");
        let addr = listener.local_addr().expect("ops addr");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept ops probe");
            handle_ops_request(stream, pg_addr, state, ops_token).await;
        });

        let mut client = TcpStream::connect(addr).await.expect("connect ops probe");
        let mut request = format!("{method} {path} HTTP/1.1\r\nhost: localhost\r\n");
        if let Some(authorization) = authorization {
            request.push_str("authorization: ");
            request.push_str(authorization);
            request.push_str("\r\n");
        }
        request.push_str("\r\n");
        client
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        let response = read_ops_test_response(&mut client).await;
        server.await.expect("ops task");
        String::from_utf8(response).expect("utf8 response")
    }

    async fn read_ops_test_response(client: &mut TcpStream) -> Vec<u8> {
        let mut response = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            match client.read(&mut chunk).await {
                Ok(0) => break,
                Ok(read) => {
                    response.extend_from_slice(&chunk[..read]);
                    if ops_test_response_complete(&response) {
                        break;
                    }
                }
                Err(err)
                    if err.kind() == std::io::ErrorKind::ConnectionReset
                        && ops_test_response_complete(&response) =>
                {
                    break;
                }
                Err(err) => panic!("read response: {err}"),
            }
        }
        response
    }

    fn ops_test_response_complete(response: &[u8]) -> bool {
        let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
            return false;
        };
        let body_start = header_end + 4;
        let Some(content_length) = ops_test_content_length(&response[..header_end]) else {
            return false;
        };
        response.len().saturating_sub(body_start) >= content_length
    }

    fn ops_test_content_length(header: &[u8]) -> Option<usize> {
        let header = std::str::from_utf8(header).ok()?;
        header.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse().ok()
            } else {
                None
            }
        })
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
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            log_connections: false,
            log_min_duration_statement_ms: -1,
            log_statement: CliLogStatementMode::None,
            idle_session_timeout_ms: 0,
            autovacuum_interval_ms: 1000,
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
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            log_connections: false,
            log_min_duration_statement_ms: -1,
            log_statement: CliLogStatementMode::None,
            idle_session_timeout_ms: 0,
            autovacuum_interval_ms: 1000,
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
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            log_connections: false,
            log_min_duration_statement_ms: -2,
            log_statement: CliLogStatementMode::Mod,
            idle_session_timeout_ms: 0,
            autovacuum_interval_ms: 1000,
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
            log_level: "info".to_owned(),
            log_format: LogFormat::Json,
            log_connections: true,
            log_min_duration_statement_ms: 25,
            log_statement: CliLogStatementMode::All,
            idle_session_timeout_ms: 0,
            autovacuum_interval_ms: 1000,
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
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            log_connections: false,
            log_min_duration_statement_ms: -1,
            log_statement: CliLogStatementMode::None,
            idle_session_timeout_ms: 0,
            autovacuum_interval_ms: 1000,
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
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            log_connections: false,
            log_min_duration_statement_ms: -1,
            log_statement: CliLogStatementMode::None,
            idle_session_timeout_ms: 0,
            autovacuum_interval_ms: 1000,
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
        };

        let auth = auth_config_from_cli(&cli).expect("password file auth config");

        assert_eq!(
            auth,
            Some(("alice".to_owned(), "very-secret-password".to_owned()))
        );
        assert!(listen_security_from_cli(&cli).is_ok());
    }

    #[test]
    fn apply_auth_config_enables_md5_password_auth() {
        let server = apply_auth_config(
            Server::with_sample_database(),
            &Some(("alice".to_owned(), "very-secret-password".to_owned())),
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
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            log_connections: false,
            log_min_duration_statement_ms: -1,
            log_statement: CliLogStatementMode::None,
            idle_session_timeout_ms: 0,
            autovacuum_interval_ms: 1000,
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
            std::os::unix::fs::symlink(&target_file, &symlink_file)
                .expect("create password symlink");
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

    fn cli_with_auth_password_file(password_file: PathBuf) -> Cli {
        Cli {
            listen: "127.0.0.1:5433".parse().expect("listen addr"),
            allow_insecure_listen: false,
            data_dir: None,
            ops_listen: None,
            ops_token: None,
            auth_user: Some("alice".to_owned()),
            auth_password_file: Some(password_file),
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            log_connections: false,
            log_min_duration_statement_ms: -1,
            log_statement: CliLogStatementMode::None,
            idle_session_timeout_ms: 0,
            autovacuum_interval_ms: 1000,
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
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            log_connections: false,
            log_min_duration_statement_ms: -1,
            log_statement: CliLogStatementMode::None,
            idle_session_timeout_ms: 0,
            autovacuum_interval_ms: 1000,
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
    fn ops_constant_time_eq_rejects_wrong_length_tokens() {
        let expected = b"0123456789abcdef";

        assert!(constant_time_eq(expected, b"0123456789abcdef"));
        assert!(!constant_time_eq(expected, b"fedcba9876543210"));
        assert!(!constant_time_eq(expected, b"0123456789abcde"));
        assert!(!constant_time_eq(expected, b"0123456789abcdef0"));
    }

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

        let err = archive_wal_once(dir.path(), failing_shell_command())
            .expect_err("failed archive command");
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
        let err = run_shell_command_with_timeout(
            hanging_shell_command(),
            Some(Duration::from_millis(25)),
        )
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

    #[test]
    fn backup_fence_start_enables_read_only_and_reports_checkpoint() {
        let server = Server::with_sample_database();

        let body = start_backup_fence(&server).expect("backup fence");

        assert!(server.is_standby_mode());
        assert!(body.contains("\"status\":\"backup_started\""));
        assert!(body.contains("\"read_only\":true"));
        assert!(body.contains("\"flushed_pages\":0"));

        let body = stop_backup_fence(&server);
        assert!(!server.is_standby_mode());
        assert!(body.contains("\"status\":\"backup_stopped\""));
    }

    #[test]
    fn scalar_render_helpers_escape_json_and_saturate_usize() {
        let mut body = String::new();
        push_metric(&mut body, "x_total", 42);
        assert_eq!(body, "x_total 42\n");
        assert_eq!(json_escape("a\\b\"c"), "a\\\\b\\\"c");
        assert_eq!(usize_to_u64_saturated(7), 7);
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
}
