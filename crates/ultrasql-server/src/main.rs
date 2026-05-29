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

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use ultrasql_server::{
    AutovacuumConfig, LogStatementMode, LoggingConfig, Server, WalArchiveConfig, run_server,
};

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

    /// Optional data directory. When set, server boots WAL-backed storage.
    #[arg(long, env = "ULTRASQL_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Optional HTTP operations endpoint for `/health`, `/ready`, `/metrics`,
    /// and backup fencing.
    #[arg(long, env = "ULTRASQL_OPS_LISTEN")]
    ops_listen: Option<SocketAddr>,

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
  - --ops-listen ADDR   serve /health, /ready, /metrics, /backup/start, /backup/stop
  - --log-format json   emit structured logs
  - --log-min-duration-statement-ms N
  - --log-statement none|ddl|mod|all
  - --idle-session-timeout-ms N
  - --archive-command CMD  archive completed WAL files; %p=path, %f=name
  - --restore-command CMD  restore archived WAL before recovery; %p=path, %f=name
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
                match restore_wal_once(path, command, cli.restore_max_segments) {
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
            tokio::spawn(async move {
                if let Err(e) = run_ops_endpoint(ops_addr, pg_addr, ops_state).await {
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
            tokio::spawn(async move {
                run_wal_archiver_loop(data_dir, command, interval_ms).await;
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

async fn run_wal_archiver_loop(data_dir: PathBuf, archive_command: String, interval_ms: u64) {
    let interval_ms = interval_ms.max(1);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
    loop {
        ticker.tick().await;
        match archive_wal_once(&data_dir, &archive_command) {
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

fn archive_wal_once(data_dir: &Path, archive_command: &str) -> Result<usize, String> {
    let wal_dir = data_dir.join("pg_wal");
    let status_dir = wal_dir.join("archive_status");
    fs::create_dir_all(&status_dir).map_err(|e| format!("create archive_status: {e}"))?;

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
        let status = run_archive_shell_command(&rendered)
            .map_err(|e| format!("archive command spawn failed for {name}: {e}"))?;
        if !status.success() {
            let failed = status_dir.join(format!("{name}.failed"));
            write_status_marker(&failed, rendered.as_bytes())?;
            return Err(format!(
                "archive command failed for {name} with status {status}"
            ));
        }
        write_status_marker(&done, rendered.as_bytes())?;
        archived = archived.saturating_add(1);
    }
    Ok(archived)
}

fn restore_wal_once(
    data_dir: &Path,
    restore_command: &str,
    max_segments: u32,
) -> Result<usize, String> {
    if restore_command.trim().is_empty() || max_segments == 0 {
        return Ok(0);
    }

    let wal_dir = data_dir.join("pg_wal");
    let status_dir = wal_dir.join("restore_status");
    fs::create_dir_all(&status_dir).map_err(|e| format!("create restore_status: {e}"))?;

    let mut restored = 0_usize;
    for index in 0..max_segments {
        let name = wal_segment_filename(index);
        let path = wal_dir.join(&name);
        if wal_file_exists(&path, &name)? {
            continue;
        }

        let rendered = render_restore_command(restore_command, &path, &name);
        let status = run_restore_shell_command(&rendered)
            .map_err(|e| format!("restore command spawn failed for {name}: {e}"))?;
        if !status.success() {
            let missing = status_dir.join(format!("{name}.missing"));
            write_status_marker(&missing, rendered.as_bytes())?;
            break;
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
    fs::write(path, bytes).map_err(|e| format!("write {}: {e}", path.display()))
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

fn run_archive_shell_command(command: &str) -> std::io::Result<std::process::ExitStatus> {
    run_shell_command(command)
}

fn run_restore_shell_command(command: &str) -> std::io::Result<std::process::ExitStatus> {
    run_shell_command(command)
}

fn run_shell_command(command: &str) -> std::io::Result<std::process::ExitStatus> {
    #[cfg(windows)]
    {
        Command::new("cmd").args(["/C", command]).status()
    }
    #[cfg(not(windows))]
    {
        Command::new("sh").args(["-c", command]).status()
    }
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
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(handle_ops_request(stream, pg_addr, Arc::clone(&state)));
    }
}

async fn handle_ops_request(mut stream: TcpStream, pg_addr: SocketAddr, state: Arc<Server>) {
    let mut buf = [0_u8; 1024];
    let n = match stream.read(&mut buf).await {
        Ok(n) => n,
        Err(_) => return,
    };
    let req = String::from_utf8_lossy(&buf[..n]);
    let path = req
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

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
        "/backup/start" => match start_backup_fence(&state) {
            Ok(body) => ("200 OK", "application/json", body),
            Err(body) => ("500 Internal Server Error", "application/json", body),
        },
        "/backup/stop" => ("200 OK", "application/json", stop_backup_fence(&state)),
        _ => (
            "404 Not Found",
            "application/json",
            "{\"error\":\"not found\"}\n".to_string(),
        ),
    };

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

        let backup_start = request_ops_path("/backup/start", missing_pg, Arc::clone(&state)).await;
        assert!(backup_start.starts_with("HTTP/1.1 200 OK"));
        assert!(backup_start.contains("\"backup_started\""));
        assert!(state.is_standby_mode());

        let backup_stop = request_ops_path("/backup/stop", missing_pg, Arc::clone(&state)).await;
        assert!(backup_stop.starts_with("HTTP/1.1 200 OK"));
        assert!(backup_stop.contains("\"backup_stopped\""));
        assert!(!state.is_standby_mode());

        let not_found = request_ops_path("/nope", missing_pg, state).await;
        assert!(not_found.starts_with("HTTP/1.1 404 Not Found"));
        assert!(not_found.contains("\"error\":\"not found\""));
    }

    async fn request_ops_path(path: &str, pg_addr: SocketAddr, state: Arc<Server>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ops probe");
        let addr = listener.local_addr().expect("ops addr");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept ops probe");
            handle_ops_request(stream, pg_addr, state).await;
        });

        let mut client = TcpStream::connect(addr).await.expect("connect ops probe");
        client
            .write_all(format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n").as_bytes())
            .await
            .expect("write request");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read response");
        server.await.expect("ops task");
        String::from_utf8(response).expect("utf8 response")
    }

    #[test]
    fn autovacuum_config_from_cli_converts_scale_factors() {
        let cli = Cli {
            listen: "127.0.0.1:5433".parse().expect("listen addr"),
            data_dir: None,
            ops_listen: None,
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
            data_dir: None,
            ops_listen: None,
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
        };

        assert!(autovacuum_config_from_cli(&cli).is_err());
    }

    #[test]
    fn logging_config_from_cli_rejects_invalid_duration() {
        let cli = Cli {
            listen: "127.0.0.1:5433".parse().expect("listen addr"),
            data_dir: None,
            ops_listen: None,
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
        };

        assert!(logging_config_from_cli(&cli).is_err());
    }

    #[test]
    fn logging_config_from_cli_accepts_duration_and_statement_mode() {
        let cli = Cli {
            listen: "127.0.0.1:5433".parse().expect("listen addr"),
            data_dir: None,
            ops_listen: None,
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
        };

        let config = logging_config_from_cli(&cli).expect("valid logging config");

        assert!(config.log_connections);
        assert_eq!(config.log_min_duration_statement_ms, 25);
        assert_eq!(config.log_statement, LogStatementMode::All);
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
        assert!(err.contains("create archive_status"));

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

    #[cfg(not(windows))]
    fn sh_single_quoted_path(path: &Path) -> String {
        format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
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
