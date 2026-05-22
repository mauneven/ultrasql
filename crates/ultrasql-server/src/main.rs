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

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use ultrasql_server::{AutovacuumConfig, LogStatementMode, LoggingConfig, Server, run_server};

/// `ultrasqld` v0.5: PostgreSQL-wire-compatible server with an
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

    /// Optional HTTP operations endpoint for `/health`, `/ready`, and `/metrics`.
    #[arg(long, env = "ULTRASQL_OPS_LISTEN")]
    ops_listen: Option<SocketAddr>,

    /// Tracing level filter, e.g. `info`, `debug`, `ultrasqld=trace`.
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Log output format.
    #[arg(long, value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,

    /// Minimum statement duration to log in milliseconds; -1 disables.
    #[arg(long, default_value_t = -1)]
    log_min_duration_statement_ms: i64,

    /// Statement classes logged regardless of duration.
    #[arg(long, value_enum, default_value_t = CliLogStatementMode::None)]
    log_statement: CliLogStatementMode,

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

Connect with any libpq-compatible client. Example session:

    psql -h 127.0.0.1 -p 5433 -d ultrasql -c 'SELECT id FROM users;'

Supported query shapes in v0.5:
  - SELECT col [, col]* FROM users
  - SELECT col FROM users WHERE int_col = literal
  - ... LIMIT n

Production-oriented v0.9 flags:
  - --data-dir DIR      boot WAL-backed storage
  - --ops-listen ADDR   serve /health, /ready, /metrics
  - --log-format json   emit structured logs
  - --log-min-duration-statement-ms N
  - --log-statement none|ddl|mod|all
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
        Some(path) => match Server::init(path) {
            Ok(mut server) => {
                server.set_autovacuum_config(autovacuum_config);
                server.set_logging_config(logging_config);
                Arc::new(server)
            }
            Err(e) => {
                error!(target: "ultrasqld", error = %e, data_dir = %path.display(), "server init failed");
                return std::process::ExitCode::from(1);
            }
        },
        None => {
            let mut server = Server::with_sample_database();
            server.set_autovacuum_config(autovacuum_config);
            server.set_logging_config(logging_config);
            Arc::new(server)
        }
    };
    if let Some(path) = &cli.data_dir {
        if path.join("standby.signal").exists() || path.join("recovery.signal").exists() {
            state.set_standby_mode(true);
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

fn logging_config_from_cli(cli: &Cli) -> Result<LoggingConfig, String> {
    if cli.log_min_duration_statement_ms < -1 {
        return Err("log_min_duration_statement_ms must be -1 or greater".to_string());
    }
    Ok(LoggingConfig {
        log_min_duration_statement_ms: cli.log_min_duration_statement_ms,
        log_statement: cli.log_statement.into(),
    })
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

    #[test]
    fn autovacuum_config_from_cli_converts_scale_factors() {
        let cli = Cli {
            listen: "127.0.0.1:5433".parse().expect("listen addr"),
            data_dir: None,
            ops_listen: None,
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
            log_min_duration_statement_ms: -1,
            log_statement: CliLogStatementMode::None,
            autovacuum_interval_ms: 1000,
            autovacuum_vacuum_threshold: 7,
            autovacuum_vacuum_scale_factor: 0.25,
            autovacuum_analyze_threshold: 11,
            autovacuum_analyze_scale_factor: 0.125,
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
            log_min_duration_statement_ms: -1,
            log_statement: CliLogStatementMode::None,
            autovacuum_interval_ms: 1000,
            autovacuum_vacuum_threshold: 50,
            autovacuum_vacuum_scale_factor: f64::NAN,
            autovacuum_analyze_threshold: 50,
            autovacuum_analyze_scale_factor: 0.1,
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
            log_min_duration_statement_ms: -2,
            log_statement: CliLogStatementMode::Mod,
            autovacuum_interval_ms: 1000,
            autovacuum_vacuum_threshold: 50,
            autovacuum_vacuum_scale_factor: 0.2,
            autovacuum_analyze_threshold: 50,
            autovacuum_analyze_scale_factor: 0.1,
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
            log_min_duration_statement_ms: 25,
            log_statement: CliLogStatementMode::All,
            autovacuum_interval_ms: 1000,
            autovacuum_vacuum_threshold: 50,
            autovacuum_vacuum_scale_factor: 0.2,
            autovacuum_analyze_threshold: 50,
            autovacuum_analyze_scale_factor: 0.1,
        };

        let config = logging_config_from_cli(&cli).expect("valid logging config");

        assert_eq!(config.log_min_duration_statement_ms, 25);
        assert_eq!(config.log_statement, LogStatementMode::All);
    }
}
