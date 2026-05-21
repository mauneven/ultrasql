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
use ultrasql_server::{Server, run_server};

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

    /// Background autovacuum/analyze maintenance interval in milliseconds.
    #[arg(long, default_value_t = 1000)]
    autovacuum_interval_ms: u64,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LogFormat {
    Text,
    Json,
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
";

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    if let Err(e) = init_tracing(&cli.log_level, cli.log_format) {
        eprintln!("ultrasqld: failed to initialise tracing: {e}");
        return std::process::ExitCode::from(1);
    }

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
            Ok(server) => Arc::new(server),
            Err(e) => {
                error!(target: "ultrasqld", error = %e, data_dir = %path.display(), "server init failed");
                return std::process::ExitCode::from(1);
            }
        },
        None => Arc::new(Server::with_sample_database()),
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
}
