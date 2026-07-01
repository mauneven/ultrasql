//! HTTP operations endpoint for `ultrasqld`.
//!
//! Serves the minimal control surface used by orchestrators and
//! backups: `/health`, `/ready`, Prometheus `/metrics`, and the
//! token-fenced `POST /backup/start` / `POST /backup/stop` routes that
//! flip the server into read-only standby. Includes the bounded
//! request-head reader, constant-time bearer-token check, and the
//! metrics-body renderer.

// Panic hardening: production (non-test) server-binary code must not
// `.unwrap()`, `.expect()`, or `panic!`. Fallible sites propagate errors;
// proven invariants carry a per-site `#[allow]` with an `// INVARIANT:`
// justification.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use ultrasql_server::Server;

const OPS_REQUEST_HEAD_LIMIT_BYTES: usize = 8 * 1024;
const OPS_REQUEST_HEAD_HARD_LIMIT_BYTES: usize = 64 * 1024;
const OPS_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) fn start_backup_fence(state: &Server) -> Result<String, String> {
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

pub(crate) fn stop_backup_fence(state: &Server) -> String {
    state.set_standby_mode(false);
    "{\"status\":\"backup_stopped\",\"read_only\":false}\n".to_string()
}

pub(crate) async fn run_ops_endpoint(
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

pub(crate) async fn handle_ops_request(
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

pub(crate) fn constant_time_eq(expected: &[u8], supplied: &[u8]) -> bool {
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

pub(crate) fn metrics_body(state: &Server) -> String {
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
        "# HELP ultrasql_connections_active Currently-open client connections.\n\
         # TYPE ultrasql_connections_active gauge\n",
    );
    push_metric(
        &mut body,
        "ultrasql_connections_active",
        usize_to_u64_saturated(state.workload_recorder.active_session_count()),
    );
    body.push_str(
        "# HELP ultrasql_transactions_committed_total Top-level transactions committed.\n\
         # TYPE ultrasql_transactions_committed_total counter\n",
    );
    push_metric(
        &mut body,
        "ultrasql_transactions_committed_total",
        state.txn_manager.xact_commit_total(),
    );
    body.push_str(
        "# HELP ultrasql_transactions_rolled_back_total Top-level transactions rolled back.\n\
         # TYPE ultrasql_transactions_rolled_back_total counter\n",
    );
    push_metric(
        &mut body,
        "ultrasql_transactions_rolled_back_total",
        state.txn_manager.xact_rollback_total(),
    );
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

    // WAL position gauges (LSN). `flushed_lsn` is the local writer's fsync point
    // (0 for in-memory sample servers); `standby_apply_lsn` is the hot-standby
    // replay cursor (seeded at recovery, advanced by `apply_landed_wal`).
    // Replication lag is derived externally (primary flushed_lsn - standby
    // apply_lsn) until continuous streaming apply is wired — see the design doc.
    body.push_str(
        "# HELP ultrasql_wal_flushed_lsn Last WAL LSN fsynced by the writer (0 if none).\n\
         # TYPE ultrasql_wal_flushed_lsn gauge\n",
    );
    push_metric(
        &mut body,
        "ultrasql_wal_flushed_lsn",
        state.runtime_wal_flushed_lsn().map_or(0, |lsn| lsn.raw()),
    );
    body.push_str(
        "# HELP ultrasql_standby_apply_lsn Hot-standby WAL-apply cursor LSN (next unapplied).\n\
         # TYPE ultrasql_standby_apply_lsn gauge\n",
    );
    push_metric(
        &mut body,
        "ultrasql_standby_apply_lsn",
        state.standby_apply_cursor_lsn().raw(),
    );
    body.push_str(
        "# HELP ultrasql_standby_mode 1 when this node is a read-only hot standby.\n\
         # TYPE ultrasql_standby_mode gauge\n",
    );
    push_metric(
        &mut body,
        "ultrasql_standby_mode",
        u64::from(state.is_standby_mode()),
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

pub(crate) fn push_metric(body: &mut String, name: &str, value: u64) {
    body.push_str(&format!("{name} {value}\n"));
}

pub(crate) fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(crate) fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
