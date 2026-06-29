//! Tests for the HTTP ops endpoint ([`crate::ops`]).

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use ultrasql_server::Server;

use crate::ops::{
    constant_time_eq, handle_ops_request, json_escape, metrics_body, push_metric,
    start_backup_fence, stop_backup_fence, usize_to_u64_saturated,
};

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
    // Core DB-health gauge: active client connections (none in this probe).
    assert!(metrics.contains("ultrasql_connections_active 0"));

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
fn ops_constant_time_eq_rejects_wrong_length_tokens() {
    let expected = b"0123456789abcdef";

    assert!(constant_time_eq(expected, b"0123456789abcdef"));
    assert!(!constant_time_eq(expected, b"fedcba9876543210"));
    assert!(!constant_time_eq(expected, b"0123456789abcde"));
    assert!(!constant_time_eq(expected, b"0123456789abcdef0"));
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
