//! End-to-end VECTOR(n) type metadata tests.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage, types::Type};
use ultrasql_server::{Server, bind_listener, serve_listener};
use ultrasql_wal::{RecordType, WalRecord};

pub mod support;

use support::{shutdown as graceful_shutdown, start_persistent_server};

async fn start_server_and_connect() -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    start_server_and_connect_to(Arc::new(Server::with_sample_database())).await
}

async fn start_crash_persistent_server_and_connect(
    data_dir: &Path,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    start_server_and_connect_to(Arc::new(
        Server::init(data_dir).expect("persistent server init"),
    ))
    .await
}

async fn start_small_segment_crash_server_and_connect(
    data_dir: &Path,
    segment_size_bytes: u64,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    start_server_and_connect_to(Arc::new(
        Server::init_with_wal_segment_size(data_dir, segment_size_bytes)
            .expect("persistent server init"),
    ))
    .await
}

async fn start_server_and_connect_to(
    server: Arc<Server>,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server_handle = tokio::spawn(serve_listener(listener, server));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=vector_type_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    (client, conn_handle, server_handle)
}

async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
    let _ = server_handle.await;
}

fn simple_rows(messages: &[SimpleQueryMessage]) -> Vec<Vec<String>> {
    messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|idx| row.get(idx).unwrap_or("").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

fn sorted_wal_segments(data_dir: &Path) -> Vec<PathBuf> {
    let wal_dir = data_dir.join("pg_wal");
    let mut segments: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap_or_else(|e| panic!("read WAL dir {wal_dir:?}: {e}"))
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("segment_"))
        .map(|entry| entry.path())
        .collect();
    segments.sort();
    segments
}

fn truncate_wal_before_first(data_dir: &Path, record_type: RecordType) {
    let segments = sorted_wal_segments(data_dir);
    for (segment_idx, segment) in segments.iter().enumerate() {
        let bytes =
            fs::read(segment).unwrap_or_else(|e| panic!("read WAL segment {segment:?}: {e}"));
        let mut offset = 0;
        while offset < bytes.len() {
            let (record, used) = WalRecord::decode(&bytes[offset..])
                .unwrap_or_else(|e| panic!("decode WAL segment {segment:?} at {offset}: {e}"));
            if record.header.record_type == record_type {
                let keep_len = u64::try_from(offset).expect("WAL offset fits u64");
                if keep_len == 0 {
                    fs::remove_file(segment)
                        .unwrap_or_else(|e| panic!("remove WAL segment {segment:?}: {e}"));
                } else {
                    let file = fs::OpenOptions::new()
                        .write(true)
                        .open(segment)
                        .unwrap_or_else(|e| panic!("open WAL segment {segment:?}: {e}"));
                    file.set_len(keep_len)
                        .unwrap_or_else(|e| panic!("truncate WAL segment {segment:?}: {e}"));
                }
                for later in segments.iter().skip(segment_idx + 1) {
                    fs::remove_file(later)
                        .unwrap_or_else(|e| panic!("remove later WAL segment {later:?}: {e}"));
                }
                return;
            }
            offset += used;
        }
    }
    panic!("WAL record type {record_type:?} not found");
}

fn lsn_before_first_wal_record(data_dir: &Path, record_type: RecordType) -> u64 {
    let segments = sorted_wal_segments(data_dir);
    let mut stream_pos = 0_u64;
    for segment in &segments {
        let bytes =
            fs::read(segment).unwrap_or_else(|e| panic!("read WAL segment {segment:?}: {e}"));
        let mut offset = 0;
        while offset < bytes.len() {
            let (record, used) = WalRecord::decode(&bytes[offset..])
                .unwrap_or_else(|e| panic!("decode WAL segment {segment:?} at {offset}: {e}"));
            if record.header.record_type == record_type {
                return stream_pos + u64::try_from(offset).expect("WAL offset fits u64");
            }
            offset += used;
        }
        stream_pos = stream_pos
            .checked_add(u64::try_from(bytes.len()).expect("WAL segment length fits u64"))
            .expect("WAL stream position fits u64");
    }
    panic!("WAL record type {record_type:?} not found");
}

fn wal_end_lsn(data_dir: &Path) -> u64 {
    let segments = sorted_wal_segments(data_dir);
    let mut stream_pos = 0_u64;
    for segment in &segments {
        let bytes =
            fs::read(segment).unwrap_or_else(|e| panic!("read WAL segment {segment:?}: {e}"));
        let mut offset = 0;
        while offset < bytes.len() {
            let (_record, used) = WalRecord::decode(&bytes[offset..])
                .unwrap_or_else(|e| panic!("decode WAL segment {segment:?} at {offset}: {e}"));
            offset += used;
        }
        stream_pos = stream_pos
            .checked_add(u64::try_from(offset).expect("WAL segment offset fits u64"))
            .expect("WAL stream position fits u64");
    }
    stream_pos
}

fn truncate_inside_first_wal_record(data_dir: &Path, record_type: RecordType) {
    let segments = sorted_wal_segments(data_dir);
    for (segment_idx, segment) in segments.iter().enumerate() {
        let bytes =
            fs::read(segment).unwrap_or_else(|e| panic!("read WAL segment {segment:?}: {e}"));
        let mut offset = 0;
        while offset < bytes.len() {
            let (record, used) = WalRecord::decode(&bytes[offset..])
                .unwrap_or_else(|e| panic!("decode WAL segment {segment:?} at {offset}: {e}"));
            if record.header.record_type == record_type {
                assert!(used > 8, "ANN WAL record should be large enough to tear");
                let keep_len =
                    u64::try_from(offset + (used / 2)).expect("WAL torn offset fits u64");
                let file = fs::OpenOptions::new()
                    .write(true)
                    .open(segment)
                    .unwrap_or_else(|e| panic!("open WAL segment {segment:?}: {e}"));
                file.set_len(keep_len)
                    .unwrap_or_else(|e| panic!("tear WAL segment {segment:?}: {e}"));
                for later in segments.iter().skip(segment_idx + 1) {
                    fs::remove_file(later)
                        .unwrap_or_else(|e| panic!("remove later WAL segment {later:?}: {e}"));
                }
                return;
            }
            offset += used;
        }
    }
    panic!("WAL record type {record_type:?} not found");
}

fn corrupt_first_vector_wal_payload_after(
    data_dir: &Path,
    record_type: RecordType,
    min_record_start_lsn: u64,
) {
    let segments = sorted_wal_segments(data_dir);
    let mut stream_pos = 0_u64;
    for segment in &segments {
        let mut bytes =
            fs::read(segment).unwrap_or_else(|e| panic!("read WAL segment {segment:?}: {e}"));
        let mut offset = 0;
        while offset < bytes.len() {
            let record_start_lsn = stream_pos + u64::try_from(offset).expect("WAL offset fits u64");
            let (record, used) = WalRecord::decode(&bytes[offset..])
                .unwrap_or_else(|e| panic!("decode WAL segment {segment:?} at {offset}: {e}"));
            if record_start_lsn >= min_record_start_lsn && record.header.record_type == record_type
            {
                let mut payload = record.payload;
                assert!(
                    payload.len() > 1,
                    "vector WAL payload should include reserved prefix bytes"
                );
                payload[1] = 1;
                let rewritten = WalRecord::new(
                    record_type,
                    record.header.xid,
                    record.header.prev_lsn,
                    record.header.flags,
                    payload,
                )
                .expect("test WAL record should fit original size limits");
                assert_eq!(rewritten.header.total_length, record.header.total_length);
                let encoded = rewritten.encode();
                assert_eq!(encoded.len(), used);
                bytes[offset..offset + used].copy_from_slice(&encoded);
                fs::write(segment, bytes)
                    .unwrap_or_else(|e| panic!("rewrite WAL segment {segment:?}: {e}"));
                return;
            }
            offset += used;
        }
        stream_pos = stream_pos
            .checked_add(u64::try_from(bytes.len()).expect("WAL segment length fits u64"))
            .expect("WAL stream position fits u64");
    }
    panic!("WAL record type {record_type:?} not found after LSN {min_record_start_lsn}");
}

fn corrupt_first_vector_wal_payload(data_dir: &Path, record_type: RecordType) {
    let segments = sorted_wal_segments(data_dir);
    for segment in &segments {
        let mut bytes =
            fs::read(segment).unwrap_or_else(|e| panic!("read WAL segment {segment:?}: {e}"));
        let mut offset = 0;
        while offset < bytes.len() {
            let (record, used) = WalRecord::decode(&bytes[offset..])
                .unwrap_or_else(|e| panic!("decode WAL segment {segment:?} at {offset}: {e}"));
            if record.header.record_type == record_type {
                let mut payload = record.payload;
                assert!(
                    payload.len() > 1,
                    "vector WAL payload should include reserved prefix bytes"
                );
                payload[1] = 1;
                let rewritten = WalRecord::new(
                    record_type,
                    record.header.xid,
                    record.header.prev_lsn,
                    record.header.flags,
                    payload,
                )
                .expect("test WAL record should fit original size limits");
                assert_eq!(rewritten.header.total_length, record.header.total_length);
                let encoded = rewritten.encode();
                assert_eq!(encoded.len(), used);
                bytes[offset..offset + used].copy_from_slice(&encoded);
                fs::write(segment, bytes)
                    .unwrap_or_else(|e| panic!("rewrite WAL segment {segment:?}: {e}"));
                return;
            }
            offset += used;
        }
    }
    panic!("WAL record type {record_type:?} not found");
}

#[path = "vector_type_round_trip/ann_cert.rs"]
mod ann_cert;
#[path = "vector_type_round_trip/ann_recovery.rs"]
mod ann_recovery;
#[path = "vector_type_round_trip/basic.rs"]
mod basic;
#[path = "vector_type_round_trip/hnsw_index.rs"]
mod hnsw_index;
#[path = "vector_type_round_trip/hnsw_recovery.rs"]
mod hnsw_recovery;
#[path = "vector_type_round_trip/hybrid.rs"]
mod hybrid;
#[path = "vector_type_round_trip/ivfflat.rs"]
mod ivfflat;
#[path = "vector_type_round_trip/vector_search_sessions.rs"]
mod vector_search_sessions;
