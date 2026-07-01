//! Streaming-replication walreceiver client — the standby side (Phase 2b).
//!
//! [`WalReceiverClient`] connects to a primary as a libpq replication client,
//! completes the startup handshake, drives `START_REPLICATION`, and lands the
//! streamed WAL into local segment files via the Phase 2a landing primitive
//! [`ultrasql_wal::receiver::WalReceiver`]. It is the networked counterpart of
//! the walsender (`session/replication.rs`) and is distinct from the offline,
//! file-copy [`crate::replication::WalReceiver`].
//!
//! Beyond the receive + durable-land path (gated by a two-node
//! byte-identical test), this module now carries the continuous standby loop
//! ([`run_standby_walreceiver`]): resume the locally-landed WAL, stream from
//! the primary named by [`PrimaryConnInfo`], and apply every landed frame
//! into the standby's heap/commit-status so read-only sessions stay current
//! — gated by a two-node live-apply round trip. `ultrasqld` launches it in
//! standby mode when a `primary_conninfo` is configured. See
//! `docs/streaming-replication-design.md`.

use std::net::SocketAddr;
use std::path::PathBuf;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use ultrasql_core::Lsn;
use ultrasql_protocol::{
    BackendMessage, FrontendMessage, ProtocolError, decode_backend, encode_frontend,
};
use ultrasql_wal::receiver::WalReceiver;
use ultrasql_wal::writer::WalWriterError;

use crate::replication::format_pg_lsn;

/// Errors from the streaming walreceiver client.
#[derive(Debug, thiserror::Error)]
pub enum WalReceiverError {
    /// Socket I/O failure talking to the primary.
    #[error("walreceiver io error: {0}")]
    Io(#[from] std::io::Error),
    /// A backend message could not be decoded.
    #[error("walreceiver protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    /// Landing received WAL into local segments failed.
    #[error("walreceiver landing error: {0}")]
    Landing(#[from] WalWriterError),
    /// The primary returned an `ErrorResponse`.
    #[error("primary returned an error: {0}")]
    Primary(String),
    /// The primary sent a message the receiver did not expect at this point.
    #[error("unexpected message from primary: {0}")]
    Unexpected(String),
    /// The primary closed the connection before the exchange completed.
    #[error("primary closed the connection unexpectedly")]
    UnexpectedEof,
}

/// Options for a physical-replication stream.
#[derive(Debug, Clone)]
pub struct StandbyStreamOptions {
    /// Physical slot to stream from, or `None` for a slot-less stream.
    pub slot: Option<String>,
    /// LSN to begin streaming from (the standby's current receive position).
    pub start_lsn: Lsn,
    /// Local directory to land received WAL into (the standby's `pg_wal`).
    pub wal_dir: PathBuf,
    /// Segment size — MUST match the primary's, or landed segments will not be
    /// byte-identical.
    pub segment_size_bytes: u64,
}

/// A libpq-wire client that streams physical WAL from a primary and lands it.
#[derive(Debug)]
pub struct WalReceiverClient {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl WalReceiverClient {
    /// Connect to `addr` and complete the replication-mode startup handshake as
    /// `user`. Assumes the primary's auth admits this user (Trust, or a
    /// pre-shared credential the caller has configured out of band).
    ///
    /// # Errors
    /// [`WalReceiverError::Io`] on connect failure, [`WalReceiverError::Primary`]
    /// if startup is rejected.
    pub async fn connect(addr: SocketAddr, user: &str) -> Result<Self, WalReceiverError> {
        let stream = TcpStream::connect(addr).await?;
        let mut client = Self {
            stream,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
        };
        client
            .send(&FrontendMessage::StartupMessage {
                protocol_major: 3,
                protocol_minor: 0,
                params: vec![
                    ("user".to_owned(), user.to_owned()),
                    ("replication".to_owned(), "true".to_owned()),
                ],
            })
            .await?;
        // Drain auth + ParameterStatus + BackendKeyData up to the first ReadyForQuery.
        loop {
            match client.read_message().await? {
                BackendMessage::ReadyForQuery { .. } => break,
                BackendMessage::ErrorResponse { fields } => {
                    return Err(WalReceiverError::Primary(error_text(&fields)));
                }
                _ => {}
            }
        }
        Ok(client)
    }

    /// Run a simple replication command that returns a normal result followed by
    /// `ReadyForQuery` (e.g. `CREATE_REPLICATION_SLOT … PHYSICAL`,
    /// `DROP_REPLICATION_SLOT`, `IDENTIFY_SYSTEM`), discarding any result rows.
    ///
    /// # Errors
    /// [`WalReceiverError::Primary`] if the primary reports an error.
    pub async fn run_command(&mut self, sql: &str) -> Result<(), WalReceiverError> {
        self.send(&FrontendMessage::Query {
            sql: sql.to_owned(),
        })
        .await?;
        loop {
            match self.read_message().await? {
                BackendMessage::ReadyForQuery { .. } => return Ok(()),
                BackendMessage::ErrorResponse { fields } => {
                    return Err(WalReceiverError::Primary(error_text(&fields)));
                }
                _ => {}
            }
        }
    }

    /// Stream physical WAL per `opts`, landing it durably, until
    /// `should_stop(&receiver)` returns `true` or the primary ends the stream.
    /// Returns the [`WalReceiver`] (carrying the standby write/flush positions).
    ///
    /// `should_stop` is evaluated before each frame is read, so a caller can
    /// stop once caught up (e.g. `|r| r.received_lsn() >= target`).
    ///
    /// # Errors
    /// Propagates protocol, I/O, and landing errors; [`WalReceiverError::Primary`]
    /// if the primary reports an error mid-stream.
    pub async fn stream_into<F>(
        &mut self,
        opts: &StandbyStreamOptions,
        mut should_stop: F,
    ) -> Result<WalReceiver, WalReceiverError>
    where
        F: FnMut(&WalReceiver) -> bool,
    {
        let receiver = WalReceiver::create(&opts.wal_dir, opts.segment_size_bytes)?;
        self.stream_with(receiver, opts, |r| Ok(should_stop(r)))
            .await
    }

    /// Like [`Self::stream_into`], but takes an already-positioned
    /// [`WalReceiver`] (e.g. one built with `WalReceiver::resume` over WAL
    /// carried in by a base backup) and a fallible `progress` hook.
    ///
    /// `progress` runs before each frame is read with mutable access to the
    /// receiver, so a continuous standby can flush and apply landed WAL
    /// inline; returning `Ok(true)` ends the stream cleanly, and an error
    /// aborts it.
    ///
    /// # Errors
    /// Propagates protocol, I/O, landing, and `progress` errors;
    /// [`WalReceiverError::Primary`] if the primary reports an error.
    pub async fn stream_with<F>(
        &mut self,
        mut receiver: WalReceiver,
        opts: &StandbyStreamOptions,
        mut progress: F,
    ) -> Result<WalReceiver, WalReceiverError>
    where
        F: FnMut(&mut WalReceiver) -> Result<bool, WalReceiverError>,
    {
        // Quote the slot name as a PostgreSQL identifier (doubling any internal
        // quotes) so it cannot alter the command, regardless of how the name was
        // sourced; the primary's parser unquotes it.
        let slot_clause = opts.slot.as_ref().map_or_else(String::new, |name| {
            format!("SLOT \"{}\" ", name.replace('"', "\"\""))
        });
        let sql = format!(
            "START_REPLICATION {slot_clause}PHYSICAL {}",
            format_pg_lsn(opts.start_lsn)
        );
        self.send(&FrontendMessage::Query { sql }).await?;

        match self.read_message().await? {
            BackendMessage::CopyBothResponse { .. } => {}
            BackendMessage::ErrorResponse { fields } => {
                return Err(WalReceiverError::Primary(error_text(&fields)));
            }
            other => return Err(WalReceiverError::Unexpected(format!("{other:?}"))),
        }

        loop {
            if progress(&mut receiver)? {
                receiver.flush()?;
                self.finish_stream(&receiver).await?;
                return Ok(receiver);
            }
            // Bounded read: the walsender keepalive cadence is seconds, so a
            // quiet stream would otherwise park this loop indefinitely and a
            // shutdown request (or a wedged primary) could never be observed.
            // `read_buf` persists across calls, so a timeout mid-frame simply
            // resumes accumulating the same frame on the next iteration.
            let message = match tokio::time::timeout(STREAM_READ_RECHECK, self.read_message()).await
            {
                Ok(result) => result?,
                Err(_elapsed) => continue,
            };
            match message {
                BackendMessage::CopyData(payload) => {
                    self.handle_copy_data(&mut receiver, &payload).await?;
                }
                BackendMessage::CopyDone => {
                    // The primary ended the stream first.
                    receiver.flush()?;
                    return Ok(receiver);
                }
                BackendMessage::ErrorResponse { fields } => {
                    return Err(WalReceiverError::Primary(error_text(&fields)));
                }
                // ParameterStatus / NoticeResponse etc. mid-stream: ignore.
                _ => {}
            }
        }
    }

    /// Process one CopyData frame: land an `XLogData` ('w') payload, or answer a
    /// keepalive ('k') that requests a reply.
    async fn handle_copy_data(
        &mut self,
        receiver: &mut WalReceiver,
        payload: &[u8],
    ) -> Result<(), WalReceiverError> {
        match payload.first() {
            // XLogData: 'w' + Int64 dataStart + Int64 walEnd + Int64 sendTime + WAL bytes.
            Some(b'w') => {
                if payload.len() < 25 {
                    return Err(WalReceiverError::Unexpected(
                        "short XLogData frame".to_owned(),
                    ));
                }
                // Bytes [1..9] are the Int64 dataStart; len >= 25 is checked above.
                let data_start = u64::from_be_bytes([
                    payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
                    payload[7], payload[8],
                ]);
                receiver.land(Lsn::new(data_start), &payload[25..])?;
            }
            // Keepalive ('k' + Int64 walEnd + Int64 sendTime + Byte replyRequested,
            // 18 bytes) with the reply-requested flag set: acknowledge our flush
            // position. The length guard rejects a truncated/forged 'k' frame.
            Some(b'k') if payload.len() >= 18 && payload.last() == Some(&1) => {
                receiver.flush()?;
                self.send_status(receiver).await?;
            }
            // Unknown stream message: ignore (forward-compatible).
            _ => {}
        }
        Ok(())
    }

    /// Send a standby status update ('r') reporting write / flush / apply LSNs.
    async fn send_status(&mut self, receiver: &WalReceiver) -> Result<(), WalReceiverError> {
        let write = receiver.written_lsn().raw();
        let flush = receiver.flushed_lsn().raw();
        // Until continuous apply (Phase 3), "applied" tracks the flush position.
        let apply = flush;
        let mut payload = Vec::with_capacity(34);
        payload.push(b'r');
        payload.extend_from_slice(&write.to_be_bytes());
        payload.extend_from_slice(&flush.to_be_bytes());
        payload.extend_from_slice(&apply.to_be_bytes());
        payload.extend_from_slice(&0_i64.to_be_bytes()); // clientTime (unused in Phase 2b)
        payload.push(0); // replyRequested = false
        self.send(&FrontendMessage::CopyData(payload)).await
    }

    /// End the stream cleanly: final status, CopyDone, then drain the primary's
    /// CopyDone + CommandComplete + ReadyForQuery.
    async fn finish_stream(&mut self, receiver: &WalReceiver) -> Result<(), WalReceiverError> {
        self.send_status(receiver).await?;
        self.send(&FrontendMessage::CopyDone).await?;
        loop {
            match self.read_message().await? {
                BackendMessage::ReadyForQuery { .. } => return Ok(()),
                BackendMessage::ErrorResponse { fields } => {
                    return Err(WalReceiverError::Primary(error_text(&fields)));
                }
                // Trailing CopyData / CopyDone / CommandComplete: drain.
                _ => {}
            }
        }
    }

    async fn send(&mut self, msg: &FrontendMessage) -> Result<(), WalReceiverError> {
        self.write_buf.clear();
        encode_frontend(msg, &mut self.write_buf);
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<BackendMessage, WalReceiverError> {
        loop {
            if let Some(msg) = decode_backend(&mut self.read_buf)? {
                return Ok(msg);
            }
            let mut chunk = [0_u8; 16 * 1024];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(WalReceiverError::UnexpectedEof);
            }
            self.read_buf.put_slice(&chunk[..n]);
        }
    }
}

/// Extract the human-readable message ('M') field from an `ErrorResponse`.
fn error_text(fields: &[(u8, String)]) -> String {
    fields
        .iter()
        .find(|(tag, _)| *tag == b'M')
        .map_or_else(|| "unknown error".to_owned(), |(_, value)| value.clone())
}

/// Connection settings a standby uses to reach its primary, parsed from a
/// `primary_conninfo` line (libpq-style `keyword=value` pairs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryConnInfo {
    /// Primary host (name or address).
    pub host: String,
    /// Primary PostgreSQL-wire port.
    pub port: u16,
    /// Replication user the primary's auth must admit.
    pub user: String,
    /// Optional physical replication slot to stream from. Without a slot the
    /// primary may recycle WAL the standby still needs across long outages.
    pub slot: Option<String>,
}

/// Parse a `primary_conninfo` line: space-separated `keyword=value` pairs.
///
/// Requires `host`, `port`, and `user`; accepts an optional `slot`. Unknown
/// keywords are rejected so a typo cannot silently drop a setting.
///
/// # Errors
/// Returns a human-readable description of the malformed input.
pub fn parse_primary_conninfo(text: &str) -> Result<PrimaryConnInfo, String> {
    let mut host = None;
    let mut port = None;
    let mut user = None;
    let mut slot = None;
    for pair in text.split_whitespace() {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| format!("primary_conninfo entry '{pair}' is not keyword=value"))?;
        if value.is_empty() {
            return Err(format!("primary_conninfo entry '{key}' has an empty value"));
        }
        match key {
            "host" => host = Some(value.to_owned()),
            "port" => {
                port = Some(value.parse::<u16>().map_err(|_| {
                    format!("primary_conninfo port '{value}' is not a valid TCP port")
                })?);
            }
            "user" => user = Some(value.to_owned()),
            "slot" => slot = Some(value.to_owned()),
            other => {
                return Err(format!(
                    "primary_conninfo keyword '{other}' is not supported \
                     (expected host, port, user, or slot)"
                ));
            }
        }
    }
    Ok(PrimaryConnInfo {
        host: host.ok_or("primary_conninfo is missing host=")?,
        port: port.ok_or("primary_conninfo is missing port=")?,
        user: user.ok_or("primary_conninfo is missing user=")?,
        slot,
    })
}

/// Reconnect backoff between standby streaming attempts.
const STANDBY_RECONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);

/// How long [`WalReceiverClient::stream_with`] waits for a frame before
/// re-running its `progress` hook (shutdown/apply recheck) on a quiet stream.
const STREAM_READ_RECHECK: std::time::Duration = std::time::Duration::from_secs(1);

/// Run the continuous standby walreceiver: connect to the primary, stream
/// physical WAL from the local landed end, and apply every landed frame into
/// this standby's heap/commit-status so read-only sessions observe the
/// primary's commits. Reconnects with a bounded backoff on any error and
/// returns only when `shutdown` is set.
///
/// Blocking by design: owns a dedicated current-thread Tokio runtime, so call
/// it from its own OS thread (`std::thread::spawn`), not from an async task.
pub fn run_standby_walreceiver(
    state: std::sync::Arc<crate::Server>,
    conninfo: &PrimaryConnInfo,
    wal_dir: PathBuf,
    segment_size_bytes: u64,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::atomic::Ordering;

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(target: "ultrasqld", error = %e, "standby walreceiver could not build a runtime");
            return;
        }
    };
    while !shutdown.load(Ordering::Acquire) {
        let outcome = runtime.block_on(stream_and_apply_once(
            &state,
            conninfo,
            &wal_dir,
            segment_size_bytes,
            &shutdown,
        ));
        match outcome {
            Ok(()) => {
                // Clean stop: shutdown requested or the primary ended the
                // stream; loop re-checks shutdown and reconnects otherwise.
            }
            Err(e) => {
                tracing::warn!(
                    target: "ultrasqld",
                    error = %e,
                    primary = %format!("{}:{}", conninfo.host, conninfo.port),
                    "standby walreceiver stream failed; will reconnect"
                );
            }
        }
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        std::thread::sleep(STANDBY_RECONNECT_BACKOFF);
    }
}

/// One streaming attempt: resume the local landed WAL, connect, stream, and
/// apply each landed frame as it arrives.
async fn stream_and_apply_once(
    state: &std::sync::Arc<crate::Server>,
    conninfo: &PrimaryConnInfo,
    wal_dir: &std::path::Path,
    segment_size_bytes: u64,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), WalReceiverError> {
    use std::net::ToSocketAddrs;
    use std::sync::atomic::Ordering;

    let receiver = WalReceiver::resume(wal_dir, segment_size_bytes)?;
    let start_lsn = receiver.flushed_lsn();
    let addr = format!("{}:{}", conninfo.host, conninfo.port)
        .to_socket_addrs()
        .map_err(WalReceiverError::Io)?
        .next()
        .ok_or_else(|| {
            WalReceiverError::Unexpected(format!(
                "primary address {}:{} did not resolve",
                conninfo.host, conninfo.port
            ))
        })?;
    let mut client = WalReceiverClient::connect(addr, &conninfo.user).await?;
    let opts = StandbyStreamOptions {
        slot: conninfo.slot.clone(),
        start_lsn,
        wal_dir: wal_dir.to_path_buf(),
        segment_size_bytes,
    };
    tracing::info!(
        target: "ultrasqld",
        primary = %format!("{}:{}", conninfo.host, conninfo.port),
        start_lsn = start_lsn.raw(),
        "standby walreceiver streaming"
    );

    let mut applied = Lsn::new(state.standby_apply_lsn_raw().max(start_lsn.raw()));
    let state_ref = std::sync::Arc::clone(state);
    let shutdown_ref = std::sync::Arc::clone(shutdown);
    client
        .stream_with(receiver, &opts, move |r| {
            if shutdown_ref.load(Ordering::Acquire) {
                return Ok(true);
            }
            // Apply everything newly landed: make it durable first (apply
            // reads the segment FILES), then replay into heap/commit status.
            if r.received_lsn().raw() > applied.raw() {
                r.flush()?;
                let flushed = r.flushed_lsn();
                applied = state_ref.apply_landed_wal(flushed).map_err(|e| {
                    WalReceiverError::Unexpected(format!("standby WAL apply failed: {e}"))
                })?;
            }
            Ok(false)
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_primary_conninfo_accepts_required_and_optional_keys() {
        let parsed = parse_primary_conninfo("host=10.0.0.5 port=5433 user=repl").expect("parses");
        assert_eq!(
            parsed,
            PrimaryConnInfo {
                host: "10.0.0.5".to_owned(),
                port: 5433,
                user: "repl".to_owned(),
                slot: None,
            }
        );

        let with_slot = parse_primary_conninfo("host=primary port=5432 user=repl slot=standby_1")
            .expect("parses");
        assert_eq!(with_slot.slot.as_deref(), Some("standby_1"));
    }

    #[test]
    fn parse_primary_conninfo_rejects_malformed_input() {
        // Missing required keys.
        assert!(parse_primary_conninfo("host=x port=5432").is_err());
        assert!(parse_primary_conninfo("port=5432 user=u").is_err());
        assert!(parse_primary_conninfo("host=x user=u").is_err());
        // Bad port.
        assert!(parse_primary_conninfo("host=x port=huge user=u").is_err());
        // Unknown keyword must not be silently dropped.
        assert!(parse_primary_conninfo("host=x port=1 user=u sslmode=disable").is_err());
        // Not keyword=value.
        assert!(parse_primary_conninfo("host").is_err());
        // Empty value.
        assert!(parse_primary_conninfo("host= port=1 user=u").is_err());
    }
}
