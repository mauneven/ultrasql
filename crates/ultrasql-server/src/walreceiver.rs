//! Streaming-replication walreceiver client — the standby side (Phase 2b).
//!
//! [`WalReceiverClient`] connects to a primary as a libpq replication client,
//! completes the startup handshake, drives `START_REPLICATION`, and lands the
//! streamed WAL into local segment files via the Phase 2a landing primitive
//! [`ultrasql_wal::receiver::WalReceiver`]. It is the networked counterpart of
//! the walsender (`session/replication.rs`) and is distinct from the offline,
//! file-copy [`crate::replication::WalReceiver`].
//!
//! Phase 2b scope is the receive + durable-land path plus standby status
//! replies, gated by a two-node byte-identical test. Continuous online apply
//! (replaying landed WAL into the heap) is Phase 3; auto-launching this client
//! from `primary_conninfo` in standby mode is the 2b follow-up. See
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
        let mut receiver = WalReceiver::create(&opts.wal_dir, opts.segment_size_bytes)?;

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
            if should_stop(&receiver) {
                receiver.flush()?;
                self.finish_stream(&receiver).await?;
                return Ok(receiver);
            }
            match self.read_message().await? {
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
