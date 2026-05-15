//! Session-side dispatch for `LISTEN` / `NOTIFY` / `UNLISTEN`.
//!
//! The execute-side handlers in this file translate the bound
//! [`LogicalPlan::Listen`] / [`LogicalPlan::Notify`] /
//! [`LogicalPlan::Unlisten`] variants into calls against the global
//! [`crate::notify::NotifyHub`]. The hub fans `NOTIFY` deliveries into
//! each subscribed session's per-pid `UnboundedSender`; the read-side
//! loop in [`super::run`] drains the receiver between `Sync` boundaries
//! and emits a wire-level `NotificationResponse` for each pending
//! record.
//!
//! All three statements run *outside* the transaction system on
//! purpose: PostgreSQL queues `NOTIFY` until commit when issued inside a
//! transaction block, but the v0.9 surface here keeps semantics simple
//! and delivers immediately. The session continues to honour the
//! caller's [`TxnState`] for the trailing `ReadyForQuery` byte.

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use ultrasql_planner::LogicalPlan;
use ultrasql_protocol::{BackendMessage, FrontendMessage, decode_frontend, encode_backend};

use super::Session;
use crate::error::ServerError;
use crate::notify::NotificationRecord;
use crate::result_encoder::SelectResult;

/// Outcome of [`Session::read_frontend_or_notify`].
///
/// Either a fully-decoded frontend message, an EOF on the socket, or a
/// pending notification that should be flushed immediately. The
/// caller's run loop branches on the variant.
pub(crate) enum ReadOrNotify {
    /// A complete frontend message was decoded from the wire.
    Frontend(FrontendMessage),
    /// The peer closed the TCP connection.
    Eof,
    /// The notification hub delivered a record to this session.
    Notification(NotificationRecord),
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    /// Dispatch a `LISTEN` / `NOTIFY` / `UNLISTEN` plan against the
    /// session's shared notification hub.
    ///
    /// Returns the [`SelectResult`] the caller forwards to the client;
    /// no rows are produced — only a `CommandComplete` tag matching the
    /// PostgreSQL command name.
    pub(crate) fn execute_pubsub(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        match plan {
            LogicalPlan::Listen { channel, .. } => {
                self.state.notify_hub.listen(self.pid, channel);
                Ok(simple_tag("LISTEN"))
            }
            LogicalPlan::Notify {
                channel, payload, ..
            } => {
                // PostgreSQL maps an omitted payload to the empty
                // string on the wire; mirror that here so listeners see
                // identical behaviour.
                let payload = payload.as_deref().unwrap_or("");
                self.state.notify_hub.notify(self.pid, channel, payload);
                Ok(simple_tag("NOTIFY"))
            }
            LogicalPlan::Unlisten { channel, .. } => {
                // `None` is the `UNLISTEN *` form — the hub interprets
                // the `"*"` sentinel as "drop every subscription owned
                // by this pid".
                let target = channel.as_deref().unwrap_or("*");
                self.state.notify_hub.unlisten(self.pid, target);
                Ok(simple_tag("UNLISTEN"))
            }
            _ => Err(ServerError::Unsupported(
                "execute_pubsub called with non-pubsub plan",
            )),
        }
    }

    /// Race the connection's socket read against the notification
    /// receiver and return whichever resolves first.
    ///
    /// Used by the main run loop to deliver `NotificationResponse`
    /// messages while the session is idle waiting for the next frontend
    /// message — PostgreSQL clients (`libpq`, `tokio-postgres`,
    /// `psycopg2`, …) all expect notifications to arrive without
    /// requiring an additional `Sync` round trip.
    ///
    /// Both halves are cancel-safe: the socket reader's progress lives
    /// inside `self.read_buf` and survives a dropped poll; the
    /// notification receiver `mpsc::UnboundedReceiver::recv` only
    /// advances on `Some(_)`.
    pub(crate) async fn read_frontend_or_notify(&mut self) -> Result<ReadOrNotify, ServerError> {
        loop {
            // Drain the wire buffer first. A previous socket read may
            // already have placed a fully-decoded message in
            // `self.read_buf` (e.g. when the client sent a Sync + the
            // next Query together) — emitting that ahead of anything
            // else keeps the protocol pipeline ordered.
            if let Some(msg) = decode_frontend(&mut self.read_buf)? {
                return Ok(ReadOrNotify::Frontend(msg));
            }
            // Borrow the fields disjointly so `tokio::select!` can race
            // them — `io` and `notify_rx` live in independent slots of
            // the `Session` struct, so a manual split-borrow is sound.
            // `read_buf` is consumed by the next iteration's
            // `decode_frontend` once bytes arrive.
            let io = &mut self.io;
            let notify_rx = &mut self.notify_rx;
            let read_buf = &mut self.read_buf;
            let outcome = tokio::select! {
                biased;
                maybe = notify_rx.recv() => {
                    match maybe {
                        Some(record) => ReadOrNotify::Notification(record),
                        // The hub deregistered this session's sender
                        // somehow (deregister + race); treat as a no-op
                        // and continue polling the socket on the next
                        // loop iteration via a synthetic retry.
                        None => continue,
                    }
                }
                read = io.read_buf(read_buf) => {
                    let n = read.map_err(ServerError::Io)?;
                    if n == 0 {
                        return Ok(ReadOrNotify::Eof);
                    }
                    // Bytes arrived but we don't yet know if a full
                    // message is buffered — restart the loop so the
                    // top-of-loop `decode_frontend` decides.
                    continue;
                }
            };
            return Ok(outcome);
        }
    }

    /// Drain every queued [`NotificationRecord`] off the session's
    /// receiver and encode each as a `NotificationResponse` into `buf`.
    ///
    /// Non-blocking: only records the hub has already delivered are
    /// drained. Called immediately before each `ReadyForQuery` so the
    /// client sees the standard PostgreSQL ordering — pending
    /// `NotificationResponse`s, then `ReadyForQuery`.
    pub(crate) fn drain_pending_notifications_into(&mut self, buf: &mut BytesMut) {
        while let Ok(record) = self.notify_rx.try_recv() {
            // Encode `process_id` as `i32` per the wire spec. The hub's
            // sender pid is `u32`; cast losslessly via `i32::from_le_bytes`
            // / `to_le_bytes` to honour AGENTS.md §3.3 ("no `as` casts").
            let process_id = i32::from_le_bytes(record.notifier_pid.to_le_bytes());
            encode_backend(
                &BackendMessage::NotificationResponse {
                    process_id,
                    channel: record.channel,
                    payload: record.payload,
                },
                buf,
            );
        }
    }
}

/// Build a `SelectResult` carrying a single `CommandComplete` tag and
/// no row data — the canonical reply for `LISTEN` / `NOTIFY` /
/// `UNLISTEN`.
fn simple_tag(tag: &str) -> SelectResult {
    SelectResult {
        messages: vec![BackendMessage::CommandComplete {
            tag: tag.to_string(),
        }],
        streamed_body: None,
        rows: 0,
    }
}
