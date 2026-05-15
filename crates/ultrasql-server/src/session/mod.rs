//! Per-connection [`Session`] state machine + impl pieces.
//!
//! The implementation is intentionally fragmented across several
//! files in this directory so no single unit exceeds the 600-line
//! ceiling. `mod.rs` carries the struct definition and the smallest
//! constructor; every other method lives in a sibling file that
//! reopens the same `impl<RW> Session<RW>` block.

#![allow(unused_imports)]

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::extended::ExtendedConnState;
use crate::notify::NotificationRecord;
use crate::{READ_BUFFER_INITIAL, Server, TxnState};

mod alter;
mod copy;
mod ddl;
mod execute;
mod explain;
mod ext;
mod io;
mod meta_stmt;
mod notify;
mod run;
mod startup;
mod txn;

pub(crate) struct Session<RW> {
    pub(super) io: RW,
    pub(super) read_buf: BytesMut,
    pub(super) write_buf: BytesMut,
    pub(super) state: Arc<Server>,
    pub(super) extended: ExtendedConnState,
    pub(super) txn_state: TxnState,
    /// Per-connection process id allocated at session construction.
    ///
    /// Used as the `pid` field in `BackendKeyData` and as the routing
    /// key into [`crate::notify::NotifyHub`]. Stable for the lifetime
    /// of the session.
    pub(super) pid: u32,
    /// Receiver half of the per-connection notification channel.
    ///
    /// `LISTEN` registers the session under [`Self::pid`] and the hub
    /// fans `NOTIFY` payloads in here. The read-side wire loop drains
    /// the channel between `Sync` boundaries and writes each pending
    /// `NotificationResponse` before the trailing `ReadyForQuery`.
    pub(super) notify_rx: mpsc::UnboundedReceiver<NotificationRecord>,
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(io: RW, state: Arc<Server>) -> Self {
        let pid = state.allocate_pid();
        // Register this session with the hub up front so a `NOTIFY`
        // racing against the `LISTEN` on this connection always finds a
        // live sender. Sending happens regardless of whether anyone is
        // listening; the receiver just buffers.
        let notify_rx = state.notify_hub.register_connection(pid);
        Self {
            io,
            read_buf: BytesMut::with_capacity(READ_BUFFER_INITIAL),
            write_buf: BytesMut::with_capacity(READ_BUFFER_INITIAL),
            state,
            extended: crate::extended::ExtendedConnState::new(),
            txn_state: TxnState::Idle,
            pid,
            notify_rx,
        }
    }
}

impl<RW> Drop for Session<RW> {
    /// Deregister the connection from the notification hub on drop so
    /// the per-pid sender is released and any orphaned subscriptions
    /// are removed.
    fn drop(&mut self) {
        self.state.notify_hub.deregister_connection(self.pid);
    }
}
