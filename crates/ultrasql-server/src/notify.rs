//! LISTEN / NOTIFY pub-sub infrastructure.
//!
//! `LISTEN channel` registers the calling connection (identified by its
//! process ID) against a channel name in the global [`NotifyHub`]. When
//! `NOTIFY channel [, payload]` is executed, every registered listener
//! receives a `NotificationResponse` message on its next wire-loop
//! iteration via an async channel.
//!
//! ## Design
//!
//! The hub uses a `DashMap<String, HashSet<u32>>` to track which PIDs
//! listen on which channel. Each connection keeps a
//! `tokio::sync::mpsc::UnboundedSender<NotificationRecord>` keyed by its
//! PID in a second `DashMap`. Sending is O(1) per listener.
//!
//! This satisfies AGENTS.md §5: `DashMap` for shared state, and
//! `parking_lot::Mutex` is not needed here because the per-connection
//! channel handles cross-thread delivery without locking.

use std::collections::HashSet;

use dashmap::DashMap;
use tokio::sync::mpsc;

/// A pending notification record routed to a listener connection.
#[derive(Clone, Debug)]
pub struct NotificationRecord {
    /// PID of the session that sent `NOTIFY`.
    pub notifier_pid: u32,
    /// Channel name.
    pub channel: String,
    /// Optional payload (empty string if none was given).
    pub payload: String,
}

/// Global LISTEN / NOTIFY hub.
///
/// Shared across all connections via `Arc`. Thread-safe: `DashMap`
/// handles concurrent mutation from multiple connection tasks.
#[derive(Debug, Default)]
pub struct NotifyHub {
    /// channel → set of listening PIDs.
    subscriptions: DashMap<String, HashSet<u32>>,
    /// PID → sender half of the per-connection notification channel.
    senders: DashMap<u32, mpsc::UnboundedSender<NotificationRecord>>,
}

impl NotifyHub {
    /// Create an empty hub.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a connection's notification sender under `pid`.
    ///
    /// Must be called once when the connection is established, before any
    /// `LISTEN` calls. The returned `Receiver` is owned by the connection
    /// session loop.
    pub fn register_connection(
        &self,
        pid: u32,
    ) -> mpsc::UnboundedReceiver<NotificationRecord> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.senders.insert(pid, tx);
        rx
    }

    /// Deregister a connection when it terminates. Removes all channel
    /// subscriptions and the sender entry.
    pub fn deregister_connection(&self, pid: u32) {
        self.senders.remove(&pid);
        // Remove pid from every channel it was subscribed to.
        self.subscriptions.retain(|_, pids| {
            pids.remove(&pid);
            !pids.is_empty()
        });
    }

    /// Subscribe `pid` to `channel`.
    pub fn listen(&self, pid: u32, channel: &str) {
        self.subscriptions
            .entry(channel.to_string())
            .or_default()
            .insert(pid);
    }

    /// Unsubscribe `pid` from `channel` (or all channels if `channel` is `"*"`).
    pub fn unlisten(&self, pid: u32, channel: &str) {
        if channel == "*" {
            self.subscriptions.alter_all(|_, mut pids| {
                pids.remove(&pid);
                pids
            });
        } else if let Some(mut pids) = self.subscriptions.get_mut(channel) {
            pids.remove(&pid);
        }
    }

    /// Send `payload` on `channel` from `notifier_pid`. Delivers to every
    /// registered listener except the notifier itself if `skip_self` is true.
    ///
    /// Listeners whose channel is gone (disconnected) are silently skipped.
    pub fn notify(&self, notifier_pid: u32, channel: &str, payload: &str) {
        let Some(listeners) = self.subscriptions.get(channel) else {
            return;
        };
        let pids: Vec<u32> = listeners.iter().copied().collect();
        drop(listeners); // release the read lock before sending
        for pid in pids {
            if let Some(tx) = self.senders.get(&pid) {
                let record = NotificationRecord {
                    notifier_pid,
                    channel: channel.to_string(),
                    payload: payload.to_string(),
                };
                // Ignore send errors: the recipient may have disconnected.
                let _ = tx.send(record);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn listen_notify_delivers_message() {
        let hub = Arc::new(NotifyHub::new());
        let mut rx = hub.register_connection(1);

        hub.listen(1, "test_channel");
        hub.notify(99, "test_channel", "hello");

        let record = rx.recv().await.expect("received");
        assert_eq!(record.channel, "test_channel");
        assert_eq!(record.payload, "hello");
        assert_eq!(record.notifier_pid, 99);
    }

    #[tokio::test]
    async fn notify_unreachable_channel_is_noop() {
        let hub = NotifyHub::new();
        // Should not panic even though nobody listens.
        hub.notify(1, "ghost_channel", "nobody home");
    }

    #[tokio::test]
    async fn unlisten_removes_subscription() {
        let hub = Arc::new(NotifyHub::new());
        let mut rx = hub.register_connection(2);

        hub.listen(2, "ch");
        hub.unlisten(2, "ch");
        hub.notify(1, "ch", "payload");

        // Channel should yield nothing (non-blocking try_recv).
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn unlisten_star_removes_all_channels() {
        let hub = Arc::new(NotifyHub::new());
        let mut rx = hub.register_connection(3);

        hub.listen(3, "a");
        hub.listen(3, "b");
        hub.unlisten(3, "*");
        hub.notify(1, "a", "x");
        hub.notify(1, "b", "y");

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn deregister_cleans_up() {
        let hub = Arc::new(NotifyHub::new());
        let _ = hub.register_connection(4);
        hub.listen(4, "ch");
        hub.deregister_connection(4);

        // Sending to the now-empty channel must not panic.
        hub.notify(1, "ch", "payload");
    }

    #[tokio::test]
    async fn multiple_listeners_all_receive() {
        let hub = Arc::new(NotifyHub::new());
        let mut rx1 = hub.register_connection(10);
        let mut rx2 = hub.register_connection(11);

        hub.listen(10, "shared");
        hub.listen(11, "shared");
        hub.notify(99, "shared", "broadcast");

        let r1 = rx1.recv().await.expect("rx1");
        let r2 = rx2.recv().await.expect("rx2");
        assert_eq!(r1.payload, "broadcast");
        assert_eq!(r2.payload, "broadcast");
    }
}
