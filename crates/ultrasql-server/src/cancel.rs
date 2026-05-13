//! CancelRequest handling and per-connection cancel flags.
//!
//! ## Protocol flow
//!
//! At startup the server sends `BackendKeyData { pid, secret }`. If the client
//! wants to cancel an in-progress query, it opens a *new* TCP connection and
//! immediately sends a `CancelRequest` carrying the same `(pid, secret)` pair.
//! The server looks the pair up in the [`CancelRegistry`], validates the secret,
//! and sets the matching connection's `AtomicBool` cancel flag.
//!
//! Each connection's execution operators poll the flag in `next_batch`. When it
//! fires, the operator returns `Err(ExecError::Cancelled)`, which propagates
//! back to the connection loop as a query-scoped error.
//!
//! ## Cancel flag
//!
//! The flag is a simple `Arc<AtomicBool>`. The connection owns one
//! `Arc`-clone; the registry holds another. Setting the flag from the cancel
//! task is race-free: the worst case is the query finishes before the flag is
//! read.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use rand::RngCore;

/// Per-connection cancel signal.
///
/// Operators call [`CancelFlag::is_set`] on each `next_batch` call. The
/// connection handler uses [`CancelFlag::cancel`] when a valid
/// `CancelRequest` is received.
#[derive(Clone, Debug)]
pub struct CancelFlag(Arc<AtomicBool>);

impl CancelFlag {
    /// Create a new, uncancelled flag.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Return `true` if the flag has been set.
    #[must_use]
    pub fn is_set(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// Set the cancel flag. Idempotent.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl Default for CancelFlag {
    fn default() -> Self {
        Self::new()
    }
}

/// Entry stored in the cancel registry for each open connection.
#[derive(Debug)]
struct CancelEntry {
    secret: u32,
    flag: CancelFlag,
}

/// Global registry of open connections and their cancel handles.
///
/// `parking_lot::Mutex` per AGENTS.md §5: this lock is held only briefly
/// during registration, lookup, and deregistration — never across I/O.
#[derive(Debug, Default)]
pub struct CancelRegistry {
    entries: Mutex<HashMap<u32, CancelEntry>>,
}

impl CancelRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new connection with a random secret.
    ///
    /// Returns `(pid, secret, flag)`. The caller sends `(pid, secret)` in
    /// `BackendKeyData`; the flag is threaded through the operator chain.
    pub fn register(&self, pid: u32) -> (u32, u32, CancelFlag) {
        let mut secret_bytes = [0u8; 4];
        rand::thread_rng().fill_bytes(&mut secret_bytes);
        let secret = u32::from_be_bytes(secret_bytes);
        let flag = CancelFlag::new();
        self.entries.lock().insert(
            pid,
            CancelEntry {
                secret,
                flag: flag.clone(),
            },
        );
        (pid, secret, flag)
    }

    /// Deregister a connection when it terminates.
    pub fn deregister(&self, pid: u32) {
        self.entries.lock().remove(&pid);
    }

    /// Try to cancel the connection identified by `(pid, secret)`.
    ///
    /// Returns `true` if the entry was found and the secret matched.
    pub fn request_cancel(&self, pid: u32, secret: u32) -> bool {
        let entries = self.entries.lock();
        if let Some(entry) = entries.get(&pid) {
            if entry.secret == secret {
                entry.flag.cancel();
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_flag_starts_unset() {
        let flag = CancelFlag::new();
        assert!(!flag.is_set());
    }

    #[test]
    fn cancel_flag_set_is_visible() {
        let flag = CancelFlag::new();
        flag.cancel();
        assert!(flag.is_set());
    }

    #[test]
    fn cancel_flag_clone_shares_state() {
        let flag = CancelFlag::new();
        let clone = flag.clone();
        flag.cancel();
        assert!(clone.is_set());
    }

    #[test]
    fn registry_register_returns_unique_pid() {
        let reg = CancelRegistry::new();
        let (pid1, _, _) = reg.register(1);
        let (pid2, _, _) = reg.register(2);
        assert_ne!(pid1, pid2);
    }

    #[test]
    fn registry_request_cancel_correct_secret_succeeds() {
        let reg = CancelRegistry::new();
        let (pid, secret, flag) = reg.register(10);
        let cancelled = reg.request_cancel(pid, secret);
        assert!(cancelled, "correct secret must succeed");
        assert!(flag.is_set(), "flag must be set after cancel");
    }

    #[test]
    fn registry_request_cancel_wrong_secret_fails() {
        let reg = CancelRegistry::new();
        let (pid, secret, flag) = reg.register(20);
        let bad_secret = secret.wrapping_add(1);
        let cancelled = reg.request_cancel(pid, bad_secret);
        assert!(!cancelled, "wrong secret must fail");
        assert!(!flag.is_set(), "flag must not be set on bad secret");
    }

    #[test]
    fn registry_deregister_removes_entry() {
        let reg = CancelRegistry::new();
        let (pid, secret, _flag) = reg.register(30);
        reg.deregister(pid);
        // After deregister, cancel is a no-op and returns false.
        assert!(!reg.request_cancel(pid, secret));
    }

    #[test]
    fn registry_cancel_unknown_pid_returns_false() {
        let reg = CancelRegistry::new();
        assert!(!reg.request_cancel(9999, 42));
    }
}
