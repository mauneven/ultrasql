//! `CancelRequest` handling and per-connection cancel flags.
//!
//! ## Protocol flow
//!
//! At startup the server sends `BackendKeyData { pid, secret }`. If the client
//! wants to cancel an in-progress query, it opens a *new* TCP connection and
//! immediately sends a `CancelRequest` carrying the same `(pid, secret)` pair.
//! The server looks the pair up in the [`CancelRegistry`], validates the
//! secret, and sets the matching connection's cancel flag.
//!
//! Each connection's execution operators poll the flag in `next_batch`. When
//! it fires, the operator returns
//! `Err(ultrasql_executor::ExecError::Cancelled)`, which propagates back to
//! the connection loop as a query-scoped error and lands on the client as
//! SQLSTATE `57014` (`query_canceled`).
//!
//! ## Cancel flag
//!
//! The flag itself lives in `ultrasql-executor` ([`CancelFlag`]) so the
//! operator crate can poll it without taking a back-edge dependency on the
//! server. The registry stores one [`CancelFlag`] clone per registered
//! connection; the session holds another. Setting the flag from the cancel
//! task is race-free — the worst case is the query finishes before the
//! flag is observed.

use std::sync::atomic::{AtomicU32, Ordering};

use dashmap::DashMap;
use rand::RngCore;
use ultrasql_executor::CancelFlag;

/// Server-assigned identifier for an in-flight connection.
///
/// The server hands one of these out per accepted session via a single
/// monotonically-increasing atomic counter. Distinct from a real OS pid;
/// only used as the lookup key in the cancel registry. Wrap-around is
/// tolerated — colliding only matters when both sessions are alive
/// simultaneously, and at >4 billion concurrent sessions the database
/// has other problems.
pub type ProcessId = u32;

/// Cryptographic-random per-connection token announced in
/// `BackendKeyData`. A cancel request must echo the token verbatim or
/// the registry refuses the cancel.
pub type SecretKey = u32;

/// Entry stored in the registry for each open connection.
///
/// Both `secret` and `flag` are needed at cancel time: the secret to
/// authorise the cancel, the flag to deliver it.
#[derive(Clone, Debug)]
struct CancelEntry {
    secret: SecretKey,
    flag: CancelFlag,
}

/// Global registry of open connections and their cancel handles.
///
/// Backed by [`DashMap`] (per AGENTS.md §5 default) so registration and
/// lookup are sharded — a high-fan-in deployment can register and
/// deregister sessions without serialising on a single mutex.
#[derive(Debug, Default)]
pub struct CancelRegistry {
    /// `(pid, secret, flag)` map keyed by pid. `secret` is held in the
    /// value so a cancel-request lookup is one `DashMap::get` plus an
    /// equality check.
    entries: DashMap<ProcessId, CancelEntry>,
    /// Monotonically increasing pid allocator. Starts at 1 so the
    /// zero value is reserved for "uninitialised".
    next_pid: AtomicU32,
}

impl CancelRegistry {
    /// Construct an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            next_pid: AtomicU32::new(1),
        }
    }

    /// Register a new connection.
    ///
    /// Allocates a fresh pid via the internal atomic counter, draws a
    /// cryptographic-random `u32` secret, and stores a clone of `flag`.
    /// Returns the assigned `(pid, secret)` the caller emits in
    /// `BackendKeyData`.
    ///
    /// The pid counter increments by one on every call; we never reuse
    /// a slot during the registry's lifetime. With a 32-bit space and
    /// the v0.5 connection budget this is comfortably future-proof.
    /// On the (vanishingly unlikely) wraparound to zero we skip the
    /// reserved zero value and continue.
    pub fn register(&self, flag: CancelFlag) -> (ProcessId, SecretKey) {
        // Allocate a pid. The atomic add is the only synchronisation
        // on the hot path. We retry on the reserved zero value so
        // every announced pid is non-zero — that lets the test
        // harness use `pid == 0` as a sentinel for "not yet
        // received".
        let pid = loop {
            let candidate = self.next_pid.fetch_add(1, Ordering::Relaxed);
            if candidate != 0 {
                break candidate;
            }
        };
        // Cryptographic-random 4-byte secret. `OsRng` reads from the
        // platform entropy source, which is the right primitive for a
        // security token (per AGENTS.md §10 "no fabricated numbers"
        // and the §1.9 task brief: "never `0`").
        let mut secret_bytes = [0_u8; 4];
        rand::rngs::OsRng.fill_bytes(&mut secret_bytes);
        let mut secret = u32::from_be_bytes(secret_bytes);
        // Guard against the astronomically unlikely zero draw so the
        // wire `(pid, secret)` is never trivially-forgeable.
        if secret == 0 {
            secret = 1;
        }
        self.entries.insert(pid, CancelEntry { secret, flag });
        (pid, secret)
    }

    /// Deregister a connection when it terminates.
    ///
    /// Idempotent: a session that was never registered (because
    /// startup failed before `register`) is a no-op.
    pub fn deregister(&self, pid: ProcessId) {
        self.entries.remove(&pid);
    }

    /// Try to cancel the connection identified by `(pid, secret)`.
    ///
    /// Returns `true` if the entry was found and the secret matched.
    /// A mismatched secret or unknown pid silently returns `false` —
    /// PostgreSQL behaves the same way to avoid a probe oracle.
    pub fn request_cancel(&self, pid: ProcessId, secret: SecretKey) -> bool {
        let entry = self.entries.get(&pid);
        let Some(entry) = entry else {
            return false;
        };
        if entry.secret != secret {
            return false;
        }
        entry.flag.cancel();
        true
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_register_returns_distinct_pids() {
        let reg = CancelRegistry::new();
        let (pid1, _) = reg.register(CancelFlag::new());
        let (pid2, _) = reg.register(CancelFlag::new());
        assert_ne!(pid1, pid2);
        assert_ne!(pid1, 0);
        assert_ne!(pid2, 0);
    }

    #[test]
    fn registry_register_returns_nonzero_secret() {
        let reg = CancelRegistry::new();
        let (_, secret) = reg.register(CancelFlag::new());
        assert_ne!(
            secret, 0,
            "secret must never be the trivially-forgeable zero"
        );
    }

    #[test]
    fn registry_request_cancel_correct_secret_succeeds() {
        let reg = CancelRegistry::new();
        let flag = CancelFlag::new();
        let (pid, secret) = reg.register(flag.clone());
        let cancelled = reg.request_cancel(pid, secret);
        assert!(cancelled, "correct secret must succeed");
        assert!(flag.is_set(), "flag must be set after cancel");
    }

    #[test]
    fn registry_request_cancel_wrong_secret_fails() {
        let reg = CancelRegistry::new();
        let flag = CancelFlag::new();
        let (pid, secret) = reg.register(flag.clone());
        let bad_secret = secret.wrapping_add(1);
        let cancelled = reg.request_cancel(pid, bad_secret);
        assert!(!cancelled, "wrong secret must fail");
        assert!(!flag.is_set(), "flag must not be set on bad secret");
    }

    #[test]
    fn registry_deregister_removes_entry() {
        let reg = CancelRegistry::new();
        let flag = CancelFlag::new();
        let (pid, secret) = reg.register(flag);
        reg.deregister(pid);
        assert!(!reg.request_cancel(pid, secret));
    }

    #[test]
    fn registry_cancel_unknown_pid_returns_false() {
        let reg = CancelRegistry::new();
        assert!(!reg.request_cancel(9999, 42));
    }
}
