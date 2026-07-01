//! Statement timeout guard for query-scoped cancellation.
//!
//! Arms the statement deadline carried inside [`CancelFlag`] — no
//! per-statement timer thread. Executor operators (and any other
//! deadline-aware wait loop) observe expiry through their regular
//! [`CancelFlag::is_set`] polls, which turn into
//! [`ExecError::Cancelled`](ultrasql_executor::ExecError::Cancelled)
//! → SQLSTATE `57014`. Arming costs one clock read plus one relaxed
//! atomic store, so a non-zero server-default `statement_timeout` adds
//! no measurable per-statement overhead.

use ultrasql_executor::CancelFlag;

/// Arms the per-statement deadline and clears it (plus any timeout-fired
/// cancel latch) on drop.
pub(super) struct StatementTimeoutGuard {
    cancel_flag: CancelFlag,
}

impl StatementTimeoutGuard {
    /// Arm a guard for `timeout_ms`; `0` means disabled.
    pub(super) fn arm(timeout_ms: u64, cancel_flag: CancelFlag) -> Option<Self> {
        if timeout_ms == 0 {
            return None;
        }
        cancel_flag.arm_deadline_in_ms(timeout_ms);
        Some(Self { cancel_flag })
    }
}

impl Drop for StatementTimeoutGuard {
    fn drop(&mut self) {
        // Order matters: read the fired state BEFORE disarming, then clear
        // the latched cancel bit only when this guard's own deadline fired.
        // A client `CancelRequest` that raced in stays latched for the
        // session's normal cancel handling.
        let fired = self.cancel_flag.deadline_expired();
        self.cancel_flag.clear_deadline();
        if fired {
            self.cancel_flag.reset();
        }
    }
}
