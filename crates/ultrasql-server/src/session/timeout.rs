//! Statement timeout guard for query-scoped cancellation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ultrasql_executor::CancelFlag;

/// Arms a per-statement timer and flips the session cancel flag on expiry.
pub(super) struct StatementTimeoutGuard {
    active: Arc<AtomicBool>,
    fired: Arc<AtomicBool>,
    cancel_flag: CancelFlag,
}

impl StatementTimeoutGuard {
    /// Arm a guard for `timeout_ms`; `0` means disabled.
    pub(super) fn arm(timeout_ms: u64, cancel_flag: CancelFlag) -> Option<Self> {
        if timeout_ms == 0 {
            return None;
        }

        let active = Arc::new(AtomicBool::new(true));
        let fired = Arc::new(AtomicBool::new(false));
        let timer_active = Arc::clone(&active);
        let timer_fired = Arc::clone(&fired);
        let timer_flag = cancel_flag.clone();
        let spawned = std::thread::Builder::new()
            .name("ultrasql-statement-timeout".to_owned())
            .spawn(move || {
                std::thread::sleep(Duration::from_millis(timeout_ms));
                if timer_active.load(Ordering::Acquire) {
                    timer_fired.store(true, Ordering::Release);
                    timer_flag.cancel();
                }
            });
        if spawned.is_err() {
            fired.store(true, Ordering::Release);
            cancel_flag.cancel();
        }

        Some(Self {
            active,
            fired,
            cancel_flag,
        })
    }
}

impl Drop for StatementTimeoutGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
        if self.fired.load(Ordering::Acquire) {
            self.cancel_flag.reset();
        }
    }
}
