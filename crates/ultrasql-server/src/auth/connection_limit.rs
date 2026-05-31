//! Per-role startup connection limit enforcement.
//!
//! `rolconnlimit` is checked during startup, after authentication has
//! identified a catalogued role. The counter intentionally tracks every
//! accepted catalogued role session, even when the role is currently
//! unlimited, so later `ALTER ROLE ... CONNECTION LIMIT n` checks count
//! already-open sessions.

use std::collections::HashMap;

use parking_lot::Mutex;

/// Error returned when a role has exhausted its startup connection budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionLimitError {
    /// Role whose limit rejected the login.
    pub role: String,
    /// Configured `rolconnlimit`.
    pub limit: i32,
    /// Already-active sessions for this role.
    pub active: u32,
}

/// Atomic per-role live-session counter used by startup authentication.
#[derive(Debug, Default)]
pub struct RoleConnectionLimiter {
    counts: Mutex<HashMap<String, u32>>,
}

impl RoleConnectionLimiter {
    /// Create an empty limiter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Try to admit one session for `role` under `limit`.
    ///
    /// `limit < 0` means unlimited, matching `rolconnlimit = -1`. The
    /// accepted session is still counted so future limit changes see it.
    pub fn try_acquire(&self, role: &str, limit: i32) -> Result<(), ConnectionLimitError> {
        let role = normalize_role_name(role);
        let mut counts = self.counts.lock();
        let active = counts.get(&role).copied().unwrap_or(0);

        if limit >= 0 {
            let limit_u32 = u32::try_from(limit).map_err(|_| ConnectionLimitError {
                role: role.clone(),
                limit,
                active,
            })?;
            if active >= limit_u32 {
                return Err(ConnectionLimitError {
                    role,
                    limit,
                    active,
                });
            }
        }

        let next_active = active.checked_add(1).ok_or_else(|| ConnectionLimitError {
            role: role.clone(),
            limit,
            active,
        })?;
        counts.insert(role, next_active);
        Ok(())
    }

    /// Release one accepted session for `role`.
    pub fn release(&self, role: &str) {
        let role = normalize_role_name(role);
        let mut counts = self.counts.lock();
        if let Some(active) = counts.get_mut(&role) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                counts.remove(&role);
            }
        }
    }
}

fn normalize_role_name(role: &str) -> String {
    role.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limited_role_rejects_after_limit_and_releases() {
        let limiter = RoleConnectionLimiter::new();
        limiter.try_acquire("App", 1).expect("first slot");

        let err = limiter
            .try_acquire("app", 1)
            .expect_err("second slot rejected");
        assert_eq!(
            err,
            ConnectionLimitError {
                role: "app".to_owned(),
                limit: 1,
                active: 1
            }
        );

        limiter.release("APP");
        limiter.try_acquire("app", 1).expect("released slot");
    }

    #[test]
    fn unlimited_roles_are_still_counted_for_later_limits() {
        let limiter = RoleConnectionLimiter::new();
        limiter.try_acquire("app", -1).expect("unlimited slot");

        let err = limiter
            .try_acquire("app", 1)
            .expect_err("existing session consumes later limit");
        assert_eq!(err.active, 1);
    }

    #[test]
    fn active_counter_overflow_rejects_login() {
        let limiter = RoleConnectionLimiter::new();
        limiter.counts.lock().insert("app".to_owned(), u32::MAX);

        let err = limiter
            .try_acquire("app", -1)
            .expect_err("overflow must reject login");

        assert_eq!(
            err,
            ConnectionLimitError {
                role: "app".to_owned(),
                limit: -1,
                active: u32::MAX,
            }
        );
    }
}
