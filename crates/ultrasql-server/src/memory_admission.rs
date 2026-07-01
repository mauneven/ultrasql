//! Process-wide memory-admission ceiling for per-statement `work_mem`.
//!
//! `work_mem` bounds one statement's memory-heavy operators, but N
//! connections × `work_mem` is unbounded — enough concurrent sorts could
//! still OOM-kill the process. [`MemoryAdmission`] adds a coarse
//! process-wide ceiling: at statement start the effective per-query budget
//! becomes
//!
//! ```text
//! effective = min(session work_mem, ceiling / max(1, active_connections))
//! ```
//!
//! so the aggregate of all statements' budgets can never exceed the
//! ceiling. The ceiling is configured with `--memory-ceiling-bytes` /
//! `ULTRASQL_MEMORY_CEILING_BYTES`; `0` (the default) auto-sizes to 75 %
//! of the physical RAM detected at startup (a conservative
//! [`FALLBACK_CEILING_BYTES`] when detection fails). Statements over the
//! effective budget spill to disk exactly as they do when `work_mem` is
//! the binding limit — this changes *when* spilling engages, never
//! correctness.
//!
//! Hot-path cost: computing the effective budget is one relaxed atomic
//! load (the live-session counter) plus integer math, once per statement.
//! No locks anywhere.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Floor for any effective per-statement budget, matching the `SET
/// work_mem` clamp: even a tiny ceiling divided across many connections
/// must not hand out a zero/near-zero budget that would make every
/// operator spill on trivial input (or divide-by-zero a spill heuristic).
pub const MIN_EFFECTIVE_WORK_MEM_BYTES: u64 = 64 * 1024;

/// Ceiling used when auto-sizing is requested but physical RAM cannot be
/// detected: 6 GiB (75 % of a nominal 8 GiB host). Deliberately
/// conservative — a beta deployment on an undetectable platform is safer
/// under-budgeted (more spilling) than OOM-killed.
pub const FALLBACK_CEILING_BYTES: u64 = 6 * 1024 * 1024 * 1024;

/// Process-wide memory-admission state shared by every session.
///
/// Cloning shares the live-session counter (the ceiling is copied); the
/// server owns the canonical instance and sessions read through
/// `Arc<Server>`.
#[derive(Clone, Debug)]
pub struct MemoryAdmission {
    /// Resolved ceiling in bytes; always non-zero after construction.
    ceiling_bytes: u64,
    /// Live client sessions (incremented at session construction,
    /// decremented at session drop).
    active_sessions: Arc<AtomicUsize>,
}

impl MemoryAdmission {
    /// Build from `ULTRASQL_MEMORY_CEILING_BYTES` (absent / `0` /
    /// unparsable → auto-size to 75 % of detected physical RAM).
    #[must_use]
    pub fn from_env_or_auto() -> Self {
        let configured = std::env::var("ULTRASQL_MEMORY_CEILING_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);
        Self::with_ceiling(configured)
    }

    /// Build with an explicit ceiling; `0` auto-sizes to 75 % of detected
    /// physical RAM.
    #[must_use]
    pub fn with_ceiling(ceiling_bytes: u64) -> Self {
        Self {
            ceiling_bytes: resolve_ceiling_bytes(ceiling_bytes, detect_physical_memory_bytes()),
            active_sessions: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Replace the ceiling (startup configuration only, before the
    /// listener accepts connections); `0` auto-sizes.
    pub fn set_ceiling_bytes(&mut self, ceiling_bytes: u64) {
        self.ceiling_bytes = resolve_ceiling_bytes(ceiling_bytes, detect_physical_memory_bytes());
    }

    /// The resolved process-wide ceiling in bytes.
    #[must_use]
    pub fn ceiling_bytes(&self) -> u64 {
        self.ceiling_bytes
    }

    /// Record a new live session. One relaxed atomic add.
    pub fn register_session(&self) {
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a session teardown. One relaxed atomic sub.
    pub fn deregister_session(&self) {
        self.active_sessions.fetch_sub(1, Ordering::Relaxed);
    }

    /// Current live-session count (test / observability surface).
    #[must_use]
    pub fn active_sessions(&self) -> usize {
        self.active_sessions.load(Ordering::Relaxed)
    }

    /// The per-statement admission cap: `ceiling / max(1, active)`,
    /// floored at [`MIN_EFFECTIVE_WORK_MEM_BYTES`]. One relaxed atomic
    /// load + one division; called once per statement.
    #[must_use]
    pub fn per_statement_cap_bytes(&self) -> u64 {
        per_statement_cap_bytes(self.ceiling_bytes, self.active_sessions())
    }
}

/// Resolve a configured ceiling: an explicit non-zero value wins; `0`
/// (auto) takes 75 % of `detected_ram_bytes`, or
/// [`FALLBACK_CEILING_BYTES`] when RAM detection failed.
#[must_use]
pub fn resolve_ceiling_bytes(configured: u64, detected_ram_bytes: Option<u64>) -> u64 {
    if configured > 0 {
        return configured.max(MIN_EFFECTIVE_WORK_MEM_BYTES);
    }
    match detected_ram_bytes {
        Some(ram) if ram > 0 => (ram / 4)
            .saturating_mul(3)
            .max(MIN_EFFECTIVE_WORK_MEM_BYTES),
        _ => FALLBACK_CEILING_BYTES,
    }
}

/// The per-statement admission cap for a given ceiling and live-session
/// count: `ceiling / max(1, active)`, floored at
/// [`MIN_EFFECTIVE_WORK_MEM_BYTES`].
#[must_use]
pub fn per_statement_cap_bytes(ceiling_bytes: u64, active_sessions: usize) -> u64 {
    let divisor = u64::try_from(active_sessions.max(1)).unwrap_or(u64::MAX);
    (ceiling_bytes / divisor).max(MIN_EFFECTIVE_WORK_MEM_BYTES)
}

/// The effective per-statement `work_mem`: the session's requested budget
/// capped by the admission cap, floored at
/// [`MIN_EFFECTIVE_WORK_MEM_BYTES`].
#[must_use]
pub fn effective_work_mem_bytes(requested_bytes: u64, per_statement_cap: u64) -> u64 {
    requested_bytes
        .min(per_statement_cap)
        .max(MIN_EFFECTIVE_WORK_MEM_BYTES)
}

/// Best-effort physical-RAM detection, evaluated once at startup (never
/// on a query path). macOS: `sysctl -n hw.memsize`; Linux (and other
/// `/proc` platforms): `MemTotal` from `/proc/meminfo`. Returns `None`
/// when the platform offers neither.
fn detect_physical_memory_bytes() -> Option<u64> {
    if cfg!(target_os = "macos") {
        std::process::Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()
            .and_then(|output| {
                output
                    .status
                    .success()
                    .then(|| String::from_utf8_lossy(&output.stdout).trim().parse().ok())
                    .flatten()
            })
    } else {
        std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|meminfo| {
                meminfo.lines().find_map(|line| {
                    let kb = line.strip_prefix("MemTotal:")?.split_whitespace().next()?;
                    kb.parse::<u64>()
                        .ok()
                        .and_then(|value| value.checked_mul(1024))
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;

    #[test]
    fn explicit_ceiling_wins_over_detected_ram() {
        assert_eq!(resolve_ceiling_bytes(512 * MIB, Some(64 * GIB)), 512 * MIB);
    }

    #[test]
    fn auto_ceiling_is_75_percent_of_injected_ram() {
        assert_eq!(resolve_ceiling_bytes(0, Some(32 * GIB)), 24 * GIB);
        assert_eq!(resolve_ceiling_bytes(0, Some(8 * GIB)), 6 * GIB);
    }

    #[test]
    fn auto_ceiling_falls_back_when_ram_undetectable() {
        assert_eq!(resolve_ceiling_bytes(0, None), FALLBACK_CEILING_BYTES);
        assert_eq!(resolve_ceiling_bytes(0, Some(0)), FALLBACK_CEILING_BYTES);
    }

    #[test]
    fn tiny_explicit_ceiling_is_floored() {
        assert_eq!(
            resolve_ceiling_bytes(1, Some(64 * GIB)),
            MIN_EFFECTIVE_WORK_MEM_BYTES
        );
    }

    #[test]
    fn cap_divides_ceiling_across_active_sessions() {
        assert_eq!(per_statement_cap_bytes(8 * MIB, 0), 8 * MIB);
        assert_eq!(per_statement_cap_bytes(8 * MIB, 1), 8 * MIB);
        assert_eq!(per_statement_cap_bytes(8 * MIB, 2), 4 * MIB);
        assert_eq!(per_statement_cap_bytes(8 * MIB, 3), 8 * MIB / 3);
    }

    #[test]
    fn cap_is_floored_for_many_sessions_on_a_small_ceiling() {
        assert_eq!(
            per_statement_cap_bytes(MIN_EFFECTIVE_WORK_MEM_BYTES, 1000),
            MIN_EFFECTIVE_WORK_MEM_BYTES
        );
    }

    #[test]
    fn effective_budget_is_min_of_session_setting_and_cap() {
        // Session asks for 1 GiB but the cap is 4 MiB → 4 MiB.
        assert_eq!(effective_work_mem_bytes(GIB, 4 * MIB), 4 * MIB);
        // Session asks for less than the cap → session value wins.
        assert_eq!(effective_work_mem_bytes(2 * MIB, 4 * MIB), 2 * MIB);
        // Never below the floor.
        assert_eq!(
            effective_work_mem_bytes(1, MIN_EFFECTIVE_WORK_MEM_BYTES),
            MIN_EFFECTIVE_WORK_MEM_BYTES
        );
    }

    #[test]
    fn registration_round_trip_drives_the_cap() {
        let admission = MemoryAdmission::with_ceiling(8 * MIB);
        assert_eq!(admission.ceiling_bytes(), 8 * MIB);
        assert_eq!(admission.per_statement_cap_bytes(), 8 * MIB);
        admission.register_session();
        admission.register_session();
        assert_eq!(admission.active_sessions(), 2);
        assert_eq!(admission.per_statement_cap_bytes(), 4 * MIB);
        admission.deregister_session();
        assert_eq!(admission.per_statement_cap_bytes(), 8 * MIB);
    }

    #[test]
    fn auto_construction_yields_a_generous_nonzero_ceiling() {
        // 0 = auto: 75 % of real RAM (or the 6 GiB fallback) — in every
        // case far above the 64 MiB default work_mem, so a default-config
        // single-session statement keeps its full budget (benchmark
        // non-regression at the policy level).
        let admission = MemoryAdmission::with_ceiling(0);
        assert!(admission.ceiling_bytes() >= crate::session::DEFAULT_WORK_MEM_BYTES);
    }
}
