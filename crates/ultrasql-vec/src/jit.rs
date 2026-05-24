//! Runtime code-generation controls for hot vector kernels.
//!
//! JIT execution is disabled in the safe production path. The public helper
//! functions keep their `Option<i64>` contract and return `None`, so callers
//! fall back to the normal safe kernels without changing query semantics.

/// Default row threshold before a lowerer considers JIT code.
pub const DEFAULT_JIT_ABOVE_ROWS: usize = 262_144;

/// Per-statement JIT controls.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JitConfig {
    /// Whether JIT paths may run.
    pub enabled: bool,
    /// Minimum input rows before the lowerer pays compile / dispatch cost.
    pub above_rows: usize,
}

impl JitConfig {
    /// JIT disabled, PostgreSQL-compatible surface default until
    /// benchmark gates prove compiled paths win broadly.
    pub const OFF: Self = Self {
        enabled: false,
        above_rows: DEFAULT_JIT_ABOVE_ROWS,
    };

    /// JIT enabled with the default threshold.
    ///
    /// The safe production build currently returns `None` from all JIT
    /// helpers, so callers still fall back to scalar kernels.
    pub const ON: Self = Self {
        enabled: true,
        above_rows: DEFAULT_JIT_ABOVE_ROWS,
    };

    /// Returns true when this statement should try a compiled kernel.
    #[inline]
    pub const fn should_jit(self, rows: usize) -> bool {
        self.enabled && rows >= self.above_rows
    }
}

impl Default for JitConfig {
    fn default() -> Self {
        Self::OFF
    }
}

/// Run the compiled `SUM(i32) WHERE i32 > threshold` kernel if available.
///
/// Returns `None` in the safe production path so callers use the normal
/// scalar fallback.
#[must_use]
pub fn filter_sum_i32_widening_gt_jit(_data: &[i32], _threshold: i32) -> Option<i64> {
    None
}

/// Run the compiled `SUM(i64) WHERE i64 > threshold` kernel if available.
///
/// Returns `None` in the safe production path so callers use the normal
/// scalar fallback.
#[must_use]
pub fn filter_sum_i64_gt_jit(_data: &[i64], _threshold: i64) -> Option<i64> {
    None
}

/// Run the compiled `SUM(abs(i64)) WHERE abs(i64) > threshold` kernel if available.
///
/// Returns `None` in the safe production path so callers use the normal
/// scalar fallback.
#[must_use]
pub fn filter_sum_abs_i64_gt_jit(_data: &[i64], _threshold: i64) -> Option<i64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jit_config_threshold_gate() {
        let cfg = JitConfig {
            enabled: true,
            above_rows: 10,
        };
        assert!(!cfg.should_jit(9));
        assert!(cfg.should_jit(10));
        assert!(!JitConfig::OFF.should_jit(1_000_000));
    }

    #[test]
    fn jit_helpers_return_none_for_safe_fallback() {
        assert_eq!(filter_sum_i32_widening_gt_jit(&[1, 2, 3], 1), None);
        assert_eq!(filter_sum_i64_gt_jit(&[1, 2, 3], 1), None);
        assert_eq!(filter_sum_abs_i64_gt_jit(&[-3, 2, 1], 1), None);
    }
}
