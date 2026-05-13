//! Optimizer error type.
//!
//! All error conditions produced during plan rewriting or (in a future wave)
//! cost-based search are surfaced through [`OptimizeError`]. The variants are
//! kept deliberately coarse-grained so that the caller always has enough
//! context to emit a useful `EXPLAIN` or user-facing error without inspecting
//! internals.

/// Errors produced by the optimizer.
///
/// A rule failure names the rule and includes a detail string so that
/// `EXPLAIN` can surface why a rewrite was abandoned. `DidNotConverge` is a
/// safety-valve that fires only when a misconfigured or oscillating rule set
/// exceeds the iteration cap.
#[derive(Debug, thiserror::Error)]
pub enum OptimizeError {
    /// A named rule failed for a plan-specific reason.
    #[error("rule {rule} failed: {detail}")]
    RuleFailed {
        /// Short rule name (same value as [`crate::rules::RewriteRule::name`]).
        rule: &'static str,
        /// Human-readable explanation of the failure.
        detail: String,
    },

    /// The rule loop reached `max_iterations` without converging to a fixed
    /// point. This indicates an oscillating rule set.
    #[error("plan exceeded {max_iterations} rule iterations without converging")]
    DidNotConverge {
        /// The configured iteration cap that was reached.
        max_iterations: u32,
    },

    /// A rule that was invoked on a plan shape it does not handle.
    ///
    /// The caller (the driver loop or a test) may treat this as a no-op and
    /// continue.
    #[error("rule not applicable: {0}")]
    NotApplicable(&'static str),
}
