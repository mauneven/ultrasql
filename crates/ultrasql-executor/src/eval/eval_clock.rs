//! Statement-scoped "evaluation clock" for the stable date/time builtins.
//!
//! PostgreSQL pins `now()` / `current_timestamp` / `current_date` to the
//! **transaction-start** instant and `statement_timestamp()` to the
//! **statement-start** instant, so every row of every statement in a
//! transaction observes the *same* value. The row-at-a-time interpreter
//! evaluates these builtins once per row through dozens of independent
//! `Eval` call sites, so threading the captured instants through every
//! `eval()` signature would be a broad, invasive refactor.
//!
//! Instead the server installs the captured instants into this
//! thread-local for the duration of a single statement's execution (the
//! connection task runs the executor synchronously, so a thread-local is
//! statement-scoped and never shared across connections). When no clock is
//! installed the builtins fall back to the live wall clock, preserving the
//! prior behavior for any path that evaluates an expression outside a
//! server statement (constraint defaults, embedded helpers, unit tests).
//!
//! The clock carries engine-epoch microseconds (the same units the engine's
//! internal timestamp helper returns), so callers convert from Unix-epoch
//! micros before installing it via [`live_engine_timestamp_micros`].

use std::cell::Cell;

/// Transaction-start and statement-start instants for the current statement,
/// in **engine-epoch microseconds**.
#[derive(Clone, Copy, Debug)]
pub struct EvalClock {
    /// Pins `now()` / `current_timestamp` / `current_date`.
    pub txn_start_micros: i64,
    /// Pins `statement_timestamp()` (statement-scoped, `>= txn_start_micros`).
    pub stmt_start_micros: i64,
}

thread_local! {
    /// `Some` while a server statement is executing; `None` otherwise.
    static EVAL_CLOCK: Cell<Option<EvalClock>> = const { Cell::new(None) };
}

/// RAII guard that installs `clock` for the current statement and restores
/// the previous value (almost always `None`) on drop.
///
/// Restoring the prior value rather than unconditionally clearing keeps the
/// installation correctly nested if a statement-execution path is ever
/// re-entered (it is not today, but the guard stays correct if it becomes so).
#[derive(Debug)]
#[must_use = "the clock is uninstalled when the guard is dropped"]
pub struct EvalClockGuard {
    previous: Option<EvalClock>,
}

impl EvalClockGuard {
    /// Install `clock` as the active evaluation clock until the guard drops.
    pub fn install(clock: EvalClock) -> Self {
        let previous = EVAL_CLOCK.with(|slot| slot.replace(Some(clock)));
        Self { previous }
    }
}

impl Drop for EvalClockGuard {
    fn drop(&mut self) {
        EVAL_CLOCK.with(|slot| slot.set(self.previous));
    }
}

/// Engine-epoch micros pinned for `now()` / `current_timestamp` /
/// `current_date`, or `None` when no statement clock is installed.
pub(crate) fn txn_start_micros() -> Option<i64> {
    EVAL_CLOCK.with(|slot| slot.get().map(|clock| clock.txn_start_micros))
}

/// The live wall-clock instant in engine-epoch microseconds.
///
/// This is the same value the stable date/time builtins fall back to when no
/// statement clock is installed. The server calls it to capture the
/// transaction-start and statement-start instants it then installs via
/// [`EvalClockGuard`], so the captured instants and the builtins' fallback
/// share one epoch definition.
#[must_use]
pub fn live_engine_timestamp_micros() -> i64 {
    super::functions_datetime::current_engine_timestamp_micros()
}
