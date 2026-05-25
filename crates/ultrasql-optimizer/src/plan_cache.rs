//! Plan cache for prepared statements.
//!
//! The plan cache stores a *generic plan* (optimized with parameter values
//! replaced by type-only placeholders) for each prepared statement. On each
//! `EXECUTE`, the cache computes a *custom plan* (optimized with the actual
//! bound parameter values) and decides whether to return the generic plan or
//! the custom plan.
//!
//! ## Generic vs. custom plans
//!
//! A **generic plan** is produced once and reused across executions, saving
//! planning time. A **custom plan** is produced fresh for each execution with
//! the real parameter values, which can yield a better physical plan when the
//! actual values select a highly non-uniform distribution (e.g., a very
//! selective equality on a skewed column).
//!
//! ## Re-plan threshold
//!
//! The cache tracks the cost of the last custom plan
//! (`PlanCacheEntry::last_custom_cost`) and compares it against the generic
//! plan cost. If:
//!
//! ```text
//! generic_cost.total_cost > replan_threshold × last_custom_cost.total_cost
//! ```
//!
//! the custom plan is returned for *this execution* and a counter is
//! incremented. If the custom plan has been cheaper for
//! `N ≥ CUSTOM_PLAN_EVICTION_THRESHOLD` consecutive executions, the generic
//! plan is evicted and the next call to [`PlanCache::get_or_plan`] will
//! re-plan as generic.
//!
//! The default `replan_threshold` of `5.0` matches PostgreSQL's
//! `plan_cache_mode = auto` heuristic.
//!
//! ## LRU eviction
//!
//! When `entries.len() >= config.max_entries`, the entry with the smallest
//! `use_count` is evicted. (A full LRU linked-list is not needed here because
//! the entry count is bounded by the number of prepared statement names, which
//! is small in practice.)
//!
//! ## Thread safety
//!
//! [`PlanCache`] uses `DashMap` for the entry table, making it safe to share
//! across threads without an outer `Mutex`. Individual entries are updated
//! through short critical sections inside `DashMap::entry`.

use dashmap::DashMap;
use ultrasql_core::Value;
use ultrasql_planner::LogicalPlan;

use crate::cost::CostEstimate;
use crate::error::OptimizeError;

// ============================================================================
// Constants
// ============================================================================

/// After this many consecutive executions where the custom plan is cheaper,
/// evict the generic plan so it will be re-planned on the next call.
const CUSTOM_PLAN_EVICTION_THRESHOLD: u64 = 5;

// ============================================================================
// Public types
// ============================================================================

/// Key used to look up a cached plan.
///
/// In PostgreSQL terms this corresponds to the prepared statement name (the
/// `name` argument of `PREPARE name AS …`). The empty string `""` maps to
/// the unnamed statement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PlanCacheKey {
    /// Prepared-statement name. Case-sensitive; empty for the unnamed statement.
    pub statement_name: String,
}

impl PlanCacheKey {
    /// Create a key for a named prepared statement.
    #[must_use]
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            statement_name: name.into(),
        }
    }

    /// Create a key for the unnamed statement.
    #[must_use]
    pub const fn unnamed() -> Self {
        Self {
            statement_name: String::new(),
        }
    }
}

/// A single entry in the plan cache.
///
/// Carries both the generic plan (optimized without actual parameter values)
/// and the latest custom-plan cost estimate (used for the re-plan threshold
/// decision).
#[derive(Clone, Debug)]
pub struct PlanCacheEntry {
    /// The generic plan produced by the planner with parameter placeholders.
    pub generic_plan: LogicalPlan,
    /// Estimated cost of the generic plan.
    pub generic_cost: CostEstimate,
    /// Number of times this entry has been used (cache hits + replans).
    pub use_count: u64,
    /// Cost of the most recent custom plan, if any.
    pub last_custom_cost: Option<CostEstimate>,
    /// Number of consecutive executions where the custom plan was cheaper
    /// than the generic plan by more than `replan_threshold`.
    custom_cheaper_streak: u64,
}

/// Configuration for the plan cache.
#[derive(Clone, Copy, Debug)]
pub struct PlanCacheConfig {
    /// Re-plan threshold multiplier.
    ///
    /// When `generic_cost.total_cost > replan_threshold × custom_cost.total_cost`,
    /// the custom plan is returned for the current execution.
    /// Default: `5.0` (PostgreSQL's default).
    pub replan_threshold: f64,
    /// Maximum number of entries before LRU eviction kicks in.
    ///
    /// Default: `1024`.
    pub max_entries: usize,
}

impl Default for PlanCacheConfig {
    fn default() -> Self {
        Self {
            replan_threshold: 5.0,
            max_entries: 1_024,
        }
    }
}

// ============================================================================
// PlanCache
// ============================================================================

/// Thread-safe cache of generic plans for prepared statements.
///
/// ## Usage
///
/// ```rust
/// use ultrasql_optimizer::plan_cache::{PlanCache, PlanCacheConfig, PlanCacheKey};
/// use ultrasql_optimizer::cost::{CostEstimate, NoStats, CostModel};
/// use ultrasql_planner::LogicalPlan;
/// use ultrasql_core::{Schema, Value};
///
/// let cache = PlanCache::new(PlanCacheConfig::default());
/// let key = PlanCacheKey::named("my_stmt");
///
/// let plan = cache.get_or_plan(&key, &[], |_params| {
///     Ok(LogicalPlan::Empty { schema: Schema::empty() })
/// }).expect("plan ok");
/// ```
pub struct PlanCache {
    entries: DashMap<PlanCacheKey, PlanCacheEntry>,
    config: PlanCacheConfig,
}

impl std::fmt::Debug for PlanCache {
    /// Print the cache shape without iterating every entry.
    ///
    /// The entry list can be large (1024 entries by default) and each
    /// entry holds a [`LogicalPlan`] tree; rendering all of that would
    /// turn an idle `tracing::debug!` into a CPU spike. We instead
    /// print the configured limits and the current entry count, which
    /// is all callers actually need.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanCache")
            .field("entries", &self.entries.len())
            .field("config", &self.config)
            .finish()
    }
}

impl PlanCache {
    /// Create a new empty plan cache with the given configuration.
    #[must_use]
    pub fn new(config: PlanCacheConfig) -> Self {
        Self {
            entries: DashMap::new(),
            config,
        }
    }

    /// Look up or create a cached plan for `key`.
    ///
    /// On the first call for a given `key`, `planner` is invoked with `params`
    /// to produce a [`LogicalPlan`]. The plan is stored as the generic plan.
    ///
    /// On subsequent calls:
    ///
    /// 1. The cached generic plan is looked up.
    /// 2. If `params` is non-empty, `planner` is called again to get a custom
    ///    plan with the actual parameter values.
    /// 3. The custom plan cost is compared against the generic plan cost using
    ///    `config.replan_threshold`. If the custom plan is significantly
    ///    cheaper, the custom plan is returned.
    /// 4. If the custom plan has been cheaper for
    ///    `CUSTOM_PLAN_EVICTION_THRESHOLD` consecutive executions, the entry is
    ///    evicted so that the generic plan will be re-built on the next call.
    ///
    /// When `params` is empty, the generic plan is always returned (no
    /// custom-plan comparison is performed).
    ///
    /// # Errors
    ///
    /// Propagates any `Err` returned by the `planner` closure.
    pub fn get_or_plan(
        &self,
        key: &PlanCacheKey,
        params: &[Value],
        planner: impl FnOnce(&[Value]) -> Result<LogicalPlan, OptimizeError>,
    ) -> Result<LogicalPlan, OptimizeError> {
        // --- Fast path: cache hit, no parameters ---
        if params.is_empty() {
            if let Some(entry) = self.entries.get(key) {
                let plan = entry.generic_plan.clone();
                drop(entry); // release the shard lock before writing
                self.increment_use_count(key);
                return Ok(plan);
            }
        }

        // --- Check for evicted or missing entry ---
        let generic_plan_opt = self.entries.get(key).map(|e| {
            (
                e.generic_plan.clone(),
                e.generic_cost,
                e.custom_cheaper_streak,
            )
        });

        match generic_plan_opt {
            None => {
                // First time: build the generic plan, cache it, and return it.
                let new_plan = planner(params)?;
                self.insert_new(key.clone(), new_plan.clone());
                Ok(new_plan)
            }

            Some((generic_plan, generic_cost, streak)) => {
                // Entry exists. Evict if the custom plan has been cheaper long
                // enough.
                if streak >= CUSTOM_PLAN_EVICTION_THRESHOLD {
                    self.entries.remove(key);
                    let new_plan = planner(params)?;
                    self.insert_new(key.clone(), new_plan.clone());
                    return Ok(new_plan);
                }

                // No parameters → return generic.
                if params.is_empty() {
                    self.increment_use_count(key);
                    return Ok(generic_plan);
                }

                // Parameters present: produce a custom plan and compare costs.
                // We call the planner with the real params. The cost is
                // estimated by a zero-stats CostModel (best effort; the caller
                // can supply a richer model by wrapping `planner`).
                let custom_plan = planner(params)?;
                let custom_cost = cost_of(&custom_plan);

                // Compare against generic cost.
                let generic_much_more_expensive =
                    generic_cost.total_cost > self.config.replan_threshold * custom_cost.total_cost;

                // Update entry.
                self.update_entry(
                    key,
                    generic_plan.clone(),
                    generic_cost,
                    custom_cost,
                    generic_much_more_expensive,
                );

                if generic_much_more_expensive {
                    Ok(custom_plan)
                } else {
                    Ok(generic_plan)
                }
            }
        }
    }

    /// Remove all entries whose key satisfies `predicate`.
    ///
    /// Useful for invalidating plans after `ANALYZE` or DDL changes.
    pub fn invalidate(&self, predicate: impl Fn(&PlanCacheKey) -> bool) {
        self.entries.retain(|k, _| !predicate(k));
    }

    /// Remove all cached entries.
    pub fn invalidate_all(&self) {
        self.entries.clear();
    }

    /// Number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return `true` if the cache contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ============================================================================
// Internal helpers
// ============================================================================

impl PlanCache {
    /// Insert a new entry. If the cache is full, evict the entry with the
    /// lowest `use_count` first.
    fn insert_new(&self, key: PlanCacheKey, plan: LogicalPlan) {
        // Evict if at capacity.
        if self.entries.len() >= self.config.max_entries {
            self.evict_lru();
        }
        let cost = cost_of(&plan);
        self.entries.insert(
            key,
            PlanCacheEntry {
                generic_plan: plan,
                generic_cost: cost,
                use_count: 1,
                last_custom_cost: None,
                custom_cheaper_streak: 0,
            },
        );
    }

    /// Increment the `use_count` of an existing entry.
    fn increment_use_count(&self, key: &PlanCacheKey) {
        if let Some(mut entry) = self.entries.get_mut(key) {
            entry.use_count = entry.use_count.saturating_add(1);
        }
    }

    /// Update an existing entry after a custom-plan comparison.
    fn update_entry(
        &self,
        key: &PlanCacheKey,
        _generic_plan: LogicalPlan,
        _generic_cost: CostEstimate,
        custom_cost: CostEstimate,
        custom_was_cheaper: bool,
    ) {
        if let Some(mut entry) = self.entries.get_mut(key) {
            entry.use_count = entry.use_count.saturating_add(1);
            entry.last_custom_cost = Some(custom_cost);
            if custom_was_cheaper {
                entry.custom_cheaper_streak = entry.custom_cheaper_streak.saturating_add(1);
            } else {
                entry.custom_cheaper_streak = 0;
            }
        }
    }

    /// Evict the entry with the lowest `use_count`.
    fn evict_lru(&self) {
        // Find the key with the minimum use_count.
        let victim = self
            .entries
            .iter()
            .min_by_key(|e| e.use_count)
            .map(|e| e.key().clone());
        if let Some(k) = victim {
            self.entries.remove(&k);
        }
    }
}

// ============================================================================
// Cost helper
// ============================================================================

/// Estimate the cost of a plan using the zero-stats model.
///
/// This is a best-effort estimate; the caller can pass a custom planner
/// closure that produces pre-costed plans to get accurate numbers.
fn cost_of(plan: &LogicalPlan) -> CostEstimate {
    let stats = crate::cost::NoStats;
    let model = crate::cost::CostModel::new(&stats);
    model.estimate(plan)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema, Value};
    use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr};

    use super::*;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn empty_plan() -> LogicalPlan {
        LogicalPlan::Empty {
            schema: Schema::empty(),
        }
    }

    fn scan_plan(table: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table: table.into(),
            schema: Schema::new(vec![Field::required("id", DataType::Int32)]).expect("schema ok"),
            projection: None,
        }
    }

    fn filter_plan(table: &str, lit: i32) -> LogicalPlan {
        LogicalPlan::Filter {
            input: Box::new(scan_plan(table)),
            predicate: ScalarExpr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(ScalarExpr::Column {
                    name: "id".into(),
                    index: 0,
                    data_type: DataType::Int32,
                }),
                right: Box::new(ScalarExpr::Literal {
                    value: Value::Int32(lit),
                    data_type: DataType::Int32,
                }),
                data_type: DataType::Bool,
            },
        }
    }

    fn always_ok_planner(
        plan: LogicalPlan,
    ) -> impl FnOnce(&[Value]) -> Result<LogicalPlan, OptimizeError> {
        move |_| Ok(plan)
    }

    // -----------------------------------------------------------------------
    // Cache construction
    // -----------------------------------------------------------------------

    #[test]
    fn new_cache_is_empty() {
        let cache = PlanCache::new(PlanCacheConfig::default());
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    // -----------------------------------------------------------------------
    // Cache hit on identical params
    // -----------------------------------------------------------------------

    #[test]
    fn first_call_inserts_plan_into_cache() {
        let cache = PlanCache::new(PlanCacheConfig::default());
        let key = PlanCacheKey::named("stmt1");
        let _plan = cache
            .get_or_plan(&key, &[], always_ok_planner(empty_plan()))
            .expect("plan ok");
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn second_call_returns_cached_plan_without_replanning() {
        let cache = PlanCache::new(PlanCacheConfig::default());
        let key = PlanCacheKey::named("stmt1");
        // First call — inserts.
        cache
            .get_or_plan(&key, &[], always_ok_planner(scan_plan("t")))
            .expect("first ok");
        // Second call — should hit cache; planner closure panics if called.
        let plan = cache
            .get_or_plan(&key, &[], |_| {
                panic!("planner should not be called on cache hit")
            })
            .expect("second ok");
        assert!(matches!(plan, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn cache_stores_use_count() {
        let cache = PlanCache::new(PlanCacheConfig::default());
        let key = PlanCacheKey::named("stmt1");
        cache
            .get_or_plan(&key, &[], always_ok_planner(empty_plan()))
            .expect("ok");
        cache
            .get_or_plan(&key, &[], |_| panic!("should not replan"))
            .expect("ok");
        let use_count = cache.entries.get(&key).expect("entry exists").use_count;
        assert_eq!(use_count, 2, "use_count should be 2 after two calls");
    }

    // -----------------------------------------------------------------------
    // Invalidation
    // -----------------------------------------------------------------------

    #[test]
    fn invalidate_removes_matching_entries() {
        let cache = PlanCache::new(PlanCacheConfig::default());
        let key_a = PlanCacheKey::named("stmt_a");
        let key_b = PlanCacheKey::named("stmt_b");
        cache
            .get_or_plan(&key_a, &[], always_ok_planner(empty_plan()))
            .expect("ok");
        cache
            .get_or_plan(&key_b, &[], always_ok_planner(empty_plan()))
            .expect("ok");
        assert_eq!(cache.len(), 2);

        cache.invalidate(|k| k.statement_name == "stmt_a");
        assert_eq!(cache.len(), 1, "stmt_a should be removed");
        assert!(cache.entries.contains_key(&key_b), "stmt_b should remain");
    }

    #[test]
    fn invalidate_all_clears_cache() {
        let cache = PlanCache::new(PlanCacheConfig::default());
        for i in 0..5_u32 {
            let key = PlanCacheKey::named(format!("stmt{i}"));
            cache
                .get_or_plan(&key, &[], always_ok_planner(empty_plan()))
                .expect("ok");
        }
        assert_eq!(cache.len(), 5);
        cache.invalidate_all();
        assert!(cache.is_empty());
    }

    // -----------------------------------------------------------------------
    // LRU eviction
    // -----------------------------------------------------------------------

    #[test]
    fn lru_evicts_when_at_capacity() {
        let config = PlanCacheConfig {
            max_entries: 3,
            replan_threshold: 5.0,
        };
        let cache = PlanCache::new(config);

        for i in 0..3_u32 {
            let key = PlanCacheKey::named(format!("s{i}"));
            cache
                .get_or_plan(&key, &[], always_ok_planner(empty_plan()))
                .expect("ok");
        }
        assert_eq!(cache.len(), 3);

        // Adding a fourth entry should trigger eviction.
        let key4 = PlanCacheKey::named("s3");
        cache
            .get_or_plan(&key4, &[], always_ok_planner(empty_plan()))
            .expect("ok");
        // After eviction one entry is removed and the new one is inserted.
        assert_eq!(cache.len(), 3, "cache should stay at max_entries=3");
    }

    // -----------------------------------------------------------------------
    // Re-plan threshold
    // -----------------------------------------------------------------------

    #[test]
    fn replan_threshold_triggers_custom_plan_when_generic_is_much_more_expensive() {
        // Arrange: inject a generic plan with a known cost and a custom plan
        // that is significantly cheaper.
        let config = PlanCacheConfig {
            replan_threshold: 5.0,
            max_entries: 1_024,
        };
        let cache = PlanCache::new(config);
        let key = PlanCacheKey::named("expensive_stmt");

        // Build generic plan (a Scan — zero cost with NoStats).
        let generic = empty_plan();
        cache
            .get_or_plan(&key, &[], always_ok_planner(generic))
            .expect("first ok");

        // Now override the generic_cost in the entry to simulate a very
        // expensive generic plan.
        {
            let mut entry = cache.entries.get_mut(&key).expect("entry");
            entry.generic_cost = CostEstimate {
                total_cost: 1_000.0,
                startup_cost: 0.0,
                rows: 1_000.0,
                width: 8,
            };
        }

        // The custom planner produces a cheap filter plan.
        // generic_cost (1000) > 5.0 × custom_cost (≈0) → custom plan returned.
        let result = cache
            .get_or_plan(&key, &[Value::Int32(42)], |_| Ok(filter_plan("t", 42)))
            .expect("ok");

        assert!(
            matches!(result, LogicalPlan::Filter { .. }),
            "should have returned custom plan when generic is much more expensive"
        );
    }

    #[test]
    fn eviction_after_consecutive_custom_cheaper_executions() {
        let config = PlanCacheConfig {
            replan_threshold: 0.5, // custom must be less than half the generic cost to trigger
            max_entries: 1_024,
        };
        let cache = PlanCache::new(config);
        let key = PlanCacheKey::named("s");

        cache
            .get_or_plan(&key, &[], always_ok_planner(empty_plan()))
            .expect("ok");

        // Force generic_cost to be large so the threshold is met every time.
        {
            let mut e = cache.entries.get_mut(&key).expect("entry");
            e.generic_cost = CostEstimate {
                total_cost: 1_000.0,
                startup_cost: 0.0,
                rows: 1_000.0,
                width: 8,
            };
        }

        // Drive CUSTOM_PLAN_EVICTION_THRESHOLD consecutive cheaper custom plans.
        for _ in 0..CUSTOM_PLAN_EVICTION_THRESHOLD {
            let _ = cache.get_or_plan(&key, &[Value::Int32(1)], |_| Ok(empty_plan()));
            // Re-set the generic cost because update_entry doesn't change it
            // (only increments streak).
            if let Some(mut e) = cache.entries.get_mut(&key) {
                e.generic_cost = CostEstimate {
                    total_cost: 1_000.0,
                    startup_cost: 0.0,
                    rows: 1_000.0,
                    width: 8,
                };
            }
        }

        // After CUSTOM_PLAN_EVICTION_THRESHOLD calls the entry streak should
        // equal the threshold; the next call evicts and rebuilds.
        let _ = cache.get_or_plan(&key, &[Value::Int32(2)], |_| Ok(scan_plan("t")));
        // After eviction + re-insert, use_count resets to 1.
        if let Some(e) = cache.entries.get(&key) {
            assert_eq!(
                e.custom_cheaper_streak, 0,
                "streak should reset after eviction"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Unnamed statement key
    // -----------------------------------------------------------------------

    #[test]
    fn unnamed_key_is_different_from_named_key() {
        let cache = PlanCache::new(PlanCacheConfig::default());
        let unnamed = PlanCacheKey::unnamed();
        let named = PlanCacheKey::named("s");
        cache
            .get_or_plan(&unnamed, &[], always_ok_planner(empty_plan()))
            .expect("ok");
        cache
            .get_or_plan(&named, &[], always_ok_planner(scan_plan("t")))
            .expect("ok");
        assert_eq!(cache.len(), 2);
    }
}
