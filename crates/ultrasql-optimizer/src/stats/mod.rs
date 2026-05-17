//! Statistics subsystem for the UltraSQL cost-based optimizer.
//!
//! This module provides the per-relation and per-column statistics that
//! the cost model uses to estimate operator output cardinalities and I/O
//! costs.
//!
//! ## Structure
//!
//! | Module | Contents |
//! |--------|----------|
//! | [`histogram`] | [`EquiDepthHistogram`] — equi-depth histogram with selectivity estimation |
//! | [`mcv`] | [`MostCommonValues`] — top-K frequent values with frequencies |
//! | [`mod@column`] | [`ColumnStats`] — per-column statistics bundle |
//! | [`mod@relation`] | [`RelationStats`] — per-relation statistics bundle |
//! | [`analyze`] | [`AnalyzeRunner`] — builds `RelationStats` from a row iterator |
//! | [`pg_statistic`] | [`PgStatisticRow`] — in-memory mirror of `pg_statistic` catalog row |
//! | `value_ord` | Internal canonical ordering and hashing for [`ultrasql_core::Value`] |
//!
//! ## Consumer contract
//!
//! The cost model obtains statistics through the [`StatsCatalog`] trait,
//! which returns a [`RelationStats`] by table name. The default in-memory
//! implementation is [`InMemoryStatsCatalog`].

pub mod analyze;
pub mod column;
pub mod histogram;
pub mod mcv;
pub mod pg_statistic;
pub mod relation;
mod value_ord;

pub use analyze::{AnalyzeOptions, AnalyzeRunner};
pub use column::ColumnStats;
pub use histogram::EquiDepthHistogram;
pub use mcv::MostCommonValues;
pub use pg_statistic::PgStatisticRow;
pub use relation::RelationStats;

use ahash::AHashMap;
use ultrasql_core::{Schema, Value};

// ============================================================================
// StatsError
// ============================================================================

/// Errors produced by the statistics subsystem.
#[derive(Debug, thiserror::Error)]
pub enum StatsError {
    /// The row supplied to [`AnalyzeRunner::run`] had the wrong number of
    /// columns.
    #[error("schema arity mismatch: expected {expected}, got {got}")]
    Arity {
        /// Expected arity (schema width).
        expected: usize,
        /// Actual arity (row width).
        got: usize,
    },
    /// A column has a type that the statistics layer does not support.
    #[error("unsupported column type at index {index}: {ty}")]
    UnsupportedType {
        /// 0-based column index.
        index: usize,
        /// The unsupported type.
        ty: ultrasql_core::DataType,
    },
}

// ============================================================================
// StatsCatalog trait
// ============================================================================

/// Read-only statistics source consumed by the cost model.
///
/// Implementations must be `Send + Sync` so they can be shared across
/// the async executor and the `rayon`-style CPU pool without additional
/// synchronization wrappers.
pub trait StatsCatalog: Send + Sync {
    /// Look up the statistics for a relation by its case-folded table name.
    ///
    /// Returns `None` if no statistics are available for the table (e.g.,
    /// `ANALYZE` has never been run on it).
    fn lookup_relation(&self, table: &str) -> Option<RelationStats>;
}

// ============================================================================
// InMemoryStatsCatalog
// ============================================================================

/// Default in-memory [`StatsCatalog`] implementation.
///
/// Statistics are stored in an `AHashMap<String, RelationStats>` keyed by
/// the case-folded table name. This catalog is not persistent: statistics
/// survive only for the lifetime of the process. Persistent storage is a
/// wave 8 task.
///
/// ## Usage
///
/// ```rust
/// use ultrasql_optimizer::stats::{InMemoryStatsCatalog, RelationStats, StatsCatalog};
///
/// let mut catalog = InMemoryStatsCatalog::new();
/// let stats = RelationStats {
///     table: "users".to_owned(),
///     row_count: 10_000,
///     page_count: 50,
///     columns: vec![],
/// };
/// catalog.register(stats);
/// assert!(catalog.lookup_relation("users").is_some());
/// ```
#[derive(Debug, Default)]
pub struct InMemoryStatsCatalog {
    entries: AHashMap<String, RelationStats>,
}

impl InMemoryStatsCatalog {
    /// Create an empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: AHashMap::new(),
        }
    }

    /// Register or replace the statistics for a relation.
    ///
    /// The table name is case-folded before storage so lookups are
    /// case-insensitive.
    pub fn register(&mut self, stats: RelationStats) {
        let key = stats.table.to_lowercase();
        self.entries.insert(key, stats);
    }

    /// Analyze a table and register its statistics.
    ///
    /// This method triggers the AnalyzeRunner to compute statistics for the
    /// specified table and registers the resulting RelationStats in the catalog.
    pub fn analyze_and_register(&mut self, table: &str, schema: &Schema, rows: impl Iterator<Item = Vec<Value>>) {
        let runner = AnalyzeRunner::new(AnalyzeOptions::default());
        if let Ok(stats) = runner.run(table, schema, rows) {
            self.register(stats);
        }
    }
}

impl StatsCatalog for InMemoryStatsCatalog {
    fn lookup_relation(&self, table: &str) -> Option<RelationStats> {
        self.entries.get(&table.to_lowercase()).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_stats(table: &str, row_count: u64) -> RelationStats {
        RelationStats {
            table: table.to_owned(),
            row_count,
            page_count: 1,
            columns: vec![],
        }
    }

    /// Register then lookup returns the registered stats.
    #[test]
    fn register_and_lookup_round_trip() {
        let mut cat = InMemoryStatsCatalog::new();
        cat.register(make_stats("orders", 5_000));
        let stats = cat.lookup_relation("orders").expect("should be found");
        assert_eq!(stats.row_count, 5_000);
        assert_eq!(stats.table, "orders");
    }

    /// Lookup for a missing table returns `None`.
    #[test]
    fn lookup_missing_table_returns_none() {
        let cat = InMemoryStatsCatalog::new();
        assert!(cat.lookup_relation("nonexistent").is_none());
    }

    /// Registering twice with the same name replaces the previous entry.
    #[test]
    fn register_replaces_existing_entry() {
        let mut cat = InMemoryStatsCatalog::new();
        cat.register(make_stats("t", 100));
        cat.register(make_stats("t", 200));
        let stats = cat.lookup_relation("t").expect("should be found");
        assert_eq!(stats.row_count, 200, "second registration should win");
    }

    /// Lookup is case-insensitive.
    #[test]
    fn lookup_is_case_insensitive() {
        let mut cat = InMemoryStatsCatalog::new();
        cat.register(make_stats("Users", 42));
        assert!(cat.lookup_relation("users").is_some());
        assert!(cat.lookup_relation("USERS").is_some());
        assert!(cat.lookup_relation("Users").is_some());
    }

    /// Multiple tables can coexist in the catalog.
    #[test]
    fn multiple_tables_coexist() {
        let mut cat = InMemoryStatsCatalog::new();
        cat.register(make_stats("a", 1));
        cat.register(make_stats("b", 2));
        cat.register(make_stats("c", 3));
        assert_eq!(cat.lookup_relation("a").unwrap().row_count, 1);
        assert_eq!(cat.lookup_relation("b").unwrap().row_count, 2);
        assert_eq!(cat.lookup_relation("c").unwrap().row_count, 3);
    }
}
