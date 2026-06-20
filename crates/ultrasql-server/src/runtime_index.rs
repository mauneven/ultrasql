//! Runtime index/aggregating-index metadata, validation report types, and
//! runtime constraint descriptors.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

/// Runtime metadata for one index beyond plain attnum keys.
#[derive(Clone, Debug, Default)]
pub struct RuntimeIndexMetadata {
    /// Bound key expressions. Empty means use the catalog entry's key columns.
    pub key_exprs: Vec<ScalarExpr>,
    /// Bound partial-index predicate, if any.
    pub predicate: Option<ScalarExpr>,
    /// 0-based table columns listed in `INCLUDE (...)`.
    pub include_columns: Vec<usize>,
    /// Access method requested by `USING`.
    pub method: LogicalIndexMethod,
    /// In-memory BRIN min/max summaries for block-range pruning.
    pub brin: Option<Arc<ultrasql_storage::access_method::BrinIndex>>,
    /// Page-backed HNSW graph for vector top-k scans.
    pub hnsw: Option<Arc<ultrasql_storage::access_method::PageBackedHnswIndex>>,
    /// Page-backed IVFFlat inverted lists for vector top-k scans.
    pub ivfflat: Option<Arc<ultrasql_storage::access_method::PageBackedIvfFlatIndex>>,
    /// Runtime aggregating-index summary for dashboard-style GROUP BY scans.
    pub aggregating: Option<Arc<RuntimeAggregatingIndex>>,
}

/// Process-wide ANN/vector-index counters for ops metrics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AnnSystemMetrics {
    /// Approximate candidate count available across runtime ANN sidecars.
    pub candidates: u64,
    /// Tombstoned ANN entries waiting for VACUUM cleanup.
    pub tombstones: u64,
    /// Approximate memory footprint of page-backed vector-index pages.
    pub vector_index_memory_bytes: u64,
    /// Number of runtime HNSW indexes.
    pub hnsw_indexes: u64,
    /// Number of runtime IVFFlat indexes.
    pub ivfflat_indexes: u64,
}

/// One admin validation check result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationCheck {
    /// Stable machine-readable check name.
    pub name: &'static str,
    /// Check outcome.
    pub status: ValidationStatus,
    /// Human-readable evidence for the outcome.
    pub detail: String,
}

/// Admin validation status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationStatus {
    /// Check passed.
    Ok,
    /// Check failed and should make `ultrasql validate` exit non-zero.
    Failed,
}

impl ValidationStatus {
    /// Lowercase status for CLI output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
        }
    }
}

/// Full admin validation report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ValidationReport {
    /// Ordered checks run by the validator.
    pub checks: Vec<ValidationCheck>,
}

impl ValidationReport {
    /// Return true when every check passed.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.checks
            .iter()
            .all(|check| check.status == ValidationStatus::Ok)
    }
}

/// Runtime sidecar for `CREATE AGGREGATING INDEX`.
#[derive(Debug)]
pub struct RuntimeAggregatingIndex {
    /// Bound aggregating-index metadata.
    pub spec: ultrasql_planner::LogicalAggregatingIndex,
    /// Materialized summary rows in `group columns + aggregates` order.
    pub rows: std::sync::RwLock<Vec<Vec<Value>>>,
    /// Set when DML touched the base table after the last summary build.
    pub dirty: std::sync::atomic::AtomicBool,
    pub(crate) explain_stats: RuntimeAggregatingIndexExplainStats,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AggregatingIndexExplainStats {
    pub aggregating_index_used: bool,
    pub stale_rebuild_used: bool,
    pub summary_rows_read: u64,
    pub base_rows_skipped: u64,
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeAggregatingIndexExplainStats {
    aggregating_index_used: std::sync::atomic::AtomicBool,
    stale_rebuild_used: std::sync::atomic::AtomicBool,
    summary_rows_read: std::sync::atomic::AtomicU64,
    base_rows_skipped: std::sync::atomic::AtomicU64,
}

impl RuntimeAggregatingIndex {
    /// Build a clean runtime summary.
    #[must_use]
    pub fn new(spec: ultrasql_planner::LogicalAggregatingIndex, rows: Vec<Vec<Value>>) -> Self {
        Self {
            spec,
            rows: std::sync::RwLock::new(rows),
            dirty: std::sync::atomic::AtomicBool::new(false),
            explain_stats: RuntimeAggregatingIndexExplainStats::default(),
        }
    }

    /// Mark summary rows stale. Next matching read rebuilds lazily.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn record_explain_read(
        &self,
        stale_rebuild_used: bool,
        summary_rows_read: usize,
        base_rows_skipped: u64,
    ) {
        self.explain_stats
            .aggregating_index_used
            .store(true, std::sync::atomic::Ordering::Release);
        self.explain_stats
            .stale_rebuild_used
            .store(stale_rebuild_used, std::sync::atomic::Ordering::Release);
        self.explain_stats.summary_rows_read.store(
            u64::try_from(summary_rows_read).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Release,
        );
        self.explain_stats
            .base_rows_skipped
            .store(base_rows_skipped, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn explain_stats_snapshot(&self) -> AggregatingIndexExplainStats {
        AggregatingIndexExplainStats {
            aggregating_index_used: self
                .explain_stats
                .aggregating_index_used
                .load(std::sync::atomic::Ordering::Acquire),
            stale_rebuild_used: self
                .explain_stats
                .stale_rebuild_used
                .load(std::sync::atomic::Ordering::Acquire),
            summary_rows_read: self
                .explain_stats
                .summary_rows_read
                .load(std::sync::atomic::Ordering::Acquire),
            base_rows_skipped: self
                .explain_stats
                .base_rows_skipped
                .load(std::sync::atomic::Ordering::Acquire),
        }
    }
}

/// One runtime CHECK constraint.
#[derive(Clone, Debug)]
pub struct RuntimeCheckConstraint {
    /// Constraint name reported on violation.
    pub name: String,
    /// Boolean expression bound against the table row schema.
    pub expr: ScalarExpr,
}

/// One runtime FOREIGN KEY constraint.
#[derive(Clone, Debug)]
pub struct RuntimeForeignKeyConstraint {
    /// Constraint name reported on violation.
    pub name: String,
    /// Referencing table column indices.
    pub columns: Vec<usize>,
    /// Referenced table name.
    pub target_table: String,
    /// Referenced table OID.
    pub target_oid: ultrasql_core::Oid,
    /// Referenced table column indices.
    pub target_columns: Vec<usize>,
    /// Action when a referenced row is deleted.
    pub on_delete: ultrasql_planner::LogicalReferentialAction,
    /// Action when a referenced key is updated.
    pub on_update: ultrasql_planner::LogicalReferentialAction,
    /// Whether this constraint may be checked at transaction commit.
    pub deferrable: bool,
    /// Whether this deferrable constraint starts in deferred mode.
    pub initially_deferred: bool,
}

/// One runtime EXCLUDE constraint.
#[derive(Clone, Debug)]
pub struct RuntimeExclusionConstraint {
    /// Constraint name reported on violation.
    pub name: String,
    /// Access method requested by `USING`.
    pub method: LogicalIndexMethod,
    /// 0-based table column indices plus operators.
    pub elements: Vec<RuntimeExclusionElement>,
}

/// One runtime EXCLUDE element.
#[derive(Clone, Debug)]
pub struct RuntimeExclusionElement {
    /// Table column index.
    pub column: usize,
    /// Operator applied to `(new_value, existing_value)`.
    pub op: BinaryOp,
}
