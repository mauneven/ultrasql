//! Same-table columnar secondary-storage runtime.
//!
//! Heap rows remain the authoritative OLTP layout. This runtime tracks
//! which heap relations have a same-version columnar shadow in
//! [`HeapAccess::column_cache`][ultrasql_storage::heap::HeapAccess::column_cache]
//! and which tables need background rebuild after committed DML.

use dashmap::DashMap;
use ultrasql_core::RelationId;

/// Rebuild metadata for one same-table columnar shadow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnarRelationStats {
    /// Folded table name.
    pub table_name: String,
    /// Heap relation whose rows feed the shadow layout.
    pub relation: RelationId,
    /// Heap column-cache version represented by the shadow layout.
    pub version: u64,
    /// Visible row count captured in the columnar shadow.
    pub row_count: usize,
    /// Number of logical columnar segments.
    pub segment_count: usize,
    /// `true` after committed DML and before the next successful rebuild.
    pub dirty: bool,
    /// Number of successful rebuilds in this server process.
    pub rebuilds: u64,
}

/// Process-local registry and rebuild queue for columnar shadows.
#[derive(Debug, Default)]
pub struct ColumnarSecondaryStore {
    stats: DashMap<String, ColumnarRelationStats>,
    pending: DashMap<String, ()>,
}

impl ColumnarSecondaryStore {
    /// Construct an empty columnar secondary-store registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark `table` dirty and queue it for a background rebuild.
    pub fn mark_dirty(&self, table: impl AsRef<str>) {
        let table = table.as_ref().to_ascii_lowercase();
        self.pending.insert(table.clone(), ());
        if let Some(mut stats) = self.stats.get_mut(&table) {
            stats.dirty = true;
        }
    }

    /// Pop one pending table name, if any.
    pub fn pop_pending(&self) -> Option<String> {
        let table = self
            .pending
            .iter()
            .next()
            .map(|entry| entry.key().clone())?;
        self.pending.remove(&table);
        Some(table)
    }

    /// Record a successful columnar rebuild.
    pub fn record_rebuild(
        &self,
        table: impl Into<String>,
        relation: RelationId,
        version: u64,
        row_count: usize,
        segment_count: usize,
    ) {
        let table_name = table.into().to_ascii_lowercase();
        let rebuilds = self
            .stats
            .get(&table_name)
            .map_or(1, |old| old.rebuilds.saturating_add(1));
        self.stats.insert(
            table_name.clone(),
            ColumnarRelationStats {
                table_name,
                relation,
                version,
                row_count,
                segment_count,
                dirty: false,
                rebuilds,
            },
        );
    }

    /// Drop runtime state for a removed table.
    pub fn remove(&self, table: impl AsRef<str>) {
        let table = table.as_ref().to_ascii_lowercase();
        self.pending.remove(&table);
        self.stats.remove(&table);
    }

    /// Look up current columnar shadow stats for `table`.
    #[must_use]
    pub fn stats(&self, table: impl AsRef<str>) -> Option<ColumnarRelationStats> {
        self.stats
            .get(&table.as_ref().to_ascii_lowercase())
            .map(|entry| entry.value().clone())
    }
}
