//! Planner-facing catalog abstraction.
//!
//! The binder needs a way to resolve a table name to its [`Schema`]. To
//! keep `ultrasql-planner` decoupled from the real catalog (which lives
//! behind MVCC machinery), the planner consumes a small [`Catalog`]
//! trait whose only requirement is table lookup. [`InMemoryCatalog`] is
//! a hash-map-backed implementation used by tests and by short-lived
//! tools (the REPL, EXPLAIN-only tooling) that do not need the full
//! catalog stack.
//!
//! The longer-term plan is to migrate this trait into
//! `ultrasql-catalog` via an RFC; defining it locally here keeps the
//! current bring-up from blocking on that decision.

use std::collections::HashMap;

use ultrasql_core::Schema;

/// Metadata about a single table, sufficient for binding.
///
/// Indexes, statistics, and constraints are *not* present at this
/// layer; the binder only needs to validate column references and
/// shape the produced [`crate::plan::LogicalPlan::Scan`] node. The
/// optimizer can fetch the richer view through a different trait.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableMeta {
    /// Ordered list of columns and their types.
    pub schema: Schema,
}

impl TableMeta {
    /// Construct a `TableMeta` over a schema.
    #[must_use]
    pub const fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

/// Catalog trait consumed by the binder.
///
/// Implementations must be cheap to call: the binder may issue several
/// lookups for a single statement. Implementations are required to be
/// `Send + Sync` so a single catalog handle can be shared across the
/// planner's worker threads.
pub trait Catalog: Send + Sync {
    /// Resolve a (case-insensitive) table name.
    ///
    /// Returns `None` if no table by that name is registered.
    fn lookup_table(&self, name: &str) -> Option<TableMeta>;
}

/// Simple hash-map catalog used by tests and by callers that do not
/// need MVCC-aware lookup.
///
/// Lookups are case-insensitive: the stored key is the ASCII
/// lowercase of the inserted name. Callers that need to register a
/// case-sensitive (quoted) identifier should fold their key
/// themselves before insertion.
#[derive(Clone, Debug, Default)]
pub struct InMemoryCatalog {
    tables: HashMap<String, TableMeta>,
}

impl InMemoryCatalog {
    /// Construct an empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a table. If a table with the same case-folded name
    /// already exists, the previous entry is returned.
    pub fn register(&mut self, name: &str, meta: TableMeta) -> Option<TableMeta> {
        self.tables.insert(name.to_ascii_lowercase(), meta)
    }
}

impl Catalog for InMemoryCatalog {
    fn lookup_table(&self, name: &str) -> Option<TableMeta> {
        self.tables.get(&name.to_ascii_lowercase()).cloned()
    }
}

/// Adapter so the binder can read from a persistent
/// [`ultrasql_catalog::CatalogSnapshot`] directly.
///
/// The persistent catalog hands out immutable snapshots for wait-free
/// reads; this impl projects each `TableEntry` down to the
/// schema-only [`TableMeta`] the binder needs. `lookup_index` and other
/// catalog APIs do not flow through the planner trait, so they are not
/// exposed here.
///
/// The case-folding contract is the same as [`InMemoryCatalog`]: the
/// snapshot stores names already folded to ASCII lowercase, so we fold
/// the query before lookup.
impl Catalog for ultrasql_catalog::CatalogSnapshot {
    fn lookup_table(&self, name: &str) -> Option<TableMeta> {
        self.tables
            .get(&name.to_ascii_lowercase())
            .map(|entry| TableMeta::new(entry.schema.clone()))
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field};

    use super::*;

    fn users_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema invariants hold for test fixture")
    }

    #[test]
    fn lookup_round_trips_case_insensitively() {
        let mut cat = InMemoryCatalog::new();
        cat.register("Users", TableMeta::new(users_schema()));
        assert!(cat.lookup_table("users").is_some());
        assert!(cat.lookup_table("USERS").is_some());
        assert!(cat.lookup_table("UsErS").is_some());
        assert!(cat.lookup_table("orders").is_none());
    }

    #[test]
    fn register_returns_previous_entry() {
        let mut cat = InMemoryCatalog::new();
        let first = TableMeta::new(users_schema());
        assert!(cat.register("users", first.clone()).is_none());
        let replacement = TableMeta::new(
            Schema::new([Field::required("only", DataType::Int64)])
                .expect("schema invariants hold for test fixture"),
        );
        let previous = cat.register("users", replacement);
        assert_eq!(previous, Some(first));
    }
}
