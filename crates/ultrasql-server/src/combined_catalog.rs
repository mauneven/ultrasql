//! Read-only combined catalog view (persistent snapshot + sample fallback)
//! consulted by the binder.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

/// Read-only catalog view consulted by the binder during query
/// execution.
///
/// The persistent catalog (`PersistentCatalog`) is the source of truth
/// for user-created relations; the in-memory `InMemoryCatalog` carries
/// the legacy sample-table registry (the v0.5 hard-coded `users`
/// fixture). Lookups try the persistent snapshot first so a runtime
/// `CREATE TABLE` immediately shadows any sample-table name collision;
/// if the snapshot has no entry, we fall back to the sample-table
/// catalog so existing duplex tests still resolve `users`.
///
/// The `'a` lifetime ties the view to the snapshot and in-memory
/// catalog held by the calling [`Session`]; binding completes
/// synchronously inside `execute_query` so the lifetime never escapes
/// a single statement.
pub(crate) struct CombinedCatalog<'a> {
    pub(crate) snapshot: &'a CatalogSnapshot,
    pub(crate) fallback: &'a InMemoryCatalog,
    pub(crate) search_path: Option<&'a str>,
}

impl PlannerCatalog for CombinedCatalog<'_> {
    fn lookup_table(&self, name: &str) -> Option<TableMeta> {
        if let Some(schema) = pipeline::catalog_views::virtual_catalog_schema(name) {
            return Some(TableMeta::new(schema));
        }
        for schema_name in search_path_schema_names(self.search_path) {
            if let Some(meta) =
                PlannerCatalog::lookup_table_in_schema(self.snapshot, &schema_name, name)
            {
                return Some(meta);
            }
            if let Some(meta) =
                PlannerCatalog::lookup_table_in_schema(self.fallback, &schema_name, name)
            {
                return Some(meta);
            }
        }
        None
    }

    fn lookup_table_in_schema(&self, schema_name: &str, name: &str) -> Option<TableMeta> {
        let table_key = ultrasql_catalog::table_lookup_key(schema_name, name);
        if let Some(schema) = pipeline::catalog_views::virtual_catalog_schema(&table_key) {
            return Some(TableMeta::with_schema_name(schema_name, schema));
        }
        PlannerCatalog::lookup_table_in_schema(self.snapshot, schema_name, name)
            .or_else(|| PlannerCatalog::lookup_table_in_schema(self.fallback, schema_name, name))
    }

    fn lookup_type(&self, name: &str) -> Option<DataType> {
        for schema_name in search_path_schema_names(self.search_path) {
            if let Some(data_type) =
                PlannerCatalog::lookup_type_in_schema(self.snapshot, &schema_name, name)
            {
                return Some(data_type);
            }
            if let Some(data_type) =
                PlannerCatalog::lookup_type_in_schema(self.fallback, &schema_name, name)
            {
                return Some(data_type);
            }
        }
        type_name_namespace_and_name(name)
            .and_then(|(schema_name, type_name)| self.lookup_type_in_schema(schema_name, type_name))
    }

    fn lookup_type_in_schema(&self, schema_name: &str, name: &str) -> Option<DataType> {
        PlannerCatalog::lookup_type_in_schema(self.snapshot, schema_name, name)
            .or_else(|| PlannerCatalog::lookup_type_in_schema(self.fallback, schema_name, name))
    }

    fn lookup_index(&self, name: &str) -> bool {
        if search_path_schema_names(self.search_path)
            .into_iter()
            .any(|schema_name| {
                PlannerCatalog::lookup_index_in_schema(self.snapshot, &schema_name, name)
                    || PlannerCatalog::lookup_index_in_schema(self.fallback, &schema_name, name)
            })
        {
            return true;
        }
        type_name_namespace_and_name(name).is_some_and(|(schema_name, index_name)| {
            self.lookup_index_in_schema(schema_name, index_name)
        })
    }

    fn lookup_index_in_schema(&self, schema_name: &str, name: &str) -> bool {
        PlannerCatalog::lookup_index_in_schema(self.snapshot, schema_name, name)
            || PlannerCatalog::lookup_index_in_schema(self.fallback, schema_name, name)
    }

    fn lookup_index_schema(&self, name: &str) -> Option<String> {
        search_path_schema_names(self.search_path)
            .into_iter()
            .find(|schema_name| self.lookup_index_in_schema(schema_name, name))
    }

    fn lookup_table_oid(&self, name: &str) -> Option<Oid> {
        for schema_name in search_path_schema_names(self.search_path) {
            if let Some(oid) =
                PlannerCatalog::lookup_table_oid_in_schema(self.snapshot, &schema_name, name)
            {
                return Some(oid);
            }
            if let Some(oid) =
                PlannerCatalog::lookup_table_oid_in_schema(self.fallback, &schema_name, name)
            {
                return Some(oid);
            }
        }
        type_name_namespace_and_name(name).and_then(|(schema_name, table_name)| {
            self.lookup_table_oid_in_schema(schema_name, table_name)
        })
    }

    fn lookup_table_oid_in_schema(&self, schema_name: &str, name: &str) -> Option<Oid> {
        PlannerCatalog::lookup_table_oid_in_schema(self.snapshot, schema_name, name).or_else(|| {
            PlannerCatalog::lookup_table_oid_in_schema(self.fallback, schema_name, name)
        })
    }

    fn lookup_type_oid(&self, name: &str) -> Option<Oid> {
        for schema_name in search_path_schema_names(self.search_path) {
            if let Some(oid) =
                PlannerCatalog::lookup_type_oid_in_schema(self.snapshot, &schema_name, name)
            {
                return Some(oid);
            }
            if let Some(oid) =
                PlannerCatalog::lookup_type_oid_in_schema(self.fallback, &schema_name, name)
            {
                return Some(oid);
            }
        }
        type_name_namespace_and_name(name).and_then(|(schema_name, type_name)| {
            self.lookup_type_oid_in_schema(schema_name, type_name)
        })
    }

    fn lookup_type_oid_in_schema(&self, schema_name: &str, name: &str) -> Option<Oid> {
        PlannerCatalog::lookup_type_oid_in_schema(self.snapshot, schema_name, name)
            .or_else(|| PlannerCatalog::lookup_type_oid_in_schema(self.fallback, schema_name, name))
    }

    fn table_schema_visible_without_qualification(&self, schema_name: &str) -> bool {
        search_path_contains_schema(self.search_path, schema_name)
    }
}
