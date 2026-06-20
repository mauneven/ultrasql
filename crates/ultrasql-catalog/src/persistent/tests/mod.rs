//! Unit tests for [`super::PersistentCatalog`].
//!
//! Helpers shared across the test submodules live here; the test cases
//! themselves are grouped by topic into sibling files.

pub(crate) use super::*;
pub(crate) use crate::entry::{CompositeTypeEntry, IndexEntry, TableEntry};
pub(crate) use crate::traits::{Catalog, MutableCatalog};
pub(crate) use ultrasql_core::{BlockNumber, DataType, Field, Lsn, Oid, Schema};

pub(super) fn sample_schema() -> Schema {
    Schema::new([
        Field::required("id", DataType::Int64),
        Field::nullable("name", DataType::Text { max_len: None }),
    ])
    .expect("schema invariants hold")
}

pub(super) fn make_table(cat: &PersistentCatalog, name: &str) -> TableEntry {
    TableEntry {
        oid: cat.next_oid(),
        name: name.to_owned(),
        schema_name: "public".to_owned(),
        schema: sample_schema(),
        created_at_lsn: Lsn::ZERO,
        n_blocks: 0,
        root_block: BlockNumber::INVALID,
        options: Vec::new(),
    }
}

pub(super) fn make_table_in_schema(
    cat: &PersistentCatalog,
    schema_name: &str,
    name: &str,
) -> TableEntry {
    let mut entry = make_table(cat, name);
    entry.schema_name = schema_name.to_owned();
    entry
}

/// A blank-page loader: every miss returns a fresh empty heap page.
/// Used to build a `HeapAccess` whose all relations have zero blocks.
pub(super) fn blank_heap() -> HeapAccess<impl PageLoader> {
    use std::sync::Arc;
    use ultrasql_core::PageId;
    use ultrasql_storage::buffer_pool::BufferPool;
    use ultrasql_storage::page::Page;
    let pool = Arc::new(BufferPool::new(16, |_: PageId| Ok(Page::new_heap())));
    HeapAccess::new(pool)
}

mod basic;
mod bootstrap;
mod mutations;
mod snapshot;
