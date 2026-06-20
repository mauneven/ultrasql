//! Unit tests for the [`SeqScan`](super::SeqScan) operator and its
//! supporting helpers.
//!
//! Shared fixtures (the in-memory `MapLoader`, schema/oracle/snapshot
//! builders, and the `drain_rows` collector) live here; the per-topic
//! `#[test]` functions are split into the sibling submodules below and
//! pull these in via `use super::*`.

mod build_batch_tests;
mod scan;

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{CommandId, DataType, Field, PageId, RelationId, Result, Schema, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{HeapAccess, InsertOptions};
use ultrasql_storage::page::Page;
use ultrasql_vec::column::Column;

use crate::Operator;

/// In-memory page loader that materialises blank heap pages on first miss
/// and persists them across evictions.
#[derive(Default, Debug)]
pub(super) struct MapLoader {
    store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
}

impl MapLoader {
    pub(super) fn new() -> Self {
        Self::default()
    }
}

impl PageLoader for MapLoader {
    fn load(&self, page_id: PageId) -> Result<Page> {
        let stored = {
            let store = self.store.lock();
            store.get(&page_id).map(|b| {
                let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
                    .into_boxed_slice()
                    .try_into()
                    .expect("alloc matches PAGE_SIZE");
                copy.copy_from_slice(&**b);
                copy
            })
        };
        if let Some(bytes) = stored {
            return Page::from_bytes(bytes)
                .map_err(|e| ultrasql_core::Error::Corruption(format!("test loader: {e}")));
        }
        let page = Page::new_heap();
        let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
            .into_boxed_slice()
            .try_into()
            .expect("alloc matches PAGE_SIZE");
        copy.copy_from_slice(page.as_bytes());
        self.store.lock().insert(page_id, copy);
        Ok(page)
    }
}

pub(super) fn rel() -> RelationId {
    RelationId::new(1)
}

pub(super) fn make_heap() -> Arc<HeapAccess<MapLoader>> {
    let pool = Arc::new(BufferPool::new(64, MapLoader::new()));
    Arc::new(HeapAccess::new(pool))
}

pub(super) fn snap_for(xid: u64) -> Snapshot {
    Snapshot::new(
        Xid::new(xid + 1),
        Xid::new(xid + 2),
        Xid::new(xid + 1),
        CommandId::FIRST,
        [],
    )
}

pub(super) fn insert_opts(xid: u64) -> InsertOptions<'static> {
    InsertOptions {
        xmin: Xid::new(xid),
        command_id: CommandId::FIRST,
        n_atts: 0,
        wal: None,
        fsm: None,
        vm: None,
    }
}

pub(super) fn schema_i32_text() -> Schema {
    Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("name", DataType::Text { max_len: None }),
    ])
    .expect("schema ok")
}

pub(super) fn schema_i32_only() -> Schema {
    Schema::new([Field::required("id", DataType::Int32)]).expect("schema ok")
}

pub(super) fn drain_rows(scan: &mut dyn Operator) -> Vec<(i32, String)> {
    let mut out = Vec::new();
    while let Some(batch) = scan.next_batch().expect("operator must not error") {
        let cols = batch.columns();
        assert_eq!(cols.len(), 2);
        let ids = match &cols[0] {
            Column::Int32(c) => c.data().to_vec(),
            other => panic!("expected Int32, got {other:?}"),
        };
        let names: Vec<String> = match &cols[1] {
            col @ (Column::Utf8(_) | Column::DictionaryUtf8(_)) => (0..col.len())
                .map(|i| {
                    col.text_value(i)
                        .expect("test scan text column should be non-null")
                        .to_owned()
                })
                .collect(),
            other => panic!("expected Utf8, got {other:?}"),
        };
        assert_eq!(ids.len(), names.len());
        for (id, name) in ids.into_iter().zip(names) {
            out.push((id, name));
        }
    }
    out
}
