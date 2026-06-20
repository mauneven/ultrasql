//! Unit tests for the heap access layer.
//!
//! Shared fixtures (the in-memory `MapLoader`, option builders, int32-pair
//! payload helpers, and oracles) live here in the module root; the actual
//! test cases are grouped into focused submodules.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use parking_lot::Mutex;
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{CommandId, PageId, Result, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_mvcc::status::{XidStatus, XidStatusOracle};

use super::*;
use crate::page::Page;

mod basic;
mod update;
mod visibility;
mod wal_emission;

/// Test loader that materializes blank heap pages on first miss
/// and persists them keyed by `PageId` so writes from one
/// pin/unpin cycle survive into the next.
#[derive(Default)]
struct MapLoader {
    store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
}

impl MapLoader {
    fn new() -> Self {
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
        // Persist a snapshot so the next `load` for the same id
        // sees the same blank page. Writes through the buffer
        // pool don't flush back into this map by themselves; the
        // tests in this module don't exercise eviction so this
        // is fine.
        let mut copy: Box<[u8; PAGE_SIZE]> = vec![0_u8; PAGE_SIZE]
            .into_boxed_slice()
            .try_into()
            .expect("alloc matches PAGE_SIZE");
        copy.copy_from_slice(page.as_bytes());
        self.store.lock().insert(page_id, copy);
        Ok(page)
    }
}

fn rel() -> RelationId {
    RelationId::new(42)
}

fn opts(xid: u64) -> InsertOptions<'static> {
    InsertOptions {
        xmin: Xid::new(xid),
        command_id: CommandId::FIRST,
        n_atts: 0,
        wal: None,
        fsm: None,
        vm: None,
    }
}

fn opts_with_n_atts(xid: u64, n_atts: u16) -> InsertOptions<'static> {
    InsertOptions {
        xmin: Xid::new(xid),
        command_id: CommandId::FIRST,
        n_atts,
        wal: None,
        fsm: None,
        vm: None,
    }
}

fn del_opts(xmax: u64, cmax: u32) -> DeleteOptions<'static> {
    DeleteOptions {
        xmax: Xid::new(xmax),
        cmax: CommandId::new(cmax),
        wal: None,
        fsm: None,
        vm: None,
    }
}

fn update_int32_scan<'a, O: ?Sized, P>(
    rel: RelationId,
    block_count: u32,
    snapshot: &'a Snapshot,
    oracle: &'a O,
    predicate: P,
) -> UpdateInt32PairScan<'a, O, P> {
    UpdateInt32PairScan {
        rel,
        block_count,
        snapshot,
        oracle,
        predicate,
    }
}

fn update_int32_edit(target_col: u8, delta: i32) -> UpdateInt32PairEdit {
    UpdateInt32PairEdit { target_col, delta }
}

fn update_int32_stamp(xid: u64) -> UpdateInt32PairStamp {
    UpdateInt32PairStamp {
        xid: Xid::new(xid),
        command_id: CommandId::FIRST,
    }
}

fn make_heap(capacity: usize) -> HeapAccess<MapLoader> {
    let pool = Arc::new(BufferPool::new(capacity, MapLoader::new()));
    HeapAccess::new(pool)
}

fn u32_to_usize(value: u32) -> usize {
    usize::try_from(value).expect("test count must fit usize")
}

fn usize_to_u8(value: usize) -> u8 {
    u8::try_from(value).expect("proptest byte seed must fit u8")
}

#[derive(Debug, Default)]
struct CountingOracle {
    inner: MapOracle,
    calls: AtomicUsize,
}

impl CountingOracle {
    fn new() -> Self {
        Self::default()
    }

    fn set_committed(&self, xid: Xid) {
        self.inner.set_committed(xid);
    }

    fn calls(&self) -> usize {
        self.calls.load(AtomicOrdering::Relaxed)
    }
}

impl XidStatusOracle for CountingOracle {
    fn status(&self, xid: Xid) -> XidStatus {
        self.calls.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.status(xid)
    }
}

// -----------------------------------------------------------------------
// UpdateOptions / UpdateOutcome helpers
// -----------------------------------------------------------------------

fn update_opts(xid: u64) -> UpdateOptions<'static> {
    UpdateOptions {
        xid: Xid::new(xid),
        command_id: CommandId::FIRST,
        hot_eligible: true,
        wal: None,
        vm: None,
    }
}

fn int32_pair_payload(id: i32, val: i32) -> [u8; 9] {
    let mut payload = [0_u8; 9];
    payload[1..5].copy_from_slice(&id.to_le_bytes());
    payload[5..9].copy_from_slice(&val.to_le_bytes());
    payload
}

fn int32_pair_from_payload(payload: &[u8]) -> (i32, i32) {
    assert!(payload.len() >= 9, "int32 pair payload must be 9 bytes");
    let id = i32::from_le_bytes(payload[1..5].try_into().unwrap());
    let val = i32::from_le_bytes(payload[5..9].try_into().unwrap());
    (id, val)
}

fn committed_snap(current_xid: u64) -> Snapshot {
    // Snapshot where all xids < 50 are outside the active set.
    Snapshot::new(
        Xid::new(50),
        Xid::new(500),
        Xid::new(current_xid),
        CommandId::FIRST,
        std::iter::empty(),
    )
}
