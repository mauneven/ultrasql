//! VACUUM kernel integration tests — undo-log GC.
//!
//! Exercises [`ultrasql_storage::heap::HeapAccess::vacuum_undo_log`].
//! The test workload writes in-place UPDATEs from several xids, then
//! advances the oldest-active xid threshold and asserts that the GC
//! removes only entries whose writer is now invisible.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    reason = "integration test: index arithmetic against compile-time loop bounds"
)]

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{CommandId, PageId, RelationId, Result, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{HeapAccess, InsertOptions};
use ultrasql_storage::page::Page;

#[derive(Default)]
struct MapLoader {
    store: Mutex<HashMap<PageId, Box<[u8; PAGE_SIZE]>>>,
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
                .map_err(|e| ultrasql_core::Error::Corruption(format!("map loader: {e}")));
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

const fn rel() -> RelationId {
    RelationId::new(7)
}

fn pair_payload(id: i32, val: i32) -> [u8; 9] {
    let mut out = [0_u8; 9];
    out[1..5].copy_from_slice(&id.to_le_bytes());
    out[5..9].copy_from_slice(&val.to_le_bytes());
    out
}

fn make_heap() -> HeapAccess<MapLoader> {
    let pool = Arc::new(BufferPool::new(64, MapLoader::default()));
    HeapAccess::new(pool)
}

#[test]
fn vacuum_undo_log_drops_only_below_threshold() {
    const ROWS: usize = 50;
    let heap = make_heap();
    let oracle = MapOracle::new();

    // Seed the relation with ROWS rows under xid 1 (committed).
    oracle.set_committed(Xid::new(1));
    for i in 0..ROWS {
        let bytes = pair_payload(i as i32, (i as i32) * 3);
        heap.insert(
            rel(),
            &bytes,
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .expect("insert");
    }
    let n_blocks = heap.block_count(rel());

    // Two committed in-place UPDATE batches under xid=2 and xid=4.
    // Each batch updates every row, so the undo log accumulates
    // 2 * ROWS entries (50 per writer for the most-recent state of
    // the slot, plus 50 from the previous writer — UPDATE pushes one
    // entry per visited row per pass).
    for xid_raw in [2_u64, 4_u64] {
        oracle.set_committed(Xid::new(xid_raw));
        let snap = Snapshot::new(
            Xid::new(xid_raw),
            Xid::new(xid_raw + 1),
            Xid::new(xid_raw),
            CommandId::FIRST,
            std::iter::empty(),
        );
        heap.update_int32_pair_inplace_undo(
            rel(),
            n_blocks,
            &snap,
            &oracle,
            |_id, _val| true,
            1,
            1,
            Xid::new(xid_raw),
            CommandId::FIRST,
            None,
        )
        .expect("update");
    }
    assert_eq!(
        heap.undo_log_len(rel()),
        2 * ROWS,
        "two committed writers should leave 2 * ROWS entries"
    );

    // Oldest active xid = 3 → trim every writer < 3 (xid=2 only).
    let trimmed = heap.vacuum_undo_log(Xid::new(3)).expect("vacuum");
    assert_eq!(trimmed, ROWS, "every xid=2 entry should be dropped");
    assert_eq!(
        heap.undo_log_len(rel()),
        ROWS,
        "only the xid=4 entries should remain"
    );

    // Oldest active xid = 5 → trim the remaining xid=4 entries too.
    let trimmed = heap.vacuum_undo_log(Xid::new(5)).expect("vacuum");
    assert_eq!(trimmed, ROWS);
    assert_eq!(heap.undo_log_len(rel()), 0);

    // Second call at the same threshold is a no-op.
    let trimmed = heap.vacuum_undo_log(Xid::new(5)).expect("vacuum");
    assert_eq!(trimmed, 0);
}

#[test]
fn vacuum_undo_log_no_op_when_threshold_below_every_writer() {
    let heap = make_heap();
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(10));
    oracle.set_committed(Xid::new(20));

    for i in 0..16_i32 {
        let bytes = pair_payload(i, i * 2);
        heap.insert(
            rel(),
            &bytes,
            InsertOptions {
                xmin: Xid::new(10),
                command_id: CommandId::FIRST,
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .expect("insert");
    }
    let snap = Snapshot::new(
        Xid::new(20),
        Xid::new(21),
        Xid::new(20),
        CommandId::FIRST,
        std::iter::empty(),
    );
    heap.update_int32_pair_inplace_undo(
        rel(),
        heap.block_count(rel()),
        &snap,
        &oracle,
        |_, _| true,
        0,
        7,
        Xid::new(20),
        CommandId::FIRST,
        None,
    )
    .expect("update");

    let before = heap.undo_log_len(rel());
    assert!(before > 0);
    // Threshold strictly below the writer xid — no entries qualify.
    let trimmed = heap.vacuum_undo_log(Xid::new(5)).expect("vacuum");
    assert_eq!(trimmed, 0);
    assert_eq!(heap.undo_log_len(rel()), before);
}
