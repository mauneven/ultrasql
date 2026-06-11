//! VACUUM kernel integration tests — undo-log GC.
//!
//! Exercises [`ultrasql_storage::heap::HeapAccess::vacuum_undo_log`].
//! The test workload writes in-place UPDATEs from several xids, then
//! advances the oldest-active xid threshold and asserts that the GC
//! removes only entries whose writer is now invisible.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{CommandId, PageId, RelationId, Result, Xid};
use ultrasql_mvcc::Snapshot;
use ultrasql_mvcc::status::test_support::MapOracle;
use ultrasql_storage::buffer_pool::{BufferPool, PageLoader};
use ultrasql_storage::heap::{
    DeleteInt32PairScan, DeleteInt32PairStamp, HeapAccess, InsertOptions, UpdateInt32PairEdit,
    UpdateInt32PairScan, UpdateInt32PairStamp,
};
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

fn pair_decode(bytes: &[u8]) -> (i32, i32) {
    assert_eq!(bytes.len(), 9);
    let id = i32::from_le_bytes(bytes[1..5].try_into().expect("id slice"));
    let val = i32::from_le_bytes(bytes[5..9].try_into().expect("val slice"));
    (id, val)
}

fn update_int32_scan<'a, O: ?Sized, P>(
    block_count: u32,
    snapshot: &'a Snapshot,
    oracle: &'a O,
    predicate: P,
) -> UpdateInt32PairScan<'a, O, P> {
    UpdateInt32PairScan {
        rel: rel(),
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

fn make_heap() -> HeapAccess<MapLoader> {
    let pool = Arc::new(BufferPool::new(64, MapLoader::default()));
    HeapAccess::new(pool)
}

fn usize_to_i32(value: usize) -> i32 {
    i32::try_from(value).expect("test row index must fit i32")
}

fn visible_pairs(
    heap: &HeapAccess<MapLoader>,
    block_count: u32,
    snapshot: &Snapshot,
    oracle: &MapOracle,
) -> Vec<(i32, i32)> {
    let mut out = Vec::new();
    heap.for_each_visible(
        rel(),
        block_count,
        snapshot,
        oracle,
        |_tid, _header, data| {
            out.push(pair_decode(data));
            Ok(())
        },
    )
    .expect("visible scan");
    out.sort_by_key(|(id, _)| *id);
    out
}

#[test]
fn vacuum_undo_log_drops_only_below_threshold() {
    const ROWS: usize = 50;
    let heap = make_heap();
    let oracle = MapOracle::new();

    // Seed the relation with ROWS rows under xid 1 (committed).
    oracle.set_committed(Xid::new(1));
    for i in 0..ROWS {
        let id = usize_to_i32(i);
        let bytes = pair_payload(id, id * 3);
        heap.insert(
            rel(),
            &bytes,
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                n_atts: 2,
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .expect("insert");
    }
    let n_blocks = heap.block_count(rel());

    // Two committed in-place UPDATE batches under xid=2 and xid=4.
    // Each batch updates every row, so compact undo represents
    // 2 * ROWS pre-images without storing one full entry per row.
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
            update_int32_scan(n_blocks, &snap, &oracle, |_id, _val| true),
            update_int32_edit(1, 1),
            update_int32_stamp(xid_raw),
            None,
            None,
        )
        .expect("update");
    }
    assert_eq!(
        heap.undo_log_len(rel()),
        0,
        "bulk int32 updates use compact undo batches"
    );
    assert_eq!(
        heap.int32_pair_undo_slot_len(rel()),
        2 * ROWS,
        "two committed writers should retain 2 * ROWS pre-images"
    );

    // Oldest active xid = 3 → trim every writer < 3 (xid=2 only).
    let trimmed = heap.vacuum_undo_log(Xid::new(3)).expect("vacuum");
    assert_eq!(trimmed, ROWS, "every xid=2 pre-image should be dropped");
    assert_eq!(
        heap.int32_pair_undo_slot_len(rel()),
        ROWS,
        "only the xid=4 pre-images should remain"
    );

    // Oldest active xid = 5 → trim the remaining xid=4 entries too.
    let trimmed = heap.vacuum_undo_log(Xid::new(5)).expect("vacuum");
    assert_eq!(trimmed, ROWS);
    assert_eq!(heap.int32_pair_undo_slot_len(rel()), 0);
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
                n_atts: 2,
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
        update_int32_scan(heap.block_count(rel()), &snap, &oracle, |_, _| true),
        update_int32_edit(0, 7),
        update_int32_stamp(20),
        None,
        None,
    )
    .expect("update");

    let before = heap.int32_pair_undo_slot_len(rel());
    assert!(before > 0);
    // Threshold strictly below the writer xid — no entries qualify.
    let trimmed = heap.vacuum_undo_log(Xid::new(5)).expect("vacuum");
    assert_eq!(trimmed, 0);
    assert_eq!(heap.int32_pair_undo_slot_len(rel()), before);
}

#[test]
fn long_snapshot_survives_update_delete_and_vacuum() {
    const ROWS: usize = 12;
    let heap = make_heap();
    let oracle = MapOracle::new();
    oracle.set_committed(Xid::new(1));

    let expected_original: Vec<(i32, i32)> = (0..ROWS)
        .map(|i| {
            let id = usize_to_i32(i);
            (id, id * 10)
        })
        .collect();
    for (id, val) in &expected_original {
        heap.insert(
            rel(),
            &pair_payload(*id, *val),
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                n_atts: 2,
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .expect("insert");
    }
    let block_count = heap.block_count(rel());
    let long_snapshot = Snapshot::new(
        Xid::new(2),
        Xid::new(2),
        Xid::new(100),
        CommandId::FIRST,
        std::iter::empty(),
    );

    let writer_update = Snapshot::new(
        Xid::new(2),
        Xid::new(3),
        Xid::new(2),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let updated = heap
        .update_int32_pair_inplace_undo(
            update_int32_scan(block_count, &writer_update, &oracle, |id, _val| id % 2 == 0),
            update_int32_edit(1, 1000),
            update_int32_stamp(2),
            None,
            None,
        )
        .expect("update");
    oracle.set_committed(Xid::new(2));
    assert_eq!(updated, ROWS / 2);
    assert_eq!(heap.undo_log_len(rel()), 0);
    assert_eq!(heap.int32_pair_undo_slot_len(rel()), updated);

    let writer_delete = Snapshot::new(
        Xid::new(3),
        Xid::new(4),
        Xid::new(3),
        CommandId::FIRST,
        std::iter::empty(),
    );
    let deleted = heap
        .delete_int32_pair_inplace(
            DeleteInt32PairScan {
                rel: rel(),
                block_count,
                snapshot: &writer_delete,
                oracle: &oracle,
                predicate: |id, _val| id % 2 != 0 && id % 3 == 0,
            },
            DeleteInt32PairStamp {
                xid: Xid::new(3),
                command_id: CommandId::FIRST,
            },
            None,
            None,
        )
        .expect("delete");
    oracle.set_committed(Xid::new(3));
    assert_eq!(deleted, 2);

    let heap_vacuum = heap
        .vacuum_heap(rel(), Xid::new(2), &oracle)
        .expect("heap vacuum");
    assert_eq!(heap_vacuum.tuples_reclaimed, 0);
    let undo_trimmed = heap.vacuum_undo_log(Xid::new(2)).expect("undo vacuum");
    assert_eq!(undo_trimmed, 0);

    assert_eq!(
        visible_pairs(&heap, block_count, &long_snapshot, &oracle),
        expected_original,
        "long reader must keep its original view across writers and vacuum"
    );
}
