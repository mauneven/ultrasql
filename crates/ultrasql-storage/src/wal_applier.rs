//! WAL replay implementation for the heap access method.
//!
//! Implements [`ultrasql_wal::HeapTarget`] for [`HeapAccess<L>`] so the WAL
//! recovery path can drive page-level redo directly through the heap's buffer
//! pool, bypassing WAL emission (which would cause an infinite loop).
//!
//! # Contract
//!
//! Each `apply_*` method is idempotent with respect to the on-page state: if
//! the page already reflects the mutation (page LSN ≥ record LSN) the method
//! is a no-op. This property is required for crash recovery when some pages
//! were flushed to disk before the crash but others were not.
//!
//! # Block counter maintenance
//!
//! [`HeapAccess`] maintains an internal per-relation block counter for v0.5.
//! The applier advances the counter whenever it writes to a block whose number
//! equals or exceeds the current counter value. This ensures that a
//! post-recovery scan driven by `block_count()` covers all replayed blocks.
//!
//! # Visibility
//!
//! The applier does not enforce MVCC visibility — it replays every WAL record
//! regardless of transaction outcome. The caller is responsible for filtering
//! aborted transactions, either by using the commit/abort records to drive a
//! CLOG update and then running a visibility filter, or by replaying only
//! records from committed transactions.

use std::sync::atomic::Ordering;

use ultrasql_core::BlockNumber;
use ultrasql_mvcc::TupleHeader;
use ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE;
use ultrasql_wal::applier::{ApplyError, HeapTarget};
use ultrasql_wal::payload::{
    FullPageWritePayload, HeapDeletePayload, HeapInsertPayload, HeapUpdatePayload,
};

use crate::buffer_pool::PageLoader;
use crate::heap::HeapAccess;
use crate::page::PageError;

impl<L: PageLoader + 'static> HeapTarget for HeapAccess<L> {
    /// Apply a heap-insert record by writing the tuple bytes into the correct
    /// slot on the target page.
    ///
    /// The method calls `insert_tuple` on the page, which appends to the next
    /// available slot. During a clean forward replay the slot number assigned
    /// by the page will match `payload.tid.slot`. If the slot already has
    /// valid data (the page was already flushed past this LSN) the insertion
    /// is skipped.
    #[allow(clippy::significant_drop_tightening)]
    fn apply_insert(&self, payload: &HeapInsertPayload) -> Result<(), ApplyError> {
        let page_id = payload.tid.page;
        let rel = page_id.relation;

        // Ensure the block counter reflects this block.
        self.advance_counter(rel, page_id.block);

        // Obtain an exclusive pin on the page.
        let guard = self
            .pool
            .get_page(page_id)
            .map_err(|e| ApplyError::Refused {
                operation: "heap_insert",
                detail: format!("buffer pool: {e}"),
            })?;

        let mut page = guard.write();

        // Idempotency check: if the page already has a slot at `tid.slot`
        // with data, skip the insert (the page was flushed past this record).
        let slot_count = page.header().slot_count();
        if payload.tid.slot < slot_count {
            if let Ok(existing) = page.read_tuple(payload.tid.slot) {
                if !existing.is_empty() {
                    // Slot already exists and has content; skip.
                    return Ok(());
                }
            }
        }

        // Write the tuple bytes into the page.
        page.insert_tuple(&payload.tuple_bytes)
            .map_err(|e| ApplyError::Refused {
                operation: "heap_insert",
                detail: format!("insert_tuple: {e}"),
            })?;
        Ok(())
    }

    /// Apply a heap-update record by writing the new tuple bytes and stamping
    /// the old tuple's header with `xmax`/`cmax`.
    ///
    /// If the old and new tids are on the same page the update is performed
    /// under a single exclusive pin. If they are on different pages the new
    /// page is written first and then the old page is stamped.
    #[allow(clippy::significant_drop_tightening)]
    fn apply_update(&self, payload: &HeapUpdatePayload) -> Result<(), ApplyError> {
        let new_page_id = payload.new_tid.page;
        let old_page_id = payload.old_tid.page;

        // Ensure block counters cover both pages.
        self.advance_counter(new_page_id.relation, new_page_id.block);
        if old_page_id != new_page_id {
            self.advance_counter(old_page_id.relation, old_page_id.block);
        }

        // Write the new tuple onto its page.
        {
            let guard = self
                .pool
                .get_page(new_page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_update_new",
                    detail: format!("buffer pool: {e}"),
                })?;
            let mut page = guard.write();
            let slot_count = page.header().slot_count();
            if payload.new_tid.slot >= slot_count
                || page
                    .read_tuple(payload.new_tid.slot)
                    .map_or(true, <[u8]>::is_empty)
            {
                page.insert_tuple(&payload.new_tuple_bytes)
                    .map_err(|e| ApplyError::Refused {
                        operation: "heap_update_new",
                        detail: format!("insert_tuple: {e}"),
                    })?;
            }
        }

        // Stamp the old tuple's header: read it, set xmax/cmax, write back.
        {
            let guard = self
                .pool
                .get_page(old_page_id)
                .map_err(|e| ApplyError::Refused {
                    operation: "heap_update_old",
                    detail: format!("buffer pool: {e}"),
                })?;
            let mut page = guard.write();
            // Read the existing header bytes.
            let existing =
                page.read_tuple(payload.old_tid.slot)
                    .map_err(|e| ApplyError::Refused {
                        operation: "heap_update_old",
                        detail: format!("read slot: {e}"),
                    })?;
            if existing.len() < TUPLE_HEADER_SIZE {
                return Err(ApplyError::Refused {
                    operation: "heap_update_old",
                    detail: String::from("slot shorter than tuple header"),
                });
            }
            let (mut hdr, _) =
                TupleHeader::decode(&existing[..TUPLE_HEADER_SIZE]).ok_or_else(|| {
                    ApplyError::Refused {
                        operation: "heap_update_old",
                        detail: String::from("header decode failed"),
                    }
                })?;

            // Decode xmax from the new tuple header (the new version's xmin
            // is the updating transaction's xid, which becomes the old
            // version's xmax).
            let (new_hdr, _) = TupleHeader::decode(&payload.new_tuple_bytes[..TUPLE_HEADER_SIZE])
                .ok_or_else(|| ApplyError::Refused {
                operation: "heap_update_old",
                detail: String::from("new header decode failed"),
            })?;
            hdr.xmax = new_hdr.xmin;
            hdr.cmax = new_hdr.cmin;
            hdr.ctid = payload.new_tid;

            // Write the patched header back into the page's raw bytes.
            let mut hdr_bytes = [0_u8; TUPLE_HEADER_SIZE];
            hdr.encode(&mut hdr_bytes);
            let page_bytes = page.as_bytes_mut();
            // Re-read item-id to get the slot offset.
            let item_id_off = crate::page::PAGE_HEADER_SIZE
                + usize::from(payload.old_tid.slot) * crate::page::ITEMID_SIZE;
            let raw = u32::from_le_bytes(
                page_bytes[item_id_off..item_id_off + crate::page::ITEMID_SIZE]
                    .try_into()
                    .map_err(|_| ApplyError::Refused {
                        operation: "heap_update_old",
                        detail: String::from("itemid slice"),
                    })?,
            );
            let item = crate::page::ItemId::from_raw(raw);
            let slot_off = item.offset() as usize;
            page_bytes[slot_off..slot_off + TUPLE_HEADER_SIZE].copy_from_slice(&hdr_bytes);
        }

        Ok(())
    }

    /// Apply a heap-delete record by stamping `xmax`/`cmax` into the tuple
    /// header at `payload.tid`.
    #[allow(clippy::significant_drop_tightening)]
    fn apply_delete(&self, payload: &HeapDeletePayload) -> Result<(), ApplyError> {
        let page_id = payload.tid.page;
        self.advance_counter(page_id.relation, page_id.block);

        let guard = self
            .pool
            .get_page(page_id)
            .map_err(|e| ApplyError::Refused {
                operation: "heap_delete",
                detail: format!("buffer pool: {e}"),
            })?;

        let mut page = guard.write();
        let existing = page.read_tuple(payload.tid.slot).map_err(|e| match e {
            // If the slot doesn't exist, the record is already beyond
            // what's on this page — treat as idempotent no-op.
            PageError::InvalidSlot { .. } | PageError::DeadSlot(_) => ApplyError::Refused {
                operation: "heap_delete",
                detail: format!("read slot: {e}"),
            },
            other => ApplyError::Refused {
                operation: "heap_delete",
                detail: format!("read slot: {other}"),
            },
        })?;
        if existing.len() < TUPLE_HEADER_SIZE {
            return Err(ApplyError::Refused {
                operation: "heap_delete",
                detail: String::from("slot shorter than tuple header"),
            });
        }
        let (mut hdr, _) =
            TupleHeader::decode(&existing[..TUPLE_HEADER_SIZE]).ok_or_else(|| {
                ApplyError::Refused {
                    operation: "heap_delete",
                    detail: String::from("header decode failed"),
                }
            })?;

        // Idempotency: if xmax is already set to the same xid, skip.
        if hdr.xmax == payload.xmax {
            return Ok(());
        }
        hdr.mark_deleted(payload.xmax, payload.cmax);
        let mut hdr_bytes = [0_u8; TUPLE_HEADER_SIZE];
        hdr.encode(&mut hdr_bytes);

        let page_bytes = page.as_bytes_mut();
        let item_id_off = crate::page::PAGE_HEADER_SIZE
            + usize::from(payload.tid.slot) * crate::page::ITEMID_SIZE;
        let raw = u32::from_le_bytes(
            page_bytes[item_id_off..item_id_off + crate::page::ITEMID_SIZE]
                .try_into()
                .map_err(|_| ApplyError::Refused {
                    operation: "heap_delete",
                    detail: String::from("itemid slice"),
                })?,
        );
        let item = crate::page::ItemId::from_raw(raw);
        let slot_off = item.offset() as usize;
        page_bytes[slot_off..slot_off + TUPLE_HEADER_SIZE].copy_from_slice(&hdr_bytes);

        Ok(())
    }

    /// Apply a full-page-write record by restoring the page image verbatim.
    ///
    /// The entire 8 KiB page image is written into the buffer pool's frame.
    /// This is the crash-recovery path for torn-write protection: if the page
    /// on disk was partially written at the time of the crash, the FPW record
    /// carries the consistent pre-mutation image so subsequent mutation records
    /// can be re-applied correctly.
    #[allow(clippy::significant_drop_tightening)]
    fn apply_full_page_write(&self, payload: &FullPageWritePayload) -> Result<(), ApplyError> {
        use ultrasql_core::constants::PAGE_SIZE;

        let page_id = payload.page;
        self.advance_counter(page_id.relation, page_id.block);

        let guard = self
            .pool
            .get_page(page_id)
            .map_err(|e| ApplyError::Refused {
                operation: "fpw",
                detail: format!("buffer pool: {e}"),
            })?;

        let mut page = guard.write();
        if payload.page_bytes.len() != PAGE_SIZE {
            return Err(ApplyError::Refused {
                operation: "fpw",
                detail: format!(
                    "page_bytes length {} != PAGE_SIZE {}",
                    payload.page_bytes.len(),
                    PAGE_SIZE
                ),
            });
        }
        page.as_bytes_mut()
            .copy_from_slice(&payload.page_bytes[..PAGE_SIZE]);
        Ok(())
    }
}

impl<L: PageLoader> HeapAccess<L> {
    /// Advance the per-relation block counter to at least `block + 1`.
    ///
    /// Called by the applier when it writes to a block during recovery to
    /// ensure that post-recovery scans driven by `block_count()` cover all
    /// replayed blocks.
    pub(crate) fn advance_counter(&self, rel: ultrasql_core::RelationId, block: BlockNumber) {
        let counter = self.counter_for(rel);
        let needed = block
            .raw()
            .checked_add(1)
            .expect("block number overflow in advance_counter; block is u32::MAX");
        // CAS loop: advance only if the current value is less than `needed`.
        let mut current = counter.load(Ordering::Acquire);
        while current < needed {
            match counter.compare_exchange_weak(
                current,
                needed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use parking_lot::Mutex;
    use ultrasql_core::constants::PAGE_SIZE;
    use ultrasql_core::{BlockNumber, CommandId, PageId, RelationId, Result, TupleId, Xid};
    use ultrasql_mvcc::TupleHeader;
    use ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE;
    use ultrasql_wal::applier::HeapTarget;
    use ultrasql_wal::payload::{HeapDeletePayload, HeapInsertPayload};

    use crate::buffer_pool::{BufferPool, PageLoader};
    use crate::heap::{HeapAccess, InsertOptions};
    use crate::page::Page;

    /// In-memory page loader that persists pages in a `HashMap` so writes
    /// survive across pin/unpin cycles.
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

    fn rel() -> RelationId {
        RelationId::new(77)
    }

    fn page_id(block: u32) -> PageId {
        PageId::new(rel(), BlockNumber::new(block))
    }

    fn tuple_id(block: u32, slot: u16) -> TupleId {
        TupleId::new(page_id(block), slot)
    }

    /// Build a minimal tuple byte vector: a fresh header with no payload.
    fn minimal_tuple(xmin: u64, tid: TupleId) -> Vec<u8> {
        let hdr = TupleHeader::fresh(Xid::new(xmin), CommandId::FIRST, tid, 0);
        let mut bytes = vec![0_u8; TUPLE_HEADER_SIZE];
        hdr.encode(&mut bytes[..TUPLE_HEADER_SIZE]);
        bytes
    }

    fn make_heap() -> HeapAccess<MapLoader> {
        let pool = Arc::new(BufferPool::new(64, MapLoader::new()));
        HeapAccess::new(pool)
    }

    #[test]
    fn apply_insert_writes_tuple_to_page() {
        let heap = make_heap();
        let tid = tuple_id(0, 0);
        let tuple_bytes = minimal_tuple(1, tid);

        let payload = HeapInsertPayload { tid, tuple_bytes };
        heap.apply_insert(&payload).unwrap();

        // Verify the tuple is readable via fetch.
        let fetched = heap.fetch(tid).unwrap();
        assert_eq!(fetched.tid, tid);
        assert_eq!(fetched.header.xmin, Xid::new(1));
    }

    #[test]
    fn apply_insert_is_idempotent() {
        let heap = make_heap();
        let tid = tuple_id(0, 0);
        let tuple_bytes = minimal_tuple(1, tid);
        let payload = HeapInsertPayload { tid, tuple_bytes };
        heap.apply_insert(&payload).unwrap();
        // Second application of the same record must not fail.
        heap.apply_insert(&payload).unwrap();
        // Only one slot should exist.
        assert_eq!(heap.block_count(rel()), 1);
    }

    #[test]
    fn apply_delete_stamps_xmax() {
        let heap = make_heap();
        let tid = tuple_id(0, 0);

        // Insert via the normal path so the slot is valid.
        heap.insert(
            rel(),
            b"hello",
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .unwrap();

        let payload = HeapDeletePayload {
            tid,
            xmax: Xid::new(2),
            cmax: CommandId::new(1),
        };
        heap.apply_delete(&payload).unwrap();

        let fetched = heap.fetch(tid).unwrap();
        assert_eq!(fetched.header.xmax, Xid::new(2));
    }

    #[test]
    fn apply_fpw_restores_page_image() {
        use ultrasql_core::constants::PAGE_SIZE;
        use ultrasql_wal::payload::FullPageWritePayload;

        let heap = make_heap();
        // Insert something to create page 0 for this relation.
        heap.insert(
            rel(),
            b"data",
            InsertOptions {
                xmin: Xid::new(1),
                command_id: CommandId::FIRST,
                wal: None,
                fsm: None,
                vm: None,
            },
        )
        .unwrap();

        // Build a zeroed-out page image (simulates a restored checkpoint image).
        let zeroed = crate::page::Page::new_heap();
        let page_bytes = zeroed.as_bytes().to_vec();
        assert_eq!(page_bytes.len(), PAGE_SIZE);

        let pid = page_id(0);
        let payload = FullPageWritePayload {
            page: pid,
            page_bytes: page_bytes.clone(),
        };
        heap.apply_full_page_write(&payload).unwrap();

        // After FPW the page bytes must match the payload.
        let guard = heap.pool.get_page(pid).unwrap();
        let actual = guard.read().as_bytes().to_vec();
        assert_eq!(actual, page_bytes, "FPW should restore page verbatim");
    }

    #[test]
    fn advance_counter_updates_block_count() {
        let heap = make_heap();
        heap.advance_counter(rel(), BlockNumber::new(4));
        assert_eq!(heap.block_count(rel()), 5);
        // Advancing to a lower block must not decrease the counter.
        heap.advance_counter(rel(), BlockNumber::new(2));
        assert_eq!(heap.block_count(rel()), 5);
    }
}
