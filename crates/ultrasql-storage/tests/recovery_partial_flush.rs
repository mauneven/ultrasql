//! Crash-recovery integration test: WAL replay over a PARTIALLY-FLUSHED
//! page store.
//!
//! # What this hardens
//!
//! The redo path's correctness hinges on the page-LSN comparison
//! `should_skip_redo` in `wal_applier.rs`: a WAL record is re-applied to a
//! page only if the on-disk page's LSN is *older* than the record's LSN. If
//! the page was already flushed at or past that LSN, redo must be SKIPPED —
//! otherwise recovery double-applies a mutation onto an already-current page,
//! which at best wastes work and at worst corrupts the page. Full-page-write
//! (FPW) records are the one exception: they are restored unconditionally
//! (not LSN-gated) because they exist to repair a torn on-disk image whose
//! LSN field may itself be stale-but-high.
//!
//! # Why the existing tests don't cover this
//!
//! `recovery_sim.rs` always replays the WAL into a FRESH BLANK `MapLoader`,
//! so every page starts at LSN 0 and `should_skip_redo` always returns
//! `false` — its TRUE (skip) branch is never exercised end-to-end. The FPW
//! torn-page repair is only unit-tested with hand-built page images. No test
//! drives a workload, flushes only *some* dirty pages to a real on-disk
//! `SegmentFileManager`, simulates a crash mid-checkpoint, replays the WAL,
//! and verifies the mixed flushed/unflushed outcome.
//!
//! # What this test does
//!
//! 1. Phase 1 — run an insert workload against a live heap wired to an
//!    `InMemoryWalSink`, capturing every `(lsn, record)` pair and the TID of
//!    every inserted row. The sink assigns each record a monotonically
//!    increasing LSN, exactly the "record LSN" the recovery applier compares
//!    against the page LSN.
//! 2. Phase 2 — build a REAL on-disk `SegmentFileManager` in a tempdir,
//!    pre-grow it to the workload's block count, and replay every record into
//!    a heap backed by that segment manager via `dispatch_record_at_lsn` (the
//!    same entry point production recovery uses). The applier stamps each
//!    page with its record LSN. Then flush ONLY a subset of the dirty pages
//!    to disk and `fsync` — the realistic "checkpoint flushed some pages,
//!    then the process was killed" state. Flushed pages land on disk at a
//!    newer LSN; unflushed pages stay blank (LSN 0) on disk.
//! 3. Phase 3 — drop the heap/pool and REOPEN the segment manager from the
//!    same tempdir with a cold buffer pool, then replay every record AGAIN.
//!    For a flushed page the on-disk LSN is >= the record LSN, so
//!    `should_skip_redo` returns TRUE and redo is skipped. For an unflushed
//!    page the on-disk LSN is 0 < the record LSN, so the record IS redone.
//! 4. Phase 4 — assert the per-page decision and that the recovered row set
//!    is identical to a full clean replay.
//!
//! To prove the skip branch *fired* (rather than redo merely being
//! idempotent), the workload DELETEs one row on a flushed block, and after
//! flushing that block its deleted tuple's `xmax` is tampered back to INVALID
//! on disk. The second replay meets the `HeapDelete` record again: the skip
//! branch leaves the tampered INVALID xmax in place, whereas a non-skipped
//! redo would re-stamp `xmax = DELETE_XID`. Asserting the recovered row reads
//! `xmax == INVALID` is therefore positive proof the skip branch ran — and a
//! mutation test that flips `should_skip_redo` to always-`false` makes that
//! assertion fail with `xmax == DELETE_XID`, confirming the test is not
//! vacuous. (Plain inserts cannot show this: their per-slot idempotency guard
//! makes their redo a no-op against an already-filled slot with or without
//! the gate.)
//!
//! # Reach / honesty note
//!
//! `should_skip_redo` is a private free function; this test cannot call it
//! directly. It is driven entirely through the crate's PUBLIC surface
//! (`SegmentFileManager`, `BufferPool`, `HeapAccess`, `dispatch_record_at_lsn`)
//! exactly as production recovery drives it, and the skip-vs-redo decision is
//! asserted by its observable effect on the page bytes. This is the strongest
//! end-to-end form reachable from `tests/`; the LSN-comparison branch itself
//! is additionally unit-tested in `wal_applier.rs`
//! (`apply_delete_at_lsn_skips_when_page_lsn_*`).

use std::sync::Arc;

use tempfile::TempDir;
use ultrasql_core::{BlockNumber, CommandId, Lsn, PageId, RelationId, TupleId, Xid};
use ultrasql_storage::buffer_pool::BufferPool;
use ultrasql_storage::heap::{DeleteOptions, HeapAccess, InsertOptions};
use ultrasql_storage::page::Page;
use ultrasql_storage::segment::{SegmentConfig, SegmentFileManager};
use ultrasql_storage::test_support::InMemoryWalSink;
use ultrasql_wal::WalRecord;
use ultrasql_wal::applier::dispatch_record_at_lsn;

/// A clonable `PageLoader` handle wrapping a shared on-disk segment
/// manager, so the buffer pool and the test body can both reach the SAME
/// segment files: the pool reads pages through it on a miss, while the test
/// writes/reads/fsyncs pages directly through its own `Arc` clone. This
/// mirrors production, where the buffer pool and the checkpointer share one
/// clonable loader handle over the segment layer.
#[derive(Clone)]
struct SharedSegments(Arc<SegmentFileManager>);

impl ultrasql_storage::buffer_pool::PageLoader for SharedSegments {
    fn load(&self, page_id: PageId) -> ultrasql_core::Result<Page> {
        self.0.read_page(page_id).map_err(Into::into)
    }
}

const fn rel() -> RelationId {
    RelationId::new(1)
}

fn insert_opts(xid: Xid, sink: &dyn ultrasql_storage::wal_sink::WalSink) -> InsertOptions<'_> {
    InsertOptions {
        xmin: xid,
        command_id: CommandId::FIRST,
        n_atts: 1,
        wal: Some(sink),
        fsm: None,
        vm: None,
    }
}

/// Small per-segment cap so a modest workload spans several segment files,
/// exercising the real on-disk layout rather than one giant file.
fn segment_config() -> SegmentConfig {
    SegmentConfig {
        segment_size_pages: 4,
        use_mmap: false,
        create_if_missing: true,
        verify_checksums: true,
    }
}

/// Open a segment manager rooted at `dir`.
fn open_segments(dir: &std::path::Path) -> Arc<SegmentFileManager> {
    Arc::new(SegmentFileManager::open(dir, segment_config()).expect("open segment manager"))
}

/// Pre-grow `rel()` to `n_blocks` blocks on disk so the applier's
/// `get_page` calls find an allocated (blank) page instead of an
/// out-of-bounds read. Production recovery achieves the same effect by
/// replaying relation-extend records / seeding durable block counts; here
/// we extend directly, which is the minimal equivalent for an insert-only
/// workload.
fn pre_grow(segments: &SegmentFileManager, n_blocks: u32) {
    let have = segments.relation_size_blocks(rel()).unwrap_or(0);
    for _ in have..n_blocks {
        segments.allocate_block(rel()).expect("allocate block");
    }
}

/// Replay every `(lsn, record)` into `heap` via the same dispatch entry
/// point production recovery uses, passing each record's WAL LSN so the
/// applier can compare it against the on-disk page LSN.
fn replay_all(
    heap: &HeapAccess<SharedSegments>,
    records: &[(Lsn, WalRecord)],
) {
    for (lsn, record) in records {
        dispatch_record_at_lsn(heap, record, *lsn).expect("replay must apply or skip cleanly");
    }
}

/// Read a page's stored LSN straight off disk (cold), bypassing any buffer
/// pool, so we observe exactly what a crash would have left durable.
fn on_disk_lsn(segments: &SegmentFileManager, block: u32) -> u64 {
    segments
        .read_page(PageId::new(rel(), BlockNumber::new(block)))
        .expect("read on-disk page")
        .header()
        .lsn
}

/// Collect the `(tid, payload)` of every live row across all blocks of
/// `rel()`, sorted by tid, for set comparison.
fn recovered_rows(heap: &HeapAccess<SharedSegments>) -> Vec<(TupleId, Vec<u8>)> {
    let n_blocks = heap.block_count(rel());
    let mut out: Vec<(TupleId, Vec<u8>)> = heap
        .scan(rel(), n_blocks)
        .flatten()
        .map(|t| (t.tid, t.data))
        .collect();
    out.sort_by_key(|(tid, _)| (tid.page.block.raw(), tid.slot));
    out
}

/// Full crash-recovery over a partially-flushed page store.
///
/// Asserts the per-page redo decision: pages whose on-disk LSN already
/// covers the record LSN are SKIPPED (the skip branch of `should_skip_redo`),
/// pages still blank on disk are REDONE, and the recovered row set matches a
/// full clean replay.
#[test]
fn wal_replay_over_partially_flushed_page_store_skips_and_redoes_per_page() {
    // --- Phase 1: live workload, capture WAL + row TIDs -------------------
    // Wide payloads pack only a handful of rows per 8 KiB page so the
    // workload spans many blocks deterministically.
    const ROWS: usize = 600;
    const PAYLOAD_LEN: usize = 200;
    // The xid that deletes a block-0 row; its `xmax` stamp is the redo signal.
    const DELETE_XID: u64 = 2;

    let sink = Arc::new(InMemoryWalSink::new());
    let mut row_payloads: Vec<(TupleId, Vec<u8>)> = Vec::with_capacity(ROWS);
    let deleted_tid: TupleId;

    {
        // The Phase-1 heap is throwaway; its only job is to emit WAL with
        // realistic TID/LSN layout. A cheap in-memory loader suffices.
        let segments = open_segments(TempDir::new().unwrap().path());
        let pool = Arc::new(BufferPool::new(512, SharedSegments(Arc::clone(&segments))));
        let heap = HeapAccess::new(pool);
        for i in 0..ROWS {
            let mut payload = vec![0_u8; PAYLOAD_LEN];
            payload[0] = u8::try_from(i % 251).expect("fits u8");
            payload[1] = u8::try_from(i / 251).expect("fits u8");
            // The live heap allocates blocks on demand via the segment
            // manager, so grow it lazily as the insert cursor advances.
            let next_block = heap.block_count(rel());
            pre_grow(&segments, next_block + 1);
            let tid = loop {
                match heap.insert(rel(), &payload, insert_opts(Xid::new(1), sink.as_ref())) {
                    Ok(tid) => break tid,
                    Err(_) => {
                        // Cursor wanted a new block: extend and retry.
                        let grow_to = heap.block_count(rel()) + 1;
                        pre_grow(&segments, grow_to);
                    }
                }
            };
            row_payloads.push((tid, payload));
        }

        // Delete one row that lives on block 0 (the block we will flush and
        // tamper). This puts a `HeapDelete` record in the stream whose redo,
        // if NOT skipped, would re-stamp `xmax` on an already-flushed page —
        // the exact write the LSN gate must suppress. `xmax = DELETE_XID`.
        let victim = row_payloads
            .iter()
            .find(|(tid, _)| tid.page.block.raw() == 0)
            .map(|(tid, _)| *tid)
            .expect("a row on block 0");
        heap.delete(
            victim,
            DeleteOptions {
                xmax: Xid::new(DELETE_XID),
                cmax: CommandId::FIRST,
                wal: Some(sink.as_ref()),
                fsm: None,
                vm: None,
            },
        )
        .expect("delete a block-0 row");
        deleted_tid = victim;
        // heap/pool/segments drop here; only `sink` and `row_payloads` live on.
    }

    let records = sink.records();
    assert!(
        !records.is_empty(),
        "workload must have emitted WAL records"
    );

    // Total blocks the workload touched (max block + 1).
    let n_blocks = row_payloads
        .iter()
        .map(|(tid, _)| tid.page.block.raw())
        .max()
        .expect("at least one row")
        + 1;
    assert!(
        n_blocks >= 4,
        "workload should span several blocks to make the partial-flush split meaningful (got {n_blocks})"
    );

    // --- Reference: a FULL clean replay into a blank on-disk store --------
    // This is the ground truth the partial-flush recovery must match.
    let reference_dir = TempDir::new().unwrap();
    let reference_rows = {
        let segments = open_segments(reference_dir.path());
        pre_grow(&segments, n_blocks);
        let pool = Arc::new(BufferPool::new(512, SharedSegments(Arc::clone(&segments))));
        let heap = HeapAccess::new(pool);
        replay_all(&heap, &records);
        recovered_rows(&heap)
    };
    assert_eq!(
        reference_rows.len(),
        ROWS,
        "clean full replay must recover every inserted row"
    );

    // --- Phase 2: replay to disk, flush only the LOWER HALF of blocks -----
    let crash_dir = TempDir::new().unwrap();
    let flush_below = n_blocks / 2; // blocks [0, flush_below) get flushed.
    assert!(
        flush_below >= 1 && flush_below < n_blocks,
        "need both a flushed and an unflushed region (flush_below={flush_below}, n_blocks={n_blocks})"
    );

    // The redo-vs-skip signal lives in the header of the deleted block-0 row.
    // After flushing block 0 (durable LSN >= the delete record's LSN) we
    // tamper the deleted tuple's `xmax` back to INVALID on disk — i.e. we
    // make the durable page look "not deleted". The second replay then meets
    // the `HeapDelete` record again:
    //   * skip branch (correct): page LSN >= record LSN ⇒ redo is SKIPPED, so
    //     the tampered INVALID xmax survives and the recovered row reads
    //     `xmax == INVALID`.
    //   * no skip (broken gate): `HeapDelete` redo runs, sees `xmax (INVALID)
    //     != DELETE_XID`, and re-stamps `xmax = DELETE_XID`, so the recovered
    //     row reads `xmax == DELETE_XID`.
    // The two outcomes are distinct, so asserting `xmax == INVALID` proves the
    // skip branch fired rather than redo being merely idempotent. (Plain
    // inserts cannot show this: their per-slot idempotency guard makes their
    // redo a no-op against an already-filled slot with or without the gate.)
    let tamper_block = deleted_tid.page.block.raw();
    assert!(
        tamper_block < flush_below,
        "the deleted row must live in the flushed region (block {tamper_block}, flush_below {flush_below})"
    );

    let flushed_lsns: Vec<(u32, u64)> = {
        let segments = open_segments(crash_dir.path());
        pre_grow(&segments, n_blocks);
        let pool = Arc::new(BufferPool::new(512, SharedSegments(Arc::clone(&segments))));
        let heap = HeapAccess::new(pool);

        // First replay: applies every record and stamps page LSNs.
        replay_all(&heap, &records);

        // Flush ONLY the lower-half blocks to disk. The writer mirrors the
        // checkpointer: it forwards a page to `SegmentFileManager::write_page`
        // only when the block is in the flush region; otherwise it leaves the
        // frame dirty (returns Ok without writing), so that block never
        // reaches disk and stays blank/LSN-0 there.
        let pool_ref = heap.buffer_pool();
        let mut flushed_blocks: Vec<u32> = Vec::new();
        pool_ref
            .try_flush_dirty(|page_id, page| {
                if page_id.relation == rel() && page_id.block.raw() < flush_below {
                    segments.write_page(page_id, page)?;
                    flushed_blocks.push(page_id.block.raw());
                }
                Ok(())
            })
            .expect("partial flush must succeed");
        assert!(
            !flushed_blocks.is_empty(),
            "partial flush must have written at least one page"
        );

        // Capture the durable LSN of every flushed block, read cold off disk.
        let mut lsns: Vec<(u32, u64)> = (0..flush_below)
            .map(|b| (b, on_disk_lsn(&segments, b)))
            .collect();
        lsns.sort();

        // Tamper the deleted tuple's `xmax` on the flushed page back to
        // INVALID (un-deleting it on disk). We locate the tuple by its bytes
        // (read through the live pool image, which equals the flushed image)
        // and patch the `xmax` field (header bytes 8..16) of the matching
        // region in the on-disk page.
        {
            let pre_tamper = on_disk_xmax(&segments, deleted_tid);
            assert_eq!(
                pre_tamper,
                DELETE_XID,
                "the flushed deleted row must carry xmax = DELETE_XID before tampering"
            );
            tamper_xmax_to_invalid(&segments, deleted_tid);
            assert_eq!(
                on_disk_xmax(&segments, deleted_tid),
                0,
                "tamper must have reset the deleted row's on-disk xmax to INVALID"
            );
        }

        segments.fsync_all().expect("fsync flushed pages");
        lsns
        // heap/pool drop here — simulated crash. Only `crash_dir` (the
        // partially-flushed on-disk store) and `sink`/`records` survive.
    };

    // Every flushed block carries a NEWER (nonzero) on-disk LSN; every
    // unflushed block is still blank (LSN 0) on disk. This is the setup the
    // skip branch must distinguish.
    for (block, lsn) in &flushed_lsns {
        assert!(
            *lsn > 0,
            "flushed block {block} must carry a nonzero durable LSN, got {lsn}"
        );
    }
    {
        let segments = open_segments(crash_dir.path());
        for block in flush_below..n_blocks {
            assert_eq!(
                on_disk_lsn(&segments, block),
                0,
                "unflushed block {block} must still be blank (LSN 0) on disk"
            );
        }
    }

    // --- Phase 3: crash, reopen cold, replay AGAIN -----------------------
    let segments = open_segments(crash_dir.path());
    // The durable on-disk size already covers every touched block, so no
    // pre-grow is needed here — recovery reads the partially-flushed store.
    assert_eq!(
        segments.relation_size_blocks(rel()).unwrap(),
        n_blocks,
        "reopened store must report the durable block count"
    );
    let pool = Arc::new(BufferPool::new(512, SharedSegments(Arc::clone(&segments))));
    let heap = HeapAccess::new(pool);

    replay_all(&heap, &records);

    // --- Phase 4: assert the per-page decision and final state -----------

    // (a) The SKIP branch fired. The deleted block-0 row's xmax was tampered
    //     to INVALID on the flushed page; because that page's durable LSN
    //     covers the `HeapDelete` record, the second replay SKIPPED the delete
    //     redo and left xmax INVALID. Had the gate not skipped, delete redo
    //     would have re-stamped xmax = DELETE_XID. So INVALID here is positive
    //     proof the skip branch ran (verified by a mutation test that flips
    //     `should_skip_redo` to always-false and watches this assert fail).
    let recovered_xmax = heap
        .fetch(deleted_tid)
        .expect("deleted row still present")
        .header
        .xmax;
    assert_eq!(
        recovered_xmax,
        Xid::INVALID,
        "skip branch must have left the tampered xmax INVALID; xmax == DELETE_XID would mean the delete redo was NOT skipped"
    );

    // (b) Despite the skip, every flushed row is present with its original
    //     data: the page image flushed in Phase 2 already holds them.
    // (c) Every unflushed row was REDONE from the WAL onto its blank on-disk
    //     page. `recovered_rows` compares `(tid, data)`; the deleted row's
    //     data is unchanged by the (skipped) delete, so the row sets match
    //     even though the deleted row's header xmax differs from the clean
    //     reference — which is exactly the point.
    let recovered = recovered_rows(&heap);
    assert_eq!(
        recovered, reference_rows,
        "partial-flush recovery must reproduce the full clean-replay row set exactly"
    );

    // Spot-check the split explicitly: at least one row in a flushed block and
    // at least one in an unflushed block, both recovered with original bytes.
    let flushed_row = row_payloads
        .iter()
        .find(|(tid, _)| tid.page.block.raw() < flush_below)
        .expect("a row in the flushed region");
    let unflushed_row = row_payloads
        .iter()
        .find(|(tid, _)| tid.page.block.raw() >= flush_below)
        .expect("a row in the unflushed region");
    assert_eq!(
        heap.fetch(flushed_row.0).expect("flushed row present").data,
        flushed_row.1,
        "row on a skipped (already-flushed) page must still be readable"
    );
    assert_eq!(
        heap.fetch(unflushed_row.0)
            .expect("unflushed row redone")
            .data,
        unflushed_row.1,
        "row on a blank (unflushed) page must have been redone from WAL"
    );
}

/// Locate `tid`'s tuple within its on-disk page and return `(page,
/// slot_offset)`, where `slot_offset` is the byte offset of the tuple
/// (header first) inside the raw page. The tuple is found by matching its
/// exact bytes (its payload prefix is unique per row), so no internal
/// slot-directory parsing is needed from the integration test.
fn locate_tuple_on_disk(segments: &SegmentFileManager, tid: TupleId) -> (Page, usize) {
    let page = segments.read_page(tid.page).expect("read page for tuple");
    let needle = page
        .read_tuple(tid.slot)
        .expect("read tuple for slot")
        .to_vec();
    let bytes = page.as_bytes();
    let off = bytes
        .windows(needle.len())
        .position(|w| w == needle.as_slice())
        .expect("tuple bytes must appear in the page");
    (page, off)
}

/// Read the deleted tuple's on-disk `xmax` (header bytes 8..16, little
/// endian) straight from the durable page image.
fn on_disk_xmax(segments: &SegmentFileManager, tid: TupleId) -> u64 {
    let (page, off) = locate_tuple_on_disk(segments, tid);
    let xmax_bytes: [u8; 8] = page.as_bytes()[off + 8..off + 16]
        .try_into()
        .expect("8 xmax bytes");
    u64::from_le_bytes(xmax_bytes)
}

/// Patch the deleted tuple's on-disk `xmax` to `INVALID` (0), refresh the
/// page checksum, and write the page back durably. This is the "un-delete on
/// disk" tamper that makes the skip-vs-redo decision observable.
fn tamper_xmax_to_invalid(segments: &SegmentFileManager, tid: TupleId) {
    let (mut page, off) = locate_tuple_on_disk(segments, tid);
    page.as_bytes_mut()[off + 8..off + 16].copy_from_slice(&0_u64.to_le_bytes());
    page.refresh_checksum();
    segments
        .write_page(tid.page, &page)
        .expect("write tampered page");
}

/// FPW torn-page repair over a partially-flushed store.
///
/// A full-page-write record must be restored UNCONDITIONALLY (it repairs a
/// torn on-disk page whose LSN may be stale-but-high), while an ordinary
/// newer page is left alone by the LSN gate. This drives both through the
/// public applier surface on a real on-disk segment store.
#[test]
fn fpw_repairs_torn_page_while_lsn_gate_leaves_newer_page_alone() {
    use ultrasql_wal::payload::{FullPageWritePayload, HeapDeletePayload};
    use ultrasql_wal::record::RecordType;

    let dir = TempDir::new().unwrap();
    let segments = open_segments(dir.path());
    pre_grow(&segments, 2);
    let pool = Arc::new(BufferPool::new(64, SharedSegments(Arc::clone(&segments))));
    let heap = HeapAccess::new(pool);

    let torn_block = 0_u32;
    let newer_block = 1_u32;
    let torn_tid = TupleId::new(PageId::new(rel(), BlockNumber::new(torn_block)), 0);
    let newer_tid = TupleId::new(PageId::new(rel(), BlockNumber::new(newer_block)), 0);

    // Lay down a "torn" page-0 image on disk: it carries a HIGH LSN but the
    // wrong row. A bare LSN gate would wrongly skip repairing it.
    {
        let mut torn = Page::new_heap();
        let mut wrong = vec![0_u8; ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE + 4];
        let hdr = ultrasql_mvcc::TupleHeader::fresh(Xid::new(99), CommandId::FIRST, torn_tid, 1);
        hdr.encode(&mut wrong[..ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE]);
        wrong[ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE..].copy_from_slice(b"BAD!");
        torn.insert_tuple(&wrong).expect("insert torn row");
        torn.set_lsn(10_000); // stale-but-high LSN
        segments
            .write_page(PageId::new(rel(), BlockNumber::new(torn_block)), &torn)
            .expect("write torn page");
    }

    // Lay down a legitimately newer page-1 image on disk: correct row, high
    // LSN. The LSN-gated incremental redo must NOT touch it.
    let good_payload = b"GOOD";
    {
        let mut good = Page::new_heap();
        let mut row = vec![0_u8; ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE + 4];
        let hdr = ultrasql_mvcc::TupleHeader::fresh(Xid::new(7), CommandId::FIRST, newer_tid, 1);
        hdr.encode(&mut row[..ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE]);
        row[ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE..].copy_from_slice(good_payload);
        good.insert_tuple(&row).expect("insert good row");
        good.set_lsn(5_000);
        segments
            .write_page(PageId::new(rel(), BlockNumber::new(newer_block)), &good)
            .expect("write newer page");
    }

    // Build the authoritative FPW image for page 0 that recovery would carry:
    // a checkpoint image with the CORRECT row.
    let correct_payload = b"OKAY";
    let fpw_image = {
        let mut p = Page::new_heap();
        let mut row = vec![0_u8; ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE + 4];
        let hdr = ultrasql_mvcc::TupleHeader::fresh(Xid::new(7), CommandId::FIRST, torn_tid, 1);
        hdr.encode(&mut row[..ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE]);
        row[ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE..].copy_from_slice(correct_payload);
        p.insert_tuple(&row).expect("insert fpw row");
        p.as_bytes().to_vec()
    };

    // FPW record LSN (3_000) is LOWER than the torn page's stale on-disk LSN
    // (10_000); an LSN gate would skip it, but FPW is unconditional.
    let fpw_payload = FullPageWritePayload {
        page: PageId::new(rel(), BlockNumber::new(torn_block)),
        page_bytes: fpw_image,
    };
    let fpw_record = WalRecord::new(
        RecordType::FullPageWrite,
        Xid::new(7),
        Lsn::ZERO,
        0,
        fpw_payload.encode().expect("encode fpw payload"),
    )
    .expect("build fpw record");
    dispatch_record_at_lsn(&heap, &fpw_record, Lsn::new(3_000)).expect("FPW must apply");

    // A stale incremental DELETE redo for page 1 at a LOWER LSN (1_000) than
    // page 1's on-disk LSN (5_000) must be SKIPPED — page 1 was legitimately
    // flushed past it. A delete (rather than an insert) makes the skip
    // observable: were the gate absent, the delete redo would stamp
    // `xmax = 4242` on the row's header; the gate suppresses that write so the
    // row stays live (`xmax == INVALID`). (An insert redo would be a no-op
    // against the already-filled slot with or without the gate, so it could
    // not distinguish skip from redo.)
    const STALE_DELETE_XID: u64 = 4242;
    let stale_delete = HeapDeletePayload {
        tid: newer_tid,
        xmax: Xid::new(STALE_DELETE_XID),
        cmax: CommandId::FIRST,
    };
    let stale_record = WalRecord::new(
        RecordType::HeapDelete,
        Xid::new(STALE_DELETE_XID),
        Lsn::ZERO,
        0,
        stale_delete.encode().expect("encode stale delete payload"),
    )
    .expect("build stale delete record");
    dispatch_record_at_lsn(&heap, &stale_record, Lsn::new(1_000))
        .expect("stale delete redo must be skipped cleanly");

    // Page 0 was repaired by the FPW (torn row gone, correct row restored)
    // even though its on-disk LSN looked newer than the FPW LSN.
    let repaired = heap.fetch(torn_tid).expect("repaired row present");
    assert_eq!(
        repaired.data, correct_payload,
        "FPW must repair the torn page unconditionally, ignoring the stale-but-high page LSN"
    );

    // Page 1 was left alone by the LSN gate: the stale delete redo did not
    // stamp `xmax`, so the legitimately newer flushed row is still live with
    // its original bytes. `xmax == INVALID` is the positive proof the skip
    // branch fired (a mutation flipping `should_skip_redo` to always-false
    // makes this assert fail with `xmax == STALE_DELETE_XID`).
    let newer = heap.fetch(newer_tid).expect("newer row present");
    assert_eq!(
        newer.data, good_payload,
        "LSN gate must not disturb the data of a legitimately newer flushed page"
    );
    assert_eq!(
        newer.header.xmax,
        Xid::INVALID,
        "LSN gate must skip the stale delete redo on a legitimately newer flushed page"
    );
}
