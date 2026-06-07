//! Free-space map (FSM).
//!
//! The FSM tracks, per relation, the approximate amount of free space
//! available on each page. Inserters query the FSM to locate a page
//! that has room before allocating a new block, avoiding unnecessary
//! block growth.
//!
//! # Design
//!
//! Each page is represented by one byte (category value 0–255) where
//! 0 means "full" and category `c` means "at least `c * PAGE_SIZE / 256`
//! bytes free." The mapping is: `category = min(255, floor(free_bytes * 256 / PAGE_SIZE))`.
//!
//! The backing store is a `DashMap<RelationId, RwLock<Vec<u8>>>` indexed
//! by block number. This is the in-memory tier; a persistent backing will
//! follow in v0.4 once the segment layer stabilises.
//!
//! # Concurrency
//!
//! The outer `DashMap` is sharded so relation-level contention is bounded.
//! The inner `RwLock<Vec<u8>>` is per-relation: readers take a shared lock,
//! writers take an exclusive lock. The lock is held for at most one slice
//! operation, so contention is negligible.

use dashmap::DashMap;
use parking_lot::RwLock;
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{BlockNumber, RelationId};

const PAGE_SIZE_U32: u32 = 8_192;
const _: () = assert!(PAGE_SIZE == 8_192);

/// Number of free-space categories.
///
/// Category `c` represents "at least `c * PAGE_SIZE / 256` bytes free."
/// Category 0 means full; 255 means at least `255 * PAGE_SIZE / 256` bytes
/// free (an almost-empty page). This matches PostgreSQL's FSM encoding.
const CATEGORY_COUNT: u32 = 256;

/// Per-relation, in-memory free-space map.
///
/// The FSM does not write to the buffer pool or disk in this version.
/// A persistent backing page structure will be added in v0.4 once the
/// segment layer is fully wired. The in-memory representation is
/// sufficient for the v0.3 heap access method.
///
/// # Thread safety
///
/// `FreeSpaceMap` is `Send + Sync` through its `DashMap` sharding and
/// per-relation `RwLock`.
#[derive(Debug, Default)]
pub struct FreeSpaceMap {
    /// `relation → [category_byte; block_count]`
    ///
    /// Each element is the free-space category for the corresponding block.
    inner: DashMap<RelationId, RwLock<Vec<u8>>>,
}

impl FreeSpaceMap {
    /// Create a new, empty free-space map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the free space for a page.
    ///
    /// `free_bytes` is clamped to `PAGE_SIZE` and converted to a
    /// category byte in `0..=255` using the formula:
    ///
    /// ```text
    /// category = min(255, floor(free_bytes * 256 / PAGE_SIZE))
    /// ```
    ///
    /// Category `c` represents "at least `c * PAGE_SIZE / 256` bytes free."
    /// Category 0 means full; 255 means almost empty.
    ///
    /// If the page's block number is beyond the currently tracked range,
    /// the vector is extended with zeros (full) up to that block, then
    /// the block's category is updated.
    pub fn record_free_space(&self, rel: RelationId, block: BlockNumber, free_bytes: u32) {
        // floor(free_bytes * 256 / PAGE_SIZE), clamped to 255.
        let clamped = free_bytes.min(PAGE_SIZE_U32);
        let category = category_byte((clamped * CATEGORY_COUNT / PAGE_SIZE_U32).min(255));
        let Some(idx) = block_index(block) else {
            return;
        };

        let entry = self
            .inner
            .entry(rel)
            .or_insert_with(|| RwLock::new(Vec::new()));
        {
            let mut vec = entry.write();
            if idx >= vec.len() {
                vec.resize(idx + 1, 0);
            }
            vec[idx] = category;
        }
    }

    /// Find any block in `rel` with at least `min_free` bytes available.
    ///
    /// Returns the first block whose category byte satisfies the condition
    /// `category * PAGE_SIZE / 256 >= min_free`, or `None` if no such
    /// block exists.
    ///
    /// Callers should fall back to allocating a new block when this
    /// returns `None`.
    pub fn find_block_with_at_least(&self, rel: RelationId, min_free: u32) -> Option<BlockNumber> {
        let min_clamped = min_free.min(PAGE_SIZE_U32);
        // Smallest category `c` such that `c * PAGE_SIZE / 256 >= min_free`.
        // That is `c >= ceil(min_free * 256 / PAGE_SIZE)`.
        let min_category = if min_clamped == 0 {
            0u8
        } else {
            category_byte(
                (min_clamped * CATEGORY_COUNT)
                    .div_ceil(PAGE_SIZE_U32)
                    .min(255),
            )
        };

        let entry = self.inner.get(&rel)?;
        {
            let vec = entry.read();
            vec.iter().enumerate().find_map(|(idx, &cat)| {
                if cat < min_category {
                    return None;
                }
                let raw = u32::try_from(idx).ok()?;
                Some(BlockNumber::new(raw))
            })
        }
    }

    /// Invalidate the free-space entry for a block.
    ///
    /// This marks the block as "full" (category 0), preventing the FSM
    /// from handing it out to new inserters. Called when the heap cannot
    /// determine the exact free space after a concurrent modification.
    pub fn invalidate_block(&self, rel: RelationId, block: BlockNumber) {
        if let Some(entry) = self.inner.get(&rel) {
            let mut vec = entry.write();
            let Some(idx) = block_index(block) else {
                return;
            };
            if idx < vec.len() {
                vec[idx] = 0;
            }
        }
    }
}

fn block_index(block: BlockNumber) -> Option<usize> {
    usize::try_from(block.raw()).ok()
}

fn category_byte(value: u32) -> u8 {
    match u8::try_from(value.min(u32::from(u8::MAX))) {
        Ok(category) => category,
        Err(_) => unreachable!("FSM category is clamped to u8::MAX"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rel(n: u32) -> RelationId {
        RelationId::new(n)
    }

    fn blk(n: u32) -> BlockNumber {
        BlockNumber::new(n)
    }

    #[test]
    fn record_and_find_basic() {
        let fsm = FreeSpaceMap::new();
        // Use a multiple of PAGE_SIZE/256 = 32 to avoid quantization loss.
        // 4000 / 32 = 125, so 4000 is a clean multiple of the category granule.
        // category = floor(4000 * 256 / 8192) = floor(125.0) = 125
        // min_category = ceil(4000 * 256 / 8192) = 125 → 125 >= 125 ✓
        fsm.record_free_space(rel(1), blk(0), 4000);
        let found = fsm.find_block_with_at_least(rel(1), 4000);
        assert_eq!(found, Some(blk(0)));
    }

    #[test]
    fn full_page_not_returned() {
        let fsm = FreeSpaceMap::new();
        // Record block 0 as completely full.
        fsm.record_free_space(rel(2), blk(0), 0);
        let found = fsm.find_block_with_at_least(rel(2), 1);
        assert!(found.is_none());
    }

    #[test]
    fn find_returns_first_sufficient() {
        let fsm = FreeSpaceMap::new();
        fsm.record_free_space(rel(3), blk(0), 0);
        fsm.record_free_space(rel(3), blk(1), 0);
        fsm.record_free_space(rel(3), blk(2), 3000);
        let found = fsm.find_block_with_at_least(rel(3), 2000);
        assert_eq!(found, Some(blk(2)));
    }

    #[test]
    fn invalidate_clears_entry() {
        let fsm = FreeSpaceMap::new();
        fsm.record_free_space(rel(4), blk(0), 4000);
        assert!(fsm.find_block_with_at_least(rel(4), 1000).is_some());
        fsm.invalidate_block(rel(4), blk(0));
        assert!(fsm.find_block_with_at_least(rel(4), 1000).is_none());
    }

    #[test]
    fn unknown_relation_returns_none() {
        let fsm = FreeSpaceMap::new();
        assert!(fsm.find_block_with_at_least(rel(99), 1).is_none());
    }

    #[test]
    fn invalidate_out_of_range_is_noop() {
        let fsm = FreeSpaceMap::new();
        // Should not panic when block is beyond current range.
        fsm.invalidate_block(rel(5), blk(100));
    }

    #[test]
    fn sparse_block_numbers_work() {
        let fsm = FreeSpaceMap::new();
        // 5120 = 160 * 32 is a clean multiple of PAGE_SIZE/256 = 32.
        // category = floor(5120 * 256 / 8192) = floor(160.0) = 160
        // min_category = ceil(5120 * 256 / 8192) = 160 → 160 >= 160 ✓
        fsm.record_free_space(rel(6), blk(0), 0);
        fsm.record_free_space(rel(6), blk(100), 5120);
        let found = fsm.find_block_with_at_least(rel(6), 5120);
        assert_eq!(found, Some(blk(100)));
    }

    #[test]
    fn empty_page_category_is_max() {
        let fsm = FreeSpaceMap::new();
        fsm.record_free_space(rel(7), blk(0), PAGE_SIZE_U32);
        let found = fsm.find_block_with_at_least(rel(7), PAGE_SIZE_U32);
        assert_eq!(found, Some(blk(0)));
    }

    #[test]
    fn category_helpers_clamp_and_convert_without_wrapping() {
        assert_eq!(category_byte(0), 0);
        assert_eq!(category_byte(255), 255);
        assert_eq!(category_byte(u32::MAX), 255);
        assert_eq!(block_index(blk(42)), Some(42));
    }

    #[test]
    fn find_with_zero_min_free_returns_any_block() {
        let fsm = FreeSpaceMap::new();
        // category 0 = full, but asking for 0 bytes should still find it
        fsm.record_free_space(rel(8), blk(0), 0);
        let found = fsm.find_block_with_at_least(rel(8), 0);
        assert_eq!(found, Some(blk(0)));
    }
}
