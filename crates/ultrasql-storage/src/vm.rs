//! Visibility map (VM).
//!
//! The VM records, per relation, which pages are *all-visible* (every live
//! tuple on the page is visible to every active snapshot) and/or
//! *all-frozen* (every tuple's `xmin` has been frozen). Both properties
//! enable important optimisations:
//!
//! - **All-visible**: an index-only scan can skip the heap fetch for any
//!   tuple on a VM-visible page because visibility is guaranteed.
//! - **All-frozen**: vacuum skips these pages on the next cycle, reducing
//!   I/O.
//!
//! # Representation
//!
//! Each block occupies 2 bits in a byte vector:
//!
//! ```text
//!  bit layout per block:  [ bit 1: frozen | bit 0: visible ]
//! ```
//!
//! All bits for a relation are packed into a `Vec<u8>` where block `b`
//! uses bits `(b*2)/8` and `(b*2)%8` within that byte.
//!
//! # Lifecycle
//!
//! - Heap `insert`, `update`, and `delete` **clear** the visible bit for
//!   any touched page (a modification makes the page not all-visible).
//! - Vacuum (future) **sets** the visible and frozen bits after scanning
//!   and rewriting old tuples.
//!
//! # Persistence
//!
//! This version is in-memory. A persistent backing will follow in v0.4
//! once the segment layer is fully wired.
//!
//! # Thread safety
//!
//! `VisibilityMap` is `Send + Sync` through its `DashMap` sharding and
//! per-relation `RwLock`.

use dashmap::DashMap;
use parking_lot::RwLock;
use ultrasql_core::{BlockNumber, RelationId};

const BIT_VISIBLE: u8 = 0b01;
const BIT_FROZEN: u8 = 0b10;

/// Per-relation, in-memory visibility map.
#[derive(Debug, Default)]
pub struct VisibilityMap {
    /// `relation → bit-packed Vec<u8>`, 2 bits per block.
    inner: DashMap<RelationId, RwLock<Vec<u8>>>,
}

impl VisibilityMap {
    /// Create a new, empty visibility map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return `true` if every live tuple on `block` is visible to all
    /// active snapshots.
    #[must_use]
    pub fn is_all_visible(&self, rel: RelationId, block: BlockNumber) -> bool {
        self.get_bits(rel, block) & BIT_VISIBLE != 0
    }

    /// Return `true` if every tuple on `block` has a frozen `xmin`.
    #[must_use]
    pub fn is_all_frozen(&self, rel: RelationId, block: BlockNumber) -> bool {
        self.get_bits(rel, block) & BIT_FROZEN != 0
    }

    /// Mark a page as all-visible.
    ///
    /// Called by vacuum after verifying that all live tuples on the page
    /// are visible to the oldest active snapshot.
    pub fn mark_all_visible(&self, rel: RelationId, block: BlockNumber) {
        self.set_bits(rel, block, BIT_VISIBLE, true);
    }

    /// Mark a page as all-frozen.
    ///
    /// Called by vacuum after rewriting every tuple's `xmin` to
    /// [`Xid::FROZEN`](ultrasql_core::Xid::FROZEN).
    pub fn mark_all_frozen(&self, rel: RelationId, block: BlockNumber) {
        self.set_bits(rel, block, BIT_FROZEN, true);
    }

    /// Clear all VM bits for a block.
    ///
    /// Called whenever the heap modifies any tuple on the page (insert,
    /// update, delete). Clearing ensures that index-only scans do not
    /// skip heap fetches on a page that is no longer all-visible.
    pub fn clear(&self, rel: RelationId, block: BlockNumber) {
        let Some(entry) = self.inner.get(&rel) else {
            return;
        };
        let mut vec = entry.write();
        let Some((byte_idx, shift)) = byte_index_and_shift(block) else {
            return;
        };
        if byte_idx >= vec.len() {
            return;
        }
        vec[byte_idx] &= !((BIT_VISIBLE | BIT_FROZEN) << shift);
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Read the 2 VM bits for `block` (bits 0–1 of the returned byte).
    fn get_bits(&self, rel: RelationId, block: BlockNumber) -> u8 {
        let Some(entry) = self.inner.get(&rel) else {
            return 0;
        };
        let vec = entry.read();
        let Some((byte_idx, shift)) = byte_index_and_shift(block) else {
            return 0;
        };
        if byte_idx >= vec.len() {
            return 0;
        }
        (vec[byte_idx] >> shift) & 0b11
    }

    /// Set or clear the bits in `mask` for `block`.
    #[allow(clippy::significant_drop_tightening)]
    fn set_bits(&self, rel: RelationId, block: BlockNumber, mask: u8, set: bool) {
        // `entry` must outlive `vec` because `vec` borrows from it.
        let entry = self
            .inner
            .entry(rel)
            .or_insert_with(|| RwLock::new(Vec::new()));
        let mut vec = entry.write();
        let Some((byte_idx, shift)) = byte_index_and_shift(block) else {
            return;
        };
        if byte_idx >= vec.len() {
            if !set {
                // Clearing a bit that does not exist is a no-op.
                return;
            }
            vec.resize(byte_idx + 1, 0);
        }
        if set {
            vec[byte_idx] |= mask << shift;
        } else {
            vec[byte_idx] &= !(mask << shift);
        }
    }
}

/// Byte index and bit-pair shift for `block`.
fn byte_index_and_shift(block: BlockNumber) -> Option<(usize, u32)> {
    let block = usize::try_from(block.raw()).ok()?;
    let bit_offset = block.checked_mul(2)?;
    let byte_idx = bit_offset / 8;
    let shift = u32::try_from(bit_offset % 8).ok()?;
    Some((byte_idx, shift))
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
    fn byte_index_and_shift_converts_without_wrapping() {
        assert_eq!(byte_index_and_shift(blk(0)), Some((0, 0)));
        assert_eq!(byte_index_and_shift(blk(3)), Some((0, 6)));
        assert_eq!(byte_index_and_shift(blk(4)), Some((1, 0)));
    }

    #[test]
    fn fresh_map_everything_invisible_and_unfrozen() {
        let vm = VisibilityMap::new();
        assert!(!vm.is_all_visible(rel(1), blk(0)));
        assert!(!vm.is_all_frozen(rel(1), blk(0)));
    }

    #[test]
    fn mark_visible_roundtrip() {
        let vm = VisibilityMap::new();
        vm.mark_all_visible(rel(1), blk(5));
        assert!(vm.is_all_visible(rel(1), blk(5)));
        assert!(!vm.is_all_frozen(rel(1), blk(5)));
    }

    #[test]
    fn mark_frozen_roundtrip() {
        let vm = VisibilityMap::new();
        vm.mark_all_frozen(rel(2), blk(3));
        assert!(vm.is_all_frozen(rel(2), blk(3)));
        assert!(!vm.is_all_visible(rel(2), blk(3)));
    }

    #[test]
    fn mark_both_bits() {
        let vm = VisibilityMap::new();
        vm.mark_all_visible(rel(3), blk(0));
        vm.mark_all_frozen(rel(3), blk(0));
        assert!(vm.is_all_visible(rel(3), blk(0)));
        assert!(vm.is_all_frozen(rel(3), blk(0)));
    }

    #[test]
    fn clear_resets_both_bits() {
        let vm = VisibilityMap::new();
        vm.mark_all_visible(rel(4), blk(7));
        vm.mark_all_frozen(rel(4), blk(7));
        vm.clear(rel(4), blk(7));
        assert!(!vm.is_all_visible(rel(4), blk(7)));
        assert!(!vm.is_all_frozen(rel(4), blk(7)));
    }

    #[test]
    fn clear_on_untracked_block_is_noop() {
        let vm = VisibilityMap::new();
        // Should not panic.
        vm.clear(rel(5), blk(1000));
    }

    #[test]
    fn different_relations_are_independent() {
        let vm = VisibilityMap::new();
        vm.mark_all_visible(rel(1), blk(0));
        assert!(vm.is_all_visible(rel(1), blk(0)));
        assert!(!vm.is_all_visible(rel(2), blk(0)));
    }

    #[test]
    fn adjacent_blocks_do_not_alias() {
        let vm = VisibilityMap::new();
        // Blocks 3 and 4 share or border bytes in the packed representation.
        vm.mark_all_visible(rel(6), blk(3));
        assert!(vm.is_all_visible(rel(6), blk(3)));
        assert!(!vm.is_all_visible(rel(6), blk(4)));
        vm.mark_all_frozen(rel(6), blk(4));
        assert!(vm.is_all_frozen(rel(6), blk(4)));
        assert!(!vm.is_all_frozen(rel(6), blk(3)));
    }

    #[test]
    fn large_block_number_works() {
        let vm = VisibilityMap::new();
        vm.mark_all_visible(rel(7), blk(10_000));
        assert!(vm.is_all_visible(rel(7), blk(10_000)));
        assert!(!vm.is_all_visible(rel(7), blk(9_999)));
    }
}
