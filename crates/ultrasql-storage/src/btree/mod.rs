//! B+ tree index access method (Lehman-Yao B-link tree variant).
//!
//! The tree indexes a single-column key (any type implementing [`Key`])
//! to a [`ultrasql_core::TupleId`]. Pages are obtained from the [`BufferPool`] and the
//! tree's per-page metadata lives in the page's *special* area, leaving
//! the body for packed key/value entries.
//!
//! Architecture
//! ------------
//!
//! - **Leaf pages** hold `[key | TupleId]` entries in ascending key
//!   order. Every leaf page maintains a `right_link` to its right
//!   sibling, threading the leaves into a singly-linked list for
//!   forward range scans.
//! - **Internal pages** hold `[key | child_block]` entries in ascending
//!   key order. The first entry's key is a sentinel ([`i64::MIN`]) so
//!   binary search routes correctly to the leftmost subtree. Internal
//!   pages also carry a `right_link` so concurrent readers traversing
//!   a node mid-split can follow the link to the newly-allocated right
//!   sibling instead of blocking.
//! - **Splits** allocate a new right sibling, copy the upper half of
//!   the keys to it, set the right sibling's `right_link` to the old
//!   page's old `right_link`, then atomically update the old page's
//!   `right_link` to the new sibling and update its `high_key` to the
//!   split key. The parent insert is then performed bottom-up.
//! - **Lehman-Yao reads** never block on a split: a reader that hits a
//!   page whose `high_key` is strictly less than or equal to the search
//!   key follows the page's `right_link` instead of recursing. This
//!   means the parent's outdated child pointer is benign — readers
//!   always reach the right leaf via the sibling chain.
//!
//! Module layout
//! -------------
//!
//! - This file holds the [`BTree`] struct, the [`Key`] trait, the
//!   [`BTreeError`] type, the on-page constants, and the
//!   `create`/`open`/`root_block` methods.
//! - `node` holds the on-page layout: `NodeMeta`, leaf/internal
//!   entry encodings, page initialisation, and the descent /
//!   right-link helpers.
//! - `insert` holds the insertion path — leaf split, internal
//!   split, and split propagation up to a new root.
//! - `lookup` holds the point-lookup path and the descent helpers
//!   that walk an existing tree to the relevant leaf.
//! - `iter` holds the forward and backward range iterators.
//! - `adapters` holds the expression / partial / covering index
//!   wrappers plus the composite-key type and the concurrent-build
//!   state machine.
//! - `vacuum` holds the `VACUUM` cleanup pass.
//!
//! On-page layout
//! --------------
//!
//! Each B-tree page reuses the standard page header. The body holds a
//! packed array of fixed-size entries from `PAGE_HEADER_SIZE` up to
//! `pd_lower` (which advances forward as entries are inserted). The
//! special area at the tail of the page stores the node metadata.
//!
//! ```text
//! 0                                                              PAGE_SIZE
//! +--------------+-------------------------------+-------------+--------+
//! |  Header (24) | packed entries  (grows ----->)|  free       | Meta   |
//! +--------------+-------------------------------+-------------+--------+
//!                ^                                ^             ^
//!                PAGE_HEADER_SIZE                 pd_lower      pd_special
//! ```
//!
//! Concurrency
//! -----------
//!
//! Each index relation has shared same-process state in the buffer pool:
//! a relation-level operation latch and a monotonic block allocator.
//! Inserts, deletes, and vacuum take the write side of the latch; point
//! probes take the read side. This conservative policy preserves index
//! correctness for reopened statement handles while the page-level
//! latch-coupling implementation is still pending.
//!
//! TODO(v1.0): replace the relation-level operation latch with latch
//! coupling and a structure-modification log.
//!
//! Limits
//! ------
//!
//! - Keys are fixed-size (8 bytes for [`i64`]). Variable-length keys
//!   (text, composite tuples) are a v1.0 concern.
//! - [`BTree::insert`] enforces unique keys. [`BTree::insert_non_unique`]
//!   stores duplicate keys as adjacent `(key, TupleId)` leaf entries for
//!   plain secondary indexes.
//! - Deletions are not yet implemented (insert + read-only at v0.5).

#![allow(clippy::type_complexity)]

use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use crate::buffer_pool::{BufferPool, BufferPoolError, PageLoader};
use crate::page::PageError;
use crate::wal_sink::WalSinkError;
use parking_lot::{Mutex, RwLock};
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::endian::{read_i64_le, write_i64_le};
use ultrasql_core::{BlockNumber, PageId, RelationId};

mod adapters;
mod insert;
mod iter;
mod lookup;
mod node;
mod vacuum;

#[cfg(test)]
mod tests;

pub use adapters::{
    CompositeKey, ConcurrentBuildStatus, ConcurrentIndexBuilder, CoveringEntry,
    CoveringIndexAdapter, ExprIndexAdapter, PartialIndexAdapter,
};
pub use iter::{BackwardRangeIter, RangeIter};

use node::{NodeMeta, init_btree_page};

// --- tunable parameters ----------------------------------------------------

/// Maximum number of entries per leaf page.
///
/// Tuned for v0.5 so that multi-level trees are reachable in the unit
/// tests. v1.0 will switch to a page-fill-based split policy.
pub(super) const MAX_LEAF_ENTRIES: usize = 32;

/// Maximum number of entries per internal page.
pub(super) const MAX_INTERNAL_ENTRIES: usize = 16;

/// Size of an internal-node entry in bytes: `[key (8) | child_block (4) | pad (4)]`.
pub(super) const INTERNAL_ENTRY_SIZE: usize = 16;

/// Size of a leaf-node entry in bytes: `[key (8) | rel (4) | block (4) | slot (2) | pad (2)]`.
pub(super) const LEAF_ENTRY_SIZE: usize = 20;

/// Size of the per-node metadata block on disk.
pub(super) const NODE_META_SIZE: usize = 24;

/// Special-area offset within a B-tree page.
pub(super) const NODE_SPECIAL_OFFSET: usize = PAGE_SIZE - NODE_META_SIZE;

/// Sentinel meaning "no right sibling."
pub(super) const NO_SIBLING: u32 = u32::MAX;

/// Bit in the node-meta `flags` field indicating a leaf page.
pub(super) const FLAG_LEAF: u16 = 1 << 0;

/// Bit in the node-meta `flags` field indicating that `high_key` is set.
pub(super) const FLAG_HAS_HIGH_KEY: u16 = 1 << 1;

// --- errors ----------------------------------------------------------------

/// Errors returned by the B+ tree API.
#[derive(Debug, thiserror::Error)]
pub enum BTreeError {
    /// A buffer-pool operation failed (typically pool exhaustion).
    #[error("buffer pool error: {0}")]
    BufferPool(#[from] BufferPoolError),

    /// A page-level invariant violation or decoding failure.
    #[error("page error: {0}")]
    Page(#[from] PageError),

    /// The supplied key encodes to more than the configured key size.
    #[error("key too large")]
    KeyTooLarge,

    /// The key already exists in the index. The B-tree is currently a
    /// unique index.
    #[error("duplicate key in index")]
    DuplicateKey,

    /// A node on disk has a layout the reader does not understand.
    #[error("malformed btree node: {0}")]
    MalformedNode(&'static str),

    /// The WAL sink rejected a record emitted for a B-tree mutation.
    #[error("wal sink: {0}")]
    Wal(#[from] WalSinkError),

    /// Encoding a typed WAL payload failed.
    #[error("wal payload encoding: {0}")]
    WalPayload(#[from] ultrasql_wal::payload::PayloadError),

    /// Encoding a full WAL record failed.
    #[error("wal record encoding: {0}")]
    WalRecord(#[from] ultrasql_wal::WalRecordError),
}

// --- key trait -------------------------------------------------------------

/// Keys stored in the tree.
///
/// v0.5 supports only fixed-size keys. Implementations must produce a
/// stable, lexicographically-comparable little-endian encoding of
/// exactly [`Key::SIZE`] bytes.
pub trait Key: Copy + Ord + std::fmt::Debug + 'static {
    /// Encoded length in bytes. Must match the leaf/internal entry
    /// layout this crate uses (currently 8).
    const SIZE: usize;

    /// Encode the key into a buffer of length [`Key::SIZE`].
    fn encode(self, out: &mut [u8]);

    /// Decode the key from a buffer of length [`Key::SIZE`].
    fn decode(bytes: &[u8]) -> Self;
}

impl Key for i64 {
    const SIZE: usize = 8;

    #[inline]
    fn encode(self, out: &mut [u8]) {
        write_i64_le(out, self);
    }

    #[inline]
    fn decode(bytes: &[u8]) -> Self {
        read_i64_le(bytes).unwrap_or(0)
    }
}

// --- B-tree ----------------------------------------------------------------

/// A concurrent Lehman-Yao B-link tree over the buffer pool.
///
/// The tree owns its root block number (via an internal mutex so that
/// concurrent readers can observe an up-to-date root after a write
/// causes the root to split).
#[derive(Debug)]
pub struct BTree<L: PageLoader> {
    pub(super) pool: Arc<BufferPool<L>>,
    pub(super) rel: RelationId,
    pub(super) root_block: Mutex<BlockNumber>,
    /// Relation-level operation latch shared by reopened handles for
    /// this index relation.
    pub(super) op_latch: Arc<RwLock<()>>,
    /// Monotonically increasing block allocator shared by reopened
    /// handles for this index relation.
    pub(super) next_block: Arc<AtomicU32>,
}

impl<L: PageLoader> BTree<L> {
    /// Initialise a new empty tree at a fresh page.
    ///
    /// The root is a leaf with no entries and no right sibling.
    pub fn create(pool: Arc<BufferPool<L>>, rel: RelationId) -> Result<Self, BTreeError> {
        let root_block = BlockNumber::new(0);
        let op_latch = pool.btree_latch(rel);
        let next_block = pool.btree_block_allocator(rel, 1);
        let tree = Self {
            pool,
            rel,
            root_block: Mutex::new(root_block),
            op_latch,
            next_block,
        };
        // Materialize the root as a fresh empty leaf.
        let guard = tree.pool.get_page(tree.page_id(root_block))?;
        {
            let mut w = guard.write();
            init_btree_page(&mut w, NodeMeta::fresh_leaf())?;
        }
        drop(guard);
        Ok(tree)
    }

    /// Open an existing tree given its root block.
    #[must_use]
    pub fn open(pool: Arc<BufferPool<L>>, rel: RelationId, root_block: BlockNumber) -> Self {
        // Allocate above resident blocks to avoid colliding with pages
        // created by a previous handle for this same index relation.
        // The segment manager will eventually own this; until then the
        // buffer-pool resident set is the authoritative same-process
        // view used by DML and index scans.
        let next = pool
            .max_resident_block(rel)
            .map_or(root_block, |block| block)
            .raw()
            .saturating_add(1);
        let op_latch = pool.btree_latch(rel);
        let next_block = pool.btree_block_allocator(rel, next);
        Self {
            pool,
            rel,
            root_block: Mutex::new(root_block),
            op_latch,
            next_block,
        }
    }

    /// Block number of the current root. Useful for persisting the
    /// tree identity in a catalog.
    pub fn root_block(&self) -> BlockNumber {
        *self.root_block.lock()
    }

    pub(super) const fn page_id(&self, block: BlockNumber) -> PageId {
        PageId::new(self.rel, block)
    }
}
