//! B+ tree index access method (Lehman-Yao B-link tree variant).
//!
//! The tree indexes a single-column key (any type implementing [`Key`])
//! to a [`TupleId`]. Pages are obtained from the [`BufferPool`] and the
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
//! On-page layout
//! --------------
//!
//! Each B-tree page reuses the standard page header. The body holds a
//! packed array of fixed-size entries from `PAGE_HEADER_SIZE` up to
//! `pd_lower` (which advances forward as entries are inserted). The
//! special area at the tail of the page stores [`NodeMeta`].
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
//! v0.5 ships with a *single-writer* assumption: callers must
//! serialize concurrent `insert` calls externally. Concurrent readers
//! work safely against a single in-flight writer because the buffer
//! pool's per-frame `RwLock` enforces shared/exclusive access on each
//! page, and the right-link mechanism ensures readers that observe an
//! in-flight split simply chase the right link. Concurrent writers may
//! panic under contention; see the TODO below.
//!
//! TODO(v1.0): support multiple concurrent writers with latch coupling
//! and a structure-modification log.
//!
//! Limits
//! ------
//!
//! - Keys are fixed-size (8 bytes for [`i64`]). Variable-length keys
//!   (text, composite tuples) are a v1.0 concern.
//! - Duplicate keys are rejected with [`BTreeError::DuplicateKey`].
//! - Deletions are not yet implemented (insert + read-only at v0.5).

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::endian::{
    read_i64_le, read_u16_le, read_u32_le, write_i64_le, write_u16_le, write_u32_le,
};
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};

use crate::buffer_pool::{BufferPool, BufferPoolError, PageGuard, PageLoader};
use crate::page::{PAGE_HEADER_SIZE, Page, PageError, PageHeader, PageKind};

// --- tunable parameters ----------------------------------------------------

/// Maximum number of entries per leaf page.
///
/// Tuned for v0.5 so that multi-level trees are reachable in the unit
/// tests. v1.0 will switch to a page-fill-based split policy.
const MAX_LEAF_ENTRIES: usize = 32;

/// Maximum number of entries per internal page.
const MAX_INTERNAL_ENTRIES: usize = 16;

/// Size of an internal-node entry in bytes: `[key (8) | child_block (4) | pad (4)]`.
const INTERNAL_ENTRY_SIZE: usize = 16;

/// Size of a leaf-node entry in bytes: `[key (8) | rel (4) | block (4) | slot (2) | pad (2)]`.
const LEAF_ENTRY_SIZE: usize = 20;

/// Size of [`NodeMeta`] on disk.
const NODE_META_SIZE: usize = 24;

/// Special-area offset within a B-tree page.
const NODE_SPECIAL_OFFSET: usize = PAGE_SIZE - NODE_META_SIZE;

/// Sentinel meaning "no right sibling."
const NO_SIBLING: u32 = u32::MAX;

/// Bit in [`NodeMeta::flags`] indicating a leaf page.
const FLAG_LEAF: u16 = 1 << 0;

/// Bit in [`NodeMeta::flags`] indicating that [`NodeMeta::high_key`] is set.
const FLAG_HAS_HIGH_KEY: u16 = 1 << 1;

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

// --- node metadata ---------------------------------------------------------

/// Per-page B-tree metadata stored in the page's special area.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NodeMeta {
    /// Block number of the right sibling, or [`NO_SIBLING`] if none.
    right_link: u32,
    /// The split key for this page: any search key `>= high_key` lives
    /// in the right sibling (or further). Only valid when
    /// [`FLAG_HAS_HIGH_KEY`] is set in `flags`.
    high_key: i64,
    /// Tree depth from this node down to a leaf (0 for leaves).
    level: u16,
    /// Number of entries currently on the page.
    n_keys: u16,
    /// Flag bits — see [`FLAG_LEAF`], [`FLAG_HAS_HIGH_KEY`].
    flags: u16,
}

impl NodeMeta {
    const fn fresh_leaf() -> Self {
        Self {
            right_link: NO_SIBLING,
            high_key: 0,
            level: 0,
            n_keys: 0,
            flags: FLAG_LEAF,
        }
    }

    const fn fresh_internal(level: u16) -> Self {
        Self {
            right_link: NO_SIBLING,
            high_key: 0,
            level,
            n_keys: 0,
            flags: 0,
        }
    }

    #[inline]
    const fn is_leaf(self) -> bool {
        self.flags & FLAG_LEAF != 0
    }

    #[inline]
    const fn has_high_key(self) -> bool {
        self.flags & FLAG_HAS_HIGH_KEY != 0
    }

    fn read_from(page: &Page) -> Result<Self, BTreeError> {
        let bytes = page.as_bytes();
        let off = NODE_SPECIAL_OFFSET;
        let right_link = read_u32_le(&bytes[off..off + 4])
            .map_err(|_| BTreeError::MalformedNode("right_link"))?;
        let high_key = read_i64_le(&bytes[off + 4..off + 12])
            .map_err(|_| BTreeError::MalformedNode("high_key"))?;
        let level = read_u16_le(&bytes[off + 12..off + 14])
            .map_err(|_| BTreeError::MalformedNode("level"))?;
        let n_keys = read_u16_le(&bytes[off + 14..off + 16])
            .map_err(|_| BTreeError::MalformedNode("n_keys"))?;
        let flags = read_u16_le(&bytes[off + 16..off + 18])
            .map_err(|_| BTreeError::MalformedNode("flags"))?;
        Ok(Self {
            right_link,
            high_key,
            level,
            n_keys,
            flags,
        })
    }

    fn write_into(self, page: &mut Page) {
        let bytes = page.as_bytes_mut();
        let off = NODE_SPECIAL_OFFSET;
        write_u32_le(&mut bytes[off..off + 4], self.right_link);
        write_i64_le(&mut bytes[off + 4..off + 12], self.high_key);
        write_u16_le(&mut bytes[off + 12..off + 14], self.level);
        write_u16_le(&mut bytes[off + 14..off + 16], self.n_keys);
        write_u16_le(&mut bytes[off + 16..off + 18], self.flags);
        // Reserved bytes (6) at offsets 18..24 are deliberately left
        // zero so future format extensions can repurpose them.
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
    pool: Arc<BufferPool<L>>,
    rel: RelationId,
    root_block: Mutex<BlockNumber>,
    /// Monotonically increasing block allocator. v0.5 hands out fresh
    /// block numbers without coordination with the segment manager;
    /// production code will route allocation through the segment layer.
    next_block: AtomicU32,
}

impl<L: PageLoader> BTree<L> {
    /// Initialise a new empty tree at a fresh page.
    ///
    /// The root is a leaf with no entries and no right sibling.
    pub fn create(pool: Arc<BufferPool<L>>, rel: RelationId) -> Result<Self, BTreeError> {
        let root_block = BlockNumber::new(0);
        let tree = Self {
            pool,
            rel,
            root_block: Mutex::new(root_block),
            next_block: AtomicU32::new(1),
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
    pub const fn open(pool: Arc<BufferPool<L>>, rel: RelationId, root_block: BlockNumber) -> Self {
        // Allocate above any existing block to avoid colliding with on-
        // disk content. The actual maximum is unknown from this entry
        // point; the segment manager will hand us the correct value
        // when integration lands. For v0.5 we conservatively start one
        // past the root.
        let next = root_block.raw().saturating_add(1);
        Self {
            pool,
            rel,
            root_block: Mutex::new(root_block),
            next_block: AtomicU32::new(next),
        }
    }

    /// Block number of the current root. Useful for persisting the
    /// tree identity in a catalog.
    pub fn root_block(&self) -> BlockNumber {
        *self.root_block.lock()
    }

    /// Insert `(key, value)` into the tree.
    ///
    /// Returns [`BTreeError::DuplicateKey`] if `key` is already
    /// present.
    pub fn insert<K: Key>(&mut self, key: K, value: TupleId) -> Result<(), BTreeError> {
        // v0.5 only supports 8-byte keys (i64-shaped). The trait is
        // generic so callers can plug a custom Key type later; we
        // decode keys as i64 internally.
        if K::SIZE != 8 {
            return Err(BTreeError::KeyTooLarge);
        }

        let mut key_buf = [0_u8; 8];
        key.encode(&mut key_buf);
        let raw_key = read_i64_le(&key_buf).map_err(|_| BTreeError::MalformedNode("key encode"))?;

        // Descend, remembering the path so we can propagate splits up.
        let root = *self.root_block.lock();
        let mut path: Vec<BlockNumber> = Vec::new();
        let leaf_block = self.descend_to_leaf(root, raw_key, &mut path)?;

        // Try to insert into the leaf, splitting if necessary.
        let split_result = self.insert_into_leaf(leaf_block, raw_key, value)?;

        if let Some((sep_key, new_right)) = split_result {
            self.propagate_split(path, sep_key, new_right)?;
        }
        Ok(())
    }

    /// Point lookup. Returns `None` if the key is absent.
    pub fn lookup<K: Key>(&self, key: K) -> Result<Option<TupleId>, BTreeError> {
        if K::SIZE != 8 {
            return Err(BTreeError::KeyTooLarge);
        }
        let mut buf = [0_u8; 8];
        key.encode(&mut buf);
        let raw_key = read_i64_le(&buf).map_err(|_| BTreeError::MalformedNode("key encode"))?;

        let root = *self.root_block.lock();
        let leaf = self.descend_to_leaf_readonly(root, raw_key)?;
        self.lookup_in_leaf(leaf, raw_key)
    }

    /// Forward range scan from `start` (inclusive) to `end` (exclusive
    /// if provided, unbounded otherwise).
    pub const fn range_scan<K: Key>(&self, start: K, end: Option<K>) -> RangeIter<'_, L, K> {
        RangeIter {
            tree: self,
            current_leaf: None,
            current_slot: 0,
            start,
            end,
            started: false,
            _key_marker: PhantomData,
        }
    }

    // ----------- descent helpers ----------------------------------------

    fn descend_to_leaf(
        &self,
        root: BlockNumber,
        key: i64,
        path: &mut Vec<BlockNumber>,
    ) -> Result<BlockNumber, BTreeError> {
        let mut current = root;
        loop {
            let guard = self.pool.get_page(self.page_id(current))?;
            let step = step_descend(&guard, key)?;
            drop(guard);
            match step {
                DescendStep::ChaseRight(next) => current = next,
                DescendStep::ReachedLeaf => return Ok(current),
                DescendStep::Descend(child) => {
                    path.push(current);
                    current = child;
                }
            }
        }
    }

    fn descend_to_leaf_readonly(
        &self,
        root: BlockNumber,
        key: i64,
    ) -> Result<BlockNumber, BTreeError> {
        let mut current = root;
        loop {
            let guard = self.pool.get_page(self.page_id(current))?;
            let step = step_descend(&guard, key)?;
            drop(guard);
            match step {
                DescendStep::ChaseRight(next) => current = next,
                DescendStep::ReachedLeaf => return Ok(current),
                DescendStep::Descend(child) => current = child,
            }
        }
    }

    // ----------- insert helpers -----------------------------------------

    /// Insert into the named leaf. Returns `Some((separator, right_sibling))`
    /// if the leaf split, `None` otherwise.
    fn insert_into_leaf(
        &self,
        leaf: BlockNumber,
        key: i64,
        value: TupleId,
    ) -> Result<Option<(i64, BlockNumber)>, BTreeError> {
        // Right-link chase if a concurrent writer (or our own past-self
        // in a stale path) has split this leaf out from under us.
        let mut current = leaf;
        loop {
            let guard = self.pool.get_page(self.page_id(current))?;
            let outcome = self.try_leaf_insert(&guard, key, value)?;
            drop(guard);
            match outcome {
                LeafInsertOutcome::ChaseRight(next) => current = next,
                LeafInsertOutcome::Inserted => return Ok(None),
                LeafInsertOutcome::Split { sep_key, new_block } => {
                    return Ok(Some((sep_key, new_block)));
                }
            }
        }
    }

    fn try_leaf_insert(
        &self,
        guard: &PageGuard<L>,
        key: i64,
        value: TupleId,
    ) -> Result<LeafInsertOutcome, BTreeError> {
        let mut w = guard.write();
        let meta = NodeMeta::read_from(&w)?;
        debug_assert!(meta.is_leaf(), "descended to non-leaf in insert");
        if let Some(next) = should_chase_right(meta, key) {
            drop(w);
            return Ok(LeafInsertOutcome::ChaseRight(BlockNumber::new(next)));
        }

        // Search for the insertion position; reject duplicates.
        let entries = read_leaf_entries(&w, meta.n_keys)?;
        let pos_result = entries.binary_search_by_key(&key, |e| e.key);
        if pos_result.is_ok() {
            return Err(BTreeError::DuplicateKey);
        }
        let pos = pos_result.unwrap_or_else(|i| i);

        if (meta.n_keys as usize) < MAX_LEAF_ENTRIES {
            let mut new_entries = entries;
            new_entries.insert(pos, LeafEntry { key, value });
            write_leaf_entries(&mut w, &new_entries);
            let new_meta = NodeMeta {
                n_keys: u16::try_from(new_entries.len())
                    .map_err(|_| BTreeError::MalformedNode("leaf overflow"))?,
                ..meta
            };
            new_meta.write_into(&mut w);
            drop(w);
            return Ok(LeafInsertOutcome::Inserted);
        }

        // Split.
        let mut all = entries;
        all.insert(pos, LeafEntry { key, value });
        let mid = all.len() / 2;
        let right = all.split_off(mid);
        let sep_key = right[0].key;

        // Allocate a new sibling.
        let new_block = self.allocate_block();
        {
            let right_guard = self.pool.get_page(self.page_id(new_block))?;
            {
                let mut rw = right_guard.write();
                let right_meta = NodeMeta {
                    right_link: meta.right_link,
                    high_key: if meta.has_high_key() {
                        meta.high_key
                    } else {
                        0
                    },
                    level: 0,
                    n_keys: u16::try_from(right.len())
                        .map_err(|_| BTreeError::MalformedNode("right leaf overflow"))?,
                    flags: FLAG_LEAF
                        | if meta.has_high_key() {
                            FLAG_HAS_HIGH_KEY
                        } else {
                            0
                        },
                };
                init_btree_page(&mut rw, right_meta)?;
                write_leaf_entries(&mut rw, &right);
                right_meta.write_into(&mut rw);
            }
            drop(right_guard);
        }

        // Update old leaf: shrink to lower half, set high_key, right_link.
        write_leaf_entries(&mut w, &all);
        let new_meta = NodeMeta {
            right_link: new_block.raw(),
            high_key: sep_key,
            level: 0,
            n_keys: u16::try_from(all.len())
                .map_err(|_| BTreeError::MalformedNode("left leaf overflow"))?,
            flags: FLAG_LEAF | FLAG_HAS_HIGH_KEY,
        };
        new_meta.write_into(&mut w);
        drop(w);
        Ok(LeafInsertOutcome::Split { sep_key, new_block })
    }

    fn lookup_in_leaf(&self, leaf: BlockNumber, key: i64) -> Result<Option<TupleId>, BTreeError> {
        let mut current = leaf;
        loop {
            let guard = self.pool.get_page(self.page_id(current))?;
            let probe = probe_leaf(&guard, key)?;
            drop(guard);
            match probe {
                LeafProbe::ChaseRight(next) => current = next,
                LeafProbe::Found(value) => return Ok(Some(value)),
                LeafProbe::Missing => return Ok(None),
            }
        }
    }

    fn propagate_split(
        &self,
        mut path: Vec<BlockNumber>,
        mut sep_key: i64,
        mut new_right: BlockNumber,
    ) -> Result<(), BTreeError> {
        while let Some(parent) = path.pop() {
            match self.insert_into_internal(parent, sep_key, new_right)? {
                None => return Ok(()),
                Some((k, r)) => {
                    sep_key = k;
                    new_right = r;
                }
            }
        }
        // The root split. Create a new root.
        let old_root = *self.root_block.lock();
        let new_root_block = self.allocate_block();
        let level = self.read_level(old_root)? + 1;
        {
            let guard = self.pool.get_page(self.page_id(new_root_block))?;
            {
                let mut w = guard.write();
                let meta = NodeMeta::fresh_internal(level);
                init_btree_page(&mut w, meta)?;
                // Two initial children: old root (covers (-inf, sep_key))
                // and the new right sibling (covers [sep_key, +inf)). We
                // encode the leftmost child as an entry with key = i64::MIN
                // so binary search routes correctly.
                let entries = [
                    InternalEntry {
                        key: i64::MIN,
                        child: old_root.raw(),
                    },
                    InternalEntry {
                        key: sep_key,
                        child: new_right.raw(),
                    },
                ];
                write_internal_entries(&mut w, &entries);
                let new_meta = NodeMeta { n_keys: 2, ..meta };
                new_meta.write_into(&mut w);
            }
            drop(guard);
        }
        *self.root_block.lock() = new_root_block;
        Ok(())
    }

    fn read_level(&self, block: BlockNumber) -> Result<u16, BTreeError> {
        let guard = self.pool.get_page(self.page_id(block))?;
        let level;
        {
            let r = guard.read();
            let meta = NodeMeta::read_from(&r)?;
            level = meta.level;
            drop(r);
        }
        drop(guard);
        Ok(level)
    }

    fn insert_into_internal(
        &self,
        block: BlockNumber,
        sep_key: i64,
        right_child: BlockNumber,
    ) -> Result<Option<(i64, BlockNumber)>, BTreeError> {
        let guard = self.pool.get_page(self.page_id(block))?;
        let outcome = self.try_internal_insert(&guard, sep_key, right_child)?;
        drop(guard);
        Ok(outcome)
    }

    fn try_internal_insert(
        &self,
        guard: &PageGuard<L>,
        sep_key: i64,
        right_child: BlockNumber,
    ) -> Result<Option<(i64, BlockNumber)>, BTreeError> {
        let mut w = guard.write();
        let meta = NodeMeta::read_from(&w)?;
        debug_assert!(!meta.is_leaf(), "internal insert on leaf");

        let entries = read_internal_entries(&w, meta.n_keys)?;
        // Find insertion position. The first entry's key is i64::MIN
        // (leftmost child placeholder); subsequent entries are real
        // separators in ascending order.
        let pos_result = entries.binary_search_by_key(&sep_key, |e| e.key);
        if pos_result.is_ok() {
            // A separator equal to an existing one is impossible in a
            // unique-key tree because we'd have caught the duplicate
            // at the leaf. Treat it as corruption.
            return Err(BTreeError::MalformedNode("duplicate internal separator"));
        }
        let pos = pos_result.unwrap_or_else(|i| i);

        if (meta.n_keys as usize) < MAX_INTERNAL_ENTRIES {
            let mut new_entries = entries;
            new_entries.insert(
                pos,
                InternalEntry {
                    key: sep_key,
                    child: right_child.raw(),
                },
            );
            write_internal_entries(&mut w, &new_entries);
            let new_meta = NodeMeta {
                n_keys: u16::try_from(new_entries.len())
                    .map_err(|_| BTreeError::MalformedNode("internal overflow"))?,
                ..meta
            };
            new_meta.write_into(&mut w);
            drop(w);
            return Ok(None);
        }

        // Split.
        let mut all = entries;
        all.insert(
            pos,
            InternalEntry {
                key: sep_key,
                child: right_child.raw(),
            },
        );
        let mid = all.len() / 2;
        let right = all.split_off(mid);
        // The first key of `right` becomes the parent separator. The
        // right sibling's leftmost entry replaces its key with i64::MIN
        // so the search invariant ("first entry's key is unused / MIN")
        // is preserved.
        let parent_sep = right[0].key;
        let mut right_entries = right;
        right_entries[0].key = i64::MIN;

        let new_block = self.allocate_block();
        {
            let right_guard = self.pool.get_page(self.page_id(new_block))?;
            {
                let mut rw = right_guard.write();
                let right_meta = NodeMeta {
                    right_link: meta.right_link,
                    high_key: if meta.has_high_key() {
                        meta.high_key
                    } else {
                        0
                    },
                    level: meta.level,
                    n_keys: u16::try_from(right_entries.len())
                        .map_err(|_| BTreeError::MalformedNode("right internal overflow"))?,
                    flags: if meta.has_high_key() {
                        FLAG_HAS_HIGH_KEY
                    } else {
                        0
                    },
                };
                init_btree_page(&mut rw, right_meta)?;
                write_internal_entries(&mut rw, &right_entries);
                right_meta.write_into(&mut rw);
            }
            drop(right_guard);
        }

        write_internal_entries(&mut w, &all);
        let new_meta = NodeMeta {
            right_link: new_block.raw(),
            high_key: parent_sep,
            level: meta.level,
            n_keys: u16::try_from(all.len())
                .map_err(|_| BTreeError::MalformedNode("left internal overflow"))?,
            flags: FLAG_HAS_HIGH_KEY,
        };
        new_meta.write_into(&mut w);
        drop(w);
        Ok(Some((parent_sep, new_block)))
    }

    fn allocate_block(&self) -> BlockNumber {
        let raw = self.next_block.fetch_add(1, Ordering::AcqRel);
        BlockNumber::new(raw)
    }

    const fn page_id(&self, block: BlockNumber) -> PageId {
        PageId::new(self.rel, block)
    }
}

// --- internal helper enums -------------------------------------------------

#[derive(Debug)]
enum DescendStep {
    ChaseRight(BlockNumber),
    ReachedLeaf,
    Descend(BlockNumber),
}

#[derive(Debug)]
enum LeafInsertOutcome {
    /// The leaf had been split underneath us; the inserter must follow
    /// the right link to retry on the new sibling.
    ChaseRight(BlockNumber),
    /// The entry was placed without splitting.
    Inserted,
    /// The leaf split; the caller propagates the new separator up to
    /// the parent.
    Split {
        sep_key: i64,
        new_block: BlockNumber,
    },
}

#[derive(Debug)]
enum LeafProbe {
    ChaseRight(BlockNumber),
    Found(TupleId),
    Missing,
}

// --- pure helper functions (no &self) --------------------------------------

fn step_descend<L: PageLoader>(guard: &PageGuard<L>, key: i64) -> Result<DescendStep, BTreeError> {
    let r = guard.read();
    let meta = NodeMeta::read_from(&r)?;
    if let Some(next) = should_chase_right(meta, key) {
        drop(r);
        return Ok(DescendStep::ChaseRight(BlockNumber::new(next)));
    }
    if meta.is_leaf() {
        drop(r);
        return Ok(DescendStep::ReachedLeaf);
    }
    let child = find_child_internal(&r, meta, key)?;
    drop(r);
    Ok(DescendStep::Descend(child))
}

fn probe_leaf<L: PageLoader>(guard: &PageGuard<L>, key: i64) -> Result<LeafProbe, BTreeError> {
    let entries;
    {
        let r = guard.read();
        let meta = NodeMeta::read_from(&r)?;
        if let Some(next) = should_chase_right(meta, key) {
            drop(r);
            return Ok(LeafProbe::ChaseRight(BlockNumber::new(next)));
        }
        entries = read_leaf_entries(&r, meta.n_keys)?;
        drop(r);
    }
    Ok(entries
        .binary_search_by_key(&key, |e| e.key)
        .map_or(LeafProbe::Missing, |i| LeafProbe::Found(entries[i].value)))
}

// --- packed entries --------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct LeafEntry {
    key: i64,
    value: TupleId,
}

#[derive(Clone, Copy, Debug)]
struct InternalEntry {
    key: i64,
    child: u32,
}

fn read_leaf_entries(page: &Page, count: u16) -> Result<Vec<LeafEntry>, BTreeError> {
    let bytes = page.as_bytes();
    let mut out = Vec::with_capacity(count as usize);
    for i in 0..(count as usize) {
        let off = PAGE_HEADER_SIZE + i * LEAF_ENTRY_SIZE;
        if off + LEAF_ENTRY_SIZE > NODE_SPECIAL_OFFSET {
            return Err(BTreeError::MalformedNode("leaf entry out of range"));
        }
        let key =
            read_i64_le(&bytes[off..off + 8]).map_err(|_| BTreeError::MalformedNode("leaf key"))?;
        let rel =
            read_u32_le(&bytes[off + 8..off + 12]).map_err(|_| BTreeError::MalformedNode("rel"))?;
        let block = read_u32_le(&bytes[off + 12..off + 16])
            .map_err(|_| BTreeError::MalformedNode("block"))?;
        let slot = read_u16_le(&bytes[off + 16..off + 18])
            .map_err(|_| BTreeError::MalformedNode("slot"))?;
        let value = TupleId::new(
            PageId::new(RelationId::new(rel), BlockNumber::new(block)),
            slot,
        );
        out.push(LeafEntry { key, value });
    }
    Ok(out)
}

fn write_leaf_entries(page: &mut Page, entries: &[LeafEntry]) {
    let bytes = page.as_bytes_mut();
    for (i, e) in entries.iter().enumerate() {
        let off = PAGE_HEADER_SIZE + i * LEAF_ENTRY_SIZE;
        write_i64_le(&mut bytes[off..off + 8], e.key);
        write_u32_le(&mut bytes[off + 8..off + 12], e.value.page.relation.0.raw());
        write_u32_le(&mut bytes[off + 12..off + 16], e.value.page.block.raw());
        write_u16_le(&mut bytes[off + 16..off + 18], e.value.slot);
        // Pad bytes 18..20 set to zero; readers ignore.
        bytes[off + 18] = 0;
        bytes[off + 19] = 0;
    }
}

fn read_internal_entries(page: &Page, count: u16) -> Result<Vec<InternalEntry>, BTreeError> {
    let bytes = page.as_bytes();
    let mut out = Vec::with_capacity(count as usize);
    for i in 0..(count as usize) {
        let off = PAGE_HEADER_SIZE + i * INTERNAL_ENTRY_SIZE;
        if off + INTERNAL_ENTRY_SIZE > NODE_SPECIAL_OFFSET {
            return Err(BTreeError::MalformedNode("internal entry out of range"));
        }
        let key = read_i64_le(&bytes[off..off + 8])
            .map_err(|_| BTreeError::MalformedNode("internal key"))?;
        let child = read_u32_le(&bytes[off + 8..off + 12])
            .map_err(|_| BTreeError::MalformedNode("child"))?;
        out.push(InternalEntry { key, child });
    }
    Ok(out)
}

fn write_internal_entries(page: &mut Page, entries: &[InternalEntry]) {
    let bytes = page.as_bytes_mut();
    for (i, e) in entries.iter().enumerate() {
        let off = PAGE_HEADER_SIZE + i * INTERNAL_ENTRY_SIZE;
        write_i64_le(&mut bytes[off..off + 8], e.key);
        write_u32_le(&mut bytes[off + 8..off + 12], e.child);
        bytes[off + 12..off + 16].fill(0);
    }
}

// --- helpers ---------------------------------------------------------------

fn init_btree_page(page: &mut Page, meta: NodeMeta) -> Result<(), BTreeError> {
    // Reinitialise the page header so it represents a B-tree page
    // with the special area carved out at the tail.
    let new_header = PageHeader {
        lsn: 0,
        checksum: 0,
        flags: 0,
        kind: PageKind::BTreeIndex,
        lower: PAGE_HEADER_SIZE as u16,
        upper: NODE_SPECIAL_OFFSET as u16,
        special: NODE_SPECIAL_OFFSET as u16,
        version: page.header().version,
    };
    page.write_header(&new_header)?;
    meta.write_into(page);
    Ok(())
}

/// Lehman-Yao right-link chase decision.
///
/// Returns `Some(right_link_block)` if the node has been split since
/// our parent pointed at it and the search key now belongs to a sibling
/// further right.
const fn should_chase_right(meta: NodeMeta, key: i64) -> Option<u32> {
    if !meta.has_high_key() {
        return None;
    }
    if key >= meta.high_key && meta.right_link != NO_SIBLING {
        Some(meta.right_link)
    } else {
        None
    }
}

fn find_child_internal(page: &Page, meta: NodeMeta, key: i64) -> Result<BlockNumber, BTreeError> {
    let entries = read_internal_entries(page, meta.n_keys)?;
    if entries.is_empty() {
        return Err(BTreeError::MalformedNode("empty internal node"));
    }
    // Find the rightmost entry whose key is <= our search key.
    // Entry 0 always has key = i64::MIN by construction.
    let idx = match entries.binary_search_by_key(&key, |e| e.key) {
        Ok(i) => i,
        Err(i) => i.saturating_sub(1),
    };
    Ok(BlockNumber::new(entries[idx].child))
}

// --- range iterator --------------------------------------------------------

/// Forward range iterator returned by [`BTree::range_scan`].
///
/// The iterator holds no buffer-pool guards across `next` calls. Each
/// step re-acquires a read guard on the current leaf, copies its
/// entries, and advances. Concurrent writes to leaves do not invalidate
/// the iterator's position because the right-link chain is followed
/// explicitly.
pub struct RangeIter<'a, L: PageLoader, K: Key> {
    tree: &'a BTree<L>,
    current_leaf: Option<BlockNumber>,
    current_slot: usize,
    start: K,
    end: Option<K>,
    started: bool,
    _key_marker: PhantomData<K>,
}

impl<L: PageLoader, K: Key> std::fmt::Debug for RangeIter<'_, L, K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeIter")
            .field("started", &self.started)
            .field("current_leaf", &self.current_leaf)
            .field("current_slot", &self.current_slot)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader, K: Key> Iterator for RangeIter<'_, L, K> {
    type Item = Result<(K, TupleId), BTreeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if K::SIZE != 8 {
            return Some(Err(BTreeError::KeyTooLarge));
        }
        if !self.started {
            self.started = true;
            let mut start_buf = [0_u8; 8];
            self.start.encode(&mut start_buf);
            let Ok(raw) = read_i64_le(&start_buf) else {
                return Some(Err(BTreeError::MalformedNode("range start")));
            };
            let root = *self.tree.root_block.lock();
            match self.tree.descend_to_leaf_readonly(root, raw) {
                Ok(leaf) => self.current_leaf = Some(leaf),
                Err(e) => return Some(Err(e)),
            }
            if let Some(leaf) = self.current_leaf {
                match self.start_slot_in_leaf(leaf, raw) {
                    Ok(slot) => self.current_slot = slot,
                    Err(e) => return Some(Err(e)),
                }
            }
        }

        loop {
            let leaf = self.current_leaf?;
            let guard = match self.tree.pool.get_page(self.tree.page_id(leaf)) {
                Ok(g) => g,
                Err(e) => return Some(Err(e.into())),
            };
            let meta_right_link;
            let entries;
            {
                let r = guard.read();
                let meta = match NodeMeta::read_from(&r) {
                    Ok(m) => m,
                    Err(e) => return Some(Err(e)),
                };
                entries = match read_leaf_entries(&r, meta.n_keys) {
                    Ok(es) => es,
                    Err(e) => return Some(Err(e)),
                };
                meta_right_link = meta.right_link;
                drop(r);
            }
            drop(guard);

            if self.current_slot < entries.len() {
                let e = entries[self.current_slot];
                self.current_slot += 1;
                if let Some(end_key) = self.end {
                    let mut end_buf = [0_u8; 8];
                    end_key.encode(&mut end_buf);
                    let Ok(raw_end) = read_i64_le(&end_buf) else {
                        return Some(Err(BTreeError::MalformedNode("range end")));
                    };
                    if e.key >= raw_end {
                        self.current_leaf = None;
                        return None;
                    }
                }
                let mut buf = [0_u8; 8];
                write_i64_le(&mut buf, e.key);
                let k = K::decode(&buf);
                return Some(Ok((k, e.value)));
            }

            // Exhausted current leaf; follow the right link.
            if meta_right_link == NO_SIBLING {
                self.current_leaf = None;
                return None;
            }
            self.current_leaf = Some(BlockNumber::new(meta_right_link));
            self.current_slot = 0;
        }
    }
}

impl<L: PageLoader, K: Key> RangeIter<'_, L, K> {
    fn start_slot_in_leaf(&self, leaf: BlockNumber, raw_start: i64) -> Result<usize, BTreeError> {
        let guard = self.tree.pool.get_page(self.tree.page_id(leaf))?;
        let entries;
        {
            let r = guard.read();
            let meta = NodeMeta::read_from(&r)?;
            entries = read_leaf_entries(&r, meta.n_keys)?;
            drop(r);
        }
        drop(guard);
        Ok(match entries.binary_search_by_key(&raw_start, |e| e.key) {
            Ok(i) | Err(i) => i,
        })
    }
}

// --- tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::thread;

    use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};

    use super::*;
    use crate::buffer_pool::{BufferPool, PageLoader};
    use crate::page::Page;

    /// In-memory loader for B-tree tests.
    ///
    /// The B-tree reinitialises every page it allocates, so the loader
    /// only needs to hand back blank heap pages on cache miss. The
    /// counter is purely diagnostic.
    #[derive(Default, Debug)]
    struct MapLoader {
        misses: AtomicU64,
    }

    impl MapLoader {
        const fn new() -> Self {
            Self {
                misses: AtomicU64::new(0),
            }
        }
    }

    impl PageLoader for MapLoader {
        fn load(&self, _page_id: PageId) -> ultrasql_core::Result<Page> {
            self.misses.fetch_add(1, AtomicOrdering::Relaxed);
            // The buffer pool keeps modifications in its own frame
            // memory while pinned/resident; the loader only services
            // misses. For tests, a blank page is fine because the
            // tree reinitialises freshly allocated blocks immediately.
            Ok(Page::new_heap())
        }
    }

    fn make_tree() -> BTree<MapLoader> {
        // Pool sized to comfortably hold the dirty-page set the tests
        // build. The buffer pool currently refuses to evict dirty
        // pages (the storage manager owns flushing), so we pre-size
        // the pool to fit the test workload.
        let pool = Arc::new(BufferPool::new(4096, MapLoader::new()));
        BTree::create(pool, RelationId::new(42)).expect("create btree")
    }

    fn tid(block: u32, slot: u16) -> TupleId {
        TupleId::new(
            PageId::new(RelationId::new(99), BlockNumber::new(block)),
            slot,
        )
    }

    #[test]
    fn empty_tree_lookup_returns_none() {
        let tree = make_tree();
        assert!(tree.lookup::<i64>(0).unwrap().is_none());
        assert!(tree.lookup::<i64>(100).unwrap().is_none());
        assert!(tree.lookup::<i64>(-100).unwrap().is_none());
    }

    #[test]
    fn insert_then_lookup_returns_value() {
        let mut tree = make_tree();
        tree.insert::<i64>(42, tid(1, 2)).unwrap();
        assert_eq!(tree.lookup::<i64>(42).unwrap(), Some(tid(1, 2)));
        assert!(tree.lookup::<i64>(43).unwrap().is_none());
    }

    #[test]
    fn insert_1000_sequential_keys() {
        let mut tree = make_tree();
        for i in 0_i64..1000 {
            let block = u32::try_from(i).expect("fits in u32");
            let slot = u16::try_from(i & 0xFFFF).expect("fits in u16");
            tree.insert::<i64>(i, tid(block, slot)).unwrap();
        }
        for i in 0_i64..1000 {
            let block = u32::try_from(i).expect("fits in u32");
            let slot = u16::try_from(i & 0xFFFF).expect("fits in u16");
            assert_eq!(
                tree.lookup::<i64>(i).unwrap(),
                Some(tid(block, slot)),
                "lookup({i}) failed",
            );
        }
        assert!(tree.lookup::<i64>(1000).unwrap().is_none());
        assert!(tree.lookup::<i64>(-1).unwrap().is_none());
    }

    #[test]
    fn insert_1000_shuffled_keys() {
        let mut tree = make_tree();
        let mut keys: Vec<i64> = (0_i64..1000).collect();
        // Deterministic xorshift permutation.
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        for i in (1..keys.len()).rev() {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            let j = (s as usize) % (i + 1);
            keys.swap(i, j);
        }
        for &k in &keys {
            let block = u32::try_from(k).expect("fits in u32");
            let slot = u16::try_from((k * 7) & 0xFFFF).expect("fits in u16");
            tree.insert::<i64>(k, tid(block, slot)).unwrap();
        }
        for &k in &keys {
            let block = u32::try_from(k).expect("fits in u32");
            let slot = u16::try_from((k * 7) & 0xFFFF).expect("fits in u16");
            assert_eq!(
                tree.lookup::<i64>(k).unwrap(),
                Some(tid(block, slot)),
                "lookup({k}) failed",
            );
        }
    }

    #[test]
    fn range_scan_visits_keys_in_order() {
        let mut tree = make_tree();
        for i in 0_i64..200 {
            let block = u32::try_from(i).expect("fits in u32");
            tree.insert::<i64>(i, tid(block, 0)).unwrap();
        }
        let collected: Vec<(i64, TupleId)> = tree
            .range_scan::<i64>(0, None)
            .map(Result::unwrap)
            .collect();
        assert_eq!(collected.len(), 200);
        for (i, (k, _)) in collected.iter().enumerate() {
            let expected = i64::try_from(i).expect("fits");
            assert_eq!(*k, expected, "out-of-order at slot {i}");
        }
    }

    #[test]
    fn range_scan_with_end_bound_stops_at_right_place() {
        let mut tree = make_tree();
        for i in 0_i64..200 {
            let block = u32::try_from(i).expect("fits in u32");
            tree.insert::<i64>(i, tid(block, 0)).unwrap();
        }
        let collected: Vec<i64> = tree
            .range_scan::<i64>(50, Some(120))
            .map(|r| r.unwrap().0)
            .collect();
        assert_eq!(collected.len(), 70);
        assert_eq!(collected.first(), Some(&50));
        assert_eq!(collected.last(), Some(&119));
    }

    #[test]
    fn duplicate_insert_returns_duplicate_key_error() {
        let mut tree = make_tree();
        tree.insert::<i64>(7, tid(1, 0)).unwrap();
        let err = tree.insert::<i64>(7, tid(2, 0)).unwrap_err();
        assert!(matches!(err, BTreeError::DuplicateKey), "got {err:?}");
        assert_eq!(tree.lookup::<i64>(7).unwrap(), Some(tid(1, 0)));
    }

    #[test]
    fn one_split_keeps_root_lookup_correct() {
        // MAX_LEAF_ENTRIES = 32; inserting 33+ keys forces a split.
        let mut tree = make_tree();
        for i in 0_i64..40 {
            let block = u32::try_from(i).expect("fits in u32");
            tree.insert::<i64>(i, tid(block, 0)).unwrap();
        }
        for i in 0_i64..40 {
            let block = u32::try_from(i).expect("fits in u32");
            assert_eq!(
                tree.lookup::<i64>(i).unwrap(),
                Some(tid(block, 0)),
                "lookup({i}) post-split failed",
            );
        }
    }

    #[test]
    fn two_level_splits_force_inner_node_split() {
        // MAX_INTERNAL_ENTRIES = 16 + MAX_LEAF_ENTRIES = 32: 1000
        // inserts comfortably forces the root to become a level-2
        // internal node.
        let mut tree = make_tree();
        for i in 0_i64..1000 {
            let block = u32::try_from(i).expect("fits in u32");
            tree.insert::<i64>(i, tid(block, 0)).unwrap();
        }
        for i in 0_i64..1000 {
            let block = u32::try_from(i).expect("fits in u32");
            assert_eq!(
                tree.lookup::<i64>(i).unwrap(),
                Some(tid(block, 0)),
                "lookup({i}) failed",
            );
        }
        let n: usize = tree.range_scan::<i64>(0, None).count();
        assert_eq!(n, 1000);
    }

    #[test]
    fn concurrent_readers_all_succeed() {
        let mut tree = make_tree();
        for i in 0_i64..500 {
            let block = u32::try_from(i).expect("fits in u32");
            tree.insert::<i64>(i, tid(block, 7)).unwrap();
        }
        let tree = Arc::new(tree);
        let threads: Vec<_> = (0_i64..8)
            .map(|t| {
                let tree = Arc::clone(&tree);
                thread::spawn(move || {
                    for round in 0_i64..50 {
                        let key = (round * 7 + t).rem_euclid(500);
                        let block = u32::try_from(key).expect("fits in u32");
                        let v = tree.lookup::<i64>(key).unwrap();
                        assert_eq!(v, Some(tid(block, 7)));
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().expect("reader thread");
        }
    }

    #[test]
    fn negative_keys_round_trip_correctly() {
        let mut tree = make_tree();
        for i in -50_i64..50 {
            let block = u32::try_from(i + 100).expect("fits in u32");
            tree.insert::<i64>(i, tid(block, 0)).unwrap();
        }
        for i in -50_i64..50 {
            let block = u32::try_from(i + 100).expect("fits in u32");
            assert_eq!(
                tree.lookup::<i64>(i).unwrap(),
                Some(tid(block, 0)),
                "lookup({i}) failed",
            );
        }
        let keys: Vec<i64> = tree
            .range_scan::<i64>(-10, Some(10))
            .map(|r| r.unwrap().0)
            .collect();
        let expected: Vec<i64> = (-10..10).collect();
        assert_eq!(keys, expected);
    }

    #[test]
    fn range_scan_from_middle_starts_at_correct_key() {
        let mut tree = make_tree();
        for i in 0_i64..100 {
            let block = u32::try_from(i).expect("fits in u32");
            tree.insert::<i64>(i * 2, tid(block, 0)).unwrap();
        }
        // Start at 49 which is *between* keys 48 and 50; expect 50 first.
        let first = tree.range_scan::<i64>(49, None).next().unwrap().unwrap();
        assert_eq!(first.0, 50);
    }

    #[test]
    fn open_recovers_existing_root() {
        let pool = Arc::new(BufferPool::new(4096, MapLoader::new()));
        let mut tree = BTree::create(Arc::clone(&pool), RelationId::new(7)).unwrap();
        for i in 0_i64..50 {
            let block = u32::try_from(i).expect("fits in u32");
            tree.insert::<i64>(i, tid(block, 0)).unwrap();
        }
        let root = tree.root_block();
        drop(tree);
        let tree2 = BTree::open(pool, RelationId::new(7), root);
        for i in 0_i64..50 {
            let block = u32::try_from(i).expect("fits in u32");
            assert_eq!(tree2.lookup::<i64>(i).unwrap(), Some(tid(block, 0)));
        }
    }
}
