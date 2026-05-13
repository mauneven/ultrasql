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
use ultrasql_core::{BlockNumber, Lsn, PageId, RelationId, TupleId, Xid};
use ultrasql_wal::WalRecord;
use ultrasql_wal::payload::{BTreeOpKind, BTreeOpPayload};
use ultrasql_wal::record::RecordType;

use crate::buffer_pool::{BufferPool, BufferPoolError, PageGuard, PageLoader};
use crate::page::{PAGE_HEADER_SIZE, Page, PageError, PageHeader, PageKind};
use crate::wal_sink::{WalSink, WalSinkError};

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

/// Size of the per-node metadata block on disk.
const NODE_META_SIZE: usize = 24;

/// Special-area offset within a B-tree page.
const NODE_SPECIAL_OFFSET: usize = PAGE_SIZE - NODE_META_SIZE;

/// Sentinel meaning "no right sibling."
const NO_SIBLING: u32 = u32::MAX;

/// Bit in the node-meta `flags` field indicating a leaf page.
const FLAG_LEAF: u16 = 1 << 0;

/// Bit in the node-meta `flags` field indicating that `high_key` is set.
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

    /// The WAL sink rejected a record emitted for a B-tree mutation.
    #[error("wal sink: {0}")]
    Wal(#[from] WalSinkError),

    /// Encoding a typed WAL payload failed.
    #[error("wal payload encoding: {0}")]
    WalPayload(#[from] ultrasql_wal::payload::PayloadError),
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
    /// Returns [`BTreeError::DuplicateKey`] if `key` is already present.
    ///
    /// When `wal` is `Some`, a `RecordType::BTreeOp` record is appended for
    /// each page mutation: one `BTreeOpKind::Insert` record for the leaf page
    /// that received the new entry, and one `BTreeOpKind::Split` record for
    /// each page split propagated up the tree. Records are emitted after the
    /// relevant page guards are released, consistent with the heap's WAL
    /// protocol.
    ///
    /// Pass `None` for `wal` during recovery replay (the WAL is the source of
    /// truth) or in tests that do not care about WAL output.
    ///
    /// `xid` identifies the inserting transaction and is embedded in every
    /// emitted record. It does not affect the tree's data.
    pub fn insert<K: Key>(
        &mut self,
        key: K,
        value: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), BTreeError> {
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

        // Emit WAL record for the leaf insert (always, even when a split occurred).
        if let Some(sink) = wal {
            let prev_lsn = sink.last_lsn_for(xid);
            let mut cv = vec![0_u8; 12]; // TupleId wire encoding
            write_u32_le(&mut cv[0..4], value.page.relation.oid().raw());
            write_u32_le(&mut cv[4..8], value.page.block.raw());
            write_u16_le(&mut cv[8..10], value.slot);
            // bytes 10-11: reserved zero
            let payload = BTreeOpPayload {
                op: BTreeOpKind::Insert,
                index_rel: self.rel,
                page: self.page_id(leaf_block),
                key_bytes: key_buf.to_vec(),
                child_or_value: cv,
            }
            .encode()?;
            let record = WalRecord::new(RecordType::BTreeOp, xid, prev_lsn, 0, payload);
            let lsn: Lsn = sink.append(record).expect(
                "wal append must succeed after a committed btree page mutation; failure is unrecoverable",
            );
            // Stamp the leaf page LSN.
            Self::stamp_page_lsn(&self.pool, self.page_id(leaf_block), lsn)?;
        }

        if let Some((sep_key, new_right)) = split_result {
            // Emit WAL record for the split before propagating up.
            if let Some(sink) = wal {
                let prev_lsn = sink.last_lsn_for(xid);
                let mut sep_buf = [0_u8; 8];
                write_i64_le(&mut sep_buf, sep_key);
                let cv = new_right.raw().to_le_bytes().to_vec();
                let payload = BTreeOpPayload {
                    op: BTreeOpKind::Split,
                    index_rel: self.rel,
                    page: self.page_id(leaf_block),
                    key_bytes: sep_buf.to_vec(),
                    child_or_value: cv,
                }
                .encode()?;
                let record = WalRecord::new(RecordType::BTreeOp, xid, prev_lsn, 0, payload);
                sink.append(record).expect(
                    "wal append must succeed after a committed btree split; failure is unrecoverable",
                );
            }
            self.propagate_split(path, sep_key, new_right)?;
        }
        Ok(())
    }

    /// Stamp `page_id`'s LSN field with `lsn`.
    ///
    /// Called after a successful WAL append so the page's LSN is never
    /// ahead of the WAL.
    fn stamp_page_lsn(
        pool: &Arc<BufferPool<L>>,
        page_id: PageId,
        lsn: Lsn,
    ) -> Result<(), BTreeError> {
        let guard = pool.get_page(page_id)?;
        guard.write().set_lsn(lsn.raw());
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

// ---------------------------------------------------------------------------
// Backward range iterator (reverse scan)
// ---------------------------------------------------------------------------

/// Backward range iterator — yields `(key, TupleId)` pairs in
/// *descending* key order from `start` (inclusive) down to `end`
/// (exclusive if provided, unbounded otherwise).
///
/// The implementation collects all leaf entries in the range with a
/// forward scan and then reverses the result. A production
/// implementation would walk the sibling chain right-to-left using a
/// doubly-linked leaf list; that optimization is
/// `TODO(btree-backward-efficient)`.
pub struct BackwardRangeIter<'a, L: PageLoader, K: Key> {
    /// Items collected from the forward scan, in ascending order.
    items: Vec<(K, TupleId)>,
    /// Current position (counts down).
    pos: usize,
    _tree: std::marker::PhantomData<&'a BTree<L>>,
}

impl<L: PageLoader, K: Key> std::fmt::Debug for BackwardRangeIter<'_, L, K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackwardRangeIter")
            .field("remaining", &self.pos)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader, K: Key> Iterator for BackwardRangeIter<'_, L, K> {
    type Item = Result<(K, TupleId), BTreeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == 0 {
            return None;
        }
        self.pos -= 1;
        Some(Ok(self.items[self.pos]))
    }
}

impl<L: PageLoader> BTree<L> {
    /// Backward (descending) range scan.
    ///
    /// Yields `(key, TupleId)` pairs in descending key order starting
    /// from `start` (inclusive) down to `end` (exclusive if `Some`,
    /// unbounded if `None`).
    ///
    /// The iterator collects the forward range into a `Vec` and
    /// reverses it.  `TODO(btree-backward-efficient)`: walk the leaf
    /// chain right-to-left once the leaf list is doubly-linked.
    pub fn backward_scan<K: Key>(
        &self,
        start: K,
        end: Option<K>,
    ) -> Result<BackwardRangeIter<'_, L, K>, BTreeError> {
        // Collect forward scan.
        let items: Vec<(K, TupleId)> = self
            .range_scan::<K>(
                end.unwrap_or(start),
                if end.is_some() { None } else { None },
            )
            .filter_map(std::result::Result::ok)
            .filter(|(k, _)| end.is_none_or(|e| *k >= e) && *k <= start)
            .collect();
        // range_scan walks ascending; reverse in memory.
        let mut items = items;
        items.reverse();
        let pos = items.len();
        Ok(BackwardRangeIter {
            items,
            pos,
            _tree: std::marker::PhantomData,
        })
    }
}

// ---------------------------------------------------------------------------
// Multi-column key support
// ---------------------------------------------------------------------------

/// A composite key made of multiple fixed-width components.
///
/// Each component is an `i64` value. The composite key encodes all
/// components concatenated in little-endian order, making the encoding
/// length `N * 8` bytes.
///
/// v0.8 restricts component count to 1–8 (yielding 8–64 bytes). The
/// existing `BTree` `Key` trait requires `SIZE == 8`; composite keys
/// bypass that restriction by using the byte-slice `AccessMethod`
/// interface instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CompositeKey<const N: usize> {
    /// Component values in declaration order.
    pub values: [i64; N],
}

impl<const N: usize> CompositeKey<N> {
    /// Construct a composite key from an array of `i64` values.
    #[must_use]
    pub const fn new(values: [i64; N]) -> Self {
        Self { values }
    }

    /// Encode the composite key into a byte buffer.
    ///
    /// The buffer must be exactly `N * 8` bytes long.
    pub fn encode_into(&self, out: &mut [u8]) {
        assert_eq!(out.len(), N * 8, "buffer length must equal N*8");
        for (i, &v) in self.values.iter().enumerate() {
            write_i64_le(&mut out[i * 8..i * 8 + 8], v);
        }
    }

    /// Decode a composite key from a byte buffer.
    pub fn decode_from(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), N * 8, "buffer length must equal N*8");
        let mut values = [0_i64; N];
        for (i, v) in values.iter_mut().enumerate() {
            *v = read_i64_le(&bytes[i * 8..i * 8 + 8]).unwrap_or(0);
        }
        Self { values }
    }
}

// ---------------------------------------------------------------------------
// Expression index helper
// ---------------------------------------------------------------------------

/// An expression index stores keys computed by a caller-supplied
/// function rather than direct column values.
///
/// The `ExprIndexAdapter` wraps a `BTree` (via the `AccessMethod`
/// interface) and a key-extraction function. The caller inserts rows;
/// the adapter extracts the key, encodes it, and forwards to the
/// underlying index.
///
/// # Usage
///
/// ```ignore
/// let idx = ExprIndexAdapter::new(
///     BTreeAccessMethod::new(true),
///     |row| {
///         // Expression: lower(email)
///         if let Some(Value::Text(s)) = row.get(2) {
///             s.to_lowercase().into_bytes()
///         } else {
///             vec![]
///         }
///     },
/// );
/// idx.insert_row(&row, tid).unwrap();
/// ```
pub struct ExprIndexAdapter {
    inner: Box<dyn crate::access_method::AccessMethod>,
    key_fn: Box<dyn Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync>,
}

impl std::fmt::Debug for ExprIndexAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExprIndexAdapter").finish_non_exhaustive()
    }
}

impl ExprIndexAdapter {
    /// Construct an expression index adapter.
    ///
    /// - `inner` — underlying access method (typically `BTreeAccessMethod`).
    /// - `key_fn` — maps a row to the index key bytes.
    pub fn new(
        inner: impl crate::access_method::AccessMethod + 'static,
        key_fn: impl Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            key_fn: Box::new(key_fn),
        }
    }

    /// Insert a row into the expression index.
    pub fn insert_row(
        &self,
        row: &[ultrasql_core::Value],
        tid: TupleId,
    ) -> Result<(), crate::access_method::AccessMethodError> {
        let key = (self.key_fn)(row);
        self.inner.insert(&key, tid)
    }

    /// Look up a pre-encoded expression key.
    pub fn lookup_key(
        &self,
        key: &[u8],
    ) -> Result<Vec<TupleId>, crate::access_method::AccessMethodError> {
        self.inner.lookup(key)
    }
}

// ---------------------------------------------------------------------------
// Partial index predicate wrapper
// ---------------------------------------------------------------------------

/// A partial index only indexes rows satisfying a predicate.
///
/// The `PartialIndexAdapter` wraps any `AccessMethod` and filters
/// inserts through a WHERE-clause predicate. Rows that do not satisfy
/// the predicate are silently skipped.
pub struct PartialIndexAdapter {
    inner: Box<dyn crate::access_method::AccessMethod>,
    predicate: Box<dyn Fn(&[ultrasql_core::Value]) -> bool + Send + Sync>,
    key_fn: Box<dyn Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync>,
}

impl std::fmt::Debug for PartialIndexAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartialIndexAdapter")
            .finish_non_exhaustive()
    }
}

impl PartialIndexAdapter {
    /// Construct a partial index adapter.
    ///
    /// - `inner` — underlying access method.
    /// - `key_fn` — extracts the key bytes from a row.
    /// - `predicate` — returns `true` when a row should be indexed.
    pub fn new(
        inner: impl crate::access_method::AccessMethod + 'static,
        key_fn: impl Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync + 'static,
        predicate: impl Fn(&[ultrasql_core::Value]) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            key_fn: Box::new(key_fn),
            predicate: Box::new(predicate),
        }
    }

    /// Insert a row if the predicate passes.
    ///
    /// Returns `Ok(())` silently when the predicate is false (the row
    /// is not indexed).
    pub fn insert_row(
        &self,
        row: &[ultrasql_core::Value],
        tid: TupleId,
    ) -> Result<(), crate::access_method::AccessMethodError> {
        if !(self.predicate)(row) {
            return Ok(()); // Row does not satisfy the partial predicate.
        }
        let key = (self.key_fn)(row);
        self.inner.insert(&key, tid)
    }

    /// Look up a pre-encoded key.
    pub fn lookup_key(
        &self,
        key: &[u8],
    ) -> Result<Vec<TupleId>, crate::access_method::AccessMethodError> {
        self.inner.lookup(key)
    }
}

// ---------------------------------------------------------------------------
// Covering index (INCLUDE columns) wrapper
// ---------------------------------------------------------------------------

/// Leaf payload for a covering index entry.
///
/// In a covering index the leaf stores the primary key columns plus
/// additional INCLUDE columns. This struct holds the INCLUDE payload as
/// raw bytes alongside the `TupleId`; the executor can satisfy a query
/// without visiting the heap.
///
/// TODO(btree-covering-persistent): store the INCLUDE payload on the
/// leaf page in the buffer pool rather than in memory.
#[derive(Clone, Debug)]
pub struct CoveringEntry {
    /// Tuple identifier (used as a fallback when INCLUDE columns
    /// do not satisfy the query).
    pub tid: TupleId,
    /// Additional INCLUDE column bytes, serialized by the caller.
    pub include_payload: Vec<u8>,
}

/// A covering index that stores INCLUDE column payloads alongside
/// the indexed key.
///
/// Keys are managed by the inner `AccessMethod`; INCLUDE payloads are
/// stored in a side-table indexed by `TupleId`.
pub struct CoveringIndexAdapter {
    inner: Box<dyn crate::access_method::AccessMethod>,
    key_fn: Box<dyn Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync>,
    include_fn: Box<dyn Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync>,
    /// INCLUDE payloads keyed by `TupleId`.
    payloads: parking_lot::Mutex<std::collections::HashMap<u64, Vec<u8>>>,
}

impl std::fmt::Debug for CoveringIndexAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoveringIndexAdapter")
            .finish_non_exhaustive()
    }
}

/// Encode a `TupleId` to a `u64` for use as a hash map key.
fn tid_to_u64(tid: TupleId) -> u64 {
    let rel_oid = u64::from(tid.page.relation.0.raw());
    let block = u64::from(tid.page.block.raw());
    let slot = u64::from(tid.slot);
    (rel_oid << 48) | (block << 16) | slot
}

impl CoveringIndexAdapter {
    /// Construct a covering index adapter.
    ///
    /// - `key_fn` — produces the key bytes from a row.
    /// - `include_fn` — produces the INCLUDE column payload bytes.
    pub fn new(
        inner: impl crate::access_method::AccessMethod + 'static,
        key_fn: impl Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync + 'static,
        include_fn: impl Fn(&[ultrasql_core::Value]) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            key_fn: Box::new(key_fn),
            include_fn: Box::new(include_fn),
            payloads: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Insert a row, storing the INCLUDE payload alongside the key.
    pub fn insert_row(
        &self,
        row: &[ultrasql_core::Value],
        tid: TupleId,
    ) -> Result<(), crate::access_method::AccessMethodError> {
        let key = (self.key_fn)(row);
        let payload = (self.include_fn)(row);
        self.inner.insert(&key, tid)?;
        self.payloads.lock().insert(tid_to_u64(tid), payload);
        Ok(())
    }

    /// Look up key + INCLUDE payloads for all matching entries.
    pub fn lookup_covering(
        &self,
        key: &[u8],
    ) -> Result<Vec<CoveringEntry>, crate::access_method::AccessMethodError> {
        let tids = self.inner.lookup(key)?;
        let payloads = self.payloads.lock();
        Ok(tids
            .into_iter()
            .map(|tid| {
                let include_payload = payloads.get(&tid_to_u64(tid)).cloned().unwrap_or_default();
                CoveringEntry {
                    tid,
                    include_payload,
                }
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// CREATE INDEX CONCURRENTLY simulation (2-pass build)
// ---------------------------------------------------------------------------

/// Status of a concurrent index build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConcurrentBuildStatus {
    /// First pass complete; the index covers rows inserted before `snapshot_xid`.
    Pass1Complete {
        /// XID at which the first pass's snapshot was taken.
        snapshot_xid: u64,
    },
    /// Both passes complete; the index is ready for use.
    Ready,
}

/// Simulated CREATE INDEX CONCURRENTLY state machine.
///
/// A concurrent build proceeds in two phases without taking an
/// `AccessExclusive` lock on the table:
///
/// 1. **Pass 1** — build the initial index from a snapshot of the table
///    taken at the caller-supplied `snapshot_xid`. Rows inserted after
///    that XID are not yet indexed.
/// 2. **Pass 2** — index rows that were inserted between the pass-1
///    snapshot and the current time. After pass 2 the index is valid.
///
/// This implementation delegates to the caller-supplied row iterators
/// rather than reading from the buffer pool, keeping the storage crate
/// decoupled from the executor. The actual page I/O (and WAL logging)
/// occurs inside the `AccessMethod::insert` calls.
///
/// `TODO(cic-complete)`: integrate with the MVCC visibility layer and
/// the lock manager to replay missed rows correctly.
#[derive(Debug)]
pub struct ConcurrentIndexBuilder {
    am: Box<dyn crate::access_method::AccessMethod>,
    status: parking_lot::Mutex<Option<ConcurrentBuildStatus>>,
}

impl ConcurrentIndexBuilder {
    /// Create a builder wrapping an already-allocated (empty) index.
    pub fn new(am: impl crate::access_method::AccessMethod + 'static) -> Self {
        Self {
            am: Box::new(am),
            status: parking_lot::Mutex::new(None),
        }
    }

    /// Execute pass 1: index every `(key, tid)` pair supplied by the
    /// iterator.
    ///
    /// `snapshot_xid` is the XID at which the pass-1 heap scan was
    /// taken; rows inserted later are deferred to pass 2.
    pub fn build_pass1(
        &self,
        rows: impl Iterator<Item = (Vec<u8>, TupleId)>,
        snapshot_xid: u64,
    ) -> Result<(), crate::access_method::AccessMethodError> {
        for (key, tid) in rows {
            self.am.insert(&key, tid)?;
        }
        *self.status.lock() = Some(ConcurrentBuildStatus::Pass1Complete { snapshot_xid });
        Ok(())
    }

    /// Execute pass 2: index rows that arrived after the pass-1 snapshot.
    ///
    /// The caller supplies only the delta rows (those with XID >
    /// `snapshot_xid`). After this call the builder reports `Ready`.
    pub fn build_pass2(
        &self,
        delta_rows: impl Iterator<Item = (Vec<u8>, TupleId)>,
    ) -> Result<(), crate::access_method::AccessMethodError> {
        for (key, tid) in delta_rows {
            // Ignore duplicate-key errors: a row may have been indexed
            // during pass 1 if the snapshot window overlapped.
            match self.am.insert(&key, tid) {
                Ok(()) | Err(crate::access_method::AccessMethodError::DuplicateKey) => {}
                Err(e) => return Err(e),
            }
        }
        *self.status.lock() = Some(ConcurrentBuildStatus::Ready);
        Ok(())
    }

    /// Return the current build status.
    pub fn status(&self) -> Option<ConcurrentBuildStatus> {
        self.status.lock().clone()
    }

    /// Consume the builder and return the finished access method.
    ///
    /// Panics if the build is not in the `Ready` state.
    pub fn finish(self) -> Box<dyn crate::access_method::AccessMethod> {
        assert_eq!(
            *self.status.lock(),
            Some(ConcurrentBuildStatus::Ready),
            "build_pass2 must complete before finish()"
        );
        self.am
    }
}

// ---------------------------------------------------------------------------
// VACUUM: reclaim dead index entries
// ---------------------------------------------------------------------------

impl<L: PageLoader> BTree<L> {
    /// Vacuum pass: remove dead leaf entries whose `TupleId`s are
    /// flagged by the caller-supplied `is_dead` predicate.
    ///
    /// The predicate receives a `TupleId` and returns `true` when the
    /// heap tuple is dead (invisible to all current snapshots). The
    /// B-tree iterates all leaves, collecting dead entries, and removes
    /// them.
    ///
    /// Returns the number of dead entries reclaimed.
    ///
    /// # Concurrency
    ///
    /// v0.8 requires exclusive access during vacuum (no concurrent
    /// writers). The caller must hold an appropriate relation lock.
    /// `TODO(btree-vacuum-concurrent)`: allow concurrent reads during
    /// vacuum using a second-pass cleanup protocol.
    pub fn vacuum(&self, is_dead: impl Fn(TupleId) -> bool) -> Result<usize, BTreeError> {
        let root = *self.root_block.lock();
        // Find the leftmost leaf.
        let mut leaf = self.leftmost_leaf(root)?;
        let mut removed = 0_usize;

        loop {
            let guard = self.pool.get_page(self.page_id(leaf))?;
            let right_link;
            {
                let mut w = guard.write();
                let meta = NodeMeta::read_from(&w)?;
                debug_assert!(meta.is_leaf());
                let mut entries = read_leaf_entries(&w, meta.n_keys)?;
                let before = entries.len();
                entries.retain(|e| !is_dead(e.value));
                let after = entries.len();
                if after < before {
                    write_leaf_entries(&mut w, &entries);
                    let new_meta = NodeMeta {
                        n_keys: u16::try_from(after)
                            .map_err(|_| BTreeError::MalformedNode("vacuum overflow"))?,
                        ..meta
                    };
                    new_meta.write_into(&mut w);
                    removed += before - after;
                }
                right_link = meta.right_link;
                drop(w);
            }
            drop(guard);
            if right_link == NO_SIBLING {
                break;
            }
            leaf = BlockNumber::new(right_link);
        }
        Ok(removed)
    }

    /// Find the leftmost leaf by descending from `root` always left.
    fn leftmost_leaf(&self, root: BlockNumber) -> Result<BlockNumber, BTreeError> {
        let mut current = root;
        loop {
            let guard = self.pool.get_page(self.page_id(current))?;
            let (is_leaf, first_child);
            {
                let r = guard.read();
                let meta = NodeMeta::read_from(&r)?;
                is_leaf = meta.is_leaf();
                first_child = if is_leaf {
                    None
                } else {
                    let entries = read_internal_entries(&r, meta.n_keys)?;
                    entries.first().map(|e| e.child)
                };
                drop(r);
            }
            drop(guard);
            if is_leaf {
                return Ok(current);
            }
            current = BlockNumber::new(
                first_child.ok_or(BTreeError::MalformedNode("empty internal node"))?,
            );
        }
    }
}

// --- tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::thread;

    use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId, Xid};

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
        tree.insert::<i64>(42, tid(1, 2), Xid::new(1), None)
            .unwrap();
        assert_eq!(tree.lookup::<i64>(42).unwrap(), Some(tid(1, 2)));
        assert!(tree.lookup::<i64>(43).unwrap().is_none());
    }

    #[test]
    fn insert_1000_sequential_keys() {
        let mut tree = make_tree();
        for i in 0_i64..1000 {
            let block = u32::try_from(i).expect("fits in u32");
            let slot = u16::try_from(i & 0xFFFF).expect("fits in u16");
            tree.insert::<i64>(i, tid(block, slot), Xid::new(1), None)
                .unwrap();
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
            tree.insert::<i64>(k, tid(block, slot), Xid::new(1), None)
                .unwrap();
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
            tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
                .unwrap();
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
            tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
                .unwrap();
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
        tree.insert::<i64>(7, tid(1, 0), Xid::new(1), None).unwrap();
        let err = tree
            .insert::<i64>(7, tid(2, 0), Xid::new(1), None)
            .unwrap_err();
        assert!(matches!(err, BTreeError::DuplicateKey), "got {err:?}");
        assert_eq!(tree.lookup::<i64>(7).unwrap(), Some(tid(1, 0)));
    }

    #[test]
    fn one_split_keeps_root_lookup_correct() {
        // MAX_LEAF_ENTRIES = 32; inserting 33+ keys forces a split.
        let mut tree = make_tree();
        for i in 0_i64..40 {
            let block = u32::try_from(i).expect("fits in u32");
            tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
                .unwrap();
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
            tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
                .unwrap();
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
            tree.insert::<i64>(i, tid(block, 7), Xid::new(1), None)
                .unwrap();
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
            tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
                .unwrap();
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
            tree.insert::<i64>(i * 2, tid(block, 0), Xid::new(1), None)
                .unwrap();
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
            tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
                .unwrap();
        }
        let root = tree.root_block();
        drop(tree);
        let tree2 = BTree::open(pool, RelationId::new(7), root);
        for i in 0_i64..50 {
            let block = u32::try_from(i).expect("fits in u32");
            assert_eq!(tree2.lookup::<i64>(i).unwrap(), Some(tid(block, 0)));
        }
    }

    // --- v0.8 additions ---

    #[test]
    fn backward_scan_returns_keys_in_descending_order() {
        let mut tree = make_tree();
        for i in 0_i64..50 {
            let block = u32::try_from(i).expect("fits");
            tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
                .unwrap();
        }
        // Backward scan from 49 with no lower bound.
        let iter = tree.backward_scan::<i64>(49_i64, None).unwrap();
        let keys: Vec<i64> = iter.map(|r| r.unwrap().0).collect();
        assert!(!keys.is_empty());
        // Verify descending order.
        for w in keys.windows(2) {
            assert!(w[0] >= w[1], "not descending: {} < {}", w[0], w[1]);
        }
    }

    #[test]
    fn backward_scan_with_bounds_respects_range() {
        let mut tree = make_tree();
        for i in 0_i64..20 {
            let block = u32::try_from(i).expect("fits");
            tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
                .unwrap();
        }
        // Keys [5, 15] descending.
        let iter = tree.backward_scan::<i64>(15_i64, Some(5_i64)).unwrap();
        let keys: Vec<i64> = iter.map(|r| r.unwrap().0).collect();
        // Should contain keys in [5..=15].
        for &k in &keys {
            assert!(k >= 5 && k <= 15, "key {k} out of [5,15]");
        }
    }

    #[test]
    fn composite_key_encode_decode_round_trip() {
        let k: CompositeKey<3> = CompositeKey::new([1, -7, 999]);
        let mut buf = [0_u8; 24];
        k.encode_into(&mut buf);
        let decoded = CompositeKey::<3>::decode_from(&buf);
        assert_eq!(k, decoded);
    }

    #[test]
    fn composite_key_ordering_is_lexicographic() {
        let a: CompositeKey<2> = CompositeKey::new([1, 5]);
        let b: CompositeKey<2> = CompositeKey::new([1, 6]);
        let c: CompositeKey<2> = CompositeKey::new([2, 0]);
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn expression_index_insert_and_lookup() {
        use crate::access_method::BTreeAccessMethod;
        use ultrasql_core::Value;

        let am = BTreeAccessMethod::new(false);
        let idx = ExprIndexAdapter::new(
            am,
            // Key: first Value as 8-byte LE i64.
            |row| {
                if let Some(Value::Int64(v)) = row.first() {
                    let mut buf = [0_u8; 8];
                    write_i64_le(&mut buf, *v);
                    buf.to_vec()
                } else {
                    vec![]
                }
            },
        );
        let row = vec![Value::Int64(42)];
        idx.insert_row(&row, tid(1, 0)).unwrap();
        let mut key_buf = [0_u8; 8];
        write_i64_le(&mut key_buf, 42);
        let results = idx.lookup_key(&key_buf).unwrap();
        assert!(results.contains(&tid(1, 0)));
    }

    #[test]
    fn partial_index_skips_rows_not_matching_predicate() {
        use crate::access_method::BTreeAccessMethod;
        use ultrasql_core::Value;

        let am = BTreeAccessMethod::new(false);
        // Only index rows where col0 > 10.
        let idx = PartialIndexAdapter::new(
            am,
            |row| {
                if let Some(Value::Int64(v)) = row.first() {
                    let mut buf = [0_u8; 8];
                    write_i64_le(&mut buf, *v);
                    buf.to_vec()
                } else {
                    vec![]
                }
            },
            |row| matches!(row.first(), Some(Value::Int64(v)) if *v > 10),
        );
        // Row with 5 — should NOT be indexed.
        idx.insert_row(&[Value::Int64(5)], tid(1, 0)).unwrap();
        // Row with 20 — should be indexed.
        idx.insert_row(&[Value::Int64(20)], tid(2, 0)).unwrap();

        let mut key_buf = [0_u8; 8];
        write_i64_le(&mut key_buf, 5);
        assert!(
            idx.lookup_key(&key_buf).unwrap().is_empty(),
            "5 should not be indexed"
        );

        write_i64_le(&mut key_buf, 20);
        assert!(
            !idx.lookup_key(&key_buf).unwrap().is_empty(),
            "20 should be indexed"
        );
    }

    #[test]
    fn covering_index_stores_include_payload() {
        use crate::access_method::BTreeAccessMethod;
        use ultrasql_core::Value;

        let am = BTreeAccessMethod::new(true);
        let idx = CoveringIndexAdapter::new(
            am,
            // Key = col0.
            |row| {
                if let Some(Value::Int64(v)) = row.first() {
                    let mut buf = [0_u8; 8];
                    write_i64_le(&mut buf, *v);
                    buf.to_vec()
                } else {
                    vec![]
                }
            },
            // INCLUDE = col1 as 8 bytes.
            |row| {
                if let Some(Value::Int64(v)) = row.get(1) {
                    let mut buf = [0_u8; 8];
                    write_i64_le(&mut buf, *v);
                    buf.to_vec()
                } else {
                    vec![]
                }
            },
        );
        let row = vec![Value::Int64(7), Value::Int64(999)];
        idx.insert_row(&row, tid(1, 0)).unwrap();
        let mut key_buf = [0_u8; 8];
        write_i64_le(&mut key_buf, 7);
        let entries = idx.lookup_covering(&key_buf).unwrap();
        assert_eq!(entries.len(), 1);
        let expected_payload = {
            let mut buf = [0_u8; 8];
            write_i64_le(&mut buf, 999);
            buf.to_vec()
        };
        assert_eq!(entries[0].include_payload, expected_payload);
    }

    #[test]
    fn concurrent_index_build_two_pass() {
        use crate::access_method::BTreeAccessMethod;

        let am = BTreeAccessMethod::new(false);
        let builder = ConcurrentIndexBuilder::new(am);
        assert!(builder.status().is_none());

        // Pass 1: snapshot at xid 100.
        let pass1_rows = (0_i64..5).map(|i| {
            let mut buf = [0_u8; 8];
            write_i64_le(&mut buf, i);
            (buf.to_vec(), tid(u32::try_from(i).unwrap(), 0))
        });
        builder.build_pass1(pass1_rows, 100).unwrap();
        assert_eq!(
            builder.status(),
            Some(ConcurrentBuildStatus::Pass1Complete { snapshot_xid: 100 })
        );

        // Pass 2: delta rows [5..10).
        let pass2_rows = (5_i64..10).map(|i| {
            let mut buf = [0_u8; 8];
            write_i64_le(&mut buf, i);
            (buf.to_vec(), tid(u32::try_from(i).unwrap(), 0))
        });
        builder.build_pass2(pass2_rows).unwrap();
        assert_eq!(builder.status(), Some(ConcurrentBuildStatus::Ready));

        let finished = builder.finish();
        for i in 0_i64..10 {
            let mut buf = [0_u8; 8];
            write_i64_le(&mut buf, i);
            let results = finished.lookup(&buf).unwrap();
            assert!(!results.is_empty(), "key {i} missing after CIC build");
        }
    }

    #[test]
    fn vacuum_removes_dead_entries() {
        let mut tree = make_tree();
        for i in 0_i64..20 {
            let block = u32::try_from(i).expect("fits");
            tree.insert::<i64>(i, tid(block, 0), Xid::new(1), None)
                .unwrap();
        }
        // Mark even-keyed TIDs as dead.
        let removed = tree
            .vacuum(|t| t.page.block.raw() % 2 == 0)
            .expect("vacuum");
        assert_eq!(removed, 10, "expected 10 dead entries removed");

        // Odd keys should still be present.
        for i in (1_i64..20).step_by(2) {
            let block = u32::try_from(i).expect("fits");
            assert_eq!(
                tree.lookup::<i64>(i).unwrap(),
                Some(tid(block, 0)),
                "odd key {i} missing"
            );
        }
        // Even keys should now be missing.
        for i in (0_i64..20).step_by(2) {
            assert!(
                tree.lookup::<i64>(i).unwrap().is_none(),
                "dead key {i} still present"
            );
        }
    }
}
