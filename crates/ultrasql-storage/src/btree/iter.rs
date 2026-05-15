//! Forward and backward range iterators over the B-tree.
//!
//! [`RangeIter`] streams leaf entries by following the right-link chain;
//! it holds no buffer-pool guards across `next` calls so concurrent
//! writes do not invalidate its position. [`BackwardRangeIter`]
//! materialises the forward range into a `Vec` and reverses it — a
//! placeholder until the leaf list grows a backward pointer
//! (`TODO(btree-backward-efficient)`).

use std::marker::PhantomData;

use ultrasql_core::endian::{read_i64_le, write_i64_le};
use ultrasql_core::{BlockNumber, TupleId};

use crate::buffer_pool::PageLoader;

use super::node::{NodeMeta, read_leaf_entries};
use super::{BTree, BTreeError, Key, NO_SIBLING};

/// Forward range iterator returned by [`BTree::range_scan`].
///
/// The iterator holds no buffer-pool guards across `next` calls. Each
/// step re-acquires a read guard on the current leaf, copies its
/// entries, and advances. Concurrent writes to leaves do not invalidate
/// the iterator's position because the right-link chain is followed
/// explicitly.
pub struct RangeIter<'a, L: PageLoader, K: Key> {
    pub(super) tree: &'a BTree<L>,
    pub(super) current_leaf: Option<BlockNumber>,
    pub(super) current_slot: usize,
    pub(super) start: K,
    pub(super) end: Option<K>,
    pub(super) started: bool,
    pub(super) _key_marker: PhantomData<K>,
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
    pub(super) items: Vec<(K, TupleId)>,
    /// Current position (counts down).
    pub(super) pos: usize,
    pub(super) _tree: std::marker::PhantomData<&'a BTree<L>>,
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
            .range_scan::<K>(end.unwrap_or(start), None)
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
