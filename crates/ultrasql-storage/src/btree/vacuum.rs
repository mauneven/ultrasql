//! `VACUUM` cleanup pass: walk the leaf chain left-to-right, drop
//! entries whose `TupleId`s satisfy a caller-supplied `is_dead`
//! predicate, and report the number removed.

use ultrasql_core::{BlockNumber, TupleId};

use crate::buffer_pool::PageLoader;

use super::node::{NodeMeta, read_internal_entries, read_leaf_entries, write_leaf_entries};
use super::{BTree, BTreeError, NO_SIBLING};

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
