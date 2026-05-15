//! Point-lookup path and descent helpers.
//!
//! [`BTree::lookup`] is the public entry; [`BTree::descend_to_leaf`] is
//! shared with the insert path and walks down a parent chain while
//! pushing visited nodes onto a fold-back stack so leaf splits can be
//! propagated up. [`BTree::descend_to_leaf_readonly`] skips that
//! bookkeeping for pure reads.

use ultrasql_core::endian::read_i64_le;
use ultrasql_core::{BlockNumber, TupleId};

use crate::buffer_pool::PageLoader;

use super::node::{DescendStep, LeafProbe, probe_leaf, step_descend};
use super::{BTree, BTreeError, Key};

impl<L: PageLoader> BTree<L> {
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

    pub(super) fn descend_to_leaf(
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

    pub(super) fn descend_to_leaf_readonly(
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

    fn lookup_in_leaf(
        &self,
        leaf: BlockNumber,
        key: i64,
    ) -> Result<Option<TupleId>, BTreeError> {
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
}
