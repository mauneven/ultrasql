//! Point-lookup path and descent helpers.
//!
//! [`BTree::lookup`] is the public entry; [`BTree::descend_to_leaf`] is
//! shared with the insert path and walks down a parent chain while
//! pushing visited nodes onto a fold-back stack so leaf splits can be
//! propagated up. [`BTree::descend_to_leaf_readonly`] skips that
//! bookkeeping for pure reads.

use std::sync::Arc;

use ultrasql_core::endian::{read_i64_le, write_u16_le, write_u32_le};
use ultrasql_core::{BlockNumber, Lsn, TupleId, Xid};
use ultrasql_wal::WalRecord;
use ultrasql_wal::payload::{BTreeOpKind, BTreeOpPayload};
use ultrasql_wal::record::RecordType;

use crate::buffer_pool::PageLoader;
use crate::wal_sink::WalSink;

use super::node::{DescendStep, LeafProbe, NodeMeta, probe_leaf, read_leaf_entries, step_descend};
use super::{BTree, BTreeError, Key};

impl<L: PageLoader> BTree<L> {
    /// Point lookup. Returns `None` if the key is absent.
    pub fn lookup<K: Key>(&self, key: K) -> Result<Option<TupleId>, BTreeError> {
        let op_latch = Arc::clone(&self.op_latch);
        let _op_guard = op_latch.read();
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

    /// Return every [`TupleId`] matching `key`.
    ///
    /// Non-unique indexes store duplicate keys as adjacent leaf entries, but
    /// duplicate groups may cross leaf splits because internal separators only
    /// carry the logical key. This descends to the leftmost leaf that can
    /// contain `key`, then follows right-links only while the equal-key run
    /// can continue (`key >= high_key`), so a point probe costs
    /// O(depth + run-leaves) page reads instead of scanning the whole leaf
    /// chain.
    pub fn lookup_all<K: Key>(&self, key: K) -> Result<Vec<TupleId>, BTreeError> {
        let op_latch = Arc::clone(&self.op_latch);
        let _op_guard = op_latch.read();
        if K::SIZE != 8 {
            return Err(BTreeError::KeyTooLarge);
        }
        let mut buf = [0_u8; 8];
        key.encode(&mut buf);
        let raw_key = read_i64_le(&buf).map_err(|_| BTreeError::MalformedNode("key encode"))?;

        // Descend to the leftmost leaf that can hold `raw_key` (a duplicate
        // run can start on the left side of a same-key split), then walk
        // right-links only while the run can continue (Lehman-Yao chase:
        // `key >= high_key`). O(depth + run-leaves) page reads — a point
        // lookup touches one root-to-leaf path, never the whole leaf chain.
        let root = *self.root_block.lock();
        let mut current = self.descend_to_leftmost_leaf_for_key(root, raw_key)?;
        let mut out = Vec::new();
        loop {
            let guard = self.pool.get_page_relieved(self.page_id(current))?;
            let (entries, chase) = {
                let r = guard.read();
                let meta = NodeMeta::read_from(&r)?;
                (
                    read_leaf_entries(&r, meta.n_keys)?,
                    super::node::should_chase_right(meta, raw_key),
                )
            };
            drop(guard);
            for entry in entries {
                if entry.key == raw_key {
                    out.push(entry.value);
                }
            }
            match chase {
                Some(next) => current = BlockNumber::new(next),
                None => break,
            }
        }
        Ok(out)
    }

    pub(super) fn descend_to_leaf(
        &self,
        root: BlockNumber,
        key: i64,
        path: &mut Vec<BlockNumber>,
    ) -> Result<BlockNumber, BTreeError> {
        let mut current = root;
        loop {
            let guard = self.pool.get_page_relieved(self.page_id(current))?;
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
            let guard = self.pool.get_page_relieved(self.page_id(current))?;
            let step = step_descend(&guard, key)?;
            drop(guard);
            match step {
                DescendStep::ChaseRight(next) => current = next,
                DescendStep::ReachedLeaf => return Ok(current),
                DescendStep::Descend(child) => current = child,
            }
        }
    }

    /// Descend to the *leftmost* leaf whose subtree can contain `key`.
    ///
    /// [`Self::descend_to_leaf_readonly`] routes to the last child of a
    /// duplicate-separator run, so for a non-unique key whose equal-key group
    /// spans a leaf split it can land to the right of the first matching
    /// entry. This routes left at every internal level (and never chases
    /// right), so the returned leaf is at or before the start of the run; the
    /// caller then scans forward across right-links to cover the whole run.
    /// Used by delete, where a `(key, value)` pair can live on the left side
    /// of a same-key split.
    pub(super) fn descend_to_leftmost_leaf_for_key(
        &self,
        root: BlockNumber,
        key: i64,
    ) -> Result<BlockNumber, BTreeError> {
        let mut current = root;
        loop {
            let guard = self.pool.get_page_relieved(self.page_id(current))?;
            let child = {
                let r = guard.read();
                let meta = NodeMeta::read_from(&r)?;
                if meta.is_leaf() {
                    None
                } else {
                    Some(super::node::find_leftmost_child_internal(&r, meta, key)?)
                }
            };
            drop(guard);
            match child {
                None => return Ok(current),
                Some(child) => current = child,
            }
        }
    }

    fn lookup_in_leaf(&self, leaf: BlockNumber, key: i64) -> Result<Option<TupleId>, BTreeError> {
        let mut current = leaf;
        loop {
            let guard = self.pool.get_page_relieved(self.page_id(current))?;
            let probe = probe_leaf(&guard, key)?;
            drop(guard);
            match probe {
                LeafProbe::ChaseRight(next) => current = next,
                LeafProbe::Found(value) => return Ok(Some(value)),
                LeafProbe::Missing => return Ok(None),
            }
        }
    }

    /// Delete a single `(key, TupleId)` entry from the tree.
    ///
    /// This is a leaf-local removal: pages are allowed to become
    /// underfull and no internal separators are merged or rebalanced.
    /// That is sufficient for secondary-index maintenance because
    /// stale keys disappear immediately while future inserts can reuse
    /// the same logical key.
    pub fn delete<K: Key>(&mut self, key: K, value: TupleId) -> Result<bool, BTreeError> {
        let op_latch = Arc::clone(&self.op_latch);
        let _op_guard = op_latch.write();
        self.delete_inner(key, value, None, None)
    }

    /// Delete a single `(key, TupleId)` entry and emit a B-tree WAL record
    /// when `wal` is configured.
    pub fn delete_logged<K: Key>(
        &mut self,
        key: K,
        value: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<bool, BTreeError> {
        let op_latch = Arc::clone(&self.op_latch);
        let _op_guard = op_latch.write();
        self.delete_inner(key, value, Some(xid), wal)
    }

    pub(super) fn delete_inner<K: Key>(
        &mut self,
        key: K,
        value: TupleId,
        xid: Option<Xid>,
        wal: Option<&dyn WalSink>,
    ) -> Result<bool, BTreeError> {
        if K::SIZE != 8 {
            return Err(BTreeError::KeyTooLarge);
        }
        let mut buf = [0_u8; 8];
        key.encode(&mut buf);
        let raw_key = read_i64_le(&buf).map_err(|_| BTreeError::MalformedNode("key encode"))?;

        let root = *self.root_block.lock();
        // Enter at the leftmost leaf that can hold `raw_key` so a duplicate
        // resident on the left side of a same-key split is reachable; the
        // forward scan in `delete_from_leaf` then walks the rest of the run.
        let leaf = self.descend_to_leftmost_leaf_for_key(root, raw_key)?;
        let Some(deleted_leaf) = self.delete_from_leaf(leaf, raw_key, value)? else {
            return Ok(false);
        };
        if let (Some(sink), Some(xid)) = (wal, xid) {
            let prev_lsn = sink.last_lsn_for(xid);
            let mut tuple_bytes = vec![0_u8; 12];
            write_u32_le(&mut tuple_bytes[0..4], value.page.relation.oid().raw());
            write_u32_le(&mut tuple_bytes[4..8], value.page.block.raw());
            write_u16_le(&mut tuple_bytes[8..10], value.slot);
            let payload = BTreeOpPayload {
                op: BTreeOpKind::Delete,
                index_rel: self.rel,
                page: self.page_id(deleted_leaf),
                key_bytes: buf.to_vec(),
                child_or_value: tuple_bytes,
            }
            .encode()?;
            let record = WalRecord::new(RecordType::BTreeOp, xid, prev_lsn, 0, payload)?;
            let lsn: Lsn = Self::append_after_page_mutation(&self.pool, sink, record)?;
            Self::stamp_page_lsn(&self.pool, self.page_id(deleted_leaf), lsn)?;
        }
        Ok(true)
    }

    fn delete_from_leaf(
        &self,
        leaf: BlockNumber,
        key: i64,
        value: TupleId,
    ) -> Result<Option<BlockNumber>, BTreeError> {
        let mut current = leaf;
        loop {
            let guard = self.pool.get_page_relieved(self.page_id(current))?;
            let mut w = guard.write();
            let meta = super::node::NodeMeta::read_from(&w)?;
            debug_assert!(meta.is_leaf(), "descended to non-leaf in delete");

            // Search this leaf BEFORE deciding to chase right. For a duplicate
            // group spanning a same-key split, the left leaves carry
            // high_key == key, so `should_chase_right` is true for them even
            // though the target entry may live on this very leaf. Searching
            // first means a left-resident `(key, value)` is found here;
            // chasing only happens when it is genuinely absent locally.
            let mut entries = super::node::read_leaf_entries(&w, meta.n_keys)?;
            if let Some(pos) = entries
                .iter()
                .position(|entry| entry.key == key && entry.value == value)
            {
                entries.remove(pos);
                super::node::write_leaf_entries(&mut w, &entries);
                let new_meta = super::node::NodeMeta {
                    n_keys: u16::try_from(entries.len())
                        .map_err(|_| BTreeError::MalformedNode("leaf underflow"))?,
                    ..meta
                };
                new_meta.write_into(&mut w);
                return Ok(Some(current));
            }

            // Not on this leaf — the equal-key run may continue on the right
            // sibling (Lehman-Yao right-link chase).
            if let Some(next) = super::node::should_chase_right(meta, key) {
                drop(w);
                drop(guard);
                current = BlockNumber::new(next);
                continue;
            }
            return Ok(None);
        }
    }
}

pub(super) fn leftmost_leaf<L: PageLoader>(
    tree: &BTree<L>,
    root: BlockNumber,
) -> Result<BlockNumber, BTreeError> {
    let mut current = root;
    loop {
        let guard = tree.pool.get_page_relieved(tree.page_id(current))?;
        let (is_leaf, first_child) = {
            let r = guard.read();
            let meta = NodeMeta::read_from(&r)?;
            let first_child = if meta.is_leaf() {
                None
            } else {
                let entries = super::node::read_internal_entries(&r, meta.n_keys)?;
                entries.first().map(|e| e.child)
            };
            (meta.is_leaf(), first_child)
        };
        drop(guard);
        if is_leaf {
            return Ok(current);
        }
        current =
            BlockNumber::new(first_child.ok_or(BTreeError::MalformedNode("empty internal node"))?);
    }
}
