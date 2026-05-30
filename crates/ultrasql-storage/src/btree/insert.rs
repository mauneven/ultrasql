//! Insertion path: leaf split, internal split, and bottom-up
//! split propagation up to a new root.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use ultrasql_core::endian::{read_i64_le, write_i64_le, write_u16_le, write_u32_le};
use ultrasql_core::{BlockNumber, Lsn, PageId, TupleId, Xid};
use ultrasql_wal::WalRecord;
use ultrasql_wal::payload::{BTreeOpKind, BTreeOpPayload};
use ultrasql_wal::record::RecordType;

use crate::buffer_pool::{BufferPool, PageGuard, PageLoader};
use crate::wal_sink::WalSink;

use super::node::{
    InternalEntry, LeafEntry, LeafInsertOutcome, NodeMeta, init_btree_page, read_internal_entries,
    read_leaf_entries, should_chase_right, write_internal_entries, write_leaf_entries,
};
use super::{
    BTree, BTreeError, FLAG_HAS_HIGH_KEY, FLAG_LEAF, Key, MAX_INTERNAL_ENTRIES, MAX_LEAF_ENTRIES,
};

impl<L: PageLoader> BTree<L> {
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
        let op_latch = Arc::clone(&self.op_latch);
        let _op_guard = op_latch.write();
        self.insert_inner(key, value, xid, wal, false)
    }

    /// Insert `(key, value)` into a non-unique tree.
    ///
    /// Duplicate keys are stored as adjacent leaf entries ordered by
    /// `(key, TupleId)`. Callers that enforce UNIQUE / PRIMARY KEY must use
    /// [`Self::insert`] instead, or pre-check the key before calling this
    /// method.
    pub fn insert_non_unique<K: Key>(
        &mut self,
        key: K,
        value: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
    ) -> Result<(), BTreeError> {
        let op_latch = Arc::clone(&self.op_latch);
        let _op_guard = op_latch.write();
        self.insert_inner(key, value, xid, wal, true)
    }

    pub(super) fn insert_inner<K: Key>(
        &mut self,
        key: K,
        value: TupleId,
        xid: Xid,
        wal: Option<&dyn WalSink>,
        allow_duplicates: bool,
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
        let split_result = self.insert_into_leaf(leaf_block, raw_key, value, allow_duplicates)?;

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
            let record = WalRecord::new(RecordType::BTreeOp, xid, prev_lsn, 0, payload)?;
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
                let record = WalRecord::new(RecordType::BTreeOp, xid, prev_lsn, 0, payload)?;
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
    pub(super) fn stamp_page_lsn(
        pool: &std::sync::Arc<BufferPool<L>>,
        page_id: PageId,
        lsn: Lsn,
    ) -> Result<(), BTreeError> {
        let guard = pool.get_page(page_id)?;
        guard.write().set_lsn(lsn.raw());
        Ok(())
    }

    // ----------- insert helpers -----------------------------------------

    /// Insert into the named leaf. Returns `Some((separator, right_sibling))`
    /// if the leaf split, `None` otherwise.
    fn insert_into_leaf(
        &self,
        leaf: BlockNumber,
        key: i64,
        value: TupleId,
        allow_duplicates: bool,
    ) -> Result<Option<(i64, BlockNumber)>, BTreeError> {
        // Right-link chase if a concurrent writer (or our own past-self
        // in a stale path) has split this leaf out from under us.
        let mut current = leaf;
        loop {
            let guard = self.pool.get_page(self.page_id(current))?;
            let outcome = self.try_leaf_insert(&guard, key, value, allow_duplicates)?;
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
        allow_duplicates: bool,
    ) -> Result<LeafInsertOutcome, BTreeError> {
        let mut w = guard.write();
        let meta = NodeMeta::read_from(&w)?;
        debug_assert!(meta.is_leaf(), "descended to non-leaf in insert");
        if let Some(next) = should_chase_right(meta, key) {
            drop(w);
            return Ok(LeafInsertOutcome::ChaseRight(BlockNumber::new(next)));
        }

        // Search for the insertion position. Unique indexes reject any
        // existing key; non-unique indexes keep same-key entries ordered by
        // TupleId so duplicate probes are deterministic.
        let entries = read_leaf_entries(&w, meta.n_keys)?;
        let pos = if allow_duplicates {
            let pos = entries.partition_point(|entry| (entry.key, entry.value) < (key, value));
            if entries
                .get(pos)
                .is_some_and(|entry| entry.key == key && entry.value == value)
            {
                return Err(BTreeError::DuplicateKey);
            }
            pos
        } else {
            let pos_result = entries.binary_search_by_key(&key, |e| e.key);
            if pos_result.is_ok() {
                return Err(BTreeError::DuplicateKey);
            }
            pos_result.unwrap_or_else(|i| i)
        };

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
        // The root split. Keep the root block stable: catalog entries
        // store the root block, and DML opens fresh BTree handles from
        // that persisted value. Copy the split-left root into a new
        // left child, then overwrite the original root block with the
        // new internal root.
        let old_root = *self.root_block.lock();
        let new_left = self.allocate_block();
        let (old_root_bytes, level) = {
            let guard = self.pool.get_page(self.page_id(old_root))?;
            let r = guard.read();
            let meta = NodeMeta::read_from(&r)?;
            (*r.as_bytes(), meta.level.saturating_add(1))
        };
        {
            let guard = self.pool.get_page(self.page_id(new_left))?;
            guard
                .write()
                .as_bytes_mut()
                .copy_from_slice(&old_root_bytes);
        }
        {
            let guard = self.pool.get_page(self.page_id(old_root))?;
            {
                let mut w = guard.write();
                let meta = NodeMeta::fresh_internal(level);
                init_btree_page(&mut w, meta)?;
                // Two initial children: copied left (covers (-inf, sep_key))
                // and the new right sibling (covers [sep_key, +inf)). We
                // encode the leftmost child as an entry with key = i64::MIN
                // so binary search routes correctly.
                let entries = [
                    InternalEntry {
                        key: i64::MIN,
                        child: new_left.raw(),
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
        Ok(())
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
        let pos = entries.partition_point(|entry| entry.key <= sep_key);

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

    pub(super) fn allocate_block(&self) -> BlockNumber {
        let raw = self.next_block.fetch_add(1, Ordering::AcqRel);
        BlockNumber::new(raw)
    }
}
