//! On-page B-tree node layout: metadata header, packed leaf and internal
//! entries, page initialisation, and the descent / right-link helpers
//! that route through the sibling chain on Lehman-Yao reads.

use std::ops::Range;
use ultrasql_core::endian::{
    read_i64_le, read_u16_le, read_u32_le, write_i64_le, write_u16_le, write_u32_le,
};
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};

use crate::buffer_pool::{PageGuard, PageLoader};
use crate::page::{PAGE_HEADER_SIZE, Page, PageHeader, PageKind};

use super::{
    BTreeError, FLAG_HAS_HIGH_KEY, FLAG_LEAF, INTERNAL_ENTRY_SIZE, LEAF_ENTRY_SIZE,
    MAX_INTERNAL_ENTRIES, MAX_LEAF_ENTRIES, NO_SIBLING, NODE_SPECIAL_OFFSET,
};

// --- node metadata ---------------------------------------------------------

/// Per-page B-tree metadata stored in the page's special area.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct NodeMeta {
    /// Block number of the right sibling, or [`NO_SIBLING`] if none.
    pub(super) right_link: u32,
    /// The split key for this page: any search key `>= high_key` lives
    /// in the right sibling (or further). Only valid when
    /// [`FLAG_HAS_HIGH_KEY`] is set in `flags`.
    pub(super) high_key: i64,
    /// Tree depth from this node down to a leaf (0 for leaves).
    pub(super) level: u16,
    /// Number of entries currently on the page.
    pub(super) n_keys: u16,
    /// Flag bits — see [`FLAG_LEAF`], [`FLAG_HAS_HIGH_KEY`].
    pub(super) flags: u16,
}

impl NodeMeta {
    const KNOWN_FLAGS: u16 = FLAG_LEAF | FLAG_HAS_HIGH_KEY;

    pub(super) const fn fresh_leaf() -> Self {
        Self {
            right_link: NO_SIBLING,
            high_key: 0,
            level: 0,
            n_keys: 0,
            flags: FLAG_LEAF,
        }
    }

    pub(super) const fn fresh_internal(level: u16) -> Self {
        Self {
            right_link: NO_SIBLING,
            high_key: 0,
            level,
            n_keys: 0,
            flags: 0,
        }
    }

    #[inline]
    pub(super) const fn is_leaf(self) -> bool {
        self.flags & FLAG_LEAF != 0
    }

    #[inline]
    pub(super) const fn has_high_key(self) -> bool {
        self.flags & FLAG_HAS_HIGH_KEY != 0
    }

    pub(super) fn read_from(page: &Page) -> Result<Self, BTreeError> {
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
        if flags & !Self::KNOWN_FLAGS != 0 {
            return Err(BTreeError::MalformedNode("node flags reserved bits"));
        }
        if bytes[off + 18..off + 24].iter().any(|&b| b != 0) {
            return Err(BTreeError::MalformedNode("node reserved bytes"));
        }
        let max_keys = if flags & FLAG_LEAF != 0 {
            MAX_LEAF_ENTRIES
        } else {
            MAX_INTERNAL_ENTRIES
        };
        if usize::from(n_keys) > max_keys {
            return Err(BTreeError::MalformedNode("node key count"));
        }
        Ok(Self {
            right_link,
            high_key,
            level,
            n_keys,
            flags,
        })
    }

    pub(super) fn write_into(self, page: &mut Page) {
        let bytes = page.as_bytes_mut();
        let off = NODE_SPECIAL_OFFSET;
        write_u32_le(&mut bytes[off..off + 4], self.right_link);
        write_i64_le(&mut bytes[off + 4..off + 12], self.high_key);
        write_u16_le(&mut bytes[off + 12..off + 14], self.level);
        write_u16_le(&mut bytes[off + 14..off + 16], self.n_keys);
        write_u16_le(&mut bytes[off + 16..off + 18], self.flags);
        // Reserved bytes (6) at offsets 18..24 are zeroed so future
        // format extensions can repurpose them without inheriting
        // stale page contents.
        bytes[off + 18..off + 24].fill(0);
    }
}

// --- internal helper enums -------------------------------------------------

#[derive(Debug)]
pub(super) enum DescendStep {
    ChaseRight(BlockNumber),
    ReachedLeaf,
    Descend(BlockNumber),
}

#[derive(Debug)]
pub(super) enum LeafInsertOutcome {
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
pub(super) enum LeafProbe {
    ChaseRight(BlockNumber),
    Found(TupleId),
    Missing,
}

// --- pure helper functions (no &self) --------------------------------------

pub(super) fn step_descend<L: PageLoader>(
    guard: &PageGuard<L>,
    key: i64,
) -> Result<DescendStep, BTreeError> {
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

pub(super) fn probe_leaf<L: PageLoader>(
    guard: &PageGuard<L>,
    key: i64,
) -> Result<LeafProbe, BTreeError> {
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
pub(super) struct LeafEntry {
    pub(super) key: i64,
    pub(super) value: TupleId,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct InternalEntry {
    pub(super) key: i64,
    pub(super) child: u32,
}

pub(super) fn read_leaf_entries(page: &Page, count: u16) -> Result<Vec<LeafEntry>, BTreeError> {
    let bytes = page.as_bytes();
    let count = usize::from(count);
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let off = entry_start(i, LEAF_ENTRY_SIZE, "leaf entry out of range")?;
        let key = read_i64_le(&bytes[entry_field(off, 0, 8, "leaf entry out of range")?])
            .map_err(|_| BTreeError::MalformedNode("leaf key"))?;
        let rel = read_u32_le(&bytes[entry_field(off, 8, 12, "leaf entry out of range")?])
            .map_err(|_| BTreeError::MalformedNode("rel"))?;
        let block = read_u32_le(&bytes[entry_field(off, 12, 16, "leaf entry out of range")?])
            .map_err(|_| BTreeError::MalformedNode("block"))?;
        let slot = read_u16_le(&bytes[entry_field(off, 16, 18, "leaf entry out of range")?])
            .map_err(|_| BTreeError::MalformedNode("slot"))?;
        if bytes[entry_field(off, 18, LEAF_ENTRY_SIZE, "leaf entry out of range")?]
            .iter()
            .any(|&b| b != 0)
        {
            return Err(BTreeError::MalformedNode("leaf entry reserved bytes"));
        }
        let value = TupleId::new(
            PageId::new(RelationId::new(rel), BlockNumber::new(block)),
            slot,
        );
        out.push(LeafEntry { key, value });
    }
    Ok(out)
}

pub(super) fn write_leaf_entries(page: &mut Page, entries: &[LeafEntry]) {
    let bytes = page.as_bytes_mut();
    for (i, e) in entries.iter().enumerate() {
        let off = entry_start_or_panic(i, LEAF_ENTRY_SIZE, "leaf entry out of range");
        write_i64_le(
            &mut bytes[entry_field_or_panic(off, 0, 8, "leaf entry out of range")],
            e.key,
        );
        write_u32_le(
            &mut bytes[entry_field_or_panic(off, 8, 12, "leaf entry out of range")],
            e.value.page.relation.0.raw(),
        );
        write_u32_le(
            &mut bytes[entry_field_or_panic(off, 12, 16, "leaf entry out of range")],
            e.value.page.block.raw(),
        );
        write_u16_le(
            &mut bytes[entry_field_or_panic(off, 16, 18, "leaf entry out of range")],
            e.value.slot,
        );
        // Pad bytes 18..20 set to zero; readers ignore.
        bytes[entry_field_or_panic(off, 18, LEAF_ENTRY_SIZE, "leaf entry out of range")].fill(0);
    }
}

pub(super) fn read_internal_entries(
    page: &Page,
    count: u16,
) -> Result<Vec<InternalEntry>, BTreeError> {
    let bytes = page.as_bytes();
    let count = usize::from(count);
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let off = entry_start(i, INTERNAL_ENTRY_SIZE, "internal entry out of range")?;
        let key = read_i64_le(&bytes[entry_field(off, 0, 8, "internal entry out of range")?])
            .map_err(|_| BTreeError::MalformedNode("internal key"))?;
        let child = read_u32_le(&bytes[entry_field(off, 8, 12, "internal entry out of range")?])
            .map_err(|_| BTreeError::MalformedNode("child"))?;
        if bytes[entry_field(off, 12, INTERNAL_ENTRY_SIZE, "internal entry out of range")?]
            .iter()
            .any(|&b| b != 0)
        {
            return Err(BTreeError::MalformedNode("internal entry reserved bytes"));
        }
        out.push(InternalEntry { key, child });
    }
    Ok(out)
}

pub(super) fn write_internal_entries(page: &mut Page, entries: &[InternalEntry]) {
    let bytes = page.as_bytes_mut();
    for (i, e) in entries.iter().enumerate() {
        let off = entry_start_or_panic(i, INTERNAL_ENTRY_SIZE, "internal entry out of range");
        write_i64_le(
            &mut bytes[entry_field_or_panic(off, 0, 8, "internal entry out of range")],
            e.key,
        );
        write_u32_le(
            &mut bytes[entry_field_or_panic(off, 8, 12, "internal entry out of range")],
            e.child,
        );
        bytes[entry_field_or_panic(off, 12, INTERNAL_ENTRY_SIZE, "internal entry out of range")]
            .fill(0);
    }
}

// --- helpers ---------------------------------------------------------------

fn entry_start(index: usize, entry_size: usize, label: &'static str) -> Result<usize, BTreeError> {
    let payload_offset = index
        .checked_mul(entry_size)
        .ok_or(BTreeError::MalformedNode(label))?;
    let start = PAGE_HEADER_SIZE
        .checked_add(payload_offset)
        .ok_or(BTreeError::MalformedNode(label))?;
    let end = start
        .checked_add(entry_size)
        .ok_or(BTreeError::MalformedNode(label))?;
    if end > NODE_SPECIAL_OFFSET {
        return Err(BTreeError::MalformedNode(label));
    }
    Ok(start)
}

#[allow(
    clippy::panic,
    reason = "INVARIANT: callers iterate `index` over a node's key count, which `NodeHeader::decode` bounds to `MAX_LEAF_ENTRIES`/`MAX_INTERNAL_ENTRIES` (chosen so all entries fit before `NODE_SPECIAL_OFFSET`), so `entry_start` cannot fail"
)]
fn entry_start_or_panic(index: usize, entry_size: usize, label: &'static str) -> usize {
    match entry_start(index, entry_size, label) {
        Ok(start) => start,
        Err(err) => panic!("B-tree entry offset invariant violated: {err}"),
    }
}

fn entry_field(
    base: usize,
    start: usize,
    end: usize,
    label: &'static str,
) -> Result<Range<usize>, BTreeError> {
    let start = base
        .checked_add(start)
        .ok_or(BTreeError::MalformedNode(label))?;
    let end = base
        .checked_add(end)
        .ok_or(BTreeError::MalformedNode(label))?;
    if start > end || end > NODE_SPECIAL_OFFSET {
        return Err(BTreeError::MalformedNode(label));
    }
    Ok(start..end)
}

#[allow(
    clippy::panic,
    reason = "INVARIANT: callers pass a `base` from `entry_start_or_panic` plus compile-time-constant field offsets within the entry size, so the field range stays inside the entry and `entry_field` cannot fail"
)]
fn entry_field_or_panic(
    base: usize,
    start: usize,
    end: usize,
    label: &'static str,
) -> Range<usize> {
    match entry_field(base, start, end, label) {
        Ok(range) => range,
        Err(err) => panic!("B-tree entry field invariant violated: {err}"),
    }
}

pub(super) fn init_btree_page(page: &mut Page, meta: NodeMeta) -> Result<(), BTreeError> {
    // Reinitialise the page header so it represents a B-tree page
    // with the special area carved out at the tail.
    let lower =
        u16::try_from(PAGE_HEADER_SIZE).map_err(|_| BTreeError::MalformedNode("page header"))?;
    let special = u16::try_from(NODE_SPECIAL_OFFSET)
        .map_err(|_| BTreeError::MalformedNode("node special offset"))?;
    let new_header = PageHeader {
        lsn: 0,
        checksum: 0,
        flags: 0,
        kind: PageKind::BTreeIndex,
        lower,
        upper: special,
        special,
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
pub(super) const fn should_chase_right(meta: NodeMeta, key: i64) -> Option<u32> {
    if !meta.has_high_key() {
        return None;
    }
    if key >= meta.high_key && meta.right_link != NO_SIBLING {
        Some(meta.right_link)
    } else {
        None
    }
}

pub(super) fn find_child_internal(
    page: &Page,
    meta: NodeMeta,
    key: i64,
) -> Result<BlockNumber, BTreeError> {
    let entries = read_internal_entries(page, meta.n_keys)?;
    if entries.is_empty() {
        return Err(BTreeError::MalformedNode("empty internal node"));
    }
    // Find the rightmost entry whose key is <= our search key. Duplicate
    // separators are legal for non-unique indexes whose same-key posting
    // chain crosses leaf splits, so use `partition_point` instead of
    // `binary_search_by_key`'s arbitrary duplicate hit.
    // Entry 0 always has key = i64::MIN by construction.
    let idx = entries
        .partition_point(|entry| entry.key <= key)
        .saturating_sub(1);
    Ok(BlockNumber::new(entries[idx].child))
}
