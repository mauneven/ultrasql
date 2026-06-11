//! 8 KiB slotted page layout.
//!
//! A page is the unit of on-disk and in-memory storage. UltraSQL pages
//! follow the established slotted-page format used by PostgreSQL and
//! SQL Server:
//!
//! ```text
//!  0                                                            8 KiB
//!  ┌──────────────┬───────────────┬────────────┬─────────────────┐
//!  │   Header     │   ItemId[]    │  free      │   Tuple data    │
//!  │ (24 bytes)   │ (grows ────►) │            │ (◄──── grows)   │
//!  └──────────────┴───────────────┴────────────┴─────────────────┘
//!  └──────────── pd_lower ───────►              ◄──── pd_upper ───┘
//! ```
//!
//! - The header is fixed-size at the start of the page.
//! - The ItemId array grows toward higher offsets immediately after the
//!   header. Each entry is 4 bytes and encodes a tuple's offset, length,
//!   and lifecycle flags.
//! - Tuple data grows from the high end of the page toward low offsets.
//! - Free space lives between the two; it is exactly `pd_upper -
//!   pd_lower` bytes.
//! - The special area (used by index access methods) lives between
//!   `pd_special` and the end of the page; heap pages set
//!   `pd_special == PAGE_SIZE`.
//!
//! This layout supports O(1) tuple lookup by slot index, in-place
//! modification of small tuples, and bulk reclamation via VACUUM.
//!
//! ## Invariants
//!
//! - `pd_lower <= pd_upper <= pd_special <= PAGE_SIZE`.
//! - `pd_lower >= PAGE_HEADER_SIZE`.
//! - `(pd_lower - PAGE_HEADER_SIZE) % ITEMID_SIZE == 0`.
//! - The number of slots equals `(pd_lower - PAGE_HEADER_SIZE) / ITEMID_SIZE`.
//! - Each live ItemId points to a contiguous, in-page region
//!   `[offset, offset+length)` with `offset >= pd_upper` and
//!   `offset + length <= pd_special`.

use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::endian::{
    read_u16_le, read_u32_le, read_u64_le, write_u16_le, write_u32_le, write_u64_le,
};

use crate::checksum::{CHECKSUM_OFFSET, compute_page_checksum};

/// Size of a [`PageHeader`] in bytes.
pub const PAGE_HEADER_SIZE: usize = 24;
const PAGE_HEADER_SIZE_U16: u16 = 24;

/// Size of a single [`ItemId`] in bytes.
pub const ITEMID_SIZE: usize = 4;
const ITEMID_SIZE_U16: u16 = 4;
const PAGE_SIZE_U16: u16 = 8_192;

/// On-disk page-format version recognized by this crate.
pub const PAGE_VERSION_CURRENT: u8 = 1;

// --- compile-time invariants -----------------------------------------------
const _: () = assert!(PAGE_SIZE.is_power_of_two());
const _: () = assert!(PAGE_SIZE == 8_192);
const _: () = assert!(PAGE_HEADER_SIZE <= 64);
const _: () = assert!(PAGE_HEADER_SIZE == 24);
const _: () = assert!(ITEMID_SIZE == 4);

/// Errors that can arise when reading or writing a page.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PageError {
    /// The page header is internally inconsistent — pointers out of
    /// range or in the wrong order.
    #[error("malformed page header: {0}")]
    Malformed(&'static str),

    /// The page checksum does not match the page contents.
    #[error("page checksum mismatch (expected {expected:08x}, got {actual:08x})")]
    ChecksumMismatch {
        /// Checksum stored in the page.
        expected: u32,
        /// Checksum recomputed from page contents.
        actual: u32,
    },

    /// The requested operation would not fit in the page's free space.
    #[error("not enough free space on page (need {needed}, have {available})")]
    NoSpace {
        /// Bytes required to satisfy the request.
        needed: usize,
        /// Bytes currently free.
        available: usize,
    },

    /// A slot index is outside the page's slot array.
    #[error("slot index {index} out of bounds (page has {len} slots)")]
    InvalidSlot {
        /// The bad index.
        index: u16,
        /// Number of slots on the page.
        len: u16,
    },

    /// The referenced slot does not point to a live tuple.
    #[error("slot {0} is not live")]
    DeadSlot(u16),

    /// The on-disk page version is not understood by this build.
    #[error("unsupported page version {0}")]
    UnsupportedVersion(u8),
}

/// Page-type tag stored in the header. Distinct from the
/// access-method-specific layout but recorded so a generic recovery
/// tool can know whether a page is a heap page, an index page, etc.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum PageKind {
    /// Heap (relation data).
    Heap = 1,
    /// B-tree index.
    BTreeIndex = 2,
    /// Free-space map page.
    FreeSpaceMap = 3,
    /// Visibility map page.
    VisibilityMap = 4,
    /// Metadata / catalog index of segments.
    Meta = 5,
}

impl PageKind {
    const fn from_u16(v: u16) -> Option<Self> {
        Some(match v {
            1 => Self::Heap,
            2 => Self::BTreeIndex,
            3 => Self::FreeSpaceMap,
            4 => Self::VisibilityMap,
            5 => Self::Meta,
            _ => return None,
        })
    }

    const fn to_u16(self) -> u16 {
        match self {
            Self::Heap => 1,
            Self::BTreeIndex => 2,
            Self::FreeSpaceMap => 3,
            Self::VisibilityMap => 4,
            Self::Meta => 5,
        }
    }
}

/// Lifecycle flags on a slot's `ItemId`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ItemIdFlags {
    /// The slot is allocated but no tuple is currently stored at it.
    Unused = 0,
    /// The slot points to a live, undeleted tuple.
    Normal = 1,
    /// The slot has been overwritten in a HOT chain and is now a
    /// redirect to another slot on the same page.
    Redirect = 2,
    /// The slot's tuple is dead (deleted / aborted) and may be
    /// reclaimed by VACUUM.
    Dead = 3,
}

impl ItemIdFlags {
    const fn from_bits(bits: u32) -> Self {
        match bits & 0b11 {
            0 => Self::Unused,
            1 => Self::Normal,
            2 => Self::Redirect,
            _ => Self::Dead,
        }
    }

    const fn to_u32(self) -> u32 {
        match self {
            Self::Unused => 0,
            Self::Normal => 1,
            Self::Redirect => 2,
            Self::Dead => 3,
        }
    }
}

/// Packed 4-byte slot pointer.
///
/// Bit layout (least significant first):
/// `[ 2 bits: flags | 15 bits: length | 15 bits: offset ]`
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ItemId(u32);

impl ItemId {
    /// Maximum representable offset within a page.
    pub const MAX_OFFSET: u32 = (1 << 15) - 1;
    /// Maximum representable length.
    pub const MAX_LENGTH: u32 = (1 << 15) - 1;

    /// Build a new `ItemId`. Panics in debug builds if `offset` or
    /// `length` exceed 15-bit limits.
    #[must_use]
    pub const fn new(offset: u32, length: u32, flags: ItemIdFlags) -> Self {
        debug_assert!(offset <= Self::MAX_OFFSET, "ItemId offset too large");
        debug_assert!(length <= Self::MAX_LENGTH, "ItemId length too large");
        let bits = flags.to_u32() | (length << 2) | (offset << 17);
        Self(bits)
    }

    /// Construct an `ItemId` from its packed `u32` representation.
    #[must_use]
    pub const fn from_raw(raw: u32) -> Self {
        Self(raw)
    }

    /// Packed `u32` representation.
    #[must_use]
    pub const fn into_raw(self) -> u32 {
        self.0
    }

    /// Lifecycle flags.
    #[must_use]
    pub const fn flags(self) -> ItemIdFlags {
        ItemIdFlags::from_bits(self.0)
    }

    /// Length in bytes of the slot's tuple.
    #[must_use]
    pub const fn length(self) -> u32 {
        (self.0 >> 2) & Self::MAX_LENGTH
    }

    /// Byte offset of the tuple within the page.
    #[must_use]
    pub const fn offset(self) -> u32 {
        (self.0 >> 17) & Self::MAX_OFFSET
    }

    /// Whether this slot points to a live, normal tuple.
    #[must_use]
    pub const fn is_normal(self) -> bool {
        matches!(self.flags(), ItemIdFlags::Normal)
    }
}

/// Decoded page header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageHeader {
    /// Last-modified LSN. Used by WAL replay to detect that a page in
    /// memory is already past a record's LSN and skip redo.
    pub lsn: u64,
    /// Page checksum.
    pub checksum: u32,
    /// Flag bits. Reserved; defined per page kind in higher layers.
    pub flags: u16,
    /// Kind of page.
    pub kind: PageKind,
    /// First free byte after the ItemId array, measured from page start.
    pub lower: u16,
    /// Start of the highest-addressed tuple in the body.
    pub upper: u16,
    /// Start of the special area (for index pages). On heap pages this
    /// equals `PAGE_SIZE`.
    pub special: u16,
    /// On-disk format version.
    pub version: u8,
}

impl PageHeader {
    /// Build a freshly-initialized header for a heap page with no
    /// special area.
    #[must_use]
    pub const fn fresh_heap() -> Self {
        Self {
            lsn: 0,
            checksum: 0,
            flags: 0,
            kind: PageKind::Heap,
            lower: PAGE_HEADER_SIZE_U16,
            upper: PAGE_SIZE_U16,
            special: PAGE_SIZE_U16,
            version: PAGE_VERSION_CURRENT,
        }
    }

    /// Decode a page header from the first [`PAGE_HEADER_SIZE`] bytes
    /// of a page.
    pub fn decode(bytes: &[u8; PAGE_SIZE]) -> Result<Self, PageError> {
        let lsn = read_u64_le(&bytes[0..8]).map_err(|_| PageError::Malformed("lsn"))?;
        let checksum = read_u32_le(&bytes[8..12]).map_err(|_| PageError::Malformed("checksum"))?;
        let flags = read_u16_le(&bytes[12..14]).map_err(|_| PageError::Malformed("flags"))?;
        let kind_raw = read_u16_le(&bytes[14..16]).map_err(|_| PageError::Malformed("kind"))?;
        let lower = read_u16_le(&bytes[16..18]).map_err(|_| PageError::Malformed("lower"))?;
        let upper = read_u16_le(&bytes[18..20]).map_err(|_| PageError::Malformed("upper"))?;
        let special = read_u16_le(&bytes[20..22]).map_err(|_| PageError::Malformed("special"))?;
        let version = bytes[22];

        let kind = PageKind::from_u16(kind_raw).ok_or(PageError::Malformed("unknown page kind"))?;

        if bytes[23] != 0 {
            return Err(PageError::Malformed("reserved header byte"));
        }

        if version != PAGE_VERSION_CURRENT {
            return Err(PageError::UnsupportedVersion(version));
        }

        let lo = usize::from(lower);
        let up = usize::from(upper);
        let sp = usize::from(special);
        if lo < PAGE_HEADER_SIZE
            || up < lo
            || sp < up
            || sp > PAGE_SIZE
            || (lo - PAGE_HEADER_SIZE) % ITEMID_SIZE != 0
        {
            return Err(PageError::Malformed("pointer ordering"));
        }

        Ok(Self {
            lsn,
            checksum,
            flags,
            kind,
            lower,
            upper,
            special,
            version,
        })
    }

    /// Encode the header into the first [`PAGE_HEADER_SIZE`] bytes of
    /// the page.
    pub fn encode(&self, bytes: &mut [u8; PAGE_SIZE]) {
        write_u64_le(&mut bytes[0..8], self.lsn);
        write_u32_le(&mut bytes[8..12], self.checksum);
        write_u16_le(&mut bytes[12..14], self.flags);
        write_u16_le(&mut bytes[14..16], self.kind.to_u16());
        write_u16_le(&mut bytes[16..18], self.lower);
        write_u16_le(&mut bytes[18..20], self.upper);
        write_u16_le(&mut bytes[20..22], self.special);
        bytes[22] = self.version;
        bytes[23] = 0;
    }

    /// Number of slots currently allocated on the page.
    #[must_use]
    pub const fn slot_count(&self) -> u16 {
        (self.lower - PAGE_HEADER_SIZE_U16) / ITEMID_SIZE_U16
    }

    /// Bytes of free space available for additional tuples.
    #[must_use]
    pub fn free_space(&self) -> usize {
        usize::from(self.upper) - usize::from(self.lower)
    }
}

/// Slot index — zero-based into the ItemId array.
pub type SlotIndex = u16;

/// Owned, page-sized buffer with slotted-page accessors.
///
/// `Page` carries its bytes inline (8 KiB, heap-allocated via `Box`).
/// Callers can hand the boxed buffer to I/O routines verbatim.
#[derive(Debug)]
pub struct Page {
    bytes: Box<[u8; PAGE_SIZE]>,
}

fn page_u16_from_usize(value: usize, field: &'static str) -> Result<u16, PageError> {
    u16::try_from(value).map_err(|_| PageError::Malformed(field))
}

fn item_offset_from_usize(offset: usize) -> Result<u32, PageError> {
    let offset =
        u32::try_from(offset).map_err(|_| PageError::Malformed("tuple offset overflow"))?;
    if offset > ItemId::MAX_OFFSET {
        return Err(PageError::Malformed("tuple offset exceeds itemid"));
    }
    Ok(offset)
}

fn item_field_to_usize(field: u32, name: &'static str) -> Result<usize, PageError> {
    usize::try_from(field).map_err(|_| PageError::Malformed(name))
}

impl Page {
    /// Allocate a freshly-initialized empty heap page.
    #[must_use]
    pub fn new_heap() -> Self {
        let bytes: Box<[u8; PAGE_SIZE]> = Box::new([0_u8; PAGE_SIZE]);
        let mut page = Self { bytes };
        PageHeader::fresh_heap().encode(&mut page.bytes);
        page
    }

    /// Read borrowed bytes.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.bytes
    }

    /// Mutable borrow of the underlying bytes. Direct mutation
    /// bypasses every invariant in this module; reserved for I/O paths
    /// that materialize a page from disk before validation.
    #[inline]
    #[must_use]
    pub fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.bytes
    }

    /// Take ownership of the underlying buffer.
    #[must_use]
    pub fn into_inner(self) -> Box<[u8; PAGE_SIZE]> {
        self.bytes
    }

    /// Construct a `Page` from an existing buffer, validating header
    /// invariants.
    pub fn from_bytes(bytes: Box<[u8; PAGE_SIZE]>) -> Result<Self, PageError> {
        let _hdr = PageHeader::decode(&bytes)?;
        Ok(Self { bytes })
    }

    /// Decode the current header. Note that this re-parses on every
    /// call; callers in hot loops should cache the header locally.
    #[allow(
        clippy::expect_used,
        reason = "Page is constructed only through validated bytes or new_heap; this is the infallible invariant accessor"
    )]
    pub fn header(&self) -> PageHeader {
        // Safety: `Self` only exists for pages whose header has been
        // validated by `from_bytes` or initialized by `new_heap`.
        PageHeader::decode(&self.bytes).expect("page invariant: valid header")
    }

    /// Write a fresh header (e.g. after a HOT update prune that
    /// changed `lower`/`upper`).
    pub fn write_header(&mut self, header: &PageHeader) -> Result<(), PageError> {
        // Validate by encoding into a scratch buffer and decoding.
        let mut scratch = [0_u8; PAGE_SIZE];
        scratch[..PAGE_HEADER_SIZE].copy_from_slice(&self.bytes[..PAGE_HEADER_SIZE]);
        header.encode(&mut scratch);
        let _ = PageHeader::decode(&scratch)?;

        header.encode(&mut self.bytes);
        Ok(())
    }

    /// Recompute the checksum from the current page bytes and write it
    /// into the header. Call before writing the page to disk.
    pub fn refresh_checksum(&mut self) {
        let sum = compute_page_checksum(&self.bytes);
        self.bytes[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].copy_from_slice(&sum.to_le_bytes());
    }

    /// Verify the page's stored checksum matches the recomputed value.
    pub fn verify_checksum(&self) -> Result<(), PageError> {
        let stored = read_u32_le(&self.bytes[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4])
            .map_err(|_| PageError::Malformed("checksum slice"))?;
        let actual = compute_page_checksum(&self.bytes);
        if stored == actual {
            Ok(())
        } else {
            Err(PageError::ChecksumMismatch {
                expected: stored,
                actual,
            })
        }
    }

    /// Set the page LSN.
    pub fn set_lsn(&mut self, lsn: u64) {
        let mut h = self.header();
        h.lsn = lsn;
        h.encode(&mut self.bytes);
    }

    /// Insert a tuple into the page. Returns the slot index assigned
    /// to it.
    ///
    /// Reuses an existing `Unused` or `Dead` slot if one is available
    /// (and that slot can accommodate the tuple); otherwise allocates
    /// a new slot at the tail of the ItemId array.
    pub fn insert_tuple(&mut self, tuple: &[u8]) -> Result<SlotIndex, PageError> {
        let tuple_len = u32::try_from(tuple.len())
            .map_err(|_| PageError::Malformed("tuple too large for page"))?;
        if tuple_len > ItemId::MAX_LENGTH {
            return Err(PageError::Malformed("tuple length exceeds itemid"));
        }

        let mut header = self.header();

        // Look for a reusable slot first.
        let reuse = self.find_reusable_slot(header.slot_count());
        let needed_data = tuple.len();
        let needed_slot = if reuse.is_some() { 0 } else { ITEMID_SIZE };
        let needed = needed_data + needed_slot;
        let free = header.free_space();
        if free < needed {
            return Err(PageError::NoSpace {
                needed,
                available: free,
            });
        }

        let new_upper = usize::from(header.upper) - needed_data;
        self.bytes[new_upper..new_upper + tuple.len()].copy_from_slice(tuple);

        let item = ItemId::new(
            item_offset_from_usize(new_upper)?,
            tuple_len,
            ItemIdFlags::Normal,
        );
        let slot = if let Some(idx) = reuse {
            self.write_item_id(idx, item);
            idx
        } else {
            let idx = header.slot_count();
            self.write_item_id(idx, item);
            header.lower =
                page_u16_from_usize(usize::from(header.lower) + ITEMID_SIZE, "page lower")?;
            idx
        };

        header.upper = page_u16_from_usize(new_upper, "page upper")?;
        header.encode(&mut self.bytes);
        Ok(slot)
    }

    /// Append a tuple to the page **without** scanning the slot
    /// directory for a reusable (Unused) slot.
    ///
    /// Identical to [`Self::insert_tuple`] for pages that have no
    /// reusable slots — the common case for freshly-loaded heap
    /// pages and for HOT-update batches where the caller knows
    /// every preceding slot is still Normal. Skipping
    /// `find_reusable_slot`'s O(slot_count) linear scan drops a
    /// quadratic per-tuple cost during bulk HOT updates from
    /// `O(slot_count²)` total slot reads down to `O(slot_count)`.
    ///
    /// Caller's contract: this allocates a **new** slot at
    /// `slot_count`. Use [`Self::insert_tuple`] when reusing a
    /// previously-deleted slot matters (e.g. inside vacuum or
    /// when reclaiming Unused entries).
    pub fn insert_tuple_appended(&mut self, tuple: &[u8]) -> Result<SlotIndex, PageError> {
        let tuple_len = u32::try_from(tuple.len())
            .map_err(|_| PageError::Malformed("tuple too large for page"))?;
        if tuple_len > ItemId::MAX_LENGTH {
            return Err(PageError::Malformed("tuple length exceeds itemid"));
        }

        let mut header = self.header();
        let needed = tuple.len() + ITEMID_SIZE;
        let free = header.free_space();
        if free < needed {
            return Err(PageError::NoSpace {
                needed,
                available: free,
            });
        }

        let new_upper = usize::from(header.upper) - tuple.len();
        self.bytes[new_upper..new_upper + tuple.len()].copy_from_slice(tuple);

        let item = ItemId::new(
            item_offset_from_usize(new_upper)?,
            tuple_len,
            ItemIdFlags::Normal,
        );
        let idx = header.slot_count();
        self.write_item_id(idx, item);
        header.lower = page_u16_from_usize(usize::from(header.lower) + ITEMID_SIZE, "page lower")?;
        header.upper = page_u16_from_usize(new_upper, "page upper")?;
        header.encode(&mut self.bytes);
        Ok(idx)
    }

    /// Read a tuple by slot index. Returns a slice into the page's
    /// data area.
    pub fn read_tuple(&self, slot: SlotIndex) -> Result<&[u8], PageError> {
        let header = self.header();
        let count = header.slot_count();
        if slot >= count {
            return Err(PageError::InvalidSlot {
                index: slot,
                len: count,
            });
        }
        let id = self.read_item_id(slot);
        if !id.is_normal() {
            return Err(PageError::DeadSlot(slot));
        }
        let off = item_field_to_usize(id.offset(), "tuple offset")?;
        let len = item_field_to_usize(id.length(), "tuple length")?;
        let end = off
            .checked_add(len)
            .ok_or(PageError::Malformed("tuple range overflow"))?;
        if off < usize::from(header.upper) || end > usize::from(header.special) {
            return Err(PageError::Malformed("tuple range out of bounds"));
        }
        Ok(&self.bytes[off..end])
    }

    /// Mark a tuple dead. The slot remains allocated; the data is left
    /// in place until VACUUM compacts the page.
    pub fn delete_tuple(&mut self, slot: SlotIndex) -> Result<(), PageError> {
        let header = self.header();
        let count = header.slot_count();
        if slot >= count {
            return Err(PageError::InvalidSlot {
                index: slot,
                len: count,
            });
        }
        let id = self.read_item_id(slot);
        if !id.is_normal() {
            return Err(PageError::DeadSlot(slot));
        }
        let dead = ItemId::new(id.offset(), id.length(), ItemIdFlags::Dead);
        self.write_item_id(slot, dead);
        Ok(())
    }

    /// Reclaim dead-tuple space by compacting live tuples to the high
    /// end of the page and clearing dead-slot data. Slot indices for
    /// live tuples are preserved.
    pub fn compact(&mut self) -> Result<(), PageError> {
        let mut header = self.header();
        let count = header.slot_count();
        // Gather (slot, length) for every live tuple, in descending
        // offset order so we can rewrite from the top down.
        let mut live: Vec<(SlotIndex, u32, u32)> = Vec::with_capacity(usize::from(count));
        for slot in 0..count {
            let id = self.read_item_id(slot);
            if id.is_normal() {
                live.push((slot, id.offset(), id.length()));
            }
        }
        // Sort by current offset descending so the highest-offset
        // tuple (right-most on disk) moves first; this keeps source
        // and destination ranges disjoint within the rolling write.
        live.sort_by_key(|&(_, off, _)| std::cmp::Reverse(off));

        let mut write_end = usize::from(header.special);
        for (slot, off, len) in live {
            let src = item_field_to_usize(off, "tuple offset")?;
            let length = item_field_to_usize(len, "tuple length")?;
            let src_end = src
                .checked_add(length)
                .ok_or(PageError::Malformed("tuple range overflow"))?;
            if src_end > self.bytes.len() {
                return Err(PageError::Malformed("tuple range out of bounds"));
            }
            let new_off = write_end
                .checked_sub(length)
                .ok_or(PageError::Malformed("tuple compact underflow"))?;
            if new_off != src {
                // copy_within is safe for overlapping ranges; the
                // ordering above guarantees `new_off >= src`.
                self.bytes.copy_within(src..src_end, new_off);
            }
            let new_id = ItemId::new(item_offset_from_usize(new_off)?, len, ItemIdFlags::Normal);
            self.write_item_id(slot, new_id);
            write_end = new_off;
        }

        // Mark dead slots as unused.
        for slot in 0..count {
            let id = self.read_item_id(slot);
            if matches!(id.flags(), ItemIdFlags::Dead) {
                self.write_item_id(slot, ItemId::new(0, 0, ItemIdFlags::Unused));
            }
        }

        header.upper = page_u16_from_usize(write_end, "page upper")?;
        header.encode(&mut self.bytes);
        Ok(())
    }

    // ----------------- internal helpers ----------------------------------

    fn find_reusable_slot(&self, count: SlotIndex) -> Option<SlotIndex> {
        for i in 0..count {
            let id = self.read_item_id(i);
            if matches!(id.flags(), ItemIdFlags::Unused) {
                return Some(i);
            }
        }
        None
    }

    /// Byte offset of the `ItemId` for `slot` within the page's
    /// slot-directory array. Exposed `pub(crate)` so bulk-insert
    /// paths in [`crate::heap`] can write item ids without paying
    /// the per-tuple `page.header()` round trip that
    /// [`Self::insert_tuple_appended`] performs.
    #[must_use]
    pub(crate) fn item_id_offset(slot: SlotIndex) -> usize {
        PAGE_HEADER_SIZE + usize::from(slot) * ITEMID_SIZE
    }

    fn read_item_id(&self, slot: SlotIndex) -> ItemId {
        let off = Self::item_id_offset(slot);
        let raw = u32::from_le_bytes([
            self.bytes[off],
            self.bytes[off + 1],
            self.bytes[off + 2],
            self.bytes[off + 3],
        ]);
        ItemId::from_raw(raw)
    }

    fn write_item_id(&mut self, slot: SlotIndex, id: ItemId) {
        let off = Self::item_id_offset(slot);
        write_u32_le(&mut self.bytes[off..off + ITEMID_SIZE], id.into_raw());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pad(s: &str, n: usize) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.resize(n, b'.');
        v
    }

    #[test]
    fn header_round_trip_via_encode_decode() {
        let mut bytes = [0_u8; PAGE_SIZE];
        let h = PageHeader::fresh_heap();
        h.encode(&mut bytes);
        let decoded = PageHeader::decode(&bytes).unwrap();
        assert_eq!(decoded, h);
    }

    #[test]
    fn fresh_page_has_zero_slots_and_full_free_space() {
        let page = Page::new_heap();
        let h = page.header();
        assert_eq!(h.slot_count(), 0);
        assert_eq!(h.free_space(), PAGE_SIZE - PAGE_HEADER_SIZE);
        assert_eq!(h.kind, PageKind::Heap);
        assert_eq!(h.version, PAGE_VERSION_CURRENT);
    }

    #[test]
    fn insert_and_read_round_trip() {
        let mut page = Page::new_heap();
        let payload = b"hello world";
        let slot = page.insert_tuple(payload).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(page.read_tuple(slot).unwrap(), payload);
        let h = page.header();
        assert_eq!(h.slot_count(), 1);
        assert_eq!(
            h.free_space(),
            PAGE_SIZE - PAGE_HEADER_SIZE - ITEMID_SIZE - payload.len()
        );
    }

    #[test]
    fn many_inserts_increase_slot_count() {
        let mut page = Page::new_heap();
        for i in 0_u32..200 {
            let tup = i.to_le_bytes();
            let slot = page.insert_tuple(&tup).unwrap();
            let expected_slot = SlotIndex::try_from(i).unwrap();
            assert_eq!(slot, expected_slot);
        }
        let h = page.header();
        assert_eq!(h.slot_count(), 200);
        for i in 0_u32..200 {
            let slot = SlotIndex::try_from(i).unwrap();
            assert_eq!(page.read_tuple(slot).unwrap(), &i.to_le_bytes());
        }
    }

    #[test]
    fn insert_overflow_returns_no_space() {
        let mut page = Page::new_heap();
        let huge = pad("X", PAGE_SIZE);
        let err = page.insert_tuple(&huge).unwrap_err();
        assert!(matches!(err, PageError::NoSpace { .. }));
    }

    #[test]
    fn delete_marks_slot_dead_and_blocks_reads() {
        let mut page = Page::new_heap();
        let s = page.insert_tuple(b"data").unwrap();
        page.delete_tuple(s).unwrap();
        let err = page.read_tuple(s).unwrap_err();
        assert!(matches!(err, PageError::DeadSlot(_)));
    }

    #[test]
    fn delete_then_compact_reclaims_space() {
        let mut page = Page::new_heap();
        // Insert three tuples, delete the middle one, compact, and
        // verify free space recovered the deleted bytes.
        let s0 = page.insert_tuple(b"aaaaaa").unwrap();
        let s1 = page.insert_tuple(b"bbbbbb").unwrap();
        let s2 = page.insert_tuple(b"cccccc").unwrap();
        let before = page.header().free_space();
        page.delete_tuple(s1).unwrap();
        page.compact().unwrap();
        let after = page.header().free_space();
        assert!(after > before, "compact must reclaim deleted tuple space");
        // Live tuples remain readable at their original slots.
        assert_eq!(page.read_tuple(s0).unwrap(), b"aaaaaa");
        assert_eq!(page.read_tuple(s2).unwrap(), b"cccccc");
    }

    #[test]
    fn checksum_round_trip() {
        let mut page = Page::new_heap();
        page.insert_tuple(b"hello").unwrap();
        page.refresh_checksum();
        page.verify_checksum().unwrap();
    }

    #[test]
    fn checksum_detects_bit_flip() {
        let mut page = Page::new_heap();
        page.insert_tuple(b"hello").unwrap();
        page.refresh_checksum();
        // Flip a random byte in the data area.
        page.as_bytes_mut()[5000] ^= 0x01;
        let err = page.verify_checksum().unwrap_err();
        assert!(matches!(err, PageError::ChecksumMismatch { .. }));
    }

    #[test]
    fn page_round_trips_via_from_bytes() {
        let mut page = Page::new_heap();
        let s = page.insert_tuple(b"persistent").unwrap();
        page.refresh_checksum();
        let bytes = page.into_inner();
        let page2 = Page::from_bytes(bytes).unwrap();
        page2.verify_checksum().unwrap();
        assert_eq!(page2.read_tuple(s).unwrap(), b"persistent");
    }

    #[test]
    fn item_id_packing_round_trips() {
        for offset in [0_u32, 24, 1000, ItemId::MAX_OFFSET] {
            for length in [1_u32, 10, 100, ItemId::MAX_LENGTH] {
                for flags in [
                    ItemIdFlags::Unused,
                    ItemIdFlags::Normal,
                    ItemIdFlags::Redirect,
                    ItemIdFlags::Dead,
                ] {
                    let id = ItemId::new(offset, length, flags);
                    let round = ItemId::from_raw(id.into_raw());
                    assert_eq!(round.offset(), offset);
                    assert_eq!(round.length(), length);
                    assert_eq!(round.flags(), flags);
                }
            }
        }
    }

    #[test]
    fn malformed_header_rejected() {
        let mut bytes = [0_u8; PAGE_SIZE];
        // kind=0 is undefined.
        write_u16_le(&mut bytes[16..18], PAGE_HEADER_SIZE_U16);
        write_u16_le(&mut bytes[18..20], PAGE_SIZE_U16);
        write_u16_le(&mut bytes[20..22], PAGE_SIZE_U16);
        bytes[22] = PAGE_VERSION_CURRENT;
        let err = PageHeader::decode(&bytes).unwrap_err();
        assert!(matches!(err, PageError::Malformed(_)));
    }

    #[test]
    fn unsupported_version_rejected() {
        let mut bytes = [0_u8; PAGE_SIZE];
        write_u16_le(&mut bytes[14..16], PageKind::Heap.to_u16());
        write_u16_le(&mut bytes[16..18], PAGE_HEADER_SIZE_U16);
        write_u16_le(&mut bytes[18..20], PAGE_SIZE_U16);
        write_u16_le(&mut bytes[20..22], PAGE_SIZE_U16);
        bytes[22] = 0xFF;
        let err = PageHeader::decode(&bytes).unwrap_err();
        assert!(matches!(err, PageError::UnsupportedVersion(0xFF)));
    }

    #[test]
    fn reserved_header_byte_rejected() {
        let mut bytes = [0_u8; PAGE_SIZE];
        let h = PageHeader::fresh_heap();
        h.encode(&mut bytes);
        bytes[23] = 1;

        let err = PageHeader::decode(&bytes).unwrap_err();

        assert!(matches!(err, PageError::Malformed("reserved header byte")));
    }

    #[test]
    fn insert_reuses_unused_slot_after_compact() {
        let mut page = Page::new_heap();
        let s0 = page.insert_tuple(b"first").unwrap();
        let _s1 = page.insert_tuple(b"second").unwrap();
        page.delete_tuple(s0).unwrap();
        page.compact().unwrap();
        // The unused slot at index 0 should be reused on the next insert.
        let s_new = page.insert_tuple(b"new").unwrap();
        assert_eq!(s_new, 0);
        assert_eq!(page.read_tuple(s_new).unwrap(), b"new");
    }

    #[test]
    fn compact_rejects_malformed_live_tuple_range() {
        let mut page = Page::new_heap();
        let slot = page.insert_tuple(b"ok").unwrap();
        let item_off = Page::item_id_offset(slot);
        let malformed = ItemId::new(8_000, 500, ItemIdFlags::Normal);
        write_u32_le(
            &mut page.as_bytes_mut()[item_off..item_off + ITEMID_SIZE],
            malformed.into_raw(),
        );

        let err = page.compact().unwrap_err();

        assert!(matches!(
            err,
            PageError::Malformed("tuple range out of bounds")
        ));
    }

    #[test]
    fn read_tuple_rejects_malformed_live_tuple_range() {
        let mut page = Page::new_heap();
        let slot = page.insert_tuple(b"ok").unwrap();
        let item_off = Page::item_id_offset(slot);
        let malformed = ItemId::new(8_000, 500, ItemIdFlags::Normal);
        write_u32_le(
            &mut page.as_bytes_mut()[item_off..item_off + ITEMID_SIZE],
            malformed.into_raw(),
        );

        let err = page.read_tuple(slot).unwrap_err();

        assert!(matches!(
            err,
            PageError::Malformed("tuple range out of bounds")
        ));
    }
}
