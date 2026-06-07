//! Primitive identifiers used across every UltraSQL subsystem.
//!
//! The naming follows PostgreSQL idiom where it does not invite
//! confusion; the widths are chosen to avoid PostgreSQL's wraparound
//! and overflow problems.
//!
//! - [`Oid`] (32-bit) — catalog-wide object identifier.
//! - [`Xid`] (64-bit) — transaction identifier. UltraSQL uses 64 bits
//!   precisely so vacuum-for-wraparound is not a thing.
//! - [`Lsn`] (64-bit) — write-ahead-log sequence number.
//! - [`CommandId`] (32-bit) — sub-transaction command counter.
//! - [`BlockNumber`] (u32), [`SegmentId`] (u32), [`PageId`] (composite),
//!   [`TupleId`] (composite).

use std::fmt;

use crate::constants::PAGE_SIZE_LOG2;

/// Catalog-wide object identifier.
///
/// `Oid` values are stable for the life of the object. They are *not*
/// recycled when an object is dropped (the catalog tombstone retains
/// the OID so cross-references remain unambiguous).
///
/// Zero is reserved as `INVALID_OID`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Oid(u32);

impl Oid {
    /// Sentinel "no object" OID. Stored where an OID may be absent.
    pub const INVALID: Self = Self(0);

    /// Construct an `Oid` from a raw `u32`.
    #[inline]
    #[must_use]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Underlying integer representation.
    #[inline]
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0
    }

    /// Whether this OID is the invalid sentinel.
    #[inline]
    #[must_use]
    pub const fn is_invalid(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Debug for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Oid({})", self.0)
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for Oid {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<Oid> for u32 {
    fn from(value: Oid) -> Self {
        value.0
    }
}

/// Relation identifier. Distinct alias of [`Oid`] for stronger typing on
/// signatures that take "the table OID, not just any OID."
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct RelationId(pub Oid);

impl RelationId {
    /// Invalid sentinel.
    pub const INVALID: Self = Self(Oid::INVALID);

    /// Construct from a raw `u32`.
    #[inline]
    #[must_use]
    pub const fn new(raw: u32) -> Self {
        Self(Oid::new(raw))
    }

    /// Underlying [`Oid`].
    #[inline]
    #[must_use]
    pub const fn oid(self) -> Oid {
        self.0
    }
}

impl fmt::Debug for RelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RelationId({})", self.0.raw())
    }
}

impl fmt::Display for RelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Convenience alias used at API boundaries where the "table"
/// interpretation is the meaningful one.
pub type TableId = RelationId;

/// Transaction identifier.
///
/// 64 bits to avoid PostgreSQL's 32-bit wraparound. The system reserves
/// the bottom three values:
/// - `0` — invalid sentinel.
/// - `1` — bootstrap transaction (catalog initialization).
/// - `2` — frozen transaction (tuples visible to every snapshot).
///
/// User transactions allocate values starting at `Xid::FIRST_USER`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Xid(u64);

impl Xid {
    /// Invalid sentinel. Used in `xmax` to mean "never deleted."
    pub const INVALID: Self = Self(0);

    /// The catalog bootstrap transaction.
    pub const BOOTSTRAP: Self = Self(1);

    /// Tuples carrying this XID are visible to every snapshot. Vacuum
    /// rewrites old tuples' `xmin` to this value once they become
    /// universally visible.
    pub const FROZEN: Self = Self(2);

    /// First XID handed out to a user transaction.
    pub const FIRST_USER: Self = Self(3);

    /// Construct from a raw integer.
    #[inline]
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Underlying integer representation.
    #[inline]
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Whether this XID is the invalid sentinel.
    #[inline]
    #[must_use]
    pub const fn is_invalid(self) -> bool {
        self.0 == 0
    }

    /// Whether this XID names a real, post-bootstrap transaction.
    #[inline]
    #[must_use]
    pub const fn is_normal(self) -> bool {
        self.0 >= Self::FIRST_USER.0
    }

    /// Successor XID. Wraps at `u64::MAX` (which we never reach in
    /// practice: at one transaction per nanosecond it takes ~584 years
    /// to exhaust).
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

impl fmt::Debug for Xid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Xid({})", self.0)
    }
}

impl fmt::Display for Xid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Log sequence number — a monotonically increasing 64-bit position in
/// the WAL. We follow PostgreSQL's convention: an LSN is conceptually a
/// byte offset into the (concatenated) WAL stream.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct Lsn(u64);

impl Lsn {
    /// "Before any record" sentinel.
    pub const ZERO: Self = Self(0);

    /// Construct an LSN from a raw integer.
    #[inline]
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Underlying integer representation.
    #[inline]
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Advance by a number of bytes.
    #[inline]
    #[must_use]
    pub const fn advance(self, bytes: u64) -> Self {
        Self(self.0 + bytes)
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // PostgreSQL prints LSNs as `HHHHHHHH/LLLLLLLL`. Match that.
        // The low 32 bits are extracted by masking, not by narrowing —
        // `0xFFFF_FFFF` is the documented low-half projection, not a
        // bit-width violation.
        write!(f, "Lsn({:X}/{:08X})", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:08X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

/// Per-transaction command identifier. Distinguishes statements within a
/// single transaction so a statement's own writes are visible to its own
/// later reads but not to its earlier reads.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct CommandId(u32);

impl CommandId {
    /// First command in a transaction.
    pub const FIRST: Self = Self(0);

    /// Construct a command id from a raw integer.
    #[inline]
    #[must_use]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Underlying integer representation.
    #[inline]
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0
    }

    /// Successor command id. Saturates at `u32::MAX`.
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Debug for CommandId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cmd({})", self.0)
    }
}

impl fmt::Display for CommandId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Block number within a relation's storage. One block is one page.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct BlockNumber(u32);

impl BlockNumber {
    /// Invalid sentinel.
    pub const INVALID: Self = Self(u32::MAX);

    /// Construct from a raw integer.
    #[inline]
    #[must_use]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Underlying integer.
    #[inline]
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0
    }

    /// Byte offset of this block in its segment, given the constant
    /// page size.
    #[inline]
    #[must_use]
    pub const fn byte_offset(self) -> u64 {
        u32_to_u64_const(self.0) << PAGE_SIZE_LOG2
    }
}

const fn u32_to_u64_const(value: u32) -> u64 {
    let [b0, b1, b2, b3] = value.to_le_bytes();
    u64::from_le_bytes([b0, b1, b2, b3, 0, 0, 0, 0])
}

impl fmt::Debug for BlockNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Block({})", self.0)
    }
}

/// Segment file identifier. A relation's storage is sharded into 1 GiB
/// segment files indexed by `SegmentId`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct SegmentId(u32);

impl SegmentId {
    /// First segment.
    pub const ZERO: Self = Self(0);

    /// Construct from a raw integer.
    #[inline]
    #[must_use]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Underlying integer.
    #[inline]
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

impl fmt::Debug for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Seg({})", self.0)
    }
}

/// A composite identifier for a page on disk: the relation that owns it
/// and its block number within that relation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct PageId {
    /// The relation that owns this page.
    pub relation: RelationId,
    /// Block number within the relation.
    pub block: BlockNumber,
}

impl PageId {
    /// Construct a `PageId`.
    #[inline]
    #[must_use]
    pub const fn new(relation: RelationId, block: BlockNumber) -> Self {
        Self { relation, block }
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.relation.0.raw(), self.block.0)
    }
}

/// A tuple identifier: the page that holds it and the slot index within
/// that page.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct TupleId {
    /// The page holding this tuple.
    pub page: PageId,
    /// Zero-based slot index within the page.
    pub slot: u16,
}

impl TupleId {
    /// Construct a `TupleId`.
    #[inline]
    #[must_use]
    pub const fn new(page: PageId, slot: u16) -> Self {
        Self { page, slot }
    }
}

impl fmt::Display for TupleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.page, self.slot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oid_sentinel_and_round_trip() {
        assert!(Oid::INVALID.is_invalid());
        let a = Oid::new(42);
        assert_eq!(a.raw(), 42);
        assert_eq!(u32::from(a), 42);
        let b: Oid = 42_u32.into();
        assert_eq!(a, b);
    }

    #[test]
    fn xid_progression() {
        assert!(Xid::INVALID.is_invalid());
        assert!(!Xid::BOOTSTRAP.is_invalid());
        assert!(!Xid::BOOTSTRAP.is_normal());
        assert!(!Xid::FROZEN.is_normal());
        assert!(Xid::FIRST_USER.is_normal());
        assert_eq!(Xid::new(10).next(), Xid::new(11));
    }

    #[test]
    fn lsn_display_matches_postgres_format() {
        let lsn = Lsn::new(0x1234_5678_9ABC_DEF0);
        let s = format!("{lsn}");
        assert_eq!(s, "12345678/9ABCDEF0");
    }

    #[test]
    fn lsn_advance() {
        let l = Lsn::new(1000);
        assert_eq!(l.advance(24).raw(), 1024);
    }

    #[test]
    fn page_id_display() {
        let rel = RelationId::new(7);
        let blk = BlockNumber::new(13);
        let pid = PageId::new(rel, blk);
        assert_eq!(format!("{pid}"), "7/13");
    }

    #[test]
    fn tuple_id_display() {
        let tid = TupleId::new(PageId::new(RelationId::new(7), BlockNumber::new(13)), 5);
        assert_eq!(format!("{tid}"), "7/13:5");
    }

    #[test]
    fn block_number_byte_offset() {
        let b = BlockNumber::new(3);
        assert_eq!(b.byte_offset(), 3 * 8192);
    }

    #[test]
    fn command_id_saturates() {
        let m = CommandId::new(u32::MAX);
        assert_eq!(m.next().raw(), u32::MAX);
    }

    #[test]
    fn ord_and_hash_work() {
        use std::collections::HashSet;
        let mut s = HashSet::new();
        assert!(s.insert(Oid::new(1)));
        assert!(!s.insert(Oid::new(1)));
        assert!(Oid::new(1) < Oid::new(2));
    }
}
