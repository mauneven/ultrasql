//! MVCC tuple header.
//!
//! The header sits at the start of every heap tuple and records the
//! transaction lifecycle bookkeeping needed for visibility, locking,
//! and HOT chains.
//!
//! ```text
//!  0       8       16      20      24      26      28      30      32 (bytes)
//!  ┌───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┐
//!  │ xmin  │ xmax  │  cmin │  cmax │ flags │ n_atts│ off   │ ctid  │
//!  │ (u64) │ (u64) │ (u32) │ (u32) │ (u16) │ (u16) │ (u16) │ (8)   │
//!  └───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┘
//! ```
//!
//! `ctid` is the redirect target used by HOT update chains: when a
//! tuple is updated in place, the original tuple stays at its slot and
//! its `ctid` points at the new version's `TupleId`.

use ultrasql_core::endian::{
    read_u16_le, read_u32_le, read_u64_le, write_u16_le, write_u32_le, write_u64_le,
};
use ultrasql_core::{BlockNumber, CommandId, PageId, RelationId, TupleId, Xid};

/// Size in bytes of a `TupleHeader` on disk.
pub const TUPLE_HEADER_SIZE: usize = 40;

/// Information mask bits.
///
/// Layout matches PostgreSQL's `t_infomask` semantics for the bits we
/// implement. Unused bits are reserved.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InfoMask(u16);

impl InfoMask {
    /// Tuple has at least one NULL attribute.
    pub const HAS_NULL: u16 = 1 << 0;
    /// Tuple has at least one variable-length attribute.
    pub const HAS_VARWIDTH: u16 = 1 << 1;
    /// `xmin` is known committed (cached from the CLOG).
    pub const XMIN_COMMITTED: u16 = 1 << 2;
    /// `xmin` is known invalid (cached aborted state).
    pub const XMIN_INVALID: u16 = 1 << 3;
    /// `xmax` is known committed.
    pub const XMAX_COMMITTED: u16 = 1 << 4;
    /// `xmax` is known invalid (cached aborted state).
    pub const XMAX_INVALID: u16 = 1 << 5;
    /// Tuple is an updated version — the prior version's `ctid`
    /// pointed here.
    pub const UPDATED: u16 = 1 << 6;
    /// Tuple is part of a HOT update chain that does not modify any
    /// indexed columns; index scans may follow the chain in place.
    pub const HOT_UPDATED: u16 = 1 << 7;
    /// Tuple has been frozen by vacuum. `xmin` is no longer
    /// authoritative; treat the tuple as visible to all snapshots.
    pub const FROZEN: u16 = 1 << 8;
    /// Tuple was written by a subtransaction (savepoint).
    ///
    /// When this bit is set, `xmin` identifies the subtransaction XID
    /// rather than the top-level transaction XID. Visibility rules must
    /// consult the subtransaction rollback set: a subtransaction whose
    /// XID appears in the rollback set is treated as aborted, making
    /// its tuples invisible even to the parent transaction.
    pub const SUBXACT: u16 = 1 << 9;

    /// Tuple was updated **in place** — the slot's payload bytes hold
    /// the *post-update* version, and a side-channel undo log carries
    /// the *pre-update* payload keyed by the same `TupleId`.
    ///
    /// When this bit is set together with a non-`INVALID` `xmax`,
    /// `is_visible` still classifies visibility using the standard
    /// rules, but scan paths must consult the undo log to recover the
    /// pre-update payload when the reader's snapshot does not yet see
    /// `xmax` as committed. Tuples without this bit follow the
    /// classical PostgreSQL contract: a non-`INVALID` `xmax` means the
    /// tuple has been deleted (or moved to a new `ctid` via the
    /// out-of-place new-version path), and the slot payload is the
    /// pre-update / pre-delete state.
    pub const UPDATED_IN_PLACE: u16 = 1 << 10;

    /// Wrap an existing 16-bit mask.
    #[must_use]
    pub const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    /// Raw 16-bit value.
    #[must_use]
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Check whether a flag is set.
    #[must_use]
    pub const fn contains(self, flag: u16) -> bool {
        (self.0 & flag) == flag
    }

    /// Set one or more flags.
    pub const fn set(&mut self, flag: u16) {
        self.0 |= flag;
    }

    /// Clear one or more flags.
    pub const fn clear(&mut self, flag: u16) {
        self.0 &= !flag;
    }

    /// `true` iff [`Self::FROZEN`] is set.
    #[must_use]
    pub const fn is_frozen(self) -> bool {
        self.contains(Self::FROZEN)
    }

    /// `true` iff [`Self::SUBXACT`] is set.
    ///
    /// When `true`, `xmin` in the owning [`TupleHeader`] is a
    /// subtransaction XID. Callers must check whether that subtransaction
    /// has been rolled back before treating the tuple as visible.
    #[must_use]
    pub const fn is_subxact(self) -> bool {
        self.contains(Self::SUBXACT)
    }
}

/// MVCC tuple header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TupleHeader {
    /// Inserter's XID.
    pub xmin: Xid,
    /// Deleter / updater's XID. `Xid::INVALID` if alive.
    pub xmax: Xid,
    /// Inserter's command id within its transaction.
    pub cmin: CommandId,
    /// Deleter's command id within its transaction. Defined only when
    /// `xmax` is the same transaction as `xmin`.
    pub cmax: CommandId,
    /// Lifecycle flag bits.
    pub infomask: InfoMask,
    /// Number of attributes the tuple physically stores. The catalog
    /// may declare more; missing trailing attributes default per
    /// `pg_attribute.attmissingval` (TODO).
    pub n_atts: u16,
    /// Byte offset of the first attribute relative to the header
    /// start. 0 means "use natural alignment + bitmap presence";
    /// non-zero means "the offset has been pre-resolved (e.g. by a
    /// fast-path build)."
    pub data_offset: u16,
    /// HOT-chain redirect: the next version of this tuple. Equals
    /// the tuple's own [`TupleId`] for terminal versions.
    pub ctid: TupleId,
}

impl TupleHeader {
    /// Build a partial header carrying only the fields needed by
    /// callers who already know the tuple is MVCC-visible via a
    /// cached `(xmin, infomask_bits)` hit — typically the
    /// `HeapAccess::for_each_visible`
    /// (in `ultrasql-storage`) hot path. Other fields are
    /// best-effort sentinels: `xmax` is `INVALID` (the cache key
    /// implied `xmax == 0`), `cmin`/`cmax`/`n_atts`/`data_offset`
    /// are zero, and `ctid` points at slot 0 of relation 0.
    ///
    /// Callers must not read any field other than `xmin` /
    /// `xmax` / `infomask` from the returned header — every other
    /// field is intentionally bogus. The minimal header exists
    /// purely to keep the existing `FnMut(tid, header, payload)`
    /// callback contract intact without paying the full
    /// 40-byte [`Self::decode`] when the fast-path
    /// visibility cache already says "visible".
    #[must_use]
    #[inline]
    pub const fn minimal_for_visible_cache_hit(xmin: Xid, infomask_bits: u16) -> Self {
        Self {
            xmin,
            xmax: Xid::INVALID,
            cmin: CommandId::FIRST,
            cmax: CommandId::FIRST,
            infomask: InfoMask::from_bits(infomask_bits),
            n_atts: 0,
            data_offset: 0,
            ctid: TupleId::new(PageId::new(RelationId::new(0), BlockNumber::new(0)), 0),
        }
    }

    /// Build a fresh header for a freshly-inserted tuple at the given
    /// position.
    #[must_use]
    pub fn fresh(xmin: Xid, cmin: CommandId, tid: TupleId, n_atts: u16) -> Self {
        Self {
            xmin,
            xmax: Xid::INVALID,
            cmin,
            cmax: CommandId::FIRST,
            infomask: InfoMask::default(),
            n_atts,
            data_offset: 0,
            ctid: tid,
        }
    }

    /// Decode a header from a byte slice. Returns the header and the
    /// number of bytes consumed (always [`TUPLE_HEADER_SIZE`]).
    pub fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        if bytes.len() < TUPLE_HEADER_SIZE {
            return None;
        }
        let xmin = Xid::new(read_u64_le(&bytes[0..8]).ok()?);
        let xmax = Xid::new(read_u64_le(&bytes[8..16]).ok()?);
        let cmin = CommandId::new(read_u32_le(&bytes[16..20]).ok()?);
        let cmax = CommandId::new(read_u32_le(&bytes[20..24]).ok()?);
        let infomask = InfoMask::from_bits(read_u16_le(&bytes[24..26]).ok()?);
        let n_atts = read_u16_le(&bytes[26..28]).ok()?;
        let data_offset = read_u16_le(&bytes[28..30]).ok()?;
        // 2 bytes of padding at 30..32 reserved.
        if read_u16_le(&bytes[30..32]).ok()? != 0 {
            return None;
        }
        let rel = RelationId::new(read_u32_le(&bytes[32..36]).ok()?);
        let block_and_slot = read_u32_le(&bytes[36..40]).ok()?;
        let block = BlockNumber::new(block_and_slot & 0x00FF_FFFF);
        let slot = u16::try_from(block_and_slot >> 24).ok()?;
        let ctid = TupleId::new(PageId::new(rel, block), slot);
        Some((
            Self {
                xmin,
                xmax,
                cmin,
                cmax,
                infomask,
                n_atts,
                data_offset,
                ctid,
            },
            TUPLE_HEADER_SIZE,
        ))
    }

    /// Encode this header into the first [`TUPLE_HEADER_SIZE`] bytes
    /// of `bytes`.
    pub fn encode(&self, bytes: &mut [u8]) {
        write_u64_le(&mut bytes[0..8], self.xmin.raw());
        write_u64_le(&mut bytes[8..16], self.xmax.raw());
        write_u32_le(&mut bytes[16..20], self.cmin.raw());
        write_u32_le(&mut bytes[20..24], self.cmax.raw());
        write_u16_le(&mut bytes[24..26], self.infomask.bits());
        write_u16_le(&mut bytes[26..28], self.n_atts);
        write_u16_le(&mut bytes[28..30], self.data_offset);
        write_u16_le(&mut bytes[30..32], 0); // reserved
        write_u32_le(&mut bytes[32..36], self.ctid.page.relation.0.raw());
        let block = self.ctid.page.block.raw() & 0x00FF_FFFF;
        let slot = u32::from(self.ctid.slot) << 24;
        write_u32_le(&mut bytes[36..40], block | slot);
    }

    /// `true` if the tuple is alive (not yet deleted).
    #[must_use]
    pub const fn is_alive(&self) -> bool {
        self.xmax.is_invalid()
    }

    /// `true` if this tuple was written by a subtransaction.
    ///
    /// When `true`, callers should verify that `self.xmin` is not in the
    /// rolled-back subtransaction set before treating the tuple as visible.
    #[must_use]
    pub const fn is_subxact(&self) -> bool {
        self.infomask.is_subxact()
    }

    /// Mark this tuple deleted by `xmax` at command `cmax`.
    pub const fn mark_deleted(&mut self, xmax: Xid, cmax: CommandId) {
        self.xmax = xmax;
        self.cmax = cmax;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_tid() -> TupleId {
        TupleId::new(PageId::new(RelationId::new(7), BlockNumber::new(42)), 13)
    }

    #[test]
    fn round_trip_via_encode_decode() {
        let mut h = TupleHeader::fresh(Xid::new(100), CommandId::new(0), sample_tid(), 5);
        h.infomask.set(InfoMask::HAS_NULL | InfoMask::HAS_VARWIDTH);
        h.mark_deleted(Xid::new(101), CommandId::new(1));

        let mut bytes = [0_u8; TUPLE_HEADER_SIZE];
        h.encode(&mut bytes);
        let (decoded, n) = TupleHeader::decode(&bytes).unwrap();
        assert_eq!(n, TUPLE_HEADER_SIZE);
        assert_eq!(decoded, h);
    }

    #[test]
    fn decode_preserves_max_packed_ctid_slot() {
        let tid = TupleId::new(
            PageId::new(RelationId::new(7), BlockNumber::new(0x00FF_FFFF)),
            u16::from(u8::MAX),
        );
        let h = TupleHeader::fresh(Xid::new(100), CommandId::new(0), tid, 5);

        let mut bytes = [0_u8; TUPLE_HEADER_SIZE];
        h.encode(&mut bytes);
        let (decoded, _) = TupleHeader::decode(&bytes).unwrap();

        assert_eq!(decoded.ctid, tid);
    }

    #[test]
    fn fresh_tuple_is_alive_until_marked() {
        let h = TupleHeader::fresh(Xid::new(1), CommandId::new(0), sample_tid(), 1);
        assert!(h.is_alive());
        let mut h = h;
        h.mark_deleted(Xid::new(2), CommandId::new(3));
        assert!(!h.is_alive());
        assert_eq!(h.cmax, CommandId::new(3));
    }

    #[test]
    fn infomask_set_clear_contains() {
        let mut m = InfoMask::default();
        assert!(!m.contains(InfoMask::HAS_NULL));
        m.set(InfoMask::HAS_NULL);
        assert!(m.contains(InfoMask::HAS_NULL));
        m.clear(InfoMask::HAS_NULL);
        assert!(!m.contains(InfoMask::HAS_NULL));
    }

    #[test]
    fn frozen_helper() {
        let mut m = InfoMask::default();
        assert!(!m.is_frozen());
        m.set(InfoMask::FROZEN);
        assert!(m.is_frozen());
    }

    #[test]
    fn decode_rejects_short_input() {
        let bytes = [0_u8; TUPLE_HEADER_SIZE - 1];
        assert!(TupleHeader::decode(&bytes).is_none());
    }

    #[test]
    fn decode_rejects_reserved_padding_bits() {
        let h = TupleHeader::fresh(Xid::new(1), CommandId::new(0), sample_tid(), 1);
        let mut bytes = [0_u8; TUPLE_HEADER_SIZE];
        h.encode(&mut bytes);
        bytes[30] = 1;
        assert!(TupleHeader::decode(&bytes).is_none());
    }

    #[test]
    fn header_size_matches_constant() {
        let h = TupleHeader::fresh(Xid::new(1), CommandId::new(0), sample_tid(), 1);
        let mut bytes = [0_u8; TUPLE_HEADER_SIZE];
        h.encode(&mut bytes);
        // Just confirm encode wrote into the full buffer (no panic).
        let (_, n) = TupleHeader::decode(&bytes).unwrap();
        assert_eq!(n, TUPLE_HEADER_SIZE);
    }
}
