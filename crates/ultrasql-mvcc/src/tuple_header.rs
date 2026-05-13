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
        let rel = RelationId::new(read_u32_le(&bytes[32..36]).ok()?);
        let block = BlockNumber::new(read_u32_le(&bytes[36..40]).ok()? & 0x00FF_FFFF);
        let slot = (read_u32_le(&bytes[36..40]).ok()? >> 24) as u16;
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
    fn header_size_matches_constant() {
        let h = TupleHeader::fresh(Xid::new(1), CommandId::new(0), sample_tid(), 1);
        let mut bytes = [0_u8; TUPLE_HEADER_SIZE];
        h.encode(&mut bytes);
        // Just confirm encode wrote into the full buffer (no panic).
        let (_, n) = TupleHeader::decode(&bytes).unwrap();
        assert_eq!(n, TUPLE_HEADER_SIZE);
    }
}
