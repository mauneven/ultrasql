//! Tests for the typed WAL payload codecs.
//!
//! Helpers live here and are shared with the per-topic test submodules.

pub(crate) use proptest::prelude::*;
pub(crate) use ultrasql_core::constants::PAGE_SIZE;
pub(crate) use ultrasql_core::{BlockNumber, CommandId, Lsn, PageId, RelationId, TupleId, Xid};

pub(crate) use super::*;

mod btree;
mod general;
mod hash;
mod roundtrip;
mod sequence;
mod vector;

// ── shared helpers ────────────────────────────────────────────────────────

    // ── helpers ───────────────────────────────────────────────────────────

    pub(crate) fn tid(rel: u32, block: u32, slot: u16) -> TupleId {
        TupleId::new(
            PageId::new(RelationId::new(rel), BlockNumber::new(block)),
            slot,
        )
    }

    pub(crate) fn page_id(rel: u32, block: u32) -> PageId {
        PageId::new(RelationId::new(rel), BlockNumber::new(block))
    }

    pub(crate) fn full_page() -> Vec<u8> {
        vec![0xAB_u8; PAGE_SIZE]
    }

    pub(crate) fn finite_f32_vec(max_len: usize) -> impl Strategy<Value = Vec<f32>> {
        proptest::collection::vec(-10_000_i16..=10_000_i16, 0..max_len)
            .prop_map(|values| values.into_iter().map(f32::from).collect())
    }

    pub(crate) fn assert_trailing_rejected<T: std::fmt::Debug>(
        mut bytes: Vec<u8>,
        decode: impl FnOnce(&[u8]) -> Result<T, PayloadError>,
    ) {
        let expected = bytes.len();
        bytes.push(0x5a);
        let err = decode(&bytes).expect_err("trailing bytes must fail");
        assert!(matches!(
            err,
            PayloadError::Trailing {
                expected: got_expected,
                have,
            } if got_expected == expected && have == expected + 1
        ));
    }

    pub(crate) fn assert_reserved_rejected<T: std::fmt::Debug>(
        mut bytes: Vec<u8>,
        offset: usize,
        decode: impl FnOnce(&[u8]) -> Result<T, PayloadError>,
    ) {
        bytes[offset] = 0x01;
        let err = decode(&bytes).expect_err("reserved bytes must fail");
        assert!(matches!(err, PayloadError::Malformed(_)));
    }
