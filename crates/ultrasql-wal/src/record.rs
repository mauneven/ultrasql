//! WAL record format.
//!
//! ```text
//!   offset  size  field
//!   ------  ----  -----
//!     0       4   total_length (u32) — bytes including this header
//!     4       4   crc32c (u32) — over the entire record with this slot zeroed
//!     8       8   prev_lsn (u64) — previous record from the same transaction
//!    16       8   xid (u64) — owning transaction
//!    24       1   record_type (u8)
//!    25       1   flags (u8)
//!    26       2   reserved
//!    28      ..   payload
//! ```
//!
//! The CRC covers the entire record with its own 4-byte slot treated
//! as zero, so the check is self-consistent regardless of stale CRC
//! state. This is the same convention the page checksum uses.

use ultrasql_core::endian::{read_u32_le, read_u64_le, write_u32_le, write_u64_le};
use ultrasql_core::{Lsn, Xid};

const TOTAL_LEN_OFFSET: usize = 0;
const CRC_OFFSET: usize = 4;
const PREV_LSN_OFFSET: usize = 8;
const XID_OFFSET: usize = 16;
const RTYPE_OFFSET: usize = 24;
const FLAGS_OFFSET: usize = 25;

/// Size of [`WalRecordHeader`] in bytes.
pub const RECORD_HEADER_SIZE: usize = 28;
/// Size of [`WalRecordHeader`] in bytes as encoded in the `u32` length field.
pub const RECORD_HEADER_SIZE_U32: u32 = 28;

/// Hard ceiling on a single record's encoded size.
///
/// Defends recovery against a corrupted or maliciously crafted segment
/// file that claims a record `total_length` of `u32::MAX`: without this
/// bound, the decoder would allocate gigabyte-class buffers (one for
/// the payload copy, one for the CRC re-encode) before the CRC even
/// gets checked.
///
/// 64 MiB is comfortably above every legitimate record format used
/// today (the widest is `FullPageWrite` carrying an 8 KiB page plus
/// header overhead). Future record types that legitimately need more
/// must update this constant explicitly.
pub const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

// Compile-time sanity.
const _: () = assert!(RECORD_HEADER_SIZE > FLAGS_OFFSET);
const _: () = assert!(RECORD_HEADER_SIZE % 4 == 0);

/// Errors that can arise when serializing or parsing WAL records.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WalRecordError {
    /// Buffer is shorter than the record's declared total length.
    #[error("wal record truncated: need {needed}, have {have}")]
    Truncated {
        /// Bytes required.
        needed: usize,
        /// Bytes available.
        have: usize,
    },

    /// The header's `total_length` field is malformed (smaller than
    /// the header, or larger than the supplied buffer).
    #[error("wal record malformed: {0}")]
    Malformed(&'static str),

    /// The encoded record would exceed the supported WAL record ceiling.
    #[error("wal record too large: payload {payload_len} bytes exceeds max {max_payload_len}")]
    TooLarge {
        /// Payload bytes supplied by the caller.
        payload_len: usize,
        /// Largest payload accepted by the record encoder.
        max_payload_len: usize,
    },

    /// The record's CRC does not match the recomputed value.
    #[error("wal crc mismatch: expected {expected:08x}, got {actual:08x}")]
    CrcMismatch {
        /// CRC stored in the record.
        expected: u32,
        /// CRC recomputed from the bytes.
        actual: u32,
    },

    /// The record type byte is unknown.
    #[error("wal unknown record type: {0}")]
    UnknownType(u8),
}

/// Type of a WAL record. The numeric values are part of the on-disk
/// format and must remain stable across releases (additions only).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// A tuple was inserted into a relation.
    HeapInsert = 1,
    /// A tuple was updated in place. The payload carries before/after.
    HeapUpdate = 2,
    /// A tuple was deleted (logically). The payload carries the
    /// affected slot identifier.
    HeapDelete = 3,
    /// An entire page was overwritten — used by full-page writes
    /// after a torn-write risk window.
    FullPageWrite = 4,
    /// A transaction committed.
    Commit = 5,
    /// A transaction aborted.
    Abort = 6,
    /// A checkpoint was completed. Recovery skips redo of records with
    /// LSNs earlier than the latest checkpoint's redo-from LSN.
    Checkpoint = 7,
    /// A B+ tree index page was modified.
    BTreeOp = 8,
    /// A tuple was updated **in place** — the slot's payload bytes
    /// were rewritten by an `update_int32_pair_inplace_undo`-style
    /// path. The payload carries both the pre-image and the
    /// post-image so recovery can rebuild both the page bytes and
    /// the in-memory `UndoRelationLog` entry.
    HeapUpdateInPlace = 9,
    /// A tuple was deleted **in place** via the single-pass
    /// `delete_int32_pair_inplace` path. Equivalent semantics to
    /// `HeapDelete` but the record carries enough metadata for
    /// recovery to stamp the source slot's `xmax`/`cmax` and clear
    /// the `UPDATED_IN_PLACE` bit if previously set.
    HeapDeleteInPlace = 10,
    /// A sequence's durable state changed.
    SequenceOp = 11,
    /// A hash-index bucket or overflow page was modified.
    HashOp = 12,
    /// An HNSW vector-index graph mutation was recorded.
    HnswOp = 13,
    /// An IVFFlat vector-index list mutation was recorded.
    IvfFlatOp = 14,
    /// Multiple in-place tuple payload rewrites on one heap page.
    HeapUpdateInPlaceBatch = 15,
    /// A no-op marker (used to round records up to alignment
    /// boundaries; ignored on replay).
    Nop = 255,
}

impl RecordType {
    /// Parse a record-type byte.
    pub const fn from_u8(v: u8) -> Result<Self, WalRecordError> {
        Ok(match v {
            1 => Self::HeapInsert,
            2 => Self::HeapUpdate,
            3 => Self::HeapDelete,
            4 => Self::FullPageWrite,
            5 => Self::Commit,
            6 => Self::Abort,
            7 => Self::Checkpoint,
            8 => Self::BTreeOp,
            9 => Self::HeapUpdateInPlace,
            10 => Self::HeapDeleteInPlace,
            11 => Self::SequenceOp,
            12 => Self::HashOp,
            13 => Self::HnswOp,
            14 => Self::IvfFlatOp,
            15 => Self::HeapUpdateInPlaceBatch,
            255 => Self::Nop,
            other => return Err(WalRecordError::UnknownType(other)),
        })
    }
}

/// Decoded WAL record header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalRecordHeader {
    /// Total length of the record in bytes, including this header.
    pub total_length: u32,
    /// CRC32C over the entire record with its CRC slot treated as
    /// zero.
    pub crc: u32,
    /// LSN of the previous record written by the same transaction. A
    /// linked list per transaction lets abort processing skip records
    /// for unrelated XIDs.
    pub prev_lsn: Lsn,
    /// Owning transaction.
    pub xid: Xid,
    /// Record type.
    pub record_type: RecordType,
    /// Reserved flag bits; per-record-type semantics.
    pub flags: u8,
}

/// A complete WAL record: header plus typed payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalRecord {
    /// Decoded header.
    pub header: WalRecordHeader,
    /// Opaque payload. Interpretation depends on
    /// `header.record_type`. Payload-typed crates downstream of WAL
    /// know how to parse it.
    pub payload: Vec<u8>,
}

impl WalRecord {
    /// Construct a new record. The CRC is computed automatically.
    pub fn new(
        record_type: RecordType,
        xid: Xid,
        prev_lsn: Lsn,
        flags: u8,
        payload: Vec<u8>,
    ) -> Result<Self, WalRecordError> {
        let total_len = RECORD_HEADER_SIZE
            .checked_add(payload.len())
            .ok_or(WalRecordError::Malformed("total_length overflow"))?;
        if total_len > MAX_RECORD_BYTES {
            return Err(WalRecordError::TooLarge {
                payload_len: payload.len(),
                max_payload_len: MAX_RECORD_BYTES - RECORD_HEADER_SIZE,
            });
        }
        let total = u32::try_from(total_len)
            .map_err(|_| WalRecordError::Malformed("total_length overflow"))?;
        let mut header = WalRecordHeader {
            total_length: total,
            crc: 0,
            prev_lsn,
            xid,
            record_type,
            flags,
        };
        header.crc = compute_record_crc(&header, &payload);
        Ok(Self { header, payload })
    }

    /// Encode the record into a freshly-allocated `Vec<u8>`.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let total = trusted_total_length_to_usize(self.header.total_length);
        let mut out = vec![0_u8; total];
        encode_header_into(&self.header, &mut out);
        out[RECORD_HEADER_SIZE..].copy_from_slice(&self.payload);
        out
    }

    /// Decode a record from a byte slice that begins at the record's
    /// header. Returns the decoded record and the byte count consumed.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), WalRecordError> {
        let header = decode_header_from(bytes)?;
        let total = total_length_to_usize(header.total_length)?;
        if total < RECORD_HEADER_SIZE {
            return Err(WalRecordError::Malformed("total_length too small"));
        }
        // Hostile-disk defence: an on-disk record that claims a giant
        // total_length must be refused before we allocate a buffer that
        // big. CRC mismatch is the normal way recovery treats a torn
        // tail, but the attacker controls bytes, so without this bound
        // they could engineer a crafted segment that forces gigabyte
        // allocations BEFORE the CRC check runs.
        if total > MAX_RECORD_BYTES {
            return Err(WalRecordError::Malformed("total_length above ceiling"));
        }
        if bytes.len() < total {
            return Err(WalRecordError::Truncated {
                needed: total,
                have: bytes.len(),
            });
        }
        let payload = bytes[RECORD_HEADER_SIZE..total].to_vec();

        let actual = compute_record_crc(&header, &payload);
        if actual != header.crc {
            return Err(WalRecordError::CrcMismatch {
                expected: header.crc,
                actual,
            });
        }
        Ok((Self { header, payload }, total))
    }
}

fn total_length_to_usize(total_length: u32) -> Result<usize, WalRecordError> {
    usize::try_from(total_length)
        .map_err(|_| WalRecordError::Malformed("total_length does not fit usize"))
}

fn trusted_total_length_to_usize(total_length: u32) -> usize {
    total_length_to_usize(total_length)
        .expect("WAL record total_length must fit usize on supported targets")
}

fn encode_header_into(header: &WalRecordHeader, bytes: &mut [u8]) {
    write_u32_le(
        &mut bytes[TOTAL_LEN_OFFSET..TOTAL_LEN_OFFSET + 4],
        header.total_length,
    );
    write_u32_le(&mut bytes[CRC_OFFSET..CRC_OFFSET + 4], header.crc);
    write_u64_le(
        &mut bytes[PREV_LSN_OFFSET..PREV_LSN_OFFSET + 8],
        header.prev_lsn.raw(),
    );
    write_u64_le(&mut bytes[XID_OFFSET..XID_OFFSET + 8], header.xid.raw());
    bytes[RTYPE_OFFSET] = header.record_type as u8;
    bytes[FLAGS_OFFSET] = header.flags;
    for b in &mut bytes[FLAGS_OFFSET + 1..RECORD_HEADER_SIZE] {
        *b = 0;
    }
}

fn decode_header_from(bytes: &[u8]) -> Result<WalRecordHeader, WalRecordError> {
    if bytes.len() < RECORD_HEADER_SIZE {
        return Err(WalRecordError::Truncated {
            needed: RECORD_HEADER_SIZE,
            have: bytes.len(),
        });
    }
    let total_length = read_u32_le(&bytes[TOTAL_LEN_OFFSET..TOTAL_LEN_OFFSET + 4])
        .map_err(|_| WalRecordError::Malformed("len"))?;
    let crc = read_u32_le(&bytes[CRC_OFFSET..CRC_OFFSET + 4])
        .map_err(|_| WalRecordError::Malformed("crc"))?;
    let prev_lsn = Lsn::new(
        read_u64_le(&bytes[PREV_LSN_OFFSET..PREV_LSN_OFFSET + 8])
            .map_err(|_| WalRecordError::Malformed("prev_lsn"))?,
    );
    let xid = Xid::new(
        read_u64_le(&bytes[XID_OFFSET..XID_OFFSET + 8])
            .map_err(|_| WalRecordError::Malformed("xid"))?,
    );
    let record_type = RecordType::from_u8(bytes[RTYPE_OFFSET])?;
    let flags = bytes[FLAGS_OFFSET];
    Ok(WalRecordHeader {
        total_length,
        crc,
        prev_lsn,
        xid,
        record_type,
        flags,
    })
}

fn compute_record_crc(header: &WalRecordHeader, payload: &[u8]) -> u32 {
    let mut buf = vec![0_u8; RECORD_HEADER_SIZE + payload.len()];
    let header_zeroed_crc = WalRecordHeader { crc: 0, ..*header };
    encode_header_into(&header_zeroed_crc, &mut buf);
    buf[RECORD_HEADER_SIZE..].copy_from_slice(payload);
    crc32c::crc32c(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn rec(rt: RecordType, payload: &[u8]) -> WalRecord {
        WalRecord::new(rt, Xid::new(42), Lsn::new(100), 0, payload.to_vec())
            .expect("test WAL record should fit size limits")
    }

    #[test]
    fn encode_decode_round_trip() {
        let payload = b"INSERT INTO t VALUES (1)".to_vec();
        let record = rec(RecordType::HeapInsert, &payload);
        let bytes = record.encode();
        let (decoded, n) = WalRecord::decode(&bytes).unwrap();
        assert_eq!(n, bytes.len());
        assert_eq!(decoded, record);
    }

    #[test]
    fn crc_detects_payload_bit_flip() {
        let record = rec(RecordType::HeapUpdate, b"abc");
        let mut bytes = record.encode();
        bytes[RECORD_HEADER_SIZE + 1] ^= 0x01;
        let err = WalRecord::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalRecordError::CrcMismatch { .. }));
    }

    #[test]
    fn crc_detects_header_bit_flip() {
        let record = rec(RecordType::Commit, b"");
        let mut bytes = record.encode();
        bytes[XID_OFFSET] ^= 0x01;
        let err = WalRecord::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalRecordError::CrcMismatch { .. }));
    }

    #[test]
    fn truncated_record_rejected() {
        let record = rec(RecordType::HeapInsert, b"hello world");
        let mut bytes = record.encode();
        bytes.truncate(bytes.len() - 1);
        let err = WalRecord::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalRecordError::Truncated { .. }));
    }

    #[test]
    fn header_only_record_round_trips() {
        let record = rec(RecordType::Checkpoint, &[]);
        let bytes = record.encode();
        assert_eq!(bytes.len(), RECORD_HEADER_SIZE);
        let (decoded, n) = WalRecord::decode(&bytes).unwrap();
        assert_eq!(n, RECORD_HEADER_SIZE);
        assert_eq!(decoded, record);
    }

    #[test]
    fn unknown_record_type_rejected() {
        let record = rec(RecordType::HeapInsert, b"x");
        let mut bytes = record.encode();
        bytes[RTYPE_OFFSET] = 99;
        let err = WalRecord::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalRecordError::UnknownType(99)));
    }

    #[test]
    fn record_type_round_trips_through_u8() {
        for &rt in &[
            RecordType::HeapInsert,
            RecordType::HeapUpdate,
            RecordType::HeapDelete,
            RecordType::FullPageWrite,
            RecordType::Commit,
            RecordType::Abort,
            RecordType::Checkpoint,
            RecordType::BTreeOp,
            RecordType::SequenceOp,
            RecordType::HeapUpdateInPlace,
            RecordType::HeapDeleteInPlace,
            RecordType::HashOp,
            RecordType::HnswOp,
            RecordType::IvfFlatOp,
            RecordType::HeapUpdateInPlaceBatch,
            RecordType::Nop,
        ] {
            let raw = rt as u8;
            let parsed = RecordType::from_u8(raw).unwrap();
            assert_eq!(parsed, rt);
        }
    }

    #[test]
    fn many_payload_sizes_round_trip() {
        for &n in &[0_usize, 1, 7, 64, 256, 1024, 4096, 17_000] {
            let payload = (0..n)
                .map(|i| u8::try_from(i & 0xFF).expect("masked to 8 bits"))
                .collect::<Vec<_>>();
            let record = rec(RecordType::HeapInsert, &payload);
            let bytes = record.encode();
            let (decoded, used) = WalRecord::decode(&bytes).unwrap();
            assert_eq!(used, bytes.len());
            assert_eq!(decoded.payload, payload);
        }
    }

    /// Adversarial input: a record header that claims `total_length`
    /// far above any legitimate value must be refused before the
    /// decoder allocates gigabyte-class buffers for the payload copy
    /// or CRC re-encode. Required because recovery treats CRC
    /// mismatch as torn-write — without this bound, a hostile actor
    /// who writes to a WAL file could force the recoverer to OOM
    /// before reaching the CRC check that would otherwise reject the
    /// record.
    #[test]
    fn oversized_total_length_rejected_before_allocation() {
        let mut bytes = vec![0_u8; RECORD_HEADER_SIZE];
        // Encode an attacker-controlled header that points past the
        // ceiling. Use u32::MAX so the test is independent of the
        // exact cap value.
        write_u32_le(&mut bytes[TOTAL_LEN_OFFSET..TOTAL_LEN_OFFSET + 4], u32::MAX);
        write_u32_le(&mut bytes[CRC_OFFSET..CRC_OFFSET + 4], 0xDEAD_BEEF);
        bytes[RTYPE_OFFSET] = RecordType::HeapInsert as u8;
        let err = WalRecord::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalRecordError::Malformed(_)), "got {err:?}");
    }

    /// A header that claims `total_length` exactly one past the
    /// ceiling triggers the same path — pin the boundary so a future
    /// tweak to `MAX_RECORD_BYTES` is caught by this test rather than
    /// silently downgrading the protection.
    #[test]
    fn total_length_just_past_ceiling_rejected() {
        let mut bytes = vec![0_u8; RECORD_HEADER_SIZE];
        let just_past = u32::try_from(MAX_RECORD_BYTES + 1).expect("fits in u32 by construction");
        write_u32_le(
            &mut bytes[TOTAL_LEN_OFFSET..TOTAL_LEN_OFFSET + 4],
            just_past,
        );
        bytes[RTYPE_OFFSET] = RecordType::HeapInsert as u8;
        let err = WalRecord::decode(&bytes).unwrap_err();
        assert!(matches!(err, WalRecordError::Malformed(_)), "got {err:?}");
    }

    #[test]
    fn oversized_constructor_returns_error() {
        let payload = vec![0_u8; MAX_RECORD_BYTES - RECORD_HEADER_SIZE + 1];
        let err =
            WalRecord::new(RecordType::HeapInsert, Xid::new(1), Lsn::ZERO, 0, payload).unwrap_err();
        assert!(
            matches!(err, WalRecordError::TooLarge { .. }),
            "got {err:?}"
        );
    }

    proptest! {
        #[test]
        fn random_wal_bytes_never_panic(
            bytes in proptest::collection::vec(any::<u8>(), 0..4096_usize),
        ) {
            let result = std::panic::catch_unwind(|| WalRecord::decode(&bytes));
            prop_assert!(result.is_ok(), "decoder panicked on random WAL bytes");
        }

        #[test]
        fn mutated_valid_wal_bytes_never_panic(
            payload in proptest::collection::vec(any::<u8>(), 0..1024_usize),
            mutations in proptest::collection::vec((0_usize..2048_usize, any::<u8>()), 0..32_usize),
        ) {
            let mut bytes = rec(RecordType::HeapInsert, &payload).encode();
            for (idx, mask) in mutations {
                let pos = idx % bytes.len();
                bytes[pos] ^= mask;
            }

            let result = std::panic::catch_unwind(|| WalRecord::decode(&bytes));
            prop_assert!(result.is_ok(), "decoder panicked on mutated valid WAL bytes");
            if let Ok(Ok((_record, used))) = result {
                prop_assert!(used <= bytes.len());
            }
        }
    }
}
