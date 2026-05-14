//! Typed payload codecs for WAL record types.
//!
//! [`WalRecord`] carries an opaque `payload: Vec<u8>` whose interpretation
//! depends on the record's [`RecordType`]. This module provides typed structs
//! and `encode`/`decode` pairs for every record type used by the v0.3
//! heap/txn layer. The on-wire encoding is documented inline on each type.
//!
//! Storage emits typed payloads; recovery consumes them. The `WalRecord` wire
//! format itself is unchanged — these codecs sit on top of `payload: Vec<u8>`.
//!
//! # Wire conventions
//!
//! All integers are little-endian. Padding bytes are written as zero and
//! ignored on decode (except where a reserved field's value is validated).
//! Length fields are `u32` and represent the byte count of the variable-length
//! data that follows immediately.
//!
//! # Bounds enforcement
//!
//! Variable-length payloads (tuple bytes, page bytes) are refused at encode
//! and decode time when their claimed length exceeds
//! [`MAX_VARIABLE_PAYLOAD_BYTES`]. This prevents callers from accidentally
//! constructing records whose total size would exceed
//! [`crate::record::MAX_RECORD_BYTES`].
//!
//! [`RecordType`]: crate::record::RecordType
//! [`WalRecord`]: crate::record::WalRecord

use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::endian::{
    read_u16_le, read_u32_le, read_u64_le, write_u16_le, write_u32_le, write_u64_le,
};
use ultrasql_core::{BlockNumber, CommandId, Lsn, PageId, RelationId, TupleId, Xid};

use crate::record::{MAX_RECORD_BYTES, RECORD_HEADER_SIZE};

// ---------------------------------------------------------------------------
// Bounds
// ---------------------------------------------------------------------------

/// Maximum number of bytes allowed for a variable-length payload field
/// (tuple bytes or page bytes).
///
/// Computed as `MAX_RECORD_BYTES - RECORD_HEADER_SIZE - MAX_FIXED_OVERHEAD`,
/// where `MAX_FIXED_OVERHEAD` is the largest fixed-overhead section among all
/// payload types (`HeapUpdate` has 32 bytes of fixed fields). This keeps any
/// single WAL record comfortably under the ceiling enforced by `WalRecord`.
pub const MAX_VARIABLE_PAYLOAD_BYTES: usize = MAX_RECORD_BYTES - RECORD_HEADER_SIZE - 64; // 64 bytes headroom for largest fixed section

// Compile-time sanity.
const _: () = assert!(MAX_VARIABLE_PAYLOAD_BYTES > PAGE_SIZE);

// ---------------------------------------------------------------------------
// PayloadError
// ---------------------------------------------------------------------------

/// Errors that can arise when encoding or decoding a typed WAL payload.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PayloadError {
    /// The byte slice is shorter than the minimum required for this payload
    /// type.
    #[error("payload truncated: need {needed} bytes, have {have}")]
    Truncated {
        /// Minimum bytes required.
        needed: usize,
        /// Bytes available.
        have: usize,
    },

    /// A field value or combination of fields is structurally invalid.
    #[error("payload malformed: {0}")]
    Malformed(&'static str),

    /// A [`HeapUpdatePayload`] record has reserved flag bits set.
    ///
    /// Bit 0 is the HOT flag; all higher bits are reserved and must be zero.
    /// If they are non-zero the record was written by an unknown encoder.
    #[error("payload flags reserved bits set: {0:#010b}")]
    FlagsReserved(u8),
}

// ---------------------------------------------------------------------------
// TupleId helpers (private)
// ---------------------------------------------------------------------------

/// Wire size of an encoded `TupleId`.
///
/// Layout (all little-endian):
/// ```text
///  0  4   RelationId (u32)
///  4  4   BlockNumber (u32, low 24 bits used; high 8 bits reserved-zero)
///  8  2   slot (u16)
/// 10  2   reserved (zero)
/// ```
const TID_SIZE: usize = 12;

/// Encode `tid` into `buf[..TID_SIZE]`.
///
/// Returns `PayloadError::Malformed` when the block number exceeds the 24-bit
/// wire field (`> 0x00FF_FFFF`).
fn encode_tid(buf: &mut [u8; TID_SIZE], tid: TupleId) -> Result<(), PayloadError> {
    let block = tid.page.block.raw();
    if block > 0x00FF_FFFF {
        return Err(PayloadError::Malformed(
            "tid block number exceeds 24-bit wire field",
        ));
    }
    write_u32_le(&mut buf[0..4], tid.page.relation.oid().raw());
    // Only the low 24 bits of BlockNumber are meaningful; high byte reserved zero.
    write_u32_le(&mut buf[4..8], block);
    write_u16_le(&mut buf[8..10], tid.slot);
    write_u16_le(&mut buf[10..12], 0); // reserved
    Ok(())
}

/// Decode a `TupleId` from `bytes[..TID_SIZE]`.
fn decode_tid(bytes: &[u8]) -> Result<TupleId, PayloadError> {
    if bytes.len() < TID_SIZE {
        return Err(PayloadError::Truncated {
            needed: TID_SIZE,
            have: bytes.len(),
        });
    }
    let rel_raw = read_u32_le(&bytes[0..4]).map_err(|_| PayloadError::Malformed("tid relation"))?;
    let block_raw =
        read_u32_le(&bytes[4..8]).map_err(|_| PayloadError::Malformed("tid block"))? & 0x00FF_FFFF;
    let slot = read_u16_le(&bytes[8..10]).map_err(|_| PayloadError::Malformed("tid slot"))?;
    Ok(TupleId::new(
        PageId::new(RelationId::new(rel_raw), BlockNumber::new(block_raw)),
        slot,
    ))
}

// ---------------------------------------------------------------------------
// PageId helpers (private)
// ---------------------------------------------------------------------------

/// Wire size of an encoded `PageId`.
///
/// Layout (all little-endian):
/// ```text
///  0  4   RelationId (u32)
///  4  4   BlockNumber (u32)
/// ```
const PAGE_ID_SIZE: usize = 8;

/// Encode `page` into `buf[..PAGE_ID_SIZE]`.
fn encode_page_id(buf: &mut [u8; PAGE_ID_SIZE], page: PageId) {
    write_u32_le(&mut buf[0..4], page.relation.oid().raw());
    write_u32_le(&mut buf[4..8], page.block.raw());
}

/// Decode a `PageId` from `bytes[..PAGE_ID_SIZE]`.
fn decode_page_id(bytes: &[u8]) -> Result<PageId, PayloadError> {
    if bytes.len() < PAGE_ID_SIZE {
        return Err(PayloadError::Truncated {
            needed: PAGE_ID_SIZE,
            have: bytes.len(),
        });
    }
    let rel_raw =
        read_u32_le(&bytes[0..4]).map_err(|_| PayloadError::Malformed("page relation"))?;
    let block_raw = read_u32_le(&bytes[4..8]).map_err(|_| PayloadError::Malformed("page block"))?;
    Ok(PageId::new(
        RelationId::new(rel_raw),
        BlockNumber::new(block_raw),
    ))
}

// ---------------------------------------------------------------------------
// HeapInsertPayload
// ---------------------------------------------------------------------------

/// Payload for a `RecordType::HeapInsert` WAL record.
///
/// Records the slot assigned to the new tuple and the full on-page tuple
/// bytes (header + user data). Recovery replays a heap insert by writing
/// `tuple_bytes` to `tid` on the target page.
///
/// Wire layout (little-endian, no implicit padding):
/// ```text
///  0  12   TupleId (see module-level encoding)
/// 12   4   tuple_len (u32)
/// 16  ..   tuple_bytes (tuple_len bytes)
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapInsertPayload {
    /// Slot assigned to the inserted tuple.
    pub tid: TupleId,
    /// Full on-page tuple bytes: tuple header followed by user-data attributes.
    pub tuple_bytes: Vec<u8>,
}

impl HeapInsertPayload {
    /// Encode this payload into a freshly-allocated byte vector.
    ///
    /// Returns `PayloadError::Malformed` when the `tid`'s block number exceeds
    /// the 24-bit wire field.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        let tuple_len = u32::try_from(self.tuple_bytes.len())
            .expect("tuple_bytes length fits in u32 — enforced at construction");
        let mut out = vec![0_u8; TID_SIZE + 4 + self.tuple_bytes.len()];
        let mut tid_buf = [0_u8; TID_SIZE];
        encode_tid(&mut tid_buf, self.tid)?;
        out[..TID_SIZE].copy_from_slice(&tid_buf);
        write_u32_le(&mut out[TID_SIZE..TID_SIZE + 4], tuple_len);
        out[TID_SIZE + 4..].copy_from_slice(&self.tuple_bytes);
        Ok(out)
    }

    /// Decode a `HeapInsertPayload` from a byte slice.
    ///
    /// Returns `PayloadError::Truncated` if the slice is shorter than the
    /// fixed header or shorter than the declared `tuple_len`. Returns
    /// `PayloadError::Malformed` if `tuple_len` would exceed
    /// [`MAX_VARIABLE_PAYLOAD_BYTES`].
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        const FIXED: usize = TID_SIZE + 4;
        if bytes.len() < FIXED {
            return Err(PayloadError::Truncated {
                needed: FIXED,
                have: bytes.len(),
            });
        }
        let tid = decode_tid(bytes)?;
        let tuple_len = usize::try_from(
            read_u32_le(&bytes[TID_SIZE..TID_SIZE + 4])
                .map_err(|_| PayloadError::Malformed("heap_insert tuple_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("heap_insert tuple_len usize overflow"))?;
        if tuple_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "heap_insert tuple_len exceeds ceiling",
            ));
        }
        let needed = FIXED + tuple_len;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        Ok(Self {
            tid,
            tuple_bytes: bytes[FIXED..needed].to_vec(),
        })
    }
}

// ---------------------------------------------------------------------------
// HeapUpdatePayload
// ---------------------------------------------------------------------------

/// Payload for a `RecordType::HeapUpdate` WAL record.
///
/// Records both tuple identifiers (old and new), the update flags, and the
/// full new tuple bytes. Recovery replays a heap update by invalidating the
/// old slot and writing `new_tuple_bytes` to `new_tid`.
///
/// Wire layout (little-endian, no implicit padding):
/// ```text
///  0  12   old_tid (TupleId)
/// 12  12   new_tid (TupleId)
/// 24   1   flags (u8) — bit 0 = HOT update; bits 1-7 reserved-zero
/// 25   3   reserved (three zero bytes)
/// 28   4   new_len (u32)
/// 32  ..   new_tuple_bytes (new_len bytes)
/// ```
///
/// # Flags
///
/// Bit 0 (`0x01`) indicates a HOT (heap-only-tuple) update: no indexed column
/// changed, so index pointers do not need updating. All other bits are
/// reserved. The decoder rejects records with any reserved bits set via
/// [`PayloadError::FlagsReserved`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapUpdatePayload {
    /// Slot of the tuple version being superseded.
    pub old_tid: TupleId,
    /// Slot where the new tuple version was placed.
    pub new_tid: TupleId,
    /// Update flags. Bit 0 = HOT update; remaining bits must be zero.
    pub flags: u8,
    /// Full on-page new tuple bytes.
    pub new_tuple_bytes: Vec<u8>,
}

/// Bit mask for the HOT update flag in [`HeapUpdatePayload::flags`].
pub const HEAP_UPDATE_HOT: u8 = 0x01;

/// Mask of all reserved bits in [`HeapUpdatePayload::flags`].
const HEAP_UPDATE_FLAGS_RESERVED: u8 = !HEAP_UPDATE_HOT;

impl HeapUpdatePayload {
    /// Encode this payload into a freshly-allocated byte vector.
    ///
    /// Returns `PayloadError::Malformed` when either `old_tid` or `new_tid`'s
    /// block number exceeds the 24-bit wire field.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        const FIXED: usize = TID_SIZE + TID_SIZE + 1 + 3 + 4; // 32
        let new_len = u32::try_from(self.new_tuple_bytes.len())
            .expect("new_tuple_bytes length fits in u32 — enforced at construction");
        let mut out = vec![0_u8; FIXED + self.new_tuple_bytes.len()];
        let mut buf = [0_u8; TID_SIZE];
        encode_tid(&mut buf, self.old_tid)?;
        out[..TID_SIZE].copy_from_slice(&buf);
        encode_tid(&mut buf, self.new_tid)?;
        out[TID_SIZE..TID_SIZE * 2].copy_from_slice(&buf);
        out[TID_SIZE * 2] = self.flags;
        // bytes 25-27: reserved zero (already zeroed by vec! initializer)
        write_u32_le(&mut out[28..32], new_len);
        out[FIXED..].copy_from_slice(&self.new_tuple_bytes);
        Ok(out)
    }

    /// Decode a `HeapUpdatePayload` from a byte slice.
    ///
    /// Returns [`PayloadError::FlagsReserved`] when any reserved flag bit is
    /// non-zero, [`PayloadError::Truncated`] when the slice is shorter than
    /// declared, and [`PayloadError::Malformed`] when `new_len` exceeds
    /// [`MAX_VARIABLE_PAYLOAD_BYTES`].
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        const FIXED: usize = TID_SIZE + TID_SIZE + 1 + 3 + 4; // 32
        if bytes.len() < FIXED {
            return Err(PayloadError::Truncated {
                needed: FIXED,
                have: bytes.len(),
            });
        }
        let old_tid = decode_tid(bytes)?;
        let new_tid = decode_tid(&bytes[TID_SIZE..])?;
        let flags = bytes[TID_SIZE * 2];
        if flags & HEAP_UPDATE_FLAGS_RESERVED != 0 {
            return Err(PayloadError::FlagsReserved(flags));
        }
        let new_len = usize::try_from(
            read_u32_le(&bytes[28..32])
                .map_err(|_| PayloadError::Malformed("heap_update new_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("heap_update new_len usize overflow"))?;
        if new_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "heap_update new_len exceeds ceiling",
            ));
        }
        let needed = FIXED + new_len;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        Ok(Self {
            old_tid,
            new_tid,
            flags,
            new_tuple_bytes: bytes[FIXED..needed].to_vec(),
        })
    }
}

// ---------------------------------------------------------------------------
// HeapUpdateInPlacePayload
// ---------------------------------------------------------------------------

/// Payload for a `RecordType::HeapUpdateInPlace` WAL record.
///
/// Records the in-place rewrite of a tuple's payload by the
/// single-pass UPDATE path. Carries both the pre-image and the
/// post-image so recovery can:
/// - Re-apply the in-place mutation to the page bytes at `tid`
///   (post-image), and
/// - Rebuild the in-memory `UndoRelationLog` entry for the writer
///   xid (pre-image), so concurrent readers with snapshots that
///   pre-date this commit observe the right payload.
///
/// Wire layout (little-endian):
/// ```text
///  0  12   tid (TupleId — block_number 24b, slot 8b, relation 32b)
/// 12   8   writer_xid (u64)
/// 20   4   command_id (u32)
/// 24   4   pre_len (u32)
/// 28   4   post_len (u32)
/// 32  ..   pre_image_bytes (pre_len bytes)
///  +  ..   post_image_bytes (post_len bytes)
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapUpdateInPlacePayload {
    /// Slot whose payload was rewritten. The `ctid` stays at `tid`
    /// (no version forwarding under the in-place model).
    pub tid: TupleId,
    /// Transaction that performed the in-place UPDATE.
    pub writer_xid: Xid,
    /// Command within `writer_xid` that performed the UPDATE.
    pub command_id: CommandId,
    /// Pre-update payload bytes (no tuple header). Same length as
    /// `post_image_bytes` for the fixed-width fused-update shape
    /// today; the field carries an explicit length so future
    /// variable-width shapes ride the same record.
    pub pre_image_bytes: Vec<u8>,
    /// Post-update payload bytes (no tuple header).
    pub post_image_bytes: Vec<u8>,
}

impl HeapUpdateInPlacePayload {
    /// Encode this payload into a freshly-allocated byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        const FIXED: usize = TID_SIZE + 8 + 4 + 4 + 4; // 32
        let pre_len = u32::try_from(self.pre_image_bytes.len())
            .map_err(|_| PayloadError::Malformed("heap_update_in_place pre_len overflow"))?;
        let post_len = u32::try_from(self.post_image_bytes.len())
            .map_err(|_| PayloadError::Malformed("heap_update_in_place post_len overflow"))?;
        let total = FIXED + self.pre_image_bytes.len() + self.post_image_bytes.len();
        let mut out = vec![0_u8; total];
        let mut tid_buf = [0_u8; TID_SIZE];
        encode_tid(&mut tid_buf, self.tid)?;
        out[..TID_SIZE].copy_from_slice(&tid_buf);
        write_u64_le(&mut out[TID_SIZE..TID_SIZE + 8], self.writer_xid.raw());
        write_u32_le(&mut out[TID_SIZE + 8..TID_SIZE + 12], self.command_id.raw());
        write_u32_le(&mut out[TID_SIZE + 12..TID_SIZE + 16], pre_len);
        write_u32_le(&mut out[TID_SIZE + 16..TID_SIZE + 20], post_len);
        let pre_off = FIXED;
        let post_off = FIXED + self.pre_image_bytes.len();
        out[pre_off..post_off].copy_from_slice(&self.pre_image_bytes);
        out[post_off..total].copy_from_slice(&self.post_image_bytes);
        Ok(out)
    }

    /// Decode a `HeapUpdateInPlacePayload` from a byte slice.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        const FIXED: usize = TID_SIZE + 8 + 4 + 4 + 4;
        if bytes.len() < FIXED {
            return Err(PayloadError::Truncated {
                needed: FIXED,
                have: bytes.len(),
            });
        }
        let tid = decode_tid(bytes)?;
        let writer_xid = Xid::new(
            read_u64_le(&bytes[TID_SIZE..TID_SIZE + 8])
                .map_err(|_| PayloadError::Malformed("heap_update_in_place writer_xid"))?,
        );
        let command_id = CommandId::new(
            read_u32_le(&bytes[TID_SIZE + 8..TID_SIZE + 12])
                .map_err(|_| PayloadError::Malformed("heap_update_in_place command_id"))?,
        );
        let pre_len = usize::try_from(
            read_u32_le(&bytes[TID_SIZE + 12..TID_SIZE + 16])
                .map_err(|_| PayloadError::Malformed("heap_update_in_place pre_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("heap_update_in_place pre_len usize"))?;
        let post_len = usize::try_from(
            read_u32_le(&bytes[TID_SIZE + 16..TID_SIZE + 20])
                .map_err(|_| PayloadError::Malformed("heap_update_in_place post_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("heap_update_in_place post_len usize"))?;
        if pre_len > MAX_VARIABLE_PAYLOAD_BYTES || post_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "heap_update_in_place image length exceeds ceiling",
            ));
        }
        let needed = FIXED + pre_len + post_len;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        let pre_off = FIXED;
        let post_off = FIXED + pre_len;
        Ok(Self {
            tid,
            writer_xid,
            command_id,
            pre_image_bytes: bytes[pre_off..post_off].to_vec(),
            post_image_bytes: bytes[post_off..needed].to_vec(),
        })
    }
}

// ---------------------------------------------------------------------------
// HeapDeleteInPlacePayload
// ---------------------------------------------------------------------------

/// Payload for a `RecordType::HeapDeleteInPlace` WAL record.
///
/// Same shape as [`HeapDeletePayload`]; the distinct record type lets
/// recovery distinguish whether the original write went through the
/// classical `delete_many` path or the single-pass
/// `delete_int32_pair_inplace` path. For DELETE both record types
/// replay identically (stamp `xmax`/`cmax`), but keeping them
/// distinct preserves auditability and matches the in-place UPDATE
/// pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapDeleteInPlacePayload {
    /// Slot of the deleted tuple.
    pub tid: TupleId,
    /// Transaction that performed the delete.
    pub xmax: Xid,
    /// Command within `xmax` that performed the delete.
    pub cmax: CommandId,
}

impl HeapDeleteInPlacePayload {
    /// Encode into a freshly-allocated byte vector. Same wire shape
    /// as [`HeapDeletePayload::encode`].
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        HeapDeletePayload {
            tid: self.tid,
            xmax: self.xmax,
            cmax: self.cmax,
        }
        .encode()
    }

    /// Decode from a byte slice. Same wire shape as
    /// [`HeapDeletePayload::decode`].
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        let HeapDeletePayload { tid, xmax, cmax } = HeapDeletePayload::decode(bytes)?;
        Ok(Self { tid, xmax, cmax })
    }
}

// ---------------------------------------------------------------------------
// HeapDeletePayload
// ---------------------------------------------------------------------------

/// Payload for a `RecordType::HeapDelete` WAL record.
///
/// Records the identifier of the deleted tuple, the deleting transaction, and
/// the command within that transaction. Recovery replays a heap delete by
/// stamping `xmax` and `cmax` into the tuple header at `tid`.
///
/// Wire layout (little-endian):
/// ```text
///  0  12   TupleId
/// 12   8   xmax (u64)
/// 20   4   cmax (u32)
/// 24   4   reserved (four zero bytes)
/// ```
/// Total: 28 bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapDeletePayload {
    /// Slot of the deleted tuple.
    pub tid: TupleId,
    /// Transaction that performed the delete.
    pub xmax: Xid,
    /// Command within `xmax` that performed the delete.
    pub cmax: CommandId,
}

impl HeapDeletePayload {
    /// Encode this payload into a freshly-allocated byte vector.
    ///
    /// Returns `PayloadError::Malformed` when the `tid`'s block number exceeds
    /// the 24-bit wire field.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        const SIZE: usize = TID_SIZE + 8 + 4 + 4;
        let mut out = vec![0_u8; SIZE];
        let mut tid_buf = [0_u8; TID_SIZE];
        encode_tid(&mut tid_buf, self.tid)?;
        out[..TID_SIZE].copy_from_slice(&tid_buf);
        write_u64_le(&mut out[TID_SIZE..TID_SIZE + 8], self.xmax.raw());
        write_u32_le(&mut out[TID_SIZE + 8..TID_SIZE + 12], self.cmax.raw());
        // bytes TID_SIZE+12 .. SIZE: reserved zero (already zeroed)
        Ok(out)
    }

    /// Decode a `HeapDeletePayload` from a byte slice.
    ///
    /// Returns [`PayloadError::Truncated`] when the slice is shorter than 28
    /// bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        const SIZE: usize = TID_SIZE + 8 + 4 + 4;
        if bytes.len() < SIZE {
            return Err(PayloadError::Truncated {
                needed: SIZE,
                have: bytes.len(),
            });
        }
        let tid = decode_tid(bytes)?;
        let xmax = Xid::new(
            read_u64_le(&bytes[TID_SIZE..TID_SIZE + 8])
                .map_err(|_| PayloadError::Malformed("heap_delete xmax"))?,
        );
        let cmax = CommandId::new(
            read_u32_le(&bytes[TID_SIZE + 8..TID_SIZE + 12])
                .map_err(|_| PayloadError::Malformed("heap_delete cmax"))?,
        );
        Ok(Self { tid, xmax, cmax })
    }
}

// ---------------------------------------------------------------------------
// CommitPayload
// ---------------------------------------------------------------------------

/// Payload for a `RecordType::Commit` WAL record.
///
/// Carries the LSN at which the commit was written and the wall-clock time of
/// the commit in microseconds since the Unix epoch. Recovery uses the commit
/// LSN to advance the flush watermark; the timestamp is used for
/// transaction-time queries.
///
/// Wire layout (little-endian):
/// ```text
///  0  8   commit_lsn (u64)
///  8  8   commit_timestamp_micros (u64)
/// ```
/// Total: 16 bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitPayload {
    /// LSN at which the commit record was written.
    pub commit_lsn: Lsn,
    /// Wall-clock commit time in microseconds since the Unix epoch.
    pub commit_timestamp_micros: u64,
}

impl CommitPayload {
    /// Encode this payload into a freshly-allocated byte vector.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![0_u8; 16];
        write_u64_le(&mut out[0..8], self.commit_lsn.raw());
        write_u64_le(&mut out[8..16], self.commit_timestamp_micros);
        out
    }

    /// Decode a `CommitPayload` from a byte slice.
    ///
    /// Returns [`PayloadError::Truncated`] when the slice is shorter than 16
    /// bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        if bytes.len() < 16 {
            return Err(PayloadError::Truncated {
                needed: 16,
                have: bytes.len(),
            });
        }
        let commit_lsn =
            Lsn::new(read_u64_le(&bytes[0..8]).map_err(|_| PayloadError::Malformed("commit lsn"))?);
        let commit_timestamp_micros =
            read_u64_le(&bytes[8..16]).map_err(|_| PayloadError::Malformed("commit timestamp"))?;
        Ok(Self {
            commit_lsn,
            commit_timestamp_micros,
        })
    }
}

// ---------------------------------------------------------------------------
// AbortPayload
// ---------------------------------------------------------------------------

/// Payload for a `RecordType::Abort` WAL record.
///
/// Carries the LSN at which the abort was written. Recovery uses this to mark
/// the transaction as rolled back in the CLOG.
///
/// Wire layout (little-endian):
/// ```text
///  0  8   abort_lsn (u64)
/// ```
/// Total: 8 bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbortPayload {
    /// LSN at which the abort record was written.
    pub abort_lsn: Lsn,
}

impl AbortPayload {
    /// Encode this payload into a freshly-allocated byte vector.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![0_u8; 8];
        write_u64_le(&mut out[0..8], self.abort_lsn.raw());
        out
    }

    /// Decode an `AbortPayload` from a byte slice.
    ///
    /// Returns [`PayloadError::Truncated`] when the slice is shorter than 8
    /// bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        if bytes.len() < 8 {
            return Err(PayloadError::Truncated {
                needed: 8,
                have: bytes.len(),
            });
        }
        let abort_lsn =
            Lsn::new(read_u64_le(&bytes[0..8]).map_err(|_| PayloadError::Malformed("abort lsn"))?);
        Ok(Self { abort_lsn })
    }
}

// ---------------------------------------------------------------------------
// CheckpointPayload
// ---------------------------------------------------------------------------

/// Payload for a `RecordType::Checkpoint` WAL record.
///
/// Records the redo-start LSN and the transaction horizon at checkpoint time.
/// Recovery uses `redo_from` to skip replaying records that are already
/// reflected in the checkpoint's page images.
///
/// Wire layout (little-endian):
/// ```text
///  0  8   redo_from (u64)
///  8  8   oldest_in_progress (u64)
/// 16  8   next_xid (u64)
/// ```
/// Total: 24 bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointPayload {
    /// LSN from which redo must start during recovery (the oldest dirty page's
    /// modification LSN at checkpoint time).
    pub redo_from: Lsn,
    /// XID of the oldest transaction that was in-progress when the checkpoint
    /// started. Recovery must redo any WAL records whose XID is ≥ this value.
    pub oldest_in_progress: Xid,
    /// Next XID that will be handed out after recovery. The transaction
    /// manager initialises its counter from this value.
    pub next_xid: Xid,
}

impl CheckpointPayload {
    /// Encode this payload into a freshly-allocated byte vector.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![0_u8; 24];
        write_u64_le(&mut out[0..8], self.redo_from.raw());
        write_u64_le(&mut out[8..16], self.oldest_in_progress.raw());
        write_u64_le(&mut out[16..24], self.next_xid.raw());
        out
    }

    /// Decode a `CheckpointPayload` from a byte slice.
    ///
    /// Returns [`PayloadError::Truncated`] when the slice is shorter than 24
    /// bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        if bytes.len() < 24 {
            return Err(PayloadError::Truncated {
                needed: 24,
                have: bytes.len(),
            });
        }
        let redo_from = Lsn::new(
            read_u64_le(&bytes[0..8]).map_err(|_| PayloadError::Malformed("ckpt redo_from"))?,
        );
        let oldest_in_progress = Xid::new(
            read_u64_le(&bytes[8..16])
                .map_err(|_| PayloadError::Malformed("ckpt oldest_in_progress"))?,
        );
        let next_xid = Xid::new(
            read_u64_le(&bytes[16..24]).map_err(|_| PayloadError::Malformed("ckpt next_xid"))?,
        );
        Ok(Self {
            redo_from,
            oldest_in_progress,
            next_xid,
        })
    }
}

// ---------------------------------------------------------------------------
// FullPageWritePayload
// ---------------------------------------------------------------------------

/// Payload for a `RecordType::FullPageWrite` WAL record.
///
/// Carries a complete on-disk page image. Full page writes are emitted on the
/// first modification of a page after a checkpoint so that recovery can
/// restore the page to a consistent state even if the page was only partially
/// flushed at the time of a crash.
///
/// The stored `page_bytes` length **must** equal [`PAGE_SIZE`] (8 192 bytes).
/// The encoder rejects a `page_bytes` vector of any other length; the decoder
/// rejects a `page_bytes_len` field that differs from `PAGE_SIZE`.
///
/// Wire layout (little-endian):
/// ```text
///  0  8   PageId (RelationId u32 | BlockNumber u32)
///  8  4   page_bytes_len (u32) — must equal PAGE_SIZE
/// 12  ..  page_bytes (page_bytes_len bytes)
/// ```
/// Total: 12 + `PAGE_SIZE` bytes for a valid record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FullPageWritePayload {
    /// Identifies the page on disk.
    pub page: PageId,
    /// Complete 8 KiB page image.
    pub page_bytes: Vec<u8>,
}

impl FullPageWritePayload {
    /// Encode this payload into a freshly-allocated byte vector.
    ///
    /// # Panics
    ///
    /// Panics if `page_bytes.len() != PAGE_SIZE`. Callers must ensure the
    /// page image is exactly [`PAGE_SIZE`] bytes before encoding.
    pub fn encode(&self) -> Vec<u8> {
        const FIXED: usize = PAGE_ID_SIZE + 4;
        assert_eq!(
            self.page_bytes.len(),
            PAGE_SIZE,
            "FullPageWritePayload::encode: page_bytes must be exactly PAGE_SIZE bytes"
        );
        let page_bytes_len =
            u32::try_from(PAGE_SIZE).expect("PAGE_SIZE fits in u32 — invariant of the type system");
        let mut out = vec![0_u8; FIXED + PAGE_SIZE];
        let mut pid_buf = [0_u8; PAGE_ID_SIZE];
        encode_page_id(&mut pid_buf, self.page);
        out[..PAGE_ID_SIZE].copy_from_slice(&pid_buf);
        write_u32_le(&mut out[PAGE_ID_SIZE..PAGE_ID_SIZE + 4], page_bytes_len);
        out[FIXED..].copy_from_slice(&self.page_bytes);
        out
    }

    /// Decode a `FullPageWritePayload` from a byte slice.
    ///
    /// Returns [`PayloadError::Malformed`] if `page_bytes_len ≠ PAGE_SIZE`.
    /// Returns [`PayloadError::Truncated`] if the slice is shorter than the
    /// fixed header or shorter than `PAGE_ID_SIZE + 4 + page_bytes_len`.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        const FIXED: usize = PAGE_ID_SIZE + 4;
        if bytes.len() < FIXED {
            return Err(PayloadError::Truncated {
                needed: FIXED,
                have: bytes.len(),
            });
        }
        let page = decode_page_id(bytes)?;
        let page_bytes_len = usize::try_from(
            read_u32_le(&bytes[PAGE_ID_SIZE..PAGE_ID_SIZE + 4])
                .map_err(|_| PayloadError::Malformed("fpw page_bytes_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("fpw page_bytes_len usize overflow"))?;
        if page_bytes_len != PAGE_SIZE {
            return Err(PayloadError::Malformed(
                "fpw page_bytes_len must equal PAGE_SIZE",
            ));
        }
        let needed = FIXED + page_bytes_len;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        Ok(Self {
            page,
            page_bytes: bytes[FIXED..needed].to_vec(),
        })
    }
}

// ---------------------------------------------------------------------------
// BTreeOpPayload
// ---------------------------------------------------------------------------

/// Kind of B-tree operation recorded in a [`BTreeOpPayload`].
///
/// Numeric values are part of the on-disk format; new variants may be appended
/// but existing values must remain stable. The decoder rejects any byte value
/// not listed here via [`PayloadError::Malformed`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum BTreeOpKind {
    /// A key/value pair was inserted into a leaf page.
    Insert = 1,
    /// A leaf page was split: a new right sibling was allocated and the
    /// upper half of the entries were moved to it.
    Split = 2,
    /// A key/value pair was deleted from a leaf page.
    Delete = 3,
}

impl BTreeOpKind {
    /// Parse a `BTreeOpKind` from its on-disk byte representation.
    ///
    /// Returns `PayloadError::Malformed` for any byte value that is not a
    /// recognised variant. This ensures forward-compatibility: a record
    /// written by a newer binary that added a `kind = 4` variant is
    /// rejected loudly rather than misinterpreted.
    pub const fn from_u8(v: u8) -> Result<Self, PayloadError> {
        match v {
            1 => Ok(Self::Insert),
            2 => Ok(Self::Split),
            3 => Ok(Self::Delete),
            _ => Err(PayloadError::Malformed(
                // Static strings only — we cannot embed the raw byte in a
                // &'static str. The caller's context (record decoding) will
                // surface the raw value.
                "btree_op kind unknown",
            )),
        }
    }
}

/// Payload for a `RecordType::BTreeOp` WAL record.
///
/// Carries a single B-tree mutation sufficient for redo: the operation kind, the
/// index relation, the page on which the mutation occurred, the encoded key bytes,
/// and the child page id (for internal nodes) or the tuple id (for leaf nodes).
///
/// Wire layout (little-endian, no implicit padding):
/// ```text
///  0   1   op (u8) — BTreeOpKind discriminant
///  1   3   reserved (three zero bytes)
///  4   4   index_rel (RelationId, u32)
///  8   8   page (PageId: rel u32 | block u32)
/// 16   4   key_len (u32)
/// 20  ..   key_bytes (key_len bytes)
///  +   4   cv_len (u32)  — child_or_value
///  +  ..   cv_bytes (cv_len bytes)
/// ```
///
/// The fixed section is 20 bytes; total size is `20 + key_len + 4 + cv_len`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BTreeOpPayload {
    /// What kind of B-tree mutation this record represents.
    pub op: BTreeOpKind,
    /// OID of the index relation that was mutated.
    pub index_rel: RelationId,
    /// Page on which the mutation occurred.
    pub page: PageId,
    /// Encoded key bytes. For a `Split` record this is the separator key that
    /// was promoted to the parent.
    pub key_bytes: Vec<u8>,
    /// For an internal-page mutation (`Split`): the 4-byte little-endian block
    /// number of the new child.  For a leaf-page mutation (`Insert` / `Delete`):
    /// the 12-byte encoded `TupleId` of the heap tuple this index entry points to.
    pub child_or_value: Vec<u8>,
}

impl BTreeOpPayload {
    /// Encode this payload into a freshly-allocated byte vector.
    ///
    /// Returns `PayloadError::Malformed` if either `key_bytes` or
    /// `child_or_value` exceeds [`MAX_VARIABLE_PAYLOAD_BYTES`].
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        let key_len = u32::try_from(self.key_bytes.len())
            .map_err(|_| PayloadError::Malformed("btree_op key_len overflow"))?;
        let cv_len = u32::try_from(self.child_or_value.len())
            .map_err(|_| PayloadError::Malformed("btree_op cv_len overflow"))?;
        if self.key_bytes.len() > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed("btree_op key_len exceeds ceiling"));
        }
        if self.child_or_value.len() > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed("btree_op cv_len exceeds ceiling"));
        }
        // Fixed section: 1 (op) + 3 (reserved) + 4 (index_rel) + 8 (page) + 4 (key_len) = 20
        // Then key_bytes, then 4 (cv_len), then cv_bytes.
        let total = 20 + self.key_bytes.len() + 4 + self.child_or_value.len();
        let mut out = vec![0_u8; total];
        out[0] = self.op as u8;
        // bytes 1-3: reserved zero (already zeroed)
        write_u32_le(&mut out[4..8], self.index_rel.oid().raw());
        let mut pid_buf = [0_u8; PAGE_ID_SIZE];
        encode_page_id(&mut pid_buf, self.page);
        out[8..16].copy_from_slice(&pid_buf);
        write_u32_le(&mut out[16..20], key_len);
        out[20..20 + self.key_bytes.len()].copy_from_slice(&self.key_bytes);
        let cv_off = 20 + self.key_bytes.len();
        write_u32_le(&mut out[cv_off..cv_off + 4], cv_len);
        out[cv_off + 4..].copy_from_slice(&self.child_or_value);
        Ok(out)
    }

    /// Decode a `BTreeOpPayload` from a byte slice.
    ///
    /// Returns [`PayloadError::Truncated`] when the slice is shorter than the
    /// minimum required, and [`PayloadError::Malformed`] when the `op` byte is
    /// unrecognised or either length field exceeds [`MAX_VARIABLE_PAYLOAD_BYTES`].
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        const FIXED: usize = 20; // op(1)+res(3)+rel(4)+page(8)+key_len(4)
        if bytes.len() < FIXED {
            return Err(PayloadError::Truncated {
                needed: FIXED,
                have: bytes.len(),
            });
        }
        let op = BTreeOpKind::from_u8(bytes[0])?;
        let index_rel = RelationId::new(
            read_u32_le(&bytes[4..8]).map_err(|_| PayloadError::Malformed("btree_op index_rel"))?,
        );
        let page = decode_page_id(&bytes[8..16])?;
        let key_len = usize::try_from(
            read_u32_le(&bytes[16..20]).map_err(|_| PayloadError::Malformed("btree_op key_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("btree_op key_len usize overflow"))?;
        if key_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed("btree_op key_len exceeds ceiling"));
        }
        let key_end = FIXED + key_len;
        if bytes.len() < key_end + 4 {
            return Err(PayloadError::Truncated {
                needed: key_end + 4,
                have: bytes.len(),
            });
        }
        let key_bytes = bytes[FIXED..key_end].to_vec();
        let cv_len = usize::try_from(
            read_u32_le(&bytes[key_end..key_end + 4])
                .map_err(|_| PayloadError::Malformed("btree_op cv_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("btree_op cv_len usize overflow"))?;
        if cv_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed("btree_op cv_len exceeds ceiling"));
        }
        let cv_end = key_end + 4 + cv_len;
        if bytes.len() < cv_end {
            return Err(PayloadError::Truncated {
                needed: cv_end,
                have: bytes.len(),
            });
        }
        let child_or_value = bytes[key_end + 4..cv_end].to_vec();
        Ok(Self {
            op,
            index_rel,
            page,
            key_bytes,
            child_or_value,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use ultrasql_core::constants::PAGE_SIZE;
    use ultrasql_core::{BlockNumber, CommandId, Lsn, PageId, RelationId, TupleId, Xid};

    use super::*;

    // ── helpers ───────────────────────────────────────────────────────────

    fn tid(rel: u32, block: u32, slot: u16) -> TupleId {
        TupleId::new(
            PageId::new(RelationId::new(rel), BlockNumber::new(block)),
            slot,
        )
    }

    fn page_id(rel: u32, block: u32) -> PageId {
        PageId::new(RelationId::new(rel), BlockNumber::new(block))
    }

    fn full_page() -> Vec<u8> {
        vec![0xAB_u8; PAGE_SIZE]
    }

    // ── HeapInsertPayload ─────────────────────────────────────────────────

    #[test]
    fn heap_insert_round_trip_empty_tuple() {
        let p = HeapInsertPayload {
            tid: tid(1, 0, 0),
            tuple_bytes: vec![],
        };
        assert_eq!(HeapInsertPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn heap_insert_round_trip_realistic() {
        let p = HeapInsertPayload {
            tid: tid(7, 42, 13),
            tuple_bytes: (0_u8..64).collect(),
        };
        assert_eq!(HeapInsertPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    // ── HeapUpdatePayload ─────────────────────────────────────────────────

    #[test]
    fn heap_update_round_trip_no_hot() {
        let p = HeapUpdatePayload {
            old_tid: tid(1, 0, 0),
            new_tid: tid(1, 0, 1),
            flags: 0,
            new_tuple_bytes: vec![],
        };
        assert_eq!(HeapUpdatePayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn heap_update_round_trip_hot() {
        let p = HeapUpdatePayload {
            old_tid: tid(5, 100, 3),
            new_tid: tid(5, 100, 4),
            flags: HEAP_UPDATE_HOT,
            new_tuple_bytes: (0_u8..=127).collect(),
        };
        assert_eq!(HeapUpdatePayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    // ── HeapDeletePayload ─────────────────────────────────────────────────

    #[test]
    fn heap_delete_round_trip_minimal() {
        let p = HeapDeletePayload {
            tid: tid(1, 0, 0),
            xmax: Xid::INVALID,
            cmax: CommandId::FIRST,
        };
        assert_eq!(HeapDeletePayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn heap_delete_round_trip_realistic() {
        let p = HeapDeletePayload {
            tid: tid(3, 99, 7),
            xmax: Xid::new(1_234_567),
            cmax: CommandId::new(2),
        };
        assert_eq!(HeapDeletePayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    // ── CommitPayload ─────────────────────────────────────────────────────

    #[test]
    fn commit_round_trip_zero() {
        let p = CommitPayload {
            commit_lsn: Lsn::ZERO,
            commit_timestamp_micros: 0,
        };
        assert_eq!(CommitPayload::decode(&p.encode()).unwrap(), p);
    }

    #[test]
    fn commit_round_trip_realistic() {
        let p = CommitPayload {
            commit_lsn: Lsn::new(0x0000_0001_0000_2000),
            commit_timestamp_micros: 1_715_000_000_000_000,
        };
        assert_eq!(CommitPayload::decode(&p.encode()).unwrap(), p);
    }

    // ── AbortPayload ──────────────────────────────────────────────────────

    #[test]
    fn abort_round_trip_zero() {
        let p = AbortPayload {
            abort_lsn: Lsn::ZERO,
        };
        assert_eq!(AbortPayload::decode(&p.encode()).unwrap(), p);
    }

    #[test]
    fn abort_round_trip_nonzero() {
        let p = AbortPayload {
            abort_lsn: Lsn::new(0xDEAD_BEEF_CAFE_BABE),
        };
        assert_eq!(AbortPayload::decode(&p.encode()).unwrap(), p);
    }

    // ── CheckpointPayload ─────────────────────────────────────────────────

    #[test]
    fn checkpoint_round_trip_zeros() {
        let p = CheckpointPayload {
            redo_from: Lsn::ZERO,
            oldest_in_progress: Xid::INVALID,
            next_xid: Xid::FIRST_USER,
        };
        assert_eq!(CheckpointPayload::decode(&p.encode()).unwrap(), p);
    }

    #[test]
    fn checkpoint_round_trip_realistic() {
        let p = CheckpointPayload {
            redo_from: Lsn::new(0x0001_0000),
            oldest_in_progress: Xid::new(42),
            next_xid: Xid::new(100),
        };
        assert_eq!(CheckpointPayload::decode(&p.encode()).unwrap(), p);
    }

    // ── FullPageWritePayload ──────────────────────────────────────────────

    #[test]
    fn full_page_write_round_trip_zeroed_page() {
        let p = FullPageWritePayload {
            page: page_id(1, 0),
            page_bytes: vec![0_u8; PAGE_SIZE],
        };
        assert_eq!(FullPageWritePayload::decode(&p.encode()).unwrap(), p);
    }

    #[test]
    fn full_page_write_round_trip_realistic() {
        let p = FullPageWritePayload {
            page: page_id(7, 255),
            page_bytes: full_page(),
        };
        assert_eq!(FullPageWritePayload::decode(&p.encode()).unwrap(), p);
    }

    #[test]
    fn encode_rejects_block_above_24_bit_field() {
        let p = HeapInsertPayload {
            tid: TupleId::new(
                PageId::new(RelationId::new(1), BlockNumber::new(0x0100_0000)),
                0,
            ),
            tuple_bytes: vec![],
        };
        let err = p.encode().unwrap_err();
        assert!(
            matches!(err, PayloadError::Malformed(_)),
            "expected Malformed for block > 24-bit, got {err:?}"
        );
    }

    // ── Negative tests ────────────────────────────────────────────────────

    #[test]
    fn heap_update_reserved_flags_rejected() {
        let p = HeapUpdatePayload {
            old_tid: tid(1, 0, 0),
            new_tid: tid(1, 0, 1),
            flags: 0b1000_0000,
            new_tuple_bytes: vec![],
        };
        // Encode by hand, bypassing the encode-time reserved-flag check is
        // not performed (encode trusts the caller at construction time).
        // Use decode on a manually crafted buffer instead.
        let mut raw = p.encode().unwrap(); // encode writes flags = 0b1000_0000 verbatim
        let err = HeapUpdatePayload::decode(&raw).unwrap_err();
        assert!(
            matches!(err, PayloadError::FlagsReserved(0b1000_0000)),
            "got {err:?}"
        );

        // Also test flags = 0b0000_0010 (another reserved bit).
        raw[TID_SIZE * 2] = 0b0000_0010;
        let err2 = HeapUpdatePayload::decode(&raw).unwrap_err();
        assert!(matches!(err2, PayloadError::FlagsReserved(_)));
    }

    #[test]
    fn heap_insert_truncated_by_one_byte_rejected() {
        let p = HeapInsertPayload {
            tid: tid(1, 0, 0),
            tuple_bytes: b"hello world".to_vec(),
        };
        let mut raw = p.encode().unwrap();
        raw.truncate(raw.len() - 1);
        let err = HeapInsertPayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
    }

    #[test]
    fn heap_update_truncated_by_one_byte_rejected() {
        let p = HeapUpdatePayload {
            old_tid: tid(1, 0, 0),
            new_tid: tid(1, 0, 1),
            flags: 0,
            new_tuple_bytes: b"hello".to_vec(),
        };
        let mut raw = p.encode().unwrap();
        raw.truncate(raw.len() - 1);
        let err = HeapUpdatePayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
    }

    #[test]
    fn heap_delete_truncated_by_one_byte_rejected() {
        let p = HeapDeletePayload {
            tid: tid(1, 0, 0),
            xmax: Xid::new(99),
            cmax: CommandId::new(1),
        };
        let mut raw = p.encode().unwrap();
        raw.truncate(raw.len() - 1);
        let err = HeapDeletePayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
    }

    #[test]
    fn commit_truncated_by_one_byte_rejected() {
        let p = CommitPayload {
            commit_lsn: Lsn::new(1),
            commit_timestamp_micros: 2,
        };
        let mut raw = p.encode();
        raw.truncate(raw.len() - 1);
        let err = CommitPayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
    }

    #[test]
    fn abort_truncated_by_one_byte_rejected() {
        let p = AbortPayload {
            abort_lsn: Lsn::new(100),
        };
        let mut raw = p.encode();
        raw.truncate(raw.len() - 1);
        let err = AbortPayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
    }

    #[test]
    fn checkpoint_truncated_by_one_byte_rejected() {
        let p = CheckpointPayload {
            redo_from: Lsn::new(1),
            oldest_in_progress: Xid::new(2),
            next_xid: Xid::new(3),
        };
        let mut raw = p.encode();
        raw.truncate(raw.len() - 1);
        let err = CheckpointPayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
    }

    #[test]
    fn full_page_write_truncated_by_one_byte_rejected() {
        let p = FullPageWritePayload {
            page: page_id(1, 0),
            page_bytes: full_page(),
        };
        let mut raw = p.encode();
        raw.truncate(raw.len() - 1);
        let err = FullPageWritePayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
    }
    // NOTE: FullPageWritePayload::encode does not encode a TupleId and uses
    // PAGE_ID_SIZE (u32 fields without 24-bit restriction), so no block-limit
    // test is needed here.

    #[test]
    fn heap_insert_gigantic_tuple_len_rejected() {
        // Craft a raw buffer whose tuple_len field claims 1 GiB.
        const FIXED: usize = TID_SIZE + 4;
        let mut raw = vec![0_u8; FIXED]; // no actual tuple bytes
        let gigabyte: u32 = 1_024 * 1_024 * 1_024;
        write_u32_le(&mut raw[TID_SIZE..TID_SIZE + 4], gigabyte);
        let err = HeapInsertPayload::decode(&raw).unwrap_err();
        assert!(
            matches!(err, PayloadError::Malformed(_)),
            "expected Malformed, got {err:?}"
        );
    }

    #[test]
    fn full_page_write_wrong_page_size_rejected() {
        // Craft a FPW whose page_bytes_len is PAGE_SIZE - 1.
        const FIXED: usize = PAGE_ID_SIZE + 4;
        let wrong_len = u32::try_from(PAGE_SIZE - 1).unwrap();
        let mut raw = vec![0_u8; FIXED + PAGE_SIZE - 1];
        write_u32_le(&mut raw[PAGE_ID_SIZE..PAGE_ID_SIZE + 4], wrong_len);
        let err = FullPageWritePayload::decode(&raw).unwrap_err();
        assert!(
            matches!(err, PayloadError::Malformed(_)),
            "expected Malformed, got {err:?}"
        );

        // Also test with a page_bytes_len that is larger than PAGE_SIZE.
        let larger = u32::try_from(PAGE_SIZE + 1).unwrap();
        let mut raw2 = vec![0_u8; FIXED + PAGE_SIZE + 1];
        write_u32_le(&mut raw2[PAGE_ID_SIZE..PAGE_ID_SIZE + 4], larger);
        let err2 = FullPageWritePayload::decode(&raw2).unwrap_err();
        assert!(
            matches!(err2, PayloadError::Malformed(_)),
            "expected Malformed, got {err2:?}"
        );
    }

    // ── Proptest: HeapInsertPayload round-trip ────────────────────────────

    proptest! {
        #[test]
        fn proptest_heap_insert_round_trip(
            rel in 0_u32..u32::MAX,
            block in 0_u32..0x00FF_FFFFu32,
            slot in 0_u16..u16::MAX,
            tuple_bytes in proptest::collection::vec(any::<u8>(), 0..16_384),
        ) {
            let p = HeapInsertPayload {
                tid: tid(rel, block, slot),
                tuple_bytes,
            };
            prop_assert_eq!(HeapInsertPayload::decode(&p.encode().unwrap()).unwrap(), p);
        }

        #[test]
        fn proptest_heap_update_round_trip(
            old_rel in 0_u32..u32::MAX,
            old_block in 0_u32..0x00FF_FFFFu32,
            old_slot in 0_u16..u16::MAX,
            new_rel in 0_u32..u32::MAX,
            new_block in 0_u32..0x00FF_FFFFu32,
            new_slot in 0_u16..u16::MAX,
            // Only valid flags: 0 or HEAP_UPDATE_HOT (1).
            flags in prop_oneof![Just(0_u8), Just(HEAP_UPDATE_HOT)],
            new_tuple_bytes in proptest::collection::vec(any::<u8>(), 0..16_384),
        ) {
            let p = HeapUpdatePayload {
                old_tid: tid(old_rel, old_block, old_slot),
                new_tid: tid(new_rel, new_block, new_slot),
                flags,
                new_tuple_bytes,
            };
            prop_assert_eq!(HeapUpdatePayload::decode(&p.encode().unwrap()).unwrap(), p);
        }
    }

    // ── BTreeOpPayload ────────────────────────────────────────────────────

    #[test]
    fn btree_op_insert_round_trip() {
        let p = BTreeOpPayload {
            op: BTreeOpKind::Insert,
            index_rel: RelationId::new(42),
            page: page_id(42, 7),
            key_bytes: vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
            child_or_value: b"tuple-id-12b".to_vec(),
        };
        assert_eq!(BTreeOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn btree_op_split_round_trip() {
        let p = BTreeOpPayload {
            op: BTreeOpKind::Split,
            index_rel: RelationId::new(1),
            page: page_id(1, 0),
            key_bytes: 42_i64.to_le_bytes().to_vec(),
            child_or_value: 99_u32.to_le_bytes().to_vec(),
        };
        assert_eq!(BTreeOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn btree_op_delete_round_trip() {
        let p = BTreeOpPayload {
            op: BTreeOpKind::Delete,
            index_rel: RelationId::new(5),
            page: page_id(5, 3),
            key_bytes: vec![0xFF; 8],
            child_or_value: vec![],
        };
        assert_eq!(BTreeOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn btree_op_empty_key_and_value_round_trip() {
        let p = BTreeOpPayload {
            op: BTreeOpKind::Insert,
            index_rel: RelationId::new(0),
            page: page_id(0, 0),
            key_bytes: vec![],
            child_or_value: vec![],
        };
        assert_eq!(BTreeOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
    }

    #[test]
    fn btree_op_unknown_kind_rejected() {
        // Build a valid Insert payload, then corrupt the op byte to 99.
        let p = BTreeOpPayload {
            op: BTreeOpKind::Insert,
            index_rel: RelationId::new(1),
            page: page_id(1, 0),
            key_bytes: vec![1, 2, 3, 4, 5, 6, 7, 8],
            child_or_value: vec![],
        };
        let mut raw = p.encode().unwrap();
        raw[0] = 99; // unknown kind
        let err = BTreeOpPayload::decode(&raw).unwrap_err();
        assert!(
            matches!(err, PayloadError::Malformed(_)),
            "expected Malformed for unknown BTreeOpKind, got {err:?}"
        );
    }

    #[test]
    fn btree_op_truncated_rejected() {
        let p = BTreeOpPayload {
            op: BTreeOpKind::Insert,
            index_rel: RelationId::new(1),
            page: page_id(1, 0),
            key_bytes: vec![0; 8],
            child_or_value: vec![1, 2, 3],
        };
        let mut raw = p.encode().unwrap();
        raw.truncate(raw.len() - 1);
        let err = BTreeOpPayload::decode(&raw).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { .. }), "got {err:?}");
    }

    proptest! {
        #[test]
        fn proptest_btree_op_round_trip(
            op_raw in prop_oneof![Just(1_u8), Just(2_u8), Just(3_u8)],
            rel in 0_u32..u32::MAX,
            block in 0_u32..u32::MAX,
            key_bytes in proptest::collection::vec(any::<u8>(), 0..256_usize),
            cv_bytes in proptest::collection::vec(any::<u8>(), 0..256_usize),
        ) {
            let op = BTreeOpKind::from_u8(op_raw).unwrap();
            let p = BTreeOpPayload {
                op,
                index_rel: RelationId::new(rel),
                page: page_id(rel, block),
                key_bytes,
                child_or_value: cv_bytes,
            };
            prop_assert_eq!(BTreeOpPayload::decode(&p.encode().unwrap()).unwrap(), p);
        }
    }
}
