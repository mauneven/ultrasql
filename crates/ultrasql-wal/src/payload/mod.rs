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
//! All integers are little-endian. Padding and reserved bytes are written as
//! zero and must decode as zero.
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
    read_i64_le, read_u16_le, read_u32_le, write_i64_le, write_u16_le, write_u32_le,
};
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};

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

    /// The byte slice contains bytes beyond the payload's declared layout.
    #[error("payload has trailing bytes: expected {expected}, have {have}")]
    Trailing {
        /// Exact byte length required by the payload layout.
        expected: usize,
        /// Bytes supplied by the caller.
        have: usize,
    },

    /// A [`HeapUpdatePayload`] record has reserved flag bits set.
    ///
    /// Bit 0 is the HOT flag; all higher bits are reserved and must be zero.
    /// If they are non-zero the record was written by an unknown encoder.
    #[error("payload flags reserved bits set: {0:#010b}")]
    FlagsReserved(u8),
}

pub(crate) fn require_exact_len(bytes: &[u8], expected: usize) -> Result<(), PayloadError> {
    match bytes.len().cmp(&expected) {
        std::cmp::Ordering::Less => Err(PayloadError::Truncated {
            needed: expected,
            have: bytes.len(),
        }),
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Greater => Err(PayloadError::Trailing {
            expected,
            have: bytes.len(),
        }),
    }
}

pub(crate) fn checked_len_sum(parts: &[usize], context: &'static str) -> Result<usize, PayloadError> {
    parts.iter().try_fold(0_usize, |total, part| {
        total
            .checked_add(*part)
            .ok_or(PayloadError::Malformed(context))
    })
}

pub(crate) fn checked_offset(start: usize, len: usize, context: &'static str) -> Result<usize, PayloadError> {
    start
        .checked_add(len)
        .ok_or(PayloadError::Malformed(context))
}

pub(crate) fn write_i64_advance(
    out: &mut [u8],
    offset: &mut usize,
    value: i64,
    context: &'static str,
) -> Result<(), PayloadError> {
    let end = checked_offset(*offset, 8, context)?;
    write_i64_le(&mut out[*offset..end], value);
    *offset = end;
    Ok(())
}

pub(crate) fn write_u32_advance(
    out: &mut [u8],
    offset: &mut usize,
    value: u32,
    context: &'static str,
) -> Result<(), PayloadError> {
    let end = checked_offset(*offset, 4, context)?;
    write_u32_le(&mut out[*offset..end], value);
    *offset = end;
    Ok(())
}

pub(crate) fn read_i64_advance(
    bytes: &[u8],
    offset: &mut usize,
    field: &'static str,
    context: &'static str,
) -> Result<i64, PayloadError> {
    let end = checked_offset(*offset, 8, context)?;
    let value = read_i64_le(&bytes[*offset..end]).map_err(|_| PayloadError::Malformed(field))?;
    *offset = end;
    Ok(value)
}

pub(crate) fn read_u32_advance(
    bytes: &[u8],
    offset: &mut usize,
    field: &'static str,
    context: &'static str,
) -> Result<u32, PayloadError> {
    let end = checked_offset(*offset, 4, context)?;
    let value = read_u32_le(&bytes[*offset..end]).map_err(|_| PayloadError::Malformed(field))?;
    *offset = end;
    Ok(value)
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
pub(crate) const TID_SIZE: usize = 12;

/// Encode `tid` into `buf[..TID_SIZE]`.
///
/// Returns `PayloadError::Malformed` when the block number exceeds the 24-bit
/// wire field (`> 0x00FF_FFFF`).
pub(crate) fn encode_tid(buf: &mut [u8; TID_SIZE], tid: TupleId) -> Result<(), PayloadError> {
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
pub(crate) fn decode_tid(bytes: &[u8]) -> Result<TupleId, PayloadError> {
    if bytes.len() < TID_SIZE {
        return Err(PayloadError::Truncated {
            needed: TID_SIZE,
            have: bytes.len(),
        });
    }
    let rel_raw = read_u32_le(&bytes[0..4]).map_err(|_| PayloadError::Malformed("tid relation"))?;
    let block_word = read_u32_le(&bytes[4..8]).map_err(|_| PayloadError::Malformed("tid block"))?;
    if block_word & 0xFF00_0000 != 0 {
        return Err(PayloadError::Malformed("tid block reserved bits set"));
    }
    let reserved =
        read_u16_le(&bytes[10..12]).map_err(|_| PayloadError::Malformed("tid reserved bytes"))?;
    if reserved != 0 {
        return Err(PayloadError::Malformed("tid reserved bytes must be zero"));
    }
    let block_raw = block_word & 0x00FF_FFFF;
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
pub(crate) const PAGE_ID_SIZE: usize = 8;

/// Encode `page` into `buf[..PAGE_ID_SIZE]`.
pub(crate) fn encode_page_id(buf: &mut [u8; PAGE_ID_SIZE], page: PageId) {
    write_u32_le(&mut buf[0..4], page.relation.oid().raw());
    write_u32_le(&mut buf[4..8], page.block.raw());
}

/// Decode a `PageId` from `bytes[..PAGE_ID_SIZE]`.
pub(crate) fn decode_page_id(bytes: &[u8]) -> Result<PageId, PayloadError> {
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
// Submodules
// ---------------------------------------------------------------------------

mod btree;
mod hash;
mod heap_delete;
mod heap_insert;
mod heap_update;
mod heap_update_delta;
mod sequence;
mod txn;
mod vector;

pub use btree::{BTreeOpKind, BTreeOpPayload};
pub use hash::{HashOpKind, HashOpPayload};
pub use heap_delete::{
    HeapDeleteInPlaceBatchEntry, HeapDeleteInPlaceBatchPayload, HeapDeleteInPlacePayload,
    HeapDeleteInPlaceRangeBatchPayload, HeapDeletePayload,
};
pub use heap_insert::{HeapInsertBatchEntry, HeapInsertBatchPayload, HeapInsertPayload};
pub use heap_update::{
    HEAP_UPDATE_HOT, HeapUpdateInPlaceBatchEntry, HeapUpdateInPlaceBatchPayload,
    HeapUpdateInPlacePayload, HeapUpdatePayload,
};
pub use heap_update_delta::{
    HeapUpdateInt32PairDeltaBatchPayload, HeapUpdateInt32PairDeltaRangeBatchPayload,
};
pub use sequence::{SequenceOpKind, SequenceOpPayload};
pub use txn::{AbortPayload, CheckpointPayload, CommitPayload, FullPageWritePayload};
pub use vector::{HnswOpKind, HnswOpPayload, IvfFlatOpKind, IvfFlatOpPayload};

// ---------------------------------------------------------------------------
// Shared decode helper
// ---------------------------------------------------------------------------

pub(crate) fn decode_bool_byte(value: u8, field: &'static str) -> Result<bool, PayloadError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(PayloadError::Malformed(field)),
    }
}

#[cfg(test)]
mod tests;
