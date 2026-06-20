//! Heap update int32-pair-delta batch payload codecs.

use ultrasql_core::endian::{
    read_u16_le, read_u32_le, read_u64_le, write_i32_le, write_u16_le, write_u32_le, write_u64_le,
};
use ultrasql_core::{CommandId, PageId, Xid};

use super::{
    MAX_VARIABLE_PAYLOAD_BYTES, PAGE_ID_SIZE, PayloadError, checked_len_sum, checked_offset,
    decode_page_id, encode_page_id, require_exact_len,
};

// HeapUpdateInt32PairDeltaBatchPayload
// ---------------------------------------------------------------------------

/// Compact page-batched WAL payload for fixed `(Int32, Int32)` delta updates.
///
/// This represents the fused `UPDATE t SET col = col + delta` shape over one
/// heap page. Recovery reads each slot's current payload, records that
/// pre-image in the in-memory undo log, applies `delta` to `target_col`, and
/// stamps the tuple header with `writer_xid`/`command_id`.
///
/// Wire layout (little-endian):
/// ```text
///  0   8   page (PageId)
///  8   8   writer_xid (u64)
/// 16   4   command_id (u32)
/// 20   1   target_col (u8; 0 or 1)
/// 21   3   reserved (zero)
/// 24   4   delta (i32)
/// 28   4   entry_count (u32)
/// 32  ..   repeated entries: slot (u16), reserved (u16)
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapUpdateInt32PairDeltaBatchPayload {
    /// Heap page containing every slot in [`Self::slots`].
    pub page: PageId,
    /// Transaction that performed the in-place UPDATE.
    pub writer_xid: Xid,
    /// Command within `writer_xid` that performed the UPDATE.
    pub command_id: CommandId,
    /// Target payload column: `0` for id, `1` for value.
    pub target_col: u8,
    /// Signed delta applied to `target_col`.
    pub delta: i32,
    /// Slots updated on `page`, in ascending slot order.
    pub slots: Vec<u16>,
}

impl HeapUpdateInt32PairDeltaBatchPayload {
    const FIXED: usize = PAGE_ID_SIZE + 8 + 4 + 1 + 3 + 4 + 4;
    const ENTRY_SIZE: usize = 4;

    /// Encode page-local updated slots without materializing per-row images.
    pub fn encode_slots(
        page: PageId,
        writer_xid: Xid,
        command_id: CommandId,
        target_col: u8,
        delta: i32,
        slots: &[u16],
    ) -> Result<Vec<u8>, PayloadError> {
        let mut out = Vec::new();
        Self::encode_slots_into(
            page, writer_xid, command_id, target_col, delta, slots, &mut out,
        )?;
        Ok(out)
    }

    /// Encode page-local updated slots into `out`, reusing its allocation.
    ///
    /// `out` is cleared before bytes are written. The resulting byte layout is
    /// identical to [`Self::encode_slots`].
    pub fn encode_slots_into(
        page: PageId,
        writer_xid: Xid,
        command_id: CommandId,
        target_col: u8,
        delta: i32,
        slots: &[u16],
        out: &mut Vec<u8>,
    ) -> Result<(), PayloadError> {
        if target_col > 1 {
            return Err(PayloadError::Malformed(
                "heap_update_int32_pair_delta_batch target_col out of range",
            ));
        }
        let entry_count = u32::try_from(slots.len()).map_err(|_| {
            PayloadError::Malformed("heap_update_int32_pair_delta_batch entry_count overflow")
        })?;
        let entries_len =
            slots
                .len()
                .checked_mul(Self::ENTRY_SIZE)
                .ok_or(PayloadError::Malformed(
                    "heap_update_int32_pair_delta_batch length overflow",
                ))?;
        let total = checked_len_sum(
            &[Self::FIXED, entries_len],
            "heap_update_int32_pair_delta_batch length overflow",
        )?;
        if total > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "heap_update_int32_pair_delta_batch length exceeds ceiling",
            ));
        }

        out.clear();
        out.resize(total, 0);
        let mut page_buf = [0_u8; PAGE_ID_SIZE];
        encode_page_id(&mut page_buf, page);
        out[..PAGE_ID_SIZE].copy_from_slice(&page_buf);
        write_u64_le(&mut out[PAGE_ID_SIZE..PAGE_ID_SIZE + 8], writer_xid.raw());
        write_u32_le(
            &mut out[PAGE_ID_SIZE + 8..PAGE_ID_SIZE + 12],
            command_id.raw(),
        );
        out[PAGE_ID_SIZE + 12] = target_col;
        write_i32_le(&mut out[PAGE_ID_SIZE + 16..PAGE_ID_SIZE + 20], delta);
        write_u32_le(&mut out[PAGE_ID_SIZE + 20..Self::FIXED], entry_count);

        let mut off = Self::FIXED;
        for slot in slots {
            let slot_end =
                checked_offset(off, 2, "heap_update_int32_pair_delta_batch length overflow")?;
            write_u16_le(&mut out[off..slot_end], *slot);
            let reserved_end = checked_offset(
                slot_end,
                2,
                "heap_update_int32_pair_delta_batch length overflow",
            )?;
            write_u16_le(&mut out[slot_end..reserved_end], 0);
            off = reserved_end;
        }
        Ok(())
    }

    /// Encode this payload into a freshly-allocated byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        Self::encode_slots(
            self.page,
            self.writer_xid,
            self.command_id,
            self.target_col,
            self.delta,
            &self.slots,
        )
    }

    /// Decode a `HeapUpdateInt32PairDeltaBatchPayload` from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        if bytes.len() < Self::FIXED {
            return Err(PayloadError::Truncated {
                needed: Self::FIXED,
                have: bytes.len(),
            });
        }
        let page = decode_page_id(bytes)?;
        let writer_xid = Xid::new(read_u64_le(&bytes[PAGE_ID_SIZE..PAGE_ID_SIZE + 8]).map_err(
            |_| PayloadError::Malformed("heap_update_int32_pair_delta_batch writer_xid"),
        )?);
        let command_id = CommandId::new(
            read_u32_le(&bytes[PAGE_ID_SIZE + 8..PAGE_ID_SIZE + 12]).map_err(|_| {
                PayloadError::Malformed("heap_update_int32_pair_delta_batch command_id")
            })?,
        );
        let target_col = bytes[PAGE_ID_SIZE + 12];
        if target_col > 1 {
            return Err(PayloadError::Malformed(
                "heap_update_int32_pair_delta_batch target_col out of range",
            ));
        }
        if bytes[PAGE_ID_SIZE + 13..PAGE_ID_SIZE + 16] != [0, 0, 0] {
            return Err(PayloadError::Malformed(
                "heap_update_int32_pair_delta_batch reserved bits set",
            ));
        }
        let delta_word = read_u32_le(&bytes[PAGE_ID_SIZE + 16..PAGE_ID_SIZE + 20])
            .map_err(|_| PayloadError::Malformed("heap_update_int32_pair_delta_batch delta"))?;
        let delta = i32::from_le_bytes(delta_word.to_le_bytes());
        let entry_count = usize::try_from(
            read_u32_le(&bytes[PAGE_ID_SIZE + 20..Self::FIXED]).map_err(|_| {
                PayloadError::Malformed("heap_update_int32_pair_delta_batch entry_count")
            })?,
        )
        .map_err(|_| {
            PayloadError::Malformed("heap_update_int32_pair_delta_batch entry_count usize")
        })?;
        let entries_len =
            entry_count
                .checked_mul(Self::ENTRY_SIZE)
                .ok_or(PayloadError::Malformed(
                    "heap_update_int32_pair_delta_batch length overflow",
                ))?;
        let needed = checked_len_sum(
            &[Self::FIXED, entries_len],
            "heap_update_int32_pair_delta_batch length overflow",
        )?;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        require_exact_len(bytes, needed)?;

        let mut slots = Vec::with_capacity(entry_count);
        let mut off = Self::FIXED;
        for _ in 0..entry_count {
            let slot_end =
                checked_offset(off, 2, "heap_update_int32_pair_delta_batch length overflow")?;
            let slot = read_u16_le(&bytes[off..slot_end])
                .map_err(|_| PayloadError::Malformed("heap_update_int32_pair_delta_batch slot"))?;
            let reserved_end = checked_offset(
                slot_end,
                2,
                "heap_update_int32_pair_delta_batch length overflow",
            )?;
            let reserved = read_u16_le(&bytes[slot_end..reserved_end]).map_err(|_| {
                PayloadError::Malformed("heap_update_int32_pair_delta_batch entry reserved")
            })?;
            if reserved != 0 {
                return Err(PayloadError::Malformed(
                    "heap_update_int32_pair_delta_batch entry reserved bits set",
                ));
            }
            slots.push(slot);
            off = reserved_end;
        }

        Ok(Self {
            page,
            writer_xid,
            command_id,
            target_col,
            delta,
            slots,
        })
    }
}

// ---------------------------------------------------------------------------
// HeapUpdateInt32PairDeltaRangeBatchPayload
// ---------------------------------------------------------------------------

/// Compact page-batched WAL payload for contiguous fixed `(Int32, Int32)` updates.
///
/// This is equivalent to [`HeapUpdateInt32PairDeltaBatchPayload`] whose
/// `slots` are `first_slot..first_slot + slot_count`, but avoids writing one
/// slot entry per tuple for dense page updates.
///
/// Wire layout (little-endian):
/// ```text
///  0   8   page (PageId)
///  8   8   writer_xid (u64)
/// 16   4   command_id (u32)
/// 20   1   target_col (u8; 0 or 1)
/// 21   3   reserved (zero)
/// 24   4   delta (i32)
/// 28   2   first_slot (u16)
/// 30   2   slot_count (u16)
/// 32   4   reserved (zero)
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapUpdateInt32PairDeltaRangeBatchPayload {
    /// Heap page containing the contiguous slot range.
    pub page: PageId,
    /// Transaction that performed the in-place UPDATE.
    pub writer_xid: Xid,
    /// Command within `writer_xid` that performed the UPDATE.
    pub command_id: CommandId,
    /// Target payload column: `0` for id, `1` for value.
    pub target_col: u8,
    /// Signed delta applied to `target_col`.
    pub delta: i32,
    /// First slot updated on `page`.
    pub first_slot: u16,
    /// Number of contiguous slots updated on `page`.
    pub slot_count: u16,
}

impl HeapUpdateInt32PairDeltaRangeBatchPayload {
    const FIXED: usize = PAGE_ID_SIZE + 8 + 4 + 1 + 3 + 4 + 2 + 2 + 4;

    /// Encode a contiguous page-local update slot range.
    pub fn encode_range(
        page: PageId,
        writer_xid: Xid,
        command_id: CommandId,
        target_col: u8,
        delta: i32,
        first_slot: u16,
        slot_count: u16,
    ) -> Result<Vec<u8>, PayloadError> {
        let mut out = Vec::new();
        Self {
            page,
            writer_xid,
            command_id,
            target_col,
            delta,
            first_slot,
            slot_count,
        }
        .encode_into(&mut out)?;
        Ok(out)
    }

    /// Encode a contiguous page-local update slot range into `out`.
    ///
    /// `out` is cleared before bytes are written.
    pub fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), PayloadError> {
        if self.target_col > 1 {
            return Err(PayloadError::Malformed(
                "heap_update_int32_pair_delta_range_batch target_col out of range",
            ));
        }
        if self.slot_count == 0 {
            return Err(PayloadError::Malformed(
                "heap_update_int32_pair_delta_range_batch slot_count must be nonzero",
            ));
        }
        let last_slot = u32::from(self.first_slot) + u32::from(self.slot_count) - 1;
        if last_slot > u32::from(u16::MAX) {
            return Err(PayloadError::Malformed(
                "heap_update_int32_pair_delta_range_batch slot range overflow",
            ));
        }

        out.clear();
        out.resize(Self::FIXED, 0);
        let mut page_buf = [0_u8; PAGE_ID_SIZE];
        encode_page_id(&mut page_buf, self.page);
        out[..PAGE_ID_SIZE].copy_from_slice(&page_buf);
        write_u64_le(
            &mut out[PAGE_ID_SIZE..PAGE_ID_SIZE + 8],
            self.writer_xid.raw(),
        );
        write_u32_le(
            &mut out[PAGE_ID_SIZE + 8..PAGE_ID_SIZE + 12],
            self.command_id.raw(),
        );
        out[PAGE_ID_SIZE + 12] = self.target_col;
        write_i32_le(&mut out[PAGE_ID_SIZE + 16..PAGE_ID_SIZE + 20], self.delta);
        write_u16_le(
            &mut out[PAGE_ID_SIZE + 20..PAGE_ID_SIZE + 22],
            self.first_slot,
        );
        write_u16_le(
            &mut out[PAGE_ID_SIZE + 22..PAGE_ID_SIZE + 24],
            self.slot_count,
        );
        write_u32_le(&mut out[PAGE_ID_SIZE + 24..Self::FIXED], 0);
        Ok(())
    }

    /// Encode this payload into a byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        let mut out = Vec::new();
        self.encode_into(&mut out)?;
        Ok(out)
    }

    /// Decode a `HeapUpdateInt32PairDeltaRangeBatchPayload` from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        require_exact_len(bytes, Self::FIXED)?;
        let page = decode_page_id(bytes)?;
        let writer_xid = Xid::new(read_u64_le(&bytes[PAGE_ID_SIZE..PAGE_ID_SIZE + 8]).map_err(
            |_| PayloadError::Malformed("heap_update_int32_pair_delta_range_batch writer_xid"),
        )?);
        let command_id = CommandId::new(
            read_u32_le(&bytes[PAGE_ID_SIZE + 8..PAGE_ID_SIZE + 12]).map_err(|_| {
                PayloadError::Malformed("heap_update_int32_pair_delta_range_batch command_id")
            })?,
        );
        let target_col = bytes[PAGE_ID_SIZE + 12];
        if target_col > 1 {
            return Err(PayloadError::Malformed(
                "heap_update_int32_pair_delta_range_batch target_col out of range",
            ));
        }
        if bytes[PAGE_ID_SIZE + 13..PAGE_ID_SIZE + 16] != [0, 0, 0] {
            return Err(PayloadError::Malformed(
                "heap_update_int32_pair_delta_range_batch reserved bits set",
            ));
        }
        let delta_word =
            read_u32_le(&bytes[PAGE_ID_SIZE + 16..PAGE_ID_SIZE + 20]).map_err(|_| {
                PayloadError::Malformed("heap_update_int32_pair_delta_range_batch delta")
            })?;
        let delta = i32::from_le_bytes(delta_word.to_le_bytes());
        let first_slot =
            read_u16_le(&bytes[PAGE_ID_SIZE + 20..PAGE_ID_SIZE + 22]).map_err(|_| {
                PayloadError::Malformed("heap_update_int32_pair_delta_range_batch first_slot")
            })?;
        let slot_count =
            read_u16_le(&bytes[PAGE_ID_SIZE + 22..PAGE_ID_SIZE + 24]).map_err(|_| {
                PayloadError::Malformed("heap_update_int32_pair_delta_range_batch slot_count")
            })?;
        if slot_count == 0 {
            return Err(PayloadError::Malformed(
                "heap_update_int32_pair_delta_range_batch slot_count must be nonzero",
            ));
        }
        let last_slot = u32::from(first_slot) + u32::from(slot_count) - 1;
        if last_slot > u32::from(u16::MAX) {
            return Err(PayloadError::Malformed(
                "heap_update_int32_pair_delta_range_batch slot range overflow",
            ));
        }
        let reserved = read_u32_le(&bytes[PAGE_ID_SIZE + 24..Self::FIXED]).map_err(|_| {
            PayloadError::Malformed("heap_update_int32_pair_delta_range_batch reserved")
        })?;
        if reserved != 0 {
            return Err(PayloadError::Malformed(
                "heap_update_int32_pair_delta_range_batch reserved bits set",
            ));
        }

        Ok(Self {
            page,
            writer_xid,
            command_id,
            target_col,
            delta,
            first_slot,
            slot_count,
        })
    }
}
