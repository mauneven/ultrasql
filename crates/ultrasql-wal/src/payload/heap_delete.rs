//! Heap delete payload codecs (in-place, batch, range batch, classic).

use ultrasql_core::endian::{
    read_u16_le, read_u32_le, read_u64_le, write_u16_le, write_u32_le, write_u64_le,
};
use ultrasql_core::{CommandId, PageId, TupleId, Xid};

use super::{
    MAX_VARIABLE_PAYLOAD_BYTES, PAGE_ID_SIZE, PayloadError, TID_SIZE, checked_len_sum,
    checked_offset, decode_page_id, decode_tid, encode_page_id, encode_tid, require_exact_len,
};

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
// HeapDeleteInPlaceBatchPayload
// ---------------------------------------------------------------------------

/// One slot stamp inside a page-batched in-place DELETE WAL record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapDeleteInPlaceBatchEntry {
    /// Slot number within [`HeapDeleteInPlaceBatchPayload::page`].
    pub slot: u16,
}

/// Payload for a `RecordType::HeapDeleteInPlaceBatch` WAL record.
///
/// Groups all in-place delete stamps that touch the same heap page into a
/// single WAL record. The durability contract matches
/// [`HeapUpdateInPlaceBatchPayload`]: the page LSN is stamped with this
/// record's LSN after append, so recovery either replays every slot stamp in
/// the batch or skips an already-flushed page image.
///
/// Wire layout (little-endian):
/// ```text
///  0   8   page (PageId)
///  8   8   xmax (u64)
/// 16   4   cmax (u32)
/// 20   4   reserved (zero)
/// 24   4   entry_count (u32)
/// 28  ..   repeated entries: slot (u16), reserved (u16)
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapDeleteInPlaceBatchPayload {
    /// Heap page containing every slot in [`Self::entries`].
    pub page: PageId,
    /// Transaction that performed the delete.
    pub xmax: Xid,
    /// Command within `xmax` that performed the delete.
    pub cmax: CommandId,
    /// Slots stamped on `page`, in ascending slot order.
    pub entries: Vec<HeapDeleteInPlaceBatchEntry>,
}

impl HeapDeleteInPlaceBatchPayload {
    const FIXED: usize = PAGE_ID_SIZE + 8 + 4 + 4 + 4;
    const ENTRY_SIZE: usize = 4;

    /// Encode page-local deleted slots without first materializing
    /// [`HeapDeleteInPlaceBatchEntry`] values.
    ///
    /// This emits the same wire layout as [`Self::encode`]. Storage uses this
    /// on the fused in-place DELETE path, where it already has a page-local
    /// slot scratch buffer.
    pub fn encode_slots(
        page: PageId,
        xmax: Xid,
        cmax: CommandId,
        slots: &[u16],
    ) -> Result<Vec<u8>, PayloadError> {
        let mut out = Vec::new();
        Self::encode_slots_into(page, xmax, cmax, slots, &mut out)?;
        Ok(out)
    }

    /// Encode page-local deleted slots into `out`, reusing its allocation.
    ///
    /// `out` is cleared before bytes are written. The resulting byte layout is
    /// identical to [`Self::encode_slots`].
    pub fn encode_slots_into(
        page: PageId,
        xmax: Xid,
        cmax: CommandId,
        slots: &[u16],
        out: &mut Vec<u8>,
    ) -> Result<(), PayloadError> {
        let entry_count = u32::try_from(slots.len()).map_err(|_| {
            PayloadError::Malformed("heap_delete_in_place_batch entry_count overflow")
        })?;
        let entries_len =
            slots
                .len()
                .checked_mul(Self::ENTRY_SIZE)
                .ok_or(PayloadError::Malformed(
                    "heap_delete_in_place_batch length overflow",
                ))?;
        let total = checked_len_sum(
            &[Self::FIXED, entries_len],
            "heap_delete_in_place_batch length overflow",
        )?;
        if total > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "heap_delete_in_place_batch length exceeds ceiling",
            ));
        }

        out.clear();
        out.resize(total, 0);
        let mut page_buf = [0_u8; PAGE_ID_SIZE];
        encode_page_id(&mut page_buf, page);
        out[..PAGE_ID_SIZE].copy_from_slice(&page_buf);
        write_u64_le(&mut out[PAGE_ID_SIZE..PAGE_ID_SIZE + 8], xmax.raw());
        write_u32_le(&mut out[PAGE_ID_SIZE + 8..PAGE_ID_SIZE + 12], cmax.raw());
        write_u32_le(&mut out[PAGE_ID_SIZE + 12..PAGE_ID_SIZE + 16], 0);
        write_u32_le(&mut out[PAGE_ID_SIZE + 16..Self::FIXED], entry_count);

        let mut off = Self::FIXED;
        for slot in slots {
            let slot_end = checked_offset(off, 2, "heap_delete_in_place_batch length overflow")?;
            write_u16_le(&mut out[off..slot_end], *slot);
            let reserved_end =
                checked_offset(slot_end, 2, "heap_delete_in_place_batch length overflow")?;
            write_u16_le(&mut out[slot_end..reserved_end], 0);
            off = reserved_end;
        }
        Ok(())
    }

    /// Encode this payload into a freshly-allocated byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        let entry_count = u32::try_from(self.entries.len()).map_err(|_| {
            PayloadError::Malformed("heap_delete_in_place_batch entry_count overflow")
        })?;
        let entries_len =
            self.entries
                .len()
                .checked_mul(Self::ENTRY_SIZE)
                .ok_or(PayloadError::Malformed(
                    "heap_delete_in_place_batch length overflow",
                ))?;
        let total = checked_len_sum(
            &[Self::FIXED, entries_len],
            "heap_delete_in_place_batch length overflow",
        )?;
        if total > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "heap_delete_in_place_batch length exceeds ceiling",
            ));
        }

        let mut out = vec![0_u8; total];
        let mut page_buf = [0_u8; PAGE_ID_SIZE];
        encode_page_id(&mut page_buf, self.page);
        out[..PAGE_ID_SIZE].copy_from_slice(&page_buf);
        write_u64_le(&mut out[PAGE_ID_SIZE..PAGE_ID_SIZE + 8], self.xmax.raw());
        write_u32_le(
            &mut out[PAGE_ID_SIZE + 8..PAGE_ID_SIZE + 12],
            self.cmax.raw(),
        );
        write_u32_le(&mut out[PAGE_ID_SIZE + 12..PAGE_ID_SIZE + 16], 0);
        write_u32_le(&mut out[PAGE_ID_SIZE + 16..Self::FIXED], entry_count);

        let mut off = Self::FIXED;
        for entry in &self.entries {
            let slot_end = checked_offset(off, 2, "heap_delete_in_place_batch length overflow")?;
            write_u16_le(&mut out[off..slot_end], entry.slot);
            let reserved_end =
                checked_offset(slot_end, 2, "heap_delete_in_place_batch length overflow")?;
            write_u16_le(&mut out[slot_end..reserved_end], 0);
            off = reserved_end;
        }
        Ok(out)
    }

    /// Decode a `HeapDeleteInPlaceBatchPayload` from a byte slice.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        if bytes.len() < Self::FIXED {
            return Err(PayloadError::Truncated {
                needed: Self::FIXED,
                have: bytes.len(),
            });
        }
        let page = decode_page_id(bytes)?;
        let xmax = Xid::new(
            read_u64_le(&bytes[PAGE_ID_SIZE..PAGE_ID_SIZE + 8])
                .map_err(|_| PayloadError::Malformed("heap_delete_in_place_batch xmax"))?,
        );
        let cmax = CommandId::new(
            read_u32_le(&bytes[PAGE_ID_SIZE + 8..PAGE_ID_SIZE + 12])
                .map_err(|_| PayloadError::Malformed("heap_delete_in_place_batch cmax"))?,
        );
        let reserved = read_u32_le(&bytes[PAGE_ID_SIZE + 12..PAGE_ID_SIZE + 16])
            .map_err(|_| PayloadError::Malformed("heap_delete_in_place_batch reserved"))?;
        if reserved != 0 {
            return Err(PayloadError::Malformed(
                "heap_delete_in_place_batch reserved bits set",
            ));
        }
        let entry_count = usize::try_from(
            read_u32_le(&bytes[PAGE_ID_SIZE + 16..Self::FIXED])
                .map_err(|_| PayloadError::Malformed("heap_delete_in_place_batch entry_count"))?,
        )
        .map_err(|_| PayloadError::Malformed("heap_delete_in_place_batch entry_count usize"))?;
        let entries_len =
            entry_count
                .checked_mul(Self::ENTRY_SIZE)
                .ok_or(PayloadError::Malformed(
                    "heap_delete_in_place_batch length overflow",
                ))?;
        let needed = checked_len_sum(
            &[Self::FIXED, entries_len],
            "heap_delete_in_place_batch length overflow",
        )?;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        require_exact_len(bytes, needed)?;

        let mut entries = Vec::with_capacity(entry_count);
        let mut off = Self::FIXED;
        for _ in 0..entry_count {
            let slot_end = checked_offset(off, 2, "heap_delete_in_place_batch length overflow")?;
            let slot = read_u16_le(&bytes[off..slot_end])
                .map_err(|_| PayloadError::Malformed("heap_delete_in_place_batch slot"))?;
            let reserved_end =
                checked_offset(slot_end, 2, "heap_delete_in_place_batch length overflow")?;
            let entry_reserved = read_u16_le(&bytes[slot_end..reserved_end]).map_err(|_| {
                PayloadError::Malformed("heap_delete_in_place_batch entry reserved")
            })?;
            if entry_reserved != 0 {
                return Err(PayloadError::Malformed(
                    "heap_delete_in_place_batch entry reserved bits set",
                ));
            }
            entries.push(HeapDeleteInPlaceBatchEntry { slot });
            off = reserved_end;
        }

        Ok(Self {
            page,
            xmax,
            cmax,
            entries,
        })
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
        if bytes[TID_SIZE + 12..SIZE].iter().any(|byte| *byte != 0) {
            return Err(PayloadError::Malformed(
                "heap_delete reserved bytes must be zero",
            ));
        }
        require_exact_len(bytes, SIZE)?;
        Ok(Self { tid, xmax, cmax })
    }
}

// ---------------------------------------------------------------------------
// HeapDeleteInPlaceRangeBatchPayload
// ---------------------------------------------------------------------------

/// Payload for a `RecordType::HeapDeleteInPlaceRangeBatch` WAL record.
///
/// Encodes a contiguous page-local slot range. This is equivalent to a
/// [`HeapDeleteInPlaceBatchPayload`] whose entries are
/// `first_slot..first_slot + slot_count`, but avoids writing one slot entry per
/// tuple for dense page deletes.
///
/// Wire layout (little-endian):
/// ```text
///  0   8   page (PageId)
///  8   8   xmax (u64)
/// 16   4   cmax (u32)
/// 20   2   first_slot (u16)
/// 22   2   slot_count (u16)
/// 24   4   reserved (zero)
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapDeleteInPlaceRangeBatchPayload {
    /// Heap page containing the contiguous slot range.
    pub page: PageId,
    /// Transaction that performed the delete.
    pub xmax: Xid,
    /// Command within `xmax` that performed the delete.
    pub cmax: CommandId,
    /// First slot stamped on `page`.
    pub first_slot: u16,
    /// Number of contiguous slots stamped on `page`.
    pub slot_count: u16,
}

impl HeapDeleteInPlaceRangeBatchPayload {
    const FIXED: usize = PAGE_ID_SIZE + 8 + 4 + 2 + 2 + 4;

    /// Encode a contiguous page-local delete slot range.
    pub fn encode_range(
        page: PageId,
        xmax: Xid,
        cmax: CommandId,
        first_slot: u16,
        slot_count: u16,
    ) -> Result<Vec<u8>, PayloadError> {
        let mut out = Vec::new();
        Self::encode_range_into(page, xmax, cmax, first_slot, slot_count, &mut out)?;
        Ok(out)
    }

    /// Encode a contiguous page-local delete slot range into `out`.
    ///
    /// `out` is cleared before bytes are written.
    pub fn encode_range_into(
        page: PageId,
        xmax: Xid,
        cmax: CommandId,
        first_slot: u16,
        slot_count: u16,
        out: &mut Vec<u8>,
    ) -> Result<(), PayloadError> {
        if slot_count == 0 {
            return Err(PayloadError::Malformed(
                "heap_delete_in_place_range_batch slot_count must be nonzero",
            ));
        }
        let last_slot = u32::from(first_slot) + u32::from(slot_count) - 1;
        if last_slot > u32::from(u16::MAX) {
            return Err(PayloadError::Malformed(
                "heap_delete_in_place_range_batch slot range overflow",
            ));
        }

        out.clear();
        out.resize(Self::FIXED, 0);
        let mut page_buf = [0_u8; PAGE_ID_SIZE];
        encode_page_id(&mut page_buf, page);
        out[..PAGE_ID_SIZE].copy_from_slice(&page_buf);
        write_u64_le(&mut out[PAGE_ID_SIZE..PAGE_ID_SIZE + 8], xmax.raw());
        write_u32_le(&mut out[PAGE_ID_SIZE + 8..PAGE_ID_SIZE + 12], cmax.raw());
        write_u16_le(&mut out[PAGE_ID_SIZE + 12..PAGE_ID_SIZE + 14], first_slot);
        write_u16_le(&mut out[PAGE_ID_SIZE + 14..PAGE_ID_SIZE + 16], slot_count);
        write_u32_le(&mut out[PAGE_ID_SIZE + 16..Self::FIXED], 0);
        Ok(())
    }

    /// Encode this payload into a byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        Self::encode_range(
            self.page,
            self.xmax,
            self.cmax,
            self.first_slot,
            self.slot_count,
        )
    }

    /// Decode a `HeapDeleteInPlaceRangeBatchPayload` from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        require_exact_len(bytes, Self::FIXED)?;
        let page = decode_page_id(bytes)?;
        let xmax = Xid::new(
            read_u64_le(&bytes[PAGE_ID_SIZE..PAGE_ID_SIZE + 8])
                .map_err(|_| PayloadError::Malformed("heap_delete_in_place_range_batch xmax"))?,
        );
        let cmax = CommandId::new(
            read_u32_le(&bytes[PAGE_ID_SIZE + 8..PAGE_ID_SIZE + 12])
                .map_err(|_| PayloadError::Malformed("heap_delete_in_place_range_batch cmax"))?,
        );
        let first_slot = read_u16_le(&bytes[PAGE_ID_SIZE + 12..PAGE_ID_SIZE + 14])
            .map_err(|_| PayloadError::Malformed("heap_delete_in_place_range_batch first_slot"))?;
        let slot_count = read_u16_le(&bytes[PAGE_ID_SIZE + 14..PAGE_ID_SIZE + 16])
            .map_err(|_| PayloadError::Malformed("heap_delete_in_place_range_batch slot_count"))?;
        if slot_count == 0 {
            return Err(PayloadError::Malformed(
                "heap_delete_in_place_range_batch slot_count must be nonzero",
            ));
        }
        let last_slot = u32::from(first_slot) + u32::from(slot_count) - 1;
        if last_slot > u32::from(u16::MAX) {
            return Err(PayloadError::Malformed(
                "heap_delete_in_place_range_batch slot range overflow",
            ));
        }
        let reserved = read_u32_le(&bytes[PAGE_ID_SIZE + 16..Self::FIXED])
            .map_err(|_| PayloadError::Malformed("heap_delete_in_place_range_batch reserved"))?;
        if reserved != 0 {
            return Err(PayloadError::Malformed(
                "heap_delete_in_place_range_batch reserved bits set",
            ));
        }
        Ok(Self {
            page,
            xmax,
            cmax,
            first_slot,
            slot_count,
        })
    }
}
