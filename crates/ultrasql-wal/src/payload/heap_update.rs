//! Heap update payload codecs (in-place + batch, MVCC tuple moves).

use ultrasql_core::endian::{
    read_u16_le, read_u32_le, read_u64_le, write_u16_le, write_u32_le, write_u64_le,
};
use ultrasql_core::{CommandId, PageId, TupleId, Xid};

use super::{
    MAX_VARIABLE_PAYLOAD_BYTES, PAGE_ID_SIZE, PayloadError, TID_SIZE, checked_len_sum,
    checked_offset, decode_page_id, decode_tid, encode_page_id, encode_tid, require_exact_len,
};

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
            .map_err(|_| PayloadError::Malformed("heap_update new_len overflow"))?;
        let total = checked_len_sum(
            &[FIXED, self.new_tuple_bytes.len()],
            "heap_update length overflow",
        )?;
        let mut out = vec![0_u8; total];
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
        if bytes[25] != 0 || bytes[26] != 0 || bytes[27] != 0 {
            return Err(PayloadError::Malformed(
                "heap_update reserved bytes must be zero",
            ));
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
        let needed = checked_len_sum(&[FIXED, new_len], "heap_update length overflow")?;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        require_exact_len(bytes, needed)?;
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
        let total = checked_len_sum(
            &[
                FIXED,
                self.pre_image_bytes.len(),
                self.post_image_bytes.len(),
            ],
            "heap_update_in_place length overflow",
        )?;
        let mut out = vec![0_u8; total];
        let mut tid_buf = [0_u8; TID_SIZE];
        encode_tid(&mut tid_buf, self.tid)?;
        out[..TID_SIZE].copy_from_slice(&tid_buf);
        write_u64_le(&mut out[TID_SIZE..TID_SIZE + 8], self.writer_xid.raw());
        write_u32_le(&mut out[TID_SIZE + 8..TID_SIZE + 12], self.command_id.raw());
        write_u32_le(&mut out[TID_SIZE + 12..TID_SIZE + 16], pre_len);
        write_u32_le(&mut out[TID_SIZE + 16..TID_SIZE + 20], post_len);
        let pre_off = FIXED;
        let post_off = checked_len_sum(
            &[FIXED, self.pre_image_bytes.len()],
            "heap_update_in_place length overflow",
        )?;
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
        let needed = checked_len_sum(
            &[FIXED, pre_len, post_len],
            "heap_update_in_place length overflow",
        )?;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        require_exact_len(bytes, needed)?;
        let pre_off = FIXED;
        let post_off = checked_len_sum(&[FIXED, pre_len], "heap_update_in_place length overflow")?;
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
// HeapUpdateInPlaceBatchPayload
// ---------------------------------------------------------------------------

/// One slot rewrite inside a page-batched in-place UPDATE WAL record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapUpdateInPlaceBatchEntry {
    /// Slot number within [`HeapUpdateInPlaceBatchPayload::page`].
    pub slot: u16,
    /// Pre-update payload bytes for the fixed `(Int32, Int32)` row body.
    pub pre_image: [u8; 9],
    /// Post-update payload bytes for the fixed `(Int32, Int32)` row body.
    pub post_image: [u8; 9],
}

/// Payload for a `RecordType::HeapUpdateInPlaceBatch` WAL record.
///
/// Groups all in-place rewrites that touch the same heap page into a
/// single WAL record. The durability contract is page-level: the page
/// LSN is stamped with this record's LSN after the mutation record is
/// appended, so recovery either replays every entry in the batch or
/// skips the already-flushed page image.
///
/// Wire layout (little-endian):
/// ```text
///  0   8   page (PageId)
///  8   8   writer_xid (u64)
/// 16   4   command_id (u32)
/// 20   2   image_len (u16, currently 9)
/// 22   2   reserved (zero)
/// 24   4   entry_count (u32)
/// 28  ..   repeated entries:
///            slot (u16), reserved (u16), pre_image[image_len],
///            post_image[image_len]
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapUpdateInPlaceBatchPayload {
    /// Heap page containing every slot in [`Self::entries`].
    pub page: PageId,
    /// Transaction that performed the in-place UPDATE.
    pub writer_xid: Xid,
    /// Command within `writer_xid` that performed the UPDATE.
    pub command_id: CommandId,
    /// Slot rewrites on `page`, in ascending slot order.
    pub entries: Vec<HeapUpdateInPlaceBatchEntry>,
}

impl HeapUpdateInPlaceBatchPayload {
    const FIXED: usize = PAGE_ID_SIZE + 8 + 4 + 2 + 2 + 4;
    pub(crate) const IMAGE_LEN: usize = 9;
    const ENTRY_FIXED: usize = 4;

    /// Encode page-local `(slot, pre_image, post_image)` entries
    /// without first materializing [`HeapUpdateInPlaceBatchEntry`] values.
    ///
    /// This emits the same wire layout as [`Self::encode`]. Storage uses this
    /// on the fused in-place UPDATE path, where it already has fixed-size
    /// scratch tuples and would otherwise copy them into a second Vec before
    /// serializing.
    pub fn encode_entries(
        page: PageId,
        writer_xid: Xid,
        command_id: CommandId,
        entries: &[(u16, [u8; 9], [u8; 9])],
    ) -> Result<Vec<u8>, PayloadError> {
        let entry_count = u32::try_from(entries.len()).map_err(|_| {
            PayloadError::Malformed("heap_update_in_place_batch entry_count overflow")
        })?;
        let entry_size = checked_len_sum(
            &[Self::ENTRY_FIXED, Self::IMAGE_LEN, Self::IMAGE_LEN],
            "heap_update_in_place_batch length overflow",
        )?;
        let entries_len = entries
            .len()
            .checked_mul(entry_size)
            .ok_or(PayloadError::Malformed(
                "heap_update_in_place_batch length overflow",
            ))?;
        let total = checked_len_sum(
            &[Self::FIXED, entries_len],
            "heap_update_in_place_batch length overflow",
        )?;
        if total > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "heap_update_in_place_batch length exceeds ceiling",
            ));
        }

        let mut out = vec![0_u8; total];
        let mut page_buf = [0_u8; PAGE_ID_SIZE];
        encode_page_id(&mut page_buf, page);
        out[..PAGE_ID_SIZE].copy_from_slice(&page_buf);
        write_u64_le(&mut out[PAGE_ID_SIZE..PAGE_ID_SIZE + 8], writer_xid.raw());
        write_u32_le(
            &mut out[PAGE_ID_SIZE + 8..PAGE_ID_SIZE + 12],
            command_id.raw(),
        );
        write_u16_le(
            &mut out[PAGE_ID_SIZE + 12..PAGE_ID_SIZE + 14],
            u16::try_from(Self::IMAGE_LEN)
                .map_err(|_| PayloadError::Malformed("heap_update_in_place_batch image_len"))?,
        );
        write_u16_le(&mut out[PAGE_ID_SIZE + 14..PAGE_ID_SIZE + 16], 0);
        write_u32_le(&mut out[PAGE_ID_SIZE + 16..Self::FIXED], entry_count);

        let mut off = Self::FIXED;
        for (slot, pre_image, post_image) in entries {
            let slot_end = checked_offset(off, 2, "heap_update_in_place_batch length overflow")?;
            write_u16_le(&mut out[off..slot_end], *slot);
            let reserved_end =
                checked_offset(slot_end, 2, "heap_update_in_place_batch length overflow")?;
            write_u16_le(&mut out[slot_end..reserved_end], 0);
            off = reserved_end;
            let pre_end = checked_offset(
                off,
                Self::IMAGE_LEN,
                "heap_update_in_place_batch length overflow",
            )?;
            out[off..pre_end].copy_from_slice(pre_image);
            off = pre_end;
            let post_end = checked_offset(
                off,
                Self::IMAGE_LEN,
                "heap_update_in_place_batch length overflow",
            )?;
            out[off..post_end].copy_from_slice(post_image);
            off = post_end;
        }
        Ok(out)
    }

    /// Encode this payload into a freshly-allocated byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        let entry_count = u32::try_from(self.entries.len()).map_err(|_| {
            PayloadError::Malformed("heap_update_in_place_batch entry_count overflow")
        })?;
        let entry_size = checked_len_sum(
            &[Self::ENTRY_FIXED, Self::IMAGE_LEN, Self::IMAGE_LEN],
            "heap_update_in_place_batch length overflow",
        )?;
        let entries_len =
            self.entries
                .len()
                .checked_mul(entry_size)
                .ok_or(PayloadError::Malformed(
                    "heap_update_in_place_batch length overflow",
                ))?;
        let total = checked_len_sum(
            &[Self::FIXED, entries_len],
            "heap_update_in_place_batch length overflow",
        )?;
        if total > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "heap_update_in_place_batch length exceeds ceiling",
            ));
        }

        let mut out = vec![0_u8; total];
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
        write_u16_le(
            &mut out[PAGE_ID_SIZE + 12..PAGE_ID_SIZE + 14],
            u16::try_from(Self::IMAGE_LEN)
                .map_err(|_| PayloadError::Malformed("heap_update_in_place_batch image_len"))?,
        );
        write_u16_le(&mut out[PAGE_ID_SIZE + 14..PAGE_ID_SIZE + 16], 0);
        write_u32_le(&mut out[PAGE_ID_SIZE + 16..Self::FIXED], entry_count);

        let mut off = Self::FIXED;
        for entry in &self.entries {
            let slot_end = checked_offset(off, 2, "heap_update_in_place_batch length overflow")?;
            write_u16_le(&mut out[off..slot_end], entry.slot);
            let reserved_end =
                checked_offset(slot_end, 2, "heap_update_in_place_batch length overflow")?;
            write_u16_le(&mut out[slot_end..reserved_end], 0);
            off = reserved_end;
            let pre_end = checked_offset(
                off,
                Self::IMAGE_LEN,
                "heap_update_in_place_batch length overflow",
            )?;
            out[off..pre_end].copy_from_slice(&entry.pre_image);
            off = pre_end;
            let post_end = checked_offset(
                off,
                Self::IMAGE_LEN,
                "heap_update_in_place_batch length overflow",
            )?;
            out[off..post_end].copy_from_slice(&entry.post_image);
            off = post_end;
        }
        Ok(out)
    }

    /// Decode a `HeapUpdateInPlaceBatchPayload` from a byte slice.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        if bytes.len() < Self::FIXED {
            return Err(PayloadError::Truncated {
                needed: Self::FIXED,
                have: bytes.len(),
            });
        }
        let page = decode_page_id(bytes)?;
        let writer_xid = Xid::new(
            read_u64_le(&bytes[PAGE_ID_SIZE..PAGE_ID_SIZE + 8])
                .map_err(|_| PayloadError::Malformed("heap_update_in_place_batch writer_xid"))?,
        );
        let command_id = CommandId::new(
            read_u32_le(&bytes[PAGE_ID_SIZE + 8..PAGE_ID_SIZE + 12])
                .map_err(|_| PayloadError::Malformed("heap_update_in_place_batch command_id"))?,
        );
        let image_len = usize::from(
            read_u16_le(&bytes[PAGE_ID_SIZE + 12..PAGE_ID_SIZE + 14])
                .map_err(|_| PayloadError::Malformed("heap_update_in_place_batch image_len"))?,
        );
        let reserved = read_u16_le(&bytes[PAGE_ID_SIZE + 14..PAGE_ID_SIZE + 16])
            .map_err(|_| PayloadError::Malformed("heap_update_in_place_batch reserved"))?;
        if reserved != 0 {
            return Err(PayloadError::Malformed(
                "heap_update_in_place_batch reserved bits set",
            ));
        }
        if image_len != Self::IMAGE_LEN {
            return Err(PayloadError::Malformed(
                "heap_update_in_place_batch unsupported image length",
            ));
        }
        let entry_count = usize::try_from(
            read_u32_le(&bytes[PAGE_ID_SIZE + 16..Self::FIXED])
                .map_err(|_| PayloadError::Malformed("heap_update_in_place_batch entry_count"))?,
        )
        .map_err(|_| PayloadError::Malformed("heap_update_in_place_batch entry_count usize"))?;
        let entry_size = checked_len_sum(
            &[Self::ENTRY_FIXED, image_len, image_len],
            "heap_update_in_place_batch length overflow",
        )?;
        let entries_len = entry_count
            .checked_mul(entry_size)
            .ok_or(PayloadError::Malformed(
                "heap_update_in_place_batch length overflow",
            ))?;
        let needed = checked_len_sum(
            &[Self::FIXED, entries_len],
            "heap_update_in_place_batch length overflow",
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
            let slot_end = checked_offset(off, 2, "heap_update_in_place_batch length overflow")?;
            let slot = read_u16_le(&bytes[off..slot_end])
                .map_err(|_| PayloadError::Malformed("heap_update_in_place_batch slot"))?;
            let reserved_end =
                checked_offset(slot_end, 2, "heap_update_in_place_batch length overflow")?;
            let entry_reserved = read_u16_le(&bytes[slot_end..reserved_end]).map_err(|_| {
                PayloadError::Malformed("heap_update_in_place_batch entry reserved")
            })?;
            if entry_reserved != 0 {
                return Err(PayloadError::Malformed(
                    "heap_update_in_place_batch entry reserved bits set",
                ));
            }
            off = reserved_end;
            let mut pre_image = [0_u8; Self::IMAGE_LEN];
            let pre_end = checked_offset(
                off,
                Self::IMAGE_LEN,
                "heap_update_in_place_batch length overflow",
            )?;
            pre_image.copy_from_slice(&bytes[off..pre_end]);
            off = pre_end;
            let mut post_image = [0_u8; Self::IMAGE_LEN];
            let post_end = checked_offset(
                off,
                Self::IMAGE_LEN,
                "heap_update_in_place_batch length overflow",
            )?;
            post_image.copy_from_slice(&bytes[off..post_end]);
            off = post_end;
            entries.push(HeapUpdateInPlaceBatchEntry {
                slot,
                pre_image,
                post_image,
            });
        }

        Ok(Self {
            page,
            writer_xid,
            command_id,
            entries,
        })
    }
}

// ---------------------------------------------------------------------------
