//! Heap insert payload codecs (single + batch).

use ultrasql_core::endian::{read_u16_le, read_u32_le, write_u16_le, write_u32_le};
use ultrasql_core::{PageId, TupleId};

use super::{
    MAX_VARIABLE_PAYLOAD_BYTES, PAGE_ID_SIZE, PayloadError, TID_SIZE, checked_len_sum,
    checked_offset, decode_page_id, decode_tid, encode_page_id, encode_tid, require_exact_len,
};

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
            .map_err(|_| PayloadError::Malformed("heap_insert tuple_len overflow"))?;
        let total = checked_len_sum(
            &[TID_SIZE, 4, self.tuple_bytes.len()],
            "heap_insert length overflow",
        )?;
        let mut out = vec![0_u8; total];
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
        let needed = checked_len_sum(&[FIXED, tuple_len], "heap_insert length overflow")?;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        require_exact_len(bytes, needed)?;
        Ok(Self {
            tid,
            tuple_bytes: bytes[FIXED..needed].to_vec(),
        })
    }
}

// ---------------------------------------------------------------------------
// HeapInsertBatchPayload
// ---------------------------------------------------------------------------

/// One tuple inserted into a page-level heap-insert batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapInsertBatchEntry {
    /// Slot assigned on [`HeapInsertBatchPayload::page`].
    pub slot: u16,
    /// Full on-page tuple bytes: tuple header followed by user-data attributes.
    pub tuple_bytes: Vec<u8>,
}

/// Payload for a `RecordType::HeapInsertBatch` WAL record.
///
/// Groups inserts that landed on the same heap page into one WAL record while
/// preserving each page-local slot assignment for exact redo.
///
/// Wire layout (little-endian):
/// ```text
///  0   8   page (PageId)
///  8   4   entry_count (u32)
/// 12  ..   repeated entries:
///            slot (u16), reserved (u16), tuple_len (u32), tuple_bytes
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeapInsertBatchPayload {
    /// Heap page containing every slot in [`Self::entries`].
    pub page: PageId,
    /// Slot payloads inserted on `page`, in slot order.
    pub entries: Vec<HeapInsertBatchEntry>,
}

impl HeapInsertBatchPayload {
    const FIXED: usize = PAGE_ID_SIZE + 4;
    const ENTRY_FIXED: usize = 2 + 2 + 4;

    /// Encode this payload into a freshly-allocated byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        let entry_count = u32::try_from(self.entries.len())
            .map_err(|_| PayloadError::Malformed("heap_insert_batch entry_count overflow"))?;
        let mut total = Self::FIXED;
        for entry in &self.entries {
            if entry.tuple_bytes.len() > MAX_VARIABLE_PAYLOAD_BYTES {
                return Err(PayloadError::Malformed(
                    "heap_insert_batch tuple_len exceeds ceiling",
                ));
            }
            total = checked_len_sum(
                &[total, Self::ENTRY_FIXED, entry.tuple_bytes.len()],
                "heap_insert_batch length overflow",
            )?;
        }
        if total > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "heap_insert_batch length exceeds ceiling",
            ));
        }

        let mut out = vec![0_u8; total];
        let mut page_buf = [0_u8; PAGE_ID_SIZE];
        encode_page_id(&mut page_buf, self.page);
        out[..PAGE_ID_SIZE].copy_from_slice(&page_buf);
        write_u32_le(&mut out[PAGE_ID_SIZE..Self::FIXED], entry_count);

        let mut off = Self::FIXED;
        for entry in &self.entries {
            let slot_end = checked_offset(off, 2, "heap_insert_batch length overflow")?;
            write_u16_le(&mut out[off..slot_end], entry.slot);
            let reserved_end = checked_offset(slot_end, 2, "heap_insert_batch length overflow")?;
            write_u16_le(&mut out[slot_end..reserved_end], 0);
            let len_end = checked_offset(reserved_end, 4, "heap_insert_batch length overflow")?;
            let tuple_len = u32::try_from(entry.tuple_bytes.len())
                .map_err(|_| PayloadError::Malformed("heap_insert_batch tuple_len overflow"))?;
            write_u32_le(&mut out[reserved_end..len_end], tuple_len);
            off = len_end;
            let tuple_end = checked_offset(
                off,
                entry.tuple_bytes.len(),
                "heap_insert_batch length overflow",
            )?;
            out[off..tuple_end].copy_from_slice(&entry.tuple_bytes);
            off = tuple_end;
        }
        Ok(out)
    }

    /// Decode a `HeapInsertBatchPayload` from a byte slice.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        if bytes.len() < Self::FIXED {
            return Err(PayloadError::Truncated {
                needed: Self::FIXED,
                have: bytes.len(),
            });
        }
        let page = decode_page_id(bytes)?;
        let entry_count = usize::try_from(
            read_u32_le(&bytes[PAGE_ID_SIZE..Self::FIXED])
                .map_err(|_| PayloadError::Malformed("heap_insert_batch entry_count"))?,
        )
        .map_err(|_| PayloadError::Malformed("heap_insert_batch entry_count usize"))?;

        let mut entries = Vec::with_capacity(entry_count);
        let mut off = Self::FIXED;
        for _ in 0..entry_count {
            let slot_end = checked_offset(off, 2, "heap_insert_batch length overflow")?;
            if bytes.len() < slot_end {
                return Err(PayloadError::Truncated {
                    needed: slot_end,
                    have: bytes.len(),
                });
            }
            let slot = read_u16_le(&bytes[off..slot_end])
                .map_err(|_| PayloadError::Malformed("heap_insert_batch slot"))?;
            let reserved_end = checked_offset(slot_end, 2, "heap_insert_batch length overflow")?;
            if bytes.len() < reserved_end {
                return Err(PayloadError::Truncated {
                    needed: reserved_end,
                    have: bytes.len(),
                });
            }
            let reserved = read_u16_le(&bytes[slot_end..reserved_end])
                .map_err(|_| PayloadError::Malformed("heap_insert_batch entry reserved"))?;
            if reserved != 0 {
                return Err(PayloadError::Malformed(
                    "heap_insert_batch entry reserved bits set",
                ));
            }
            let len_end = checked_offset(reserved_end, 4, "heap_insert_batch length overflow")?;
            if bytes.len() < len_end {
                return Err(PayloadError::Truncated {
                    needed: len_end,
                    have: bytes.len(),
                });
            }
            let tuple_len = usize::try_from(
                read_u32_le(&bytes[reserved_end..len_end])
                    .map_err(|_| PayloadError::Malformed("heap_insert_batch tuple_len"))?,
            )
            .map_err(|_| PayloadError::Malformed("heap_insert_batch tuple_len usize"))?;
            if tuple_len > MAX_VARIABLE_PAYLOAD_BYTES {
                return Err(PayloadError::Malformed(
                    "heap_insert_batch tuple_len exceeds ceiling",
                ));
            }
            off = len_end;
            let tuple_end = checked_offset(off, tuple_len, "heap_insert_batch length overflow")?;
            if bytes.len() < tuple_end {
                return Err(PayloadError::Truncated {
                    needed: tuple_end,
                    have: bytes.len(),
                });
            }
            entries.push(HeapInsertBatchEntry {
                slot,
                tuple_bytes: bytes[off..tuple_end].to_vec(),
            });
            off = tuple_end;
        }
        require_exact_len(bytes, off)?;
        Ok(Self { page, entries })
    }
}
