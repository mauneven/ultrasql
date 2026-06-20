//! Transaction-control and checkpoint/full-page-write payload codecs.

use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{Lsn, PageId, Xid};
use ultrasql_core::endian::{read_u32_le, read_u64_le, write_u32_le, write_u64_le};

use super::{
    PAGE_ID_SIZE, PayloadError, checked_offset, decode_page_id, encode_page_id, require_exact_len,
};

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
        require_exact_len(bytes, 16)?;
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
        require_exact_len(bytes, 8)?;
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
        require_exact_len(bytes, 24)?;
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
    /// # Errors
    ///
    /// Returns [`PayloadError::Malformed`] if `page_bytes.len() != PAGE_SIZE`.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        const FIXED: usize = PAGE_ID_SIZE + 4;
        if self.page_bytes.len() != PAGE_SIZE {
            return Err(PayloadError::Malformed("fpw page_bytes length"));
        }
        let page_bytes_len =
            u32::try_from(PAGE_SIZE).map_err(|_| PayloadError::Malformed("fpw page size"))?;
        let mut out = vec![0_u8; FIXED + PAGE_SIZE];
        let mut pid_buf = [0_u8; PAGE_ID_SIZE];
        encode_page_id(&mut pid_buf, self.page);
        out[..PAGE_ID_SIZE].copy_from_slice(&pid_buf);
        write_u32_le(&mut out[PAGE_ID_SIZE..PAGE_ID_SIZE + 4], page_bytes_len);
        out[FIXED..].copy_from_slice(&self.page_bytes);
        Ok(out)
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
        let needed = checked_offset(FIXED, page_bytes_len, "fpw length overflow")?;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        require_exact_len(bytes, needed)?;
        Ok(Self {
            page,
            page_bytes: bytes[FIXED..needed].to_vec(),
        })
    }
}

