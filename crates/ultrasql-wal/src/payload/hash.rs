//! Hash-index operation payload codecs.

use ultrasql_core::{PageId, RelationId};
use ultrasql_core::endian::{read_u32_le, read_u64_le, write_u32_le, write_u64_le};

use super::{
    MAX_VARIABLE_PAYLOAD_BYTES, PAGE_ID_SIZE, PayloadError, checked_offset, decode_page_id,
    encode_page_id, require_exact_len,
};

// ---------------------------------------------------------------------------
// HashOpPayload
// ---------------------------------------------------------------------------

/// Kind of hash-index operation recorded in a [`HashOpPayload`].
///
/// Numeric values are part of the on-disk format; new variants may be appended
/// but existing values must remain stable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum HashOpKind {
    /// A key/TID entry was inserted into a bucket or overflow page.
    Insert = 1,
    /// A key/TID entry was removed from a bucket or overflow page.
    Delete = 2,
    /// A new overflow page was linked from a bucket chain.
    OverflowLink = 3,
}

impl HashOpKind {
    /// Parse a `HashOpKind` from its on-disk byte representation.
    pub const fn from_u8(v: u8) -> Result<Self, PayloadError> {
        match v {
            1 => Ok(Self::Insert),
            2 => Ok(Self::Delete),
            3 => Ok(Self::OverflowLink),
            _ => Err(PayloadError::Malformed("hash_op kind unknown")),
        }
    }
}

impl From<HashOpKind> for u8 {
    fn from(kind: HashOpKind) -> Self {
        match kind {
            HashOpKind::Insert => 1,
            HashOpKind::Delete => 2,
            HashOpKind::OverflowLink => 3,
        }
    }
}

/// Payload for a `RecordType::HashOp` WAL record.
///
/// Carries the hash-index mutation shape independently from the B-tree WAL
/// path: the fixed bucket number, touched hash page, stable key hash, encoded
/// key bytes, and encoded value bytes. Insert/delete records use `value_bytes`
/// for the encoded heap `TupleId`; `OverflowLink` records use it for the
/// implementation-defined link payload.
///
/// Wire layout (little-endian, no implicit padding):
/// ```text
///  0   1   op (u8) — HashOpKind discriminant
///  1   3   reserved (three zero bytes)
///  4   4   index_rel (RelationId, u32)
///  8   4   bucket (u32)
/// 12   8   page (PageId: rel u32 | block u32)
/// 20   8   key_hash (u64)
/// 28   4   key_len (u32)
/// 32  ..   key_bytes (key_len bytes)
///  +   4   value_len (u32)
///  +  ..   value_bytes (value_len bytes)
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashOpPayload {
    /// Hash-index mutation kind.
    pub op: HashOpKind,
    /// OID of the hash index relation that was mutated.
    pub index_rel: RelationId,
    /// Static bucket number addressed by this operation.
    pub bucket: u32,
    /// Bucket or overflow page touched by this operation.
    pub page: PageId,
    /// Stable hash of the encoded key.
    pub key_hash: u64,
    /// Encoded key bytes.
    pub key_bytes: Vec<u8>,
    /// Encoded value bytes, usually the heap `TupleId`.
    pub value_bytes: Vec<u8>,
}

impl HashOpPayload {
    /// Encode this payload into a freshly-allocated byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        let key_len = u32::try_from(self.key_bytes.len())
            .map_err(|_| PayloadError::Malformed("hash_op key_len overflow"))?;
        let value_len = u32::try_from(self.value_bytes.len())
            .map_err(|_| PayloadError::Malformed("hash_op value_len overflow"))?;
        if self.key_bytes.len() > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed("hash_op key_len exceeds ceiling"));
        }
        if self.value_bytes.len() > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed("hash_op value_len exceeds ceiling"));
        }
        const FIXED: usize = 32;
        let key_end = checked_offset(FIXED, self.key_bytes.len(), "hash_op length overflow")?;
        let value_len_end = checked_offset(key_end, 4, "hash_op length overflow")?;
        let total = checked_offset(
            value_len_end,
            self.value_bytes.len(),
            "hash_op length overflow",
        )?;
        let mut out = vec![0_u8; total];
        out[0] = u8::from(self.op);
        write_u32_le(&mut out[4..8], self.index_rel.oid().raw());
        write_u32_le(&mut out[8..12], self.bucket);
        let mut pid_buf = [0_u8; PAGE_ID_SIZE];
        encode_page_id(&mut pid_buf, self.page);
        out[12..20].copy_from_slice(&pid_buf);
        write_u64_le(&mut out[20..28], self.key_hash);
        write_u32_le(&mut out[28..32], key_len);
        out[FIXED..key_end].copy_from_slice(&self.key_bytes);
        write_u32_le(&mut out[key_end..value_len_end], value_len);
        out[value_len_end..].copy_from_slice(&self.value_bytes);
        Ok(out)
    }

    /// Decode a `HashOpPayload` from a byte slice.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        const FIXED: usize = 32;
        if bytes.len() < FIXED {
            return Err(PayloadError::Truncated {
                needed: FIXED,
                have: bytes.len(),
            });
        }
        let op = HashOpKind::from_u8(bytes[0])?;
        if bytes[1] != 0 || bytes[2] != 0 || bytes[3] != 0 {
            return Err(PayloadError::Malformed(
                "hash_op reserved bytes must be zero",
            ));
        }
        let index_rel = RelationId::new(
            read_u32_le(&bytes[4..8]).map_err(|_| PayloadError::Malformed("hash_op index_rel"))?,
        );
        let bucket =
            read_u32_le(&bytes[8..12]).map_err(|_| PayloadError::Malformed("hash_op bucket"))?;
        let page = decode_page_id(&bytes[12..20])?;
        let key_hash =
            read_u64_le(&bytes[20..28]).map_err(|_| PayloadError::Malformed("hash_op key_hash"))?;
        let key_len = usize::try_from(
            read_u32_le(&bytes[28..32]).map_err(|_| PayloadError::Malformed("hash_op key_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("hash_op key_len usize overflow"))?;
        if key_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed("hash_op key_len exceeds ceiling"));
        }
        let key_end = checked_offset(FIXED, key_len, "hash_op length overflow")?;
        let value_len_end = checked_offset(key_end, 4, "hash_op length overflow")?;
        if bytes.len() < value_len_end {
            return Err(PayloadError::Truncated {
                needed: value_len_end,
                have: bytes.len(),
            });
        }
        let key_bytes = bytes[FIXED..key_end].to_vec();
        let value_len = usize::try_from(
            read_u32_le(&bytes[key_end..value_len_end])
                .map_err(|_| PayloadError::Malformed("hash_op value_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("hash_op value_len usize overflow"))?;
        if value_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed("hash_op value_len exceeds ceiling"));
        }
        let value_end = checked_offset(value_len_end, value_len, "hash_op length overflow")?;
        if bytes.len() < value_end {
            return Err(PayloadError::Truncated {
                needed: value_end,
                have: bytes.len(),
            });
        }
        require_exact_len(bytes, value_end)?;
        Ok(Self {
            op,
            index_rel,
            bucket,
            page,
            key_hash,
            key_bytes,
            value_bytes: bytes[value_len_end..value_end].to_vec(),
        })
    }
}

