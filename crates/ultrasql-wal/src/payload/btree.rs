//! B-tree operation payload codecs.

use ultrasql_core::endian::{read_u32_le, write_u32_le};
use ultrasql_core::{PageId, RelationId};

use super::{
    MAX_VARIABLE_PAYLOAD_BYTES, PAGE_ID_SIZE, PayloadError, checked_offset, decode_page_id,
    encode_page_id, require_exact_len,
};

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

impl From<BTreeOpKind> for u8 {
    fn from(kind: BTreeOpKind) -> Self {
        match kind {
            BTreeOpKind::Insert => 1,
            BTreeOpKind::Split => 2,
            BTreeOpKind::Delete => 3,
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
        const FIXED: usize = 20;
        let key_end = checked_offset(FIXED, self.key_bytes.len(), "btree_op length overflow")?;
        let cv_len_end = checked_offset(key_end, 4, "btree_op length overflow")?;
        let total = checked_offset(
            cv_len_end,
            self.child_or_value.len(),
            "btree_op length overflow",
        )?;
        let mut out = vec![0_u8; total];
        out[0] = u8::from(self.op);
        // bytes 1-3: reserved zero (already zeroed)
        write_u32_le(&mut out[4..8], self.index_rel.oid().raw());
        let mut pid_buf = [0_u8; PAGE_ID_SIZE];
        encode_page_id(&mut pid_buf, self.page);
        out[8..16].copy_from_slice(&pid_buf);
        write_u32_le(&mut out[16..20], key_len);
        out[FIXED..key_end].copy_from_slice(&self.key_bytes);
        write_u32_le(&mut out[key_end..cv_len_end], cv_len);
        out[cv_len_end..].copy_from_slice(&self.child_or_value);
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
        if bytes[1] != 0 || bytes[2] != 0 || bytes[3] != 0 {
            return Err(PayloadError::Malformed(
                "btree_op reserved bytes must be zero",
            ));
        }
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
        let key_end = checked_offset(FIXED, key_len, "btree_op length overflow")?;
        let cv_len_end = checked_offset(key_end, 4, "btree_op length overflow")?;
        if bytes.len() < cv_len_end {
            return Err(PayloadError::Truncated {
                needed: cv_len_end,
                have: bytes.len(),
            });
        }
        let key_bytes = bytes[FIXED..key_end].to_vec();
        let cv_len = usize::try_from(
            read_u32_le(&bytes[key_end..cv_len_end])
                .map_err(|_| PayloadError::Malformed("btree_op cv_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("btree_op cv_len usize overflow"))?;
        if cv_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed("btree_op cv_len exceeds ceiling"));
        }
        let cv_end = checked_offset(cv_len_end, cv_len, "btree_op length overflow")?;
        if bytes.len() < cv_end {
            return Err(PayloadError::Truncated {
                needed: cv_end,
                have: bytes.len(),
            });
        }
        require_exact_len(bytes, cv_end)?;
        let child_or_value = bytes[cv_len_end..cv_end].to_vec();
        Ok(Self {
            op,
            index_rel,
            page,
            key_bytes,
            child_or_value,
        })
    }
}
