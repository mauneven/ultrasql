//! Vector-index (HNSW, IVFFlat) operation payload codecs.

use ultrasql_core::endian::{read_u16_le, read_u32_le, write_u16_le, write_u32_le};
use ultrasql_core::{BlockNumber, PageId, RelationId, TupleId};

use super::{MAX_VARIABLE_PAYLOAD_BYTES, PayloadError, checked_offset, require_exact_len};

// ---------------------------------------------------------------------------
// SequenceOpPayload
// ---------------------------------------------------------------------------

/// Kind of HNSW graph operation recorded in a [`HnswOpPayload`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum HnswOpKind {
    /// Inserted a live vector node.
    Insert = 1,
    /// Marked a vector node deleted.
    Delete = 2,
    /// Compacted tombstoned nodes out of the graph.
    Compact = 3,
}

impl HnswOpKind {
    /// Parse a `HnswOpKind` from its on-disk byte representation.
    pub const fn from_u8(v: u8) -> Result<Self, PayloadError> {
        match v {
            1 => Ok(Self::Insert),
            2 => Ok(Self::Delete),
            3 => Ok(Self::Compact),
            _ => Err(PayloadError::Malformed("hnsw_op kind unknown")),
        }
    }
}

impl From<HnswOpKind> for u8 {
    fn from(kind: HnswOpKind) -> Self {
        match kind {
            HnswOpKind::Insert => 1,
            HnswOpKind::Delete => 2,
            HnswOpKind::Compact => 3,
        }
    }
}

/// Payload for a `RecordType::HnswOp` WAL record.
///
/// The record logs runtime HNSW graph mutations in a redo-friendly shape:
/// the index relation, affected tuple id, and vector payload for inserts.
/// Deletes and compaction records carry an empty vector. Future page-backed
/// HNSW recovery can replay these records into graph pages or use `Compact`
/// as a rebuild boundary.
///
/// Wire layout (little-endian, no implicit padding):
/// ```text
///  0   1   op (u8) — HnswOpKind discriminant
///  1   3   reserved (zero)
///  4   4   index_rel (RelationId/OID, u32)
///  8  12   tid (TupleId)
/// 20   4   dims (u32)
/// 24   4   vector_len (u32)
/// 28  ..   f32 vector values as little-endian bytes
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct HnswOpPayload {
    /// Mutation kind.
    pub op: HnswOpKind,
    /// OID of the HNSW index relation.
    pub index_rel: RelationId,
    /// Heap tuple identifier affected by insert/delete.
    pub tid: TupleId,
    /// Vector payload for inserts. Empty for delete/compact.
    pub vector: Vec<f32>,
}

impl HnswOpPayload {
    /// Encode this payload into a freshly allocated byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        let vector_bytes_len = self
            .vector
            .len()
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or(PayloadError::Malformed("hnsw_op vector length overflow"))?;
        if vector_bytes_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "hnsw_op vector length exceeds ceiling",
            ));
        }
        let dims = u32::try_from(self.vector.len())
            .map_err(|_| PayloadError::Malformed("hnsw_op dims overflow"))?;
        let vector_len = u32::try_from(self.vector.len())
            .map_err(|_| PayloadError::Malformed("hnsw_op vector_len overflow"))?;
        const FIXED: usize = 28;
        let total = checked_offset(FIXED, vector_bytes_len, "hnsw_op length overflow")?;
        let mut out = vec![0_u8; total];
        out[0] = u8::from(self.op);
        write_u32_le(&mut out[4..8], self.index_rel.oid().raw());
        write_u32_le(&mut out[8..12], self.tid.page.relation.oid().raw());
        write_u32_le(&mut out[12..16], self.tid.page.block.raw());
        write_u16_le(&mut out[16..18], self.tid.slot);
        write_u16_le(&mut out[18..20], 0);
        write_u32_le(&mut out[20..24], dims);
        write_u32_le(&mut out[24..28], vector_len);
        let mut off = 28;
        for value in &self.vector {
            if !value.is_finite() {
                return Err(PayloadError::Malformed(
                    "hnsw_op vector elements must be finite",
                ));
            }
            let next = checked_offset(off, 4, "hnsw_op length overflow")?;
            out[off..next].copy_from_slice(&value.to_le_bytes());
            off = next;
        }
        Ok(out)
    }

    /// Decode a `HnswOpPayload` from a byte slice.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        const FIXED: usize = 28;
        if bytes.len() < FIXED {
            return Err(PayloadError::Truncated {
                needed: FIXED,
                have: bytes.len(),
            });
        }
        let op = HnswOpKind::from_u8(bytes[0])?;
        if bytes[1] != 0 || bytes[2] != 0 || bytes[3] != 0 {
            return Err(PayloadError::Malformed(
                "hnsw_op reserved prefix bytes must be zero",
            ));
        }
        let index_rel = RelationId::new(
            read_u32_le(&bytes[4..8]).map_err(|_| PayloadError::Malformed("hnsw_op index_rel"))?,
        );
        if bytes[18] != 0 || bytes[19] != 0 {
            return Err(PayloadError::Malformed(
                "hnsw_op tid reserved bytes must be zero",
            ));
        }
        let tid_rel = read_u32_le(&bytes[8..12])
            .map_err(|_| PayloadError::Malformed("hnsw_op tid relation"))?;
        let tid_block = read_u32_le(&bytes[12..16])
            .map_err(|_| PayloadError::Malformed("hnsw_op tid block"))?;
        let tid_slot =
            read_u16_le(&bytes[16..18]).map_err(|_| PayloadError::Malformed("hnsw_op tid slot"))?;
        let tid = TupleId::new(
            PageId::new(RelationId::new(tid_rel), BlockNumber::new(tid_block)),
            tid_slot,
        );
        let dims = usize::try_from(
            read_u32_le(&bytes[20..24]).map_err(|_| PayloadError::Malformed("hnsw_op dims"))?,
        )
        .map_err(|_| PayloadError::Malformed("hnsw_op dims usize overflow"))?;
        let vector_len = usize::try_from(
            read_u32_le(&bytes[24..28])
                .map_err(|_| PayloadError::Malformed("hnsw_op vector_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("hnsw_op vector_len usize overflow"))?;
        if dims != vector_len {
            return Err(PayloadError::Malformed(
                "hnsw_op dims and vector_len disagree",
            ));
        }
        let vector_bytes_len = vector_len
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or(PayloadError::Malformed("hnsw_op vector length overflow"))?;
        if vector_bytes_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "hnsw_op vector length exceeds ceiling",
            ));
        }
        let needed = checked_offset(FIXED, vector_bytes_len, "hnsw_op length overflow")?;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        require_exact_len(bytes, needed)?;
        let mut vector = Vec::with_capacity(vector_len);
        for chunk in bytes[FIXED..needed].chunks_exact(std::mem::size_of::<f32>()) {
            let value = f32::from_le_bytes(
                chunk
                    .try_into()
                    .map_err(|_| PayloadError::Malformed("hnsw_op f32 chunk"))?,
            );
            if !value.is_finite() {
                return Err(PayloadError::Malformed(
                    "hnsw_op vector elements must be finite",
                ));
            }
            vector.push(value);
        }
        Ok(Self {
            op,
            index_rel,
            tid,
            vector,
        })
    }
}

/// Kind of IVFFlat inverted-list operation recorded in an
/// [`IvfFlatOpPayload`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum IvfFlatOpKind {
    /// Installed or replaced one centroid page.
    Centroid = 1,
    /// Inserted one vector into an inverted list.
    Insert = 2,
    /// Marked one tuple id tombstoned.
    Delete = 3,
    /// Compacted tombstoned entries out of list pages.
    Compact = 4,
}

impl IvfFlatOpKind {
    /// Parse an `IvfFlatOpKind` from its on-disk byte representation.
    pub const fn from_u8(v: u8) -> Result<Self, PayloadError> {
        match v {
            1 => Ok(Self::Centroid),
            2 => Ok(Self::Insert),
            3 => Ok(Self::Delete),
            4 => Ok(Self::Compact),
            _ => Err(PayloadError::Malformed("ivfflat_op kind unknown")),
        }
    }
}

impl From<IvfFlatOpKind> for u8 {
    fn from(kind: IvfFlatOpKind) -> Self {
        match kind {
            IvfFlatOpKind::Centroid => 1,
            IvfFlatOpKind::Insert => 2,
            IvfFlatOpKind::Delete => 3,
            IvfFlatOpKind::Compact => 4,
        }
    }
}

/// Payload for a `RecordType::IvfFlatOp` WAL record.
///
/// The record carries a redo-friendly logical mutation for page-backed
/// IVFFlat storage: centroid materialization, list insert, tombstone, or
/// compaction. Insert and centroid records include a finite `f32` vector;
/// delete and compact records use an empty vector.
///
/// Wire layout (little-endian, no implicit padding):
/// ```text
///  0   1   op (u8) — IvfFlatOpKind discriminant
///  1   3   reserved (zero)
///  4   4   index_rel (RelationId/OID, u32)
///  8  12   tid (TupleId)
/// 20   4   list_id (u32)
/// 24   4   dims (u32)
/// 28   4   vector_len (u32)
/// 32  ..   f32 vector values as little-endian bytes
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct IvfFlatOpPayload {
    /// Mutation kind.
    pub op: IvfFlatOpKind,
    /// OID of the IVFFlat index relation.
    pub index_rel: RelationId,
    /// Heap tuple identifier affected by insert/delete.
    pub tid: TupleId,
    /// Inverted list or centroid slot affected by the operation.
    pub list_id: u32,
    /// Vector payload for centroid/insert records.
    pub vector: Vec<f32>,
}

impl IvfFlatOpPayload {
    /// Encode this payload into a freshly allocated byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        let vector_bytes_len = self
            .vector
            .len()
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or(PayloadError::Malformed("ivfflat_op vector length overflow"))?;
        if vector_bytes_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "ivfflat_op vector length exceeds ceiling",
            ));
        }
        let dims = u32::try_from(self.vector.len())
            .map_err(|_| PayloadError::Malformed("ivfflat_op dims overflow"))?;
        let vector_len = u32::try_from(self.vector.len())
            .map_err(|_| PayloadError::Malformed("ivfflat_op vector_len overflow"))?;
        const FIXED: usize = 32;
        let total = checked_offset(FIXED, vector_bytes_len, "ivfflat_op length overflow")?;
        let mut out = vec![0_u8; total];
        out[0] = u8::from(self.op);
        write_u32_le(&mut out[4..8], self.index_rel.oid().raw());
        write_u32_le(&mut out[8..12], self.tid.page.relation.oid().raw());
        write_u32_le(&mut out[12..16], self.tid.page.block.raw());
        write_u16_le(&mut out[16..18], self.tid.slot);
        write_u16_le(&mut out[18..20], 0);
        write_u32_le(&mut out[20..24], self.list_id);
        write_u32_le(&mut out[24..28], dims);
        write_u32_le(&mut out[28..32], vector_len);
        let mut off = 32;
        for value in &self.vector {
            if !value.is_finite() {
                return Err(PayloadError::Malformed(
                    "ivfflat_op vector elements must be finite",
                ));
            }
            let next = checked_offset(off, 4, "ivfflat_op length overflow")?;
            out[off..next].copy_from_slice(&value.to_le_bytes());
            off = next;
        }
        Ok(out)
    }

    /// Decode an `IvfFlatOpPayload` from a byte slice.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        const FIXED: usize = 32;
        if bytes.len() < FIXED {
            return Err(PayloadError::Truncated {
                needed: FIXED,
                have: bytes.len(),
            });
        }
        let op = IvfFlatOpKind::from_u8(bytes[0])?;
        if bytes[1] != 0 || bytes[2] != 0 || bytes[3] != 0 {
            return Err(PayloadError::Malformed(
                "ivfflat_op reserved prefix bytes must be zero",
            ));
        }
        let index_rel = RelationId::new(
            read_u32_le(&bytes[4..8])
                .map_err(|_| PayloadError::Malformed("ivfflat_op index_rel"))?,
        );
        if bytes[18] != 0 || bytes[19] != 0 {
            return Err(PayloadError::Malformed(
                "ivfflat_op tid reserved bytes must be zero",
            ));
        }
        let tid_rel = read_u32_le(&bytes[8..12])
            .map_err(|_| PayloadError::Malformed("ivfflat_op tid relation"))?;
        let tid_block = read_u32_le(&bytes[12..16])
            .map_err(|_| PayloadError::Malformed("ivfflat_op tid block"))?;
        let tid_slot = read_u16_le(&bytes[16..18])
            .map_err(|_| PayloadError::Malformed("ivfflat_op tid slot"))?;
        let tid = TupleId::new(
            PageId::new(RelationId::new(tid_rel), BlockNumber::new(tid_block)),
            tid_slot,
        );
        let list_id = read_u32_le(&bytes[20..24])
            .map_err(|_| PayloadError::Malformed("ivfflat_op list_id"))?;
        let dims = usize::try_from(
            read_u32_le(&bytes[24..28]).map_err(|_| PayloadError::Malformed("ivfflat_op dims"))?,
        )
        .map_err(|_| PayloadError::Malformed("ivfflat_op dims usize overflow"))?;
        let vector_len = usize::try_from(
            read_u32_le(&bytes[28..32])
                .map_err(|_| PayloadError::Malformed("ivfflat_op vector_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("ivfflat_op vector_len usize overflow"))?;
        if dims != vector_len {
            return Err(PayloadError::Malformed(
                "ivfflat_op dims and vector_len disagree",
            ));
        }
        let vector_bytes_len = vector_len
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or(PayloadError::Malformed("ivfflat_op vector length overflow"))?;
        if vector_bytes_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "ivfflat_op vector length exceeds ceiling",
            ));
        }
        let needed = checked_offset(FIXED, vector_bytes_len, "ivfflat_op length overflow")?;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        require_exact_len(bytes, needed)?;
        let mut vector = Vec::with_capacity(vector_len);
        for chunk in bytes[FIXED..needed].chunks_exact(std::mem::size_of::<f32>()) {
            let value = f32::from_le_bytes(
                chunk
                    .try_into()
                    .map_err(|_| PayloadError::Malformed("ivfflat_op f32 chunk"))?,
            );
            if !value.is_finite() {
                return Err(PayloadError::Malformed(
                    "ivfflat_op vector elements must be finite",
                ));
            }
            vector.push(value);
        }
        Ok(Self {
            op,
            index_rel,
            tid,
            list_id,
            vector,
        })
    }
}
