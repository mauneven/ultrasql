//! Durable byte serialization for `PageBackedHnswIndex` snapshots.

#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use super::*;

/// Snapshot container magic. Distinguishes this format from WAL/page bytes.
pub(crate) const HNSW_SNAPSHOT_MAGIC: &[u8; 8] = b"USQLHNS1";
/// Snapshot format version. Bump on any incompatible layout change.
pub(crate) const HNSW_SNAPSHOT_VERSION: u32 = 2;

const HNSW_PAGE_KIND_META: u8 = 0;
pub(crate) const HNSW_PAGE_KIND_NODE: u8 = 1;
const HNSW_PAGE_KIND_OVERFLOW: u8 = 2;
const HNSW_PAGE_KIND_FREE_LIST: u8 = 3;

const HNSW_OVERFLOW_KIND_VECTOR: u8 = 0;
const HNSW_OVERFLOW_KIND_NEIGHBORS: u8 = 1;

const ANN_QUANTIZED_KIND_F32: u8 = 0;
const ANN_QUANTIZED_KIND_BF16: u8 = 1;
const ANN_QUANTIZED_KIND_INT8: u8 = 2;

pub(crate) const fn encode_hnsw_metric(metric: HnswMetric) -> u8 {
    match metric {
        HnswMetric::L2 => 0,
        HnswMetric::Cosine => 1,
        HnswMetric::NegativeInnerProduct => 2,
        HnswMetric::L1 => 3,
    }
}

pub(crate) fn decode_hnsw_metric(tag: u8) -> Result<HnswMetric, AccessMethodError> {
    match tag {
        0 => Ok(HnswMetric::L2),
        1 => Ok(HnswMetric::Cosine),
        2 => Ok(HnswMetric::NegativeInnerProduct),
        3 => Ok(HnswMetric::L1),
        other => Err(AccessMethodError::Storage(format!(
            "hnsw snapshot invalid metric tag {other}"
        ))),
    }
}

pub(crate) const fn encode_ann_payload_kind(kind: AnnPayloadKind) -> u8 {
    match kind {
        AnnPayloadKind::F32 => 0,
        AnnPayloadKind::Bf16 => 1,
        AnnPayloadKind::Int8 => 2,
    }
}

pub(crate) fn decode_ann_payload_kind(tag: u8) -> Result<AnnPayloadKind, AccessMethodError> {
    match tag {
        0 => Ok(AnnPayloadKind::F32),
        1 => Ok(AnnPayloadKind::Bf16),
        2 => Ok(AnnPayloadKind::Int8),
        other => Err(AccessMethodError::Storage(format!(
            "hnsw snapshot invalid payload kind tag {other}"
        ))),
    }
}

/// Append a `usize` as a `u64` length prefix (lossless on 16/32/64-bit).
pub(crate) fn push_len(out: &mut Vec<u8>, len: usize) {
    let len_u64 = u64::try_from(len).unwrap_or(u64::MAX);
    out.extend_from_slice(&len_u64.to_le_bytes());
}

/// Append an `Option<BlockNumber>` as a one-byte present flag plus the raw u32.
pub(crate) fn push_opt_block(out: &mut Vec<u8>, block: Option<BlockNumber>) {
    match block {
        Some(block) => {
            out.push(1);
            out.extend_from_slice(&block.raw().to_le_bytes());
        }
        None => {
            out.push(0);
            out.extend_from_slice(&0_u32.to_le_bytes());
        }
    }
}

/// Append an `Option<HnswNodeId>` as a one-byte present flag plus the raw u64.
fn push_opt_node_id(out: &mut Vec<u8>, node: Option<HnswNodeId>) {
    match node {
        Some(node) => {
            out.push(1);
            out.extend_from_slice(&node.to_le_bytes());
        }
        None => {
            out.push(0);
            out.extend_from_slice(&0_u64.to_le_bytes());
        }
    }
}

/// Append a `TupleId` (heap pointer, so its relation is encoded in full).
pub(crate) fn push_tuple_id(out: &mut Vec<u8>, tid: TupleId) {
    out.extend_from_slice(&tid.page.relation.oid().raw().to_le_bytes());
    out.extend_from_slice(&tid.page.block.raw().to_le_bytes());
    out.extend_from_slice(&tid.slot.to_le_bytes());
}

/// Append a length-prefixed f32 vector: `[len:u32][f32 * len]`.
pub(crate) fn push_vec_f32(out: &mut Vec<u8>, values: &[f32]) {
    let len = u32::try_from(values.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_le_bytes());
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
}

/// Read a length-prefixed f32 vector. With `allow_empty`, a zero length yields an
/// empty vector (an unpopulated centroid slot); otherwise the length must equal
/// `dims`. Every element must be finite. Allocation is bounded by `dims` (itself
/// validated against `MAX_VECTOR_DIMS` by the caller), so it cannot be a bomb.
pub(crate) fn take_vec_f32(
    cursor: &mut SnapshotCursor<'_>,
    dims: usize,
    allow_empty: bool,
) -> Result<Vec<f32>, AccessMethodError> {
    let len = cursor.take_usize_len_u32()?;
    if len == 0 {
        if allow_empty {
            return Ok(Vec::new());
        }
        return Err(AccessMethodError::Storage(
            "ivfflat snapshot vector is unexpectedly empty".to_owned(),
        ));
    }
    if len != dims {
        return Err(AccessMethodError::Storage(
            "ivfflat snapshot vector dimension mismatch".to_owned(),
        ));
    }
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        let value = cursor.take_f32()?;
        if !value.is_finite() {
            return Err(AccessMethodError::Storage(
                "ivfflat snapshot vector element is not finite".to_owned(),
            ));
        }
        values.push(value);
    }
    Ok(values)
}

/// Append an ANN vector payload: kind tag, exact f32 values, and the quantized
/// body. The exact f32 values and the quantized values are written separately
/// so decode can rebuild the payload by struct literal without re-quantizing.
fn encode_ann_vector_payload(out: &mut Vec<u8>, payload: &AnnVectorPayload) {
    out.push(encode_ann_payload_kind(payload.kind));
    let exact = &payload.exact_f32;
    push_len(out, exact.len());
    for value in exact {
        out.extend_from_slice(&value.to_le_bytes());
    }
    match &payload.quantized {
        AnnQuantizedPayload::F32(values) => {
            out.push(ANN_QUANTIZED_KIND_F32);
            push_len(out, values.len());
            for value in values {
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
        AnnQuantizedPayload::Bf16(values) => {
            out.push(ANN_QUANTIZED_KIND_BF16);
            push_len(out, values.len());
            for value in values {
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
        AnnQuantizedPayload::Int8 { scale, values } => {
            out.push(ANN_QUANTIZED_KIND_INT8);
            out.extend_from_slice(&scale.to_le_bytes());
            push_len(out, values.len());
            for value in values {
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
    }
}

/// Append one page record: `u32 block`, `u64 lsn`, `u8 page_kind`, body.
pub(crate) fn encode_hnsw_page_record(out: &mut Vec<u8>, image: &PageBackedHnswPageImage) {
    out.extend_from_slice(&image.page_id.block.raw().to_le_bytes());
    out.extend_from_slice(&image.lsn.raw().to_le_bytes());
    match &image.page {
        HnswPersistentPage::Meta(meta) => {
            out.push(HNSW_PAGE_KIND_META);
            let dims = u32::try_from(meta.dims).unwrap_or(u32::MAX);
            out.extend_from_slice(&dims.to_le_bytes());
            out.push(encode_hnsw_metric(meta.metric));
            let m = u32::try_from(meta.m).unwrap_or(u32::MAX);
            out.extend_from_slice(&m.to_le_bytes());
            let ef = u32::try_from(meta.ef_search).unwrap_or(u32::MAX);
            out.extend_from_slice(&ef.to_le_bytes());
            out.push(encode_ann_payload_kind(meta.payload_kind));
            push_opt_node_id(out, meta.entry_node);
            out.extend_from_slice(&meta.next_node_id.to_le_bytes());
            push_len(out, meta.live_nodes);
            push_len(out, meta.tombstones);
            out.extend_from_slice(&meta.next_block_number.to_le_bytes());
            out.extend_from_slice(&meta.free_list_page.raw().to_le_bytes());
        }
        HnswPersistentPage::Node(node) => {
            out.push(HNSW_PAGE_KIND_NODE);
            out.extend_from_slice(&node.node_id.to_le_bytes());
            push_tuple_id(out, node.tid);
            push_len(out, node.vector_len);
            out.extend_from_slice(&node.vector_head.raw().to_le_bytes());
            push_len(out, node.neighbor_count);
            push_opt_block(out, node.neighbor_head);
            out.push(u8::from(node.deleted));
            // v2 extension: upper-layer neighbor chains. Appended after the v1
            // fields so a `level == 0` node encodes identically to v1 below this
            // point (the trailing `level` byte aside) and decoders that know v2
            // read exactly `level` upper-layer entries.
            push_len(out, node.level);
            debug_assert_eq!(node.upper_levels.len(), node.level);
            for upper in &node.upper_levels {
                push_opt_block(out, upper.head);
                push_len(out, upper.count);
            }
        }
        HnswPersistentPage::Overflow(overflow) => {
            out.push(HNSW_PAGE_KIND_OVERFLOW);
            out.extend_from_slice(&overflow.owner_node.to_le_bytes());
            push_opt_block(out, overflow.next);
            match &overflow.payload {
                HnswOverflowPayload::Vector(payload) => {
                    out.push(HNSW_OVERFLOW_KIND_VECTOR);
                    encode_ann_vector_payload(out, payload);
                }
                HnswOverflowPayload::Neighbors(neighbors) => {
                    out.push(HNSW_OVERFLOW_KIND_NEIGHBORS);
                    push_len(out, neighbors.len());
                    for node in neighbors {
                        out.extend_from_slice(&node.to_le_bytes());
                    }
                }
            }
        }
        HnswPersistentPage::FreeList(free_list) => {
            out.push(HNSW_PAGE_KIND_FREE_LIST);
            push_len(out, free_list.blocks.len());
            for block in &free_list.blocks {
                out.extend_from_slice(&block.raw().to_le_bytes());
            }
        }
    }
}

/// Forward-only reader over snapshot bytes. Every accessor is bounds-checked
/// and returns `Err` (never panics) on a short read.
pub(crate) struct SnapshotCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SnapshotCursor<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    pub(crate) fn take(&mut self, len: usize) -> Result<&'a [u8], AccessMethodError> {
        let end = self.pos.checked_add(len).ok_or_else(|| {
            AccessMethodError::Storage("hnsw snapshot length overflow".to_owned())
        })?;
        let slice = self.bytes.get(self.pos..end).ok_or_else(|| {
            AccessMethodError::Storage("hnsw snapshot unexpected end of buffer".to_owned())
        })?;
        self.pos = end;
        Ok(slice)
    }

    pub(crate) fn take_u8(&mut self) -> Result<u8, AccessMethodError> {
        let slice = self.take(1)?;
        slice
            .first()
            .copied()
            .ok_or_else(|| AccessMethodError::Storage("hnsw snapshot u8 read".to_owned()))
    }

    fn take_u16(&mut self) -> Result<u16, AccessMethodError> {
        let slice = self.take(2)?;
        let array: [u8; 2] = slice
            .try_into()
            .map_err(|_| AccessMethodError::Storage("hnsw snapshot u16 read".to_owned()))?;
        Ok(u16::from_le_bytes(array))
    }

    pub(crate) fn take_u32(&mut self) -> Result<u32, AccessMethodError> {
        let slice = self.take(4)?;
        let array: [u8; 4] = slice
            .try_into()
            .map_err(|_| AccessMethodError::Storage("hnsw snapshot u32 read".to_owned()))?;
        Ok(u32::from_le_bytes(array))
    }

    pub(crate) fn take_u64(&mut self) -> Result<u64, AccessMethodError> {
        let slice = self.take(8)?;
        let array: [u8; 8] = slice
            .try_into()
            .map_err(|_| AccessMethodError::Storage("hnsw snapshot u64 read".to_owned()))?;
        Ok(u64::from_le_bytes(array))
    }

    fn take_i8(&mut self) -> Result<i8, AccessMethodError> {
        let slice = self.take(1)?;
        let array: [u8; 1] = slice
            .try_into()
            .map_err(|_| AccessMethodError::Storage("hnsw snapshot i8 read".to_owned()))?;
        Ok(i8::from_le_bytes(array))
    }

    fn take_f32(&mut self) -> Result<f32, AccessMethodError> {
        let slice = self.take(4)?;
        let array: [u8; 4] = slice
            .try_into()
            .map_err(|_| AccessMethodError::Storage("hnsw snapshot f32 read".to_owned()))?;
        Ok(f32::from_le_bytes(array))
    }

    fn take_usize_len(&mut self) -> Result<usize, AccessMethodError> {
        let len = self.take_u64()?;
        usize::try_from(len).map_err(|_| {
            AccessMethodError::Storage("hnsw snapshot length overflows usize".to_owned())
        })
    }

    /// Read a `u32` field and widen it to `usize` (used for `dims`/`m`/`ef`).
    pub(crate) fn take_usize_len_u32(&mut self) -> Result<usize, AccessMethodError> {
        let value = self.take_u32()?;
        usize::try_from(value).map_err(|_| {
            AccessMethodError::Storage("hnsw snapshot u32 length overflows usize".to_owned())
        })
    }

    pub(crate) fn take_bool(&mut self) -> Result<bool, AccessMethodError> {
        match self.take_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(AccessMethodError::Storage(format!(
                "hnsw snapshot invalid bool byte {other}"
            ))),
        }
    }
}

fn decode_opt_block(
    cursor: &mut SnapshotCursor<'_>,
) -> Result<Option<BlockNumber>, AccessMethodError> {
    let present = cursor.take_bool()?;
    let raw = cursor.take_u32()?;
    if present {
        Ok(Some(BlockNumber::new(raw)))
    } else {
        Ok(None)
    }
}

fn decode_opt_node_id(
    cursor: &mut SnapshotCursor<'_>,
) -> Result<Option<HnswNodeId>, AccessMethodError> {
    let present = cursor.take_bool()?;
    let raw = cursor.take_u64()?;
    if present { Ok(Some(raw)) } else { Ok(None) }
}

pub(crate) fn decode_tuple_id(cursor: &mut SnapshotCursor<'_>) -> Result<TupleId, AccessMethodError> {
    let relation = RelationId::new(cursor.take_u32()?);
    let block = BlockNumber::new(cursor.take_u32()?);
    let slot = cursor.take_u16()?;
    Ok(TupleId::new(PageId::new(relation, block), slot))
}

fn decode_ann_vector_payload(
    cursor: &mut SnapshotCursor<'_>,
) -> Result<AnnVectorPayload, AccessMethodError> {
    let kind = decode_ann_payload_kind(cursor.take_u8()?)?;
    let exact_len = cursor.take_usize_len()?;
    let mut exact_f32 = Vec::with_capacity(exact_len.min(1 << 20));
    for _ in 0..exact_len {
        exact_f32.push(cursor.take_f32()?);
    }
    let quantized = match cursor.take_u8()? {
        ANN_QUANTIZED_KIND_F32 => {
            let len = cursor.take_usize_len()?;
            let mut values = Vec::with_capacity(len.min(1 << 20));
            for _ in 0..len {
                values.push(cursor.take_f32()?);
            }
            AnnQuantizedPayload::F32(values)
        }
        ANN_QUANTIZED_KIND_BF16 => {
            let len = cursor.take_usize_len()?;
            let mut values = Vec::with_capacity(len.min(1 << 20));
            for _ in 0..len {
                values.push(cursor.take_u16()?);
            }
            AnnQuantizedPayload::Bf16(values)
        }
        ANN_QUANTIZED_KIND_INT8 => {
            let scale = cursor.take_f32()?;
            let len = cursor.take_usize_len()?;
            let mut values = Vec::with_capacity(len.min(1 << 20));
            for _ in 0..len {
                values.push(cursor.take_i8()?);
            }
            AnnQuantizedPayload::Int8 { scale, values }
        }
        other => {
            return Err(AccessMethodError::Storage(format!(
                "hnsw snapshot invalid quantized kind tag {other}"
            )));
        }
    };
    // Build by struct literal to preserve the exact stored values; using
    // `AnnVectorPayload::new` here would re-quantize and lose round-trip parity.
    Ok(AnnVectorPayload {
        kind,
        exact_f32,
        quantized,
    })
}

/// Decode one page record into a [`PageBackedHnswPageImage`]. `index_rel` is the
/// owning relation for the page id; `payload_kind` is unused here but kept in
/// the signature so vector overflow records can be validated against it without
/// a wider rework (the meta page remains the source of truth on rebuild).
pub(crate) fn decode_hnsw_page_record(
    cursor: &mut SnapshotCursor<'_>,
    index_rel: RelationId,
    payload_kind: AnnPayloadKind,
    version: u32,
) -> Result<PageBackedHnswPageImage, AccessMethodError> {
    let _ = payload_kind;
    let block = BlockNumber::new(cursor.take_u32()?);
    let page_id = PageId::new(index_rel, block);
    let lsn = Lsn::new(cursor.take_u64()?);
    let page_kind = cursor.take_u8()?;
    let page = match page_kind {
        HNSW_PAGE_KIND_META => {
            let dims = cursor.take_usize_len_u32()?;
            let metric = decode_hnsw_metric(cursor.take_u8()?)?;
            let m = cursor.take_usize_len_u32()?;
            let ef_search = cursor.take_usize_len_u32()?;
            let meta_payload_kind = decode_ann_payload_kind(cursor.take_u8()?)?;
            let entry_node = decode_opt_node_id(cursor)?;
            let next_node_id = cursor.take_u64()?;
            let live_nodes = cursor.take_usize_len()?;
            let tombstones = cursor.take_usize_len()?;
            let next_block_number = cursor.take_u32()?;
            let free_list_page = BlockNumber::new(cursor.take_u32()?);
            HnswPersistentPage::Meta(HnswMetaPage {
                page_id,
                lsn,
                dims,
                metric,
                m,
                ef_search,
                payload_kind: meta_payload_kind,
                entry_node,
                next_node_id,
                live_nodes,
                tombstones,
                next_block_number,
                free_list_page,
            })
        }
        HNSW_PAGE_KIND_NODE => {
            let node_id = cursor.take_u64()?;
            let tid = decode_tuple_id(cursor)?;
            let vector_len = cursor.take_usize_len()?;
            let vector_head = BlockNumber::new(cursor.take_u32()?);
            let neighbor_count = cursor.take_usize_len()?;
            let neighbor_head = decode_opt_block(cursor)?;
            let deleted = cursor.take_bool()?;
            // v2 trailer: upper-layer neighbor chains. v1 nodes are base-only.
            let (level, upper_levels) = if version >= 2 {
                let level = cursor.take_usize_len()?;
                let mut upper_levels = Vec::with_capacity(level.min(1 << 16));
                for _ in 0..level {
                    let head = decode_opt_block(cursor)?;
                    let count = cursor.take_usize_len()?;
                    upper_levels.push(HnswLevelNeighbors { head, count });
                }
                (level, upper_levels)
            } else {
                (0, Vec::new())
            };
            HnswPersistentPage::Node(HnswNodePage {
                page_id,
                lsn,
                node_id,
                tid,
                vector_len,
                vector_head,
                neighbor_count,
                neighbor_head,
                level,
                upper_levels,
                deleted,
            })
        }
        HNSW_PAGE_KIND_OVERFLOW => {
            let owner_node = cursor.take_u64()?;
            let next = decode_opt_block(cursor)?;
            let payload = match cursor.take_u8()? {
                HNSW_OVERFLOW_KIND_VECTOR => {
                    HnswOverflowPayload::Vector(decode_ann_vector_payload(cursor)?)
                }
                HNSW_OVERFLOW_KIND_NEIGHBORS => {
                    let len = cursor.take_usize_len()?;
                    let mut neighbors = Vec::with_capacity(len.min(1 << 20));
                    for _ in 0..len {
                        neighbors.push(cursor.take_u64()?);
                    }
                    HnswOverflowPayload::Neighbors(neighbors)
                }
                other => {
                    return Err(AccessMethodError::Storage(format!(
                        "hnsw snapshot invalid overflow kind tag {other}"
                    )));
                }
            };
            HnswPersistentPage::Overflow(HnswOverflowPage {
                page_id,
                lsn,
                owner_node,
                next,
                payload,
            })
        }
        HNSW_PAGE_KIND_FREE_LIST => {
            let len = cursor.take_usize_len()?;
            let mut blocks = Vec::with_capacity(len.min(1 << 20));
            for _ in 0..len {
                blocks.push(BlockNumber::new(cursor.take_u32()?));
            }
            HnswPersistentPage::FreeList(HnswFreeListPage {
                page_id,
                lsn,
                blocks,
            })
        }
        other => {
            return Err(AccessMethodError::Storage(format!(
                "hnsw snapshot invalid page kind tag {other}"
            )));
        }
    };
    Ok(PageBackedHnswPageImage { page_id, lsn, page })
}
