//! TOAST (The Oversized Attribute Storage Technique).
//!
//! Values larger than [`TOAST_INLINE_MAX`] bytes (2 KiB) are split into
//! fixed-size chunks and stored out-of-line in a per-relation *TOAST
//! table*. On retrieval the chunks are reassembled and (if compressed)
//! decompressed transparently.
//!
//! # Structure
//!
//! A [`ToastTable`] wraps a [`HeapAccess`] that
//! stores chunk tuples. Each chunk tuple consists of:
//!
//! ```text
//! [value_oid: u64 (8 bytes)] [chunk_seq: u32 (4 bytes)] [payload: up to TOAST_MAX_CHUNK_SIZE bytes]
//! ```
//!
//! The total header is 12 bytes; the chunk payload follows directly.
//!
//! # Compression
//!
//! External values are compressed with `lz4_flex` before chunking when the
//! compressed form is smaller than the original (the compressible check
//! uses a 5-byte savings threshold). If compression does not help, the
//! raw bytes are stored. The [`ToastPointer`] records both `raw_size`
//! and `compressed_size` so the reader knows whether to decompress.
//!
//! # Value OIDs
//!
//! Each stored value is assigned a unique 64-bit OID via a per-relation
//! `AtomicU64` counter. OIDs are
//! allocated starting at 1 (0 is reserved as "invalid").
//!
//! # Inline path
//!
//! Values ≤ `threshold` bytes (default [`TOAST_INLINE_MAX`], 2 KiB) are
//! returned as [`ToastDatum::Inline`] without any storage interaction.
//! The caller should store the raw bytes in the main tuple payload.
//!
//! # Thread safety
//!
//! [`ToastTable`] is `Send + Sync`: the OID counter uses atomics, and the
//! underlying [`HeapAccess`] is already `Send + Sync`.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    reason = "on-disk format / fixed-width packing; narrowings bounded by PAGE_SIZE / relation size"
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use ultrasql_core::{BlockNumber, CommandId, PageId, RelationId, TupleId, Xid};

use crate::buffer_pool::{BufferPool, PageLoader};
use crate::heap::{HeapAccess, HeapError, InsertOptions};

/// Values ≤ this threshold are stored inline.
pub const TOAST_INLINE_MAX: usize = 2048;

/// Maximum payload bytes per chunk tuple (matches PostgreSQL's
/// `TOAST_MAX_CHUNK_SIZE`).
pub const TOAST_MAX_CHUNK_SIZE: usize = 2000;

/// Per-chunk header size: `value_oid` (8) + `chunk_seq` (4).
const CHUNK_HEADER_SIZE: usize = 12;

/// Compression is skipped if the compressed form is not at least this many
/// bytes smaller than the original.
const COMPRESSION_SAVINGS_THRESHOLD: usize = 5;

/// Errors that arise from TOAST operations.
#[derive(Debug, thiserror::Error)]
pub enum ToastError {
    /// The underlying heap access method returned an error.
    #[error("heap: {0}")]
    Heap(#[from] HeapError),

    /// lz4 decompression failed.
    #[error("lz4 decompress: {0}")]
    Decompress(String),

    /// The stored chunk data is shorter than the expected header.
    #[error("malformed chunk: too short for header ({len} bytes)")]
    MalformedChunk {
        /// Length of the chunk that was read.
        len: usize,
    },

    /// A reassembly gap was detected: an expected `chunk_seq` was missing.
    #[error("missing chunk {seq} for value_oid {oid}")]
    MissingChunk {
        /// The OID of the value being fetched.
        oid: u64,
        /// The chunk sequence number that was expected but not found.
        seq: u32,
    },

    /// The caller provided a pointer for a value stored in a different
    /// TOAST table.
    #[error("pointer belongs to relation {ptr_rel} but this table serves relation {table_rel}")]
    WrongRelation {
        /// Relation in the pointer.
        ptr_rel: u32,
        /// Relation this table serves.
        table_rel: u32,
    },

    /// A TOAST value or stored representation cannot fit the on-disk
    /// 32-bit size fields.
    #[error("TOAST {context} value is too large: {len} bytes exceeds {max} bytes")]
    ValueTooLarge {
        /// Which byte length was being converted.
        context: &'static str,
        /// The attempted byte length.
        len: usize,
        /// Maximum representable byte length.
        max: u32,
    },

    /// Reassembled chunk bytes did not match the pointer metadata.
    #[error("TOAST value_oid {oid} assembled {actual} bytes, expected {expected}")]
    SizeMismatch {
        /// OID of the value being fetched.
        oid: u64,
        /// Actual byte count assembled from chunks.
        actual: usize,
        /// Expected byte count recorded by the pointer.
        expected: usize,
    },

    /// A chunk sequence number cannot fit the on-disk 32-bit field.
    #[error("TOAST value_oid {oid} has too many chunks: sequence {seq} exceeds u32")]
    TooManyChunks {
        /// OID of the value being written or fetched.
        oid: u64,
        /// Chunk sequence that could not be represented.
        seq: usize,
    },

    /// The per-relation TOAST value OID counter has no next valid value.
    #[error("TOAST value OID counter exhausted for relation {rel}")]
    OidExhausted {
        /// TOAST relation whose counter is exhausted.
        rel: u32,
    },
}

/// Pointer to an external TOAST value.
///
/// When a value is too large to fit inline, the caller stores a
/// `ToastPointer` in the main tuple payload (encoding is the caller's
/// responsibility) and the real bytes live in the TOAST table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ToastPointer {
    /// OID of the TOAST relation that owns this value.
    pub toast_relation: RelationId,
    /// Unique 64-bit identifier for this value within the TOAST table.
    pub value_oid: u64,
    /// Uncompressed size of the value in bytes.
    pub raw_size: u32,
    /// Compressed size stored in the TOAST table. Equal to `raw_size`
    /// when no compression was applied.
    pub compressed_size: u32,
}

impl ToastPointer {
    /// `true` when the stored bytes are compressed.
    #[must_use]
    pub const fn is_compressed(&self) -> bool {
        self.compressed_size < self.raw_size
    }
}

/// The result of [`maybe_toast`]: either an inline value or an external
/// pointer.
#[derive(Debug)]
pub enum ToastDatum {
    /// Value is small enough to store inline in the main tuple.
    Inline(Vec<u8>),
    /// Value was stored externally; the pointer identifies it.
    External(ToastPointer),
}

/// Per-relation TOAST table.
///
/// A `ToastTable` wraps a heap relation (identified by `rel`) and exposes
/// `store`/`fetch`/`free` for oversize attribute values.
///
/// # XID / `command_id` for chunk writes
///
/// TOAST writes use an internal bootstrap XID and command id. In production
/// these would be threaded from the owning transaction; wiring that through
/// is deferred to the executor-integration milestone.
pub struct ToastTable<L: PageLoader> {
    pool: Arc<BufferPool<L>>,
    rel: RelationId,
    /// Per-relation OID counter; starts at 1 (0 is reserved as invalid).
    next_oid: Arc<AtomicU64>,
    /// Shared heap accessor backed by the same pool.
    heap: Arc<HeapAccess<L>>,
    /// Track which `TupleId`s belong to which `value_oid` for `free`.
    ///
    /// `DashMap<value_oid, Vec<TupleId>>` — populated on `store` and
    /// consumed by `free`. In a persistent implementation this index
    /// would be rebuilt from the TOAST heap on recovery; for v0.3 the
    /// in-memory map is sufficient.
    chunk_index: DashMap<u64, Vec<TupleId>>,
}

impl<L: PageLoader> std::fmt::Debug for ToastTable<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToastTable")
            .field("rel", &self.rel)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader> ToastTable<L> {
    /// Create a new TOAST table backed by `pool` using relation `rel`.
    #[must_use]
    pub fn new(pool: Arc<BufferPool<L>>, rel: RelationId) -> Self {
        let heap = Arc::new(HeapAccess::new(Arc::clone(&pool)));
        Self {
            pool,
            rel,
            next_oid: Arc::new(AtomicU64::new(1)),
            heap,
            chunk_index: DashMap::new(),
        }
    }

    /// Store `bytes` in the TOAST table and return a pointer.
    ///
    /// The bytes are compressed with lz4 if compression saves at least
    /// 5 bytes. The data is then split
    /// into chunks of at most [`TOAST_MAX_CHUNK_SIZE`] bytes each and
    /// stored as heap tuples in `self.rel`.
    pub fn store(&self, bytes: &[u8]) -> Result<ToastPointer, ToastError> {
        let raw_size = toast_size_from_len(bytes.len(), "raw")?;

        // Try lz4 compression.
        let (stored_bytes, compressed_size) = {
            let compressed = compress_prepend_size(bytes);
            if bytes.len().saturating_sub(compressed.len()) >= COMPRESSION_SAVINGS_THRESHOLD {
                let csz = toast_size_from_len(compressed.len(), "compressed")?;
                (compressed, csz)
            } else {
                let rsz = raw_size;
                (bytes.to_vec(), rsz)
            }
        };

        let value_oid = self.allocate_value_oid()?;
        let mut tids = Vec::new();

        let opts = InsertOptions {
            xmin: Xid::BOOTSTRAP,
            command_id: CommandId::FIRST,
            wal: None,
            fsm: None,
            vm: None,
        };

        for (seq, chunk) in stored_bytes.chunks(TOAST_MAX_CHUNK_SIZE).enumerate() {
            let chunk_seq = chunk_seq_from_index(value_oid, seq)?;
            let chunk_tuple = build_chunk_tuple(value_oid, chunk_seq, chunk);
            let tid = self.heap.insert(self.rel, &chunk_tuple, opts)?;
            tids.push(tid);
        }

        self.chunk_index.insert(value_oid, tids);

        Ok(ToastPointer {
            toast_relation: self.rel,
            value_oid,
            raw_size,
            compressed_size,
        })
    }

    fn allocate_value_oid(&self) -> Result<u64, ToastError> {
        loop {
            let current = self.next_oid.load(Ordering::Relaxed);
            if current == 0 || current == u64::MAX {
                return Err(ToastError::OidExhausted {
                    rel: self.rel.oid().raw(),
                });
            }
            let next = current.checked_add(1).ok_or(ToastError::OidExhausted {
                rel: self.rel.oid().raw(),
            })?;
            match self
                .next_oid
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return Ok(current),
                Err(_) => continue,
            }
        }
    }

    /// Fetch and reassemble the value identified by `ptr`.
    ///
    /// Returns the original uncompressed bytes. Decompresses if
    /// `ptr.is_compressed()`.
    pub fn fetch(&self, ptr: &ToastPointer) -> Result<Vec<u8>, ToastError> {
        if ptr.toast_relation != self.rel {
            return Err(ToastError::WrongRelation {
                ptr_rel: ptr.toast_relation.oid().raw(),
                table_rel: self.rel.oid().raw(),
            });
        }

        // Collect all chunks by scanning the heap for this value_oid.
        let mut chunks: Vec<(u32, Vec<u8>)> = Vec::new();

        let block_count = self.heap.block_count(self.rel);
        for block in 0..block_count {
            let page_id = PageId::new(self.rel, BlockNumber::new(block));
            let guard = self.pool.get_page(page_id).map_err(HeapError::BufferPool)?;
            let page = guard.read();
            let header = page.header();
            let slot_count = header.slot_count();
            drop(page);
            drop(guard);

            for slot in 0..slot_count {
                let tid = TupleId::new(page_id, slot);
                let Ok(heap_tuple) = self.heap.fetch(tid) else {
                    continue;
                };
                let data = &heap_tuple.data;
                if data.len() < CHUNK_HEADER_SIZE {
                    continue;
                }
                let oid = read_u64_le(data);
                if oid != ptr.value_oid {
                    continue;
                }
                let seq = read_u32_le(&data[8..]);
                let payload = data[CHUNK_HEADER_SIZE..].to_vec();
                chunks.push((seq, payload));
            }
        }

        // Sort by sequence number and reassemble.
        chunks.sort_by_key(|(seq, _)| *seq);

        let compressed_size = toast_size_to_usize(ptr.compressed_size, "pointer compressed")?;
        let expected_chunks = compressed_size.div_ceil(TOAST_MAX_CHUNK_SIZE);
        if chunks.len() < expected_chunks {
            let seq = chunk_seq_from_index(ptr.value_oid, chunks.len())?;
            return Err(ToastError::MissingChunk {
                oid: ptr.value_oid,
                seq,
            });
        }

        let mut assembled: Vec<u8> = Vec::with_capacity(compressed_size.min(TOAST_MAX_CHUNK_SIZE));
        for (expected_seq, (seq, payload)) in chunks.into_iter().enumerate() {
            if expected_seq >= expected_chunks {
                let actual = match assembled.len().checked_add(payload.len()) {
                    Some(len) => len,
                    None => usize::MAX,
                };
                return Err(ToastError::SizeMismatch {
                    oid: ptr.value_oid,
                    actual,
                    expected: compressed_size,
                });
            }
            let expected_seq = chunk_seq_from_index(ptr.value_oid, expected_seq)?;
            if seq != expected_seq {
                return Err(ToastError::MissingChunk {
                    oid: ptr.value_oid,
                    seq: expected_seq,
                });
            }
            let next_len =
                assembled
                    .len()
                    .checked_add(payload.len())
                    .ok_or(ToastError::SizeMismatch {
                        oid: ptr.value_oid,
                        actual: usize::MAX,
                        expected: compressed_size,
                    })?;
            if next_len > compressed_size {
                return Err(ToastError::SizeMismatch {
                    oid: ptr.value_oid,
                    actual: next_len,
                    expected: compressed_size,
                });
            }
            assembled.extend_from_slice(&payload);
        }
        if assembled.len() != compressed_size {
            return Err(ToastError::SizeMismatch {
                oid: ptr.value_oid,
                actual: assembled.len(),
                expected: compressed_size,
            });
        }

        // Decompress if the data was compressed.
        if ptr.is_compressed() {
            decompress_size_prepended(&assembled).map_err(|e| ToastError::Decompress(e.to_string()))
        } else {
            Ok(assembled)
        }
    }

    /// Free all chunks belonging to `ptr`.
    ///
    /// After this call the chunks are marked dead in the heap; VACUUM
    /// will reclaim the space.
    pub fn free(&self, ptr: &ToastPointer) -> Result<(), ToastError> {
        if ptr.toast_relation != self.rel {
            return Err(ToastError::WrongRelation {
                ptr_rel: ptr.toast_relation.oid().raw(),
                table_rel: self.rel.oid().raw(),
            });
        }

        if let Some((_, tids)) = self.chunk_index.remove(&ptr.value_oid) {
            let delete_opts = crate::heap::DeleteOptions {
                xmax: Xid::BOOTSTRAP,
                cmax: CommandId::FIRST,
                wal: None,
                fsm: None,
                vm: None,
            };
            for tid in tids {
                self.heap.delete(tid, delete_opts)?;
            }
        }
        Ok(())
    }
}

/// Store or inline `value` depending on whether it exceeds `threshold`.
///
/// - If `value.len() <= threshold`: return `ToastDatum::Inline(value.to_vec())`.
/// - Otherwise: call [`ToastTable::store`] and return `ToastDatum::External(ptr)`.
pub fn maybe_toast<L: PageLoader>(
    value: &[u8],
    target: &ToastTable<L>,
    threshold: usize,
) -> Result<ToastDatum, ToastError> {
    if value.len() <= threshold {
        Ok(ToastDatum::Inline(value.to_vec()))
    } else {
        let ptr = target.store(value)?;
        Ok(ToastDatum::External(ptr))
    }
}

// ------------------------------------------------------------------
// Internal helpers
// ------------------------------------------------------------------

/// Build a chunk tuple: `value_oid (u64 LE) | chunk_seq (u32 LE) | payload`.
fn build_chunk_tuple(value_oid: u64, chunk_seq: u32, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(CHUNK_HEADER_SIZE + payload.len());
    buf.extend_from_slice(&value_oid.to_le_bytes());
    buf.extend_from_slice(&chunk_seq.to_le_bytes());
    buf.extend_from_slice(payload);
    buf
}

fn toast_size_from_len(len: usize, context: &'static str) -> Result<u32, ToastError> {
    u32::try_from(len).map_err(|_| ToastError::ValueTooLarge {
        context,
        len,
        max: u32::MAX,
    })
}

fn toast_size_to_usize(size: u32, context: &'static str) -> Result<usize, ToastError> {
    usize::try_from(size).map_err(|_| ToastError::ValueTooLarge {
        context,
        len: usize::MAX,
        max: u32::MAX,
    })
}

fn chunk_seq_from_index(oid: u64, seq: usize) -> Result<u32, ToastError> {
    u32::try_from(seq).map_err(|_| ToastError::TooManyChunks { oid, seq })
}

fn read_u64_le(bytes: &[u8]) -> u64 {
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

fn read_u32_le(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{PageId, Result};

    use super::*;
    use crate::buffer_pool::BufferPool;
    use crate::page::Page;

    struct MemLoader;

    impl crate::buffer_pool::PageLoader for MemLoader {
        fn load(&self, _page_id: PageId) -> Result<Page> {
            Ok(Page::new_heap())
        }
    }

    fn make_toast(rel_id: u32) -> ToastTable<MemLoader> {
        let pool = Arc::new(BufferPool::new(512, MemLoader));
        ToastTable::new(pool, RelationId::new(rel_id))
    }

    #[test]
    fn inline_small_value() {
        let table = make_toast(1);
        let data: Vec<u8> = (0..100).collect();
        match maybe_toast(&data, &table, TOAST_INLINE_MAX).unwrap() {
            ToastDatum::Inline(v) => assert_eq!(v, data),
            ToastDatum::External(_) => panic!("expected inline"),
        }
    }

    #[test]
    fn store_and_fetch_2_kib() {
        let table = make_toast(2);
        // 2049 bytes — just over the inline threshold.
        let data: Vec<u8> = (0u8..=255).cycle().take(2049).collect();
        let ptr = table.store(&data).unwrap();
        let recovered = table.fetch(&ptr).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn store_and_fetch_100_kib() {
        let table = make_toast(3);
        let data: Vec<u8> = (0u8..=255).cycle().take(100 * 1024).collect();
        let ptr = table.store(&data).unwrap();
        let recovered = table.fetch(&ptr).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn store_and_fetch_1_mib() {
        let table = make_toast(4);
        let data: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
        let ptr = table.store(&data).unwrap();
        let recovered = table.fetch(&ptr).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn lz4_compresses_repeating_data() {
        let table = make_toast(5);
        // All-zero slice: highly compressible.
        let data: Vec<u8> = vec![0u8; 64 * 1024];
        let ptr = table.store(&data).unwrap();
        // The compressed size should be much smaller than raw.
        assert!(
            ptr.is_compressed(),
            "expected compression for all-zero 64 KiB"
        );
        let recovered = table.fetch(&ptr).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn free_marks_chunks_dead() {
        let table = make_toast(6);
        let data: Vec<u8> = (0u8..=255).cycle().take(8 * 1024).collect();
        let ptr = table.store(&data).unwrap();
        // Ensure we can fetch before free.
        let _ = table.fetch(&ptr).unwrap();
        // Free succeeds without error.
        table.free(&ptr).unwrap();
        // The chunk_index entry is removed.
        assert!(!table.chunk_index.contains_key(&ptr.value_oid));
    }

    #[test]
    fn wrong_relation_error_on_fetch() {
        let table = make_toast(7);
        let ptr = ToastPointer {
            toast_relation: RelationId::new(999),
            value_oid: 1,
            raw_size: 100,
            compressed_size: 100,
        };
        let err = table.fetch(&ptr).unwrap_err();
        assert!(matches!(err, ToastError::WrongRelation { .. }));
    }

    #[test]
    fn wrong_relation_error_on_free() {
        let table = make_toast(8);
        let ptr = ToastPointer {
            toast_relation: RelationId::new(888),
            value_oid: 1,
            raw_size: 10,
            compressed_size: 10,
        };
        let err = table.free(&ptr).unwrap_err();
        assert!(matches!(err, ToastError::WrongRelation { .. }));
    }

    #[test]
    fn maybe_toast_external_path() {
        let table = make_toast(9);
        let data: Vec<u8> = vec![42u8; TOAST_INLINE_MAX + 1];
        match maybe_toast(&data, &table, TOAST_INLINE_MAX).unwrap() {
            ToastDatum::External(ptr) => {
                let recovered = table.fetch(&ptr).unwrap();
                assert_eq!(recovered, data);
            }
            ToastDatum::Inline(_) => panic!("expected external"),
        }
    }

    #[test]
    fn pointer_oids_are_unique() {
        let table = make_toast(10);
        let a_data: Vec<u8> = vec![1u8; 3000];
        let b_data: Vec<u8> = vec![2u8; 3000];
        let a = table.store(&a_data).unwrap();
        let b = table.store(&b_data).unwrap();
        assert_ne!(a.value_oid, b.value_oid);
    }

    #[test]
    fn toast_size_conversion_rejects_u32_overflow() {
        let too_large = usize::try_from(u64::from(u32::MAX) + 1).unwrap();
        let err = toast_size_from_len(too_large, "raw").unwrap_err();
        assert!(matches!(err, ToastError::ValueTooLarge { .. }));
    }

    #[test]
    fn fetch_rejects_pointer_size_smaller_than_stored_payload() {
        let table = make_toast(11);
        let data: Vec<u8> = (0u8..=255).cycle().take(3000).collect();
        let mut ptr = table.store(&data).unwrap();
        ptr.raw_size = 1;
        ptr.compressed_size = 1;

        let err = table.fetch(&ptr).unwrap_err();
        assert!(matches!(err, ToastError::SizeMismatch { .. }));
    }

    #[test]
    fn store_rejects_value_oid_exhaustion_without_wrapping() {
        let table = make_toast(12);
        table.next_oid.store(u64::MAX, Ordering::Relaxed);
        let data: Vec<u8> = vec![7u8; 3000];

        let err = table.store(&data).unwrap_err();
        assert!(matches!(err, ToastError::OidExhausted { .. }));
        assert_eq!(table.next_oid.load(Ordering::Relaxed), u64::MAX);
    }
}
