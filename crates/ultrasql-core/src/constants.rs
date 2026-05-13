//! Shared engine-wide constants. These values are *protocol* in the sense
//! that other crates depend on them being stable. Changing any of them is
//! an RFC-level change.

/// On-disk and in-memory page size. Matches PostgreSQL's default; chosen
/// for compatibility with tuple-layout interop.
pub const PAGE_SIZE: usize = 8192;

/// Power-of-two log of [`PAGE_SIZE`].
pub const PAGE_SIZE_LOG2: u32 = 13;

/// Alignment of a tuple within a page. Matches the natural alignment of an
/// 8-byte primitive on every supported platform.
pub const TUPLE_ALIGN: usize = 8;

/// Maximum number of attributes (columns) in a single relation.
///
/// The catalog tuple stores attribute counts in `u16`, so the protocol
/// limit is `u16::MAX`; this lower bound is the engineering limit and is
/// enforced in DDL.
pub const MAX_ATTRIBUTES_PER_RELATION: u16 = 1_600;

/// Maximum identifier length. PostgreSQL defaults to `NAMEDATALEN = 64`;
/// UltraSQL doubles it to leave room for catalog name escaping.
pub const MAX_IDENTIFIER_LENGTH: usize = 128;

/// Maximum size of a single tuple before TOAST-style external storage
/// becomes mandatory. Conservatively chosen so a maximum-size tuple fits
/// inside a single page with header and slot overhead.
pub const MAX_INLINE_TUPLE_SIZE: usize = PAGE_SIZE - 256;

/// Default size of an executor batch (number of rows per `Batch`). The
/// value 4 096 keeps a typical batch in L1 / L2 cache while amortizing
/// per-batch overhead across enough rows to make vectorization pay.
pub const DEFAULT_BATCH_SIZE: usize = 4_096;

/// Cache-line size in bytes.
///
/// We use 64 because every supported CPU has 64-byte L1 lines today
/// (Apple Silicon, AMD Zen, Intel `x86_64`, Graviton, etc.). The Apple
/// M-series has 128-byte L2 cache lines but 64-byte L1 lines, so 64 is
/// the right granularity for false-sharing avoidance.
pub const CACHE_LINE_SIZE: usize = 64;
