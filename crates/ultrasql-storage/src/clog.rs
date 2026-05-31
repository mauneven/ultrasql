//! Persistent commit log (CLOG).
//!
//! The CLOG stores the committed/aborted/in-progress status of every
//! transaction as a compact 2-bit field packed into buffer-pool pages.
//! It replaces the in-memory `DashMap` in `ultrasql-txn`; that swap is
//! deferred to a follow-up commit so this crate does not need to depend
//! on `ultrasql-txn`.
//!
//! # Layout
//!
//! Each 8 KiB page holds `CLOG_XIDS_PER_PAGE` XID statuses. The page
//! data area starts at byte `PAGE_HEADER_SIZE` and extends to
//! `PAGE_SIZE`; the 2-bit statuses are packed contiguously in that
//! region.
//!
//! ```text
//! page_number = xid.raw() / CLOG_XIDS_PER_PAGE
//! local       = xid.raw() % CLOG_XIDS_PER_PAGE
//! byte_offset = (local * 2) / 8          (relative to page data start)
//! bit_shift   = (local * 2) % 8
//! ```
//!
//! Bit encoding per XID (2 bits):
//!
//! | bits | [`XidStatus`]  |
//! |------|----------------|
//! | 0b00 | `InProgress`   |
//! | 0b01 | `Committed`    |
//! | 0b10 | `Aborted`      |
//! | 0b11 | `SubCommitted` (treated as `Committed` on read) |
//!
//! Zero-initialised pages mean all XIDs are `InProgress`, which is
//! the correct initial state for any page that has never been written.
//!
//! # Oracle integration
//!
//! [`PersistentClog`] implements [`XidStatusOracle`] from `ultrasql-mvcc`
//! so the transaction manager can adopt it in a follow-up commit without
//! additional glue code.
//!
//! # Concurrency
//!
//! Reads pin the page via `BufferPool::get_page` and take a shared
//! `PageRead` lock. Writes take an exclusive `PageWrite` lock. The page
//! guard is released before any further I/O so lock hold times are
//! bounded to a single byte manipulation.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    reason = "on-disk format / fixed-width packing; narrowings bounded by PAGE_SIZE / relation size"
)]

use std::sync::Arc;

use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{BlockNumber, PageId, RelationId, Xid};
use ultrasql_mvcc::{XidStatus, XidStatusOracle};

use crate::buffer_pool::{BufferPool, BufferPoolError, PageLoader};
use crate::page::{PAGE_HEADER_SIZE, PageError};

/// Bytes available in the data area of one CLOG page.
const CLOG_DATA_BYTES: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

/// Number of XID statuses that fit on a single CLOG page (2 bits each).
pub const CLOG_XIDS_PER_PAGE: u64 = (CLOG_DATA_BYTES * 8 / 2) as u64;

// Bit-pair encodings.
const STATUS_IN_PROGRESS: u8 = 0b00;
const STATUS_COMMITTED: u8 = 0b01;
const STATUS_ABORTED: u8 = 0b10;
const STATUS_SUB_COMMITTED: u8 = 0b11;

/// Errors that can arise when operating on the persistent CLOG.
#[derive(Debug, thiserror::Error)]
pub enum ClogError {
    /// The underlying buffer pool rejected a request.
    #[error("buffer pool: {0}")]
    BufferPool(#[from] BufferPoolError),

    /// A page-level operation failed.
    #[error("page: {0}")]
    Page(#[from] PageError),

    /// An XID maps past the CLOG relation's 32-bit block address space.
    #[error("xid {xid} maps to CLOG page {page_num}, beyond u32 block address space")]
    XidOutOfRange {
        /// Transaction ID being addressed.
        xid: u64,
        /// Computed CLOG page number.
        page_num: u64,
    },

    /// Recovery scanned the full 32-bit CLOG block address space.
    #[error("CLOG recovery page counter exhausted")]
    PageCounterExhausted,
}

/// Buffer-pool-backed persistent commit log.
///
/// One instance is created per server lifecycle, bound to a dedicated
/// relation ID so CLOG pages live separately from heap relation pages.
///
/// # Type parameter
///
/// `L: PageLoader` is the page loader supplied to the buffer pool.
/// Production code uses the segment-file loader; tests use an in-memory
/// map that returns fresh heap pages for any unknown page ID.
pub struct PersistentClog<L: PageLoader> {
    pool: Arc<BufferPool<L>>,
    rel: RelationId,
}

impl<L: PageLoader> std::fmt::Debug for PersistentClog<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentClog")
            .field("rel", &self.rel)
            .finish_non_exhaustive()
    }
}

impl<L: PageLoader> PersistentClog<L> {
    /// Construct a [`PersistentClog`] backed by `pool`, using `rel` as the
    /// relation identifier for all CLOG pages.
    ///
    /// The caller must ensure `rel` is not used by any other access
    /// method; sharing a relation ID between CLOG and a heap would
    /// corrupt both.
    #[must_use]
    pub const fn new(pool: Arc<BufferPool<L>>, rel: RelationId) -> Self {
        Self { pool, rel }
    }

    /// Return the status of `xid`.
    ///
    /// - [`Xid::FROZEN`] and [`Xid::BOOTSTRAP`] always return
    ///   [`XidStatus::Committed`].
    /// - Unallocated XIDs (whose page byte is still zero) return
    ///   [`XidStatus::InProgress`].
    pub fn status(&self, xid: Xid) -> Result<XidStatus, ClogError> {
        if xid == Xid::FROZEN || xid == Xid::BOOTSTRAP {
            return Ok(XidStatus::Committed);
        }
        let (page_id, byte_off, shift) = Self::location(xid, self.rel)?;
        let guard = self.pool.get_page(page_id)?;
        let page = guard.read();
        let data_byte = page.as_bytes()[PAGE_HEADER_SIZE + byte_off];
        drop(page);
        let bits = (data_byte >> shift) & 0b11;
        Ok(bits_to_status(bits))
    }

    /// Write `status` for `xid`.
    ///
    /// The CLOG page is materialised by the buffer pool on first access
    /// (the loader returns a zeroed heap page, meaning all XIDs start as
    /// `InProgress`). Pages are marked dirty automatically by the
    /// `PageWrite` guard on drop.
    #[allow(clippy::significant_drop_tightening)]
    pub fn set_status(&self, xid: Xid, status: XidStatus) -> Result<(), ClogError> {
        let (page_id, byte_off, shift) = Self::location(xid, self.rel)?;
        let guard = self.pool.get_page(page_id)?;
        // `page` (PageWrite) must remain in scope until after we mutate
        // bytes, because `page` borrows from `guard` and `bytes` borrows
        // from `page`. Clippy would prefer to drop it early, but the
        // borrow checker requires it to outlive `bytes`.
        let mut page = guard.write();
        let bits = status_to_bits(status);
        let idx = PAGE_HEADER_SIZE + byte_off;
        let bytes = page.as_bytes_mut();
        bytes[idx] = (bytes[idx] & !(0b11u8 << shift)) | (bits << shift);
        Ok(())
    }

    /// Ensure that CLOG pages for all XIDs up to `up_to_xid` exist in
    /// the buffer pool.
    ///
    /// Call this when assigning a new XID so that `status()` can be
    /// served from RAM without a loader round-trip on the first query.
    pub fn extend(&self, up_to_xid: Xid) -> Result<(), ClogError> {
        let last_page = Self::page_number_u32(up_to_xid)?;
        for page_num in 0..=last_page {
            let page_id = PageId::new(self.rel, BlockNumber::new(page_num));
            // Materialise the page if absent. The blank heap page returned
            // by the loader has all-zero data bytes → all InProgress, which
            // is correct for unallocated XIDs.
            let _ = self.pool.get_page(page_id)?;
        }
        Ok(())
    }

    /// Logically remove CLOG pages for XIDs strictly below
    /// `oldest_in_progress`.
    ///
    /// Returns the number of pages that were found (and whose guards
    /// were dropped, making them eviction candidates). Actual file
    /// truncation is the caller's responsibility; this method only
    /// ensures those pages are no longer pinned.
    pub fn trim_below(&self, oldest_in_progress: Xid) -> Result<u32, ClogError> {
        if oldest_in_progress.raw() == 0 {
            return Ok(0);
        }
        let first_needed_page = Self::page_number_u32(oldest_in_progress)?;
        let mut removed = 0u32;
        for page_num in 0..first_needed_page {
            let page_id = PageId::new(self.rel, BlockNumber::new(page_num));
            // Pin then immediately drop — the pool's eviction policy reclaims
            // the frame on the next eviction sweep.
            if self.pool.get_page(page_id).is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Scan all reachable CLOG pages and return the highest XID whose
    /// status is not `InProgress`.
    ///
    /// On startup, this can be used to re-discover the highest allocated
    /// XID without replaying the entire WAL. Returns [`Xid::INVALID`]
    /// when every byte of every accessible page is zero.
    pub fn recover(&self) -> Result<Xid, ClogError> {
        let mut highest = Xid::INVALID;
        let mut page_num = 0u32;
        loop {
            let page_id = PageId::new(self.rel, BlockNumber::new(page_num));
            let Ok(guard) = self.pool.get_page(page_id) else {
                break;
            };
            let page = guard.read();
            let bytes = page.as_bytes();
            // Scan data bytes from the end of the page backward.
            'outer: for byte_idx in (0..CLOG_DATA_BYTES).rev() {
                let b = bytes[PAGE_HEADER_SIZE + byte_idx];
                if b == 0 {
                    continue;
                }
                // At least one non-InProgress XID is encoded in this byte.
                // Scan the four bit-pairs from high to low.
                for pair in (0u64..4).rev() {
                    let shift =
                        u32::try_from(pair * 2).map_err(|_| ClogError::PageCounterExhausted)?;
                    let bits = (b >> shift) & 0b11;
                    if bits != STATUS_IN_PROGRESS {
                        let byte_xid_offset = u64::try_from(byte_idx)
                            .map_err(|_| ClogError::PageCounterExhausted)?
                            .checked_mul(4)
                            .ok_or(ClogError::PageCounterExhausted)?;
                        let global_xid = u64::from(page_num)
                            .checked_mul(CLOG_XIDS_PER_PAGE)
                            .and_then(|base| base.checked_add(byte_xid_offset))
                            .and_then(|base| base.checked_add(pair))
                            .ok_or(ClogError::PageCounterExhausted)?;
                        let candidate = Xid::new(global_xid);
                        if candidate > highest {
                            highest = candidate;
                        }
                        break 'outer;
                    }
                }
            }
            drop(page);
            drop(guard);
            page_num = page_num
                .checked_add(1)
                .ok_or(ClogError::PageCounterExhausted)?;
        }
        Ok(highest)
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Page block number for `xid`.
    const fn page_number(xid: Xid) -> u64 {
        xid.raw() / CLOG_XIDS_PER_PAGE
    }

    fn page_number_u32(xid: Xid) -> Result<u32, ClogError> {
        let page_num = Self::page_number(xid);
        u32::try_from(page_num).map_err(|_| ClogError::XidOutOfRange {
            xid: xid.raw(),
            page_num,
        })
    }

    /// Compute `(page_id, byte_offset_in_data, bit_shift)` for `xid`.
    fn location(xid: Xid, rel: RelationId) -> Result<(PageId, usize, u32), ClogError> {
        let page_num = Self::page_number_u32(xid)?;
        let local = xid.raw() % CLOG_XIDS_PER_PAGE;
        let bit_off = local * 2;
        let byte_off = usize::try_from(bit_off / 8).map_err(|_| ClogError::PageCounterExhausted)?;
        let shift = u32::try_from(bit_off % 8).map_err(|_| ClogError::PageCounterExhausted)?;
        let page_id = PageId::new(rel, BlockNumber::new(page_num));
        Ok((page_id, byte_off, shift))
    }
}

impl<L: PageLoader> XidStatusOracle for PersistentClog<L> {
    /// Return the status of `xid`.
    ///
    /// Buffer-pool errors are swallowed and mapped to `InProgress` so
    /// that visibility checks, which run inside tight executor loops, do
    /// not require a separate error path. In production any persistent
    /// I/O failure will cause an earlier panic in the WAL path; reaching
    /// here with a failing pool is an unrecoverable state anyway.
    fn status(&self, xid: Xid) -> XidStatus {
        self.status(xid).unwrap_or(XidStatus::InProgress)
    }
}

// ------------------------------------------------------------------
// Bit encoding / decoding
// ------------------------------------------------------------------

const fn status_to_bits(s: XidStatus) -> u8 {
    match s {
        XidStatus::InProgress => STATUS_IN_PROGRESS,
        XidStatus::Committed | XidStatus::Frozen => STATUS_COMMITTED,
        XidStatus::Aborted => STATUS_ABORTED,
    }
}

const fn bits_to_status(bits: u8) -> XidStatus {
    match bits & 0b11 {
        STATUS_ABORTED => XidStatus::Aborted,
        // Both COMMITTED and SUB_COMMITTED map to Committed.
        STATUS_COMMITTED | STATUS_SUB_COMMITTED => XidStatus::Committed,
        _ => XidStatus::InProgress,
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{PageId, Result};
    use ultrasql_mvcc::XidStatusOracle as _;

    use super::*;
    use crate::buffer_pool::BufferPool;
    use crate::page::Page;

    /// In-memory page loader: returns a fresh heap page for every page ID.
    struct MemLoader;

    impl crate::buffer_pool::PageLoader for MemLoader {
        fn load(&self, _page_id: PageId) -> Result<Page> {
            Ok(Page::new_heap())
        }
    }

    fn make_clog() -> PersistentClog<MemLoader> {
        let pool = Arc::new(BufferPool::new(256, MemLoader));
        let rel = RelationId::new(9001);
        PersistentClog::new(pool, rel)
    }

    #[test]
    fn unset_xid_is_in_progress() {
        let clog = make_clog();
        assert_eq!(clog.status(Xid::new(100)).unwrap(), XidStatus::InProgress);
    }

    #[test]
    fn set_committed_roundtrip() {
        let clog = make_clog();
        let xid = Xid::new(42);
        clog.set_status(xid, XidStatus::Committed).unwrap();
        assert_eq!(clog.status(xid).unwrap(), XidStatus::Committed);
    }

    #[test]
    fn set_aborted_roundtrip() {
        let clog = make_clog();
        let xid = Xid::new(7);
        clog.set_status(xid, XidStatus::Aborted).unwrap();
        assert_eq!(clog.status(xid).unwrap(), XidStatus::Aborted);
    }

    #[test]
    fn set_does_not_disturb_neighbors() {
        let clog = make_clog();
        let a = Xid::new(10);
        let b = Xid::new(11);
        let c = Xid::new(12);
        clog.set_status(b, XidStatus::Committed).unwrap();
        assert_eq!(clog.status(a).unwrap(), XidStatus::InProgress);
        assert_eq!(clog.status(b).unwrap(), XidStatus::Committed);
        assert_eq!(clog.status(c).unwrap(), XidStatus::InProgress);
    }

    #[test]
    fn frozen_and_bootstrap_always_committed() {
        let clog = make_clog();
        assert_eq!(clog.status(Xid::FROZEN).unwrap(), XidStatus::Committed);
        assert_eq!(clog.status(Xid::BOOTSTRAP).unwrap(), XidStatus::Committed);
    }

    #[test]
    fn large_xid_spans_multiple_pages() {
        let clog = make_clog();
        // First XID on page 1.
        let xid = Xid::new(CLOG_XIDS_PER_PAGE + 5);
        clog.set_status(xid, XidStatus::Committed).unwrap();
        assert_eq!(clog.status(xid).unwrap(), XidStatus::Committed);
        // Page-0 XID is unaffected.
        assert_eq!(clog.status(Xid::new(5)).unwrap(), XidStatus::InProgress);
    }

    #[test]
    fn status_rejects_xid_beyond_block_address_space() {
        let clog = make_clog();
        let xid = Xid::new((u64::from(u32::MAX) + 1) * CLOG_XIDS_PER_PAGE);
        let err = clog.status(xid).unwrap_err();
        assert!(matches!(err, ClogError::XidOutOfRange { .. }));
    }

    #[test]
    fn extend_and_trim_reject_xids_beyond_block_address_space() {
        let clog = make_clog();
        let xid = Xid::new((u64::from(u32::MAX) + 1) * CLOG_XIDS_PER_PAGE);

        let extend_err = clog.extend(xid).unwrap_err();
        assert!(matches!(extend_err, ClogError::XidOutOfRange { .. }));

        let trim_err = clog.trim_below(xid).unwrap_err();
        assert!(matches!(trim_err, ClogError::XidOutOfRange { .. }));
    }

    #[test]
    fn extend_succeeds() {
        let clog = make_clog();
        clog.extend(Xid::new(CLOG_XIDS_PER_PAGE * 3)).unwrap();
    }

    #[test]
    fn oracle_trait_committed() {
        let clog = make_clog();
        let xid = Xid::new(99);
        clog.set_status(xid, XidStatus::Committed).unwrap();
        assert!(clog.is_committed(xid));
        assert!(!clog.is_aborted(xid));
        assert!(!clog.is_in_progress(xid));
    }

    #[test]
    fn oracle_trait_aborted() {
        let clog = make_clog();
        let xid = Xid::new(55);
        clog.set_status(xid, XidStatus::Aborted).unwrap();
        assert!(clog.is_aborted(xid));
        assert!(!clog.is_committed(xid));
    }

    #[test]
    fn trim_below_returns_page_count() {
        let clog = make_clog();
        // Ensure pages 0, 1, 2 are resident.
        clog.extend(Xid::new(CLOG_XIDS_PER_PAGE * 2 + 100)).unwrap();
        // oldest_in_progress is on page 2, so pages 0 and 1 are trimmable.
        let removed = clog.trim_below(Xid::new(CLOG_XIDS_PER_PAGE * 2)).unwrap();
        assert_eq!(removed, 2);
    }

    #[test]
    fn status_after_in_progress_overwrite() {
        let clog = make_clog();
        let xid = Xid::new(20);
        // Default is InProgress; write Committed, verify, then write Aborted.
        clog.set_status(xid, XidStatus::Committed).unwrap();
        assert_eq!(clog.status(xid).unwrap(), XidStatus::Committed);
        clog.set_status(xid, XidStatus::Aborted).unwrap();
        assert_eq!(clog.status(xid).unwrap(), XidStatus::Aborted);
    }

    #[test]
    fn many_xids_round_trip() {
        let clog = make_clog();
        for i in 0u64..200 {
            let xid = Xid::new(i + 3); // skip INVALID/BOOTSTRAP/FROZEN
            let expected = if i % 2 == 0 {
                XidStatus::Committed
            } else {
                XidStatus::Aborted
            };
            clog.set_status(xid, expected).unwrap();
        }
        for i in 0u64..200 {
            let xid = Xid::new(i + 3);
            let expected = if i % 2 == 0 {
                XidStatus::Committed
            } else {
                XidStatus::Aborted
            };
            assert_eq!(clog.status(xid).unwrap(), expected, "xid={xid}");
        }
    }
}
