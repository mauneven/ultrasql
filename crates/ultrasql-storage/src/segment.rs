//! Segment file manager.
//!
//! A *segment* is a fixed-size on-disk file holding a contiguous range
//! of 8 KiB pages for a single relation. UltraSQL splits each relation
//! into segments of up to [`SegmentConfig::segment_size_pages`] pages
//! (default `131_072`, i.e. 1 GiB at the default page size). The split
//! has three benefits:
//!
//! - File systems with per-file size limits (older ext4 inode quirks,
//!   FAT-derived host shares, networked file systems with conservative
//!   defaults) tolerate small files better than one giant per-relation
//!   blob.
//! - Bulk archival (`tar`, `cp`, `rsync`) chunks naturally on segment
//!   boundaries; backup streams cap memory at one segment.
//! - On macOS, our mmap path can map each segment with a private
//!   `MmapMut`. Re-mapping a 1 GiB segment after a `set_len` growth is
//!   cheap; re-mapping a 16 GiB relation is not.
//!
//! Layout on disk:
//!
//! ```text
//! <base_dir>/
//!   1/                  # relation OID 1
//!     0                 # segment 0 (up to N pages)
//!     1                 # segment 1
//!     ...
//!   7/
//!     0
//!     ...
//! ```
//!
//! Two IO backends are available:
//!
//! - **mmap** — each segment is mapped read-write. Reads `memcpy` from
//!   the map into a [`Page`] buffer (we don't expose the map to higher
//!   layers because every consumer expects an owned 8 KiB box). Writes
//!   `memcpy` into the map. Growth `set_len`s the file and re-mmaps.
//! - **pread/pwrite** — `read_at`/`write_at_all` via
//!   [`std::os::unix::fs::FileExt`]. This is the default on Linux where
//!   mmap-of-relations interacts badly with the page cache and dirty-
//!   page accounting (PostgreSQL has spent a decade not adopting mmap
//!   for these reasons). On macOS the default is mmap because Darwin's
//!   unified buffer cache makes the mmap path slightly faster in
//!   microbenchmarks; both paths are exercised by the test suite on
//!   either platform.
//!
//! Durability:
//!
//! - [`SegmentFileManager::fsync_relation`] syncs every segment file
//!   owned by the relation. On macOS, after the syscall `fsync(2)` we
//!   issue `fcntl(F_FULLFSYNC)`: Apple's `fsync(2)` is documented to
//!   buffer through the on-disk write cache and is *not* sufficient for
//!   crash durability; `F_FULLFSYNC` flushes the platter / NAND. The
//!   call is a no-op on Linux.

use std::fs::{self, File, OpenOptions};
use std::io;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use dashmap::DashMap;
use memmap2::{MmapMut, MmapOptions};
use parking_lot::{Mutex, RwLock};
use tracing::{debug, trace};
use ultrasql_core::constants::PAGE_SIZE;
use ultrasql_core::{BlockNumber, Error, PageId, RelationId, Result, SegmentId};

use crate::buffer_pool::PageLoader;
use crate::page::{Page, PageError};

/// Default number of pages per segment file (1 GiB at 8 KiB pages).
pub const DEFAULT_SEGMENT_SIZE_PAGES: u32 = 131_072;

/// Errors raised by the segment file manager.
#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    /// An IO call returned an error.
    #[error("segment io: {0}")]
    Io(#[from] io::Error),

    /// A page on disk failed structural or checksum validation.
    #[error("segment corruption: {0}")]
    Corruption(#[from] PageError),

    /// The on-disk layout violated an invariant: a file with a
    /// non-numeric name, a segment file of unexpected size, etc.
    #[error("segment layout: {0}")]
    Layout(&'static str),

    /// A read referenced a block past the end of its relation.
    #[error("block {requested:?} out of bounds for relation {rel:?} (relation has {size} blocks)")]
    OutOfBounds {
        /// The relation that was queried.
        rel: RelationId,
        /// The block number the caller asked for.
        requested: BlockNumber,
        /// The relation's current size in blocks.
        size: u32,
    },
}

impl From<SegmentError> for Error {
    fn from(value: SegmentError) -> Self {
        match value {
            SegmentError::Io(e) => Self::Io(e),
            SegmentError::Corruption(e) => Self::Corruption(e.to_string()),
            SegmentError::Layout(msg) => Self::Internal(msg),
            SegmentError::OutOfBounds { .. } => Self::InvalidArgument(value.to_string()),
        }
    }
}

/// Configuration for the segment file manager.
#[derive(Clone, Copy, Debug)]
pub struct SegmentConfig {
    /// Maximum number of pages stored in each segment file. The
    /// default (`131_072`, i.e. 1 GiB) keeps individual files small
    /// enough for ergonomic backup tools while large enough that the
    /// segment-boundary crossover is rare.
    pub segment_size_pages: u32,
    /// If `true`, use `memmap2::MmapMut` for the per-segment IO path.
    /// If `false`, fall back to `read_at` / `write_all_at`.
    ///
    /// The default mirrors the platform: mmap on macOS, pread/pwrite on
    /// Linux and other Unixes. Both paths are correct and pass the
    /// same test suite; this is a performance, not a correctness,
    /// switch.
    pub use_mmap: bool,
    /// When opening, create `base_dir` (and per-relation subdirs on
    /// demand) if they do not exist. Set to `false` to fail fast in
    /// production deployments where the engine should be pointed at an
    /// existing data directory.
    pub create_if_missing: bool,
    /// If `true`, every successful page read verifies the page's
    /// checksum and returns `SegmentError::Corruption` on mismatch.
    /// Disabling this is a 5–8 % throughput win for cold-cache scans
    /// and a correctness gun; leave it on outside benchmarks.
    pub verify_checksums: bool,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            segment_size_pages: DEFAULT_SEGMENT_SIZE_PAGES,
            use_mmap: cfg!(target_os = "macos"),
            create_if_missing: true,
            verify_checksums: true,
        }
    }
}

/// File-level handle for a single segment.
///
/// We store both the underlying [`File`] and an optional `MmapMut`. The
/// file is the source of truth for size; the mmap is a view that must
/// be re-created after a growth. We never drop the `File` while a map
/// exists pointing at it: the map borrows the kernel's reference to the
/// inode, but keeping the descriptor open is the simplest way to keep
/// `fsync`/`F_FULLFSYNC` available for the durability path.
#[derive(Debug)]
struct SegmentFile {
    /// On-disk path of the segment file. Used by errors and tracing.
    path: PathBuf,
    /// Open descriptor. Read/write; we never re-open after construction.
    file: File,
    /// Active mmap, or `None` for pread/pwrite mode. The map is
    /// recreated on file growth.
    mmap: Option<MmapMut>,
    /// Pages currently allocated in this segment. Always `<= cap`.
    pages: u32,
    /// Maximum pages this segment can hold.
    cap: u32,
}

impl SegmentFile {
    /// Open an existing segment file or create one of size zero.
    fn open(path: PathBuf, cap: u32, create: bool, use_mmap: bool) -> Result<Self, SegmentError> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);
        if create {
            opts.create(true);
        }
        let file = opts.open(&path)?;
        let meta = file.metadata()?;
        let len = meta.len();
        if len % PAGE_SIZE as u64 != 0 {
            return Err(SegmentError::Layout(
                "segment file length is not a multiple of PAGE_SIZE",
            ));
        }
        let pages_u64 = len / PAGE_SIZE as u64;
        let pages = u32::try_from(pages_u64)
            .map_err(|_| SegmentError::Layout("segment file holds more pages than u32"))?;
        if pages > cap {
            return Err(SegmentError::Layout("segment file exceeds configured cap"));
        }
        let mmap = if use_mmap && pages > 0 {
            Some(Self::map(&file, len)?)
        } else {
            None
        };
        Ok(Self {
            path,
            file,
            mmap,
            pages,
            cap,
        })
    }

    /// Build a fresh mmap for the file's current length.
    fn map(file: &File, len: u64) -> Result<MmapMut, SegmentError> {
        if len == 0 {
            return Err(SegmentError::Layout("cannot map a zero-length segment"));
        }
        let len_usize = usize::try_from(len)
            .map_err(|_| SegmentError::Layout("segment too large to map on this target"))?;
        // SAFETY: We hold an open `File` for the duration of the map's
        // lifetime (`mmap` is dropped before `file` because struct
        // fields drop in declaration order). The mapping is rw shared,
        // backed by a regular file we own exclusively in this process;
        // no other entity in this address space maps the same range.
        // Length matches the file's reported metadata, taken under the
        // segment's exclusive growth lock.
        let map = unsafe { MmapOptions::new().len(len_usize).map_mut(file)? };
        Ok(map)
    }

    /// Grow the file to hold `new_pages` total pages, re-mmap if
    /// necessary. The caller must already hold the relation-level
    /// growth lock so concurrent growers cannot race.
    fn grow_to(&mut self, new_pages: u32, use_mmap: bool) -> Result<(), SegmentError> {
        if new_pages <= self.pages {
            return Ok(());
        }
        if new_pages > self.cap {
            return Err(SegmentError::Layout("grow past segment cap"));
        }
        let new_len = u64::from(new_pages) * PAGE_SIZE as u64;
        // Drop the existing map BEFORE resizing the file. On macOS,
        // resizing a mapped file is technically allowed but the kernel
        // may surprise us with SIGBUS on access past the old size; on
        // Linux the `MAP_SHARED` mapping's behavior is "undefined past
        // the original end." Drop and re-map.
        self.mmap = None;
        self.file.set_len(new_len)?;
        self.pages = new_pages;
        if use_mmap {
            self.mmap = Some(Self::map(&self.file, new_len)?);
        }
        Ok(())
    }

    /// Truncate to `new_pages` pages. Drops the map first (see
    /// `grow_to` rationale) and re-maps after if mmap mode and pages
    /// remain.
    fn truncate_to(&mut self, new_pages: u32, use_mmap: bool) -> Result<(), SegmentError> {
        if new_pages >= self.pages {
            return Ok(());
        }
        let new_len = u64::from(new_pages) * PAGE_SIZE as u64;
        self.mmap = None;
        self.file.set_len(new_len)?;
        self.pages = new_pages;
        if use_mmap && new_pages > 0 {
            self.mmap = Some(Self::map(&self.file, new_len)?);
        }
        Ok(())
    }

    /// Copy a single page's bytes from disk into `dst`. The page must
    /// already exist on disk (`block_in_segment < self.pages`).
    fn read_page_into(
        &self,
        block_in_segment: u32,
        dst: &mut [u8; PAGE_SIZE],
    ) -> Result<(), SegmentError> {
        debug_assert!(block_in_segment < self.pages);
        let offset = u64::from(block_in_segment) * PAGE_SIZE as u64;
        if let Some(map) = self.mmap.as_ref() {
            let start = usize::try_from(offset)
                .map_err(|_| SegmentError::Layout("offset overflowed usize"))?;
            let end = start
                .checked_add(PAGE_SIZE)
                .ok_or(SegmentError::Layout("offset overflowed usize"))?;
            if end > map.len() {
                return Err(SegmentError::Layout("mmap shorter than expected"));
            }
            dst.copy_from_slice(&map[start..end]);
            return Ok(());
        }
        self.file.read_exact_at(dst, offset)?;
        Ok(())
    }

    /// Write a single page's bytes to disk.
    fn write_page_at(
        &mut self,
        block_in_segment: u32,
        src: &[u8; PAGE_SIZE],
    ) -> Result<(), SegmentError> {
        debug_assert!(block_in_segment < self.pages);
        let offset = u64::from(block_in_segment) * PAGE_SIZE as u64;
        if let Some(map) = self.mmap.as_mut() {
            let start = usize::try_from(offset)
                .map_err(|_| SegmentError::Layout("offset overflowed usize"))?;
            let end = start
                .checked_add(PAGE_SIZE)
                .ok_or(SegmentError::Layout("offset overflowed usize"))?;
            if end > map.len() {
                return Err(SegmentError::Layout("mmap shorter than expected"));
            }
            map[start..end].copy_from_slice(src);
            return Ok(());
        }
        self.file.write_all_at(src, offset)?;
        Ok(())
    }

    /// Flush mmap writes (if applicable) and `fsync` the file. On
    /// macOS, follows with `F_FULLFSYNC` for actual durability.
    fn fsync(&self) -> Result<(), SegmentError> {
        if let Some(map) = self.mmap.as_ref() {
            map.flush()?;
        }
        self.file.sync_all()?;
        full_fsync(&self.file)?;
        Ok(())
    }
}

/// Issue an OS-level "really flush to platter" call after the regular
/// `fsync`. Mac's `fsync(2)` is famously documented to leave data in
/// the drive's volatile cache; `fcntl(F_FULLFSYNC)` is the documented
/// way to force the flush. On Linux this is a no-op because `fsync(2)`
/// already does the right thing (modulo a few exotic file systems with
/// their own knobs).
#[cfg(target_os = "macos")]
fn full_fsync(file: &File) -> Result<(), SegmentError> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `file` is an open, owned descriptor for the entirety of
    // this call. `libc::F_FULLFSYNC` is the documented Apple-specific
    // command code; `fcntl` returns -1 and sets errno on failure.
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
    if rc == -1 {
        return Err(SegmentError::Io(io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
const fn full_fsync(_file: &File) -> Result<(), SegmentError> {
    Ok(())
}

/// Per-relation file set.
///
/// Holds the open segment files for one relation. Growing the relation
/// (allocating a new block or a new segment) serializes on
/// `growth_lock`; reads and per-page writes take only the segment's
/// `RwLock` and can proceed concurrently across segments.
#[derive(Debug)]
pub struct RelationFiles {
    /// Relation OID this file set belongs to.
    rel: RelationId,
    /// Directory holding the segment files.
    dir: PathBuf,
    /// Maximum pages per segment.
    segment_cap: u32,
    /// Use mmap?
    use_mmap: bool,
    /// Open segment files. `Vec` indexed by segment id; the entries are
    /// `Arc<RwLock<_>>` so callers can take a read lock without
    /// blocking peers and `allocate_block` can promote to write under
    /// the growth lock.
    segments: RwLock<Vec<Arc<RwLock<SegmentFile>>>>,
    /// Serializes growth (adding pages or segments). Reads never take
    /// this lock.
    growth_lock: Mutex<()>,
    /// Cached total page count. Updated under `growth_lock`. Reads use
    /// `Acquire` ordering.
    n_blocks: AtomicU32,
}

impl RelationFiles {
    /// Construct a new file set, scanning `dir` for existing segments.
    fn open_or_create(
        rel: RelationId,
        dir: PathBuf,
        segment_cap: u32,
        use_mmap: bool,
        create_if_missing: bool,
    ) -> Result<Arc<Self>, SegmentError> {
        if !dir.exists() {
            if !create_if_missing {
                return Err(SegmentError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("relation dir {} does not exist", dir.display()),
                )));
            }
            fs::create_dir_all(&dir)?;
        }

        let mut entries: Vec<(u32, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else {
                return Err(SegmentError::Layout("non-utf8 segment filename"));
            };
            let idx: u32 = name_str
                .parse()
                .map_err(|_| SegmentError::Layout("non-numeric segment filename"))?;
            entries.push((idx, entry.path()));
        }
        entries.sort_by_key(|&(idx, _)| idx);

        // Segment ids must be contiguous from 0; a hole indicates a
        // truncation gone wrong or filesystem corruption.
        for (expected, (idx, _)) in entries.iter().enumerate() {
            if u32::try_from(expected).is_ok_and(|e| e != *idx) {
                return Err(SegmentError::Layout("segment ids not contiguous"));
            }
        }

        let mut segments = Vec::with_capacity(entries.len());
        let mut total_blocks: u64 = 0;
        for (_idx, path) in entries {
            let seg = SegmentFile::open(path, segment_cap, false, use_mmap)?;
            total_blocks += u64::from(seg.pages);
            segments.push(Arc::new(RwLock::new(seg)));
        }

        // Enforce: every non-last segment is full to capacity. This
        // detects an interrupted truncate or an external tool that
        // edited the directory.
        if segments.len() >= 2 {
            for (i, seg_arc) in segments.iter().enumerate().take(segments.len() - 1) {
                let seg = seg_arc.read();
                if seg.pages != segment_cap {
                    debug!(
                        target: "ultrasql::storage::segment",
                        rel = ?rel,
                        segment = i,
                        pages = seg.pages,
                        cap = segment_cap,
                        "non-last segment is not full; possible truncate-in-progress"
                    );
                    return Err(SegmentError::Layout(
                        "non-last segment is not full to capacity",
                    ));
                }
            }
        }

        let n_blocks_u32 = u32::try_from(total_blocks)
            .map_err(|_| SegmentError::Layout("relation has more blocks than u32"))?;
        Ok(Arc::new(Self {
            rel,
            dir,
            segment_cap,
            use_mmap,
            segments: RwLock::new(segments),
            growth_lock: Mutex::new(()),
            n_blocks: AtomicU32::new(n_blocks_u32),
        }))
    }

    /// Total number of blocks currently allocated.
    fn size_blocks(&self) -> u32 {
        self.n_blocks.load(Ordering::Acquire)
    }

    const fn segment_for(&self, block: BlockNumber) -> (SegmentId, u32) {
        let raw = block.raw();
        let seg = raw / self.segment_cap;
        let within = raw % self.segment_cap;
        (SegmentId::new(seg), within)
    }

    fn read_page(&self, block: BlockNumber) -> Result<Page, SegmentError> {
        let size = self.size_blocks();
        if block.raw() >= size {
            return Err(SegmentError::OutOfBounds {
                rel: self.rel,
                requested: block,
                size,
            });
        }
        let (seg_id, within) = self.segment_for(block);
        let seg_arc = {
            let guard = self.segments.read();
            let idx = usize::try_from(seg_id.raw())
                .map_err(|_| SegmentError::Layout("segment id overflowed usize"))?;
            guard
                .get(idx)
                .ok_or(SegmentError::Layout("segment missing for in-range block"))?
                .clone()
        };
        let mut bytes_box: Box<[u8; PAGE_SIZE]> = {
            // Allocate zero-initialized; the read below fills it.
            let v = vec![0_u8; PAGE_SIZE].into_boxed_slice();
            v.try_into()
                .map_err(|_| SegmentError::Layout("allocation size mismatch"))?
        };
        {
            let seg = seg_arc.read();
            seg.read_page_into(within, &mut bytes_box)?;
        }
        let page = Page::from_bytes(bytes_box)?;
        trace!(
            target: "ultrasql::storage::segment",
            rel = ?self.rel,
            block = block.raw(),
            "read page"
        );
        Ok(page)
    }

    fn write_page(&self, block: BlockNumber, page: &mut Page) -> Result<(), SegmentError> {
        let size = self.size_blocks();
        if block.raw() >= size {
            return Err(SegmentError::OutOfBounds {
                rel: self.rel,
                requested: block,
                size,
            });
        }
        let (seg_id, within) = self.segment_for(block);
        page.refresh_checksum();
        let seg_arc = {
            let guard = self.segments.read();
            let idx = usize::try_from(seg_id.raw())
                .map_err(|_| SegmentError::Layout("segment id overflowed usize"))?;
            guard
                .get(idx)
                .ok_or(SegmentError::Layout("segment missing for in-range block"))?
                .clone()
        };
        {
            let mut seg = seg_arc.write();
            seg.write_page_at(within, page.as_bytes())?;
        }
        trace!(
            target: "ultrasql::storage::segment",
            rel = ?self.rel,
            block = block.raw(),
            "wrote page"
        );
        Ok(())
    }

    fn allocate_block(&self) -> Result<BlockNumber, SegmentError> {
        let _g = self.growth_lock.lock();
        // Re-read size under the lock; this is the authoritative value
        // because every grower holds `growth_lock`.
        let current = self.n_blocks.load(Ordering::Acquire);
        let new_block = current;
        let next_total = current
            .checked_add(1)
            .ok_or(SegmentError::Layout("block count overflow"))?;

        let (seg_id, within) = self.segment_for(BlockNumber::new(new_block));

        // Three cases on `seg_idx` versus the current segment count:
        //   - Equal: a new segment is needed.
        //   - Less:  grow an existing segment.
        //   - Greater: impossible if `growth_lock` is held correctly.
        let seg_idx = usize::try_from(seg_id.raw())
            .map_err(|_| SegmentError::Layout("segment id overflowed usize"))?;
        let current_seg_count = self.segments.read().len();
        match seg_idx.cmp(&current_seg_count) {
            std::cmp::Ordering::Equal => {
                let path = self.dir.join(format!("{}", seg_id.raw()));
                let mut new_seg = SegmentFile::open(path, self.segment_cap, true, self.use_mmap)?;
                // Brand-new file is length zero; grow it to one page.
                new_seg.grow_to(1, self.use_mmap)?;
                let mut guard = self.segments.write();
                guard.push(Arc::new(RwLock::new(new_seg)));
            }
            std::cmp::Ordering::Less => {
                let seg_arc = {
                    let guard = self.segments.read();
                    guard[seg_idx].clone()
                };
                let mut seg = seg_arc.write();
                // The next page index inside the segment is `within`;
                // total page count becomes `within + 1`.
                seg.grow_to(within + 1, self.use_mmap)?;
            }
            std::cmp::Ordering::Greater => {
                return Err(SegmentError::Layout(
                    "segment index jumped past current end",
                ));
            }
        }

        // Initialize the freshly-allocated page on disk with a
        // canonical empty heap page so a subsequent unconditional read
        // does not see arbitrary zeros that would fail header
        // validation.
        let mut blank = Page::new_heap();
        blank.refresh_checksum();
        let seg_arc = {
            let guard = self.segments.read();
            guard[seg_idx].clone()
        };
        {
            let mut seg = seg_arc.write();
            seg.write_page_at(within, blank.as_bytes())?;
        }

        self.n_blocks.store(next_total, Ordering::Release);
        debug!(
            target: "ultrasql::storage::segment",
            rel = ?self.rel,
            block = new_block,
            "allocated block"
        );
        Ok(BlockNumber::new(new_block))
    }

    fn truncate(&self, n_blocks: u32) -> Result<(), SegmentError> {
        let _g = self.growth_lock.lock();
        let current = self.n_blocks.load(Ordering::Acquire);
        if n_blocks >= current {
            return Ok(());
        }
        // Compute target segments and within-segment counts.
        let (last_seg_id_after, within_after) = if n_blocks == 0 {
            (0_u32, 0_u32)
        } else {
            let last_block = n_blocks - 1;
            let seg = last_block / self.segment_cap;
            let within = last_block % self.segment_cap;
            (seg, within + 1)
        };
        let segs_after_count = if n_blocks == 0 {
            0_usize
        } else {
            usize::try_from(last_seg_id_after + 1)
                .map_err(|_| SegmentError::Layout("segment count overflowed usize"))?
        };

        // Drop the surplus segments. We need to actually unlink the
        // files so disk space comes back and subsequent re-open sees a
        // consistent view.
        let surplus: Vec<Arc<RwLock<SegmentFile>>> = {
            let mut guard = self.segments.write();
            let mut surplus = Vec::new();
            while guard.len() > segs_after_count {
                if let Some(seg_arc) = guard.pop() {
                    surplus.push(seg_arc);
                }
            }
            surplus
        };
        // Unlink each surplus file. We must drop the SegmentFile (and
        // therefore close the descriptor) before unlinking on some
        // file systems; in practice POSIX permits unlinking an open
        // file, but it is cleaner to close first.
        for seg_arc in surplus {
            // Unique owner; the only references are this `seg_arc` and
            // the one we just popped (we own both via the pop).
            let path = seg_arc.read().path.clone();
            drop(seg_arc);
            if let Err(e) = fs::remove_file(&path) {
                if e.kind() != io::ErrorKind::NotFound {
                    return Err(e.into());
                }
            }
        }

        // Truncate the new-last segment to `within_after` pages, if
        // it exists.
        if segs_after_count > 0 {
            let seg_arc = {
                let guard = self.segments.read();
                guard[segs_after_count - 1].clone()
            };
            let mut seg = seg_arc.write();
            seg.truncate_to(within_after, self.use_mmap)?;
        }

        self.n_blocks.store(n_blocks, Ordering::Release);
        debug!(
            target: "ultrasql::storage::segment",
            rel = ?self.rel,
            n_blocks,
            "truncated relation"
        );
        Ok(())
    }

    fn fsync(&self) -> Result<(), SegmentError> {
        let segs: Vec<Arc<RwLock<SegmentFile>>> = self.segments.read().clone();
        for seg in segs {
            seg.read().fsync()?;
        }
        Ok(())
    }
}

/// Top-level segment file manager.
///
/// Maintains the set of opened [`RelationFiles`] in a [`DashMap`] so
/// concurrent access to distinct relations does not contend on a
/// single lock. The map is populated lazily: a relation's directory is
/// opened (or created) on the first call that names that relation.
#[derive(Debug)]
pub struct SegmentFileManager {
    base_dir: PathBuf,
    relations: DashMap<RelationId, Arc<RelationFiles>>,
    config: SegmentConfig,
}

impl SegmentFileManager {
    /// Open a segment manager rooted at `base_dir`. If
    /// `config.create_if_missing` is true and `base_dir` does not
    /// exist, the directory is created. Existing relation directories
    /// are *not* opened here; they are opened lazily on first
    /// reference, so a manager over a populated data directory boots
    /// in constant time.
    pub fn open(base_dir: impl Into<PathBuf>, config: SegmentConfig) -> Result<Self, SegmentError> {
        let base_dir = base_dir.into();
        if config.segment_size_pages == 0 {
            return Err(SegmentError::Layout("segment_size_pages must be non-zero"));
        }
        if !base_dir.exists() {
            if !config.create_if_missing {
                return Err(SegmentError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("base dir {} does not exist", base_dir.display()),
                )));
            }
            fs::create_dir_all(&base_dir)?;
        } else if !base_dir.is_dir() {
            return Err(SegmentError::Layout("base_dir is not a directory"));
        }
        debug!(
            target: "ultrasql::storage::segment",
            base = ?base_dir,
            ?config,
            "opened segment file manager"
        );
        Ok(Self {
            base_dir,
            relations: DashMap::new(),
            config,
        })
    }

    /// Read a single page from disk.
    pub fn read_page(&self, page_id: PageId) -> Result<Page, SegmentError> {
        let rel = self.relation(page_id.relation)?;
        let page = rel.read_page(page_id.block)?;
        if self.config.verify_checksums {
            page.verify_checksum()?;
        }
        Ok(page)
    }

    /// Write a single page to disk. The page's checksum is refreshed
    /// before the bytes hit storage.
    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<(), SegmentError> {
        let rel = self.relation(page_id.relation)?;
        // We need to refresh the checksum on the bytes being written,
        // but `write_page` takes `&Page`. The cheapest correct path is
        // to clone the bytes once; the alternative would be to require
        // `&mut Page` everywhere, which leaks the on-disk format up
        // into every caller. The clone is one box allocation; segment
        // writes are not the bottleneck.
        let bytes_box: Box<[u8; PAGE_SIZE]> = {
            let v = page.as_bytes().to_vec().into_boxed_slice();
            v.try_into()
                .map_err(|_| SegmentError::Layout("allocation size mismatch"))?
        };
        let mut copy = Page::from_bytes(bytes_box)?;
        rel.write_page(page_id.block, &mut copy)
    }

    /// Allocate a new block at the end of the relation. Returns the
    /// freshly-assigned block number. The block is initialized on disk
    /// to an empty heap page so a subsequent `read_page` is well-
    /// defined.
    pub fn allocate_block(&self, rel: RelationId) -> Result<BlockNumber, SegmentError> {
        let r = self.relation(rel)?;
        r.allocate_block()
    }

    /// Current number of allocated blocks in `rel`.
    pub fn relation_size_blocks(&self, rel: RelationId) -> Result<u32, SegmentError> {
        let r = self.relation(rel)?;
        Ok(r.size_blocks())
    }

    /// Flush all segments owned by `rel` to disk durably. On macOS,
    /// this includes `F_FULLFSYNC`.
    pub fn fsync_relation(&self, rel: RelationId) -> Result<(), SegmentError> {
        let r = self.relation(rel)?;
        r.fsync()
    }

    /// Truncate `rel` to `n_blocks` blocks. Surplus blocks (and their
    /// surplus segments) are dropped and the disk files unlinked. If
    /// `n_blocks` is greater than or equal to the current size, this
    /// is a no-op.
    pub fn truncate_relation(&self, rel: RelationId, n_blocks: u32) -> Result<(), SegmentError> {
        let r = self.relation(rel)?;
        r.truncate(n_blocks)
    }

    /// Open (or look up) the file set for one relation.
    fn relation(&self, rel: RelationId) -> Result<Arc<RelationFiles>, SegmentError> {
        if let Some(existing) = self.relations.get(&rel) {
            return Ok(existing.value().clone());
        }
        // Outside the fast path: open (possibly create) and insert.
        let dir = self.base_dir.join(format!("{}", rel.oid().raw()));
        let entry = self.relations.entry(rel).or_try_insert_with(|| {
            RelationFiles::open_or_create(
                rel,
                dir,
                self.config.segment_size_pages,
                self.config.use_mmap,
                self.config.create_if_missing,
            )
        })?;
        Ok(entry.value().clone())
    }
}

impl PageLoader for SegmentFileManager {
    fn load(&self, page_id: PageId) -> Result<Page> {
        self.read_page(page_id).map_err(Into::into)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use tempfile::TempDir;
    use ultrasql_core::{BlockNumber, PageId, RelationId};

    use super::*;
    use crate::buffer_pool::PageLoader;
    use crate::page::Page;

    fn config_mmap(use_mmap: bool) -> SegmentConfig {
        SegmentConfig {
            // 4 pages per segment so tests exercise segment-boundary
            // crossings without enormous files.
            segment_size_pages: 4,
            use_mmap,
            create_if_missing: true,
            verify_checksums: true,
        }
    }

    fn rel(n: u32) -> RelationId {
        RelationId::new(n)
    }

    fn pid(r: u32, b: u32) -> PageId {
        PageId::new(RelationId::new(r), BlockNumber::new(b))
    }

    fn make_payload(tag: u8) -> Vec<u8> {
        let mut v = vec![tag; 64];
        v[0] = tag;
        v[63] = tag.wrapping_add(1);
        v
    }

    /// Build a `Page` with a single inserted tuple so we can verify
    /// content survives the round-trip.
    fn page_with_tag(tag: u8) -> Page {
        let mut p = Page::new_heap();
        let slot = p.insert_tuple(&make_payload(tag)).unwrap();
        debug_assert_eq!(slot, 0);
        p
    }

    #[test]
    fn open_creates_base_dir_when_missing() {
        let tmp = TempDir::new().unwrap();
        let inner = tmp.path().join("nested").join("data");
        assert!(!inner.exists());
        let _mgr = SegmentFileManager::open(&inner, config_mmap(false)).unwrap();
        assert!(inner.exists());
    }

    #[test]
    fn write_then_read_round_trip_pread() {
        let tmp = TempDir::new().unwrap();
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        let r = rel(1);
        let blk = mgr.allocate_block(r).unwrap();
        let page = page_with_tag(0x42);
        mgr.write_page(PageId::new(r, blk), &page).unwrap();

        let read = mgr.read_page(PageId::new(r, blk)).unwrap();
        let tuple = read.read_tuple(0).unwrap();
        assert_eq!(tuple, make_payload(0x42).as_slice());
    }

    #[test]
    fn write_then_read_round_trip_mmap() {
        let tmp = TempDir::new().unwrap();
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(true)).unwrap();
        let r = rel(1);
        let blk = mgr.allocate_block(r).unwrap();
        let page = page_with_tag(0xA1);
        mgr.write_page(PageId::new(r, blk), &page).unwrap();

        let read = mgr.read_page(PageId::new(r, blk)).unwrap();
        let tuple = read.read_tuple(0).unwrap();
        assert_eq!(tuple, make_payload(0xA1).as_slice());
    }

    #[test]
    fn allocate_block_returns_increasing_block_numbers() {
        let tmp = TempDir::new().unwrap();
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        let r = rel(3);
        let mut prev: Option<u32> = None;
        // Allocate enough blocks to span multiple segments (cap = 4).
        for _ in 0..10_u32 {
            let blk = mgr.allocate_block(r).unwrap();
            if let Some(p) = prev {
                assert_eq!(blk.raw(), p + 1);
            } else {
                assert_eq!(blk.raw(), 0);
            }
            prev = Some(blk.raw());
        }
        assert_eq!(mgr.relation_size_blocks(r).unwrap(), 10);
    }

    #[test]
    fn allocate_block_spans_segments_with_correct_files() {
        let tmp = TempDir::new().unwrap();
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        let r = rel(7);
        // 6 blocks across 4-page segments → segments 0 (full), 1
        // (2 pages).
        for _ in 0..6 {
            mgr.allocate_block(r).unwrap();
        }
        let dir = tmp.path().join("7");
        let seg0 = dir.join("0");
        let seg1 = dir.join("1");
        assert!(seg0.exists());
        assert!(seg1.exists());
        let meta0 = std::fs::metadata(&seg0).unwrap();
        let meta1 = std::fs::metadata(&seg1).unwrap();
        assert_eq!(meta0.len(), 4 * PAGE_SIZE as u64);
        assert_eq!(meta1.len(), 2 * PAGE_SIZE as u64);
    }

    #[test]
    fn reopen_recovers_relation_size_and_data() {
        let tmp = TempDir::new().unwrap();
        let r = rel(11);
        {
            let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
            for i in 0..6_u32 {
                let blk = mgr.allocate_block(r).unwrap();
                let mut p = Page::new_heap();
                p.insert_tuple(&make_payload((i as u8).wrapping_add(1)))
                    .unwrap();
                mgr.write_page(PageId::new(r, blk), &p).unwrap();
            }
            mgr.fsync_relation(r).unwrap();
        }
        // Re-open and verify size + per-page content.
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        assert_eq!(mgr.relation_size_blocks(r).unwrap(), 6);
        for i in 0..6_u32 {
            let page = mgr.read_page(pid(11, i)).unwrap();
            let tup = page.read_tuple(0).unwrap();
            assert_eq!(tup, make_payload((i as u8).wrapping_add(1)).as_slice());
        }
    }

    #[test]
    fn corrupted_page_returns_corruption_error() {
        use std::io::{Seek, SeekFrom, Write};

        let tmp = TempDir::new().unwrap();
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        let r = rel(13);
        let blk = mgr.allocate_block(r).unwrap();
        let p = page_with_tag(0xCC);
        mgr.write_page(PageId::new(r, blk), &p).unwrap();
        // Drop the manager so we own the file exclusively for the
        // explicit corruption write.
        drop(mgr);

        // Flip a byte deep in the page payload area.
        let seg0 = tmp.path().join("13").join("0");
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&seg0)
            .unwrap();
        f.seek(SeekFrom::Start(5000)).unwrap();
        f.write_all(&[0xFF]).unwrap();
        f.sync_all().unwrap();
        drop(f);

        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        let err = mgr.read_page(PageId::new(r, blk)).unwrap_err();
        assert!(
            matches!(
                err,
                SegmentError::Corruption(PageError::ChecksumMismatch { .. })
            ),
            "expected ChecksumMismatch, got {err:?}"
        );
    }

    #[test]
    fn read_past_end_of_relation_is_out_of_bounds() {
        let tmp = TempDir::new().unwrap();
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        let r = rel(17);
        // Touch the relation by allocating one block, then read past
        // the end.
        let _ = mgr.allocate_block(r).unwrap();
        let err = mgr
            .read_page(PageId::new(r, BlockNumber::new(99)))
            .unwrap_err();
        match err {
            SegmentError::OutOfBounds {
                rel: e_rel,
                requested,
                size,
            } => {
                assert_eq!(e_rel, r);
                assert_eq!(requested.raw(), 99);
                assert_eq!(size, 1);
            }
            other => panic!("expected OutOfBounds, got {other:?}"),
        }
    }

    #[test]
    fn fsync_relation_succeeds() {
        let tmp = TempDir::new().unwrap();
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        let r = rel(19);
        for _ in 0..3 {
            mgr.allocate_block(r).unwrap();
        }
        mgr.fsync_relation(r).unwrap();
        // Second fsync must also succeed.
        mgr.fsync_relation(r).unwrap();
    }

    #[test]
    fn truncate_shrinks_segment_count() {
        let tmp = TempDir::new().unwrap();
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        let r = rel(23);
        // 10 blocks → segments 0,1 full (4 each), segment 2 with 2.
        for _ in 0..10 {
            mgr.allocate_block(r).unwrap();
        }
        assert_eq!(mgr.relation_size_blocks(r).unwrap(), 10);
        assert!(tmp.path().join("23").join("0").exists());
        assert!(tmp.path().join("23").join("1").exists());
        assert!(tmp.path().join("23").join("2").exists());

        // Truncate down to 3 blocks → only segment 0 should remain,
        // with 3 pages on disk.
        mgr.truncate_relation(r, 3).unwrap();
        assert_eq!(mgr.relation_size_blocks(r).unwrap(), 3);
        assert!(tmp.path().join("23").join("0").exists());
        assert!(!tmp.path().join("23").join("1").exists());
        assert!(!tmp.path().join("23").join("2").exists());
        let meta = std::fs::metadata(tmp.path().join("23").join("0")).unwrap();
        assert_eq!(meta.len(), 3 * PAGE_SIZE as u64);
    }

    #[test]
    fn truncate_to_zero_removes_all_files_but_keeps_dir() {
        let tmp = TempDir::new().unwrap();
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        let r = rel(29);
        for _ in 0..5 {
            mgr.allocate_block(r).unwrap();
        }
        mgr.truncate_relation(r, 0).unwrap();
        assert_eq!(mgr.relation_size_blocks(r).unwrap(), 0);
        assert!(tmp.path().join("29").exists());
        assert!(!tmp.path().join("29").join("0").exists());
    }

    #[test]
    fn concurrent_reads_do_not_panic() {
        let tmp = TempDir::new().unwrap();
        let mgr = Arc::new(SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap());
        let r = rel(31);
        // Set up 8 blocks, each with a uniquely-tagged tuple.
        for i in 0..8_u32 {
            let blk = mgr.allocate_block(r).unwrap();
            let mut p = Page::new_heap();
            p.insert_tuple(&make_payload(i as u8)).unwrap();
            mgr.write_page(PageId::new(r, blk), &p).unwrap();
        }

        let mut handles = Vec::new();
        for thread_id in 0..4_u32 {
            let mgr = Arc::clone(&mgr);
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    for b in 0..8_u32 {
                        let page = mgr.read_page(pid(31, b)).unwrap();
                        let t = page.read_tuple(0).unwrap();
                        assert_eq!(t, make_payload(b as u8).as_slice());
                    }
                }
                thread_id
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn concurrent_reads_mmap_do_not_panic() {
        let tmp = TempDir::new().unwrap();
        let mgr = Arc::new(SegmentFileManager::open(tmp.path(), config_mmap(true)).unwrap());
        let r = rel(37);
        for i in 0..6_u32 {
            let blk = mgr.allocate_block(r).unwrap();
            let mut p = Page::new_heap();
            p.insert_tuple(&make_payload(i as u8)).unwrap();
            mgr.write_page(PageId::new(r, blk), &p).unwrap();
        }
        let mut handles = Vec::new();
        for _ in 0..4_u32 {
            let mgr = Arc::clone(&mgr);
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    for b in 0..6_u32 {
                        let page = mgr.read_page(pid(37, b)).unwrap();
                        let _ = page.read_tuple(0).unwrap();
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn write_to_unallocated_block_returns_out_of_bounds() {
        let tmp = TempDir::new().unwrap();
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        let r = rel(41);
        // Touch the relation.
        mgr.allocate_block(r).unwrap();
        let p = page_with_tag(0x01);
        let err = mgr
            .write_page(PageId::new(r, BlockNumber::new(5)), &p)
            .unwrap_err();
        assert!(matches!(err, SegmentError::OutOfBounds { .. }));
    }

    #[test]
    fn page_loader_impl_loads_via_segment_files() {
        let tmp = TempDir::new().unwrap();
        let mgr = Arc::new(SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap());
        let r = rel(43);
        let blk = mgr.allocate_block(r).unwrap();
        let p = page_with_tag(0x55);
        mgr.write_page(PageId::new(r, blk), &p).unwrap();

        // Use the manager polymorphically as a PageLoader.
        let loader: &dyn PageLoader = mgr.as_ref();
        let fetched = loader.load(PageId::new(r, blk)).unwrap();
        assert_eq!(
            fetched.read_tuple(0).unwrap(),
            make_payload(0x55).as_slice()
        );
    }

    #[test]
    fn corrupt_layout_rejected_on_open() {
        // Create a segment dir with a non-numeric file in it; opening
        // the relation must fail with Layout.
        let tmp = TempDir::new().unwrap();
        let rel_dir = tmp.path().join("47");
        std::fs::create_dir_all(&rel_dir).unwrap();
        std::fs::write(rel_dir.join("not-a-number"), b"oops").unwrap();
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        let err = mgr.allocate_block(rel(47)).unwrap_err();
        assert!(matches!(err, SegmentError::Layout(_)), "got {err:?}");
    }

    #[test]
    fn reject_segment_with_non_page_aligned_length() {
        let tmp = TempDir::new().unwrap();
        let rel_dir = tmp.path().join("53");
        std::fs::create_dir_all(&rel_dir).unwrap();
        // Write a segment file of length 1 (not a multiple of PAGE_SIZE).
        std::fs::write(rel_dir.join("0"), b"x").unwrap();
        let mgr = SegmentFileManager::open(tmp.path(), config_mmap(false)).unwrap();
        let err = mgr.read_page(pid(53, 0)).unwrap_err();
        assert!(matches!(err, SegmentError::Layout(_)), "got {err:?}");
    }

    #[test]
    fn default_config_uses_platform_default_mmap() {
        let cfg = SegmentConfig::default();
        assert_eq!(cfg.use_mmap, cfg!(target_os = "macos"));
        assert_eq!(cfg.segment_size_pages, DEFAULT_SEGMENT_SIZE_PAGES);
        assert!(cfg.create_if_missing);
        assert!(cfg.verify_checksums);
    }
}
