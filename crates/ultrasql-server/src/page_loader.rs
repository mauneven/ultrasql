//! Spill-capable `PageLoader` backing the development server's buffer pool.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

/// Spill-capable `PageLoader` used by the development server.
///
/// Unwritten pages return freshly-initialized heap pages. Dirty pages can be
/// flushed into a per-process segment store, letting large in-process
/// benchmarks cycle buffer frames without losing heap contents.
///
/// `BufferPool` and `HeapAccess` are generic over `PageLoader`; making
/// the type concrete here lets us name the heap (`Arc<HeapAccess<BlankPageLoader>>`)
/// on `Server` and on the per-statement lowering context.
#[derive(Debug, Clone)]
pub struct BlankPageLoader {
    backing: Arc<BlankPageBacking>,
}

#[derive(Debug)]
pub(crate) enum BlankPageBacking {
    Segment {
        manager: Arc<SegmentFileManager>,
        _temp_dir: Option<tempfile::TempDir>,
    },
    Memory(Arc<dashmap::DashMap<PageId, Arc<[u8; PAGE_SIZE]>>>),
}

impl Default for BlankPageLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl BlankPageLoader {
    /// Create a loader backed by a temporary segment directory.
    #[must_use]
    pub fn new() -> Self {
        if matches!(
            std::env::var("ULTRASQL_PAGE_SPILL_BACKING").ok().as_deref(),
            Some("memory" | "MEMORY")
        ) {
            return Self {
                backing: Arc::new(BlankPageBacking::Memory(Arc::new(dashmap::DashMap::new()))),
            };
        }
        let config = SegmentConfig {
            use_mmap: false,
            verify_checksums: false,
            ..SegmentConfig::default()
        };
        let spill_root = std::env::var_os("ULTRASQL_PAGE_SPILL_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let backing = match tempfile::Builder::new()
            .prefix("ultrasql-page-spill-")
            .tempdir_in(spill_root)
            .and_then(|dir| {
                SegmentFileManager::open(dir.path().to_path_buf(), config)
                    .map(|manager| (dir, manager))
                    .map_err(std::io::Error::other)
            }) {
            Ok((dir, manager)) => BlankPageBacking::Segment {
                manager: Arc::new(manager),
                _temp_dir: Some(dir),
            },
            Err(e) => {
                warn!(
                    error = %e,
                    "page spill segment store unavailable; falling back to in-memory page store"
                );
                BlankPageBacking::Memory(Arc::new(dashmap::DashMap::new()))
            }
        };
        Self {
            backing: Arc::new(backing),
        }
    }

    /// Create a loader backed by stable segment files under `base_dir`.
    pub fn persistent(base_dir: impl AsRef<std::path::Path>) -> Result<Self, SegmentError> {
        // Durable user data: verify page checksums on every read so silent
        // bit-rot or a torn write on a page that escaped full-page-write
        // protection surfaces as a hard error instead of wrong query results.
        // The write path refreshes the checksum before each page hits storage,
        // so verification is consistent for data written by this build.
        let config = SegmentConfig {
            use_mmap: false,
            verify_checksums: true,
            ..SegmentConfig::default()
        };
        let manager = SegmentFileManager::open(base_dir.as_ref().to_path_buf(), config)?;
        Ok(Self {
            backing: Arc::new(BlankPageBacking::Segment {
                manager: Arc::new(manager),
                _temp_dir: None,
            }),
        })
    }

    /// Persist a dirty page so the buffer pool may evict its frame safely.
    pub fn store(&self, page_id: PageId, page: &Page) -> ultrasql_core::Result<()> {
        match self.backing.as_ref() {
            BlankPageBacking::Segment { manager, .. } => {
                while manager
                    .relation_size_blocks(page_id.relation)
                    .map_err(ultrasql_core::Error::from)?
                    <= page_id.block.raw()
                {
                    manager
                        .allocate_block(page_id.relation)
                        .map_err(ultrasql_core::Error::from)?;
                }
                manager
                    .write_page(page_id, page)
                    .map_err(ultrasql_core::Error::from)
            }
            BlankPageBacking::Memory(pages) => {
                pages.insert(page_id, Arc::new(*page.as_bytes()));
                Ok(())
            }
        }
    }

    /// Make every previously-stored page durable on disk.
    ///
    /// The checkpoint path calls this after flushing dirty heap pages so the
    /// checkpoint LSN reflects a durable on-disk state. A memory-backed loader
    /// has nothing to fsync.
    pub fn fsync_all(&self) -> ultrasql_core::Result<()> {
        match self.backing.as_ref() {
            BlankPageBacking::Segment { manager, .. } => {
                manager.fsync_all().map_err(ultrasql_core::Error::from)
            }
            BlankPageBacking::Memory(_) => Ok(()),
        }
    }

    /// Durable per-relation block counts discovered from the on-disk heap.
    ///
    /// Recovery seeds the heap's block counters from these so relation sizes are
    /// recovered from the durable storage rather than purely from WAL replay —
    /// the only thing that keeps scans correct once low WAL segments have been
    /// recycled. Empty for a memory-backed loader (nothing durable to discover).
    pub fn durable_relation_block_counts(
        &self,
    ) -> ultrasql_core::Result<Vec<(ultrasql_core::RelationId, u32)>> {
        match self.backing.as_ref() {
            BlankPageBacking::Segment { manager, .. } => manager
                .discover_relation_block_counts()
                .map_err(ultrasql_core::Error::from),
            BlankPageBacking::Memory(_) => Ok(Vec::new()),
        }
    }
}

impl PageLoader for BlankPageLoader {
    fn load(&self, page_id: PageId) -> ultrasql_core::Result<Page> {
        match self.backing.as_ref() {
            BlankPageBacking::Segment { manager, .. } => match manager.read_page(page_id) {
                Ok(page) => Ok(page),
                Err(SegmentError::OutOfBounds { .. }) => Ok(Page::new_heap()),
                Err(e) => Err(e.into()),
            },
            BlankPageBacking::Memory(pages) => {
                let Some(bytes) = pages.get(&page_id) else {
                    return Ok(Page::new_heap());
                };
                Page::from_bytes(Box::new(**bytes))
                    .map_err(|e| ultrasql_core::Error::Corruption(e.to_string()))
            }
        }
    }
}
