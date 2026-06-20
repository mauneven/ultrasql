//! BRIN (Block Range Index) min/max summaries.
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::type_complexity)]

use parking_lot::Mutex;
use ultrasql_core::TupleId;

use super::{AccessMethod, AccessMethodError};

// ---------------------------------------------------------------------------
// BRIN (Block Range Index) min/max summaries
// ---------------------------------------------------------------------------

/// Summary entry for one page range.
///
/// Each summary holds the min and max key observed across all tuples in
/// the page range. The executor uses this to skip ranges that cannot
/// contain the query's target key.
#[derive(Debug, Clone)]
struct BrinSummary {
    /// First block of the range.
    first_block: u32,
    /// Last block of the range (inclusive).
    last_block: u32,
    /// Minimum key seen in the range, or empty if no tuples inserted.
    min_key: Vec<u8>,
    /// Maximum key seen in the range.
    max_key: Vec<u8>,
}

/// BRIN (Block Range `INdex`) min/max index.
///
/// BRIN stores per-page-range min/max summaries rather than per-tuple
/// entries, making it highly space-efficient for naturally ordered data
/// (timestamps, sequential IDs). The trade-off is that a lookup must
/// scan all ranges whose `[min, max]` interval overlaps the query key.
///
/// # Key contract
///
/// Keys compare lexicographically. Integer callers should use
/// [`Self::encode_i64_key`] so signed `i64` order is preserved in the
/// byte domain.
///
/// # Status
///
/// Summaries are maintained in memory by the SQL runtime and consulted
/// by the heap-scan lowerer for block-range pruning. Page-backed,
/// WAL-recovered summary storage and non-integer operator classes remain
/// future work.
#[derive(Debug)]
pub struct BrinIndex {
    /// Summaries keyed by page range start.
    ///
    /// Future page-backed BRIN storage replaces this with WAL-logged
    /// summary pages in the buffer pool.
    summaries: Mutex<Vec<BrinSummary>>,
    /// Number of heap blocks per summary range.
    pages_per_range: u32,
}

impl BrinIndex {
    /// Create a BRIN index.
    ///
    /// `pages_per_range` controls how many heap pages each summary
    /// covers. The PostgreSQL default is 128.
    #[must_use]
    pub fn new(pages_per_range: u32) -> Self {
        Self {
            summaries: Mutex::new(Vec::new()),
            pages_per_range: pages_per_range.max(1),
        }
    }

    /// Build or refresh a summary for the page range containing
    /// `block_number`.
    ///
    /// Callers invoke this after inserting a batch of tuples into a heap
    /// page range. A real implementation reads every tuple in the range
    /// from the heap and recomputes min/max; this stub accepts the
    /// caller-supplied `min_key` / `max_key` directly.
    pub fn summarize_range(
        &self,
        first_block: u32,
        last_block: u32,
        min_key: Vec<u8>,
        max_key: Vec<u8>,
    ) {
        let mut summaries = self.summaries.lock();
        // Remove any existing summary for this range.
        summaries.retain(|s| s.first_block != first_block);
        summaries.push(BrinSummary {
            first_block,
            last_block,
            min_key,
            max_key,
        });
        summaries.sort_by_key(|s| s.first_block);
    }

    /// Encode a signed integer key so lexicographic byte order matches
    /// normal signed integer order.
    #[must_use]
    pub fn encode_i64_key(key: i64) -> [u8; 8] {
        (u64::from_ne_bytes(key.to_ne_bytes()) ^ (1_u64 << 63)).to_be_bytes()
    }

    /// Number of summary ranges currently stored.
    #[must_use]
    pub fn summary_count(&self) -> usize {
        self.summaries.lock().len()
    }

    /// Drop all current summaries before a full VACUUM re-summarize pass.
    pub fn clear_summaries(&self) {
        self.summaries.lock().clear();
    }

    /// Candidate page ranges for a point probe.
    ///
    /// Returned ranges are inclusive `(first_block, last_block)` pairs.
    /// The executor must still recheck the SQL predicate against every
    /// visible tuple in those ranges because BRIN summaries can include
    /// false positives by design.
    #[must_use]
    pub fn candidate_ranges_for_key(&self, key: &[u8]) -> Vec<(u32, u32)> {
        self.candidate_ranges_for_bounds(Some(key), Some(key))
    }

    /// Candidate page ranges for an inclusive key interval.
    ///
    /// `None` on either side means unbounded. A summary overlaps the
    /// query interval when `summary.max >= low && summary.min <= high`.
    #[must_use]
    pub fn candidate_ranges_for_bounds(
        &self,
        low: Option<&[u8]>,
        high: Option<&[u8]>,
    ) -> Vec<(u32, u32)> {
        let summaries = self.summaries.lock();
        summaries
            .iter()
            .filter(|s| {
                let above_low = low.is_none_or(|lo| s.max_key.as_slice() >= lo);
                let below_high = high.is_none_or(|hi| s.min_key.as_slice() <= hi);
                above_low && below_high
            })
            .map(|s| (s.first_block, s.last_block))
            .collect()
    }
}

impl AccessMethod for BrinIndex {
    fn name(&self) -> &'static str {
        "brin"
    }

    fn insert(&self, key: &[u8], tid: TupleId) -> Result<(), AccessMethodError> {
        let block = tid.page.block.raw();
        let range_start = (block / self.pages_per_range) * self.pages_per_range;
        let range_end = range_start + self.pages_per_range - 1;
        let mut summaries = self.summaries.lock();
        if let Some(s) = summaries.iter_mut().find(|s| s.first_block == range_start) {
            if key < s.min_key.as_slice() {
                s.min_key = key.to_vec();
            }
            if key > s.max_key.as_slice() {
                s.max_key = key.to_vec();
            }
        } else {
            summaries.push(BrinSummary {
                first_block: range_start,
                last_block: range_end,
                min_key: key.to_vec(),
                max_key: key.to_vec(),
            });
            summaries.sort_by_key(|s| s.first_block);
        }
        Ok(())
    }

    fn lookup(&self, key: &[u8]) -> Result<Vec<TupleId>, AccessMethodError> {
        let _ = self.candidate_ranges_for_key(key);
        // BRIN lookup yields candidate page ranges, not exact TupleIds;
        // SQL execution calls `candidate_ranges_*` directly and scans
        // those heap ranges with predicate recheck.
        Ok(Vec::new())
    }

    fn delete(&self, _key: &[u8], _tid: TupleId) -> Result<(), AccessMethodError> {
        // BRIN does not track individual TupleIds. Stale min/max
        // summaries over-include after deletes or shrinking updates,
        // which is correct because heap predicate recheck filters
        // false positives. Future page-backed summaries can recompute
        // exact ranges during VACUUM.
        Ok(())
    }
}
