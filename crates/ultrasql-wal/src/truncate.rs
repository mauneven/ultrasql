//! WAL segment recycling (truncation).
//!
//! Once a checkpoint has made every heap mutation up to some LSN durable — and
//! every persistent secondary structure can reconstruct itself without the WAL
//! records below that LSN — those low segments are dead weight. This module
//! removes them and advances the recovery-floor manifest so recovery starts from
//! the first surviving segment instead of replaying (and demanding) a stream that
//! no longer begins at LSN 0.
//!
//! Segment ↔ LSN mapping
//! ---------------------
//!
//! The WAL LSN is an absolute byte offset into the logical stream formed by
//! concatenating the segment files in index order. The writer never pads a
//! segment and only rotates on a record boundary, so **a closed segment's file
//! length equals the byte-span of LSNs it covers**. Therefore, seeding from the
//! current floor, each segment's start LSN is the floor LSN plus the cumulative
//! lengths of the segments before it — the exact accounting [`crate::recovery`]
//! performs when it advances `stream_pos`. We reuse that invariant to decide
//! which whole segments lie entirely below a target floor LSN.
//!
//! Crash safety
//! ------------
//!
//! [`truncate_below`] writes the new floor manifest (atomic + fsync) **before**
//! unlinking any segment. A crash at any point leaves a consistent state:
//!
//! * Crash before the manifest write → old floor, all segments present.
//! * Crash after the manifest write, mid-unlink → new floor; some below-floor
//!   segments may linger, but recovery filters them out (`index >= floor`) and
//!   the next truncation sweeps them. No surviving segment is ever below the
//!   durable floor, so no needed record is lost.
//!
//! The active (highest-index) segment — the one the writer is still appending —
//! is never a removal candidate: the target floor is always at or below the
//! checkpoint position, which lives in that segment, so it is always kept.

use std::path::Path;

use tracing::warn;
use ultrasql_core::Lsn;

use crate::manifest::{WalFloor, read_floor, write_floor};
use crate::recovery::RecoveryError;
use crate::segment::list_segments;

/// What a truncation attempt did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncationOutcome {
    /// The floor after the attempt — unchanged from the prior floor on a no-op.
    pub floor: WalFloor,
    /// Indices of segment files removed, ascending.
    pub removed_segments: Vec<u32>,
    /// Bytes reclaimed by the removals.
    pub reclaimed_bytes: u64,
}

impl TruncationOutcome {
    fn noop(floor: WalFloor) -> Self {
        Self {
            floor,
            removed_segments: Vec::new(),
            reclaimed_bytes: 0,
        }
    }

    /// True when nothing was removed (the floor did not move).
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.removed_segments.is_empty()
    }
}

/// Pure planner: pick the new floor given the current floor, a target floor LSN,
/// and the byte lengths of the segments at or above the current floor.
///
/// `segments` must be `(index, byte_len)` pairs sorted by index ascending. It is
/// expected to be contiguous starting at `current.segment_index` (the only layout
/// the byte accounting is valid for). Any anomaly — empty, a gap, or a first
/// index that does not match the floor — yields the current floor unchanged
/// (refuse to truncate rather than mis-seed).
///
/// The returned floor satisfies two invariants:
/// * `floor_lsn <= target_floor_lsn` — never advances past the target, so no
///   record at or above the target is ever discarded.
/// * the highest (active) segment is never selected for removal.
fn plan_floor(current: WalFloor, target_floor_lsn: Lsn, segments: &[(u32, u64)]) -> WalFloor {
    let Some(&(first_index, _)) = segments.first() else {
        return current;
    };
    // The stream below `current.segment_index` is already gone; the planning list
    // must begin exactly at the floor or the cumulative byte math is wrong.
    if first_index != current.segment_index {
        return current;
    }
    // Require contiguity: a gap would mean the absolute-LSN accounting no longer
    // matches the on-disk page headers, which is already-broken territory.
    for (offset, (index, _)) in segments.iter().enumerate() {
        let Ok(offset) = u32::try_from(offset) else {
            return current;
        };
        let Some(expected) = current.segment_index.checked_add(offset) else {
            return current;
        };
        if *index != expected {
            return current;
        }
    }

    let target = target_floor_lsn.raw();
    let last = segments.len() - 1; // safe: non-empty checked above
    let mut start = current.floor_lsn.raw();
    let mut chosen = current;
    for (position, (index, len)) in segments.iter().enumerate() {
        let end = start.saturating_add(*len);
        // The first segment whose byte range reaches past the target is the
        // lowest one we must keep; everything before it is entirely below the
        // target and removable. Never select the active (last) segment.
        if end > target || position == last {
            chosen = WalFloor {
                segment_index: *index,
                floor_lsn: Lsn::new(start),
            };
            break;
        }
        start = end;
    }
    chosen
}

/// Remove every WAL segment that lies entirely below `target_floor_lsn`, and
/// advance the recovery-floor manifest to the first surviving segment.
///
/// Reads the current floor itself, so it is safe to call repeatedly; a target at
/// or below the current floor (or that only the active segment can satisfy) is a
/// no-op. See the module docs for the crash-safety ordering.
///
/// The caller is responsible for ensuring `target_floor_lsn` is a genuinely safe
/// recycling point (heap durable up to it, every secondary structure able to
/// rebuild without the records below it). This function only enforces the
/// segment-granularity and crash-safety mechanics.
pub fn truncate_below(
    wal_dir: &Path,
    target_floor_lsn: Lsn,
) -> Result<TruncationOutcome, RecoveryError> {
    let current = read_floor(wal_dir)?;
    let all = list_segments(wal_dir).map_err(RecoveryError::Io)?;

    // Byte lengths of segments at or above the floor, for the cumulative map.
    let mut planning: Vec<(u32, u64)> = Vec::new();
    for (index, path) in &all {
        if *index >= current.segment_index {
            let len = std::fs::metadata(path).map_err(RecoveryError::Io)?.len();
            planning.push((*index, len));
        }
    }

    let new_floor = plan_floor(current, target_floor_lsn, &planning);
    if new_floor.segment_index == current.segment_index {
        return Ok(TruncationOutcome::noop(current));
    }
    // Defensive: the planner must never advance the floor past the target. If a
    // bug ever produced such a floor, recycling would drop still-needed records,
    // so refuse rather than corrupt the stream.
    if new_floor.floor_lsn.raw() > target_floor_lsn.raw() {
        warn!(
            new_floor = new_floor.floor_lsn.raw(),
            target = target_floor_lsn.raw(),
            "wal truncation: planner produced a floor above the target; refusing"
        );
        return Ok(TruncationOutcome::noop(current));
    }

    // Durable floor first: after this returns, recovery will ignore everything
    // below `new_floor.segment_index` even if the unlinks below are interrupted.
    write_floor(wal_dir, new_floor)?;

    // Unlink every segment below the new floor. This sweeps the recycled range
    // plus any below-floor stragglers left by a prior interrupted truncation.
    let mut removed = Vec::new();
    let mut reclaimed: u64 = 0;
    for (index, path) in &all {
        if *index < new_floor.segment_index {
            let len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            match std::fs::remove_file(path) {
                Ok(()) => {
                    removed.push(*index);
                    reclaimed = reclaimed.saturating_add(len);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => warn!(
                    error = %e,
                    segment = *index,
                    "wal truncation: failed to remove recycled segment (harmless: below durable floor)"
                ),
            }
        }
    }
    sync_dir(wal_dir);

    Ok(TruncationOutcome {
        floor: new_floor,
        removed_segments: removed,
        reclaimed_bytes: reclaimed,
    })
}

#[cfg(unix)]
fn sync_dir(path: &Path) {
    if let Ok(dir) = std::fs::File::open(path) {
        let _ = dir.sync_all();
    }
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::segment_path;
    use tempfile::TempDir;

    fn floor(index: u32, lsn: u64) -> WalFloor {
        WalFloor {
            segment_index: index,
            floor_lsn: Lsn::new(lsn),
        }
    }

    #[test]
    fn plan_keeps_segment_containing_the_target() {
        // Three 100-byte segments from origin: [0,100) [100,200) [200,300).
        let segs = [(0, 100), (1, 100), (2, 100)];
        // Target 150 lands inside segment 1 → keep from segment 1.
        let got = plan_floor(WalFloor::ORIGIN, Lsn::new(150), &segs);
        assert_eq!(got, floor(1, 100));
        // Safety invariant: floor never advances past the target.
        assert!(got.floor_lsn.raw() <= 150);
    }

    #[test]
    fn plan_on_a_boundary_keeps_the_following_segment() {
        let segs = [(0, 100), (1, 100), (2, 100)];
        // Target exactly 100 == end of segment 0 → segment 0 fully below, keep 1.
        let got = plan_floor(WalFloor::ORIGIN, Lsn::new(100), &segs);
        assert_eq!(got, floor(1, 100));
    }

    #[test]
    fn plan_never_removes_the_active_segment() {
        let segs = [(0, 100), (1, 100), (2, 100)];
        // Target beyond the whole stream → still keep the last (active) segment.
        let got = plan_floor(WalFloor::ORIGIN, Lsn::new(10_000), &segs);
        assert_eq!(got, floor(2, 200));
        assert!(got.floor_lsn.raw() <= 10_000);
    }

    #[test]
    fn plan_target_below_floor_is_a_noop() {
        let segs = [(0, 100), (1, 100)];
        // Target 0 → nothing is below it, keep everything from segment 0.
        assert_eq!(
            plan_floor(WalFloor::ORIGIN, Lsn::new(0), &segs),
            WalFloor::ORIGIN
        );
    }

    #[test]
    fn plan_respects_a_nonzero_starting_floor() {
        // Already truncated: floor at segment 5 / LSN 500, two 100-byte segments
        // covering [500,600) [600,700).
        let current = floor(5, 500);
        let segs = [(5, 100), (6, 100)];
        // Target 650 lands in segment 6 → keep from segment 6.
        let got = plan_floor(current, Lsn::new(650), &segs);
        assert_eq!(got, floor(6, 600));
    }

    #[test]
    fn plan_refuses_a_noncontiguous_or_misaligned_list() {
        let current = floor(5, 500);
        // First index does not match the floor.
        assert_eq!(plan_floor(current, Lsn::new(9_999), &[(6, 100)]), current);
        // Gap between 5 and 7.
        assert_eq!(
            plan_floor(current, Lsn::new(9_999), &[(5, 100), (7, 100)]),
            current
        );
        // Empty list.
        assert_eq!(plan_floor(current, Lsn::new(9_999), &[]), current);
    }

    #[test]
    fn truncate_below_removes_low_segments_and_advances_the_floor() {
        let dir = TempDir::new().unwrap();
        // Four 100-byte segments from origin.
        for index in 0..4u32 {
            std::fs::write(segment_path(dir.path(), index), vec![b'x'; 100]).unwrap();
        }
        // Target 250 → segments 0 and 1 fully below (end 100, 200), keep from 2.
        let outcome = truncate_below(dir.path(), Lsn::new(250)).unwrap();
        assert_eq!(outcome.floor, floor(2, 200));
        assert_eq!(outcome.removed_segments, vec![0, 1]);
        assert_eq!(outcome.reclaimed_bytes, 200);
        assert!(!segment_path(dir.path(), 0).exists());
        assert!(!segment_path(dir.path(), 1).exists());
        assert!(segment_path(dir.path(), 2).exists());
        assert!(segment_path(dir.path(), 3).exists());
        // The durable manifest matches the returned floor.
        assert_eq!(read_floor(dir.path()).unwrap(), floor(2, 200));
    }

    #[test]
    fn truncate_below_is_a_noop_when_only_the_active_segment_qualifies() {
        let dir = TempDir::new().unwrap();
        std::fs::write(segment_path(dir.path(), 0), vec![b'x'; 100]).unwrap();
        // Target beyond the stream cannot remove the sole active segment.
        let outcome = truncate_below(dir.path(), Lsn::new(10_000)).unwrap();
        assert!(outcome.is_noop());
        assert_eq!(outcome.floor, WalFloor::ORIGIN);
        assert!(segment_path(dir.path(), 0).exists());
        // No manifest is written for a no-op.
        assert_eq!(read_floor(dir.path()).unwrap(), WalFloor::ORIGIN);
    }

    #[test]
    fn truncate_then_recover_is_byte_exact_with_real_records() {
        use crate::record::{RecordType, WalRecord};
        use crate::recovery::recover;
        use ultrasql_core::Xid;

        let dir = TempDir::new().unwrap();
        let nop = |xid: u64| {
            WalRecord::new(RecordType::Nop, Xid::new(xid), Lsn::ZERO, 0, Vec::new())
                .expect("nop record")
                .encode()
        };
        // Three segments of real records: [10,11] [20,21] [30,31].
        let mut seg0 = nop(10);
        seg0.extend_from_slice(&nop(11));
        let mut seg1 = nop(20);
        seg1.extend_from_slice(&nop(21));
        let mut seg2 = nop(30);
        seg2.extend_from_slice(&nop(31));
        let s0 = u64::try_from(seg0.len()).unwrap();
        let s1 = u64::try_from(seg1.len()).unwrap();
        std::fs::write(segment_path(dir.path(), 0), &seg0).unwrap();
        std::fs::write(segment_path(dir.path(), 1), &seg1).unwrap();
        std::fs::write(segment_path(dir.path(), 2), &seg2).unwrap();

        // Full recovery: every record, absolute end LSN at the stream end.
        let mut full = Vec::new();
        let full_end = recover(dir.path(), |r| {
            full.push(r.header.xid.raw());
            Ok(())
        })
        .unwrap();
        assert_eq!(full, vec![10, 11, 20, 21, 30, 31]);

        // Recycle everything below the seg1/seg2 boundary (LSN s0 + s1): removes
        // segments 0 and 1, floor lands on segment 2 at exactly that LSN.
        let outcome = truncate_below(dir.path(), Lsn::new(s0 + s1)).unwrap();
        assert_eq!(outcome.floor, floor(2, s0 + s1));
        assert_eq!(outcome.removed_segments, vec![0, 1]);
        assert!(!segment_path(dir.path(), 0).exists());
        assert!(!segment_path(dir.path(), 1).exists());

        // Recovery over the truncated stream sees only the surviving tail and,
        // crucially, reconstructs the SAME absolute end LSN as the full run —
        // proving the recycled floor LSN is byte-exact against the page headers.
        let mut tail = Vec::new();
        let tail_end = recover(dir.path(), |r| {
            tail.push(r.header.xid.raw());
            Ok(())
        })
        .unwrap();
        assert_eq!(tail, vec![30, 31], "only the above-floor records survive");
        assert_eq!(
            tail_end, full_end,
            "truncated recovery must reconstruct the same absolute end LSN"
        );
    }

    #[test]
    fn truncate_below_sweeps_stragglers_below_a_prior_floor() {
        let dir = TempDir::new().unwrap();
        // Simulate a prior crashed truncation: floor already at segment 2, but
        // below-floor segments 0 and 1 still linger on disk.
        for index in 0..5u32 {
            std::fs::write(segment_path(dir.path(), index), vec![b'x'; 100]).unwrap();
        }
        write_floor(dir.path(), floor(2, 200)).unwrap();
        // Advance the floor again into segment 4; segments 0,1 (stragglers) and
        // 2,3 (newly recycled) must all be removed.
        let outcome = truncate_below(dir.path(), Lsn::new(450)).unwrap();
        assert_eq!(outcome.floor, floor(4, 400));
        assert_eq!(outcome.removed_segments, vec![0, 1, 2, 3]);
        for index in 0..4u32 {
            assert!(!segment_path(dir.path(), index).exists());
        }
        assert!(segment_path(dir.path(), 4).exists());
    }
}
