//! UltraSQL write-ahead log.
//!
//! Append-only segmented log with group-commit fsync batching. Records
//! carry a 64-bit LSN, a CRC32C checksum, and a typed payload. Recovery
//! replays committed records into the buffer pool; uncommitted records
//! are discarded.

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]
// Panic hardening: production (non-test) WAL code must not `.unwrap()`,
// `.expect()`, or `panic!`. Fallible sites propagate errors; proven invariants
// carry a per-site `#[allow]` with an `// INVARIANT:` justification.
// `#[cfg(test)]` modules are exempt.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod applier;
pub mod buffer;
pub mod manifest;
pub mod payload;
pub mod reader;
pub mod record;
pub mod recovery;
pub(crate) mod segment;
pub mod truncate;
pub mod writer;

pub use applier::{ApplyError, HeapTarget, dispatch_record, dispatch_record_at_lsn, replay_into};
pub use buffer::{WalBuffer, WalBufferError};
pub use manifest::{WalFloor, read_floor, write_floor};
pub use payload::{
    AbortPayload, BTreeOpKind, BTreeOpPayload, CheckpointPayload, CommitPayload,
    FullPageWritePayload, HashOpKind, HashOpPayload, HeapDeleteInPlaceBatchEntry,
    HeapDeleteInPlaceBatchPayload, HeapDeleteInPlacePayload, HeapDeleteInPlaceRangeBatchPayload,
    HeapDeletePayload, HeapInsertBatchEntry, HeapInsertBatchPayload, HeapInsertPayload,
    HeapUpdateInPlaceBatchEntry, HeapUpdateInPlaceBatchPayload, HeapUpdateInPlacePayload,
    HeapUpdateInt32PairDeltaBatchPayload, HeapUpdateInt32PairDeltaRangeBatchPayload,
    HeapUpdatePayload, HnswOpKind, HnswOpPayload, IvfFlatOpKind, IvfFlatOpPayload, PayloadError,
    SequenceOpKind, SequenceOpPayload,
};
pub use record::{RECORD_HEADER_SIZE, RecordType, WalRecord, WalRecordError, WalRecordHeader};
pub use recovery::{
    RecoveryError, RecoveryTarget, recover, recover_with_target, repair_final_segment_tail,
};
pub use truncate::{TruncationOutcome, truncate_below};
pub use writer::{WalDurabilityHandle, WalWriter, WalWriterConfig, WalWriterError, WalWriterStats};
