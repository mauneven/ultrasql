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

pub mod applier;
pub mod buffer;
pub mod payload;
pub mod record;
pub mod recovery;
pub(crate) mod segment;
pub mod writer;

pub use applier::{ApplyError, HeapTarget, dispatch_record, replay_into};
pub use buffer::{WalBuffer, WalBufferError};
pub use payload::{
    AbortPayload, BTreeOpKind, BTreeOpPayload, CheckpointPayload, CommitPayload,
    FullPageWritePayload, HashOpKind, HashOpPayload, HeapDeleteInPlacePayload, HeapDeletePayload,
    HeapInsertPayload, HeapUpdateInPlacePayload, HeapUpdatePayload, HnswOpKind, HnswOpPayload,
    IvfFlatOpKind, IvfFlatOpPayload, PayloadError, SequenceOpKind, SequenceOpPayload,
};
pub use record::{RECORD_HEADER_SIZE, RecordType, WalRecord, WalRecordError, WalRecordHeader};
pub use recovery::{RecoveryError, recover};
pub use writer::{WalWriter, WalWriterConfig, WalWriterError, WalWriterStats};
