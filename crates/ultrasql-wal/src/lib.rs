//! UltraSQL write-ahead log.
//!
//! Append-only segmented log with group-commit fsync batching. Records carry
//! a 64-bit LSN, a CRC32C checksum, and a typed payload. Recovery replays
//! committed records into the buffer pool; uncommitted records are discarded.

#![forbid(unsafe_op_in_unsafe_fn)]
