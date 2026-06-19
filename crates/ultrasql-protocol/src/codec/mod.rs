//! Encode and decode functions for PostgreSQL wire-protocol v3
//! messages.
//!
//! Each [`FrontendMessage`] and [`BackendMessage`] variant has a fixed
//! framing on the wire. After the initial
//! [`FrontendMessage::StartupMessage`], every message has the layout:
//!
//! ```text
//!   ┌───────┬────────────┬─────────┐
//!   │ tag   │ length     │ payload │
//!   │ (u8)  │ (i32 BE)   │  ...    │
//!   └───────┴────────────┴─────────┘
//! ```
//!
//! `length` is the byte count of the length field plus the payload —
//! it does **not** include the type tag. The encoders in this module
//! write the tag first, reserve four bytes for the length, write the
//! payload, then back-fill the length once the final payload size is
//! known. The decoders mirror that pattern: they peek the length,
//! confirm that enough bytes are available, then parse the payload
//! and consume the framed slice from the input buffer.
//!
//! ## Truncation semantics
//!
//! The public [`decode_frontend`] and [`decode_backend`] entry points
//! convert truncation into `Ok(None)` so callers driving a streaming
//! socket can react with "read more bytes and retry" without matching
//! on an error variant. Internal helpers still propagate
//! [`ProtocolError::Truncated`] so the entry points can decide.
//!
//! ## Endianness
//!
//! All multi-byte integers on the wire are big-endian. The helpers in
//! this module hide that detail; everything exposed in the typed API
//! is host-endian.
//!
//! ## Module layout
//!
//! - This file holds the public entry points, the wire-budget constants,
//!   the `decode_with` / `payload_truncated_is_malformed` dispatch
//!   helpers, and the `DescribeKind` byte-conversion helpers.
//! - [`decode_frontend`] holds the frontend payload decoders and the
//!   special startup-message path.
//! - [`decode_backend`] holds the backend payload decoder.
//! - `encode` holds the encoders for both directions.
//! - `util` holds the framing helpers, the `PayloadReader`, and the
//!   small integer-conversion utilities used across the codec.

use bytes::BytesMut;

use crate::error::ProtocolError;
use crate::messages::{BackendMessage, DescribeKind, FrontendMessage};

mod decode_backend;
mod decode_frontend;
mod encode;
mod util;

#[cfg(test)]
mod tests;

pub use encode::{encode_backend, encode_frontend};

use util::decode_with;

/// Length of the framing prefix on every non-startup message: the
/// 1-byte type tag and the 4-byte length field.
pub(super) const HEADER_LEN: usize = 5;

/// Maximum on-wire message length (in bytes) accepted by either decoder.
///
/// A hostile client can otherwise advertise `length = u32::MAX` and
/// force the server to either allocate a gigabyte-class buffer or
/// pretend it has done so while waiting for bytes that will never
/// arrive. Cap the value at 16 MiB so a single misbehaving client
/// cannot starve every other session for memory.
///
/// 16 MiB is comfortably larger than every legitimate Parse/Query/Bind
/// message in practice (PostgreSQL's `MaxAllocSize` is 1 GiB, but no
/// production traffic uses anywhere near that for a single message);
/// libraries that bulk-load very large rows do so via COPY, not a
/// single message. Tune via the constant if a workload demonstrably
/// requires more.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// Backward-compatibility alias retained for callers that referenced
/// the prior internal name. Renamed in the security audit; both
/// identifiers point at the same byte budget.
pub(super) const MAX_PAYLOAD: usize = MAX_MESSAGE_BYTES;

/// Decode a single [`FrontendMessage`] from `buf`.
///
/// Returns `Ok(Some(msg))` and consumes the message bytes when a full
/// message is present. Returns `Ok(None)` (without consuming) when the
/// buffer does not yet hold a complete message. Returns
/// `Err(ProtocolError::...)` for definitive protocol violations.
///
/// The first call on a fresh connection decodes a
/// [`FrontendMessage::StartupMessage`], which has the wire-format
/// quirk of starting with the length field instead of a type tag.
/// Subsequent calls decode tagged messages.
///
/// This function does not perform any I/O. It is the caller's
/// responsibility to feed bytes into `buf` as they arrive from the
/// network.
///
/// Allocation: a decoded message owns its data. Every string field is
/// copied into an owned `String` and every byte payload into an owned
/// `Vec<u8>`, so the returned message is independent of `buf` and the
/// caller may reuse or drop the buffer immediately.
pub fn decode_frontend(buf: &mut BytesMut) -> Result<Option<FrontendMessage>, ProtocolError> {
    decode_with(buf, decode_frontend::decode_frontend_inner)
}

/// Frame the next tagged frontend message without interpreting its payload,
/// returning `Ok(Some((tag, payload)))` once a full frame is available.
///
/// Used during SASL authentication: the `'p'` message tag is shared by
/// `PasswordMessage`, `SASLInitialResponse`, and `SASLResponse`, so only the
/// server's auth state can disambiguate them. See [`decode_frontend`] for the
/// short-input / error contract.
pub fn decode_frontend_raw(buf: &mut BytesMut) -> Result<Option<(u8, Vec<u8>)>, ProtocolError> {
    decode_with(buf, decode_frontend::decode_frontend_raw_inner)
}

/// Decode a single [`BackendMessage`] from `buf`.
///
/// Mirrors [`decode_frontend`]: returns `Ok(None)` on short input,
/// `Ok(Some(msg))` once a full message is parsed, or an error on a
/// definitive protocol violation. All backend messages carry a type
/// tag; there is no equivalent to the unframed startup message.
///
/// Allocation: like [`decode_frontend`], the returned message owns its
/// strings (`String`) and payloads (`Vec<u8>`) independently of `buf`.
pub fn decode_backend(buf: &mut BytesMut) -> Result<Option<BackendMessage>, ProtocolError> {
    decode_with(buf, decode_backend::decode_backend_inner)
}

// ---------------------------------------------------------------------------
// DescribeKind ↔ wire byte helpers
// ---------------------------------------------------------------------------

/// Encode a [`DescribeKind`] to its 1-byte wire representation.
///
/// `Statement` → `b'S'`, `Portal` → `b'P'` per the PostgreSQL spec.
pub(super) const fn describe_kind_byte(kind: DescribeKind) -> u8 {
    match kind {
        DescribeKind::Statement => b'S',
        DescribeKind::Portal => b'P',
    }
}

/// Decode a [`DescribeKind`] from its 1-byte wire representation.
///
/// Returns [`ProtocolError::Malformed`] for any byte other than `b'S'`
/// or `b'P'`.
pub(super) const fn describe_kind_from_byte(b: u8) -> Result<DescribeKind, ProtocolError> {
    match b {
        b'S' => Ok(DescribeKind::Statement),
        b'P' => Ok(DescribeKind::Portal),
        _ => Err(ProtocolError::Malformed("describe/close kind byte")),
    }
}
