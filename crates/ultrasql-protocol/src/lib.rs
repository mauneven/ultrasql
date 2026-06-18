//! UltraSQL PostgreSQL wire protocol v3.
//!
//! This crate is a *pure codec*: it knows how to translate between
//! [`bytes::BytesMut`] buffers and typed Rust enums that represent
//! PostgreSQL v3 wire-protocol messages. It does not own a socket, run
//! a session loop, or implement any SQL semantics. That separation
//! keeps the protocol layer testable in isolation and lets callers
//! plug it into any transport — Tokio TCP today, in-process channels
//! for fuzzing, recorded files for replay.
//!
//! ## Surface
//!
//! Two enums describe the message catalog:
//!
//! - [`FrontendMessage`] — messages a client sends to the server.
//! - [`BackendMessage`] — messages a server sends to a client.
//!
//! Two function pairs perform the framing work:
//!
//! - [`encode_frontend`] / [`decode_frontend`]
//! - [`encode_backend`] / [`decode_backend`]
//!
//! Decoders return `Ok(None)` when the buffer does not yet contain a
//! full message, `Ok(Some(msg))` once a message has been parsed and
//! consumed, and `Err(ProtocolError)` for definitive protocol
//! violations. Callers driving a streaming connection use the
//! `Ok(None)` path as the signal to read more bytes.
//!
//! ## Wire-format references
//!
//! The canonical specification is the PostgreSQL documentation,
//! "Message Formats" section:
//! <https://www.postgresql.org/docs/current/protocol-message-formats.html>.
//! Every variant in this crate cites the v3 message tag it implements.
//! Multi-byte integers are big-endian on the wire; that convention is
//! confined to this crate.

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap
)]
// Panic hardening: production (non-test) protocol code must not `.unwrap()`,
// `.expect()`, or `panic!`. Fallible sites propagate errors; proven invariants
// carry a per-site `#[allow]` with an `// INVARIANT:` justification.
// `#[cfg(test)]` modules are exempt.
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod codec;
pub mod error;
pub mod messages;

pub use codec::{decode_backend, decode_frontend, encode_backend, encode_frontend};
pub use error::ProtocolError;
pub use messages::{BackendMessage, DescribeKind, FieldDescription, FrontendMessage};
