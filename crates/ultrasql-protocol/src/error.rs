//! Error type for the PostgreSQL wire-protocol codec.
//!
//! [`ProtocolError`] is the single error returned by every public codec
//! entry point in this crate. The variants partition recoverable parsing
//! failures (`Truncated`, which only means "read more bytes and try
//! again") from definitive protocol violations (`Malformed`,
//! `UnknownMessageType`, `InvalidUtf8`).
//!
//! `Truncated` is never returned by the public [`decode_frontend`] or
//! [`decode_backend`] entry points: short input is represented as
//! `Ok(None)` so the caller can simply read more bytes. The variant
//! still exists for internal control flow and for callers that drive
//! the parser slice-by-slice.
//!
//! [`decode_frontend`]: crate::decode_frontend
//! [`decode_backend`]: crate::decode_backend

use std::str::Utf8Error;

/// Errors produced while encoding or decoding wire-protocol messages.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// The buffer ended before the message was complete. Callers driving
    /// a streaming connection should treat this as "read more bytes and
    /// retry"; the public `decode_*` functions translate this case into
    /// `Ok(None)` so callers do not need to match on it.
    #[error("protocol message truncated")]
    Truncated,

    /// The message bytes were well-framed but violated the protocol's
    /// structural rules — a negative length, an out-of-range parameter
    /// count, a missing NUL terminator, etc. The static string names
    /// the specific invariant that was violated.
    #[error("malformed protocol message: {0}")]
    Malformed(&'static str),

    /// The first byte of a message was not a recognized type tag.
    /// Carries the offending byte so observability tools can log it.
    #[error("unknown protocol message type: {0:#04x}")]
    UnknownMessageType(u8),

    /// A field declared as a C string contained invalid UTF-8. The
    /// PostgreSQL wire protocol is encoding-agnostic at the framing
    /// level, but UltraSQL's typed API uses Rust `String`s; any byte
    /// sequence that is not valid UTF-8 is rejected here so higher
    /// layers never see invalid strings.
    #[error("invalid utf-8 in protocol string: {0}")]
    InvalidUtf8(#[from] Utf8Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_byte_value() {
        let err = ProtocolError::UnknownMessageType(0xAB);
        let s = err.to_string();
        assert!(s.contains("0xab") || s.contains("0xAB"), "got {s}");
    }

    #[test]
    fn malformed_carries_static_reason() {
        let err = ProtocolError::Malformed("bad length");
        assert!(err.to_string().contains("bad length"));
    }

    #[test]
    fn truncated_displays_a_human_message() {
        let err = ProtocolError::Truncated;
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn utf8_error_converts() {
        // 0xFF is never a valid UTF-8 start byte. Build the slice
        // through a Vec so the linter cannot evaluate
        // `std::str::from_utf8` at compile time and flag the literal.
        let bad: Vec<u8> = vec![0xFF];
        let utf8_err = std::str::from_utf8(&bad).expect_err("0xFF is not valid UTF-8");
        let err: ProtocolError = utf8_err.into();
        assert!(matches!(err, ProtocolError::InvalidUtf8(_)));
    }
}
