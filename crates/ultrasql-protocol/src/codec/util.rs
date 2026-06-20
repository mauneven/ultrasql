//! Codec utility primitives:
//! - [`PayloadReader`] — slice-based reader with truncation tracking
//! - [`decode_with`] — entry-point glue that turns truncation into `None`
//! - [`payload_truncated_is_malformed`] — boundary between framing and payload
//! - [`take_framed_message`] — validate header + length, split payload
//! - [`read_row_description`], [`read_error_fields`] — shared field readers
//! - small integer-conversion saturating casts

use bytes::{Buf, BytesMut};

use crate::error::ProtocolError;
use crate::messages::FieldDescription;

use super::{HEADER_LEN, MAX_PAYLOAD};

/// Run `inner` against the bytes in `buf`. On `Ok((msg, consumed))`,
/// advance `buf` by `consumed`. On `Err(Truncated)`, leave `buf`
/// untouched and translate the result to `Ok(None)`. On any other
/// error, leave `buf` untouched and propagate.
///
/// "Truncated" here is unambiguous because the framing layer is the
/// only producer of [`ProtocolError::Truncated`]: once the framed
/// slice has been delimited, every payload-internal truncation is
/// remapped to [`ProtocolError::Malformed`] before it bubbles back
/// here. See [`payload_truncated_is_malformed`].
pub(super) fn decode_with<T, F>(buf: &mut BytesMut, inner: F) -> Result<Option<T>, ProtocolError>
where
    F: FnOnce(&[u8]) -> Result<(T, usize), ProtocolError>,
{
    match inner(buf.as_ref()) {
        Ok((msg, consumed)) => {
            buf.advance(consumed);
            Ok(Some(msg))
        }
        Err(ProtocolError::Truncated) => Ok(None),
        Err(other) => Err(other),
    }
}

/// Remap an inner parser's [`ProtocolError::Truncated`] to a
/// [`ProtocolError::Malformed`]. Used after the framing layer has
/// already confirmed that the entire framed slice is available: a
/// payload that runs out of bytes from that point is a protocol
/// violation, not a "read more bytes and retry" condition.
pub(super) fn payload_truncated_is_malformed<T>(
    result: Result<T, ProtocolError>,
) -> Result<T, ProtocolError> {
    match result {
        Err(ProtocolError::Truncated) => Err(ProtocolError::Malformed("payload truncated")),
        other => other,
    }
}

/// Validate the framing of a tagged message and return its payload
/// slice (no header) together with the total bytes consumed
/// (header + payload).
pub(super) fn take_framed_message(bytes: &[u8]) -> Result<(&[u8], usize), ProtocolError> {
    if bytes.len() < HEADER_LEN {
        return Err(ProtocolError::Truncated);
    }
    let mut len_buf = [0_u8; 4];
    len_buf.copy_from_slice(&bytes[1..5]);
    let length = i32::from_be_bytes(len_buf);
    if length < 4 {
        return Err(ProtocolError::Malformed("message length too small"));
    }
    let length = usize_from_i32(length, "message length")?;
    if length > MAX_PAYLOAD {
        return Err(ProtocolError::Malformed("message length too large"));
    }
    let total = length
        .checked_add(1)
        .ok_or(ProtocolError::Malformed("length overflow"))?;
    if bytes.len() < total {
        return Err(ProtocolError::Truncated);
    }
    let payload_end = total;
    let payload_start = HEADER_LEN;
    Ok((&bytes[payload_start..payload_end], total))
}

pub(super) fn read_row_description(
    p: &mut PayloadReader<'_>,
) -> Result<Vec<FieldDescription>, ProtocolError> {
    let count = p.read_i16()?;
    let count = nonneg_usize(count, "row description field count")?;
    let mut fields = Vec::with_capacity(count.min(64));
    for _ in 0..count {
        let name = p.read_cstring()?;
        let table_oid = p.read_u32()?;
        let col_attnum = p.read_i16()?;
        let type_oid = p.read_u32()?;
        let type_size = p.read_i16()?;
        let type_modifier = p.read_i32()?;
        let format_code = p.read_i16()?;
        fields.push(FieldDescription {
            name,
            table_oid,
            col_attnum,
            type_oid,
            type_size,
            type_modifier,
            format_code,
        });
    }
    Ok(fields)
}

pub(super) fn read_error_fields(
    p: &mut PayloadReader<'_>,
) -> Result<Vec<(u8, String)>, ProtocolError> {
    let mut fields = Vec::new();
    loop {
        if p.is_empty() {
            return Err(ProtocolError::Malformed("error fields missing terminator"));
        }
        let code = p.read_u8()?;
        if code == 0 {
            return Ok(fields);
        }
        let value = p.read_cstring()?;
        fields.push((code, value));
    }
}

// ---------------------------------------------------------------------------
// Integer-conversion helpers that surface protocol errors rather than
// panic on bad input.
// ---------------------------------------------------------------------------

pub(super) fn usize_from_i32(value: i32, what: &'static str) -> Result<usize, ProtocolError> {
    // `what` names the specific field (e.g. "startup length") so a malformed
    // client surfaces *which* count/length was negative, not a generic message.
    usize::try_from(value).map_err(|_| ProtocolError::Malformed(what))
}

pub(super) fn nonneg_usize(value: i16, what: &'static str) -> Result<usize, ProtocolError> {
    usize::try_from(value).map_err(|_| ProtocolError::Malformed(what))
}

/// Encoder helper. The wire length is encoded as a signed 32-bit
/// integer; messages larger than `i32::MAX` cannot be expressed by the
/// protocol and are clamped to the upper bound. This crate never
/// constructs such messages in practice — callers above the protocol
/// layer chunk large payloads — but we still avoid a wrapping cast.
pub(super) fn i32_from_usize(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

/// Same logic for `i16`-sized counts (parameter counts, column
/// counts). Saturating cast is the safe choice; a real overflow here
/// indicates a buggy caller.
pub(super) fn i16_from_usize(value: usize) -> i16 {
    i16::try_from(value).unwrap_or(i16::MAX)
}

// ---------------------------------------------------------------------------
// Payload reader. Tracks a slice and an offset, exposing
// fixed-width reads, NUL-terminated strings, and a length-prefixed
// value reader. Every method returns [`ProtocolError::Truncated`] when
// the request exceeds the remaining bytes.
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(super) struct PayloadReader<'a> {
    bytes: &'a [u8],
}

impl<'a> PayloadReader<'a> {
    pub(super) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub(super) const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub(super) const fn remaining(&self) -> usize {
        self.bytes.len()
    }

    pub(super) fn advance(&mut self, n: usize) {
        if let Some(rest) = self.bytes.get(n..) {
            self.bytes = rest;
        } else {
            self.bytes = &[];
        }
    }

    pub(super) fn peek_u8(&self) -> Result<u8, ProtocolError> {
        self.bytes.first().copied().ok_or(ProtocolError::Truncated)
    }

    pub(super) fn read_u8(&mut self) -> Result<u8, ProtocolError> {
        let v = self.peek_u8()?;
        self.advance(1);
        Ok(v)
    }

    pub(super) fn read_u16(&mut self) -> Result<u16, ProtocolError> {
        let mut out = [0_u8; 2];
        self.read_exact(&mut out)?;
        Ok(u16::from_be_bytes(out))
    }

    /// Consume and return all remaining bytes in the payload.
    ///
    /// Used for variable-length opaque payloads such as `CopyData`
    /// where the entire framed body is the data.
    pub(super) fn read_remaining(&mut self) -> Vec<u8> {
        let v = self.bytes.to_vec();
        self.bytes = &[];
        v
    }

    pub(super) fn read_u32(&mut self) -> Result<u32, ProtocolError> {
        let mut out = [0_u8; 4];
        self.read_exact(&mut out)?;
        Ok(u32::from_be_bytes(out))
    }

    pub(super) fn read_i16(&mut self) -> Result<i16, ProtocolError> {
        let mut out = [0_u8; 2];
        self.read_exact(&mut out)?;
        Ok(i16::from_be_bytes(out))
    }

    pub(super) fn read_i32(&mut self) -> Result<i32, ProtocolError> {
        let mut out = [0_u8; 4];
        self.read_exact(&mut out)?;
        Ok(i32::from_be_bytes(out))
    }

    pub(super) fn read_exact(&mut self, out: &mut [u8]) -> Result<(), ProtocolError> {
        if self.remaining() < out.len() {
            return Err(ProtocolError::Truncated);
        }
        let (head, rest) = self.bytes.split_at(out.len());
        out.copy_from_slice(head);
        self.bytes = rest;
        Ok(())
    }

    pub(super) fn read_cstring(&mut self) -> Result<String, ProtocolError> {
        let nul = self
            .bytes
            .iter()
            .position(|&b| b == 0)
            .ok_or(ProtocolError::Truncated)?;
        let s = std::str::from_utf8(&self.bytes[..nul])?.to_owned();
        let advance_by = nul
            .checked_add(1)
            .ok_or(ProtocolError::Malformed("cstring length overflow"))?;
        self.advance(advance_by);
        Ok(s)
    }

    pub(super) fn read_value(&mut self) -> Result<Option<Vec<u8>>, ProtocolError> {
        let len = self.read_i32()?;
        if len == -1 {
            return Ok(None);
        }
        if len < 0 {
            return Err(ProtocolError::Malformed("negative value length"));
        }
        // Guard above proves `len >= 0`; the `try_from` then can only
        // fail on a 16-bit `usize` target, which we do not support.
        let len = usize::try_from(len)
            .map_err(|_| ProtocolError::Malformed("value length exceeds usize"))?;
        if len > MAX_PAYLOAD {
            return Err(ProtocolError::Malformed("value length too large"));
        }
        if self.remaining() < len {
            return Err(ProtocolError::Truncated);
        }
        let (head, rest) = self.bytes.split_at(len);
        let out = head.to_vec();
        self.bytes = rest;
        Ok(Some(out))
    }

    pub(super) const fn ensure_drained(&self) -> Result<(), ProtocolError> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(ProtocolError::Malformed("trailing bytes in payload"))
        }
    }
}
