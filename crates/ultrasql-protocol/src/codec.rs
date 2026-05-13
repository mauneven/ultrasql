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

use bytes::{Buf, BufMut, BytesMut};

use crate::error::ProtocolError;
use crate::messages::{BackendMessage, FieldDescription, FrontendMessage};

/// Length of the framing prefix on every non-startup message: the
/// 1-byte type tag and the 4-byte length field.
const HEADER_LEN: usize = 5;

/// Maximum payload size accepted by either decoder. Matches
/// PostgreSQL's `MaxAllocSize` of 1 GiB minus 1 byte, so any value
/// the upstream protocol can describe still fits comfortably in a
/// `usize` on every supported target.
const MAX_PAYLOAD: usize = (1 << 30) - 1;

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

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
pub fn decode_frontend(buf: &mut BytesMut) -> Result<Option<FrontendMessage>, ProtocolError> {
    decode_with(buf, decode_frontend_inner)
}

/// Encode a [`FrontendMessage`] into `buf`.
///
/// The bytes are appended to the existing contents of `buf`; callers
/// that need a fresh allocation should pass a freshly-constructed
/// [`BytesMut`].
pub fn encode_frontend(msg: &FrontendMessage, buf: &mut BytesMut) {
    match msg {
        FrontendMessage::StartupMessage {
            protocol_major,
            protocol_minor,
            params,
        } => encode_startup(*protocol_major, *protocol_minor, params, buf),
        FrontendMessage::Query { sql } => {
            write_tagged(buf, b'Q', |payload| {
                write_cstring(payload, sql);
            });
        }
        FrontendMessage::Parse {
            name,
            sql,
            param_types,
        } => {
            write_tagged(buf, b'P', |payload| {
                write_cstring(payload, name);
                write_cstring(payload, sql);
                payload.put_i16(i16_from_usize(param_types.len()));
                for oid in param_types {
                    payload.put_u32(*oid);
                }
            });
        }
        FrontendMessage::Bind {
            portal_name,
            statement_name,
            params,
            result_formats,
        } => {
            write_tagged(buf, b'B', |payload| {
                write_cstring(payload, portal_name);
                write_cstring(payload, statement_name);
                // Parameter format codes: zero indicates "default text
                // for every parameter", matching this crate's
                // simplified Bind shape.
                payload.put_i16(0);
                payload.put_i16(i16_from_usize(params.len()));
                for value in params {
                    write_value(payload, value.as_deref());
                }
                payload.put_i16(i16_from_usize(result_formats.len()));
                for code in result_formats {
                    payload.put_i16(*code);
                }
            });
        }
        FrontendMessage::Describe { kind, name } => {
            write_tagged(buf, b'D', |payload| {
                payload.put_u8(*kind);
                write_cstring(payload, name);
            });
        }
        FrontendMessage::Execute { portal, max_rows } => {
            write_tagged(buf, b'E', |payload| {
                write_cstring(payload, portal);
                payload.put_i32(*max_rows);
            });
        }
        FrontendMessage::Sync => write_tagged(buf, b'S', |_| {}),
        FrontendMessage::Terminate => write_tagged(buf, b'X', |_| {}),
        FrontendMessage::Password { password } => {
            write_tagged(buf, b'p', |payload| {
                write_cstring(payload, password);
            });
        }
    }
}

/// Decode a single [`BackendMessage`] from `buf`.
///
/// Mirrors [`decode_frontend`]: returns `Ok(None)` on short input,
/// `Ok(Some(msg))` once a full message is parsed, or an error on a
/// definitive protocol violation. All backend messages carry a type
/// tag; there is no equivalent to the unframed startup message.
pub fn decode_backend(buf: &mut BytesMut) -> Result<Option<BackendMessage>, ProtocolError> {
    decode_with(buf, decode_backend_inner)
}

/// Encode a [`BackendMessage`] into `buf`.
pub fn encode_backend(msg: &BackendMessage, buf: &mut BytesMut) {
    match msg {
        BackendMessage::AuthenticationOk => {
            write_tagged(buf, b'R', |payload| {
                payload.put_i32(0);
            });
        }
        BackendMessage::AuthenticationCleartextPassword => {
            write_tagged(buf, b'R', |payload| {
                payload.put_i32(3);
            });
        }
        BackendMessage::AuthenticationMD5Password { salt } => {
            write_tagged(buf, b'R', |payload| {
                payload.put_i32(5);
                payload.put_slice(salt);
            });
        }
        BackendMessage::ParameterStatus { name, value } => {
            write_tagged(buf, b'S', |payload| {
                write_cstring(payload, name);
                write_cstring(payload, value);
            });
        }
        BackendMessage::BackendKeyData {
            process_id,
            secret_key,
        } => {
            write_tagged(buf, b'K', |payload| {
                payload.put_i32(*process_id);
                payload.put_i32(*secret_key);
            });
        }
        BackendMessage::ReadyForQuery { status } => {
            write_tagged(buf, b'Z', |payload| {
                payload.put_u8(*status);
            });
        }
        BackendMessage::RowDescription { fields } => {
            write_tagged(buf, b'T', |payload| {
                payload.put_i16(i16_from_usize(fields.len()));
                for field in fields {
                    write_cstring(payload, &field.name);
                    payload.put_u32(field.table_oid);
                    payload.put_i16(field.col_attnum);
                    payload.put_u32(field.type_oid);
                    payload.put_i16(field.type_size);
                    payload.put_i32(field.type_modifier);
                    payload.put_i16(field.format_code);
                }
            });
        }
        BackendMessage::DataRow { columns } => {
            write_tagged(buf, b'D', |payload| {
                payload.put_i16(i16_from_usize(columns.len()));
                for value in columns {
                    write_value(payload, value.as_deref());
                }
            });
        }
        BackendMessage::CommandComplete { tag } => {
            write_tagged(buf, b'C', |payload| {
                write_cstring(payload, tag);
            });
        }
        BackendMessage::ErrorResponse { fields } => {
            write_tagged(buf, b'E', |payload| write_error_fields(payload, fields));
        }
        BackendMessage::EmptyQueryResponse => write_tagged(buf, b'I', |_| {}),
        BackendMessage::NoticeResponse { fields } => {
            write_tagged(buf, b'N', |payload| write_error_fields(payload, fields));
        }
    }
}

// ---------------------------------------------------------------------------
// Inner decoders — they work on byte slices and return the consumed
// length together with the parsed message. The top-level entry points
// translate `Truncated` into `Ok(None)` and otherwise advance `buf`.
// ---------------------------------------------------------------------------

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
fn decode_with<T, F>(buf: &mut BytesMut, inner: F) -> Result<Option<T>, ProtocolError>
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
fn payload_truncated_is_malformed<T>(result: Result<T, ProtocolError>) -> Result<T, ProtocolError> {
    match result {
        Err(ProtocolError::Truncated) => Err(ProtocolError::Malformed("payload truncated")),
        other => other,
    }
}

/// Decode either a startup message (no type tag) or a tagged frontend
/// message from `bytes`. The discriminator is whether the first byte
/// is a valid ASCII frontend tag: startup messages begin with the
/// first byte of their `i32` length, which for sane protocol numbers
/// is `0x00` — outside the tag space.
fn decode_frontend_inner(bytes: &[u8]) -> Result<(FrontendMessage, usize), ProtocolError> {
    // The startup-vs-tagged discriminator: every tagged frontend
    // message type tag is an ASCII letter. The startup message's
    // first byte is the most-significant byte of an `i32` length, and
    // for any realistic startup length (< 16 MiB) that byte is zero.
    // We therefore treat a leading zero as "startup".
    let first = *bytes.first().ok_or(ProtocolError::Truncated)?;
    if first == 0 {
        return decode_startup(bytes);
    }

    let (payload, total) = take_framed_message(bytes)?;
    let payload = PayloadReader::new(payload);

    let msg = payload_truncated_is_malformed(decode_frontend_payload(first, payload))?;
    Ok((msg, total))
}

fn decode_frontend_payload(
    first: u8,
    payload: PayloadReader<'_>,
) -> Result<FrontendMessage, ProtocolError> {
    let msg = match first {
        b'Q' => {
            let mut p = payload;
            let sql = p.read_cstring()?;
            p.ensure_drained()?;
            FrontendMessage::Query { sql }
        }
        b'P' => {
            let mut p = payload;
            let name = p.read_cstring()?;
            let sql = p.read_cstring()?;
            let count = p.read_i16()?;
            let count = nonneg_usize(count, "parse param count")?;
            let mut param_types = Vec::with_capacity(count.min(64));
            for _ in 0..count {
                param_types.push(p.read_u32()?);
            }
            p.ensure_drained()?;
            FrontendMessage::Parse {
                name,
                sql,
                param_types,
            }
        }
        b'B' => {
            let mut p = payload;
            let portal_name = p.read_cstring()?;
            let statement_name = p.read_cstring()?;
            let format_count = p.read_i16()?;
            let format_count = nonneg_usize(format_count, "bind format count")?;
            // The simplified Bind serializer always emits zero
            // per-parameter format codes (meaning "all text"). The
            // decoder accepts any count and skips the values to stay
            // interoperable with libpq clients.
            for _ in 0..format_count {
                let _ = p.read_i16()?;
            }
            let param_count = p.read_i16()?;
            let param_count = nonneg_usize(param_count, "bind param count")?;
            let mut params = Vec::with_capacity(param_count.min(64));
            for _ in 0..param_count {
                params.push(p.read_value()?);
            }
            let result_count = p.read_i16()?;
            let result_count = nonneg_usize(result_count, "bind result format count")?;
            let mut result_formats = Vec::with_capacity(result_count.min(64));
            for _ in 0..result_count {
                result_formats.push(p.read_i16()?);
            }
            p.ensure_drained()?;
            FrontendMessage::Bind {
                portal_name,
                statement_name,
                params,
                result_formats,
            }
        }
        b'D' => {
            let mut p = payload;
            let kind = p.read_u8()?;
            if kind != b'S' && kind != b'P' {
                return Err(ProtocolError::Malformed("describe kind"));
            }
            let name = p.read_cstring()?;
            p.ensure_drained()?;
            FrontendMessage::Describe { kind, name }
        }
        b'E' => {
            let mut p = payload;
            let portal = p.read_cstring()?;
            let max_rows = p.read_i32()?;
            p.ensure_drained()?;
            FrontendMessage::Execute { portal, max_rows }
        }
        b'S' => {
            payload.ensure_drained()?;
            FrontendMessage::Sync
        }
        b'X' => {
            payload.ensure_drained()?;
            FrontendMessage::Terminate
        }
        b'p' => {
            let mut p = payload;
            let password = p.read_cstring()?;
            p.ensure_drained()?;
            FrontendMessage::Password { password }
        }
        other => return Err(ProtocolError::UnknownMessageType(other)),
    };

    Ok(msg)
}

fn decode_startup(bytes: &[u8]) -> Result<(FrontendMessage, usize), ProtocolError> {
    if bytes.len() < 4 {
        return Err(ProtocolError::Truncated);
    }
    let mut len_buf = [0_u8; 4];
    len_buf.copy_from_slice(&bytes[..4]);
    let length = i32::from_be_bytes(len_buf);
    if length < 8 {
        return Err(ProtocolError::Malformed("startup length too small"));
    }
    let total = usize_from_i32(length, "startup length")?;
    if total > MAX_PAYLOAD {
        return Err(ProtocolError::Malformed("startup length too large"));
    }
    if bytes.len() < total {
        return Err(ProtocolError::Truncated);
    }

    let payload = &bytes[4..total];
    let msg = payload_truncated_is_malformed(decode_startup_payload(payload))?;
    Ok((msg, total))
}

fn decode_startup_payload(payload: &[u8]) -> Result<FrontendMessage, ProtocolError> {
    let mut p = PayloadReader::new(payload);
    let protocol_major = p.read_u16()?;
    let protocol_minor = p.read_u16()?;

    let mut params = Vec::new();
    loop {
        if p.is_empty() {
            return Err(ProtocolError::Malformed(
                "startup parameters missing terminator",
            ));
        }
        if p.peek_u8()? == 0 {
            p.advance(1);
            break;
        }
        let name = p.read_cstring()?;
        let value = p.read_cstring()?;
        params.push((name, value));
    }
    p.ensure_drained()?;
    Ok(FrontendMessage::StartupMessage {
        protocol_major,
        protocol_minor,
        params,
    })
}

fn decode_backend_inner(bytes: &[u8]) -> Result<(BackendMessage, usize), ProtocolError> {
    let first = *bytes.first().ok_or(ProtocolError::Truncated)?;
    let (payload, total) = take_framed_message(bytes)?;
    let p = PayloadReader::new(payload);

    let msg = payload_truncated_is_malformed(decode_backend_payload(first, p))?;
    Ok((msg, total))
}

fn decode_backend_payload(
    first: u8,
    mut p: PayloadReader<'_>,
) -> Result<BackendMessage, ProtocolError> {
    let msg = match first {
        b'R' => {
            let kind = p.read_i32()?;
            match kind {
                0 => {
                    p.ensure_drained()?;
                    BackendMessage::AuthenticationOk
                }
                3 => {
                    p.ensure_drained()?;
                    BackendMessage::AuthenticationCleartextPassword
                }
                5 => {
                    let mut salt = [0_u8; 4];
                    p.read_exact(&mut salt)?;
                    p.ensure_drained()?;
                    BackendMessage::AuthenticationMD5Password { salt }
                }
                _ => return Err(ProtocolError::Malformed("unknown authentication subtype")),
            }
        }
        b'S' => {
            let name = p.read_cstring()?;
            let value = p.read_cstring()?;
            p.ensure_drained()?;
            BackendMessage::ParameterStatus { name, value }
        }
        b'K' => {
            let process_id = p.read_i32()?;
            let secret_key = p.read_i32()?;
            p.ensure_drained()?;
            BackendMessage::BackendKeyData {
                process_id,
                secret_key,
            }
        }
        b'Z' => {
            let status = p.read_u8()?;
            if status != b'I' && status != b'T' && status != b'E' {
                return Err(ProtocolError::Malformed("ready-for-query status"));
            }
            p.ensure_drained()?;
            BackendMessage::ReadyForQuery { status }
        }
        b'T' => {
            let fields = read_row_description(&mut p)?;
            p.ensure_drained()?;
            BackendMessage::RowDescription { fields }
        }
        b'D' => {
            let count = p.read_i16()?;
            let count = nonneg_usize(count, "data row column count")?;
            let mut columns = Vec::with_capacity(count.min(64));
            for _ in 0..count {
                columns.push(p.read_value()?);
            }
            p.ensure_drained()?;
            BackendMessage::DataRow { columns }
        }
        b'C' => {
            let tag = p.read_cstring()?;
            p.ensure_drained()?;
            BackendMessage::CommandComplete { tag }
        }
        b'E' => {
            let fields = read_error_fields(&mut p)?;
            p.ensure_drained()?;
            BackendMessage::ErrorResponse { fields }
        }
        b'I' => {
            p.ensure_drained()?;
            BackendMessage::EmptyQueryResponse
        }
        b'N' => {
            let fields = read_error_fields(&mut p)?;
            p.ensure_drained()?;
            BackendMessage::NoticeResponse { fields }
        }
        other => return Err(ProtocolError::UnknownMessageType(other)),
    };

    Ok(msg)
}

fn read_row_description(p: &mut PayloadReader<'_>) -> Result<Vec<FieldDescription>, ProtocolError> {
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

fn read_error_fields(p: &mut PayloadReader<'_>) -> Result<Vec<(u8, String)>, ProtocolError> {
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
// Framing helpers
// ---------------------------------------------------------------------------

/// Validate the framing of a tagged message and return its payload
/// slice (no header) together with the total bytes consumed
/// (header + payload).
fn take_framed_message(bytes: &[u8]) -> Result<(&[u8], usize), ProtocolError> {
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
    // Subtracting 4 is safe: we already verified `length >= 4`.
    let payload_end = total;
    let payload_start = HEADER_LEN;
    Ok((&bytes[payload_start..payload_end], total))
}

/// Write a tagged message: type byte, placeholder length, payload,
/// then back-fill the length once the payload is complete.
fn write_tagged<F>(buf: &mut BytesMut, tag: u8, write_payload: F)
where
    F: FnOnce(&mut BytesMut),
{
    buf.put_u8(tag);
    let length_index = buf.len();
    buf.put_i32(0); // placeholder
    let payload_start = buf.len();
    write_payload(buf);
    let payload_end = buf.len();
    let payload_len = payload_end - payload_start;
    // `length` on the wire includes the 4 length bytes themselves.
    let length = i32_from_usize(payload_len + 4);
    buf[length_index..length_index + 4].copy_from_slice(&length.to_be_bytes());
}

fn encode_startup(
    protocol_major: u16,
    protocol_minor: u16,
    params: &[(String, String)],
    buf: &mut BytesMut,
) {
    let length_index = buf.len();
    buf.put_i32(0); // placeholder
    let payload_start = buf.len();
    buf.put_u16(protocol_major);
    buf.put_u16(protocol_minor);
    for (name, value) in params {
        write_cstring(buf, name);
        write_cstring(buf, value);
    }
    buf.put_u8(0); // empty key terminates the parameter list
    let payload_end = buf.len();
    // Total message length includes the 4 length bytes.
    let total = i32_from_usize(payload_end - payload_start + 4);
    buf[length_index..length_index + 4].copy_from_slice(&total.to_be_bytes());
}

fn write_cstring(buf: &mut BytesMut, s: &str) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}

fn write_value(buf: &mut BytesMut, value: Option<&[u8]>) {
    match value {
        None => buf.put_i32(-1),
        Some(bytes) => {
            buf.put_i32(i32_from_usize(bytes.len()));
            buf.put_slice(bytes);
        }
    }
}

fn write_error_fields(buf: &mut BytesMut, fields: &[(u8, String)]) {
    for (code, value) in fields {
        buf.put_u8(*code);
        write_cstring(buf, value);
    }
    buf.put_u8(0);
}

// ---------------------------------------------------------------------------
// Integer-conversion helpers that surface protocol errors rather than
// panic on bad input.
// ---------------------------------------------------------------------------

const fn usize_from_i32(value: i32, _what: &'static str) -> Result<usize, ProtocolError> {
    if value < 0 {
        return Err(ProtocolError::Malformed("negative length"));
    }
    Ok(value as usize)
}

const fn nonneg_usize(value: i16, _what: &'static str) -> Result<usize, ProtocolError> {
    if value < 0 {
        return Err(ProtocolError::Malformed("negative count"));
    }
    Ok(value as usize)
}

/// Encoder helper. The wire length is encoded as a signed 32-bit
/// integer; messages larger than `i32::MAX` cannot be expressed by the
/// protocol and are clamped to the upper bound. This crate never
/// constructs such messages in practice — callers above the protocol
/// layer chunk large payloads — but we still avoid a wrapping cast.
fn i32_from_usize(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

/// Same logic for `i16`-sized counts (parameter counts, column
/// counts). Saturating cast is the safe choice; a real overflow here
/// indicates a buggy caller.
fn i16_from_usize(value: usize) -> i16 {
    i16::try_from(value).unwrap_or(i16::MAX)
}

// ---------------------------------------------------------------------------
// Payload reader. Tracks a slice and an offset, exposing
// fixed-width reads, NUL-terminated strings, and a length-prefixed
// value reader. Every method returns [`ProtocolError::Truncated`] when
// the request exceeds the remaining bytes.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct PayloadReader<'a> {
    bytes: &'a [u8],
}

impl<'a> PayloadReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    const fn remaining(&self) -> usize {
        self.bytes.len()
    }

    fn advance(&mut self, n: usize) {
        self.bytes = &self.bytes[n..];
    }

    fn peek_u8(&self) -> Result<u8, ProtocolError> {
        self.bytes.first().copied().ok_or(ProtocolError::Truncated)
    }

    fn read_u8(&mut self) -> Result<u8, ProtocolError> {
        let v = self.peek_u8()?;
        self.advance(1);
        Ok(v)
    }

    fn read_u16(&mut self) -> Result<u16, ProtocolError> {
        let mut out = [0_u8; 2];
        self.read_exact(&mut out)?;
        Ok(u16::from_be_bytes(out))
    }

    fn read_u32(&mut self) -> Result<u32, ProtocolError> {
        let mut out = [0_u8; 4];
        self.read_exact(&mut out)?;
        Ok(u32::from_be_bytes(out))
    }

    fn read_i16(&mut self) -> Result<i16, ProtocolError> {
        let mut out = [0_u8; 2];
        self.read_exact(&mut out)?;
        Ok(i16::from_be_bytes(out))
    }

    fn read_i32(&mut self) -> Result<i32, ProtocolError> {
        let mut out = [0_u8; 4];
        self.read_exact(&mut out)?;
        Ok(i32::from_be_bytes(out))
    }

    fn read_exact(&mut self, out: &mut [u8]) -> Result<(), ProtocolError> {
        if self.remaining() < out.len() {
            return Err(ProtocolError::Truncated);
        }
        let (head, rest) = self.bytes.split_at(out.len());
        out.copy_from_slice(head);
        self.bytes = rest;
        Ok(())
    }

    fn read_cstring(&mut self) -> Result<String, ProtocolError> {
        let nul = self
            .bytes
            .iter()
            .position(|&b| b == 0)
            .ok_or(ProtocolError::Truncated)?;
        let s = std::str::from_utf8(&self.bytes[..nul])?.to_owned();
        self.advance(nul + 1);
        Ok(s)
    }

    fn read_value(&mut self) -> Result<Option<Vec<u8>>, ProtocolError> {
        let len = self.read_i32()?;
        if len == -1 {
            return Ok(None);
        }
        if len < 0 {
            return Err(ProtocolError::Malformed("negative value length"));
        }
        let len = len as usize;
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

    const fn ensure_drained(&self) -> Result<(), ProtocolError> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(ProtocolError::Malformed("trailing bytes in payload"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------
    // Round-trip helpers
    // -------------------------------------------------------------------

    fn round_trip_frontend(msg: &FrontendMessage) -> FrontendMessage {
        let mut buf = BytesMut::new();
        encode_frontend(msg, &mut buf);
        let decoded = decode_frontend(&mut buf).expect("decode").expect("some");
        assert!(buf.is_empty(), "decoder did not consume all bytes");
        decoded
    }

    fn round_trip_backend(msg: &BackendMessage) -> BackendMessage {
        let mut buf = BytesMut::new();
        encode_backend(msg, &mut buf);
        let decoded = decode_backend(&mut buf).expect("decode").expect("some");
        assert!(buf.is_empty(), "decoder did not consume all bytes");
        decoded
    }

    // -------------------------------------------------------------------
    // StartupMessage
    // -------------------------------------------------------------------

    #[test]
    fn startup_round_trip() {
        let msg = FrontendMessage::StartupMessage {
            protocol_major: 3,
            protocol_minor: 0,
            params: vec![
                ("user".into(), "ultrasql".into()),
                ("database".into(), "ultrasql".into()),
                ("application_name".into(), "psql".into()),
            ],
        };
        assert_eq!(round_trip_frontend(&msg), msg);
    }

    #[test]
    fn startup_has_no_leading_type_byte() {
        let msg = FrontendMessage::StartupMessage {
            protocol_major: 3,
            protocol_minor: 0,
            params: vec![("user".into(), "x".into())],
        };
        let mut buf = BytesMut::new();
        encode_frontend(&msg, &mut buf);
        // First byte is the high byte of an i32 length; for a short
        // startup it must be 0, never an ASCII tag like b'Q' or b'P'.
        assert_eq!(buf[0], 0);
        // Bytes 4..6 must be the major version 3 big-endian.
        assert_eq!(&buf[4..6], &[0, 3]);
    }

    #[test]
    fn startup_empty_params() {
        let msg = FrontendMessage::StartupMessage {
            protocol_major: 3,
            protocol_minor: 0,
            params: vec![],
        };
        assert_eq!(round_trip_frontend(&msg), msg);
    }

    #[test]
    fn startup_missing_terminator_rejected() {
        // Length 8 ⇒ 4-byte length plus two `u16` protocol numbers,
        // and no terminator. The PostgreSQL spec requires a trailing
        // NUL to end the parameter list.
        let mut bytes = BytesMut::new();
        bytes.put_i32(8);
        bytes.put_u16(3);
        bytes.put_u16(0);
        let err = decode_frontend(&mut bytes).unwrap_err();
        assert!(matches!(err, ProtocolError::Malformed(_)));
    }

    // -------------------------------------------------------------------
    // Frontend round-trips
    // -------------------------------------------------------------------

    #[test]
    fn query_round_trip() {
        let msg = FrontendMessage::Query {
            sql: "SELECT 1".into(),
        };
        assert_eq!(round_trip_frontend(&msg), msg);
    }

    #[test]
    fn parse_round_trip() {
        let msg = FrontendMessage::Parse {
            name: "stmt1".into(),
            sql: "SELECT $1::int + $2::int".into(),
            param_types: vec![23, 23],
        };
        assert_eq!(round_trip_frontend(&msg), msg);
    }

    #[test]
    fn bind_round_trip() {
        let msg = FrontendMessage::Bind {
            portal_name: "p1".into(),
            statement_name: "stmt1".into(),
            params: vec![
                Some(b"42".to_vec()),
                None,
                Some(b"hello world".to_vec()),
                Some(Vec::new()),
            ],
            result_formats: vec![0, 1],
        };
        assert_eq!(round_trip_frontend(&msg), msg);
    }

    #[test]
    fn describe_statement_round_trip() {
        let msg = FrontendMessage::Describe {
            kind: b'S',
            name: "stmt1".into(),
        };
        assert_eq!(round_trip_frontend(&msg), msg);
    }

    #[test]
    fn describe_portal_round_trip() {
        let msg = FrontendMessage::Describe {
            kind: b'P',
            name: String::new(),
        };
        assert_eq!(round_trip_frontend(&msg), msg);
    }

    #[test]
    fn execute_round_trip() {
        let msg = FrontendMessage::Execute {
            portal: "p1".into(),
            max_rows: 100,
        };
        assert_eq!(round_trip_frontend(&msg), msg);
    }

    #[test]
    fn execute_unlimited_round_trip() {
        let msg = FrontendMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        };
        assert_eq!(round_trip_frontend(&msg), msg);
    }

    #[test]
    fn sync_round_trip() {
        assert_eq!(
            round_trip_frontend(&FrontendMessage::Sync),
            FrontendMessage::Sync
        );
    }

    #[test]
    fn terminate_round_trip() {
        assert_eq!(
            round_trip_frontend(&FrontendMessage::Terminate),
            FrontendMessage::Terminate
        );
    }

    #[test]
    fn password_round_trip() {
        let msg = FrontendMessage::Password {
            password: "hunter2".into(),
        };
        assert_eq!(round_trip_frontend(&msg), msg);
    }

    // -------------------------------------------------------------------
    // Backend round-trips
    // -------------------------------------------------------------------

    #[test]
    fn authentication_ok_round_trip() {
        assert_eq!(
            round_trip_backend(&BackendMessage::AuthenticationOk),
            BackendMessage::AuthenticationOk
        );
    }

    #[test]
    fn authentication_cleartext_round_trip() {
        assert_eq!(
            round_trip_backend(&BackendMessage::AuthenticationCleartextPassword),
            BackendMessage::AuthenticationCleartextPassword
        );
    }

    #[test]
    fn authentication_md5_round_trip() {
        let msg = BackendMessage::AuthenticationMD5Password {
            salt: [0xDE, 0xAD, 0xBE, 0xEF],
        };
        assert_eq!(round_trip_backend(&msg), msg);
    }

    #[test]
    fn parameter_status_round_trip() {
        let msg = BackendMessage::ParameterStatus {
            name: "server_version".into(),
            value: "16.0 (UltraSQL)".into(),
        };
        assert_eq!(round_trip_backend(&msg), msg);
    }

    #[test]
    fn backend_key_data_round_trip() {
        let msg = BackendMessage::BackendKeyData {
            process_id: 12345,
            secret_key: -42,
        };
        assert_eq!(round_trip_backend(&msg), msg);
    }

    #[test]
    fn ready_for_query_round_trip() {
        for status in [b'I', b'T', b'E'] {
            let msg = BackendMessage::ReadyForQuery { status };
            let decoded = round_trip_backend(&msg);
            assert_eq!(decoded, msg);
        }
    }

    #[test]
    fn ready_for_query_rejects_invalid_status() {
        let mut buf = BytesMut::new();
        // Build a Z message with status 'X' — not in {I, T, E}.
        encode_backend(&BackendMessage::ReadyForQuery { status: b'I' }, &mut buf);
        // Mutate the status byte: payload is at offset 5 (tag + len).
        buf[5] = b'X';
        let err = decode_backend(&mut buf).unwrap_err();
        assert!(matches!(err, ProtocolError::Malformed(_)));
    }

    #[test]
    fn row_description_round_trip() {
        let msg = BackendMessage::RowDescription {
            fields: vec![
                FieldDescription {
                    name: "id".into(),
                    table_oid: 1234,
                    col_attnum: 1,
                    type_oid: 23, // int4
                    type_size: 4,
                    type_modifier: -1,
                    format_code: 0,
                },
                FieldDescription {
                    name: "label".into(),
                    table_oid: 1234,
                    col_attnum: 2,
                    type_oid: 25, // text
                    type_size: -1,
                    type_modifier: -1,
                    format_code: 0,
                },
            ],
        };
        assert_eq!(round_trip_backend(&msg), msg);
    }

    #[test]
    fn row_description_empty_round_trip() {
        let msg = BackendMessage::RowDescription { fields: vec![] };
        assert_eq!(round_trip_backend(&msg), msg);
    }

    #[test]
    fn data_row_round_trip() {
        let msg = BackendMessage::DataRow {
            columns: vec![
                Some(b"1".to_vec()),
                None,
                Some(b"alpha".to_vec()),
                Some(vec![]),
            ],
        };
        assert_eq!(round_trip_backend(&msg), msg);
    }

    #[test]
    fn command_complete_round_trip() {
        let msg = BackendMessage::CommandComplete {
            tag: "SELECT 42".into(),
        };
        assert_eq!(round_trip_backend(&msg), msg);
    }

    #[test]
    fn error_response_round_trip() {
        let msg = BackendMessage::ErrorResponse {
            fields: vec![
                (b'S', "ERROR".into()),
                (b'C', "42601".into()),
                (b'M', "syntax error".into()),
            ],
        };
        assert_eq!(round_trip_backend(&msg), msg);
    }

    #[test]
    fn empty_query_round_trip() {
        assert_eq!(
            round_trip_backend(&BackendMessage::EmptyQueryResponse),
            BackendMessage::EmptyQueryResponse
        );
    }

    #[test]
    fn notice_response_round_trip() {
        let msg = BackendMessage::NoticeResponse {
            fields: vec![(b'S', "NOTICE".into()), (b'M', "interesting fact".into())],
        };
        assert_eq!(round_trip_backend(&msg), msg);
    }

    // -------------------------------------------------------------------
    // Truncation / error behavior
    // -------------------------------------------------------------------

    #[test]
    fn truncated_returns_none_without_consuming() {
        let msg = FrontendMessage::Query {
            sql: "SELECT 1".into(),
        };
        let mut full = BytesMut::new();
        encode_frontend(&msg, &mut full);
        // Try every prefix shorter than the full message — each must
        // be reported as "not enough data yet" without consuming.
        for cut in 0..full.len() {
            let mut buf = BytesMut::from(&full[..cut]);
            let before = buf.len();
            let result = decode_frontend(&mut buf).expect("no protocol error on prefix");
            assert!(result.is_none(), "expected None at cut={cut}");
            assert_eq!(
                buf.len(),
                before,
                "decoder consumed on truncation cut={cut}"
            );
        }
    }

    #[test]
    fn truncated_backend_returns_none() {
        let msg = BackendMessage::ParameterStatus {
            name: "client_encoding".into(),
            value: "UTF8".into(),
        };
        let mut full = BytesMut::new();
        encode_backend(&msg, &mut full);
        for cut in 0..full.len() {
            let mut buf = BytesMut::from(&full[..cut]);
            let before = buf.len();
            let result = decode_backend(&mut buf).expect("no protocol error on prefix");
            assert!(result.is_none(), "expected None at cut={cut}");
            assert_eq!(buf.len(), before);
        }
    }

    #[test]
    fn unknown_frontend_type_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'?'); // not a valid frontend tag
        buf.put_i32(4);
        let err = decode_frontend(&mut buf).unwrap_err();
        match err {
            ProtocolError::UnknownMessageType(t) => assert_eq!(t, b'?'),
            other => panic!("expected UnknownMessageType, got {other:?}"),
        }
    }

    #[test]
    fn unknown_backend_type_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'?');
        buf.put_i32(4);
        let err = decode_backend(&mut buf).unwrap_err();
        assert!(matches!(err, ProtocolError::UnknownMessageType(b'?')));
    }

    #[test]
    fn negative_length_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32(-1);
        let err = decode_frontend(&mut buf).unwrap_err();
        assert!(matches!(err, ProtocolError::Malformed(_)));
    }

    #[test]
    fn length_too_small_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32(3); // must be ≥ 4
        let err = decode_frontend(&mut buf).unwrap_err();
        assert!(matches!(err, ProtocolError::Malformed(_)));
    }

    #[test]
    fn invalid_utf8_cstring_rejected() {
        // Hand-build a Query message with an invalid UTF-8 byte in
        // the SQL string.
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        // length = 4 (length itself) + 1 (bad byte) + 1 (NUL) = 6
        buf.put_i32(6);
        buf.put_u8(0xFF);
        buf.put_u8(0);
        let err = decode_frontend(&mut buf).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidUtf8(_)));
    }

    #[test]
    fn cstring_missing_nul_reported_as_truncated() {
        // A frontend Query whose declared length covers the bytes,
        // but where the SQL string never reaches a NUL terminator.
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32(8); // 4 + 4 bytes of payload
        buf.put_slice(b"ABCD"); // no NUL
        let err = decode_frontend(&mut buf).unwrap_err();
        // No NUL inside the framed payload is a payload-internal
        // truncation. The public decoder turns top-level truncation
        // into Ok(None); here the framed slice is complete so we
        // surface the inner Truncated as an error. Either way it must
        // not be a successful decode.
        assert!(matches!(
            err,
            ProtocolError::Truncated | ProtocolError::Malformed(_)
        ));
    }

    #[test]
    fn multiple_messages_decoded_in_sequence() {
        let mut buf = BytesMut::new();
        encode_frontend(
            &FrontendMessage::Query {
                sql: "SELECT 1".into(),
            },
            &mut buf,
        );
        encode_frontend(&FrontendMessage::Sync, &mut buf);
        encode_frontend(&FrontendMessage::Terminate, &mut buf);

        let first = decode_frontend(&mut buf).unwrap().unwrap();
        let second = decode_frontend(&mut buf).unwrap().unwrap();
        let third = decode_frontend(&mut buf).unwrap().unwrap();
        assert!(buf.is_empty());
        assert!(matches!(first, FrontendMessage::Query { .. }));
        assert!(matches!(second, FrontendMessage::Sync));
        assert!(matches!(third, FrontendMessage::Terminate));
    }

    #[test]
    fn describe_invalid_kind_rejected() {
        let mut buf = BytesMut::new();
        // Build a Describe with an invalid kind byte.
        buf.put_u8(b'D');
        // length = 4 + 1 (kind) + 1 (NUL for empty name) = 6
        buf.put_i32(6);
        buf.put_u8(b'X'); // not S or P
        buf.put_u8(0);
        let err = decode_frontend(&mut buf).unwrap_err();
        assert!(matches!(err, ProtocolError::Malformed(_)));
    }

    #[test]
    fn encode_appends_does_not_clear() {
        // Encoding into a non-empty buffer must append, not overwrite.
        let mut buf = BytesMut::new();
        buf.put_slice(b"prefix");
        encode_frontend(&FrontendMessage::Sync, &mut buf);
        assert!(buf.starts_with(b"prefix"));
        assert!(buf.len() > b"prefix".len());
    }
}
