//! Frontend message decoder.
//!
//! Distinguishes the unframed startup message (whose first byte is the
//! high byte of an `i32` length, conventionally zero for any realistic
//! protocol number) from the regular tagged messages. The tagged path
//! delegates to [`decode_frontend_payload`], which dispatches on the
//! type byte.

use crate::error::ProtocolError;
use crate::messages::FrontendMessage;

use super::describe_kind_from_byte;
use super::util::{
    PayloadReader, nonneg_usize, payload_truncated_is_malformed, take_framed_message,
    usize_from_i32,
};
use super::MAX_PAYLOAD;

/// Decode either a startup message (no type tag) or a tagged frontend
/// message from `bytes`. The discriminator is whether the first byte
/// is a valid ASCII frontend tag: startup messages begin with the
/// first byte of their `i32` length, which for sane protocol numbers
/// is `0x00` — outside the tag space.
pub(super) fn decode_frontend_inner(
    bytes: &[u8],
) -> Result<(FrontendMessage, usize), ProtocolError> {
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

// Large match; same structural argument as encode_frontend.
#[allow(clippy::too_many_lines)]
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
            // Per-spec: empty → all-text; single → applies to every
            // parameter; one-per → per-parameter. The decoder
            // preserves the raw vector; the resolution rules live in
            // the consumer.
            let mut param_formats = Vec::with_capacity(format_count.min(64));
            for _ in 0..format_count {
                param_formats.push(p.read_i16()?);
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
                param_formats,
                params,
                result_formats,
            }
        }
        b'D' => {
            // Spec §55.7: Describe (F). Byte1 kind ('S' or 'P'),
            // String name.
            let mut p = payload;
            let kind_byte = p.read_u8()?;
            let kind = describe_kind_from_byte(kind_byte)?;
            let name = p.read_cstring()?;
            p.ensure_drained()?;
            FrontendMessage::Describe { kind, name }
        }
        b'E' => {
            // Spec §55.7: Execute (F). String portal, Int32 max_rows.
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
        b'C' => {
            // Spec §55.7: Close (F). Byte1 kind ('S' or 'P'),
            // String name.
            let mut p = payload;
            let kind_byte = p.read_u8()?;
            let kind = describe_kind_from_byte(kind_byte)?;
            let name = p.read_cstring()?;
            p.ensure_drained()?;
            FrontendMessage::Close { kind, name }
        }
        b'H' => {
            // Spec §55.7: Flush (F). No payload.
            payload.ensure_drained()?;
            FrontendMessage::Flush
        }
        b'd' => {
            // Spec §55.7: CopyData (F&B). Byte[n] data — all remaining
            // payload bytes.
            let mut p = payload;
            let data = p.read_remaining();
            FrontendMessage::CopyData(data)
        }
        b'c' => {
            // Spec §55.7: CopyDone (F&B). No payload.
            payload.ensure_drained()?;
            FrontendMessage::CopyDone
        }
        b'f' => {
            // Spec §55.7: CopyFail (F). String error_message.
            let mut p = payload;
            let msg = p.read_cstring()?;
            p.ensure_drained()?;
            FrontendMessage::CopyFail(msg)
        }
        b'F' => {
            // Spec §55.7: FunctionCall (F).
            // Int32 funcid, Int16 nformat_codes, Int16[nformat_codes],
            // Int16 nargs, for each: Int32 len + data,
            // Int16 result_format.
            let mut p = payload;
            let function_oid = p.read_u32()?;
            let nformats = p.read_i16()?;
            let nformats = nonneg_usize(nformats, "function call format count")?;
            let mut arg_formats = Vec::with_capacity(nformats.min(64));
            for _ in 0..nformats {
                arg_formats.push(p.read_u16()?);
            }
            let nargs = p.read_i16()?;
            let nargs = nonneg_usize(nargs, "function call arg count")?;
            let mut args = Vec::with_capacity(nargs.min(64));
            for _ in 0..nargs {
                args.push(p.read_value()?);
            }
            let result_format = p.read_u16()?;
            p.ensure_drained()?;
            FrontendMessage::FunctionCall {
                function_oid,
                arg_formats,
                args,
                result_format,
            }
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

    // Magic code 80877102 = (1234, 5678) signals a `CancelRequest` riding
    // on the startup-packet framing instead of a real protocol version.
    // The remaining 8 bytes are `(pid, secret)` as `i32` big-endian per
    // the PostgreSQL spec.
    if protocol_major == 1234 && protocol_minor == 5678 {
        let process_id = p.read_i32()?;
        let secret_key = p.read_i32()?;
        p.ensure_drained()?;
        return Ok(FrontendMessage::CancelRequest {
            process_id,
            secret_key,
        });
    }

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
