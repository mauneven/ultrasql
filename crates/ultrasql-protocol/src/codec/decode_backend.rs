//! Backend message decoder.
//!
//! All backend messages carry a 1-byte type tag and a length-prefixed
//! payload. [`decode_backend_inner`] reads the framed body via
//! [`take_framed_message`] and dispatches the payload to
//! [`decode_backend_payload`].

use crate::error::ProtocolError;
use crate::messages::BackendMessage;

use super::util::{
    PayloadReader, nonneg_usize, payload_truncated_is_malformed, read_error_fields,
    read_row_description, take_framed_message,
};

pub(super) fn decode_backend_inner(bytes: &[u8]) -> Result<(BackendMessage, usize), ProtocolError> {
    let first = *bytes.first().ok_or(ProtocolError::Truncated)?;
    let (payload, total) = take_framed_message(bytes)?;
    let p = PayloadReader::new(payload);

    let msg = payload_truncated_is_malformed(decode_backend_payload(first, p))?;
    Ok((msg, total))
}

// Large match; same structural argument as encode_backend.
#[allow(clippy::too_many_lines)]
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
                10 => {
                    // Mechanism names, each a NUL-terminated string, the list
                    // terminated by a final empty string.
                    let mut mechanisms = Vec::new();
                    loop {
                        let name = p.read_cstring()?;
                        if name.is_empty() {
                            break;
                        }
                        mechanisms.push(name);
                    }
                    p.ensure_drained()?;
                    BackendMessage::AuthenticationSASL { mechanisms }
                }
                11 => BackendMessage::AuthenticationSASLContinue {
                    data: p.read_remaining(),
                },
                12 => BackendMessage::AuthenticationSASLFinal {
                    data: p.read_remaining(),
                },
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
        b'1' => {
            // Spec §55.7: ParseComplete (B). No payload.
            p.ensure_drained()?;
            BackendMessage::ParseComplete
        }
        b'2' => {
            // Spec §55.7: BindComplete (B). No payload.
            p.ensure_drained()?;
            BackendMessage::BindComplete
        }
        b'3' => {
            // Spec §55.7: CloseComplete (B). No payload.
            p.ensure_drained()?;
            BackendMessage::CloseComplete
        }
        b'n' => {
            // Spec §55.7: NoData (B). No payload.
            p.ensure_drained()?;
            BackendMessage::NoData
        }
        b't' => {
            // Spec §55.7: ParameterDescription (B).
            // Int16 nparams, Int32[nparams] type_oids.
            let count = p.read_i16()?;
            let count = nonneg_usize(count, "parameter description count")?;
            let mut type_oids = Vec::with_capacity(count.min(64));
            for _ in 0..count {
                type_oids.push(p.read_u32()?);
            }
            p.ensure_drained()?;
            BackendMessage::ParameterDescription { type_oids }
        }
        b's' => {
            // Spec §55.7: PortalSuspended (B). No payload.
            p.ensure_drained()?;
            BackendMessage::PortalSuspended
        }
        b'G' => {
            // Spec §55.7: CopyInResponse (B).
            // Int8 overall_format, Int16 ncols, Int16[ncols] col_formats.
            let overall_format = p.read_u8()?;
            let ncols = p.read_i16()?;
            let ncols = nonneg_usize(ncols, "copy-in column count")?;
            let mut column_formats = Vec::with_capacity(ncols.min(64));
            for _ in 0..ncols {
                column_formats.push(p.read_u16()?);
            }
            p.ensure_drained()?;
            BackendMessage::CopyInResponse {
                overall_format,
                column_formats,
            }
        }
        b'H' => {
            // Spec §55.7: CopyOutResponse (B).
            // Int8 overall_format, Int16 ncols, Int16[ncols] col_formats.
            let overall_format = p.read_u8()?;
            let ncols = p.read_i16()?;
            let ncols = nonneg_usize(ncols, "copy-out column count")?;
            let mut column_formats = Vec::with_capacity(ncols.min(64));
            for _ in 0..ncols {
                column_formats.push(p.read_u16()?);
            }
            p.ensure_drained()?;
            BackendMessage::CopyOutResponse {
                overall_format,
                column_formats,
            }
        }
        b'd' => {
            // Spec §55.7: CopyData (F&B). All remaining payload bytes.
            let data = p.read_remaining();
            BackendMessage::CopyData(data)
        }
        b'c' => {
            // Spec §55.7: CopyDone (F&B). No payload.
            p.ensure_drained()?;
            BackendMessage::CopyDone
        }
        b'A' => {
            // Spec §55.7: NotificationResponse (B), Int32 pid,
            // String channel, String payload.
            let process_id = p.read_i32()?;
            let channel = p.read_cstring()?;
            let payload = p.read_cstring()?;
            p.ensure_drained()?;
            BackendMessage::NotificationResponse {
                process_id,
                channel,
                payload,
            }
        }
        other => return Err(ProtocolError::UnknownMessageType(other)),
    };

    Ok(msg)
}
