//! Encoders for the PostgreSQL wire protocol.
//!
//! Both directions share the same framing helper [`write_tagged`]: it
//! writes the tag byte, reserves the 4-byte length placeholder, runs
//! the caller's payload writer, and backfills the length once the
//! body is complete. The unframed [`FrontendMessage::StartupMessage`]
//! goes through [`encode_startup`] which omits the tag.

use bytes::{BufMut, BytesMut};

use crate::messages::{BackendMessage, FrontendMessage};

use super::describe_kind_byte;
use super::util::{i16_from_usize, i32_from_usize};

/// Encode a [`FrontendMessage`] into `buf`.
///
/// The bytes are appended to the existing contents of `buf`; callers
/// that need a fresh allocation should pass a freshly-constructed
/// [`BytesMut`].
// The large match arms are unavoidable for a complete wire-protocol
// codec: each variant encodes a structurally distinct message type.
// Splitting into per-variant helpers would only add indirection without
// improving readability.
#[allow(clippy::too_many_lines)]
pub fn encode_frontend(msg: &FrontendMessage, buf: &mut BytesMut) {
    match msg {
        FrontendMessage::StartupMessage {
            protocol_major,
            protocol_minor,
            params,
        } => encode_startup(*protocol_major, *protocol_minor, params, buf),
        FrontendMessage::CancelRequest {
            process_id,
            secret_key,
        } => encode_cancel_request(*process_id, *secret_key, buf),
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
            param_formats,
            params,
            result_formats,
        } => {
            write_tagged(buf, b'B', |payload| {
                write_cstring(payload, portal_name);
                write_cstring(payload, statement_name);
                payload.put_i16(i16_from_usize(param_formats.len()));
                for code in param_formats {
                    payload.put_i16(*code);
                }
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
            // Spec §55.7: Describe (F), Byte1('D'), Int32 len,
            // Byte1 ('S' or 'P'), String name.
            write_tagged(buf, b'D', |payload| {
                payload.put_u8(describe_kind_byte(*kind));
                write_cstring(payload, name);
            });
        }
        FrontendMessage::Execute { portal, max_rows } => {
            // Spec §55.7: Execute (F), Byte1('E'), Int32 len,
            // String portal, Int32 max_rows.
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
        FrontendMessage::Close { kind, name } => {
            // Spec §55.7: Close (F), Byte1('C'), Int32 len,
            // Byte1 ('S' or 'P'), String name.
            write_tagged(buf, b'C', |payload| {
                payload.put_u8(describe_kind_byte(*kind));
                write_cstring(payload, name);
            });
        }
        FrontendMessage::Flush => write_tagged(buf, b'H', |_| {}),
        FrontendMessage::CopyData(data) => {
            // Spec §55.7: CopyData (F&B), Byte1('d'), Int32 len,
            // Byte[n] data.
            write_tagged(buf, b'd', |payload| {
                payload.put_slice(data);
            });
        }
        FrontendMessage::CopyDone => write_tagged(buf, b'c', |_| {}),
        FrontendMessage::CopyFail(msg) => {
            // Spec §55.7: CopyFail (F), Byte1('f'), Int32 len,
            // String error_message.
            write_tagged(buf, b'f', |payload| {
                write_cstring(payload, msg);
            });
        }
        FrontendMessage::FunctionCall {
            function_oid,
            arg_formats,
            args,
            result_format,
        } => {
            // Spec §55.7: FunctionCall (F), Byte1('F'), Int32 len,
            // Int32 funcid, Int16 nformat_codes, Int16[nformat_codes] format_codes,
            // Int16 nargs, for each: Int32 len (-1 = NULL) + Byte[len],
            // Int16 result_format.
            write_tagged(buf, b'F', |payload| {
                payload.put_u32(*function_oid);
                payload.put_i16(i16_from_usize(arg_formats.len()));
                for code in arg_formats {
                    payload.put_u16(*code);
                }
                payload.put_i16(i16_from_usize(args.len()));
                for value in args {
                    write_value(payload, value.as_deref());
                }
                payload.put_u16(*result_format);
            });
        }
    }
}

/// Encode a [`BackendMessage`] into `buf`.
// Same rationale as `encode_frontend`: every message variant differs
// structurally and splitting the match into per-variant helpers buys
// nothing.
#[allow(clippy::too_many_lines)]
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
        BackendMessage::ParseComplete => write_tagged(buf, b'1', |_| {}),
        BackendMessage::BindComplete => write_tagged(buf, b'2', |_| {}),
        BackendMessage::CloseComplete => write_tagged(buf, b'3', |_| {}),
        BackendMessage::NoData => write_tagged(buf, b'n', |_| {}),
        BackendMessage::ParameterDescription { type_oids } => {
            // Spec §55.7: ParameterDescription (B), Byte1('t'),
            // Int32 len, Int16 nparams, Int32[nparams] type_oids.
            write_tagged(buf, b't', |payload| {
                payload.put_i16(i16_from_usize(type_oids.len()));
                for oid in type_oids {
                    payload.put_u32(*oid);
                }
            });
        }
        BackendMessage::PortalSuspended => write_tagged(buf, b's', |_| {}),
        BackendMessage::CopyInResponse {
            overall_format,
            column_formats,
        } => {
            // Spec §55.7: CopyInResponse (B), Byte1('G'),
            // Int32 len, Int8 overall_format, Int16 ncols,
            // Int16[ncols] column_formats.
            write_tagged(buf, b'G', |payload| {
                payload.put_u8(*overall_format);
                payload.put_i16(i16_from_usize(column_formats.len()));
                for code in column_formats {
                    payload.put_u16(*code);
                }
            });
        }
        BackendMessage::CopyOutResponse {
            overall_format,
            column_formats,
        } => {
            // Spec §55.7: CopyOutResponse (B), Byte1('H'),
            // Int32 len, Int8 overall_format, Int16 ncols,
            // Int16[ncols] column_formats.
            write_tagged(buf, b'H', |payload| {
                payload.put_u8(*overall_format);
                payload.put_i16(i16_from_usize(column_formats.len()));
                for code in column_formats {
                    payload.put_u16(*code);
                }
            });
        }
        BackendMessage::CopyData(data) => {
            // Spec §55.7: CopyData (F&B), Byte1('d'),
            // Int32 len, Byte[n] data.
            write_tagged(buf, b'd', |payload| {
                payload.put_slice(data);
            });
        }
        BackendMessage::CopyDone => write_tagged(buf, b'c', |_| {}),
        BackendMessage::NotificationResponse {
            process_id,
            channel,
            payload,
        } => {
            // Spec §55.7: NotificationResponse (B), Byte1('A'),
            // Int32 len, Int32 pid, String channel, String payload.
            write_tagged(buf, b'A', |out| {
                out.put_i32(*process_id);
                write_cstring(out, channel);
                write_cstring(out, payload);
            });
        }
    }
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

fn encode_cancel_request(process_id: i32, secret_key: i32, buf: &mut BytesMut) {
    // Wire layout: i32 length=16, i32 code=80877102, i32 pid, i32 secret.
    // No type tag — same framing convention as the startup packet.
    buf.put_i32(16);
    buf.put_u16(1234);
    buf.put_u16(5678);
    buf.put_i32(process_id);
    buf.put_i32(secret_key);
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
