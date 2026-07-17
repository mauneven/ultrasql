//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use ultrasql_protocol::{
    BackendMessage, FrontendMessage, decode_frontend, decode_frontend_raw, encode_backend,
    error_fields,
};

use super::Session;
use crate::error::{ServerError, split_message_hint};

/// Maximum amount of coalesced Extended Query output retained between writes.
///
/// One protocol frame may exceed this limit; in that case the frame is sent as
/// soon as it has been encoded and the oversized allocation is released.
const EXTENDED_WRITE_WINDOW_BYTES: usize = 64 * 1024;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) async fn read_frontend(&mut self) -> Result<FrontendMessage, ServerError> {
        loop {
            if let Some(msg) = decode_frontend(&mut self.read_buf)? {
                return Ok(msg);
            }
            // Pull more bytes from the socket.
            let n = self.io.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(ServerError::UnexpectedEof);
            }
        }
    }

    /// Read the next tagged frontend frame as raw `(tag, payload)` bytes.
    ///
    /// Used during the SASL handshake, where the `'p'` tag is shared by
    /// `PasswordMessage` / `SASLInitialResponse` / `SASLResponse` and the
    /// auth state machine must interpret the payload itself.
    pub(crate) async fn read_raw_frontend_frame(&mut self) -> Result<(u8, Vec<u8>), ServerError> {
        loop {
            if let Some(frame) = decode_frontend_raw(&mut self.read_buf)? {
                return Ok(frame);
            }
            let n = self.io.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(ServerError::UnexpectedEof);
            }
        }
    }

    /// Encode and flush a single immediate backend message.
    ///
    /// Any successful Extended Query replies queued before this message are
    /// written first. Error and COPY paths use this method, so interactive
    /// protocol boundaries cannot overtake earlier Parse/Bind/Execute output.
    pub(crate) async fn send(&mut self, msg: &BackendMessage) -> Result<(), ServerError> {
        if !self.extended_write_buf.is_empty() {
            self.io.write_all(&self.extended_write_buf).await?;
            self.extended_write_buf.clear();
        }
        self.write_buf.clear();
        encode_backend(msg, &mut self.write_buf);
        self.io.write_all(&self.write_buf).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Queue one successful Extended Query response.
    ///
    /// Responses normally remain buffered until the client sends `Flush` or
    /// `Sync`. Reaching the bounded write window sends the accumulated bytes
    /// early, which is permitted by the protocol and prevents large result
    /// sets from growing the per-session queue without bound.
    pub(crate) async fn queue_extended_response(
        &mut self,
        msg: &BackendMessage,
    ) -> Result<(), ServerError> {
        encode_backend(msg, &mut self.extended_write_buf);
        if self.extended_write_buf.len() >= EXTENDED_WRITE_WINDOW_BYTES {
            self.flush_extended_responses().await?;
            if self.extended_write_buf.capacity() > EXTENDED_WRITE_WINDOW_BYTES {
                self.extended_write_buf =
                    bytes::BytesMut::with_capacity(EXTENDED_WRITE_WINDOW_BYTES);
            }
        }
        Ok(())
    }

    /// Write and flush every queued Extended Query response.
    pub(crate) async fn flush_extended_responses(&mut self) -> Result<(), ServerError> {
        if !self.extended_write_buf.is_empty() {
            self.io.write_all(&self.extended_write_buf).await?;
            self.extended_write_buf.clear();
        }
        self.io.flush().await?;
        Ok(())
    }

    /// Send a wire `ErrorResponse` with the structured field set
    /// (`S`, `V`, `C`, `M`, and `H` when the message carries a jammed
    /// `HINT:` line — see [`split_message_hint`]).
    pub(crate) async fn send_error(
        &mut self,
        message: &str,
        sqlstate: &str,
    ) -> Result<(), ServerError> {
        let (primary, hint) = split_message_hint(message);
        let msg = BackendMessage::ErrorResponse {
            fields: error_fields("ERROR", sqlstate, primary, None, hint),
        };
        self.send(&msg).await
    }
}
