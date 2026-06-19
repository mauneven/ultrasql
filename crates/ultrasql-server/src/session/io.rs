//! Part of the `session` module split. The
//! `impl<RW> Session<RW>` block is reopened here to add a handful
//! of methods to the type defined in `session/mod.rs`. Splitting
//! across files keeps every unit under the 600-line ceiling without
//! changing semantics.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use ultrasql_protocol::{
    BackendMessage, FrontendMessage, decode_frontend, decode_frontend_raw, encode_backend,
};

use super::Session;
use crate::error::ServerError;

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

    /// Encode and flush a single backend message.
    pub(crate) async fn send(&mut self, msg: &BackendMessage) -> Result<(), ServerError> {
        self.write_buf.clear();
        encode_backend(msg, &mut self.write_buf);
        self.io.write_all(&self.write_buf).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Send a wire `ErrorResponse`. The fields are
    /// the minimal set every libpq client expects: severity, code,
    /// message.
    pub(crate) async fn send_error(
        &mut self,
        message: &str,
        sqlstate: &str,
    ) -> Result<(), ServerError> {
        let msg = BackendMessage::ErrorResponse {
            fields: vec![
                (b'S', "ERROR".to_string()),
                (b'C', sqlstate.to_string()),
                (b'M', message.to_string()),
            ],
        };
        self.send(&msg).await
    }
}
