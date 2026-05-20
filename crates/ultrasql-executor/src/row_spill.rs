//! Row-oriented temp-file spill support for executor operators.
//!
//! The file format is intentionally small and stable inside one process:
//! repeated `u32 little-endian length` + `RowCodec` payload records. Each
//! operator owns its temp files and decides how to order, partition, or
//! merge spilled rows.

use std::io::{Read, Seek, SeekFrom, Write};

use ultrasql_core::Value;

use crate::ExecError;
use crate::row_codec::RowCodec;
use crate::work_mem::temp_file_limit;

const SPILL_ROW_LEN_BYTES: u64 = 4;

/// Append/read temp-file wrapper for row payloads encoded with [`RowCodec`].
#[derive(Debug)]
pub(crate) struct RowSpillFile {
    file: tempfile::NamedTempFile,
    label: &'static str,
    bytes: u64,
}

impl RowSpillFile {
    /// Create a new temp spill file.
    pub(crate) fn new(label: &'static str) -> Result<Self, ExecError> {
        let file = tempfile::NamedTempFile::new().map_err(|error| {
            ExecError::TypeMismatch(format!("{label} spill create failed: {error}"))
        })?;
        Ok(Self {
            file,
            label,
            bytes: 0,
        })
    }

    /// Encode and append one row.
    pub(crate) fn append_row(&mut self, codec: &RowCodec, row: &[Value]) -> Result<(), ExecError> {
        let encoded = encode_row(codec, row, self.label)?;
        self.append_encoded(&encoded)
    }

    /// Append a pre-encoded row payload.
    pub(crate) fn append_encoded(&mut self, encoded: &[u8]) -> Result<(), ExecError> {
        let len = u32::try_from(encoded.len()).map_err(|_| {
            ExecError::TypeMismatch(format!("{} spill row exceeds u32 length", self.label))
        })?;
        let row_bytes = u64::from(len).saturating_add(SPILL_ROW_LEN_BYTES);
        if self.bytes.saturating_add(row_bytes) > temp_file_limit() {
            return Err(ExecError::Unsupported("spill exceeded temp_file_limit"));
        }

        let handle = self.file.as_file_mut();
        handle
            .write_all(&len.to_le_bytes())
            .map_err(|error| spill_io_error(self.label, "write row length", error))?;
        handle
            .write_all(encoded)
            .map_err(|error| spill_io_error(self.label, "write row", error))?;
        self.bytes = self.bytes.saturating_add(row_bytes);
        Ok(())
    }

    /// Rewind the file so subsequent [`Self::read_next_row`] calls start at
    /// the first record.
    pub(crate) fn rewind(&mut self) -> Result<(), ExecError> {
        let handle = self.file.as_file_mut();
        handle
            .flush()
            .map_err(|error| spill_io_error(self.label, "flush", error))?;
        handle
            .seek(SeekFrom::Start(0))
            .map_err(|error| spill_io_error(self.label, "rewind", error))?;
        Ok(())
    }

    /// Read one row from the current file cursor.
    pub(crate) fn read_next_row(
        &mut self,
        codec: &RowCodec,
    ) -> Result<Option<Vec<Value>>, ExecError> {
        let handle = self.file.as_file_mut();
        let mut len_buf = [0_u8; 4];
        match handle.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(spill_io_error(self.label, "read row length", error)),
        }
        let len = usize::try_from(u32::from_le_bytes(len_buf)).map_err(|_| {
            ExecError::TypeMismatch(format!("{} spill row length exceeds usize", self.label))
        })?;
        let mut encoded = vec![0_u8; len];
        handle
            .read_exact(&mut encoded)
            .map_err(|error| spill_io_error(self.label, "read row", error))?;
        let row = codec.decode(&encoded).map_err(|error| {
            ExecError::TypeMismatch(format!("{} spill decode failed: {error}", self.label))
        })?;
        Ok(Some(row))
    }

    /// Rewind and visit every row.
    pub(crate) fn scan_rows<F>(&mut self, codec: &RowCodec, mut visit: F) -> Result<(), ExecError>
    where
        F: FnMut(Vec<Value>) -> Result<(), ExecError>,
    {
        self.rewind()?;
        while let Some(row) = self.read_next_row(codec)? {
            visit(row)?;
        }
        Ok(())
    }
}

/// Return encoded row bytes including the record-length prefix.
pub(crate) fn encoded_row_bytes(
    codec: &RowCodec,
    row: &[Value],
    label: &'static str,
) -> Result<u64, ExecError> {
    let encoded = encode_row(codec, row, label)?;
    let len = u64::try_from(encoded.len())
        .map_err(|_| ExecError::TypeMismatch(format!("{label} spill row too large to account")))?;
    Ok(len.saturating_add(SPILL_ROW_LEN_BYTES))
}

fn encode_row(codec: &RowCodec, row: &[Value], label: &'static str) -> Result<Vec<u8>, ExecError> {
    codec
        .encode(row)
        .map_err(|error| ExecError::TypeMismatch(format!("{label} spill encode failed: {error}")))
}

fn spill_io_error(label: &str, action: &str, error: std::io::Error) -> ExecError {
    ExecError::TypeMismatch(format!("{label} spill {action} failed: {error}"))
}
