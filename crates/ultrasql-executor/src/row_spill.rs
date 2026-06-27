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
        let row_bytes = spill_row_bytes_for_len(encoded.len(), self.label)?;
        let next_bytes = checked_temp_spill_total(self.bytes, row_bytes, self.label)?;

        let handle = self.file.as_file_mut();
        handle
            .write_all(&len.to_le_bytes())
            .map_err(|error| spill_io_error(self.label, "write row length", error))?;
        handle
            .write_all(encoded)
            .map_err(|error| spill_io_error(self.label, "write row", error))?;
        self.bytes = next_bytes;
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

    /// Bytes written to this spill file, including record-length prefixes.
    pub(crate) const fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// Return encoded row bytes including the record-length prefix.
pub(crate) fn encoded_row_bytes(
    codec: &RowCodec,
    row: &[Value],
    label: &'static str,
) -> Result<u64, ExecError> {
    let encoded = encode_row(codec, row, label)?;
    spill_row_bytes_for_len(encoded.len(), label)
}

pub(crate) fn spill_row_bytes_for_len(encoded_len: usize, label: &str) -> Result<u64, ExecError> {
    let len = u32::try_from(encoded_len)
        .map_err(|_| ExecError::TypeMismatch(format!("{label} spill row exceeds u32 length")))?;
    u64::from(len)
        .checked_add(SPILL_ROW_LEN_BYTES)
        .ok_or_else(|| ExecError::TypeMismatch(format!("{label} spill row byte overflow")))
}

pub(crate) fn checked_spill_bytes_add(
    current: u64,
    delta: u64,
    label: &str,
) -> Result<u64, ExecError> {
    current
        .checked_add(delta)
        .ok_or_else(|| ExecError::TypeMismatch(format!("{label} spill byte counter overflow")))
}

pub(crate) fn checked_temp_spill_total(
    current: u64,
    delta: u64,
    label: &str,
) -> Result<u64, ExecError> {
    let next = checked_spill_bytes_add(current, delta, label)?;
    if next > temp_file_limit() {
        // Both the in-memory work_mem budget and the on-disk spill allowance
        // are exhausted: surface a recoverable out-of-memory ERROR (SQLSTATE
        // 53200) rather than letting the heap grow toward a process OOM-kill.
        return Err(ExecError::OutOfMemory("spill exceeded temp_file_limit"));
    }
    Ok(next)
}

fn encode_row(codec: &RowCodec, row: &[Value], label: &'static str) -> Result<Vec<u8>, ExecError> {
    codec
        .encode(row)
        .map_err(|error| ExecError::TypeMismatch(format!("{label} spill encode failed: {error}")))
}

fn spill_io_error(label: &str, action: &str, error: std::io::Error) -> ExecError {
    ExecError::TypeMismatch(format!("{label} spill {action} failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_row_bytes_rejects_u32_overflow() {
        let too_large = usize::try_from(u64::from(u32::MAX) + 1).unwrap();
        let err = spill_row_bytes_for_len(too_large, "test").unwrap_err();
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }

    #[test]
    fn spill_byte_add_rejects_u64_overflow() {
        let err = checked_spill_bytes_add(u64::MAX, 1, "test").unwrap_err();
        assert!(matches!(err, ExecError::TypeMismatch(_)));
    }

    #[test]
    fn temp_spill_total_under_limit_succeeds() {
        let total = checked_temp_spill_total(0, 4096, "test").expect("within limit");
        assert_eq!(total, 4096);
    }

    #[test]
    fn temp_spill_total_over_temp_file_limit_is_out_of_memory() {
        // Both the in-memory work_mem budget and the on-disk spill allowance
        // are exhausted: this must be a recoverable out-of-memory ERROR
        // (server maps it to SQLSTATE 53200), not a process OOM-kill and not
        // the spill-retry signal.
        let err = checked_temp_spill_total(temp_file_limit(), 1, "test")
            .expect_err("exceeds temp_file_limit");
        assert!(
            matches!(err, ExecError::OutOfMemory(_)),
            "temp_file_limit exhaustion must surface as OutOfMemory, got {err:?}"
        );
    }
}
