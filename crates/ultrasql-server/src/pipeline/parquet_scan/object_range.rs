//! Object-store range reader bridging `read_parquet` to `ChunkReader`.

use std::io::{self, Read};

use bytes::Bytes;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};
use ultrasql_objectstore::{ObjectLocation, read_object_range, read_object_range_with_metadata};

use crate::error::ServerError;

#[derive(Clone, Debug)]
pub(super) struct ObjectRangeChunkReader {
    location: ObjectLocation,
    display: String,
    len: u64,
}

impl ObjectRangeChunkReader {
    pub(super) fn new(location: ObjectLocation) -> Result<Self, ServerError> {
        let display = location.display_uri();
        let probe = read_object_range_with_metadata(&location, 0, 1)
            .map_err(|err| ServerError::CopyFormat(format!("read_parquet: {err}")))?;
        let len = probe.object_size().ok_or_else(|| {
            ServerError::CopyFormat(format!(
                "read_parquet cannot determine object size for {display}: missing Content-Range"
            ))
        })?;
        Ok(Self {
            location,
            display,
            len,
        })
    }
}

impl Length for ObjectRangeChunkReader {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for ObjectRangeChunkReader {
    type T = ObjectRangeReadCursor;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.len {
            return Err(parquet_range_error(format!(
                "read_parquet range start {start} beyond {} length {}",
                self.display, self.len
            )));
        }
        Ok(ObjectRangeReadCursor {
            location: self.location.clone(),
            display: self.display.clone(),
            pos: start,
            len: self.len,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let length = validate_object_range(&self.display, start, length, self.len)?;
        let bytes = read_object_range(&self.location, start, length).map_err(|err| {
            parquet_range_error(format!(
                "read_parquet range GET {} bytes {start}+{length}: {err}",
                self.display
            ))
        })?;
        Ok(Bytes::from(bytes))
    }
}

#[derive(Debug)]
pub(super) struct ObjectRangeReadCursor {
    location: ObjectLocation,
    display: String,
    pos: u64,
    len: u64,
}

impl Read for ObjectRangeReadCursor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.pos >= self.len {
            return Ok(0);
        }
        let remaining = self.len - self.pos;
        let requested = remaining.min(parquet_range_read_len(buf.len(), &self.display)?);
        let bytes = read_object_range(&self.location, self.pos, requested).map_err(|err| {
            io::Error::other(format!(
                "read_parquet range GET {} bytes {}+{}: {err}",
                self.display, self.pos, requested
            ))
        })?;
        let read = bytes.len().min(buf.len());
        buf[..read].copy_from_slice(&bytes[..read]);
        let read_len = parquet_range_read_len(read, &self.display)?;
        self.pos = parquet_range_pos_add(self.pos, read_len, &self.display)?;
        Ok(read)
    }
}

fn parquet_range_read_len(len: usize, display: &str) -> io::Result<u64> {
    u64::try_from(len)
        .map_err(|_| io::Error::other(format!("{display}: range read byte count exceeds u64")))
}

pub(super) fn parquet_range_pos_add(pos: u64, read_len: u64, display: &str) -> io::Result<u64> {
    pos.checked_add(read_len)
        .ok_or_else(|| io::Error::other(format!("{display}: range cursor position overflow")))
}

fn validate_object_range(
    display: &str,
    start: u64,
    length: usize,
    object_len: u64,
) -> ParquetResult<u64> {
    let length = u64::try_from(length).map_err(|err| {
        parquet_range_error(format!(
            "read_parquet range length overflow for {display}: {err}"
        ))
    })?;
    let end = start.checked_add(length).ok_or_else(|| {
        parquet_range_error(format!(
            "read_parquet range overflows for {display}: start={start} length={length}"
        ))
    })?;
    if end > object_len {
        return Err(parquet_range_error(format!(
            "read_parquet range beyond {display}: start={start} length={length} object_len={object_len}"
        )));
    }
    Ok(length)
}

fn parquet_range_error(message: String) -> ParquetError {
    ParquetError::External(Box::new(io::Error::other(message)))
}
