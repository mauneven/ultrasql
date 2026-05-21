//! Bounded object-store readers for table-function scans.

use std::io::{self, BufRead, Read};

use ultrasql_objectstore::{ObjectLocation, read_object_range_with_metadata};

const OBJECT_STREAM_CHUNK_BYTES: u64 = 64 * 1024;

/// Range-backed reader for S3/R2/GCS-compatible objects.
#[derive(Debug)]
pub(super) struct ObjectRangeReader {
    location: ObjectLocation,
    display: String,
    chunk_size: u64,
    pos: u64,
    object_size: Option<u64>,
    buffer: Vec<u8>,
    cursor: usize,
    eof: bool,
}

impl ObjectRangeReader {
    /// Open `location` for bounded sequential range reads.
    pub(super) fn new(location: ObjectLocation) -> Self {
        let display = location.display_uri();
        Self {
            location,
            display,
            chunk_size: OBJECT_STREAM_CHUNK_BYTES,
            pos: 0,
            object_size: None,
            buffer: Vec::new(),
            cursor: 0,
            eof: false,
        }
    }

    fn refill(&mut self) -> io::Result<()> {
        if self.cursor < self.buffer.len() || self.eof {
            return Ok(());
        }
        self.buffer.clear();
        self.cursor = 0;
        let requested = self.object_size.map_or(self.chunk_size, |size| {
            size.saturating_sub(self.pos).min(self.chunk_size)
        });
        if requested == 0 {
            self.eof = true;
            return Ok(());
        }
        let range = read_object_range_with_metadata(&self.location, self.pos, requested)
            .map_err(|err| io::Error::other(format!("{}: {err}", self.display)))?;
        if let Some(size) = range.object_size() {
            self.object_size = Some(size);
        }
        let bytes = range.into_bytes();
        if bytes.is_empty() {
            self.eof = true;
            return Ok(());
        }
        let read_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        self.pos = self.pos.saturating_add(read_len);
        if self.object_size.is_some_and(|size| self.pos >= size)
            || self.object_size.is_none() && read_len < requested
        {
            self.eof = true;
        }
        self.buffer = bytes;
        Ok(())
    }
}

impl Read for ObjectRangeReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        self.refill()?;
        let available = self.buffer.len().saturating_sub(self.cursor);
        if available == 0 {
            return Ok(0);
        }
        let n = available.min(out.len());
        out[..n].copy_from_slice(&self.buffer[self.cursor..self.cursor + n]);
        self.cursor += n;
        Ok(n)
    }
}

impl BufRead for ObjectRangeReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.refill()?;
        Ok(&self.buffer[self.cursor..])
    }

    fn consume(&mut self, amt: usize) {
        self.cursor = self.cursor.saturating_add(amt).min(self.buffer.len());
    }
}
