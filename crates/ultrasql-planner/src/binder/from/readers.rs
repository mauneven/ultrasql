//! Parquet/Arrow schema readers, object-store range readers, and the byte
//! stream sources shared by the file-reading table functions.

use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};

use arrow_ipc::reader::FileReader as ArrowFileReader;
use arrow_schema::DataType as ArrowDataType;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};
use ultrasql_core::DataType;
use ultrasql_objectstore::{
    ObjectLocation, expand_object_store_specs, read_first_object_bytes, read_object_range,
    read_object_range_with_metadata,
};

use super::paths::{
    expand_file_path_specs, first_expanded_file, open_local_regular_file,
    path_specs_use_object_store,
};
use super::{JSON_STREAM_CHUNK_BYTES, PlanError};

pub(super) fn read_parquet_arrow_schema(path: &Path) -> Result<arrow_schema::SchemaRef, PlanError> {
    let file = open_local_regular_file("read_parquet", path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "read_parquet cannot inspect {}: {err}",
            path.display()
        ))
    })?;
    Ok(builder.schema().clone())
}

pub(super) fn read_parquet_object_schema(
    patterns: &[String],
) -> Result<arrow_schema::SchemaRef, PlanError> {
    let objects = expand_object_store_specs(patterns)
        .map_err(|err| PlanError::TypeMismatch(format!("read_parquet: {err}")))?;
    let location = objects.first().ok_or_else(|| {
        PlanError::TypeMismatch("read_parquet object path list is empty".to_owned())
    })?;
    let reader = PlannerObjectRangeChunkReader::new(location.clone())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "read_parquet cannot inspect {}: {err}",
            location.display_uri()
        ))
    })?;
    Ok(builder.schema().clone())
}

#[derive(Clone, Debug)]
struct PlannerObjectRangeChunkReader {
    location: ObjectLocation,
    display: String,
    len: u64,
}

impl PlannerObjectRangeChunkReader {
    fn new(location: ObjectLocation) -> Result<Self, PlanError> {
        let display = location.display_uri();
        let probe = read_object_range_with_metadata(&location, 0, 1)
            .map_err(|err| PlanError::TypeMismatch(format!("read_parquet: {err}")))?;
        let len = probe.object_size().ok_or_else(|| {
            PlanError::TypeMismatch(format!(
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

impl Length for PlannerObjectRangeChunkReader {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for PlannerObjectRangeChunkReader {
    type T = PlannerObjectRangeReadCursor;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.len {
            return Err(planner_parquet_range_error(format!(
                "read_parquet range start {start} beyond {} length {}",
                self.display, self.len
            )));
        }
        Ok(PlannerObjectRangeReadCursor {
            location: self.location.clone(),
            display: self.display.clone(),
            pos: start,
            len: self.len,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let length = validate_planner_object_range(&self.display, start, length, self.len)?;
        let bytes = read_object_range(&self.location, start, length).map_err(|err| {
            planner_parquet_range_error(format!(
                "read_parquet range GET {} bytes {start}+{length}: {err}",
                self.display
            ))
        })?;
        Ok(Bytes::from(bytes))
    }
}

#[derive(Debug)]
struct PlannerObjectRangeReadCursor {
    location: ObjectLocation,
    display: String,
    pos: u64,
    len: u64,
}

impl Read for PlannerObjectRangeReadCursor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.pos >= self.len {
            return Ok(0);
        }
        let remaining = self.len - self.pos;
        let requested = remaining.min(u64::try_from(buf.len()).unwrap_or(u64::MAX));
        let bytes = read_object_range(&self.location, self.pos, requested).map_err(|err| {
            io::Error::other(format!(
                "read_parquet range GET {} bytes {}+{}: {err}",
                self.display, self.pos, requested
            ))
        })?;
        let read = bytes.len().min(buf.len());
        buf[..read].copy_from_slice(&bytes[..read]);
        self.pos = self
            .pos
            .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        Ok(read)
    }
}

pub(super) fn validate_planner_object_range(
    display: &str,
    start: u64,
    length: usize,
    object_len: u64,
) -> ParquetResult<u64> {
    let length = u64::try_from(length).map_err(|err| {
        planner_parquet_range_error(format!(
            "read_parquet range length overflow for {display}: {err}"
        ))
    })?;
    let end = start.checked_add(length).ok_or_else(|| {
        planner_parquet_range_error(format!(
            "read_parquet range overflows for {display}: start={start} length={length}"
        ))
    })?;
    if end > object_len {
        return Err(planner_parquet_range_error(format!(
            "read_parquet range beyond {display}: start={start} length={length} object_len={object_len}"
        )));
    }
    Ok(length)
}

pub(super) fn planner_parquet_range_error(message: String) -> ParquetError {
    ParquetError::External(Box::new(io::Error::other(message)))
}

pub(super) fn parquet_arrow_type_to_sql(data_type: &ArrowDataType) -> Result<DataType, PlanError> {
    arrow_type_to_sql("read_parquet", data_type)
}

pub(super) fn arrow_type_to_sql(
    function_name: &str,
    data_type: &ArrowDataType,
) -> Result<DataType, PlanError> {
    match data_type {
        ArrowDataType::Boolean => Ok(DataType::Bool),
        ArrowDataType::Int32 => Ok(DataType::Int32),
        ArrowDataType::Int64 => Ok(DataType::Int64),
        ArrowDataType::Float32 => Ok(DataType::Float32),
        ArrowDataType::Float64 => Ok(DataType::Float64),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Ok(DataType::Text { max_len: None }),
        other => Err(PlanError::TypeMismatch(format!(
            "{function_name} unsupported Arrow type: {other}"
        ))),
    }
}

pub(super) fn read_arrow_schema_from_path_specs(
    path_specs: &[String],
) -> Result<arrow_schema::SchemaRef, PlanError> {
    if path_specs_use_object_store("read_arrow", path_specs)? {
        let (location, bytes) = read_first_object_bytes(path_specs)
            .map_err(|err| PlanError::TypeMismatch(format!("read_arrow: {err}")))?;
        let reader = ArrowFileReader::try_new(Cursor::new(bytes), None).map_err(|err| {
            PlanError::TypeMismatch(format!(
                "read_arrow cannot inspect {}: {err}",
                location.display_uri()
            ))
        })?;
        return Ok(reader.schema());
    }

    let first_path = first_expanded_file("read_arrow", path_specs)?;
    let file = open_local_regular_file("read_arrow", &first_path)?;
    let reader = ArrowFileReader::try_new(file, None).map_err(|err| {
        PlanError::TypeMismatch(format!(
            "read_arrow cannot inspect {}: {err}",
            first_path.display()
        ))
    })?;
    Ok(reader.schema())
}

#[derive(Clone, Debug)]
pub(super) enum PlannerStreamSpec {
    Local(PathBuf),
    Object(ObjectLocation),
}

impl PlannerStreamSpec {
    pub(super) fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::Object(object) => object.display_uri(),
        }
    }
}

pub(super) fn planner_stream_specs(
    function_name: &str,
    path_specs: &[String],
) -> Result<Vec<PlannerStreamSpec>, PlanError> {
    if path_specs_use_object_store(function_name, path_specs)? {
        let objects = expand_object_store_specs(path_specs)
            .map_err(|err| PlanError::TypeMismatch(format!("{function_name}: {err}")))?;
        return Ok(objects.into_iter().map(PlannerStreamSpec::Object).collect());
    }
    Ok(expand_file_path_specs(function_name, path_specs)?
        .into_iter()
        .map(PlannerStreamSpec::Local)
        .collect())
}

pub(super) fn open_planner_stream(
    function_name: &str,
    source: &PlannerStreamSpec,
) -> Result<Box<dyn Read>, PlanError> {
    match source {
        PlannerStreamSpec::Local(path) => {
            let file = open_local_regular_file(function_name, path)?;
            Ok(Box::new(file))
        }
        PlannerStreamSpec::Object(object) => {
            Ok(Box::new(PlannerObjectRangeReader::new(object.clone())))
        }
    }
}

struct PlannerObjectRangeReader {
    location: ObjectLocation,
    display: String,
    pos: u64,
    object_size: Option<u64>,
    buffer: Vec<u8>,
    cursor: usize,
    eof: bool,
}

impl PlannerObjectRangeReader {
    fn new(location: ObjectLocation) -> Self {
        let display = location.display_uri();
        Self {
            location,
            display,
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
        let requested = self.object_size.map_or(JSON_STREAM_CHUNK_BYTES, |size| {
            size.saturating_sub(self.pos).min(JSON_STREAM_CHUNK_BYTES)
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

impl Read for PlannerObjectRangeReader {
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
