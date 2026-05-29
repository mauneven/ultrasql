//! Local CSV table-function scans.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use ultrasql_core::csv::{
    CsvInferredType, CsvParseOptions, CsvSniff, CsvSniffColumn, expand_csv_path_specs,
    expand_csv_paths, parse_csv_records_with_options, sniff_csv_path, sniff_csv_text,
};
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_executor::{ExecError, Operator};
use ultrasql_objectstore::{
    ObjectLocation, expand_object_store_specs, is_object_store_uri, read_object_range_with_metadata,
};
use ultrasql_planner::{BinaryOp, ScalarExpr};
use ultrasql_vec::Batch;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

use crate::error::ServerError;

use super::object_stream::ObjectRangeReader;

const CSV_BATCH_TARGET_ROWS: usize = 4096;
const CSV_SNIFF_SAMPLE_BYTES: usize = 64 * 1024;
const CSV_SNIFF_SAMPLE_BYTES_U64: u64 = 64 * 1024;

/// File-backed scan for `read_csv(path_or_glob)`.
#[derive(Debug)]
pub(super) struct CsvTableScan {
    schema: Schema,
    projection: CsvProjection,
    predicate: Option<CsvPredicateEval>,
    stream: CsvStream,
}

impl CsvTableScan {
    /// Load matching CSV files into a query-local scan.
    pub(super) fn from_pattern(pattern: &str) -> Result<Self, ServerError> {
        let paths = expand_csv_paths(pattern)
            .map_err(|err| ServerError::CopyFormat(format!("read_csv: {err}")))?;
        Self::from_paths(paths, None, None, None)
    }

    /// Load CSV files with optional projection, predicate, and reject artifact.
    pub(super) fn from_path_specs_with_options(
        patterns: &[String],
        projection: Option<&[String]>,
        predicate: Option<&CsvPredicate>,
        reject_path: Option<&Path>,
    ) -> Result<Self, ServerError> {
        if path_specs_use_object_store("read_csv", patterns)? {
            return Self::from_object_specs(patterns, projection, predicate, reject_path);
        }
        if let [pattern] = patterns {
            if projection.is_none() && predicate.is_none() && reject_path.is_none() {
                return Self::from_pattern(pattern);
            }
            let paths = expand_csv_paths(pattern)
                .map_err(|err| ServerError::CopyFormat(format!("read_csv: {err}")))?;
            return Self::from_paths(paths, projection, predicate, reject_path);
        }
        let paths = expand_csv_path_specs(patterns)
            .map_err(|err| ServerError::CopyFormat(format!("read_csv: {err}")))?;
        Self::from_paths(paths, projection, predicate, reject_path)
    }

    fn from_paths(
        paths: Vec<PathBuf>,
        requested: Option<&[String]>,
        predicate: Option<&CsvPredicate>,
        reject_path: Option<&Path>,
    ) -> Result<Self, ServerError> {
        let mut expected_header: Option<Vec<String>> = None;
        let mut readers = VecDeque::new();

        for path in paths {
            let (header, reader) = CsvReaderState::from_path(&path, reject_path.is_some())?;
            validate_header(&header, &path)?;
            if let Some(expected) = &expected_header {
                if &header != expected {
                    return Err(ServerError::CopyFormat(format!(
                        "read_csv header mismatch in {}",
                        path.display()
                    )));
                }
            } else {
                expected_header = Some(header);
            }
            readers.push_back(reader);
        }

        let Some(header) = expected_header else {
            return Err(ServerError::CopyFormat(
                "read_csv path expansion returned no files".to_owned(),
            ));
        };
        let projection = CsvProjection::resolve(&header, requested)?;
        let predicate = predicate
            .map(|predicate| predicate.resolve(&header))
            .transpose()?;
        let schema = projection.schema.clone();
        Ok(Self {
            schema,
            projection,
            predicate,
            stream: CsvStream::new(readers, reject_path)?,
        })
    }

    fn from_object_specs(
        patterns: &[String],
        requested: Option<&[String]>,
        predicate: Option<&CsvPredicate>,
        reject_path: Option<&Path>,
    ) -> Result<Self, ServerError> {
        let objects = expand_object_store_specs(patterns)
            .map_err(|err| ServerError::CopyFormat(format!("read_csv: {err}")))?;
        let mut expected_header: Option<Vec<String>> = None;
        let mut readers = VecDeque::new();

        for object in objects {
            let display = object.display_uri();
            let (header, reader) = CsvReaderState::from_object(object, reject_path.is_some())?;
            validate_object_header(&header, &display)?;
            if let Some(expected) = &expected_header {
                if &header != expected {
                    return Err(ServerError::CopyFormat(format!(
                        "read_csv header mismatch in {display}"
                    )));
                }
            } else {
                expected_header = Some(header);
            }
            readers.push_back(reader);
        }

        let Some(header) = expected_header else {
            return Err(ServerError::CopyFormat(
                "read_csv object expansion returned no files".to_owned(),
            ));
        };
        let projection = CsvProjection::resolve(&header, requested)?;
        let predicate = predicate
            .map(|predicate| predicate.resolve(&header))
            .transpose()?;
        let schema = projection.schema.clone();
        Ok(Self {
            schema,
            projection,
            predicate,
            stream: CsvStream::new(readers, reject_path)?,
        })
    }
}

impl Operator for CsvTableScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        let mut columns = self.projection.new_buffers(CSV_BATCH_TARGET_ROWS);
        let mut rows = 0_usize;

        for _ in 0..CSV_BATCH_TARGET_ROWS {
            let Some(row) = self.stream.next_row(self.predicate.as_ref())? else {
                break;
            };
            self.projection.push_row(&mut columns, row)?;
            rows += 1;
        }

        if rows == 0 {
            return Ok(None);
        }
        csv_batch_from_buffers(columns).map(Some)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Debug)]
struct CsvStream {
    readers: VecDeque<CsvReaderState>,
    rejects: Option<CsvRejectSink>,
}

impl CsvStream {
    fn new(
        readers: VecDeque<CsvReaderState>,
        reject_path: Option<&Path>,
    ) -> Result<Self, ServerError> {
        Ok(Self {
            readers,
            rejects: reject_path.map(CsvRejectSink::create).transpose()?,
        })
    }

    fn next_row(
        &mut self,
        predicate: Option<&CsvPredicateEval>,
    ) -> Result<Option<CsvOutputRow>, ExecError> {
        let Self { readers, rejects } = self;
        loop {
            let Some(reader) = readers.front_mut() else {
                return Ok(None);
            };
            match reader
                .next_row(rejects.as_mut())
                .map_err(ExecError::TypeMismatch)?
            {
                Some(row) if predicate.is_none_or(|predicate| predicate.matches(&row)) => {
                    return Ok(Some(row));
                }
                Some(_) => {}
                None => {
                    readers.pop_front();
                }
            }
        }
    }
}

#[derive(Debug)]
struct CsvRejectSink {
    writer: BufWriter<File>,
}

impl CsvRejectSink {
    fn create(path: &Path) -> Result<Self, ServerError> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|err| {
                ServerError::CopyFormat(format!(
                    "read_csv cannot create reject artifact {}: {err}",
                    path.display()
                ))
            })?;
        let mut writer = BufWriter::new(file);
        writer
            .write_all(b"filename,row_number,error,raw_row\n")
            .map_err(|err| {
                ServerError::CopyFormat(format!(
                    "read_csv cannot write reject artifact {}: {err}",
                    path.display()
                ))
            })?;
        Ok(Self { writer })
    }

    fn write_reject(
        &mut self,
        filename: &str,
        row_number: i64,
        error: &str,
        raw_row: &str,
    ) -> Result<(), String> {
        let raw_row = raw_row.trim_end_matches(['\r', '\n']);
        let line = format!(
            "{},{},{},{}\n",
            csv_escape_reject_field(filename),
            row_number,
            csv_escape_reject_field(error),
            csv_escape_reject_field(raw_row)
        );
        self.writer
            .write_all(line.as_bytes())
            .and_then(|()| self.writer.flush())
            .map_err(|err| format!("read_csv cannot write reject artifact: {err}"))
    }
}

#[derive(Debug)]
struct CsvOutputRow {
    values: Vec<String>,
    filename: String,
    row_number: i64,
}

/// Simple `read_csv` predicate pushed into the streaming row reader.
#[derive(Clone, Debug)]
pub(super) struct CsvPredicate {
    column: String,
    op: BinaryOp,
    literal: CsvLiteral,
}

#[derive(Clone, Debug)]
enum CsvLiteral {
    Text(String),
    Number(f64),
}

impl CsvPredicate {
    /// Extract `column OP literal` or commuted `literal OP column`.
    pub(super) fn from_scalar(expr: &ScalarExpr) -> Option<Self> {
        let ScalarExpr::Binary {
            op, left, right, ..
        } = expr
        else {
            return None;
        };
        if !csv_cmp_supported(*op) {
            return None;
        }
        if let (Some(column), Some(literal)) = (csv_column_name(left), csv_literal(right))
            && csv_literal_supported(*op, &literal)
        {
            return Some(Self {
                column,
                op: *op,
                literal,
            });
        }
        if let (Some(literal), Some(column)) = (csv_literal(left), csv_column_name(right))
            && csv_literal_supported(*op, &literal)
        {
            return Some(Self {
                column,
                op: reverse_csv_cmp(*op),
                literal,
            });
        }
        None
    }

    fn resolve(&self, header: &[String]) -> Result<CsvPredicateEval, ServerError> {
        let full_schema = csv_schema(header)?;
        let (idx, _field) = full_schema.find(&self.column).ok_or_else(|| {
            ServerError::CopyFormat(format!(
                "read_csv predicate column not found: {}",
                self.column
            ))
        })?;
        let source = if idx < header.len() {
            CsvFilterSource::Data(idx)
        } else if idx == header.len() {
            CsvFilterSource::Filename
        } else {
            CsvFilterSource::RowNumber
        };
        Ok(CsvPredicateEval {
            source,
            op: self.op,
            literal: self.literal.clone(),
        })
    }
}

#[derive(Clone, Debug)]
struct CsvPredicateEval {
    source: CsvFilterSource,
    op: BinaryOp,
    literal: CsvLiteral,
}

impl CsvPredicateEval {
    fn matches(&self, row: &CsvOutputRow) -> bool {
        match self.source.value(row) {
            CsvFilterValue::Text(value) => self.matches_text(value),
            CsvFilterValue::Number(value) => self.matches_number(value),
        }
    }

    fn matches_text(&self, value: &str) -> bool {
        match &self.literal {
            CsvLiteral::Text(literal) if matches!(self.op, BinaryOp::Eq | BinaryOp::NotEq) => {
                let is_equal = value == literal;
                if self.op == BinaryOp::Eq {
                    is_equal
                } else {
                    !is_equal
                }
            }
            CsvLiteral::Text(_) => false,
            CsvLiteral::Number(literal) => compare_numeric_value(value, self.op, *literal),
        }
    }

    fn matches_number(&self, value: i64) -> bool {
        let Some(value) = value.to_string().parse::<f64>().ok() else {
            return false;
        };
        match &self.literal {
            CsvLiteral::Number(literal) => compare_numbers(value, self.op, *literal),
            CsvLiteral::Text(literal) if matches!(self.op, BinaryOp::Eq | BinaryOp::NotEq) => {
                let is_equal = value.to_string() == *literal;
                if self.op == BinaryOp::Eq {
                    is_equal
                } else {
                    !is_equal
                }
            }
            CsvLiteral::Text(literal) => literal
                .parse::<f64>()
                .ok()
                .is_some_and(|literal| compare_numbers(value, self.op, literal)),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum CsvFilterSource {
    Data(usize),
    Filename,
    RowNumber,
}

impl CsvFilterSource {
    fn value<'a>(self, row: &'a CsvOutputRow) -> CsvFilterValue<'a> {
        match self {
            Self::Data(idx) => CsvFilterValue::Text(row.values.get(idx).map_or("", String::as_str)),
            Self::Filename => CsvFilterValue::Text(&row.filename),
            Self::RowNumber => CsvFilterValue::Number(row.row_number),
        }
    }
}

enum CsvFilterValue<'a> {
    Text(&'a str),
    Number(i64),
}

#[derive(Debug)]
struct CsvProjection {
    schema: Schema,
    sources: Vec<CsvColumnSource>,
}

impl CsvProjection {
    fn resolve(header: &[String], requested: Option<&[String]>) -> Result<Self, ServerError> {
        let full_schema = csv_schema(header)?;
        let indices = if let Some(columns) = requested {
            columns
                .iter()
                .map(|name| {
                    full_schema.index_of(name).map_err(|err| {
                        ServerError::CopyFormat(format!("read_csv projection: {err}"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            (0..full_schema.len()).collect()
        };
        let schema = full_schema
            .project(&indices)
            .map_err(|err| ServerError::CopyFormat(format!("read_csv projection: {err}")))?;
        let sources = indices
            .into_iter()
            .map(|idx| {
                if idx < header.len() {
                    CsvColumnSource::Data(idx)
                } else if idx == header.len() {
                    CsvColumnSource::Filename
                } else {
                    CsvColumnSource::RowNumber
                }
            })
            .collect();
        Ok(Self { schema, sources })
    }

    fn new_buffers(&self, capacity: usize) -> Vec<CsvColumnBuffer> {
        self.sources
            .iter()
            .map(|source| match source {
                CsvColumnSource::Data(_) | CsvColumnSource::Filename => {
                    CsvColumnBuffer::Text(Vec::with_capacity(capacity))
                }
                CsvColumnSource::RowNumber => CsvColumnBuffer::Int64(Vec::with_capacity(capacity)),
            })
            .collect()
    }

    fn push_row(
        &self,
        buffers: &mut [CsvColumnBuffer],
        row: CsvOutputRow,
    ) -> Result<(), ExecError> {
        let CsvOutputRow {
            mut values,
            filename,
            row_number,
        } = row;
        for (source, buffer) in self.sources.iter().zip(buffers) {
            match (source, buffer) {
                (CsvColumnSource::Data(idx), CsvColumnBuffer::Text(out_values)) => {
                    let value = values
                        .get_mut(*idx)
                        .map(std::mem::take)
                        .ok_or(ExecError::Internal("csv projection index out of bounds"))?;
                    out_values.push(value);
                }
                (CsvColumnSource::Filename, CsvColumnBuffer::Text(out_values)) => {
                    out_values.push(filename.clone());
                }
                (CsvColumnSource::RowNumber, CsvColumnBuffer::Int64(out_values)) => {
                    out_values.push(row_number);
                }
                _ => return Err(ExecError::Internal("csv projection buffer type mismatch")),
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum CsvColumnSource {
    Data(usize),
    Filename,
    RowNumber,
}

#[derive(Debug)]
enum CsvColumnBuffer {
    Text(Vec<String>),
    Int64(Vec<i64>),
}

#[derive(Debug)]
struct CsvReaderState {
    display: String,
    width: usize,
    reader: CsvRecordReader,
    row_number: i64,
}

impl CsvReaderState {
    fn from_path(path: &Path, allow_rejects: bool) -> Result<(Vec<String>, Self), ServerError> {
        let display = path.display().to_string();
        let sample = read_csv_sample_from_path(path)?;
        let sniff = sniff_csv_text_for_read_csv(&display, &sample, allow_rejects)?;
        let header = sniff_header(&sniff);
        let file = open_csv_input_file(path)?;
        let reader = CsvRecordReader::new(
            display.clone(),
            CsvRecordSource::File(BufReader::new(file)),
            sniff.parse_options(),
        );
        let mut state = Self {
            display,
            width: header.len(),
            reader,
            row_number: 0,
        };
        if sniff.has_header {
            state.skip_header(&header)?;
        }
        Ok((header, state))
    }

    fn from_object(
        object: ObjectLocation,
        allow_rejects: bool,
    ) -> Result<(Vec<String>, Self), ServerError> {
        let display = object.display_uri();
        let sample = read_csv_sample_from_object(&object, &display)?;
        let sniff = sniff_csv_text_for_read_csv(&display, &sample, allow_rejects)?;
        let header = sniff_header(&sniff);
        let reader = CsvRecordReader::new(
            display.clone(),
            CsvRecordSource::Object(ObjectRangeReader::new(object)),
            sniff.parse_options(),
        );
        let mut state = Self {
            display,
            width: header.len(),
            reader,
            row_number: 0,
        };
        if sniff.has_header {
            state.skip_header(&header)?;
        }
        Ok((header, state))
    }

    fn skip_header(&mut self, expected: &[String]) -> Result<(), ServerError> {
        let Some(record) = self
            .reader
            .next_record()
            .map_err(|err| ServerError::CopyFormat(err.message))?
        else {
            return Err(ServerError::CopyFormat(format!(
                "read_csv header missing in {}",
                self.display
            )));
        };
        if record.values != expected {
            return Err(ServerError::CopyFormat(format!(
                "read_csv header mismatch in {}",
                self.display
            )));
        }
        Ok(())
    }

    fn next_row(
        &mut self,
        mut rejects: Option<&mut CsvRejectSink>,
    ) -> Result<Option<CsvOutputRow>, String> {
        loop {
            let record = match self.reader.next_record() {
                Ok(Some(record)) => record,
                Ok(None) => return Ok(None),
                Err(err) => {
                    let row_number = self.next_physical_row_number()?;
                    if let Some(rejects) = rejects.as_deref_mut() {
                        rejects.write_reject(&self.display, row_number, &err.message, &err.raw)?;
                        continue;
                    }
                    return Err(err.message);
                }
            };
            let row_number = self.next_physical_row_number()?;
            if record.values.len() != self.width {
                let message = format!(
                    "read_csv row {row_number} in {} has {} columns, expected {}",
                    self.display,
                    record.values.len(),
                    self.width
                );
                if let Some(rejects) = rejects.as_deref_mut() {
                    rejects.write_reject(&self.display, row_number, &message, &record.raw)?;
                    continue;
                }
                return Err(message);
            }
            return Ok(Some(CsvOutputRow {
                values: record.values,
                filename: self.display.clone(),
                row_number,
            }));
        }
    }

    fn next_physical_row_number(&mut self) -> Result<i64, String> {
        let row_number = self
            .row_number
            .checked_add(1)
            .ok_or_else(|| format!("read_csv row number overflow in {}", self.display))?;
        self.row_number = row_number;
        Ok(row_number)
    }
}

#[derive(Debug)]
struct CsvRecord {
    values: Vec<String>,
    raw: String,
}

#[derive(Debug)]
struct CsvRowError {
    message: String,
    raw: String,
}

#[derive(Debug)]
struct CsvRecordReader {
    display: String,
    source: CsvRecordSource,
    options: CsvParseOptions,
}

impl CsvRecordReader {
    fn new(display: String, source: CsvRecordSource, options: CsvParseOptions) -> Self {
        Self {
            display,
            source,
            options,
        }
    }

    fn next_record(&mut self) -> Result<Option<CsvRecord>, CsvRowError> {
        let mut buffer = String::new();
        loop {
            let mut line = String::new();
            let bytes = self
                .source
                .read_line(&mut line)
                .map_err(|err| CsvRowError {
                    message: format!("read_csv cannot read {}: {err}", self.display),
                    raw: buffer.clone(),
                })?;
            if bytes == 0 {
                if buffer.is_empty() {
                    return Ok(None);
                }
                return self.parse_buffer(buffer);
            }
            buffer.push_str(&line);
            match self.parse_complete_record(&buffer) {
                Ok(Some(values)) => {
                    return Ok(Some(CsvRecord {
                        values,
                        raw: buffer,
                    }));
                }
                Ok(None) => {
                    buffer.clear();
                }
                Err(err) if err.contains("unterminated quoted field") => {}
                Err(message) => {
                    return Err(CsvRowError {
                        message,
                        raw: buffer,
                    });
                }
            }
        }
    }

    fn parse_buffer(&self, buffer: String) -> Result<Option<CsvRecord>, CsvRowError> {
        match self.parse_complete_record(&buffer) {
            Ok(Some(values)) => Ok(Some(CsvRecord {
                values,
                raw: buffer,
            })),
            Ok(None) => Ok(None),
            Err(message) => Err(CsvRowError {
                message,
                raw: buffer,
            }),
        }
    }

    fn parse_complete_record(&self, text: &str) -> Result<Option<Vec<String>>, String> {
        let mut records = parse_csv_records_with_options(text, self.options)
            .map_err(|err| format!("read_csv parse {}: {err}", self.display))?;
        match records.len() {
            0 => Ok(None),
            1 => Ok(records.pop()),
            _ => Err(format!(
                "read_csv parse {}: streaming buffer produced multiple records",
                self.display
            )),
        }
    }
}

#[derive(Debug)]
enum CsvRecordSource {
    File(BufReader<File>),
    Object(ObjectRangeReader),
}

impl CsvRecordSource {
    fn read_line(&mut self, buffer: &mut String) -> std::io::Result<usize> {
        match self {
            Self::File(reader) => reader.read_line(buffer),
            Self::Object(reader) => read_utf8_line(reader, buffer),
        }
    }
}

fn read_utf8_line(reader: &mut dyn BufRead, buffer: &mut String) -> std::io::Result<usize> {
    let mut bytes = Vec::new();
    let read = reader.read_until(b'\n', &mut bytes)?;
    if read == 0 {
        return Ok(0);
    }
    let text = String::from_utf8(bytes)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    buffer.push_str(&text);
    Ok(read)
}

fn csv_escape_reject_field(value: &str) -> String {
    if !value
        .chars()
        .any(|ch| matches!(ch, ',' | '"' | '\n' | '\r'))
    {
        return value.to_owned();
    }
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for ch in value.chars() {
        if ch == '"' {
            escaped.push('"');
        }
        escaped.push(ch);
    }
    escaped.push('"');
    escaped
}

fn csv_batch_from_buffers(buffers: Vec<CsvColumnBuffer>) -> Result<Batch, ExecError> {
    let mut columns = Vec::with_capacity(buffers.len());
    for buffer in buffers {
        match buffer {
            CsvColumnBuffer::Text(values) => {
                columns.push(Column::Utf8(StringColumn::from_data(values)));
            }
            CsvColumnBuffer::Int64(values) => {
                columns.push(Column::Int64(NumericColumn::from_data(values)));
            }
        }
    }
    Batch::new(columns).map_err(ExecError::from)
}

fn csv_column_name(expr: &ScalarExpr) -> Option<String> {
    match expr {
        ScalarExpr::Column { name, .. } => Some(name.clone()),
        _ => None,
    }
}

fn csv_literal(expr: &ScalarExpr) -> Option<CsvLiteral> {
    match expr {
        ScalarExpr::Literal {
            value: Value::Int16(value),
            ..
        } => Some(CsvLiteral::Number(f64::from(*value))),
        ScalarExpr::Literal {
            value: Value::Int32(value),
            ..
        } => Some(CsvLiteral::Number(f64::from(*value))),
        ScalarExpr::Literal {
            value: Value::Int64(value),
            ..
        } => value
            .to_string()
            .parse::<f64>()
            .ok()
            .map(CsvLiteral::Number),
        ScalarExpr::Literal {
            value: Value::Float32(value),
            ..
        } => Some(CsvLiteral::Number(f64::from(*value))),
        ScalarExpr::Literal {
            value: Value::Float64(value),
            ..
        } => Some(CsvLiteral::Number(*value)),
        ScalarExpr::Literal {
            value: Value::Text(value),
            ..
        } => Some(CsvLiteral::Text(value.clone())),
        _ => None,
    }
}

fn csv_cmp_supported(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
    )
}

fn csv_literal_supported(op: BinaryOp, literal: &CsvLiteral) -> bool {
    match literal {
        CsvLiteral::Text(_) if matches!(op, BinaryOp::Eq | BinaryOp::NotEq) => true,
        CsvLiteral::Text(_) => false,
        CsvLiteral::Number(value) => value.is_finite(),
    }
}

fn reverse_csv_cmp(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

fn compare_numeric_value(value: &str, op: BinaryOp, literal: f64) -> bool {
    value
        .parse::<f64>()
        .ok()
        .is_some_and(|value| compare_numbers(value, op, literal))
}

fn compare_numbers(left: f64, op: BinaryOp, right: f64) -> bool {
    if !left.is_finite() || !right.is_finite() {
        return false;
    }
    match op {
        BinaryOp::Eq => left.total_cmp(&right) == Ordering::Equal,
        BinaryOp::NotEq => left.total_cmp(&right) != Ordering::Equal,
        BinaryOp::Lt => left < right,
        BinaryOp::LtEq => left <= right,
        BinaryOp::Gt => left > right,
        BinaryOp::GtEq => left >= right,
        _ => false,
    }
}

fn sniff_header(sniff: &CsvSniff) -> Vec<String> {
    sniff
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect()
}

fn sniff_csv_text_for_read_csv(
    display: &str,
    sample: &str,
    allow_rejects: bool,
) -> Result<CsvSniff, ServerError> {
    match sniff_csv_text(display, sample) {
        Ok(sniff) => Ok(sniff),
        Err(err) if allow_rejects => sniff_csv_header_only(display, sample).map_err(|fallback| {
            ServerError::CopyFormat(format!(
                "read_csv sniff {display}: {err}; header fallback failed: {fallback}"
            ))
        }),
        Err(err) => Err(ServerError::CopyFormat(format!(
            "read_csv sniff {display}: {err}"
        ))),
    }
}

fn sniff_csv_header_only(display: &str, sample: &str) -> Result<CsvSniff, String> {
    let mut best: Option<(CsvParseOptions, Vec<String>)> = None;
    let mut last_error: Option<String> = None;
    for delimiter in [',', ';', '\t', '|'] {
        let options = CsvParseOptions {
            delimiter,
            quote: Some('"'),
            escape: Some('"'),
        };
        match first_csv_record_with_options(display, sample, options) {
            Ok(record)
                if best
                    .as_ref()
                    .is_none_or(|(_, best)| record.len() > best.len()) =>
            {
                best = Some((options, record));
            }
            Ok(_) => {}
            Err(err) => last_error = Some(err),
        }
    }
    let Some((options, header)) = best else {
        return Err(last_error.unwrap_or_else(|| "read_csv header missing".to_owned()));
    };
    if header.is_empty() || header.iter().any(String::is_empty) {
        return Err("read_csv header contains an empty column name".to_owned());
    }
    let columns = header
        .into_iter()
        .map(|name| CsvSniffColumn {
            name,
            data_type: CsvInferredType::Text,
        })
        .collect();
    Ok(CsvSniff {
        path: display.to_owned(),
        delimiter: options.delimiter,
        quote: options.quote,
        escape: options.escape,
        newline: detect_sample_newline(sample).to_owned(),
        has_header: true,
        columns,
    })
}

fn first_csv_record_with_options(
    display: &str,
    sample: &str,
    options: CsvParseOptions,
) -> Result<Vec<String>, String> {
    let mut buffer = String::new();
    for line in sample.split_inclusive('\n') {
        buffer.push_str(line);
        match parse_csv_records_with_options(&buffer, options) {
            Ok(mut records) if records.len() == 1 => {
                return Ok(records.pop().expect("records length checked"));
            }
            Ok(records) if records.is_empty() => buffer.clear(),
            Ok(_) => {
                return Err(format!(
                    "read_csv parse {display}: first-record buffer produced multiple records"
                ));
            }
            Err(err) if err.to_string().contains("unterminated quoted field") => {}
            Err(err) => return Err(format!("read_csv parse {display}: {err}")),
        }
    }
    if buffer.is_empty() {
        return Err("read_csv header missing".to_owned());
    }
    let mut records = parse_csv_records_with_options(&buffer, options)
        .map_err(|err| format!("read_csv parse {display}: {err}"))?;
    if records.len() == 1 {
        Ok(records.pop().expect("records length checked"))
    } else {
        Err(format!(
            "read_csv parse {display}: first-record buffer produced {} records",
            records.len()
        ))
    }
}

fn detect_sample_newline(sample: &str) -> &'static str {
    if sample.contains("\r\n") {
        "\\r\\n"
    } else if sample.contains('\r') {
        "\\r"
    } else {
        "\\n"
    }
}

fn read_csv_sample_from_path(path: &Path) -> Result<String, ServerError> {
    let file = open_csv_input_file(path)?;
    let mut bytes = Vec::new();
    file.take(CSV_SNIFF_SAMPLE_BYTES_U64)
        .read_to_end(&mut bytes)
        .map_err(|err| {
            ServerError::CopyFormat(format!("read_csv cannot read {}: {err}", path.display()))
        })?;
    let valid_len = match std::str::from_utf8(&bytes) {
        Ok(text) => {
            return Ok(csv_sample_from_text_with_truncation(
                text,
                bytes.len() == CSV_SNIFF_SAMPLE_BYTES,
            )
            .to_owned());
        }
        Err(err) => err.valid_up_to(),
    };
    if valid_len == 0 {
        return Err(ServerError::CopyFormat(format!(
            "read_csv cannot decode {}: invalid UTF-8",
            path.display()
        )));
    }
    let text = std::str::from_utf8(&bytes[..valid_len]).map_err(|err| {
        ServerError::CopyFormat(format!("read_csv cannot decode {}: {err}", path.display()))
    })?;
    Ok(csv_sample_from_text_with_truncation(text, true).to_owned())
}

fn open_csv_input_file(path: &Path) -> Result<File, ServerError> {
    let metadata = fs::symlink_metadata(path).map_err(|err| {
        ServerError::CopyFormat(format!("read_csv cannot inspect {}: {err}", path.display()))
    })?;
    if !metadata.file_type().is_file() {
        return Err(ServerError::CopyFormat(format!(
            "read_csv path is not a regular file: {}",
            path.display()
        )));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path).map_err(|err| {
        ServerError::CopyFormat(format!("read_csv cannot open {}: {err}", path.display()))
    })
}

fn read_csv_sample_from_object(
    object: &ObjectLocation,
    display: &str,
) -> Result<String, ServerError> {
    let range = read_object_range_with_metadata(object, 0, CSV_SNIFF_SAMPLE_BYTES_U64)
        .map_err(|err| ServerError::CopyFormat(format!("read_csv: {err}")))?;
    let bytes = range.bytes();
    let valid_len = match std::str::from_utf8(bytes) {
        Ok(text) => {
            return Ok(csv_sample_from_text_with_truncation(
                text,
                bytes.len() == CSV_SNIFF_SAMPLE_BYTES,
            )
            .to_owned());
        }
        Err(err) => err.valid_up_to(),
    };
    if valid_len == 0 {
        return Err(ServerError::CopyFormat(format!(
            "read_csv cannot decode {display}: invalid UTF-8"
        )));
    }
    let text = std::str::from_utf8(&bytes[..valid_len]).map_err(|err| {
        ServerError::CopyFormat(format!("read_csv cannot decode {display}: {err}"))
    })?;
    Ok(csv_sample_from_text_with_truncation(text, true).to_owned())
}

fn csv_sample_from_text_with_truncation(text: &str, truncated: bool) -> &str {
    if !truncated {
        return text;
    }
    let Some((idx, ch)) = text
        .char_indices()
        .rev()
        .find(|(_, ch)| *ch == '\n' || *ch == '\r')
    else {
        return text;
    };
    let end = idx + ch.len_utf8();
    let trimmed = &text[..end];
    if trimmed.is_empty() { text } else { trimmed }
}

fn csv_schema(header: &[String]) -> Result<Schema, ServerError> {
    let mut fields = header
        .iter()
        .cloned()
        .map(|name| Field::nullable(name, DataType::Text { max_len: None }))
        .collect::<Vec<_>>();
    fields.push(Field::nullable(
        "_filename",
        DataType::Text { max_len: None },
    ));
    fields.push(Field::required("_row_number", DataType::Int64));
    Schema::new(fields).map_err(|err| ServerError::CopyFormat(format!("read_csv schema: {err}")))
}

fn validate_header(header: &[String], path: &std::path::Path) -> Result<(), ServerError> {
    if header.is_empty() || header.iter().any(String::is_empty) {
        return Err(ServerError::CopyFormat(format!(
            "read_csv header contains an empty column name: {}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_object_header(header: &[String], display: &str) -> Result<(), ServerError> {
    if header.is_empty() || header.iter().any(String::is_empty) {
        return Err(ServerError::CopyFormat(format!(
            "read_csv header contains an empty column name: {display}"
        )));
    }
    Ok(())
}

fn path_specs_use_object_store(
    function_name: &str,
    patterns: &[String],
) -> Result<bool, ServerError> {
    let object_count = patterns
        .iter()
        .filter(|pattern| is_object_store_uri(pattern))
        .count();
    if object_count == 0 {
        return Ok(false);
    }
    if object_count == patterns.len() {
        return Ok(true);
    }
    Err(ServerError::CopyFormat(format!(
        "{function_name}: cannot mix local and object-store paths"
    )))
}

/// Single-row scan for `sniff_csv(path)`.
#[derive(Debug)]
pub(super) struct CsvSniffScan {
    schema: Schema,
    sniff: Option<CsvSniff>,
}

impl CsvSniffScan {
    /// Sniff one CSV file into a query-local one-row result.
    pub(super) fn from_path(path: &str) -> Result<Self, ServerError> {
        let sniff = sniff_csv_path(std::path::Path::new(path))
            .map_err(|err| ServerError::CopyFormat(format!("{err}")))?;
        Ok(Self {
            schema: sniff_csv_schema()?,
            sniff: Some(sniff),
        })
    }
}

impl Operator for CsvSniffScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        let Some(sniff) = self.sniff.take() else {
            return Ok(None);
        };
        Ok(Some(sniff_csv_batch(&sniff)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn sniff_csv_schema() -> Result<Schema, ServerError> {
    Schema::new([
        Field::nullable("Delimiter", DataType::Text { max_len: None }),
        Field::nullable("Quote", DataType::Text { max_len: None }),
        Field::nullable("Escape", DataType::Text { max_len: None }),
        Field::nullable("NewLineDelimiter", DataType::Text { max_len: None }),
        Field::required("SkipRows", DataType::Int64),
        Field::required("HasHeader", DataType::Bool),
        Field::nullable("Columns", DataType::Text { max_len: None }),
        Field::nullable("DateFormat", DataType::Text { max_len: None }),
        Field::nullable("TimestampFormat", DataType::Text { max_len: None }),
        Field::nullable("UserArguments", DataType::Text { max_len: None }),
        Field::nullable("Prompt", DataType::Text { max_len: None }),
    ])
    .map_err(|err| ServerError::CopyFormat(format!("sniff_csv schema: {err}")))
}

fn sniff_csv_batch(sniff: &CsvSniff) -> Result<Batch, ExecError> {
    Batch::new([
        Column::Utf8(StringColumn::from_data(vec![sniff.delimiter_text()])),
        Column::Utf8(StringColumn::from_data(vec![sniff.quote_text()])),
        Column::Utf8(StringColumn::from_data(vec![sniff.escape_text()])),
        Column::Utf8(StringColumn::from_data(vec![sniff.newline.clone()])),
        Column::Int64(NumericColumn::from_data(vec![0_i64])),
        Column::Bool(BoolColumn::from_data(vec![sniff.has_header])),
        Column::Utf8(StringColumn::from_data(vec![sniff.columns_sql()])),
        Column::Utf8(StringColumn::from_data(vec![String::new()])),
        Column::Utf8(StringColumn::from_data(vec![String::new()])),
        Column::Utf8(StringColumn::from_data(vec![String::new()])),
        Column::Utf8(StringColumn::from_data(vec![sniff.prompt_sql()])),
    ])
    .map_err(ExecError::from)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write as _;

    use ultrasql_executor::Operator as _;

    use super::{CSV_BATCH_TARGET_ROWS, CsvRejectSink, CsvTableScan, read_csv_sample_from_path};

    #[test]
    fn csv_scan_construction_does_not_parse_past_first_batch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("stream.csv");
        let mut file = fs::File::create(&csv_path).expect("create csv");
        writeln!(file, "id,payload").expect("write header");
        for row in 0..CSV_BATCH_TARGET_ROWS {
            writeln!(file, "{row},{}", "x".repeat(48)).expect("write row");
        }
        writeln!(file, "bad,\"unterminated").expect("write malformed tail");

        let mut scan = CsvTableScan::from_pattern(csv_path.to_str().expect("utf8 path"))
            .expect("constructs without reading malformed tail");
        let first = scan
            .next_batch()
            .expect("first batch reads")
            .expect("first batch exists");
        assert_eq!(first.rows(), CSV_BATCH_TARGET_ROWS);
    }

    #[test]
    fn csv_scan_yields_target_sized_batches_lazily() {
        let dir = tempfile::tempdir().expect("tempdir");
        let csv_path = dir.path().join("batches.csv");
        let mut file = fs::File::create(&csv_path).expect("create csv");
        writeln!(file, "id,payload").expect("write header");
        for row in 0..=CSV_BATCH_TARGET_ROWS {
            writeln!(file, "{row},value-{row}").expect("write row");
        }

        let mut scan = CsvTableScan::from_pattern(csv_path.to_str().expect("utf8 path"))
            .expect("construct scan");
        let first = scan
            .next_batch()
            .expect("first batch reads")
            .expect("first batch exists");
        let second = scan
            .next_batch()
            .expect("second batch reads")
            .expect("second batch exists");
        assert_eq!(first.rows(), CSV_BATCH_TARGET_ROWS);
        assert_eq!(second.rows(), 1);
        assert!(scan.next_batch().expect("eof reads").is_none());
    }

    #[cfg(unix)]
    #[test]
    fn csv_scan_rejects_symlinked_input_and_reject_sink() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let input_target = dir.path().join("input.csv");
        let input_link = dir.path().join("input-link.csv");
        fs::write(&input_target, "id,name\n1,Ada\n").expect("write input");
        symlink(&input_target, &input_link).expect("input symlink");

        assert!(read_csv_sample_from_path(&input_link).is_err());

        let reject_target = dir.path().join("rejects.csv");
        let reject_link = dir.path().join("rejects-link.csv");
        fs::write(&reject_target, "keep").expect("write reject target");
        symlink(&reject_target, &reject_link).expect("reject symlink");

        assert!(CsvRejectSink::create(&reject_link).is_err());
        assert_eq!(
            fs::read_to_string(&reject_target).expect("reject target"),
            "keep"
        );
    }
}
