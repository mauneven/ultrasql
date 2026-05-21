//! Parquet table-function scan for local files and object-store URIs.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    mpsc::{Receiver, SyncSender, sync_channel},
};
use std::thread;

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray,
};
use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use bytes::Bytes;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowFilter,
};
use parquet::basic::{Encoding, Type as ParquetPhysicalType};
use parquet::column::page::Page;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{ChunkReader, Length, SerializedPageReader};
use parquet::file::statistics::Statistics;
use ultrasql_arrow::record_batch_to_ultrasql_batch;
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_executor::{ExecError, Operator};
use ultrasql_objectstore::{
    ObjectLocation, expand_object_store_specs, is_object_store_uri, read_object_range,
    read_object_range_with_metadata,
};
use ultrasql_planner::{BinaryOp, LogicalPlan, ScalarExpr};

use crate::error::ServerError;

const PARQUET_BATCH_TARGET_ROWS: usize = 4096;
const PARQUET_MAX_ROW_GROUP_WORKERS: usize = 8;

/// Row-group pruning evidence for `read_parquet` scans.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ParquetRowGroupSummary {
    pub(crate) scanned: u64,
    pub(crate) skipped: u64,
}

impl ParquetRowGroupSummary {
    fn add(&mut self, other: Self) {
        self.scanned = self.scanned.saturating_add(other.scanned);
        self.skipped = self.skipped.saturating_add(other.skipped);
    }
}

/// File-backed scan for `read_parquet(path_or_glob)`.
#[derive(Debug)]
pub(super) struct ParquetTableScan {
    schema: Schema,
    expected_arrow_schema: ArrowSchemaRef,
    projection: Option<Vec<String>>,
    predicate: Option<ParquetPredicate>,
    sources: VecDeque<ParquetScanSource>,
    active: Option<ActiveParquetReader>,
}

impl ParquetTableScan {
    /// Load Parquet files from one or more path/glob specs into a
    /// query-local scan.
    pub(super) fn from_path_specs(
        patterns: &[String],
        projection: Option<&[String]>,
        predicate: Option<&ParquetPredicate>,
    ) -> Result<Self, ServerError> {
        if path_specs_use_object_store("read_parquet", patterns)? {
            return Self::from_object_specs(patterns, projection, predicate);
        }
        let paths = expand_parquet_path_specs(patterns)?;
        Self::from_paths(paths, projection, predicate)
    }

    fn from_object_specs(
        patterns: &[String],
        projection: Option<&[String]>,
        predicate: Option<&ParquetPredicate>,
    ) -> Result<Self, ServerError> {
        let objects = expand_object_store_specs(patterns)
            .map_err(|err| ServerError::CopyFormat(format!("read_parquet: {err}")))?;
        let Some(first_object) = objects.first() else {
            return Err(ServerError::CopyFormat(
                "read_parquet path list cannot be empty".to_owned(),
            ));
        };
        let base_arrow_schema = read_object_arrow_schema(first_object)?;
        let projection = resolve_projection_names(base_arrow_schema.as_ref(), projection)?;
        let predicate = predicate
            .map(|p| p.resolved_for_schema(base_arrow_schema.as_ref()))
            .transpose()?;
        let schema = parquet_schema_to_ultrasql(base_arrow_schema.as_ref(), projection.as_deref())?;
        let mut sources = VecDeque::new();

        for object in objects {
            sources.push_back(ParquetScanSource::Object(object));
        }

        Ok(Self {
            schema,
            expected_arrow_schema: base_arrow_schema,
            projection,
            predicate,
            sources,
            active: None,
        })
    }

    fn from_paths(
        paths: Vec<PathBuf>,
        projection: Option<&[String]>,
        predicate: Option<&ParquetPredicate>,
    ) -> Result<Self, ServerError> {
        let Some(first_path) = paths.first() else {
            return Err(ServerError::CopyFormat(
                "read_parquet path list cannot be empty".to_owned(),
            ));
        };
        let base_arrow_schema = read_arrow_schema(first_path)?;
        let projection = resolve_projection_names(base_arrow_schema.as_ref(), projection)?;
        let predicate = predicate
            .map(|p| p.resolved_for_schema(base_arrow_schema.as_ref()))
            .transpose()?;
        let schema = parquet_schema_to_ultrasql(base_arrow_schema.as_ref(), projection.as_deref())?;
        let mut sources = VecDeque::new();

        for path in paths {
            let arrow_schema = read_arrow_schema(&path)?;
            if arrow_schema.as_ref() != base_arrow_schema.as_ref() {
                return Err(ServerError::CopyFormat(format!(
                    "read_parquet schema mismatch in {}",
                    path.display()
                )));
            }
            sources.push_back(ParquetScanSource::Path(path));
        }

        Ok(Self {
            schema,
            expected_arrow_schema: base_arrow_schema,
            projection,
            predicate,
            sources,
            active: None,
        })
    }
}

impl Operator for ParquetTableScan {
    fn next_batch(&mut self) -> Result<Option<ultrasql_vec::Batch>, ExecError> {
        loop {
            if self.active.is_none() && !self.open_next_reader()? {
                return Ok(None);
            }
            let Some(active) = &mut self.active else {
                return Ok(None);
            };
            match active.next_batch() {
                Ok(Some(batch)) => return Ok(Some(batch)),
                Ok(None) => self.active = None,
                Err(err) => {
                    self.active = None;
                    return Err(err);
                }
            }
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl ParquetTableScan {
    fn open_next_reader(&mut self) -> Result<bool, ExecError> {
        while let Some(source) = self.sources.pop_front() {
            let reader = match source {
                ParquetScanSource::Path(path) => open_path_reader(
                    &path,
                    self.expected_arrow_schema.as_ref(),
                    self.projection.as_deref(),
                    self.predicate.as_ref(),
                ),
                ParquetScanSource::Object(object) => open_object_reader(
                    &object,
                    self.expected_arrow_schema.as_ref(),
                    self.projection.as_deref(),
                    self.predicate.as_ref(),
                ),
            }
            .map_err(server_error_to_exec)?;
            if let Some(reader) = reader {
                self.active = Some(reader);
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[derive(Debug)]
enum ParquetScanSource {
    Path(PathBuf),
    Object(ObjectLocation),
}

#[derive(Debug)]
struct ActiveParquetReader {
    display: String,
    receiver: Option<Receiver<ParquetWorkerMessage>>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl ActiveParquetReader {
    fn new(
        display: String,
        receiver: Receiver<ParquetWorkerMessage>,
        workers: Vec<thread::JoinHandle<()>>,
    ) -> Self {
        Self {
            display,
            receiver: Some(receiver),
            workers,
        }
    }

    fn next_batch(&mut self) -> Result<Option<ultrasql_vec::Batch>, ExecError> {
        let receiver = self
            .receiver
            .as_ref()
            .ok_or(ExecError::Internal("parquet worker receiver missing"))?;
        match receiver.recv() {
            Ok(Ok(batch)) => Ok(Some(batch)),
            Ok(Err(err)) => {
                let _ = self.finish_workers();
                Err(ExecError::TypeMismatch(err))
            }
            Err(_) => {
                self.finish_workers()?;
                Ok(None)
            }
        }
    }

    fn finish_workers(&mut self) -> Result<(), ExecError> {
        self.receiver.take();
        for worker in self.workers.drain(..) {
            if worker.join().is_err() {
                return Err(ExecError::TypeMismatch(format!(
                    "read_parquet worker for {} panicked",
                    self.display
                )));
            }
        }
        Ok(())
    }
}

impl Drop for ActiveParquetReader {
    fn drop(&mut self) {
        let _ = self.finish_workers();
    }
}

type ParquetWorkerMessage = Result<ultrasql_vec::Batch, String>;

#[derive(Clone, Debug)]
enum ParquetWorkerSource {
    Path(PathBuf),
    Object(ObjectRangeChunkReader),
}

#[derive(Clone, Debug)]
struct ObjectRangeChunkReader {
    location: ObjectLocation,
    display: String,
    len: u64,
}

impl ObjectRangeChunkReader {
    fn new(location: ObjectLocation) -> Result<Self, ServerError> {
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
struct ObjectRangeReadCursor {
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

/// Predicate shape that can be pushed into a Parquet scan.
#[derive(Clone, Debug)]
pub(super) struct ParquetPredicate {
    column: String,
    op: BinaryOp,
    literal: ParquetLiteral,
}

#[derive(Clone, Debug)]
enum ParquetLiteral {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
}

impl ParquetPredicate {
    /// Extract a simple `column OP literal` predicate.
    pub(super) fn from_scalar(expr: &ScalarExpr) -> Option<Self> {
        let ScalarExpr::Binary {
            op, left, right, ..
        } = expr
        else {
            return None;
        };
        if !is_supported_cmp(*op) {
            return None;
        }
        if let (Some(column), Some(literal)) = (column_name(left), literal_value(right)) {
            return Some(Self {
                column,
                op: *op,
                literal,
            });
        }
        if let (Some(literal), Some(column)) = (literal_value(left), column_name(right)) {
            return Some(Self {
                column,
                op: reverse_cmp(*op),
                literal,
            });
        }
        None
    }

    fn resolved_for_schema(&self, schema: &ArrowSchema) -> Result<Self, ServerError> {
        let field = schema
            .fields()
            .iter()
            .find(|field| field.name().eq_ignore_ascii_case(&self.column))
            .ok_or_else(|| {
                ServerError::CopyFormat(format!(
                    "read_parquet predicate column not found: {}",
                    self.column
                ))
            })?;
        Ok(Self {
            column: field.name().clone(),
            op: self.op,
            literal: self.literal.clone(),
        })
    }

    fn row_filter(&self, parquet_schema: &parquet::schema::types::SchemaDescriptor) -> RowFilter {
        let column = self.column.clone();
        let op = self.op;
        let literal = self.literal.clone();
        let projection = ProjectionMask::columns(parquet_schema, [column.as_str()]);
        let predicate = ArrowPredicateFn::new(projection, move |batch: RecordBatch| {
            let array = batch.column(0).as_ref();
            evaluate_arrow_predicate(array, op, &literal)
        });
        RowFilter::new(vec![Box::new(predicate)])
    }
}

fn open_path_reader(
    path: &Path,
    expected_schema: &ArrowSchema,
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
) -> Result<Option<ActiveParquetReader>, ServerError> {
    let file = File::open(path).map_err(|err| {
        ServerError::CopyFormat(format!(
            "read_parquet cannot open {}: {err}",
            path.display()
        ))
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
        ServerError::CopyFormat(format!(
            "read_parquet cannot inspect {}: {err}",
            path.display()
        ))
    })?;
    if builder.schema().as_ref() != expected_schema {
        return Err(ServerError::CopyFormat(format!(
            "read_parquet schema mismatch in {}",
            path.display()
        )));
    }

    let row_group_reader = File::open(path).map_err(|err| {
        ServerError::CopyFormat(format!(
            "read_parquet cannot open {} for pruning: {err}",
            path.display()
        ))
    })?;
    let row_groups = selected_row_groups_with_dictionary(
        Arc::new(row_group_reader),
        builder.metadata(),
        expected_schema,
        predicate,
    )?;
    if row_groups.is_empty() {
        return Ok(None);
    }
    let display = path.display().to_string();
    spawn_parquet_row_group_workers(
        display,
        ParquetWorkerSource::Path(path.to_path_buf()),
        projection,
        predicate,
        &row_groups,
    )
    .map(Some)
}

fn open_object_reader(
    object: &ObjectLocation,
    expected_schema: &ArrowSchema,
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
) -> Result<Option<ActiveParquetReader>, ServerError> {
    let display = object.display_uri();
    let reader = ObjectRangeChunkReader::new(object.clone())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader.clone()).map_err(|err| {
        ServerError::CopyFormat(format!("read_parquet cannot inspect {display}: {err}"))
    })?;
    if builder.schema().as_ref() != expected_schema {
        return Err(ServerError::CopyFormat(format!(
            "read_parquet schema mismatch in {display}"
        )));
    }

    let row_groups = selected_row_groups_with_dictionary(
        Arc::new(reader.clone()),
        builder.metadata(),
        expected_schema,
        predicate,
    )?;
    if row_groups.is_empty() {
        return Ok(None);
    }
    spawn_parquet_row_group_workers(
        display,
        ParquetWorkerSource::Object(reader),
        projection,
        predicate,
        &row_groups,
    )
    .map(Some)
}

fn spawn_parquet_row_group_workers(
    display: String,
    source: ParquetWorkerSource,
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
    row_groups: &[usize],
) -> Result<ActiveParquetReader, ServerError> {
    let chunks = split_row_groups(row_groups);
    let channel_bound = chunks.len().saturating_mul(2).max(1);
    let (sender, receiver) = sync_channel(channel_bound);
    let projection = projection.map(<[String]>::to_vec);
    let predicate = predicate.cloned();
    let mut workers = Vec::with_capacity(chunks.len());
    for (idx, chunk) in chunks.into_iter().enumerate() {
        let worker_display = display.clone();
        let worker_source = source.clone();
        let worker_projection = projection.clone();
        let worker_predicate = predicate.clone();
        let worker_sender = sender.clone();
        let worker = thread::Builder::new()
            .name(format!("ultrasql-parquet-rg-{idx}"))
            .spawn(move || {
                run_parquet_worker(
                    worker_source,
                    worker_display,
                    worker_projection.as_deref(),
                    worker_predicate.as_ref(),
                    chunk,
                    worker_sender,
                );
            })
            .map_err(|err| {
                ServerError::CopyFormat(format!(
                    "read_parquet cannot spawn row-group worker for {display}: {err}"
                ))
            })?;
        workers.push(worker);
    }
    drop(sender);
    Ok(ActiveParquetReader::new(display, receiver, workers))
}

fn run_parquet_worker(
    source: ParquetWorkerSource,
    display: String,
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
    row_groups: Vec<usize>,
    sender: SyncSender<ParquetWorkerMessage>,
) {
    if let Err(err) =
        read_parquet_row_groups(source, &display, projection, predicate, row_groups, &sender)
    {
        let _ = sender.send(Err(err.to_string()));
    }
}

fn read_parquet_row_groups(
    source: ParquetWorkerSource,
    display: &str,
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
    row_groups: Vec<usize>,
    sender: &SyncSender<ParquetWorkerMessage>,
) -> Result<(), ServerError> {
    let mut reader = build_row_group_reader(source, display, projection, predicate, row_groups)?;
    for batch in &mut reader {
        let batch = batch.map_err(|err| {
            ServerError::CopyFormat(format!("read_parquet read {display}: {err}"))
        })?;
        if batch.num_rows() == 0 {
            continue;
        }
        let batch = arrow_record_batch_to_ultrasql(batch)?;
        if sender.send(Ok(batch)).is_err() {
            break;
        }
    }
    Ok(())
}

fn build_row_group_reader(
    source: ParquetWorkerSource,
    display: &str,
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
    row_groups: Vec<usize>,
) -> Result<ParquetRecordBatchReader, ServerError> {
    match source {
        ParquetWorkerSource::Path(path) => {
            let file = File::open(&path).map_err(|err| {
                ServerError::CopyFormat(format!("read_parquet cannot open {display}: {err}"))
            })?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
                ServerError::CopyFormat(format!("read_parquet cannot inspect {display}: {err}"))
            })?;
            build_row_group_reader_from_builder(builder, display, projection, predicate, row_groups)
        }
        ParquetWorkerSource::Object(reader) => {
            let builder = ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|err| {
                ServerError::CopyFormat(format!("read_parquet cannot inspect {display}: {err}"))
            })?;
            build_row_group_reader_from_builder(builder, display, projection, predicate, row_groups)
        }
    }
}

fn build_row_group_reader_from_builder<T: parquet::file::reader::ChunkReader + 'static>(
    builder: ParquetRecordBatchReaderBuilder<T>,
    display: &str,
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
    row_groups: Vec<usize>,
) -> Result<ParquetRecordBatchReader, ServerError> {
    let projection_mask = match projection {
        Some(names) => ProjectionMask::columns(
            builder.parquet_schema(),
            names.iter().map(std::string::String::as_str),
        ),
        None => ProjectionMask::all(),
    };
    let row_filter = predicate.map(|p| p.row_filter(builder.parquet_schema()));
    let mut builder = builder
        .with_batch_size(PARQUET_BATCH_TARGET_ROWS)
        .with_projection(projection_mask)
        .with_row_groups(row_groups);
    if let Some(row_filter) = row_filter {
        builder = builder.with_row_filter(row_filter);
    }
    builder.build().map_err(|err| {
        ServerError::CopyFormat(format!("read_parquet cannot read {display}: {err}"))
    })
}

fn split_row_groups(row_groups: &[usize]) -> Vec<Vec<usize>> {
    if row_groups.is_empty() {
        return Vec::new();
    }
    let workers = parquet_row_group_worker_count(row_groups.len());
    let chunk_len = row_groups.len().div_ceil(workers);
    row_groups
        .chunks(chunk_len)
        .map(<[usize]>::to_vec)
        .collect()
}

fn parquet_row_group_worker_count(selected_row_groups: usize) -> usize {
    if selected_row_groups <= 1 {
        return selected_row_groups;
    }
    let available = thread::available_parallelism().map_or(2, std::num::NonZeroUsize::get);
    available
        .clamp(2, PARQUET_MAX_ROW_GROUP_WORKERS)
        .min(selected_row_groups)
}

fn selected_row_groups_with_dictionary<R>(
    reader: Arc<R>,
    metadata: &ParquetMetaData,
    schema: &ArrowSchema,
    predicate: Option<&ParquetPredicate>,
) -> Result<Vec<usize>, ServerError>
where
    R: ChunkReader + 'static,
{
    if let Some(predicate) = predicate {
        return select_row_groups(metadata, schema, predicate, |row_group, column| {
            dictionary_page_may_match(Arc::clone(&reader), metadata, row_group, column, predicate)
        });
    }
    Ok((0..metadata.num_row_groups()).collect())
}

fn row_group_summary_with_dictionary<R>(
    reader: Arc<R>,
    metadata: &ParquetMetaData,
    schema: &ArrowSchema,
    predicate: Option<&ParquetPredicate>,
) -> Result<ParquetRowGroupSummary, ServerError>
where
    R: ChunkReader + 'static,
{
    let total = metadata.num_row_groups();
    let selected = selected_row_groups_with_dictionary(reader, metadata, schema, predicate)?.len();
    let skipped = total.saturating_sub(selected);
    Ok(ParquetRowGroupSummary {
        scanned: u64::try_from(selected).unwrap_or(u64::MAX),
        skipped: u64::try_from(skipped).unwrap_or(u64::MAX),
    })
}

/// Summarize Parquet row groups that a lowered plan shape will scan.
pub(crate) fn parquet_row_group_summary_for_plan(
    plan: &LogicalPlan,
) -> Result<Option<ParquetRowGroupSummary>, ServerError> {
    let mut summary = None;
    collect_parquet_row_group_summary(plan, &mut summary)?;
    Ok(summary)
}

/// Summarize physical Parquet columns read by a lowered plan shape.
pub(crate) fn parquet_columns_read_for_plan(
    plan: &LogicalPlan,
) -> Result<Option<Vec<String>>, ServerError> {
    let mut columns = None;
    collect_parquet_columns_read(plan, &mut columns)?;
    Ok(columns)
}

fn collect_parquet_row_group_summary(
    plan: &LogicalPlan,
    summary: &mut Option<ParquetRowGroupSummary>,
) -> Result<(), ServerError> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            if let LogicalPlan::FunctionScan { name, args, .. } = input.as_ref()
                && name == "read_parquet"
            {
                let pushed = ParquetPredicate::from_scalar(predicate);
                add_parquet_function_summary(args, pushed.as_ref(), summary)?;
                return Ok(());
            }
            collect_parquet_row_group_summary(input, summary)
        }
        LogicalPlan::FunctionScan { name, args, .. } if name == "read_parquet" => {
            add_parquet_function_summary(args, None, summary)
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Update { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Delete { input, .. } => collect_parquet_row_group_summary(input, summary),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            collect_parquet_row_group_summary(left, summary)?;
            collect_parquet_row_group_summary(right, summary)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            collect_parquet_row_group_summary(definition, summary)?;
            collect_parquet_row_group_summary(body, summary)
        }
        LogicalPlan::Insert { source, .. } => collect_parquet_row_group_summary(source, summary),
        _ => Ok(()),
    }
}

fn collect_parquet_columns_read(
    plan: &LogicalPlan,
    columns: &mut Option<Vec<String>>,
) -> Result<(), ServerError> {
    match plan {
        LogicalPlan::Project { input, exprs, .. } => {
            let projection = projection_names_from_exprs(exprs);
            match input.as_ref() {
                LogicalPlan::FunctionScan { name, args, .. } if name == "read_parquet" => {
                    add_parquet_columns_read(args, projection.as_deref(), None, columns)?;
                    Ok(())
                }
                LogicalPlan::Filter {
                    input, predicate, ..
                } => {
                    if let LogicalPlan::FunctionScan { name, args, .. } = input.as_ref()
                        && name == "read_parquet"
                    {
                        let pushed = ParquetPredicate::from_scalar(predicate);
                        add_parquet_columns_read(
                            args,
                            projection.as_deref(),
                            pushed.as_ref(),
                            columns,
                        )?;
                        return Ok(());
                    }
                    collect_parquet_columns_read(input, columns)
                }
                _ => collect_parquet_columns_read(input, columns),
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            if let LogicalPlan::FunctionScan { name, args, .. } = input.as_ref()
                && name == "read_parquet"
            {
                let pushed = ParquetPredicate::from_scalar(predicate);
                add_parquet_columns_read(args, None, pushed.as_ref(), columns)?;
                return Ok(());
            }
            collect_parquet_columns_read(input, columns)
        }
        LogicalPlan::FunctionScan { name, args, .. } if name == "read_parquet" => {
            add_parquet_columns_read(args, None, None, columns)
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Update { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Delete { input, .. } => collect_parquet_columns_read(input, columns),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            collect_parquet_columns_read(left, columns)?;
            collect_parquet_columns_read(right, columns)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => {
            collect_parquet_columns_read(definition, columns)?;
            collect_parquet_columns_read(body, columns)
        }
        LogicalPlan::Insert { source, .. } => collect_parquet_columns_read(source, columns),
        _ => Ok(()),
    }
}

fn add_parquet_function_summary(
    args: &[ScalarExpr],
    predicate: Option<&ParquetPredicate>,
    summary: &mut Option<ParquetRowGroupSummary>,
) -> Result<(), ServerError> {
    let path_specs = super::external_scan::read_external_path_specs("read_parquet", args)?;
    let next = parquet_row_group_summary_for_path_specs(&path_specs, predicate)?;
    if let Some(summary) = summary {
        summary.add(next);
    } else {
        *summary = Some(next);
    }
    Ok(())
}

fn add_parquet_columns_read(
    args: &[ScalarExpr],
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
    columns: &mut Option<Vec<String>>,
) -> Result<(), ServerError> {
    let path_specs = super::external_scan::read_external_path_specs("read_parquet", args)?;
    let mut next = parquet_columns_read_for_path_specs(&path_specs, projection, predicate)?;
    if let Some(columns) = columns {
        columns.append(&mut next);
        columns.sort();
        columns.dedup();
    } else {
        next.sort();
        next.dedup();
        *columns = Some(next);
    }
    Ok(())
}

fn parquet_row_group_summary_for_path_specs(
    patterns: &[String],
    predicate: Option<&ParquetPredicate>,
) -> Result<ParquetRowGroupSummary, ServerError> {
    if path_specs_use_object_store("read_parquet", patterns)? {
        let objects = expand_object_store_specs(patterns)
            .map_err(|err| ServerError::CopyFormat(format!("read_parquet: {err}")))?;
        let mut summary = ParquetRowGroupSummary::default();
        for object in objects {
            summary.add(parquet_object_row_group_summary(&object, predicate)?);
        }
        return Ok(summary);
    }
    let mut summary = ParquetRowGroupSummary::default();
    for path in expand_parquet_path_specs(patterns)? {
        summary.add(parquet_path_row_group_summary(&path, predicate)?);
    }
    Ok(summary)
}

fn parquet_columns_read_for_path_specs(
    patterns: &[String],
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
) -> Result<Vec<String>, ServerError> {
    let schema = if path_specs_use_object_store("read_parquet", patterns)? {
        let objects = expand_object_store_specs(patterns)
            .map_err(|err| ServerError::CopyFormat(format!("read_parquet: {err}")))?;
        let Some(first) = objects.first() else {
            return Err(ServerError::CopyFormat(
                "read_parquet object expansion returned no files".to_owned(),
            ));
        };
        read_object_arrow_schema(first)?
    } else {
        let paths = expand_parquet_path_specs(patterns)?;
        let Some(first) = paths.first() else {
            return Err(ServerError::CopyFormat(
                "read_parquet path expansion returned no files".to_owned(),
            ));
        };
        read_arrow_schema(first)?
    };
    let mut columns = match projection {
        Some(projection) => {
            resolve_projection_names(schema.as_ref(), Some(projection))?.unwrap_or_default()
        }
        None => schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>(),
    };
    if let Some(predicate) = predicate {
        let predicate = predicate.resolved_for_schema(schema.as_ref())?;
        if !columns.iter().any(|column| column == &predicate.column) {
            columns.push(predicate.column);
        }
    }
    columns.sort();
    columns.dedup();
    Ok(columns)
}

fn projection_names_from_exprs(exprs: &[(ScalarExpr, String)]) -> Option<Vec<String>> {
    exprs
        .iter()
        .map(|(expr, alias)| match expr {
            ScalarExpr::Column { name, .. } if name == alias => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn parquet_path_row_group_summary(
    path: &Path,
    predicate: Option<&ParquetPredicate>,
) -> Result<ParquetRowGroupSummary, ServerError> {
    let file = File::open(path).map_err(|err| {
        ServerError::CopyFormat(format!(
            "read_parquet cannot open {}: {err}",
            path.display()
        ))
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
        ServerError::CopyFormat(format!(
            "read_parquet cannot inspect {}: {err}",
            path.display()
        ))
    })?;
    let predicate = predicate
        .map(|predicate| predicate.resolved_for_schema(builder.schema().as_ref()))
        .transpose()?;
    row_group_summary_with_dictionary(
        Arc::new(File::open(path).map_err(|err| {
            ServerError::CopyFormat(format!(
                "read_parquet cannot open {} for pruning: {err}",
                path.display()
            ))
        })?),
        builder.metadata(),
        builder.schema().as_ref(),
        predicate.as_ref(),
    )
}

fn parquet_object_row_group_summary(
    object: &ObjectLocation,
    predicate: Option<&ParquetPredicate>,
) -> Result<ParquetRowGroupSummary, ServerError> {
    let display = object.display_uri();
    let reader = ObjectRangeChunkReader::new(object.clone())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader.clone()).map_err(|err| {
        ServerError::CopyFormat(format!("read_parquet cannot inspect {display}: {err}"))
    })?;
    let predicate = predicate
        .map(|predicate| predicate.resolved_for_schema(builder.schema().as_ref()))
        .transpose()?;
    row_group_summary_with_dictionary(
        Arc::new(reader.clone()),
        builder.metadata(),
        builder.schema().as_ref(),
        predicate.as_ref(),
    )
}

fn read_arrow_schema(path: &Path) -> Result<arrow_schema::SchemaRef, ServerError> {
    let file = File::open(path).map_err(|err| {
        ServerError::CopyFormat(format!(
            "read_parquet cannot open {}: {err}",
            path.display()
        ))
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
        ServerError::CopyFormat(format!(
            "read_parquet cannot inspect {}: {err}",
            path.display()
        ))
    })?;
    Ok(builder.schema().clone())
}

fn read_object_arrow_schema(
    object: &ultrasql_objectstore::ObjectLocation,
) -> Result<arrow_schema::SchemaRef, ServerError> {
    let display = object.display_uri();
    let reader = ObjectRangeChunkReader::new(object.clone())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|err| {
        ServerError::CopyFormat(format!("read_parquet cannot inspect {display}: {err}"))
    })?;
    Ok(builder.schema().clone())
}

fn parquet_schema_to_ultrasql(
    arrow_schema: &ArrowSchema,
    projection: Option<&[String]>,
) -> Result<Schema, ServerError> {
    let fields = match projection {
        Some(names) => names
            .iter()
            .map(|name| {
                let field = arrow_schema
                    .fields()
                    .iter()
                    .find(|field| field.name() == name)
                    .ok_or_else(|| {
                        ServerError::CopyFormat(format!("read_parquet column not found: {name}"))
                    })?;
                arrow_field_to_ultrasql(field)
            })
            .collect::<Result<Vec<_>, ServerError>>()?,
        None => arrow_schema
            .fields()
            .iter()
            .map(|field| arrow_field_to_ultrasql(field))
            .collect::<Result<Vec<_>, ServerError>>()?,
    };
    Schema::new(fields)
        .map_err(|err| ServerError::CopyFormat(format!("read_parquet schema: {err}")))
}

fn arrow_field_to_ultrasql(field: &arrow_schema::Field) -> Result<Field, ServerError> {
    let data_type = arrow_type_to_ultrasql(field.data_type())?;
    Ok(if field.is_nullable() {
        Field::nullable(field.name().clone(), data_type)
    } else {
        Field::required(field.name().clone(), data_type)
    })
}

fn arrow_type_to_ultrasql(data_type: &ArrowDataType) -> Result<DataType, ServerError> {
    match data_type {
        ArrowDataType::Boolean => Ok(DataType::Bool),
        ArrowDataType::Int32 => Ok(DataType::Int32),
        ArrowDataType::Int64 => Ok(DataType::Int64),
        ArrowDataType::Float32 => Ok(DataType::Float32),
        ArrowDataType::Float64 => Ok(DataType::Float64),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Ok(DataType::Text { max_len: None }),
        other => Err(ServerError::CopyFormat(format!(
            "read_parquet unsupported Arrow type: {other}"
        ))),
    }
}

fn arrow_record_batch_to_ultrasql(batch: RecordBatch) -> Result<ultrasql_vec::Batch, ServerError> {
    record_batch_to_ultrasql_batch(batch)
        .map_err(|err| ServerError::CopyFormat(format!("read_parquet Arrow bridge: {err}")))
}

fn server_error_to_exec(err: ServerError) -> ExecError {
    ExecError::TypeMismatch(err.to_string())
}

fn evaluate_arrow_predicate(
    array: &dyn Array,
    op: BinaryOp,
    literal: &ParquetLiteral,
) -> Result<BooleanArray, ArrowError> {
    let values = match literal {
        ParquetLiteral::Bool(value) => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ArrowError::ComputeError(
                        "read_parquet Boolean predicate downcast failed".to_owned(),
                    )
                })?;
            (0..typed.len())
                .map(|idx| !typed.is_null(idx) && compare_bool(typed.value(idx), *value, op))
                .collect::<Vec<_>>()
        }
        ParquetLiteral::Int64(value) => {
            if let Some(typed) = array.as_any().downcast_ref::<Int64Array>() {
                (0..typed.len())
                    .map(|idx| !typed.is_null(idx) && compare_i64(typed.value(idx), *value, op))
                    .collect::<Vec<_>>()
            } else if let Some(typed) = array.as_any().downcast_ref::<Int32Array>() {
                (0..typed.len())
                    .map(|idx| {
                        !typed.is_null(idx) && compare_i64(i64::from(typed.value(idx)), *value, op)
                    })
                    .collect::<Vec<_>>()
            } else {
                return Err(ArrowError::ComputeError(
                    "read_parquet integer predicate downcast failed".to_owned(),
                ));
            }
        }
        ParquetLiteral::Float64(value) => {
            if let Some(typed) = array.as_any().downcast_ref::<Float64Array>() {
                (0..typed.len())
                    .map(|idx| !typed.is_null(idx) && compare_f64(typed.value(idx), *value, op))
                    .collect::<Vec<_>>()
            } else if let Some(typed) = array.as_any().downcast_ref::<Float32Array>() {
                (0..typed.len())
                    .map(|idx| {
                        !typed.is_null(idx) && compare_f64(f64::from(typed.value(idx)), *value, op)
                    })
                    .collect::<Vec<_>>()
            } else {
                return Err(ArrowError::ComputeError(
                    "read_parquet float predicate downcast failed".to_owned(),
                ));
            }
        }
        ParquetLiteral::Text(value) => {
            if let Some(typed) = array.as_any().downcast_ref::<StringArray>() {
                (0..typed.len())
                    .map(|idx| {
                        !typed.is_null(idx) && compare_str(typed.value(idx), value.as_str(), op)
                    })
                    .collect::<Vec<_>>()
            } else if let Some(typed) = array.as_any().downcast_ref::<LargeStringArray>() {
                (0..typed.len())
                    .map(|idx| {
                        !typed.is_null(idx) && compare_str(typed.value(idx), value.as_str(), op)
                    })
                    .collect::<Vec<_>>()
            } else {
                return Err(ArrowError::ComputeError(
                    "read_parquet text predicate downcast failed".to_owned(),
                ));
            }
        }
    };
    Ok(BooleanArray::from(values))
}

fn select_row_groups(
    metadata: &ParquetMetaData,
    schema: &ArrowSchema,
    predicate: &ParquetPredicate,
    mut dictionary_may_match: impl FnMut(usize, usize) -> Result<bool, ServerError>,
) -> Result<Vec<usize>, ServerError> {
    let Some(column_index) = schema
        .fields()
        .iter()
        .position(|field| field.name() == &predicate.column)
    else {
        return Err(ServerError::CopyFormat(format!(
            "read_parquet predicate column not found: {}",
            predicate.column
        )));
    };
    let mut row_groups = Vec::new();
    for index in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(index);
        let stats = row_group.column(column_index).statistics();
        let row_count = parquet_row_group_row_count(row_group);
        if stats.is_some_and(|stats| !statistics_may_match(stats, predicate, row_count)) {
            continue;
        }
        if !dictionary_may_match(index, column_index)? {
            continue;
        }
        row_groups.push(index);
    }
    Ok(row_groups)
}

fn statistics_may_match(stats: &Statistics, predicate: &ParquetPredicate, row_count: u64) -> bool {
    if row_count > 0
        && stats
            .null_count_opt()
            .is_some_and(|nulls| nulls >= row_count)
    {
        return false;
    }
    match (stats, &predicate.literal) {
        (Statistics::Boolean(stats), ParquetLiteral::Bool(value)) => {
            range_may_match(stats.min_opt(), stats.max_opt(), predicate.op, value)
        }
        (Statistics::Int32(stats), ParquetLiteral::Int64(value)) => {
            let min = stats.min_opt().map(|v| i64::from(*v));
            let max = stats.max_opt().map(|v| i64::from(*v));
            range_may_match(min.as_ref(), max.as_ref(), predicate.op, value)
        }
        (Statistics::Int64(stats), ParquetLiteral::Int64(value)) => {
            range_may_match(stats.min_opt(), stats.max_opt(), predicate.op, value)
        }
        (Statistics::Float(stats), ParquetLiteral::Float64(value)) => {
            let min = stats.min_opt().map(|v| f64::from(*v));
            let max = stats.max_opt().map(|v| f64::from(*v));
            range_may_match(min.as_ref(), max.as_ref(), predicate.op, value)
        }
        (Statistics::Double(stats), ParquetLiteral::Float64(value)) => {
            range_may_match(stats.min_opt(), stats.max_opt(), predicate.op, value)
        }
        (Statistics::ByteArray(stats), ParquetLiteral::Text(value)) => {
            let min = stats.min_opt().map(parquet::data_type::ByteArray::data);
            let max = stats.max_opt().map(parquet::data_type::ByteArray::data);
            range_may_match(min, max, predicate.op, value.as_bytes())
        }
        _ => true,
    }
}

fn parquet_row_group_row_count(row_group: &parquet::file::metadata::RowGroupMetaData) -> u64 {
    u64::try_from(row_group.num_rows()).unwrap_or(0)
}

fn dictionary_page_may_match<R>(
    reader: Arc<R>,
    metadata: &ParquetMetaData,
    row_group_index: usize,
    column_index: usize,
    predicate: &ParquetPredicate,
) -> Result<bool, ServerError>
where
    R: ChunkReader + 'static,
{
    if predicate.op != BinaryOp::Eq {
        return Ok(true);
    }
    let row_group = metadata.row_group(row_group_index);
    let column = row_group.column(column_index);
    if !column_chunk_is_dictionary_prunable(column) {
        return Ok(true);
    }
    let total_rows = usize::try_from(row_group.num_rows()).unwrap_or(0);
    let mut page_reader =
        SerializedPageReader::new(reader, column, total_rows, None).map_err(|err| {
            ServerError::CopyFormat(format!(
                "read_parquet cannot inspect dictionary for row group {row_group_index}: {err}"
            ))
        })?;
    if let Some(page) = page_reader.next() {
        let page = page.map_err(|err| {
            ServerError::CopyFormat(format!(
                "read_parquet cannot inspect dictionary for row group {row_group_index}: {err}"
            ))
        })?;
        match page {
            Page::DictionaryPage { .. } => {
                return Ok(
                    dictionary_contains_literal(&page, column.column_type(), predicate)
                        .unwrap_or(true),
                );
            }
            Page::DataPage { .. } | Page::DataPageV2 { .. } => return Ok(true),
        }
    }
    Ok(true)
}

fn column_chunk_is_dictionary_prunable(
    column: &parquet::file::metadata::ColumnChunkMetaData,
) -> bool {
    column.dictionary_page_offset().is_some()
        && column.page_encoding_stats_mask().is_some_and(|mask| {
            mask.is_only(Encoding::PLAIN_DICTIONARY) || mask.is_only(Encoding::RLE_DICTIONARY)
        })
}

fn dictionary_contains_literal(
    page: &Page,
    physical_type: ParquetPhysicalType,
    predicate: &ParquetPredicate,
) -> Option<bool> {
    let Page::DictionaryPage {
        buf,
        num_values,
        encoding,
        ..
    } = page
    else {
        return None;
    };
    if *encoding != Encoding::PLAIN {
        return None;
    }
    match (physical_type, &predicate.literal) {
        (ParquetPhysicalType::BYTE_ARRAY, ParquetLiteral::Text(value)) => {
            plain_byte_array_dictionary_contains(buf, *num_values, value.as_bytes())
        }
        (ParquetPhysicalType::INT32, ParquetLiteral::Int64(value)) => {
            let needle = i32::try_from(*value).ok()?;
            plain_i32_dictionary_contains(buf, *num_values, needle)
        }
        (ParquetPhysicalType::INT64, ParquetLiteral::Int64(value)) => {
            plain_i64_dictionary_contains(buf, *num_values, *value)
        }
        (ParquetPhysicalType::DOUBLE, ParquetLiteral::Float64(value)) => {
            plain_f64_dictionary_contains(buf, *num_values, *value)
        }
        _ => None,
    }
}

fn plain_byte_array_dictionary_contains(
    buf: &[u8],
    num_values: u32,
    needle: &[u8],
) -> Option<bool> {
    let mut offset = 0_usize;
    for _ in 0..usize::try_from(num_values).ok()? {
        let len_bytes = buf.get(offset..offset.checked_add(4)?)?;
        let len = usize::try_from(u32::from_le_bytes(len_bytes.try_into().ok()?)).ok()?;
        offset = offset.checked_add(4)?;
        let end = offset.checked_add(len)?;
        let value = buf.get(offset..end)?;
        if value == needle {
            return Some(true);
        }
        offset = end;
    }
    Some(false)
}

fn plain_i32_dictionary_contains(buf: &[u8], num_values: u32, needle: i32) -> Option<bool> {
    plain_fixed_dictionary_contains(buf, num_values, 4, |bytes| {
        let Ok(bytes) = <[u8; 4]>::try_from(bytes) else {
            return false;
        };
        i32::from_le_bytes(bytes) == needle
    })
}

fn plain_i64_dictionary_contains(buf: &[u8], num_values: u32, needle: i64) -> Option<bool> {
    plain_fixed_dictionary_contains(buf, num_values, 8, |bytes| {
        let Ok(bytes) = <[u8; 8]>::try_from(bytes) else {
            return false;
        };
        i64::from_le_bytes(bytes) == needle
    })
}

fn plain_f64_dictionary_contains(buf: &[u8], num_values: u32, needle: f64) -> Option<bool> {
    plain_fixed_dictionary_contains(buf, num_values, 8, |bytes| {
        let Ok(bytes) = <[u8; 8]>::try_from(bytes) else {
            return false;
        };
        f64::from_le_bytes(bytes) == needle
    })
}

fn plain_fixed_dictionary_contains(
    buf: &[u8],
    num_values: u32,
    width: usize,
    mut matches: impl FnMut(&[u8]) -> bool,
) -> Option<bool> {
    let count = usize::try_from(num_values).ok()?;
    for idx in 0..count {
        let start = idx.checked_mul(width)?;
        let end = start.checked_add(width)?;
        if matches(buf.get(start..end)?) {
            return Some(true);
        }
    }
    Some(false)
}

fn range_may_match<T: PartialOrd + PartialEq + ?Sized>(
    min: Option<&T>,
    max: Option<&T>,
    op: BinaryOp,
    value: &T,
) -> bool {
    match op {
        BinaryOp::Eq => {
            if min.is_some_and(|min| value < min) {
                return false;
            }
            if max.is_some_and(|max| value > max) {
                return false;
            }
            true
        }
        BinaryOp::NotEq => {
            !(min.is_some_and(|min| min == value) && max.is_some_and(|max| max == value))
        }
        BinaryOp::Lt => min.is_none_or(|min| min < value),
        BinaryOp::LtEq => min.is_none_or(|min| min <= value),
        BinaryOp::Gt => max.is_none_or(|max| max > value),
        BinaryOp::GtEq => max.is_none_or(|max| max >= value),
        _ => true,
    }
}

fn compare_bool(left: bool, right: bool, op: BinaryOp) -> bool {
    compare_ordering(left.cmp(&right), op)
}

fn compare_i64(left: i64, right: i64, op: BinaryOp) -> bool {
    compare_ordering(left.cmp(&right), op)
}

fn compare_f64(left: f64, right: f64, op: BinaryOp) -> bool {
    left.partial_cmp(&right)
        .is_some_and(|ordering| compare_ordering(ordering, op))
}

fn compare_str(left: &str, right: &str, op: BinaryOp) -> bool {
    compare_ordering(left.cmp(right), op)
}

fn compare_ordering(ordering: Ordering, op: BinaryOp) -> bool {
    match op {
        BinaryOp::Eq => ordering == Ordering::Equal,
        BinaryOp::NotEq => ordering != Ordering::Equal,
        BinaryOp::Lt => ordering == Ordering::Less,
        BinaryOp::LtEq => matches!(ordering, Ordering::Less | Ordering::Equal),
        BinaryOp::Gt => ordering == Ordering::Greater,
        BinaryOp::GtEq => matches!(ordering, Ordering::Greater | Ordering::Equal),
        _ => false,
    }
}

fn resolve_projection_names(
    schema: &ArrowSchema,
    projection: Option<&[String]>,
) -> Result<Option<Vec<String>>, ServerError> {
    let Some(projection) = projection else {
        return Ok(None);
    };
    projection
        .iter()
        .map(|name| {
            schema
                .fields()
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(name))
                .map(|field| field.name().clone())
                .ok_or_else(|| {
                    ServerError::CopyFormat(format!("read_parquet column not found: {name}"))
                })
        })
        .collect::<Result<Vec<_>, ServerError>>()
        .map(Some)
}

fn column_name(expr: &ScalarExpr) -> Option<String> {
    match expr {
        ScalarExpr::Column { name, .. } => Some(name.clone()),
        _ => None,
    }
}

fn literal_value(expr: &ScalarExpr) -> Option<ParquetLiteral> {
    match expr {
        ScalarExpr::Literal {
            value: Value::Bool(value),
            ..
        } => Some(ParquetLiteral::Bool(*value)),
        ScalarExpr::Literal {
            value: Value::Int16(value),
            ..
        } => Some(ParquetLiteral::Int64(i64::from(*value))),
        ScalarExpr::Literal {
            value: Value::Int32(value),
            ..
        } => Some(ParquetLiteral::Int64(i64::from(*value))),
        ScalarExpr::Literal {
            value: Value::Int64(value),
            ..
        } => Some(ParquetLiteral::Int64(*value)),
        ScalarExpr::Literal {
            value: Value::Float32(value),
            ..
        } => Some(ParquetLiteral::Float64(f64::from(*value))),
        ScalarExpr::Literal {
            value: Value::Float64(value),
            ..
        } => Some(ParquetLiteral::Float64(*value)),
        ScalarExpr::Literal {
            value: Value::Text(value),
            ..
        } => Some(ParquetLiteral::Text(value.clone())),
        _ => None,
    }
}

fn is_supported_cmp(op: BinaryOp) -> bool {
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

fn reverse_cmp(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

fn expand_parquet_path_specs(patterns: &[String]) -> Result<Vec<PathBuf>, ServerError> {
    if patterns.is_empty() {
        return Err(ServerError::CopyFormat(
            "read_parquet path list cannot be empty".to_owned(),
        ));
    }
    let mut paths = Vec::new();
    for pattern in patterns {
        paths.extend(expand_parquet_paths(pattern)?);
    }
    Ok(paths)
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

fn expand_parquet_paths(pattern: &str) -> Result<Vec<PathBuf>, ServerError> {
    let path = Path::new(pattern);
    let file_pattern = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            ServerError::CopyFormat(format!(
                "read_parquet path must name a file or wildcard: {pattern}"
            ))
        })?;
    if !contains_wildcard(file_pattern) {
        return Ok(vec![path.to_path_buf()]);
    }

    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut paths = Vec::new();
    for entry in fs::read_dir(parent).map_err(|err| {
        ServerError::CopyFormat(format!(
            "read_parquet cannot read directory {}: {err}",
            parent.display()
        ))
    })? {
        let entry = entry.map_err(|err| ServerError::CopyFormat(format!("read_parquet: {err}")))?;
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if wildcard_match(file_pattern, &name) {
            paths.push(entry.path());
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Err(ServerError::CopyFormat(format!(
            "read_parquet pattern matched no files: {pattern}"
        )));
    }
    Ok(paths)
}

fn contains_wildcard(s: &str) -> bool {
    s.chars().any(|ch| matches!(ch, '*' | '?'))
}

fn wildcard_match(pattern: &str, text: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let text = text.chars().collect::<Vec<_>>();
    let mut dp = vec![vec![false; text.len() + 1]; pattern.len() + 1];
    dp[0][0] = true;
    for (i, ch) in pattern.iter().enumerate() {
        if *ch == '*' {
            dp[i + 1][0] = dp[i][0];
        }
    }
    for (i, pattern_ch) in pattern.iter().enumerate() {
        for (j, text_ch) in text.iter().enumerate() {
            dp[i + 1][j + 1] = match pattern_ch {
                '*' => dp[i][j + 1] || dp[i + 1][j],
                '?' => dp[i][j],
                ch => dp[i][j] && ch == text_ch,
            };
        }
    }
    dp[pattern.len()][text.len()]
}

#[cfg(test)]
mod tests {
    use super::{ParquetPredicate, ParquetTableScan};
    use std::fs;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use ultrasql_core::{DataType, Value};
    use ultrasql_executor::Operator;
    use ultrasql_planner::{BinaryOp, ScalarExpr};
    use ultrasql_vec::column::Column;

    #[test]
    fn simple_column_literal_predicate_is_pushable() {
        let expr = ScalarExpr::Binary {
            op: BinaryOp::GtEq,
            left: Box::new(ScalarExpr::Column {
                name: "id".to_owned(),
                index: 0,
                data_type: DataType::Int64,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int64(100),
                data_type: DataType::Int64,
            }),
            data_type: DataType::Bool,
        };
        let predicate = ParquetPredicate::from_scalar(&expr).expect("pushable predicate");
        assert_eq!(predicate.column, "id");
        assert_eq!(predicate.op, BinaryOp::GtEq);
    }

    #[test]
    fn literal_column_predicate_reverses_operator() {
        let expr = ScalarExpr::Binary {
            op: BinaryOp::LtEq,
            left: Box::new(ScalarExpr::Literal {
                value: Value::Int64(100),
                data_type: DataType::Int64,
            }),
            right: Box::new(ScalarExpr::Column {
                name: "id".to_owned(),
                index: 0,
                data_type: DataType::Int64,
            }),
            data_type: DataType::Bool,
        };
        let predicate = ParquetPredicate::from_scalar(&expr).expect("pushable predicate");
        assert_eq!(predicate.column, "id");
        assert_eq!(predicate.op, BinaryOp::GtEq);
    }

    #[test]
    fn wildcard_match_supports_star_and_question_mark() {
        let paths = ParquetTableScan::from_path_specs(&[], None, None)
            .expect_err("empty path list must fail");
        assert!(paths.to_string().contains("path list cannot be empty"));
        assert!(super::wildcard_match("part-*.parquet", "part-001.parquet"));
        assert!(super::wildcard_match("part-??.parquet", "part-01.parquet"));
        assert!(!super::wildcard_match(
            "part-??.parquet",
            "part-001.parquet"
        ));
    }

    #[test]
    fn parquet_scan_defers_later_file_batches_until_needed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = dir.path().join("first.parquet");
        let second = dir.path().join("second.parquet");
        write_i64_parquet(&first, &[1]);
        write_i64_parquet(&second, &[2]);
        let path_specs = vec![first.display().to_string(), second.display().to_string()];

        let mut scan =
            ParquetTableScan::from_path_specs(&path_specs, None, None).expect("construct scan");
        fs::remove_file(&second).expect("remove second parquet");

        let first_batch = scan
            .next_batch()
            .expect("read first file")
            .expect("first batch");
        let Column::Int64(values) = &first_batch.columns()[0] else {
            panic!("expected int64 column");
        };
        assert_eq!(values.data(), &[1]);

        let err = scan
            .next_batch()
            .expect_err("second file read should be lazy");
        let message = err.to_string();
        assert!(
            message.contains("cannot open") && message.contains("second.parquet"),
            "unexpected lazy read error: {message}"
        );
    }

    #[test]
    fn parquet_scan_splits_selected_row_groups_across_workers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("groups.parquet");
        write_i64_parquet_groups(&path, &[&[1], &[2], &[3], &[4]]);
        let path_specs = vec![path.display().to_string()];
        let predicate = ParquetPredicate {
            column: "id".to_owned(),
            op: BinaryOp::GtEq,
            literal: super::ParquetLiteral::Int64(2),
        };

        let mut scan = ParquetTableScan::from_path_specs(&path_specs, None, Some(&predicate))
            .expect("construct scan");
        let first_batch = scan
            .next_batch()
            .expect("read first parallel batch")
            .expect("first batch");
        let worker_count = scan
            .active
            .as_ref()
            .map_or(0, |active| active.workers.len());
        assert!(
            worker_count > 1,
            "selected row groups must split across workers, got {worker_count}"
        );

        let mut ids = collect_i64_ids(&first_batch);
        while let Some(batch) = scan.next_batch().expect("read next parallel batch") {
            ids.extend(collect_i64_ids(&batch));
        }
        ids.sort_unstable();
        assert_eq!(ids, vec![2, 3, 4]);
    }

    #[test]
    fn parquet_predicate_pushdown_skips_all_null_row_groups() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nulls.parquet");
        write_nullable_i64_parquet_groups(&path, &[vec![None, None], vec![Some(7), Some(9)]]);
        let predicate = ParquetPredicate {
            column: "id".to_owned(),
            op: BinaryOp::Eq,
            literal: super::ParquetLiteral::Int64(7),
        };

        let summary =
            super::parquet_path_row_group_summary(&path, Some(&predicate)).expect("summary");

        assert_eq!(
            summary,
            super::ParquetRowGroupSummary {
                scanned: 1,
                skipped: 1,
            }
        );
    }

    #[test]
    fn parquet_dictionary_pruning_skips_absent_text_values() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("dict.parquet");
        write_string_dictionary_parquet_groups(&path, &[&["alpha", "gamma"], &["delta"]]);
        let predicate = ParquetPredicate {
            column: "category".to_owned(),
            op: BinaryOp::Eq,
            literal: super::ParquetLiteral::Text("beta".to_owned()),
        };

        let summary =
            super::parquet_path_row_group_summary(&path, Some(&predicate)).expect("summary");

        assert_eq!(
            summary,
            super::ParquetRowGroupSummary {
                scanned: 0,
                skipped: 2,
            }
        );
    }

    fn write_i64_parquet(path: &std::path::Path, values: &[i64]) {
        write_i64_parquet_groups(path, &[values]);
    }

    fn write_i64_parquet_groups(path: &std::path::Path, groups: &[&[i64]]) {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let file = fs::File::create(path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("writer");
        for values in groups {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values.to_vec()))],
            )
            .expect("record batch");
            writer.write(&batch).expect("write parquet row group");
            writer.flush().expect("flush parquet row group");
        }
        writer.close().expect("close parquet");
    }

    fn write_nullable_i64_parquet_groups(path: &std::path::Path, groups: &[Vec<Option<i64>>]) {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int64,
            true,
        )]));
        let file = fs::File::create(path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("writer");
        for values in groups {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values.clone()))],
            )
            .expect("record batch");
            writer.write(&batch).expect("write parquet row group");
            writer.flush().expect("flush parquet row group");
        }
        writer.close().expect("close parquet");
    }

    fn write_string_dictionary_parquet_groups(path: &std::path::Path, groups: &[&[&str]]) {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "category",
            ArrowDataType::Utf8,
            false,
        )]));
        let props = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .build();
        let file = fs::File::create(path).expect("create parquet");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).expect("writer");
        for values in groups {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from_iter_values(
                    values.iter().copied(),
                ))],
            )
            .expect("record batch");
            writer.write(&batch).expect("write parquet row group");
            writer.flush().expect("flush parquet row group");
        }
        writer.close().expect("close parquet");
    }

    fn collect_i64_ids(batch: &ultrasql_vec::Batch) -> Vec<i64> {
        let Column::Int64(values) = &batch.columns()[0] else {
            panic!("expected int64 column");
        };
        values.data().to_vec()
    }
}
