//! File- and object-backed parallel scan engine for `read_parquet`.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    mpsc::{Receiver, SyncSender, sync_channel},
};
use std::thread;

use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use ultrasql_core::Schema;
use ultrasql_executor::{ExecError, Operator};
use ultrasql_objectstore::{ObjectLocation, expand_object_store_specs};

use crate::error::ServerError;

use super::PARQUET_BATCH_TARGET_ROWS;
use super::object_range::ObjectRangeChunkReader;
use super::paths::{expand_parquet_path_specs, path_specs_use_object_store};
use super::predicate::ParquetPredicate;
use super::pruning::selected_row_groups_with_dictionary;
use super::schema::{
    arrow_record_batch_to_ultrasql, parquet_schema_to_ultrasql, read_arrow_schema,
    read_object_arrow_schema, resolve_projection_names,
};

const PARQUET_MAX_ROW_GROUP_WORKERS: usize = 8;

/// File-backed scan for `read_parquet(path_or_glob)`.
#[derive(Debug)]
pub(crate) struct ParquetTableScan {
    schema: Schema,
    expected_arrow_schema: ArrowSchemaRef,
    projection: Option<Vec<String>>,
    predicate: Option<ParquetPredicate>,
    sources: VecDeque<ParquetScanSource>,
    pub(super) active: Option<ActiveParquetReader>,
}

impl ParquetTableScan {
    /// Load Parquet files from one or more path/glob specs into a
    /// query-local scan.
    pub(crate) fn from_path_specs(
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
pub(super) struct ActiveParquetReader {
    display: String,
    receiver: Option<Receiver<ParquetWorkerMessage>>,
    pub(super) workers: Vec<thread::JoinHandle<()>>,
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

fn open_path_reader(
    path: &Path,
    expected_schema: &ArrowSchema,
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
) -> Result<Option<ActiveParquetReader>, ServerError> {
    let display = path.display().to_string();
    let file = open_regular_parquet_file(path, &display, "open")?;
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

    let row_group_reader = open_regular_parquet_file(path, &display, "open for pruning")?;
    let row_groups = selected_row_groups_with_dictionary(
        Arc::new(row_group_reader),
        builder.metadata(),
        expected_schema,
        predicate,
    )?;
    if row_groups.is_empty() {
        return Ok(None);
    }
    spawn_parquet_row_group_workers(
        display,
        ParquetWorkerSource::Path(path.to_path_buf()),
        projection,
        predicate,
        &row_groups,
    )
    .map(Some)
}

pub(super) fn open_regular_parquet_file(
    path: &Path,
    display: &str,
    purpose: &str,
) -> Result<File, ServerError> {
    let metadata = fs::symlink_metadata(path).map_err(|err| {
        ServerError::CopyFormat(format!("read_parquet cannot inspect {display}: {err}"))
    })?;
    if !metadata.file_type().is_file() {
        return Err(ServerError::CopyFormat(format!(
            "read_parquet path is not a regular file: {display}"
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
        ServerError::CopyFormat(format!("read_parquet cannot {purpose} {display}: {err}"))
    })
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
            let file = open_regular_parquet_file(&path, display, "open")?;
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

fn server_error_to_exec(err: ServerError) -> ExecError {
    ExecError::TypeMismatch(err.to_string())
}
