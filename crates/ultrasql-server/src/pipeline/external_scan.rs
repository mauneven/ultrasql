//! Unified executor for external file table functions.
//!
//! Format-specific readers parse CSV, Parquet, JSON, NDJSON, Arrow IPC,
//! and Iceberg metadata into UltraSQL batches. This operator owns the
//! common scan contract seen by the rest of the executor.

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Cursor, ErrorKind, Read};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use arrow_ipc::reader::FileReader as ArrowFileReader;
use serde_json::{Map as JsonMap, Value as JsonValue};
use ultrasql_arrow::{record_batch_to_ultrasql_batch, schema_from_arrow};
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_executor::{Eval, ExecError, MemTableScan, Operator};
use ultrasql_iceberg::plan_iceberg_scan;
use ultrasql_objectstore::{
    ObjectLocation, expand_object_store_specs, is_object_store_uri, read_object_bytes,
};
use ultrasql_planner::{LogicalPlan, ScalarExpr};
use ultrasql_vec::Batch;
use ultrasql_vec::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

use crate::error::ServerError;

use super::csv_scan::CsvTableScan;
use super::object_stream::ObjectRangeReader;
use super::parquet_scan::{ParquetPredicate, ParquetTableScan};

const EXTERNAL_BATCH_TARGET_ROWS: usize = 4096;
const DEFAULT_EXTERNAL_LOCAL_READ_LIMIT_BYTES: u64 = 128 * 1024 * 1024;
const EXTERNAL_LOCAL_READ_LIMIT_ENV: &str = "ULTRASQL_EXTERNAL_LOCAL_READ_LIMIT_BYTES";
const DEFAULT_JSON_RECORD_READ_LIMIT_BYTES: u64 = 16 * 1024 * 1024;
const JSON_RECORD_READ_LIMIT_ENV: &str = "ULTRASQL_JSON_RECORD_LIMIT_BYTES";
const MAX_LOCAL_WILDCARD_PATTERN_CHARS: usize = 4096;

/// Return true for file-backed table functions lowered through
/// [`ExternalTableScan`].
pub(super) fn is_external_table_function(name: &str) -> bool {
    ExternalTableFormat::from_function_name(name).is_some()
}

/// Decide whether a server-file table function (`read_csv`, `read_parquet`,
/// `read_json`, `read_ndjson`, `read_arrow`, `read_iceberg`, `iceberg_scan`,
/// or `sniff_csv`) would read at least one LOCAL server file rather than an
/// object-store URI.
///
/// Returns `Ok(true)` when any argument resolves to a local filesystem path
/// (the host-filesystem case that must be gated), and `Ok(false)` when every
/// path is an object-store URI (`s3://`, `r2://`, …) or when the function is
/// not a server-file reader. Argument-evaluation failures are reported as
/// `Ok(false)` here: this is a *pre-authorization* probe, and a genuinely
/// malformed argument is surfaced later by the lowerer with its richer error.
/// `sniff_csv` is unconditionally local, so it always returns `Ok(true)`.
pub(super) fn function_scan_reads_local_file(
    name: &str,
    args: &[ScalarExpr],
) -> Result<bool, ServerError> {
    if name == "sniff_csv" {
        // `sniff_csv` has no object-store branch; it always opens a local file.
        return Ok(true);
    }
    if !is_external_table_function(name) {
        return Ok(false);
    }
    // `read_csv` carries an optional trailing reject-path argument; evaluate
    // only the leading path argument(s) for the local-vs-remote decision.
    let path_specs = if name == "read_csv" {
        match read_csv_external_args(args) {
            Ok(parsed) => parsed.path_specs,
            Err(_) => return Ok(false),
        }
    } else {
        match read_external_path_specs(name, args) {
            Ok(specs) => specs,
            Err(_) => return Ok(false),
        }
    };
    if path_specs.is_empty() {
        return Ok(false);
    }
    // `Ok(false)` means every spec is local; `Err` means a mixed list, which
    // still includes at least one local spec and so must be gated.
    match path_specs_use_object_store(name, &path_specs) {
        Ok(uses_object_store) => Ok(!uses_object_store),
        Err(_) => Ok(true),
    }
}

/// Walk a bound [`LogicalPlan`] and return `true` if any `FunctionScan` would
/// read a LOCAL server-side file via one of the external-file table functions
/// (or `sniff_csv`).
///
/// This is the detection half of the server-file privilege gate. It mirrors
/// the recursion shape used by the Parquet `EXPLAIN` plan-summary walk so that
/// every query shape — subqueries, CTEs, joins, set operations, `INSERT
/// ... SELECT`, `COPY (SELECT ...)`, and `EXPLAIN` — is inspected. The caller
/// (see [`crate::pipeline::ensure_external_local_file_access`]) decides whether
/// to deny based on the current role's superuser status; this function carries
/// no policy of its own.
pub(super) fn plan_reads_local_external_file(plan: &LogicalPlan) -> Result<bool, ServerError> {
    match plan {
        LogicalPlan::FunctionScan { name, args, .. } => function_scan_reads_local_file(name, args),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::LockRows { input, .. }
        | LogicalPlan::Pivot { input, .. }
        | LogicalPlan::Unpivot { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Update { input, .. }
        | LogicalPlan::Delete { input, .. } => plan_reads_local_external_file(input),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            Ok(plan_reads_local_external_file(left)? || plan_reads_local_external_file(right)?)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => Ok(
            plan_reads_local_external_file(definition)? || plan_reads_local_external_file(body)?
        ),
        LogicalPlan::Insert { source, .. } => plan_reads_local_external_file(source),
        // `COPY (SELECT ...) TO file` carries the inner query in `input`;
        // `source` is the wire endpoint (`STDIN`/`STDOUT`), not a sub-plan.
        LogicalPlan::Copy {
            input: Some(input), ..
        } => plan_reads_local_external_file(input),
        _ => Ok(false),
    }
}

/// Lower one supported external table function into the shared scan.
pub(super) fn lower_external_table_scan(
    name: &str,
    args: &[ScalarExpr],
) -> Result<Box<dyn Operator>, ServerError> {
    let Some(format) = ExternalTableFormat::from_function_name(name) else {
        return Err(ServerError::Unsupported(
            "external table function name is not supported",
        ));
    };
    let scan = match format {
        ExternalTableFormat::Csv => ExternalTableScan::from_csv(args)?,
        ExternalTableFormat::Parquet => ExternalTableScan::from_parquet(args)?,
        ExternalTableFormat::Json => ExternalTableScan::from_json(args, JsonInputKind::Json)?,
        ExternalTableFormat::Ndjson => ExternalTableScan::from_json(args, JsonInputKind::Ndjson)?,
        ExternalTableFormat::Arrow => ExternalTableScan::from_arrow(args)?,
        ExternalTableFormat::Iceberg => ExternalTableScan::from_iceberg(name, args)?,
    };
    Ok(Box::new(scan))
}

/// Lower a Parquet scan with optional projection/predicate pushdown into
/// the shared external scan executor.
pub(super) fn lower_external_parquet_scan(
    path_specs: &[String],
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
) -> Result<Box<dyn Operator>, ServerError> {
    Ok(Box::new(ExternalTableScan::from_parquet_path_specs(
        path_specs, projection, predicate,
    )?))
}

/// Evaluate a table-function path argument into one or more path specs.
pub(super) fn read_external_path_specs(
    function_name: &str,
    args: &[ScalarExpr],
) -> Result<Vec<String>, ServerError> {
    if args.len() != 1 {
        return Err(ServerError::CopyFormat(format!(
            "{function_name}: expected one path, glob, or path-list argument"
        )));
    }
    read_external_path_arg(function_name, &args[0])
}

#[derive(Debug)]
pub(super) struct CsvExternalArgs {
    pub(super) path_specs: Vec<String>,
    pub(super) reject_path: Option<PathBuf>,
}

/// Evaluate `read_csv` arguments, including the optional reject artifact path.
pub(super) fn read_csv_external_args(args: &[ScalarExpr]) -> Result<CsvExternalArgs, ServerError> {
    if !matches!(args.len(), 1 | 2) {
        return Err(ServerError::CopyFormat(
            "read_csv: expected path, glob, or path-list argument plus optional reject path"
                .to_owned(),
        ));
    }
    let path_specs = read_external_path_arg("read_csv", &args[0])?;
    let reject_path = args.get(1).map(read_csv_reject_path_arg).transpose()?;
    Ok(CsvExternalArgs {
        path_specs,
        reject_path,
    })
}

fn read_external_path_arg(
    function_name: &str,
    arg: &ScalarExpr,
) -> Result<Vec<String>, ServerError> {
    let value = Eval::new(arg.clone()).eval(&[]).map_err(|err| {
        ServerError::Ddl(format!("{function_name} argument evaluation failed: {err}"))
    })?;
    match value {
        Value::Text(pattern) => Ok(vec![pattern]),
        Value::Array {
            element_type: DataType::Text { max_len: None },
            elements,
        } => elements
            .into_iter()
            .map(|element| match element {
                Value::Text(path) => Ok(path),
                _ => Err(ServerError::CopyFormat(format!(
                    "{function_name}: path-list elements must be string literals"
                ))),
            })
            .collect(),
        _ => Err(ServerError::CopyFormat(format!(
            "{function_name}: argument must be a string literal or text array literal"
        ))),
    }
}

fn read_csv_reject_path_arg(arg: &ScalarExpr) -> Result<PathBuf, ServerError> {
    let value = Eval::new(arg.clone()).eval(&[]).map_err(|err| {
        ServerError::Ddl(format!("read_csv reject path evaluation failed: {err}"))
    })?;
    let Value::Text(path) = value else {
        return Err(ServerError::CopyFormat(
            "read_csv: reject path must be a string literal".to_owned(),
        ));
    };
    if path.is_empty() {
        return Err(ServerError::CopyFormat(
            "read_csv: reject path must not be empty".to_owned(),
        ));
    }
    if is_object_store_uri(&path) {
        return Err(ServerError::CopyFormat(
            "read_csv: reject path must be a local file path".to_owned(),
        ));
    }
    Ok(PathBuf::from(path))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExternalTableFormat {
    Csv,
    Parquet,
    Json,
    Ndjson,
    Arrow,
    Iceberg,
}

impl ExternalTableFormat {
    fn from_function_name(name: &str) -> Option<Self> {
        match name {
            "read_csv" => Some(Self::Csv),
            "read_parquet" => Some(Self::Parquet),
            "read_json" => Some(Self::Json),
            "read_ndjson" => Some(Self::Ndjson),
            "read_arrow" => Some(Self::Arrow),
            "read_iceberg" | "iceberg_scan" => Some(Self::Iceberg),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonInputKind {
    Json,
    Ndjson,
}

impl JsonInputKind {
    const fn function_name(self) -> &'static str {
        match self {
            Self::Json => "read_json",
            Self::Ndjson => "read_ndjson",
        }
    }
}

/// Shared scan node for external table functions.
#[derive(Debug)]
pub(super) struct ExternalTableScan {
    schema: Schema,
    source: ExternalScanSource,
}

#[derive(Debug)]
enum ExternalScanSource {
    Streaming(Box<dyn Operator>),
    Buffered(VecDeque<Batch>),
}

impl ExternalTableScan {
    fn streaming(source: Box<dyn Operator>) -> Self {
        let schema = source.schema().clone();
        Self {
            schema,
            source: ExternalScanSource::Streaming(source),
        }
    }

    fn buffered(schema: Schema, batches: VecDeque<Batch>) -> Self {
        Self {
            schema,
            source: ExternalScanSource::Buffered(batches),
        }
    }

    fn from_csv(args: &[ScalarExpr]) -> Result<Self, ServerError> {
        let csv_args = read_csv_external_args(args)?;
        let scan = CsvTableScan::from_path_specs_with_options(
            &csv_args.path_specs,
            None,
            None,
            csv_args.reject_path.as_deref(),
        )?;
        Ok(Self::streaming(Box::new(scan)))
    }

    fn from_parquet(args: &[ScalarExpr]) -> Result<Self, ServerError> {
        let path_specs = read_external_path_specs("read_parquet", args)?;
        Self::from_parquet_path_specs(&path_specs, None, None)
    }

    fn from_parquet_path_specs(
        path_specs: &[String],
        projection: Option<&[String]>,
        predicate: Option<&ParquetPredicate>,
    ) -> Result<Self, ServerError> {
        let scan = ParquetTableScan::from_path_specs(path_specs, projection, predicate)?;
        Ok(Self::streaming(Box::new(scan)))
    }

    fn from_json(args: &[ScalarExpr], kind: JsonInputKind) -> Result<Self, ServerError> {
        let function_name = kind.function_name();
        let path_specs = read_external_path_specs(function_name, args)?;
        let scan = JsonTableScan::from_path_specs(function_name, &path_specs, kind)?;
        Ok(Self::streaming(Box::new(scan)))
    }

    fn from_arrow(args: &[ScalarExpr]) -> Result<Self, ServerError> {
        let path_specs = read_external_path_specs("read_arrow", args)?;
        let sources = read_external_sources("read_arrow", &path_specs)?;
        let (schema, batches) = read_arrow_batches(&sources)?;
        Ok(Self::buffered(schema, batches))
    }

    fn from_iceberg(function_name: &str, args: &[ScalarExpr]) -> Result<Self, ServerError> {
        let path_specs = read_external_path_specs(function_name, args)?;
        let [path] = path_specs.as_slice() else {
            return Err(ServerError::CopyFormat(format!(
                "{function_name}: expected one table root or metadata JSON path argument"
            )));
        };
        let plan = plan_iceberg_scan(path)
            .map_err(|err| ServerError::CopyFormat(format!("{function_name}: {err}")))?;
        let source: Box<dyn Operator> = if plan.data_files.is_empty() {
            Box::new(MemTableScan::new(plan.schema, vec![]))
        } else {
            Box::new(ParquetTableScan::from_path_specs(
                &plan.data_files,
                None,
                None,
            )?)
        };
        Ok(Self::streaming(source))
    }
}

impl Operator for ExternalTableScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        match &mut self.source {
            ExternalScanSource::Streaming(source) => source.next_batch(),
            ExternalScanSource::Buffered(batches) => Ok(batches.pop_front()),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Clone, Debug)]
enum ExternalStreamSpec {
    Local(PathBuf),
    Object(ObjectLocation),
}

impl ExternalStreamSpec {
    fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::Object(object) => object.display_uri(),
        }
    }
}

#[derive(Debug)]
enum ExternalStreamReader {
    File(BufReader<File>),
    Object(ObjectRangeReader),
}

impl Read for ExternalStreamReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(reader) => reader.read(out),
            Self::Object(reader) => reader.read(out),
        }
    }
}

impl BufRead for ExternalStreamReader {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        match self {
            Self::File(reader) => reader.fill_buf(),
            Self::Object(reader) => reader.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            Self::File(reader) => reader.consume(amt),
            Self::Object(reader) => reader.consume(amt),
        }
    }
}

fn external_stream_specs(
    function_name: &str,
    path_specs: &[String],
) -> Result<Vec<ExternalStreamSpec>, ServerError> {
    if path_specs_use_object_store(function_name, path_specs)? {
        let objects = expand_object_store_specs(path_specs)
            .map_err(|err| ServerError::CopyFormat(format!("{function_name}: {err}")))?;
        return Ok(objects
            .into_iter()
            .map(ExternalStreamSpec::Object)
            .collect());
    }
    Ok(expand_file_path_specs(function_name, path_specs)?
        .into_iter()
        .map(ExternalStreamSpec::Local)
        .collect())
}

fn open_external_stream(
    function_name: &str,
    source: &ExternalStreamSpec,
) -> Result<ExternalStreamReader, ServerError> {
    match source {
        ExternalStreamSpec::Local(path) => {
            let file = open_local_external_file(function_name, path)?;
            Ok(ExternalStreamReader::File(BufReader::new(file)))
        }
        ExternalStreamSpec::Object(object) => Ok(ExternalStreamReader::Object(
            ObjectRangeReader::new(object.clone()),
        )),
    }
}

#[derive(Clone, Debug)]
struct ExternalBytes {
    display: String,
    bytes: Vec<u8>,
}

fn read_external_sources(
    function_name: &str,
    path_specs: &[String],
) -> Result<Vec<ExternalBytes>, ServerError> {
    if path_specs_use_object_store(function_name, path_specs)? {
        let objects = expand_object_store_specs(path_specs)
            .map_err(|err| ServerError::CopyFormat(format!("{function_name}: {err}")))?;
        return objects
            .into_iter()
            .map(|object| {
                let display = object.display_uri();
                let bytes = read_object_bytes(&object)
                    .map_err(|err| ServerError::CopyFormat(format!("{function_name}: {err}")))?;
                Ok(ExternalBytes { display, bytes })
            })
            .collect();
    }

    let paths = expand_file_path_specs(function_name, path_specs)?;
    paths
        .into_iter()
        .map(|path| {
            let display = path.display().to_string();
            let bytes = read_local_external_file(function_name, &path)?;
            Ok(ExternalBytes { display, bytes })
        })
        .collect()
}

fn open_local_external_file(function_name: &str, path: &Path) -> Result<File, ServerError> {
    ensure_regular_external_file(function_name, path)?;
    open_regular_external_file(path).map_err(|err| {
        ServerError::CopyFormat(format!(
            "{function_name} cannot open {}: {err}",
            path.display()
        ))
    })
}

fn read_local_external_file(function_name: &str, path: &Path) -> Result<Vec<u8>, ServerError> {
    let metadata = ensure_regular_external_file(function_name, path)?;
    let limit = external_local_read_limit_bytes();
    if metadata.len() > limit {
        return Err(external_local_read_limit_error(
            function_name,
            path,
            metadata.len(),
            limit,
        ));
    }

    let file = open_regular_external_file(path).map_err(|err| {
        ServerError::CopyFormat(format!(
            "{function_name} cannot open {}: {err}",
            path.display()
        ))
    })?;
    let mut limited = file.take(external_local_take_limit(function_name, path, limit)?);
    let mut bytes = Vec::new();
    limited.read_to_end(&mut bytes).map_err(|err| {
        ServerError::CopyFormat(format!(
            "{function_name} cannot read {}: {err}",
            path.display()
        ))
    })?;
    let bytes_read = external_local_bytes_read_len(function_name, path, bytes.len())?;
    if bytes_read > limit {
        return Err(external_local_read_limit_error(
            function_name,
            path,
            bytes_read,
            limit,
        ));
    }
    Ok(bytes)
}

fn ensure_regular_external_file(
    function_name: &str,
    path: &Path,
) -> Result<fs::Metadata, ServerError> {
    let metadata = fs::symlink_metadata(path).map_err(|err| {
        ServerError::CopyFormat(format!(
            "{function_name} cannot inspect {}: {err}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_file() {
        Ok(metadata)
    } else {
        Err(ServerError::CopyFormat(format!(
            "{function_name} path is not a regular file: {}",
            path.display()
        )))
    }
}

fn open_regular_external_file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if metadata.file_type().is_file() {
        Ok(file)
    } else {
        Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("path is not a regular file: {}", path.display()),
        ))
    }
}

fn external_local_read_limit_bytes() -> u64 {
    std::env::var(EXTERNAL_LOCAL_READ_LIMIT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|&limit| limit > 0)
        .unwrap_or(DEFAULT_EXTERNAL_LOCAL_READ_LIMIT_BYTES)
}

fn external_local_take_limit(
    function_name: &str,
    path: &Path,
    limit: u64,
) -> Result<u64, ServerError> {
    limit.checked_add(1).ok_or_else(|| {
        ServerError::CopyFormat(format!(
            "{function_name} local file read limit is too large: path={} limit={} env={}",
            path.display(),
            limit,
            EXTERNAL_LOCAL_READ_LIMIT_ENV
        ))
    })
}

fn external_local_bytes_read_len(
    function_name: &str,
    path: &Path,
    len: usize,
) -> Result<u64, ServerError> {
    u64::try_from(len).map_err(|_| {
        ServerError::CopyFormat(format!(
            "{function_name} local file byte count exceeds u64: path={} bytes={len}",
            path.display()
        ))
    })
}

fn external_local_read_limit_error(
    function_name: &str,
    path: &Path,
    bytes: u64,
    limit: u64,
) -> ServerError {
    ServerError::CopyFormat(format!(
        "{function_name} file exceeds read limit: path={} bytes={} limit={} env={}",
        path.display(),
        bytes,
        limit,
        EXTERNAL_LOCAL_READ_LIMIT_ENV
    ))
}

fn path_specs_use_object_store(
    function_name: &str,
    path_specs: &[String],
) -> Result<bool, ServerError> {
    let object_count = path_specs
        .iter()
        .filter(|spec| is_object_store_uri(spec))
        .count();
    if object_count == 0 {
        return Ok(false);
    }
    if object_count == path_specs.len() {
        return Ok(true);
    }
    Err(ServerError::CopyFormat(format!(
        "{function_name}: cannot mix local and object-store paths"
    )))
}

fn expand_file_path_specs(
    function_name: &str,
    patterns: &[String],
) -> Result<Vec<PathBuf>, ServerError> {
    if patterns.is_empty() {
        return Err(ServerError::CopyFormat(format!(
            "{function_name}: path list cannot be empty"
        )));
    }
    let mut paths = Vec::new();
    for pattern in patterns {
        paths.extend(expand_file_paths(function_name, pattern)?);
    }
    Ok(paths)
}

fn expand_file_paths(function_name: &str, pattern: &str) -> Result<Vec<PathBuf>, ServerError> {
    let path = Path::new(pattern);
    let file_pattern = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            ServerError::CopyFormat(format!(
                "{function_name}: path must name a file or wildcard: {pattern}"
            ))
        })?;
    if !contains_wildcard(file_pattern) {
        return Ok(vec![path.to_path_buf()]);
    }
    validate_wildcard_pattern_len(function_name, file_pattern)?;

    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut paths = Vec::new();
    for entry in fs::read_dir(parent).map_err(|err| {
        ServerError::CopyFormat(format!(
            "{function_name}: cannot read directory {}: {err}",
            parent.display()
        ))
    })? {
        let entry =
            entry.map_err(|err| ServerError::CopyFormat(format!("{function_name}: {err}")))?;
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
            "{function_name}: pattern matched no files: {pattern}"
        )));
    }
    Ok(paths)
}

fn contains_wildcard(s: &str) -> bool {
    s.chars().any(|ch| matches!(ch, '*' | '?'))
}

fn validate_wildcard_pattern_len(
    function_name: &str,
    file_pattern: &str,
) -> Result<(), ServerError> {
    let pattern_chars = file_pattern.chars().count();
    if pattern_chars > MAX_LOCAL_WILDCARD_PATTERN_CHARS {
        return Err(ServerError::CopyFormat(format!(
            "{function_name}: wildcard pattern too long: chars={pattern_chars} limit={MAX_LOCAL_WILDCARD_PATTERN_CHARS}"
        )));
    }
    Ok(())
}

fn advance_index(index: &mut usize) -> bool {
    let Some(next) = index.checked_add(1) else {
        return false;
    };
    *index = next;
    true
}

fn wildcard_match(pattern: &str, text: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let text = text.chars().collect::<Vec<_>>();

    let mut pattern_idx = 0;
    let mut text_idx = 0;
    let mut last_star = None;
    let mut star_text_idx = 0;

    while let Some(&text_ch) = text.get(text_idx) {
        match pattern.get(pattern_idx).copied() {
            Some('?') => {
                if !advance_index(&mut pattern_idx) || !advance_index(&mut text_idx) {
                    return false;
                }
            }
            Some('*') => {
                last_star = Some(pattern_idx);
                if !advance_index(&mut pattern_idx) {
                    return false;
                }
                star_text_idx = text_idx;
            }
            Some(pattern_ch) if pattern_ch == text_ch => {
                if !advance_index(&mut pattern_idx) || !advance_index(&mut text_idx) {
                    return false;
                }
            }
            _ => {
                let Some(star_idx) = last_star else {
                    return false;
                };
                let Some(next_pattern_idx) = star_idx.checked_add(1) else {
                    return false;
                };
                pattern_idx = next_pattern_idx;
                if !advance_index(&mut star_text_idx) {
                    return false;
                }
                text_idx = star_text_idx;
            }
        }
    }

    while matches!(pattern.get(pattern_idx), Some('*')) {
        if !advance_index(&mut pattern_idx) {
            return false;
        }
    }
    pattern_idx == pattern.len()
}

type JsonObject = JsonMap<String, JsonValue>;

#[derive(Debug)]
struct JsonTableScan {
    schema: Schema,
    columns: Vec<JsonColumnSpec>,
    readers: VecDeque<JsonReaderState>,
    kind: JsonInputKind,
}

impl JsonTableScan {
    fn from_path_specs(
        function_name: &str,
        path_specs: &[String],
        kind: JsonInputKind,
    ) -> Result<Self, ServerError> {
        let sources = external_stream_specs(function_name, path_specs)?;
        let columns = infer_json_columns_from_streams(function_name, kind, &sources)?;
        let schema = json_schema(function_name, &columns)?;
        let readers = sources
            .iter()
            .map(|source| JsonReaderState::open(function_name, kind, source))
            .collect::<Result<VecDeque<_>, _>>()?;
        Ok(Self {
            schema,
            columns,
            readers,
            kind,
        })
    }

    fn next_json_row(&mut self) -> Result<Option<JsonObject>, ServerError> {
        loop {
            let Some(reader) = self.readers.front_mut() else {
                return Ok(None);
            };
            match reader.next_object(self.kind)? {
                Some(row) => return Ok(Some(row)),
                None => {
                    self.readers.pop_front();
                }
            }
        }
    }
}

impl Operator for JsonTableScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        let mut rows = Vec::with_capacity(EXTERNAL_BATCH_TARGET_ROWS);
        for _ in 0..EXTERNAL_BATCH_TARGET_ROWS {
            let Some(row) = self.next_json_row().map_err(|err| {
                ExecError::TypeMismatch(format!("{} stream: {err}", self.kind.function_name()))
            })?
            else {
                break;
            };
            rows.push(row);
        }
        if rows.is_empty() {
            return Ok(None);
        }
        json_batch(self.kind.function_name(), &self.columns, &rows)
            .map(Some)
            .map_err(|err| ExecError::TypeMismatch(err.to_string()))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[derive(Debug)]
struct JsonReaderState {
    display: String,
    reader: JsonRecordReader,
}

impl JsonReaderState {
    fn open(
        function_name: &str,
        kind: JsonInputKind,
        source: &ExternalStreamSpec,
    ) -> Result<Self, ServerError> {
        let display = source.display();
        let stream = open_external_stream(function_name, source)?;
        Ok(Self {
            display,
            reader: JsonRecordReader::new(kind, stream),
        })
    }

    fn next_object(&mut self, kind: JsonInputKind) -> Result<Option<JsonObject>, ServerError> {
        match self.reader.next_text(&self.display)? {
            Some((row_number, text)) => {
                let value = serde_json::from_str::<JsonValue>(&text).map_err(|err| {
                    ServerError::CopyFormat(format!(
                        "{} parse {} row {}: {err}",
                        kind.function_name(),
                        self.display,
                        row_number
                    ))
                })?;
                json_value_to_object(kind.function_name(), &self.display, row_number, value)
                    .map(Some)
            }
            None => Ok(None),
        }
    }
}

#[derive(Debug)]
enum JsonRecordReader {
    Ndjson {
        reader: ExternalStreamReader,
        line_number: usize,
    },
    Json {
        reader: ExternalStreamReader,
        state: JsonDocumentState,
        row_number: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonDocumentState {
    Start,
    Array,
    Done,
}

impl JsonRecordReader {
    fn new(kind: JsonInputKind, reader: ExternalStreamReader) -> Self {
        match kind {
            JsonInputKind::Ndjson => Self::Ndjson {
                reader,
                line_number: 0,
            },
            JsonInputKind::Json => Self::Json {
                reader,
                state: JsonDocumentState::Start,
                row_number: 0,
            },
        }
    }

    fn next_text(&mut self, display: &str) -> Result<Option<(usize, String)>, ServerError> {
        match self {
            Self::Ndjson {
                reader,
                line_number,
            } => next_ndjson_text(reader, line_number, display),
            Self::Json {
                reader,
                state,
                row_number,
            } => next_json_document_text(reader, state, row_number, display),
        }
    }
}

fn next_ndjson_text(
    reader: &mut ExternalStreamReader,
    line_number: &mut usize,
    display: &str,
) -> Result<Option<(usize, String)>, ServerError> {
    loop {
        let Some(line) = read_bounded_json_line(reader, display)? else {
            return Ok(None);
        };
        *line_number = line_number.saturating_add(1);
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            return Ok(Some((*line_number, trimmed.to_owned())));
        }
    }
}

fn next_json_document_text(
    reader: &mut ExternalStreamReader,
    state: &mut JsonDocumentState,
    row_number: &mut usize,
    display: &str,
) -> Result<Option<(usize, String)>, ServerError> {
    loop {
        match state {
            JsonDocumentState::Start => {
                let Some(byte) = read_non_ws_byte(reader, display)? else {
                    return Ok(None);
                };
                match byte {
                    b'{' => {
                        *state = JsonDocumentState::Done;
                        *row_number = 1;
                        return read_json_container(reader, byte, display)
                            .map(|text| Some((*row_number, text)));
                    }
                    b'[' => *state = JsonDocumentState::Array,
                    other => {
                        return Err(ServerError::CopyFormat(format!(
                            "read_json expected object or array of objects in {display}, got byte {other}"
                        )));
                    }
                }
            }
            JsonDocumentState::Array => {
                let Some(byte) = read_non_ws_byte(reader, display)? else {
                    return Err(ServerError::CopyFormat(format!(
                        "read_json array in {display} ended before closing bracket"
                    )));
                };
                match byte {
                    b']' => {
                        *state = JsonDocumentState::Done;
                        return Ok(None);
                    }
                    b',' => {}
                    b'{' => {
                        *row_number = row_number.saturating_add(1);
                        return read_json_container(reader, byte, display)
                            .map(|text| Some((*row_number, text)));
                    }
                    other => {
                        return Err(ServerError::CopyFormat(format!(
                            "read_json expected object in array {display}, got byte {other}"
                        )));
                    }
                }
            }
            JsonDocumentState::Done => return Ok(None),
        }
    }
}

fn read_non_ws_byte(
    reader: &mut ExternalStreamReader,
    display: &str,
) -> Result<Option<u8>, ServerError> {
    let mut buf = [0_u8; 1];
    loop {
        let read = reader.read(&mut buf).map_err(|err| {
            ServerError::CopyFormat(format!("read_json cannot read {display}: {err}"))
        })?;
        if read == 0 {
            return Ok(None);
        }
        if !buf[0].is_ascii_whitespace() {
            return Ok(Some(buf[0]));
        }
    }
}

fn read_json_container(
    reader: &mut ExternalStreamReader,
    first: u8,
    display: &str,
) -> Result<String, ServerError> {
    let limit = checked_json_record_limit("read_json", display, json_record_read_limit_bytes())?;
    let mut bytes = vec![first];
    let mut depth = 1_i32;
    let mut in_string = false;
    let mut escaped = false;
    let mut byte = [0_u8; 1];
    while depth > 0 {
        let read = reader.read(&mut byte).map_err(|err| {
            ServerError::CopyFormat(format!("read_json cannot read {display}: {err}"))
        })?;
        if read == 0 {
            return Err(ServerError::CopyFormat(format!(
                "read_json object in {display} ended before closing brace"
            )));
        }
        let b = byte[0];
        let next_len_u64 = checked_json_record_len("read_json", display, bytes.len(), 1, limit)?;
        if next_len_u64 > limit {
            return Err(json_record_limit_error(
                "read_json",
                display,
                next_len_u64,
                limit,
            ));
        }
        bytes.push(b);
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => depth = depth.saturating_add(1),
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    String::from_utf8(bytes)
        .map_err(|err| ServerError::CopyFormat(format!("read_json cannot decode {display}: {err}")))
}

fn read_bounded_json_line(
    reader: &mut ExternalStreamReader,
    display: &str,
) -> Result<Option<String>, ServerError> {
    let limit = checked_json_record_limit("read_ndjson", display, json_record_read_limit_bytes())?;
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf().map_err(|err| {
            ServerError::CopyFormat(format!("read_ndjson cannot read {display}: {err}"))
        })?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Ok(None);
            }
            break;
        }
        let newline_pos = available.iter().position(|&byte| byte == b'\n');
        let take = if let Some(idx) = newline_pos {
            idx.checked_add(1).ok_or_else(|| {
                ServerError::CopyFormat(format!(
                    "read_ndjson record length overflow: path={display}"
                ))
            })?
        } else {
            available.len()
        };
        let next_len_u64 =
            checked_json_record_len("read_ndjson", display, bytes.len(), take, limit)?;
        if next_len_u64 > limit {
            return Err(json_record_limit_error(
                "read_ndjson",
                display,
                next_len_u64,
                limit,
            ));
        }
        bytes.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline_pos.is_some() {
            break;
        }
    }
    String::from_utf8(bytes).map(Some).map_err(|err| {
        ServerError::CopyFormat(format!("read_ndjson cannot decode {display}: {err}"))
    })
}

fn json_record_read_limit_bytes() -> u64 {
    std::env::var(JSON_RECORD_READ_LIMIT_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|&limit| limit > 0)
        .unwrap_or(DEFAULT_JSON_RECORD_READ_LIMIT_BYTES)
}

fn checked_json_record_limit(
    function_name: &str,
    display: &str,
    limit: u64,
) -> Result<u64, ServerError> {
    if limit == u64::MAX {
        return Err(ServerError::CopyFormat(format!(
            "{function_name} record read limit is too large: path={display} limit={limit} env={JSON_RECORD_READ_LIMIT_ENV}"
        )));
    }
    Ok(limit)
}

fn checked_json_record_len(
    function_name: &str,
    display: &str,
    current: usize,
    added: usize,
    limit: u64,
) -> Result<u64, ServerError> {
    let next = current.checked_add(added).ok_or_else(|| {
        ServerError::CopyFormat(format!(
            "{function_name} record length overflow: path={display}"
        ))
    })?;
    u64::try_from(next).map_err(|_| {
        ServerError::CopyFormat(format!(
            "{function_name} record byte count exceeds u64: path={display} bytes={next} limit={limit} env={JSON_RECORD_READ_LIMIT_ENV}"
        ))
    })
}

fn json_record_limit_error(
    function_name: &str,
    display: &str,
    bytes: u64,
    limit: u64,
) -> ServerError {
    ServerError::CopyFormat(format!(
        "{function_name} record exceeds read limit: path={display} bytes={bytes} limit={limit} env={JSON_RECORD_READ_LIMIT_ENV}"
    ))
}

fn infer_json_columns_from_streams(
    function_name: &str,
    kind: JsonInputKind,
    sources: &[ExternalStreamSpec],
) -> Result<Vec<JsonColumnSpec>, ServerError> {
    let mut acc = JsonSchemaAccumulator::default();
    for source in sources {
        let mut reader = JsonReaderState::open(function_name, kind, source)?;
        while let Some(row) = reader.next_object(kind)? {
            acc.observe(function_name, &row)?;
        }
    }
    Ok(acc.finish())
}

#[derive(Debug, Default)]
struct JsonSchemaAccumulator {
    columns: BTreeMap<String, JsonColumnSpec>,
    present: BTreeMap<String, usize>,
    rows: usize,
}

impl JsonSchemaAccumulator {
    fn observe(&mut self, function_name: &str, row: &JsonObject) -> Result<(), ServerError> {
        self.rows = self.rows.saturating_add(1);
        for (name, value) in row {
            if name.is_empty() {
                return Err(ServerError::CopyFormat(format!(
                    "{function_name}: JSON object contains an empty column name"
                )));
            }
            let kind = json_value_kind(value);
            let nullable = value.is_null();
            self.columns
                .entry(name.clone())
                .and_modify(|spec| {
                    spec.kind = widen_json_kind(spec.kind, kind);
                    spec.nullable |= nullable;
                })
                .or_insert_with(|| JsonColumnSpec {
                    name: name.clone(),
                    kind,
                    nullable,
                });
            *self.present.entry(name.clone()).or_insert(0) += 1;
        }
        Ok(())
    }

    fn finish(mut self) -> Vec<JsonColumnSpec> {
        for spec in self.columns.values_mut() {
            if self.present.get(&spec.name).copied().unwrap_or(0) < self.rows {
                spec.nullable = true;
            }
        }
        self.columns.into_values().collect()
    }
}

fn json_value_to_object(
    function_name: &str,
    display: &str,
    row_number: usize,
    value: JsonValue,
) -> Result<JsonObject, ServerError> {
    match value {
        JsonValue::Object(object) => Ok(object),
        _ => Err(ServerError::CopyFormat(format!(
            "{function_name} row {row_number} in {display} is not a JSON object"
        ))),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonColumnKind {
    Unknown,
    Bool,
    Int64,
    Float64,
    Text,
}

#[derive(Clone, Debug)]
struct JsonColumnSpec {
    name: String,
    kind: JsonColumnKind,
    nullable: bool,
}

fn json_value_kind(value: &JsonValue) -> JsonColumnKind {
    match value {
        JsonValue::Null => JsonColumnKind::Unknown,
        JsonValue::Bool(_) => JsonColumnKind::Bool,
        JsonValue::Number(number) => {
            if number.as_i64().is_some()
                || number
                    .as_u64()
                    .is_some_and(|value| i64::try_from(value).is_ok())
            {
                JsonColumnKind::Int64
            } else if number.as_f64().is_some() {
                JsonColumnKind::Float64
            } else {
                JsonColumnKind::Text
            }
        }
        JsonValue::String(_) | JsonValue::Array(_) | JsonValue::Object(_) => JsonColumnKind::Text,
    }
}

fn widen_json_kind(left: JsonColumnKind, right: JsonColumnKind) -> JsonColumnKind {
    match (left, right) {
        (JsonColumnKind::Unknown, kind) | (kind, JsonColumnKind::Unknown) => kind,
        (JsonColumnKind::Text, _) | (_, JsonColumnKind::Text) => JsonColumnKind::Text,
        (JsonColumnKind::Float64, _) | (_, JsonColumnKind::Float64) => JsonColumnKind::Float64,
        (JsonColumnKind::Int64, JsonColumnKind::Int64) => JsonColumnKind::Int64,
        (JsonColumnKind::Bool, JsonColumnKind::Bool) => JsonColumnKind::Bool,
        _ => JsonColumnKind::Text,
    }
}

fn json_schema(function_name: &str, columns: &[JsonColumnSpec]) -> Result<Schema, ServerError> {
    let fields = columns
        .iter()
        .map(|column| {
            let data_type = match column.kind {
                JsonColumnKind::Unknown => DataType::Text { max_len: None },
                JsonColumnKind::Bool => DataType::Bool,
                JsonColumnKind::Int64 => DataType::Int64,
                JsonColumnKind::Float64 => DataType::Float64,
                JsonColumnKind::Text => DataType::Text { max_len: None },
            };
            if column.nullable {
                Field::nullable(column.name.clone(), data_type)
            } else {
                Field::required(column.name.clone(), data_type)
            }
        })
        .collect::<Vec<_>>();
    Schema::new(fields)
        .map_err(|err| ServerError::CopyFormat(format!("{function_name} schema: {err}")))
}

fn json_batch(
    function_name: &str,
    columns: &[JsonColumnSpec],
    rows: &[JsonObject],
) -> Result<Batch, ServerError> {
    let mut batch_columns = Vec::with_capacity(columns.len());
    for column in columns {
        batch_columns.push(json_column(function_name, column, rows)?);
    }
    Batch::new(batch_columns)
        .map_err(|err| ServerError::CopyFormat(format!("{function_name} batch: {err}")))
}

fn json_column(
    function_name: &str,
    column: &JsonColumnSpec,
    rows: &[JsonObject],
) -> Result<Column, ServerError> {
    let mut validity = Bitmap::new(rows.len(), true);
    match column.kind {
        JsonColumnKind::Unknown | JsonColumnKind::Text => {
            let mut values = Vec::with_capacity(rows.len());
            for (idx, row) in rows.iter().enumerate() {
                match row.get(&column.name) {
                    Some(JsonValue::Null) | None => {
                        values.push(String::new());
                        validity.set(idx, false);
                    }
                    Some(JsonValue::String(value)) => values.push(value.clone()),
                    Some(value) => values.push(value.to_string()),
                }
            }
            string_column(function_name, values, validity)
        }
        JsonColumnKind::Bool => {
            let mut values = Vec::with_capacity(rows.len());
            for (idx, row) in rows.iter().enumerate() {
                if let Some(value) = row.get(&column.name).and_then(JsonValue::as_bool) {
                    values.push(value);
                } else {
                    values.push(false);
                    validity.set(idx, false);
                }
            }
            bool_column(function_name, values, validity)
        }
        JsonColumnKind::Int64 => {
            let mut values = Vec::with_capacity(rows.len());
            for (idx, row) in rows.iter().enumerate() {
                if let Some(value) = row.get(&column.name).and_then(json_i64) {
                    values.push(value);
                } else {
                    values.push(0_i64);
                    validity.set(idx, false);
                }
            }
            i64_column(function_name, values, validity)
        }
        JsonColumnKind::Float64 => {
            let mut values = Vec::with_capacity(rows.len());
            for (idx, row) in rows.iter().enumerate() {
                if let Some(value) = row.get(&column.name).and_then(json_f64) {
                    values.push(value);
                } else {
                    values.push(0.0_f64);
                    validity.set(idx, false);
                }
            }
            f64_column(function_name, values, validity)
        }
    }
}

fn json_i64(value: &JsonValue) -> Option<i64> {
    let number = value.as_number()?;
    number
        .as_i64()
        .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn json_f64(value: &JsonValue) -> Option<f64> {
    value.as_number()?.as_f64()
}

fn bool_column(
    function_name: &str,
    values: Vec<bool>,
    validity: Bitmap,
) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Bool(BoolColumn::from_data(values)))
    } else {
        BoolColumn::with_nulls(values, validity)
            .map(Column::Bool)
            .map_err(|err| ServerError::CopyFormat(format!("{function_name} bool column: {err}")))
    }
}

fn i64_column(
    function_name: &str,
    values: Vec<i64>,
    validity: Bitmap,
) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Int64(NumericColumn::from_data(values)))
    } else {
        NumericColumn::with_nulls(values, validity)
            .map(Column::Int64)
            .map_err(|err| ServerError::CopyFormat(format!("{function_name} int64 column: {err}")))
    }
}

fn f64_column(
    function_name: &str,
    values: Vec<f64>,
    validity: Bitmap,
) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Float64(NumericColumn::from_data(values)))
    } else {
        NumericColumn::with_nulls(values, validity)
            .map(Column::Float64)
            .map_err(|err| {
                ServerError::CopyFormat(format!("{function_name} float64 column: {err}"))
            })
    }
}

fn string_column(
    function_name: &str,
    values: Vec<String>,
    validity: Bitmap,
) -> Result<Column, ServerError> {
    if validity.count_ones() == validity.len() {
        Ok(Column::Utf8(StringColumn::from_data(values)))
    } else {
        StringColumn::with_nulls(values, validity)
            .map(Column::Utf8)
            .map_err(|err| ServerError::CopyFormat(format!("{function_name} text column: {err}")))
    }
}

fn read_arrow_batches(sources: &[ExternalBytes]) -> Result<(Schema, VecDeque<Batch>), ServerError> {
    let mut expected_schema: Option<arrow_schema::SchemaRef> = None;
    let mut batches = VecDeque::new();

    for source in sources {
        let cursor = Cursor::new(source.bytes.clone());
        let reader = ArrowFileReader::try_new(cursor, None).map_err(|err| {
            ServerError::CopyFormat(format!(
                "read_arrow cannot inspect {}: {err}",
                source.display
            ))
        })?;
        let arrow_schema = reader.schema();
        if let Some(expected) = &expected_schema {
            if arrow_schema.as_ref() != expected.as_ref() {
                return Err(ServerError::CopyFormat(format!(
                    "read_arrow schema mismatch in {}",
                    source.display
                )));
            }
        } else {
            expected_schema = Some(arrow_schema);
        }

        for batch in reader {
            let batch = batch.map_err(|err| {
                ServerError::CopyFormat(format!("read_arrow read {}: {err}", source.display))
            })?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = record_batch_to_ultrasql_batch(batch).map_err(|err| {
                ServerError::CopyFormat(format!("read_arrow Arrow bridge: {err}"))
            })?;
            batches.push_back(batch);
        }
    }

    let Some(arrow_schema) = expected_schema else {
        return Err(ServerError::CopyFormat(
            "read_arrow path list cannot be empty".to_owned(),
        ));
    };
    let schema = schema_from_arrow(arrow_schema.as_ref())
        .map_err(|err| ServerError::CopyFormat(format!("read_arrow Arrow bridge: {err}")))?;
    Ok((schema, batches))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[derive(Debug)]
    struct CountingScan {
        schema: Schema,
        pulls: Arc<AtomicUsize>,
    }

    impl CountingScan {
        fn new(pulls: Arc<AtomicUsize>) -> Self {
            Self {
                schema: Schema::new([Field::required("id", DataType::Int64)]).expect("test schema"),
                pulls,
            }
        }
    }

    impl Operator for CountingScan {
        fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
            let previous = self.pulls.fetch_add(1, Ordering::SeqCst);
            if previous > 0 {
                return Ok(None);
            }
            Batch::new([Column::Int64(NumericColumn::from_data(vec![1_i64, 2]))])
                .map(Some)
                .map_err(ExecError::from)
        }

        fn schema(&self) -> &Schema {
            &self.schema
        }
    }

    #[test]
    fn streaming_source_is_not_drained_at_construction() {
        let pulls = Arc::new(AtomicUsize::new(0));
        let child = CountingScan::new(Arc::clone(&pulls));
        let mut scan = ExternalTableScan::streaming(Box::new(child));

        assert_eq!(pulls.load(Ordering::SeqCst), 0);
        let batch = scan
            .next_batch()
            .expect("stream next")
            .expect("first batch");
        assert_eq!(batch.rows(), 2);
        assert_eq!(pulls.load(Ordering::SeqCst), 1);
        assert!(scan.next_batch().expect("stream eof").is_none());
    }

    #[test]
    fn external_glob_rejects_oversized_wildcard_pattern() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pattern = dir.path().join(format!("{}*.json", "x".repeat(4096)));

        let err = super::expand_file_paths("read_json", &pattern.to_string_lossy())
            .expect_err("oversized wildcard pattern must fail before directory scan");

        assert!(
            err.to_string().contains("wildcard pattern too long"),
            "unexpected error: {err}"
        );
    }

    fn text_lit(value: &str) -> ScalarExpr {
        ScalarExpr::Literal {
            value: Value::Text(value.to_owned()),
            data_type: DataType::Text { max_len: None },
        }
    }

    #[test]
    fn read_json_uses_streaming_scan_source() {
        let _env_guard = external_env_test_lock();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("people.json");
        fs::write(&path, r#"[{"id":1,"name":"Ada"},{"id":2,"name":"Grace"}]"#).expect("write json");

        let scan = ExternalTableScan::from_json(
            &[text_lit(path.to_str().expect("utf8 path"))],
            JsonInputKind::Json,
        )
        .expect("json scan");

        assert!(matches!(scan.source, ExternalScanSource::Streaming(_)));
    }

    #[test]
    fn read_json_rejects_configured_oversized_record() {
        let _env_guard = external_env_test_lock();
        // SAFETY: external_env_test_lock serializes process-env mutation in
        // this module's tests.
        unsafe {
            std::env::set_var("ULTRASQL_JSON_RECORD_LIMIT_BYTES", "3");
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("people.json");
        fs::write(&path, r#"[{"id":1}]"#).expect("write json");

        let err = ExternalTableScan::from_json(
            &[text_lit(path.to_str().expect("utf8 path"))],
            JsonInputKind::Json,
        )
        .expect_err("oversized json record rejected");

        assert!(err.to_string().contains("record exceeds read limit"));

        // SAFETY: external_env_test_lock serializes process-env mutation in
        // this module's tests.
        unsafe {
            std::env::remove_var("ULTRASQL_JSON_RECORD_LIMIT_BYTES");
        }
    }

    #[test]
    fn read_json_rejects_unbounded_record_limit() {
        let _env_guard = external_env_test_lock();
        // SAFETY: external_env_test_lock serializes process-env mutation in
        // this module's tests.
        unsafe {
            std::env::set_var(JSON_RECORD_READ_LIMIT_ENV, u64::MAX.to_string());
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("people.json");
        fs::write(&path, r#"[{"id":1}]"#).expect("write json");

        let err = ExternalTableScan::from_json(
            &[text_lit(path.to_str().expect("utf8 path"))],
            JsonInputKind::Json,
        )
        .expect_err("unbounded json record limit rejected");

        assert!(err.to_string().contains("record read limit is too large"));

        // SAFETY: external_env_test_lock serializes process-env mutation in
        // this module's tests.
        unsafe {
            std::env::remove_var(JSON_RECORD_READ_LIMIT_ENV);
        }
    }

    #[cfg(unix)]
    #[test]
    fn external_local_sources_reject_symlinked_files() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("people.json");
        let link = dir.path().join("link.json");
        fs::write(&target, r#"[{"id":1}]"#).expect("write json");
        symlink(&target, &link).expect("symlink json");

        let source = ExternalStreamSpec::Local(link.clone());
        assert!(open_external_stream("read_json", &source).is_err());
        assert!(read_external_sources("read_json", &[link.to_string_lossy().to_string()]).is_err());
    }

    #[test]
    fn external_local_sources_reject_configured_oversized_files() {
        let _env_guard = external_env_test_lock();
        // SAFETY: external_env_test_lock serializes process-env mutation in
        // this module's tests.
        unsafe {
            std::env::set_var("ULTRASQL_EXTERNAL_LOCAL_READ_LIMIT_BYTES", "3");
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("oversized.arrow");
        fs::write(&path, b"abcd").expect("write file");

        let err = read_external_sources("read_arrow", &[path.to_string_lossy().to_string()])
            .expect_err("oversized local file rejected");

        assert!(err.to_string().contains("exceeds read limit"));

        // SAFETY: external_env_test_lock serializes process-env mutation in
        // this module's tests.
        unsafe {
            std::env::remove_var("ULTRASQL_EXTERNAL_LOCAL_READ_LIMIT_BYTES");
        }
    }

    #[test]
    fn external_local_sources_reject_unbounded_read_limit() {
        let _env_guard = external_env_test_lock();
        // SAFETY: external_env_test_lock serializes process-env mutation in
        // this module's tests.
        unsafe {
            std::env::set_var(EXTERNAL_LOCAL_READ_LIMIT_ENV, u64::MAX.to_string());
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("input.arrow");
        fs::write(&path, b"ok").expect("write file");

        let err = read_external_sources("read_arrow", &[path.to_string_lossy().to_string()])
            .expect_err("unbounded local file limit rejected");

        assert!(
            err.to_string()
                .contains("local file read limit is too large")
        );

        // SAFETY: external_env_test_lock serializes process-env mutation in
        // this module's tests.
        unsafe {
            std::env::remove_var(EXTERNAL_LOCAL_READ_LIMIT_ENV);
        }
    }

    #[test]
    fn read_ndjson_uses_streaming_scan_source() {
        let _env_guard = external_env_test_lock();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("people.ndjson");
        fs::write(
            &path,
            "{\"id\":1,\"name\":\"Ada\"}\n{\"id\":2,\"name\":\"Grace\"}\n",
        )
        .expect("write ndjson");

        let scan = ExternalTableScan::from_json(
            &[text_lit(path.to_str().expect("utf8 path"))],
            JsonInputKind::Ndjson,
        )
        .expect("ndjson scan");

        assert!(matches!(scan.source, ExternalScanSource::Streaming(_)));
    }

    fn external_env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn function_scan(name: &str, args: Vec<ScalarExpr>) -> LogicalPlan {
        LogicalPlan::FunctionScan {
            name: name.to_owned(),
            args,
            schema: Schema::empty(),
        }
    }

    #[test]
    fn local_path_external_function_is_flagged_local() {
        for name in [
            "read_csv",
            "read_parquet",
            "read_json",
            "read_ndjson",
            "read_arrow",
            "read_iceberg",
            "iceberg_scan",
        ] {
            assert!(
                function_scan_reads_local_file(name, &[text_lit("/etc/passwd")]).expect("probe ok"),
                "{name} on a local path must be flagged as a local read"
            );
        }
    }

    #[test]
    fn object_store_path_external_function_is_not_local() {
        for name in ["read_csv", "read_parquet", "read_json", "read_iceberg"] {
            assert!(
                !function_scan_reads_local_file(name, &[text_lit("s3://bucket/data.bin")])
                    .expect("probe ok"),
                "{name} on an s3:// URI must not be flagged as a local read"
            );
        }
    }

    #[test]
    fn sniff_csv_is_always_local() {
        assert!(
            function_scan_reads_local_file("sniff_csv", &[text_lit("/tmp/data.csv")])
                .expect("probe ok"),
            "sniff_csv is local-only and must always be flagged"
        );
        // Even an s3-looking argument is local: sniff_csv has no remote branch.
        assert!(
            function_scan_reads_local_file("sniff_csv", &[text_lit("s3://bucket/data.csv")])
                .expect("probe ok"),
        );
    }

    #[test]
    fn non_file_function_is_not_local() {
        assert!(
            !function_scan_reads_local_file("generate_series", &[]).expect("probe ok"),
            "generate_series is not a server-file reader"
        );
        assert!(!function_scan_reads_local_file("unnest", &[]).expect("probe ok"),);
    }

    #[test]
    fn read_csv_with_reject_path_still_flags_local() {
        // The trailing reject-path argument must not confuse the local probe.
        assert!(
            function_scan_reads_local_file(
                "read_csv",
                &[text_lit("/data/in.csv"), text_lit("/tmp/rejects")],
            )
            .expect("probe ok"),
        );
    }

    #[test]
    fn plan_walk_finds_local_read_under_filter_and_join() {
        // FunctionScan nested under Filter -> still detected.
        let filtered = LogicalPlan::Filter {
            input: Box::new(function_scan(
                "read_parquet",
                vec![text_lit("/srv/data.parquet")],
            )),
            predicate: ScalarExpr::Literal {
                value: Value::Bool(true),
                data_type: DataType::Bool,
            },
        };
        assert!(plan_reads_local_external_file(&filtered).expect("walk ok"));

        // One side of a join reads remote, the other local -> detected.
        let join = LogicalPlan::Join {
            left: Box::new(function_scan(
                "read_csv",
                vec![text_lit("s3://bucket/a.csv")],
            )),
            right: Box::new(function_scan("read_csv", vec![text_lit("/local/b.csv")])),
            join_type: ultrasql_planner::LogicalJoinType::Cross,
            condition: ultrasql_planner::LogicalJoinCondition::None,
            schema: Schema::empty(),
        };
        assert!(plan_reads_local_external_file(&join).expect("walk ok"));
    }

    #[test]
    fn plan_walk_ignores_remote_only_reads() {
        let remote = function_scan("read_parquet", vec![text_lit("s3://bucket/data.parquet")]);
        assert!(!plan_reads_local_external_file(&remote).expect("walk ok"));
    }
}
