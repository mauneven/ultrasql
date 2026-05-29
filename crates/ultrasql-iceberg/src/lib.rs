//! Read-only Apache Iceberg metadata planning.
//!
//! This crate resolves a table root or metadata JSON file into the current
//! snapshot's live Parquet data files. It intentionally does not perform
//! writes or catalog commits; those belong in later slices.

use std::fs;
use std::io::Cursor;
use std::io::ErrorKind;
use std::io::Read;
use std::path::{Path, PathBuf};

use apache_avro::{Reader, types::Value as AvroValue};
use serde::Deserialize;
use ultrasql_core::{DataType, Field, Schema};
use ultrasql_objectstore::{expand_object_store_specs, is_object_store_uri, read_object_bytes};

const DEFAULT_ICEBERG_LOCAL_READ_LIMIT_BYTES: u64 = 256 * 1024 * 1024;

/// Result type for Iceberg metadata planning.
pub type Result<T> = std::result::Result<T, IcebergError>;

/// Iceberg table metadata, manifest, or format error.
#[derive(Debug, thiserror::Error)]
pub enum IcebergError {
    /// Filesystem access failed.
    #[error("{0}")]
    Io(String),
    /// JSON metadata parse failed.
    #[error("{0}")]
    Json(String),
    /// Avro manifest read failed.
    #[error("{0}")]
    Avro(String),
    /// Iceberg metadata is malformed or incomplete.
    #[error("{0}")]
    InvalidMetadata(String),
    /// Iceberg feature is not supported by the read-only scanner yet.
    #[error("{0}")]
    Unsupported(String),
}

/// Current Iceberg snapshot scan plan.
#[derive(Clone, Debug)]
pub struct IcebergScanPlan {
    /// SQL schema projected from Iceberg table metadata.
    pub schema: Schema,
    /// Live Parquet data file locations in manifest order.
    pub data_files: Vec<String>,
    /// Snapshot id selected for the scan, or `None` when table has no snapshot.
    pub snapshot_id: Option<i64>,
    /// Data-content manifests read after manifest-list pruning.
    pub manifests_scanned: usize,
    /// Data-content manifests skipped by manifest-list partition summaries.
    pub manifests_skipped: usize,
    /// Data files retained after partition pruning.
    pub data_files_scanned: usize,
    /// Data files skipped by data-file partition values.
    pub data_files_skipped: usize,
}

/// Iceberg scan planning options.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IcebergScanOptions {
    /// Explicit snapshot id for time-travel planning. Defaults to current snapshot.
    pub snapshot_id: Option<i64>,
    /// Optional identity-partition equality filter for pruning.
    pub partition_filter: Option<IcebergPartitionFilter>,
}

/// Identity-partition equality filter used by read-only Iceberg planning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergPartitionFilter {
    /// Partition field name from the Iceberg partition spec.
    pub field: String,
    /// Literal value to match.
    pub value: String,
}

impl IcebergPartitionFilter {
    /// Build an equality filter for an identity partition field.
    #[must_use]
    pub fn equals(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            value: value.into(),
        }
    }
}

/// Read only the current schema from an Iceberg table root or metadata file.
pub fn read_iceberg_schema(table: &str) -> Result<Schema> {
    let metadata = load_metadata(table)?;
    metadata.schema()
}

/// Plan live Parquet data files for the current Iceberg snapshot.
pub fn plan_iceberg_scan(table: &str) -> Result<IcebergScanPlan> {
    plan_iceberg_scan_with_options(table, &IcebergScanOptions::default())
}

/// Plan live Parquet data files with snapshot and partition pruning options.
pub fn plan_iceberg_scan_with_options(
    table: &str,
    options: &IcebergScanOptions,
) -> Result<IcebergScanPlan> {
    let metadata = load_metadata(table)?;
    let schema = metadata.schema()?;
    let planned_files = metadata.data_files(options)?;
    Ok(IcebergScanPlan {
        schema,
        data_files: planned_files.data_files,
        snapshot_id: planned_files.snapshot_id,
        manifests_scanned: planned_files.manifests_scanned,
        manifests_skipped: planned_files.manifests_skipped,
        data_files_scanned: planned_files.data_files_scanned,
        data_files_skipped: planned_files.data_files_skipped,
    })
}

#[derive(Debug)]
struct LoadedMetadata {
    table_root: String,
    metadata: TableMetadata,
}

impl LoadedMetadata {
    fn schema(&self) -> Result<Schema> {
        let schema = self
            .metadata
            .schemas
            .iter()
            .find(|schema| schema.schema_id == self.metadata.current_schema_id)
            .ok_or_else(|| {
                IcebergError::InvalidMetadata(format!(
                    "iceberg_scan: current schema id {} not found",
                    self.metadata.current_schema_id
                ))
            })?;
        let fields = schema
            .fields
            .iter()
            .map(|field| {
                let data_type = iceberg_type_to_sql(&field.field_type)?;
                Ok(if field.required {
                    Field::required(field.name.clone(), data_type)
                } else {
                    Field::nullable(field.name.clone(), data_type)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Schema::new(fields)
            .map_err(|err| IcebergError::InvalidMetadata(format!("iceberg_scan schema: {err}")))
    }

    fn data_files(&self, options: &IcebergScanOptions) -> Result<PlannedDataFiles> {
        let snapshot_id = options.snapshot_id.or(self.metadata.current_snapshot_id);
        let Some(snapshot_id) = snapshot_id else {
            return Ok(PlannedDataFiles::default());
        };
        let snapshot = self
            .metadata
            .snapshots
            .iter()
            .find(|snapshot| snapshot.snapshot_id == snapshot_id)
            .ok_or_else(|| {
                IcebergError::InvalidMetadata(format!(
                    "iceberg_scan: current snapshot id {snapshot_id} not found"
                ))
            })?;
        let manifest_list = resolve_location(&self.table_root, &snapshot.manifest_list);
        let manifest_entries = read_manifest_list(&manifest_list)?;
        let mut planned = PlannedDataFiles {
            snapshot_id: Some(snapshot_id),
            ..PlannedDataFiles::default()
        };
        for manifest in manifest_entries {
            if !self.manifest_matches_partition_filter(&manifest, options.partition_filter.as_ref())
            {
                planned.manifests_skipped += 1;
                continue;
            }
            planned.manifests_scanned += 1;
            let manifest_path = resolve_location(&self.table_root, &manifest.manifest_path);
            let manifest_files = read_manifest_data_files(
                &self.table_root,
                &manifest_path,
                options.partition_filter.as_ref(),
            )?;
            planned.data_files_scanned += manifest_files.scanned;
            planned.data_files_skipped += manifest_files.skipped;
            planned.data_files.extend(manifest_files.data_files);
        }
        Ok(planned)
    }

    fn manifest_matches_partition_filter(
        &self,
        manifest: &ManifestListEntry,
        filter: Option<&IcebergPartitionFilter>,
    ) -> bool {
        let Some(filter) = filter else {
            return true;
        };
        let Some(summary_index) = self.partition_field_index(manifest.partition_spec_id, filter)
        else {
            return true;
        };
        let Some(summary) = manifest.partitions.get(summary_index) else {
            return true;
        };
        let filter_value = filter.value.as_bytes();
        if let Some(lower) = summary.lower_bound.as_deref() {
            if filter_value < lower {
                return false;
            }
        }
        if let Some(upper) = summary.upper_bound.as_deref() {
            if filter_value > upper {
                return false;
            }
        }
        true
    }

    fn partition_field_index(
        &self,
        spec_id: i32,
        filter: &IcebergPartitionFilter,
    ) -> Option<usize> {
        self.metadata
            .partition_specs
            .iter()
            .find(|spec| spec.spec_id == spec_id)?
            .fields
            .iter()
            .position(|field| field.name == filter.field)
    }
}

#[derive(Debug, Default)]
struct PlannedDataFiles {
    data_files: Vec<String>,
    snapshot_id: Option<i64>,
    manifests_scanned: usize,
    manifests_skipped: usize,
    data_files_scanned: usize,
    data_files_skipped: usize,
}

#[derive(Debug, Deserialize)]
struct TableMetadata {
    #[serde(rename = "current-schema-id")]
    current_schema_id: i32,
    schemas: Vec<IcebergSchema>,
    #[serde(rename = "current-snapshot-id")]
    current_snapshot_id: Option<i64>,
    #[serde(default)]
    snapshots: Vec<IcebergSnapshot>,
    #[serde(rename = "partition-specs", default)]
    partition_specs: Vec<IcebergPartitionSpec>,
}

#[derive(Debug, Deserialize)]
struct IcebergSchema {
    #[serde(rename = "schema-id")]
    schema_id: i32,
    fields: Vec<IcebergField>,
}

#[derive(Debug, Deserialize)]
struct IcebergField {
    name: String,
    required: bool,
    #[serde(rename = "type")]
    field_type: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct IcebergSnapshot {
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(rename = "manifest-list")]
    manifest_list: String,
}

#[derive(Debug, Deserialize)]
struct IcebergPartitionSpec {
    #[serde(rename = "spec-id")]
    spec_id: i32,
    #[serde(default)]
    fields: Vec<IcebergPartitionField>,
}

#[derive(Debug, Deserialize)]
struct IcebergPartitionField {
    name: String,
}

fn load_metadata(table: &str) -> Result<LoadedMetadata> {
    let metadata_location = metadata_location_for(table)?;
    let text = String::from_utf8(read_location_bytes(&metadata_location.metadata_path)?).map_err(
        |err| {
            IcebergError::Json(format!(
                "iceberg_scan metadata {} is not UTF-8: {err}",
                metadata_location.metadata_path
            ))
        },
    )?;
    let metadata = serde_json::from_str::<TableMetadata>(&text).map_err(|err| {
        IcebergError::Json(format!(
            "iceberg_scan cannot parse {}: {err}",
            metadata_location.metadata_path
        ))
    })?;
    Ok(LoadedMetadata {
        table_root: metadata_location.table_root,
        metadata,
    })
}

#[derive(Clone, Debug)]
struct MetadataLocation {
    table_root: String,
    metadata_path: String,
}

fn metadata_location_for(table: &str) -> Result<MetadataLocation> {
    if is_object_store_uri(table) {
        if table.ends_with(".metadata.json") {
            return Ok(MetadataLocation {
                table_root: object_table_root_from_metadata(table),
                metadata_path: table.to_owned(),
            });
        }
        return Err(IcebergError::Unsupported(
            "iceberg_scan: object-store table roots require explicit metadata JSON path".to_owned(),
        ));
    }

    let path = Path::new(table);
    if local_regular_file_exists(path)? {
        return Ok(MetadataLocation {
            table_root: local_table_root_from_metadata(path).display().to_string(),
            metadata_path: path.display().to_string(),
        });
    }

    let metadata_dir = path.join("metadata");
    let metadata_path = discover_metadata_json(&metadata_dir)?;
    Ok(MetadataLocation {
        table_root: path.display().to_string(),
        metadata_path: metadata_path.display().to_string(),
    })
}

fn discover_metadata_json(metadata_dir: &Path) -> Result<PathBuf> {
    let hint = metadata_dir.join("version-hint.text");
    if let Some(text) = read_local_regular_text_if_exists(&hint)? {
        let version = text.trim().parse::<i64>().map_err(|err| {
            IcebergError::InvalidMetadata(format!(
                "iceberg_scan cannot parse {}: {err}",
                hint.display()
            ))
        })?;
        for candidate in [
            metadata_dir.join(format!("v{version}.metadata.json")),
            metadata_dir.join(format!("{version}.metadata.json")),
        ] {
            if local_regular_file_exists(&candidate)? {
                return Ok(candidate);
            }
        }
    }

    ensure_local_directory(metadata_dir)?;
    let mut candidates = Vec::new();
    for entry in fs::read_dir(metadata_dir).map_err(|err| {
        IcebergError::Io(format!(
            "iceberg_scan cannot read metadata directory {}: {err}",
            metadata_dir.display()
        ))
    })? {
        let entry = entry.map_err(|err| {
            IcebergError::Io(format!(
                "iceberg_scan cannot read metadata directory {}: {err}",
                metadata_dir.display()
            ))
        })?;
        let path = entry.path();
        if !path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".metadata.json"))
        {
            continue;
        }
        let file_type = entry.file_type().map_err(|err| {
            IcebergError::Io(format!(
                "iceberg_scan cannot inspect metadata file {}: {err}",
                path.display()
            ))
        })?;
        if file_type.is_symlink() {
            return Err(IcebergError::Io(format!(
                "iceberg_scan refuses symlinked local metadata file {}",
                path.display()
            )));
        }
        if file_type.is_file() {
            candidates.push(path);
        }
    }
    candidates.sort_by(|left, right| {
        metadata_version(left)
            .cmp(&metadata_version(right))
            .then_with(|| left.cmp(right))
    });
    candidates.pop().ok_or_else(|| {
        IcebergError::InvalidMetadata(format!(
            "iceberg_scan found no metadata JSON files in {}",
            metadata_dir.display()
        ))
    })
}

fn metadata_version(path: &Path) -> Option<i64> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_suffix(".metadata.json")?;
    let digits = stem.strip_prefix('v').unwrap_or(stem);
    digits.parse::<i64>().ok()
}

fn local_table_root_from_metadata(path: &Path) -> PathBuf {
    let Some(parent) = path.parent() else {
        return PathBuf::from(".");
    };
    if parent.file_name().and_then(|name| name.to_str()) == Some("metadata") {
        return parent.parent().unwrap_or(parent).to_path_buf();
    }
    parent.to_path_buf()
}

fn object_table_root_from_metadata(path: &str) -> String {
    let Some((root, _)) = path.rsplit_once("/metadata/") else {
        return path
            .rsplit_once('/')
            .map_or(path, |(root, _)| root)
            .to_owned();
    };
    root.to_owned()
}

#[derive(Clone, Debug, Default)]
struct ManifestListEntry {
    manifest_path: String,
    partition_spec_id: i32,
    partitions: Vec<PartitionSummary>,
}

#[derive(Clone, Debug, Default)]
struct PartitionSummary {
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
}

#[derive(Debug, Default)]
struct ManifestDataFiles {
    data_files: Vec<String>,
    scanned: usize,
    skipped: usize,
}

fn read_manifest_list(path: &str) -> Result<Vec<ManifestListEntry>> {
    let mut manifests = Vec::new();
    for value in read_avro_values(path)? {
        let content = avro_record_field(&value, "content")
            .and_then(avro_i32)
            .unwrap_or(0);
        if content != 0 {
            continue;
        }
        let manifest_path = avro_record_field(&value, "manifest_path")
            .and_then(avro_string)
            .ok_or_else(|| {
                IcebergError::InvalidMetadata(format!(
                    "iceberg_scan manifest list {path} missing manifest_path"
                ))
            })?;
        let partition_spec_id = avro_record_field(&value, "partition_spec_id")
            .and_then(avro_i32)
            .unwrap_or(0);
        let partitions = avro_record_field(&value, "partitions")
            .and_then(avro_array)
            .map(|values| values.iter().map(partition_summary_from_avro).collect())
            .unwrap_or_default();
        manifests.push(ManifestListEntry {
            manifest_path: manifest_path.to_owned(),
            partition_spec_id,
            partitions,
        });
    }
    Ok(manifests)
}

fn read_manifest_data_files(
    table_root: &str,
    path: &str,
    filter: Option<&IcebergPartitionFilter>,
) -> Result<ManifestDataFiles> {
    let mut data_files = ManifestDataFiles::default();
    for value in read_avro_values(path)? {
        let status = avro_record_field(&value, "status")
            .and_then(avro_i32)
            .ok_or_else(|| {
                IcebergError::InvalidMetadata(format!(
                    "iceberg_scan manifest {path} missing status"
                ))
            })?;
        if status == 2 {
            continue;
        }
        let data_file = avro_record_field(&value, "data_file").ok_or_else(|| {
            IcebergError::InvalidMetadata(format!("iceberg_scan manifest {path} missing data_file"))
        })?;
        let content = avro_record_field(data_file, "content")
            .and_then(avro_i32)
            .unwrap_or(0);
        if content != 0 {
            continue;
        }
        let format = avro_record_field(data_file, "file_format")
            .and_then(avro_string)
            .ok_or_else(|| {
                IcebergError::InvalidMetadata(format!(
                    "iceberg_scan manifest {path} missing data_file.file_format"
                ))
            })?;
        if !format.eq_ignore_ascii_case("PARQUET") {
            return Err(IcebergError::Unsupported(format!(
                "iceberg_scan data file format not supported: {format}"
            )));
        }
        if let Some(filter) = filter {
            if let Some(false) = partition_matches_filter(data_file, filter) {
                data_files.skipped += 1;
                continue;
            }
        }
        let file_path = avro_record_field(data_file, "file_path")
            .and_then(avro_string)
            .ok_or_else(|| {
                IcebergError::InvalidMetadata(format!(
                    "iceberg_scan manifest {path} missing data_file.file_path"
                ))
            })?;
        data_files.scanned += 1;
        data_files
            .data_files
            .push(resolve_location(table_root, file_path));
    }
    Ok(data_files)
}

fn partition_summary_from_avro(value: &AvroValue) -> PartitionSummary {
    PartitionSummary {
        lower_bound: avro_record_field(value, "lower_bound")
            .and_then(avro_bytes)
            .map(ToOwned::to_owned),
        upper_bound: avro_record_field(value, "upper_bound")
            .and_then(avro_bytes)
            .map(ToOwned::to_owned),
    }
}

fn partition_matches_filter(
    data_file: &AvroValue,
    filter: &IcebergPartitionFilter,
) -> Option<bool> {
    let partition = avro_record_field(data_file, "partition")?;
    let value = avro_record_field(partition, &filter.field)?;
    let value = avro_scalar_to_string(value)?;
    Some(value == filter.value)
}

fn read_avro_values(path: &str) -> Result<Vec<AvroValue>> {
    let bytes = read_location_bytes(path)?;
    let reader = Reader::new(Cursor::new(bytes))
        .map_err(|err| IcebergError::Avro(format!("iceberg_scan cannot open {path}: {err}")))?;
    reader
        .map(|value| {
            value.map_err(|err| {
                IcebergError::Avro(format!("iceberg_scan cannot read {path}: {err}"))
            })
        })
        .collect()
}

fn read_location_bytes(location: &str) -> Result<Vec<u8>> {
    if is_object_store_uri(location) {
        let locations = expand_object_store_specs(&[location.to_owned()])
            .map_err(|err| IcebergError::Io(format!("iceberg_scan: {err}")))?;
        let Some(location) = locations.first() else {
            return Err(IcebergError::Io(
                "iceberg_scan object location matched no objects".to_owned(),
            ));
        };
        return read_object_bytes(location)
            .map_err(|err| IcebergError::Io(format!("iceberg_scan: {err}")));
    }
    let path = local_path_from_location(location);
    read_local_regular_bytes(&path)
}

fn read_local_regular_bytes(path: &Path) -> Result<Vec<u8>> {
    let metadata = ensure_local_regular_file(path)?;
    let limit = iceberg_local_read_limit_bytes();
    if metadata.len() > limit {
        return Err(IcebergError::Io(format!(
            "iceberg_scan local file exceeds limit: {} size={} limit={limit}",
            path.display(),
            metadata.len()
        )));
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(|err| {
        IcebergError::Io(format!(
            "iceberg_scan cannot read {}: {err}",
            path.display()
        ))
    })?;
    let mut bytes = Vec::new();
    let mut limited = file.take(limit.saturating_add(1));
    limited.read_to_end(&mut bytes).map_err(|err| {
        IcebergError::Io(format!(
            "iceberg_scan cannot read {}: {err}",
            path.display()
        ))
    })?;
    let read_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if read_len > limit {
        return Err(IcebergError::Io(format!(
            "iceberg_scan local file exceeds limit: {} size={read_len} limit={limit}",
            path.display()
        )));
    }
    Ok(bytes)
}

fn read_local_regular_text_if_exists(path: &Path) -> Result<Option<String>> {
    match fs::symlink_metadata(path) {
        Ok(_) => read_local_regular_bytes(path)
            .and_then(|bytes| {
                String::from_utf8(bytes).map_err(|err| {
                    IcebergError::Io(format!(
                        "iceberg_scan cannot read {}: {err}",
                        path.display()
                    ))
                })
            })
            .map(Some),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(IcebergError::Io(format!(
            "iceberg_scan cannot inspect {}: {err}",
            path.display()
        ))),
    }
}

fn local_regular_file_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(metadata) if metadata.file_type().is_symlink() => Err(IcebergError::Io(format!(
            "iceberg_scan refuses symlinked local file {}",
            path.display()
        ))),
        Ok(_) => Ok(false),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(IcebergError::Io(format!(
            "iceberg_scan cannot inspect {}: {err}",
            path.display()
        ))),
    }
}

fn ensure_local_regular_file(path: &Path) -> Result<fs::Metadata> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(metadata),
        Ok(metadata) if metadata.file_type().is_symlink() => Err(IcebergError::Io(format!(
            "iceberg_scan refuses symlinked local file {}",
            path.display()
        ))),
        Ok(_) => Err(IcebergError::Io(format!(
            "iceberg_scan refuses non-regular local file {}",
            path.display()
        ))),
        Err(err) => Err(IcebergError::Io(format!(
            "iceberg_scan cannot inspect {}: {err}",
            path.display()
        ))),
    }
}

fn iceberg_local_read_limit_bytes() -> u64 {
    std::env::var("ULTRASQL_ICEBERG_LOCAL_READ_LIMIT_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_ICEBERG_LOCAL_READ_LIMIT_BYTES)
}

fn ensure_local_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(metadata) if metadata.file_type().is_symlink() => Err(IcebergError::Io(format!(
            "iceberg_scan refuses symlinked local directory {}",
            path.display()
        ))),
        Ok(_) => Err(IcebergError::Io(format!(
            "iceberg_scan refuses non-directory local path {}",
            path.display()
        ))),
        Err(err) => Err(IcebergError::Io(format!(
            "iceberg_scan cannot inspect {}: {err}",
            path.display()
        ))),
    }
}

fn local_path_from_location(location: &str) -> PathBuf {
    if let Some(path) = location.strip_prefix("file://") {
        return PathBuf::from(percent_decode(path));
    }
    PathBuf::from(location)
}

fn resolve_location(root: &str, location: &str) -> String {
    if is_object_store_uri(location) {
        return location.to_owned();
    }
    if let Some(path) = location.strip_prefix("file://") {
        return percent_decode(path);
    }
    let path = Path::new(location);
    if path.is_absolute() || location.contains("://") {
        return location.to_owned();
    }
    if is_object_store_uri(root) {
        return format!("{}/{}", root.trim_end_matches('/'), location);
    }
    Path::new(root).join(location).display().to_string()
}

fn avro_record_field<'a>(value: &'a AvroValue, name: &str) -> Option<&'a AvroValue> {
    let value = avro_union_inner(value);
    let AvroValue::Record(fields) = value else {
        return None;
    };
    fields
        .iter()
        .find_map(|(field_name, value)| (field_name == name).then_some(value))
}

fn avro_union_inner(value: &AvroValue) -> &AvroValue {
    match value {
        AvroValue::Union(_, inner) => inner.as_ref(),
        other => other,
    }
}

fn avro_i32(value: &AvroValue) -> Option<i32> {
    match avro_union_inner(value) {
        AvroValue::Int(value) => Some(*value),
        _ => None,
    }
}

fn avro_array(value: &AvroValue) -> Option<&[AvroValue]> {
    match avro_union_inner(value) {
        AvroValue::Array(value) => Some(value),
        _ => None,
    }
}

fn avro_bytes(value: &AvroValue) -> Option<&[u8]> {
    match avro_union_inner(value) {
        AvroValue::Bytes(value) => Some(value),
        AvroValue::Fixed(_, value) => Some(value),
        _ => None,
    }
}

fn avro_string(value: &AvroValue) -> Option<&str> {
    match avro_union_inner(value) {
        AvroValue::String(value) => Some(value),
        _ => None,
    }
}

fn avro_scalar_to_string(value: &AvroValue) -> Option<String> {
    match avro_union_inner(value) {
        AvroValue::String(value) => Some(value.clone()),
        AvroValue::Int(value) => Some(value.to_string()),
        AvroValue::Long(value) => Some(value.to_string()),
        AvroValue::Boolean(value) => Some(value.to_string()),
        AvroValue::Float(value) => Some(value.to_string()),
        AvroValue::Double(value) => Some(value.to_string()),
        _ => None,
    }
}

fn iceberg_type_to_sql(value: &serde_json::Value) -> Result<DataType> {
    let type_name = match value {
        serde_json::Value::String(type_name) => type_name.as_str(),
        serde_json::Value::Object(object) => object
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                IcebergError::Unsupported(format!("iceberg_scan unsupported type: {value}"))
            })?,
        _ => {
            return Err(IcebergError::Unsupported(format!(
                "iceberg_scan unsupported type: {value}"
            )));
        }
    };
    match type_name {
        "boolean" => Ok(DataType::Bool),
        "int" => Ok(DataType::Int32),
        "long" => Ok(DataType::Int64),
        "float" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "string" => Ok(DataType::Text { max_len: None }),
        other => Err(IcebergError::Unsupported(format!(
            "iceberg_scan unsupported type: {other}"
        ))),
    }
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = &value[i + 1..i + 3];
            if let Ok(decoded) = u8::from_str_radix(hex, 16) {
                out.push(decoded);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|err| String::from_utf8_lossy(err.as_bytes()).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use apache_avro::{Codec, Schema as AvroSchema, Writer};

    #[test]
    fn metadata_version_parses_v_prefix() {
        assert_eq!(
            metadata_version(Path::new("metadata/v12.metadata.json")),
            Some(12)
        );
        assert_eq!(
            metadata_version(Path::new("metadata/7.metadata.json")),
            Some(7)
        );
    }

    #[test]
    fn file_uri_resolves_to_local_path() {
        assert_eq!(
            resolve_location("/tmp/table", "file:///tmp/table/data/a.parquet"),
            "/tmp/table/data/a.parquet"
        );
    }

    #[test]
    fn public_schema_reader_accepts_explicit_metadata_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let table = temp.path();
        write_iceberg_metadata(table);
        let metadata_path = table.join("metadata/v1.metadata.json");

        let schema = read_iceberg_schema(metadata_path.to_str().expect("metadata path utf8"))
            .expect("schema from metadata file");

        assert_eq!(schema.len(), 2);
        assert_eq!(schema.field_at(0).name, "id");
        assert_eq!(schema.field_at(0).data_type, DataType::Int64);
        assert_eq!(schema.field_at(1).name, "category");
        assert_eq!(
            schema.field_at(1).data_type,
            DataType::Text { max_len: None }
        );
    }

    #[test]
    fn public_scan_uses_current_snapshot_by_default() {
        let temp = tempfile::tempdir().expect("tempdir");
        let table = temp.path();
        write_iceberg_metadata(table);
        let metadata_dir = table.join("metadata");
        let new_manifest = metadata_dir.join("new.avro");
        let new_list = metadata_dir.join("new-list.avro");

        write_manifest_with_partitions(&new_manifest, &[("current.parquet", &[])]);
        write_manifest_list_with_bounds(&new_list, &[(&new_manifest, "", "zz")]);

        let plan =
            plan_iceberg_scan(&table.display().to_string()).expect("plan current snapshot scan");

        assert_eq!(plan.snapshot_id, Some(2));
        assert_eq!(
            plan.data_files,
            vec![table.join("current.parquet").display().to_string()]
        );
    }

    #[test]
    fn scan_without_current_snapshot_returns_empty_plan() {
        let temp = tempfile::tempdir().expect("tempdir");
        let table = temp.path();
        let metadata_dir = table.join("metadata");
        fs::create_dir_all(&metadata_dir).expect("metadata dir");
        fs::write(metadata_dir.join("version-hint.text"), "1\n").expect("version hint");
        write_metadata_json(
            &metadata_dir.join("v1.metadata.json"),
            table,
            serde_json::Value::Null,
        );

        let plan = plan_iceberg_scan(&table.display().to_string()).expect("empty snapshot plan");

        assert_eq!(plan.snapshot_id, None);
        assert!(plan.data_files.is_empty());
        assert_eq!(plan.manifests_scanned, 0);
    }

    #[test]
    fn metadata_discovery_uses_highest_version_without_hint() {
        let temp = tempfile::tempdir().expect("tempdir");
        let table = temp.path();
        let metadata_dir = table.join("metadata");
        fs::create_dir_all(&metadata_dir).expect("metadata dir");
        write_metadata_json(&metadata_dir.join("v1.metadata.json"), table, 1.into());
        write_metadata_json(&metadata_dir.join("v2.metadata.json"), table, 2.into());

        let discovered = discover_metadata_json(&metadata_dir).expect("discover metadata");

        assert_eq!(discovered, metadata_dir.join("v2.metadata.json"));
    }

    #[cfg(unix)]
    #[test]
    fn local_metadata_reads_reject_symlinked_files() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let table = temp.path().join("table");
        write_iceberg_metadata(&table);
        let metadata = table.join("metadata/v1.metadata.json");
        let link = temp.path().join("linked.metadata.json");
        symlink(&metadata, &link).expect("metadata symlink");

        let Err(err) = read_iceberg_schema(link.to_str().expect("metadata utf8")) else {
            panic!("symlinked metadata should be rejected");
        };

        assert!(err.to_string().contains("symlink"));
    }

    #[cfg(unix)]
    #[test]
    fn metadata_discovery_rejects_symlinked_version_hint() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let table = temp.path();
        let metadata_dir = table.join("metadata");
        fs::create_dir_all(&metadata_dir).expect("metadata dir");
        write_metadata_json(&metadata_dir.join("v1.metadata.json"), table, 1.into());
        let hint_target = temp.path().join("version-hint-target");
        fs::write(&hint_target, "1\n").expect("hint target");
        symlink(&hint_target, metadata_dir.join("version-hint.text")).expect("hint symlink");

        let err = discover_metadata_json(&metadata_dir).expect_err("symlinked hint rejected");

        assert!(err.to_string().contains("symlink"));
    }

    #[cfg(unix)]
    #[test]
    fn iceberg_plan_rejects_symlinked_manifest_lists() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let table = temp.path();
        write_iceberg_metadata(table);
        let metadata_dir = table.join("metadata");
        let manifest = metadata_dir.join("new.avro");
        let real_list = metadata_dir.join("real-new-list.avro");
        let linked_list = metadata_dir.join("new-list.avro");
        write_manifest_with_partitions(&manifest, &[("current.parquet", &[])]);
        write_manifest_list_with_bounds(&real_list, &[(&manifest, "", "zz")]);
        symlink(&real_list, &linked_list).expect("manifest-list symlink");

        let Err(err) = plan_iceberg_scan(&table.display().to_string()) else {
            panic!("symlinked manifest list should be rejected");
        };

        assert!(err.to_string().contains("symlink"));
    }

    #[test]
    fn object_metadata_locations_are_explicit_only() {
        let metadata = metadata_location_for("s3://bucket/table/metadata/v7.metadata.json")
            .expect("object metadata path");

        assert_eq!(metadata.table_root, "s3://bucket/table");
        assert_eq!(
            metadata.metadata_path,
            "s3://bucket/table/metadata/v7.metadata.json"
        );
        let err = metadata_location_for("s3://bucket/table").expect_err("object root rejected");
        assert!(err.to_string().contains("require explicit metadata JSON"));
    }

    #[test]
    fn location_helpers_decode_file_and_object_paths() {
        assert_eq!(
            local_path_from_location("file:///tmp/table%20name/metadata.json"),
            PathBuf::from("/tmp/table name/metadata.json")
        );
        assert_eq!(
            resolve_location("s3://bucket/table", "data/a.parquet"),
            "s3://bucket/table/data/a.parquet"
        );
        assert_eq!(
            resolve_location("/tmp/table", "https://example.invalid/a.parquet"),
            "https://example.invalid/a.parquet"
        );
    }

    #[test]
    fn rejects_malformed_metadata_and_unsupported_types() {
        let temp = tempfile::tempdir().expect("tempdir");
        let metadata = temp.path().join("bad.metadata.json");
        fs::write(&metadata, [0xff, 0xfe]).expect("write invalid utf8");

        let err =
            read_iceberg_schema(metadata.to_str().expect("metadata utf8")).expect_err("bad utf8");

        assert!(err.to_string().contains("is not UTF-8"));
        assert!(
            iceberg_type_to_sql(&serde_json::json!({"logicalType": "uuid"}))
                .expect_err("missing object type")
                .to_string()
                .contains("unsupported type")
        );
        assert!(
            iceberg_type_to_sql(&serde_json::json!("decimal"))
                .expect_err("unsupported string type")
                .to_string()
                .contains("unsupported type: decimal")
        );
    }

    #[test]
    fn read_local_regular_bytes_rejects_configured_oversized_file() {
        let _env_guard = iceberg_env_test_lock();
        // SAFETY: iceberg_env_test_lock serializes process-env mutation in
        // this module's tests.
        unsafe {
            std::env::set_var("ULTRASQL_ICEBERG_LOCAL_READ_LIMIT_BYTES", "3");
        }
        let temp = tempfile::tempdir().expect("tempdir");
        let metadata = temp.path().join("oversized.metadata.json");
        fs::write(&metadata, b"abcd").expect("write oversized metadata");

        let err = read_local_regular_bytes(&metadata).expect_err("oversized metadata rejected");

        assert!(err.to_string().contains("local file exceeds limit"));
        // SAFETY: iceberg_env_test_lock serializes process-env mutation in
        // this module's tests.
        unsafe {
            std::env::remove_var("ULTRASQL_ICEBERG_LOCAL_READ_LIMIT_BYTES");
        }
    }

    #[test]
    fn avro_scalar_helpers_cover_supported_primitives() {
        assert_eq!(
            avro_bytes(&AvroValue::Fixed(2, vec![b'a', b'b'])),
            Some(&b"ab"[..])
        );
        assert_eq!(
            avro_scalar_to_string(&AvroValue::Int(7)),
            Some("7".to_owned())
        );
        assert_eq!(
            avro_scalar_to_string(&AvroValue::Long(9)),
            Some("9".to_owned())
        );
        assert_eq!(
            avro_scalar_to_string(&AvroValue::Boolean(true)),
            Some("true".to_owned())
        );
        assert_eq!(
            avro_scalar_to_string(&AvroValue::Float(1.5)),
            Some("1.5".to_owned())
        );
        assert_eq!(
            avro_scalar_to_string(&AvroValue::Double(2.5)),
            Some("2.5".to_owned())
        );
    }

    #[test]
    fn scan_options_select_snapshot_and_prune_partitions() {
        let temp = tempfile::tempdir().expect("tempdir");
        let table = temp.path();
        write_iceberg_metadata(table);
        let metadata_dir = table.join("metadata");
        let old_manifest = metadata_dir.join("old.avro");
        let new_manifest = metadata_dir.join("new.avro");
        let old_list = metadata_dir.join("old-list.avro");
        let new_list = metadata_dir.join("new-list.avro");

        write_manifest_with_partitions(&old_manifest, &[("old.parquet", &[("category", "old")])]);
        write_manifest_with_partitions(
            &new_manifest,
            &[
                ("keep.parquet", &[("category", "keep")]),
                ("skip.parquet", &[("category", "skip")]),
            ],
        );
        write_manifest_list_with_bounds(&old_list, &[(&old_manifest, "old", "old")]);
        write_manifest_list_with_bounds(&new_list, &[(&new_manifest, "keep", "skip")]);

        let plan = plan_iceberg_scan_with_options(
            &table.display().to_string(),
            &IcebergScanOptions {
                snapshot_id: Some(2),
                partition_filter: Some(IcebergPartitionFilter::equals("category", "keep")),
            },
        )
        .expect("plan iceberg scan with pruning");

        assert_eq!(plan.snapshot_id, Some(2));
        assert_eq!(plan.manifests_scanned, 1);
        assert_eq!(plan.manifests_skipped, 0);
        assert_eq!(
            plan.data_files,
            vec![table.join("keep.parquet").display().to_string()]
        );
        assert_eq!(plan.data_files_scanned, 1);
        assert_eq!(plan.data_files_skipped, 1);
    }

    #[test]
    fn manifest_partition_bounds_skip_unmatched_manifests() {
        let temp = tempfile::tempdir().expect("tempdir");
        let table = temp.path();
        write_iceberg_metadata(table);
        let metadata_dir = table.join("metadata");
        let skip_manifest = metadata_dir.join("skip.avro");
        let keep_manifest = metadata_dir.join("keep.avro");
        let new_list = metadata_dir.join("new-list.avro");

        write_manifest_with_partitions(
            &skip_manifest,
            &[("skip.parquet", &[("category", "skip")])],
        );
        write_manifest_with_partitions(
            &keep_manifest,
            &[("keep.parquet", &[("category", "keep")])],
        );
        write_manifest_list_with_bounds(
            &new_list,
            &[
                (&skip_manifest, "skip", "skip"),
                (&keep_manifest, "keep", "keep"),
            ],
        );

        let plan = plan_iceberg_scan_with_options(
            &table.display().to_string(),
            &IcebergScanOptions {
                snapshot_id: Some(2),
                partition_filter: Some(IcebergPartitionFilter::equals("category", "keep")),
            },
        )
        .expect("plan iceberg scan with manifest pruning");

        assert_eq!(plan.manifests_scanned, 1);
        assert_eq!(plan.manifests_skipped, 1);
        assert_eq!(
            plan.data_files,
            vec![table.join("keep.parquet").display().to_string()]
        );
    }

    fn write_iceberg_metadata(table_dir: &Path) {
        let metadata_dir = table_dir.join("metadata");
        fs::create_dir_all(&metadata_dir).expect("metadata dir");
        fs::write(metadata_dir.join("version-hint.text"), "1\n").expect("version hint");
        write_metadata_json(&metadata_dir.join("v1.metadata.json"), table_dir, 2.into());
    }

    fn write_metadata_json(path: &Path, table_dir: &Path, current_snapshot_id: serde_json::Value) {
        let metadata_dir = table_dir.join("metadata");
        let metadata_json = serde_json::json!({
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000045",
            "location": table_dir.to_str().expect("table path utf8"),
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 2,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "category", "required": false, "type": "string"}
                ]
            }],
            "current-schema-id": 0,
            "partition-specs": [{
                "spec-id": 0,
                "fields": [{
                    "source-id": 2,
                    "field-id": 1000,
                    "name": "category",
                    "transform": "identity"
                }]
            }],
            "default-spec-id": 0,
            "last-partition-id": 1000,
            "properties": {},
            "current-snapshot-id": current_snapshot_id,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "sequence-number": 1,
                    "timestamp-ms": 0,
                    "manifest-list": metadata_dir.join("old-list.avro").to_str().expect("old list utf8")
                },
                {
                    "snapshot-id": 2,
                    "sequence-number": 2,
                    "timestamp-ms": 0,
                    "manifest-list": metadata_dir.join("new-list.avro").to_str().expect("new list utf8")
                }
            ],
            "snapshot-log": [
                {"timestamp-ms": 0, "snapshot-id": 1},
                {"timestamp-ms": 1, "snapshot-id": 2}
            ],
            "metadata-log": []
        });
        fs::write(
            path,
            serde_json::to_string_pretty(&metadata_json).expect("metadata json"),
        )
        .expect("write metadata json");
    }

    fn write_manifest_list_with_bounds(path: &Path, manifests: &[(&Path, &str, &str)]) {
        let schema = AvroSchema::parse_str(
            r#"{
              "type": "record",
              "name": "manifest_file",
              "fields": [
                {"name": "manifest_path", "type": "string"},
                {"name": "manifest_length", "type": "long"},
                {"name": "partition_spec_id", "type": "int"},
                {"name": "content", "type": "int"},
                {"name": "sequence_number", "type": "long"},
                {"name": "min_sequence_number", "type": "long"},
                {"name": "added_snapshot_id", "type": "long"},
                {"name": "added_data_files_count", "type": "int"},
                {"name": "existing_data_files_count", "type": "int"},
                {"name": "deleted_data_files_count", "type": "int"},
                {"name": "added_rows_count", "type": "long"},
                {"name": "existing_rows_count", "type": "long"},
                {"name": "deleted_rows_count", "type": "long"},
                {
                  "name": "partitions",
                  "type": ["null", {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "field_summary",
                      "fields": [
                        {"name": "contains_null", "type": "boolean"},
                        {"name": "contains_nan", "type": ["null", "boolean"], "default": null},
                        {"name": "lower_bound", "type": ["null", "bytes"], "default": null},
                        {"name": "upper_bound", "type": ["null", "bytes"], "default": null}
                      ]
                    }
                  }],
                  "default": null
                }
              ]
            }"#,
        )
        .expect("manifest-list avro schema");
        let file = fs::File::create(path).expect("create manifest list");
        let mut writer = Writer::with_codec(&schema, file, Codec::Null);
        for (manifest_path, lower, upper) in manifests {
            writer
                .append(AvroValue::Record(vec![
                    (
                        "manifest_path".to_string(),
                        AvroValue::String(
                            manifest_path.to_str().expect("manifest utf8").to_string(),
                        ),
                    ),
                    ("manifest_length".to_string(), AvroValue::Long(0)),
                    ("partition_spec_id".to_string(), AvroValue::Int(0)),
                    ("content".to_string(), AvroValue::Int(0)),
                    ("sequence_number".to_string(), AvroValue::Long(1)),
                    ("min_sequence_number".to_string(), AvroValue::Long(1)),
                    ("added_snapshot_id".to_string(), AvroValue::Long(2)),
                    ("added_data_files_count".to_string(), AvroValue::Int(1)),
                    ("existing_data_files_count".to_string(), AvroValue::Int(0)),
                    ("deleted_data_files_count".to_string(), AvroValue::Int(0)),
                    ("added_rows_count".to_string(), AvroValue::Long(1)),
                    ("existing_rows_count".to_string(), AvroValue::Long(0)),
                    ("deleted_rows_count".to_string(), AvroValue::Long(0)),
                    (
                        "partitions".to_string(),
                        AvroValue::Union(
                            1,
                            Box::new(AvroValue::Array(vec![AvroValue::Record(vec![
                                ("contains_null".to_string(), AvroValue::Boolean(false)),
                                (
                                    "contains_nan".to_string(),
                                    AvroValue::Union(0, Box::new(AvroValue::Null)),
                                ),
                                (
                                    "lower_bound".to_string(),
                                    AvroValue::Union(
                                        1,
                                        Box::new(AvroValue::Bytes(lower.as_bytes().to_vec())),
                                    ),
                                ),
                                (
                                    "upper_bound".to_string(),
                                    AvroValue::Union(
                                        1,
                                        Box::new(AvroValue::Bytes(upper.as_bytes().to_vec())),
                                    ),
                                ),
                            ])])),
                        ),
                    ),
                ]))
                .expect("write manifest list row");
        }
        writer.flush().expect("flush manifest list");
    }

    fn write_manifest_with_partitions(path: &Path, files: &[(&str, &[(&str, &str)])]) {
        let schema = AvroSchema::parse_str(
            r#"{
              "type": "record",
              "name": "manifest_entry",
              "fields": [
                {"name": "status", "type": "int"},
                {"name": "snapshot_id", "type": ["null", "long"], "default": null},
                {"name": "sequence_number", "type": ["null", "long"], "default": null},
                {"name": "file_sequence_number", "type": ["null", "long"], "default": null},
                {
                  "name": "data_file",
                  "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                      {"name": "content", "type": "int"},
                      {"name": "file_path", "type": "string"},
                      {"name": "file_format", "type": "string"},
                      {"name": "record_count", "type": "long"},
                      {"name": "file_size_in_bytes", "type": "long"},
                      {"name": "partition", "type": {
                        "type": "record",
                        "name": "partition",
                        "fields": [{"name": "category", "type": ["null", "string"], "default": null}]
                      }}
                    ]
                  }
                }
              ]
            }"#,
        )
        .expect("manifest avro schema");
        let file = fs::File::create(path).expect("create manifest");
        let mut writer = Writer::with_codec(&schema, file, Codec::Null);
        let table_dir = path
            .parent()
            .and_then(Path::parent)
            .expect("manifest under table metadata");
        for (file_name, partitions) in files {
            let category = partitions
                .iter()
                .find_map(|(field, value)| (*field == "category").then_some(*value))
                .unwrap_or_default();
            writer
                .append(AvroValue::Record(vec![
                    ("status".to_string(), AvroValue::Int(1)),
                    (
                        "snapshot_id".to_string(),
                        AvroValue::Union(1, Box::new(AvroValue::Long(2))),
                    ),
                    (
                        "sequence_number".to_string(),
                        AvroValue::Union(1, Box::new(AvroValue::Long(2))),
                    ),
                    (
                        "file_sequence_number".to_string(),
                        AvroValue::Union(1, Box::new(AvroValue::Long(2))),
                    ),
                    (
                        "data_file".to_string(),
                        AvroValue::Record(vec![
                            ("content".to_string(), AvroValue::Int(0)),
                            (
                                "file_path".to_string(),
                                AvroValue::String(file_name.to_string()),
                            ),
                            (
                                "file_format".to_string(),
                                AvroValue::String("PARQUET".to_string()),
                            ),
                            ("record_count".to_string(), AvroValue::Long(1)),
                            ("file_size_in_bytes".to_string(), AvroValue::Long(1)),
                            (
                                "partition".to_string(),
                                AvroValue::Record(vec![(
                                    "category".to_string(),
                                    AvroValue::Union(
                                        1,
                                        Box::new(AvroValue::String(category.to_string())),
                                    ),
                                )]),
                            ),
                        ]),
                    ),
                ]))
                .expect("write manifest row");
            fs::write(table_dir.join(file_name), b"PAR1").expect("write data placeholder");
        }
        writer.flush().expect("flush manifest");
    }

    fn iceberg_env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .expect("iceberg env test lock")
    }
}
