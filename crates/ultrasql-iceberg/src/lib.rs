//! Read-only Apache Iceberg metadata planning.
//!
//! This crate resolves a table root or metadata JSON file into the current
//! snapshot's live Parquet data files. It intentionally does not perform
//! writes, catalog commits, or time travel; those belong in later slices.

use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use apache_avro::{Reader, types::Value as AvroValue};
use serde::Deserialize;
use ultrasql_core::{DataType, Field, Schema};
use ultrasql_objectstore::{expand_object_store_specs, is_object_store_uri, read_object_bytes};

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
}

/// Read only the current schema from an Iceberg table root or metadata file.
pub fn read_iceberg_schema(table: &str) -> Result<Schema> {
    let metadata = load_metadata(table)?;
    metadata.schema()
}

/// Plan live Parquet data files for the current Iceberg snapshot.
pub fn plan_iceberg_scan(table: &str) -> Result<IcebergScanPlan> {
    let metadata = load_metadata(table)?;
    let schema = metadata.schema()?;
    let data_files = metadata.current_data_files()?;
    Ok(IcebergScanPlan { schema, data_files })
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

    fn current_data_files(&self) -> Result<Vec<String>> {
        let Some(snapshot_id) = self.metadata.current_snapshot_id else {
            return Ok(Vec::new());
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
        let manifest_paths = read_manifest_list(&manifest_list)?;
        let mut data_files = Vec::new();
        for manifest_path in manifest_paths {
            let manifest_path = resolve_location(&self.table_root, &manifest_path);
            data_files.extend(read_manifest_data_files(&self.table_root, &manifest_path)?);
        }
        Ok(data_files)
    }
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
    if path.is_file() {
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
    if let Ok(text) = fs::read_to_string(&hint) {
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
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }

    let mut candidates = fs::read_dir(metadata_dir)
        .map_err(|err| {
            IcebergError::Io(format!(
                "iceberg_scan cannot read metadata directory {}: {err}",
                metadata_dir.display()
            ))
        })?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".metadata.json"))
        })
        .collect::<Vec<_>>();
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

fn read_manifest_list(path: &str) -> Result<Vec<String>> {
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
        manifests.push(manifest_path.to_owned());
    }
    Ok(manifests)
}

fn read_manifest_data_files(table_root: &str, path: &str) -> Result<Vec<String>> {
    let mut data_files = Vec::new();
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
        let file_path = avro_record_field(data_file, "file_path")
            .and_then(avro_string)
            .ok_or_else(|| {
                IcebergError::InvalidMetadata(format!(
                    "iceberg_scan manifest {path} missing data_file.file_path"
                ))
            })?;
        data_files.push(resolve_location(table_root, file_path));
    }
    Ok(data_files)
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
    fs::read(&path).map_err(|err| {
        IcebergError::Io(format!(
            "iceberg_scan cannot read {}: {err}",
            path.display()
        ))
    })
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

fn avro_string(value: &AvroValue) -> Option<&str> {
    match avro_union_inner(value) {
        AvroValue::String(value) => Some(value),
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
}
