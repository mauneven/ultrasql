//! Arrow/Parquet schema inspection and conversion to UltraSQL schema.

use std::path::Path;

use arrow_array::RecordBatch;
use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use ultrasql_arrow::record_batch_to_ultrasql_batch;
use ultrasql_core::{DataType, Field, Schema};

use crate::error::ServerError;

use super::object_range::ObjectRangeChunkReader;
use super::scan::open_regular_parquet_file;

pub(super) fn read_arrow_schema(path: &Path) -> Result<arrow_schema::SchemaRef, ServerError> {
    let display = path.display().to_string();
    let file = open_regular_parquet_file(path, &display, "open")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
        ServerError::CopyFormat(format!(
            "read_parquet cannot inspect {}: {err}",
            path.display()
        ))
    })?;
    Ok(builder.schema().clone())
}

pub(super) fn read_object_arrow_schema(
    object: &ultrasql_objectstore::ObjectLocation,
) -> Result<arrow_schema::SchemaRef, ServerError> {
    let display = object.display_uri();
    let reader = ObjectRangeChunkReader::new(object.clone())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|err| {
        ServerError::CopyFormat(format!("read_parquet cannot inspect {display}: {err}"))
    })?;
    Ok(builder.schema().clone())
}

pub(super) fn parquet_schema_to_ultrasql(
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

pub(super) fn arrow_record_batch_to_ultrasql(
    batch: RecordBatch,
) -> Result<ultrasql_vec::Batch, ServerError> {
    record_batch_to_ultrasql_batch(batch)
        .map_err(|err| ServerError::CopyFormat(format!("read_parquet Arrow bridge: {err}")))
}

pub(super) fn resolve_projection_names(
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
