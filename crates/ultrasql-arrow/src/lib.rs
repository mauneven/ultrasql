//! Apache Arrow record batch bridge.
//!
//! This crate is the public Rust boundary for moving columnar data between
//! UltraSQL batches and Arrow `RecordBatch` values.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer,
};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use ultrasql_core::{DataType, Field, Schema};
use ultrasql_vec::Batch;
use ultrasql_vec::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

/// Result type for Arrow bridge operations.
pub type Result<T> = std::result::Result<T, ArrowBridgeError>;

/// Arrow import/export bridge error.
#[derive(Debug, thiserror::Error)]
pub enum ArrowBridgeError {
    /// Arrow rejected constructed schema or array data.
    #[error("{0}")]
    Arrow(String),
    /// UltraSQL schema or batch shape is invalid for Arrow export.
    #[error("{0}")]
    Schema(String),
    /// A column's logical type and physical batch column disagree.
    #[error("{0}")]
    Type(String),
    /// Arrow type is outside the first bridge slice.
    #[error("{0}")]
    Unsupported(String),
    /// UltraSQL batch construction failed.
    #[error("{0}")]
    Batch(String),
}

/// Convert an UltraSQL schema into an Arrow schema.
pub fn schema_to_arrow(schema: &Schema) -> Result<ArrowSchema> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            Ok(ArrowField::new(
                field.name.clone(),
                sql_type_to_arrow(&field.data_type)?,
                field.nullable,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ArrowSchema::new(fields))
}

/// Convert an Arrow schema into an UltraSQL schema.
pub fn schema_from_arrow(schema: &ArrowSchema) -> Result<Schema> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = arrow_type_to_sql(field.data_type())?;
            Ok(if field.is_nullable() {
                Field::nullable(field.name().clone(), data_type)
            } else {
                Field::required(field.name().clone(), data_type)
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Schema::new(fields).map_err(|err| ArrowBridgeError::Schema(format!("Arrow schema: {err}")))
}

/// Export an owned UltraSQL batch into an Arrow record batch.
///
/// Fixed-width value buffers, UTF-8 value bytes, and native validity
/// buffers are moved into Arrow without copying when their layouts
/// match. Boolean values and UTF-8 offsets are repacked because
/// UltraSQL currently stores booleans one byte per row and UTF-8
/// offsets as `u32`.
pub fn batch_to_record_batch(schema: &Schema, batch: Batch) -> Result<RecordBatch> {
    if schema.len() != batch.width() {
        return Err(ArrowBridgeError::Schema(format!(
            "Arrow export expected {} columns, got {}",
            schema.len(),
            batch.width()
        )));
    }
    let arrow_schema = Arc::new(schema_to_arrow(schema)?);
    let arrays = schema
        .fields()
        .iter()
        .zip(batch.into_columns())
        .map(|(field, column)| column_to_arrow_array(field, column))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|err| ArrowBridgeError::Arrow(format!("Arrow record batch: {err}")))
}

/// Import an owned Arrow record batch into an UltraSQL schema and batch.
///
/// The importer consumes the Arrow batch. For owned, zero-offset
/// fixed-width arrays, Arrow buffers are reclaimed as UltraSQL vectors
/// without copying. Shared or sliced buffers fall back to copying the
/// logical slice.
pub fn record_batch_to_batch(record_batch: RecordBatch) -> Result<(Schema, Batch)> {
    let schema = schema_from_arrow(record_batch.schema_ref().as_ref())?;
    let (_, arrays, _) = record_batch.into_parts();
    let columns = arrays
        .into_iter()
        .map(arrow_array_to_column)
        .collect::<Result<Vec<_>>>()?;
    let batch = Batch::new(columns)
        .map_err(|err| ArrowBridgeError::Batch(format!("Arrow import batch: {err}")))?;
    Ok((schema, batch))
}

/// Import an owned Arrow record batch and return only the UltraSQL batch.
pub fn record_batch_to_ultrasql_batch(record_batch: RecordBatch) -> Result<Batch> {
    record_batch_to_batch(record_batch).map(|(_, batch)| batch)
}

fn sql_type_to_arrow(data_type: &DataType) -> Result<ArrowDataType> {
    match data_type {
        DataType::Bool => Ok(ArrowDataType::Boolean),
        DataType::Int32 => Ok(ArrowDataType::Int32),
        DataType::Int64 => Ok(ArrowDataType::Int64),
        DataType::Float32 => Ok(ArrowDataType::Float32),
        DataType::Float64 => Ok(ArrowDataType::Float64),
        DataType::Text { .. } => Ok(ArrowDataType::Utf8),
        other => Err(ArrowBridgeError::Unsupported(format!(
            "Arrow bridge unsupported UltraSQL type: {other}"
        ))),
    }
}

fn arrow_type_to_sql(data_type: &ArrowDataType) -> Result<DataType> {
    match data_type {
        ArrowDataType::Boolean => Ok(DataType::Bool),
        ArrowDataType::Int32 => Ok(DataType::Int32),
        ArrowDataType::Int64 => Ok(DataType::Int64),
        ArrowDataType::Float32 => Ok(DataType::Float32),
        ArrowDataType::Float64 => Ok(DataType::Float64),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Ok(DataType::Text { max_len: None }),
        other => Err(ArrowBridgeError::Unsupported(format!(
            "Arrow bridge unsupported Arrow type: {other}"
        ))),
    }
}

fn column_to_arrow_array(field: &Field, column: Column) -> Result<ArrayRef> {
    let has_nulls = column_has_nulls(&column);
    if has_nulls && !field.nullable {
        return Err(ArrowBridgeError::Type(format!(
            "Arrow export column {} is NOT NULL but batch contains nulls",
            field.name
        )));
    }
    match (&field.data_type, column) {
        (DataType::Bool, Column::Bool(column)) => bool_to_arrow(column),
        (DataType::Int32, Column::Int32(column)) => {
            let (data, nulls) = column.into_parts();
            Ok(Arc::new(Int32Array::new(data.into(), null_buffer(nulls))))
        }
        (DataType::Int64, Column::Int64(column)) => {
            let (data, nulls) = column.into_parts();
            Ok(Arc::new(Int64Array::new(data.into(), null_buffer(nulls))))
        }
        (DataType::Float32, Column::Float32(column)) => {
            let (data, nulls) = column.into_parts();
            Ok(Arc::new(Float32Array::new(data.into(), null_buffer(nulls))))
        }
        (DataType::Float64, Column::Float64(column)) => {
            let (data, nulls) = column.into_parts();
            Ok(Arc::new(Float64Array::new(data.into(), null_buffer(nulls))))
        }
        (DataType::Text { .. }, Column::Utf8(column)) => utf8_to_arrow(column),
        (DataType::Text { .. }, Column::DictionaryUtf8(column)) => {
            let rows = (0..column.len())
                .map(|row| {
                    if column.codes.nulls().is_some_and(|nulls| !nulls.get(row)) {
                        None
                    } else {
                        Some(column.decode_at(row))
                    }
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(StringArray::from(rows)))
        }
        (expected, got) => Err(ArrowBridgeError::Type(format!(
            "Arrow export column {} expected {}, got {}",
            field.name,
            expected,
            got.data_type()
        ))),
    }
}

fn column_has_nulls(column: &Column) -> bool {
    match column {
        Column::Int32(column) => column
            .nulls()
            .is_some_and(|nulls| nulls.count_ones() != column.len()),
        Column::Int64(column) => column
            .nulls()
            .is_some_and(|nulls| nulls.count_ones() != column.len()),
        Column::Float32(column) => column
            .nulls()
            .is_some_and(|nulls| nulls.count_ones() != column.len()),
        Column::Float64(column) => column
            .nulls()
            .is_some_and(|nulls| nulls.count_ones() != column.len()),
        Column::Bool(column) => column
            .nulls()
            .is_some_and(|nulls| nulls.count_ones() != column.len()),
        Column::Utf8(column) => column
            .nulls()
            .is_some_and(|nulls| nulls.count_ones() != column.len()),
        Column::DictionaryUtf8(column) => column
            .codes
            .nulls()
            .is_some_and(|nulls| nulls.count_ones() != column.len()),
    }
}

fn bool_to_arrow(column: BoolColumn) -> Result<ArrayRef> {
    let (data, nulls) = column.into_parts();
    let values = data.into_iter().map(|value| value != 0).collect::<Vec<_>>();
    Ok(Arc::new(BooleanArray::new(
        BooleanBuffer::from(values),
        null_buffer(nulls),
    )))
}

fn utf8_to_arrow(column: StringColumn) -> Result<ArrayRef> {
    let (offsets, values, nulls) = column.into_parts();
    let offsets = offsets
        .into_iter()
        .map(|offset| {
            i32::try_from(offset)
                .map_err(|_| ArrowBridgeError::Type("Arrow UTF-8 offset exceeds i32".to_owned()))
        })
        .collect::<Result<Vec<_>>>()?;
    let array = StringArray::try_new(
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Buffer::from_vec(values),
        null_buffer(nulls),
    )
    .map_err(|err| ArrowBridgeError::Arrow(format!("Arrow UTF-8 array: {err}")))?;
    Ok(Arc::new(array))
}

fn null_buffer(nulls: Option<Bitmap>) -> Option<NullBuffer> {
    let nulls = nulls?;
    let len = nulls.len();
    let words = nulls.into_words();
    #[cfg(target_endian = "little")]
    {
        Some(NullBuffer::new(BooleanBuffer::new(
            Buffer::from_vec(words),
            0,
            len,
        )))
    }
    #[cfg(not(target_endian = "little"))]
    {
        let bytes = words
            .into_iter()
            .flat_map(u64::to_le_bytes)
            .collect::<Vec<_>>();
        Some(NullBuffer::new(BooleanBuffer::new(
            Buffer::from(bytes),
            0,
            len,
        )))
    }
}

fn arrow_array_to_column(array: ArrayRef) -> Result<Column> {
    let data = array.to_data();
    drop(array);
    let (data_type, len, nulls, offset, buffers, child_data) = data.into_parts();
    if !child_data.is_empty() {
        return Err(ArrowBridgeError::Unsupported(format!(
            "Arrow bridge unsupported nested Arrow type: {data_type}"
        )));
    }
    match data_type {
        ArrowDataType::Boolean => boolean_from_arrow(len, nulls, offset, buffers),
        ArrowDataType::Int32 => {
            let values = primitive_values::<i32>(len, offset, buffers)?;
            numeric_column(values, nulls, len, Column::Int32)
        }
        ArrowDataType::Int64 => {
            let values = primitive_values::<i64>(len, offset, buffers)?;
            numeric_column(values, nulls, len, Column::Int64)
        }
        ArrowDataType::Float32 => {
            let values = primitive_values::<f32>(len, offset, buffers)?;
            numeric_column(values, nulls, len, Column::Float32)
        }
        ArrowDataType::Float64 => {
            let values = primitive_values::<f64>(len, offset, buffers)?;
            numeric_column(values, nulls, len, Column::Float64)
        }
        ArrowDataType::Utf8 => utf8_from_arrow(len, nulls, offset, buffers),
        ArrowDataType::LargeUtf8 => large_utf8_from_arrow(len, nulls, offset, buffers),
        other => Err(ArrowBridgeError::Unsupported(format!(
            "Arrow bridge unsupported Arrow type: {other}"
        ))),
    }
}

fn numeric_column<T, F>(
    values: Vec<T>,
    nulls: Option<NullBuffer>,
    len: usize,
    wrap: F,
) -> Result<Column>
where
    F: FnOnce(NumericColumn<T>) -> Column,
{
    let nulls = bitmap_from_nulls(nulls, len)?;
    let column = match nulls {
        Some(nulls) => NumericColumn::with_nulls(values, nulls)
            .map_err(|err| ArrowBridgeError::Type(format!("Arrow numeric nulls: {err}")))?,
        None => NumericColumn::from_data(values),
    };
    Ok(wrap(column))
}

fn primitive_values<T>(len: usize, offset: usize, mut buffers: Vec<Buffer>) -> Result<Vec<T>>
where
    T: ArrowNativeType,
{
    if buffers.len() != 1 {
        return Err(ArrowBridgeError::Arrow(format!(
            "Arrow primitive array expected one buffer, got {}",
            buffers.len()
        )));
    }
    let buffer = buffers.remove(0);
    let elem_size = std::mem::size_of::<T>();
    let full_len = buffer.len() / elem_size;
    if offset == 0 && full_len == len {
        return match buffer.into_vec::<T>() {
            Ok(values) => Ok(values),
            Err(buffer) => Ok(ScalarBuffer::<T>::new(buffer, 0, len).to_vec()),
        };
    }
    Ok(ScalarBuffer::<T>::new(buffer, offset, len).to_vec())
}

fn boolean_from_arrow(
    len: usize,
    nulls: Option<NullBuffer>,
    offset: usize,
    mut buffers: Vec<Buffer>,
) -> Result<Column> {
    if buffers.len() != 1 {
        return Err(ArrowBridgeError::Arrow(format!(
            "Arrow Boolean array expected one buffer, got {}",
            buffers.len()
        )));
    }
    let values = BooleanBuffer::new(buffers.remove(0), offset, len);
    let data = (0..len).map(|idx| values.value(idx)).collect::<Vec<_>>();
    let nulls = bitmap_from_nulls(nulls, len)?;
    let column = match nulls {
        Some(nulls) => BoolColumn::with_nulls(data, nulls)
            .map_err(|err| ArrowBridgeError::Type(format!("Arrow Boolean nulls: {err}")))?,
        None => BoolColumn::from_data(data),
    };
    Ok(Column::Bool(column))
}

fn utf8_from_arrow(
    len: usize,
    nulls: Option<NullBuffer>,
    offset: usize,
    buffers: Vec<Buffer>,
) -> Result<Column> {
    string_from_arrow_i32_offsets(len, nulls, offset, buffers)
}

fn large_utf8_from_arrow(
    len: usize,
    nulls: Option<NullBuffer>,
    offset: usize,
    buffers: Vec<Buffer>,
) -> Result<Column> {
    string_from_arrow_i64_offsets(len, nulls, offset, buffers)
}

fn string_from_arrow_i32_offsets(
    len: usize,
    nulls: Option<NullBuffer>,
    offset: usize,
    mut buffers: Vec<Buffer>,
) -> Result<Column> {
    if buffers.len() != 2 {
        return Err(ArrowBridgeError::Arrow(format!(
            "Arrow UTF-8 array expected two buffers, got {}",
            buffers.len()
        )));
    }
    let offsets = ScalarBuffer::<i32>::new(buffers.remove(0), offset, len + 1);
    let base = nonnegative_usize(offsets[0], "Arrow UTF-8 base offset")?;
    let end = nonnegative_usize(offsets[len], "Arrow UTF-8 end offset")?;
    let values = take_value_bytes(buffers.remove(0), base, end)?;
    let normalized = offsets_to_u32(offsets.iter().copied(), base)?;
    let nulls = bitmap_from_nulls(nulls, len)?;
    Ok(Column::Utf8(
        StringColumn::from_parts(normalized, values, nulls)
            .map_err(|err| ArrowBridgeError::Type(format!("Arrow UTF-8 column: {err}")))?,
    ))
}

fn string_from_arrow_i64_offsets(
    len: usize,
    nulls: Option<NullBuffer>,
    offset: usize,
    mut buffers: Vec<Buffer>,
) -> Result<Column> {
    if buffers.len() != 2 {
        return Err(ArrowBridgeError::Arrow(format!(
            "Arrow LargeUtf8 array expected two buffers, got {}",
            buffers.len()
        )));
    }
    let offsets = ScalarBuffer::<i64>::new(buffers.remove(0), offset, len + 1);
    let base = nonnegative_usize(offsets[0], "Arrow LargeUtf8 base offset")?;
    let end = nonnegative_usize(offsets[len], "Arrow LargeUtf8 end offset")?;
    let values = take_value_bytes(buffers.remove(0), base, end)?;
    let normalized = offsets_to_u32(offsets.iter().copied(), base)?;
    let nulls = bitmap_from_nulls(nulls, len)?;
    Ok(Column::Utf8(
        StringColumn::from_parts(normalized, values, nulls)
            .map_err(|err| ArrowBridgeError::Type(format!("Arrow LargeUtf8 column: {err}")))?,
    ))
}

fn offsets_to_u32<I, T>(offsets: I, base: usize) -> Result<Vec<u32>>
where
    I: IntoIterator<Item = T>,
    T: TryInto<usize>,
{
    offsets
        .into_iter()
        .map(|offset| {
            let offset = offset
                .try_into()
                .map_err(|_| ArrowBridgeError::Type("Arrow UTF-8 offset is negative".to_owned()))?;
            let normalized = offset.checked_sub(base).ok_or_else(|| {
                ArrowBridgeError::Type("Arrow UTF-8 offsets are not monotonic".to_owned())
            })?;
            u32::try_from(normalized)
                .map_err(|_| ArrowBridgeError::Type("Arrow UTF-8 offset exceeds u32".to_owned()))
        })
        .collect()
}

fn nonnegative_usize<T>(value: T, label: &str) -> Result<usize>
where
    T: TryInto<usize>,
{
    value
        .try_into()
        .map_err(|_| ArrowBridgeError::Type(format!("{label} is negative")))
}

fn take_value_bytes(buffer: Buffer, base: usize, end: usize) -> Result<Vec<u8>> {
    if end < base || end > buffer.len() {
        return Err(ArrowBridgeError::Type(
            "Arrow UTF-8 offsets exceed value buffer".to_owned(),
        ));
    }
    if base == 0 && end == buffer.len() {
        return match buffer.into_vec::<u8>() {
            Ok(values) => Ok(values),
            Err(buffer) => Ok(buffer.as_slice().to_vec()),
        };
    }
    Ok(buffer.as_slice()[base..end].to_vec())
}

fn bitmap_from_nulls(nulls: Option<NullBuffer>, len: usize) -> Result<Option<Bitmap>> {
    let Some(nulls) = nulls else {
        return Ok(None);
    };
    let boolean = nulls.into_inner();
    if boolean.len() != len {
        return Err(ArrowBridgeError::Arrow(format!(
            "Arrow null buffer length {} does not match array length {len}",
            boolean.len()
        )));
    }
    if boolean.count_set_bits() == len {
        return Ok(None);
    }
    if boolean.offset() == 0 {
        let buffer = boolean.into_inner();
        return match buffer.into_vec::<u64>() {
            Ok(words) => Ok(Some(Bitmap::from_words(words, len))),
            Err(buffer) => {
                let boolean = BooleanBuffer::new(buffer, 0, len);
                Ok(Some(copy_boolean_buffer_to_bitmap(&boolean, len)))
            }
        };
    }
    Ok(Some(copy_boolean_buffer_to_bitmap(&boolean, len)))
}

fn copy_boolean_buffer_to_bitmap(boolean: &BooleanBuffer, len: usize) -> Bitmap {
    let mut bitmap = Bitmap::new(len, true);
    for idx in 0..len {
        if !boolean.value(idx) {
            bitmap.set(idx, false);
        }
    }
    bitmap
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        Array, BooleanArray, Float32Array, Float64Array, Int64Array, LargeStringArray, RecordBatch,
        StringArray,
    };
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use ultrasql_core::{DataType, Field, Schema};
    use ultrasql_vec::column::{Column, NumericColumn, StringColumn};
    use ultrasql_vec::dict::DictionaryColumn;
    use ultrasql_vec::{Batch, Bitmap};

    use crate::{batch_to_record_batch, record_batch_to_batch, schema_from_arrow, schema_to_arrow};

    #[test]
    fn exports_owned_numeric_buffer_without_copying() {
        let column = NumericColumn::from_data(vec![10_i64, 20, 30]);
        let original_ptr = column.data().as_ptr();
        let batch = Batch::new([Column::Int64(column)]).expect("batch");
        let schema =
            Schema::new([Field::required("id", DataType::Int64)]).expect("ultrasql schema");

        let record_batch = batch_to_record_batch(&schema, batch).expect("arrow export");

        let exported = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 array");
        assert_eq!(exported.values().as_ptr(), original_ptr);
        assert_eq!(exported.values(), &[10_i64, 20, 30]);
        assert_eq!(record_batch.schema().field(0).name(), "id");
    }

    #[test]
    fn exports_owned_utf8_value_buffer_without_copying() {
        let column = StringColumn::from_data(["ada".to_owned(), "grace".to_owned()]);
        let original_values_ptr = column.values().as_ptr();
        let batch = Batch::new([Column::Utf8(column)]).expect("batch");
        let schema = Schema::new([Field::required("name", DataType::Text { max_len: None })])
            .expect("ultrasql schema");

        let record_batch = batch_to_record_batch(&schema, batch).expect("arrow export");

        let exported = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(exported.values().as_ptr(), original_values_ptr);
        assert_eq!(exported.value(0), "ada");
        assert_eq!(exported.value(1), "grace");
    }

    #[test]
    fn exports_bool_float_and_dictionary_columns() {
        let mut bool_validity = Bitmap::new(4, true);
        bool_validity.set(1, false);
        let bools = ultrasql_vec::column::BoolColumn::with_nulls(
            vec![true, false, true, false],
            bool_validity,
        )
        .expect("bool nulls");
        let dictionary =
            DictionaryColumn::from_strings([Some("red"), None, Some("red"), Some("blue")]);
        let batch = Batch::new([
            Column::Bool(bools),
            Column::Float32(NumericColumn::from_data(vec![1.5_f32, 2.5, 3.5, 4.5])),
            Column::Float64(NumericColumn::from_data(vec![10.0_f64, 20.0, 30.0, 40.0])),
            Column::DictionaryUtf8(dictionary),
        ])
        .expect("batch");
        let schema = Schema::new([
            Field::nullable("ok", DataType::Bool),
            Field::required("f32", DataType::Float32),
            Field::required("f64", DataType::Float64),
            Field::nullable("label", DataType::Text { max_len: None }),
        ])
        .expect("schema");

        let record_batch = batch_to_record_batch(&schema, batch).expect("export");

        let bools = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("bool array");
        assert!(bools.value(0));
        assert!(bools.is_null(1));
        assert!(bools.value(2));
        let floats = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("float32 array");
        assert_eq!(floats.values(), &[1.5_f32, 2.5, 3.5, 4.5]);
        let doubles = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64 array");
        assert_eq!(doubles.values(), &[10.0_f64, 20.0, 30.0, 40.0]);
        let labels = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(labels.value(0), "red");
        assert!(labels.is_null(1));
        assert_eq!(labels.value(2), "red");
        assert_eq!(labels.value(3), "blue");
    }

    #[test]
    fn imports_owned_numeric_arrow_buffer_without_copying() {
        let values = vec![7_i64, 11, 13];
        let original_ptr = values.as_ptr();
        let arrow_array = Int64Array::from(values);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "score",
            ArrowDataType::Int64,
            false,
        )]));
        let record_batch =
            RecordBatch::try_new(Arc::clone(&arrow_schema), vec![Arc::new(arrow_array)])
                .expect("record batch");

        let (schema, batch) = record_batch_to_batch(record_batch).expect("arrow import");

        assert_eq!(schema.field_at(0).name, "score");
        assert_eq!(schema.field_at(0).data_type, DataType::Int64);
        let Column::Int64(imported) = &batch.columns()[0] else {
            panic!("expected int64 column");
        };
        assert_eq!(imported.data().as_ptr(), original_ptr);
        assert_eq!(imported.data(), &[7_i64, 11, 13]);
    }

    #[test]
    fn imports_sliced_boolean_array_from_logical_offset() {
        let arrow_array = BooleanArray::from(vec![true, false, true, true]).slice(1, 2);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "ok",
            ArrowDataType::Boolean,
            false,
        )]));
        let record_batch =
            RecordBatch::try_new(Arc::clone(&arrow_schema), vec![Arc::new(arrow_array)])
                .expect("record batch");

        let (schema, batch) = record_batch_to_batch(record_batch).expect("arrow import");

        assert_eq!(schema.field_at(0).data_type, DataType::Bool);
        let Column::Bool(imported) = &batch.columns()[0] else {
            panic!("expected bool column");
        };
        assert_eq!(imported.len(), 2);
        assert!(!imported.value(0));
        assert!(imported.value(1));
    }

    #[test]
    fn imports_sliced_utf8_array_with_nulls() {
        let arrow_array =
            StringArray::from(vec![Some("zero"), Some("one"), None, Some("three")]).slice(1, 2);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "name",
            ArrowDataType::Utf8,
            true,
        )]));
        let record_batch =
            RecordBatch::try_new(Arc::clone(&arrow_schema), vec![Arc::new(arrow_array)])
                .expect("record batch");

        let batch = crate::record_batch_to_ultrasql_batch(record_batch).expect("arrow import");

        let Column::Utf8(imported) = &batch.columns()[0] else {
            panic!("expected utf8 column");
        };
        assert_eq!(imported.len(), 2);
        assert_eq!(imported.value(0), "one");
        let nulls = imported.nulls().expect("utf8 nulls");
        assert!(nulls.get(0));
        assert!(!nulls.get(1));
    }

    #[test]
    fn imports_large_utf8_array() {
        let arrow_array = LargeStringArray::from(vec![Some("wide"), Some("text")]);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "body",
            ArrowDataType::LargeUtf8,
            true,
        )]));
        let record_batch =
            RecordBatch::try_new(Arc::clone(&arrow_schema), vec![Arc::new(arrow_array)])
                .expect("record batch");

        let (_schema, batch) = record_batch_to_batch(record_batch).expect("arrow import");

        let Column::Utf8(imported) = &batch.columns()[0] else {
            panic!("expected utf8 column");
        };
        assert_eq!(imported.value(0), "wide");
        assert_eq!(imported.value(1), "text");
    }

    #[test]
    fn maps_schemas_in_both_directions() {
        let schema = Schema::new([
            Field::required("ok", DataType::Bool),
            Field::required("id", DataType::Int64),
            Field::required("weight32", DataType::Float32),
            Field::required("weight64", DataType::Float64),
            Field::nullable("name", DataType::Text { max_len: None }),
        ])
        .expect("ultrasql schema");

        let arrow = schema_to_arrow(&schema).expect("to arrow schema");
        assert_eq!(arrow.field(0).data_type(), &ArrowDataType::Boolean);
        assert_eq!(arrow.field(1).data_type(), &ArrowDataType::Int64);
        assert_eq!(arrow.field(2).data_type(), &ArrowDataType::Float32);
        assert_eq!(arrow.field(3).data_type(), &ArrowDataType::Float64);
        assert_eq!(arrow.field(4).data_type(), &ArrowDataType::Utf8);

        let imported = schema_from_arrow(&arrow).expect("from arrow schema");
        assert_eq!(imported, schema);
    }

    #[test]
    fn rejects_schema_and_type_mismatches() {
        let schema = Schema::new([Field::required("id", DataType::Int64)]).expect("schema");
        let batch = Batch::new([
            Column::Int64(NumericColumn::from_data(vec![1_i64])),
            Column::Int64(NumericColumn::from_data(vec![2_i64])),
        ])
        .expect("batch");
        let err = batch_to_record_batch(&schema, batch).expect_err("schema width mismatch");
        assert!(err.to_string().contains("expected 1 columns, got 2"));

        let schema = Schema::new([Field::required("d", DataType::Date)]).expect("schema");
        let err = schema_to_arrow(&schema).expect_err("unsupported SQL type");
        assert!(err.to_string().contains("unsupported UltraSQL type: date"));

        let arrow_schema =
            ArrowSchema::new(vec![ArrowField::new("d", ArrowDataType::Date32, false)]);
        let err = schema_from_arrow(&arrow_schema).expect_err("unsupported Arrow type");
        assert!(err.to_string().contains("unsupported Arrow type: Date32"));
    }

    #[test]
    fn preserves_nullable_numeric_values_through_arrow() {
        let mut validity = Bitmap::new(3, true);
        validity.set(1, false);
        let column = NumericColumn::with_nulls(vec![1_i32, 0, 3], validity).expect("nulls");
        let schema = Schema::new([Field::nullable("x", DataType::Int32)]).expect("schema");
        let batch = Batch::new([Column::Int32(column)]).expect("batch");

        let record_batch = batch_to_record_batch(&schema, batch).expect("export");
        let (imported_schema, imported_batch) =
            record_batch_to_batch(record_batch).expect("import");

        assert_eq!(imported_schema, schema);
        let Column::Int32(imported) = &imported_batch.columns()[0] else {
            panic!("expected int32 column");
        };
        assert_eq!(imported.data(), &[1_i32, 0, 3]);
        let imported_nulls = imported.nulls().expect("imported nulls");
        assert!(imported_nulls.get(0));
        assert!(!imported_nulls.get(1));
        assert!(imported_nulls.get(2));
    }
}
