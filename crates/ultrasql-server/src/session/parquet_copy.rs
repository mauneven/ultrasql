//! Parquet-backed server-side `COPY` helpers.

use std::fs::{self, File, OpenOptions};
use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray,
};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::TableEntry;
use ultrasql_core::{DataType, RelationId, Schema, Value, coerce_bpchar_text};
use ultrasql_executor::RowCodec;
use ultrasql_txn::IsolationLevel;

use super::Session;
use super::copy::{CopyInsertBatch, add_copy_batch_rows, copy_table_key, increment_copy_rows};
use crate::error::ServerError;

const PARQUET_COPY_BATCH_ROWS: usize = 4096;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(super) fn copy_to_parquet_file(
        &mut self,
        entry: &TableEntry,
        columns: &[usize],
        stream_schema: &Schema,
        path: &str,
    ) -> Result<u64, ServerError> {
        let arrow_schema = Arc::new(copy_arrow_schema(stream_schema)?);
        let file = create_parquet_output_file(path)?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&arrow_schema), None)
            .map_err(|err| ServerError::CopyFormat(format!("COPY TO parquet {path}: {err}")))?;

        let rel = RelationId(entry.oid);
        let block_count = self.state.heap.block_count(rel).max(entry.n_blocks);
        let codec = RowCodec::new(entry.schema.clone());

        // `COPY ... TO '<path>' (FORMAT parquet)` is a read: in an explicit
        // transaction it scans the session txn's (command-advanced) snapshot so
        // it sees this session's own in-txn writes, without begin/commit; in
        // autocommit it runs today's implicit read txn. The whole scan + write
        // is synchronous (parquet file I/O is blocking), so no borrow crosses an
        // await. The Arrow writer is moved into the closure and closed there so
        // a scan/write/close failure routes through the same finalisation.
        let rows = self.with_copy_read_snapshot(
            "COPY TO parquet scan commit",
            "COPY TO parquet rollback after scan error",
            move |session, snapshot| {
                let mut batch = ParquetBatchBuilder::new(stream_schema)?;
                let mut rows = 0_u64;
                let scan = session.state.heap.scan_visible(
                    rel,
                    block_count,
                    snapshot,
                    session.state.txn_manager.as_ref(),
                );
                for tuple in scan {
                    let tuple = tuple.map_err(|err| {
                        ServerError::ddl(format!("COPY TO parquet heap scan: {err}"))
                    })?;
                    let row = codec.decode(&tuple.data).map_err(|err| {
                        ServerError::CopyFormat(format!("COPY TO parquet row decode: {err}"))
                    })?;
                    batch.push_projected_row(&row, &entry.schema, columns)?;
                    if batch.len() == PARQUET_COPY_BATCH_ROWS {
                        let record_batch = batch.take_record_batch(Arc::clone(&arrow_schema))?;
                        writer.write(&record_batch).map_err(|err| {
                            ServerError::CopyFormat(format!("COPY TO parquet {path}: {err}"))
                        })?;
                    }
                    increment_copy_rows(&mut rows, "COPY TO parquet")?;
                }
                if !batch.is_empty() {
                    let record_batch = batch.take_record_batch(Arc::clone(&arrow_schema))?;
                    writer.write(&record_batch).map_err(|err| {
                        ServerError::CopyFormat(format!("COPY TO parquet {path}: {err}"))
                    })?;
                }
                writer.close().map_err(|err| {
                    ServerError::CopyFormat(format!("COPY TO parquet {path}: {err}"))
                })?;
                Ok(rows)
            },
        )?;
        Ok(rows)
    }

    pub(super) fn copy_from_parquet_file(
        &mut self,
        entry: &TableEntry,
        columns: &[usize],
        stream_schema: &Schema,
        path: &str,
    ) -> Result<u64, ServerError> {
        let file = open_parquet_input_file(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
            ServerError::CopyFormat(format!("COPY FROM parquet cannot inspect {path}: {err}"))
        })?;
        validate_parquet_copy_schema(builder.schema().as_ref(), stream_schema, path)?;
        let reader = builder
            .with_batch_size(PARQUET_COPY_BATCH_ROWS)
            .build()
            .map_err(|err| {
                ServerError::CopyFormat(format!("COPY FROM parquet cannot read {path}: {err}"))
            })?;

        // Shape A: in an explicit transaction block the parquet import rides the
        // SESSION txn so a later ROLLBACK discards its rows and a COMMIT makes
        // them durable atomically; in autocommit it opens today's implicit txn.
        // The parquet reader is blocking file I/O (no `.await`), so the session
        // txn taken out here never has a borrow cross an await.
        let session_mode = self.copy_in_session_txn();
        let txn = if session_mode {
            match std::mem::replace(&mut self.txn_state, crate::TxnState::Idle) {
                crate::TxnState::InTransaction(mut txn) => {
                    self.state.txn_manager.refresh_snapshot(&mut txn);
                    txn
                }
                other => {
                    self.txn_state = other;
                    return Err(ServerError::Unsupported(
                        "COPY FROM parquet session txn vanished mid-dispatch",
                    ));
                }
            }
        } else {
            self.state.txn_manager.begin(IsolationLevel::ReadCommitted)
        };
        // PostgreSQL applies omitted-column defaults under parquet COPY exactly
        // as under text/CSV/binary. When a defaulted column is omitted, build
        // NARROW rows (stream schema) and apply defaults downstream.
        let apply_defaults = self.copy_column_list_applies_defaults(entry, columns);
        let codec = if apply_defaults {
            RowCodec::new(stream_schema.clone())
        } else {
            RowCodec::new(entry.schema.clone())
        };
        let mut payload_batch: Vec<Vec<u8>> = Vec::with_capacity(PARQUET_COPY_BATCH_ROWS);
        let mut rows_inserted = 0_u64;

        let import_result = {
            (|| -> Result<u64, ServerError> {
                for batch in reader {
                    let batch = batch.map_err(|err| {
                        ServerError::CopyFormat(format!("COPY FROM parquet read {path}: {err}"))
                    })?;
                    validate_parquet_copy_schema(batch.schema().as_ref(), stream_schema, path)?;
                    for row_index in 0..batch.num_rows() {
                        let row = parquet_batch_row_to_values(
                            &batch,
                            row_index,
                            entry,
                            columns,
                            apply_defaults,
                        )?;
                        let payload = codec.encode(&row).map_err(|err| {
                            ServerError::CopyFormat(format!("COPY FROM parquet row encode: {err}"))
                        })?;
                        payload_batch.push(payload);
                        if payload_batch.len() == PARQUET_COPY_BATCH_ROWS {
                            add_copy_batch_rows(
                                &mut rows_inserted,
                                payload_batch.len(),
                                "COPY FROM parquet",
                            )?;
                            self.flush_copy_insert_batch(
                                entry,
                                CopyInsertBatch {
                                    payloads: &payload_batch,
                                    columns,
                                    stream_schema,
                                    apply_defaults,
                                },
                                &txn,
                                !session_mode,
                            )?;
                            payload_batch.clear();
                        }
                    }
                }
                if !payload_batch.is_empty() {
                    add_copy_batch_rows(
                        &mut rows_inserted,
                        payload_batch.len(),
                        "COPY FROM parquet",
                    )?;
                    self.flush_copy_insert_batch(
                        entry,
                        CopyInsertBatch {
                            payloads: &payload_batch,
                            columns,
                            stream_schema,
                            apply_defaults,
                        },
                        &txn,
                        !session_mode,
                    )?;
                    payload_batch.clear();
                }
                Ok(rows_inserted)
            })()
        };

        let rows = match import_result {
            Ok(rows) => rows,
            Err(err) => {
                return Err(self.fail_or_rollback_copy_from(
                    session_mode,
                    txn,
                    err,
                    "COPY FROM parquet rollback after import error",
                ));
            }
        };
        if session_mode {
            // Park the session txn back FIRST, then note the table (see the text
            // STDIN path for the full rationale): `note_copy_in_session` touches
            // only `self.pending_table_modifications`, valid after the park, and
            // an overflow error then routes through `fail_if_in_transaction` (a
            // real `Failed(txn)` transition) rather than dropping the owned txn
            // and leaving the session `Idle`.
            self.txn_state = crate::TxnState::InTransaction(txn);
            self.note_copy_in_session(&copy_table_key(entry), rows)
                .map_err(|e| self.fail_if_in_transaction(e))?;
        } else {
            // Autocommit parquet COPY keeps its prior finalisation byte-for-byte
            // (the caller `copy_from_file` notes GC + table modifications +
            // plan-cache invalidation after this returns).
            self.finalise_copy_from_commit(txn, rows, "COPY FROM parquet")?;
        }
        Ok(rows)
    }
}

fn create_parquet_output_file(path: &str) -> Result<File, ServerError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|err| ServerError::Io(std::io::Error::other(format!("{path}: {err}"))))
}

fn open_parquet_input_file(path: &str) -> Result<File, ServerError> {
    let metadata = fs::symlink_metadata(Path::new(path))
        .map_err(|err| ServerError::Io(std::io::Error::other(format!("{path}: {err}"))))?;
    if !metadata.file_type().is_file() {
        return Err(ServerError::CopyFormat(format!(
            "COPY FROM parquet file is not a regular file: {path}"
        )));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    options
        .open(path)
        .map_err(|err| ServerError::Io(std::io::Error::other(format!("{path}: {err}"))))
}

struct ParquetBatchBuilder<'a> {
    schema: &'a Schema,
    columns: Vec<ParquetColumnBuffer>,
    rows: usize,
}

impl<'a> ParquetBatchBuilder<'a> {
    fn new(schema: &'a Schema) -> Result<Self, ServerError> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| ParquetColumnBuffer::new(&field.data_type))
            .collect::<Result<Vec<_>, ServerError>>()?;
        Ok(Self {
            schema,
            columns,
            rows: 0,
        })
    }

    fn is_empty(&self) -> bool {
        self.rows == 0
    }

    fn len(&self) -> usize {
        self.rows
    }

    fn push_projected_row(
        &mut self,
        row: &[Value],
        table_schema: &Schema,
        columns: &[usize],
    ) -> Result<(), ServerError> {
        for (stream_index, field) in self.schema.fields().iter().enumerate() {
            let table_index = projected_column_index(columns, stream_index);
            let value = row.get(table_index).ok_or_else(|| {
                ServerError::CopyFormat(format!(
                    "COPY TO parquet row missing column {stream_index}"
                ))
            })?;
            let table_field = table_schema.field_at(table_index);
            self.columns[stream_index].push(value, &table_field.data_type)?;
            if table_field.data_type != field.data_type {
                return Err(ServerError::CopyFormat(format!(
                    "COPY TO parquet schema mismatch for column {}",
                    field.name
                )));
            }
        }
        self.rows += 1;
        Ok(())
    }

    fn take_record_batch(
        &mut self,
        arrow_schema: Arc<ArrowSchema>,
    ) -> Result<RecordBatch, ServerError> {
        let arrays = self
            .columns
            .iter_mut()
            .map(ParquetColumnBuffer::take_array)
            .collect::<Vec<_>>();
        self.rows = 0;
        RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|err| ServerError::CopyFormat(format!("COPY parquet batch: {err}")))
    }
}

enum ParquetColumnBuffer {
    Bool(Vec<Option<bool>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Utf8(Vec<Option<String>>),
}

impl ParquetColumnBuffer {
    fn new(data_type: &DataType) -> Result<Self, ServerError> {
        Ok(match data_type {
            DataType::Bool => Self::Bool(Vec::with_capacity(PARQUET_COPY_BATCH_ROWS)),
            DataType::Int16 | DataType::Int32 => {
                Self::Int32(Vec::with_capacity(PARQUET_COPY_BATCH_ROWS))
            }
            DataType::Int64 => Self::Int64(Vec::with_capacity(PARQUET_COPY_BATCH_ROWS)),
            DataType::Float32 => Self::Float32(Vec::with_capacity(PARQUET_COPY_BATCH_ROWS)),
            DataType::Float64 => Self::Float64(Vec::with_capacity(PARQUET_COPY_BATCH_ROWS)),
            DataType::Text { .. } | DataType::Char { .. } => {
                Self::Utf8(Vec::with_capacity(PARQUET_COPY_BATCH_ROWS))
            }
            other => {
                return Err(ServerError::CopyFormat(format!(
                    "COPY parquet unsupported type: {other}"
                )));
            }
        })
    }

    fn push(&mut self, value: &Value, data_type: &DataType) -> Result<(), ServerError> {
        if matches!(value, Value::Null) {
            self.push_null();
            return Ok(());
        }
        match (self, data_type, value) {
            (Self::Bool(values), DataType::Bool, Value::Bool(value)) => values.push(Some(*value)),
            (Self::Int32(values), DataType::Int16, Value::Int16(value)) => {
                values.push(Some(i32::from(*value)));
            }
            (Self::Int32(values), DataType::Int32, Value::Int32(value)) => {
                values.push(Some(*value));
            }
            (Self::Int64(values), DataType::Int64, Value::Int64(value)) => {
                values.push(Some(*value));
            }
            (Self::Float32(values), DataType::Float32, Value::Float32(value)) => {
                values.push(Some(*value));
            }
            (Self::Float64(values), DataType::Float64, Value::Float64(value)) => {
                values.push(Some(*value));
            }
            (Self::Utf8(values), DataType::Text { .. }, Value::Text(value))
            | (Self::Utf8(values), DataType::Char { .. }, Value::Char(value)) => {
                values.push(Some(value.clone()));
            }
            (_, expected, got) => {
                return Err(ServerError::CopyFormat(format!(
                    "COPY parquet type mismatch: expected {expected}, got {}",
                    got.data_type()
                )));
            }
        }
        Ok(())
    }

    fn push_null(&mut self) {
        match self {
            Self::Bool(values) => values.push(None),
            Self::Int32(values) => values.push(None),
            Self::Int64(values) => values.push(None),
            Self::Float32(values) => values.push(None),
            Self::Float64(values) => values.push(None),
            Self::Utf8(values) => values.push(None),
        }
    }

    fn take_array(&mut self) -> ArrayRef {
        match self {
            Self::Bool(values) => Arc::new(BooleanArray::from(std::mem::take(values))),
            Self::Int32(values) => Arc::new(Int32Array::from(std::mem::take(values))),
            Self::Int64(values) => Arc::new(Int64Array::from(std::mem::take(values))),
            Self::Float32(values) => Arc::new(Float32Array::from(std::mem::take(values))),
            Self::Float64(values) => Arc::new(Float64Array::from(std::mem::take(values))),
            Self::Utf8(values) => {
                let owned = std::mem::take(values);
                let refs = owned.iter().map(Option::as_deref).collect::<Vec<_>>();
                Arc::new(StringArray::from(refs))
            }
        }
    }
}

fn parquet_batch_row_to_values(
    batch: &RecordBatch,
    row_index: usize,
    entry: &TableEntry,
    columns: &[usize],
    apply_defaults: bool,
) -> Result<Vec<Value>, ServerError> {
    // `apply_defaults` builds a NARROW row (only the streamed columns, in stream
    // order) so the downstream INSERT operator fills omitted-column defaults;
    // otherwise build a FULL-WIDTH row with NULL in omitted positions.
    let mut row = if apply_defaults {
        Vec::with_capacity(batch.schema().fields().len())
    } else {
        vec![Value::Null; entry.schema.len()]
    };
    for (stream_index, arrow_field) in batch.schema().fields().iter().enumerate() {
        let table_index = projected_column_index(columns, stream_index);
        let field = entry.schema.field_at(table_index);
        if !arrow_field.name().eq_ignore_ascii_case(&field.name) {
            return Err(ServerError::CopyFormat(format!(
                "COPY FROM parquet column {} expected {}, got {}",
                stream_index,
                field.name,
                arrow_field.name()
            )));
        }
        let value = arrow_cell_to_value(
            batch.column(stream_index).as_ref(),
            row_index,
            &field.data_type,
        )?;
        if apply_defaults {
            row.push(value);
        } else {
            row[table_index] = value;
        }
    }
    Ok(row)
}

fn copy_arrow_schema(schema: &Schema) -> Result<ArrowSchema, ServerError> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            Ok(ArrowField::new(
                field.name.clone(),
                copy_arrow_data_type(&field.data_type)?,
                field.nullable,
            ))
        })
        .collect::<Result<Vec<_>, ServerError>>()?;
    Ok(ArrowSchema::new(fields))
}

fn validate_parquet_copy_schema(
    arrow_schema: &ArrowSchema,
    stream_schema: &Schema,
    path: &str,
) -> Result<(), ServerError> {
    if arrow_schema.fields().len() != stream_schema.len() {
        return Err(ServerError::CopyFormat(format!(
            "COPY FROM parquet {path}: expected {} columns, got {}",
            stream_schema.len(),
            arrow_schema.fields().len()
        )));
    }
    for (index, (arrow_field, field)) in arrow_schema
        .fields()
        .iter()
        .zip(stream_schema.fields())
        .enumerate()
    {
        if !arrow_field.name().eq_ignore_ascii_case(&field.name) {
            return Err(ServerError::CopyFormat(format!(
                "COPY FROM parquet {path}: column {index} expected {}, got {}",
                field.name,
                arrow_field.name()
            )));
        }
        if !arrow_type_matches_copy(arrow_field.data_type(), &field.data_type) {
            return Err(ServerError::CopyFormat(format!(
                "COPY FROM parquet {path}: column {} expected {}, got {}",
                field.name,
                copy_arrow_data_type(&field.data_type)?,
                arrow_field.data_type()
            )));
        }
    }
    Ok(())
}

fn copy_arrow_data_type(data_type: &DataType) -> Result<ArrowDataType, ServerError> {
    match data_type {
        DataType::Bool => Ok(ArrowDataType::Boolean),
        DataType::Int16 | DataType::Int32 => Ok(ArrowDataType::Int32),
        DataType::Int64 => Ok(ArrowDataType::Int64),
        DataType::Float32 => Ok(ArrowDataType::Float32),
        DataType::Float64 => Ok(ArrowDataType::Float64),
        DataType::Text { .. } | DataType::Char { .. } => Ok(ArrowDataType::Utf8),
        other => Err(ServerError::CopyFormat(format!(
            "COPY parquet unsupported type: {other}"
        ))),
    }
}

fn arrow_type_matches_copy(arrow_type: &ArrowDataType, data_type: &DataType) -> bool {
    match (arrow_type, data_type) {
        (
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8,
            DataType::Text { .. } | DataType::Char { .. },
        ) => true,
        (_, _) => copy_arrow_data_type(data_type).is_ok_and(|expected| &expected == arrow_type),
    }
}

fn projected_column_index(columns: &[usize], stream_index: usize) -> usize {
    columns.get(stream_index).copied().unwrap_or(stream_index)
}

fn arrow_cell_to_value(
    array: &dyn Array,
    row_index: usize,
    data_type: &DataType,
) -> Result<Value, ServerError> {
    if array.is_null(row_index) {
        return Ok(Value::Null);
    }
    match data_type {
        DataType::Bool => bool_cell(array, row_index),
        DataType::Int16 => int16_cell(array, row_index),
        DataType::Int32 => int32_cell(array, row_index),
        DataType::Int64 => int64_cell(array, row_index),
        DataType::Float32 => float32_cell(array, row_index),
        DataType::Float64 => float64_cell(array, row_index),
        DataType::Text { .. } => text_cell(array, row_index),
        DataType::Char { len } => char_cell(array, row_index, *len),
        other => Err(ServerError::CopyFormat(format!(
            "COPY parquet unsupported type: {other}"
        ))),
    }
}

fn bool_cell(array: &dyn Array, row_index: usize) -> Result<Value, ServerError> {
    let typed = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            ServerError::CopyFormat("COPY FROM parquet Boolean downcast failed".to_owned())
        })?;
    Ok(Value::Bool(typed.value(row_index)))
}

fn int16_cell(array: &dyn Array, row_index: usize) -> Result<Value, ServerError> {
    if let Some(typed) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(Value::Int16(typed.value(row_index)));
    }
    if let Some(typed) = array.as_any().downcast_ref::<Int32Array>() {
        let value = i16::try_from(typed.value(row_index)).map_err(|_| {
            ServerError::CopyFormat("COPY FROM parquet SMALLINT overflow".to_owned())
        })?;
        return Ok(Value::Int16(value));
    }
    Err(ServerError::CopyFormat(
        "COPY FROM parquet Int16 downcast failed".to_owned(),
    ))
}

fn int32_cell(array: &dyn Array, row_index: usize) -> Result<Value, ServerError> {
    if let Some(typed) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(Value::Int32(i32::from(typed.value(row_index))));
    }
    if let Some(typed) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(Value::Int32(typed.value(row_index)));
    }
    if let Some(typed) = array.as_any().downcast_ref::<Int64Array>() {
        let value = i32::try_from(typed.value(row_index)).map_err(|_| {
            ServerError::CopyFormat("COPY FROM parquet INTEGER overflow".to_owned())
        })?;
        return Ok(Value::Int32(value));
    }
    Err(ServerError::CopyFormat(
        "COPY FROM parquet Int32 downcast failed".to_owned(),
    ))
}

fn int64_cell(array: &dyn Array, row_index: usize) -> Result<Value, ServerError> {
    if let Some(typed) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(Value::Int64(i64::from(typed.value(row_index))));
    }
    if let Some(typed) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(Value::Int64(i64::from(typed.value(row_index))));
    }
    if let Some(typed) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Value::Int64(typed.value(row_index)));
    }
    Err(ServerError::CopyFormat(
        "COPY FROM parquet Int64 downcast failed".to_owned(),
    ))
}

fn float32_cell(array: &dyn Array, row_index: usize) -> Result<Value, ServerError> {
    let typed = array
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| {
            ServerError::CopyFormat("COPY FROM parquet Float32 downcast failed".to_owned())
        })?;
    Ok(Value::Float32(typed.value(row_index)))
}

fn float64_cell(array: &dyn Array, row_index: usize) -> Result<Value, ServerError> {
    if let Some(typed) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(Value::Float64(f64::from(typed.value(row_index))));
    }
    if let Some(typed) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(Value::Float64(typed.value(row_index)));
    }
    Err(ServerError::CopyFormat(
        "COPY FROM parquet Float64 downcast failed".to_owned(),
    ))
}

fn text_cell(array: &dyn Array, row_index: usize) -> Result<Value, ServerError> {
    if let Some(typed) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Value::Text(typed.value(row_index).to_owned()));
    }
    if let Some(typed) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Value::Text(typed.value(row_index).to_owned()));
    }
    Err(ServerError::CopyFormat(
        "COPY FROM parquet Utf8 downcast failed".to_owned(),
    ))
}

fn char_cell(array: &dyn Array, row_index: usize, len: Option<u32>) -> Result<Value, ServerError> {
    let Value::Text(text) = text_cell(array, row_index)? else {
        return Err(ServerError::CopyFormat(
            "COPY FROM parquet Utf8 downcast failed".to_owned(),
        ));
    };
    coerce_bpchar_text(&text, len, false)
        .map(Value::Char)
        .map_err(|err| ServerError::CopyFormat(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::super::copy::add_copy_rows;

    #[test]
    fn parquet_copy_row_count_helpers_reject_overflow() {
        let mut rows = u64::MAX;
        let err = add_copy_rows(&mut rows, 1, "COPY parquet")
            .expect_err("parquet COPY row counter overflow must not saturate");
        assert_eq!(err.sqlstate(), "22003");
        assert_eq!(rows, u64::MAX);
    }

    #[cfg(unix)]
    #[test]
    fn parquet_file_helpers_reject_symlinks() {
        use super::{create_parquet_output_file, open_parquet_input_file};
        use std::fs;
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().expect("temp dir");
        let input_target = dir.path().join("input.parquet");
        let input_link = dir.path().join("input-link.parquet");
        fs::write(&input_target, b"not parquet").expect("write input");
        symlink(&input_target, &input_link).expect("input symlink");

        assert!(open_parquet_input_file(input_link.to_str().expect("utf8 input")).is_err());

        let output_target = dir.path().join("output-target.parquet");
        let output_link = dir.path().join("output-link.parquet");
        fs::write(&output_target, b"keep").expect("write output target");
        symlink(&output_target, &output_link).expect("output symlink");

        assert!(create_parquet_output_file(output_link.to_str().expect("utf8 output")).is_err());
        assert_eq!(
            fs::read(&output_target).expect("read output target"),
            b"keep"
        );
    }
}
