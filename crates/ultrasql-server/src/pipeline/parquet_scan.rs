//! Local Parquet table-function scan.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray,
};
use arrow_schema::{ArrowError, DataType as ArrowDataType, Schema as ArrowSchema};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_executor::{ExecError, Operator};
use ultrasql_planner::{BinaryOp, ScalarExpr};
use ultrasql_vec::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

use crate::error::ServerError;

const PARQUET_BATCH_TARGET_ROWS: usize = 4096;

/// File-backed scan for `read_parquet(path_or_glob)`.
#[derive(Debug)]
pub(super) struct ParquetTableScan {
    schema: Schema,
    batches: VecDeque<ultrasql_vec::Batch>,
}

impl ParquetTableScan {
    /// Load Parquet files from one or more path/glob specs into a
    /// query-local scan.
    pub(super) fn from_path_specs(
        patterns: &[String],
        projection: Option<&[String]>,
        predicate: Option<&ParquetPredicate>,
    ) -> Result<Self, ServerError> {
        let paths = expand_parquet_path_specs(patterns)?;
        Self::from_paths(paths, projection, predicate)
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
        let mut batches = VecDeque::new();

        for path in paths {
            append_path_batches(
                &path,
                base_arrow_schema.as_ref(),
                projection.as_deref(),
                predicate.as_ref(),
                &mut batches,
            )?;
        }

        Ok(Self { schema, batches })
    }
}

impl Operator for ParquetTableScan {
    fn next_batch(&mut self) -> Result<Option<ultrasql_vec::Batch>, ExecError> {
        Ok(self.batches.pop_front())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
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

fn append_path_batches(
    path: &Path,
    expected_schema: &ArrowSchema,
    projection: Option<&[String]>,
    predicate: Option<&ParquetPredicate>,
    batches: &mut VecDeque<ultrasql_vec::Batch>,
) -> Result<(), ServerError> {
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

    let projection_mask = match projection {
        Some(names) => ProjectionMask::columns(
            builder.parquet_schema(),
            names.iter().map(std::string::String::as_str),
        ),
        None => ProjectionMask::all(),
    };
    let row_groups = predicate
        .map(|p| select_row_groups(builder.metadata(), expected_schema, p))
        .transpose()?;
    if row_groups.as_ref().is_some_and(Vec::is_empty) {
        return Ok(());
    }
    let row_filter = predicate.map(|p| p.row_filter(builder.parquet_schema()));

    let mut builder = builder
        .with_batch_size(PARQUET_BATCH_TARGET_ROWS)
        .with_projection(projection_mask);
    if let Some(row_groups) = row_groups {
        builder = builder.with_row_groups(row_groups);
    }
    if let Some(row_filter) = row_filter {
        builder = builder.with_row_filter(row_filter);
    }
    let reader = builder.build().map_err(|err| {
        ServerError::CopyFormat(format!(
            "read_parquet cannot read {}: {err}",
            path.display()
        ))
    })?;
    for batch in reader {
        let batch = batch.map_err(|err| {
            ServerError::CopyFormat(format!("read_parquet read {}: {err}", path.display()))
        })?;
        if batch.num_rows() == 0 {
            continue;
        }
        batches.push_back(arrow_batch_to_ultrasql(&batch)?);
    }
    Ok(())
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

fn arrow_batch_to_ultrasql(batch: &RecordBatch) -> Result<ultrasql_vec::Batch, ServerError> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, field) in batch.schema().fields().iter().enumerate() {
        columns.push(arrow_array_to_column(
            batch.column(index).as_ref(),
            field.data_type(),
        )?);
    }
    ultrasql_vec::Batch::new(columns)
        .map_err(|err| ServerError::CopyFormat(format!("read_parquet batch: {err}")))
}

fn arrow_array_to_column(
    array: &dyn Array,
    data_type: &ArrowDataType,
) -> Result<Column, ServerError> {
    match data_type {
        ArrowDataType::Boolean => bool_column(array),
        ArrowDataType::Int32 => numeric_i32_column(array),
        ArrowDataType::Int64 => numeric_i64_column(array),
        ArrowDataType::Float32 => numeric_f32_column(array),
        ArrowDataType::Float64 => numeric_f64_column(array),
        ArrowDataType::Utf8 => utf8_column(array),
        ArrowDataType::LargeUtf8 => large_utf8_column(array),
        other => Err(ServerError::CopyFormat(format!(
            "read_parquet unsupported Arrow type: {other}"
        ))),
    }
}

fn numeric_i32_column(array: &dyn Array) -> Result<Column, ServerError> {
    let typed = array
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| ServerError::CopyFormat("read_parquet Int32 downcast failed".to_owned()))?;
    let values = (0..typed.len())
        .map(|idx| {
            if typed.is_null(idx) {
                0
            } else {
                typed.value(idx)
            }
        })
        .collect::<Vec<_>>();
    let column = match validity_bitmap(typed) {
        Some(nulls) => {
            Column::Int32(NumericColumn::with_nulls(values, nulls).map_err(|err| {
                ServerError::CopyFormat(format!("read_parquet Int32 nulls: {err}"))
            })?)
        }
        None => Column::Int32(NumericColumn::from_data(values)),
    };
    Ok(column)
}

fn numeric_i64_column(array: &dyn Array) -> Result<Column, ServerError> {
    let typed = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ServerError::CopyFormat("read_parquet Int64 downcast failed".to_owned()))?;
    let values = (0..typed.len())
        .map(|idx| {
            if typed.is_null(idx) {
                0
            } else {
                typed.value(idx)
            }
        })
        .collect::<Vec<_>>();
    let column = match validity_bitmap(typed) {
        Some(nulls) => {
            Column::Int64(NumericColumn::with_nulls(values, nulls).map_err(|err| {
                ServerError::CopyFormat(format!("read_parquet Int64 nulls: {err}"))
            })?)
        }
        None => Column::Int64(NumericColumn::from_data(values)),
    };
    Ok(column)
}

fn numeric_f32_column(array: &dyn Array) -> Result<Column, ServerError> {
    let typed = array
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| {
            ServerError::CopyFormat("read_parquet Float32 downcast failed".to_owned())
        })?;
    let values = (0..typed.len())
        .map(|idx| {
            if typed.is_null(idx) {
                0.0
            } else {
                typed.value(idx)
            }
        })
        .collect::<Vec<_>>();
    let column = match validity_bitmap(typed) {
        Some(nulls) => {
            Column::Float32(NumericColumn::with_nulls(values, nulls).map_err(|err| {
                ServerError::CopyFormat(format!("read_parquet Float32 nulls: {err}"))
            })?)
        }
        None => Column::Float32(NumericColumn::from_data(values)),
    };
    Ok(column)
}

fn numeric_f64_column(array: &dyn Array) -> Result<Column, ServerError> {
    let typed = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            ServerError::CopyFormat("read_parquet Float64 downcast failed".to_owned())
        })?;
    let values = (0..typed.len())
        .map(|idx| {
            if typed.is_null(idx) {
                0.0
            } else {
                typed.value(idx)
            }
        })
        .collect::<Vec<_>>();
    let column = match validity_bitmap(typed) {
        Some(nulls) => {
            Column::Float64(NumericColumn::with_nulls(values, nulls).map_err(|err| {
                ServerError::CopyFormat(format!("read_parquet Float64 nulls: {err}"))
            })?)
        }
        None => Column::Float64(NumericColumn::from_data(values)),
    };
    Ok(column)
}

fn bool_column(array: &dyn Array) -> Result<Column, ServerError> {
    let typed = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            ServerError::CopyFormat("read_parquet Boolean downcast failed".to_owned())
        })?;
    let values = (0..typed.len())
        .map(|idx| !typed.is_null(idx) && typed.value(idx))
        .collect::<Vec<_>>();
    let column = match validity_bitmap(typed) {
        Some(nulls) => Column::Bool(BoolColumn::with_nulls(values, nulls).map_err(|err| {
            ServerError::CopyFormat(format!("read_parquet Boolean nulls: {err}"))
        })?),
        None => Column::Bool(BoolColumn::from_data(values)),
    };
    Ok(column)
}

fn utf8_column(array: &dyn Array) -> Result<Column, ServerError> {
    let typed = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ServerError::CopyFormat("read_parquet Utf8 downcast failed".to_owned()))?;
    let values = (0..typed.len())
        .map(|idx| {
            if typed.is_null(idx) {
                String::new()
            } else {
                typed.value(idx).to_owned()
            }
        })
        .collect::<Vec<_>>();
    let column = match validity_bitmap(typed) {
        Some(nulls) => {
            Column::Utf8(StringColumn::with_nulls(values, nulls).map_err(|err| {
                ServerError::CopyFormat(format!("read_parquet Utf8 nulls: {err}"))
            })?)
        }
        None => Column::Utf8(StringColumn::from_data(values)),
    };
    Ok(column)
}

fn large_utf8_column(array: &dyn Array) -> Result<Column, ServerError> {
    let typed = array
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .ok_or_else(|| {
            ServerError::CopyFormat("read_parquet LargeUtf8 downcast failed".to_owned())
        })?;
    let values = (0..typed.len())
        .map(|idx| {
            if typed.is_null(idx) {
                String::new()
            } else {
                typed.value(idx).to_owned()
            }
        })
        .collect::<Vec<_>>();
    let column = match validity_bitmap(typed) {
        Some(nulls) => Column::Utf8(StringColumn::with_nulls(values, nulls).map_err(|err| {
            ServerError::CopyFormat(format!("read_parquet LargeUtf8 nulls: {err}"))
        })?),
        None => Column::Utf8(StringColumn::from_data(values)),
    };
    Ok(column)
}

fn validity_bitmap(array: &dyn Array) -> Option<Bitmap> {
    if array.null_count() == 0 {
        return None;
    }
    let mut nulls = Bitmap::new(array.len(), true);
    for idx in 0..array.len() {
        if array.is_null(idx) {
            nulls.set(idx, false);
        }
    }
    Some(nulls)
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
        if stats.is_none_or(|stats| statistics_may_match(stats, predicate)) {
            row_groups.push(index);
        }
    }
    Ok(row_groups)
}

fn statistics_may_match(stats: &Statistics, predicate: &ParquetPredicate) -> bool {
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
    use ultrasql_core::{DataType, Value};
    use ultrasql_planner::{BinaryOp, ScalarExpr};

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
}
