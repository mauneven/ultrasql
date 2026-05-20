//! Local CSV table-function scans.

use ultrasql_core::csv::{
    CsvSniff, expand_csv_path_specs, expand_csv_paths, read_csv_data_from_path,
    read_csv_data_from_text, sniff_csv_path,
};
use ultrasql_core::{DataType, Field, Schema};
use ultrasql_executor::{ExecError, Operator};
use ultrasql_objectstore::{expand_object_store_specs, is_object_store_uri, read_object_bytes};
use ultrasql_vec::Batch;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn, StringColumn};

use crate::error::ServerError;

const CSV_BATCH_TARGET_ROWS: usize = 4096;

/// File-backed scan for `read_csv(path_or_glob)`.
#[derive(Debug)]
pub(super) struct CsvTableScan {
    schema: Schema,
    rows: Vec<CsvTableRow>,
    position: usize,
}

#[derive(Clone, Debug)]
struct CsvTableRow {
    values: Vec<String>,
    filename: String,
    row_number: i64,
}

impl CsvTableScan {
    /// Load matching CSV files into a query-local scan.
    pub(super) fn from_pattern(pattern: &str) -> Result<Self, ServerError> {
        let paths = expand_csv_paths(pattern)
            .map_err(|err| ServerError::CopyFormat(format!("read_csv: {err}")))?;
        Self::from_paths(paths)
    }

    /// Load CSV files from one or more path/glob specs into a query-local scan.
    pub(super) fn from_path_specs(patterns: &[String]) -> Result<Self, ServerError> {
        if path_specs_use_object_store("read_csv", patterns)? {
            return Self::from_object_specs(patterns);
        }
        if let [pattern] = patterns {
            return Self::from_pattern(pattern);
        }
        let paths = expand_csv_path_specs(patterns)
            .map_err(|err| ServerError::CopyFormat(format!("read_csv: {err}")))?;
        Self::from_paths(paths)
    }

    fn from_paths(paths: Vec<std::path::PathBuf>) -> Result<Self, ServerError> {
        let mut expected_header: Option<Vec<String>> = None;
        let mut rows = Vec::new();

        for path in paths {
            let data = read_csv_data_from_path(&path)
                .map_err(|err| ServerError::CopyFormat(format!("{err}")))?;
            let header = &data.header;
            validate_header(header, &path)?;
            if let Some(expected) = &expected_header {
                if header != expected {
                    return Err(ServerError::CopyFormat(format!(
                        "read_csv header mismatch in {}",
                        path.display()
                    )));
                }
            } else {
                expected_header = Some(header.clone());
            }

            for (row_index, record) in data.records.iter().enumerate() {
                if record.len() != header.len() {
                    return Err(ServerError::CopyFormat(format!(
                        "read_csv row {} in {} has {} columns, expected {}",
                        row_index + 1,
                        path.display(),
                        record.len(),
                        header.len()
                    )));
                }
                let row_number = i64::try_from(row_index + 1).map_err(|_| {
                    ServerError::CopyFormat(format!(
                        "read_csv row number overflow in {}",
                        path.display()
                    ))
                })?;
                rows.push(CsvTableRow {
                    values: record.clone(),
                    filename: path.display().to_string(),
                    row_number,
                });
            }
        }

        let header = expected_header.expect("CSV path expansion returns at least one file");
        let schema = csv_schema(&header)?;
        Ok(Self {
            schema,
            rows,
            position: 0,
        })
    }

    fn from_object_specs(patterns: &[String]) -> Result<Self, ServerError> {
        let objects = expand_object_store_specs(patterns)
            .map_err(|err| ServerError::CopyFormat(format!("read_csv: {err}")))?;
        let mut expected_header: Option<Vec<String>> = None;
        let mut rows = Vec::new();

        for object in objects {
            let display = object.display_uri();
            let bytes = read_object_bytes(&object)
                .map_err(|err| ServerError::CopyFormat(format!("read_csv: {err}")))?;
            let text = String::from_utf8(bytes).map_err(|err| {
                ServerError::CopyFormat(format!("read_csv cannot decode {display}: {err}"))
            })?;
            let data = read_csv_data_from_text(&display, &text)
                .map_err(|err| ServerError::CopyFormat(format!("{err}")))?;
            let header = &data.header;
            validate_object_header(header, &display)?;
            if let Some(expected) = &expected_header {
                if header != expected {
                    return Err(ServerError::CopyFormat(format!(
                        "read_csv header mismatch in {display}"
                    )));
                }
            } else {
                expected_header = Some(header.clone());
            }

            for (row_index, record) in data.records.iter().enumerate() {
                if record.len() != header.len() {
                    return Err(ServerError::CopyFormat(format!(
                        "read_csv row {} in {} has {} columns, expected {}",
                        row_index + 1,
                        display,
                        record.len(),
                        header.len()
                    )));
                }
                let row_number = i64::try_from(row_index + 1).map_err(|_| {
                    ServerError::CopyFormat(format!("read_csv row number overflow in {display}"))
                })?;
                rows.push(CsvTableRow {
                    values: record.clone(),
                    filename: display.clone(),
                    row_number,
                });
            }
        }

        let header = expected_header.expect("object expansion returns at least one file");
        let schema = csv_schema(&header)?;
        Ok(Self {
            schema,
            rows,
            position: 0,
        })
    }
}

impl Operator for CsvTableScan {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.position >= self.rows.len() {
            return Ok(None);
        }
        let end = self
            .position
            .saturating_add(CSV_BATCH_TARGET_ROWS)
            .min(self.rows.len());
        let batch = csv_batch(&self.rows[self.position..end])?;
        self.position = end;
        Ok(Some(batch))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn csv_batch(rows: &[CsvTableRow]) -> Result<Batch, ExecError> {
    let width = rows.first().map_or(0, |row| row.values.len());
    let mut columns = Vec::with_capacity(width + 2);
    for col_idx in 0..width {
        let values = rows
            .iter()
            .map(|row| row.values[col_idx].clone())
            .collect::<Vec<_>>();
        columns.push(Column::Utf8(StringColumn::from_data(values)));
    }
    columns.push(Column::Utf8(StringColumn::from_data(
        rows.iter().map(|row| row.filename.clone()),
    )));
    columns.push(Column::Int64(NumericColumn::from_data(
        rows.iter().map(|row| row.row_number).collect(),
    )));
    Batch::new(columns).map_err(ExecError::from)
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
