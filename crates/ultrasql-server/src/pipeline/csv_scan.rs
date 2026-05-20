//! Local CSV table-function scan.

use ultrasql_core::csv::{expand_csv_paths, read_csv_records_from_path};
use ultrasql_core::{DataType, Field, Schema};
use ultrasql_executor::{ExecError, Operator};
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, StringColumn};

use crate::error::ServerError;

const CSV_BATCH_TARGET_ROWS: usize = 4096;

/// File-backed scan for `read_csv(path_or_glob)`.
#[derive(Debug)]
pub(super) struct CsvTableScan {
    schema: Schema,
    rows: Vec<Vec<String>>,
    position: usize,
}

impl CsvTableScan {
    /// Load matching CSV files into a query-local scan.
    pub(super) fn from_pattern(pattern: &str) -> Result<Self, ServerError> {
        let paths = expand_csv_paths(pattern)
            .map_err(|err| ServerError::CopyFormat(format!("read_csv: {err}")))?;
        let mut expected_header: Option<Vec<String>> = None;
        let mut rows = Vec::new();

        for path in paths {
            let records = read_csv_records_from_path(&path)
                .map_err(|err| ServerError::CopyFormat(format!("{err}")))?;
            let header = records.first().ok_or_else(|| {
                ServerError::CopyFormat(format!(
                    "read_csv file has no header row: {}",
                    path.display()
                ))
            })?;
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

            for (row_index, record) in records.iter().enumerate().skip(1) {
                if record.len() != header.len() {
                    return Err(ServerError::CopyFormat(format!(
                        "read_csv row {} in {} has {} columns, expected {}",
                        row_index + 1,
                        path.display(),
                        record.len(),
                        header.len()
                    )));
                }
                rows.push(record.clone());
            }
        }

        let header = expected_header.expect("expand_csv_paths returns at least one file");
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

fn csv_batch(rows: &[Vec<String>]) -> Result<Batch, ExecError> {
    let width = rows.first().map_or(0, Vec::len);
    let mut columns = Vec::with_capacity(width);
    for col_idx in 0..width {
        let values = rows
            .iter()
            .map(|row| row[col_idx].clone())
            .collect::<Vec<_>>();
        columns.push(Column::Utf8(StringColumn::from_data(values)));
    }
    Batch::new(columns).map_err(ExecError::from)
}

fn csv_schema(header: &[String]) -> Result<Schema, ServerError> {
    let fields = header
        .iter()
        .cloned()
        .map(|name| Field::nullable(name, DataType::Text { max_len: None }))
        .collect::<Vec<_>>();
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
