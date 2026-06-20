//! `read_csv` / `sniff_csv` schema inference: header sniffing across delimiters
//! and from first-record fallbacks, for both local and object-store paths.

use std::io::Read;
use std::path::Path;

use ultrasql_core::{
    DataType, Field, Schema, Value,
    csv::{CsvParseOptions, parse_csv_records_with_options, read_csv_header_from_specs},
};
use ultrasql_objectstore::{
    expand_object_store_specs, is_object_store_uri, read_object_range_with_metadata,
};

use super::paths::{
    first_expanded_file, open_local_regular_file, path_specs_use_object_store, read_file_path_specs,
};
use super::{PlanError, READ_CSV_HEADER_SAMPLE_BYTES, ScalarExpr, ScopeEntry};

pub(super) fn bind_read_csv_table_function(
    bound_args: &[ScalarExpr],
    qualifier: &str,
) -> Result<(Schema, Vec<ScopeEntry>), PlanError> {
    if !matches!(bound_args.len(), 1 | 2) {
        return Err(PlanError::NotSupported(
            "read_csv: expected path, glob, or path-list argument plus optional reject path",
        ));
    }
    let path_specs = read_csv_path_specs(&bound_args[0])?;
    let has_reject_path = bound_args.get(1).is_some();
    if let Some(reject_arg) = bound_args.get(1) {
        validate_read_csv_reject_path_arg(reject_arg)?;
    }
    let header = if has_reject_path {
        read_csv_header_from_path_specs_with_rejects(&path_specs)?
    } else {
        read_csv_header_from_path_specs(&path_specs)?
    };
    let mut fields = header
        .into_iter()
        .map(|name| Field::nullable(name, DataType::Text { max_len: None }))
        .collect::<Vec<_>>();
    fields.push(Field::nullable(
        "_filename",
        DataType::Text { max_len: None },
    ));
    fields.push(Field::required("_row_number", DataType::Int64));
    let schema = Schema::new(fields.clone())
        .map_err(|err| PlanError::TypeMismatch(format!("read_csv schema: {err}")))?;
    let from_scope = fields
        .into_iter()
        .enumerate()
        .map(|(field_index, field)| ScopeEntry {
            qualifier: qualifier.to_owned(),
            field_index,
            field,
        })
        .collect();
    Ok((schema, from_scope))
}

fn read_csv_path_specs(arg: &ScalarExpr) -> Result<Vec<String>, PlanError> {
    read_file_path_specs("read_csv", arg)
}

pub(super) fn validate_read_csv_reject_path_arg(arg: &ScalarExpr) -> Result<(), PlanError> {
    let ScalarExpr::Literal {
        value: Value::Text(path),
        ..
    } = arg
    else {
        return Err(PlanError::TypeMismatch(
            "read_csv: reject path must be a string literal".to_owned(),
        ));
    };
    if path.is_empty() {
        return Err(PlanError::TypeMismatch(
            "read_csv: reject path must not be empty".to_owned(),
        ));
    }
    if is_object_store_uri(path) {
        return Err(PlanError::TypeMismatch(
            "read_csv: reject path must be a local file path".to_owned(),
        ));
    }
    Ok(())
}

fn read_csv_header_from_path_specs_with_rejects(
    path_specs: &[String],
) -> Result<Vec<String>, PlanError> {
    match read_csv_header_from_path_specs(path_specs) {
        Ok(header) => Ok(header),
        Err(original) => match read_csv_header_from_first_record(path_specs) {
            Ok(header) => Ok(header),
            Err(_) => Err(original),
        },
    }
}

pub(super) fn read_csv_header_from_first_record(
    path_specs: &[String],
) -> Result<Vec<String>, PlanError> {
    let (display, bytes) = if path_specs_use_object_store("read_csv", path_specs)? {
        read_first_object_csv_sample(path_specs)?
    } else {
        let first = first_expanded_file("read_csv", path_specs)?;
        let display = first.display().to_string();
        let bytes = read_local_csv_header_sample(&display, &first)?;
        (display, bytes)
    };
    let text = String::from_utf8(bytes).map_err(|err| {
        PlanError::TypeMismatch(format!("read_csv: {display} is not UTF-8: {err}"))
    })?;
    infer_csv_header_from_first_record(&display, &text)
}

fn read_local_csv_header_sample(display: &str, path: &Path) -> Result<Vec<u8>, PlanError> {
    let file = open_local_regular_file("read_csv", path)?;
    let mut bytes = Vec::new();
    file.take(READ_CSV_HEADER_SAMPLE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|err| PlanError::TypeMismatch(format!("read_csv cannot read {display}: {err}")))?;
    let sample_limit = usize::try_from(READ_CSV_HEADER_SAMPLE_BYTES).unwrap_or(usize::MAX);
    if bytes.len() > sample_limit {
        if !csv_header_sample_has_complete_record(&bytes[..sample_limit]) {
            return Err(PlanError::TypeMismatch(format!(
                "read_csv: {display} first record exceeds sample limit: limit={READ_CSV_HEADER_SAMPLE_BYTES}"
            )));
        }
        bytes.truncate(sample_limit);
    }
    Ok(bytes)
}

pub(super) fn csv_header_sample_has_complete_record(sample: &[u8]) -> bool {
    let mut in_quotes = false;
    let mut i = 0;
    while i < sample.len() {
        match sample[i] {
            b'"' if in_quotes && i + 1 < sample.len() && sample[i + 1] == b'"' => {
                i += 2;
                continue;
            }
            b'"' => in_quotes = !in_quotes,
            b'\n' | b'\r' if !in_quotes => return true,
            _ => {}
        }
        i += 1;
    }
    false
}

pub(super) fn infer_csv_header_from_first_record(
    display: &str,
    text: &str,
) -> Result<Vec<String>, PlanError> {
    let mut best: Option<Vec<String>> = None;
    let mut last_error: Option<String> = None;
    for delimiter in [',', ';', '\t', '|'] {
        let options = CsvParseOptions {
            delimiter,
            quote: Some('"'),
            escape: Some('"'),
        };
        match first_csv_record_with_options(display, text, options) {
            Ok(record) if best.as_ref().is_none_or(|best| record.len() > best.len()) => {
                best = Some(record);
            }
            Ok(_) => {}
            Err(err) => last_error = Some(err),
        }
    }
    let Some(header) = best else {
        return Err(PlanError::TypeMismatch(last_error.unwrap_or_else(|| {
            format!("read_csv header missing in {display}")
        })));
    };
    if header.is_empty() || header.iter().any(String::is_empty) {
        return Err(PlanError::TypeMismatch(format!(
            "read_csv: header contains an empty column name: {display}"
        )));
    }
    Ok(header)
}

pub(super) fn first_csv_record_with_options(
    display: &str,
    text: &str,
    options: CsvParseOptions,
) -> Result<Vec<String>, String> {
    let mut buffer = String::new();
    for line in text.split_inclusive('\n') {
        buffer.push_str(line);
        match parse_csv_records_with_options(&buffer, options) {
            Ok(mut records) if records.len() == 1 => {
                return Ok(records.remove(0));
            }
            Ok(records) if records.is_empty() => buffer.clear(),
            Ok(_) => {
                return Err(format!(
                    "read_csv parse {display}: first-record buffer produced multiple records"
                ));
            }
            Err(err) if err.to_string().contains("unterminated quoted field") => {}
            Err(err) => return Err(format!("read_csv parse {display}: {err}")),
        }
    }
    if buffer.is_empty() {
        return Err(format!("read_csv header missing in {display}"));
    }
    let mut records = parse_csv_records_with_options(&buffer, options)
        .map_err(|err| format!("read_csv parse {display}: {err}"))?;
    if records.len() == 1 {
        Ok(records.remove(0))
    } else {
        Err(format!(
            "read_csv parse {display}: first-record buffer produced {} records",
            records.len()
        ))
    }
}

fn read_csv_header_from_path_specs(path_specs: &[String]) -> Result<Vec<String>, PlanError> {
    if path_specs_use_object_store("read_csv", path_specs)? {
        let (display, bytes) = read_first_object_csv_sample(path_specs)?;
        let text = String::from_utf8(bytes).map_err(|err| {
            PlanError::TypeMismatch(format!("read_csv: {display} is not UTF-8: {err}"))
        })?;
        let header = infer_csv_header_from_first_record(&display, &text)?;
        if header.is_empty() || header.iter().any(String::is_empty) {
            return Err(PlanError::TypeMismatch(format!(
                "read_csv: header contains an empty column name: {display}"
            )));
        }
        return Ok(header);
    }
    read_csv_header_from_specs(path_specs)
        .map_err(|err| PlanError::TypeMismatch(format!("read_csv: {err}")))
}

fn read_first_object_csv_sample(path_specs: &[String]) -> Result<(String, Vec<u8>), PlanError> {
    let objects = expand_object_store_specs(path_specs)
        .map_err(|err| PlanError::TypeMismatch(format!("read_csv: {err}")))?;
    let first = objects
        .first()
        .ok_or_else(|| PlanError::TypeMismatch("read_csv: object path list is empty".to_owned()))?;
    let bytes = read_object_range_with_metadata(first, 0, READ_CSV_HEADER_SAMPLE_BYTES)
        .map_err(|err| PlanError::TypeMismatch(format!("read_csv: {err}")))?
        .into_bytes();
    Ok((first.display_uri(), bytes))
}

pub(super) fn bind_sniff_csv_table_function(
    bound_args: &[ScalarExpr],
    qualifier: &str,
) -> Result<(Schema, Vec<ScopeEntry>), PlanError> {
    if bound_args.len() != 1 {
        return Err(PlanError::NotSupported(
            "sniff_csv: expected one path argument",
        ));
    }
    let ScalarExpr::Literal {
        value: Value::Text(_),
        ..
    } = &bound_args[0]
    else {
        return Err(PlanError::TypeMismatch(
            "sniff_csv: path argument must be a string literal".to_owned(),
        ));
    };
    let fields = vec![
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
    ];
    let schema = Schema::new(fields.clone())
        .map_err(|err| PlanError::TypeMismatch(format!("sniff_csv schema: {err}")))?;
    let from_scope = fields
        .into_iter()
        .enumerate()
        .map(|(field_index, field)| ScopeEntry {
            qualifier: qualifier.to_owned(),
            field_index,
            field,
        })
        .collect();
    Ok((schema, from_scope))
}
