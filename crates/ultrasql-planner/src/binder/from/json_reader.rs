//! Streaming JSON / NDJSON record reader and the column-type accumulator that
//! infers a SQL schema from `read_json` / `read_ndjson` inputs.

use std::collections::BTreeMap;
use std::io::Read;

use serde_json::{Map as JsonMap, Value as JsonValue};
use ultrasql_core::{DataType, Field};

use super::readers::{open_planner_stream, planner_stream_specs};
use super::{PLANNER_JSON_RECORD_LIMIT_BYTES, PlanError};

pub(super) type JsonObject = JsonMap<String, JsonValue>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum JsonInputKind {
    Json,
    Ndjson,
}

pub(super) fn infer_json_fields_from_path_specs(
    function_name: &str,
    kind: JsonInputKind,
    path_specs: &[String],
) -> Result<Vec<Field>, PlanError> {
    let sources = planner_stream_specs(function_name, path_specs)?;
    let mut acc = JsonFieldAccumulator::default();
    for source in sources {
        let display = source.display();
        let mut reader =
            PlannerJsonRecordReader::new(kind, open_planner_stream(function_name, &source)?);
        while let Some((row_number, text)) = reader.next_text(function_name, &display)? {
            let value = serde_json::from_str::<JsonValue>(&text).map_err(|err| {
                PlanError::TypeMismatch(format!(
                    "{function_name} parse {display} row {row_number}: {err}"
                ))
            })?;
            let row = json_value_to_object(function_name, &display, row_number, value)?;
            acc.observe(function_name, &row)?;
        }
    }
    Ok(acc.finish())
}

pub(super) enum PlannerJsonRecordReader {
    Ndjson {
        reader: Box<dyn Read>,
        line_number: usize,
    },
    Json {
        reader: Box<dyn Read>,
        state: PlannerJsonDocumentState,
        row_number: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PlannerJsonDocumentState {
    Start,
    Array,
    Done,
}

impl PlannerJsonRecordReader {
    pub(super) fn new(kind: JsonInputKind, reader: Box<dyn Read>) -> Self {
        match kind {
            JsonInputKind::Ndjson => Self::Ndjson {
                reader,
                line_number: 0,
            },
            JsonInputKind::Json => Self::Json {
                reader,
                state: PlannerJsonDocumentState::Start,
                row_number: 0,
            },
        }
    }

    pub(super) fn next_text(
        &mut self,
        function_name: &str,
        display: &str,
    ) -> Result<Option<(usize, String)>, PlanError> {
        match self {
            Self::Ndjson {
                reader,
                line_number,
            } => planner_next_ndjson_text(reader.as_mut(), line_number, function_name, display),
            Self::Json {
                reader,
                state,
                row_number,
            } => planner_next_json_text(reader.as_mut(), state, row_number, function_name, display),
        }
    }
}

fn planner_next_ndjson_text(
    reader: &mut dyn Read,
    line_number: &mut usize,
    function_name: &str,
    display: &str,
) -> Result<Option<(usize, String)>, PlanError> {
    let mut bytes = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        bytes.clear();
        loop {
            let read = reader.read(&mut byte).map_err(|err| {
                PlanError::TypeMismatch(format!("{function_name} cannot read {display}: {err}"))
            })?;
            if read == 0 {
                if bytes.is_empty() {
                    return Ok(None);
                }
                break;
            }
            bytes.push(byte[0]);
            if bytes.len() > PLANNER_JSON_RECORD_LIMIT_BYTES {
                return Err(PlanError::TypeMismatch(format!(
                    "{function_name} record in {display} exceeds record limit: limit={PLANNER_JSON_RECORD_LIMIT_BYTES}"
                )));
            }
            if byte[0] == b'\n' {
                break;
            }
        }
        *line_number = line_number.saturating_add(1);
        let text = String::from_utf8(bytes.clone()).map_err(|err| {
            PlanError::TypeMismatch(format!("{function_name} cannot decode {display}: {err}"))
        })?;
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            return Ok(Some((*line_number, trimmed.to_owned())));
        }
    }
}

fn planner_next_json_text(
    reader: &mut dyn Read,
    state: &mut PlannerJsonDocumentState,
    row_number: &mut usize,
    function_name: &str,
    display: &str,
) -> Result<Option<(usize, String)>, PlanError> {
    loop {
        match state {
            PlannerJsonDocumentState::Start => {
                let Some(byte) = planner_read_non_ws_byte(reader, function_name, display)? else {
                    return Ok(None);
                };
                match byte {
                    b'{' => {
                        *state = PlannerJsonDocumentState::Done;
                        *row_number = 1;
                        return planner_read_json_container(reader, byte, function_name, display)
                            .map(|text| Some((*row_number, text)));
                    }
                    b'[' => *state = PlannerJsonDocumentState::Array,
                    other => {
                        return Err(PlanError::TypeMismatch(format!(
                            "{function_name} expected object or array of objects in {display}, got byte {other}"
                        )));
                    }
                }
            }
            PlannerJsonDocumentState::Array => {
                let Some(byte) = planner_read_non_ws_byte(reader, function_name, display)? else {
                    return Err(PlanError::TypeMismatch(format!(
                        "{function_name} array in {display} ended before closing bracket"
                    )));
                };
                match byte {
                    b']' => {
                        *state = PlannerJsonDocumentState::Done;
                        return Ok(None);
                    }
                    b',' => {}
                    b'{' => {
                        *row_number = row_number.saturating_add(1);
                        return planner_read_json_container(reader, byte, function_name, display)
                            .map(|text| Some((*row_number, text)));
                    }
                    other => {
                        return Err(PlanError::TypeMismatch(format!(
                            "{function_name} expected object in array {display}, got byte {other}"
                        )));
                    }
                }
            }
            PlannerJsonDocumentState::Done => return Ok(None),
        }
    }
}

fn planner_read_non_ws_byte(
    reader: &mut dyn Read,
    function_name: &str,
    display: &str,
) -> Result<Option<u8>, PlanError> {
    let mut buf = [0_u8; 1];
    loop {
        let read = reader.read(&mut buf).map_err(|err| {
            PlanError::TypeMismatch(format!("{function_name} cannot read {display}: {err}"))
        })?;
        if read == 0 {
            return Ok(None);
        }
        if !buf[0].is_ascii_whitespace() {
            return Ok(Some(buf[0]));
        }
    }
}

fn planner_read_json_container(
    reader: &mut dyn Read,
    first: u8,
    function_name: &str,
    display: &str,
) -> Result<String, PlanError> {
    let mut bytes = vec![first];
    let mut depth = 1_i32;
    let mut in_string = false;
    let mut escaped = false;
    let mut byte = [0_u8; 1];
    while depth > 0 {
        let read = reader.read(&mut byte).map_err(|err| {
            PlanError::TypeMismatch(format!("{function_name} cannot read {display}: {err}"))
        })?;
        if read == 0 {
            return Err(PlanError::TypeMismatch(format!(
                "{function_name} object in {display} ended before closing brace"
            )));
        }
        let b = byte[0];
        bytes.push(b);
        if bytes.len() > PLANNER_JSON_RECORD_LIMIT_BYTES {
            return Err(PlanError::TypeMismatch(format!(
                "{function_name} record in {display} exceeds record limit: limit={PLANNER_JSON_RECORD_LIMIT_BYTES}"
            )));
        }
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
    String::from_utf8(bytes).map_err(|err| {
        PlanError::TypeMismatch(format!("{function_name} cannot decode {display}: {err}"))
    })
}

pub(super) fn json_value_to_object(
    function_name: &str,
    display: &str,
    row_number: usize,
    value: JsonValue,
) -> Result<JsonObject, PlanError> {
    match value {
        JsonValue::Object(object) => Ok(object),
        _ => Err(PlanError::TypeMismatch(format!(
            "{function_name} row {row_number} in {display} is not a JSON object"
        ))),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum JsonColumnKind {
    Unknown,
    Bool,
    Int64,
    Float64,
    Text,
}

#[derive(Clone, Debug)]
struct JsonFieldSpec {
    name: String,
    kind: JsonColumnKind,
    nullable: bool,
}

#[derive(Default)]
pub(super) struct JsonFieldAccumulator {
    columns: BTreeMap<String, JsonFieldSpec>,
    present: BTreeMap<String, usize>,
    rows: usize,
}

impl JsonFieldAccumulator {
    pub(super) fn observe(
        &mut self,
        function_name: &str,
        row: &JsonObject,
    ) -> Result<(), PlanError> {
        self.rows = self.rows.saturating_add(1);
        for (name, value) in row {
            if name.is_empty() {
                return Err(PlanError::TypeMismatch(format!(
                    "{function_name}: JSON object contains an empty column name"
                )));
            }
            let kind = json_value_kind(value);
            self.columns
                .entry(name.clone())
                .and_modify(|spec| {
                    spec.kind = widen_json_kind(spec.kind, kind);
                    spec.nullable |= value.is_null();
                })
                .or_insert_with(|| JsonFieldSpec {
                    name: name.clone(),
                    kind,
                    nullable: value.is_null(),
                });
            *self.present.entry(name.clone()).or_insert(0) += 1;
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> Vec<Field> {
        for spec in self.columns.values_mut() {
            if self.present.get(&spec.name).copied().unwrap_or(0) < self.rows {
                spec.nullable = true;
            }
        }
        self.columns
            .into_values()
            .map(|spec| {
                let data_type = match spec.kind {
                    JsonColumnKind::Unknown => DataType::Text { max_len: None },
                    JsonColumnKind::Bool => DataType::Bool,
                    JsonColumnKind::Int64 => DataType::Int64,
                    JsonColumnKind::Float64 => DataType::Float64,
                    JsonColumnKind::Text => DataType::Text { max_len: None },
                };
                if spec.nullable {
                    Field::nullable(spec.name, data_type)
                } else {
                    Field::required(spec.name, data_type)
                }
            })
            .collect()
    }
}

pub(super) fn json_value_kind(value: &JsonValue) -> JsonColumnKind {
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

pub(super) fn widen_json_kind(left: JsonColumnKind, right: JsonColumnKind) -> JsonColumnKind {
    match (left, right) {
        (JsonColumnKind::Unknown, kind) | (kind, JsonColumnKind::Unknown) => kind,
        (JsonColumnKind::Text, _) | (_, JsonColumnKind::Text) => JsonColumnKind::Text,
        (JsonColumnKind::Float64, _) | (_, JsonColumnKind::Float64) => JsonColumnKind::Float64,
        (JsonColumnKind::Int64, JsonColumnKind::Int64) => JsonColumnKind::Int64,
        (JsonColumnKind::Bool, JsonColumnKind::Bool) => JsonColumnKind::Bool,
        _ => JsonColumnKind::Text,
    }
}
