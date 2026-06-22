//! `search_path` resolution helpers and embedded local-query result decoding.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

pub(crate) fn search_path_contains_schema(search_path: Option<&str>, schema_name: &str) -> bool {
    let folded = schema_name.to_ascii_lowercase();
    if matches!(folded.as_str(), "pg_catalog" | "information_schema") {
        return true;
    }
    let Some(search_path) = search_path else {
        return folded == "public";
    };
    search_path.split(',').any(|part| {
        normalize_search_path_schema(part)
            .as_deref()
            .is_some_and(|schema| schema == folded)
    })
}

pub(crate) fn type_name_namespace_and_name(name: &str) -> Option<(&str, &str)> {
    let (schema_name, type_name) = name.rsplit_once('.')?;
    (!schema_name.is_empty() && !type_name.is_empty()).then_some((schema_name, type_name))
}

pub(crate) fn parse_pg_identifier_path(text: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut chars = text.chars().peekable();
    loop {
        match chars.peek().copied()? {
            '"' => {
                chars.next();
                let mut part = String::new();
                loop {
                    match chars.next()? {
                        '"' if chars.peek() == Some(&'"') => {
                            chars.next();
                            part.push('"');
                        }
                        '"' => break,
                        ch => part.push(ch),
                    }
                }
                parts.push(part);
            }
            _ => {
                let mut part = String::new();
                while let Some(ch) = chars.peek().copied() {
                    if ch == '.' {
                        break;
                    }
                    part.push(ch);
                    chars.next();
                }
                if part.is_empty() {
                    return None;
                }
                parts.push(part);
            }
        }
        match chars.next() {
            Some('.') => continue,
            None => return Some(parts),
            Some(_) => return None,
        }
    }
}

pub(crate) fn sequence_lookup_key(schema_name: &str, sequence_name: &str) -> String {
    ultrasql_catalog::table_lookup_key(schema_name, sequence_name)
}

pub(crate) fn table_entry_lookup_key(entry: &TableEntry) -> String {
    ultrasql_catalog::table_lookup_key(&entry.schema_name, &entry.name)
}

pub(crate) fn search_path_schema_names(search_path: Option<&str>) -> Vec<String> {
    let Some(search_path) = search_path else {
        return vec!["public".to_owned()];
    };
    search_path
        .split(',')
        .filter_map(normalize_search_path_schema)
        .collect()
}

pub(crate) fn normalize_search_path_schema(part: &str) -> Option<String> {
    let trimmed = part.trim();
    if trimmed.is_empty() {
        return None;
    }
    let unquoted = trimmed
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(trimmed);
    (unquoted != "$user").then(|| unquoted.to_ascii_lowercase())
}

pub(crate) fn is_local_read_plan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::Empty { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::FunctionScan { .. } => true,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::LockRows { input, .. } => is_local_read_plan(input),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
            is_local_read_plan(left) && is_local_read_plan(right)
        }
        LogicalPlan::Cte {
            definition, body, ..
        } => is_local_read_plan(definition) && is_local_read_plan(body),
        _ => false,
    }
}

pub(crate) fn local_output_from_select_result(
    result: SelectResult,
) -> Result<LocalQueryOutput, ServerError> {
    let messages = local_result_messages(result)?;
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let mut command_tag = String::new();
    for message in messages {
        match message {
            BackendMessage::RowDescription { fields } => {
                columns = fields
                    .into_iter()
                    .map(|field| LocalResultColumn {
                        name: field.name,
                        type_oid: field.type_oid,
                    })
                    .collect();
            }
            BackendMessage::DataRow { columns } => {
                let row = columns
                    .into_iter()
                    .map(|cell| {
                        cell.map(|bytes| {
                            String::from_utf8(bytes).map_err(|err| {
                                ServerError::CopyFormat(format!(
                                    "ultrasql-local result is not UTF-8: {err}"
                                ))
                            })
                        })
                        .transpose()
                    })
                    .collect::<Result<Vec<_>, ServerError>>()?;
                rows.push(row);
            }
            BackendMessage::CommandComplete { tag } => {
                command_tag = tag;
            }
            _ => {}
        }
    }
    Ok(LocalQueryOutput {
        columns,
        rows,
        command_tag,
    })
}

pub(crate) fn local_result_messages(
    result: SelectResult,
) -> Result<Vec<BackendMessage>, ServerError> {
    // Local / embedded execution decodes a complete contiguous body and
    // cannot drive a streaming handle. Every caller reaches here via a
    // dispatch context that passes `allow_streaming: false`, so the SELECT
    // arm never produced a streaming handle; assert it to catch a future
    // regression that would otherwise silently drop rows and leak the XID.
    debug_assert!(
        result.streaming.is_none(),
        "local_result_messages received a streaming SelectResult; \
         local/embedded execution cannot drive it (allow_streaming must be false)"
    );
    if let Some(body) = result.streamed_body {
        return decode_local_result_body(body);
    }
    if let Some(body) = result.shared_streamed_body {
        return decode_local_result_body(bytes::BytesMut::from(body.as_ref()));
    }
    Ok(result.messages)
}

pub(crate) fn decode_local_result_body(
    mut body: bytes::BytesMut,
) -> Result<Vec<BackendMessage>, ServerError> {
    let mut messages = Vec::new();
    while !body.is_empty() {
        match ultrasql_protocol::decode_backend(&mut body)? {
            Some(message) => messages.push(message),
            None => {
                return Err(ServerError::CopyFormat(
                    "embedded result ended with a partial wire frame".to_owned(),
                ));
            }
        }
    }
    Ok(messages)
}
