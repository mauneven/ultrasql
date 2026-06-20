use super::*;

/// Return `true` when `text` is one locally parsed XML document.
///
/// The parser rejects DTD declarations and unknown entity references. It never
/// resolves external entities, so validation cannot read local files or touch
/// the network.
#[must_use]
pub fn xml_document_is_well_formed(text: &str) -> bool {
    let text = text.trim();
    if text.is_empty() {
        return false;
    }
    let mut stack: Vec<String> = Vec::new();
    let mut cursor = 0_usize;
    let mut saw_root = false;
    let mut root_closed = false;

    while let Some(relative) = text[cursor..].find('<') {
        let Some(open) = cursor.checked_add(relative) else {
            return false;
        };
        let Some(text_segment) = text.get(cursor..open) else {
            return false;
        };
        if !xml_text_segment_is_well_formed(text_segment) {
            return false;
        }
        if stack.is_empty() && !saw_root && !text_segment.trim().is_empty() {
            return false;
        }
        if stack.is_empty() && root_closed && !text_segment.trim().is_empty() {
            return false;
        }
        let Some(next_start) = open.checked_add(1) else {
            return false;
        };
        let Some(next) = text.as_bytes().get(next_start).copied() else {
            return false;
        };
        match next {
            b'?' => {
                let Some(next_cursor) = xml_terminated_cursor(text, open, 2, "?>") else {
                    return false;
                };
                cursor = next_cursor;
            }
            b'!' if text[open..].starts_with("<!--") => {
                let Some(next_cursor) = xml_terminated_cursor(text, open, 4, "-->") else {
                    return false;
                };
                cursor = next_cursor;
            }
            b'!' if text[open..].starts_with("<![CDATA[") => {
                if stack.is_empty() {
                    return false;
                }
                let Some(next_cursor) = xml_terminated_cursor(text, open, 9, "]]>") else {
                    return false;
                };
                cursor = next_cursor;
            }
            b'!' => return false,
            b'/' => {
                let Some(name_start) = open.checked_add(2) else {
                    return false;
                };
                let Some(close) = xml_tag_end(text, name_start) else {
                    return false;
                };
                let Some(name) = text.get(name_start..close).map(str::trim) else {
                    return false;
                };
                if name.is_empty()
                    || name.bytes().any(|byte| byte.is_ascii_whitespace())
                    || xml_name_len(name.as_bytes()) != name.len()
                    || stack.pop().as_deref() != Some(name)
                {
                    return false;
                }
                if stack.is_empty() {
                    root_closed = true;
                }
                let Some(next_cursor) = close.checked_add(1) else {
                    return false;
                };
                cursor = next_cursor;
            }
            _ => {
                if root_closed {
                    return false;
                }
                let Some(content_start) = open.checked_add(1) else {
                    return false;
                };
                let Some(close) = xml_tag_end(text, content_start) else {
                    return false;
                };
                let Some(mut content) = text.get(content_start..close).map(str::trim) else {
                    return false;
                };
                let self_closing = content.ends_with('/');
                if let Some(stripped) = content.strip_suffix('/') {
                    content = stripped.trim_end();
                }
                let name_len = xml_name_len(content.as_bytes());
                if name_len == 0 {
                    return false;
                }
                let name = &content[..name_len];
                let rest = &content[name_len..];
                if !xml_attributes_are_well_formed(rest) {
                    return false;
                }
                saw_root = true;
                if self_closing {
                    if stack.is_empty() {
                        root_closed = true;
                    }
                } else {
                    stack.push(name.to_owned());
                }
                let Some(next_cursor) = close.checked_add(1) else {
                    return false;
                };
                cursor = next_cursor;
            }
        }
    }

    let trailing = &text[cursor..];
    saw_root
        && stack.is_empty()
        && xml_text_segment_is_well_formed(trailing)
        && trailing.trim().is_empty()
}

/// Return `true` when `text` is locally parsed XML content.
///
/// Content accepts more than one top-level element by validating it inside a
/// synthetic wrapper. DTD declarations and unknown entity references remain
/// rejected.
#[must_use]
pub fn xml_content_is_well_formed(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }
    let wrapped = format!("<__ultrasql_xml_content>{trimmed}</__ultrasql_xml_content>");
    xml_document_is_well_formed(&wrapped)
}

pub(crate) fn xml_terminated_cursor(
    text: &str,
    open: usize,
    body_offset: usize,
    terminator: &str,
) -> Option<usize> {
    xml_terminated_cursor_before(text, open, body_offset, terminator, text.len())
}

pub(crate) fn xml_terminated_cursor_before(
    text: &str,
    open: usize,
    body_offset: usize,
    terminator: &str,
    limit: usize,
) -> Option<usize> {
    let body_start = open.checked_add(body_offset)?;
    let relative_end = text.get(body_start..limit)?.find(terminator)?;
    let cursor = body_start
        .checked_add(relative_end)?
        .checked_add(terminator.len())?;
    (cursor <= limit).then_some(cursor)
}
