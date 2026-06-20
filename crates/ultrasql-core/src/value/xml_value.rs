use super::*;

pub(crate) fn xml_xpath_string_value(fragment: &str) -> String {
    let trimmed = fragment.trim();
    let Some(root) = xml_root_element(trimmed) else {
        return fragment.to_owned();
    };
    let mut out = String::new();
    xml_collect_string_value(trimmed, &root, &mut out);
    out
}

pub(crate) fn xml_xpath_first_string_value(matches: &[String]) -> String {
    matches
        .first()
        .map_or_else(String::new, |fragment| xml_xpath_string_value(fragment))
}

pub(crate) fn xml_xpath_substring_value(value: &str, start: f64, length: Option<f64>) -> String {
    if start.is_nan() || length.is_some_and(f64::is_nan) {
        return String::new();
    }
    let start = xml_xpath_round_number(start);
    let end = length.map(|length| xml_xpath_round_number(start + length));
    if end.is_some_and(f64::is_nan) {
        return String::new();
    }
    value
        .chars()
        .enumerate()
        .filter_map(|(idx, ch)| {
            let position = usize_to_f64(idx.saturating_add(1));
            (position >= start && end.is_none_or(|end| position < end)).then_some(ch)
        })
        .collect()
}

pub(crate) fn xml_xpath_translate_value(value: &str, from: &str, to: &str) -> String {
    let from_chars: Vec<char> = from.chars().collect();
    let to_chars: Vec<char> = to.chars().collect();
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match from_chars.iter().position(|candidate| *candidate == ch) {
            Some(idx) => {
                if let Some(replacement) = to_chars.get(idx) {
                    out.push(*replacement);
                }
            }
            None => out.push(ch),
        }
    }
    out
}

pub(crate) fn xml_element_string_value(text: &str, element: &XmlElement) -> String {
    let mut out = String::new();
    xml_collect_string_value(text, element, &mut out);
    out
}

pub(crate) fn xml_xpath_number_function_value(
    inner_path: &str,
    document: &str,
    namespace_bindings: &[(String, String)],
) -> Option<f64> {
    let matches =
        xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
    let value = xml_xpath_first_string_value(&matches);
    Some(value.trim().parse::<f64>().unwrap_or(f64::NAN))
}

pub(crate) fn xml_xpath_format_number(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value.is_infinite() && value.is_sign_positive() {
        "Infinity".to_owned()
    } else if value.is_infinite() {
        "-Infinity".to_owned()
    } else {
        value.to_string()
    }
}

pub(crate) fn xml_xpath_round_number(value: f64) -> f64 {
    if value.is_finite() {
        (value + 0.5).floor()
    } else {
        value
    }
}

pub(crate) fn xml_xpath_sum_value(matches: &[String]) -> f64 {
    let mut sum = 0.0_f64;
    for fragment in matches {
        let value = xml_xpath_string_value(fragment);
        let Ok(number) = value.trim().parse::<f64>() else {
            return f64::NAN;
        };
        sum += number;
    }
    sum
}

pub(crate) fn xml_collect_string_value(text: &str, element: &XmlElement, out: &mut String) {
    let mut cursor = element.content_start;
    while cursor < element.close_start {
        let Some(body) = text.get(cursor..element.close_start) else {
            break;
        };
        let Some(relative) = body.find('<') else {
            out.push_str(body);
            break;
        };
        let Some(open) = cursor.checked_add(relative) else {
            break;
        };
        let Some(prefix) = text.get(cursor..open) else {
            break;
        };
        out.push_str(prefix);
        let Some(next) = open
            .checked_add(1)
            .and_then(|idx| text.as_bytes().get(idx))
            .copied()
        else {
            break;
        };
        match next {
            b'?' => {
                let Some(next_cursor) =
                    xml_terminated_cursor_before(text, open, 2, "?>", element.close_start)
                else {
                    break;
                };
                cursor = next_cursor;
            }
            b'!' if text[open..].starts_with("<!--") => {
                let Some(next_cursor) =
                    xml_terminated_cursor_before(text, open, 4, "-->", element.close_start)
                else {
                    break;
                };
                cursor = next_cursor;
            }
            b'!' if text[open..].starts_with("<![CDATA[") => {
                let Some((cdata, next_cursor)) =
                    xml_cdata_body_before(text, open, element.close_start)
                else {
                    break;
                };
                out.push_str(cdata);
                cursor = next_cursor;
            }
            b'/' => break,
            _ => {
                let Some(child) = read_xml_element_at(text, open, &element.namespaces) else {
                    break;
                };
                xml_collect_string_value(text, &child, out);
                cursor = child.close_end;
            }
        }
    }
}

pub(crate) fn xml_cdata_body_before(
    text: &str,
    open: usize,
    limit: usize,
) -> Option<(&str, usize)> {
    let body_start = open.checked_add(9)?;
    let cursor = xml_terminated_cursor_before(text, open, 9, "]]>", limit)?;
    let body_end = cursor.checked_sub("]]>".len())?;
    Some((text.get(body_start..body_end)?, cursor))
}

pub(crate) fn xml_xpath_name_value(fragment: &str) -> String {
    xml_root_element(fragment.trim()).map_or_else(String::new, |root| root.name)
}

pub(crate) fn xml_xpath_local_name_value(fragment: &str) -> String {
    let name = xml_xpath_name_value(fragment);
    if let Some((_, local)) = name.rsplit_once(':') {
        local.to_owned()
    } else {
        name
    }
}

pub(crate) fn xml_xpath_normalize_space_value(fragment: &str) -> String {
    let value = xml_xpath_string_value(fragment);
    let mut out = String::new();
    let mut saw_space = false;
    for ch in value.chars() {
        if ch.is_whitespace() {
            saw_space = true;
        } else {
            if saw_space && !out.is_empty() {
                out.push(' ');
            }
            out.push(ch);
            saw_space = false;
        }
    }
    out
}

pub(crate) fn xml_namespace_context(
    inherited: &[(String, String)],
    attrs: &[(String, String)],
) -> Vec<(String, String)> {
    let mut namespaces = inherited.to_vec();
    for (name, value) in attrs {
        if name == "xmlns" {
            xml_upsert_namespace(&mut namespaces, "", value);
        } else if let Some(prefix) = name.strip_prefix("xmlns:")
            && !prefix.is_empty()
        {
            xml_upsert_namespace(&mut namespaces, prefix, value);
        }
    }
    namespaces
}

pub(crate) fn xml_upsert_namespace(
    namespaces: &mut Vec<(String, String)>,
    prefix: &str,
    uri: &str,
) {
    if let Some((_, existing_uri)) = namespaces
        .iter_mut()
        .find(|(existing_prefix, _)| existing_prefix == prefix)
    {
        *existing_uri = uri.to_owned();
    } else {
        namespaces.push((prefix.to_owned(), uri.to_owned()));
    }
}

pub(crate) fn xml_name_matches(
    actual: &str,
    actual_namespaces: &[(String, String)],
    expected: &str,
    namespace_bindings: &[(String, String)],
    default_namespace_applies: bool,
) -> bool {
    let (expected_prefix, expected_local) = xml_split_qname(expected);
    if namespace_bindings.is_empty() || expected_prefix.is_empty() {
        return actual == expected;
    }
    let Some(expected_uri) = xml_namespace_uri(namespace_bindings, expected_prefix) else {
        return false;
    };
    let (actual_prefix, actual_local) = xml_split_qname(actual);
    if actual_local != expected_local {
        return false;
    }
    xml_namespace_uri_for_name(actual_namespaces, actual_prefix, default_namespace_applies)
        .is_some_and(|actual_uri| actual_uri == expected_uri)
}

pub(crate) fn xml_path_name_is_valid(name: &str) -> bool {
    !name.is_empty() && (name == "*" || xml_name_len(name.as_bytes()) == name.len())
}

pub(crate) fn xml_element_name_matches(
    actual: &str,
    actual_namespaces: &[(String, String)],
    expected: &str,
    namespace_bindings: &[(String, String)],
) -> bool {
    expected == "*"
        || xml_name_matches(
            actual,
            actual_namespaces,
            expected,
            namespace_bindings,
            true,
        )
}

pub(crate) fn xml_attribute_matches(
    actual: &str,
    actual_namespaces: &[(String, String)],
    expected: &str,
    namespace_bindings: &[(String, String)],
) -> bool {
    if expected == "*" {
        return !xml_is_namespace_attribute(actual);
    }
    xml_name_matches(
        actual,
        actual_namespaces,
        expected,
        namespace_bindings,
        false,
    )
}

pub(crate) fn xml_is_namespace_attribute(name: &str) -> bool {
    name == "xmlns"
        || name
            .strip_prefix("xmlns:")
            .is_some_and(|prefix| !prefix.is_empty())
}

pub(crate) fn xml_split_qname(name: &str) -> (&str, &str) {
    name.split_once(':')
        .map_or(("", name), |(prefix, local)| (prefix, local))
}

pub(crate) fn xml_namespace_uri<'a>(
    namespaces: &'a [(String, String)],
    prefix: &str,
) -> Option<&'a str> {
    namespaces
        .iter()
        .rev()
        .find(|(candidate, _)| candidate == prefix)
        .map(|(_, uri)| uri.as_str())
}

pub(crate) fn xml_namespace_uri_for_name<'a>(
    namespaces: &'a [(String, String)],
    prefix: &str,
    default_namespace_applies: bool,
) -> Option<&'a str> {
    if prefix.is_empty() && !default_namespace_applies {
        return None;
    }
    xml_namespace_uri(namespaces, prefix)
}

pub(crate) fn xml_step_matches(
    document: &str,
    element: &XmlElement,
    step: &XmlPathStep,
    namespace_bindings: &[(String, String)],
) -> bool {
    let XmlPathStep::Element {
        name,
        attr_filter,
        text_filter,
        child_text_filter,
        ..
    } = step
    else {
        return false;
    };
    xml_element_name_matches(&element.name, &element.namespaces, name, namespace_bindings)
        && attr_filter
            .as_ref()
            .is_none_or(|(expected_name, expected_value)| {
                element.attrs.iter().any(|(name, value)| {
                    value == expected_value
                        && xml_attribute_matches(
                            name,
                            &element.namespaces,
                            expected_name,
                            namespace_bindings,
                        )
                })
            })
        && text_filter.as_ref().is_none_or(|expected| {
            xml_direct_text(document, element).is_some_and(|text| text == *expected)
        })
        && child_text_filter
            .as_ref()
            .is_none_or(|(expected_name, expected_value)| {
                xml_direct_child_elements(document, element)
                    .into_iter()
                    .any(|child| {
                        xml_element_name_matches(
                            &child.name,
                            &child.namespaces,
                            expected_name,
                            namespace_bindings,
                        ) && xml_element_string_value(document, &child) == *expected_value
                    })
            })
}

pub(crate) fn xml_tag_end(text: &str, start: usize) -> Option<usize> {
    let mut quote = None;
    for (offset, byte) in text.as_bytes().get(start..)?.iter().copied().enumerate() {
        match (quote, byte) {
            (Some(q), b) if b == q => quote = None,
            (None, b'\'' | b'"') => quote = Some(byte),
            (None, b'>') => return start.checked_add(offset),
            _ => {}
        }
    }
    None
}

pub(crate) fn xml_name_len(bytes: &[u8]) -> usize {
    let Some((&first, rest)) = bytes.split_first() else {
        return 0;
    };
    if !xml_name_start_byte(first) {
        return 0;
    }
    let mut len = 1_usize;
    for byte in rest {
        if !xml_name_byte(*byte) {
            break;
        }
        len = len.saturating_add(1);
    }
    len
}

pub(crate) fn xml_name_start_byte(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || matches!(byte, b'_' | b':')
}

pub(crate) fn xml_name_byte(byte: u8) -> bool {
    xml_name_start_byte(byte) || byte.is_ascii_digit() || matches!(byte, b'-' | b'.')
}

pub(crate) fn xml_attributes_are_well_formed(rest: &str) -> bool {
    xml_parse_attributes(rest).is_some()
}

pub(crate) fn xml_parse_attributes(rest: &str) -> Option<Vec<(String, String)>> {
    let bytes = rest.as_bytes();
    let mut cursor = 0_usize;
    let mut attrs = Vec::new();
    while cursor < bytes.len() {
        while bytes
            .get(cursor)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            cursor = cursor.checked_add(1)?;
        }
        if cursor == bytes.len() {
            return Some(attrs);
        }
        let name_len = xml_name_len(&bytes[cursor..]);
        if name_len == 0 {
            return None;
        }
        let name_end = cursor.checked_add(name_len)?;
        let name = rest.get(cursor..name_end)?.to_owned();
        cursor = name_end;
        while bytes
            .get(cursor)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            cursor = cursor.checked_add(1)?;
        }
        if bytes.get(cursor) != Some(&b'=') {
            return None;
        }
        cursor = cursor.checked_add(1)?;
        while bytes
            .get(cursor)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            cursor = cursor.checked_add(1)?;
        }
        let Some(quote @ (b'\'' | b'"')) = bytes.get(cursor).copied() else {
            return None;
        };
        cursor = cursor.checked_add(1)?;
        let value_start = cursor;
        while bytes.get(cursor).is_some_and(|byte| *byte != quote) {
            if bytes[cursor] == b'<' {
                return None;
            }
            cursor = cursor.checked_add(1)?;
        }
        let value = rest.get(value_start..cursor)?;
        if !xml_text_segment_is_well_formed(value) {
            return None;
        }
        if bytes.get(cursor) != Some(&quote) {
            return None;
        }
        attrs.push((name, value.to_owned()));
        cursor = cursor.checked_add(1)?;
    }
    Some(attrs)
}

pub(crate) fn xml_text_segment_is_well_formed(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut cursor = 0_usize;
    while let Some(relative) = bytes
        .get(cursor..)
        .and_then(|tail| tail.iter().position(|byte| *byte == b'&'))
    {
        let Some(amp) = cursor.checked_add(relative) else {
            return false;
        };
        let Some(entity_len) = xml_entity_ref_len(&bytes[amp..]) else {
            return false;
        };
        let Some(next_cursor) = amp.checked_add(entity_len) else {
            return false;
        };
        cursor = next_cursor;
    }
    true
}

pub(crate) fn xml_entity_ref_len(bytes: &[u8]) -> Option<usize> {
    if bytes.first() != Some(&b'&') {
        return None;
    }
    let semi = bytes.iter().take(64).position(|byte| *byte == b';')?;
    if semi <= 1 {
        return None;
    }
    let body = std::str::from_utf8(bytes.get(1..semi)?).ok()?;
    if matches!(body, "amp" | "lt" | "gt" | "apos" | "quot") {
        return semi.checked_add(1);
    }
    if let Some(hex) = body.strip_prefix("#x").or_else(|| body.strip_prefix("#X")) {
        if !hex.is_empty() && hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return semi.checked_add(1);
        }
    } else if let Some(dec) = body.strip_prefix('#')
        && !dec.is_empty()
        && dec.bytes().all(|byte| byte.is_ascii_digit())
    {
        return semi.checked_add(1);
    }
    None
}
