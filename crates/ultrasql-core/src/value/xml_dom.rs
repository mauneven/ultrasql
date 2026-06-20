use super::*;

#[derive(Clone, Debug)]
pub(crate) enum XmlPathStep {
    Element {
        name: String,
        attr_filter: Option<(String, String)>,
        text_filter: Option<String>,
        child_text_filter: Option<(String, String)>,
        position_filter: Option<XmlPositionPredicate>,
        descendant: bool,
    },
    Attribute(String),
    Text,
    SelfNode,
}

#[derive(Clone, Debug)]
pub(crate) enum XmlPositionPredicate {
    Index(usize),
    Last,
}

#[derive(Clone, Debug)]
pub(crate) struct XmlElement {
    pub(crate) name: String,
    pub(crate) attrs: Vec<(String, String)>,
    pub(crate) namespaces: Vec<(String, String)>,
    pub(crate) open_start: usize,
    pub(crate) content_start: usize,
    pub(crate) close_start: usize,
    pub(crate) close_end: usize,
}

pub(crate) fn parse_xml_path(path: &str) -> Option<Vec<XmlPathStep>> {
    let path = path.trim();
    if !path.starts_with('/') {
        return None;
    }
    let mut steps = Vec::new();
    let mut cursor = 0_usize;
    while cursor < path.len() {
        let descendant = if path[cursor..].starts_with("//") {
            cursor = cursor.checked_add(2)?;
            true
        } else if path[cursor..].starts_with('/') {
            cursor = cursor.checked_add(1)?;
            false
        } else {
            return None;
        };
        if cursor >= path.len() {
            return None;
        }
        let relative_end = path[cursor..].find('/');
        let segment_end =
            relative_end.map_or(Some(path.len()), |offset| cursor.checked_add(offset))?;
        let terminal = segment_end == path.len();
        let segment = path[cursor..segment_end].trim();
        if segment.is_empty() || segment == ".." {
            return None;
        }
        if segment == "." || segment == "self::node()" {
            if descendant {
                return None;
            }
            steps.push(XmlPathStep::SelfNode);
            cursor = segment_end;
            continue;
        }
        if segment == "text()" {
            if descendant || !terminal {
                return None;
            }
            steps.push(XmlPathStep::Text);
            cursor = segment_end;
            continue;
        }
        if let Some(attr_name) = segment.strip_prefix("attribute::") {
            if descendant || !terminal || attr_name.is_empty() || !xml_path_name_is_valid(attr_name)
            {
                return None;
            }
            steps.push(XmlPathStep::Attribute(attr_name.to_owned()));
            cursor = segment_end;
            continue;
        }
        if let Some(attr_name) = segment.strip_prefix('@') {
            if descendant || !terminal || attr_name.is_empty() || !xml_path_name_is_valid(attr_name)
            {
                return None;
            }
            steps.push(XmlPathStep::Attribute(attr_name.to_owned()));
            cursor = segment_end;
            continue;
        }
        let (segment, descendant) = if let Some(name) = segment.strip_prefix("child::") {
            (name, descendant)
        } else if let Some(name) = segment.strip_prefix("descendant::") {
            if descendant {
                return None;
            }
            (name, true)
        } else if segment.contains("::") {
            return None;
        } else {
            (segment, descendant)
        };
        let (name, attr_filter, text_filter, child_text_filter, position_filter) =
            if let Some(open) = segment.find('[') {
                let predicate = segment
                    .get(open.checked_add(1)?..segment.len().checked_sub(1)?)?
                    .trim();
                if !segment.ends_with(']') {
                    return None;
                }
                if let Some(attr_predicate) = predicate.strip_prefix('@') {
                    let (attr_name, attr_value) = attr_predicate.split_once('=')?;
                    let attr_name = attr_name.trim();
                    let attr_value = unquote_xml_path_literal(attr_value.trim())?;
                    (
                        &segment[..open],
                        Some((attr_name.to_owned(), attr_value)),
                        None,
                        None,
                        None,
                    )
                } else if let Some(text_value) = parse_xml_text_equality_predicate(predicate) {
                    (&segment[..open], None, Some(text_value), None, None)
                } else if let Some((child_name, child_value)) =
                    parse_xml_child_text_equality_predicate(predicate)
                {
                    (
                        &segment[..open],
                        None,
                        None,
                        Some((child_name, child_value)),
                        None,
                    )
                } else {
                    (
                        &segment[..open],
                        None,
                        None,
                        None,
                        Some(parse_xml_position_predicate(predicate)?),
                    )
                }
            } else {
                (segment, None, None, None, None)
            };
        if !xml_path_name_is_valid(name) {
            return None;
        }
        if let Some((attr_name, _)) = &attr_filter
            && !xml_path_name_is_valid(attr_name)
        {
            return None;
        }
        steps.push(XmlPathStep::Element {
            name: name.to_owned(),
            attr_filter,
            text_filter,
            child_text_filter,
            position_filter,
            descendant,
        });
        cursor = segment_end;
    }
    if steps.is_empty() { None } else { Some(steps) }
}

pub(crate) fn parse_xml_position_predicate(predicate: &str) -> Option<XmlPositionPredicate> {
    let predicate = predicate.trim();
    if predicate == "last()" {
        return Some(XmlPositionPredicate::Last);
    }
    if let Ok(index) = predicate.parse::<usize>() {
        return (index > 0).then_some(XmlPositionPredicate::Index(index));
    }
    let (left, right) = predicate.split_once('=')?;
    if left.trim() != "position()" {
        return None;
    }
    let right = right.trim();
    if right == "last()" {
        Some(XmlPositionPredicate::Last)
    } else {
        let index = right.parse::<usize>().ok()?;
        (index > 0).then_some(XmlPositionPredicate::Index(index))
    }
}

pub(crate) fn parse_xml_text_equality_predicate(predicate: &str) -> Option<String> {
    let (left, right) = predicate.split_once('=')?;
    (left.trim() == "text()").then(|| unquote_xml_path_literal(right.trim()))?
}

pub(crate) fn parse_xml_child_text_equality_predicate(predicate: &str) -> Option<(String, String)> {
    let (left, right) = predicate.split_once('=')?;
    let name = left.trim();
    if !xml_path_name_is_valid(name) {
        return None;
    }
    let value = unquote_xml_path_literal(right.trim())?;
    Some((name.to_owned(), value))
}

pub(crate) fn unquote_xml_path_literal(text: &str) -> Option<String> {
    let quote = text.as_bytes().first().copied()?;
    if !matches!(quote, b'\'' | b'"') || text.as_bytes().last().copied() != Some(quote) {
        return None;
    }
    Some(text.get(1..text.len().checked_sub(1)?)?.to_owned())
}

pub(crate) fn xml_root_element(text: &str) -> Option<XmlElement> {
    let mut cursor = 0_usize;
    while let Some(relative) = text[cursor..].find('<') {
        let open = cursor.checked_add(relative)?;
        let next = text.as_bytes().get(open.checked_add(1)?).copied()?;
        match next {
            b'?' => {
                cursor = xml_terminated_cursor(text, open, 2, "?>")?;
            }
            b'!' if text[open..].starts_with("<!--") => {
                cursor = xml_terminated_cursor(text, open, 4, "-->")?;
            }
            b'!' => return None,
            b'/' => return None,
            _ => return read_xml_element_at(text, open, &[]),
        }
    }
    None
}

pub(crate) fn read_xml_element_at(
    text: &str,
    open: usize,
    inherited_namespaces: &[(String, String)],
) -> Option<XmlElement> {
    if text.as_bytes().get(open) != Some(&b'<') {
        return None;
    }
    let tag_start = open.checked_add(1)?;
    let next = text.as_bytes().get(tag_start).copied()?;
    if matches!(next, b'/' | b'!' | b'?') {
        return None;
    }
    let tag_close = xml_tag_end(text, tag_start)?;
    let mut content = text.get(tag_start..tag_close)?.trim();
    let self_closing = content.ends_with('/');
    if let Some(stripped) = content.strip_suffix('/') {
        content = stripped.trim_end();
    }
    let name_len = xml_name_len(content.as_bytes());
    if name_len == 0 {
        return None;
    }
    let name = content[..name_len].to_owned();
    let attrs = xml_parse_attributes(&content[name_len..])?;
    let namespaces = xml_namespace_context(inherited_namespaces, &attrs);
    let content_start = tag_close.checked_add(1)?;
    if self_closing {
        return Some(XmlElement {
            name,
            attrs,
            namespaces,
            open_start: open,
            content_start,
            close_start: content_start,
            close_end: content_start,
        });
    }

    let mut cursor = content_start;
    let mut same_name_depth = 1_usize;
    while let Some(relative) = text[cursor..].find('<') {
        let tag_open = cursor.checked_add(relative)?;
        let next = text.as_bytes().get(tag_open.checked_add(1)?).copied()?;
        match next {
            b'?' => {
                cursor = xml_terminated_cursor(text, tag_open, 2, "?>")?;
            }
            b'!' if text[tag_open..].starts_with("<!--") => {
                cursor = xml_terminated_cursor(text, tag_open, 4, "-->")?;
            }
            b'!' if text[tag_open..].starts_with("<![CDATA[") => {
                cursor = xml_terminated_cursor(text, tag_open, 9, "]]>")?;
            }
            b'/' => {
                let close_name_start = tag_open.checked_add(2)?;
                let close = xml_tag_end(text, close_name_start)?;
                let closing_name = text.get(close_name_start..close)?.trim();
                if closing_name == name {
                    same_name_depth = same_name_depth.checked_sub(1)?;
                    if same_name_depth == 0 {
                        let close_end = close.checked_add(1)?;
                        return Some(XmlElement {
                            name,
                            attrs,
                            namespaces,
                            open_start: open,
                            content_start,
                            close_start: tag_open,
                            close_end,
                        });
                    }
                }
                cursor = close.checked_add(1)?;
            }
            _ => {
                let child_start = tag_open.checked_add(1)?;
                let child_close = xml_tag_end(text, child_start)?;
                let mut child_content = text.get(child_start..child_close)?.trim();
                let child_self_closing = child_content.ends_with('/');
                if let Some(stripped) = child_content.strip_suffix('/') {
                    child_content = stripped.trim_end();
                }
                let child_name_len = xml_name_len(child_content.as_bytes());
                if child_name_len == 0 {
                    return None;
                }
                if child_content[..child_name_len] == name && !child_self_closing {
                    same_name_depth = same_name_depth.checked_add(1)?;
                }
                cursor = child_close.checked_add(1)?;
            }
        }
    }
    None
}

pub(crate) fn xml_direct_child_elements(text: &str, parent: &XmlElement) -> Vec<XmlElement> {
    let mut out = Vec::new();
    let mut cursor = parent.content_start;
    while cursor < parent.close_start {
        let Some(relative) = text
            .get(cursor..parent.close_start)
            .and_then(|body| body.find('<'))
        else {
            break;
        };
        let Some(open) = cursor.checked_add(relative) else {
            break;
        };
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
                    xml_terminated_cursor_before(text, open, 2, "?>", parent.close_start)
                else {
                    break;
                };
                cursor = next_cursor;
            }
            b'!' if text[open..].starts_with("<!--") => {
                let Some(next_cursor) =
                    xml_terminated_cursor_before(text, open, 4, "-->", parent.close_start)
                else {
                    break;
                };
                cursor = next_cursor;
            }
            b'!' if text[open..].starts_with("<![CDATA[") => {
                let Some(next_cursor) =
                    xml_terminated_cursor_before(text, open, 9, "]]>", parent.close_start)
                else {
                    break;
                };
                cursor = next_cursor;
            }
            b'/' => break,
            _ => {
                let Some(element) = read_xml_element_at(text, open, &parent.namespaces) else {
                    break;
                };
                cursor = element.close_end;
                out.push(element);
            }
        }
    }
    out
}

pub(crate) fn collect_xml_descendant_elements<F>(
    text: &str,
    parent: &XmlElement,
    out: &mut Vec<XmlElement>,
    matches: &mut F,
) where
    F: FnMut(&XmlElement) -> bool,
{
    for child in xml_direct_child_elements(text, parent) {
        if matches(&child) {
            out.push(child.clone());
        }
        collect_xml_descendant_elements(text, &child, out, matches);
    }
}

pub(crate) fn xml_apply_position_filter(
    elements: Vec<XmlElement>,
    filter: Option<&XmlPositionPredicate>,
) -> Vec<XmlElement> {
    match filter {
        None => elements,
        Some(XmlPositionPredicate::Index(index)) => elements
            .into_iter()
            .nth(index.saturating_sub(1))
            .into_iter()
            .collect(),
        Some(XmlPositionPredicate::Last) => elements.into_iter().last().into_iter().collect(),
    }
}

pub(crate) fn xml_direct_text(text: &str, element: &XmlElement) -> Option<String> {
    let mut out = String::new();
    let mut cursor = element.content_start;
    while cursor < element.close_start {
        let body = text.get(cursor..element.close_start)?;
        let Some(relative) = body.find('<') else {
            out.push_str(body);
            break;
        };
        let open = cursor.checked_add(relative)?;
        out.push_str(text.get(cursor..open)?);
        let next = text.as_bytes().get(open.checked_add(1)?).copied()?;
        match next {
            b'?' => {
                cursor = xml_terminated_cursor_before(text, open, 2, "?>", element.close_start)?;
            }
            b'!' if text[open..].starts_with("<!--") => {
                cursor = xml_terminated_cursor_before(text, open, 4, "-->", element.close_start)?;
            }
            b'!' if text[open..].starts_with("<![CDATA[") => {
                let (cdata, next_cursor) = xml_cdata_body_before(text, open, element.close_start)?;
                out.push_str(cdata);
                cursor = next_cursor;
            }
            b'/' => break,
            _ => {
                let child = read_xml_element_at(text, open, &element.namespaces)?;
                cursor = child.close_end;
            }
        }
    }
    (!out.is_empty()).then_some(out)
}
