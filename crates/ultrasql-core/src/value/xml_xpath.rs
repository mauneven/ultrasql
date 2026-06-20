use super::*;

/// Return fragments selected by a small, deterministic XPath subset.
///
/// Supported paths are absolute child paths such as `/root/item/name` with
/// optional equality filters on element attributes:
/// `/root/item[@id="42"]`. Element wildcards, terminal `@attr`/`@*`,
/// `text()` selections, basic explicit axes, and bounded scalar functions
/// are also supported. Unsupported path syntax returns `None`. Missing matches
/// return `Some(Vec::new())`.
#[must_use]
pub fn xml_xpath_element_fragments(path: &str, document: &str) -> Option<Vec<String>> {
    xml_xpath_element_fragments_with_namespaces(path, document, &[])
}

/// Return fragments selected by the supported XPath subset using explicit
/// namespace alias-to-URI bindings.
///
/// Bindings use `(alias, uri)` pairs matching PostgreSQL's `xpath(...,
/// nsarray)` contract. Empty bindings preserve the legacy raw-name matching
/// behavior for unqualified paths.
#[must_use]
pub fn xml_xpath_element_fragments_with_namespaces(
    path: &str,
    document: &str,
    namespace_bindings: &[(String, String)],
) -> Option<Vec<String>> {
    let document = document.trim();
    if !xml_document_is_well_formed(document) {
        return None;
    }
    match path.trim() {
        "true()" => return Some(vec!["true".to_owned()]),
        "false()" => return Some(vec!["false".to_owned()]),
        _ => {}
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "string") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![matches.first().map_or_else(String::new, |fragment| {
            xml_xpath_string_value(fragment)
        })]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "boolean") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![(!matches.is_empty()).to_string()]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "not") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![matches.is_empty().to_string()]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "name") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![matches.first().map_or_else(String::new, |fragment| {
            xml_xpath_name_value(fragment)
        })]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "local-name") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![matches.first().map_or_else(String::new, |fragment| {
            xml_xpath_local_name_value(fragment)
        })]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "normalize-space") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![matches.first().map_or_else(String::new, |fragment| {
            xml_xpath_normalize_space_value(fragment)
        })]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "string-length") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![
            matches
                .first()
                .map_or_else(String::new, |fragment| xml_xpath_string_value(fragment))
                .chars()
                .count()
                .to_string(),
        ]);
    }
    if let Some((inner_path, needle)) =
        xml_xpath_string_literal_function_arguments(path, "contains")
    {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        let value = xml_xpath_first_string_value(&matches);
        return Some(vec![value.contains(&needle).to_string()]);
    }
    if let Some((inner_path, prefix)) =
        xml_xpath_string_literal_function_arguments(path, "starts-with")
    {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        let value = xml_xpath_first_string_value(&matches);
        return Some(vec![value.starts_with(&prefix).to_string()]);
    }
    if let Some((inner_path, delimiter)) =
        xml_xpath_string_literal_function_arguments(path, "substring-before")
    {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        let value = xml_xpath_first_string_value(&matches);
        let before = if delimiter.is_empty() {
            String::new()
        } else {
            value
                .find(&delimiter)
                .map_or_else(String::new, |idx| value[..idx].to_owned())
        };
        return Some(vec![before]);
    }
    if let Some((inner_path, delimiter)) =
        xml_xpath_string_literal_function_arguments(path, "substring-after")
    {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        let value = xml_xpath_first_string_value(&matches);
        let after = if delimiter.is_empty() {
            value
        } else {
            value
                .find(&delimiter)
                .and_then(|idx| idx.checked_add(delimiter.len()))
                .and_then(|start| value.get(start..))
                .map_or_else(String::new, ToOwned::to_owned)
        };
        return Some(vec![after]);
    }
    if let Some(arguments) = xml_xpath_substring_arguments(path) {
        let value = match arguments.source {
            XmlXPathValueArgument::Path(inner_path) => {
                let matches = xml_xpath_element_fragments_with_namespaces(
                    inner_path,
                    document,
                    namespace_bindings,
                )?;
                xml_xpath_first_string_value(&matches)
            }
            XmlXPathValueArgument::Literal(value) => value,
        };
        return Some(vec![xml_xpath_substring_value(
            &value,
            arguments.start,
            arguments.length,
        )]);
    }
    if let Some(arguments) = xml_xpath_translate_arguments(path) {
        let value = match arguments.source {
            XmlXPathValueArgument::Path(inner_path) => {
                let matches = xml_xpath_element_fragments_with_namespaces(
                    inner_path,
                    document,
                    namespace_bindings,
                )?;
                xml_xpath_first_string_value(&matches)
            }
            XmlXPathValueArgument::Literal(value) => value,
        };
        return Some(vec![xml_xpath_translate_value(
            &value,
            &arguments.from,
            &arguments.to,
        )]);
    }
    if let Some(arguments) = xml_xpath_concat_arguments(path) {
        let mut out = String::new();
        for argument in arguments {
            match argument {
                XmlXPathValueArgument::Path(inner_path) => {
                    let matches = xml_xpath_element_fragments_with_namespaces(
                        inner_path,
                        document,
                        namespace_bindings,
                    )?;
                    out.push_str(&xml_xpath_first_string_value(&matches));
                }
                XmlXPathValueArgument::Literal(value) => out.push_str(&value),
            }
        }
        return Some(vec![out]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "number") {
        let number = xml_xpath_number_function_value(inner_path, document, namespace_bindings)?;
        return Some(vec![xml_xpath_format_number(number)]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "floor") {
        let number = xml_xpath_number_function_value(inner_path, document, namespace_bindings)?;
        return Some(vec![xml_xpath_format_number(number.floor())]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "ceiling") {
        let number = xml_xpath_number_function_value(inner_path, document, namespace_bindings)?;
        return Some(vec![xml_xpath_format_number(number.ceil())]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "round") {
        let number = xml_xpath_number_function_value(inner_path, document, namespace_bindings)?;
        return Some(vec![xml_xpath_format_number(xml_xpath_round_number(
            number,
        ))]);
    }
    if let Some(inner_path) = xml_xpath_function_argument(path, "sum") {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![xml_xpath_format_number(xml_xpath_sum_value(&matches))]);
    }
    if let Some(inner_path) = xml_xpath_count_argument(path) {
        let matches =
            xml_xpath_element_fragments_with_namespaces(inner_path, document, namespace_bindings)?;
        return Some(vec![matches.len().to_string()]);
    }
    let steps = parse_xml_path(path)?;
    let root = xml_root_element(document)?;
    let mut current = match &steps[0] {
        XmlPathStep::Element {
            descendant,
            position_filter,
            ..
        } => {
            if *descendant {
                let mut matches = Vec::new();
                if xml_step_matches(document, &root, &steps[0], namespace_bindings) {
                    matches.push(root.clone());
                }
                let mut step_matches = |element: &XmlElement| {
                    xml_step_matches(document, element, &steps[0], namespace_bindings)
                };
                collect_xml_descendant_elements(document, &root, &mut matches, &mut step_matches);
                xml_apply_position_filter(matches, position_filter.as_ref())
            } else if xml_step_matches(document, &root, &steps[0], namespace_bindings) {
                xml_apply_position_filter(vec![root], position_filter.as_ref())
            } else {
                Vec::new()
            }
        }
        XmlPathStep::SelfNode => vec![root],
        XmlPathStep::Attribute(_) | XmlPathStep::Text => return None,
    };
    for (idx, step) in steps[1..].iter().enumerate() {
        let terminal = idx.checked_add(2) == Some(steps.len());
        match step {
            XmlPathStep::Element {
                descendant,
                position_filter,
                ..
            } => {
                let mut next = Vec::new();
                for element in &current {
                    if *descendant {
                        let mut matches = Vec::new();
                        let mut step_matches = |child: &XmlElement| {
                            xml_step_matches(document, child, step, namespace_bindings)
                        };
                        collect_xml_descendant_elements(
                            document,
                            element,
                            &mut matches,
                            &mut step_matches,
                        );
                        next.extend(xml_apply_position_filter(matches, position_filter.as_ref()));
                    } else {
                        let matches = xml_direct_child_elements(document, element)
                            .into_iter()
                            .filter(|child| {
                                xml_step_matches(document, child, step, namespace_bindings)
                            })
                            .collect();
                        next.extend(xml_apply_position_filter(matches, position_filter.as_ref()));
                    }
                }
                current = next;
                if current.is_empty() {
                    break;
                }
            }
            XmlPathStep::Attribute(name) if terminal => {
                return Some(
                    current
                        .iter()
                        .flat_map(|element| {
                            element
                                .attrs
                                .iter()
                                .filter(|(attr_name, _)| {
                                    xml_attribute_matches(
                                        attr_name,
                                        &element.namespaces,
                                        name,
                                        namespace_bindings,
                                    )
                                })
                                .map(|(_, value)| value.clone())
                        })
                        .collect(),
                );
            }
            XmlPathStep::Text if terminal => {
                return Some(
                    current
                        .iter()
                        .filter_map(|element| xml_direct_text(document, element))
                        .collect(),
                );
            }
            XmlPathStep::SelfNode => {}
            XmlPathStep::Attribute(_) | XmlPathStep::Text => return None,
        }
    }
    Some(
        current
            .into_iter()
            .map(|element| document[element.open_start..element.close_end].to_owned())
            .collect(),
    )
}

pub(crate) fn xml_xpath_count_argument(path: &str) -> Option<&str> {
    xml_xpath_function_argument(path, "count")
}

pub(crate) fn xml_xpath_function_argument<'a>(path: &'a str, function: &str) -> Option<&'a str> {
    let trimmed = path.trim();
    let inner = trimmed
        .strip_prefix(function)?
        .trim_start()
        .strip_prefix('(')?
        .strip_suffix(')')?
        .trim();
    inner.starts_with('/').then_some(inner)
}

pub(crate) fn xml_xpath_string_literal_function_arguments<'a>(
    path: &'a str,
    function: &str,
) -> Option<(&'a str, String)> {
    let trimmed = path.trim();
    let inner = trimmed
        .strip_prefix(function)?
        .trim_start()
        .strip_prefix('(')?
        .strip_suffix(')')?
        .trim();
    let comma = xml_xpath_top_level_comma(inner)?;
    let left = inner[..comma].trim();
    let right = inner.get(comma.checked_add(1)?..)?.trim();
    let literal = unquote_xml_path_literal(right)?;
    left.starts_with('/').then_some((left, literal))
}

#[derive(Debug)]
pub(crate) enum XmlXPathValueArgument<'a> {
    Path(&'a str),
    Literal(String),
}

#[derive(Debug)]
pub(crate) struct XmlXPathSubstringArguments<'a> {
    source: XmlXPathValueArgument<'a>,
    start: f64,
    length: Option<f64>,
}

#[derive(Debug)]
pub(crate) struct XmlXPathTranslateArguments<'a> {
    source: XmlXPathValueArgument<'a>,
    from: String,
    to: String,
}

pub(crate) fn xml_xpath_substring_arguments(path: &str) -> Option<XmlXPathSubstringArguments<'_>> {
    let trimmed = path.trim();
    let inner = trimmed
        .strip_prefix("substring")?
        .trim_start()
        .strip_prefix('(')?
        .strip_suffix(')')?
        .trim();
    let parts = xml_xpath_top_level_comma_split(inner)?;
    if !(2..=3).contains(&parts.len()) {
        return None;
    }
    Some(XmlXPathSubstringArguments {
        source: xml_xpath_value_argument(parts[0])?,
        start: parts[1].trim().parse::<f64>().ok()?,
        length: parts
            .get(2)
            .map(|part| part.trim().parse::<f64>())
            .transpose()
            .ok()?,
    })
}

pub(crate) fn xml_xpath_translate_arguments(path: &str) -> Option<XmlXPathTranslateArguments<'_>> {
    let trimmed = path.trim();
    let inner = trimmed
        .strip_prefix("translate")?
        .trim_start()
        .strip_prefix('(')?
        .strip_suffix(')')?
        .trim();
    let parts = xml_xpath_top_level_comma_split(inner)?;
    if parts.len() != 3 {
        return None;
    }
    Some(XmlXPathTranslateArguments {
        source: xml_xpath_value_argument(parts[0])?,
        from: unquote_xml_path_literal(parts[1].trim())?,
        to: unquote_xml_path_literal(parts[2].trim())?,
    })
}

pub(crate) fn xml_xpath_concat_arguments(path: &str) -> Option<Vec<XmlXPathValueArgument<'_>>> {
    let trimmed = path.trim();
    let inner = trimmed
        .strip_prefix("concat")?
        .trim_start()
        .strip_prefix('(')?
        .strip_suffix(')')?
        .trim();
    let parts = xml_xpath_top_level_comma_split(inner)?;
    (parts.len() >= 2)
        .then(|| {
            parts
                .into_iter()
                .map(xml_xpath_value_argument)
                .collect::<Option<Vec<_>>>()
        })
        .flatten()
}

pub(crate) fn xml_xpath_value_argument(argument: &str) -> Option<XmlXPathValueArgument<'_>> {
    let argument = argument.trim();
    if argument.starts_with('/') {
        Some(XmlXPathValueArgument::Path(argument))
    } else {
        unquote_xml_path_literal(argument).map(XmlXPathValueArgument::Literal)
    }
}

pub(crate) fn xml_xpath_top_level_comma_split(text: &str) -> Option<Vec<&str>> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quote = None;
    for (idx, ch) in text.char_indices() {
        match quote {
            Some(active) if ch == active => quote = None,
            Some(_) => {}
            None if matches!(ch, '\'' | '"') => quote = Some(ch),
            None if ch == ',' => {
                let part = text[start..idx].trim();
                if part.is_empty() {
                    return None;
                }
                parts.push(part);
                start = idx.checked_add(ch.len_utf8())?;
            }
            None => {}
        }
    }
    if quote.is_some() {
        return None;
    }
    let part = text[start..].trim();
    if part.is_empty() {
        return None;
    }
    parts.push(part);
    Some(parts)
}

pub(crate) fn xml_xpath_top_level_comma(text: &str) -> Option<usize> {
    let mut quote = None;
    for (idx, ch) in text.char_indices() {
        match quote {
            Some(active) if ch == active => quote = None,
            Some(_) => {}
            None if matches!(ch, '\'' | '"') => quote = Some(ch),
            None if ch == ',' => return Some(idx),
            None => {}
        }
    }
    None
}

