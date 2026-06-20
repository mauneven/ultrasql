use super::*;

pub(crate) fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => byte.checked_sub(b'0'),
        b'a'..=b'f' => byte
            .checked_sub(b'a')
            .and_then(|digit| digit.checked_add(HEX_ALPHA_BASE)),
        b'A'..=b'F' => byte
            .checked_sub(b'A')
            .and_then(|digit| digit.checked_add(HEX_ALPHA_BASE)),
        _ => None,
    }
}

pub(crate) fn pack_hex_byte(high: u8, low: u8) -> Option<u8> {
    high.checked_shl(4).map(|upper| upper | low)
}

pub(crate) fn packed_bit_position(idx: usize) -> Option<(usize, u32)> {
    let byte_idx = idx.checked_div(BITS_PER_BYTE)?;
    let bit_idx = idx.checked_rem(BITS_PER_BYTE)?;
    let shift = HIGH_BIT_INDEX.checked_sub(bit_idx)?;
    Some((byte_idx, u32::try_from(shift).ok()?))
}

pub(crate) fn write_decimal_text(f: &mut fmt::Formatter<'_>, value: i64, scale: i32) -> fmt::Result {
    let sign = if value < 0 { "-" } else { "" };
    let mag = value.unsigned_abs().to_string();
    if scale <= 0 {
        f.write_str(sign)?;
        f.write_str(&mag)?;
        let zeros = usize::try_from(scale.unsigned_abs()).map_err(|_| fmt::Error)?;
        return write_zeros(f, zeros);
    }

    let scale =
        usize::try_from(u32::try_from(scale).map_err(|_| fmt::Error)?).map_err(|_| fmt::Error)?;
    f.write_str(sign)?;
    if mag.len() > scale {
        let split = mag.len().checked_sub(scale).ok_or(fmt::Error)?;
        f.write_str(mag.get(..split).ok_or(fmt::Error)?)?;
        f.write_str(".")?;
        f.write_str(mag.get(split..).ok_or(fmt::Error)?)
    } else {
        f.write_str("0.")?;
        write_zeros(f, scale.checked_sub(mag.len()).ok_or(fmt::Error)?)?;
        f.write_str(&mag)
    }
}

pub(crate) fn write_zeros(f: &mut fmt::Formatter<'_>, count: usize) -> fmt::Result {
    for _ in 0..count {
        f.write_str("0")?;
    }
    Ok(())
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Bool(b) => f.write_str(if *b { "true" } else { "false" }),
            Self::Int16(v) => write!(f, "{v}"),
            Self::Int32(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}"),
            Self::Oid(v) | Self::RegClass(v) | Self::RegType(v) => write!(f, "{}", v.raw()),
            Self::PgLsn(v) => write!(f, "{v}"),
            Self::Float32(v) => write!(f, "{v}"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::Text(s) | Self::Char(s) | Self::Json(s) | Self::Jsonb(s) | Self::Xml(s) => {
                write!(f, "{s}")
            }
            Self::Bytea(b) => {
                f.write_str("\\x")?;
                for byte in b {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
            Self::Timestamp(us) => f.write_str(&format_timestamp_micros(*us)),
            Self::TimestampTz(us) => f.write_str(&format_timestamptz_micros_utc(*us)),
            Self::Date(d) => write!(f, "{}", format_date(*d)),
            Self::Time(t) => f.write_str(&format_time_micros(*t)),
            Self::TimeTz {
                micros,
                offset_seconds,
            } => f.write_str(&format_timetz(*micros, *offset_seconds)),
            Self::Decimal { value, scale } => {
                // PostgreSQL-style fixed-point text. `value` is the
                // scaled integer; insert the decimal point `scale`
                // digits from the right. Negative scale (allowed by
                // the type) appends trailing zeros instead.
                write_decimal_text(f, *value, *scale)
            }
            Self::Money(v) => f.write_str(&format_money_text(*v)),
            Self::Interval {
                months,
                days,
                microseconds,
            } => write!(f, "{months}mon {days}d {microseconds}us"),
            Self::Range(v) => write!(f, "{v}"),
            Self::Geometry(v) => write!(f, "{v}"),
            Self::Vector(values) | Self::HalfVec(values) => {
                f.write_str("[")?;
                for (idx, value) in values.iter().enumerate() {
                    if idx > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{value}")?;
                }
                f.write_str("]")
            }
            Self::SparseVec(v) => write!(f, "{v}"),
            Self::BitString(v) => write!(f, "{v}"),
            Self::Network(v) => write!(f, "{v}"),
            Self::BitVec { dims, bytes } => {
                let dims_usize = usize::try_from(*dims).map_err(|_| fmt::Error)?;
                let required_bytes = dims_usize.div_ceil(BITS_PER_BYTE);
                if bytes.len() < required_bytes {
                    return Err(fmt::Error);
                }
                for idx in 0..dims_usize {
                    let (byte_idx, shift) = packed_bit_position(idx).ok_or(fmt::Error)?;
                    let byte = bytes.get(byte_idx).ok_or(fmt::Error)?;
                    let bit = (byte >> shift) & 1;
                    f.write_str(if bit == 1 { "1" } else { "0" })?;
                }
                Ok(())
            }
            Self::Array { elements, .. } => {
                f.write_str("{")?;
                for (idx, element) in elements.iter().enumerate() {
                    if idx > 0 {
                        f.write_str(",")?;
                    }
                    write_array_element(f, element)?;
                }
                f.write_str("}")
            }
            Self::Record(fields) => {
                f.write_str("(")?;
                for (idx, (_, value)) in fields.iter().enumerate() {
                    if idx > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{value}")?;
                }
                f.write_str(")")
            }
            Self::Uuid(u) => {
                write!(
                    f,
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    u[0],
                    u[1],
                    u[2],
                    u[3],
                    u[4],
                    u[5],
                    u[6],
                    u[7],
                    u[8],
                    u[9],
                    u[10],
                    u[11],
                    u[12],
                    u[13],
                    u[14],
                    u[15]
                )
            }
        }
    }
}

pub(crate) fn parse_array_element(element_type: &DataType, raw: &str) -> Option<Value> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("NULL") {
        return Some(Value::Null);
    }
    if let DataType::Array(inner) = element_type {
        return Value::parse_array((**inner).clone(), trimmed);
    }
    let text = unescape_array_text(trimmed)?;
    match element_type {
        DataType::Bool => match text.to_ascii_lowercase().as_str() {
            "t" | "true" => Some(Value::Bool(true)),
            "f" | "false" => Some(Value::Bool(false)),
            _ => None,
        },
        DataType::Int16 => text.parse::<i16>().ok().map(Value::Int16),
        DataType::Int32 => text.parse::<i32>().ok().map(Value::Int32),
        DataType::Int64 => text.parse::<i64>().ok().map(Value::Int64),
        DataType::Oid => Value::parse_oid_text(&text).map(Value::Oid),
        DataType::RegClass => Value::parse_oid_text(&text).map(Value::RegClass),
        DataType::RegType => Value::parse_oid_text(&text).map(Value::RegType),
        DataType::PgLsn => Value::parse_pg_lsn_text(&text).map(Value::PgLsn),
        DataType::Float32 => text.parse::<f32>().ok().map(Value::Float32),
        DataType::Float64 => text.parse::<f64>().ok().map(Value::Float64),
        DataType::Text { .. } | DataType::TsVector | DataType::TsQuery => Some(Value::Text(text)),
        DataType::Char { len } => coerce_bpchar_text(&text, *len, false).ok().map(Value::Char),
        DataType::Json => Some(Value::Json(text)),
        DataType::Jsonb => Some(Value::Jsonb(text)),
        DataType::Xml => Value::validate_xml_text(&text).map(Value::Xml),
        DataType::Bytea => Value::parse_bytea(&text).map(Value::Bytea),
        DataType::Uuid => Value::parse_uuid(&text).map(Value::Uuid),
        DataType::Money => parse_money_text(&text).ok(),
        _ => None,
    }
}

pub(crate) fn split_array_elements(text: &str) -> Option<Vec<&str>> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut in_string = false;
    let mut escape = false;
    let mut depth = 0_usize;
    for (idx, ch) in text.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_string => escape = true,
            '"' => in_string = !in_string,
            '{' if !in_string => {
                depth = depth.checked_add(1)?;
            }
            '}' if !in_string => {
                depth = depth.checked_sub(1)?;
            }
            ',' if !in_string && depth == 0 => {
                out.push(&text[start..idx]);
                start = idx.checked_add(ch.len_utf8())?;
            }
            _ => {}
        }
    }
    if in_string || escape || depth != 0 {
        return None;
    }
    out.push(&text[start..]);
    Some(out)
}

pub(crate) fn array_dimensions(element_type: &DataType, elements: &[Value]) -> Option<Vec<usize>> {
    let mut dims = vec![elements.len()];
    if matches!(element_type, DataType::Array(_)) {
        let mut nested_dims: Option<Vec<usize>> = None;
        for element in elements {
            if element.is_null() {
                continue;
            }
            if !matches!(element, Value::Array { .. }) {
                return None;
            }
            let dims = element.array_dimensions()?;
            if let Some(expected) = &nested_dims {
                if expected != &dims {
                    return None;
                }
            } else {
                nested_dims = Some(dims);
            }
        }
        if let Some(mut nested) = nested_dims {
            dims.append(&mut nested);
        }
    }
    Some(dims)
}

pub(crate) fn unescape_array_text(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if !(trimmed.starts_with('"') || trimmed.ends_with('"')) {
        return Some(trimmed.to_owned());
    }
    if !(trimmed.starts_with('"') && trimmed.ends_with('"')) || trimmed.len() < 2 {
        return None;
    }
    let inner = &trimmed[1..trimmed.len().checked_sub(1)?];
    let mut out = String::with_capacity(inner.len());
    let mut escape = false;
    for ch in inner.chars() {
        if escape {
            out.push(ch);
            escape = false;
        } else if ch == '\\' {
            escape = true;
        } else {
            out.push(ch);
        }
    }
    if escape {
        return None;
    }
    Some(out)
}

pub(crate) fn write_array_element(f: &mut fmt::Formatter<'_>, value: &Value) -> fmt::Result {
    match value {
        Value::Null => f.write_str("NULL"),
        Value::Array { .. } => write!(f, "{value}"),
        Value::Text(s) | Value::Char(s) => write_array_text(f, s),
        other => write_array_text(f, &other.to_string()),
    }
}

pub(crate) fn write_array_text(f: &mut fmt::Formatter<'_>, text: &str) -> fmt::Result {
    let needs_quotes = text.is_empty()
        || text.eq_ignore_ascii_case("NULL")
        || text
            .chars()
            .any(|ch| matches!(ch, ',' | '{' | '}' | '"' | '\\') || ch.is_whitespace());
    if !needs_quotes {
        return f.write_str(text);
    }
    f.write_str("\"")?;
    for ch in text.chars() {
        if matches!(ch, '"' | '\\') {
            f.write_str("\\")?;
        }
        write!(f, "{ch}")?;
    }
    f.write_str("\"")
}

pub(crate) fn split_once_unquoted_comma(s: &str) -> Option<(&str, &str)> {
    split_once_byte(s, ',')
}

pub(crate) fn split_once_unquoted_slash(s: &str) -> Option<(&str, &str)> {
    split_once_byte(s, '/')
}

pub(crate) fn split_once_unquoted_colon(s: &str) -> Option<(&str, &str)> {
    split_once_byte(s, ':')
}

pub(crate) fn split_once_byte(s: &str, needle: char) -> Option<(&str, &str)> {
    let idx = s.find(needle)?;
    let right_start = idx.checked_add(needle.len_utf8())?;
    Some((s.get(..idx)?, s.get(right_start..)?))
}

pub(crate) fn parse_range_bound(range_type: RangeType, text: &str) -> Option<Option<f64>> {
    if text.is_empty() {
        return Some(None);
    }
    let text = text.trim_matches('"').trim_matches('\'');
    match range_type {
        RangeType::Int4 | RangeType::Int8 => text.parse::<i64>().ok().map(|v| Some(i64_to_f64(v))),
        RangeType::Num | RangeType::Timestamp | RangeType::TimestampTz => {
            text.parse::<f64>().ok().map(Some)
        }
        RangeType::Date => parse_date_days(text).map(|v| Some(f64::from(v))),
    }
}

pub(crate) fn range_is_empty(
    lower: Option<f64>,
    upper: Option<f64>,
    lower_inc: bool,
    upper_inc: bool,
) -> bool {
    match (lower, upper) {
        (Some(l), Some(u)) if l > u => true,
        (Some(l), Some(u)) if l == u => !(lower_inc && upper_inc),
        _ => false,
    }
}

pub(crate) fn upper_before_lower(
    upper: Option<f64>,
    upper_inc: bool,
    lower: Option<f64>,
    lower_inc: bool,
) -> bool {
    match (upper, lower) {
        (Some(u), Some(l)) if u < l => true,
        (Some(u), Some(l)) if u > l => false,
        (Some(_), Some(_)) => !(upper_inc && lower_inc),
        (None, _) | (_, None) => false,
    }
}

pub(crate) fn lower_covers_lower(
    container: Option<f64>,
    container_inc: bool,
    inner: Option<f64>,
    inner_inc: bool,
) -> bool {
    match (container, inner) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(c), Some(i)) if c < i => true,
        (Some(c), Some(i)) if c > i => false,
        (Some(_), Some(_)) => container_inc || !inner_inc,
    }
}

pub(crate) fn upper_covers_upper(
    container: Option<f64>,
    container_inc: bool,
    inner: Option<f64>,
    inner_inc: bool,
) -> bool {
    match (container, inner) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(c), Some(i)) if c > i => true,
        (Some(c), Some(i)) if c < i => false,
        (Some(_), Some(_)) => container_inc || !inner_inc,
    }
}

pub(crate) fn write_range_number(f: &mut fmt::Formatter<'_>, v: f64) -> fmt::Result {
    if v.fract() == 0.0 {
        write!(f, "{v:.0}")
    } else {
        write!(f, "{v}")
    }
}

pub(crate) fn extract_numbers(text: &str) -> Option<Vec<f64>> {
    let mut out = Vec::new();
    let mut buf = String::new();
    for ch in text.chars() {
        if ch.is_ascii_digit() || matches!(ch, '-' | '+' | '.' | 'e' | 'E') {
            buf.push(ch);
        } else if !buf.is_empty() {
            out.push(buf.parse::<f64>().ok()?);
            buf.clear();
        }
    }
    if !buf.is_empty() {
        out.push(buf.parse::<f64>().ok()?);
    }
    Some(out)
}

