use super::*;

impl Value {
    /// The dynamic [`DataType`] of this value.
    #[must_use]
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Bool(_) => DataType::Bool,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::Oid(_) => DataType::Oid,
            Self::RegClass(_) => DataType::RegClass,
            Self::RegType(_) => DataType::RegType,
            Self::PgLsn(_) => DataType::PgLsn,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Text(_) => DataType::Text { max_len: None },
            Self::Char(s) => DataType::Char {
                len: u32::try_from(s.chars().count()).ok(),
            },
            Self::Json(_) => DataType::Json,
            Self::Jsonb(_) => DataType::Jsonb,
            Self::Xml(_) => DataType::Xml,
            Self::Vector(v) => DataType::Vector {
                dims: u32::try_from(v.len()).ok(),
            },
            Self::HalfVec(v) => DataType::HalfVec {
                dims: u32::try_from(v.len()).ok(),
            },
            Self::SparseVec(v) => DataType::SparseVec { dims: Some(v.dims) },
            Self::BitVec { dims, .. } => DataType::BitVec { dims: Some(*dims) },
            Self::BitString(v) => DataType::VarBit {
                max_len: Some(v.len()),
            },
            Self::Network(v) => v.data_type(),
            Self::Bytea(_) => DataType::Bytea,
            Self::Timestamp(_) => DataType::Timestamp,
            Self::TimestampTz(_) => DataType::TimestampTz,
            Self::Date(_) => DataType::Date,
            Self::Time(_) => DataType::Time,
            Self::TimeTz { .. } => DataType::TimeTz,
            Self::Uuid(_) => DataType::Uuid,
            Self::Decimal { scale, .. } => DataType::Decimal {
                precision: None,
                scale: Some(*scale),
            },
            Self::Money(_) => DataType::Money,
            Self::Interval { .. } => DataType::Interval,
            Self::Range(v) => DataType::Range(v.range_type),
            Self::Geometry(v) => DataType::Geometry(v.geometry_type),
            Self::Array { element_type, .. } => DataType::Array(Box::new(element_type.clone())),
            Self::Record(fields) => DataType::Record(
                fields
                    .iter()
                    .map(|(name, value)| (name.clone(), value.data_type()))
                    .collect(),
            ),
        }
    }

    /// Width category used during planning: `None` for varlena values.
    #[must_use]
    pub const fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::Bool(_) => Some(1),
            Self::Int16(_) => Some(2),
            Self::Int32(_)
            | Self::Float32(_)
            | Self::Date(_)
            | Self::Oid(_)
            | Self::RegClass(_)
            | Self::RegType(_) => Some(4),
            Self::Int64(_)
            | Self::Money(_)
            | Self::Float64(_)
            | Self::Time(_)
            | Self::TimeTz { .. }
            | Self::Timestamp(_)
            | Self::TimestampTz(_)
            | Self::PgLsn(_) => Some(8),
            Self::Uuid(_) => Some(16),
            _ => None,
        }
    }

    /// `true` iff this value is SQL NULL.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Parse a PostgreSQL UUID literal into raw 16-byte storage.
    ///
    /// Accepts canonical hyphenated text and compact 32-hex-digit text.
    #[must_use]
    pub fn parse_uuid(text: &str) -> Option<[u8; 16]> {
        let mut nibbles = [0_u8; UUID_HEX_NIBBLES];
        let mut len = 0_usize;
        for byte in text.bytes() {
            if byte == b'-' {
                continue;
            }
            if len >= nibbles.len() {
                return None;
            }
            nibbles[len] = hex_nibble(byte)?;
            len = len.checked_add(1)?;
        }
        if len != nibbles.len() {
            return None;
        }
        let mut out = [0_u8; 16];
        for (slot, pair) in out
            .iter_mut()
            .zip(nibbles.chunks_exact(HEX_NIBBLES_PER_BYTE))
        {
            *slot = pack_hex_byte(pair[0], pair[1])?;
        }
        Some(out)
    }

    /// Parse PostgreSQL hex-style `bytea` text (`\xdeadbeef`).
    #[must_use]
    pub fn parse_bytea(text: &str) -> Option<Vec<u8>> {
        let hex = text
            .strip_prefix("\\x")
            .or_else(|| text.strip_prefix("\\X"))?;
        if hex.len().checked_rem(HEX_NIBBLES_PER_BYTE) != Some(0) {
            return None;
        }
        let mut out = Vec::with_capacity(hex.len().checked_div(HEX_NIBBLES_PER_BYTE).unwrap_or(0));
        let bytes = hex.as_bytes();
        for pair in bytes.chunks_exact(HEX_NIBBLES_PER_BYTE) {
            out.push(pack_hex_byte(hex_nibble(pair[0])?, hex_nibble(pair[1])?)?);
        }
        Some(out)
    }

    /// Validate a basic PostgreSQL `xml` literal and return stored text.
    ///
    /// The initial XML surface accepts one well-formed document with
    /// balanced element tags, quoted attributes, comments, CDATA, and
    /// processing instructions. It intentionally does not implement DTD
    /// validation, namespaces, or XPath semantics.
    #[must_use]
    pub fn validate_xml_text(text: &str) -> Option<String> {
        let trimmed = text.trim();
        if trimmed.is_empty() || !xml_document_is_well_formed(trimmed) {
            return None;
        }
        Some(trimmed.to_owned())
    }

    /// Return `true` when text is one well-formed XML document.
    ///
    /// Validation is local only: DTD declarations and external entity
    /// expansion are rejected rather than resolved.
    #[must_use]
    pub fn xml_document_is_well_formed(text: &str) -> bool {
        xml_document_is_well_formed(text)
    }

    /// Return `true` when text is well-formed XML content.
    ///
    /// Content may contain more than one top-level element. The same local-only
    /// security policy as [`Self::xml_document_is_well_formed`] applies.
    #[must_use]
    pub fn xml_content_is_well_formed(text: &str) -> bool {
        xml_content_is_well_formed(text)
    }

    /// Parse a pgvector-style vector literal, such as `[1,2.5,-3]`.
    ///
    /// Elements are `f32` and must be finite. Empty vectors and values
    /// above [`MAX_VECTOR_DIMS`] are rejected.
    #[must_use]
    pub fn parse_vector(text: &str) -> Option<Self> {
        let trimmed = text.trim();
        let inner = trimmed.strip_prefix('[')?.strip_suffix(']')?.trim();
        if inner.is_empty() {
            return None;
        }
        let mut values = Vec::new();
        for raw in inner.split(',') {
            let element = raw.trim();
            if element.is_empty() {
                return None;
            }
            let value = element.parse::<f32>().ok()?;
            if !value.is_finite() {
                return None;
            }
            values.push(value);
        }
        let dims = u32::try_from(values.len()).ok()?;
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return None;
        }
        Some(Self::Vector(values))
    }

    /// Parse a pgvector-style `halfvec` literal. Runtime values remain
    /// finite `f32` values; the SQL type controls storage/precision policy.
    #[must_use]
    pub fn parse_halfvec(text: &str) -> Option<Self> {
        let Self::Vector(values) = Self::parse_vector(text)? else {
            return None;
        };
        Some(Self::HalfVec(values))
    }

    /// Parse a pgvector-style sparse vector literal, e.g. `{1:1,3:2}/5`.
    #[must_use]
    pub fn parse_sparsevec(text: &str) -> Option<Self> {
        let trimmed = text.trim();
        let (entries_text, dims_text) = split_once_unquoted_slash(trimmed)?;
        let dims = dims_text.trim().parse::<u32>().ok()?;
        let inner = entries_text
            .trim()
            .strip_prefix('{')?
            .strip_suffix('}')?
            .trim();
        let mut entries = Vec::new();
        if !inner.is_empty() {
            for raw in inner.split(',') {
                let (idx, value) = split_once_unquoted_colon(raw)?;
                let idx = idx.trim().parse::<u32>().ok()?;
                let value = value.trim().parse::<f32>().ok()?;
                entries.push((idx, value));
            }
        }
        SparseVector::new(dims, entries).map(Self::SparseVec)
    }

    /// Parse a dense bit-vector literal containing only `0` and `1`.
    #[must_use]
    pub fn parse_bitvec(text: &str) -> Option<Self> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return None;
        }
        let dims = u32::try_from(trimmed.len()).ok()?;
        if dims == 0 || dims > MAX_VECTOR_DIMS {
            return None;
        }
        let mut bytes = vec![0_u8; trimmed.len().div_ceil(BITS_PER_BYTE)];
        for (idx, byte) in trimmed.bytes().enumerate() {
            match byte {
                b'0' => {}
                b'1' => {
                    let (byte_idx, shift) = packed_bit_position(idx)?;
                    let mask = 1_u8.checked_shl(shift)?;
                    *bytes.get_mut(byte_idx)? |= mask;
                }
                _ => return None,
            }
        }
        Some(Self::BitVec { dims, bytes })
    }

    /// Parse a SQL bit-string literal containing only `0` and `1`.
    #[must_use]
    pub fn parse_bit_string(text: &str) -> Option<Self> {
        BitString::parse(text).map(Self::BitString)
    }

    /// Parse a PostgreSQL network-address literal for a target type.
    #[must_use]
    pub fn parse_network(target: &DataType, text: &str) -> Option<Self> {
        NetworkValue::parse_for_type(target, text).map(Self::Network)
    }

    /// Parse a PostgreSQL `oid` text literal.
    #[must_use]
    pub fn parse_oid_text(text: &str) -> Option<Oid> {
        let trimmed = text.trim();
        if trimmed.is_empty() || trimmed.starts_with('-') {
            return None;
        }
        let raw = trimmed.parse::<u64>().ok()?;
        u32::try_from(raw).ok().map(Oid::new)
    }

    /// Parse a PostgreSQL `pg_lsn` text literal (`HEX/HEX`).
    #[must_use]
    pub fn parse_pg_lsn_text(text: &str) -> Option<Lsn> {
        let (high, low) = text.trim().split_once('/')?;
        if high.is_empty() || low.is_empty() {
            return None;
        }
        let high = u32::try_from(u64::from_str_radix(high, 16).ok()?).ok()?;
        let low = u32::try_from(u64::from_str_radix(low, 16).ok()?).ok()?;
        Some(Lsn::new((u64::from(high) << 32) | u64::from(low)))
    }

    /// Parse PostgreSQL's common text-array form, e.g. `{1,2,NULL}`.
    ///
    /// The parser is intentionally conservative: it supports the
    /// scalar element families UltraSQL can already store in rows and
    /// rejects malformed input instead of guessing.
    #[must_use]
    pub fn parse_array(element_type: DataType, text: &str) -> Option<Self> {
        let trimmed = text.trim();
        if !(trimmed.starts_with('{') && trimmed.ends_with('}')) {
            return None;
        }
        let inner = &trimmed[1..trimmed.len().checked_sub(1)?];
        let elements = if inner.is_empty() {
            Vec::new()
        } else {
            split_array_elements(inner)?
                .into_iter()
                .map(|part| parse_array_element(&element_type, part))
                .collect::<Option<Vec<_>>>()?
        };
        let value = Self::Array {
            element_type,
            elements,
        };
        value.array_dimensions()?;
        Some(value)
    }

    /// Borrowed `i64` view if this is an integer type, widening from
    /// narrower integers losslessly. `None` for non-integers.
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int16(v) => Some(i64::from(*v)),
            Self::Int32(v) => Some(i64::from(*v)),
            Self::Int64(v) => Some(*v),
            Self::Oid(v) | Self::RegClass(v) | Self::RegType(v) => Some(i64::from(v.raw())),
            _ => None,
        }
    }

    /// Borrowed `f64` view if this is a floating-point type, widening
    /// `f32` to `f64`. `None` for non-floats.
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float32(v) => Some(f64::from(*v)),
            Self::Float64(v) => Some(*v),
            _ => None,
        }
    }

    /// Borrowed string view if this is text. `None` otherwise.
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) | Self::Char(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Borrowed byte-slice view if this is bytea. `None` otherwise.
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytea(b) => Some(b.as_slice()),
            _ => None,
        }
    }

    /// Borrowed array view. `None` for non-array values.
    #[must_use]
    pub fn as_array(&self) -> Option<(&DataType, &[Value])> {
        match self {
            Self::Array {
                element_type,
                elements,
            } => Some((element_type, elements.as_slice())),
            _ => None,
        }
    }

    /// Dimensions of a rectangular PostgreSQL array value.
    ///
    /// Returns `None` for non-array values and for ragged nested arrays.
    /// Empty nested arrays report the dimensions that can be proven from
    /// stored values, matching the runtime representation's lack of
    /// explicit dimension headers.
    #[must_use]
    pub fn array_dimensions(&self) -> Option<Vec<usize>> {
        match self {
            Self::Array {
                element_type,
                elements,
            } => array_dimensions(element_type, elements),
            _ => None,
        }
    }

    /// Borrowed bool view. `None` for non-boolean.
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }
}
