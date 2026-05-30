//! Binary encoding for persistent-catalog rows.
//!
//! Item 4 substrate. The persistent catalog needs to write
//! `pg_class` and `pg_attribute` rows to its underlying heap so
//! [`crate::PersistentCatalog::bootstrap_from_heap`] can rebuild the
//! in-memory `TableEntry` map after a restart. The on-disk shape is
//! **not** protocol wire format — it is an internal length-
//! prefixed binary encoding designed for compact, fast round-trips
//! during bootstrap, not for client interoperability.
//!
//! Each row is a single payload the heap will wrap with a
//! `ultrasql_mvcc::TupleHeader` at insert time, exactly like a user
//! table row. Decoding consumes the slot's `data` slice (bytes after
//! the tuple header).
//!
//! Format
//! ------
//!
//! Multi-byte integers are little-endian. Strings are length-prefixed
//! with a `u32` byte length followed by UTF-8 bytes. Booleans are
//! `0x00` (false) or `0x01` (true). `DataType` is a single-byte tag
//! followed by zero or more parameter bytes — see
//! `encode_data_type`.

use ultrasql_core::{DataType, Error as CoreError, Field, GeometryType, Oid, RangeType, Schema};

use crate::persistent::{
    AttributeRow, ClassRow, ConType, ConstraintRow, DescriptionRow, EnumRow, IndexRow, RelKind,
    SequenceRow, StatisticExtRow, StatisticRow, TypeRow,
};

/// Errors raised while writing a row to bytes.
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    /// The supplied [`DataType`] is outside the v0.7 catalog
    /// persistence set. The catalog can still hold the type in
    /// memory; it cannot durably store a column declaration that
    /// uses it.
    #[error("data type not yet supported for catalog persistence: {0:?}")]
    UnsupportedType(DataType),
}

/// Errors raised while reading a row from bytes.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// The byte slice ended before the encoder's expected payload was
    /// fully consumed. Recovery treats this as catalog corruption.
    #[error("unexpected end of bytes (need {needed} more, have {have})")]
    UnexpectedEnd {
        /// How many additional bytes the decoder still required.
        needed: usize,
        /// How many were available.
        have: usize,
    },

    /// A tag byte did not match any known variant. Same recovery
    /// treatment as [`Self::UnexpectedEnd`].
    #[error("invalid tag byte {tag:#x} at offset {offset}")]
    InvalidTag {
        /// The offending byte.
        tag: u8,
        /// Byte offset within the slice.
        offset: usize,
    },

    /// A length-prefixed UTF-8 string contained invalid bytes.
    #[error("invalid UTF-8 in string field")]
    InvalidUtf8,

    /// The decoded `Schema` could not be constructed from its
    /// `Field`s — duplicate column names typically.
    #[error("schema rebuild failed: {0}")]
    Schema(#[from] CoreError),
}

// ---------------------------------------------------------------------------
// Low-level reader / writer helpers
// ---------------------------------------------------------------------------

struct Writer<'a>(&'a mut Vec<u8>);

impl Writer<'_> {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn i16(&mut self, v: i16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn i32(&mut self, v: i32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn f32(&mut self, v: f32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn f64(&mut self, v: f64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn bool(&mut self, v: bool) {
        self.0.push(u8::from(v));
    }
    fn str(&mut self, s: &str) {
        let bytes = s.as_bytes();
        self.u32(u32::try_from(bytes.len()).expect("string fits in u32"));
        self.0.extend_from_slice(bytes);
    }
    fn string_pairs(&mut self, pairs: &[(String, String)]) {
        self.u32(u32::try_from(pairs.len()).expect("pair count fits in u32"));
        for (key, value) in pairs {
            self.str(key);
            self.str(value);
        }
    }
    fn opt_u32(&mut self, v: Option<u32>) {
        match v {
            None => self.0.push(0),
            Some(x) => {
                self.0.push(1);
                self.u32(x);
            }
        }
    }
    fn opt_i32(&mut self, v: Option<i32>) {
        match v {
            None => self.0.push(0),
            Some(x) => {
                self.0.push(1);
                self.0.extend_from_slice(&x.to_le_bytes());
            }
        }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.pos + n > self.bytes.len() {
            return Err(DecodeError::UnexpectedEnd {
                needed: n,
                have: self.bytes.len().saturating_sub(self.pos),
            });
        }
        let out = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let bytes = self.take(N)?;
        bytes.try_into().map_err(|_| DecodeError::UnexpectedEnd {
            needed: N,
            have: bytes.len(),
        })
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    fn i16(&mut self) -> Result<i16, DecodeError> {
        Ok(i16::from_le_bytes(self.fixed()?))
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.fixed()?))
    }
    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_le_bytes(self.fixed()?))
    }
    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_le_bytes(self.fixed()?))
    }
    fn f32(&mut self) -> Result<f32, DecodeError> {
        Ok(f32::from_le_bytes(self.fixed()?))
    }
    fn f64(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from_le_bytes(self.fixed()?))
    }
    fn bool(&mut self) -> Result<bool, DecodeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            tag => Err(DecodeError::InvalidTag {
                tag,
                offset: self.pos - 1,
            }),
        }
    }
    fn str(&mut self) -> Result<String, DecodeError> {
        let len = self.u32()? as usize;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| DecodeError::InvalidUtf8)
    }
    fn opt_u32(&mut self) -> Result<Option<u32>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u32()?)),
            tag => Err(DecodeError::InvalidTag {
                tag,
                offset: self.pos - 1,
            }),
        }
    }
    fn opt_i32(&mut self) -> Result<Option<i32>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.i32()?)),
            tag => Err(DecodeError::InvalidTag {
                tag,
                offset: self.pos - 1,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// DataType tags
// ---------------------------------------------------------------------------

const DT_BOOL: u8 = 0x01;
const DT_INT16: u8 = 0x02;
const DT_INT32: u8 = 0x03;
const DT_INT64: u8 = 0x04;
const DT_FLOAT32: u8 = 0x05;
const DT_FLOAT64: u8 = 0x06;
const DT_DECIMAL: u8 = 0x07;
const DT_TEXT: u8 = 0x08;
const DT_BYTEA: u8 = 0x09;
const DT_TIMESTAMP: u8 = 0x0a;
const DT_TIMESTAMPTZ: u8 = 0x0b;
const DT_DATE: u8 = 0x0c;
const DT_TIME: u8 = 0x0d;
const DT_INTERVAL: u8 = 0x0e;
const DT_UUID: u8 = 0x0f;
const DT_JSONB: u8 = 0x10;
const DT_NULL: u8 = 0x11;
const DT_RANGE: u8 = 0x12;
const DT_GEOMETRY: u8 = 0x13;
const DT_ARRAY: u8 = 0x14;
const DT_VECTOR: u8 = 0x15;
const DT_HALFVEC: u8 = 0x16;
const DT_SPARSEVEC: u8 = 0x17;
const DT_BITVEC: u8 = 0x18;
const DT_MONEY: u8 = 0x19;
const DT_CHAR: u8 = 0x1a;
const DT_TIMETZ: u8 = 0x1b;
const DT_BIT: u8 = 0x1c;
const DT_VARBIT: u8 = 0x1d;
const DT_INET: u8 = 0x1e;
const DT_CIDR: u8 = 0x1f;
const DT_MACADDR: u8 = 0x20;
const DT_MACADDR8: u8 = 0x21;
const DT_JSON: u8 = 0x22;
const DT_ENUM: u8 = 0x23;
const DT_COMPOSITE: u8 = 0x24;
const DT_DOMAIN: u8 = 0x25;
const DT_OID: u8 = 0x26;
const DT_REGCLASS: u8 = 0x27;
const DT_REGTYPE: u8 = 0x28;
const DT_PG_LSN: u8 = 0x29;
const DT_XML: u8 = 0x2a;

fn encode_data_type(w: &mut Writer<'_>, ty: &DataType) -> Result<(), EncodeError> {
    match ty {
        DataType::Bool => w.u8(DT_BOOL),
        DataType::Int16 => w.u8(DT_INT16),
        DataType::Int32 => w.u8(DT_INT32),
        DataType::Int64 => w.u8(DT_INT64),
        DataType::Float32 => w.u8(DT_FLOAT32),
        DataType::Float64 => w.u8(DT_FLOAT64),
        DataType::Decimal { precision, scale } => {
            w.u8(DT_DECIMAL);
            w.opt_u32(*precision);
            w.opt_i32(*scale);
        }
        DataType::Money => w.u8(DT_MONEY),
        DataType::Oid => w.u8(DT_OID),
        DataType::RegClass => w.u8(DT_REGCLASS),
        DataType::RegType => w.u8(DT_REGTYPE),
        DataType::PgLsn => w.u8(DT_PG_LSN),
        DataType::Text { max_len } => {
            w.u8(DT_TEXT);
            w.opt_u32(*max_len);
        }
        DataType::Char { len } => {
            w.u8(DT_CHAR);
            w.opt_u32(*len);
        }
        DataType::Enum { oid, name, labels } => {
            w.u8(DT_ENUM);
            w.u32(oid.raw());
            w.str(name);
            w.u32(u32::try_from(labels.len()).expect("enum label count fits in u32"));
            for label in labels.iter() {
                w.str(label);
            }
        }
        DataType::Composite { oid, name, fields } => {
            w.u8(DT_COMPOSITE);
            w.u32(oid.raw());
            w.str(name);
            w.u32(u32::try_from(fields.len()).expect("composite field count fits in u32"));
            for (field_name, field_type) in fields.iter() {
                w.str(field_name);
                encode_data_type(w, field_type)?;
            }
        }
        DataType::Domain {
            oid,
            name,
            base_type,
            not_null,
        } => {
            w.u8(DT_DOMAIN);
            w.u32(oid.raw());
            w.str(name);
            w.bool(*not_null);
            encode_data_type(w, base_type)?;
        }
        DataType::Bit { len } => {
            w.u8(DT_BIT);
            w.opt_u32(*len);
        }
        DataType::VarBit { max_len } => {
            w.u8(DT_VARBIT);
            w.opt_u32(*max_len);
        }
        DataType::Inet => w.u8(DT_INET),
        DataType::Cidr => w.u8(DT_CIDR),
        DataType::MacAddr => w.u8(DT_MACADDR),
        DataType::MacAddr8 => w.u8(DT_MACADDR8),
        DataType::Bytea => w.u8(DT_BYTEA),
        DataType::Timestamp => w.u8(DT_TIMESTAMP),
        DataType::TimestampTz => w.u8(DT_TIMESTAMPTZ),
        DataType::Date => w.u8(DT_DATE),
        DataType::Time => w.u8(DT_TIME),
        DataType::TimeTz => w.u8(DT_TIMETZ),
        DataType::Interval => w.u8(DT_INTERVAL),
        DataType::Uuid => w.u8(DT_UUID),
        DataType::Json => w.u8(DT_JSON),
        DataType::Jsonb => w.u8(DT_JSONB),
        DataType::Xml => w.u8(DT_XML),
        DataType::Vector { dims } => {
            w.u8(DT_VECTOR);
            w.opt_u32(*dims);
        }
        DataType::HalfVec { dims } => {
            w.u8(DT_HALFVEC);
            w.opt_u32(*dims);
        }
        DataType::SparseVec { dims } => {
            w.u8(DT_SPARSEVEC);
            w.opt_u32(*dims);
        }
        DataType::BitVec { dims } => {
            w.u8(DT_BITVEC);
            w.opt_u32(*dims);
        }
        DataType::Null => w.u8(DT_NULL),
        DataType::Range(range_type) => {
            w.u8(DT_RANGE);
            w.u8(encode_range_type(*range_type));
        }
        DataType::Geometry(geometry_type) => {
            w.u8(DT_GEOMETRY);
            w.u8(encode_geometry_type(*geometry_type));
        }
        DataType::Array(inner) => {
            w.u8(DT_ARRAY);
            encode_data_type(w, inner)?;
        }
        DataType::Record(_) => {
            return Err(EncodeError::UnsupportedType(ty.clone()));
        }
        // `DataType` is `#[non_exhaustive]`; treat any future variant
        // as an unsupported persistence target so adding a new type
        // to core does not silently produce malformed catalog rows.
        _ => return Err(EncodeError::UnsupportedType(ty.clone())),
    }
    Ok(())
}

fn decode_data_type(r: &mut Reader<'_>) -> Result<DataType, DecodeError> {
    let tag = r.u8()?;
    Ok(match tag {
        DT_BOOL => DataType::Bool,
        DT_INT16 => DataType::Int16,
        DT_INT32 => DataType::Int32,
        DT_INT64 => DataType::Int64,
        DT_FLOAT32 => DataType::Float32,
        DT_FLOAT64 => DataType::Float64,
        DT_DECIMAL => DataType::Decimal {
            precision: r.opt_u32()?,
            scale: r.opt_i32()?,
        },
        DT_MONEY => DataType::Money,
        DT_OID => DataType::Oid,
        DT_REGCLASS => DataType::RegClass,
        DT_REGTYPE => DataType::RegType,
        DT_PG_LSN => DataType::PgLsn,
        DT_TEXT => DataType::Text {
            max_len: r.opt_u32()?,
        },
        DT_CHAR => DataType::Char { len: r.opt_u32()? },
        DT_ENUM => {
            let oid = Oid::new(r.u32()?);
            let name = r.str()?;
            let label_count =
                usize::try_from(r.u32()?).expect("u32 fits in usize on supported targets");
            let mut labels = Vec::with_capacity(label_count);
            for _ in 0..label_count {
                labels.push(r.str()?);
            }
            DataType::Enum {
                oid,
                name: name.into(),
                labels: labels.into(),
            }
        }
        DT_COMPOSITE => {
            let oid = Oid::new(r.u32()?);
            let name = r.str()?;
            let field_count =
                usize::try_from(r.u32()?).expect("u32 fits in usize on supported targets");
            let mut fields = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                let field_name = r.str()?;
                let field_type = decode_data_type(r)?;
                fields.push((field_name, field_type));
            }
            DataType::Composite {
                oid,
                name: name.into(),
                fields: fields.into(),
            }
        }
        DT_DOMAIN => {
            let oid = Oid::new(r.u32()?);
            let name = r.str()?;
            let not_null = r.bool()?;
            let base_type = decode_data_type(r)?;
            DataType::Domain {
                oid,
                name: name.into(),
                base_type: Box::new(base_type),
                not_null,
            }
        }
        DT_BIT => DataType::Bit { len: r.opt_u32()? },
        DT_VARBIT => DataType::VarBit {
            max_len: r.opt_u32()?,
        },
        DT_INET => DataType::Inet,
        DT_CIDR => DataType::Cidr,
        DT_MACADDR => DataType::MacAddr,
        DT_MACADDR8 => DataType::MacAddr8,
        DT_BYTEA => DataType::Bytea,
        DT_TIMESTAMP => DataType::Timestamp,
        DT_TIMESTAMPTZ => DataType::TimestampTz,
        DT_DATE => DataType::Date,
        DT_TIME => DataType::Time,
        DT_TIMETZ => DataType::TimeTz,
        DT_INTERVAL => DataType::Interval,
        DT_UUID => DataType::Uuid,
        DT_JSON => DataType::Json,
        DT_JSONB => DataType::Jsonb,
        DT_XML => DataType::Xml,
        DT_VECTOR => DataType::Vector { dims: r.opt_u32()? },
        DT_HALFVEC => DataType::HalfVec { dims: r.opt_u32()? },
        DT_SPARSEVEC => DataType::SparseVec { dims: r.opt_u32()? },
        DT_BITVEC => DataType::BitVec { dims: r.opt_u32()? },
        DT_NULL => DataType::Null,
        DT_RANGE => DataType::Range(decode_range_type(r.u8()?)?),
        DT_GEOMETRY => DataType::Geometry(decode_geometry_type(r.u8()?)?),
        DT_ARRAY => DataType::Array(Box::new(decode_data_type(r)?)),
        other => {
            return Err(DecodeError::InvalidTag {
                tag: other,
                offset: r.pos - 1,
            });
        }
    })
}

const fn encode_range_type(range_type: RangeType) -> u8 {
    match range_type {
        RangeType::Int4 => 1,
        RangeType::Int8 => 2,
        RangeType::Num => 3,
        RangeType::Date => 4,
        RangeType::Timestamp => 5,
        RangeType::TimestampTz => 6,
    }
}

fn decode_range_type(tag: u8) -> Result<RangeType, DecodeError> {
    match tag {
        1 => Ok(RangeType::Int4),
        2 => Ok(RangeType::Int8),
        3 => Ok(RangeType::Num),
        4 => Ok(RangeType::Date),
        5 => Ok(RangeType::Timestamp),
        6 => Ok(RangeType::TimestampTz),
        other => Err(DecodeError::InvalidTag {
            tag: other,
            offset: 0,
        }),
    }
}

const fn encode_geometry_type(geometry_type: GeometryType) -> u8 {
    match geometry_type {
        GeometryType::Point => 1,
        GeometryType::Box => 2,
        GeometryType::Circle => 3,
        GeometryType::Line => 4,
        GeometryType::Lseg => 5,
        GeometryType::Path => 6,
        GeometryType::Polygon => 7,
    }
}

fn decode_geometry_type(tag: u8) -> Result<GeometryType, DecodeError> {
    match tag {
        1 => Ok(GeometryType::Point),
        2 => Ok(GeometryType::Box),
        3 => Ok(GeometryType::Circle),
        4 => Ok(GeometryType::Line),
        5 => Ok(GeometryType::Lseg),
        6 => Ok(GeometryType::Path),
        7 => Ok(GeometryType::Polygon),
        other => Err(DecodeError::InvalidTag {
            tag: other,
            offset: 0,
        }),
    }
}

// ---------------------------------------------------------------------------
// RelKind tags
// ---------------------------------------------------------------------------

const RK_TABLE: u8 = b'r';
const RK_INDEX: u8 = b'i';
const RK_SEQ: u8 = b'S';
const RK_VIEW: u8 = b'v';
const RK_MAT_VIEW: u8 = b'm';
const RK_COMP: u8 = b'c';
const RK_TOAST: u8 = b't';
const RK_FOREIGN: u8 = b'f';
const RK_DROPPED: u8 = b'd';

const fn encode_relkind(k: RelKind) -> u8 {
    match k {
        RelKind::Table => RK_TABLE,
        RelKind::Index => RK_INDEX,
        RelKind::Sequence => RK_SEQ,
        RelKind::View => RK_VIEW,
        RelKind::MaterializedView => RK_MAT_VIEW,
        RelKind::CompositeType => RK_COMP,
        RelKind::Toast => RK_TOAST,
        RelKind::ForeignTable => RK_FOREIGN,
        RelKind::Dropped => RK_DROPPED,
    }
}

fn decode_relkind(b: u8, offset: usize) -> Result<RelKind, DecodeError> {
    Ok(match b {
        RK_TABLE => RelKind::Table,
        RK_INDEX => RelKind::Index,
        RK_SEQ => RelKind::Sequence,
        RK_VIEW => RelKind::View,
        RK_MAT_VIEW => RelKind::MaterializedView,
        RK_COMP => RelKind::CompositeType,
        RK_TOAST => RelKind::Toast,
        RK_FOREIGN => RelKind::ForeignTable,
        RK_DROPPED => RelKind::Dropped,
        other => return Err(DecodeError::InvalidTag { tag: other, offset }),
    })
}

// ---------------------------------------------------------------------------
// ClassRow
// ---------------------------------------------------------------------------

impl ClassRow {
    /// Serialise this row into the catalog's internal binary format.
    /// See the module-level documentation for the byte layout.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64 + self.relname.len());
        let mut w = Writer(&mut out);
        w.u32(self.oid.raw());
        w.str(&self.relname);
        w.u32(self.relnamespace.raw());
        w.u8(encode_relkind(self.relkind));
        w.u32(self.relpages);
        w.f64(self.reltuples);
        w.u32(self.relfilenode);
        w.bool(self.relhasindex);
        w.string_pairs(&self.reloptions);
        out
    }

    /// Deserialise a row produced by [`Self::encode`].
    ///
    /// Returns [`DecodeError`] when the slice is truncated, the
    /// [`RelKind`] byte does not match a known variant, or UTF-8
    /// validation fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = Reader::new(bytes);
        let oid = Oid::new(r.u32()?);
        let relname = r.str()?;
        let relnamespace = Oid::new(r.u32()?);
        let relkind_byte = r.u8()?;
        let relkind = decode_relkind(relkind_byte, r.pos - 1)?;
        let relpages = r.u32()?;
        let reltuples = r.f64()?;
        let relfilenode = r.u32()?;
        let relhasindex = r.bool()?;
        let count = r.u32()?;
        let mut reloptions = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
        for _ in 0..count {
            let key = r.str()?;
            let value = r.str()?;
            reloptions.push((key, value));
        }
        Ok(Self {
            oid,
            relname,
            relnamespace,
            relkind,
            relpages,
            reltuples,
            relfilenode,
            relhasindex,
            reloptions,
        })
    }
}

// ---------------------------------------------------------------------------
// AttributeRow
// ---------------------------------------------------------------------------
//
// `AttributeRow` does not carry the column's [`DataType`] — its
// `atttypid` is a `u32` OID intended for a future `pg_type` lookup.
// For catalog persistence we need the full type, so the on-disk
// encoding adds two trailing fields not present on the in-memory
// struct: the `DataType` tag and the column's `nullable` flag. They
// are read back into the bootstrap path that rebuilds the `Schema`.

/// Encode an attribute row plus its column's [`DataType`] +
/// nullability into a single byte string. The two trailing fields are
/// required for catalog bootstrap to rebuild a `Schema`; they are not
/// present on the in-memory [`AttributeRow`].
///
/// # Errors
///
/// Returns [`EncodeError::UnsupportedType`] if `data_type` is outside
/// the catalog-persistable set.
pub fn encode_attribute_row(
    row: &AttributeRow,
    data_type: &DataType,
    nullable: bool,
) -> Result<Vec<u8>, EncodeError> {
    let mut out = Vec::with_capacity(32 + row.attname.len());
    let mut w = Writer(&mut out);
    w.u32(row.attrelid.raw());
    w.str(&row.attname);
    w.u32(row.atttypid);
    w.i16(row.attnum);
    w.bool(row.attnotnull);
    w.bool(row.atthasdef);
    w.bool(row.attisdropped);
    encode_data_type(&mut w, data_type)?;
    w.bool(nullable);
    Ok(out)
}

/// Round-trip pair for [`encode_attribute_row`]. Returns the row plus
/// the column's [`DataType`] and nullability so the bootstrap path
/// can reconstruct a [`Field`].
pub fn decode_attribute_row(bytes: &[u8]) -> Result<(AttributeRow, DataType, bool), DecodeError> {
    let mut r = Reader::new(bytes);
    let attrelid = Oid::new(r.u32()?);
    let attname = r.str()?;
    let atttypid = r.u32()?;
    let attnum = r.i16()?;
    let attnotnull = r.bool()?;
    let atthasdef = r.bool()?;
    let attisdropped = r.bool()?;
    let data_type = decode_data_type(&mut r)?;
    let nullable = r.bool()?;
    Ok((
        AttributeRow {
            attrelid,
            attname,
            atttypid,
            attnum,
            attnotnull,
            atthasdef,
            attisdropped,
        },
        data_type,
        nullable,
    ))
}

// ---------------------------------------------------------------------------
// Convenience: build a Schema from a list of decoded attribute rows
// ---------------------------------------------------------------------------

/// Rebuild a [`Schema`] from `(AttributeRow, DataType, nullable)`
/// triples produced by [`decode_attribute_row`]. The triples are
/// sorted by `attnum` (ascending) before the [`Field`] list is built
/// so the column order matches the original `CREATE TABLE`.
///
/// Dropped columns (`attisdropped == true`) are skipped — they live
/// in `pg_attribute` for catalog history but never appear in a query
/// schema.
///
/// # Errors
///
/// Returns the underlying `ultrasql_core::Error` from `Schema::new`
/// if the resulting field list is invalid (duplicate names, etc.).
pub fn schema_from_attributes(
    mut rows: Vec<(AttributeRow, DataType, bool)>,
) -> Result<Schema, DecodeError> {
    rows.sort_by_key(|(r, _, _)| r.attnum);
    let fields: Vec<Field> = rows
        .into_iter()
        .filter(|(r, _, _)| !r.attisdropped)
        .map(|(row, dt, nullable)| Field {
            name: row.attname,
            data_type: dt,
            nullable,
        })
        .collect();
    Schema::new(fields).map_err(DecodeError::from)
}

// ---------------------------------------------------------------------------
// TypeRow / EnumRow
// ---------------------------------------------------------------------------

/// Encode a `pg_type` row into the catalog's internal binary format.
#[must_use]
pub fn encode_type_row(row: &TypeRow) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + row.typname.len());
    let mut w = Writer(&mut out);
    w.u32(row.oid.raw());
    w.str(&row.typname);
    w.u32(row.typnamespace.raw());
    w.str(&row.typtype.to_string());
    w.str(&row.typcategory.to_string());
    w.i16(row.typlen);
    w.u32(row.typelem);
    out
}

/// Decode a `pg_type` row from the catalog's internal binary format.
pub fn decode_type_row(bytes: &[u8]) -> Result<TypeRow, DecodeError> {
    let mut r = Reader::new(bytes);
    let oid = Oid::new(r.u32()?);
    let typname = r.str()?;
    let typnamespace = Oid::new(r.u32()?);
    let typtype = decode_single_char(&r.str()?)?;
    let typcategory = decode_single_char(&r.str()?)?;
    let typlen = r.i16()?;
    let typelem = r.u32()?;
    Ok(TypeRow {
        oid,
        typname,
        typnamespace,
        typtype,
        typcategory,
        typlen,
        typelem,
    })
}

/// Encode a `pg_enum` row into the catalog's internal binary format.
#[must_use]
pub fn encode_enum_row(row: &EnumRow) -> Vec<u8> {
    let mut out = Vec::with_capacity(24 + row.enumlabel.len());
    let mut w = Writer(&mut out);
    w.u32(row.oid.raw());
    w.u32(row.enumtypid.raw());
    w.u32(row.enumsortorder);
    w.str(&row.enumlabel);
    out
}

/// Decode a `pg_enum` row from the catalog's internal binary format.
pub fn decode_enum_row(bytes: &[u8]) -> Result<EnumRow, DecodeError> {
    let mut r = Reader::new(bytes);
    Ok(EnumRow {
        oid: Oid::new(r.u32()?),
        enumtypid: Oid::new(r.u32()?),
        enumsortorder: r.u32()?,
        enumlabel: r.str()?,
    })
}

fn decode_single_char(text: &str) -> Result<char, DecodeError> {
    let mut chars = text.chars();
    let Some(ch) = chars.next() else {
        return Err(DecodeError::InvalidUtf8);
    };
    if chars.next().is_some() {
        return Err(DecodeError::InvalidUtf8);
    }
    Ok(ch)
}

// ---------------------------------------------------------------------------
// IndexRow
// ---------------------------------------------------------------------------

/// Encode a `pg_index` row into the catalog's internal binary format.
#[must_use]
pub fn encode_index_row(row: &IndexRow) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + row.indkey.len() * 2);
    let mut w = Writer(&mut out);
    w.u32(row.indexrelid.raw());
    w.u32(row.indrelid.raw());
    w.u32(u32::from(row.indnatts));
    w.bool(row.indisunique);
    w.bool(row.indisprimary);
    w.bool(row.indisvalid);
    w.u32(u32::try_from(row.indkey.len()).expect("indkey length fits in u32"));
    for attnum in &row.indkey {
        w.i16(*attnum);
    }
    w.str(&row.indmethod);
    w.u32(u32::try_from(row.indopclasses.len()).expect("opclass length fits in u32"));
    for opclass in &row.indopclasses {
        match opclass {
            Some(opclass) => {
                w.bool(true);
                w.str(opclass);
            }
            None => w.bool(false),
        }
    }
    w.u32(u32::try_from(row.indoptions.len()).expect("option length fits in u32"));
    for (name, value) in &row.indoptions {
        w.str(name);
        w.str(value);
    }
    out
}

/// Decode a row produced by [`encode_index_row`].
pub fn decode_index_row(bytes: &[u8]) -> Result<IndexRow, DecodeError> {
    let mut r = Reader::new(bytes);
    let indexrelid = Oid::new(r.u32()?);
    let indrelid = Oid::new(r.u32()?);
    let indnatts = u16::try_from(r.u32()?).map_err(|_| DecodeError::InvalidTag {
        tag: 0,
        offset: r.pos.saturating_sub(4),
    })?;
    let indisunique = r.bool()?;
    let indisprimary = r.bool()?;
    let indisvalid = r.bool()?;
    let key_len = usize::try_from(r.u32()?).expect("u32 length fits usize on supported targets");
    let mut indkey = Vec::with_capacity(key_len);
    for _ in 0..key_len {
        indkey.push(r.i16()?);
    }
    let (indmethod, indopclasses, indoptions) = if r.remaining() == 0 {
        ("btree".to_owned(), vec![None; indkey.len()], Vec::new())
    } else {
        let indmethod = r.str()?;
        let opclass_len =
            usize::try_from(r.u32()?).expect("u32 length fits usize on supported targets");
        let mut indopclasses = Vec::with_capacity(opclass_len);
        for _ in 0..opclass_len {
            indopclasses.push(if r.bool()? { Some(r.str()?) } else { None });
        }
        let option_len =
            usize::try_from(r.u32()?).expect("u32 length fits usize on supported targets");
        let mut indoptions = Vec::with_capacity(option_len);
        for _ in 0..option_len {
            indoptions.push((r.str()?, r.str()?));
        }
        (indmethod, indopclasses, indoptions)
    };
    Ok(IndexRow {
        indexrelid,
        indrelid,
        indnatts,
        indisunique,
        indisprimary,
        indisvalid,
        indkey,
        indmethod,
        indopclasses,
        indoptions,
    })
}

// ---------------------------------------------------------------------------
// ConstraintRow
// ---------------------------------------------------------------------------

fn encode_con_type(contype: ConType) -> u8 {
    match contype {
        ConType::Check => b'c',
        ConType::ForeignKey => b'f',
        ConType::PrimaryKey => b'p',
        ConType::Unique => b'u',
        ConType::Trigger => b't',
        ConType::Exclusion => b'x',
    }
}

fn decode_con_type(tag: u8, offset: usize) -> Result<ConType, DecodeError> {
    match tag {
        b'c' => Ok(ConType::Check),
        b'f' => Ok(ConType::ForeignKey),
        b'p' => Ok(ConType::PrimaryKey),
        b'u' => Ok(ConType::Unique),
        b't' => Ok(ConType::Trigger),
        b'x' => Ok(ConType::Exclusion),
        _ => Err(DecodeError::InvalidTag { tag, offset }),
    }
}

fn write_i16_vec(w: &mut Writer<'_>, values: &[i16]) {
    w.u32(u32::try_from(values.len()).expect("i16 vec length fits in u32"));
    for value in values {
        w.i16(*value);
    }
}

fn read_i16_vec(r: &mut Reader<'_>) -> Result<Vec<i16>, DecodeError> {
    let len = usize::try_from(r.u32()?).expect("u32 length fits usize on supported targets");
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(r.i16()?);
    }
    Ok(values)
}

/// Encode a `pg_constraint` row into the catalog's internal binary format.
#[must_use]
pub fn encode_constraint_row(row: &ConstraintRow) -> Vec<u8> {
    let mut out = Vec::new();
    let mut w = Writer(&mut out);
    w.u32(row.oid.raw());
    w.str(&row.conname);
    w.u32(row.conrelid.raw());
    w.u8(encode_con_type(row.contype));
    w.bool(row.condeferrable);
    w.bool(row.condeferred);
    write_i16_vec(&mut w, &row.conkey);
    w.u32(row.confrelid.raw());
    write_i16_vec(&mut w, &row.confkey);
    out
}

/// Decode a row produced by [`encode_constraint_row`].
pub fn decode_constraint_row(bytes: &[u8]) -> Result<ConstraintRow, DecodeError> {
    let mut r = Reader::new(bytes);
    let oid = Oid::new(r.u32()?);
    let conname = r.str()?;
    let conrelid = Oid::new(r.u32()?);
    let contype_offset = r.pos;
    let contype = decode_con_type(r.u8()?, contype_offset)?;
    let condeferrable = r.bool()?;
    let condeferred = r.bool()?;
    let conkey = read_i16_vec(&mut r)?;
    let confrelid = Oid::new(r.u32()?);
    let confkey = read_i16_vec(&mut r)?;
    Ok(ConstraintRow {
        oid,
        conname,
        conrelid,
        contype,
        condeferrable,
        condeferred,
        conkey,
        confrelid,
        confkey,
    })
}

// ---------------------------------------------------------------------------
// SequenceRow
// ---------------------------------------------------------------------------

/// Encode a `pg_sequence` row into the catalog's internal binary format.
#[must_use]
pub fn encode_sequence_row(row: &SequenceRow) -> Vec<u8> {
    let mut out = Vec::with_capacity(49);
    let mut w = Writer(&mut out);
    w.u32(row.seqrelid.raw());
    w.u32(row.seqtypid);
    w.i64(row.seqstart);
    w.i64(row.seqincrement);
    w.i64(row.seqmax);
    w.i64(row.seqmin);
    w.i64(row.seqcache);
    w.bool(row.seqcycle);
    out
}

/// Decode a row produced by [`encode_sequence_row`].
pub fn decode_sequence_row(bytes: &[u8]) -> Result<SequenceRow, DecodeError> {
    let mut r = Reader::new(bytes);
    Ok(SequenceRow {
        seqrelid: Oid::new(r.u32()?),
        seqtypid: r.u32()?,
        seqstart: r.i64()?,
        seqincrement: r.i64()?,
        seqmax: r.i64()?,
        seqmin: r.i64()?,
        seqcache: r.i64()?,
        seqcycle: r.bool()?,
    })
}

// ---------------------------------------------------------------------------
// DescriptionRow
// ---------------------------------------------------------------------------

/// Encode a `pg_description` row into the catalog's internal binary format.
///
/// `deleted` marks an append-only tombstone for `COMMENT ... IS NULL`.
#[must_use]
pub fn encode_description_row(row: &DescriptionRow, deleted: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(17 + row.description.len());
    let mut w = Writer(&mut out);
    w.u32(row.objoid.raw());
    w.u32(row.classoid.raw());
    w.i32(row.objsubid);
    w.str(&row.description);
    w.bool(deleted);
    out
}

/// Decode a row produced by [`encode_description_row`].
pub fn decode_description_row(bytes: &[u8]) -> Result<(DescriptionRow, bool), DecodeError> {
    let mut r = Reader::new(bytes);
    let row = DescriptionRow {
        objoid: Oid::new(r.u32()?),
        classoid: Oid::new(r.u32()?),
        objsubid: r.i32()?,
        description: r.str()?,
    };
    let deleted = r.bool()?;
    Ok((row, deleted))
}

// ---------------------------------------------------------------------------
// StatisticRow
// ---------------------------------------------------------------------------

/// Encode a `pg_statistic` row into the catalog's internal binary format.
#[must_use]
pub fn encode_statistic_row(row: &StatisticRow) -> Vec<u8> {
    let mut out = Vec::with_capacity(18);
    let mut w = Writer(&mut out);
    w.u32(row.starelid.raw());
    w.i16(row.staattnum);
    w.f32(row.stanullfrac);
    w.f32(row.stadistinct);
    out
}

/// Decode a row produced by [`encode_statistic_row`].
pub fn decode_statistic_row(bytes: &[u8]) -> Result<StatisticRow, DecodeError> {
    let mut r = Reader::new(bytes);
    Ok(StatisticRow {
        starelid: Oid::new(r.u32()?),
        staattnum: r.i16()?,
        stanullfrac: r.f32()?,
        stadistinct: r.f32()?,
    })
}

/// Encode a `pg_statistic_ext` row into the catalog's internal binary format.
#[must_use]
pub fn encode_statistic_ext_row(row: &StatisticExtRow) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + row.stxname.len());
    let mut w = Writer(&mut out);
    w.u32(row.oid.raw());
    w.str(&row.stxname);
    w.u32(row.stxrelid.raw());
    w.u32(u32::try_from(row.stxkeys.len()).expect("stxkeys length fits in u32"));
    for key in &row.stxkeys {
        w.i16(*key);
    }
    let stxkind: String = row.stxkind.iter().collect();
    w.str(&stxkind);
    out
}

/// Decode a row produced by [`encode_statistic_ext_row`].
pub fn decode_statistic_ext_row(bytes: &[u8]) -> Result<StatisticExtRow, DecodeError> {
    let mut r = Reader::new(bytes);
    let oid = Oid::new(r.u32()?);
    let stxname = r.str()?;
    let stxrelid = Oid::new(r.u32()?);
    let key_len = usize::try_from(r.u32()?).expect("u32 length fits usize on supported targets");
    let mut stxkeys = Vec::with_capacity(key_len);
    for _ in 0..key_len {
        stxkeys.push(r.i16()?);
    }
    let stxkind = r.str()?.chars().collect();
    Ok(StatisticExtRow {
        oid,
        stxname,
        stxrelid,
        stxkeys,
        stxkind,
    })
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};

    use super::*;

    fn sample_class_row(oid_raw: u32) -> ClassRow {
        ClassRow {
            oid: Oid::new(oid_raw),
            relname: format!("rel_{oid_raw}"),
            relnamespace: Oid::new(99),
            relkind: RelKind::Table,
            relpages: 17,
            reltuples: 12345.5,
            relfilenode: 42,
            relhasindex: true,
            reloptions: vec![("autovacuum_vacuum_threshold".to_owned(), "7".to_owned())],
        }
    }

    #[test]
    fn class_row_round_trip() {
        let row = sample_class_row(1234);
        let bytes = row.encode();
        let decoded = ClassRow::decode(&bytes).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn class_row_all_relkinds_round_trip() {
        for k in [
            RelKind::Table,
            RelKind::Index,
            RelKind::Sequence,
            RelKind::View,
            RelKind::MaterializedView,
            RelKind::CompositeType,
            RelKind::Toast,
            RelKind::ForeignTable,
            RelKind::Dropped,
        ] {
            let mut row = sample_class_row(7);
            row.relkind = k;
            let bytes = row.encode();
            let decoded = ClassRow::decode(&bytes).expect("decode");
            assert_eq!(decoded.relkind, k);
        }
    }

    #[test]
    fn attribute_row_round_trip_with_text_max_len() {
        let row = AttributeRow {
            attrelid: Oid::new(1000),
            attname: "email".to_owned(),
            atttypid: 25,
            attnum: 3,
            attnotnull: true,
            atthasdef: false,
            attisdropped: false,
        };
        let dt = DataType::Text { max_len: Some(255) };
        let bytes = encode_attribute_row(&row, &dt, false).expect("encode");
        let (decoded, dt_out, nullable_out) = decode_attribute_row(&bytes).expect("decode");
        assert_eq!(decoded, row);
        assert_eq!(dt_out, dt);
        assert!(!nullable_out);
    }

    #[test]
    fn attribute_row_round_trip_all_scalar_types() {
        let cases = vec![
            DataType::Bool,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Money,
            DataType::Bytea,
            DataType::Date,
            DataType::Time,
            DataType::TimeTz,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::Interval,
            DataType::Uuid,
            DataType::Json,
            DataType::Jsonb,
            DataType::Xml,
            DataType::Null,
            DataType::Decimal {
                precision: Some(20),
                scale: Some(4),
            },
            DataType::Decimal {
                precision: None,
                scale: None,
            },
            DataType::Vector { dims: Some(1536) },
            DataType::Vector { dims: None },
            DataType::HalfVec { dims: Some(1536) },
            DataType::HalfVec { dims: None },
            DataType::SparseVec { dims: Some(1536) },
            DataType::SparseVec { dims: None },
            DataType::BitVec { dims: Some(512) },
            DataType::BitVec { dims: None },
            DataType::Bit { len: Some(4) },
            DataType::Bit { len: None },
            DataType::VarBit { max_len: Some(64) },
            DataType::VarBit { max_len: None },
            DataType::Inet,
            DataType::Cidr,
            DataType::MacAddr,
            DataType::MacAddr8,
            DataType::Text { max_len: None },
            DataType::Text {
                max_len: Some(1024),
            },
        ];
        for (i, dt) in cases.into_iter().enumerate() {
            let row = AttributeRow {
                attrelid: Oid::new(1),
                attname: format!("c{i}"),
                atttypid: 0,
                attnum: i16::try_from(i + 1).expect("test attribute index fits in i16"),
                attnotnull: i % 2 == 0,
                atthasdef: false,
                attisdropped: false,
            };
            let bytes = encode_attribute_row(&row, &dt, !row.attnotnull).expect("encode");
            let (got_row, got_dt, got_nullable) = decode_attribute_row(&bytes).expect("decode");
            assert_eq!(got_row, row);
            assert_eq!(got_dt, dt);
            assert_eq!(got_nullable, !row.attnotnull);
        }
    }

    #[test]
    fn unsupported_types_reported() {
        let row = AttributeRow {
            attrelid: Oid::new(1),
            attname: "a".into(),
            atttypid: 0,
            attnum: 1,
            attnotnull: false,
            atthasdef: false,
            attisdropped: false,
        };
        let dt = DataType::Record(vec![("x".to_owned(), DataType::Int32)]);
        let err = encode_attribute_row(&row, &dt, true).unwrap_err();
        assert!(matches!(err, EncodeError::UnsupportedType(_)));
    }

    #[test]
    fn array_type_round_trip() {
        let row = AttributeRow {
            attrelid: Oid::new(1),
            attname: "tags".into(),
            atttypid: 0,
            attnum: 1,
            attnotnull: false,
            atthasdef: false,
            attisdropped: false,
        };
        let dt = DataType::Array(Box::new(DataType::Text { max_len: None }));
        let bytes = encode_attribute_row(&row, &dt, true).expect("encode");
        let (_row, got_dt, nullable) = decode_attribute_row(&bytes).expect("decode");
        assert_eq!(got_dt, dt);
        assert!(nullable);
    }

    #[test]
    fn schema_from_attributes_sorts_by_attnum_and_drops_dropped() {
        let rows = vec![
            (
                AttributeRow {
                    attrelid: Oid::new(1),
                    attname: "b".into(),
                    atttypid: 0,
                    attnum: 2,
                    attnotnull: false,
                    atthasdef: false,
                    attisdropped: false,
                },
                DataType::Int32,
                true,
            ),
            (
                AttributeRow {
                    attrelid: Oid::new(1),
                    attname: "old".into(),
                    atttypid: 0,
                    attnum: 3,
                    attnotnull: false,
                    atthasdef: false,
                    attisdropped: true,
                },
                DataType::Text { max_len: None },
                true,
            ),
            (
                AttributeRow {
                    attrelid: Oid::new(1),
                    attname: "a".into(),
                    atttypid: 0,
                    attnum: 1,
                    attnotnull: true,
                    atthasdef: false,
                    attisdropped: false,
                },
                DataType::Int64,
                false,
            ),
        ];
        let schema = schema_from_attributes(rows).expect("schema rebuild");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.fields()[0].name, "a");
        assert_eq!(schema.fields()[0].data_type, DataType::Int64);
        assert!(!schema.fields()[0].nullable);
        assert_eq!(schema.fields()[1].name, "b");
        assert_eq!(schema.fields()[1].data_type, DataType::Int32);
        assert!(schema.fields()[1].nullable);
    }

    #[test]
    fn truncated_payload_is_caught() {
        let row = sample_class_row(1);
        let bytes = row.encode();
        for cut in 0..bytes.len() {
            assert!(
                ClassRow::decode(&bytes[..cut]).is_err(),
                "decode should fail at cut={cut}"
            );
        }
    }

    #[test]
    fn full_schema_round_trip_int32_text_bool() {
        let original = Schema::new(vec![
            Field {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            Field {
                name: "name".into(),
                data_type: DataType::Text { max_len: None },
                nullable: true,
            },
            Field {
                name: "active".into(),
                data_type: DataType::Bool,
                nullable: false,
            },
        ])
        .expect("schema");
        let rows: Vec<(AttributeRow, DataType, bool)> = original
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let row = AttributeRow {
                    attrelid: Oid::new(7),
                    attname: f.name.clone(),
                    atttypid: 0,
                    attnum: i16::try_from(i + 1).expect("test attribute index fits in i16"),
                    attnotnull: !f.nullable,
                    atthasdef: false,
                    attisdropped: false,
                };
                let bytes = encode_attribute_row(&row, &f.data_type, f.nullable).expect("encode");
                decode_attribute_row(&bytes).expect("decode")
            })
            .collect();
        let rebuilt = schema_from_attributes(rows).expect("rebuild");
        assert_eq!(rebuilt, original);
    }

    #[test]
    fn statistic_row_round_trip() {
        let row = StatisticRow {
            starelid: Oid::new(42_000),
            staattnum: 2,
            stanullfrac: 0.125,
            stadistinct: -0.75,
        };
        let bytes = encode_statistic_row(&row);
        let decoded = decode_statistic_row(&bytes).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn index_row_round_trip() {
        let row = IndexRow {
            indexrelid: Oid::new(42_100),
            indrelid: Oid::new(42_000),
            indnatts: 2,
            indisunique: true,
            indisprimary: false,
            indisvalid: true,
            indkey: vec![0, 1],
            indmethod: "hnsw".to_owned(),
            indopclasses: vec![Some("vector_l2_ops".to_owned()), None],
            indoptions: vec![("m".to_owned(), "16".to_owned())],
        };
        let bytes = encode_index_row(&row);
        let decoded = decode_index_row(&bytes).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn constraint_row_round_trip() {
        let row = ConstraintRow {
            oid: Oid::new(42_200),
            conname: "orders_customer_fk".to_owned(),
            conrelid: Oid::new(42_000),
            contype: ConType::ForeignKey,
            condeferrable: true,
            condeferred: false,
            conkey: vec![2],
            confrelid: Oid::new(42_001),
            confkey: vec![0],
        };
        let bytes = encode_constraint_row(&row);
        let decoded = decode_constraint_row(&bytes).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn sequence_row_round_trip() {
        let row = SequenceRow {
            seqrelid: Oid::new(42_300),
            seqtypid: 20,
            seqstart: 1,
            seqincrement: 5,
            seqmax: i64::MAX,
            seqmin: 1,
            seqcache: 32,
            seqcycle: true,
        };
        let bytes = encode_sequence_row(&row);
        let decoded = decode_sequence_row(&bytes).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn statistic_ext_row_round_trip() {
        let row = StatisticExtRow {
            oid: Oid::new(42_010),
            stxname: "s_ab".to_owned(),
            stxrelid: Oid::new(42_000),
            stxkeys: vec![1, 2],
            stxkind: vec!['d', 'f', 'm'],
        };
        let bytes = encode_statistic_ext_row(&row);
        let decoded = decode_statistic_ext_row(&bytes).expect("decode");
        assert_eq!(decoded, row);
    }
}
