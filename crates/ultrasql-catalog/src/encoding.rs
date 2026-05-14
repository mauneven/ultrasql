//! Binary encoding for persistent-catalog rows.
//!
//! Item 4 substrate. The persistent catalog needs to write
//! `pg_class` and `pg_attribute` rows to its underlying heap so
//! [`crate::PersistentCatalog::bootstrap_from_heap`] can rebuild the
//! in-memory `TableEntry` map after a restart. The on-disk shape is
//! **not** PostgreSQL wire-compatible — it is an internal length-
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

use ultrasql_core::{DataType, Error as CoreError, Field, Oid, Schema};

use crate::persistent::{AttributeRow, ClassRow, RelKind};

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
    fn u32(&mut self, v: u32) {
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
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    fn i16(&mut self) -> Result<i16, DecodeError> {
        Ok(i16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn f64(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
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
        DataType::Text { max_len } => {
            w.u8(DT_TEXT);
            w.opt_u32(*max_len);
        }
        DataType::Bytea => w.u8(DT_BYTEA),
        DataType::Timestamp => w.u8(DT_TIMESTAMP),
        DataType::TimestampTz => w.u8(DT_TIMESTAMPTZ),
        DataType::Date => w.u8(DT_DATE),
        DataType::Time => w.u8(DT_TIME),
        DataType::Interval => w.u8(DT_INTERVAL),
        DataType::Uuid => w.u8(DT_UUID),
        DataType::Jsonb => w.u8(DT_JSONB),
        DataType::Null => w.u8(DT_NULL),
        DataType::Array(_) | DataType::Record(_) => {
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
        DT_TEXT => DataType::Text {
            max_len: r.opt_u32()?,
        },
        DT_BYTEA => DataType::Bytea,
        DT_TIMESTAMP => DataType::Timestamp,
        DT_TIMESTAMPTZ => DataType::TimestampTz,
        DT_DATE => DataType::Date,
        DT_TIME => DataType::Time,
        DT_INTERVAL => DataType::Interval,
        DT_UUID => DataType::Uuid,
        DT_JSONB => DataType::Jsonb,
        DT_NULL => DataType::Null,
        other => {
            return Err(DecodeError::InvalidTag {
                tag: other,
                offset: r.pos - 1,
            });
        }
    })
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
        Ok(Self {
            oid,
            relname,
            relnamespace,
            relkind,
            relpages,
            reltuples,
            relfilenode,
            relhasindex,
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
            DataType::Bytea,
            DataType::Date,
            DataType::Time,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::Interval,
            DataType::Uuid,
            DataType::Jsonb,
            DataType::Null,
            DataType::Decimal {
                precision: Some(20),
                scale: Some(4),
            },
            DataType::Decimal {
                precision: None,
                scale: None,
            },
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
                attnum: (i + 1) as i16,
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
        let dt = DataType::Array(Box::new(DataType::Int32));
        let err = encode_attribute_row(&row, &dt, true).unwrap_err();
        assert!(matches!(err, EncodeError::UnsupportedType(_)));
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
                    attnum: (i + 1) as i16,
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
}
