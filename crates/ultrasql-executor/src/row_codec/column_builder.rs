//! Per-column streaming accumulators and their finalisation.

use super::{RowCodecError, u32_payload_len_to_usize};
use ultrasql_core::DataType;
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn};
use ultrasql_vec::{DictionaryEncodingPolicy, StringEncoding, encode_strings_auto};

// ---------------------------------------------------------------------------
// Streaming column builders
// ---------------------------------------------------------------------------

/// Lazy packed null tracker that grows by one bit per row at O(1)
/// amortised cost. The [`Bitmap`] is materialised lazily on first
/// observed null and finalised at `finish` time.
#[derive(Debug, Default)]
pub(crate) struct NullTracker {
    words: Vec<u64>,
    len: usize,
    active: bool,
}

impl NullTracker {
    /// Mark a previously-pushed row as valid.
    #[inline]
    pub(crate) fn push_valid(&mut self) {
        if !self.active {
            return;
        }
        let bit_idx = self.len;
        let word_idx = bit_idx / 64;
        if word_idx >= self.words.len() {
            self.words.push(0);
        }
        self.words[word_idx] |= 1_u64 << (bit_idx % 64);
        self.len += 1;
    }

    /// Mark a previously-pushed row as null, activating the tracker
    /// if necessary.
    #[inline]
    fn push_null(&mut self, prior_rows: usize) {
        if !self.active {
            self.activate(prior_rows);
        }
        let bit_idx = self.len;
        let word_idx = bit_idx / 64;
        if word_idx >= self.words.len() {
            self.words.push(0);
        }
        self.len += 1;
    }

    #[cold]
    fn activate(&mut self, prior_rows: usize) {
        debug_assert!(!self.active);
        self.active = true;
        let words = prior_rows.div_ceil(64);
        self.words = vec![u64::MAX; words];
        if prior_rows % 64 != 0 {
            let mask = (1_u64 << (prior_rows % 64)) - 1;
            if let Some(last) = self.words.last_mut() {
                *last &= mask;
            }
        }
        self.len = prior_rows;
    }

    fn finish(self) -> Option<Bitmap> {
        if self.active {
            Some(Bitmap::from_words(self.words, self.len))
        } else {
            None
        }
    }
}

/// Per-column accumulator owning a typed `Vec<T>` plus a null tracker.
#[derive(Debug)]
pub(crate) enum ColumnBuilder {
    Bool {
        data: Vec<u8>,
        nulls: NullTracker,
    },
    Int16 {
        data: Vec<i32>,
        nulls: NullTracker,
    },
    Int32 {
        data: Vec<i32>,
        nulls: NullTracker,
    },
    Int64 {
        data: Vec<i64>,
        nulls: NullTracker,
    },
    Float32 {
        data: Vec<f32>,
        nulls: NullTracker,
    },
    Float64 {
        data: Vec<f64>,
        nulls: NullTracker,
    },
    Utf8 {
        offsets: Vec<u32>,
        values: Vec<u8>,
        nulls: NullTracker,
    },
}

impl ColumnBuilder {
    pub(super) fn new(
        ty: &DataType,
        capacity: usize,
        col_idx: usize,
    ) -> Result<Self, RowCodecError> {
        Ok(match ty {
            DataType::Bool => Self::Bool {
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Int16 => Self::Int16 {
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Int32 => Self::Int32 {
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Int64 => Self::Int64 {
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Float32 => Self::Float32 {
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Float64 => Self::Float64 {
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Date => Self::Int32 {
                // `Date` storage shares the `Int32` builder: days
                // since 2000-01-01 are i32 by definition. Schema
                // tags carry the date semantics so downstream
                // operators that care about date comparisons (range
                // filters, sort) still see a `DataType::Date` field.
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Decimal { .. } => Self::Utf8 {
                // `Decimal` storage materialises as decimal text so the
                // full i128-backed mantissa (~38 digits) round-trips
                // losslessly through the batch column; the schema field
                // carries the semantic tag and scale. (A fixed-width
                // `Int64` batch column would silently truncate values
                // beyond i64.)
                offsets: {
                    let mut o = Vec::with_capacity(capacity + 1);
                    o.push(0);
                    o
                },
                values: Vec::with_capacity(capacity.saturating_mul(16)),
                nulls: NullTracker::default(),
            },
            DataType::Money
            | DataType::Oid
            | DataType::RegClass
            | DataType::RegType
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Time
            | DataType::TimeTz => Self::Int64 {
                // `Timestamp` / `Time` / `Money` storage shares the
                // `Int64` builder; the schema field carries the
                // semantic tag.
                data: Vec::with_capacity(capacity),
                nulls: NullTracker::default(),
            },
            DataType::Text { .. }
            | DataType::Enum { .. }
            | DataType::Composite { .. }
            | DataType::Char { .. }
            | DataType::Bit { .. }
            | DataType::VarBit { .. }
            | DataType::Inet
            | DataType::Cidr
            | DataType::MacAddr
            | DataType::MacAddr8
            | DataType::Json
            | DataType::Jsonb
            | DataType::Xml
            | DataType::Vector { .. }
            | DataType::HalfVec { .. }
            | DataType::SparseVec { .. }
            | DataType::BitVec { .. }
            | DataType::Range(_)
            | DataType::Geometry(_)
            | DataType::Array(_)
            | DataType::Uuid
            | DataType::Bytea
            | DataType::Interval
            | DataType::PgLsn => Self::Utf8 {
                offsets: {
                    let mut o = Vec::with_capacity(capacity + 1);
                    o.push(0);
                    o
                },
                values: Vec::with_capacity(capacity.saturating_mul(16)),
                nulls: NullTracker::default(),
            },
            other => {
                return Err(RowCodecError::UnsupportedType {
                    column: col_idx,
                    ty: other.clone(),
                });
            }
        })
    }

    pub(super) fn push_null(&mut self) {
        match self {
            Self::Bool { data, nulls } => {
                let prior = data.len();
                nulls.push_null(prior);
                data.push(0);
            }
            Self::Int16 { data, nulls } | Self::Int32 { data, nulls } => {
                let prior = data.len();
                nulls.push_null(prior);
                data.push(0);
            }
            Self::Int64 { data, nulls } => {
                let prior = data.len();
                nulls.push_null(prior);
                data.push(0);
            }
            Self::Float32 { data, nulls } => {
                let prior = data.len();
                nulls.push_null(prior);
                data.push(0.0);
            }
            Self::Float64 { data, nulls } => {
                let prior = data.len();
                nulls.push_null(prior);
                data.push(0.0);
            }
            Self::Utf8 {
                offsets,
                values,
                nulls,
            } => {
                let prior = offsets.len().saturating_sub(1);
                nulls.push_null(prior);
                offsets.push(u32::try_from(values.len()).unwrap_or(u32::MAX));
            }
        }
    }

    pub(super) fn push_i32(&mut self, v: i32) {
        match self {
            Self::Int32 { data, nulls } | Self::Int16 { data, nulls } => {
                data.push(v);
                nulls.push_valid();
            }
            _ => unreachable!("push_i32 called on non-Int32/Int16 builder"),
        }
    }
}

pub(crate) fn finish_builders(builders: Vec<ColumnBuilder>) -> Result<Vec<Column>, RowCodecError> {
    let mut out: Vec<Column> = Vec::with_capacity(builders.len());
    for b in builders {
        let col = match b {
            ColumnBuilder::Bool { data, nulls } => {
                let bools: Vec<bool> = data.iter().map(|&b| b != 0).collect();
                match nulls.finish() {
                    Some(bm) => BoolColumn::with_nulls(bools, bm)
                        .map(Column::Bool)
                        .map_err(|_| RowCodecError::BuilderInvariant("bool null bitmap length"))?,
                    None => Column::Bool(BoolColumn::from_data(bools)),
                }
            }
            ColumnBuilder::Int16 { data, nulls } | ColumnBuilder::Int32 { data, nulls } => {
                match nulls.finish() {
                    Some(bm) => NumericColumn::with_nulls(data, bm)
                        .map(Column::Int32)
                        .map_err(|_| RowCodecError::BuilderInvariant("i32 null bitmap length"))?,
                    None => Column::Int32(NumericColumn::from_data(data)),
                }
            }
            ColumnBuilder::Int64 { data, nulls } => match nulls.finish() {
                Some(bm) => NumericColumn::with_nulls(data, bm)
                    .map(Column::Int64)
                    .map_err(|_| RowCodecError::BuilderInvariant("i64 null bitmap length"))?,
                None => Column::Int64(NumericColumn::from_data(data)),
            },
            ColumnBuilder::Float32 { data, nulls } => match nulls.finish() {
                Some(bm) => NumericColumn::with_nulls(data, bm)
                    .map(Column::Float32)
                    .map_err(|_| RowCodecError::BuilderInvariant("f32 null bitmap length"))?,
                None => Column::Float32(NumericColumn::from_data(data)),
            },
            ColumnBuilder::Float64 { data, nulls } => match nulls.finish() {
                Some(bm) => NumericColumn::with_nulls(data, bm)
                    .map(Column::Float64)
                    .map_err(|_| RowCodecError::BuilderInvariant("f64 null bitmap length"))?,
                None => Column::Float64(NumericColumn::from_data(data)),
            },
            ColumnBuilder::Utf8 {
                offsets,
                values,
                nulls,
            } => text_column_from_parts(&offsets, &values, nulls.finish())?,
        };
        out.push(col);
    }
    Ok(out)
}

fn text_column_from_parts(
    offsets: &[u32],
    values: &[u8],
    nulls: Option<Bitmap>,
) -> Result<Column, RowCodecError> {
    let n = offsets.len().saturating_sub(1);
    let mut rows: Vec<Option<String>> = Vec::with_capacity(n);
    for i in 0..n {
        if nulls.as_ref().is_some_and(|bm| !bm.get(i)) {
            rows.push(None);
        } else {
            let start = u32_payload_len_to_usize(offsets[i])?;
            let end = u32_payload_len_to_usize(offsets[i + 1])?;
            if start > end || end > values.len() {
                return Err(RowCodecError::BuilderInvariant("text offset bounds"));
            }
            let s = String::from_utf8(values[start..end].to_vec())
                .map_err(|error| RowCodecError::InvalidUtf8(error, "text builder"))?;
            rows.push(Some(s));
        }
    }
    Ok(
        match encode_strings_auto(
            rows.iter().map(|v| v.as_deref()),
            DictionaryEncodingPolicy::default(),
        ) {
            StringEncoding::Raw(c) => Column::Utf8(c),
            StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
        },
    )
}
