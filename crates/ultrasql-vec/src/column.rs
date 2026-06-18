//! Typed column buffers.
//!
//! Each variant holds an aligned data buffer plus an optional null
//! bitmap. Where a bitmap is `None`, the column is non-nullable; the
//! validity check is elided at the kernel level.

use std::fmt;

use ultrasql_core::DataType;

use crate::bitmap::Bitmap;
use crate::dict::DictionaryColumn;

/// Errors specific to column construction.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ColumnError {
    /// The supplied null bitmap's length disagrees with the data
    /// length.
    #[error("nulls bitmap length {bitmap} does not match column length {column}")]
    LengthMismatch {
        /// Bitmap length in bits.
        bitmap: usize,
        /// Column length in rows.
        column: usize,
    },
    /// UTF-8 offsets are empty, non-monotonic, or do not cover the
    /// values buffer exactly.
    #[error("invalid UTF-8 offsets")]
    InvalidUtf8Offsets,
    /// UTF-8 value bytes fail string validation.
    #[error("invalid UTF-8 value bytes")]
    InvalidUtf8,
    /// UTF-8 values exceeded the 32-bit offset capacity.
    #[error("UTF-8 values buffer length {len} exceeds u32 offset capacity")]
    Utf8ValuesTooLarge {
        /// Value-buffer length that did not fit in `u32`.
        len: usize,
    },
    /// Row count exceeded the host's addressable capacity.
    #[error("column row count exceeds usize capacity")]
    RowCountTooLarge,
}

/// A column of one of UltraSQL's primitive types.
#[derive(Clone, PartialEq)]
pub enum Column {
    /// 32-bit signed integers.
    Int32(NumericColumn<i32>),
    /// 64-bit signed integers.
    Int64(NumericColumn<i64>),
    /// 32-bit floats.
    Float32(NumericColumn<f32>),
    /// 64-bit floats.
    Float64(NumericColumn<f64>),
    /// Booleans, packed one-per-byte for now. SIMD-friendlier when
    /// the workload is densely populated.
    Bool(BoolColumn),
    /// UTF-8 strings, length-prefixed offsets layout.
    Utf8(StringColumn),
    /// UTF-8 strings, dictionary-encoded for low-cardinality data.
    DictionaryUtf8(DictionaryColumn),
}

impl Column {
    /// Row count.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Int32(c) => c.len(),
            Self::Int64(c) => c.len(),
            Self::Float32(c) => c.len(),
            Self::Float64(c) => c.len(),
            Self::Bool(c) => c.len(),
            Self::Utf8(c) => c.len(),
            Self::DictionaryUtf8(c) => c.len(),
        }
    }

    /// Whether the column has zero rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Logical [`DataType`].
    #[must_use]
    pub const fn data_type(&self) -> DataType {
        match self {
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Bool(_) => DataType::Bool,
            Self::Utf8(_) | Self::DictionaryUtf8(_) => DataType::Text { max_len: None },
        }
    }

    /// `true` iff this column may contain nulls.
    #[must_use]
    pub const fn is_nullable(&self) -> bool {
        match self {
            Self::Int32(c) => c.nulls.is_some(),
            Self::Int64(c) => c.nulls.is_some(),
            Self::Float32(c) => c.nulls.is_some(),
            Self::Float64(c) => c.nulls.is_some(),
            Self::Bool(c) => c.nulls.is_some(),
            Self::Utf8(c) => c.nulls.is_some(),
            Self::DictionaryUtf8(c) => c.codes.nulls().is_some(),
        }
    }

    /// Borrow the text value at `row` for either raw or dictionary
    /// UTF-8 columns. Returns `None` for non-text columns and SQL NULLs.
    #[must_use]
    pub fn text_value(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(c) => {
                if row >= c.len() || bitmap_row_is_null(c.nulls(), row) {
                    None
                } else {
                    c.try_value(row)
                }
            }
            Self::DictionaryUtf8(c) => {
                if row >= c.len() || bitmap_row_is_null(c.codes.nulls(), row) {
                    None
                } else {
                    c.try_decode_at(row)
                }
            }
            _ => None,
        }
    }
}

impl fmt::Debug for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Column")
            .field("type", &self.data_type())
            .field("len", &self.len())
            .field("nullable", &self.is_nullable())
            .finish()
    }
}

/// A numeric column.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NumericColumn<T> {
    data: Vec<T>,
    nulls: Option<Bitmap>,
}

impl<T> NumericColumn<T> {
    /// Build a non-nullable numeric column.
    #[must_use]
    pub const fn from_data(data: Vec<T>) -> Self {
        Self { data, nulls: None }
    }

    /// Build a nullable numeric column.
    ///
    /// `nulls.len()` must equal `data.len()`. A 1 bit means "the row
    /// is *valid* / non-null"; a 0 bit means "the row is null."
    /// (Same convention as Apache Arrow.)
    pub fn with_nulls(data: Vec<T>, nulls: Bitmap) -> Result<Self, ColumnError> {
        if nulls.len() != data.len() {
            return Err(ColumnError::LengthMismatch {
                bitmap: nulls.len(),
                column: data.len(),
            });
        }
        Ok(Self {
            data,
            nulls: Some(nulls),
        })
    }

    /// Borrow the underlying data slice.
    #[must_use]
    pub fn data(&self) -> &[T] {
        &self.data
    }

    /// Borrow the data slice mutably.
    pub fn data_mut(&mut self) -> &mut [T] {
        &mut self.data
    }

    /// Borrow the optional null bitmap.
    #[must_use]
    pub const fn nulls(&self) -> Option<&Bitmap> {
        self.nulls.as_ref()
    }

    /// Consume the column and return its owned buffers.
    ///
    /// Bridge crates use this to hand the value buffer to Arrow
    /// without copying.
    #[must_use]
    pub fn into_parts(self) -> (Vec<T>, Option<Bitmap>) {
        (self.data, self.nulls)
    }

    /// Number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the column has zero rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// Boolean column. Stored one-byte-per-bit for hot-path access. A
/// future optimization compacts into a `Bitmap` once the executor
/// gains a `BoolBitmapColumn` variant for filter-heavy workloads.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BoolColumn {
    data: Vec<u8>,
    nulls: Option<Bitmap>,
}

impl BoolColumn {
    /// Build a non-nullable boolean column.
    #[must_use]
    pub fn from_data(data: Vec<bool>) -> Self {
        Self {
            data: data.into_iter().map(u8::from).collect(),
            nulls: None,
        }
    }

    /// Build a nullable boolean column.
    ///
    /// `nulls.len()` must equal `data.len()`. Same convention as
    /// [`NumericColumn::with_nulls`]: 1 = valid, 0 = null.
    pub fn with_nulls(data: Vec<bool>, nulls: Bitmap) -> Result<Self, ColumnError> {
        if nulls.len() != data.len() {
            return Err(ColumnError::LengthMismatch {
                bitmap: nulls.len(),
                column: data.len(),
            });
        }
        Ok(Self {
            data: data.into_iter().map(u8::from).collect(),
            nulls: Some(nulls),
        })
    }

    /// Borrow the underlying bytes (1 = true, 0 = false).
    #[must_use]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the column has zero rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Borrow the optional null bitmap.
    #[must_use]
    pub const fn nulls(&self) -> Option<&Bitmap> {
        self.nulls.as_ref()
    }

    /// Consume the column and return the one-byte-per-value buffer.
    #[must_use]
    pub fn into_parts(self) -> (Vec<u8>, Option<Bitmap>) {
        (self.data, self.nulls)
    }

    /// Read by index.
    #[must_use]
    pub fn value(&self, i: usize) -> bool {
        self.data[i] != 0
    }
}

/// UTF-8 string column.
///
/// Storage is Arrow-style: a contiguous `values: Vec<u8>` buffer plus
/// `offsets: Vec<u32>` where row `i` is `&values[offsets[i]..offsets[i+1]]`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StringColumn {
    offsets: Vec<u32>,
    values: Vec<u8>,
    nulls: Option<Bitmap>,
}

impl StringColumn {
    /// Build a non-nullable UTF-8 string column.
    ///
    /// Infallible convenience wrapper over [`Self::try_from_data`]. If the
    /// combined values overflow the 32-bit offset budget
    /// ([`ColumnError::Utf8ValuesTooLarge`]) this returns an **empty** column
    /// rather than surfacing the error. Callers that ingest inputs which may
    /// exceed `u32::MAX` total bytes and need to distinguish that case must use
    /// [`Self::try_from_data`] instead.
    #[must_use]
    pub fn from_data<I: IntoIterator<Item = String>>(rows: I) -> Self {
        match Self::try_from_data(rows) {
            Ok(column) => column,
            Err(_) => Self::empty(),
        }
    }

    /// Try to build a non-nullable UTF-8 string column.
    ///
    /// Returns [`ColumnError::Utf8ValuesTooLarge`] if the contiguous values
    /// buffer cannot be addressed by 32-bit offsets.
    pub fn try_from_data<I: IntoIterator<Item = String>>(rows: I) -> Result<Self, ColumnError> {
        let (offsets, values, _) = build_string_buffers(rows)?;
        Ok(Self {
            offsets,
            values,
            nulls: None,
        })
    }

    /// Build a nullable UTF-8 string column.
    ///
    /// `nulls.len()` must equal the number of rows in `data`. Same
    /// convention as [`NumericColumn::with_nulls`]: 1 = valid, 0 = null.
    pub fn with_nulls<I>(rows: I, nulls: Bitmap) -> Result<Self, ColumnError>
    where
        I: IntoIterator<Item = String>,
    {
        let (offsets, values, row_count) = build_string_buffers(rows)?;
        if nulls.len() != row_count {
            return Err(ColumnError::LengthMismatch {
                bitmap: nulls.len(),
                column: row_count,
            });
        }
        Ok(Self {
            offsets,
            values,
            nulls: Some(nulls),
        })
    }

    /// Build a UTF-8 column from Arrow-style buffers.
    ///
    /// `offsets` must contain at least one value, start at zero, be
    /// monotonically increasing, and end exactly at `values.len()`.
    /// `nulls`, when present, must have `offsets.len() - 1` bits.
    pub fn from_parts(
        offsets: Vec<u32>,
        values: Vec<u8>,
        nulls: Option<Bitmap>,
    ) -> Result<Self, ColumnError> {
        if offsets.is_empty()
            || offsets.first().copied() != Some(0)
            || offsets.windows(2).any(|window| window[0] > window[1])
            || offsets
                .last()
                .copied()
                .and_then(|v| usize::try_from(v).ok())
                != Some(values.len())
        {
            return Err(ColumnError::InvalidUtf8Offsets);
        }
        for window in offsets.windows(2) {
            let start = usize::try_from(window[0]).map_err(|_| ColumnError::InvalidUtf8Offsets)?;
            let end = usize::try_from(window[1]).map_err(|_| ColumnError::InvalidUtf8Offsets)?;
            let value = values
                .get(start..end)
                .ok_or(ColumnError::InvalidUtf8Offsets)?;
            std::str::from_utf8(value).map_err(|_| ColumnError::InvalidUtf8)?;
        }
        let row_count = offsets
            .len()
            .checked_sub(1)
            .ok_or(ColumnError::InvalidUtf8Offsets)?;
        if let Some(nulls) = &nulls
            && nulls.len() != row_count
        {
            return Err(ColumnError::LengthMismatch {
                bitmap: nulls.len(),
                column: row_count,
            });
        }
        Ok(Self {
            offsets,
            values,
            nulls,
        })
    }

    /// Borrow the offsets slice.
    #[must_use]
    pub fn offsets(&self) -> &[u32] {
        &self.offsets
    }

    /// Borrow the underlying value bytes.
    #[must_use]
    pub fn values(&self) -> &[u8] {
        &self.values
    }

    /// Borrow the optional null bitmap.
    #[must_use]
    pub const fn nulls(&self) -> Option<&Bitmap> {
        self.nulls.as_ref()
    }

    /// Consume the column and return its owned buffers.
    ///
    /// The returned buffers use the same layout as Arrow UTF-8 except
    /// offsets are `u32`; Arrow export converts them to signed `i32`
    /// while keeping the values buffer zero-copy.
    #[must_use]
    pub fn into_parts(self) -> (Vec<u32>, Vec<u8>, Option<Bitmap>) {
        (self.offsets, self.values, self.nulls)
    }

    /// Row count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Whether the column has zero rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Checked borrowed string at row `i`.
    ///
    /// Returns `None` if `i` is out of bounds or if internal buffers do not
    /// satisfy the UTF-8/offset invariants.
    #[must_use]
    pub fn try_value(&self, i: usize) -> Option<&str> {
        let start = usize::try_from(*self.offsets.get(i)?).ok()?;
        let end = usize::try_from(*self.offsets.get(i.checked_add(1)?)?).ok()?;
        if start > end || end > self.values.len() {
            return None;
        }
        std::str::from_utf8(self.values.get(start..end)?).ok()
    }

    /// Borrowed string at row `i`.
    ///
    /// Prefer [`Self::try_value`] for externally supplied row indexes. This
    /// convenience accessor fails closed to the empty string if invariants are
    /// violated.
    #[must_use]
    pub fn value(&self, i: usize) -> &str {
        self.try_value(i).unwrap_or("")
    }

    fn empty() -> Self {
        Self {
            offsets: vec![0],
            values: Vec::new(),
            nulls: None,
        }
    }
}

fn build_string_buffers<I: IntoIterator<Item = String>>(
    rows: I,
) -> Result<(Vec<u32>, Vec<u8>, usize), ColumnError> {
    let mut offsets: Vec<u32> = vec![0];
    let mut values: Vec<u8> = Vec::new();
    let mut row_count = 0usize;
    for s in rows {
        values.extend_from_slice(s.as_bytes());
        let offset = u32::try_from(values.len())
            .map_err(|_| ColumnError::Utf8ValuesTooLarge { len: values.len() })?;
        offsets.push(offset);
        row_count = row_count
            .checked_add(1)
            .ok_or(ColumnError::RowCountTooLarge)?;
    }
    Ok((offsets, values, row_count))
}

fn bitmap_row_is_null(nulls: Option<&Bitmap>, row: usize) -> bool {
    match nulls {
        Some(bitmap) if row < bitmap.len() => !bitmap.get(row),
        Some(_) => true,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_column_basic() {
        let c = NumericColumn::from_data(vec![1_i32, 2, 3, 4]);
        assert_eq!(c.len(), 4);
        assert!(!c.is_empty());
        assert_eq!(c.data(), &[1, 2, 3, 4]);
        assert!(c.nulls().is_none());
    }

    #[test]
    fn numeric_column_with_nulls() {
        let data = vec![1_i32, 2, 3, 4];
        let mut nulls = Bitmap::new(4, true);
        nulls.set(1, false);
        let c = NumericColumn::with_nulls(data, nulls).unwrap();
        assert!(c.nulls().unwrap().get(0));
        assert!(!c.nulls().unwrap().get(1));
    }

    #[test]
    fn nulls_length_mismatch_rejected() {
        let data = vec![1_i32, 2, 3, 4];
        let nulls = Bitmap::new(5, true);
        assert!(NumericColumn::with_nulls(data, nulls).is_err());
    }

    #[test]
    fn column_data_type_dispatch() {
        let c = Column::Int32(NumericColumn::from_data(vec![1, 2]));
        assert_eq!(c.data_type(), DataType::Int32);
        assert_eq!(c.len(), 2);
        assert!(!c.is_nullable());
    }

    #[test]
    fn bool_column_basic() {
        let c = BoolColumn::from_data(vec![true, false, true]);
        assert_eq!(c.len(), 3);
        assert!(c.value(0));
        assert!(!c.value(1));
        assert!(c.value(2));
    }

    #[test]
    fn string_column_round_trip() {
        let c = StringColumn::from_data(vec![
            "alpha".to_string(),
            "beta".to_string(),
            "gamma".to_string(),
        ]);
        assert_eq!(c.len(), 3);
        assert_eq!(c.value(0), "alpha");
        assert_eq!(c.value(1), "beta");
        assert_eq!(c.value(2), "gamma");
    }

    #[test]
    fn string_column_empty() {
        let c = StringColumn::from_data(Vec::<String>::new());
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());
    }

    #[test]
    fn string_column_from_parts_rejects_out_of_bounds_offsets() {
        let err = StringColumn::from_parts(vec![0, 8], b"abc".to_vec(), None).unwrap_err();
        assert_eq!(err, ColumnError::InvalidUtf8Offsets);
    }

    #[test]
    fn column_text_value_utf8_out_of_bounds_returns_none() {
        let c = Column::Utf8(StringColumn::from_data(vec!["alpha".to_string()]));
        assert_eq!(c.text_value(1), None);
    }

    #[test]
    fn column_text_value_dictionary_out_of_bounds_returns_none() {
        let dict = DictionaryColumn::from_strings([Some("alpha")]).unwrap();
        let c = Column::DictionaryUtf8(dict);
        assert_eq!(c.text_value(1), None);
    }
}
