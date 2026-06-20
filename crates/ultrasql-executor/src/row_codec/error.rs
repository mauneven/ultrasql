//! Error type for the row codec.

use ultrasql_core::DataType;

/// Errors raised by [`RowCodec`].
#[derive(Debug, thiserror::Error)]
pub enum RowCodecError {
    /// Arity mismatch.
    #[error("arity mismatch: schema has {schema}, row has {row}")]
    Arity {
        /// Schema arity.
        schema: usize,
        /// Caller-supplied row arity.
        row: usize,
    },
    /// Type mismatch.
    #[error("type mismatch at column {column}: expected {expected}, got {got}")]
    Type {
        /// Column index.
        column: usize,
        /// Expected schema type.
        expected: DataType,
        /// Runtime type name.
        got: String,
    },
    /// A character value exceeds its declared length.
    #[error("{detail}")]
    StringDataRightTruncation {
        /// Column index.
        column: usize,
        /// Expected schema type.
        ty: DataType,
        /// User-facing error detail.
        detail: String,
    },
    /// A numeric value exceeds declared precision.
    #[error("{detail}")]
    NumericFieldOverflow {
        /// Column index.
        column: usize,
        /// Expected schema type.
        ty: DataType,
        /// User-facing error detail.
        detail: String,
    },
    /// Truncated payload.
    #[error("payload truncated: needed {needed}, have {have}")]
    Truncated {
        /// Required byte count.
        needed: usize,
        /// Actual byte count.
        have: usize,
    },
    /// A length prefix does not fit the host address space.
    #[error("payload length prefix does not fit usize: {len}")]
    LengthOverflow {
        /// The raw little-endian `u32` length prefix.
        len: u32,
    },
    /// A decode builder violated a finish-time invariant.
    #[error("row builder invariant violation: {0}")]
    BuilderInvariant(&'static str),
    /// Batch construction failed after decoding builders.
    #[error(transparent)]
    Batch(#[from] ultrasql_vec::BatchError),
    /// Unsupported type.
    #[error("unsupported type at column {column}: {ty}")]
    UnsupportedType {
        /// Column index.
        column: usize,
        /// Unsupported `DataType`.
        ty: DataType,
    },
    /// Invalid UTF-8 in a Text column.
    #[error("invalid utf8 at column {1}: {0}")]
    InvalidUtf8(#[source] std::string::FromUtf8Error, &'static str),
    /// Invalid UTF-8 in a borrowed Text column payload.
    #[error("invalid utf8 at column {1}: {0}")]
    InvalidUtf8Slice(#[source] std::str::Utf8Error, &'static str),
}
