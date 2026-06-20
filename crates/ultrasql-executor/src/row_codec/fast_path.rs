//! Specialised fixed-width fast-path decode loops.

use super::*;

impl RowCodec {
    /// Dispatch a fast-path decode for the all-non-null branch of the
    /// detected [`DecodeShape`]. Returns `Some(())` on a hit,
    /// `None` to fall through to the generic path (NULL present,
    /// truncated tuple, or schema not covered by a fast path).
    ///
    /// The fast path:
    ///
    /// - Reads the leading null-bitmap byte and bails to the generic
    ///   path if any of the schema's NULL bits are set.
    /// - Confirms the payload has the exact fixed width for the
    ///   shape (single bounds check, not one per column).
    /// - Inline-decodes each fixed-width column via
    ///   `i32::from_le_bytes` / `i64::from_le_bytes` on stack-resident
    ///   4- / 8-byte arrays — no `try_into` round trips, no
    ///   `&mut ColumnBuilder` match dispatch.
    /// - Marks every position valid via `nulls.push_valid()`.
    #[inline]
    #[allow(clippy::too_many_lines)]
    pub(super) fn try_decode_fast_path(
        shape: DecodeShape,
        bytes: &[u8],
        builders: &mut [ColumnBuilder],
    ) -> Result<Option<()>, RowCodecError> {
        // Common preamble: 1 bitmap byte for any schema with ≤ 8
        // columns. All specialised shapes are 1-, 2-, or 3-column,
        // so the bitmap byte count is always 1.
        if bytes.is_empty() {
            return Ok(None);
        }
        let bitmap0 = bytes[0];
        if bitmap0 != 0 {
            // Any column is NULL — defer to the generic path which
            // emits `push_null()` correctly.
            return Ok(None);
        }
        match shape {
            DecodeShape::Generic => Ok(None),
            DecodeShape::I32x1 => {
                if bytes.len() < 1 + 4 {
                    return Err(RowCodecError::Truncated {
                        needed: 1 + 4,
                        have: bytes.len(),
                    });
                }
                let v0 = i32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                if let ColumnBuilder::Int32 { data, nulls } = &mut builders[0] {
                    data.push(v0);
                    nulls.push_valid();
                    Ok(Some(()))
                } else {
                    // Builder mismatch: caller built the wrong
                    // builder type for this codec. Defer to generic
                    // path which surfaces a clearer error.
                    Ok(None)
                }
            }
            DecodeShape::I32x2 => {
                if bytes.len() < 1 + 8 {
                    return Err(RowCodecError::Truncated {
                        needed: 1 + 8,
                        have: bytes.len(),
                    });
                }
                let v0 = i32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                let v1 = i32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
                let (head, tail) = builders.split_at_mut(1);
                if let (
                    ColumnBuilder::Int32 {
                        data: d0,
                        nulls: n0,
                    },
                    ColumnBuilder::Int32 {
                        data: d1,
                        nulls: n1,
                    },
                ) = (&mut head[0], &mut tail[0])
                {
                    d0.push(v0);
                    n0.push_valid();
                    d1.push(v1);
                    n1.push_valid();
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            }
            DecodeShape::I32x3 => {
                if bytes.len() < 1 + 12 {
                    return Err(RowCodecError::Truncated {
                        needed: 1 + 12,
                        have: bytes.len(),
                    });
                }
                let v0 = i32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                let v1 = i32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
                let v2 = i32::from_le_bytes([bytes[9], bytes[10], bytes[11], bytes[12]]);
                let (head, rest) = builders.split_at_mut(1);
                let (mid, tail) = rest.split_at_mut(1);
                if let (
                    ColumnBuilder::Int32 {
                        data: d0,
                        nulls: n0,
                    },
                    ColumnBuilder::Int32 {
                        data: d1,
                        nulls: n1,
                    },
                    ColumnBuilder::Int32 {
                        data: d2,
                        nulls: n2,
                    },
                ) = (&mut head[0], &mut mid[0], &mut tail[0])
                {
                    d0.push(v0);
                    n0.push_valid();
                    d1.push(v1);
                    n1.push_valid();
                    d2.push(v2);
                    n2.push_valid();
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            }
            DecodeShape::I64x1 => {
                if bytes.len() < 1 + 8 {
                    return Err(RowCodecError::Truncated {
                        needed: 1 + 8,
                        have: bytes.len(),
                    });
                }
                let mut cursor = 1;
                let raw = read_fixed::<8>(bytes, &mut cursor)?;
                let v0 = i64::from_le_bytes(raw);
                if let ColumnBuilder::Int64 { data, nulls } = &mut builders[0] {
                    data.push(v0);
                    nulls.push_valid();
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            }
            DecodeShape::I64x2 => {
                if bytes.len() < 1 + 16 {
                    return Err(RowCodecError::Truncated {
                        needed: 1 + 16,
                        have: bytes.len(),
                    });
                }
                let mut cursor = 1;
                let r0 = read_fixed::<8>(bytes, &mut cursor)?;
                let r1 = read_fixed::<8>(bytes, &mut cursor)?;
                let v0 = i64::from_le_bytes(r0);
                let v1 = i64::from_le_bytes(r1);
                let (head, tail) = builders.split_at_mut(1);
                if let (
                    ColumnBuilder::Int64 {
                        data: d0,
                        nulls: n0,
                    },
                    ColumnBuilder::Int64 {
                        data: d1,
                        nulls: n1,
                    },
                ) = (&mut head[0], &mut tail[0])
                {
                    d0.push(v0);
                    n0.push_valid();
                    d1.push(v1);
                    n1.push_valid();
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            }
        }
    }
}
