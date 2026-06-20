//! Sequence operation payload codecs.

use ultrasql_core::RelationId;
use ultrasql_core::endian::{read_u32_le, write_u32_le};

use super::{
    MAX_VARIABLE_PAYLOAD_BYTES, PayloadError, checked_offset, decode_bool_byte, read_i64_advance,
    read_u32_advance, require_exact_len, write_i64_advance, write_u32_advance,
};

/// Kind of sequence operation recorded in a [`SequenceOpPayload`].
///
/// Each WAL record carries the complete sequence state after the operation, so
/// redo is idempotent and can restore the state without replaying arithmetic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum SequenceOpKind {
    /// `CREATE SEQUENCE` installed the initial state.
    Create = 1,
    /// `nextval` advanced the sequence.
    Advance = 2,
    /// `setval` replaced `last_value` / `is_called`.
    Set = 3,
    /// `ALTER SEQUENCE` replaced options and maybe restarted the sequence.
    Alter = 4,
    /// `DROP SEQUENCE` removed the sequence. State fields contain the last
    /// known state before removal.
    Drop = 5,
}

impl SequenceOpKind {
    /// Parse a `SequenceOpKind` from its on-disk byte representation.
    pub const fn from_u8(v: u8) -> Result<Self, PayloadError> {
        match v {
            1 => Ok(Self::Create),
            2 => Ok(Self::Advance),
            3 => Ok(Self::Set),
            4 => Ok(Self::Alter),
            5 => Ok(Self::Drop),
            _ => Err(PayloadError::Malformed("sequence_op kind unknown")),
        }
    }
}

impl From<SequenceOpKind> for u8 {
    fn from(kind: SequenceOpKind) -> Self {
        match kind {
            SequenceOpKind::Create => 1,
            SequenceOpKind::Advance => 2,
            SequenceOpKind::Set => 3,
            SequenceOpKind::Alter => 4,
            SequenceOpKind::Drop => 5,
        }
    }
}

/// Payload for a `RecordType::SequenceOp` WAL record.
///
/// Wire layout (little-endian, no implicit padding):
/// ```text
///  0   1   op (u8) — SequenceOpKind discriminant
///  1   3   reserved (zero)
///  4   4   seqrelid (RelationId/OID, u32; may be INVALID during bootstrap)
///  8   4   name_len (u32)
/// 12  ..   UTF-8 sequence name bytes
///  +   8   start_value (i64)
///  +   8   last_value (i64)
///  +   8   min_value (i64)
///  +   8   max_value (i64)
///  +   8   increment (i64)
///  +   4   cache_size (u32)
///  +   1   is_called (bool as u8)
///  +   1   cycle (bool as u8)
///  +   2   reserved (zero)
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceOpPayload {
    /// Operation that produced this state.
    pub op: SequenceOpKind,
    /// Sequence relation OID when available.
    pub seqrelid: RelationId,
    /// Folded sequence name.
    pub name: String,
    /// Configured restart value.
    pub start_value: i64,
    /// Last value returned, or next value when `is_called` is false.
    pub last_value: i64,
    /// Lower bound.
    pub min_value: i64,
    /// Upper bound.
    pub max_value: i64,
    /// Step.
    pub increment: i64,
    /// Configured cache size.
    pub cache_size: u32,
    /// PostgreSQL `is_called` state.
    pub is_called: bool,
    /// Whether CYCLE is enabled.
    pub cycle: bool,
}

impl SequenceOpPayload {
    /// Encode this payload into a freshly allocated byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, PayloadError> {
        const FIXED_PREFIX: usize = 12;
        const FIXED_SUFFIX: usize = 48;
        let name_bytes = self.name.as_bytes();
        let name_len = u32::try_from(name_bytes.len())
            .map_err(|_| PayloadError::Malformed("sequence_op name_len overflow"))?;
        if name_bytes.len() > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "sequence_op name_len exceeds ceiling",
            ));
        }
        let name_end = checked_offset(
            FIXED_PREFIX,
            name_bytes.len(),
            "sequence_op length overflow",
        )?;
        let total = checked_offset(name_end, FIXED_SUFFIX, "sequence_op length overflow")?;
        let mut out = vec![0_u8; total];
        out[0] = u8::from(self.op);
        write_u32_le(&mut out[4..8], self.seqrelid.oid().raw());
        write_u32_le(&mut out[8..12], name_len);
        out[FIXED_PREFIX..name_end].copy_from_slice(name_bytes);
        let mut off = name_end;
        write_i64_advance(
            &mut out,
            &mut off,
            self.start_value,
            "sequence_op length overflow",
        )?;
        write_i64_advance(
            &mut out,
            &mut off,
            self.last_value,
            "sequence_op length overflow",
        )?;
        write_i64_advance(
            &mut out,
            &mut off,
            self.min_value,
            "sequence_op length overflow",
        )?;
        write_i64_advance(
            &mut out,
            &mut off,
            self.max_value,
            "sequence_op length overflow",
        )?;
        write_i64_advance(
            &mut out,
            &mut off,
            self.increment,
            "sequence_op length overflow",
        )?;
        write_u32_advance(
            &mut out,
            &mut off,
            self.cache_size,
            "sequence_op length overflow",
        )?;
        let cycle_off = checked_offset(off, 1, "sequence_op length overflow")?;
        out[off] = u8::from(self.is_called);
        out[cycle_off] = u8::from(self.cycle);
        Ok(out)
    }

    /// Decode a `SequenceOpPayload` from a byte slice.
    pub fn decode(bytes: &[u8]) -> Result<Self, PayloadError> {
        const FIXED_PREFIX: usize = 12;
        const FIXED_SUFFIX: usize = 48;
        if bytes.len() < FIXED_PREFIX {
            return Err(PayloadError::Truncated {
                needed: FIXED_PREFIX,
                have: bytes.len(),
            });
        }
        let op = SequenceOpKind::from_u8(bytes[0])?;
        if bytes[1] != 0 || bytes[2] != 0 || bytes[3] != 0 {
            return Err(PayloadError::Malformed(
                "sequence_op reserved prefix bytes must be zero",
            ));
        }
        let seqrelid = RelationId::new(
            read_u32_le(&bytes[4..8]).map_err(|_| PayloadError::Malformed("seqrelid"))?,
        );
        let name_len = usize::try_from(
            read_u32_le(&bytes[8..12]).map_err(|_| PayloadError::Malformed("name_len"))?,
        )
        .map_err(|_| PayloadError::Malformed("sequence_op name_len usize overflow"))?;
        if name_len > MAX_VARIABLE_PAYLOAD_BYTES {
            return Err(PayloadError::Malformed(
                "sequence_op name_len exceeds ceiling",
            ));
        }
        let name_end = checked_offset(FIXED_PREFIX, name_len, "sequence_op length overflow")?;
        let needed = checked_offset(name_end, FIXED_SUFFIX, "sequence_op length overflow")?;
        if bytes.len() < needed {
            return Err(PayloadError::Truncated {
                needed,
                have: bytes.len(),
            });
        }
        let name = std::str::from_utf8(&bytes[FIXED_PREFIX..name_end])
            .map_err(|_| PayloadError::Malformed("sequence_op name utf8"))?
            .to_owned();
        let mut off = name_end;
        let start_value = read_i64_advance(
            bytes,
            &mut off,
            "sequence_op start_value",
            "sequence_op length overflow",
        )?;
        let last_value = read_i64_advance(
            bytes,
            &mut off,
            "sequence_op last_value",
            "sequence_op length overflow",
        )?;
        let min_value = read_i64_advance(
            bytes,
            &mut off,
            "sequence_op min_value",
            "sequence_op length overflow",
        )?;
        let max_value = read_i64_advance(
            bytes,
            &mut off,
            "sequence_op max_value",
            "sequence_op length overflow",
        )?;
        let increment = read_i64_advance(
            bytes,
            &mut off,
            "sequence_op increment",
            "sequence_op length overflow",
        )?;
        let cache_size = read_u32_advance(
            bytes,
            &mut off,
            "sequence_op cache_size",
            "sequence_op length overflow",
        )?;
        let cycle_off = checked_offset(off, 1, "sequence_op length overflow")?;
        let reserved_start = checked_offset(off, 2, "sequence_op length overflow")?;
        let reserved_end = checked_offset(off, 4, "sequence_op length overflow")?;
        let is_called = decode_bool_byte(bytes[off], "sequence_op is_called")?;
        let cycle = decode_bool_byte(bytes[cycle_off], "sequence_op cycle")?;
        if bytes[reserved_start..reserved_end]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(PayloadError::Malformed(
                "sequence_op reserved suffix bytes must be zero",
            ));
        }
        require_exact_len(bytes, needed)?;
        Ok(Self {
            op,
            seqrelid,
            name,
            start_value,
            last_value,
            min_value,
            max_value,
            increment,
            cache_size,
            is_called,
            cycle,
        })
    }
}
