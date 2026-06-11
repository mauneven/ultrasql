//! PostgreSQL `BIT` / `VARBIT` runtime payload.
//!
//! Bits are stored most-significant-bit first inside each byte, matching
//! the existing vector bit packing but with SQL string semantics and type
//! modifiers (`bit(n)` exact length, `varbit(n)` bounded length).

use std::fmt;

use crate::DataType;

/// Packed SQL bit string.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BitString {
    len: u32,
    bytes: Vec<u8>,
}

impl BitString {
    /// Build a bit string from an already packed payload.
    #[must_use]
    pub fn new(len: u32, mut bytes: Vec<u8>) -> Option<Self> {
        let expected = byte_len(len)?;
        if bytes.len() != expected {
            return None;
        }
        mask_unused_bits(len, &mut bytes);
        Some(Self { len, bytes })
    }

    /// Parse text containing only `0` and `1`.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let trimmed = text.trim();
        let len = u32::try_from(trimmed.len()).ok()?;
        let mut bytes = vec![0_u8; byte_len(len)?];
        for (idx, byte) in trimmed.bytes().enumerate() {
            match byte {
                b'0' => {}
                b'1' => set_raw_bit(&mut bytes, idx, true)?,
                _ => return None,
            }
        }
        Self::new(len, bytes)
    }

    /// Create a `bit(width)` value from an integer using PostgreSQL's
    /// rightmost-bit copy and sign-extension rule.
    #[must_use]
    pub fn from_i64(width: u32, value: i64) -> Option<Self> {
        let mut bytes = vec![0_u8; byte_len(width)?];
        let width_usize = usize::try_from(width).ok()?;
        for idx in 0..width_usize {
            let from_right = width_usize.checked_sub(idx)?.checked_sub(1)?;
            let bit = if from_right >= 64 {
                value.is_negative()
            } else {
                let shift = u32::try_from(from_right).ok()?;
                ((value >> shift) & 1) == 1
            };
            if bit {
                set_raw_bit(&mut bytes, idx, true)?;
            }
        }
        Self::new(width, bytes)
    }

    /// Logical bit length.
    #[must_use]
    pub const fn len(&self) -> u32 {
        self.len
    }

    /// Whether this string has no bits.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Packed bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// PostgreSQL text form.
    #[must_use]
    pub fn to_bit_text(&self) -> String {
        let Some(len) = usize::try_from(self.len).ok() else {
            return String::new();
        };
        let mut out = String::with_capacity(len);
        for idx in 0..len {
            out.push(if self.bit(idx).unwrap_or(false) {
                '1'
            } else {
                '0'
            });
        }
        out
    }

    /// Return bit at zero-based index, where index 0 is leftmost bit.
    #[must_use]
    pub fn bit(&self, idx: usize) -> Option<bool> {
        if idx >= usize::try_from(self.len).ok()? {
            return None;
        }
        raw_bit(&self.bytes, idx)
    }

    /// Return new bit string with one bit changed.
    #[must_use]
    pub fn set_bit(&self, idx: usize, bit: bool) -> Option<Self> {
        if idx >= usize::try_from(self.len).ok()? {
            return None;
        }
        let mut bytes = self.bytes.clone();
        set_raw_bit(&mut bytes, idx, bit)?;
        Self::new(self.len, bytes)
    }

    /// Bitwise NOT.
    #[must_use]
    pub fn bit_not(&self) -> Self {
        let mut bytes = self.bytes.iter().map(|byte| !byte).collect::<Vec<_>>();
        mask_unused_bits(self.len, &mut bytes);
        Self {
            len: self.len,
            bytes,
        }
    }

    /// Bitwise AND. Inputs must have same logical length.
    #[must_use]
    pub fn bit_and(&self, other: &Self) -> Option<Self> {
        self.zip_bits(other, |left, right| left & right)
    }

    /// Bitwise OR. Inputs must have same logical length.
    #[must_use]
    pub fn bit_or(&self, other: &Self) -> Option<Self> {
        self.zip_bits(other, |left, right| left | right)
    }

    /// Bitwise XOR. Inputs must have same logical length.
    #[must_use]
    pub fn bit_xor(&self, other: &Self) -> Option<Self> {
        self.zip_bits(other, |left, right| left ^ right)
    }

    /// Shift left by `amount`, preserving length and shifting zeros in.
    #[must_use]
    pub fn shift_left(&self, amount: usize) -> Option<Self> {
        let len = usize::try_from(self.len).ok()?;
        let mut bytes = vec![0_u8; byte_len(self.len)?];
        for idx in 0..len {
            if let Some(source) = idx.checked_add(amount)
                && source < len
                && self.bit(source)?
            {
                set_raw_bit(&mut bytes, idx, true)?;
            }
        }
        Self::new(self.len, bytes)
    }

    /// Shift right by `amount`, preserving length and shifting zeros in.
    #[must_use]
    pub fn shift_right(&self, amount: usize) -> Option<Self> {
        let len = usize::try_from(self.len).ok()?;
        let mut bytes = vec![0_u8; byte_len(self.len)?];
        for idx in 0..len {
            if idx >= amount && self.bit(idx - amount)? {
                set_raw_bit(&mut bytes, idx, true)?;
            }
        }
        Self::new(self.len, bytes)
    }

    /// Concatenate two bit strings.
    #[must_use]
    pub fn concat(&self, other: &Self) -> Option<Self> {
        let new_len = self.len.checked_add(other.len)?;
        let mut bytes = vec![0_u8; byte_len(new_len)?];
        let left_len = usize::try_from(self.len).ok()?;
        for idx in 0..left_len {
            if self.bit(idx)? {
                set_raw_bit(&mut bytes, idx, true)?;
            }
        }
        let right_len = usize::try_from(other.len).ok()?;
        for idx in 0..right_len {
            if other.bit(idx)? {
                set_raw_bit(&mut bytes, left_len.checked_add(idx)?, true)?;
            }
        }
        Self::new(new_len, bytes)
    }

    /// Count set bits.
    #[must_use]
    pub fn bit_count(&self) -> u32 {
        let Some(len) = usize::try_from(self.len).ok() else {
            return 0;
        };
        (0..len)
            .filter(|idx| self.bit(*idx).unwrap_or(false))
            .count()
            .try_into()
            .unwrap_or(u32::MAX)
    }

    /// Bytes needed to hold the logical bit length.
    #[must_use]
    pub fn octet_len(&self) -> u32 {
        self.len.div_ceil(8)
    }

    /// Convert to PostgreSQL binary wire/COPY shape: int32 bit length,
    /// followed by packed bytes.
    #[must_use]
    pub fn to_pg_binary(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.bytes.len());
        let len = i32::try_from(self.len).unwrap_or(i32::MAX);
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&self.bytes);
        out
    }

    /// Decode PostgreSQL binary wire/COPY shape.
    #[must_use]
    pub fn from_pg_binary(bytes: &[u8]) -> Option<Self> {
        let len = i32::from_be_bytes(bytes.get(..4)?.try_into().ok()?);
        if len < 0 {
            return None;
        }
        let len = u32::try_from(len).ok()?;
        let payload = bytes.get(4..)?;
        if payload.len() != byte_len(len)? {
            return None;
        }
        Self::new(len, payload.to_vec())
    }

    /// Coerce to a target SQL bit-string type.
    #[must_use]
    pub fn coerce_to(&self, target: &DataType, explicit_cast: bool) -> Option<Self> {
        match target {
            DataType::Bit {
                len: Some(target_len),
            } => {
                if self.len == *target_len {
                    Some(self.clone())
                } else if explicit_cast {
                    self.resize_fixed(*target_len)
                } else {
                    None
                }
            }
            DataType::Bit { len: None } => Some(self.clone()),
            DataType::VarBit {
                max_len: Some(max_len),
            } => {
                if self.len <= *max_len {
                    Some(self.clone())
                } else if explicit_cast {
                    self.truncate(*max_len)
                } else {
                    None
                }
            }
            DataType::VarBit { max_len: None } => Some(self.clone()),
            _ => None,
        }
    }

    /// True when this value can be stored in target type without explicit
    /// cast semantics.
    #[must_use]
    pub fn matches_type(&self, target: &DataType) -> bool {
        self.coerce_to(target, false).is_some()
    }

    fn resize_fixed(&self, target_len: u32) -> Option<Self> {
        let mut bytes = vec![0_u8; byte_len(target_len)?];
        let copy_len = self.len.min(target_len);
        let copy_len = usize::try_from(copy_len).ok()?;
        for idx in 0..copy_len {
            if self.bit(idx)? {
                set_raw_bit(&mut bytes, idx, true)?;
            }
        }
        Self::new(target_len, bytes)
    }

    fn truncate(&self, target_len: u32) -> Option<Self> {
        self.resize_fixed(target_len)
    }

    fn zip_bits(&self, other: &Self, op: impl Fn(bool, bool) -> bool) -> Option<Self> {
        if self.len != other.len {
            return None;
        }
        let len = usize::try_from(self.len).ok()?;
        let mut bytes = vec![0_u8; byte_len(self.len)?];
        for idx in 0..len {
            if op(self.bit(idx)?, other.bit(idx)?) {
                set_raw_bit(&mut bytes, idx, true)?;
            }
        }
        Self::new(self.len, bytes)
    }
}

impl fmt::Display for BitString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_bit_text())
    }
}

fn byte_len(len: u32) -> Option<usize> {
    usize::try_from(len.div_ceil(8)).ok()
}

fn raw_bit(bytes: &[u8], idx: usize) -> Option<bool> {
    let byte_idx = idx / 8;
    let bit_idx = idx % 8;
    Some(((bytes.get(byte_idx)? >> (7 - bit_idx)) & 1) == 1)
}

fn set_raw_bit(bytes: &mut [u8], idx: usize, bit: bool) -> Option<()> {
    let byte_idx = idx / 8;
    let bit_idx = idx % 8;
    let mask = 1_u8 << (7 - bit_idx);
    let byte = bytes.get_mut(byte_idx)?;
    if bit {
        *byte |= mask;
    } else {
        *byte &= !mask;
    }
    Some(())
}

fn mask_unused_bits(len: u32, bytes: &mut [u8]) {
    let rem = len % 8;
    if rem == 0 || bytes.is_empty() {
        return;
    }
    let keep = u8::MAX << (8 - rem);
    if let Some(last) = bytes.last_mut() {
        *last &= keep;
    }
}

#[cfg(test)]
mod tests {
    use super::BitString;
    use crate::DataType;

    #[test]
    fn bit_string_parse_display_and_ops() {
        let bits = BitString::parse("1010").unwrap();
        assert_eq!(bits.to_string(), "1010");
        assert_eq!(bits.bit_count(), 2);
        assert_eq!(bits.octet_len(), 1);
        assert_eq!(
            bits.bit_and(&BitString::parse("0011").unwrap())
                .unwrap()
                .to_string(),
            "0010"
        );
        assert_eq!(
            bits.bit_or(&BitString::parse("0101").unwrap())
                .unwrap()
                .to_string(),
            "1111"
        );
        assert_eq!(
            bits.bit_xor(&BitString::parse("1111").unwrap())
                .unwrap()
                .to_string(),
            "0101"
        );
        assert_eq!(bits.bit_not().to_string(), "0101");
        assert_eq!(bits.shift_left(1).unwrap().to_string(), "0100");
        assert_eq!(bits.shift_right(2).unwrap().to_string(), "0010");
        assert_eq!(bits.set_bit(1, true).unwrap().to_string(), "1110");
    }

    #[test]
    fn bit_string_cast_resizes_and_integer_sign_extends() {
        assert_eq!(
            BitString::parse("10")
                .unwrap()
                .coerce_to(&crate::DataType::Bit { len: Some(4) }, true,)
                .unwrap()
                .to_string(),
            "1000"
        );
        assert_eq!(
            BitString::parse("1010101")
                .unwrap()
                .coerce_to(&crate::DataType::VarBit { max_len: Some(6) }, true,)
                .unwrap()
                .to_string(),
            "101010"
        );
        assert_eq!(
            BitString::from_i64(10, 44).unwrap().to_string(),
            "0000101100"
        );
        assert_eq!(
            BitString::from_i64(12, -44).unwrap().to_string(),
            "111111010100"
        );
    }

    #[test]
    fn bit_string_binary_masks_padding_and_type_checks() {
        let bits = BitString::new(5, vec![0b1010_1111]).unwrap();
        assert_eq!(bits.to_string(), "10101");
        assert_eq!(bits.bytes(), &[0b1010_1000]);
        assert_eq!(bits.len(), 5);
        assert!(!bits.is_empty());
        assert_eq!(bits.bit(5), None);
        assert_eq!(bits.set_bit(5, true), None);

        let encoded = bits.to_pg_binary();
        assert_eq!(BitString::from_pg_binary(&encoded), Some(bits.clone()));
        assert_eq!(BitString::from_pg_binary(&encoded[..3]), None);

        let mut negative_len = (-1_i32).to_be_bytes().to_vec();
        negative_len.push(0);
        assert_eq!(BitString::from_pg_binary(&negative_len), None);

        let mut wrong_payload_len = encoded;
        wrong_payload_len.push(0);
        assert_eq!(BitString::from_pg_binary(&wrong_payload_len), None);
        assert_eq!(BitString::new(9, vec![0]), None);
        assert_eq!(BitString::parse("10x"), None);
        assert!(BitString::parse("").unwrap().is_empty());

        assert!(bits.matches_type(&DataType::Bit { len: Some(5) }));
        assert!(!bits.matches_type(&DataType::Bit { len: Some(4) }));
        assert!(
            bits.coerce_to(&DataType::Bit { len: None }, false)
                .is_some()
        );
        assert!(
            bits.coerce_to(&DataType::VarBit { max_len: Some(4) }, false)
                .is_none()
        );
        assert!(
            bits.coerce_to(&DataType::VarBit { max_len: None }, false)
                .is_some()
        );
        assert!(bits.coerce_to(&DataType::Int32, false).is_none());
        assert!(bits.bit_and(&BitString::parse("1010").unwrap()).is_none());
    }
}
