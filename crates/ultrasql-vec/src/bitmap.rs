//! Packed bitmap used for null indicators and predicate masks.
//!
//! The bitmap is little-endian within each byte: bit 0 is the least
//! significant. This is the same convention as Apache Arrow and lets
//! us reuse populated bitmaps across boundaries without conversion.

use std::fmt;

/// Packed bitmap with a logical length in bits.
#[derive(Clone, PartialEq, Eq)]
pub struct Bitmap {
    bits: Vec<u64>,
    len_bits: usize,
}

impl Bitmap {
    /// Allocate a bitmap of `len` bits, all set to `value`.
    #[must_use]
    pub fn new(len: usize, value: bool) -> Self {
        let words = len.div_ceil(64);
        let pattern = if value { u64::MAX } else { 0 };
        let mut bits = vec![pattern; words];
        if value && len % 64 != 0 {
            // Clear the trailing high bits in the last word so the
            // logical bit count is exactly `len` and popcount is
            // honest.
            let mask = (1_u64 << (len % 64)) - 1;
            if let Some(last) = bits.last_mut() {
                *last &= mask;
            }
        }
        Self { bits, len_bits: len }
    }

    /// Number of logical bits.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len_bits
    }

    /// Whether the bitmap is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len_bits == 0
    }

    /// Read a bit. Panics if `i >= len`.
    #[inline]
    #[must_use]
    pub fn get(&self, i: usize) -> bool {
        assert!(i < self.len_bits, "bitmap index out of bounds");
        let word = i / 64;
        let bit = i % 64;
        (self.bits[word] >> bit) & 1 == 1
    }

    /// Write a bit. Panics if `i >= len`.
    #[inline]
    pub fn set(&mut self, i: usize, value: bool) {
        assert!(i < self.len_bits, "bitmap index out of bounds");
        let word = i / 64;
        let bit = i % 64;
        if value {
            self.bits[word] |= 1_u64 << bit;
        } else {
            self.bits[word] &= !(1_u64 << bit);
        }
    }

    /// Count set bits.
    #[must_use]
    pub fn count_ones(&self) -> usize {
        self.bits.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Borrow the underlying u64 words.
    #[must_use]
    pub fn words(&self) -> &[u64] {
        &self.bits
    }

    /// Iterate over indices whose bit is set.
    pub fn iter_ones(&self) -> impl Iterator<Item = usize> + '_ {
        SetBitsIter {
            bits: &self.bits,
            len_bits: self.len_bits,
            word_idx: 0,
            current: self.bits.first().copied().unwrap_or(0),
        }
    }
}

impl fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bitmap")
            .field("len", &self.len_bits)
            .field("count_ones", &self.count_ones())
            .finish_non_exhaustive()
    }
}

struct SetBitsIter<'a> {
    bits: &'a [u64],
    len_bits: usize,
    word_idx: usize,
    current: u64,
}

impl Iterator for SetBitsIter<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        loop {
            if self.current != 0 {
                let bit = self.current.trailing_zeros() as usize;
                let i = self.word_idx * 64 + bit;
                self.current &= self.current - 1;
                if i < self.len_bits {
                    return Some(i);
                }
                return None;
            }
            self.word_idx += 1;
            if self.word_idx >= self.bits.len() {
                return None;
            }
            self.current = self.bits[self.word_idx];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_all_zeros_count_ones_is_zero() {
        let bm = Bitmap::new(100, false);
        assert_eq!(bm.len(), 100);
        assert_eq!(bm.count_ones(), 0);
    }

    #[test]
    fn new_all_ones_count_ones_matches_len() {
        for len in [0_usize, 1, 7, 63, 64, 65, 100, 1024] {
            let bm = Bitmap::new(len, true);
            assert_eq!(bm.count_ones(), len, "len = {len}");
        }
    }

    #[test]
    fn set_and_get_round_trip() {
        let mut bm = Bitmap::new(100, false);
        for i in [0_usize, 1, 7, 63, 64, 99] {
            bm.set(i, true);
        }
        for i in 0..100 {
            let expected = matches!(i, 0 | 1 | 7 | 63 | 64 | 99);
            assert_eq!(bm.get(i), expected, "i = {i}");
        }
    }

    #[test]
    fn iter_ones_yields_set_indices_in_order() {
        let mut bm = Bitmap::new(80, false);
        for i in [5_usize, 13, 42, 64, 79] {
            bm.set(i, true);
        }
        let got: Vec<_> = bm.iter_ones().collect();
        assert_eq!(got, vec![5, 13, 42, 64, 79]);
    }

    #[test]
    #[should_panic(expected = "bitmap index out of bounds")]
    fn get_out_of_bounds_panics() {
        let bm = Bitmap::new(10, true);
        let _ = bm.get(10);
    }

    #[test]
    fn empty_bitmap_has_zero_count() {
        let bm = Bitmap::new(0, true);
        assert!(bm.is_empty());
        assert_eq!(bm.count_ones(), 0);
    }
}
