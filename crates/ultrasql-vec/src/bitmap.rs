//! Packed bitmap used for null indicators and predicate masks.
//!
//! The bitmap is little-endian within each byte: bit 0 is the least
//! significant. This is the same convention as Apache Arrow and lets
//! us reuse populated bitmaps across boundaries without conversion.

use std::fmt;

use crate::int_cast::u32_to_usize;

const BITS_PER_WORD: usize = 64;

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
        let words = len.div_ceil(BITS_PER_WORD);
        let pattern = if value { u64::MAX } else { 0 };
        let mut bits = vec![pattern; words];
        if let (true, Some(mask)) = (value, trailing_word_mask(len)) {
            // Clear the trailing high bits in the last word so the
            // logical bit count is exactly `len` and popcount is
            // honest.
            if let Some(last) = bits.last_mut() {
                *last &= mask;
            }
        }
        Self {
            bits,
            len_bits: len,
        }
    }

    /// Build a bitmap directly from a packed-word buffer.
    ///
    /// `words.len()` must be at least `len.div_ceil(64)`. Any high bits
    /// in the last word beyond `len % 64` are forced to zero so that
    /// [`Self::count_ones`] and word-level scans never see padding bits.
    /// This is the constructor used by SIMD kernels that compute 64
    /// lanes of mask at a time and want to commit the word directly
    /// without going through [`Self::set`].
    ///
    /// # Panics
    ///
    /// Panics if `words.len() < len.div_ceil(64)`.
    #[must_use]
    pub fn from_words(mut words: Vec<u64>, len: usize) -> Self {
        let required = len.div_ceil(BITS_PER_WORD);
        assert!(
            words.len() >= required,
            "Bitmap::from_words: words.len() = {} < required {} for {} bits",
            words.len(),
            required,
            len
        );
        words.truncate(required);
        if let Some(mask) = trailing_word_mask(len) {
            if let Some(last) = words.last_mut() {
                *last &= mask;
            }
        }
        Self {
            bits: words,
            len_bits: len,
        }
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
        let Some((word, bit)) = word_and_bit(i) else {
            return false;
        };
        (self.bits[word] >> bit) & 1 == 1
    }

    /// Write a bit. Panics if `i >= len`.
    #[inline]
    pub fn set(&mut self, i: usize, value: bool) {
        assert!(i < self.len_bits, "bitmap index out of bounds");
        let Some((word, bit)) = word_and_bit(i) else {
            return;
        };
        let Some(mask) = 1_u64.checked_shl(bit) else {
            return;
        };
        if value {
            self.bits[word] |= mask;
        } else {
            self.bits[word] &= !mask;
        }
    }

    /// Count set bits.
    #[must_use]
    pub fn count_ones(&self) -> usize {
        self.bits.iter().map(|w| u32_to_usize(w.count_ones())).sum()
    }

    /// Borrow the underlying u64 words.
    #[must_use]
    pub fn words(&self) -> &[u64] {
        &self.bits
    }

    /// Consume the bitmap and return its packed words.
    ///
    /// Word bit order is Arrow-layout: bit 0 is the least
    /// significant bit of word 0. Bridge crates use this to transfer
    /// validity buffers without touching each bit.
    #[must_use]
    pub fn into_words(self) -> Vec<u64> {
        self.bits
    }

    /// Mutably borrow the underlying u64 words.
    ///
    /// Used by SIMD kernels that emit 64 packed lanes of mask in a
    /// single store. The caller is responsible for keeping any bits
    /// beyond `len % 64` (in the final word) zero so that
    /// [`Self::count_ones`] and word-level scans stay correct.
    pub fn words_mut(&mut self) -> &mut [u64] {
        &mut self.bits
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
                let bit = u32_to_usize(self.current.trailing_zeros());
                let i = self
                    .word_idx
                    .checked_mul(BITS_PER_WORD)
                    .and_then(|base| base.checked_add(bit))?;
                self.current &= self.current.wrapping_sub(1);
                if i < self.len_bits {
                    return Some(i);
                }
                return None;
            }
            self.word_idx = self.word_idx.checked_add(1)?;
            if self.word_idx >= self.bits.len() {
                return None;
            }
            self.current = self.bits[self.word_idx];
        }
    }
}

fn trailing_word_mask(len: usize) -> Option<u64> {
    let tail_bits = len.rem_euclid(BITS_PER_WORD);
    if tail_bits == 0 {
        return None;
    }
    let shift = u32::try_from(tail_bits).ok()?;
    1_u64.checked_shl(shift)?.checked_sub(1)
}

fn word_and_bit(i: usize) -> Option<(usize, u32)> {
    Some((
        i.div_euclid(BITS_PER_WORD),
        u32::try_from(i.rem_euclid(BITS_PER_WORD)).ok()?,
    ))
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
    fn iter_ones_ignores_masked_padding_bits() {
        let bm = Bitmap::from_words(vec![u64::MAX, u64::MAX], 65);
        let got: Vec<_> = bm.iter_ones().collect();
        assert_eq!(got.len(), 65);
        assert_eq!(got.first(), Some(&0));
        assert_eq!(got.last(), Some(&64));
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

    #[test]
    fn from_words_round_trip() {
        // 130 bits across 3 words. Set every other bit.
        let words = vec![0xAAAA_AAAA_AAAA_AAAA_u64; 3];
        let bm = Bitmap::from_words(words, 130);
        assert_eq!(bm.len(), 130);
        for i in 0..130 {
            assert_eq!(bm.get(i), i % 2 == 1, "i = {i}");
        }
    }

    #[test]
    fn from_words_masks_trailing_bits() {
        // 65 bits → 2 words; second word should be masked to bit 0 only.
        let words = vec![u64::MAX, u64::MAX];
        let bm = Bitmap::from_words(words, 65);
        assert_eq!(bm.count_ones(), 65);
        // High word should be exactly 1.
        assert_eq!(bm.words()[1], 1);
    }

    #[test]
    fn words_mut_lets_kernel_write_packed() {
        let mut bm = Bitmap::new(192, false);
        for w in bm.words_mut() {
            *w = 0xFFFF_FFFF_FFFF_FFFF;
        }
        assert_eq!(bm.count_ones(), 192);
    }

    #[test]
    #[should_panic(expected = "Bitmap::from_words")]
    fn from_words_panics_on_short_buffer() {
        let _ = Bitmap::from_words(vec![0], 65);
    }
}
