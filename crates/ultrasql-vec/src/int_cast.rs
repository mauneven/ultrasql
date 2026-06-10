//! Integer conversion helpers for hot vector kernels.

#[cfg(not(any(target_pointer_width = "32", target_pointer_width = "64")))]
compile_error!("ultrasql-vec requires a 32-bit or 64-bit target pointer width");

/// Convert a `u32` into `usize` on supported production targets.
///
/// The implementation avoids fallible conversion and integer-width `as` casts
/// in hot paths. It reconstructs the value from `u16` halves, where Rust
/// provides lossless `From<u16> for usize` on supported targets.
#[inline]
#[must_use]
pub(crate) fn u32_to_usize(value: u32) -> usize {
    let bytes = value.to_le_bytes();
    let lo = u16::from_le_bytes([bytes[0], bytes[1]]);
    let hi = u16::from_le_bytes([bytes[2], bytes[3]]);
    usize::from(lo) | (usize::from(hi) << 16)
}

#[cfg(test)]
mod tests {
    use super::u32_to_usize;

    #[test]
    fn u32_to_usize_preserves_representative_values() {
        for value in [0, 1, 63, 64, 65_535, 65_536, 1_000_000, u32::MAX] {
            assert_eq!(u32::try_from(u32_to_usize(value)), Ok(value));
        }
    }
}
