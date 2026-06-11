//! Little-endian integer read/write helpers.
//!
//! Every on-disk and on-wire integer in UltraSQL is little-endian. These
//! helpers are explicit so callers cannot accidentally read a host-byte-
//! order integer. The functions panic on out-of-bounds; the caller is
//! responsible for bounds checking, because on hot paths we want the
//! check elided after one bound assertion at the top of the function.
//!
//! These functions are deliberately tiny so the compiler can inline them
//! and emit the natural CPU load/store.

use std::array::TryFromSliceError;

macro_rules! impl_reader {
    ($name:ident, $ty:ty, $size:expr) => {
        /// Read a little-endian integer of the given width from `buf`.
        ///
        /// Returns an error if `buf` is shorter than the integer width.
        #[inline]
        pub fn $name(buf: &[u8]) -> Result<$ty, TryFromSliceError> {
            let arr: [u8; $size] = buf
                .get(..$size)
                .ok_or_else(|| {
                    // Force a TryFromSliceError. The standard way to mint
                    // one is via TryInto on a slice of wrong size.
                    let empty: &[u8] = &[];
                    <[u8; $size]>::try_from(empty).unwrap_err()
                })?
                .try_into()?;
            Ok(<$ty>::from_le_bytes(arr))
        }
    };
}

macro_rules! impl_writer {
    ($name:ident, $ty:ty) => {
        /// Write a little-endian integer to `buf`. The slice must be at
        /// least `size_of::<$ty>()` bytes; otherwise the function panics
        /// with a named bounds invariant.
        #[inline]
        pub fn $name(buf: &mut [u8], value: $ty) {
            let bytes = value.to_le_bytes();
            let dst = buf
                .get_mut(..bytes.len())
                .expect("endian writer requires enough output bytes");
            dst.copy_from_slice(&bytes);
        }
    };
}

impl_reader!(read_u16_le, u16, 2);
impl_reader!(read_u32_le, u32, 4);
impl_reader!(read_u64_le, u64, 8);
impl_reader!(read_i16_le, i16, 2);
impl_reader!(read_i32_le, i32, 4);
impl_reader!(read_i64_le, i64, 8);

impl_writer!(write_u16_le, u16);
impl_writer!(write_u32_le, u32);
impl_writer!(write_u64_le, u64);
impl_writer!(write_i16_le, i16);
impl_writer!(write_i32_le, i32);
impl_writer!(write_i64_le, i64);

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn u32_round_trip(value: u32) {
            let mut buf = [0_u8; 4];
            write_u32_le(&mut buf, value);
            prop_assert_eq!(read_u32_le(&buf).unwrap(), value);
        }

        #[test]
        fn u64_round_trip(value: u64) {
            let mut buf = [0_u8; 8];
            write_u64_le(&mut buf, value);
            prop_assert_eq!(read_u64_le(&buf).unwrap(), value);
        }

        #[test]
        fn i32_round_trip(value: i32) {
            let mut buf = [0_u8; 4];
            write_i32_le(&mut buf, value);
            prop_assert_eq!(read_i32_le(&buf).unwrap(), value);
        }

        #[test]
        fn i64_round_trip(value: i64) {
            let mut buf = [0_u8; 8];
            write_i64_le(&mut buf, value);
            prop_assert_eq!(read_i64_le(&buf).unwrap(), value);
        }

        #[test]
        fn u16_round_trip(value: u16) {
            let mut buf = [0_u8; 2];
            write_u16_le(&mut buf, value);
            prop_assert_eq!(read_u16_le(&buf).unwrap(), value);
        }
    }

    #[test]
    fn read_rejects_short_buffer() {
        let buf = [0_u8; 3];
        assert!(read_u32_le(&buf).is_err());
    }

    #[test]
    fn little_endian_explicit_byte_order() {
        // 0x12345678 little-endian == 78 56 34 12.
        let mut buf = [0_u8; 4];
        write_u32_le(&mut buf, 0x1234_5678);
        assert_eq!(buf, [0x78, 0x56, 0x34, 0x12]);
    }
}
