//! Cache-line awareness primitives.
//!
//! Multiple cores contending on the *same cache line* even when reading
//! and writing *different fields* generate cache-coherence traffic that
//! looks like contention on the lock itself. The cure is alignment.
//!
//! `CachePadded<T>` aligns `T` to a 64-byte boundary and pads it to a
//! full cache line. Use for per-shard counters, per-shard locks, and
//! anything else where adjacent fields would otherwise share a line.

use std::ops::{Deref, DerefMut};

use crate::constants::CACHE_LINE_SIZE;

/// Wrapper that aligns its payload to a cache-line boundary and pads it
/// out to a whole cache line.
///
/// `CachePadded<T>` is a `repr(C, align(64))` type wrapping a single
/// field. The compiler ensures any subsequent struct field starts on a
/// new cache line, eliminating false sharing.
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct CachePadded<T> {
    /// The wrapped value.
    pub value: T,
}

impl<T> CachePadded<T> {
    /// Wrap a value with cache-line padding.
    #[inline]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    /// Unwrap, returning the payload.
    #[inline]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for CachePadded<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T> From<T> for CachePadded<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

/// Compile-time assertion: cache line is 64 bytes on every supported
/// platform.
const _: () = assert!(CACHE_LINE_SIZE == 64);

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;

    #[test]
    fn padded_alignment_is_64() {
        assert_eq!(align_of::<CachePadded<u8>>(), 64);
        assert_eq!(align_of::<CachePadded<u64>>(), 64);
        assert_eq!(align_of::<CachePadded<AtomicU64>>(), 64);
    }

    #[test]
    fn padded_size_is_64_for_small_payloads() {
        assert_eq!(size_of::<CachePadded<u8>>(), 64);
        assert_eq!(size_of::<CachePadded<u64>>(), 64);
    }

    #[test]
    fn deref_round_trip() {
        let mut x = CachePadded::new(7_u64);
        assert_eq!(*x, 7);
        *x = 11;
        assert_eq!(x.into_inner(), 11);
    }

    #[test]
    fn from_impl_works() {
        let x: CachePadded<u32> = 5.into();
        assert_eq!(*x, 5);
    }
}
