//! Page checksum.
//!
//! Pages embed a 32-bit checksum to detect torn writes and disk-level
//! corruption. We use xxh3-64 truncated to 32 bits — fast (≈ 10 GB/s on
//! M-series), produces a strong scrambling, and the truncation cost is
//! negligible at our error-detection budget (false-positive 1 in 2³²).
//!
//! The checksum *field itself* must be zeroed during computation so the
//! check is self-consistent. Callers should use [`compute_page_checksum`],
//! which handles the zero-field convention.

use xxhash_rust::xxh3::xxh3_64;

use ultrasql_core::constants::PAGE_SIZE;

/// Byte offset of the 4-byte checksum within a page.
pub const CHECKSUM_OFFSET: usize = 8;

/// Compute a page's checksum.
///
/// The bytes at `[CHECKSUM_OFFSET, CHECKSUM_OFFSET + 4)` are treated as
/// zero during the hash, so a page hashes the same whether its
/// checksum field is set, zeroed, or stale. Callers can write the
/// returned value back into the field unconditionally.
#[must_use]
pub fn compute_page_checksum(page: &[u8; PAGE_SIZE]) -> u32 {
    let mut hasher_input = [0_u8; PAGE_SIZE];
    hasher_input.copy_from_slice(page);
    hasher_input[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
    let h = xxh3_64(&hasher_input);
    (h ^ (h >> 32)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_ignores_checksum_field() {
        let mut a = [0_u8; PAGE_SIZE];
        let mut b = [0_u8; PAGE_SIZE];

        // Identical content with a different stale checksum field
        // must hash to the same value.
        a[0..8].copy_from_slice(&42_u64.to_le_bytes());
        a[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].copy_from_slice(&0xDEAD_BEEF_u32.to_le_bytes());

        b[0..8].copy_from_slice(&42_u64.to_le_bytes());
        b[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].copy_from_slice(&0xCAFE_BABE_u32.to_le_bytes());

        assert_eq!(compute_page_checksum(&a), compute_page_checksum(&b));
    }

    #[test]
    fn checksum_distinguishes_content() {
        let a = [0_u8; PAGE_SIZE];
        let mut b = [0_u8; PAGE_SIZE];
        b[1000] = 1;
        assert_ne!(compute_page_checksum(&a), compute_page_checksum(&b));
    }

    #[test]
    fn checksum_is_deterministic() {
        let mut p = [0_u8; PAGE_SIZE];
        for (i, byte) in p.iter_mut().enumerate() {
            *byte = (i & 0xFF) as u8;
        }
        assert_eq!(compute_page_checksum(&p), compute_page_checksum(&p));
    }
}
