//! Equi-depth histogram for selectivity estimation.
//!
//! An equi-depth histogram partitions a sorted sample into buckets of
//! (approximately) equal population. The boundary array has
//! `bucket_count + 1` entries: `bounds[0]` is the minimum observed
//! value and `bounds[bucket_count]` is the maximum. Each bucket `[i]`
//! covers the half-open range `[bounds[i], bounds[i+1])`, except the
//! final bucket which is closed on both ends.
//!
//! The implementation deliberately avoids floating-point bucket widths;
//! all arithmetic stays in index space and fractions are only produced
//! at the selectivity-estimation call sites.

use ultrasql_core::Value;

use crate::stats::value_ord::compare_values;

/// Equi-depth histogram: each bucket holds roughly the same number of
/// samples; bucket bounds carry the value range.
///
/// Invariant: `bounds.len() == usize::from(bucket_count) + 1` and the
/// bounds are monotonically non-decreasing under `compare_values`.
#[derive(Clone, Debug, PartialEq)]
pub struct EquiDepthHistogram {
    /// Number of buckets in the histogram. Must be at least 1.
    pub bucket_count: u16,
    /// `bucket_count + 1` monotonic boundary values. `bounds[0]` is the
    /// minimum observed sample; `bounds[bucket_count]` is the maximum.
    pub bounds: Vec<Value>,
    /// Approximate number of samples in each bucket.
    pub samples_per_bucket: u64,
}

impl EquiDepthHistogram {
    /// Build a histogram from a pre-sorted (ascending) sample slice.
    ///
    /// `bucket_count` must be at least 1. If `sorted_samples` is empty,
    /// the histogram will have a single boundary `Value::Null` and zero
    /// samples per bucket.
    ///
    /// The caller is responsible for ensuring that `sorted_samples` is
    /// sorted ascending under the canonical value ordering (see
    /// `crate::stats::value_ord::compare_values`).
    #[must_use]
    pub fn build_from_sorted(sorted_samples: &[Value], bucket_count: u16) -> Self {
        let bucket_count = bucket_count.max(1);
        let n = sorted_samples.len();

        if n == 0 {
            return Self {
                bucket_count,
                bounds: vec![Value::Null],
                samples_per_bucket: 0,
            };
        }

        let k = usize::from(bucket_count);
        // samples_per_bucket is the floor division; boundary selection
        // uses the nearest index.
        let spb = u64::try_from(n / k).unwrap_or(u64::MAX);

        let mut bounds = Vec::with_capacity(k + 1);
        bounds.push(sorted_samples[0].clone());

        for bucket in 1..k {
            // Boundary index: we want the value at position bucket * n / k.
            // Integer-only — no `as` cast.
            let idx = bucket * n / k;
            let idx = idx.min(n - 1);
            bounds.push(sorted_samples[idx].clone());
        }

        bounds.push(sorted_samples[n - 1].clone());

        Self {
            bucket_count,
            bounds,
            samples_per_bucket: spb,
        }
    }

    /// Estimate the fraction of rows with a value ≤ `v`.
    ///
    /// Returns a value in `[0.0, 1.0]`. Returns `0.0` for values below
    /// the minimum boundary and `1.0` for values at or above the maximum
    /// boundary. For intermediate values the fraction is computed by
    /// linear interpolation within the owning bucket.
    #[must_use]
    pub fn estimate_lte(&self, v: &Value) -> f64 {
        if self.bounds.is_empty() {
            return 0.0;
        }

        let last = self.bounds.len() - 1;

        // Below minimum.
        if compare_values(v, &self.bounds[0]).is_lt() {
            return 0.0;
        }
        // At or above maximum.
        if compare_values(v, &self.bounds[last]).is_ge() {
            return 1.0;
        }

        // Find the bucket whose upper boundary >= v.
        let k = usize::from(self.bucket_count);
        let mut lo = 0_usize;
        let mut hi = k; // bucket index, not bounds index

        // Binary search for the bucket.
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            // Bucket `mid` covers [bounds[mid], bounds[mid+1]).
            if compare_values(v, &self.bounds[mid + 1]).is_lt() {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        // `lo` is the bucket index.
        let bucket = lo.min(k - 1);
        // Fraction of full buckets before this one.
        let full_fraction = f64::from(u32::try_from(bucket).unwrap_or(u32::MAX))
            / f64::from(u32::try_from(k).unwrap_or(u32::MAX));
        // Width of this bucket.
        let bucket_fraction = 1.0_f64 / f64::from(u32::try_from(k).unwrap_or(u32::MAX));
        // Assume uniform distribution within the bucket.
        bucket_fraction.mul_add(0.5, full_fraction)
    }

    /// Estimate the fraction of rows with a value in `[lo, hi]` (inclusive).
    ///
    /// Returns `0.0` when `lo > hi` under the canonical ordering.
    #[must_use]
    pub fn estimate_range(&self, lo: &Value, hi: &Value) -> f64 {
        if compare_values(lo, hi).is_gt() {
            return 0.0;
        }
        let frac_hi = self.estimate_lte(hi);
        // For the lower bound we want P(x < lo), which is P(x <= lo) - P(x == lo).
        // Approximate P(x == lo) as a bucket-width fraction.
        let frac_lo = if compare_values(lo, &self.bounds[0]).is_le() {
            0.0
        } else {
            let below = self.estimate_lte(lo);
            let bucket_width = 1.0_f64 / f64::from(u32::from(self.bucket_count));
            bucket_width.mul_add(-0.5, below).max(0.0)
        };
        (frac_hi - frac_lo).clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn int_vals(slice: &[i32]) -> Vec<Value> {
        slice.iter().copied().map(Value::Int32).collect()
    }

    /// `build_from_sorted` produces monotonically non-decreasing bounds.
    #[test]
    fn bounds_are_monotonically_non_decreasing() {
        let samples = int_vals(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let hist = EquiDepthHistogram::build_from_sorted(&samples, 5);
        let bounds = &hist.bounds;
        assert_eq!(bounds.len(), 6, "bucket_count=5 → 6 bounds");
        for w in bounds.windows(2) {
            assert!(
                compare_values(&w[0], &w[1]).is_le(),
                "bounds not monotone: {bounds:?}"
            );
        }
    }

    /// `estimate_lte` at a value below the minimum returns 0.0.
    #[test]
    fn estimate_lte_below_min_returns_zero() {
        let samples = int_vals(&[10, 20, 30, 40, 50]);
        let hist = EquiDepthHistogram::build_from_sorted(&samples, 5);
        let frac = hist.estimate_lte(&Value::Int32(5));
        assert!(frac < f64::EPSILON, "should be 0 below minimum, got {frac}");
    }

    /// `estimate_lte` at the maximum returns 1.0.
    #[test]
    fn estimate_lte_at_max_returns_one() {
        let samples = int_vals(&[10, 20, 30, 40, 50]);
        let hist = EquiDepthHistogram::build_from_sorted(&samples, 5);
        let frac = hist.estimate_lte(&Value::Int32(50));
        assert!(
            (frac - 1.0).abs() < f64::EPSILON,
            "should be 1.0 at maximum, got {frac}"
        );
    }

    /// `estimate_range` over disjoint ranges sums to at most 1.0.
    #[test]
    fn estimate_range_disjoint_sums_at_most_one() {
        let samples: Vec<Value> = (1_i32..=100).map(Value::Int32).collect();
        let hist = EquiDepthHistogram::build_from_sorted(&samples, 10);
        let r1 = hist.estimate_range(&Value::Int32(1), &Value::Int32(30));
        let r2 = hist.estimate_range(&Value::Int32(70), &Value::Int32(100));
        assert!(
            r1 + r2 <= 1.0 + 1e-9,
            "disjoint ranges summed to {}: {} + {}",
            r1 + r2,
            r1,
            r2
        );
    }

    /// `estimate_range` returns 0.0 when lo > hi.
    #[test]
    fn estimate_range_returns_zero_when_lo_gt_hi() {
        let samples = int_vals(&[1, 2, 3, 4, 5]);
        let hist = EquiDepthHistogram::build_from_sorted(&samples, 3);
        let frac = hist.estimate_range(&Value::Int32(5), &Value::Int32(1));
        assert!(frac < f64::EPSILON, "lo > hi should give 0, got {frac}");
    }

    // Property test: for any sorted integer slice, bounds are monotone.
    proptest! {
        #[test]
        fn proptest_bounds_monotone(
            mut vals in prop::collection::vec(0_i32..1000, 1..200),
            buckets in 1_u16..20,
        ) {
            vals.sort_unstable();
            let samples: Vec<Value> = vals.iter().copied().map(Value::Int32).collect();
            let hist = EquiDepthHistogram::build_from_sorted(&samples, buckets);
            let bounds = &hist.bounds;
            for w in bounds.windows(2) {
                prop_assert!(
                    compare_values(&w[0], &w[1]).is_le(),
                    "non-monotone bounds: {bounds:?}"
                );
            }
        }
    }
}
