//! Most-common-values (MCV) list for selectivity estimation.
//!
//! An MCV list records the `top_k` most frequently observed values in a
//! column sample together with their relative frequencies. The frequencies
//! sum to at most 1.0; values not in the list are assumed to be distributed
//! uniformly over the remainder `1.0 - covered_fraction()`.
//!
//! This matches the `pg_statistic` `stakind = 1` slot semantics.

use std::cmp::Reverse;

use ahash::AHashMap;

use ultrasql_core::Value;

use crate::stats::value_ord::value_key;

/// Most-common-values list with per-value frequencies.
///
/// `values[i]` occurs with frequency `frequencies[i]` in the sampled
/// population. The two `Vec`s always have the same length.
///
/// Invariant: `frequencies` are non-negative; their sum is ≤ 1.0.
#[derive(Clone, Debug, PartialEq)]
pub struct MostCommonValues {
    /// The top-K distinct values, ordered by descending frequency.
    pub values: Vec<Value>,
    /// Relative frequency of each value; `frequencies[i]` corresponds to
    /// `values[i]`.
    pub frequencies: Vec<f64>,
}

impl MostCommonValues {
    /// Build an MCV list from a flat sample slice by extracting the `top_k`
    /// most-frequent non-NULL values.
    ///
    /// If `top_k` is 0, returns an empty MCV list.
    /// NULL values are excluded from both `values` and `frequencies`.
    #[must_use]
    pub fn build_from_samples(samples: &[Value], top_k: u16) -> Self {
        if top_k == 0 || samples.is_empty() {
            return Self {
                values: Vec::new(),
                frequencies: Vec::new(),
            };
        }

        // Count occurrences by canonical key.
        let total = samples.len();
        let mut counts: AHashMap<Vec<u8>, (Value, u64)> = AHashMap::new();

        for v in samples {
            if v.is_null() {
                continue;
            }
            let key = value_key(v);
            let entry = counts.entry(key).or_insert_with(|| (v.clone(), 0));
            entry.1 = entry.1.saturating_add(1);
        }

        // Sort by descending count then take top_k.
        let mut pairs: Vec<(Value, u64)> = counts.into_values().collect();
        pairs.sort_by_key(|b| Reverse(b.1));
        pairs.truncate(usize::from(top_k));

        let total_f = total as f64;
        let mut values = Vec::with_capacity(pairs.len());
        let mut frequencies = Vec::with_capacity(pairs.len());

        for (v, count) in pairs {
            values.push(v);
            let freq = count as f64 / total_f;
            frequencies.push(freq);
        }

        Self {
            values,
            frequencies,
        }
    }

    /// Return the recorded frequency for `v`, or `0.0` if `v` is not in the
    /// MCV list.
    #[must_use]
    pub fn frequency_of(&self, v: &Value) -> f64 {
        let key = value_key(v);
        for (stored, freq) in self.values.iter().zip(self.frequencies.iter()) {
            if value_key(stored) == key {
                return *freq;
            }
        }
        0.0
    }

    /// The total frequency covered by this MCV list (sum of all
    /// `frequencies`). The remaining `1.0 - covered_fraction()` is
    /// distributed among values not in the list.
    #[must_use]
    pub fn covered_fraction(&self) -> f64 {
        self.frequencies.iter().copied().fold(0.0_f64, |a, b| a + b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples() -> Vec<Value> {
        // Five copies of value 1, three of value 2, two of value 3, one of value 4.
        vec![
            Value::Int32(1),
            Value::Int32(1),
            Value::Int32(1),
            Value::Int32(1),
            Value::Int32(1),
            Value::Int32(2),
            Value::Int32(2),
            Value::Int32(2),
            Value::Int32(3),
            Value::Int32(3),
            Value::Int32(4),
        ]
    }

    /// Top-3 extraction produces the three most-frequent values.
    #[test]
    fn top_k_extraction_identifies_most_frequent() {
        let mcv = MostCommonValues::build_from_samples(&samples(), 3);
        assert_eq!(mcv.values.len(), 3, "expected 3 values");
        assert_eq!(
            mcv.values[0],
            Value::Int32(1),
            "most frequent should be first"
        );
    }

    /// The frequencies of the top-3 values sum to ≤ 1.0.
    #[test]
    fn frequencies_sum_le_one() {
        let mcv = MostCommonValues::build_from_samples(&samples(), 3);
        let sum: f64 = mcv.frequencies.iter().copied().sum();
        assert!(sum <= 1.0 + 1e-9, "frequencies sum to {sum}");
        assert!(
            sum > 0.0,
            "frequencies should be positive for non-empty sample"
        );
    }

    /// `covered_fraction` equals the sum of all frequencies.
    #[test]
    fn covered_fraction_equals_sum() {
        let mcv = MostCommonValues::build_from_samples(&samples(), 3);
        let manual_sum: f64 = mcv.frequencies.iter().copied().sum();
        let diff = (mcv.covered_fraction() - manual_sum).abs();
        assert!(
            diff < 1e-12,
            "covered_fraction differs from manual sum by {diff}"
        );
    }

    /// `frequency_of` returns the stored frequency for a known value.
    #[test]
    fn frequency_of_known_value() {
        let total = samples().len();
        let mcv = MostCommonValues::build_from_samples(&samples(), 4);
        let f1 = mcv.frequency_of(&Value::Int32(1));
        let expected = 5.0 / total as f64;
        assert!(
            (f1 - expected).abs() < 1e-9,
            "expected {expected}, got {f1}"
        );
    }

    /// `frequency_of` returns 0.0 for a value absent from the list.
    #[test]
    fn frequency_of_absent_value_is_zero() {
        let mcv = MostCommonValues::build_from_samples(&samples(), 2);
        let f = mcv.frequency_of(&Value::Int32(999));
        assert!(f < f64::EPSILON, "expected 0.0 for absent value, got {f}");
    }

    /// NULL samples are not included in the MCV list.
    #[test]
    fn null_values_excluded() {
        let samples_with_null = vec![Value::Null, Value::Int32(1), Value::Int32(1), Value::Null];
        let mcv = MostCommonValues::build_from_samples(&samples_with_null, 10);
        for v in &mcv.values {
            assert!(!v.is_null(), "NULL should not appear in MCV values");
        }
    }
}
