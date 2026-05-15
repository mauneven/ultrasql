//! Cross-implementation property tests for the arithmetic kernels.
//!
//! Every vectorized kernel in [`super`] has a `_scalar` reference
//! implementation that computes the same result one element at a time.
//! The cases below feed 1024 random inputs through both implementations
//! and assert the outputs match bit-for-bit (using `to_bits` for floats so
//! `NaN` payloads compare exactly).

use super::*;
use crate::column::{BoolColumn, NumericColumn};

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig {
        cases: 1024, .. proptest::prelude::ProptestConfig::default()
    })]

    // ---- i64 ----
    #[test]
    fn prop_add_i64_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<i64>(), proptest::prelude::any::<i64>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = add_i64(&a, &b, None);
        let want = add_i64_scalar(&a, &b, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_sub_i64_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<i64>(), proptest::prelude::any::<i64>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = sub_i64(&a, &b, None);
        let want = sub_i64_scalar(&a, &b, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_mul_i64_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<i64>(), proptest::prelude::any::<i64>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = mul_i64(&a, &b, None);
        let want = mul_i64_scalar(&a, &b, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_compare_i64_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<i64>(), proptest::prelude::any::<i64>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = compare_i64(&a, &b, None);
        let want = compare_i64_scalar(&a, &b, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_add_i64_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<i64>(), 0_usize..=200),
        lit in proptest::prelude::any::<i64>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = add_i64_scalar_lit(&c, lit, None);
        let want = add_i64_scalar_lit_scalar(&c, lit, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_sub_i64_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<i64>(), 0_usize..=200),
        lit in proptest::prelude::any::<i64>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = sub_i64_scalar_lit(&c, lit, None);
        let want = sub_i64_scalar_lit_scalar(&c, lit, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_mul_i64_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<i64>(), 0_usize..=200),
        lit in proptest::prelude::any::<i64>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = mul_i64_scalar_lit(&c, lit, None);
        let want = mul_i64_scalar_lit_scalar(&c, lit, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_compare_i64_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<i64>(), 0_usize..=200),
        lit in proptest::prelude::any::<i64>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = compare_i64_scalar_lit(&c, lit, None);
        let want = compare_i64_scalar_lit_scalar(&c, lit, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    // ---- i32 ----
    #[test]
    fn prop_add_i32_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<i32>(), proptest::prelude::any::<i32>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = add_i32(&a, &b, None);
        let want = add_i32_scalar(&a, &b, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_sub_i32_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<i32>(), proptest::prelude::any::<i32>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = sub_i32(&a, &b, None);
        let want = sub_i32_scalar(&a, &b, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_mul_i32_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<i32>(), proptest::prelude::any::<i32>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = mul_i32(&a, &b, None);
        let want = mul_i32_scalar(&a, &b, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_compare_i32_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<i32>(), proptest::prelude::any::<i32>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = compare_i32(&a, &b, None);
        let want = compare_i32_scalar(&a, &b, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_add_i32_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=200),
        lit in proptest::prelude::any::<i32>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = add_i32_scalar_lit(&c, lit, None);
        let want = add_i32_scalar_lit_scalar(&c, lit, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_sub_i32_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=200),
        lit in proptest::prelude::any::<i32>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = sub_i32_scalar_lit(&c, lit, None);
        let want = sub_i32_scalar_lit_scalar(&c, lit, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_mul_i32_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=200),
        lit in proptest::prelude::any::<i32>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = mul_i32_scalar_lit(&c, lit, None);
        let want = mul_i32_scalar_lit_scalar(&c, lit, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_compare_i32_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=200),
        lit in proptest::prelude::any::<i32>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = compare_i32_scalar_lit(&c, lit, None);
        let want = compare_i32_scalar_lit_scalar(&c, lit, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    // ---- f32 — uses bit-equality so NaN payloads match exactly. ----
    #[test]
    fn prop_add_f32_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<f32>(), proptest::prelude::any::<f32>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = add_f32(&a, &b, None);
        let want = add_f32_scalar(&a, &b, None);
        let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_sub_f32_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<f32>(), proptest::prelude::any::<f32>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = sub_f32(&a, &b, None);
        let want = sub_f32_scalar(&a, &b, None);
        let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_mul_f32_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<f32>(), proptest::prelude::any::<f32>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = mul_f32(&a, &b, None);
        let want = mul_f32_scalar(&a, &b, None);
        let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_compare_f32_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<f32>(), proptest::prelude::any::<f32>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = compare_f32(&a, &b, None);
        let want = compare_f32_scalar(&a, &b, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_add_f32_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<f32>(), 0_usize..=200),
        lit in proptest::prelude::any::<f32>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = add_f32_scalar_lit(&c, lit, None);
        let want = add_f32_scalar_lit_scalar(&c, lit, None);
        let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_sub_f32_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<f32>(), 0_usize..=200),
        lit in proptest::prelude::any::<f32>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = sub_f32_scalar_lit(&c, lit, None);
        let want = sub_f32_scalar_lit_scalar(&c, lit, None);
        let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_mul_f32_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<f32>(), 0_usize..=200),
        lit in proptest::prelude::any::<f32>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = mul_f32_scalar_lit(&c, lit, None);
        let want = mul_f32_scalar_lit_scalar(&c, lit, None);
        let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_compare_f32_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<f32>(), 0_usize..=200),
        lit in proptest::prelude::any::<f32>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = compare_f32_scalar_lit(&c, lit, None);
        let want = compare_f32_scalar_lit_scalar(&c, lit, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    // ---- f64 ----
    #[test]
    fn prop_add_f64_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<f64>(), proptest::prelude::any::<f64>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = add_f64(&a, &b, None);
        let want = add_f64_scalar(&a, &b, None);
        let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_sub_f64_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<f64>(), proptest::prelude::any::<f64>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = sub_f64(&a, &b, None);
        let want = sub_f64_scalar(&a, &b, None);
        let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_mul_f64_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<f64>(), proptest::prelude::any::<f64>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = mul_f64(&a, &b, None);
        let want = mul_f64_scalar(&a, &b, None);
        let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_compare_f64_matches_scalar(
        pairs in proptest::collection::vec(
            (proptest::prelude::any::<f64>(), proptest::prelude::any::<f64>()),
            0_usize..=200,
        )
    ) {
        let a = NumericColumn::from_data(pairs.iter().map(|&(x, _)| x).collect::<Vec<_>>());
        let b = NumericColumn::from_data(pairs.iter().map(|&(_, y)| y).collect::<Vec<_>>());
        let got = compare_f64(&a, &b, None);
        let want = compare_f64_scalar(&a, &b, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_add_f64_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<f64>(), 0_usize..=200),
        lit in proptest::prelude::any::<f64>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = add_f64_scalar_lit(&c, lit, None);
        let want = add_f64_scalar_lit_scalar(&c, lit, None);
        let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_sub_f64_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<f64>(), 0_usize..=200),
        lit in proptest::prelude::any::<f64>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = sub_f64_scalar_lit(&c, lit, None);
        let want = sub_f64_scalar_lit_scalar(&c, lit, None);
        let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_mul_f64_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<f64>(), 0_usize..=200),
        lit in proptest::prelude::any::<f64>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = mul_f64_scalar_lit(&c, lit, None);
        let want = mul_f64_scalar_lit_scalar(&c, lit, None);
        let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_compare_f64_scalar_lit_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<f64>(), 0_usize..=200),
        lit in proptest::prelude::any::<f64>(),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = compare_f64_scalar_lit(&c, lit, None);
        let want = compare_f64_scalar_lit_scalar(&c, lit, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    // ---- Unary ----
    #[test]
    fn prop_neg_i32_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<i32>(), 0_usize..=200),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = neg_i32(&c, None);
        let want = neg_i32_scalar(&c, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_neg_i64_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<i64>(), 0_usize..=200),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = neg_i64(&c, None);
        let want = neg_i64_scalar(&c, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }

    #[test]
    fn prop_neg_f32_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<f32>(), 0_usize..=200),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = neg_f32(&c, None);
        let want = neg_f32_scalar(&c, None);
        let got_bits: Vec<u32> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u32> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_neg_f64_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<f64>(), 0_usize..=200),
    ) {
        let c = NumericColumn::from_data(xs);
        let got = neg_f64(&c, None);
        let want = neg_f64_scalar(&c, None);
        let got_bits: Vec<u64> = got.data().iter().map(|f| f.to_bits()).collect();
        let want_bits: Vec<u64> = want.data().iter().map(|f| f.to_bits()).collect();
        proptest::prop_assert_eq!(got_bits, want_bits);
    }

    #[test]
    fn prop_not_bool_matches_scalar(
        xs in proptest::collection::vec(proptest::prelude::any::<bool>(), 0_usize..=200),
    ) {
        let c = BoolColumn::from_data(xs);
        let got = not_bool(&c, None);
        let want = not_bool_scalar(&c, None);
        proptest::prop_assert_eq!(got.data(), want.data());
    }
}
