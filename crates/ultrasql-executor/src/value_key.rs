use std::hash::{Hash, Hasher};

pub(crate) fn canonical_decimal_key(value: i128, scale: i32) -> (i128, i64) {
    let mut significand = value;
    if significand == 0 {
        return (0, 0);
    }

    let mut scale = i64::from(scale);
    while significand % 10 == 0 {
        significand /= 10;
        scale -= 1;
    }
    (significand, scale)
}

pub(crate) fn decimal_values_equal(
    left_value: i128,
    left_scale: i32,
    right_value: i128,
    right_scale: i32,
) -> bool {
    canonical_decimal_key(left_value, left_scale) == canonical_decimal_key(right_value, right_scale)
}

pub(crate) fn hash_decimal_key<H: Hasher>(state: &mut H, value: i128, scale: i32) {
    let (value, scale) = canonical_decimal_key(value, scale);
    value.hash(state);
    scale.hash(state);
}
