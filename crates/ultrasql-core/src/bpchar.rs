//! PostgreSQL-compatible blank-padded character helpers.
//!
//! `CHAR(n)` / `BPCHAR(n)` stores exactly `n` characters by padding with
//! ASCII spaces on assignment. Equality and ordering ignore those trailing
//! pad spaces; pattern matching still consumes the stored bytes.

/// Error raised while coercing text into a blank-padded character value.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BpCharError {
    /// The declared character length is invalid for `CHAR(n)`.
    #[error("length for type character must be at least 1")]
    InvalidLength,

    /// The input contains non-space characters beyond the declared length.
    #[error("value too long for type character({len})")]
    TooLong {
        /// Declared maximum character count.
        len: u32,
    },
}

/// Return the comparison form of a blank-padded character string.
#[must_use]
pub fn bpchar_semantic_text(text: &str) -> &str {
    text.trim_end_matches(' ')
}

/// Coerce `input` into PostgreSQL blank-padded character storage form.
///
/// Assignment coercion rejects overlength non-space data. Explicit casts
/// truncate to the declared length, matching PostgreSQL's documented
/// `CHAR(n)` cast behavior.
pub fn coerce_bpchar_text(
    input: &str,
    len: Option<u32>,
    explicit_cast: bool,
) -> Result<String, BpCharError> {
    let Some(len) = len else {
        return Ok(input.to_owned());
    };
    if len == 0 {
        return Err(BpCharError::InvalidLength);
    }
    let target = usize::try_from(len).map_err(|_| BpCharError::TooLong { len })?;
    let char_count = input.chars().count();
    if char_count > target {
        let split_at = input
            .char_indices()
            .nth(target)
            .map_or(input.len(), |(idx, _)| idx);
        let excess = &input[split_at..];
        if explicit_cast || excess.chars().all(|ch| ch == ' ') {
            return Ok(input[..split_at].to_owned());
        }
        return Err(BpCharError::TooLong { len });
    }

    let mut out = input.to_owned();
    for _ in char_count..target {
        out.push(' ');
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignment_pads_and_rejects_non_space_overflow() {
        assert_eq!(
            coerce_bpchar_text("xy", Some(4), false).expect("pads"),
            "xy  "
        );
        assert_eq!(
            coerce_bpchar_text("xy  ", Some(2), false).expect("space overflow truncates"),
            "xy"
        );
        assert!(matches!(
            coerce_bpchar_text("xyz", Some(2), false),
            Err(BpCharError::TooLong { len: 2 })
        ));
    }

    #[test]
    fn explicit_cast_truncates_at_character_boundary() {
        assert_eq!(
            coerce_bpchar_text("abce", Some(3), true).expect("truncates"),
            "abc"
        );
        assert_eq!(
            coerce_bpchar_text("aéz", Some(2), true).expect("unicode boundary"),
            "aé"
        );
    }

    #[test]
    fn semantic_text_trims_only_ascii_spaces() {
        assert_eq!(bpchar_semantic_text("ok  "), "ok");
        assert_eq!(bpchar_semantic_text("ok\t "), "ok\t");
    }
}
