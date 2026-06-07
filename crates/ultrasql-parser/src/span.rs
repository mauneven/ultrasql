//! Byte-offset source spans.
//!
//! A `Span` is a half-open byte-offset range `[start, end)` into the
//! original SQL text. Spans travel with every token and AST node so
//! diagnostics can quote source.

use std::fmt;

/// Half-open byte-offset range into a SQL source string.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Span {
    /// Inclusive start offset, in bytes.
    pub start: u32,
    /// Exclusive end offset, in bytes.
    pub end: u32,
}

impl Span {
    /// Construct a span. `start` must be `<= end`.
    #[inline]
    #[must_use]
    pub const fn new(start: u32, end: u32) -> Self {
        debug_assert!(start <= end, "span start must not exceed end");
        Self { start, end }
    }

    /// Construct a span from `usize` offsets, saturating at `u32::MAX`.
    /// SQL statements are bounded to a few megabytes upstream, so normal
    /// callers never reach saturation.
    #[inline]
    #[must_use]
    pub fn from_usize(start: usize, end: usize) -> Self {
        Self::new(
            u32_from_usize_saturating(start),
            u32_from_usize_saturating(end),
        )
    }

    /// Length in bytes.
    #[inline]
    #[must_use]
    pub const fn len(self) -> u32 {
        self.end - self.start
    }

    /// Whether the span covers zero bytes.
    #[inline]
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.start == self.end
    }

    /// Slice the source string at this span.
    ///
    /// Returns `None` if the span is out of range; this is a defensive
    /// check — the parser should never construct an out-of-range span.
    #[inline]
    #[must_use]
    pub fn slice(self, source: &str) -> Option<&str> {
        let start = usize::try_from(self.start).ok()?;
        let end = usize::try_from(self.end).ok()?;
        source.get(start..end)
    }

    /// Smallest span enclosing both `self` and `other`.
    #[inline]
    #[must_use]
    pub const fn merge(self, other: Self) -> Self {
        let start = if self.start < other.start {
            self.start
        } else {
            other.start
        };
        let end = if self.end > other.end {
            self.end
        } else {
            other.end
        };
        Self::new(start, end)
    }
}

fn u32_from_usize_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

impl fmt::Debug for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

impl fmt::Display for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_returns_substring() {
        let span = Span::new(0, 5);
        assert_eq!(span.slice("SELECT *").unwrap(), "SELEC");
    }

    #[test]
    fn slice_out_of_range_returns_none() {
        let span = Span::new(0, 100);
        assert!(span.slice("short").is_none());
    }

    #[test]
    fn merge_envelopes_both() {
        let a = Span::new(2, 5);
        let b = Span::new(7, 10);
        assert_eq!(a.merge(b), Span::new(2, 10));
        assert_eq!(b.merge(a), Span::new(2, 10));
    }

    #[test]
    fn empty_span() {
        let s = Span::new(3, 3);
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn from_usize_saturates() {
        let s = Span::from_usize(10, 20);
        assert_eq!(s.start, 10);
        assert_eq!(s.end, 20);

        if let Ok(overflow) = usize::try_from(u64::from(u32::MAX) + 1) {
            let s = Span::from_usize(overflow, overflow);
            assert_eq!(s.start, u32::MAX);
            assert_eq!(s.end, u32::MAX);
        }
    }
}
