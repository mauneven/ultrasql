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

    /// Construct a span from `usize` offsets, truncating to `u32`. The
    /// truncation is fine because SQL statements are bounded to a few
    /// megabytes; spans never need 64-bit precision.
    #[inline]
    #[must_use]
    pub const fn from_usize(start: usize, end: usize) -> Self {
        Self::new(start as u32, end as u32)
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
        source.get(self.start as usize..self.end as usize)
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
    fn from_usize_truncates() {
        let s = Span::from_usize(10, 20);
        assert_eq!(s.start, 10);
        assert_eq!(s.end, 20);
    }
}
