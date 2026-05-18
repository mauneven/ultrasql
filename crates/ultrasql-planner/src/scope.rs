//! Outer-scope tracking for correlated subquery binding.
//!
//! When the binder recurses into a subquery, it pushes the outer query's
//! schema(s) onto a scope stack. Inside the subquery, column references
//! that fail to resolve against the inner FROM bind against any matching
//! outer scope. A resolved outer-scope reference marks the subquery
//! correlated and the binder records the outer-column index used.
//!
//! The stack grows as subqueries nest: frame 0 is the innermost outer
//! scope, frame 1 the next outer, and so on. The
//! [`OuterRef::frame_depth`] field records which level was matched.

use ultrasql_core::DataType;

/// A single frame on the outer-scope stack, corresponding to one outer
/// query level.
///
/// Each frame records the schema of the outer query's FROM clause and an
/// optional qualifier (table alias) used for qualified column references.
#[derive(Debug)]
pub struct ScopeFrame {
    /// Fields available in this outer scope.
    pub schema: ultrasql_core::Schema,
    /// Optional table qualifier that owns these fields.
    pub qualifier: Option<String>,
}

/// A reference from a subquery expression to a column in an enclosing
/// outer query.
///
/// `frame_depth` is 1-based: 1 means the immediately enclosing scope,
/// 2 means one level further out, etc.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OuterRef {
    /// How many scopes outward: 1 = immediately enclosing query.
    pub frame_depth: usize,
    /// 0-based column index within the frame's schema.
    pub column_index: usize,
    /// The inferred data type of the outer column.
    pub data_type: DataType,
}

/// Stack of outer scopes accumulated as the binder descends into
/// subqueries.
///
/// The head of the stack (the last element of `frames`) is the
/// **innermost** outer query.  [`resolve`] walks from the head outward,
/// so a column that is shadowed in an immediate outer scope wins over a
/// column with the same name in a farther outer scope.
///
/// [`resolve`]: ScopeStack::resolve
#[derive(Debug)]
pub struct ScopeStack {
    /// Frames in order from oldest (first declared) to newest (most
    /// recently pushed).  The youngest frame is `frames.last()`.
    frames: Vec<ScopeFrame>,
}

impl ScopeStack {
    /// Create a new, empty scope stack.
    #[must_use]
    pub const fn new() -> Self {
        Self { frames: Vec::new() }
    }

    /// Push a new outer scope frame onto the top of the stack.
    ///
    /// Call this just before recursing into a subquery so that column
    /// resolution inside the subquery can fall back to the pushed schema.
    pub fn push(&mut self, frame: ScopeFrame) {
        self.frames.push(frame);
    }

    /// Pop the most-recently-pushed frame.
    ///
    /// Call this after finishing the recursive bind of a subquery.
    ///
    /// # Panics
    ///
    /// Panics in debug builds when the stack is already empty, indicating
    /// a push/pop imbalance in the binder.
    pub fn pop(&mut self) {
        debug_assert!(!self.frames.is_empty(), "ScopeStack::pop on empty stack");
        self.frames.pop();
    }

    /// Attempt to resolve `name` against the outer scopes.
    ///
    /// The search begins with the most-recently-pushed (innermost) frame
    /// and walks outward.  Returns the first match as an [`OuterRef`],
    /// where `frame_depth = 1` means the innermost outer scope.
    ///
    /// Returns `None` when the name is not found in any outer frame.
    #[must_use]
    pub fn resolve(&self, name: &str) -> Option<OuterRef> {
        // Walk from innermost (last) to outermost (first).
        for (rev_idx, frame) in self.frames.iter().rev().enumerate() {
            let depth = rev_idx + 1; // 1-based
            if let Some((col_idx, field)) = frame.schema.find(name) {
                return Some(OuterRef {
                    frame_depth: depth,
                    column_index: col_idx,
                    data_type: field.data_type.clone(),
                });
            }
            let mut suffix_hits = frame
                .schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, field)| {
                    field
                        .name
                        .rsplit_once('.')
                        .is_some_and(|(_, suffix)| suffix.eq_ignore_ascii_case(name))
                });
            if let Some((col_idx, field)) = suffix_hits.next()
                && suffix_hits.next().is_none()
            {
                return Some(OuterRef {
                    frame_depth: depth,
                    column_index: col_idx,
                    data_type: field.data_type.clone(),
                });
            }
        }
        None
    }

    /// Returns `true` when no frames have been pushed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

impl Default for ScopeStack {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field, Schema};

    use super::*;

    fn make_frame(fields: &[(&str, DataType)]) -> ScopeFrame {
        let schema_fields: Vec<Field> = fields
            .iter()
            .map(|(name, ty)| Field::nullable(*name, ty.clone()))
            .collect();
        ScopeFrame {
            schema: Schema::new(schema_fields).expect("test schema ok"),
            qualifier: None,
        }
    }

    /// Pushing a frame and resolving a name that lives in it returns the
    /// correct `OuterRef` at depth 1.
    #[test]
    fn resolve_walks_frames_top_down() {
        let mut stack = ScopeStack::new();
        stack.push(make_frame(&[("outer_id", DataType::Int32)]));

        let result = stack.resolve("outer_id").expect("should resolve");
        assert_eq!(result.frame_depth, 1, "innermost frame is depth 1");
        assert_eq!(result.column_index, 0);
        assert_eq!(result.data_type, DataType::Int32);
    }

    /// Resolving a name that exists in both an outer and an even-more-outer
    /// frame returns the innermost match (smallest depth).
    #[test]
    fn resolve_returns_innermost_match_when_name_appears_in_multiple_frames() {
        let mut stack = ScopeStack::new();
        // Push outer-outer first (depth 2 when another frame is on top).
        stack.push(make_frame(&[("x", DataType::Int64)]));
        // Push the immediate outer on top (depth 1).
        stack.push(make_frame(&[("x", DataType::Int32)]));

        let r = stack.resolve("x").expect("should resolve");
        assert_eq!(r.frame_depth, 1, "should return innermost (depth 1)");
        assert_eq!(r.data_type, DataType::Int32);
    }

    /// A name not in any frame returns `None`.
    #[test]
    fn resolve_returns_none_when_no_frame_matches() {
        let mut stack = ScopeStack::new();
        stack.push(make_frame(&[("a", DataType::Int32)]));

        assert!(stack.resolve("z").is_none());
    }

    /// After `pop()` the frame is gone and previously-reachable names
    /// become inaccessible.
    #[test]
    fn resolve_after_pop_does_not_see_popped_frame() {
        let mut stack = ScopeStack::new();
        stack.push(make_frame(&[("gone", DataType::Int32)]));
        stack.pop();

        assert!(stack.resolve("gone").is_none());
    }

    /// An empty stack always returns `None`.
    #[test]
    fn resolve_on_empty_stack_returns_none() {
        let stack = ScopeStack::new();
        assert!(stack.resolve("anything").is_none());
    }
}
