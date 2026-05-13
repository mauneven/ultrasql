//! UltraSQL cost-based optimizer.
//!
//! Two-phase optimizer: rule-based rewrites (predicate pushdown, constant
//! folding, projection pushdown, subquery decorrelation) followed by a
//! Cascades-style top-down search for join order and physical operator
//! selection. Statistics are sourced from the catalog.

#![forbid(unsafe_op_in_unsafe_fn)]
