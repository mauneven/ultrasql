//! UltraSQL logical planner.
//!
//! Stage 1: name resolution / binding (column refs, table aliases, function
//! lookups). Stage 2: type checking and implicit cast insertion. Stage 3:
//! produce a fully typed logical plan tree consumed by the optimizer.

#![forbid(unsafe_op_in_unsafe_fn)]
