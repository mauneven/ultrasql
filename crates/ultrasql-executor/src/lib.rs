//! UltraSQL execution engine.
//!
//! Hybrid push/pull pipeline executor. OLTP point queries use a tuple-at-a-time
//! pull pipeline for minimum latency; OLAP scans use a batched push pipeline
//! with vectorized operators from `ultrasql-vec`. Choice is made at planning
//! time and recorded on the physical plan.

#![forbid(unsafe_op_in_unsafe_fn)]
