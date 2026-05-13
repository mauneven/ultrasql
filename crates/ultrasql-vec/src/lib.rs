//! UltraSQL vectorized execution primitives.
//!
//! Column-oriented in-memory format with explicit null bitmaps, length-
//! prefixed varbinary buffers, and aligned numeric storage. Kernels are
//! auto-vectorized; hot paths have hand-written NEON intrinsics for ARM64
//! and AVX2/AVX-512 for x86_64.

#![forbid(unsafe_op_in_unsafe_fn)]
