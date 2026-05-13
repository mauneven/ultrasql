//! UltraSQL core — foundational primitives shared across every subsystem.
//!
//! Nothing in this crate depends on any other UltraSQL crate. It is the
//! lowest layer: error type, OIDs, scalar types, datum representation,
//! schema descriptors, and primitive identifiers.
//!
//! Stability: types here are part of the cross-crate ABI; breaking changes
//! must go through the RFC process.

#![forbid(unsafe_op_in_unsafe_fn)]
