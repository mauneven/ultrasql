//! Expression binder. Split out of `binder/mod.rs` to keep each
//! production source file under the 600-line ceiling required by
//! AGENTS.md §3 while preserving the original public surface
//! (`make_binary` for the rest of the binder).
//!
//! Every entry point is `pub(super)` so other binder submodules can
//! call it; nothing leaves the `binder` module.
//!
//! Hot helpers carry `#[inline]` so cross-module inlining (which the
//! compiler does not do for `pub` items by default in non-LTO
//! builds) preserves the perf characteristics the original
//! single-file layout had.
//!
//! The implementation is carved into cohesive topic submodules; this
//! `mod.rs` keeps the shared imports, the collation/predicate types,
//! and the `pub(super)` re-exports that reconstitute the original
//! flat `expr_bind` namespace for the rest of the `binder` module.

// These imports are re-exported (`pub(super) use`) so the topic
// submodules can pull the whole shared prelude in with a single
// `use super::*;`, mirroring the flat single-file layout they came
// from.
pub(super) use num_traits::ToPrimitive;
pub(super) use ultrasql_core::{
    BitString, DataType, GeometryType, GeometryValue, MAX_VECTOR_DIMS, Oid, RangeType, RangeValue,
    Value, coerce_bpchar_text, composite_text_matches_arity, parse_decimal_text, parse_money_text,
    parse_time_text, parse_timestamptz_text, parse_timetz_text,
};
pub(super) use ultrasql_parser::ast::{BinaryOp, Expr, Literal, ObjectName, UnaryOp};

pub(super) use super::expr_type::{binary_result_type, comparable, display_unary};
pub(super) use super::{
    Catalog, PlanError, ScalarExpr, Schema, ScopeFrame, ScopeStack, bind_select_with_ctes,
    derive_agg_output_name, is_aggregate_name, is_scalar_min_max_call, parse_pg_identifier_path,
    plan_contains_outer_column,
};

mod between;
mod builtins;
mod cast_type;
mod coerce_common;
mod coerce_type;
mod column;
mod dispatch;
mod literal;
mod numeric;
mod validate;

#[cfg(test)]
mod tests;

// Re-export every topic submodule's items back into the `expr_bind`
// namespace so the historic `expr_bind::<name>` paths used by the rest
// of the `binder` module continue to resolve identically.
pub(super) use between::*;
pub(super) use builtins::*;
pub(super) use cast_type::*;
pub(super) use coerce_common::*;
pub(super) use coerce_type::*;
pub(super) use column::*;
pub(super) use dispatch::*;
pub(super) use literal::*;
pub(super) use numeric::*;
pub(super) use validate::*;

const MICROS_PER_DAY: i64 = 86_400_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BuiltinCollation {
    Default,
    C,
    Posix,
}

impl BuiltinCollation {
    pub(super) const fn oid(self) -> u32 {
        match self {
            Self::Default => 100,
            Self::C => 950,
            Self::Posix => 951,
        }
    }
}
