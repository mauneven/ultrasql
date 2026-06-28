//! Runtime scalar value representation.
//!
//! `Datum` is the tagged in-memory representation of a single scalar. It
//! is used everywhere a value crosses an executor boundary at row level;
//! column-oriented batch storage uses the dedicated layouts in
//! `ultrasql-vec`.
//!
//! The variants are deliberately *not* zero-cost — each access pays for
//! a discriminant check. That is the right tradeoff for OLTP paths
//! (tuple-at-a-time, type known per row, branch predictor is happy)
//! while OLAP paths bypass this representation entirely.

use num_traits::ToPrimitive;

// Shared imports re-exported with `pub(crate)` so each submodule inherits them
// through `use super::*;`. These mirror the imports of the original single-file
// module; nothing escapes the crate.
pub(crate) use std::fmt;
pub(crate) use std::hash::{Hash, Hasher};

pub(crate) use chrono::{Days, LocalResult, NaiveDate, NaiveTime, Offset, TimeZone};
pub(crate) use chrono_tz::OffsetName;

pub(crate) use crate::bit_string::BitString;
pub(crate) use crate::bpchar::{bpchar_semantic_text, coerce_bpchar_text};
pub(crate) use crate::id::{Lsn, Oid};
pub(crate) use crate::money::{format_money_text, parse_money_text};
pub(crate) use crate::network::NetworkValue;
pub(crate) use crate::types::{DataType, GeometryType, MAX_VECTOR_DIMS, RangeType};

mod convert;
mod datetime;
mod format;
mod geometry;
mod methods;
mod value_enum;
mod xml_dom;
mod xml_validation;
mod xml_value;
mod xml_xpath;

#[cfg(test)]
mod tests;

// Internal glob re-exports so every submodule can reach the crate-internal
// helpers that live in a sibling file via `use super::*;`. These carry
// `pub(crate)` visibility on the underlying items, so nothing escapes the crate.
pub(crate) use datetime::*;
pub(crate) use format::*;
pub(crate) use xml_dom::*;
pub(crate) use xml_validation::*;
pub(crate) use xml_value::*;

pub use datetime::{
    date_parts_from_days, format_date_days, format_interval_pg, format_time_micros,
    format_timestamp_micros, format_timestamptz_micros_in_timezone, format_timestamptz_micros_utc,
    format_timestamptz_micros_with_offset, format_timetz, format_timezone_offset_seconds,
    pack_timetz, parse_date_text, parse_interval_pg, parse_time_text, parse_timestamp_text,
    parse_timestamptz_text, parse_timetz_text, timestamp_micros_at_timezone,
    timestamp_parts_from_micros, timestamptz_display_in_timezone, timetz_at_timezone,
    timetz_utc_micros, unpack_timetz,
};
pub use geometry::{BoundingBox, GeometryValue, RangeValue, SparseVector, TimestampTzDisplay};
pub use value_enum::Value;
pub use xml_validation::{xml_content_is_well_formed, xml_document_is_well_formed};
pub use xml_xpath::{xml_xpath_element_fragments, xml_xpath_element_fragments_with_namespaces};

/// Microseconds in one civil day.
pub const MICROS_PER_DAY: i64 = 86_400_000_000;

pub(crate) const MICROS_PER_HOUR: i64 = 3_600_000_000;
pub(crate) const MICROS_PER_MINUTE: i64 = 60_000_000;
pub(crate) const MICROS_PER_SECOND: i64 = 1_000_000;
pub(crate) const TIMETZ_OFFSET_BITS: u32 = 18;
pub(crate) const TIMETZ_OFFSET_BIAS_SECONDS: i32 = 86_400;
pub(crate) const TIMETZ_OFFSET_MASK: i64 = (1_i64 << TIMETZ_OFFSET_BITS) - 1;
pub(crate) const HEX_ALPHA_BASE: u8 = 10;
pub(crate) const HEX_NIBBLES_PER_BYTE: usize = 2;
pub(crate) const UUID_HEX_NIBBLES: usize = 32;
pub(crate) const BITS_PER_BYTE: usize = 8;
pub(crate) const HIGH_BIT_INDEX: usize = 7;
pub(crate) const COORDINATES_PER_POINT: usize = 2;

#[must_use]
pub(crate) fn i64_to_f64(value: i64) -> f64 {
    value.to_f64().unwrap_or_else(|| {
        if value.is_negative() {
            f64::MIN
        } else {
            f64::MAX
        }
    })
}

#[must_use]
pub(crate) fn usize_to_f64(value: usize) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}

/// Conventional alias used in PostgreSQL literature.
pub type Datum = Value;
