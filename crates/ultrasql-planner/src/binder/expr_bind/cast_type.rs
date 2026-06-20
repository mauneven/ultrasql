//! CAST target-type resolution from type names, and the
//! cast-result compatibility checks.

use super::*;

pub(in crate::binder) fn resolve_cast_type(type_name: &str) -> Option<DataType> {
    let type_name = type_name.to_ascii_lowercase();
    if let Some(data_type) = parse_vector_family_type_name(&type_name) {
        return Some(data_type);
    }
    if let Some(data_type) = parse_decimal_type_name(&type_name) {
        return Some(data_type);
    }
    if let Some(data_type) = parse_bpchar_type_name(&type_name) {
        return Some(data_type);
    }
    if let Some(data_type) = parse_varchar_type_name(&type_name) {
        return Some(data_type);
    }
    if let Some(data_type) = parse_bit_type_name(&type_name) {
        return Some(data_type);
    }
    if let Some(data_type) = parse_network_type_name(&type_name) {
        return Some(data_type);
    }
    match type_name.as_str() {
        "int" | "integer" | "int4" => Some(DataType::Int32),
        "bigint" | "int8" => Some(DataType::Int64),
        "smallint" | "int2" => Some(DataType::Int16),
        "bool" | "boolean" => Some(DataType::Bool),
        "real" | "float4" => Some(DataType::Float32),
        "double" | "double precision" | "float" | "float8" => Some(DataType::Float64),
        "text" => Some(DataType::Text { max_len: None }),
        "tsvector" => Some(DataType::TsVector),
        "tsquery" => Some(DataType::TsQuery),
        "bytea" => Some(DataType::Bytea),
        "date" => Some(DataType::Date),
        "time" | "time without time zone" => Some(DataType::Time),
        "timetz" | "time with time zone" => Some(DataType::TimeTz),
        "timestamp" | "timestamp without time zone" => Some(DataType::Timestamp),
        "timestamptz" | "timestamp with time zone" => Some(DataType::TimestampTz),
        "uuid" => Some(DataType::Uuid),
        "json" => Some(DataType::Json),
        "jsonb" => Some(DataType::Jsonb),
        "xml" => Some(DataType::Xml),
        "money" => Some(DataType::Money),
        "oid" => Some(DataType::Oid),
        "regnamespace" => Some(DataType::Oid),
        "regclass" => Some(DataType::RegClass),
        "regtype" => Some(DataType::RegType),
        "pg_lsn" => Some(DataType::PgLsn),
        "int4range" => Some(DataType::Range(RangeType::Int4)),
        "int8range" => Some(DataType::Range(RangeType::Int8)),
        "numrange" => Some(DataType::Range(RangeType::Num)),
        "daterange" => Some(DataType::Range(RangeType::Date)),
        "tsrange" => Some(DataType::Range(RangeType::Timestamp)),
        "tstzrange" => Some(DataType::Range(RangeType::TimestampTz)),
        "point" => Some(DataType::Geometry(GeometryType::Point)),
        "box" => Some(DataType::Geometry(GeometryType::Box)),
        "circle" => Some(DataType::Geometry(GeometryType::Circle)),
        "line" => Some(DataType::Geometry(GeometryType::Line)),
        "lseg" => Some(DataType::Geometry(GeometryType::Lseg)),
        "path" => Some(DataType::Geometry(GeometryType::Path)),
        "polygon" => Some(DataType::Geometry(GeometryType::Polygon)),
        _ => None,
    }
}

pub(in crate::binder) const MAX_CAST_NUMERIC_PRECISION: u32 = 131_072;

pub(in crate::binder) fn parse_decimal_type_name(type_name: &str) -> Option<DataType> {
    if matches!(type_name, "numeric" | "decimal") {
        return Some(DataType::Decimal {
            precision: None,
            scale: None,
        });
    }
    let (base, modifiers) = parse_type_modifiers(type_name)?;
    if !matches!(base, "numeric" | "decimal") || modifiers.is_empty() || modifiers.len() > 2 {
        return None;
    }
    let precision = *modifiers.first()?;
    if precision == 0 || precision > MAX_CAST_NUMERIC_PRECISION {
        return None;
    }
    let scale = match modifiers.as_slice() {
        [_] => Some(0),
        [_, scale] => Some(i32::try_from(*scale).ok()?),
        _ => return None,
    };
    Some(DataType::Decimal {
        precision: Some(precision),
        scale,
    })
}

pub(in crate::binder) fn resolve_cast_type_with_catalog(
    type_name: &str,
    catalog: &dyn Catalog,
) -> Option<DataType> {
    resolve_cast_type(type_name).or_else(|| {
        let parts = parse_pg_identifier_path(type_name)?;
        match parts.as_slice() {
            [name] => resolve_cast_type(name).or_else(|| catalog.lookup_type(name)),
            [schema_name, type_name] => {
                if schema_name.eq_ignore_ascii_case("pg_catalog")
                    && let Some(data_type) = resolve_cast_type(type_name)
                {
                    return Some(data_type);
                }
                catalog.lookup_type_in_schema(schema_name, type_name)
            }
            _ => None,
        }
    })
}

pub(in crate::binder) fn parse_network_type_name(type_name: &str) -> Option<DataType> {
    match type_name {
        "inet" => Some(DataType::Inet),
        "cidr" => Some(DataType::Cidr),
        "macaddr" => Some(DataType::MacAddr),
        "macaddr8" => Some(DataType::MacAddr8),
        _ => None,
    }
}

pub(in crate::binder) fn parse_bpchar_type_name(type_name: &str) -> Option<DataType> {
    match type_name {
        "char" | "character" => return Some(DataType::Char { len: Some(1) }),
        "bpchar" => return Some(DataType::Char { len: None }),
        _ => {}
    }
    let (base, len) = parse_single_type_modifier(type_name)?;
    match base {
        "char" | "character" | "bpchar" if len > 0 => Some(DataType::Char { len: Some(len) }),
        _ => None,
    }
}

pub(in crate::binder) fn parse_varchar_type_name(type_name: &str) -> Option<DataType> {
    if type_name == "varchar" {
        return Some(DataType::Text { max_len: None });
    }
    let (base, len) = parse_single_type_modifier(type_name)?;
    (base == "varchar").then_some(DataType::Text { max_len: Some(len) })
}

pub(in crate::binder) fn parse_bit_type_name(type_name: &str) -> Option<DataType> {
    match type_name {
        "bit" => return Some(DataType::Bit { len: Some(1) }),
        "varbit" | "bit varying" => return Some(DataType::VarBit { max_len: None }),
        _ => {}
    }
    let (base, len) = parse_single_type_modifier(type_name)?;
    if len == 0 {
        return None;
    }
    match base {
        "bit" => Some(DataType::Bit { len: Some(len) }),
        "varbit" | "bit varying" => Some(DataType::VarBit { max_len: Some(len) }),
        _ => None,
    }
}

pub(in crate::binder) fn parse_single_type_modifier(type_name: &str) -> Option<(&str, u32)> {
    let (base, modifiers) = parse_type_modifiers(type_name)?;
    let [len] = modifiers.as_slice() else {
        return None;
    };
    Some((base, *len))
}

pub(in crate::binder) fn parse_type_modifiers(type_name: &str) -> Option<(&str, Vec<u32>)> {
    let (base, rest) = type_name.split_once('(')?;
    let raw = rest.strip_suffix(')')?;
    let modifiers = raw
        .split(',')
        .map(str::trim)
        .map(str::parse::<u32>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    Some((base, modifiers))
}

pub(in crate::binder) fn parse_vector_family_type_name(type_name: &str) -> Option<DataType> {
    for base in ["vector", "halfvec", "sparsevec", "bitvec"] {
        if type_name == base {
            return build_vector_family_type(base, None);
        }
        if let Some(dim_text) = type_name
            .strip_prefix(base)
            .and_then(|rest| rest.strip_prefix('('))
            .and_then(|rest| rest.strip_suffix(')'))
        {
            let dims: u32 = dim_text.parse().ok()?;
            if dims == 0 || dims > MAX_VECTOR_DIMS {
                return None;
            }
            return build_vector_family_type(base, Some(dims));
        }
    }
    None
}

pub(in crate::binder) fn build_vector_family_type(
    base: &str,
    dims: Option<u32>,
) -> Option<DataType> {
    match base {
        "vector" => Some(DataType::Vector { dims }),
        "halfvec" => Some(DataType::HalfVec { dims }),
        "sparsevec" => Some(DataType::SparseVec { dims }),
        "bitvec" => Some(DataType::BitVec { dims }),
        _ => None,
    }
}

pub(in crate::binder) fn parse_vector_family_value(target: &DataType, text: &str) -> Option<Value> {
    match target {
        DataType::Vector { .. } => Value::parse_vector(text),
        DataType::HalfVec { .. } => Value::parse_halfvec(text),
        DataType::SparseVec { .. } => Value::parse_sparsevec(text),
        DataType::BitVec { .. } => Value::parse_bitvec(text),
        _ => None,
    }
}

pub(in crate::binder) fn vector_family_cast_matches(target: &DataType, actual: &DataType) -> bool {
    vector_family_kind(target) == vector_family_kind(actual)
        && dims_compatible(
            target.vector_dims().flatten(),
            actual.vector_dims().flatten(),
        )
}

pub(in crate::binder) fn vector_family_kind(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Vector { .. } => Some(0),
        DataType::HalfVec { .. } => Some(1),
        DataType::SparseVec { .. } => Some(2),
        DataType::BitVec { .. } => Some(3),
        _ => None,
    }
}

pub(in crate::binder) const fn dims_compatible(left: Option<u32>, right: Option<u32>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}

pub(in crate::binder) fn cast_result_matches(target: &DataType, actual: &DataType) -> bool {
    target == actual
        || matches!(
            (target, actual),
            (
                DataType::Vector { dims: None },
                DataType::Vector { dims: Some(_) }
            ) | (
                DataType::Decimal {
                    precision: None,
                    scale: None
                },
                DataType::Decimal { .. }
            )
        )
        || (target.is_vector_family()
            && actual.is_vector_family()
            && vector_family_cast_matches(target, actual))
}

pub(in crate::binder) fn coerce_literal_to_match(left: &mut ScalarExpr, right: &mut ScalarExpr) {
    let right_target = right.data_type();
    let left_target = left.data_type();
    coerce_literal_to_type(left, &right_target);
    coerce_literal_to_type(right, &left_target);
}
