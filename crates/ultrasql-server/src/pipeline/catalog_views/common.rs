//! Shared constants and value/schema helpers for the virtual catalog scans.
//!
//! These items are private to the `catalog_views` module tree and reused by
//! every per-relation submodule.

use ultrasql_core::{DataType, Field, Oid, RangeType, Schema, Value};

use crate::auth::pg_authid::AuthCatalog;
use crate::pipeline::LowerCtx;

pub(super) const PG_CATALOG_OID: i64 = 11;
pub(super) const INFORMATION_SCHEMA_OID: i64 = 12;
pub(super) const PUBLIC_OID: i64 = 2200;
pub(super) const PG_CLASS_OID: i64 = 1259;
pub(super) const PG_CONSTRAINT_OID: i64 = 2606;
pub(super) const PG_COLLATION_DEFAULT_OID: u32 = 100;
pub(super) const COLUMN_COLLATION_OPTION_PREFIX: &str = "ultrasql.attcollation.";
pub(super) const PG_PROC_BASE_OID: u32 = 9_000;
pub(super) const PROC_TYPE_BOOL: u32 = 16;
pub(super) const PROC_TYPE_INT4: u32 = 23;
pub(super) const PROC_TYPE_INT8: u32 = 20;
pub(super) const PROC_TYPE_FLOAT8: u32 = 701;
pub(super) const PROC_TYPE_TEXT: u32 = 25;
pub(super) const PROC_TYPE_OID: u32 = 26;
pub(super) const PROC_TYPE_TEXT_ARRAY: u32 = 1009;
pub(super) const PROC_TYPE_XML: u32 = 142;
pub(super) const PROC_TYPE_XML_ARRAY: u32 = 143;
pub(super) const PROC_TYPE_TSVECTOR: u32 = 3614;
pub(super) const PROC_TYPE_TSQUERY: u32 = 3615;
pub(super) const PROC_TYPE_UUID: u32 = 2950;
pub(super) const PG_TYPE_BOOL: i32 = 16;
pub(super) const PG_TYPE_BOOL_ARRAY: i32 = 1000;
pub(super) const PG_TYPE_INT2: i32 = 21;
pub(super) const PG_TYPE_INT2_ARRAY: i32 = 1005;
pub(super) const PG_TYPE_INT4: i32 = 23;
pub(super) const PG_TYPE_INT4_ARRAY: i32 = 1007;
pub(super) const PG_TYPE_INT8: i32 = 20;
pub(super) const PG_TYPE_INT8_ARRAY: i32 = 1016;
pub(super) const PG_TYPE_FLOAT4: i32 = 700;
pub(super) const PG_TYPE_FLOAT4_ARRAY: i32 = 1021;
pub(super) const PG_TYPE_FLOAT8: i32 = 701;
pub(super) const PG_TYPE_FLOAT8_ARRAY: i32 = 1022;
pub(super) const PG_TYPE_TEXT: i32 = 25;
pub(super) const PG_TYPE_TEXT_ARRAY: i32 = 1009;
pub(super) const PG_TYPE_OID: i32 = 26;
pub(super) const PG_TYPE_OID_ARRAY: i32 = 1028;
pub(super) const PG_TYPE_REGCLASS: i32 = 2205;
pub(super) const PG_TYPE_REGCLASS_ARRAY: i32 = 2210;
pub(super) const PG_TYPE_REGTYPE: i32 = 2206;
pub(super) const PG_TYPE_REGTYPE_ARRAY: i32 = 2211;
pub(super) const PG_TYPE_PG_LSN: i32 = 3220;
pub(super) const PG_TYPE_PG_LSN_ARRAY: i32 = 3221;
pub(super) const PG_TYPE_BPCHAR: i32 = 1042;
pub(super) const PG_TYPE_BPCHAR_ARRAY: i32 = 1014;
pub(super) const PG_TYPE_BIT: i32 = 1560;
pub(super) const PG_TYPE_BIT_ARRAY: i32 = 1561;
pub(super) const PG_TYPE_VARBIT: i32 = 1562;
pub(super) const PG_TYPE_VARBIT_ARRAY: i32 = 1563;
pub(super) const PG_TYPE_CIDR: i32 = 650;
pub(super) const PG_TYPE_CIDR_ARRAY: i32 = 651;
pub(super) const PG_TYPE_INET: i32 = 869;
pub(super) const PG_TYPE_INET_ARRAY: i32 = 1041;
pub(super) const PG_TYPE_MACADDR: i32 = 829;
pub(super) const PG_TYPE_MACADDR_ARRAY: i32 = 1040;
pub(super) const PG_TYPE_MACADDR8: i32 = 774;
pub(super) const PG_TYPE_MACADDR8_ARRAY: i32 = 775;
pub(super) const PG_TYPE_NUMERIC: i32 = 1700;
pub(super) const PG_TYPE_NUMERIC_ARRAY: i32 = 1231;
pub(super) const PG_TYPE_MONEY: i32 = 790;
pub(super) const PG_TYPE_MONEY_ARRAY: i32 = 791;
pub(super) const PG_TYPE_INT4RANGE: i32 = 3904;
pub(super) const PG_TYPE_INT4RANGE_ARRAY: i32 = 3905;
pub(super) const PG_TYPE_NUMRANGE: i32 = 3906;
pub(super) const PG_TYPE_NUMRANGE_ARRAY: i32 = 3907;
pub(super) const PG_TYPE_TSRANGE: i32 = 3908;
pub(super) const PG_TYPE_TSRANGE_ARRAY: i32 = 3909;
pub(super) const PG_TYPE_TSTZRANGE: i32 = 3910;
pub(super) const PG_TYPE_TSTZRANGE_ARRAY: i32 = 3911;
pub(super) const PG_TYPE_DATERANGE: i32 = 3912;
pub(super) const PG_TYPE_DATERANGE_ARRAY: i32 = 3913;
pub(super) const PG_TYPE_INT8RANGE: i32 = 3926;
pub(super) const PG_TYPE_INT8RANGE_ARRAY: i32 = 3927;
pub(super) const PG_TYPE_DATE: i32 = 1082;
pub(super) const PG_TYPE_DATE_ARRAY: i32 = 1182;
pub(super) const PG_TYPE_TIMESTAMP: i32 = 1114;
pub(super) const PG_TYPE_TIMESTAMP_ARRAY: i32 = 1115;
pub(super) const PG_TYPE_TIMESTAMPTZ: i32 = 1184;
pub(super) const PG_TYPE_TIMESTAMPTZ_ARRAY: i32 = 1185;
pub(super) const PG_TYPE_TIME: i32 = 1083;
pub(super) const PG_TYPE_TIME_ARRAY: i32 = 1183;
pub(super) const PG_TYPE_TIMETZ: i32 = 1266;
pub(super) const PG_TYPE_TIMETZ_ARRAY: i32 = 1270;
pub(super) const PG_TYPE_UUID: i32 = 2950;
pub(super) const PG_TYPE_UUID_ARRAY: i32 = 2951;
pub(super) const PG_TYPE_JSON: i32 = 114;
pub(super) const PG_TYPE_JSON_ARRAY: i32 = 199;
pub(super) const PG_TYPE_JSONB: i32 = 3802;
pub(super) const PG_TYPE_JSONB_ARRAY: i32 = 3807;
pub(super) const PG_TYPE_XML: i32 = 142;
pub(super) const PG_TYPE_XML_ARRAY: i32 = 143;
pub(super) const PG_TYPE_TSVECTOR: i32 = 3614;
pub(super) const PG_TYPE_TSVECTOR_ARRAY: i32 = 3643;
pub(super) const PG_TYPE_TSQUERY: i32 = 3615;
pub(super) const PG_TYPE_TSQUERY_ARRAY: i32 = 3645;
pub(super) const PG_TYPE_BYTEA: i32 = 17;
pub(super) const PG_TYPE_BYTEA_ARRAY: i32 = 1001;

pub(super) fn schema(fields: impl IntoIterator<Item = Field>) -> Schema {
    match Schema::new(fields) {
        Ok(schema) => schema,
        Err(err) => {
            tracing::error!(error = %err, "virtual catalog schema construction failed");
            Schema::empty()
        }
    }
}

pub(super) fn text() -> DataType {
    DataType::Text { max_len: None }
}

pub(super) fn text_array() -> DataType {
    DataType::Array(Box::new(text()))
}

pub(super) fn v_text(v: impl Into<String>) -> Value {
    Value::Text(v.into())
}

pub(super) fn v_i64(v: u32) -> Value {
    Value::Int64(i64::from(v))
}

pub(super) fn v_oid(v: u32) -> Value {
    Value::Oid(Oid::new(v))
}

pub(super) fn v_oid_i32(v: i32) -> Value {
    match u32::try_from(v) {
        Ok(raw) => v_oid(raw),
        Err(err) => {
            tracing::error!(value = v, error = %err, "virtual catalog OID conversion failed");
            v_oid(0)
        }
    }
}

pub(super) fn relation_type_oid(rel_oid: Oid) -> u32 {
    rel_oid.raw().checked_add(1_000_000_000).unwrap_or(0)
}

pub(super) fn namespace_oid(schema_name: &str) -> i64 {
    match schema_name {
        "pg_catalog" => PG_CATALOG_OID,
        "information_schema" => INFORMATION_SCHEMA_OID,
        "public" => PUBLIC_OID,
        other => i64::from(crate::runtime_schema_oid(other)),
    }
}

pub(super) fn namespace_owner_oid(ctx: &LowerCtx<'_>, owner_role: &str) -> i64 {
    ctx.role_catalog
        .lookup_role(owner_role)
        .map_or(10, |role| i64::from(role.oid))
}

pub(super) fn runtime_schema_rows(ctx: &LowerCtx<'_>) -> Vec<(String, String, i64)> {
    let mut rows = ctx
        .schemas
        .iter()
        .map(|entry| {
            let schema = entry.value();
            (
                schema.name.clone(),
                schema.owner_role.clone(),
                i64::from(crate::runtime_schema_oid(&schema.name)),
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows
}

pub(super) fn namespace_oid_u32(namespace: &str) -> u32 {
    if namespace.eq_ignore_ascii_case("pg_catalog") {
        u32::try_from(PG_CATALOG_OID).unwrap_or(0)
    } else if namespace.eq_ignore_ascii_case("information_schema") {
        u32::try_from(INFORMATION_SCHEMA_OID).unwrap_or(0)
    } else if namespace.eq_ignore_ascii_case("public") {
        u32::try_from(PUBLIC_OID).unwrap_or(0)
    } else {
        crate::runtime_schema_oid(&namespace.to_ascii_lowercase())
    }
}

pub(super) fn type_oid(dt: &DataType) -> i32 {
    match dt {
        DataType::Bool => PG_TYPE_BOOL,
        DataType::Int16 => PG_TYPE_INT2,
        DataType::Int32 => PG_TYPE_INT4,
        DataType::Int64 => PG_TYPE_INT8,
        DataType::Float32 => PG_TYPE_FLOAT4,
        DataType::Float64 => PG_TYPE_FLOAT8,
        DataType::Decimal { .. } => PG_TYPE_NUMERIC,
        DataType::Money => PG_TYPE_MONEY,
        DataType::Oid => PG_TYPE_OID,
        DataType::RegClass => PG_TYPE_REGCLASS,
        DataType::RegType => PG_TYPE_REGTYPE,
        DataType::PgLsn => PG_TYPE_PG_LSN,
        DataType::Char { .. } => PG_TYPE_BPCHAR,
        DataType::Bit { .. } => PG_TYPE_BIT,
        DataType::VarBit { .. } => PG_TYPE_VARBIT,
        DataType::Inet => PG_TYPE_INET,
        DataType::Cidr => PG_TYPE_CIDR,
        DataType::MacAddr => PG_TYPE_MACADDR,
        DataType::MacAddr8 => PG_TYPE_MACADDR8,
        DataType::Date => PG_TYPE_DATE,
        DataType::Timestamp => PG_TYPE_TIMESTAMP,
        DataType::TimestampTz => PG_TYPE_TIMESTAMPTZ,
        DataType::Time => PG_TYPE_TIME,
        DataType::TimeTz => PG_TYPE_TIMETZ,
        DataType::Uuid => PG_TYPE_UUID,
        DataType::Json => PG_TYPE_JSON,
        DataType::Jsonb => PG_TYPE_JSONB,
        DataType::Xml => PG_TYPE_XML,
        DataType::TsVector => PG_TYPE_TSVECTOR,
        DataType::TsQuery => PG_TYPE_TSQUERY,
        DataType::Bytea => PG_TYPE_BYTEA,
        DataType::Range(range_type) => range_type_oid(*range_type),
        DataType::Enum { oid, .. }
        | DataType::Composite { oid, .. }
        | DataType::Domain { oid, .. } => i32::try_from(oid.raw()).unwrap_or(PG_TYPE_TEXT),
        _ => PG_TYPE_TEXT,
    }
}

pub(super) fn type_collation_oid(dt: &DataType) -> u32 {
    match dt {
        DataType::Text { .. } | DataType::Char { .. } => PG_COLLATION_DEFAULT_OID,
        DataType::Domain { base_type, .. } => type_collation_oid(base_type),
        _ => 0,
    }
}

pub(super) fn attribute_collation_oid(entry: &ultrasql_catalog::TableEntry, idx: usize) -> u32 {
    let key = format!("{COLUMN_COLLATION_OPTION_PREFIX}{idx}");
    entry
        .options
        .iter()
        .find_map(|(name, value)| {
            if name == &key {
                value.parse::<u32>().ok()
            } else {
                None
            }
        })
        .unwrap_or_else(|| type_collation_oid(&entry.schema.field_at(idx).data_type))
}

pub(super) fn range_type_oid(range_type: RangeType) -> i32 {
    match range_type {
        RangeType::Int4 => PG_TYPE_INT4RANGE,
        RangeType::Int8 => PG_TYPE_INT8RANGE,
        RangeType::Num => PG_TYPE_NUMRANGE,
        RangeType::Date => PG_TYPE_DATERANGE,
        RangeType::Timestamp => PG_TYPE_TSRANGE,
        RangeType::TimestampTz => PG_TYPE_TSTZRANGE,
    }
}

pub(super) fn data_type_name(dt: &DataType) -> std::borrow::Cow<'static, str> {
    match dt {
        DataType::Bool => "boolean".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "integer".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Float32 => "real".into(),
        DataType::Float64 => "double precision".into(),
        DataType::Decimal { .. } => "numeric".into(),
        DataType::Money => "money".into(),
        DataType::Oid => "oid".into(),
        DataType::RegClass => "regclass".into(),
        DataType::RegType => "regtype".into(),
        DataType::PgLsn => "pg_lsn".into(),
        DataType::Text { .. } => "text".into(),
        DataType::Char { .. } => "character".into(),
        DataType::Enum { name, .. } => name.to_string().into(),
        DataType::Composite { name, .. } => name.to_string().into(),
        DataType::Domain { name, .. } => name.to_string().into(),
        DataType::Bit { .. } => "bit".into(),
        DataType::VarBit { .. } => "bit varying".into(),
        DataType::Inet => "inet".into(),
        DataType::Cidr => "cidr".into(),
        DataType::MacAddr => "macaddr".into(),
        DataType::MacAddr8 => "macaddr8".into(),
        DataType::Bytea => "bytea".into(),
        DataType::Timestamp => "timestamp without time zone".into(),
        DataType::TimestampTz => "timestamp with time zone".into(),
        DataType::Date => "date".into(),
        DataType::Time => "time without time zone".into(),
        DataType::TimeTz => "time with time zone".into(),
        DataType::Interval => "interval".into(),
        DataType::Uuid => "uuid".into(),
        DataType::Json => "json".into(),
        DataType::Jsonb => "jsonb".into(),
        DataType::Xml => "xml".into(),
        DataType::TsVector => "tsvector".into(),
        DataType::TsQuery => "tsquery".into(),
        DataType::Vector { .. } => "vector".into(),
        DataType::HalfVec { .. } => "halfvec".into(),
        DataType::SparseVec { .. } => "sparsevec".into(),
        DataType::BitVec { .. } => "bitvec".into(),
        DataType::Array(_) => "array".into(),
        DataType::Record(_) => "record".into(),
        DataType::Null => "unknown".into(),
        _ => "text".into(),
    }
}

pub(super) fn table_entries(ctx: &LowerCtx<'_>) -> Vec<ultrasql_catalog::TableEntry> {
    let mut entries: Vec<ultrasql_catalog::TableEntry> =
        ctx.catalog_snapshot.tables.values().cloned().collect();
    entries.sort_by(|a, b| {
        (a.schema_name.as_str(), a.name.as_str()).cmp(&(b.schema_name.as_str(), b.name.as_str()))
    });
    entries
}

pub(super) fn is_materialized_view_entry(entry: &ultrasql_catalog::TableEntry) -> bool {
    entry
        .options
        .iter()
        .any(|(key, value)| key == "ultrasql.relkind" && value == "materialized_view")
}

pub(super) fn is_regular_view_entry(entry: &ultrasql_catalog::TableEntry) -> bool {
    entry
        .options
        .iter()
        .any(|(key, value)| key == "ultrasql.relkind" && value == "view")
}
