//! Virtual `pg_catalog` and `information_schema` relations.
//!
//! These scans expose metadata from the same catalog snapshot used by the
//! binder. They are deliberately read-only and statement-local: a SELECT sees
//! the snapshot captured at statement start, matching normal catalog lookup.

use std::collections::HashMap;

use ultrasql_core::{DataType, Field, Oid, RangeType, Schema, Value};
use ultrasql_executor::{MemTableScan, Operator, build_batch};
use ultrasql_mvcc::{Visibility, XidStatusOracle, is_visible};
use ultrasql_planner::LogicalReferentialAction;
use ultrasql_txn::{IsolationLevel, LockMode, LockTag};

use crate::auth::pg_authid::AuthCatalog;
use crate::error::ServerError;

use super::LowerCtx;

const PG_CATALOG_OID: i64 = 11;
const INFORMATION_SCHEMA_OID: i64 = 12;
const PUBLIC_OID: i64 = 2200;
const PG_CLASS_OID: i64 = 1259;
const PG_CONSTRAINT_OID: i64 = 2606;
const PG_COLLATION_DEFAULT_OID: u32 = 100;
const COLUMN_COLLATION_OPTION_PREFIX: &str = "ultrasql.attcollation.";
const PG_PROC_BASE_OID: u32 = 9_000;
const PROC_TYPE_BOOL: u32 = 16;
const PROC_TYPE_INT4: u32 = 23;
const PROC_TYPE_INT8: u32 = 20;
const PROC_TYPE_TEXT: u32 = 25;
const PROC_TYPE_OID: u32 = 26;
const PROC_TYPE_TEXT_ARRAY: u32 = 1009;
const PROC_TYPE_XML: u32 = 142;
const PROC_TYPE_XML_ARRAY: u32 = 143;
const PROC_TYPE_UUID: u32 = 2950;
const PG_TYPE_BOOL: i32 = 16;
const PG_TYPE_BOOL_ARRAY: i32 = 1000;
const PG_TYPE_INT2: i32 = 21;
const PG_TYPE_INT2_ARRAY: i32 = 1005;
const PG_TYPE_INT4: i32 = 23;
const PG_TYPE_INT4_ARRAY: i32 = 1007;
const PG_TYPE_INT8: i32 = 20;
const PG_TYPE_INT8_ARRAY: i32 = 1016;
const PG_TYPE_FLOAT4: i32 = 700;
const PG_TYPE_FLOAT4_ARRAY: i32 = 1021;
const PG_TYPE_FLOAT8: i32 = 701;
const PG_TYPE_FLOAT8_ARRAY: i32 = 1022;
const PG_TYPE_TEXT: i32 = 25;
const PG_TYPE_TEXT_ARRAY: i32 = 1009;
const PG_TYPE_OID: i32 = 26;
const PG_TYPE_OID_ARRAY: i32 = 1028;
const PG_TYPE_REGCLASS: i32 = 2205;
const PG_TYPE_REGCLASS_ARRAY: i32 = 2210;
const PG_TYPE_REGTYPE: i32 = 2206;
const PG_TYPE_REGTYPE_ARRAY: i32 = 2211;
const PG_TYPE_PG_LSN: i32 = 3220;
const PG_TYPE_PG_LSN_ARRAY: i32 = 3221;
const PG_TYPE_BPCHAR: i32 = 1042;
const PG_TYPE_BPCHAR_ARRAY: i32 = 1014;
const PG_TYPE_BIT: i32 = 1560;
const PG_TYPE_BIT_ARRAY: i32 = 1561;
const PG_TYPE_VARBIT: i32 = 1562;
const PG_TYPE_VARBIT_ARRAY: i32 = 1563;
const PG_TYPE_CIDR: i32 = 650;
const PG_TYPE_CIDR_ARRAY: i32 = 651;
const PG_TYPE_INET: i32 = 869;
const PG_TYPE_INET_ARRAY: i32 = 1041;
const PG_TYPE_MACADDR: i32 = 829;
const PG_TYPE_MACADDR_ARRAY: i32 = 1040;
const PG_TYPE_MACADDR8: i32 = 774;
const PG_TYPE_MACADDR8_ARRAY: i32 = 775;
const PG_TYPE_NUMERIC: i32 = 1700;
const PG_TYPE_NUMERIC_ARRAY: i32 = 1231;
const PG_TYPE_MONEY: i32 = 790;
const PG_TYPE_MONEY_ARRAY: i32 = 791;
const PG_TYPE_INT4RANGE: i32 = 3904;
const PG_TYPE_INT4RANGE_ARRAY: i32 = 3905;
const PG_TYPE_NUMRANGE: i32 = 3906;
const PG_TYPE_NUMRANGE_ARRAY: i32 = 3907;
const PG_TYPE_TSRANGE: i32 = 3908;
const PG_TYPE_TSRANGE_ARRAY: i32 = 3909;
const PG_TYPE_TSTZRANGE: i32 = 3910;
const PG_TYPE_TSTZRANGE_ARRAY: i32 = 3911;
const PG_TYPE_DATERANGE: i32 = 3912;
const PG_TYPE_DATERANGE_ARRAY: i32 = 3913;
const PG_TYPE_INT8RANGE: i32 = 3926;
const PG_TYPE_INT8RANGE_ARRAY: i32 = 3927;
const PG_TYPE_DATE: i32 = 1082;
const PG_TYPE_DATE_ARRAY: i32 = 1182;
const PG_TYPE_TIMESTAMP: i32 = 1114;
const PG_TYPE_TIMESTAMP_ARRAY: i32 = 1115;
const PG_TYPE_TIMESTAMPTZ: i32 = 1184;
const PG_TYPE_TIMESTAMPTZ_ARRAY: i32 = 1185;
const PG_TYPE_TIME: i32 = 1083;
const PG_TYPE_TIME_ARRAY: i32 = 1183;
const PG_TYPE_TIMETZ: i32 = 1266;
const PG_TYPE_TIMETZ_ARRAY: i32 = 1270;
const PG_TYPE_UUID: i32 = 2950;
const PG_TYPE_UUID_ARRAY: i32 = 2951;
const PG_TYPE_JSON: i32 = 114;
const PG_TYPE_JSON_ARRAY: i32 = 199;
const PG_TYPE_JSONB: i32 = 3802;
const PG_TYPE_JSONB_ARRAY: i32 = 3807;
const PG_TYPE_XML: i32 = 142;
const PG_TYPE_XML_ARRAY: i32 = 143;
const PG_TYPE_TSVECTOR: i32 = 3614;
const PG_TYPE_TSVECTOR_ARRAY: i32 = 3643;
const PG_TYPE_TSQUERY: i32 = 3615;
const PG_TYPE_TSQUERY_ARRAY: i32 = 3645;
const PG_TYPE_BYTEA: i32 = 17;
const PG_TYPE_BYTEA_ARRAY: i32 = 1001;

/// Return the schema for a virtual catalog relation or view.
#[must_use]
pub(crate) fn virtual_catalog_schema(name: &str) -> Option<Schema> {
    match normalized_name(name).as_str() {
        "pg_catalog.pg_namespace" => Some(schema_pg_namespace()),
        "pg_catalog.pg_class" => Some(schema_pg_class()),
        "pg_catalog.pg_attribute" => Some(schema_pg_attribute()),
        "pg_catalog.pg_attrdef" => Some(schema_pg_attrdef()),
        "pg_catalog.pg_type" => Some(schema_pg_type()),
        "pg_catalog.pg_am" => Some(schema_pg_am()),
        "pg_catalog.pg_range" => Some(schema_pg_range()),
        "pg_catalog.pg_collation" => Some(schema_pg_collation()),
        "pg_catalog.pg_enum" => Some(schema_pg_enum()),
        "pg_catalog.pg_index" => Some(schema_pg_index()),
        "pg_catalog.pg_inherits" => Some(schema_pg_inherits()),
        "pg_catalog.pg_constraint" => Some(schema_pg_constraint()),
        "pg_catalog.pg_policy" => Some(schema_pg_policy()),
        "pg_catalog.pg_sequence" => Some(schema_pg_sequence()),
        "pg_catalog.pg_operator" => Some(schema_pg_operator()),
        "pg_catalog.pg_depend" => Some(schema_pg_depend()),
        "pg_catalog.pg_description" => Some(schema_pg_description()),
        "pg_catalog.pg_statistic" => Some(schema_pg_statistic()),
        "pg_catalog.pg_statistic_ext" => Some(schema_pg_statistic_ext()),
        "pg_catalog.pg_tables" => Some(schema_pg_tables()),
        "pg_catalog.pg_indexes" => Some(schema_pg_indexes()),
        "pg_catalog.pg_views" => Some(schema_pg_views()),
        "pg_catalog.pg_matviews" => Some(schema_pg_matviews()),
        "pg_catalog.pg_sequences" => Some(schema_pg_sequences()),
        "pg_catalog.pg_roles" => Some(schema_pg_roles()),
        "pg_catalog.pg_auth_members" => Some(schema_pg_auth_members()),
        "pg_catalog.pg_user" => Some(schema_pg_user()),
        "pg_catalog.pg_get_keywords" => Some(schema_pg_get_keywords()),
        "pg_catalog.pg_settings" => Some(schema_pg_settings()),
        "pg_catalog.pg_stat_statements" => Some(schema_pg_stat_statements()),
        "pg_catalog.pg_locks" => Some(schema_pg_locks()),
        "pg_catalog.pg_stat_activity" => Some(schema_pg_stat_activity()),
        "pg_catalog.pg_stat_user_tables" => Some(schema_pg_stat_user_tables()),
        "pg_catalog.pg_stat_user_indexes" => Some(schema_pg_stat_user_indexes()),
        "pg_catalog.pg_statio_user_tables" => Some(schema_pg_statio_user_tables()),
        "pg_catalog.pg_statio_user_indexes" => Some(schema_pg_statio_user_indexes()),
        "pg_catalog.pg_stat_database" => Some(schema_pg_stat_database()),
        "pg_catalog.pg_stat_bgwriter" => Some(schema_pg_stat_bgwriter()),
        "pg_catalog.pg_stat_wal" => Some(schema_pg_stat_wal()),
        "pg_catalog.pg_stat_progress_vacuum" => Some(schema_pg_stat_progress_vacuum()),
        "pg_catalog.pg_stat_progress_analyze" => Some(schema_pg_stat_progress_analyze()),
        "pg_catalog.pg_stat_progress_create_index" => Some(schema_pg_stat_progress_create_index()),
        "pg_catalog.pg_replication_slots" => Some(schema_pg_replication_slots()),
        "pg_catalog.pg_stat_replication" => Some(schema_pg_stat_replication()),
        "pg_catalog.pg_stat_subscription" => Some(schema_pg_stat_subscription()),
        "pg_catalog.pg_publication" => Some(schema_pg_publication()),
        "pg_catalog.pg_subscription" => Some(schema_pg_subscription()),
        "pg_catalog.pg_publication_rel" => Some(schema_pg_publication_rel()),
        "pg_catalog.pg_publication_tables" => Some(schema_pg_publication_tables()),
        "pg_catalog.pg_proc" => Some(schema_pg_proc()),
        "pg_catalog.pg_database" => Some(schema_pg_database()),
        "information_schema.tables" => Some(schema_information_schema_tables()),
        "information_schema.columns" => Some(schema_information_schema_columns()),
        "information_schema.table_constraints" => {
            Some(schema_information_schema_table_constraints())
        }
        "information_schema.key_column_usage" => Some(schema_information_schema_key_column_usage()),
        "information_schema.referential_constraints" => {
            Some(schema_information_schema_referential_constraints())
        }
        "information_schema.check_constraints" => {
            Some(schema_information_schema_check_constraints())
        }
        "information_schema.schemata" => Some(schema_information_schema_schemata()),
        "information_schema.sequences" => Some(schema_information_schema_sequences()),
        "information_schema.routines" => Some(schema_information_schema_routines()),
        "information_schema.triggers" => Some(schema_information_schema_triggers()),
        _ => None,
    }
}

/// Build a scan for a virtual catalog relation when `table` names one.
pub(super) fn try_virtual_catalog_scan(
    table: &str,
    ctx: &LowerCtx<'_>,
) -> Result<Option<Box<dyn Operator>>, ServerError> {
    let normalized = normalized_name(table);
    let Some((schema, rows)) = virtual_rows(&normalized, ctx) else {
        return Ok(None);
    };
    let batches = if rows.is_empty() {
        Vec::new()
    } else {
        vec![build_batch(&rows, &schema)?]
    };
    Ok(Some(Box::new(MemTableScan::new(schema, batches))))
}

fn virtual_rows(name: &str, ctx: &LowerCtx<'_>) -> Option<(Schema, Vec<Vec<Value>>)> {
    match name {
        "pg_catalog.pg_namespace" => Some((schema_pg_namespace(), rows_pg_namespace(ctx))),
        "pg_catalog.pg_class" => Some((schema_pg_class(), rows_pg_class(ctx))),
        "pg_catalog.pg_attribute" => Some((schema_pg_attribute(), rows_pg_attribute(ctx))),
        "pg_catalog.pg_attrdef" => Some((schema_pg_attrdef(), rows_pg_attrdef(ctx))),
        "pg_catalog.pg_type" => Some((schema_pg_type(), rows_pg_type(ctx))),
        "pg_catalog.pg_am" => Some((schema_pg_am(), rows_pg_am())),
        "pg_catalog.pg_range" => Some((schema_pg_range(), rows_pg_range())),
        "pg_catalog.pg_collation" => Some((schema_pg_collation(), rows_pg_collation())),
        "pg_catalog.pg_enum" => Some((schema_pg_enum(), rows_pg_enum(ctx))),
        "pg_catalog.pg_index" => Some((schema_pg_index(), rows_pg_index(ctx))),
        "pg_catalog.pg_inherits" => Some((schema_pg_inherits(), Vec::new())),
        "pg_catalog.pg_constraint" => Some((schema_pg_constraint(), rows_pg_constraint(ctx))),
        "pg_catalog.pg_policy" => Some((schema_pg_policy(), rows_pg_policy(ctx))),
        "pg_catalog.pg_sequence" => Some((schema_pg_sequence(), rows_pg_sequence(ctx))),
        "pg_catalog.pg_operator" => Some((schema_pg_operator(), rows_pg_operator(ctx))),
        "pg_catalog.pg_depend" => Some((schema_pg_depend(), rows_pg_depend(ctx))),
        "pg_catalog.pg_description" => Some((schema_pg_description(), rows_pg_description(ctx))),
        "pg_catalog.pg_statistic" => Some((schema_pg_statistic(), rows_pg_statistic(ctx))),
        "pg_catalog.pg_statistic_ext" => {
            Some((schema_pg_statistic_ext(), rows_pg_statistic_ext(ctx)))
        }
        "pg_catalog.pg_tables" => Some((schema_pg_tables(), rows_pg_tables(ctx))),
        "pg_catalog.pg_indexes" => Some((schema_pg_indexes(), rows_pg_indexes(ctx))),
        "pg_catalog.pg_views" => Some((schema_pg_views(), Vec::new())),
        "pg_catalog.pg_matviews" => Some((schema_pg_matviews(), rows_pg_matviews(ctx))),
        "pg_catalog.pg_sequences" => Some((schema_pg_sequences(), rows_pg_sequences(ctx))),
        "pg_catalog.pg_roles" => Some((schema_pg_roles(), rows_pg_roles(ctx))),
        "pg_catalog.pg_auth_members" => Some((schema_pg_auth_members(), rows_pg_auth_members(ctx))),
        "pg_catalog.pg_user" => Some((schema_pg_user(), rows_pg_user(ctx))),
        "pg_catalog.pg_get_keywords" => Some((schema_pg_get_keywords(), rows_pg_get_keywords())),
        "pg_catalog.pg_settings" => Some((schema_pg_settings(), rows_pg_settings(ctx))),
        "pg_catalog.pg_stat_statements" => {
            Some((schema_pg_stat_statements(), rows_pg_stat_statements(ctx)))
        }
        "pg_catalog.pg_locks" => Some((schema_pg_locks(), rows_pg_locks(ctx))),
        "pg_catalog.pg_stat_activity" => {
            Some((schema_pg_stat_activity(), rows_pg_stat_activity(ctx)))
        }
        "pg_catalog.pg_stat_user_tables" => {
            Some((schema_pg_stat_user_tables(), rows_pg_stat_user_tables(ctx)))
        }
        "pg_catalog.pg_stat_user_indexes" => Some((
            schema_pg_stat_user_indexes(),
            rows_pg_stat_user_indexes(ctx),
        )),
        "pg_catalog.pg_statio_user_tables" => Some((
            schema_pg_statio_user_tables(),
            rows_pg_statio_user_tables(ctx),
        )),
        "pg_catalog.pg_statio_user_indexes" => Some((
            schema_pg_statio_user_indexes(),
            rows_pg_statio_user_indexes(ctx),
        )),
        "pg_catalog.pg_stat_database" => {
            Some((schema_pg_stat_database(), rows_pg_stat_database(ctx)))
        }
        "pg_catalog.pg_stat_bgwriter" => {
            Some((schema_pg_stat_bgwriter(), rows_pg_stat_bgwriter(ctx)))
        }
        "pg_catalog.pg_stat_wal" => Some((schema_pg_stat_wal(), rows_pg_stat_wal(ctx))),
        "pg_catalog.pg_stat_progress_vacuum" => Some((
            schema_pg_stat_progress_vacuum(),
            rows_pg_stat_progress_vacuum(ctx),
        )),
        "pg_catalog.pg_stat_progress_analyze" => Some((
            schema_pg_stat_progress_analyze(),
            rows_pg_stat_progress_analyze(ctx),
        )),
        "pg_catalog.pg_stat_progress_create_index" => Some((
            schema_pg_stat_progress_create_index(),
            rows_pg_stat_progress_create_index(ctx),
        )),
        "pg_catalog.pg_replication_slots" => Some((
            schema_pg_replication_slots(),
            rows_pg_replication_slots(ctx),
        )),
        "pg_catalog.pg_stat_replication" => {
            Some((schema_pg_stat_replication(), rows_pg_stat_replication(ctx)))
        }
        "pg_catalog.pg_stat_subscription" => Some((
            schema_pg_stat_subscription(),
            rows_pg_stat_subscription(ctx),
        )),
        "pg_catalog.pg_publication" => Some((schema_pg_publication(), rows_pg_publication(ctx))),
        "pg_catalog.pg_subscription" => Some((schema_pg_subscription(), rows_pg_subscription(ctx))),
        "pg_catalog.pg_publication_rel" => {
            Some((schema_pg_publication_rel(), rows_pg_publication_rel(ctx)))
        }
        "pg_catalog.pg_publication_tables" => Some((
            schema_pg_publication_tables(),
            rows_pg_publication_tables(ctx),
        )),
        "pg_catalog.pg_proc" => Some((schema_pg_proc(), rows_pg_proc())),
        "pg_catalog.pg_database" => Some((schema_pg_database(), rows_pg_database())),
        "information_schema.tables" => Some((
            schema_information_schema_tables(),
            rows_information_schema_tables(ctx),
        )),
        "information_schema.columns" => Some((
            schema_information_schema_columns(),
            rows_information_schema_columns(ctx),
        )),
        "information_schema.table_constraints" => Some((
            schema_information_schema_table_constraints(),
            rows_information_schema_table_constraints(ctx),
        )),
        "information_schema.key_column_usage" => Some((
            schema_information_schema_key_column_usage(),
            rows_information_schema_key_column_usage(ctx),
        )),
        "information_schema.referential_constraints" => Some((
            schema_information_schema_referential_constraints(),
            rows_information_schema_referential_constraints(ctx),
        )),
        "information_schema.check_constraints" => Some((
            schema_information_schema_check_constraints(),
            rows_information_schema_check_constraints(ctx),
        )),
        "information_schema.schemata" => Some((
            schema_information_schema_schemata(),
            rows_information_schema_schemata(ctx),
        )),
        "information_schema.sequences" => Some((
            schema_information_schema_sequences(),
            rows_information_schema_sequences(ctx),
        )),
        "information_schema.routines" => Some((
            schema_information_schema_routines(),
            rows_information_schema_routines(),
        )),
        "information_schema.triggers" => Some((schema_information_schema_triggers(), Vec::new())),
        _ => None,
    }
}

fn normalized_name(name: &str) -> String {
    let folded = name.to_ascii_lowercase();
    if folded.contains('.') {
        return folded;
    }
    match folded.as_str() {
        "pg_namespace"
        | "pg_class"
        | "pg_attribute"
        | "pg_attrdef"
        | "pg_type"
        | "pg_am"
        | "pg_range"
        | "pg_collation"
        | "pg_enum"
        | "pg_index"
        | "pg_inherits"
        | "pg_constraint"
        | "pg_policy"
        | "pg_sequence"
        | "pg_operator"
        | "pg_depend"
        | "pg_description"
        | "pg_tables"
        | "pg_indexes"
        | "pg_statistic"
        | "pg_statistic_ext"
        | "pg_views"
        | "pg_matviews"
        | "pg_sequences"
        | "pg_roles"
        | "pg_auth_members"
        | "pg_user"
        | "pg_get_keywords"
        | "pg_settings"
        | "pg_stat_statements"
        | "pg_locks"
        | "pg_stat_activity"
        | "pg_proc"
        | "pg_stat_user_tables"
        | "pg_stat_user_indexes"
        | "pg_statio_user_tables"
        | "pg_statio_user_indexes"
        | "pg_stat_database"
        | "pg_stat_bgwriter"
        | "pg_stat_wal"
        | "pg_stat_progress_vacuum"
        | "pg_stat_progress_analyze"
        | "pg_stat_progress_create_index"
        | "pg_replication_slots"
        | "pg_stat_replication"
        | "pg_stat_subscription"
        | "pg_publication"
        | "pg_subscription"
        | "pg_publication_rel"
        | "pg_publication_tables"
        | "pg_database" => {
            format!("pg_catalog.{folded}")
        }
        "tables"
        | "columns"
        | "table_constraints"
        | "key_column_usage"
        | "referential_constraints"
        | "check_constraints"
        | "schemata"
        | "sequences"
        | "routines"
        | "triggers" => {
            format!("information_schema.{folded}")
        }
        _ => folded,
    }
}

fn schema(fields: impl IntoIterator<Item = Field>) -> Schema {
    match Schema::new(fields) {
        Ok(schema) => schema,
        Err(err) => {
            tracing::error!(error = %err, "virtual catalog schema construction failed");
            Schema::empty()
        }
    }
}

fn text() -> DataType {
    DataType::Text { max_len: None }
}

fn text_array() -> DataType {
    DataType::Array(Box::new(text()))
}

fn v_text(v: impl Into<String>) -> Value {
    Value::Text(v.into())
}

fn v_i64(v: u32) -> Value {
    Value::Int64(i64::from(v))
}

fn v_oid(v: u32) -> Value {
    Value::Oid(Oid::new(v))
}

fn v_oid_i32(v: i32) -> Value {
    match u32::try_from(v) {
        Ok(raw) => v_oid(raw),
        Err(err) => {
            tracing::error!(value = v, error = %err, "virtual catalog OID conversion failed");
            v_oid(0)
        }
    }
}

fn namespace_oid(schema_name: &str) -> i64 {
    match schema_name {
        "pg_catalog" => PG_CATALOG_OID,
        "information_schema" => INFORMATION_SCHEMA_OID,
        "public" => PUBLIC_OID,
        other => i64::from(crate::runtime_schema_oid(other)),
    }
}

fn namespace_owner_oid(ctx: &LowerCtx<'_>, owner_role: &str) -> i64 {
    ctx.role_catalog
        .lookup_role(owner_role)
        .map_or(10, |role| i64::from(role.oid))
}

fn runtime_schema_rows(ctx: &LowerCtx<'_>) -> Vec<(String, String, i64)> {
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

fn namespace_oid_u32(namespace: &str) -> u32 {
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

fn type_oid(dt: &DataType) -> i32 {
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

fn type_collation_oid(dt: &DataType) -> u32 {
    match dt {
        DataType::Text { .. } | DataType::Char { .. } => PG_COLLATION_DEFAULT_OID,
        DataType::Domain { base_type, .. } => type_collation_oid(base_type),
        _ => 0,
    }
}

fn attribute_collation_oid(entry: &ultrasql_catalog::TableEntry, idx: usize) -> u32 {
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

fn range_type_oid(range_type: RangeType) -> i32 {
    match range_type {
        RangeType::Int4 => PG_TYPE_INT4RANGE,
        RangeType::Int8 => PG_TYPE_INT8RANGE,
        RangeType::Num => PG_TYPE_NUMRANGE,
        RangeType::Date => PG_TYPE_DATERANGE,
        RangeType::Timestamp => PG_TYPE_TSRANGE,
        RangeType::TimestampTz => PG_TYPE_TSTZRANGE,
    }
}

fn data_type_name(dt: &DataType) -> std::borrow::Cow<'static, str> {
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

fn table_entries(ctx: &LowerCtx<'_>) -> Vec<ultrasql_catalog::TableEntry> {
    let mut entries: Vec<ultrasql_catalog::TableEntry> =
        ctx.catalog_snapshot.tables.values().cloned().collect();
    entries.sort_by(|a, b| {
        (a.schema_name.as_str(), a.name.as_str()).cmp(&(b.schema_name.as_str(), b.name.as_str()))
    });
    entries
}

fn is_materialized_view_entry(entry: &ultrasql_catalog::TableEntry) -> bool {
    entry
        .options
        .iter()
        .any(|(key, value)| key == "ultrasql.relkind" && value == "materialized_view")
}

fn schema_pg_namespace() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("nspname", text()),
        Field::required("nspowner", DataType::Int64),
        Field::nullable("nspacl", text_array()),
    ])
}

fn rows_pg_namespace(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = vec![
        vec![
            Value::Int64(PG_CATALOG_OID),
            v_text("pg_catalog"),
            Value::Int64(10),
            Value::Null,
        ],
        vec![
            Value::Int64(INFORMATION_SCHEMA_OID),
            v_text("information_schema"),
            Value::Int64(10),
            Value::Null,
        ],
        vec![
            Value::Int64(PUBLIC_OID),
            v_text("public"),
            Value::Int64(10),
            Value::Null,
        ],
    ];
    rows.extend(
        runtime_schema_rows(ctx)
            .into_iter()
            .map(|(name, owner_role, oid)| {
                vec![
                    Value::Int64(oid),
                    v_text(name),
                    Value::Int64(namespace_owner_oid(ctx, &owner_role)),
                    Value::Null,
                ]
            }),
    );
    rows
}

fn schema_pg_class() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("relname", text()),
        Field::required("relnamespace", DataType::Int64),
        Field::required("relkind", DataType::Text { max_len: Some(1) }),
        Field::required("relpages", DataType::Int32),
        Field::required("reltuples", DataType::Float64),
        Field::required("relfilenode", DataType::Int32),
        Field::required("relhasindex", DataType::Bool),
        Field::required("relchecks", DataType::Int32),
        Field::required("relhasrules", DataType::Bool),
        Field::required("relhastriggers", DataType::Bool),
        Field::required("relrowsecurity", DataType::Bool),
        Field::required("relforcerowsecurity", DataType::Bool),
        Field::required("relispartition", DataType::Bool),
        Field::required("reltablespace", DataType::Int64),
        Field::required("reloftype", DataType::Int64),
        Field::required("relpersistence", DataType::Text { max_len: Some(1) }),
        Field::required("relreplident", DataType::Text { max_len: Some(1) }),
        Field::required("reltoastrelid", DataType::Int64),
        Field::required("relam", DataType::Int64),
        Field::nullable("relpartbound", text()),
        Field::required("relowner", DataType::Int64),
        Field::nullable("relacl", text_array()),
        Field::nullable("reloptions", text_array()),
    ])
}

fn rows_pg_class(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for entry in table_entries(ctx) {
        let relkind = if is_materialized_view_entry(&entry) {
            "m"
        } else {
            "r"
        };
        rows.push(pg_class_row(
            entry.oid.raw(),
            entry.name.clone(),
            namespace_oid(&entry.schema_name),
            relkind,
            i32::try_from(entry.n_blocks).unwrap_or(i32::MAX),
            i32::try_from(entry.root_block.raw()).unwrap_or(i32::MAX),
            ctx.catalog_snapshot
                .indexes_by_table
                .contains_key(&entry.oid),
        ));
    }
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    for index in indexes {
        rows.push(pg_class_row(
            index.oid.raw(),
            index.name.clone(),
            PUBLIC_OID,
            "i",
            0,
            i32::try_from(index.root_block.raw()).unwrap_or(i32::MAX),
            false,
        ));
    }
    let mut composites = ctx
        .catalog_snapshot
        .composite_types_by_oid
        .values()
        .collect::<Vec<_>>();
    composites.sort_by_key(|entry| entry.oid.raw());
    for entry in composites {
        rows.push(pg_class_row(
            entry.oid.raw(),
            entry.name.clone(),
            namespace_oid(&entry.schema_name),
            "c",
            0,
            0,
            false,
        ));
    }
    rows
}

fn pg_class_row(
    oid: u32,
    relname: String,
    relnamespace: i64,
    relkind: &str,
    relpages: i32,
    relfilenode: i32,
    relhasindex: bool,
) -> Vec<Value> {
    vec![
        v_i64(oid),
        v_text(relname),
        Value::Int64(relnamespace),
        v_text(relkind),
        Value::Int32(relpages),
        Value::Float64(0.0),
        Value::Int32(relfilenode),
        Value::Bool(relhasindex),
        Value::Int32(0),
        Value::Bool(false),
        Value::Bool(false),
        Value::Bool(false),
        Value::Bool(false),
        Value::Bool(false),
        Value::Int64(0),
        Value::Int64(0),
        v_text("p"),
        v_text("d"),
        Value::Int64(0),
        Value::Int64(2),
        Value::Null,
        Value::Int64(10),
        Value::Null,
        Value::Null,
    ]
}

fn schema_pg_attribute() -> Schema {
    schema([
        Field::required("attrelid", DataType::Int64),
        Field::required("attname", text()),
        Field::required("atttypid", DataType::Oid),
        Field::required("attnum", DataType::Int16),
        Field::required("attnotnull", DataType::Bool),
        Field::required("atthasdef", DataType::Bool),
        Field::required("attisdropped", DataType::Bool),
        Field::required("atttypmod", DataType::Int32),
        Field::required("attcollation", DataType::Oid),
        Field::required("attidentity", DataType::Text { max_len: Some(1) }),
        Field::required("attgenerated", DataType::Text { max_len: Some(1) }),
        Field::nullable("attacl", text_array()),
        Field::nullable("attoptions", text_array()),
    ])
}

fn rows_pg_attribute(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for entry in table_entries(ctx) {
        for (idx, field) in entry.schema.fields().iter().enumerate() {
            rows.push(vec![
                v_i64(entry.oid.raw()),
                v_text(field.name.clone()),
                v_oid_i32(type_oid(&field.data_type)),
                Value::Int16(i16::try_from(idx + 1).unwrap_or(i16::MAX)),
                Value::Bool(!field.nullable),
                Value::Bool(column_default_expr(ctx, entry.oid, idx).is_some()),
                Value::Bool(false),
                Value::Int32(-1),
                v_oid(attribute_collation_oid(&entry, idx)),
                v_text(""),
                v_text(""),
                Value::Null,
                Value::Null,
            ]);
        }
    }
    let mut composites = ctx
        .catalog_snapshot
        .composite_types_by_oid
        .values()
        .collect::<Vec<_>>();
    composites.sort_by_key(|entry| entry.oid.raw());
    for entry in composites {
        for (idx, field) in entry.schema.fields().iter().enumerate() {
            rows.push(vec![
                v_i64(entry.oid.raw()),
                v_text(field.name.clone()),
                v_oid_i32(type_oid(&field.data_type)),
                Value::Int16(i16::try_from(idx + 1).unwrap_or(i16::MAX)),
                Value::Bool(!field.nullable),
                Value::Bool(false),
                Value::Bool(false),
                Value::Int32(-1),
                v_oid(type_collation_oid(&field.data_type)),
                v_text(""),
                v_text(""),
                Value::Null,
                Value::Null,
            ]);
        }
    }
    rows
}

fn schema_pg_attrdef() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("adrelid", DataType::Int64),
        Field::required("adnum", DataType::Int16),
        Field::required("adbin", text()),
    ])
}

fn rows_pg_attrdef(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for entry in table_entries(ctx) {
        for idx in 0..entry.schema.fields().len() {
            let Some(expr) = column_default_expr(ctx, entry.oid, idx) else {
                continue;
            };
            let attnum = i16::try_from(idx + 1).unwrap_or(i16::MAX);
            rows.push(vec![
                Value::Int64(attrdef_oid(entry.oid, attnum)),
                v_i64(entry.oid.raw()),
                Value::Int16(attnum),
                v_text(expr),
            ]);
        }
    }
    rows
}

fn attrdef_oid(relid: Oid, attnum: i16) -> i64 {
    i64::from(relid.raw())
        .saturating_mul(1_000)
        .saturating_add(i64::from(attnum))
}

fn schema_pg_type() -> Schema {
    schema([
        Field::required("oid", DataType::Oid),
        Field::required("typname", text()),
        Field::required("typnamespace", DataType::Int64),
        Field::required("typowner", DataType::Int64),
        Field::required("typtype", text()),
        Field::required("typcategory", text()),
        Field::required("typlen", DataType::Int16),
        Field::required("typelem", DataType::Int32),
        Field::required("typarray", DataType::Oid),
        Field::required("typdelim", text()),
        Field::required("typinput", text()),
        Field::required("typbasetype", DataType::Oid),
        Field::required("typcollation", DataType::Oid),
    ])
}

fn rows_pg_type(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    const BUILTINS: &[(i32, &str, &str, &str, i16, i32, i32)] = &[
        (PG_TYPE_BOOL, "bool", "b", "B", 1, 0, PG_TYPE_BOOL_ARRAY),
        (PG_TYPE_INT2, "int2", "b", "N", 2, 0, PG_TYPE_INT2_ARRAY),
        (PG_TYPE_INT4, "int4", "b", "N", 4, 0, PG_TYPE_INT4_ARRAY),
        (PG_TYPE_INT8, "int8", "b", "N", 8, 0, PG_TYPE_INT8_ARRAY),
        (
            PG_TYPE_FLOAT4,
            "float4",
            "b",
            "N",
            4,
            0,
            PG_TYPE_FLOAT4_ARRAY,
        ),
        (
            PG_TYPE_FLOAT8,
            "float8",
            "b",
            "N",
            8,
            0,
            PG_TYPE_FLOAT8_ARRAY,
        ),
        (PG_TYPE_TEXT, "text", "b", "S", -1, 0, PG_TYPE_TEXT_ARRAY),
        (
            PG_TYPE_BPCHAR,
            "bpchar",
            "b",
            "S",
            -1,
            0,
            PG_TYPE_BPCHAR_ARRAY,
        ),
        (
            PG_TYPE_NUMERIC,
            "numeric",
            "b",
            "N",
            -1,
            0,
            PG_TYPE_NUMERIC_ARRAY,
        ),
        (PG_TYPE_MONEY, "money", "b", "N", 8, 0, PG_TYPE_MONEY_ARRAY),
        (PG_TYPE_OID, "oid", "b", "N", 4, 0, PG_TYPE_OID_ARRAY),
        (
            PG_TYPE_REGCLASS,
            "regclass",
            "b",
            "N",
            4,
            0,
            PG_TYPE_REGCLASS_ARRAY,
        ),
        (
            PG_TYPE_REGTYPE,
            "regtype",
            "b",
            "N",
            4,
            0,
            PG_TYPE_REGTYPE_ARRAY,
        ),
        (
            PG_TYPE_PG_LSN,
            "pg_lsn",
            "b",
            "U",
            8,
            0,
            PG_TYPE_PG_LSN_ARRAY,
        ),
        (PG_TYPE_BYTEA, "bytea", "b", "U", -1, 0, PG_TYPE_BYTEA_ARRAY),
        (PG_TYPE_JSON, "json", "b", "U", -1, 0, PG_TYPE_JSON_ARRAY),
        (PG_TYPE_JSONB, "jsonb", "b", "U", -1, 0, PG_TYPE_JSONB_ARRAY),
        (PG_TYPE_XML, "xml", "b", "U", -1, 0, PG_TYPE_XML_ARRAY),
        (
            PG_TYPE_TSVECTOR,
            "tsvector",
            "b",
            "U",
            -1,
            0,
            PG_TYPE_TSVECTOR_ARRAY,
        ),
        (
            PG_TYPE_TSQUERY,
            "tsquery",
            "b",
            "U",
            -1,
            0,
            PG_TYPE_TSQUERY_ARRAY,
        ),
        (PG_TYPE_DATE, "date", "b", "D", 4, 0, PG_TYPE_DATE_ARRAY),
        (PG_TYPE_TIME, "time", "b", "D", 8, 0, PG_TYPE_TIME_ARRAY),
        (
            PG_TYPE_TIMETZ,
            "timetz",
            "b",
            "D",
            8,
            0,
            PG_TYPE_TIMETZ_ARRAY,
        ),
        (
            PG_TYPE_TIMESTAMP,
            "timestamp",
            "b",
            "D",
            8,
            0,
            PG_TYPE_TIMESTAMP_ARRAY,
        ),
        (
            PG_TYPE_TIMESTAMPTZ,
            "timestamptz",
            "b",
            "D",
            8,
            0,
            PG_TYPE_TIMESTAMPTZ_ARRAY,
        ),
        (PG_TYPE_UUID, "uuid", "b", "U", 16, 0, PG_TYPE_UUID_ARRAY),
        (PG_TYPE_BIT, "bit", "b", "V", -1, 0, PG_TYPE_BIT_ARRAY),
        (
            PG_TYPE_VARBIT,
            "varbit",
            "b",
            "V",
            -1,
            0,
            PG_TYPE_VARBIT_ARRAY,
        ),
        (PG_TYPE_INET, "inet", "b", "I", -1, 0, PG_TYPE_INET_ARRAY),
        (PG_TYPE_CIDR, "cidr", "b", "I", -1, 0, PG_TYPE_CIDR_ARRAY),
        (
            PG_TYPE_MACADDR,
            "macaddr",
            "b",
            "U",
            -1,
            0,
            PG_TYPE_MACADDR_ARRAY,
        ),
        (
            PG_TYPE_MACADDR8,
            "macaddr8",
            "b",
            "U",
            -1,
            0,
            PG_TYPE_MACADDR8_ARRAY,
        ),
        (
            PG_TYPE_INT4RANGE,
            "int4range",
            "r",
            "R",
            -1,
            0,
            PG_TYPE_INT4RANGE_ARRAY,
        ),
        (
            PG_TYPE_INT8RANGE,
            "int8range",
            "r",
            "R",
            -1,
            0,
            PG_TYPE_INT8RANGE_ARRAY,
        ),
        (
            PG_TYPE_NUMRANGE,
            "numrange",
            "r",
            "R",
            -1,
            0,
            PG_TYPE_NUMRANGE_ARRAY,
        ),
        (
            PG_TYPE_DATERANGE,
            "daterange",
            "r",
            "R",
            -1,
            0,
            PG_TYPE_DATERANGE_ARRAY,
        ),
        (
            PG_TYPE_TSRANGE,
            "tsrange",
            "r",
            "R",
            -1,
            0,
            PG_TYPE_TSRANGE_ARRAY,
        ),
        (
            PG_TYPE_TSTZRANGE,
            "tstzrange",
            "r",
            "R",
            -1,
            0,
            PG_TYPE_TSTZRANGE_ARRAY,
        ),
    ];

    let mut rows = BUILTINS
        .iter()
        .map(
            |(oid, name, typtype, typcategory, typlen, typelem, typarray)| {
                vec![
                    v_oid_i32(*oid),
                    v_text(*name),
                    Value::Int64(PG_CATALOG_OID),
                    Value::Int64(10),
                    v_text(*typtype),
                    v_text(*typcategory),
                    Value::Int16(*typlen),
                    Value::Int32(*typelem),
                    v_oid_i32(*typarray),
                    v_text(","),
                    v_text(format!("{name}in")),
                    v_oid(0),
                    v_oid(if matches!(*oid, PG_TYPE_TEXT | PG_TYPE_BPCHAR) {
                        PG_COLLATION_DEFAULT_OID
                    } else {
                        0
                    }),
                ]
            },
        )
        .collect::<Vec<_>>();
    rows.extend(BUILTINS.iter().filter_map(
        |(element_oid, element_name, _typtype, _typcategory, _typlen, _typelem, array_oid)| {
            if *array_oid == 0 {
                return None;
            }
            Some(vec![
                v_oid_i32(*array_oid),
                v_text(format!("_{element_name}")),
                Value::Int64(PG_CATALOG_OID),
                Value::Int64(10),
                v_text("b"),
                v_text("A"),
                Value::Int16(-1),
                Value::Int32(*element_oid),
                v_oid(0),
                v_text(","),
                v_text("array_in"),
                v_oid(0),
                v_oid(if matches!(*element_oid, PG_TYPE_TEXT | PG_TYPE_BPCHAR) {
                    PG_COLLATION_DEFAULT_OID
                } else {
                    0
                }),
            ])
        },
    ));
    let mut enums = ctx
        .catalog_snapshot
        .enum_types_by_oid
        .values()
        .collect::<Vec<_>>();
    enums.sort_by_key(|entry| entry.oid.raw());
    for entry in enums {
        rows.push(vec![
            v_oid(entry.oid.raw()),
            v_text(entry.name.clone()),
            Value::Int64(namespace_oid(&entry.schema_name)),
            Value::Int64(10),
            v_text("e"),
            v_text("E"),
            Value::Int16(-1),
            Value::Int32(0),
            v_oid(0),
            v_text(","),
            v_text("enum_in"),
            v_oid(0),
            v_oid(0),
        ]);
    }
    let mut composites = ctx
        .catalog_snapshot
        .composite_types_by_oid
        .values()
        .collect::<Vec<_>>();
    composites.sort_by_key(|entry| entry.oid.raw());
    for entry in composites {
        rows.push(vec![
            v_oid(entry.oid.raw()),
            v_text(entry.name.clone()),
            Value::Int64(namespace_oid(&entry.schema_name)),
            Value::Int64(10),
            v_text("c"),
            v_text("C"),
            Value::Int16(-1),
            Value::Int32(0),
            v_oid(0),
            v_text(","),
            v_text("record_in"),
            v_oid(0),
            v_oid(0),
        ]);
    }
    let mut domains = ctx
        .catalog_snapshot
        .domain_types_by_oid
        .values()
        .collect::<Vec<_>>();
    domains.sort_by_key(|entry| entry.oid.raw());
    for entry in domains {
        rows.push(vec![
            v_oid(entry.oid.raw()),
            v_text(entry.name.clone()),
            Value::Int64(namespace_oid(&entry.schema_name)),
            Value::Int64(10),
            v_text("d"),
            v_text(type_category_text(&entry.base_type)),
            Value::Int16(
                entry
                    .base_type
                    .fixed_size()
                    .and_then(|len| i16::try_from(len).ok())
                    .unwrap_or(-1),
            ),
            Value::Int32(0),
            v_oid(0),
            v_text(","),
            v_text("domain_in"),
            v_oid_i32(type_oid(&entry.base_type)),
            v_oid(type_collation_oid(&entry.base_type)),
        ]);
    }
    rows
}

fn schema_pg_am() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("amname", text()),
        Field::required("amhandler", DataType::Int64),
        Field::required("amtype", DataType::Text { max_len: Some(1) }),
    ])
}

fn rows_pg_am() -> Vec<Vec<Value>> {
    vec![
        vec![
            Value::Int64(2),
            v_text("heap"),
            Value::Int64(0),
            v_text("t"),
        ],
        vec![
            Value::Int64(403),
            v_text("btree"),
            Value::Int64(0),
            v_text("i"),
        ],
        vec![
            Value::Int64(405),
            v_text("hash"),
            Value::Int64(0),
            v_text("i"),
        ],
        vec![
            Value::Int64(783),
            v_text("gist"),
            Value::Int64(0),
            v_text("i"),
        ],
        vec![
            Value::Int64(2742),
            v_text("gin"),
            Value::Int64(0),
            v_text("i"),
        ],
        vec![
            Value::Int64(3580),
            v_text("brin"),
            Value::Int64(0),
            v_text("i"),
        ],
    ]
}

fn schema_pg_range() -> Schema {
    schema([
        Field::required("rngtypid", DataType::Oid),
        Field::required("rngsubtype", DataType::Oid),
    ])
}

fn rows_pg_range() -> Vec<Vec<Value>> {
    vec![
        vec![v_oid_i32(PG_TYPE_INT4RANGE), v_oid_i32(PG_TYPE_INT4)],
        vec![v_oid_i32(PG_TYPE_INT8RANGE), v_oid_i32(PG_TYPE_INT8)],
        vec![v_oid_i32(PG_TYPE_NUMRANGE), v_oid_i32(PG_TYPE_NUMERIC)],
        vec![v_oid_i32(PG_TYPE_DATERANGE), v_oid_i32(PG_TYPE_DATE)],
        vec![v_oid_i32(PG_TYPE_TSRANGE), v_oid_i32(PG_TYPE_TIMESTAMP)],
        vec![v_oid_i32(PG_TYPE_TSTZRANGE), v_oid_i32(PG_TYPE_TIMESTAMPTZ)],
    ]
}

fn schema_pg_collation() -> Schema {
    schema([
        Field::required("oid", DataType::Oid),
        Field::required("collname", text()),
    ])
}

fn rows_pg_collation() -> Vec<Vec<Value>> {
    vec![
        vec![
            Value::Oid(Oid::new(PG_COLLATION_DEFAULT_OID)),
            v_text("default"),
        ],
        vec![Value::Oid(Oid::new(950)), v_text("C")],
        vec![Value::Oid(Oid::new(951)), v_text("POSIX")],
    ]
}

fn type_category_text(ty: &DataType) -> &'static str {
    match ty {
        DataType::Bool => "B",
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal { .. }
        | DataType::Money
        | DataType::Oid
        | DataType::RegClass
        | DataType::RegType => "N",
        DataType::Text { .. } | DataType::Char { .. } => "S",
        DataType::Bit { .. } | DataType::VarBit { .. } => "V",
        DataType::Date
        | DataType::Time
        | DataType::TimeTz
        | DataType::Timestamp
        | DataType::TimestampTz
        | DataType::Interval => "D",
        DataType::Array(_) => "A",
        DataType::Enum { .. } => "E",
        DataType::Composite { .. } | DataType::Record(_) => "C",
        DataType::Domain { base_type, .. } => type_category_text(base_type),
        _ => "U",
    }
}

fn schema_pg_enum() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("enumtypid", DataType::Int64),
        Field::required("enumsortorder", DataType::Float32),
        Field::required("enumlabel", text()),
    ])
}

fn rows_pg_enum(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut enums = ctx
        .catalog_snapshot
        .enum_types_by_oid
        .values()
        .collect::<Vec<_>>();
    enums.sort_by_key(|entry| entry.oid.raw());
    let mut rows = Vec::new();
    for entry in enums {
        let mut labels = entry.labels.clone();
        labels.sort_by_key(|label| label.sort_order);
        for label in labels {
            rows.push(vec![
                v_i64(label.oid.raw()),
                v_i64(entry.oid.raw()),
                Value::Float32(enum_sort_order_f32(label.sort_order)),
                v_text(label.label),
            ]);
        }
    }
    rows
}

fn enum_sort_order_f32(sort_order: u32) -> f32 {
    sort_order.to_string().parse::<f32>().unwrap_or(f32::MAX)
}

fn column_default_expr(ctx: &LowerCtx<'_>, relid: Oid, idx: usize) -> Option<String> {
    let constraints = ctx.table_constraints.get(&relid)?;
    if let Some(expr) = constraints.defaults.get(idx).and_then(Option::as_ref) {
        return Some(format!("{expr:?}"));
    }
    if let Some(seq_name) = constraints
        .sequence_defaults
        .get(idx)
        .and_then(Option::as_ref)
    {
        return Some(format!("nextval('{seq_name}'::regclass)"));
    }
    if constraints
        .identity_always
        .get(idx)
        .copied()
        .unwrap_or(false)
    {
        return Some("generated always as identity".to_owned());
    }
    constraints
        .generated_stored
        .get(idx)
        .and_then(Option::as_ref)
        .map(|expr| format!("generated always as ({expr:?}) stored"))
}

fn schema_pg_index() -> Schema {
    schema([
        Field::required("indexrelid", DataType::Int64),
        Field::required("indrelid", DataType::Int64),
        Field::required("indnatts", DataType::Int16),
        Field::required("indisunique", DataType::Bool),
        Field::required("indisprimary", DataType::Bool),
        Field::required("indisclustered", DataType::Bool),
        Field::required("indisvalid", DataType::Bool),
        Field::required("indisreplident", DataType::Bool),
        Field::required("indkey", DataType::Array(Box::new(DataType::Int16))),
    ])
}

fn rows_pg_index(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    indexes
        .into_iter()
        .map(|idx| {
            vec![
                v_i64(idx.oid.raw()),
                v_i64(idx.table_oid.raw()),
                Value::Int16(i16::try_from(idx.columns.len()).unwrap_or(i16::MAX)),
                Value::Bool(idx.is_unique),
                Value::Bool(idx.name.ends_with("_pkey")),
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(false),
                Value::Array {
                    element_type: DataType::Int16,
                    elements: idx
                        .columns
                        .iter()
                        .map(|col| {
                            let attnum = i16::try_from(usize::from(*col) + 1).unwrap_or(i16::MAX);
                            Value::Int16(attnum)
                        })
                        .collect(),
                },
            ]
        })
        .collect()
}

fn schema_pg_inherits() -> Schema {
    schema([
        Field::required("inhrelid", DataType::Int64),
        Field::required("inhparent", DataType::Int64),
        Field::required("inhseqno", DataType::Int32),
        Field::required("inhdetachpending", DataType::Bool),
    ])
}

#[derive(Clone, Debug)]
struct VirtualConstraint {
    oid: i64,
    name: String,
    kind: &'static str,
    table_oid: Oid,
    index_oid: Option<Oid>,
    table_schema: String,
    table_name: String,
    columns: Vec<usize>,
    foreign_table_oid: Option<Oid>,
    foreign_columns: Vec<usize>,
    on_delete: LogicalReferentialAction,
    on_update: LogicalReferentialAction,
    deferrable: bool,
    initially_deferred: bool,
    check_clause: Option<String>,
}

fn virtual_constraints(ctx: &LowerCtx<'_>) -> Vec<VirtualConstraint> {
    let mut out = Vec::new();
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    for index in indexes {
        if !index.is_unique {
            continue;
        }
        let Some(table) = ctx.catalog_snapshot.tables_by_oid.get(&index.table_oid) else {
            continue;
        };
        out.push(VirtualConstraint {
            oid: 30_000 + i64::from(index.oid.raw()),
            name: index.name.clone(),
            kind: if index.name.ends_with("_pkey") {
                "p"
            } else {
                "u"
            },
            table_oid: table.oid,
            index_oid: Some(index.oid),
            table_schema: table.schema_name.clone(),
            table_name: table.name.clone(),
            columns: index.columns.iter().map(|c| usize::from(*c)).collect(),
            foreign_table_oid: None,
            foreign_columns: Vec::new(),
            on_delete: LogicalReferentialAction::NoAction,
            on_update: LogicalReferentialAction::NoAction,
            deferrable: false,
            initially_deferred: false,
            check_clause: None,
        });
    }

    let mut runtime: Vec<_> = ctx
        .table_constraints
        .iter()
        .map(|item| (*item.key(), item.value().clone()))
        .collect();
    runtime.sort_by_key(|(oid, _)| oid.raw());
    for (table_oid, constraints) in runtime {
        let Some(table) = ctx.catalog_snapshot.tables_by_oid.get(&table_oid) else {
            continue;
        };
        for (idx, check) in constraints.checks.iter().enumerate() {
            out.push(VirtualConstraint {
                oid: 40_000
                    + i64::from(table_oid.raw()) * 100
                    + i64::try_from(idx).unwrap_or(i64::MAX),
                name: check.name.clone(),
                kind: "c",
                table_oid,
                index_oid: None,
                table_schema: table.schema_name.clone(),
                table_name: table.name.clone(),
                columns: Vec::new(),
                foreign_table_oid: None,
                foreign_columns: Vec::new(),
                on_delete: LogicalReferentialAction::NoAction,
                on_update: LogicalReferentialAction::NoAction,
                deferrable: false,
                initially_deferred: false,
                check_clause: Some(check.expr.to_string()),
            });
        }
        for (idx, fk) in constraints.foreign_keys.iter().enumerate() {
            out.push(VirtualConstraint {
                oid: 50_000
                    + i64::from(table_oid.raw()) * 100
                    + i64::try_from(idx).unwrap_or(i64::MAX),
                name: fk.name.clone(),
                kind: "f",
                table_oid,
                index_oid: None,
                table_schema: table.schema_name.clone(),
                table_name: table.name.clone(),
                columns: fk.columns.clone(),
                foreign_table_oid: Some(fk.target_oid),
                foreign_columns: fk.target_columns.clone(),
                on_delete: fk.on_delete,
                on_update: fk.on_update,
                deferrable: fk.deferrable,
                initially_deferred: fk.initially_deferred,
                check_clause: None,
            });
        }
    }
    out.sort_by(|a, b| {
        (
            a.table_schema.as_str(),
            a.table_name.as_str(),
            a.name.as_str(),
        )
            .cmp(&(
                b.table_schema.as_str(),
                b.table_name.as_str(),
                b.name.as_str(),
            ))
    });
    out
}

fn attnums_text(columns: &[usize]) -> Value {
    if columns.is_empty() {
        return Value::Null;
    }
    v_text(
        columns
            .iter()
            .map(|col| (col + 1).to_string())
            .collect::<Vec<_>>()
            .join(" "),
    )
}

fn schema_pg_constraint() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("conname", text()),
        Field::required("connamespace", DataType::Int64),
        Field::required("contype", DataType::Text { max_len: Some(1) }),
        Field::required("conrelid", DataType::Int64),
        Field::required("conindid", DataType::Int64),
        Field::required("confrelid", DataType::Int64),
        Field::nullable("conkey", text()),
        Field::nullable("confkey", text()),
        Field::required("convalidated", DataType::Bool),
        Field::required("condeferrable", DataType::Bool),
        Field::required("condeferred", DataType::Bool),
    ])
}

fn rows_pg_constraint(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    virtual_constraints(ctx)
        .into_iter()
        .map(|c| {
            vec![
                Value::Int64(c.oid),
                v_text(c.name),
                Value::Int64(namespace_oid(&c.table_schema)),
                v_text(c.kind),
                v_i64(c.table_oid.raw()),
                c.index_oid.map_or(Value::Int64(0), |oid| v_i64(oid.raw())),
                c.foreign_table_oid
                    .map_or(Value::Int64(0), |oid| v_i64(oid.raw())),
                attnums_text(&c.columns),
                attnums_text(&c.foreign_columns),
                Value::Bool(true),
                Value::Bool(c.deferrable),
                Value::Bool(c.initially_deferred),
            ]
        })
        .collect()
}

fn schema_pg_policy() -> Schema {
    schema([
        Field::required("polname", text()),
        Field::required("polrelid", DataType::Int64),
        Field::required("polcmd", DataType::Text { max_len: Some(1) }),
        Field::required("polpermissive", DataType::Bool),
        Field::required("polroles", DataType::Array(Box::new(DataType::Int64))),
        Field::nullable("polqual", text()),
        Field::nullable("polwithcheck", text()),
    ])
}

fn rows_pg_policy(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let roles = role_oid_map(ctx);
    let mut policies = ctx
        .row_security
        .iter()
        .map(|item| (*item.key(), item.value().clone()))
        .collect::<Vec<_>>();
    policies.sort_by_key(|(oid, _)| oid.raw());

    let mut rows = Vec::new();
    for (table_oid, runtime) in policies {
        if !ctx.catalog_snapshot.tables_by_oid.contains_key(&table_oid) {
            continue;
        }
        let mut table_policies = runtime.policies.clone();
        table_policies.sort_by(|left, right| left.name.cmp(&right.name));
        for policy in table_policies {
            rows.push(vec![
                v_text(policy.name),
                v_i64(table_oid.raw()),
                v_text(policy_command_code(policy.command)),
                Value::Bool(matches!(
                    policy.permissiveness,
                    crate::RuntimeRlsPermissiveness::Permissive
                )),
                Value::Array {
                    element_type: DataType::Int64,
                    elements: policy_role_oids(&policy.roles, &roles),
                },
                policy_expr_text(policy.using.as_ref()),
                policy_expr_text(policy.with_check.as_ref()),
            ]);
        }
    }
    rows
}

fn policy_command_code(command: crate::RuntimeRlsCommand) -> &'static str {
    match command {
        crate::RuntimeRlsCommand::All => "*",
        crate::RuntimeRlsCommand::Select => "r",
        crate::RuntimeRlsCommand::Insert => "a",
        crate::RuntimeRlsCommand::Update => "w",
        crate::RuntimeRlsCommand::Delete => "d",
    }
}

fn policy_role_oids(policy_roles: &[String], role_oids: &HashMap<String, i64>) -> Vec<Value> {
    if policy_roles.is_empty() {
        return vec![Value::Int64(0)];
    }
    policy_roles
        .iter()
        .map(|role| {
            if role == "public" {
                Value::Int64(0)
            } else {
                Value::Int64(role_oids.get(role).copied().unwrap_or(0))
            }
        })
        .collect()
}

fn policy_expr_text(expr: Option<&crate::RuntimeTenantPolicyExpr>) -> Value {
    expr.map_or(Value::Null, |expr| {
        v_text(format!(
            "{} = current_setting('{}', true)",
            expr.column_name, expr.setting_name
        ))
    })
}

fn schema_pg_sequence() -> Schema {
    schema([
        Field::required("seqrelid", DataType::Int64),
        Field::required("seqtypid", DataType::Int32),
        Field::required("seqstart", DataType::Int64),
        Field::required("seqincrement", DataType::Int64),
        Field::required("seqmax", DataType::Int64),
        Field::required("seqmin", DataType::Int64),
        Field::required("seqcache", DataType::Int64),
        Field::required("seqcycle", DataType::Bool),
    ])
}

fn rows_pg_sequence(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    let mut seqs: Vec<_> = ctx
        .sequences
        .iter()
        .map(|e| (e.key().clone(), e.value().clone()))
        .collect();
    seqs.sort_by(|a, b| a.0.cmp(&b.0));
    for (idx, (_name, seq)) in seqs.into_iter().enumerate() {
        let opts = seq.options_snapshot();
        rows.push(vec![
            Value::Int64(20_000 + i64::try_from(idx).unwrap_or(i64::MAX)),
            Value::Int32(PG_TYPE_INT8),
            Value::Int64(opts.start),
            Value::Int64(opts.increment),
            Value::Int64(seq.max_value()),
            Value::Int64(seq.min_value()),
            Value::Int64(i64::from(opts.cache)),
            Value::Bool(opts.cycle),
        ]);
    }
    rows
}

fn schema_pg_operator() -> Schema {
    schema([
        Field::required("oid", DataType::Oid),
        Field::required("oprname", text()),
        Field::required("oprnamespace", DataType::Oid),
        Field::required("oprowner", DataType::Oid),
        Field::required("oprkind", DataType::Text { max_len: Some(1) }),
        Field::required("oprcanmerge", DataType::Bool),
        Field::required("oprcanhash", DataType::Bool),
        Field::required("oprleft", DataType::Oid),
        Field::required("oprright", DataType::Oid),
        Field::required("oprresult", DataType::Oid),
        Field::required("oprcom", DataType::Oid),
        Field::required("oprnegate", DataType::Oid),
        Field::required("oprcode", DataType::Oid),
        Field::required("oprrest", DataType::Oid),
        Field::required("oprjoin", DataType::Oid),
    ])
}

fn rows_pg_operator(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut operators: Vec<crate::RuntimeOperator> = ctx
        .operators
        .iter()
        .map(|entry| entry.value().as_ref().clone())
        .collect();
    operators.sort_by(|a, b| a.oid.cmp(&b.oid).then_with(|| a.name.cmp(&b.name)));
    operators
        .into_iter()
        .map(|operator| {
            let kind = match (&operator.left_type, &operator.right_type) {
                (Some(_), Some(_)) => "b",
                (None, Some(_)) => "l",
                (Some(_), None) => "r",
                (None, None) => "b",
            };
            vec![
                Value::Oid(Oid::new(operator.oid)),
                v_text(&operator.name),
                Value::Oid(Oid::new(namespace_oid_u32(&operator.namespace))),
                Value::Oid(Oid::new(10)),
                v_text(kind),
                Value::Bool(false),
                Value::Bool(false),
                Value::Oid(Oid::new(
                    operator
                        .left_type
                        .as_ref()
                        .map_or(0, pg_type_oid_for_data_type),
                )),
                Value::Oid(Oid::new(
                    operator
                        .right_type
                        .as_ref()
                        .map_or(0, pg_type_oid_for_data_type),
                )),
                Value::Oid(Oid::new(pg_type_oid_for_data_type(&operator.result_type))),
                Value::Oid(Oid::new(0)),
                Value::Oid(Oid::new(0)),
                Value::Oid(Oid::new(
                    pg_proc_oid_by_name(&operator.procedure).unwrap_or(0),
                )),
                Value::Oid(Oid::new(0)),
                Value::Oid(Oid::new(0)),
            ]
        })
        .collect()
}

fn schema_pg_depend() -> Schema {
    schema([
        Field::required("classid", DataType::Int64),
        Field::required("objid", DataType::Int64),
        Field::required("objsubid", DataType::Int32),
        Field::required("refclassid", DataType::Int64),
        Field::required("refobjid", DataType::Int64),
        Field::required("refobjsubid", DataType::Int32),
        Field::required("deptype", DataType::Text { max_len: Some(1) }),
    ])
}

fn rows_pg_depend(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for c in virtual_constraints(ctx) {
        rows.push(vec![
            Value::Int64(PG_CONSTRAINT_OID),
            Value::Int64(c.oid),
            Value::Int32(0),
            Value::Int64(PG_CLASS_OID),
            v_i64(c.table_oid.raw()),
            Value::Int32(0),
            v_text("a"),
        ]);
        if let Some(foreign_table_oid) = c.foreign_table_oid {
            rows.push(vec![
                Value::Int64(PG_CONSTRAINT_OID),
                Value::Int64(c.oid),
                Value::Int32(0),
                Value::Int64(PG_CLASS_OID),
                v_i64(foreign_table_oid.raw()),
                Value::Int32(0),
                v_text("n"),
            ]);
        }
    }
    rows.sort_by(|a, b| {
        let key = |row: &[Value]| match (&row[1], &row[4], &row[6]) {
            (Value::Int64(objid), Value::Int64(refobjid), Value::Text(deptype)) => {
                (*objid, *refobjid, deptype.clone())
            }
            _ => (0, 0, String::new()),
        };
        key(a).cmp(&key(b))
    });
    rows
}

fn schema_pg_description() -> Schema {
    schema([
        Field::required("objoid", DataType::Int64),
        Field::required("classoid", DataType::Int64),
        Field::required("objsubid", DataType::Int32),
        Field::required("description", text()),
    ])
}

fn rows_pg_description(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut descriptions: Vec<_> = ctx
        .catalog_snapshot
        .descriptions
        .values()
        .cloned()
        .collect();
    descriptions.sort_by_key(|row| (row.objoid.raw(), row.objsubid, row.description.clone()));
    descriptions
        .into_iter()
        .map(|row| {
            vec![
                v_i64(row.objoid.raw()),
                v_i64(row.classoid.raw()),
                Value::Int32(row.objsubid),
                v_text(row.description),
            ]
        })
        .collect()
}

fn schema_pg_statistic() -> Schema {
    schema([
        Field::required("starelid", DataType::Int64),
        Field::required("staattnum", DataType::Int16),
        Field::required("stanullfrac", DataType::Float32),
        Field::required("stadistinct", DataType::Float32),
    ])
}

fn rows_pg_statistic(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows: Vec<_> = ctx.catalog_snapshot.statistics.values().cloned().collect();
    rows.sort_by_key(|row| (row.starelid.raw(), row.staattnum));
    rows.into_iter()
        .map(|row| {
            vec![
                v_i64(row.starelid.raw()),
                Value::Int16(row.staattnum),
                Value::Float32(row.stanullfrac),
                Value::Float32(row.stadistinct),
            ]
        })
        .collect()
}

fn schema_pg_statistic_ext() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("stxname", text()),
        Field::required("stxrelid", DataType::Int64),
        Field::required("stxnamespace", DataType::Int64),
        Field::required("stxkeys", text()),
        Field::required(
            "stxkind",
            DataType::Array(Box::new(DataType::Text { max_len: None })),
        ),
        Field::required("stxstattarget", DataType::Int32),
    ])
}

fn rows_pg_statistic_ext(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows: Vec<_> = ctx
        .catalog_snapshot
        .statistic_ext
        .values()
        .cloned()
        .collect();
    rows.sort_by(|a, b| a.stxname.cmp(&b.stxname));
    rows.into_iter()
        .map(|row| {
            let stxkeys = row
                .stxkeys
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" ");
            let stxkind = row.stxkind.into_iter().collect::<String>();
            vec![
                v_i64(row.oid.raw()),
                v_text(row.stxname),
                v_i64(row.stxrelid.raw()),
                Value::Int64(namespace_oid("public")),
                v_text(stxkeys),
                Value::Array {
                    element_type: DataType::Text { max_len: None },
                    elements: stxkind
                        .chars()
                        .map(|kind| v_text(kind.to_string()))
                        .collect(),
                },
                Value::Int32(-1),
            ]
        })
        .collect()
}

fn schema_pg_tables() -> Schema {
    schema([
        Field::required("schemaname", text()),
        Field::required("tablename", text()),
        Field::required("tableowner", text()),
        Field::nullable("tablespace", text()),
        Field::required("hasindexes", DataType::Bool),
        Field::required("hasrules", DataType::Bool),
        Field::required("hastriggers", DataType::Bool),
        Field::required("rowsecurity", DataType::Bool),
    ])
}

fn rows_pg_tables(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    table_entries(ctx)
        .into_iter()
        .filter(|entry| {
            entry.schema_name != "pg_catalog"
                && entry.schema_name != "information_schema"
                && !is_materialized_view_entry(entry)
        })
        .map(|entry| {
            vec![
                v_text(entry.schema_name.clone()),
                v_text(entry.name.clone()),
                v_text("ultrasql"),
                Value::Null,
                Value::Bool(
                    ctx.catalog_snapshot
                        .indexes_by_table
                        .contains_key(&entry.oid),
                ),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(false),
            ]
        })
        .collect()
}

fn schema_pg_indexes() -> Schema {
    schema([
        Field::required("schemaname", text()),
        Field::required("tablename", text()),
        Field::required("indexname", text()),
        Field::nullable("tablespace", text()),
        Field::nullable("indexdef", text()),
    ])
}

fn rows_pg_indexes(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    for idx in indexes {
        let table = ctx.catalog_snapshot.tables_by_oid.get(&idx.table_oid);
        let (schema_name, table_name) = table
            .map(|entry| (entry.schema_name.clone(), entry.name.clone()))
            .unwrap_or_else(|| ("public".to_owned(), idx.table_oid.raw().to_string()));
        rows.push(vec![
            v_text(schema_name),
            v_text(table_name),
            v_text(idx.name.clone()),
            Value::Null,
            Value::Null,
        ]);
    }
    rows
}

fn schema_pg_views() -> Schema {
    schema([
        Field::required("schemaname", text()),
        Field::required("viewname", text()),
        Field::required("viewowner", text()),
        Field::nullable("definition", text()),
    ])
}

fn schema_pg_matviews() -> Schema {
    schema([
        Field::required("schemaname", text()),
        Field::required("matviewname", text()),
        Field::required("matviewowner", text()),
        Field::nullable("tablespace", text()),
        Field::required("hasindexes", DataType::Bool),
        Field::required("ispopulated", DataType::Bool),
        Field::nullable("definition", text()),
    ])
}

fn rows_pg_matviews(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    table_entries(ctx)
        .into_iter()
        .filter(is_materialized_view_entry)
        .map(|entry| {
            vec![
                v_text(entry.schema_name.clone()),
                v_text(entry.name.clone()),
                v_text("ultrasql"),
                Value::Null,
                Value::Bool(
                    ctx.catalog_snapshot
                        .indexes_by_table
                        .contains_key(&entry.oid),
                ),
                Value::Bool(true),
                Value::Null,
            ]
        })
        .collect()
}

fn schema_pg_sequences() -> Schema {
    schema([
        Field::required("schemaname", text()),
        Field::required("sequencename", text()),
        Field::required("sequenceowner", text()),
        Field::required("data_type", text()),
        Field::required("start_value", DataType::Int64),
        Field::required("min_value", DataType::Int64),
        Field::required("max_value", DataType::Int64),
        Field::required("increment_by", DataType::Int64),
        Field::required("cycle", DataType::Bool),
        Field::required("cache_size", DataType::Int64),
        Field::nullable("last_value", DataType::Int64),
    ])
}

fn rows_pg_sequences(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut seqs: Vec<_> = ctx
        .sequences
        .iter()
        .map(|e| (e.key().clone(), e.value().clone()))
        .collect();
    seqs.sort_by(|a, b| a.0.cmp(&b.0));
    seqs.into_iter()
        .map(|(name, seq)| {
            let opts = seq.options_snapshot();
            let owner = ctx
                .sequence_owners
                .get(&name)
                .map_or_else(|| "ultrasql".to_owned(), |entry| entry.value().clone());
            vec![
                v_text("public"),
                v_text(name),
                v_text(owner),
                v_text("bigint"),
                Value::Int64(opts.start),
                Value::Int64(seq.min_value()),
                Value::Int64(seq.max_value()),
                Value::Int64(opts.increment),
                Value::Bool(opts.cycle),
                Value::Int64(i64::from(opts.cache)),
                Value::Null,
            ]
        })
        .collect()
}

fn schema_pg_roles() -> Schema {
    schema([
        Field::required("rolname", text()),
        Field::required("rolsuper", DataType::Bool),
        Field::required("rolinherit", DataType::Bool),
        Field::required("rolcreaterole", DataType::Bool),
        Field::required("rolcreatedb", DataType::Bool),
        Field::required("rolcanlogin", DataType::Bool),
        Field::required("rolreplication", DataType::Bool),
        Field::required("rolbypassrls", DataType::Bool),
        Field::required("rolconnlimit", DataType::Int32),
        Field::nullable("rolpassword", text()),
        Field::nullable("rolvaliduntil", DataType::TimestampTz),
        Field::nullable("rolconfig", text()),
        Field::required("oid", DataType::Int64),
    ])
}

fn rows_pg_roles(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.role_catalog
        .list_roles()
        .into_iter()
        .map(|role| {
            vec![
                v_text(&role.name),
                Value::Bool(role.is_superuser),
                Value::Bool(role.inherit),
                Value::Bool(role.create_role),
                Value::Bool(role.create_db),
                Value::Bool(role.can_login),
                Value::Bool(role.replication),
                Value::Bool(role.bypass_rls),
                Value::Int32(role.connection_limit),
                masked_password_value(role.password.is_some()),
                role.valid_until.map_or(Value::Null, Value::TimestampTz),
                Value::Null,
                Value::Int64(i64::from(role.oid)),
            ]
        })
        .collect()
}

fn schema_pg_auth_members() -> Schema {
    schema([
        Field::required("roleid", DataType::Int64),
        Field::required("member", DataType::Int64),
        Field::required("grantor", DataType::Int64),
        Field::required("admin_option", DataType::Bool),
    ])
}

fn rows_pg_auth_members(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let roles = role_oid_map(ctx);
    ctx.role_catalog
        .list_memberships()
        .into_iter()
        .filter_map(|membership| {
            let roleid = roles.get(&membership.role).copied()?;
            let member = roles.get(&membership.member).copied()?;
            let grantor = roles.get(&membership.grantor).copied()?;
            Some(vec![
                Value::Int64(roleid),
                Value::Int64(member),
                Value::Int64(grantor),
                Value::Bool(membership.admin_option),
            ])
        })
        .collect()
}

fn schema_pg_user() -> Schema {
    schema([
        Field::required("usename", text()),
        Field::required("usesysid", DataType::Int64),
        Field::required("usecreatedb", DataType::Bool),
        Field::required("usesuper", DataType::Bool),
        Field::required("userepl", DataType::Bool),
        Field::required("usebypassrls", DataType::Bool),
        Field::nullable("passwd", text()),
        Field::nullable("valuntil", DataType::TimestampTz),
        Field::nullable("useconfig", text()),
    ])
}

fn rows_pg_user(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.role_catalog
        .list_roles()
        .into_iter()
        .filter(|role| role.can_login)
        .map(|role| {
            vec![
                v_text(&role.name),
                Value::Int64(i64::from(role.oid)),
                Value::Bool(role.create_db),
                Value::Bool(role.is_superuser),
                Value::Bool(role.replication),
                Value::Bool(role.bypass_rls),
                masked_password_value(role.password.is_some()),
                role.valid_until.map_or(Value::Null, Value::TimestampTz),
                Value::Null,
            ]
        })
        .collect()
}

fn role_oid_map(ctx: &LowerCtx<'_>) -> HashMap<String, i64> {
    ctx.role_catalog
        .list_roles()
        .into_iter()
        .map(|role| (role.name, i64::from(role.oid)))
        .collect()
}

fn schema_pg_get_keywords() -> Schema {
    schema([
        Field::required("word", text()),
        Field::required("catcode", DataType::Text { max_len: Some(1) }),
        Field::required("barelabel", DataType::Bool),
        Field::required("catdesc", text()),
        Field::required("baredesc", text()),
    ])
}

fn rows_pg_get_keywords() -> Vec<Vec<Value>> {
    vec![vec![
        v_text("abort"),
        v_text("U"),
        Value::Bool(true),
        v_text("unreserved"),
        v_text("can be bare label"),
    ]]
}

fn masked_password_value(has_password: bool) -> Value {
    if has_password {
        v_text("********")
    } else {
        Value::Null
    }
}

fn schema_pg_settings() -> Schema {
    schema([
        Field::required("name", text()),
        Field::required("setting", text()),
        Field::nullable("unit", text()),
        Field::required("category", text()),
        Field::required("short_desc", text()),
        Field::required("vartype", text()),
        Field::required("context", text()),
    ])
}

fn rows_pg_settings(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let autovacuum = ctx.autovacuum_config;
    vec![
        vec![
            v_text("server_version"),
            v_text(crate::REPORTED_SERVER_VERSION),
            Value::Null,
            v_text("Preset Options"),
            v_text("Wire version reported to drivers."),
            v_text("string"),
            v_text("internal"),
        ],
        vec![
            v_text("server_version_num"),
            v_text("140000"),
            Value::Null,
            v_text("Preset Options"),
            v_text("Server version number reported to drivers."),
            v_text("integer"),
            v_text("internal"),
        ],
        vec![
            v_text("server_encoding"),
            v_text("UTF8"),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the server character set encoding."),
            v_text("string"),
            v_text("internal"),
        ],
        vec![
            v_text("client_encoding"),
            v_text("UTF8"),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the client character set encoding."),
            v_text("string"),
            v_text("user"),
        ],
        vec![
            v_text("application_name"),
            v_text(session_setting(ctx, "application_name", "")),
            Value::Null,
            v_text("Reporting and Logging / What to Log"),
            v_text("Sets the application name reported in activity views."),
            v_text("string"),
            v_text("user"),
        ],
        vec![
            v_text("client_min_messages"),
            v_text(session_setting(ctx, "client_min_messages", "notice")),
            Value::Null,
            v_text("Client Connection Defaults / Statement Behavior"),
            v_text("Sets the message levels sent to the client."),
            v_text("enum"),
            v_text("user"),
        ],
        vec![
            v_text("DateStyle"),
            v_text(session_setting(ctx, "datestyle", "ISO, MDY")),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the display format for date and time values."),
            v_text("string"),
            v_text("user"),
        ],
        vec![
            v_text("extra_float_digits"),
            v_text(session_setting(ctx, "extra_float_digits", "1")),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the number of digits displayed for floating-point values."),
            v_text("integer"),
            v_text("user"),
        ],
        vec![
            v_text("IntervalStyle"),
            v_text(session_setting(ctx, "intervalstyle", "postgres")),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the display format for interval values."),
            v_text("enum"),
            v_text("user"),
        ],
        vec![
            v_text("lc_monetary"),
            v_text(session_setting(ctx, "lc_monetary", "C")),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the locale for formatting monetary amounts."),
            v_text("string"),
            v_text("user"),
        ],
        vec![
            v_text("max_identifier_length"),
            v_text("63"),
            Value::Null,
            v_text("Preset Options"),
            v_text("Shows the maximum identifier length in bytes."),
            v_text("integer"),
            v_text("internal"),
        ],
        vec![
            v_text("search_path"),
            v_text(session_setting(ctx, "search_path", "\"$user\", public")),
            Value::Null,
            v_text("Client Connection Defaults / Statement Behavior"),
            v_text("Sets the schema search order."),
            v_text("string"),
            v_text("user"),
        ],
        vec![
            v_text("transaction_isolation"),
            v_text(isolation_level_setting(ctx.isolation)),
            Value::Null,
            v_text("Client Connection Defaults / Statement Behavior"),
            v_text("Sets the current transaction isolation level."),
            v_text("enum"),
            v_text("user"),
        ],
        vec![
            v_text("standard_conforming_strings"),
            v_text("on"),
            Value::Null,
            v_text("Version and Platform Compatibility"),
            v_text("Causes string literals to treat backslashes literally."),
            v_text("bool"),
            v_text("user"),
        ],
        vec![
            v_text("statement_timeout"),
            v_text(session_setting(ctx, "statement_timeout", "0")),
            v_text("ms"),
            v_text("Client Connection Defaults / Statement Behavior"),
            v_text("Sets the maximum allowed duration of any statement."),
            v_text("integer"),
            v_text("user"),
        ],
        vec![
            v_text("TimeZone"),
            v_text(session_setting(ctx, "timezone", "UTC")),
            Value::Null,
            v_text("Client Connection Defaults / Locale and Formatting"),
            v_text("Sets the time zone for displaying and interpreting timestamps."),
            v_text("string"),
            v_text("user"),
        ],
        vec![
            v_text("work_mem"),
            v_text("4194304"),
            v_text("B"),
            v_text("Resource Usage / Memory"),
            v_text("Sets the maximum memory to use for query work areas."),
            v_text("integer"),
            v_text("user"),
        ],
        vec![
            v_text("autovacuum"),
            v_text("on"),
            Value::Null,
            v_text("Autovacuum"),
            v_text("Starts the autovacuum launcher."),
            v_text("bool"),
            v_text("sighup"),
        ],
        vec![
            v_text("autovacuum_vacuum_threshold"),
            v_text(autovacuum.vacuum_threshold.to_string()),
            Value::Null,
            v_text("Autovacuum"),
            v_text("Minimum dead tuples before vacuum."),
            v_text("integer"),
            v_text("sighup"),
        ],
        vec![
            v_text("autovacuum_vacuum_scale_factor"),
            v_text(format_scale_factor(autovacuum.vacuum_scale_factor())),
            Value::Null,
            v_text("Autovacuum"),
            v_text("Fraction of table size before vacuum."),
            v_text("real"),
            v_text("sighup"),
        ],
        vec![
            v_text("autovacuum_analyze_threshold"),
            v_text(autovacuum.analyze_threshold.to_string()),
            Value::Null,
            v_text("Autovacuum"),
            v_text("Minimum changed tuples before analyze."),
            v_text("integer"),
            v_text("sighup"),
        ],
        vec![
            v_text("autovacuum_analyze_scale_factor"),
            v_text(format_scale_factor(autovacuum.analyze_scale_factor())),
            Value::Null,
            v_text("Autovacuum"),
            v_text("Fraction of table size before analyze."),
            v_text("real"),
            v_text("sighup"),
        ],
        vec![
            v_text("synchronous_commit"),
            v_text("on"),
            Value::Null,
            v_text("Write-Ahead Log / Settings"),
            v_text("Sets the commit durability level."),
            v_text("enum"),
            v_text("user"),
        ],
        vec![
            v_text("archive_command"),
            sensitive_setting_value(&ctx.wal_archive_config.archive_command),
            Value::Null,
            v_text("Write-Ahead Log / Archiving"),
            v_text("Command to archive completed WAL files."),
            v_text("string"),
            v_text("sighup"),
        ],
        vec![
            v_text("restore_command"),
            sensitive_setting_value(&ctx.wal_archive_config.restore_command),
            Value::Null,
            v_text("Write-Ahead Log / Recovery"),
            v_text("Command to restore archived WAL files."),
            v_text("string"),
            v_text("postmaster"),
        ],
        vec![
            v_text("log_connections"),
            v_text(if ctx.logging_config.log_connections {
                "on"
            } else {
                "off"
            }),
            Value::Null,
            v_text("Reporting and Logging / What to Log"),
            v_text("Logs each successful connection."),
            v_text("bool"),
            v_text("sighup"),
        ],
        vec![
            v_text("log_min_duration_statement"),
            v_text(ctx.logging_config.log_min_duration_statement_ms.to_string()),
            v_text("ms"),
            v_text("Reporting and Logging / When to Log"),
            v_text("Logs statements running at least this long."),
            v_text("integer"),
            v_text("sighup"),
        ],
        vec![
            v_text("log_statement"),
            v_text(ctx.logging_config.log_statement.as_str()),
            Value::Null,
            v_text("Reporting and Logging / What to Log"),
            v_text("Sets the statements logged by class."),
            v_text("enum"),
            v_text("sighup"),
        ],
    ]
}

fn isolation_level_setting(isolation: IsolationLevel) -> &'static str {
    match isolation {
        IsolationLevel::ReadCommitted => "read committed",
        IsolationLevel::RepeatableRead => "repeatable read",
        IsolationLevel::Serializable => "serializable",
    }
}

fn session_setting(ctx: &LowerCtx<'_>, name: &str, default: &'static str) -> String {
    ctx.session_settings
        .get(name)
        .cloned()
        .unwrap_or_else(|| default.to_owned())
}

fn sensitive_setting_value(value: &str) -> Value {
    if value.is_empty() {
        v_text("")
    } else {
        v_text("<redacted>")
    }
}

fn format_scale_factor(value: f64) -> String {
    let rendered = format!("{value:.6}");
    rendered
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned()
}

fn schema_pg_stat_statements() -> Schema {
    schema([
        Field::required("queryid", DataType::Int64),
        Field::required("query", text()),
        Field::required("calls", DataType::Int64),
        Field::required("total_exec_time", DataType::Float64),
        Field::required("min_exec_time", DataType::Float64),
        Field::required("max_exec_time", DataType::Float64),
        Field::required("rows", DataType::Int64),
        Field::required("errors", DataType::Int64),
        Field::required("plan_hash", DataType::Int64),
        Field::required("bind_param_count", DataType::Int32),
        Field::required("bind_params_redacted", DataType::Bool),
        Field::nullable("last_error", text()),
    ])
}

fn rows_pg_stat_statements(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.workload_recorder
        .snapshot()
        .into_iter()
        .map(|stat| {
            vec![
                Value::Int64(u64_to_i64_saturating(stat.query_id)),
                v_text(stat.query),
                Value::Int64(u64_to_i64_saturating(stat.calls)),
                Value::Float64(duration_ms(stat.total_exec_time)),
                Value::Float64(duration_ms(stat.min_exec_time)),
                Value::Float64(duration_ms(stat.max_exec_time)),
                Value::Int64(u64_to_i64_saturating(stat.rows)),
                Value::Int64(u64_to_i64_saturating(stat.errors)),
                Value::Int64(u64_to_i64_saturating(stat.plan_hash)),
                Value::Int32(i32::try_from(stat.bind_param_count).unwrap_or(i32::MAX)),
                Value::Bool(stat.bind_params_redacted),
                stat.last_error.map_or(Value::Null, v_text),
            ]
        })
        .collect()
}

fn duration_ms(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn u64_to_i64_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn schema_pg_locks() -> Schema {
    schema([
        Field::nullable("locktype", text()),
        Field::nullable("database", DataType::Int64),
        Field::nullable("relation", DataType::Int64),
        Field::nullable("page", DataType::Int32),
        Field::nullable("tuple", DataType::Int16),
        Field::nullable("virtualxid", text()),
        Field::nullable("transactionid", DataType::Int64),
        Field::nullable("classid", DataType::Int64),
        Field::nullable("objid", DataType::Int64),
        Field::nullable("objsubid", DataType::Int16),
        Field::nullable("virtualtransaction", text()),
        Field::nullable("pid", DataType::Int32),
        Field::nullable("mode", text()),
        Field::required("granted", DataType::Bool),
        Field::required("fastpath", DataType::Bool),
        Field::nullable("waitstart", DataType::TimestampTz),
    ])
}

fn rows_pg_locks(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for (tag, snapshot) in ctx.oracle.lock_manager.snapshot() {
        for (xid, mode) in snapshot.grants {
            rows.push(pg_lock_row(tag, xid.raw(), mode, true));
        }
        for (xid, mode) in snapshot.waiters {
            rows.push(pg_lock_row(tag, xid.raw(), mode, false));
        }
    }
    rows
}

fn pg_lock_row(tag: LockTag, owner_xid: u64, mode: LockMode, granted: bool) -> Vec<Value> {
    let pid = advisory_owner_pid(owner_xid)
        .map(Value::Int32)
        .unwrap_or(Value::Null);
    let virtualtransaction = v_text(format!("0/{owner_xid}"));
    let mode = v_text(lock_mode_name(mode));
    let granted = Value::Bool(granted);
    let fastpath = Value::Bool(false);
    let waitstart = Value::Null;
    match tag {
        LockTag::Relation(relation) => vec![
            v_text("relation"),
            Value::Int64(1),
            Value::Int64(i64::from(relation.oid().raw())),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            virtualtransaction,
            pid,
            mode,
            granted,
            fastpath,
            waitstart,
        ],
        LockTag::Tuple(tid) => vec![
            v_text("tuple"),
            Value::Int64(1),
            Value::Int64(i64::from(tid.page.relation.oid().raw())),
            Value::Int32(i32::try_from(tid.page.block.raw()).unwrap_or(i32::MAX)),
            Value::Int16(i16::try_from(tid.slot).unwrap_or(i16::MAX)),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            virtualtransaction,
            pid,
            mode,
            granted,
            fastpath,
            waitstart,
        ],
        LockTag::Advisory { classid, objid } => vec![
            v_text("advisory"),
            Value::Int64(1),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Int64(i64::from(classid)),
            Value::Int64(i64::from(objid)),
            Value::Int16(1),
            virtualtransaction,
            pid,
            mode,
            granted,
            fastpath,
            waitstart,
        ],
    }
}

fn advisory_owner_pid(owner_xid: u64) -> Option<i32> {
    let pid = u64::MAX.checked_sub(owner_xid)?;
    i32::try_from(pid).ok()
}

fn lock_mode_name(mode: LockMode) -> &'static str {
    match mode {
        LockMode::AccessShare => "AccessShareLock",
        LockMode::RowShare => "RowShareLock",
        LockMode::RowExclusive => "RowExclusiveLock",
        LockMode::ShareUpdateExclusive => "ShareUpdateExclusiveLock",
        LockMode::Share => "ShareLock",
        LockMode::ShareRowExclusive => "ShareRowExclusiveLock",
        LockMode::Exclusive => "ExclusiveLock",
        LockMode::AccessExclusive => "AccessExclusiveLock",
    }
}

fn schema_pg_stat_activity() -> Schema {
    schema([
        Field::required("datid", DataType::Int64),
        Field::required("datname", text()),
        Field::required("pid", DataType::Int32),
        Field::required("usename", text()),
        Field::nullable("application_name", text()),
        Field::required("state", text()),
        Field::nullable("query", text()),
        Field::nullable("backend_start", DataType::TimestampTz),
        Field::nullable("xact_start", DataType::TimestampTz),
        Field::nullable("query_start", DataType::TimestampTz),
        Field::nullable("state_change", DataType::TimestampTz),
        Field::nullable("wait_event_type", text()),
        Field::nullable("wait_event", text()),
    ])
}

fn rows_pg_stat_activity(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let sessions = ctx.workload_recorder.active_sessions();
    if !sessions.is_empty() {
        return sessions
            .into_iter()
            .map(|session| {
                vec![
                    Value::Int64(session.datid),
                    v_text(session.datname),
                    Value::Int32(session.pid),
                    v_text(session.usename),
                    session.application_name.map_or(Value::Null, v_text),
                    v_text(session.state),
                    session.query.map_or(Value::Null, v_text),
                    Value::TimestampTz(session.backend_start),
                    session.xact_start.map_or(Value::Null, Value::TimestampTz),
                    session.query_start.map_or(Value::Null, Value::TimestampTz),
                    Value::TimestampTz(session.state_change),
                    session.wait_event_type.map_or(Value::Null, v_text),
                    session.wait_event.map_or(Value::Null, v_text),
                ]
            })
            .collect();
    }

    let application_name = ctx
        .session_settings
        .get("application_name")
        .cloned()
        .map_or(Value::Null, v_text);
    vec![vec![
        Value::Int64(1),
        v_text("ultrasql"),
        Value::Int32(0),
        v_text(ctx.current_user.clone()),
        application_name,
        v_text("active"),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
    ]]
}

fn schema_pg_stat_user_tables() -> Schema {
    schema([
        Field::required("relid", DataType::Int64),
        Field::required("schemaname", text()),
        Field::required("relname", text()),
        Field::required("seq_scan", DataType::Int64),
        Field::required("idx_scan", DataType::Int64),
        Field::required("n_live_tup", DataType::Int64),
        Field::required("n_dead_tup", DataType::Int64),
        Field::nullable("last_vacuum", DataType::TimestampTz),
        Field::nullable("last_autovacuum", DataType::TimestampTz),
        Field::nullable("last_analyze", DataType::TimestampTz),
        Field::nullable("last_autoanalyze", DataType::TimestampTz),
        Field::required("vacuum_count", DataType::Int64),
        Field::required("autovacuum_count", DataType::Int64),
        Field::required("analyze_count", DataType::Int64),
        Field::required("autoanalyze_count", DataType::Int64),
    ])
}

fn rows_pg_stat_user_tables(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    table_entries(ctx)
        .into_iter()
        .filter(|entry| {
            entry.schema_name != "pg_catalog" && entry.schema_name != "information_schema"
        })
        .map(|entry| {
            let (live_tuples, dead_tuples) = table_tuple_counts(ctx, &entry);
            let maintenance = ctx
                .workload_recorder
                .table_maintenance_stats(entry.oid.raw());
            vec![
                v_i64(entry.oid.raw()),
                v_text(entry.schema_name),
                v_text(entry.name),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(live_tuples),
                Value::Int64(dead_tuples),
                maintenance
                    .last_vacuum
                    .map_or(Value::Null, Value::TimestampTz),
                maintenance
                    .last_autovacuum
                    .map_or(Value::Null, Value::TimestampTz),
                maintenance
                    .last_analyze
                    .map_or(Value::Null, Value::TimestampTz),
                maintenance
                    .last_autoanalyze
                    .map_or(Value::Null, Value::TimestampTz),
                Value::Int64(u64_to_i64_saturating(maintenance.vacuum_count)),
                Value::Int64(u64_to_i64_saturating(maintenance.autovacuum_count)),
                Value::Int64(u64_to_i64_saturating(maintenance.analyze_count)),
                Value::Int64(u64_to_i64_saturating(maintenance.autoanalyze_count)),
            ]
        })
        .collect()
}

fn table_tuple_counts(ctx: &LowerCtx<'_>, entry: &ultrasql_catalog::TableEntry) -> (i64, i64) {
    let rel = ultrasql_core::RelationId(entry.oid);
    let block_count = ctx.heap.block_count(rel).max(entry.n_blocks);
    if block_count == 0 {
        return (0, 0);
    }

    let oldest = ctx.oracle.oldest_in_progress();
    let mut live = 0_u64;
    let mut dead = 0_u64;
    for tuple in ctx.heap.scan(rel, block_count) {
        let tuple = match tuple {
            Ok(tuple) => tuple,
            Err(err) => {
                tracing::warn!(
                    table = %entry.name,
                    error = %err,
                    "pg_stat_user_tables tuple-count scan failed",
                );
                return (0, 0);
            }
        };
        if matches!(
            is_visible(&tuple.header, &ctx.snapshot, ctx.oracle.as_ref()),
            Visibility::Visible | Visibility::VisiblePreImage
        ) {
            live = live.saturating_add(1);
        }
        let xmax = tuple.header.xmax;
        if !xmax.is_invalid() && xmax < oldest && ctx.oracle.is_committed(xmax) {
            dead = dead.saturating_add(1);
        }
    }
    (u64_to_i64_saturating(live), u64_to_i64_saturating(dead))
}

fn schema_pg_stat_user_indexes() -> Schema {
    schema([
        Field::required("relid", DataType::Int64),
        Field::required("indexrelid", DataType::Int64),
        Field::required("schemaname", text()),
        Field::required("relname", text()),
        Field::required("indexrelname", text()),
        Field::required("idx_scan", DataType::Int64),
        Field::required("idx_tup_read", DataType::Int64),
        Field::required("idx_tup_fetch", DataType::Int64),
    ])
}

fn rows_pg_stat_user_indexes(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    indexes
        .into_iter()
        .filter_map(|idx| {
            let table = ctx.catalog_snapshot.tables_by_oid.get(&idx.table_oid)?;
            let usage = ctx.workload_recorder.index_usage_for(idx.oid.raw());
            Some(vec![
                v_i64(table.oid.raw()),
                v_i64(idx.oid.raw()),
                v_text(table.schema_name.clone()),
                v_text(table.name.clone()),
                v_text(idx.name.clone()),
                Value::Int64(u64_to_i64_saturating(usage.idx_scan)),
                Value::Int64(u64_to_i64_saturating(usage.idx_tup_read)),
                Value::Int64(u64_to_i64_saturating(usage.idx_tup_fetch)),
            ])
        })
        .collect()
}

fn schema_pg_statio_user_tables() -> Schema {
    schema([
        Field::required("relid", DataType::Int64),
        Field::required("schemaname", text()),
        Field::required("relname", text()),
        Field::required("heap_blks_read", DataType::Int64),
        Field::required("heap_blks_hit", DataType::Int64),
        Field::required("idx_blks_read", DataType::Int64),
        Field::required("idx_blks_hit", DataType::Int64),
        Field::required("toast_blks_read", DataType::Int64),
        Field::required("toast_blks_hit", DataType::Int64),
        Field::required("tidx_blks_read", DataType::Int64),
        Field::required("tidx_blks_hit", DataType::Int64),
    ])
}

fn rows_pg_statio_user_tables(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    table_entries(ctx)
        .into_iter()
        .filter(|entry| {
            entry.schema_name != "pg_catalog" && entry.schema_name != "information_schema"
        })
        .map(|entry| {
            let heap_io = ctx
                .heap
                .buffer_pool()
                .relation_stats(ultrasql_core::RelationId(entry.oid));
            vec![
                v_i64(entry.oid.raw()),
                v_text(entry.schema_name),
                v_text(entry.name),
                Value::Int64(u64_to_i64_saturating(heap_io.reads)),
                Value::Int64(u64_to_i64_saturating(heap_io.hits)),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
            ]
        })
        .collect()
}

fn schema_pg_statio_user_indexes() -> Schema {
    schema([
        Field::required("relid", DataType::Int64),
        Field::required("indexrelid", DataType::Int64),
        Field::required("schemaname", text()),
        Field::required("relname", text()),
        Field::required("indexrelname", text()),
        Field::required("idx_blks_read", DataType::Int64),
        Field::required("idx_blks_hit", DataType::Int64),
    ])
}

fn rows_pg_statio_user_indexes(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    indexes
        .into_iter()
        .filter_map(|idx| {
            let table = ctx.catalog_snapshot.tables_by_oid.get(&idx.table_oid)?;
            let index_io = ctx
                .heap
                .buffer_pool()
                .relation_stats(ultrasql_core::RelationId(idx.oid));
            Some(vec![
                v_i64(table.oid.raw()),
                v_i64(idx.oid.raw()),
                v_text(table.schema_name.clone()),
                v_text(table.name.clone()),
                v_text(idx.name.clone()),
                Value::Int64(u64_to_i64_saturating(index_io.reads)),
                Value::Int64(u64_to_i64_saturating(index_io.hits)),
            ])
        })
        .collect()
}

fn schema_pg_stat_database() -> Schema {
    schema([
        Field::required("datid", DataType::Int64),
        Field::required("datname", text()),
        Field::required("numbackends", DataType::Int32),
        Field::required("xact_commit", DataType::Int64),
        Field::required("xact_rollback", DataType::Int64),
        Field::required("deadlocks", DataType::Int64),
    ])
}

fn rows_pg_stat_database(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut calls = 0_u64;
    let mut errors = 0_u64;
    for stat in ctx.workload_recorder.snapshot() {
        calls = calls.saturating_add(stat.calls);
        errors = errors.saturating_add(stat.errors);
    }
    let commits = calls.saturating_sub(errors);
    vec![vec![
        Value::Int64(1),
        v_text("ultrasql"),
        Value::Int32(1),
        Value::Int64(u64_to_i64_saturating(commits)),
        Value::Int64(u64_to_i64_saturating(errors)),
        Value::Int64(0),
    ]]
}

fn schema_pg_stat_bgwriter() -> Schema {
    schema([
        Field::required("checkpoints_timed", DataType::Int64),
        Field::required("checkpoints_req", DataType::Int64),
        Field::required("checkpoint_write_time", DataType::Float64),
        Field::required("checkpoint_sync_time", DataType::Float64),
        Field::required("buffers_checkpoint", DataType::Int64),
        Field::required("buffers_clean", DataType::Int64),
        Field::required("maxwritten_clean", DataType::Int64),
        Field::required("buffers_backend", DataType::Int64),
        Field::required("buffers_alloc", DataType::Int64),
    ])
}

fn rows_pg_stat_bgwriter(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let pool = ctx.heap.buffer_pool().stats();
    vec![vec![
        Value::Int64(0),
        Value::Int64(0),
        Value::Float64(0.0),
        Value::Float64(0.0),
        Value::Int64(0),
        Value::Int64(u64_to_i64_saturating(pool.evictions)),
        Value::Int64(0),
        Value::Int64(u64_to_i64_saturating(pool.gets)),
        Value::Int64(u64_to_i64_saturating(pool.misses)),
    ]]
}

fn schema_pg_stat_wal() -> Schema {
    schema([
        Field::required("wal_records", DataType::Int64),
        Field::required("wal_fpi", DataType::Int64),
        Field::required("wal_bytes", DataType::Int64),
        Field::required("wal_sync", DataType::Int64),
        Field::required("wal_write", DataType::Int64),
    ])
}

fn rows_pg_stat_wal(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let stats = ctx
        .heap
        .wal_sink()
        .map(|sink| sink.stats())
        .unwrap_or_default();
    vec![vec![
        Value::Int64(u64_to_i64_saturating(stats.wal_records)),
        Value::Int64(u64_to_i64_saturating(stats.wal_fpi)),
        Value::Int64(u64_to_i64_saturating(stats.wal_bytes)),
        Value::Int64(0),
        Value::Int64(u64_to_i64_saturating(stats.wal_write)),
    ]]
}

fn schema_pg_stat_progress_vacuum() -> Schema {
    schema([
        Field::required("pid", DataType::Int32),
        Field::required("datid", DataType::Int64),
        Field::required("datname", text()),
        Field::required("relid", DataType::Int64),
        Field::required("phase", text()),
        Field::required("heap_blks_total", DataType::Int64),
        Field::required("heap_blks_scanned", DataType::Int64),
        Field::required("heap_blks_vacuumed", DataType::Int64),
    ])
}

fn rows_pg_stat_progress_vacuum(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.workload_recorder
        .vacuum_progress()
        .into_iter()
        .map(|row| {
            vec![
                Value::Int32(row.pid),
                Value::Int64(row.datid),
                v_text(row.datname),
                Value::Int64(row.relid),
                v_text(row.phase),
                Value::Int64(row.heap_blks_total),
                Value::Int64(row.heap_blks_scanned),
                Value::Int64(row.heap_blks_vacuumed),
            ]
        })
        .collect()
}

fn schema_pg_stat_progress_analyze() -> Schema {
    schema([
        Field::required("pid", DataType::Int32),
        Field::required("datid", DataType::Int64),
        Field::required("datname", text()),
        Field::required("relid", DataType::Int64),
        Field::required("phase", text()),
        Field::required("sample_blks_total", DataType::Int64),
        Field::required("sample_blks_scanned", DataType::Int64),
    ])
}

fn rows_pg_stat_progress_analyze(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.workload_recorder
        .analyze_progress()
        .into_iter()
        .map(|row| {
            vec![
                Value::Int32(row.pid),
                Value::Int64(row.datid),
                v_text(row.datname),
                Value::Int64(row.relid),
                v_text(row.phase),
                Value::Int64(row.sample_blks_total),
                Value::Int64(row.sample_blks_scanned),
            ]
        })
        .collect()
}

fn schema_pg_stat_progress_create_index() -> Schema {
    schema([
        Field::required("pid", DataType::Int32),
        Field::required("datid", DataType::Int64),
        Field::required("datname", text()),
        Field::required("relid", DataType::Int64),
        Field::required("index_relid", DataType::Int64),
        Field::required("phase", text()),
        Field::required("blocks_total", DataType::Int64),
        Field::required("blocks_done", DataType::Int64),
    ])
}

fn rows_pg_stat_progress_create_index(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.workload_recorder
        .create_index_progress()
        .into_iter()
        .map(|row| {
            vec![
                Value::Int32(row.pid),
                Value::Int64(row.datid),
                v_text(row.datname),
                Value::Int64(row.relid),
                Value::Int64(row.index_relid),
                v_text(row.phase),
                Value::Int64(row.blocks_total),
                Value::Int64(row.blocks_done),
            ]
        })
        .collect()
}

fn schema_pg_replication_slots() -> Schema {
    schema([
        Field::required("slot_name", text()),
        Field::required("plugin", text()),
        Field::required("slot_type", text()),
        Field::required("datoid", DataType::Int64),
        Field::required("database", text()),
        Field::required("active", DataType::Bool),
        Field::nullable("restart_lsn", text()),
        Field::nullable("confirmed_flush_lsn", text()),
    ])
}

fn rows_pg_replication_slots(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let Some(data_dir) = &ctx.data_dir else {
        return Vec::new();
    };
    let Ok(store) = crate::replication::ReplicationSlotStore::open(data_dir.join("pg_replslot"))
    else {
        return Vec::new();
    };
    let Ok(slots) = store.list() else {
        return Vec::new();
    };
    slots
        .into_iter()
        .map(|slot| {
            vec![
                v_text(slot.name),
                v_text(""),
                v_text("physical"),
                Value::Int64(1),
                v_text("ultrasql"),
                Value::Bool(false),
                slot.restart_lsn.map_or(Value::Null, v_text),
                slot.confirmed_flush_lsn.map_or(Value::Null, v_text),
            ]
        })
        .collect()
}

fn schema_pg_stat_replication() -> Schema {
    schema([
        Field::required("pid", DataType::Int32),
        Field::required("usename", text()),
        Field::required("application_name", text()),
        Field::required("client_addr", text()),
        Field::required("state", text()),
        Field::nullable("sent_lsn", text()),
        Field::nullable("write_lsn", text()),
        Field::nullable("flush_lsn", text()),
        Field::nullable("replay_lsn", text()),
        Field::required("sync_state", text()),
    ])
}

fn rows_pg_stat_replication(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let Some(data_dir) = &ctx.data_dir else {
        return Vec::new();
    };
    let Ok(store) = crate::replication::ReplicationSlotStore::open(data_dir.join("pg_replslot"))
    else {
        return Vec::new();
    };
    let Ok(slots) = store.list() else {
        return Vec::new();
    };

    slots
        .into_iter()
        .map(|slot| {
            let sent_lsn = slot.restart_lsn.clone().map_or(Value::Null, v_text);
            let confirmed_flush_lsn = slot.confirmed_flush_lsn.clone();
            let write_lsn = confirmed_flush_lsn.clone().map_or(Value::Null, v_text);
            let flush_lsn = confirmed_flush_lsn.clone().map_or(Value::Null, v_text);
            let replay_lsn = confirmed_flush_lsn.map_or(Value::Null, v_text);
            let state = match (&slot.restart_lsn, &slot.confirmed_flush_lsn) {
                (Some(_), Some(_)) => "streaming",
                (Some(_), None) => "catchup",
                (None, _) => "startup",
            };

            vec![
                Value::Int32(0),
                v_text("ultrasql"),
                v_text(slot.name),
                v_text(""),
                v_text(state),
                sent_lsn,
                write_lsn,
                flush_lsn,
                replay_lsn,
                v_text("async"),
            ]
        })
        .collect()
}

fn schema_pg_stat_subscription() -> Schema {
    schema([
        Field::required("subid", DataType::Int64),
        Field::required("subname", text()),
        Field::required("pid", DataType::Int32),
        Field::required("relid", DataType::Int64),
        Field::nullable("received_lsn", text()),
        Field::nullable("latest_end_lsn", text()),
    ])
}

fn rows_pg_stat_subscription(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.logical_replication
        .subscriptions()
        .into_iter()
        .enumerate()
        .map(|(idx, subscription)| {
            vec![
                Value::Int64(subscription_oid(idx)),
                v_text(subscription.name),
                Value::Int32(0),
                Value::Int64(0),
                Value::Null,
                Value::Null,
            ]
        })
        .collect()
}

fn schema_pg_publication() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("pubname", text()),
        Field::required("pubowner", DataType::Int64),
        Field::required("puballtables", DataType::Bool),
        Field::required("pubinsert", DataType::Bool),
        Field::required("pubupdate", DataType::Bool),
        Field::required("pubdelete", DataType::Bool),
        Field::required("pubtruncate", DataType::Bool),
    ])
}

fn rows_pg_publication(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.logical_replication
        .publications()
        .into_iter()
        .enumerate()
        .map(|(idx, publication)| {
            vec![
                Value::Int64(publication_oid(idx)),
                v_text(publication.name),
                Value::Int64(10),
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(false),
            ]
        })
        .collect()
}

fn publication_oid(idx: usize) -> i64 {
    90_000 + i64::try_from(idx).unwrap_or(i64::MAX)
}

fn schema_pg_subscription() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("subdbid", DataType::Int64),
        Field::required("subname", text()),
        Field::required("subowner", DataType::Int64),
        Field::required("subenabled", DataType::Bool),
        Field::nullable("subconninfo", text()),
        Field::required("subslotname", text()),
        Field::required("subpublications", text()),
    ])
}

fn rows_pg_subscription(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.logical_replication
        .subscriptions()
        .into_iter()
        .enumerate()
        .map(|(idx, subscription)| {
            vec![
                Value::Int64(subscription_oid(idx)),
                Value::Int64(1),
                v_text(subscription.name),
                Value::Int64(10),
                Value::Bool(subscription.enabled),
                v_text(subscription.conninfo),
                v_text(subscription.slot_name),
                v_text(subscription.publications.join(",")),
            ]
        })
        .collect()
}

fn subscription_oid(idx: usize) -> i64 {
    91_000 + i64::try_from(idx).unwrap_or(i64::MAX)
}

fn schema_pg_publication_rel() -> Schema {
    schema([
        Field::required("prpubid", DataType::Int64),
        Field::required("prrelid", DataType::Int64),
    ])
}

fn rows_pg_publication_rel(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let public_table_oids = table_entries(ctx)
        .into_iter()
        .filter(|entry| entry.schema_name == "public")
        .map(|entry| (entry.name, i64::from(entry.oid.raw())))
        .collect::<HashMap<_, _>>();

    ctx.logical_replication
        .publications()
        .into_iter()
        .enumerate()
        .flat_map(|(idx, publication)| {
            let prpubid = publication_oid(idx);
            let table_oids = publication
                .tables()
                .filter_map(|table| public_table_oids.get(table).copied())
                .collect::<Vec<_>>();
            table_oids
                .into_iter()
                .map(move |prrelid| vec![Value::Int64(prpubid), Value::Int64(prrelid)])
        })
        .collect()
}

fn schema_pg_publication_tables() -> Schema {
    schema([
        Field::required("pubname", text()),
        Field::required("schemaname", text()),
        Field::required("tablename", text()),
        Field::nullable("attnames", text()),
        Field::nullable("rowfilter", text()),
    ])
}

fn rows_pg_publication_tables(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    ctx.logical_replication
        .publications()
        .into_iter()
        .flat_map(|publication| {
            let pubname = publication.name.clone();
            let tables = publication.tables().map(str::to_owned).collect::<Vec<_>>();
            tables.into_iter().map(move |table| {
                vec![
                    v_text(pubname.clone()),
                    v_text("public"),
                    v_text(table),
                    Value::Null,
                    Value::Null,
                ]
            })
        })
        .collect()
}

fn schema_pg_proc() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("proname", text()),
        Field::required("pronamespace", DataType::Int64),
        Field::required("proowner", DataType::Int64),
        Field::required("prolang", DataType::Int64),
        Field::required("prokind", DataType::Text { max_len: Some(1) }),
        Field::required("proretset", DataType::Bool),
        Field::required("provolatile", DataType::Text { max_len: Some(1) }),
        Field::required("pronargs", DataType::Int16),
        Field::required("pronargdefaults", DataType::Int16),
        Field::required("prorettype", DataType::Int64),
        Field::required("proargtypes", DataType::Array(Box::new(DataType::Oid))),
        Field::nullable("proallargtypes", DataType::Array(Box::new(DataType::Oid))),
        Field::nullable("proargnames", text_array()),
        Field::nullable(
            "proargmodes",
            DataType::Array(Box::new(DataType::Text { max_len: Some(1) })),
        ),
        Field::required("prosrc", text()),
        Field::nullable("proacl", text_array()),
    ])
}

fn rows_pg_proc() -> Vec<Vec<Value>> {
    pg_proc_builtins()
        .iter()
        .enumerate()
        .filter_map(|(offset, builtin)| {
            let oid = pg_proc_oid(offset)?;
            let pronargs = i16::try_from(builtin.arg_type_oids.len()).ok()?;
            Some(vec![
                Value::Int64(oid),
                v_text(builtin.name),
                Value::Int64(PG_CATALOG_OID),
                Value::Int64(10),
                Value::Int64(14),
                v_text("f"),
                Value::Bool(false),
                v_text(builtin.volatility),
                Value::Int16(pronargs),
                Value::Int16(0),
                Value::Int64(i64::from(builtin.return_type_oid)),
                proc_argtypes_value(builtin.arg_type_oids),
                Value::Null,
                Value::Null,
                Value::Null,
                v_text(builtin.name),
                Value::Null,
            ])
        })
        .collect()
}

fn pg_proc_oid(offset: usize) -> Option<i64> {
    let offset = u32::try_from(offset).ok()?;
    PG_PROC_BASE_OID.checked_add(offset).map(i64::from)
}

fn pg_proc_oid_by_name(name: &str) -> Option<u32> {
    pg_proc_builtins()
        .iter()
        .enumerate()
        .find(|(_, builtin)| builtin.name.eq_ignore_ascii_case(name))
        .and_then(|(offset, _)| {
            let offset = u32::try_from(offset).ok()?;
            PG_PROC_BASE_OID.checked_add(offset)
        })
}

fn pg_type_oid_for_data_type(data_type: &DataType) -> u32 {
    match data_type {
        DataType::Bool => 16,
        DataType::Int16 => 21,
        DataType::Int32 => 23,
        DataType::Int64 => 20,
        DataType::Float32 => 700,
        DataType::Float64 => 701,
        DataType::Decimal { .. } => 1700,
        DataType::Money => 790,
        DataType::Oid => 26,
        DataType::RegClass => 2205,
        DataType::RegType => 2206,
        DataType::PgLsn => 3220,
        DataType::Text { .. } => 25,
        DataType::Char { .. } => 1042,
        DataType::Bytea => 17,
        DataType::Date => 1082,
        DataType::Timestamp => 1114,
        DataType::TimestampTz => 1184,
        DataType::Time => 1083,
        DataType::TimeTz => 1266,
        DataType::Json => 114,
        DataType::Jsonb => 3802,
        DataType::Xml => 142,
        DataType::Uuid => 2950,
        DataType::Bit { .. } => 1560,
        DataType::VarBit { .. } => 1562,
        DataType::Inet => 869,
        DataType::Cidr => 650,
        DataType::MacAddr => 829,
        DataType::MacAddr8 => 774,
        DataType::Array(inner) => pg_array_type_oid_for_data_type(inner),
        DataType::Range(range) => pg_range_type_oid(*range),
        DataType::Domain { oid, .. }
        | DataType::Enum { oid, .. }
        | DataType::Composite { oid, .. } => oid.raw(),
        _ => 25,
    }
}

fn pg_array_type_oid_for_data_type(data_type: &DataType) -> u32 {
    match data_type {
        DataType::Bool => 1000,
        DataType::Int16 => 1005,
        DataType::Int32 => 1007,
        DataType::Int64 => 1016,
        DataType::Float32 => 1021,
        DataType::Float64 => 1022,
        DataType::Decimal { .. } => 1231,
        DataType::Money => 791,
        DataType::Oid => 1028,
        DataType::RegClass => 2210,
        DataType::RegType => 2211,
        DataType::PgLsn => 3221,
        DataType::Text { .. } => 1009,
        DataType::Char { .. } => 1014,
        DataType::Bytea => 1001,
        DataType::Date => 1182,
        DataType::Timestamp => 1115,
        DataType::TimestampTz => 1185,
        DataType::Time => 1183,
        DataType::TimeTz => 1270,
        DataType::Json => 199,
        DataType::Jsonb => 3807,
        DataType::Xml => 143,
        DataType::Uuid => 2951,
        DataType::Bit { .. } => 1561,
        DataType::VarBit { .. } => 1563,
        DataType::Inet => 1041,
        DataType::Cidr => 651,
        DataType::MacAddr => 1040,
        DataType::MacAddr8 => 775,
        _ => 1009,
    }
}

fn pg_range_type_oid(range: RangeType) -> u32 {
    match range {
        RangeType::Int4 => 3904,
        RangeType::Num => 3906,
        RangeType::Timestamp => 3908,
        RangeType::TimestampTz => 3910,
        RangeType::Date => 3912,
        RangeType::Int8 => 3926,
    }
}

struct PgProcBuiltin {
    name: &'static str,
    return_type_oid: u32,
    arg_type_oids: &'static [u32],
    volatility: &'static str,
}

fn proc_argtypes_value(arg_type_oids: &[u32]) -> Value {
    Value::Array {
        element_type: DataType::Oid,
        elements: arg_type_oids
            .iter()
            .map(|oid| Value::Oid(Oid::new(*oid)))
            .collect(),
    }
}

fn pg_type_name_from_oid(oid: u32) -> &'static str {
    match oid {
        PROC_TYPE_BOOL => "boolean",
        PROC_TYPE_INT4 => "integer",
        PROC_TYPE_INT8 => "bigint",
        PROC_TYPE_TEXT => "text",
        PROC_TYPE_OID => "oid",
        PROC_TYPE_TEXT_ARRAY => "ARRAY",
        PROC_TYPE_XML => "xml",
        PROC_TYPE_XML_ARRAY => "xml[]",
        PROC_TYPE_UUID => "uuid",
        _ => "text",
    }
}

const fn pg_proc_builtins() -> &'static [PgProcBuiltin] {
    &[
        PgProcBuiltin {
            name: "col_description",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_OID, PROC_TYPE_INT4],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "current_catalog",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "current_database",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "current_schema",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "current_schemas",
            return_type_oid: PROC_TYPE_TEXT_ARRAY,
            arg_type_oids: &[PROC_TYPE_BOOL],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "current_user",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "format_type",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_OID, PROC_TYPE_INT4],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "gen_random_uuid",
            return_type_oid: PROC_TYPE_UUID,
            arg_type_oids: &[],
            volatility: "v",
        },
        PgProcBuiltin {
            name: "obj_description",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_OID, PROC_TYPE_TEXT],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "pg_encoding_to_char",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_INT4],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "pg_function_is_visible",
            return_type_oid: PROC_TYPE_BOOL,
            arg_type_oids: &[PROC_TYPE_OID],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "pg_get_constraintdef",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_OID, PROC_TYPE_BOOL],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "pg_get_expr",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_OID],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "pg_get_function_arguments",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_OID],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "pg_get_function_result",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_OID],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "pg_get_indexdef",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_OID],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "pg_get_serial_sequence",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_TEXT],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "pg_get_statisticsobjdef_columns",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_OID],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "pg_get_userbyid",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_OID],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "pg_is_other_temp_schema",
            return_type_oid: PROC_TYPE_BOOL,
            arg_type_oids: &[PROC_TYPE_OID],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "pg_relation_is_publishable",
            return_type_oid: PROC_TYPE_BOOL,
            arg_type_oids: &[PROC_TYPE_OID],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "pg_relation_size",
            return_type_oid: PROC_TYPE_INT8,
            arg_type_oids: &[PROC_TYPE_OID],
            volatility: "v",
        },
        PgProcBuiltin {
            name: "pg_size_pretty",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_INT8],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "pg_table_is_visible",
            return_type_oid: PROC_TYPE_BOOL,
            arg_type_oids: &[PROC_TYPE_OID],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "session_user",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "set_config",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_TEXT, PROC_TYPE_BOOL],
            volatility: "v",
        },
        PgProcBuiltin {
            name: "shobj_description",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_OID, PROC_TYPE_TEXT],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "version",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[],
            volatility: "s",
        },
        PgProcBuiltin {
            name: "xml_is_well_formed",
            return_type_oid: PROC_TYPE_BOOL,
            arg_type_oids: &[PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "xml_is_well_formed_content",
            return_type_oid: PROC_TYPE_BOOL,
            arg_type_oids: &[PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "xml_is_well_formed_document",
            return_type_oid: PROC_TYPE_BOOL,
            arg_type_oids: &[PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "xpath",
            return_type_oid: PROC_TYPE_XML_ARRAY,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_XML],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "xpath",
            return_type_oid: PROC_TYPE_XML_ARRAY,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_XML, PROC_TYPE_TEXT_ARRAY],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "xpath_exists",
            return_type_oid: PROC_TYPE_BOOL,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_XML],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "xpath_exists",
            return_type_oid: PROC_TYPE_BOOL,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_XML, PROC_TYPE_TEXT_ARRAY],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "bool_eq",
            return_type_oid: PROC_TYPE_BOOL,
            arg_type_oids: &[PROC_TYPE_BOOL, PROC_TYPE_BOOL],
            volatility: "i",
        },
    ]
}

fn schema_pg_database() -> Schema {
    schema([
        Field::required("datname", text()),
        Field::required("datdba", DataType::Int64),
        Field::required("encoding", DataType::Int32),
        Field::required("datallowconn", DataType::Bool),
        Field::required("datcollate", text()),
        Field::required("datctype", text()),
        Field::nullable(
            "datacl",
            DataType::Array(Box::new(DataType::Text { max_len: None })),
        ),
    ])
}

fn rows_pg_database() -> Vec<Vec<Value>> {
    vec![vec![
        v_text("ultrasql"),
        Value::Int64(10),
        Value::Int32(6),
        Value::Bool(true),
        v_text("C"),
        v_text("C"),
        Value::Null,
    ]]
}

fn schema_information_schema_tables() -> Schema {
    schema([
        Field::required("table_catalog", text()),
        Field::required("table_schema", text()),
        Field::required("table_name", text()),
        Field::required("table_type", text()),
        Field::nullable("self_referencing_column_name", text()),
        Field::nullable("reference_generation", text()),
        Field::nullable("user_defined_type_catalog", text()),
        Field::nullable("user_defined_type_schema", text()),
        Field::nullable("user_defined_type_name", text()),
        Field::required("is_insertable_into", text()),
        Field::required("is_typed", text()),
        Field::nullable("commit_action", text()),
    ])
}

fn rows_information_schema_tables(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    table_entries(ctx)
        .into_iter()
        .filter(|entry| {
            entry.schema_name != "pg_catalog"
                && entry.schema_name != "information_schema"
                && !is_materialized_view_entry(entry)
        })
        .map(|entry| {
            vec![
                v_text("ultrasql"),
                v_text(entry.schema_name.clone()),
                v_text(entry.name.clone()),
                v_text("BASE TABLE"),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                v_text("YES"),
                v_text("NO"),
                Value::Null,
            ]
        })
        .collect()
}

fn schema_information_schema_columns() -> Schema {
    schema([
        Field::required("table_catalog", text()),
        Field::required("table_schema", text()),
        Field::required("table_name", text()),
        Field::required("column_name", text()),
        Field::required("ordinal_position", DataType::Int32),
        Field::nullable("column_default", text()),
        Field::required("is_nullable", text()),
        Field::required("data_type", text()),
        Field::nullable("character_maximum_length", DataType::Int32),
        Field::nullable("character_octet_length", DataType::Int32),
        Field::nullable("numeric_precision", DataType::Int32),
        Field::nullable("numeric_precision_radix", DataType::Int32),
        Field::nullable("numeric_scale", DataType::Int32),
        Field::nullable("datetime_precision", DataType::Int32),
        Field::nullable("interval_type", text()),
        Field::nullable("interval_precision", DataType::Int32),
    ])
}

fn rows_information_schema_columns(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for entry in table_entries(ctx) {
        if entry.schema_name == "pg_catalog" || entry.schema_name == "information_schema" {
            continue;
        }
        for (idx, field) in entry.schema.fields().iter().enumerate() {
            rows.push(vec![
                v_text("ultrasql"),
                v_text(entry.schema_name.clone()),
                v_text(entry.name.clone()),
                v_text(field.name.clone()),
                Value::Int32(i32::try_from(idx + 1).unwrap_or(i32::MAX)),
                Value::Null,
                v_text(if field.nullable { "YES" } else { "NO" }),
                v_text(data_type_name(&field.data_type)),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ]);
        }
    }
    rows
}

fn constraint_type_name(kind: &str) -> &'static str {
    match kind {
        "p" => "PRIMARY KEY",
        "u" => "UNIQUE",
        "f" => "FOREIGN KEY",
        "c" => "CHECK",
        _ => "CHECK",
    }
}

fn schema_information_schema_table_constraints() -> Schema {
    schema([
        Field::required("constraint_catalog", text()),
        Field::required("constraint_schema", text()),
        Field::required("constraint_name", text()),
        Field::required("table_schema", text()),
        Field::required("table_name", text()),
        Field::required("constraint_type", text()),
        Field::required("is_deferrable", text()),
        Field::required("initially_deferred", text()),
        Field::required("enforced", text()),
        Field::required("nulls_distinct", text()),
    ])
}

fn rows_information_schema_table_constraints(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    virtual_constraints(ctx)
        .into_iter()
        .map(|c| {
            vec![
                v_text("ultrasql"),
                v_text(c.table_schema.clone()),
                v_text(c.name),
                v_text(c.table_schema),
                v_text(c.table_name),
                v_text(constraint_type_name(c.kind)),
                v_text(if c.deferrable { "YES" } else { "NO" }),
                v_text(if c.initially_deferred { "YES" } else { "NO" }),
                v_text("YES"),
                v_text("YES"),
            ]
        })
        .collect()
}

fn field_name_for_attnum(ctx: &LowerCtx<'_>, table_oid: Oid, col_idx: usize) -> Option<String> {
    let table = ctx.catalog_snapshot.tables_by_oid.get(&table_oid)?;
    Some(table.schema.field(col_idx)?.name.clone())
}

fn schema_information_schema_key_column_usage() -> Schema {
    schema([
        Field::required("constraint_catalog", text()),
        Field::required("constraint_schema", text()),
        Field::required("constraint_name", text()),
        Field::required("table_catalog", text()),
        Field::required("table_schema", text()),
        Field::required("table_name", text()),
        Field::required("column_name", text()),
        Field::required("ordinal_position", DataType::Int32),
        Field::nullable("position_in_unique_constraint", DataType::Int32),
    ])
}

fn rows_information_schema_key_column_usage(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for c in virtual_constraints(ctx) {
        if !matches!(c.kind, "p" | "u" | "f") {
            continue;
        }
        for (idx, col_idx) in c.columns.iter().copied().enumerate() {
            let Some(column_name) = field_name_for_attnum(ctx, c.table_oid, col_idx) else {
                continue;
            };
            rows.push(vec![
                v_text("ultrasql"),
                v_text(c.table_schema.clone()),
                v_text(c.name.clone()),
                v_text("ultrasql"),
                v_text(c.table_schema.clone()),
                v_text(c.table_name.clone()),
                v_text(column_name),
                Value::Int32(i32::try_from(idx + 1).unwrap_or(i32::MAX)),
                if c.kind == "f" {
                    Value::Int32(i32::try_from(idx + 1).unwrap_or(i32::MAX))
                } else {
                    Value::Null
                },
            ]);
        }
    }
    rows
}

fn referenced_constraint_name(ctx: &LowerCtx<'_>, table_oid: Oid) -> String {
    ctx.catalog_snapshot
        .indexes_by_table
        .get(&table_oid)
        .and_then(|indexes| {
            indexes
                .iter()
                .find(|idx| idx.is_unique && idx.name.ends_with("_pkey"))
                .or_else(|| indexes.iter().find(|idx| idx.is_unique))
        })
        .map(|idx| idx.name.clone())
        .unwrap_or_else(|| format!("{}_key", table_oid.raw()))
}

const fn referential_action_name(action: LogicalReferentialAction) -> &'static str {
    match action {
        LogicalReferentialAction::NoAction => "NO ACTION",
        LogicalReferentialAction::Restrict => "RESTRICT",
        LogicalReferentialAction::Cascade => "CASCADE",
        LogicalReferentialAction::SetNull => "SET NULL",
        LogicalReferentialAction::SetDefault => "SET DEFAULT",
    }
}

fn schema_information_schema_referential_constraints() -> Schema {
    schema([
        Field::required("constraint_catalog", text()),
        Field::required("constraint_schema", text()),
        Field::required("constraint_name", text()),
        Field::required("unique_constraint_catalog", text()),
        Field::required("unique_constraint_schema", text()),
        Field::required("unique_constraint_name", text()),
        Field::required("match_option", text()),
        Field::required("update_rule", text()),
        Field::required("delete_rule", text()),
    ])
}

fn rows_information_schema_referential_constraints(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    virtual_constraints(ctx)
        .into_iter()
        .filter(|c| c.kind == "f")
        .map(|c| {
            let target_oid = c.foreign_table_oid.unwrap_or(c.table_oid);
            let unique_name = referenced_constraint_name(ctx, target_oid);
            let target_schema = ctx
                .catalog_snapshot
                .tables_by_oid
                .get(&target_oid)
                .map(|table| table.schema_name.clone())
                .unwrap_or_else(|| "public".to_owned());
            vec![
                v_text("ultrasql"),
                v_text(c.table_schema),
                v_text(c.name),
                v_text("ultrasql"),
                v_text(target_schema),
                v_text(unique_name),
                v_text("NONE"),
                v_text(referential_action_name(c.on_update)),
                v_text(referential_action_name(c.on_delete)),
            ]
        })
        .collect()
}

fn schema_information_schema_check_constraints() -> Schema {
    schema([
        Field::required("constraint_catalog", text()),
        Field::required("constraint_schema", text()),
        Field::required("constraint_name", text()),
        Field::nullable("check_clause", text()),
    ])
}

fn rows_information_schema_check_constraints(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    virtual_constraints(ctx)
        .into_iter()
        .filter(|c| c.kind == "c")
        .map(|c| {
            vec![
                v_text("ultrasql"),
                v_text(c.table_schema),
                v_text(c.name),
                c.check_clause.map_or(Value::Null, v_text),
            ]
        })
        .collect()
}

fn schema_information_schema_schemata() -> Schema {
    schema([
        Field::required("catalog_name", text()),
        Field::required("schema_name", text()),
        Field::required("schema_owner", text()),
        Field::nullable("default_character_set_catalog", text()),
        Field::nullable("default_character_set_schema", text()),
        Field::nullable("default_character_set_name", text()),
        Field::nullable("sql_path", text()),
    ])
}

fn rows_information_schema_schemata(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = vec![
        vec![
            v_text("ultrasql"),
            v_text("pg_catalog"),
            v_text("ultrasql"),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
        vec![
            v_text("ultrasql"),
            v_text("information_schema"),
            v_text("ultrasql"),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
        vec![
            v_text("ultrasql"),
            v_text("public"),
            v_text("ultrasql"),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
    ];
    rows.extend(
        runtime_schema_rows(ctx)
            .into_iter()
            .map(|(name, owner_role, _)| {
                vec![
                    v_text("ultrasql"),
                    v_text(name),
                    v_text(owner_role),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]
            }),
    );
    rows
}

fn schema_information_schema_sequences() -> Schema {
    schema([
        Field::required("sequence_catalog", text()),
        Field::required("sequence_schema", text()),
        Field::required("sequence_name", text()),
        Field::required("data_type", text()),
        Field::nullable("numeric_precision", DataType::Int32),
        Field::nullable("numeric_precision_radix", DataType::Int32),
        Field::nullable("numeric_scale", DataType::Int32),
        Field::required("start_value", text()),
        Field::required("minimum_value", text()),
        Field::required("maximum_value", text()),
        Field::required("increment", text()),
        Field::required("cycle_option", text()),
    ])
}

fn rows_information_schema_sequences(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut seqs: Vec<_> = ctx
        .sequences
        .iter()
        .map(|e| (e.key().clone(), e.value().clone()))
        .collect();
    seqs.sort_by(|a, b| a.0.cmp(&b.0));
    seqs.into_iter()
        .map(|(name, seq)| {
            let opts = seq.options_snapshot();
            vec![
                v_text("ultrasql"),
                v_text("public"),
                v_text(name),
                v_text("bigint"),
                Value::Null,
                Value::Null,
                Value::Null,
                v_text(opts.start.to_string()),
                v_text(seq.min_value().to_string()),
                v_text(seq.max_value().to_string()),
                v_text(opts.increment.to_string()),
                v_text(if opts.cycle { "YES" } else { "NO" }),
            ]
        })
        .collect()
}

fn schema_information_schema_routines() -> Schema {
    schema([
        Field::required("specific_catalog", text()),
        Field::required("specific_schema", text()),
        Field::required("specific_name", text()),
        Field::required("routine_catalog", text()),
        Field::required("routine_schema", text()),
        Field::required("routine_name", text()),
        Field::required("routine_type", text()),
        Field::nullable("data_type", text()),
        Field::nullable("type_udt_catalog", text()),
        Field::nullable("type_udt_schema", text()),
        Field::nullable("type_udt_name", text()),
        Field::required("is_deterministic", text()),
        Field::required("sql_data_access", text()),
        Field::required("security_type", text()),
    ])
}

fn rows_information_schema_routines() -> Vec<Vec<Value>> {
    pg_proc_builtins()
        .iter()
        .enumerate()
        .filter_map(|(offset, builtin)| {
            let oid = pg_proc_oid(offset)?;
            Some(vec![
                v_text("ultrasql"),
                v_text("pg_catalog"),
                v_text(format!("{}_{}", builtin.name, oid)),
                v_text("ultrasql"),
                v_text("pg_catalog"),
                v_text(builtin.name),
                v_text("FUNCTION"),
                v_text(pg_type_name_from_oid(builtin.return_type_oid)),
                Value::Null,
                Value::Null,
                Value::Null,
                v_text("NO"),
                v_text("READS SQL DATA"),
                v_text("INVOKER"),
            ])
        })
        .collect()
}

fn schema_information_schema_triggers() -> Schema {
    schema([
        Field::required("trigger_catalog", text()),
        Field::required("trigger_schema", text()),
        Field::required("trigger_name", text()),
        Field::required("event_manipulation", text()),
        Field::required("event_object_catalog", text()),
        Field::required("event_object_schema", text()),
        Field::required("event_object_table", text()),
        Field::required("action_order", DataType::Int32),
        Field::nullable("action_condition", text()),
        Field::required("action_statement", text()),
        Field::required("action_orientation", text()),
        Field::required("action_timing", text()),
    ])
}

#[allow(dead_code)]
fn _oid_value(oid: Oid) -> Value {
    v_i64(oid.raw())
}
