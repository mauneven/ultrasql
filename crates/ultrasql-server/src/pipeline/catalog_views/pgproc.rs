//! `pg_proc` scan and the built-in procedure catalog plus the type-OID and
//! type-name helpers shared with the other catalog views.

use ultrasql_core::{DataType, Field, Oid, RangeType, Schema, Value};

use super::common::*;

pub(super) fn schema_pg_proc() -> Schema {
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

pub(super) fn rows_pg_proc() -> Vec<Vec<Value>> {
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

pub(super) fn pg_proc_oid(offset: usize) -> Option<i64> {
    let offset = u32::try_from(offset).ok()?;
    PG_PROC_BASE_OID.checked_add(offset).map(i64::from)
}

pub(super) fn pg_proc_oid_by_name(name: &str) -> Option<u32> {
    pg_proc_builtins()
        .iter()
        .enumerate()
        .find(|(_, builtin)| builtin.name.eq_ignore_ascii_case(name))
        .and_then(|(offset, _)| {
            let offset = u32::try_from(offset).ok()?;
            PG_PROC_BASE_OID.checked_add(offset)
        })
}

pub(super) fn pg_type_oid_for_data_type(data_type: &DataType) -> u32 {
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

pub(super) fn pg_array_type_oid_for_data_type(data_type: &DataType) -> u32 {
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

pub(super) fn pg_range_type_oid(range: RangeType) -> u32 {
    match range {
        RangeType::Int4 => 3904,
        RangeType::Num => 3906,
        RangeType::Timestamp => 3908,
        RangeType::TimestampTz => 3910,
        RangeType::Date => 3912,
        RangeType::Int8 => 3926,
    }
}

pub(super) struct PgProcBuiltin {
    pub(super) name: &'static str,
    pub(super) return_type_oid: u32,
    pub(super) arg_type_oids: &'static [u32],
    pub(super) volatility: &'static str,
}

pub(super) fn proc_argtypes_value(arg_type_oids: &[u32]) -> Value {
    Value::Array {
        element_type: DataType::Oid,
        elements: arg_type_oids
            .iter()
            .map(|oid| Value::Oid(Oid::new(*oid)))
            .collect(),
    }
}

pub(super) fn pg_type_name_from_oid(oid: u32) -> &'static str {
    match oid {
        PROC_TYPE_BOOL => "boolean",
        PROC_TYPE_INT4 => "integer",
        PROC_TYPE_INT8 => "bigint",
        PROC_TYPE_FLOAT8 => "double precision",
        PROC_TYPE_TEXT => "text",
        PROC_TYPE_OID => "oid",
        PROC_TYPE_TEXT_ARRAY => "ARRAY",
        PROC_TYPE_XML => "xml",
        PROC_TYPE_XML_ARRAY => "xml[]",
        PROC_TYPE_TSVECTOR => "tsvector",
        PROC_TYPE_TSQUERY => "tsquery",
        PROC_TYPE_UUID => "uuid",
        _ => "text",
    }
}

pub(super) const fn pg_proc_builtins() -> &'static [PgProcBuiltin] {
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
        PgProcBuiltin {
            name: "to_tsvector",
            return_type_oid: PROC_TYPE_TSVECTOR,
            arg_type_oids: &[PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "to_tsvector",
            return_type_oid: PROC_TYPE_TSVECTOR,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "to_tsquery",
            return_type_oid: PROC_TYPE_TSQUERY,
            arg_type_oids: &[PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "to_tsquery",
            return_type_oid: PROC_TYPE_TSQUERY,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "plainto_tsquery",
            return_type_oid: PROC_TYPE_TSQUERY,
            arg_type_oids: &[PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "plainto_tsquery",
            return_type_oid: PROC_TYPE_TSQUERY,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "websearch_to_tsquery",
            return_type_oid: PROC_TYPE_TSQUERY,
            arg_type_oids: &[PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "websearch_to_tsquery",
            return_type_oid: PROC_TYPE_TSQUERY,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "phraseto_tsquery",
            return_type_oid: PROC_TYPE_TSQUERY,
            arg_type_oids: &[PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "phraseto_tsquery",
            return_type_oid: PROC_TYPE_TSQUERY,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_TEXT],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "ts_rank",
            return_type_oid: PROC_TYPE_FLOAT8,
            arg_type_oids: &[PROC_TYPE_TSVECTOR, PROC_TYPE_TSQUERY],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "ts_rank_cd",
            return_type_oid: PROC_TYPE_FLOAT8,
            arg_type_oids: &[PROC_TYPE_TSVECTOR, PROC_TYPE_TSQUERY],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "ts_headline",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_TSQUERY],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "ts_headline",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_TEXT, PROC_TYPE_TEXT, PROC_TYPE_TSQUERY],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "numnode",
            return_type_oid: PROC_TYPE_INT4,
            arg_type_oids: &[PROC_TYPE_TSQUERY],
            volatility: "i",
        },
        PgProcBuiltin {
            name: "querytree",
            return_type_oid: PROC_TYPE_TEXT,
            arg_type_oids: &[PROC_TYPE_TSQUERY],
            volatility: "i",
        },
    ]
}

pub(crate) fn pg_proc_builtin_exists(name: &str) -> bool {
    pg_proc_builtins()
        .iter()
        .any(|builtin| builtin.name.eq_ignore_ascii_case(name))
}
