//! `pg_type`, `pg_am`, `pg_range`, `pg_collation`, and `pg_enum` scans plus
//! the column-default expression helper shared with `pg_attrdef`.

use ultrasql_core::{DataType, Field, Oid, Schema, Value};

use crate::pipeline::LowerCtx;

use super::common::*;

pub(super) fn schema_pg_type() -> Schema {
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

pub(super) fn rows_pg_type(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
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
    for entry in table_entries(ctx) {
        rows.push(vec![
            v_oid(relation_type_oid(entry.oid)),
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

pub(super) fn schema_pg_am() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("amname", text()),
        Field::required("amhandler", DataType::Int64),
        Field::required("amtype", DataType::Text { max_len: Some(1) }),
    ])
}

pub(super) fn rows_pg_am() -> Vec<Vec<Value>> {
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

pub(super) fn schema_pg_range() -> Schema {
    schema([
        Field::required("rngtypid", DataType::Oid),
        Field::required("rngsubtype", DataType::Oid),
    ])
}

pub(super) fn rows_pg_range() -> Vec<Vec<Value>> {
    vec![
        vec![v_oid_i32(PG_TYPE_INT4RANGE), v_oid_i32(PG_TYPE_INT4)],
        vec![v_oid_i32(PG_TYPE_INT8RANGE), v_oid_i32(PG_TYPE_INT8)],
        vec![v_oid_i32(PG_TYPE_NUMRANGE), v_oid_i32(PG_TYPE_NUMERIC)],
        vec![v_oid_i32(PG_TYPE_DATERANGE), v_oid_i32(PG_TYPE_DATE)],
        vec![v_oid_i32(PG_TYPE_TSRANGE), v_oid_i32(PG_TYPE_TIMESTAMP)],
        vec![v_oid_i32(PG_TYPE_TSTZRANGE), v_oid_i32(PG_TYPE_TIMESTAMPTZ)],
    ]
}

pub(super) fn schema_pg_collation() -> Schema {
    schema([
        Field::required("oid", DataType::Oid),
        Field::required("collname", text()),
    ])
}

pub(super) fn rows_pg_collation() -> Vec<Vec<Value>> {
    vec![
        vec![
            Value::Oid(Oid::new(PG_COLLATION_DEFAULT_OID)),
            v_text("default"),
        ],
        vec![Value::Oid(Oid::new(950)), v_text("C")],
        vec![Value::Oid(Oid::new(951)), v_text("POSIX")],
    ]
}

pub(super) fn type_category_text(ty: &DataType) -> &'static str {
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

pub(super) fn schema_pg_enum() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("enumtypid", DataType::Int64),
        Field::required("enumsortorder", DataType::Float32),
        Field::required("enumlabel", text()),
    ])
}

pub(super) fn rows_pg_enum(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
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

pub(super) fn enum_sort_order_f32(sort_order: u32) -> f32 {
    sort_order.to_string().parse::<f32>().unwrap_or(f32::MAX)
}

pub(super) fn column_default_expr(ctx: &LowerCtx<'_>, relid: Oid, idx: usize) -> Option<String> {
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
