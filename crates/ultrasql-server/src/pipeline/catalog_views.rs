//! Virtual `pg_catalog` and `information_schema` relations.
//!
//! These scans expose metadata from the same catalog snapshot used by the
//! binder. They are deliberately read-only and statement-local: a SELECT sees
//! the snapshot captured at statement start, matching normal catalog lookup.

use ultrasql_core::{DataType, Field, Oid, Schema, Value};
use ultrasql_executor::{MemTableScan, Operator, build_batch};
use ultrasql_planner::LogicalReferentialAction;

use crate::error::ServerError;

use super::LowerCtx;

const PG_CATALOG_OID: i64 = 11;
const INFORMATION_SCHEMA_OID: i64 = 12;
const PUBLIC_OID: i64 = 2200;
const PG_CLASS_OID: i64 = 1259;
const PG_CONSTRAINT_OID: i64 = 2606;
const PG_TYPE_BOOL: i32 = 16;
const PG_TYPE_INT2: i32 = 21;
const PG_TYPE_INT4: i32 = 23;
const PG_TYPE_INT8: i32 = 20;
const PG_TYPE_FLOAT4: i32 = 700;
const PG_TYPE_FLOAT8: i32 = 701;
const PG_TYPE_TEXT: i32 = 25;
const PG_TYPE_NUMERIC: i32 = 1700;
const PG_TYPE_DATE: i32 = 1082;
const PG_TYPE_TIMESTAMP: i32 = 1114;
const PG_TYPE_TIMESTAMPTZ: i32 = 1184;
const PG_TYPE_TIME: i32 = 1083;

/// Return the schema for a virtual catalog relation or view.
#[must_use]
pub(crate) fn virtual_catalog_schema(name: &str) -> Option<Schema> {
    match normalized_name(name).as_str() {
        "pg_catalog.pg_namespace" => Some(schema_pg_namespace()),
        "pg_catalog.pg_class" => Some(schema_pg_class()),
        "pg_catalog.pg_attribute" => Some(schema_pg_attribute()),
        "pg_catalog.pg_index" => Some(schema_pg_index()),
        "pg_catalog.pg_constraint" => Some(schema_pg_constraint()),
        "pg_catalog.pg_sequence" => Some(schema_pg_sequence()),
        "pg_catalog.pg_depend" => Some(schema_pg_depend()),
        "pg_catalog.pg_description" => Some(schema_pg_description()),
        "pg_catalog.pg_statistic" => Some(schema_pg_statistic()),
        "pg_catalog.pg_statistic_ext" => Some(schema_pg_statistic_ext()),
        "pg_catalog.pg_tables" => Some(schema_pg_tables()),
        "pg_catalog.pg_indexes" => Some(schema_pg_indexes()),
        "pg_catalog.pg_views" => Some(schema_pg_views()),
        "pg_catalog.pg_sequences" => Some(schema_pg_sequences()),
        "pg_catalog.pg_roles" => Some(schema_pg_roles()),
        "pg_catalog.pg_user" => Some(schema_pg_user()),
        "pg_catalog.pg_settings" => Some(schema_pg_settings()),
        "pg_catalog.pg_locks" => Some(schema_pg_locks()),
        "pg_catalog.pg_stat_activity" => Some(schema_pg_stat_activity()),
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
        "pg_catalog.pg_namespace" => Some((schema_pg_namespace(), rows_pg_namespace())),
        "pg_catalog.pg_class" => Some((schema_pg_class(), rows_pg_class(ctx))),
        "pg_catalog.pg_attribute" => Some((schema_pg_attribute(), rows_pg_attribute(ctx))),
        "pg_catalog.pg_index" => Some((schema_pg_index(), rows_pg_index(ctx))),
        "pg_catalog.pg_constraint" => Some((schema_pg_constraint(), rows_pg_constraint(ctx))),
        "pg_catalog.pg_sequence" => Some((schema_pg_sequence(), rows_pg_sequence(ctx))),
        "pg_catalog.pg_depend" => Some((schema_pg_depend(), rows_pg_depend(ctx))),
        "pg_catalog.pg_description" => Some((schema_pg_description(), rows_pg_description(ctx))),
        "pg_catalog.pg_statistic" => Some((schema_pg_statistic(), rows_pg_statistic(ctx))),
        "pg_catalog.pg_statistic_ext" => {
            Some((schema_pg_statistic_ext(), rows_pg_statistic_ext(ctx)))
        },
        "pg_catalog.pg_tables" => Some((schema_pg_tables(), rows_pg_tables(ctx))),
        "pg_catalog.pg_indexes" => Some((schema_pg_indexes(), rows_pg_indexes(ctx))),
        "pg_catalog.pg_views" => Some((schema_pg_views(), Vec::new())),
        "pg_catalog.pg_sequences" => Some((schema_pg_sequences(), rows_pg_sequences(ctx))),
        "pg_catalog.pg_roles" => Some((schema_pg_roles(), rows_pg_roles())),
        "pg_catalog.pg_user" => Some((schema_pg_user(), rows_pg_user())),
        "pg_catalog.pg_settings" => Some((schema_pg_settings(), rows_pg_settings())),
        "pg_catalog.pg_locks" => Some((schema_pg_locks(), Vec::new())),
        "pg_catalog.pg_stat_activity" => Some((schema_pg_stat_activity(), rows_pg_stat_activity())),
        "pg_catalog.pg_proc" => Some((schema_pg_proc(), Vec::new())),
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
            rows_information_schema_schemata(),
        )),
        "information_schema.sequences" => Some((
            schema_information_schema_sequences(),
            rows_information_schema_sequences(ctx),
        )),
        "information_schema.routines" => Some((schema_information_schema_routines(), Vec::new())),
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
        "pg_namespace" | "pg_class" | "pg_attribute" | "pg_index" | "pg_constraint"
        | "pg_sequence" | "pg_depend" | "pg_description" | "pg_tables" | "pg_indexes"
        | "pg_statistic" | "pg_statistic_ext" | "pg_views" | "pg_sequences" | "pg_roles"
        | "pg_user" | "pg_settings" | "pg_locks" | "pg_stat_activity" | "pg_proc"
        | "pg_database" => format!("pg_catalog.{folded}"),
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
    Schema::new(fields).expect("virtual catalog schema has unique columns")
}

fn text() -> DataType {
    DataType::Text { max_len: None }
}

fn v_text(v: impl Into<String>) -> Value {
    Value::Text(v.into())
}

fn v_i64(v: u32) -> Value {
    Value::Int64(i64::from(v))
}

fn namespace_oid(schema_name: &str) -> i64 {
    match schema_name {
        "pg_catalog" => PG_CATALOG_OID,
        "information_schema" => INFORMATION_SCHEMA_OID,
        _ => PUBLIC_OID,
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
        DataType::Date => PG_TYPE_DATE,
        DataType::Timestamp => PG_TYPE_TIMESTAMP,
        DataType::TimestampTz => PG_TYPE_TIMESTAMPTZ,
        DataType::Time => PG_TYPE_TIME,
        _ => PG_TYPE_TEXT,
    }
}

fn data_type_name(dt: &DataType) -> &'static str {
    match dt {
        DataType::Bool => "boolean",
        DataType::Int16 => "smallint",
        DataType::Int32 => "integer",
        DataType::Int64 => "bigint",
        DataType::Float32 => "real",
        DataType::Float64 => "double precision",
        DataType::Decimal { .. } => "numeric",
        DataType::Text { .. } => "text",
        DataType::Bytea => "bytea",
        DataType::Timestamp => "timestamp without time zone",
        DataType::TimestampTz => "timestamp with time zone",
        DataType::Date => "date",
        DataType::Time => "time without time zone",
        DataType::Interval => "interval",
        DataType::Uuid => "uuid",
        DataType::Jsonb => "jsonb",
        DataType::Array(_) => "array",
        DataType::Record(_) => "record",
        DataType::Null => "unknown",
        _ => "text",
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

fn schema_pg_namespace() -> Schema {
    schema([
        Field::required("oid", DataType::Int64),
        Field::required("nspname", text()),
        Field::required("nspowner", DataType::Int64),
    ])
}

fn rows_pg_namespace() -> Vec<Vec<Value>> {
    vec![
        vec![
            Value::Int64(PG_CATALOG_OID),
            v_text("pg_catalog"),
            Value::Int64(10),
        ],
        vec![
            Value::Int64(INFORMATION_SCHEMA_OID),
            v_text("information_schema"),
            Value::Int64(10),
        ],
        vec![Value::Int64(PUBLIC_OID), v_text("public"), Value::Int64(10)],
    ]
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
    ])
}

fn rows_pg_class(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for entry in table_entries(ctx) {
        rows.push(vec![
            v_i64(entry.oid.raw()),
            v_text(entry.name.clone()),
            Value::Int64(namespace_oid(&entry.schema_name)),
            v_text("r"),
            Value::Int32(i32::try_from(entry.n_blocks).unwrap_or(i32::MAX)),
            Value::Float64(0.0),
            Value::Int32(i32::try_from(entry.root_block.raw()).unwrap_or(i32::MAX)),
            Value::Bool(
                ctx.catalog_snapshot
                    .indexes_by_table
                    .contains_key(&entry.oid),
            ),
        ]);
    }
    let mut indexes: Vec<_> = ctx.catalog_snapshot.indexes.values().collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    for index in indexes {
        rows.push(vec![
            v_i64(index.oid.raw()),
            v_text(index.name.clone()),
            Value::Int64(PUBLIC_OID),
            v_text("i"),
            Value::Int32(0),
            Value::Float64(0.0),
            Value::Int32(i32::try_from(index.root_block.raw()).unwrap_or(i32::MAX)),
            Value::Bool(false),
        ]);
    }
    rows
}

fn schema_pg_attribute() -> Schema {
    schema([
        Field::required("attrelid", DataType::Int64),
        Field::required("attname", text()),
        Field::required("atttypid", DataType::Int32),
        Field::required("attnum", DataType::Int16),
        Field::required("attnotnull", DataType::Bool),
        Field::required("atthasdef", DataType::Bool),
        Field::required("attisdropped", DataType::Bool),
    ])
}

fn rows_pg_attribute(ctx: &LowerCtx<'_>) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for entry in table_entries(ctx) {
        for (idx, field) in entry.schema.fields().iter().enumerate() {
            rows.push(vec![
                v_i64(entry.oid.raw()),
                v_text(field.name.clone()),
                Value::Int32(type_oid(&field.data_type)),
                Value::Int16(i16::try_from(idx + 1).unwrap_or(i16::MAX)),
                Value::Bool(!field.nullable),
                Value::Bool(false),
                Value::Bool(false),
            ]);
        }
    }
    rows
}

fn schema_pg_index() -> Schema {
    schema([
        Field::required("indexrelid", DataType::Int64),
        Field::required("indrelid", DataType::Int64),
        Field::required("indnatts", DataType::Int16),
        Field::required("indisunique", DataType::Bool),
        Field::required("indisprimary", DataType::Bool),
        Field::required("indisvalid", DataType::Bool),
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
                Value::Bool(true),
            ]
        })
        .collect()
}

#[derive(Clone, Debug)]
struct VirtualConstraint {
    oid: i64,
    name: String,
    kind: &'static str,
    table_oid: Oid,
    table_schema: String,
    table_name: String,
    columns: Vec<usize>,
    foreign_table_oid: Option<Oid>,
    foreign_columns: Vec<usize>,
    on_delete: LogicalReferentialAction,
    on_update: LogicalReferentialAction,
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
            table_schema: table.schema_name.clone(),
            table_name: table.name.clone(),
            columns: index.columns.iter().map(|c| usize::from(*c)).collect(),
            foreign_table_oid: None,
            foreign_columns: Vec::new(),
            on_delete: LogicalReferentialAction::NoAction,
            on_update: LogicalReferentialAction::NoAction,
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
                table_schema: table.schema_name.clone(),
                table_name: table.name.clone(),
                columns: Vec::new(),
                foreign_table_oid: None,
                foreign_columns: Vec::new(),
                on_delete: LogicalReferentialAction::NoAction,
                on_update: LogicalReferentialAction::NoAction,
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
                table_schema: table.schema_name.clone(),
                table_name: table.name.clone(),
                columns: fk.columns.clone(),
                foreign_table_oid: Some(fk.target_oid),
                foreign_columns: fk.target_columns.clone(),
                on_delete: fk.on_delete,
                on_update: fk.on_update,
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
                c.foreign_table_oid
                    .map_or(Value::Int64(0), |oid| v_i64(oid.raw())),
                attnums_text(&c.columns),
                attnums_text(&c.foreign_columns),
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false),
            ]
        })
        .collect()
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
        Field::required("stxkeys", text()),
        Field::required("stxkind", text()),
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
                v_text(stxkeys),
                v_text(stxkind),
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
            entry.schema_name != "pg_catalog" && entry.schema_name != "information_schema"
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
            vec![
                v_text("public"),
                v_text(name),
                v_text("ultrasql"),
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

fn rows_pg_roles() -> Vec<Vec<Value>> {
    vec![vec![
        v_text("ultrasql"),
        Value::Bool(true),
        Value::Bool(true),
        Value::Bool(true),
        Value::Bool(true),
        Value::Bool(true),
        Value::Bool(false),
        Value::Bool(false),
        Value::Int32(-1),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Int64(10),
    ]]
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

fn rows_pg_user() -> Vec<Vec<Value>> {
    vec![vec![
        v_text("ultrasql"),
        Value::Int64(10),
        Value::Bool(true),
        Value::Bool(true),
        Value::Bool(false),
        Value::Bool(false),
        Value::Null,
        Value::Null,
        Value::Null,
    ]]
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

fn rows_pg_settings() -> Vec<Vec<Value>> {
    vec![
        vec![
            v_text("server_version"),
            v_text(env!("CARGO_PKG_VERSION")),
            Value::Null,
            v_text("Preset Options"),
            v_text("UltraSQL server version."),
            v_text("string"),
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
            v_text("search_path"),
            v_text("public"),
            Value::Null,
            v_text("Client Connection Defaults / Statement Behavior"),
            v_text("Sets the schema search order."),
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
    ]
}

fn schema_pg_locks() -> Schema {
    schema([
        Field::nullable("locktype", text()),
        Field::nullable("database", DataType::Int64),
        Field::nullable("relation", DataType::Int64),
        Field::nullable("page", DataType::Int32),
        Field::nullable("tuple", DataType::Int16),
        Field::required("pid", DataType::Int32),
        Field::nullable("mode", text()),
        Field::required("granted", DataType::Bool),
    ])
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
    ])
}

fn rows_pg_stat_activity() -> Vec<Vec<Value>> {
    vec![vec![
        Value::Int64(1),
        v_text("ultrasql"),
        Value::Int32(0),
        v_text("ultrasql"),
        Value::Null,
        v_text("active"),
        Value::Null,
    ]]
}

fn schema_pg_proc() -> Schema {
    schema([
        Field::required("proname", text()),
        Field::required("pronamespace", DataType::Int64),
        Field::required("prokind", DataType::Text { max_len: Some(1) }),
    ])
}

fn schema_pg_database() -> Schema {
    schema([
        Field::required("datname", text()),
        Field::required("datdba", DataType::Int64),
    ])
}

fn rows_pg_database() -> Vec<Vec<Value>> {
    vec![vec![v_text("ultrasql"), Value::Int64(10)]]
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
            entry.schema_name != "pg_catalog" && entry.schema_name != "information_schema"
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
                v_text("NO"),
                v_text("NO"),
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

fn rows_information_schema_schemata() -> Vec<Vec<Value>> {
    vec![
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
    ]
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
