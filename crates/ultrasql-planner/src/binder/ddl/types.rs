//! Type-name resolution and the type-defining DDL binders: `CREATE
//! TYPE`, `CREATE DOMAIN`, and `CREATE OPERATOR`.

use ultrasql_core::{DataType, Field, GeometryType, MAX_VECTOR_DIMS, RangeType, Schema};
use ultrasql_parser::ast::{
    CreateDomainStmt, CreateOperatorStmt, CreateTypeKind, CreateTypeStmt, DomainConstraint,
    ObjectName, TypeName,
};

use super::super::expr_bind::resolve_builtin_collation;
use super::super::{
    Catalog, LogicalPlan, PlanError, ScopeStack, bind_expr, object_name_simple,
    parse_pg_identifier_path,
};
use super::shared::{MAX_NUMERIC_PRECISION, named_or, object_name_namespace};
use crate::plan::LogicalCheckConstraint;

pub(super) fn bind_column_collation(
    column_name: &str,
    data_type: &DataType,
    collation: Option<&ObjectName>,
) -> Result<Option<u32>, PlanError> {
    let Some(collation) = collation else {
        return Ok(None);
    };
    if !column_type_is_collatable(data_type) {
        return Err(PlanError::TypeMismatch(format!(
            "COLLATE applies to text types, column '{column_name}' has type {data_type}"
        )));
    }
    Ok(Some(resolve_builtin_collation(collation)?.oid()))
}

fn column_type_is_collatable(data_type: &DataType) -> bool {
    match data_type {
        DataType::Text { .. } | DataType::Char { .. } => true,
        DataType::Domain { base_type, .. } => column_type_is_collatable(base_type),
        _ => false,
    }
}

pub(in crate::binder) fn bind_create_type(
    s: &CreateTypeStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let type_name = object_name_simple(&s.name);
    let namespace = object_name_namespace(&s.name);
    if catalog
        .lookup_type_in_schema(&namespace, &type_name)
        .is_some()
    {
        return Err(PlanError::TypeMismatch(format!(
            "type '{type_name}' already exists"
        )));
    }
    match &s.kind {
        CreateTypeKind::Enum { labels } => {
            if labels.is_empty() {
                return Err(PlanError::TypeMismatch(format!(
                    "enum type '{type_name}' must have at least one label"
                )));
            }
            let mut seen = std::collections::HashSet::with_capacity(labels.len());
            for label in labels {
                if !seen.insert(label) {
                    return Err(PlanError::TypeMismatch(format!(
                        "enum type '{type_name}' repeats label '{label}'"
                    )));
                }
            }
            Ok(LogicalPlan::CreateTypeEnum {
                type_name,
                namespace,
                labels: labels.clone(),
                schema: Schema::empty(),
            })
        }
        CreateTypeKind::Composite { attributes } => {
            if attributes.is_empty() {
                return Err(PlanError::TypeMismatch(format!(
                    "composite type '{type_name}' must have at least one attribute"
                )));
            }
            let fields = attributes
                .iter()
                .map(|attr| {
                    let data_type = resolve_type_name_with_catalog(&attr.data_type, catalog)?;
                    Ok(Field::nullable(attr.name.value.clone(), data_type))
                })
                .collect::<Result<Vec<_>, PlanError>>()?;
            let attributes =
                Schema::new(fields).map_err(|e| PlanError::TypeMismatch(e.to_string()))?;
            Ok(LogicalPlan::CreateTypeComposite {
                type_name,
                namespace,
                attributes,
                schema: Schema::empty(),
            })
        }
    }
}

pub(in crate::binder) fn bind_create_domain(
    s: &CreateDomainStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let domain_name = object_name_simple(&s.name);
    let namespace = object_name_namespace(&s.name);
    if catalog
        .lookup_type_in_schema(&namespace, &domain_name)
        .is_some()
    {
        return Err(PlanError::TypeMismatch(format!(
            "type '{domain_name}' already exists"
        )));
    }
    let base_type = resolve_type_name_with_catalog(&s.data_type, catalog)?;
    if matches!(base_type, DataType::Domain { .. }) {
        return Err(PlanError::NotSupported(
            "CREATE DOMAIN over another domain is not implemented",
        ));
    }
    let mut not_null = false;
    let mut check_ordinal = 0usize;
    let check_scope = Schema::new([Field::nullable("value", base_type.clone())])
        .map_err(|err| PlanError::TypeMismatch(err.to_string()))?;
    let mut checks = Vec::new();
    for constraint in &s.constraints {
        match constraint {
            DomainConstraint::NotNull { .. } => not_null = true,
            DomainConstraint::Null { .. } => not_null = false,
            DomainConstraint::Check { name, expr, .. } => {
                check_ordinal += 1;
                let mut scope = ScopeStack::new();
                let bound = bind_expr(expr, &check_scope, catalog, &mut scope)?;
                let ty = bound.data_type();
                if ty != DataType::Bool && ty != DataType::Null {
                    return Err(PlanError::TypeMismatch(format!(
                        "CHECK constraint on domain '{domain_name}' has type {:?}, expected Bool",
                        ty
                    )));
                }
                checks.push(LogicalCheckConstraint {
                    name: named_or(name.as_ref(), || {
                        format!("{domain_name}_check_{check_ordinal}")
                    }),
                    expr: bound,
                });
            }
        }
    }
    Ok(LogicalPlan::CreateDomain {
        domain_name,
        namespace,
        base_type,
        not_null,
        checks,
        schema: Schema::empty(),
    })
}

pub(in crate::binder) fn bind_create_operator(
    s: &CreateOperatorStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    if s.name.is_empty() {
        return Err(PlanError::TypeMismatch(
            "operator name must not be empty".to_owned(),
        ));
    }
    if s.left_arg.is_none() && s.right_arg.is_none() {
        return Err(PlanError::TypeMismatch(format!(
            "operator '{}' must declare LEFTARG or RIGHTARG",
            s.name
        )));
    }
    let left_type = s
        .left_arg
        .as_ref()
        .map(|ty| resolve_type_name_with_catalog(ty, catalog))
        .transpose()?;
    let right_type = s
        .right_arg
        .as_ref()
        .map(|ty| resolve_type_name_with_catalog(ty, catalog))
        .transpose()?;
    let procedure = object_name_simple(&s.procedure);
    let result_type = resolve_operator_procedure(&s.name, &left_type, &right_type, &procedure)?;
    Ok(LogicalPlan::CreateOperator {
        operator_name: s.name.clone(),
        namespace: String::from("public"),
        left_type,
        right_type,
        procedure,
        result_type,
        schema: Schema::empty(),
    })
}

fn resolve_operator_procedure(
    operator_name: &str,
    left_type: &Option<DataType>,
    right_type: &Option<DataType>,
    procedure: &str,
) -> Result<DataType, PlanError> {
    match (procedure, left_type, right_type) {
        ("bool_eq", Some(DataType::Bool), Some(DataType::Bool)) => Ok(DataType::Bool),
        ("bool_eq", _, _) => Err(PlanError::TypeMismatch(format!(
            "CREATE OPERATOR {operator_name}: bool_eq requires boolean LEFTARG and RIGHTARG"
        ))),
        _ => Err(PlanError::NotSupported(
            "CREATE OPERATOR currently supports built-in bool_eq",
        )),
    }
}

/// Resolve a parser [`TypeName`] to an UltraSQL [`DataType`].
///
/// The v0.5 type surface is intentionally narrow; types outside the
/// listed set return [`PlanError::NotSupported`]. Length modifiers
/// (e.g. `VARCHAR(255)`) are honored where the target [`DataType`]
/// carries a `max_len` slot.
pub(in crate::binder) fn resolve_type_name(t: &TypeName) -> Result<DataType, PlanError> {
    if t.is_array {
        let mut inner = t.clone();
        inner.is_array = false;
        inner.array_dimensions = 0;
        let mut ty = resolve_type_name(&inner)?;
        for _ in 0..t.array_dimensions.max(1) {
            ty = DataType::Array(Box::new(ty));
        }
        return Ok(ty);
    }
    let max_len_modifier = || t.type_modifiers.first().copied();
    match t.name.value.as_str() {
        "int" | "integer" | "int4" => Ok(DataType::Int32),
        "bigint" | "int8" => Ok(DataType::Int64),
        "smallint" | "int2" => Ok(DataType::Int16),
        "bool" | "boolean" => Ok(DataType::Bool),
        "real" | "float4" => Ok(DataType::Float32),
        "double" | "double precision" | "float" | "float8" => Ok(DataType::Float64),
        "text" => Ok(DataType::Text { max_len: None }),
        "varchar" | "character varying" => Ok(DataType::Text {
            max_len: max_len_modifier(),
        }),
        "char" | "character" => resolve_bpchar_type(max_len_modifier().or(Some(1))),
        "bpchar" => resolve_bpchar_type(max_len_modifier()),
        "bit" => resolve_bit_type(max_len_modifier().or(Some(1))),
        "varbit" | "bit varying" => resolve_varbit_type(max_len_modifier()),
        "json" => Ok(DataType::Json),
        "jsonb" => Ok(DataType::Jsonb),
        "xml" => Ok(DataType::Xml),
        "vector" => resolve_vector_family_type("VECTOR", t, |dims| DataType::Vector { dims }),
        "halfvec" => resolve_vector_family_type("HALFVEC", t, |dims| DataType::HalfVec { dims }),
        "sparsevec" => {
            resolve_vector_family_type("SPARSEVEC", t, |dims| DataType::SparseVec { dims })
        }
        "bitvec" => resolve_vector_family_type("BITVEC", t, |dims| DataType::BitVec { dims }),
        "bytea" => Ok(DataType::Bytea),
        // `DATE` columns are encoded by the row codec as 4-byte
        // little-endian i32 days since 2000-01-01 (see
        // `crates/ultrasql-executor/src/row_codec.rs`); the SQL
        // surface is enabled.
        "date" => Ok(DataType::Date),
        // Bare `NUMERIC` is unconstrained. `NUMERIC(p)` gets scale
        // zero, and `NUMERIC(p, s)` carries its declared display scale.
        // The row codec stores values in PostgreSQL's base-10000 numeric
        // payload shape; executor arithmetic still narrows runtime values
        // to the current Decimal representation.
        "decimal" | "numeric" => resolve_decimal_type(t),
        "money" => Ok(DataType::Money),
        "oid" => Ok(DataType::Oid),
        "regclass" => Ok(DataType::RegClass),
        "regtype" => Ok(DataType::RegType),
        "pg_lsn" => Ok(DataType::PgLsn),
        "inet" => Ok(DataType::Inet),
        "cidr" => Ok(DataType::Cidr),
        "macaddr" => Ok(DataType::MacAddr),
        "macaddr8" => Ok(DataType::MacAddr8),
        "time" | "time without time zone" => Ok(DataType::Time),
        "timetz" | "time with time zone" => Ok(DataType::TimeTz),
        "timestamp" | "timestamp without time zone" => Ok(DataType::Timestamp),
        "timestamptz" | "timestamp with time zone" => Ok(DataType::TimestampTz),
        "interval" => Ok(DataType::Interval),
        "uuid" => Ok(DataType::Uuid),
        "int4range" => Ok(DataType::Range(RangeType::Int4)),
        "int8range" => Ok(DataType::Range(RangeType::Int8)),
        "numrange" => Ok(DataType::Range(RangeType::Num)),
        "daterange" => Ok(DataType::Range(RangeType::Date)),
        "tsrange" => Ok(DataType::Range(RangeType::Timestamp)),
        "tstzrange" => Ok(DataType::Range(RangeType::TimestampTz)),
        "point" => Ok(DataType::Geometry(GeometryType::Point)),
        "box" => Ok(DataType::Geometry(GeometryType::Box)),
        "circle" => Ok(DataType::Geometry(GeometryType::Circle)),
        "line" => Ok(DataType::Geometry(GeometryType::Line)),
        "lseg" => Ok(DataType::Geometry(GeometryType::Lseg)),
        "path" => Ok(DataType::Geometry(GeometryType::Path)),
        "polygon" => Ok(DataType::Geometry(GeometryType::Polygon)),
        _ => Err(PlanError::NotSupported(
            "CREATE TABLE: column type not implemented in v0.5",
        )),
    }
}

pub(super) fn resolve_type_name_with_catalog(
    t: &TypeName,
    catalog: &dyn Catalog,
) -> Result<DataType, PlanError> {
    if t.is_array {
        let mut inner = t.clone();
        inner.is_array = false;
        inner.array_dimensions = 0;
        let mut ty = resolve_type_name_with_catalog(&inner, catalog)?;
        for _ in 0..t.array_dimensions.max(1) {
            ty = DataType::Array(Box::new(ty));
        }
        return Ok(ty);
    }
    match resolve_type_name(t) {
        Ok(dtype) => Ok(dtype),
        Err(PlanError::NotSupported(_)) => lookup_custom_type_name(catalog, t).ok_or({
            PlanError::NotSupported("CREATE TABLE: column type not implemented in v0.5")
        }),
        Err(err) => Err(err),
    }
}

fn lookup_custom_type_name(catalog: &dyn Catalog, t: &TypeName) -> Option<DataType> {
    let parts = parse_pg_identifier_path(&t.name.value)?;
    match parts.as_slice() {
        [type_name] => catalog.lookup_type(type_name),
        [schema_name, type_name] => {
            if schema_name.eq_ignore_ascii_case("pg_catalog") {
                let mut unqualified = t.clone();
                unqualified.name.value = type_name.to_owned();
                if let Ok(dtype) = resolve_type_name(&unqualified) {
                    return Some(dtype);
                }
            }
            catalog.lookup_type_in_schema(schema_name, type_name)
        }
        _ => None,
    }
}

fn resolve_bpchar_type(len: Option<u32>) -> Result<DataType, PlanError> {
    if matches!(len, Some(0)) {
        return Err(PlanError::TypeMismatch(
            "length for type character must be at least 1".to_owned(),
        ));
    }
    Ok(DataType::Char { len })
}

fn resolve_bit_type(len: Option<u32>) -> Result<DataType, PlanError> {
    if matches!(len, Some(0)) {
        return Err(PlanError::TypeMismatch(
            "length for type bit must be at least 1".to_owned(),
        ));
    }
    Ok(DataType::Bit { len })
}

fn resolve_varbit_type(max_len: Option<u32>) -> Result<DataType, PlanError> {
    if matches!(max_len, Some(0)) {
        return Err(PlanError::TypeMismatch(
            "length for type bit varying must be at least 1".to_owned(),
        ));
    }
    Ok(DataType::VarBit { max_len })
}

fn resolve_decimal_type(t: &TypeName) -> Result<DataType, PlanError> {
    if t.type_modifiers.len() > 2 {
        return Err(PlanError::TypeMismatch(
            "NUMERIC accepts at most precision and scale modifiers".to_owned(),
        ));
    }
    let precision = t.type_modifiers.first().copied();
    if matches!(precision, Some(0)) {
        return Err(PlanError::TypeMismatch(
            "NUMERIC precision must be at least 1".to_owned(),
        ));
    }
    if matches!(precision, Some(p) if p > MAX_NUMERIC_PRECISION) {
        return Err(PlanError::TypeMismatch(format!(
            "NUMERIC precision must be at most {MAX_NUMERIC_PRECISION}"
        )));
    }
    let modifiers = t.type_modifiers.as_slice();
    let scale = match modifiers {
        [] => None,
        [_] => Some(0),
        [_, s] => {
            let scale = i32::try_from(*s).map_err(|_| {
                PlanError::TypeMismatch("NUMERIC scale does not fit int32".to_owned())
            })?;
            Some(scale)
        }
        _ => unreachable!("modifier length checked above"),
    };
    Ok(DataType::Decimal { precision, scale })
}

fn resolve_vector_family_type(
    sql_name: &str,
    t: &TypeName,
    build: fn(Option<u32>) -> DataType,
) -> Result<DataType, PlanError> {
    if t.type_modifiers.len() > 1 {
        return Err(PlanError::TypeMismatch(format!(
            "{sql_name} accepts at most one dimension modifier"
        )));
    }
    let dims = t.type_modifiers.first().copied();
    if matches!(dims, Some(0)) {
        return Err(PlanError::TypeMismatch(format!(
            "{sql_name} dimension must be at least 1"
        )));
    }
    if matches!(dims, Some(n) if n > MAX_VECTOR_DIMS) {
        return Err(PlanError::TypeMismatch(format!(
            "{sql_name} dimension must be at most {MAX_VECTOR_DIMS}"
        )));
    }
    Ok(build(dims))
}

pub(super) fn resolve_column_type(
    table_name: &str,
    column_name: &str,
    t: &TypeName,
    catalog: &dyn Catalog,
) -> Result<(DataType, Option<String>), PlanError> {
    if !t.type_modifiers.is_empty() {
        match t.name.value.as_str() {
            "serial" | "serial4" | "bigserial" | "serial8" | "smallserial" | "serial2" => {
                return Err(PlanError::NotSupported(
                    "CREATE TABLE: SERIAL type modifiers",
                ));
            }
            _ => {}
        }
    }
    let dtype = match t.name.value.as_str() {
        "serial" | "serial4" => DataType::Int32,
        "bigserial" | "serial8" => DataType::Int64,
        "smallserial" | "serial2" => DataType::Int16,
        _ => return resolve_type_name_with_catalog(t, catalog).map(|dtype| (dtype, None)),
    };
    Ok((
        dtype,
        Some(format!(
            "{}_{}_seq",
            table_name.to_ascii_lowercase(),
            column_name.to_ascii_lowercase()
        )),
    ))
}
