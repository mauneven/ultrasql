//! `CREATE INDEX` (including aggregating indexes), `DROP INDEX`, and
//! `DROP TABLE`.

use ultrasql_core::{DataType, Schema};
use ultrasql_parser::ast::{CreateIndexStmt, DropIndexStmt, DropTableStmt, Expr};

use super::super::{
    Catalog, LogicalPlan, PlanError, ScalarExpr, ScopeStack, bind_expr, lookup_table_reference,
    object_name_simple,
};
use super::shared::{index_option_value_to_string, object_name_explicit_namespace};
use crate::catalog::TableMeta;
use crate::plan::{
    AggregateFunc, LogicalAggregatingIndex, LogicalAggregatingIndexExpr, LogicalIndexMethod,
    LogicalIndexOption,
};

// ---------------------------------------------------------------------------
// CREATE INDEX
// ---------------------------------------------------------------------------

/// Bind a `CREATE [UNIQUE] INDEX [IF NOT EXISTS] [name] ON table (cols)`.
///
/// Accepted shapes for this wave:
///
/// - bare column-reference keys (`(col1, col2, ...)`) and single
///   expression keys (`(lower(col))`) for B-tree storage.
/// - `USING hash`, `USING gin`, `USING gist`, `USING brin`, and `USING hnsw`
///   are preserved in the logical plan so catalog/runtime metadata can route
///   maintenance to the requested access method.
/// - `INCLUDE` covering columns and `WHERE` partial-index predicates
///   are bound into runtime metadata; they do not change the key
///   encoding.
/// - per-key direction / nulls ordering is parsed but not actionable
///   until [`crate::plan::LogicalPlan`] carries order metadata through.
///
/// The binder synthesises a default index name `"{table}_{c1}_{c2}_..._idx"`
/// when one was not supplied so the executor always has a stable
/// catalog key to write.
pub(in crate::binder) fn bind_create_index(
    s: &CreateIndexStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    // Resolve the target table.
    let resolved = lookup_table_reference(catalog, &s.table)?;
    let table_bare_name = object_name_simple(&s.table);
    let table_name = resolved.plan_name;
    let meta = resolved.meta;
    let index_namespace = meta.schema_name.clone();
    let table_schema = &meta.schema;

    let method = if s.aggregating {
        if s.method.is_some() {
            return Err(PlanError::NotSupported(
                "CREATE AGGREGATING INDEX may not also specify USING",
            ));
        }
        LogicalIndexMethod::Aggregating
    } else {
        match s.method.as_ref().map(|m| m.value.to_ascii_lowercase()) {
            None => LogicalIndexMethod::Btree,
            Some(method) if method == "btree" => LogicalIndexMethod::Btree,
            Some(method) if method == "hash" => LogicalIndexMethod::Hash,
            Some(method) if method == "gin" => LogicalIndexMethod::Gin,
            Some(method) if method == "gist" => LogicalIndexMethod::Gist,
            Some(method) if method == "brin" => LogicalIndexMethod::Brin,
            Some(method) if method == "hnsw" => LogicalIndexMethod::Hnsw,
            Some(method) if method == "ivfflat" => LogicalIndexMethod::IvfFlat,
            Some(_) => {
                return Err(PlanError::NotSupported(
                    "CREATE INDEX: only btree, hash, gin, gist, brin, hnsw, and ivfflat methods are supported",
                ));
            }
        }
    };

    if s.columns.is_empty() {
        return Err(PlanError::NotSupported("CREATE INDEX: zero key columns"));
    }
    if method == LogicalIndexMethod::Aggregating {
        return bind_create_aggregating_index(
            s,
            table_name,
            table_bare_name,
            index_namespace,
            table_schema,
        );
    }
    if method == LogicalIndexMethod::Hash && s.columns.len() != 1 {
        return Err(PlanError::NotSupported(
            "CREATE INDEX USING hash: exactly one key is supported in this wave",
        ));
    }
    if method == LogicalIndexMethod::Hash && s.unique {
        return Err(PlanError::NotSupported(
            "CREATE UNIQUE INDEX USING hash: hash indexes do not enforce uniqueness",
        ));
    }
    if matches!(
        method,
        LogicalIndexMethod::Gin | LogicalIndexMethod::Gist | LogicalIndexMethod::Brin
    ) && s.unique
    {
        return Err(PlanError::NotSupported(
            "CREATE UNIQUE INDEX: gin, gist, and brin indexes do not enforce uniqueness",
        ));
    }
    if matches!(
        method,
        LogicalIndexMethod::Hnsw | LogicalIndexMethod::IvfFlat
    ) && s.unique
    {
        return Err(PlanError::NotSupported(
            "CREATE UNIQUE INDEX USING vector ANN: hnsw and ivfflat indexes do not enforce uniqueness",
        ));
    }
    let mut col_indices: Vec<usize> = Vec::with_capacity(s.columns.len());
    let mut col_names: Vec<String> = Vec::with_capacity(s.columns.len());
    let mut key_exprs: Vec<ScalarExpr> = Vec::with_capacity(s.columns.len());
    let mut opclasses: Vec<Option<String>> = Vec::with_capacity(s.columns.len());
    let mut saw_expression_key = false;
    for key in &s.columns {
        let mut scope = ScopeStack::new();
        let bound = bind_expr(&key.expr, table_schema, catalog, &mut scope)?;
        opclasses.push(
            key.opclass
                .as_ref()
                .map(|ident| ident.value.to_ascii_lowercase()),
        );
        match &bound {
            ScalarExpr::Column { name, index, .. } => {
                col_indices.push(*index);
                col_names.push(name.to_ascii_lowercase());
            }
            _ => {
                saw_expression_key = true;
                col_names.push(index_expr_name_part(&bound));
            }
        }
        key_exprs.push(bound);
    }
    if saw_expression_key {
        if s.columns.len() != 1 {
            return Err(PlanError::NotSupported(
                "CREATE INDEX: expression indexes support exactly one key in this wave",
            ));
        }
        col_indices.clear();
    }

    if matches!(
        method,
        LogicalIndexMethod::Hnsw | LogicalIndexMethod::IvfFlat
    ) {
        if s.columns.len() != 1 || col_indices.len() != 1 {
            return Err(PlanError::NotSupported(
                "CREATE INDEX USING vector ANN: exactly one vector column key is supported",
            ));
        }
        let field = table_schema
            .field(col_indices[0])
            .ok_or_else(|| PlanError::ColumnNotFound(format!("column index {}", col_indices[0])))?;
        if !matches!(
            field.data_type,
            DataType::Vector { dims: Some(_) } | DataType::HalfVec { dims: Some(_) }
        ) {
            return Err(PlanError::TypeMismatch(format!(
                "CREATE INDEX USING vector ANN requires a vector(n) or halfvec(n) column, got {}",
                field.data_type
            )));
        }
        if let Some(opclass) = opclasses.first().and_then(Option::as_ref)
            && !matches!(
                opclass.as_str(),
                "vector_l2_ops" | "vector_cosine_ops" | "vector_ip_ops" | "vector_l1_ops"
            )
        {
            return Err(PlanError::NotSupported(
                "CREATE INDEX USING vector ANN: supported vector opclasses are vector_l2_ops, vector_cosine_ops, vector_ip_ops, vector_l1_ops",
            ));
        }
        if !s.include.is_empty() {
            return Err(PlanError::NotSupported(
                "CREATE INDEX USING vector ANN: INCLUDE columns are not supported in this wave",
            ));
        }
        if s.r#where.is_some() {
            return Err(PlanError::NotSupported(
                "CREATE INDEX USING vector ANN: partial indexes are not supported in this wave",
            ));
        }
    }

    let index_options = s
        .options
        .iter()
        .map(|option| {
            let name = option.name.value.to_ascii_lowercase();
            let value = index_option_value_to_string(&option.value)?;
            Ok(LogicalIndexOption { name, value })
        })
        .collect::<Result<Vec<_>, PlanError>>()?;
    if !matches!(
        method,
        LogicalIndexMethod::Hnsw | LogicalIndexMethod::IvfFlat
    ) && !index_options.is_empty()
    {
        return Err(PlanError::NotSupported(
            "CREATE INDEX WITH options are supported only for hnsw and ivfflat in this wave",
        ));
    }
    if method == LogicalIndexMethod::Hnsw {
        for option in &index_options {
            if option.name != "payload" {
                return Err(PlanError::NotSupported(
                    "CREATE INDEX USING hnsw supports only the payload option",
                ));
            }
            validate_ann_payload_option(&option.value)?;
        }
    }
    if method == LogicalIndexMethod::IvfFlat {
        for option in &index_options {
            if !matches!(option.name.as_str(), "lists" | "probes" | "payload") {
                return Err(PlanError::NotSupported(
                    "CREATE INDEX USING ivfflat supports only lists, probes, and payload options",
                ));
            }
            if option.name == "payload" {
                validate_ann_payload_option(&option.value)?;
            }
        }
    }

    let mut include_columns = Vec::with_capacity(s.include.len());
    for ident in &s.include {
        let folded = ident.value.to_ascii_lowercase();
        let (idx, _) = table_schema
            .find(&folded)
            .ok_or_else(|| PlanError::ColumnNotFound(ident.value.clone()))?;
        include_columns.push(idx);
    }

    let predicate = if let Some(pred_ast) = &s.r#where {
        let mut scope = ScopeStack::new();
        let pred = bind_expr(pred_ast, table_schema, catalog, &mut scope)?;
        let pred_ty = pred.data_type();
        if pred_ty != DataType::Bool {
            return Err(PlanError::TypeMismatch(format!(
                "CREATE INDEX WHERE predicate must be boolean, got {pred_ty}"
            )));
        }
        Some(pred)
    } else {
        None
    };

    let index_name = s.name.as_ref().map_or_else(
        || synthesise_index_name(&table_bare_name, &col_names),
        |ident| ident.value.to_ascii_lowercase(),
    );

    Ok(LogicalPlan::CreateIndex {
        index_name,
        index_namespace,
        table_name,
        columns: col_indices,
        key_exprs,
        opclasses,
        index_options,
        include_columns,
        predicate,
        method,
        aggregating: None,
        unique: s.unique,
        primary_key: false,
        concurrently: s.concurrently,
        if_not_exists: s.if_not_exists,
        schema: Schema::empty(),
    })
}

fn bind_create_aggregating_index(
    s: &CreateIndexStmt,
    table_name: String,
    table_bare_name: String,
    index_namespace: String,
    table_schema: &Schema,
) -> Result<LogicalPlan, PlanError> {
    if s.unique {
        return Err(PlanError::NotSupported(
            "CREATE UNIQUE AGGREGATING INDEX is not supported",
        ));
    }
    if s.concurrently {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX CONCURRENTLY is not supported",
        ));
    }
    if !s.include.is_empty() {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX does not support INCLUDE",
        ));
    }
    if s.r#where.is_some() {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX does not support partial predicates in this wave",
        ));
    }
    if !s.options.is_empty() {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX does not support WITH options in this wave",
        ));
    }

    let mut group_columns = Vec::new();
    let mut group_exprs = Vec::new();
    let mut col_names = Vec::new();
    let mut aggregates = Vec::new();
    let mut saw_aggregate = false;

    for key in &s.columns {
        if key.opclass.is_some() {
            return Err(PlanError::NotSupported(
                "CREATE AGGREGATING INDEX does not support operator classes",
            ));
        }
        match &key.expr {
            Expr::Call { .. } => {
                saw_aggregate = true;
                let aggregate = bind_aggregating_index_call(&key.expr, table_schema)?;
                col_names.push(aggregate.output_name.clone());
                aggregates.push(aggregate);
            }
            _ => {
                if saw_aggregate {
                    return Err(PlanError::NotSupported(
                        "CREATE AGGREGATING INDEX group columns must precede aggregates",
                    ));
                }
                let mut scope = ScopeStack::new();
                let bound = bind_expr(&key.expr, table_schema, &NoopCatalog, &mut scope)?;
                let ScalarExpr::Column { name, index, .. } = bound else {
                    return Err(PlanError::NotSupported(
                        "CREATE AGGREGATING INDEX group keys must be bare columns",
                    ));
                };
                group_columns.push(index);
                group_exprs.push(ScalarExpr::Column {
                    name: name.clone(),
                    index,
                    data_type: table_schema.field_at(index).data_type.clone(),
                });
                col_names.push(name.to_ascii_lowercase());
            }
        }
    }

    if group_columns.is_empty() || aggregates.is_empty() {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX requires at least one group column and one aggregate",
        ));
    }

    let index_name = s.name.as_ref().map_or_else(
        || synthesise_index_name(&table_bare_name, &col_names),
        |ident| ident.value.to_ascii_lowercase(),
    );

    Ok(LogicalPlan::CreateIndex {
        index_name,
        index_namespace,
        table_name,
        columns: group_columns.clone(),
        key_exprs: group_exprs,
        opclasses: vec![None; group_columns.len()],
        index_options: Vec::new(),
        include_columns: Vec::new(),
        predicate: None,
        method: LogicalIndexMethod::Aggregating,
        aggregating: Some(LogicalAggregatingIndex {
            group_columns,
            aggregates,
        }),
        unique: false,
        primary_key: false,
        concurrently: false,
        if_not_exists: s.if_not_exists,
        schema: Schema::empty(),
    })
}

struct NoopCatalog;

impl Catalog for NoopCatalog {
    fn lookup_table(&self, _name: &str) -> Option<TableMeta> {
        None
    }
}

fn bind_aggregating_index_call(
    expr: &Expr,
    table_schema: &Schema,
) -> Result<LogicalAggregatingIndexExpr, PlanError> {
    let Expr::Call {
        name,
        args,
        distinct,
        over,
        ..
    } = expr
    else {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX aggregate key must be a function call",
        ));
    };
    if *distinct || over.is_some() {
        return Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX does not support DISTINCT or window aggregates",
        ));
    }
    let func_name = name
        .parts
        .last()
        .map_or("", |part| part.value.as_str())
        .to_ascii_lowercase();
    let is_star_arg = args.len() == 1
        && matches!(&args[0], Expr::Column { name }
            if name.parts.len() == 1 && name.parts[0].value == "*");
    match func_name.as_str() {
        "count" if args.is_empty() || is_star_arg => Ok(LogicalAggregatingIndexExpr {
            func: AggregateFunc::CountStar,
            arg_column: None,
            output_name: "count".to_owned(),
            data_type: DataType::Int64,
        }),
        "sum" if args.len() == 1 => {
            let mut scope = ScopeStack::new();
            let bound = bind_expr(&args[0], table_schema, &NoopCatalog, &mut scope)?;
            let ScalarExpr::Column {
                name,
                index,
                data_type,
            } = bound
            else {
                return Err(PlanError::NotSupported(
                    "CREATE AGGREGATING INDEX sum() argument must be a bare column",
                ));
            };
            if !data_type.is_numeric() {
                return Err(PlanError::TypeMismatch(format!(
                    "CREATE AGGREGATING INDEX sum({name}) requires numeric input, got {data_type}"
                )));
            }
            let out_ty = match data_type {
                DataType::Float32 | DataType::Float64 => DataType::Float64,
                _ => DataType::Int64,
            };
            Ok(LogicalAggregatingIndexExpr {
                func: AggregateFunc::Sum,
                arg_column: Some(index),
                output_name: format!("sum({})", name.to_ascii_lowercase()),
                data_type: out_ty,
            })
        }
        _ => Err(PlanError::NotSupported(
            "CREATE AGGREGATING INDEX supports sum(column) and count(*) in this wave",
        )),
    }
}

fn index_expr_name_part(expr: &ScalarExpr) -> String {
    expr.to_string()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_owned()
}

fn validate_ann_payload_option(value: &str) -> Result<(), PlanError> {
    match value.to_ascii_lowercase().as_str() {
        "f32" | "float32" | "bf16" | "bfloat16" | "int8" | "i8" => Ok(()),
        _ => Err(PlanError::NotSupported(
            "CREATE INDEX USING vector ANN payload supports f32, bf16, and int8",
        )),
    }
}

/// Build a stable default index name when the user did not supply one:
/// `{table}_{col1}_{col2}_..._idx`. Matches PostgreSQL's
/// `ChooseIndexName` for the common single-column / multi-column case
/// closely enough that EXPLAIN-style output stays familiar.
fn synthesise_index_name(table: &str, columns: &[String]) -> String {
    let mut s = String::with_capacity(table.len() + 16);
    s.push_str(table);
    for c in columns {
        s.push('_');
        s.push_str(c);
    }
    s.push_str("_idx");
    s
}

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

/// Bind a `DROP TABLE [IF EXISTS] name [, ...] [CASCADE|RESTRICT]`.
///
/// Each name is folded to lowercase and resolved against the catalog.
/// Without `IF EXISTS`, a missing relation is rejected with
/// [`PlanError::TableNotFound`]; with `IF EXISTS`, missing relations
/// are silently dropped from the resulting plan so the executor never
/// has to re-check the catalog.
pub(in crate::binder) fn bind_drop_table(
    s: &DropTableStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let mut tables: Vec<String> = Vec::with_capacity(s.names.len());
    for obj in &s.names {
        let name = object_name_simple(obj);
        if let Ok(resolved) = lookup_table_reference(catalog, obj) {
            tables.push(resolved.plan_name);
        } else if !s.if_exists {
            return Err(PlanError::TableNotFound(name));
        }
    }
    Ok(LogicalPlan::DropTable {
        tables,
        if_exists: s.if_exists,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}

/// Bind `DROP INDEX [IF EXISTS] name [, ...]`.
pub(in crate::binder) fn bind_drop_index(
    s: &DropIndexStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    let mut indexes = Vec::with_capacity(s.names.len());
    let mut index_namespaces = Vec::with_capacity(s.names.len());
    for obj in &s.names {
        let name = object_name_simple(obj);
        if let Some(namespace) = object_name_explicit_namespace(obj) {
            if !catalog.lookup_index_in_schema(&namespace, &name) {
                if !s.if_exists {
                    return Err(PlanError::IndexNotFound(format!("{namespace}.{name}")));
                }
                continue;
            }
            indexes.push(name);
            index_namespaces.push(Some(namespace));
        } else if let Some(namespace) = catalog.lookup_index_schema(&name) {
            indexes.push(name);
            index_namespaces.push((namespace != "public").then_some(namespace));
        } else {
            if !s.if_exists {
                return Err(PlanError::IndexNotFound(name));
            }
        }
    }
    Ok(LogicalPlan::DropIndex {
        indexes,
        index_namespaces,
        if_exists: s.if_exists,
        cascade: s.cascade,
        schema: Schema::empty(),
    })
}
