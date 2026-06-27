//! Binder unit tests.
//!
//! Shared catalog fixtures and bind helpers live here; the individual test
//! cases are organised into topic submodules.

use ultrasql_core::{DataType, Field, Schema};
use ultrasql_parser::Parser;

use super::*;
use crate::catalog::{InMemoryCatalog, TableMeta};

mod aggregates;
mod between;
mod create_table_basic;
mod create_table_constraints;
mod ddl_index_alter;
mod derived_lateral;
mod distinct_on;
mod joins;
mod merge_dml;
mod misc_statements;
mod setops_subquery;
mod vectors_arrays;
mod window_positional;

/// Catalog with a single `users` table: id INT, name TEXT, score FLOAT8.
fn users_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
        Field::nullable("score", DataType::Float64),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("users", TableMeta::new(schema));
    cat
}

fn app_users_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("users", TableMeta::with_schema_name("app", schema));
    cat
}

fn users_index_catalog() -> InMemoryCatalog {
    let mut cat = users_catalog();
    cat.register_index("users_id_idx");
    cat
}

fn embeddings_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("embedding", DataType::Vector { dims: Some(3) }),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("embeddings", TableMeta::new(schema));
    cat
}

fn money_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("amount", DataType::Money),
        Field::required("qty", DataType::Int32),
        Field::required(
            "price",
            DataType::Decimal {
                precision: Some(10),
                scale: Some(3),
            },
        ),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("ledger", TableMeta::new(schema));
    cat
}

fn fact_events_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("tenant_id", DataType::Int32),
        Field::required("bucket", DataType::Int32),
        Field::required("amount", DataType::Int64),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("fact_events", TableMeta::new(schema));
    cat
}

fn sales_pivot_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("region", DataType::Text { max_len: None }),
        Field::required("quarter", DataType::Text { max_len: None }),
        Field::nullable("amount", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("sales", TableMeta::new(schema));
    cat
}

fn quarterly_unpivot_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("q1", DataType::Int32),
        Field::nullable("q2", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("quarterly", TableMeta::new(schema));
    cat
}

fn timestamp_catalog() -> InMemoryCatalog {
    let schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("locked_at", DataType::Timestamp),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("locks", TableMeta::new(schema));
    cat
}

fn parse_and_bind(sql: &str, cat: &dyn Catalog) -> Result<LogicalPlan, PlanError> {
    let stmt = Parser::new(sql)
        .parse_statement()
        .expect("test SQL parses cleanly");
    bind(&stmt, cat)
}

fn parse_bind_ok(sql: &str) -> LogicalPlan {
    let cat = users_catalog();
    parse_and_bind(sql, &cat).expect("bind ok")
}

fn two_table_catalog() -> InMemoryCatalog {
    let users_schema = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
    ])
    .expect("schema ok");
    let orders_schema = Schema::new([
        Field::required("oid", DataType::Int32),
        Field::required("user_id", DataType::Int32),
    ])
    .expect("schema ok");
    let mut cat = InMemoryCatalog::new();
    cat.register("users", TableMeta::new(users_schema));
    cat.register("orders", TableMeta::new(orders_schema));
    cat
}
