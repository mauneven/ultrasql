//! Planner fuzz target.
//!
//! Parses arbitrary UTF-8 as SQL, then tries to bind successfully parsed
//! statements against a small deterministic catalog. The contract is
//! "no panic": malformed SQL or invalid plans must return typed errors.

#![no_main]

use libfuzzer_sys::fuzz_target;
use ultrasql_core::{DataType, Field, Schema};
use ultrasql_parser::Parser;
use ultrasql_planner::{InMemoryCatalog, TableMeta, bind};

fuzz_target!(|data: &[u8]| {
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(statements) = Parser::new(sql).parse_statements() else {
        return;
    };
    let Some(catalog) = catalog() else {
        return;
    };
    for statement in statements {
        let _ = bind(&statement, &catalog);
    }
});

fn catalog() -> Option<InMemoryCatalog> {
    let users = Schema::new([
        Field::required("id", DataType::Int32),
        Field::nullable("name", DataType::Text { max_len: None }),
        Field::nullable("score", DataType::Float64),
        Field::nullable("active", DataType::Bool),
    ])
    .ok()?;
    let orders = Schema::new([
        Field::required("id", DataType::Int64),
        Field::required("user_id", DataType::Int32),
        Field::nullable(
            "amount",
            DataType::Decimal {
                precision: Some(18),
                scale: Some(2),
            },
        ),
        Field::nullable("created_at", DataType::Timestamp),
    ])
    .ok()?;
    let mut catalog = InMemoryCatalog::new();
    let _ = catalog.register("users", TableMeta::new(users));
    let _ = catalog.register("orders", TableMeta::new(orders));
    Some(catalog)
}
