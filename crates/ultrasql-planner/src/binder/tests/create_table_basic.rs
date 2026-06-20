//! Binder tests for CREATE TABLE column typing: scalar types, enums,
//! composites, domains, numeric/char typmods and the money type.

use ultrasql_core::{DataType, Schema, Value};

use super::*;
use crate::catalog::InMemoryCatalog;

// CREATE TABLE
// -----------------------------------------------------------------------

#[test]
fn binds_create_table_resolves_basic_column_types() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE accounts (id BIGINT NOT NULL, name TEXT, balance FLOAT8)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable {
        table_name,
        namespace,
        columns,
        if_not_exists,
        schema,
        ..
    } = plan
    else {
        panic!("expected CreateTable, got other plan");
    };
    assert_eq!(table_name, "accounts");
    assert_eq!(namespace, "public");
    assert!(!if_not_exists);
    assert_eq!(schema, Schema::empty());
    assert_eq!(columns.len(), 3);
    assert_eq!(columns.fields()[0].name, "id");
    assert_eq!(columns.fields()[0].data_type, DataType::Int64);
    assert!(!columns.fields()[0].nullable, "NOT NULL honored");
    assert_eq!(
        columns.fields()[1].data_type,
        DataType::Text { max_len: None }
    );
    assert!(columns.fields()[1].nullable, "no constraint = nullable");
    assert_eq!(columns.fields()[2].data_type, DataType::Float64);
}

#[test]
fn binds_create_type_enum_and_uses_catalog_type_in_create_table() {
    let cat = InMemoryCatalog::new();
    let plan =
        parse_and_bind("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')", &cat).expect("bind ok");
    let LogicalPlan::CreateTypeEnum {
        type_name,
        namespace,
        labels,
        ..
    } = plan
    else {
        panic!("expected CreateTypeEnum");
    };
    assert_eq!(type_name, "mood");
    assert_eq!(namespace, "public");
    assert_eq!(labels, ["sad", "ok", "happy"]);

    let mut cat = InMemoryCatalog::new();
    let enum_type = DataType::Enum {
        oid: ultrasql_core::Oid::new(42_000),
        name: "mood".into(),
        labels: vec!["sad".to_owned(), "ok".to_owned(), "happy".to_owned()].into(),
    };
    cat.register_type("mood", enum_type.clone());
    let plan =
        parse_and_bind("CREATE TABLE enum_probe (id INT, mood mood)", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(columns.fields()[1].data_type, enum_type);
}

#[test]
fn binds_create_type_composite_and_uses_catalog_type_in_create_table() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TYPE postal_address AS (street TEXT, zip INT)", &cat)
        .expect("bind ok");
    let LogicalPlan::CreateTypeComposite {
        type_name,
        namespace,
        attributes,
        ..
    } = plan
    else {
        panic!("expected CreateTypeComposite");
    };
    assert_eq!(type_name, "postal_address");
    assert_eq!(namespace, "public");
    assert_eq!(attributes.fields()[0].name, "street");
    assert_eq!(
        attributes.fields()[0].data_type,
        DataType::Text { max_len: None }
    );
    assert_eq!(attributes.fields()[1].name, "zip");
    assert_eq!(attributes.fields()[1].data_type, DataType::Int32);

    let mut cat = InMemoryCatalog::new();
    let composite_type = DataType::Composite {
        oid: ultrasql_core::Oid::new(42_001),
        name: "postal_address".into(),
        fields: vec![
            ("street".to_owned(), DataType::Text { max_len: None }),
            ("zip".to_owned(), DataType::Int32),
        ]
        .into(),
    };
    cat.register_type("postal_address", composite_type.clone());
    let plan = parse_and_bind(
        "CREATE TABLE contact_book (id INT, addr postal_address)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(columns.fields()[1].data_type, composite_type);
}

#[test]
fn binds_create_domain_and_uses_catalog_type_in_create_table() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE DOMAIN positive_int AS INT NOT NULL CHECK (VALUE > 0)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateDomain {
        domain_name,
        namespace,
        base_type,
        not_null,
        checks,
        ..
    } = plan
    else {
        panic!("expected CreateDomain");
    };
    assert_eq!(domain_name, "positive_int");
    assert_eq!(namespace, "public");
    assert_eq!(base_type, DataType::Int32);
    assert!(not_null);
    assert_eq!(checks.len(), 1);

    let mut cat = InMemoryCatalog::new();
    let domain_type = DataType::Domain {
        oid: ultrasql_core::Oid::new(42_002),
        name: "positive_int".into(),
        base_type: Box::new(DataType::Int32),
        not_null: true,
    };
    cat.register_type("positive_int", domain_type.clone());
    let plan = parse_and_bind(
        "CREATE TABLE domain_probe (id INT, score positive_int)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(columns.fields()[1].data_type, domain_type);
    assert!(!columns.fields()[1].nullable);
}

#[test]
fn binds_create_table_range_partition_column() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE metrics (ts TIMESTAMP NOT NULL, v INT) PARTITION BY RANGE (ts)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable { partition, .. } = plan else {
        panic!("expected CreateTable");
    };
    let partition = partition.expect("time partition spec");
    assert_eq!(partition.column, "ts");
    assert_eq!(partition.column_index, 0);
}

#[test]
fn binds_create_table_with_varchar_modifier() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE t (s VARCHAR(255))", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };
    assert_eq!(
        columns.fields()[0].data_type,
        DataType::Text { max_len: Some(255) }
    );
}

#[test]
fn binds_create_table_numeric_typmods_like_postgres() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE t (a NUMERIC, b NUMERIC(10), c DECIMAL(12,4))",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };

    assert_eq!(
        columns.fields()[0].data_type,
        DataType::Decimal {
            precision: None,
            scale: None
        }
    );
    assert_eq!(
        columns.fields()[1].data_type,
        DataType::Decimal {
            precision: Some(10),
            scale: Some(0)
        }
    );
    assert_eq!(
        columns.fields()[2].data_type,
        DataType::Decimal {
            precision: Some(12),
            scale: Some(4)
        }
    );
}

#[test]
fn rejects_zero_numeric_precision() {
    let cat = InMemoryCatalog::new();
    let err = parse_and_bind("CREATE TABLE t (n NUMERIC(0,2))", &cat).expect_err("must reject");
    let PlanError::TypeMismatch(message) = err else {
        panic!("expected TypeMismatch, got {err:?}");
    };
    assert!(message.contains("NUMERIC precision"));
}

#[test]
fn binds_text_literal_cast_to_numeric_with_literal_scale() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("SELECT '12.340'::numeric AS amount", &cat).expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        panic!("expected Project");
    };
    assert_eq!(
        schema.field_at(0).data_type,
        DataType::Decimal {
            precision: None,
            scale: Some(3)
        }
    );
    let ScalarExpr::Literal { value, data_type } = &exprs[0].0 else {
        panic!("expected folded decimal literal");
    };
    assert_eq!(
        value,
        &Value::Decimal {
            value: 12_340,
            scale: 3
        }
    );
    assert_eq!(
        data_type,
        &DataType::Decimal {
            precision: None,
            scale: Some(3)
        }
    );
}

#[test]
fn binds_create_table_money_type() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("CREATE TABLE ledger (amount MONEY)", &cat).expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };

    assert_eq!(columns.fields()[0].data_type, DataType::Money);
}

#[test]
fn binds_text_literal_cast_to_money() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("SELECT '$1,234.56'::money AS amount", &cat).expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        panic!("expected Project");
    };
    assert_eq!(schema.field_at(0).data_type, DataType::Money);
    let ScalarExpr::Literal { value, data_type } = &exprs[0].0 else {
        panic!("expected folded money literal");
    };
    assert_eq!(value, &Value::Money(123_456));
    assert_eq!(data_type, &DataType::Money);
}

#[test]
fn binds_money_addition_and_subtraction() {
    let cat = InMemoryCatalog::new();
    for sql in [
        "SELECT '$1.00'::money + '$2.50'::money",
        "SELECT '$3.00'::money - '$1.25'::money",
    ] {
        let plan = parse_and_bind(sql, &cat).expect("bind ok");
        assert_eq!(plan.schema().field_at(0).data_type, DataType::Money);
    }
}

#[test]
fn binds_money_division_matrix() {
    let cat = InMemoryCatalog::new();
    let ratio = parse_and_bind("SELECT '$5.00'::money / '$2.00'::money", &cat).expect("bind ok");
    assert_eq!(ratio.schema().field_at(0).data_type, DataType::Float64);

    let divided = parse_and_bind("SELECT '$5.01'::money / 2", &cat).expect("bind ok");
    assert_eq!(divided.schema().field_at(0).data_type, DataType::Money);

    let rounded = parse_and_bind("SELECT '$5.01'::money / 2.0::float8", &cat).expect("bind ok");
    assert_eq!(rounded.schema().field_at(0).data_type, DataType::Money);
}

#[test]
fn binds_money_scalar_multiplication() {
    let cat = InMemoryCatalog::new();
    for sql in [
        "SELECT '$1.25'::money * 3",
        "SELECT 3 * '$1.25'::money",
        "SELECT '$1.25'::money * 1.5::float8",
        "SELECT 1.5::float8 * '$1.25'::money",
    ] {
        let plan = parse_and_bind(sql, &cat).expect("bind ok");
        assert_eq!(plan.schema().field_at(0).data_type, DataType::Money);
    }
}

#[test]
fn binds_money_runtime_casts() {
    let cat = money_catalog();
    let plan = parse_and_bind(
        "SELECT amount::numeric, amount::text, qty::money, price::money FROM ledger",
        &cat,
    )
    .expect("bind ok");
    let schema = plan.schema();
    assert_eq!(
        schema.field_at(0).data_type,
        DataType::Decimal {
            precision: None,
            scale: Some(2)
        }
    );
    assert_eq!(
        schema.field_at(1).data_type,
        DataType::Text { max_len: None }
    );
    assert_eq!(schema.field_at(2).data_type, DataType::Money);
    assert_eq!(schema.field_at(3).data_type, DataType::Money);
}

#[test]
fn binds_money_unary_signs() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("SELECT -('$1.25'::money), +'$2.00'::money", &cat).expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        panic!("expected Project");
    };
    assert_eq!(schema.field_at(0).data_type, DataType::Money);
    assert_eq!(schema.field_at(1).data_type, DataType::Money);
    let ScalarExpr::Literal { value, data_type } = &exprs[0].0 else {
        panic!("expected folded negative money literal");
    };
    assert_eq!(value, &Value::Money(-125));
    assert_eq!(data_type, &DataType::Money);
}

#[test]
fn binds_create_table_char_and_bpchar_types() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind(
        "CREATE TABLE codes (c CHAR(4), b BPCHAR(3), d CHARACTER)",
        &cat,
    )
    .expect("bind ok");
    let LogicalPlan::CreateTable { columns, .. } = plan else {
        panic!("expected CreateTable");
    };

    assert_eq!(
        columns.fields()[0].data_type,
        DataType::Char { len: Some(4) }
    );
    assert_eq!(
        columns.fields()[1].data_type,
        DataType::Char { len: Some(3) }
    );
    assert_eq!(
        columns.fields()[2].data_type,
        DataType::Char { len: Some(1) }
    );
}

#[test]
fn binds_text_literal_cast_to_char_with_padding() {
    let cat = InMemoryCatalog::new();
    let plan = parse_and_bind("SELECT CAST('abcdef' AS CHAR(3)) AS code", &cat).expect("bind ok");
    let LogicalPlan::Project { exprs, schema, .. } = plan else {
        panic!("expected Project");
    };
    assert_eq!(
        schema.field_at(0).data_type,
        DataType::Char { len: Some(3) }
    );
    let ScalarExpr::Literal { value, data_type } = &exprs[0].0 else {
        panic!("expected folded char literal");
    };
    assert_eq!(value, &Value::Char("abc".to_owned()));
    assert_eq!(data_type, &DataType::Char { len: Some(3) });
}
