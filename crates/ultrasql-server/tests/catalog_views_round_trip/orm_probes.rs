//! ORM catalog-introspection probes (SQLAlchemy, ActiveRecord) and the
//! `pg_range` builtin range-type metadata they depend on.

use super::*;

#[tokio::test]
async fn sqlalchemy_has_table_catalog_probe_uses_any_array_and_visibility() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE sqlalchemy_cert (id INT, name TEXT)")
        .await
        .expect("create SQLAlchemy probe table");

    let rows = client
        .query(
            "SELECT pg_catalog.pg_class.relname
             FROM pg_catalog.pg_class
             JOIN pg_catalog.pg_namespace
               ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace
             WHERE pg_catalog.pg_class.relname = $1::VARCHAR
               AND pg_catalog.pg_class.relkind = ANY (ARRAY[$2::VARCHAR, $3::VARCHAR, $4::VARCHAR, $5::VARCHAR, $6::VARCHAR])
               AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid)
               AND pg_catalog.pg_namespace.nspname != $7::VARCHAR",
            &[&"sqlalchemy_cert", &"r", &"p", &"f", &"v", &"m", &"pg_catalog"],
        )
        .await
        .expect("SQLAlchemy has_table catalog probe");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "sqlalchemy_cert");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn active_record_data_source_probe_uses_current_schemas_any() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE rails_cert (id INT, label TEXT)")
        .await
        .expect("create Rails data source probe table");

    let rows = client
        .query(
            "SELECT c.relname \
             FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = ANY (current_schemas(false)) \
               AND c.relname = 'rails_cert' \
               AND c.relkind IN ('r','v','m','p','f')",
            &[],
        )
        .await
        .expect("ActiveRecord data_source_sql catalog probe");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "rails_cert");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn active_record_type_map_probe_left_joins_pg_range() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query(
            "SELECT \
                t.oid, \
                t.typname, \
                t.typelem, \
                t.typdelim, \
                t.typinput, \
                r.rngsubtype, \
                t.typtype, \
                t.typbasetype \
             FROM pg_type AS t \
             LEFT JOIN pg_range AS r ON oid = rngtypid \
             WHERE t.typname IN ('int4', 'text') \
             ORDER BY t.oid",
            &[],
        )
        .await
        .expect("ActiveRecord pg_type/pg_range type-map probe");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, u32>(0), 23);
    assert_eq!(rows[0].get::<_, String>(1), "int4");
    assert_eq!(rows[0].get::<_, i32>(2), 0);
    assert_eq!(rows[0].get::<_, String>(3), ",");
    assert_eq!(rows[0].get::<_, String>(4), "int4in");
    assert_eq!(rows[0].get::<_, Option<u32>>(5), None);
    assert_eq!(rows[0].get::<_, String>(6), "b");
    assert_eq!(rows[0].get::<_, u32>(7), 0);
    assert_eq!(rows[1].get::<_, u32>(0), 25);
    assert_eq!(rows[1].get::<_, String>(1), "text");
    assert_eq!(rows[1].get::<_, String>(4), "textin");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn pg_range_lists_builtin_range_type_metadata() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query(
            "SELECT t.typname, t.oid, r.rngsubtype \
             FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid \
             WHERE t.typname IN ('int4range', 'int8range', 'numrange', 'daterange', 'tsrange', 'tstzrange') \
             ORDER BY t.typname",
            &[],
        )
        .await
        .expect("pg_range builtin rows");

    let actual = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, u32>(1),
                row.get::<_, u32>(2),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            ("daterange".to_owned(), 3912, 1082),
            ("int4range".to_owned(), 3904, 23),
            ("int8range".to_owned(), 3926, 20),
            ("numrange".to_owned(), 3906, 1700),
            ("tsrange".to_owned(), 3908, 1114),
            ("tstzrange".to_owned(), 3910, 1184),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn active_record_column_definitions_probe_uses_catalog_helpers() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    let collations = client
        .query(
            "SELECT oid, collname FROM pg_catalog.pg_collation ORDER BY oid",
            &[],
        )
        .await
        .expect("pg_collation base rows");
    assert_eq!(collations.len(), 3);
    assert_eq!(collations[0].get::<_, u32>(0), 100);
    assert_eq!(collations[0].get::<_, String>(1), "default");
    assert_eq!(collations[1].get::<_, String>(1), "C");
    assert_eq!(collations[2].get::<_, String>(1), "POSIX");

    client
        .batch_execute("CREATE TABLE rails_cert (id INT NOT NULL, label TEXT NOT NULL)")
        .await
        .expect("create Rails probe table");

    let rows = client
        .query(
            "SELECT \
                a.attname, \
                format_type(a.atttypid, a.atttypmod), \
                pg_get_expr(d.adbin, d.adrelid), \
                a.attnotnull, \
                a.atttypid, \
                a.atttypmod, \
                c.collname, \
                col_description(a.attrelid, a.attnum) AS comment, \
                a.attidentity AS identity, \
                a.attgenerated AS attgenerated \
             FROM pg_attribute a \
             LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum \
             LEFT JOIN pg_type t ON a.atttypid = t.oid \
             LEFT JOIN pg_collation c ON a.attcollation = c.oid AND a.attcollation <> t.typcollation \
             WHERE a.attrelid = '\"rails_cert\"'::regclass \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[],
        )
        .await
        .expect("ActiveRecord column_definitions catalog probe");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "id");
    assert_eq!(rows[0].get::<_, String>(1), "integer");
    assert_eq!(rows[0].get::<_, Option<String>>(2), None);
    assert!(rows[0].get::<_, bool>(3));
    assert_eq!(rows[0].get::<_, u32>(4), 23);
    assert_eq!(rows[0].get::<_, i32>(5), -1);
    assert_eq!(rows[0].get::<_, Option<String>>(6), None);
    assert_eq!(rows[0].get::<_, Option<String>>(7), None);
    assert_eq!(rows[0].get::<_, String>(8), "");
    assert_eq!(rows[0].get::<_, String>(9), "");
    assert_eq!(rows[1].get::<_, String>(0), "label");
    assert_eq!(rows[1].get::<_, String>(1), "text");
    assert_eq!(rows[1].get::<_, u32>(4), 25);
    assert_eq!(rows[1].get::<_, String>(8), "");
    assert_eq!(rows[1].get::<_, String>(9), "");

    let collation_rows = client
        .query(
            "SELECT a.attname, a.attcollation, t.typcollation, c.collname \
             FROM pg_attribute a \
             JOIN pg_type t ON a.atttypid = t.oid \
             LEFT JOIN pg_collation c ON a.attcollation = c.oid \
             WHERE a.attrelid = '\"rails_cert\"'::regclass \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[],
        )
        .await
        .expect("column collation metadata");
    assert_eq!(collation_rows.len(), 2);
    assert_eq!(collation_rows[0].get::<_, String>(0), "id");
    assert_eq!(collation_rows[0].get::<_, u32>(1), 0);
    assert_eq!(collation_rows[0].get::<_, u32>(2), 0);
    assert_eq!(collation_rows[0].get::<_, Option<String>>(3), None);
    assert_eq!(collation_rows[1].get::<_, String>(0), "label");
    assert_eq!(collation_rows[1].get::<_, u32>(1), 100);
    assert_eq!(collation_rows[1].get::<_, u32>(2), 100);
    assert_eq!(
        collation_rows[1].get::<_, Option<String>>(3).as_deref(),
        Some("default")
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn active_record_primary_keys_probe_uses_pg_index_indkey() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE rails_pk_cert (id INT PRIMARY KEY, label TEXT)")
        .await
        .expect("create Rails primary key probe table");

    let rows = client
        .query(
            "SELECT a.attname \
             FROM pg_index i \
             JOIN pg_attribute a \
               ON a.attrelid = i.indrelid \
              AND a.attnum = ANY(i.indkey) \
             WHERE i.indrelid = '\"rails_pk_cert\"'::regclass \
               AND i.indisprimary \
             ORDER BY array_position(i.indkey, a.attnum)",
            &[],
        )
        .await
        .expect("ActiveRecord primary_keys catalog probe");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "id");

    shutdown(client, server_handle).await;
}
