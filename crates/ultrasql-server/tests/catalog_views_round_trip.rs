//! End-to-end metadata view tests.
//!
//! These tests drive the virtual `pg_catalog` / `information_schema`
//! relations through the normal SQL path used by CLI `\d`-style commands.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, bind_listener, serve_listener};

async fn start_server_and_connect() -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(Server::with_sample_database());
    let server_handle = tokio::spawn(serve_listener(listener, server));
    let conn_str = format!(
        "host={host} port={port} user=tester application_name=catalog_views_test",
        host = bound.ip(),
        port = bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("tokio-postgres connect");
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    (client, conn_handle, server_handle)
}

async fn shutdown(
    client: tokio_postgres::Client,
    server_handle: tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    drop(client);
    tokio::time::sleep(Duration::from_millis(20)).await;
    server_handle.abort();
}

#[tokio::test]
async fn pg_catalog_and_information_schema_reflect_runtime_objects() {
    let (client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE meta_t (id INT NOT NULL, name TEXT DEFAULT 'anon')")
        .await
        .expect("create table");
    client
        .batch_execute("CREATE INDEX meta_t_id_idx ON meta_t(id)")
        .await
        .expect("create index");
    client
        .batch_execute("CREATE SEQUENCE meta_s START WITH 7 INCREMENT BY 3")
        .await
        .expect("create sequence");
    client
        .batch_execute("CREATE TABLE meta_parent (id INT PRIMARY KEY, v INT CHECK (v > 0))")
        .await
        .expect("create constrained parent");
    client
        .batch_execute("CREATE TABLE meta_child (parent_id INT REFERENCES meta_parent(id))")
        .await
        .expect("create constrained child");
    client
        .batch_execute("COMMENT ON TABLE meta_t IS 'table comment'")
        .await
        .expect("comment on table");
    client
        .batch_execute("COMMENT ON INDEX meta_t_id_idx IS 'index comment'")
        .await
        .expect("comment on index");
    client
        .batch_execute("COMMENT ON COLUMN meta_t.name IS 'name comment'")
        .await
        .expect("comment on column");
    client
        .batch_execute("COMMENT ON COLUMN meta_t.id IS 'temporary comment'")
        .await
        .expect("comment on id column");
    client
        .batch_execute("COMMENT ON COLUMN meta_t.id IS NULL")
        .await
        .expect("clear column comment");

    let tables = client
        .query(
            "SELECT schemaname, tablename, hasindexes \
             FROM pg_catalog.pg_tables \
             WHERE tablename = 'meta_t'",
            &[],
        )
        .await
        .expect("pg_tables query");
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].get::<_, String>(0), "public");
    assert_eq!(tables[0].get::<_, String>(1), "meta_t");
    assert!(tables[0].get::<_, bool>(2));

    let columns = client
        .query(
            "SELECT column_name, data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_name = 'meta_t' \
             ORDER BY ordinal_position",
            &[],
        )
        .await
        .expect("information_schema.columns query");
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].get::<_, String>(0), "id");
    assert_eq!(columns[0].get::<_, String>(1), "integer");
    assert_eq!(columns[0].get::<_, String>(2), "NO");
    assert_eq!(columns[1].get::<_, String>(0), "name");
    assert_eq!(columns[1].get::<_, String>(1), "text");
    assert_eq!(columns[1].get::<_, String>(2), "YES");

    let attrdefs = client
        .query(
            "SELECT a.atthasdef, d.adbin \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_attrdef d \
               ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
             WHERE a.attname = 'name'",
            &[],
        )
        .await
        .expect("pg_attrdef query");
    assert_eq!(attrdefs.len(), 1);
    assert!(attrdefs[0].get::<_, bool>(0));
    assert!(attrdefs[0].get::<_, String>(1).contains("anon"));

    let indexes = client
        .query(
            "SELECT indexname \
             FROM pg_catalog.pg_indexes \
             WHERE tablename = 'meta_t'",
            &[],
        )
        .await
        .expect("pg_indexes query");
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].get::<_, String>(0), "meta_t_id_idx");

    let sequences = client
        .query(
            "SELECT sequencename, start_value, increment_by \
             FROM pg_catalog.pg_sequences \
             WHERE sequencename = 'meta_s'",
            &[],
        )
        .await
        .expect("pg_sequences query");
    assert_eq!(sequences.len(), 1);
    assert_eq!(sequences[0].get::<_, String>(0), "meta_s");
    assert_eq!(sequences[0].get::<_, i64>(1), 7);
    assert_eq!(sequences[0].get::<_, i64>(2), 3);

    let schemas = client
        .query(
            "SELECT nspname, pg_catalog.pg_get_userbyid(nspowner) \
             FROM pg_catalog.pg_namespace \
             WHERE nspname = 'public'",
            &[],
        )
        .await
        .expect("pg_get_userbyid meta query");
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].get::<_, String>(0), "public");
    assert_eq!(schemas[0].get::<_, String>(1), "ultrasql");

    let databases = client
        .query(
            "SELECT datname AS name, pg_catalog.pg_get_userbyid(datdba) AS owner \
             FROM pg_catalog.pg_database \
             ORDER BY 1",
            &[],
        )
        .await
        .expect("pg_database meta query");
    assert_eq!(databases.len(), 1);
    assert_eq!(databases[0].get::<_, String>(0), "ultrasql");
    assert_eq!(databases[0].get::<_, String>(1), "ultrasql");

    let functions = client
        .query(
            "SELECT n.nspname AS schemaname, p.proname AS name \
             FROM pg_catalog.pg_proc p \
             JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid \
             WHERE n.nspname NOT IN ('pg_catalog','information_schema') \
             ORDER BY 1, 2",
            &[],
        )
        .await
        .expect("pg_proc/pg_namespace meta join");
    assert!(functions.is_empty());

    let descriptions = client
        .query(
            "SELECT objsubid, description \
             FROM pg_catalog.pg_description \
             ORDER BY description",
            &[],
        )
        .await
        .expect("pg_description query");
    let description_rows: Vec<(i32, String)> = descriptions
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        description_rows,
        vec![
            (0, "index comment".to_owned()),
            (2, "name comment".to_owned()),
            (0, "table comment".to_owned()),
        ]
    );

    let dependencies = client
        .query(
            "SELECT deptype \
             FROM pg_catalog.pg_depend \
             ORDER BY deptype",
            &[],
        )
        .await
        .expect("pg_depend query");
    let deptypes: Vec<String> = dependencies.iter().map(|row| row.get(0)).collect();
    assert!(
        deptypes.contains(&"a".to_owned()) && deptypes.contains(&"n".to_owned()),
        "expected automatic and normal dependency rows, got {deptypes:?}"
    );

    let constraints = client
        .query(
            "SELECT constraint_type \
             FROM information_schema.table_constraints \
             WHERE table_name IN ('meta_parent', 'meta_child') \
             ORDER BY constraint_type",
            &[],
        )
        .await
        .expect("information_schema.table_constraints query");
    let constraint_types: Vec<String> = constraints.iter().map(|row| row.get(0)).collect();
    assert_eq!(
        constraint_types,
        vec![
            "CHECK".to_owned(),
            "FOREIGN KEY".to_owned(),
            "PRIMARY KEY".to_owned()
        ]
    );

    let key_usage = client
        .query(
            "SELECT constraint_name, column_name \
             FROM information_schema.key_column_usage \
             WHERE table_name IN ('meta_parent', 'meta_child') \
             ORDER BY constraint_name",
            &[],
        )
        .await
        .expect("information_schema.key_column_usage query");
    let key_columns: Vec<(String, String)> = key_usage
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        key_columns,
        vec![
            (
                "meta_child_parent_id_fkey".to_owned(),
                "parent_id".to_owned()
            ),
            ("meta_parent_pkey".to_owned(), "id".to_owned()),
        ]
    );

    let settings = client
        .query(
            "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'server_encoding'",
            &[],
        )
        .await
        .expect("pg_settings query");
    assert_eq!(settings.len(), 1);
    assert_eq!(settings[0].get::<_, String>(0), "UTF8");

    let routines = client
        .query(
            "SELECT routine_schema, routine_name \
             FROM information_schema.routines \
             ORDER BY 1, 2",
            &[],
        )
        .await
        .expect("information_schema.routines query");
    assert!(routines.is_empty());

    let triggers = client
        .query(
            "SELECT trigger_schema, trigger_name \
             FROM information_schema.triggers \
             ORDER BY 1, 2",
            &[],
        )
        .await
        .expect("information_schema.triggers query");
    assert!(triggers.is_empty());

    shutdown(client, server_handle).await;
}
