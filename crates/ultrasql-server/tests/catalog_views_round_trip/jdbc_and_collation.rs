//! JDBC metadata probes (`pg_class` existence, keyword list, namespace schemas)
//! and column collation defaults that survive restart.

use super::*;

#[tokio::test]
async fn pg_class_exists_probe_returns_bool_after_create_table() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE flyway_schema_history (installed_rank INT NOT NULL)")
        .await
        .expect("create table");

    let row = client
        .query_one(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1
                   AND c.relname = $2
                   AND c.relkind = 'r'
             )",
            &[&"public", &"flyway_schema_history"],
        )
        .await
        .expect("exists probe");
    assert!(row.get::<_, bool>(0));

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn pg_get_keywords_supports_jdbc_metadata_probe() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    let row = client
        .query_one(
            "SELECT word, catcode, catdesc \
             FROM pg_catalog.pg_get_keywords() \
             WHERE catcode = 'U' \
             ORDER BY word \
             LIMIT 1",
            &[],
        )
        .await
        .expect("pg_get_keywords metadata probe");
    assert_eq!(row.get::<_, String>(0), "abort");
    assert_eq!(row.get::<_, String>(1), "U");
    assert_eq!(row.get::<_, String>(2), "unreserved");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn pg_namespace_supports_jdbc_get_schemas_probe() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query(
            "SELECT nspname AS \"TABLE_SCHEM\", current_database() AS \"TABLE_CATALOG\" \
             FROM pg_catalog.pg_namespace \
             WHERE nspname <> 'pg_toast' \
               AND (nspname !~ '^pg_temp_' OR nspname = (pg_catalog.current_schemas(true))[1]) \
               AND (nspname !~ '^pg_toast_temp_' \
                    OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_')) \
             ORDER BY \"TABLE_SCHEM\"",
            &[],
        )
        .await
        .expect("JDBC getSchemas probe");

    let schemas: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
    assert!(schemas.contains(&"public".to_owned()));

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn create_table_column_collate_default_is_validated_and_visible() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE collate_default (name TEXT COLLATE default)")
        .await
        .expect("create table with default column collation");

    let attcollation = client
        .query_one(
            "SELECT a.attcollation \
             FROM pg_attribute a \
             WHERE a.attrelid = '\"collate_default\"'::regclass \
               AND a.attname = 'name'",
            &[],
        )
        .await
        .expect("column collation oid")
        .get::<_, u32>(0);
    assert_eq!(attcollation, 100);

    client
        .batch_execute("CREATE TABLE collate_c (name TEXT COLLATE \"C\")")
        .await
        .expect("create table with C column collation");
    let row = client
        .query_one(
            "SELECT a.attcollation, c.collname \
             FROM pg_attribute a \
             JOIN pg_collation c ON a.attcollation = c.oid \
             WHERE a.attrelid = '\"collate_c\"'::regclass \
               AND a.attname = 'name'",
            &[],
        )
        .await
        .expect("C column collation metadata");
    assert_eq!(row.get::<_, u32>(0), 950);
    assert_eq!(row.get::<_, String>(1), "C");

    let err = client
        .batch_execute("CREATE TABLE collate_int (id INT COLLATE default)")
        .await
        .expect_err("non-text column collation rejected");
    let message = err
        .as_db_error()
        .map(tokio_postgres::error::DbError::message)
        .unwrap_or_default();
    assert!(
        message.contains("COLLATE applies to text types"),
        "unexpected error: {err}"
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn explicit_column_collation_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp dir");
    let (_server, client, _conn, server_handle) =
        start_server_and_connect_with(Server::init(data_dir.path()).expect("server init")).await;

    client
        .batch_execute("CREATE TABLE collate_restart (name TEXT COLLATE \"POSIX\")")
        .await
        .expect("create table with POSIX column collation");

    shutdown(client, server_handle).await;

    let (_server, client, _conn, server_handle) =
        start_server_and_connect_with(Server::init(data_dir.path()).expect("server restart")).await;
    let row = client
        .query_one(
            "SELECT a.attcollation, c.collname \
             FROM pg_attribute a \
             JOIN pg_collation c ON a.attcollation = c.oid \
             WHERE a.attrelid = '\"collate_restart\"'::regclass \
               AND a.attname = 'name'",
            &[],
        )
        .await
        .expect("POSIX column collation after restart");
    assert_eq!(row.get::<_, u32>(0), 951);
    assert_eq!(row.get::<_, String>(1), "POSIX");

    shutdown(client, server_handle).await;
}
