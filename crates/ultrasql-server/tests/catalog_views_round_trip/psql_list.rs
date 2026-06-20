//! `psql` `\d`/`\df`/`\du`/`\l`-style listing probes for tables, functions,
//! roles, and databases.

use super::*;

#[tokio::test]
async fn psql_list_tables_probe_uses_pg_class_owner() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE psql_meta_table (id INT NOT NULL, label TEXT)")
        .await
        .expect("create psql list tables probe table");

    let rows = client
        .query(
            "SELECT n.nspname AS \"Schema\", \
                    c.relname AS \"Name\", \
                    CASE c.relkind \
                         WHEN 'r' THEN 'table' \
                         WHEN 'v' THEN 'view' \
                         WHEN 'm' THEN 'materialized view' \
                         WHEN 'i' THEN 'index' \
                         WHEN 'S' THEN 'sequence' \
                         WHEN 's' THEN 'special' \
                         WHEN 't' THEN 'TOAST table' \
                         WHEN 'f' THEN 'foreign table' \
                         WHEN 'p' THEN 'partitioned table' \
                         WHEN 'I' THEN 'partitioned index' \
                    END AS \"Type\", \
                    pg_catalog.pg_get_userbyid(c.relowner) AS \"Owner\" \
             FROM pg_catalog.pg_class c \
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam \
             WHERE c.relkind IN ('r','p','') \
               AND n.nspname <> 'pg_catalog' \
               AND n.nspname !~ '^pg_toast' \
               AND n.nspname <> 'information_schema' \
               AND pg_catalog.pg_table_is_visible(c.oid) \
             ORDER BY 1,2",
            &[],
        )
        .await
        .expect("psql list tables probe");

    assert!(
        rows.iter()
            .any(|row| row.get::<_, String>(1) == "psql_meta_table")
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn psql_list_functions_probe_filters_builtin_pg_proc() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    let builtin_rows = client
        .query(
            "SELECT proname, prokind, pronargs, \
                    pg_catalog.format_type(prorettype, NULL), \
                    provolatile, proretset \
             FROM pg_catalog.pg_proc \
             WHERE proname IN ('pg_get_userbyid', 'version') \
             ORDER BY proname",
            &[],
        )
        .await
        .expect("builtin pg_proc rows");
    assert_eq!(builtin_rows.len(), 2);
    assert_eq!(builtin_rows[0].get::<_, String>(0), "pg_get_userbyid");
    assert_eq!(builtin_rows[0].get::<_, String>(1), "f");
    assert_eq!(builtin_rows[0].get::<_, i16>(2), 1);
    assert_eq!(builtin_rows[0].get::<_, String>(3), "text");
    assert_eq!(builtin_rows[0].get::<_, String>(4), "s");
    assert!(!builtin_rows[0].get::<_, bool>(5));
    assert_eq!(builtin_rows[1].get::<_, String>(0), "version");
    assert_eq!(builtin_rows[1].get::<_, String>(1), "f");
    assert_eq!(builtin_rows[1].get::<_, i16>(2), 0);
    assert_eq!(builtin_rows[1].get::<_, String>(3), "text");
    assert_eq!(builtin_rows[1].get::<_, String>(4), "s");
    assert!(!builtin_rows[1].get::<_, bool>(5));

    let rows = client
        .query(
            "SELECT n.nspname AS \"Schema\", \
                    p.proname AS \"Name\", \
                    pg_catalog.pg_get_function_result(p.oid) AS \"Result data type\", \
                    pg_catalog.pg_get_function_arguments(p.oid) AS \"Argument data types\", \
                    CASE p.prokind \
                         WHEN 'a' THEN 'agg' \
                         WHEN 'w' THEN 'window' \
                         WHEN 'p' THEN 'proc' \
                         ELSE 'func' \
                    END AS \"Type\" \
             FROM pg_catalog.pg_proc p \
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
             WHERE pg_catalog.pg_function_is_visible(p.oid) \
               AND n.nspname <> 'pg_catalog' \
               AND n.nspname <> 'information_schema' \
             ORDER BY 1, 2, 4",
            &[],
        )
        .await
        .expect("psql list functions probe");

    assert!(rows.is_empty());

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn psql_list_roles_probe_accepts_empty_pg_auth_members() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE ROLE psql_meta_role LOGIN")
        .await
        .expect("create psql roles probe role");

    let rows = client
        .query(
            "SELECT r.rolname, r.rolsuper, r.rolinherit, \
                    r.rolcreaterole, r.rolcreatedb, r.rolcanlogin, \
                    r.rolconnlimit, r.rolvaliduntil, \
                    ARRAY(SELECT b.rolname \
                          FROM pg_catalog.pg_auth_members m \
                          JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) \
                          WHERE m.member = r.oid) AS memberof, \
                    r.rolreplication, \
                    r.rolbypassrls \
             FROM pg_catalog.pg_roles r \
             WHERE r.rolname !~ '^pg_' \
             ORDER BY 1",
            &[],
        )
        .await
        .expect("psql list roles probe");

    assert!(
        rows.iter()
            .any(|row| row.get::<_, String>(0) == "psql_meta_role")
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn psql_list_databases_probe_uses_pg_database_shape() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query(
            "SELECT d.datname AS \"Name\", \
                    pg_catalog.pg_get_userbyid(d.datdba) AS \"Owner\", \
                    pg_catalog.pg_encoding_to_char(d.encoding) AS \"Encoding\", \
                    d.datcollate AS \"Collate\", \
                    d.datctype AS \"Ctype\", \
                    pg_catalog.array_to_string(d.datacl, E'\\n') AS \"Access privileges\" \
             FROM pg_catalog.pg_database d \
             ORDER BY 1",
            &[],
        )
        .await
        .expect("psql list databases probe");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "ultrasql");
    assert_eq!(rows[0].get::<_, String>(2), "UTF8");

    shutdown(client, server_handle).await;
}
