//! GUI client schema/table/column browser probes (pgAdmin, DBeaver, DataGrip).

use super::*;

#[tokio::test]
async fn pgadmin_schema_browser_probe_uses_namespace_acl_and_descriptions() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query(
            "SELECT n.oid, n.nspname, pg_catalog.pg_get_userbyid(n.nspowner), \
                    n.nspacl, pg_catalog.obj_description(n.oid, 'pg_namespace') \
             FROM pg_catalog.pg_namespace n \
             WHERE NOT pg_catalog.pg_is_other_temp_schema(n.oid) \
               AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
             ORDER BY n.nspname",
            &[],
        )
        .await
        .expect("pgAdmin schema browser probe");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(1), "public");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn dbeaver_table_browser_probe_uses_relation_acl_options_and_description() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE gui_meta_table (id INT PRIMARY KEY, label TEXT); \
             COMMENT ON TABLE gui_meta_table IS 'gui meta table'",
        )
        .await
        .expect("create GUI table browser probe table");

    let rows = client
        .query(
            "SELECT c.oid, n.nspname, c.relname, c.relkind, c.relowner, \
                    c.relacl, c.reloptions, \
                    pg_catalog.obj_description(c.oid, 'pg_class') AS description \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' \
               AND c.relname = 'gui_meta_table' \
             ORDER BY c.relname",
            &[],
        )
        .await
        .expect("DBeaver table browser probe");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(2), "gui_meta_table");
    assert_eq!(rows[0].get::<_, Option<String>>(7), None);

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn datagrip_column_browser_probe_uses_attribute_options_and_serial_helper() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "CREATE TABLE gui_meta_table (id INT PRIMARY KEY, label TEXT); \
             COMMENT ON COLUMN gui_meta_table.label IS 'gui label'",
        )
        .await
        .expect("create DataGrip column browser probe table");

    let rows = client
        .query(
            "SELECT a.attname, a.attnum, a.attnotnull, a.attacl, a.attoptions, \
                    t.typname, t.typowner, pg_catalog.format_type(a.atttypid, a.atttypmod), \
                    pg_catalog.col_description(a.attrelid, a.attnum), \
                    pg_catalog.pg_get_serial_sequence('gui_meta_table', a.attname) \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = 'gui_meta_table'::pg_catalog.regclass \
               AND a.attnum > 0 \
               AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[],
        )
        .await
        .expect("DataGrip column browser probe");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].get::<_, String>(0), "label");
    assert_eq!(rows[1].get::<_, Option<String>>(8), None);
    assert_eq!(rows[1].get::<_, Option<String>>(9), None);

    shutdown(client, server_handle).await;
}
