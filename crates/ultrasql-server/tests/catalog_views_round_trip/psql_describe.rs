//! `psql` `\d`-style table-detail probes: relation/index shape, policy,
//! statistics, publications, and inheritance/partition links.

use super::*;

#[tokio::test]
async fn psql_describe_table_relation_detail_probe_uses_pg_class_shape() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE psql_meta_table (id INT NOT NULL, label TEXT)")
        .await
        .expect("create psql describe probe table");

    let rows = client
        .query(
            "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, \
                    c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, \
                    false AS relhasoids, c.relispartition, '', c.reltablespace, \
                    CASE WHEN c.reloftype = 0 THEN '' \
                         ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, \
                    c.relpersistence, c.relreplident, am.amname \
             FROM pg_catalog.pg_class c \
             LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid) \
             LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid) \
             WHERE c.oid = '16384'",
            &[],
        )
        .await
        .expect("psql describe table relation detail probe");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 0);
    assert_eq!(rows[0].get::<_, String>(1), "r");
    assert!(!rows[0].get::<_, bool>(3));
    assert_eq!(rows[0].get::<_, String>(11), "");
    assert_eq!(rows[0].get::<_, String>(12), "p");
    assert_eq!(rows[0].get::<_, String>(13), "d");
    assert_eq!(
        rows[0].get::<_, Option<String>>(14).as_deref(),
        Some("heap")
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn psql_describe_table_index_detail_probe_uses_constraint_shape() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE psql_meta_table (id INT NOT NULL, label TEXT)")
        .await
        .expect("create psql index probe table");
    client
        .batch_execute("CREATE INDEX psql_meta_table_label_idx ON psql_meta_table(label)")
        .await
        .expect("create psql index probe index");

    let rows = client
        .query(
            "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, \
                    i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), \
                    pg_catalog.pg_get_constraintdef(con.oid, true), contype, \
                    condeferrable, condeferred, i.indisreplident, c2.reltablespace \
             FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i \
             LEFT JOIN pg_catalog.pg_constraint con \
                    ON (conrelid = i.indrelid AND conindid = i.indexrelid \
                        AND contype IN ('p','u','x')) \
             WHERE c.oid = '16384' AND c.oid = i.indrelid AND i.indexrelid = c2.oid \
             ORDER BY i.indisprimary DESC, c2.relname",
            &[],
        )
        .await
        .expect("psql describe table index detail probe");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "psql_meta_table_label_idx");
    assert!(!rows[0].get::<_, bool>(1));
    assert!(!rows[0].get::<_, bool>(2));
    assert!(rows[0].get::<_, bool>(4));
    assert!(rows[0].get::<_, Option<String>>(5).is_some());
    assert_eq!(rows[0].get::<_, Option<String>>(6), None);
    assert!(!rows[0].get::<_, bool>(10));

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn psql_describe_table_policy_probe_accepts_empty_pg_policy() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE psql_meta_table (id INT NOT NULL, label TEXT)")
        .await
        .expect("create psql policy probe table");

    let rows = client
        .query(
            "SELECT pol.polname, pol.polpermissive, \
                    CASE WHEN pol.polroles = '{0}' THEN NULL \
                         ELSE pg_catalog.array_to_string( \
                             array(select rolname \
                                   from pg_catalog.pg_roles \
                                   where oid = any (pol.polroles) \
                                   order by 1), ',') \
                    END, \
                    pg_catalog.pg_get_expr(pol.polqual, pol.polrelid), \
                    pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid), \
                    CASE pol.polcmd \
                         WHEN 'r' THEN 'SELECT' \
                         WHEN 'a' THEN 'INSERT' \
                         WHEN 'w' THEN 'UPDATE' \
                         WHEN 'd' THEN 'DELETE' \
                    END AS cmd \
             FROM pg_catalog.pg_policy pol \
             WHERE pol.polrelid = '16384' \
             ORDER BY 1",
            &[],
        )
        .await
        .expect("psql describe table policy probe");

    assert!(rows.is_empty());

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn psql_describe_table_statistics_probe_accepts_empty_pg_statistic_ext() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE psql_meta_table (id INT NOT NULL, label TEXT)")
        .await
        .expect("create psql statistics probe table");

    let rows = client
        .query(
            "SELECT oid, stxrelid::pg_catalog.regclass, \
                    stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, \
                    stxname, \
                    pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns, \
                    'd' = any(stxkind) AS ndist_enabled, \
                    'f' = any(stxkind) AS deps_enabled, \
                    'm' = any(stxkind) AS mcv_enabled, \
                    stxstattarget \
             FROM pg_catalog.pg_statistic_ext \
             WHERE stxrelid = '16384' \
             ORDER BY nsp, stxname",
            &[],
        )
        .await
        .expect("psql describe table statistics probe");

    assert!(rows.is_empty());

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn psql_describe_table_publication_probe_accepts_empty_links() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE psql_meta_table (id INT NOT NULL, label TEXT)")
        .await
        .expect("create psql publication probe table");

    let rows = client
        .query(
            "SELECT pubname \
             FROM pg_catalog.pg_publication p \
             JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid \
             WHERE pr.prrelid = '16384' \
             UNION ALL \
             SELECT pubname \
             FROM pg_catalog.pg_publication p \
             WHERE p.puballtables \
               AND pg_catalog.pg_relation_is_publishable('16384') \
             ORDER BY 1",
            &[],
        )
        .await
        .expect("psql describe table publication probe");

    assert!(rows.is_empty());

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn psql_describe_table_inherits_probe_accepts_empty_pg_inherits() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE psql_meta_table (id INT NOT NULL, label TEXT)")
        .await
        .expect("create psql inherits probe table");

    let rows = client
        .query(
            "SELECT c.oid::pg_catalog.regclass \
             FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i \
             WHERE c.oid = i.inhparent \
               AND i.inhrelid = '16384' \
               AND c.relkind != 'p' \
               AND c.relkind != 'I' \
             ORDER BY inhseqno",
            &[],
        )
        .await
        .expect("psql describe table inherits probe");

    assert!(rows.is_empty());

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn psql_describe_table_partition_child_probe_accepts_empty_pg_inherits() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("CREATE TABLE psql_meta_table (id INT NOT NULL, label TEXT)")
        .await
        .expect("create psql partition child probe table");

    let rows = client
        .query(
            "SELECT c.oid::pg_catalog.regclass, c.relkind, \
                    inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid) \
             FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i \
             WHERE c.oid = i.inhrelid \
               AND i.inhparent = '16384' \
             ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', \
                      c.oid::pg_catalog.regclass::pg_catalog.text",
            &[],
        )
        .await
        .expect("psql describe table partition child probe");

    assert!(rows.is_empty());

    shutdown(client, server_handle).await;
}
