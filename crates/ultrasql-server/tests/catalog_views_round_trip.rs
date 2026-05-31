//! End-to-end metadata view tests.
//!
//! These tests drive the virtual `pg_catalog` / `information_schema`
//! relations through the normal SQL path used by CLI `\d`-style commands.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::NoTls;
use ultrasql_server::{Server, WalArchiveConfig, bind_listener, serve_listener};

async fn start_server_and_connect() -> (
    Arc<Server>,
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    start_server_and_connect_with(Server::with_sample_database()).await
}

async fn start_server_and_connect_with(
    server: Server,
) -> (
    Arc<Server>,
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    start_server_and_connect_with_user(server, "tester").await
}

async fn start_server_and_connect_with_user(
    server: Server,
    user: &str,
) -> (
    Arc<Server>,
    tokio_postgres::Client,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<Result<(), ultrasql_server::ServerError>>,
) {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let server = Arc::new(server);
    let server_handle = tokio::spawn(serve_listener(listener, Arc::clone(&server)));
    let conn_str = format!(
        "host={host} port={port} user={user} application_name=catalog_views_test",
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
    (server, client, conn_handle, server_handle)
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
    let (server, client, _conn, server_handle) = start_server_and_connect().await;

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
            "SELECT datname AS name, pg_catalog.pg_get_userbyid(datdba) AS owner, datallowconn \
             FROM pg_catalog.pg_database \
             ORDER BY 1",
            &[],
        )
        .await
        .expect("pg_database meta query");
    assert_eq!(databases.len(), 1);
    assert_eq!(databases[0].get::<_, String>(0), "ultrasql");
    assert_eq!(databases[0].get::<_, String>(1), "ultrasql");
    assert!(databases[0].get::<_, bool>(2));

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
    let autovacuum_settings = client
        .query(
            "SELECT name, setting \
             FROM pg_catalog.pg_settings \
             WHERE name IN ('autovacuum_vacuum_threshold', 'autovacuum_analyze_scale_factor') \
             ORDER BY name",
            &[],
        )
        .await
        .expect("pg_settings autovacuum query");
    assert_eq!(autovacuum_settings.len(), 2);
    assert_eq!(
        autovacuum_settings[0].get::<_, String>(0),
        "autovacuum_analyze_scale_factor"
    );
    assert_eq!(autovacuum_settings[0].get::<_, String>(1), "0.1");
    assert_eq!(
        autovacuum_settings[1].get::<_, String>(0),
        "autovacuum_vacuum_threshold"
    );
    assert_eq!(autovacuum_settings[1].get::<_, String>(1), "50");
    let logging_settings = client
        .query(
            "SELECT name, setting \
             FROM pg_catalog.pg_settings \
             WHERE name IN ('log_connections', 'log_min_duration_statement', 'log_statement') \
             ORDER BY name",
            &[],
        )
        .await
        .expect("pg_settings logging query");
    let logging_pairs: Vec<(String, String)> = logging_settings
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        logging_pairs,
        vec![
            ("log_connections".to_owned(), "off".to_owned()),
            ("log_min_duration_statement".to_owned(), "-1".to_owned()),
            ("log_statement".to_owned(), "none".to_owned()),
        ]
    );

    client
        .batch_execute("CREATE TABLE stat_t (id INT)")
        .await
        .expect("create stats table");
    client
        .batch_execute("INSERT INTO stat_t VALUES (1), (2), (3)")
        .await
        .expect("insert stats rows");
    client
        .batch_execute("DELETE FROM stat_t WHERE id = 2")
        .await
        .expect("delete stats row");
    let table_stats = client
        .query(
            "SELECT n_live_tup, n_dead_tup \
             FROM pg_catalog.pg_stat_user_tables \
             WHERE relname = 'stat_t'",
            &[],
        )
        .await
        .expect("pg_stat_user_tables tuple counters");
    assert_eq!(table_stats.len(), 1);
    assert_eq!(table_stats[0].get::<_, i64>(0), 2);
    assert_eq!(table_stats[0].get::<_, i64>(1), 1);
    client
        .batch_execute("VACUUM stat_t")
        .await
        .expect("vacuum stats table");
    let vacuumed_stats = client
        .query(
            "SELECT n_live_tup, n_dead_tup \
             FROM pg_catalog.pg_stat_user_tables \
             WHERE relname = 'stat_t'",
            &[],
        )
        .await
        .expect("pg_stat_user_tables tuple counters after vacuum");
    assert_eq!(vacuumed_stats.len(), 1);
    assert_eq!(vacuumed_stats[0].get::<_, i64>(0), 2);
    assert_eq!(vacuumed_stats[0].get::<_, i64>(1), 0);
    let table_io = client
        .query(
            "SELECT heap_blks_read, heap_blks_hit \
             FROM pg_catalog.pg_statio_user_tables \
             WHERE relname = 'stat_t'",
            &[],
        )
        .await
        .expect("pg_statio_user_tables heap counters");
    assert_eq!(table_io.len(), 1);
    assert!(
        table_io[0].get::<_, i64>(0) > 0 || table_io[0].get::<_, i64>(1) > 0,
        "expected stat_t heap reads or hits to be recorded"
    );

    client
        .batch_execute("CREATE TABLE stat_idx_t (id INT)")
        .await
        .expect("create index stats table");
    client
        .batch_execute("INSERT INTO stat_idx_t VALUES (1), (2), (3)")
        .await
        .expect("insert index stats rows");
    client
        .batch_execute("CREATE INDEX stat_idx_t_id_idx ON stat_idx_t(id)")
        .await
        .expect("create stats index");
    let selected = client
        .query("SELECT id FROM stat_idx_t WHERE id = 2", &[])
        .await
        .expect("run indexed point lookup");
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].get::<_, i32>(0), 2);
    let index_stats = client
        .query(
            "SELECT idx_scan, idx_tup_read, idx_tup_fetch \
             FROM pg_catalog.pg_stat_user_indexes \
             WHERE indexrelname = 'stat_idx_t_id_idx'",
            &[],
        )
        .await
        .expect("pg_stat_user_indexes counters");
    assert_eq!(index_stats.len(), 1);
    assert!(index_stats[0].get::<_, i64>(0) >= 1);
    assert!(index_stats[0].get::<_, i64>(1) >= 1);
    assert!(index_stats[0].get::<_, i64>(2) >= 1);

    server.workload_recorder.begin_create_index(321, 41, 42, 7);
    server
        .workload_recorder
        .update_create_index(321, "building index", 3);
    let create_index_progress = client
        .query(
            "SELECT pid, relid, index_relid, phase, blocks_total, blocks_done \
             FROM pg_catalog.pg_stat_progress_create_index \
             WHERE pid = 321",
            &[],
        )
        .await
        .expect("pg_stat_progress_create_index rows");
    assert_eq!(create_index_progress.len(), 1);
    assert_eq!(create_index_progress[0].get::<_, i32>(0), 321);
    assert_eq!(create_index_progress[0].get::<_, i64>(1), 41);
    assert_eq!(create_index_progress[0].get::<_, i64>(2), 42);
    assert_eq!(
        create_index_progress[0].get::<_, String>(3),
        "building index"
    );
    assert_eq!(create_index_progress[0].get::<_, i64>(4), 7);
    assert_eq!(create_index_progress[0].get::<_, i64>(5), 3);
    server.workload_recorder.finish_create_index(321);

    let database_stats = client
        .query(
            "SELECT xact_commit, xact_rollback \
             FROM pg_catalog.pg_stat_database \
             WHERE datname = 'ultrasql'",
            &[],
        )
        .await
        .expect("pg_stat_database counters");
    assert_eq!(database_stats.len(), 1);
    assert!(database_stats[0].get::<_, i64>(0) > 0);
    assert_eq!(database_stats[0].get::<_, i64>(1), 0);

    let bgwriter_stats = client
        .query(
            "SELECT buffers_backend, buffers_alloc \
             FROM pg_catalog.pg_stat_bgwriter",
            &[],
        )
        .await
        .expect("pg_stat_bgwriter counters");
    assert_eq!(bgwriter_stats.len(), 1);
    assert!(bgwriter_stats[0].get::<_, i64>(0) > 0);
    assert!(bgwriter_stats[0].get::<_, i64>(1) > 0);

    let meta_t_oid = server
        .catalog_snapshot()
        .tables
        .get("meta_t")
        .expect("meta_t catalog entry")
        .oid
        .raw();
    server.workload_recorder.begin_vacuum(42, meta_t_oid, 3);
    server
        .workload_recorder
        .update_vacuum(42, "vacuuming heap", 2, 1);
    let vacuum_progress = client
        .query(
            "SELECT relid, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed \
             FROM pg_catalog.pg_stat_progress_vacuum \
             WHERE pid = 42",
            &[],
        )
        .await
        .expect("pg_stat_progress_vacuum query");
    assert_eq!(vacuum_progress.len(), 1);
    assert_eq!(vacuum_progress[0].get::<_, i64>(0), i64::from(meta_t_oid));
    assert_eq!(vacuum_progress[0].get::<_, String>(1), "vacuuming heap");
    assert_eq!(vacuum_progress[0].get::<_, i64>(2), 3);
    assert_eq!(vacuum_progress[0].get::<_, i64>(3), 2);
    assert_eq!(vacuum_progress[0].get::<_, i64>(4), 1);
    server.workload_recorder.finish_vacuum(42);

    server.workload_recorder.begin_analyze(43, meta_t_oid, 5);
    server
        .workload_recorder
        .update_analyze(43, "computing statistics", 4);
    let analyze_progress = client
        .query(
            "SELECT relid, phase, sample_blks_total, sample_blks_scanned \
             FROM pg_catalog.pg_stat_progress_analyze \
             WHERE pid = 43",
            &[],
        )
        .await
        .expect("pg_stat_progress_analyze query");
    assert_eq!(analyze_progress.len(), 1);
    assert_eq!(analyze_progress[0].get::<_, i64>(0), i64::from(meta_t_oid));
    assert_eq!(
        analyze_progress[0].get::<_, String>(1),
        "computing statistics"
    );
    assert_eq!(analyze_progress[0].get::<_, i64>(2), 5);
    assert_eq!(analyze_progress[0].get::<_, i64>(3), 4);
    server.workload_recorder.finish_analyze(43);

    let routines = client
        .query(
            "SELECT routine_schema, routine_name \
             FROM information_schema.routines \
             ORDER BY 1, 2",
            &[],
        )
        .await
        .expect("information_schema.routines query");
    assert!(
        routines.iter().any(|row| {
            row.get::<_, String>(0) == "pg_catalog" && row.get::<_, String>(1) == "version"
        }),
        "information_schema.routines should expose builtin routines"
    );

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

#[tokio::test]
async fn pg_settings_redacts_wal_archive_commands() {
    let mut server = Server::with_sample_database();
    server.set_wal_archive_config(WalArchiveConfig {
        archive_command: "aws s3 cp %p s3://bucket/%f --token secret".to_owned(),
        restore_command: "curl -H 'Authorization: Bearer secret' %f > %p".to_owned(),
    });
    let (_server, client, _conn, server_handle) =
        start_server_and_connect_with_user(server, "ultrasql").await;

    let rows = client
        .query(
            "SELECT name, setting \
             FROM pg_catalog.pg_settings \
             WHERE name IN ('archive_command', 'restore_command') \
             ORDER BY name",
            &[],
        )
        .await
        .expect("pg_settings WAL command query");
    let pairs: Vec<(String, String)> = rows.iter().map(|row| (row.get(0), row.get(1))).collect();

    assert_eq!(
        pairs,
        vec![
            ("archive_command".to_owned(), "<redacted>".to_owned()),
            ("restore_command".to_owned(), "<redacted>".to_owned()),
        ]
    );

    shutdown(client, server_handle).await;
}

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
