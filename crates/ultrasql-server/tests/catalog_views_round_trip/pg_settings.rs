//! `pg_settings` views: redacted WAL archive commands, session/runtime GUCs,
//! isolation level, search path, statement timeout, and static driver defaults.

use super::*;
use ultrasql_server::WalArchiveConfig;

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
async fn pg_settings_reflects_active_transaction_isolation() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin repeatable read");
    let row = client
        .query_one(
            "SELECT setting \
             FROM pg_catalog.pg_settings \
             WHERE name = 'transaction_isolation'",
            &[],
        )
        .await
        .expect("pg_settings transaction_isolation");
    assert_eq!(row.get::<_, String>(0), "repeatable read");
    client.batch_execute("COMMIT").await.expect("commit");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn pg_settings_reflects_session_search_path() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("SET search_path TO public, \"$user\"")
        .await
        .expect("set search_path");
    let row = client
        .query_one(
            "SELECT setting \
             FROM pg_catalog.pg_settings \
             WHERE name = 'search_path'",
            &[],
        )
        .await
        .expect("pg_settings search_path");
    assert_eq!(row.get::<_, String>(0), "public, \"$user\"");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn pg_settings_reflects_runtime_gucs() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute(
            "SET application_name = 'catalog_settings_probe'; \
             SET client_min_messages = warning; \
             SET DateStyle TO SQL, DMY; \
             SET extra_float_digits = 2; \
             SET IntervalStyle = iso_8601; \
             SET lc_monetary = 'C'; \
             SET TimeZone = 'America/Bogota'",
        )
        .await
        .expect("set runtime gucs");

    for (name, expected) in [
        ("application_name", "catalog_settings_probe"),
        ("client_min_messages", "warning"),
        ("DateStyle", "SQL, DMY"),
        ("extra_float_digits", "2"),
        ("IntervalStyle", "iso_8601"),
        ("lc_monetary", "C"),
        ("TimeZone", "America/Bogota"),
    ] {
        let row = client
            .query_one(
                "SELECT setting \
                 FROM pg_catalog.pg_settings \
                 WHERE name = $1",
                &[&name],
            )
            .await
            .expect("pg_settings runtime GUC");
        assert_eq!(row.get::<_, String>(0), expected, "{name}");
    }

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn pg_settings_reflects_statement_timeout() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    client
        .batch_execute("SET statement_timeout = 250")
        .await
        .expect("set statement_timeout");
    let row = client
        .query_one(
            "SELECT setting, unit \
             FROM pg_catalog.pg_settings \
             WHERE name = 'statement_timeout'",
            &[],
        )
        .await
        .expect("pg_settings statement_timeout");
    assert_eq!(row.get::<_, String>(0), "250");
    assert_eq!(row.get::<_, String>(1), "ms");

    client
        .batch_execute("RESET statement_timeout")
        .await
        .expect("reset statement_timeout");
    let row = client
        .query_one(
            "SELECT setting \
             FROM pg_catalog.pg_settings \
             WHERE name = 'statement_timeout'",
            &[],
        )
        .await
        .expect("pg_settings reset statement_timeout");
    assert_eq!(row.get::<_, String>(0), "0");

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn pg_settings_exposes_static_driver_defaults() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    for (name, expected) in [
        ("max_identifier_length", "63"),
        ("server_version_num", "140000"),
    ] {
        let row = client
            .query_one(
                "SELECT setting \
                 FROM pg_catalog.pg_settings \
                 WHERE name = $1",
                &[&name],
            )
            .await
            .expect("pg_settings static driver default");
        assert_eq!(row.get::<_, String>(0), expected, "{name}");
    }

    shutdown(client, server_handle).await;
}
