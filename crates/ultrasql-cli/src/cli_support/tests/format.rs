//! Formatting, statement-splitting, meta-query, and live-session tests.

use std::fs;

use tokio_postgres::NoTls;
use ultrasql_server::Server;

use super::super::cli_args::ConnParams;
use super::super::fileio::read_sql_script_file;
use super::super::session::{
    Session, build_separator, describe_table_sql, list_indexes_sql, list_tables_sql,
    split_statements,
};
use super::cli_env_test_lock;

// --- Statement splitter ---

#[test]
fn split_single_stmt() {
    let stmts = split_statements("SELECT 1;");
    assert_eq!(stmts, vec!["SELECT 1"]);
}

#[test]
fn split_multiple_stmts() {
    let stmts = split_statements("SELECT 1; SELECT 2; SELECT 3;");
    assert_eq!(stmts, vec!["SELECT 1", "SELECT 2", "SELECT 3"]);
}

#[test]
fn split_respects_quoted_semicolon() {
    let stmts = split_statements("SELECT ';' AS c;");
    assert_eq!(stmts, vec!["SELECT ';' AS c"]);
}

#[test]
fn split_comment_skipped_for_semicolon_detection() {
    // The splitter skips `--` comments when searching for `;`, so the
    // semicolon on the next line terminates the statement. The comment
    // text is retained in the slice (the SQL engine will ignore it).
    let stmts = split_statements("SELECT 1 -- comment\n;");
    assert_eq!(stmts, vec!["SELECT 1 -- comment"]);
}

#[test]
fn split_no_trailing_semicolon() {
    let stmts = split_statements("SELECT 1");
    assert_eq!(stmts, vec!["SELECT 1"]);
}

// --- Formatting helpers ---

#[test]
fn build_separator_correct_width() {
    let sep = build_separator(&[3, 5]);
    // Each column: width + 2 spaces + border
    // "+-----+-------+"
    assert_eq!(sep, "+-----+-------+");
}

#[test]
fn meta_query_builders_sanitize_patterns() {
    assert!(list_tables_sql("").contains("pg_catalog.pg_tables"));
    let tables = list_tables_sql("foo';DROP%bar");
    assert!(tables.contains("LIKE 'fooDROP%bar'"));
    assert!(!tables.contains("foo'"));

    let describe = describe_table_sql("public.users;DELETE");
    assert!(describe.contains("table_name = 'public.usersDELETE'"));

    let indexes = list_indexes_sql("idx_%';");
    assert!(indexes.contains("LIKE 'idx_%'"));
    assert!(!indexes.contains("idx_%';"));
}

#[test]
fn sql_script_file_reads_are_bounded() {
    let _env_guard = cli_env_test_lock();
    // SAFETY: cli_env_test_lock serializes process-env mutation in this
    // module's tests.
    unsafe {
        std::env::set_var("ULTRASQL_SQL_SCRIPT_FILE_LIMIT_BYTES", "3");
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let script = dir.path().join("script.sql");
    fs::write(&script, "SELECT 1;").expect("write script");

    let err = read_sql_script_file(&script).expect_err("oversized script rejected");

    assert!(err.to_string().contains("exceeds read limit"), "{err}");
    // SAFETY: cli_env_test_lock serializes process-env mutation in this
    // module's tests.
    unsafe {
        std::env::remove_var("ULTRASQL_SQL_SCRIPT_FILE_LIMIT_BYTES");
    }
}

#[tokio::test]
async fn session_meta_batch_and_sql_paths_execute_against_in_process_server() {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("socket literal");
    let (listener, bound) = ultrasql_server::bind_listener(addr)
        .await
        .expect("bind in-process listener");
    let server = std::sync::Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(ultrasql_server::serve_listener(listener, server));
    let conn = format!(
        "host={} port={} user=cli_tester application_name=ultrasql_cli_test",
        bound.ip(),
        bound.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn, NoTls)
        .await
        .expect("connect in-process server");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let params = ConnParams {
        host: bound.ip().to_string(),
        port: bound.port(),
        dbname: "ultrasql".to_owned(),
        user: "cli_tester".to_owned(),
        password: None,
    };
    let mut session = Session::new(client, params);

    session
        .exec_sql("SELECT 1 AS one")
        .await
        .expect("select row");
    session
        .exec_sql("SELECT 1 AS one WHERE false")
        .await
        .expect("empty select");
    session
        .exec_sql("SELECT no_such_column")
        .await
        .expect("error path");

    for cmd in [
        "\\?",
        "\\timing",
        "\\conninfo",
        "\\dt",
        "\\dt users",
        "\\d",
        "\\d users",
        "\\di",
        "\\dn",
        "\\l",
        "\\du",
        "\\df",
        "\\dv",
        "\\ds",
        "\\x",
        "\\x on",
        "\\x off",
        "\\pset",
        "\\pset expanded off",
        "\\pset format aligned",
        "\\pset unknown value",
        "\\c",
        "\\c otherdb",
        "\\unknown",
    ] {
        assert!(!session.handle_meta(cmd).await.expect("meta command"));
    }
    assert!(!session.handle_meta("\\x bad").await.expect("invalid x"));

    let dir = tempfile::tempdir().expect("tempdir");
    let script = dir.path().join("script.sql");
    fs::write(&script, "SELECT 2 AS two;\\ignored\nSELECT 3 AS three;")
        .expect("write include script");
    let include_cmd = format!("\\i {}", script.display());
    assert!(
        !session
            .handle_meta(&include_cmd)
            .await
            .expect("include command")
    );

    session
        .exec_batch("\\timing; SELECT 4 AS four; \\q; SELECT 5 AS five;")
        .await
        .expect("batch execution");
    assert!(session.handle_meta("\\q").await.expect("quit command"));

    handle.abort();
}
