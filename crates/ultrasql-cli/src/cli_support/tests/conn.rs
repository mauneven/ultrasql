//! Connection-parameter tests: URL parsing, `~/.pgpass`, merge/override
//! precedence, and connection-string rendering.

#[cfg(unix)]
use std::fs;

use super::super::cli_args::{
    ConnParams, PGPASS_FILE_LIMIT_BYTES, PGPASS_FILE_READ_LIMIT_BYTES, pgpass_lookup_in_home,
};
use super::{test_cli, write_pgpass};

// --- URL parsing ---

#[test]
fn url_full_parse() {
    let p = ConnParams::from_url("postgresql://alice:s3cr3t@db.example.com:5433/mydb")
        .expect("valid URL");
    assert_eq!(p.host, "db.example.com");
    assert_eq!(p.port, 5433);
    assert_eq!(p.user, "alice");
    assert_eq!(p.password.as_deref(), Some("s3cr3t"));
    assert_eq!(p.dbname, "mydb");
}

#[test]
fn url_minimal_parse() {
    let p = ConnParams::from_url("postgres://localhost/testdb").expect("valid URL");
    assert_eq!(p.host, "localhost");
    assert_eq!(p.dbname, "testdb");
    assert!(p.password.is_none());
}

#[test]
fn url_without_path_uses_default_dbname() {
    // No path component — dbname stays as whatever the default was.
    let p = ConnParams::from_url("postgresql://myhost:5432").expect("valid URL");
    assert_eq!(p.host, "myhost");
    assert_eq!(p.port, 5432);
}

#[test]
fn url_invalid_scheme_rejects() {
    let err = ConnParams::from_url("mysql://localhost/db");
    assert!(err.is_err(), "non-pg URL must fail");
}

// --- ~/.pgpass ---

#[test]
fn pgpass_wildcard_host_matches() {
    // Build a temp pgpass file.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(".pgpass");
    write_pgpass(&path, "*:5432:mydb:bob:hunter2\n");

    let pw = pgpass_lookup_in_home(dir.path(), "anyhost", 5432, "mydb", "bob");
    assert_eq!(pw.as_deref(), Some("hunter2"));
}

#[test]
fn pgpass_wrong_user_no_match() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(".pgpass");
    write_pgpass(&path, "localhost:5432:mydb:alice:pw\n");

    let pw = pgpass_lookup_in_home(dir.path(), "localhost", 5432, "mydb", "bob");
    assert!(pw.is_none(), "wrong user must not match");
}

#[test]
fn pgpass_read_limit_is_one_byte_past_file_limit() {
    assert_eq!(
        PGPASS_FILE_READ_LIMIT_BYTES,
        u64::try_from(PGPASS_FILE_LIMIT_BYTES + 1).expect("pgpass limit fits u64"),
    );
}

#[cfg(unix)]
#[test]
fn pgpass_world_readable_file_is_ignored() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(".pgpass");
    fs::write(&path, "localhost:5432:db:user:secret\n").expect("write");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("chmod");

    let pw = pgpass_lookup_in_home(dir.path(), "localhost", 5432, "db", "user");
    assert!(pw.is_none());
}

#[test]
fn pgpass_missing_file_returns_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    // No .pgpass file in dir.
    let pw = pgpass_lookup_in_home(dir.path(), "localhost", 5432, "db", "user");
    assert!(pw.is_none());
}

#[test]
fn pgpass_ignores_comments_malformed_and_non_matching_lines() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_pgpass(
        &dir.path().join(".pgpass"),
        "# comment\nbad-line\nlocalhost:9999:db:user:nope\nlocalhost:5432:db:user:pw\n",
    );

    let pw = pgpass_lookup_in_home(dir.path(), "localhost", 5432, "db", "user");
    assert_eq!(pw.as_deref(), Some("pw"));
}

#[test]
fn pgpass_oversized_file_is_ignored() {
    let dir = tempfile::tempdir().expect("tempdir");
    let content = format!("{}\nlocalhost:5432:db:user:pw\n", "#".repeat(70 * 1024));
    write_pgpass(&dir.path().join(".pgpass"), &content);

    let pw = pgpass_lookup_in_home(dir.path(), "localhost", 5432, "db", "user");

    assert_eq!(pw, None);
}

// --- merge / override precedence and connection string ---

#[test]
fn conn_params_merge_overrides_and_connection_string_are_stable() {
    let mut params = ConnParams::default();
    params.merge_from(
        &ConnParams::from_url("postgresql://bob:pw@db.internal:15432/app").expect("valid URL"),
    );
    params.apply_overrides(
        Some("override.internal".to_owned()),
        Some(25432),
        Some("prod".to_owned()),
        Some("alice".to_owned()),
        Some("secret".to_owned()),
    );

    assert_eq!(params.host, "override.internal");
    assert_eq!(params.port, 25432);
    assert_eq!(params.dbname, "prod");
    assert_eq!(params.user, "alice");
    assert_eq!(params.password.as_deref(), Some("secret"));
    assert_eq!(
        crate::build_conn_string(&params),
        "host=override.internal port=25432 dbname=prod user=alice password=secret"
    );

    let err =
        ConnParams::from_url("postgresql://host:notaport/db").expect_err("invalid URL port fails");
    assert!(format!("{err:#}").contains("invalid port in URL"));

    let p = ConnParams::from_url("postgresql://carol@/db").expect("empty host accepted");
    assert_eq!(p.user, "carol");
    assert_eq!(p.dbname, "db");
}

#[test]
fn connection_string_quotes_keyword_values() {
    let params = ConnParams {
        host: "db internal".to_owned(),
        port: 25432,
        dbname: "prod db".to_owned(),
        user: "alice admin".to_owned(),
        password: Some("p a's\\word".to_owned()),
    };

    let rendered = crate::build_conn_string(&params);
    let parsed = rendered
        .parse::<tokio_postgres::Config>()
        .expect("rendered connection string must parse");

    match parsed.get_hosts() {
        [tokio_postgres::config::Host::Tcp(host)] => assert_eq!(host, "db internal"),
        other => panic!("expected one TCP host, got {other:?}"),
    }
    assert_eq!(parsed.get_dbname(), Some("prod db"));
    assert_eq!(parsed.get_user(), Some("alice admin"));
    assert_eq!(parsed.get_password(), Some("p a's\\word".as_bytes()));
}

#[test]
fn resolve_params_honors_url_position_and_flags() {
    let mut cli = test_cli();
    cli.url = Some("postgresql://u1:p1@url-host:5555/url_db".to_owned());
    cli.positional_url = Some("pos-host".to_owned());
    cli.host = Some("flag-host".to_owned());
    cli.port = Some(7777);
    cli.dbname = Some("flag_db".to_owned());
    cli.username = Some("flag_user".to_owned());
    cli.password = Some("flag_pw".to_owned());

    let params = crate::resolve_params(&cli).expect("resolve params");

    assert_eq!(params.host, "flag-host");
    assert_eq!(params.port, 7777);
    assert_eq!(params.dbname, "flag_db");
    assert_eq!(params.user, "flag_user");
    assert_eq!(params.password.as_deref(), Some("flag_pw"));

    let mut positional = test_cli();
    positional.positional_url = Some("postgresql://pos_user@pos-host/pos_db".to_owned());
    let params = crate::resolve_params(&positional).expect("resolve positional URL");
    assert_eq!(params.host, "pos-host");
    assert_eq!(params.user, "pos_user");
    assert_eq!(params.dbname, "pos_db");
}
