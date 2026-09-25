//! `EXPLAIN [ANALYZE]` must enforce the privileges, row-level security, and
//! transaction semantics of the statement it explains.

use super::*;

type TestSession = Session<tokio::io::DuplexStream>;

fn session_as(server: &Arc<Server>, user: &str) -> TestSession {
    let (io, _peer) = duplex(64);
    let mut session = Session::new(io, Arc::clone(server), None);
    user.clone_into(&mut session.auth_user);
    user.clone_into(&mut session.current_user);
    session
}

fn run_ok(session: &mut TestSession, sql: &str) -> crate::LocalQueryOutput {
    let result = session
        .execute_query(sql, false)
        .unwrap_or_else(|err| panic!("{sql}: {err}"));
    crate::local_output_from_select_result(result).expect("decode result")
}

fn run_err(session: &mut TestSession, sql: &str) -> ServerError {
    match session.execute_query(sql, false) {
        Ok(_) => panic!("{sql}: expected an error"),
        Err(err) => err,
    }
}

fn count(session: &mut TestSession, table: &str) -> String {
    let sql = format!("SELECT count(*) FROM {table}");
    run_ok(session, &sql)
        .rows
        .first()
        .and_then(|row| row.first().cloned().flatten())
        .unwrap_or_else(|| panic!("{sql}: no value"))
}

/// Superuser session, a `secrets` table with two rows, and a `alice` login
/// with no grants.
fn secrets_server() -> (Arc<Server>, TestSession) {
    let server = Arc::new(Server::with_sample_database());
    let mut admin = session_as(&server, "ultrasql");
    for sql in [
        "CREATE ROLE alice LOGIN",
        "CREATE TABLE secrets (id int, ssn text)",
        "INSERT INTO secrets VALUES (1, 'a'), (2, 'b')",
    ] {
        run_ok(&mut admin, sql);
    }
    (server, admin)
}

#[test]
fn explain_requires_the_privileges_of_the_explained_statement() {
    let (server, mut admin) = secrets_server();
    let mut alice = session_as(&server, "alice");
    for sql in [
        "EXPLAIN SELECT * FROM secrets",
        "EXPLAIN DELETE FROM secrets",
        "EXPLAIN ANALYZE SELECT * FROM secrets",
        "EXPLAIN ANALYZE DELETE FROM secrets",
        "EXPLAIN ANALYZE UPDATE secrets SET ssn = 'x'",
        "EXPLAIN ANALYZE INSERT INTO secrets VALUES (3, 'c')",
    ] {
        let err = run_err(&mut alice, sql);
        assert_eq!(err.sqlstate(), "42501", "{sql}: {err}");
    }
    assert_eq!(count(&mut admin, "secrets"), "2");
    run_ok(&mut alice, "EXPLAIN SELECT 1");
}

#[test]
fn explain_analyze_dml_runs_inside_the_session_transaction() {
    let (_server, mut admin) = secrets_server();

    run_ok(&mut admin, "BEGIN");
    run_ok(&mut admin, "EXPLAIN ANALYZE DELETE FROM secrets");
    assert_eq!(
        count(&mut admin, "secrets"),
        "0",
        "the delete is visible in its txn"
    );
    run_ok(&mut admin, "ROLLBACK");
    assert_eq!(
        count(&mut admin, "secrets"),
        "2",
        "ROLLBACK undoes EXPLAIN ANALYZE"
    );

    run_ok(&mut admin, "BEGIN READ ONLY");
    let err = run_err(&mut admin, "EXPLAIN ANALYZE UPDATE secrets SET ssn = 'z'");
    assert_eq!(err.sqlstate(), "25006", "{err}");
    run_ok(&mut admin, "ROLLBACK");
    let rows = run_ok(&mut admin, "SELECT ssn FROM secrets ORDER BY id").rows;
    assert_eq!(
        rows,
        vec![vec![Some("a".to_owned())], vec![Some("b".to_owned())]]
    );
}

#[test]
fn explain_analyze_dml_applies_row_level_security() {
    let server = Arc::new(Server::with_sample_database());
    let mut admin = session_as(&server, "ultrasql");
    for sql in [
        "CREATE ROLE alice LOGIN",
        "CREATE TABLE docs (tenant text, body text)",
        "INSERT INTO docs VALUES ('a', 'mine'), ('b', 'theirs')",
        "GRANT SELECT, DELETE ON TABLE docs TO alice",
        "ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY tenant_docs ON docs USING (tenant = current_setting('app.tenant', true))",
    ] {
        run_ok(&mut admin, sql);
    }
    let mut alice = session_as(&server, "alice");
    run_ok(&mut alice, "SET app.tenant = 'a'");
    run_ok(&mut alice, "EXPLAIN ANALYZE DELETE FROM docs");
    let rows = run_ok(&mut admin, "SELECT tenant FROM docs").rows;
    assert_eq!(
        rows,
        vec![vec![Some("b".to_owned())]],
        "other tenants' rows stay"
    );
}

#[test]
fn explain_analyze_dml_commit_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp dir");
    {
        let server = Arc::new(Server::init(data_dir.path()).expect("init server"));
        let mut admin = session_as(&server, "ultrasql");
        run_ok(&mut admin, "CREATE TABLE durable (id int, val int)");
        run_ok(&mut admin, "INSERT INTO durable VALUES (1, 1), (2, 2)");
        run_ok(&mut admin, "EXPLAIN ANALYZE DELETE FROM durable");
        assert_eq!(count(&mut admin, "durable"), "0");
    }
    let server = Arc::new(Server::init(data_dir.path()).expect("reopen server"));
    let mut admin = session_as(&server, "ultrasql");
    assert_eq!(
        count(&mut admin, "durable"),
        "0",
        "an acknowledged EXPLAIN ANALYZE DELETE must survive a restart"
    );
}
