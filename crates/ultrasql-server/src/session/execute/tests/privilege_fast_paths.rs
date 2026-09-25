//! Privilege, row-security, and catalog-staleness checks on the Simple Query
//! fast paths and per-session caches.

use super::*;

type TestSession = Session<tokio::io::DuplexStream>;

fn shared_server() -> Arc<Server> {
    Arc::new(Server::with_sample_database())
}

fn session_as(server: &Arc<Server>, user: &str) -> TestSession {
    let (io, _peer) = duplex(64);
    let mut session = Session::new(io, Arc::clone(server), None);
    user.clone_into(&mut session.auth_user);
    user.clone_into(&mut session.current_user);
    session
}

fn run_ok(session: &mut TestSession, sql: &str) -> SelectResult {
    session
        .execute_query(sql, false)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

fn run_err(session: &mut TestSession, sql: &str) -> ServerError {
    match session.execute_query(sql, false) {
        Ok(result) => panic!("{sql}: expected an error, got {:?}", command_tag(result)),
        Err(err) => err,
    }
}

fn command_tag(result: SelectResult) -> String {
    crate::local_output_from_select_result(result)
        .expect("decode result")
        .command_tag
}

fn count(session: &mut TestSession, sql: &str) -> String {
    let output =
        crate::local_output_from_select_result(run_ok(session, sql)).expect("decode result");
    output
        .rows
        .first()
        .and_then(|row| row.first().cloned().flatten())
        .unwrap_or_else(|| panic!("{sql}: no value in {:?}", output.rows))
}

fn assert_insufficient_privilege(err: &ServerError, context: &str) {
    assert_eq!(err.sqlstate(), "42501", "{context}: {err}");
}

/// Superuser session plus a registered `alice` login with no grants.
fn server_with_alice() -> (Arc<Server>, TestSession) {
    let server = shared_server();
    let mut admin = session_as(&server, "ultrasql");
    run_ok(&mut admin, "CREATE ROLE alice LOGIN");
    (server, admin)
}

#[test]
fn fast_insert_values_requires_insert_privilege() {
    let (server, mut admin) = server_with_alice();
    run_ok(&mut admin, "CREATE TABLE pairs (id int, val int)");

    let mut alice = session_as(&server, "alice");
    let err = run_err(&mut alice, "INSERT INTO pairs VALUES (1, 2)");
    assert_insufficient_privilege(&err, "autocommit fast INSERT without INSERT");

    run_ok(&mut alice, "BEGIN");
    let err = run_err(&mut alice, "INSERT INTO pairs VALUES (3, 4)");
    assert_insufficient_privilege(&err, "in-transaction fast INSERT without INSERT");
    run_ok(&mut alice, "ROLLBACK");

    run_ok(&mut admin, "SET ROLE alice");
    let err = run_err(&mut admin, "INSERT INTO pairs VALUES (5, 6)");
    assert_insufficient_privilege(&err, "fast INSERT after SET ROLE");
    run_ok(&mut admin, "RESET ROLE");
    assert_eq!(count(&mut admin, "SELECT count(*) FROM pairs"), "0");

    run_ok(&mut admin, "GRANT INSERT ON TABLE pairs TO alice");
    let snapshot = server.catalog_snapshot();
    let granted = alice
        .try_execute_fast_insert_int32_pair_sql("INSERT INTO pairs VALUES (7, 8)", &snapshot)
        .expect("granted fast INSERT succeeds");
    assert!(
        granted.is_some(),
        "a granted role still takes the fast path"
    );
    assert_eq!(count(&mut admin, "SELECT count(*) FROM pairs"), "1");
}

#[test]
fn fast_insert_values_resolves_the_table_through_search_path() {
    let server = shared_server();
    let mut admin = session_as(&server, "ultrasql");
    run_ok(&mut admin, "CREATE SCHEMA app");
    run_ok(&mut admin, "CREATE TABLE t (id int, val int)");
    run_ok(&mut admin, "CREATE TABLE app.t (id int, val int)");
    run_ok(&mut admin, "SET search_path = app, public");

    let snapshot = server.catalog_snapshot();
    let fast = admin
        .try_execute_fast_insert_int32_pair_sql("INSERT INTO t VALUES (1, 1)", &snapshot)
        .expect("fast INSERT succeeds");
    assert!(
        fast.is_some(),
        "search_path INSERT still takes the fast path"
    );

    assert_eq!(count(&mut admin, "SELECT count(*) FROM app.t"), "1");
    assert_eq!(count(&mut admin, "SELECT count(*) FROM public.t"), "0");
}

#[test]
fn fast_insert_values_participates_in_serializable_conflict_detection() {
    let server = shared_server();
    let mut admin = session_as(&server, "ultrasql");
    run_ok(&mut admin, "CREATE TABLE ssi_pairs (id int, val int)");

    let mut s1 = session_as(&server, "ultrasql");
    let mut s2 = session_as(&server, "ultrasql");
    run_ok(&mut s1, "BEGIN ISOLATION LEVEL SERIALIZABLE");
    assert_eq!(
        count(&mut s1, "SELECT count(*) FROM ssi_pairs WHERE id = 5"),
        "0"
    );
    run_ok(&mut s2, "BEGIN ISOLATION LEVEL SERIALIZABLE");
    assert_eq!(
        count(&mut s2, "SELECT count(*) FROM ssi_pairs WHERE id = 5"),
        "0"
    );
    let first = s1.execute_query("INSERT INTO ssi_pairs VALUES (5, 1)", false);
    let second = s2.execute_query("INSERT INTO ssi_pairs VALUES (5, 2)", false);
    let first_commit = first.and_then(|_| s1.execute_query("COMMIT", false));
    let second_commit = second.and_then(|_| s2.execute_query("COMMIT", false));

    let failures = [&first_commit, &second_commit]
        .iter()
        .filter(|outcome| outcome.as_ref().is_err_and(|err| err.sqlstate() == "40001"))
        .count();
    assert_eq!(
        failures, 1,
        "write skew must abort exactly one transaction: {first_commit:?} / {second_commit:?}"
    );
    assert_eq!(count(&mut admin, "SELECT count(*) FROM ssi_pairs"), "1");
}

/// Drop the per-table owner/RLS runtime entries so the fused-DELETE precheck
/// cache is admissible (it is only used while no table carries RLS metadata).
fn clear_row_security_metadata(server: &Server) {
    server.row_security.clear();
}

fn prime_cached_delete(session: &mut TestSession, sql: &str) {
    run_ok(session, sql);
    run_ok(session, sql);
    assert!(
        !session.prechecked_fast_dml.borrow().is_empty(),
        "the second run must populate the fused-DELETE precheck cache"
    );
}

#[test]
fn cached_delete_rechecks_privileges_after_revoke_in_another_session() {
    let (server, mut admin) = server_with_alice();
    run_ok(&mut admin, "CREATE TABLE pairs (a int, b int)");
    run_ok(&mut admin, "GRANT SELECT, DELETE ON TABLE pairs TO alice");
    clear_row_security_metadata(&server);

    let mut alice = session_as(&server, "alice");
    let delete = "DELETE FROM pairs WHERE a = 1000";
    prime_cached_delete(&mut alice, delete);

    run_ok(&mut admin, "REVOKE DELETE ON TABLE pairs FROM alice");
    run_ok(&mut admin, "INSERT INTO pairs VALUES (1000, 1)");
    let err = run_err(&mut alice, delete);
    assert_insufficient_privilege(&err, "cached DELETE after REVOKE");
    assert_eq!(count(&mut admin, "SELECT count(*) FROM pairs"), "1");
}

#[test]
fn cached_delete_applies_row_security_enabled_by_another_session() {
    let (server, mut admin) = server_with_alice();
    run_ok(&mut admin, "CREATE TABLE pairs (a int, b int)");
    run_ok(&mut admin, "GRANT SELECT, DELETE ON TABLE pairs TO alice");
    clear_row_security_metadata(&server);

    let mut alice = session_as(&server, "alice");
    let delete = "DELETE FROM pairs WHERE a >= 0";
    prime_cached_delete(&mut alice, delete);

    run_ok(&mut admin, "ALTER TABLE pairs ENABLE ROW LEVEL SECURITY");
    run_ok(&mut admin, "INSERT INTO pairs VALUES (1000, 1), (1001, 2)");
    let result = run_ok(&mut alice, delete);
    assert_eq!(
        command_tag(result),
        "DELETE 0",
        "RLS with no policy hides every row from a non-owner"
    );
    assert_eq!(count(&mut admin, "SELECT count(*) FROM pairs"), "2");
}

#[test]
fn cached_select_rebinds_after_drop_column_in_another_session() {
    let server = shared_server();
    let mut admin = session_as(&server, "ultrasql");
    run_ok(&mut admin, "CREATE TABLE wide (a int, b int, c int)");
    run_ok(&mut admin, "INSERT INTO wide VALUES (1, 2, 3)");

    let mut reader = session_as(&server, "ultrasql");
    assert_eq!(count(&mut reader, "SELECT b FROM wide"), "2");
    assert_eq!(count(&mut reader, "SELECT b FROM wide"), "2");

    run_ok(&mut admin, "ALTER TABLE wide DROP COLUMN a");
    assert_eq!(count(&mut reader, "SELECT b FROM wide"), "2");
}

#[test]
fn cached_update_rebinds_after_drop_column_in_another_session() {
    let server = shared_server();
    let mut admin = session_as(&server, "ultrasql");
    run_ok(&mut admin, "CREATE TABLE wide (a int, b int, c int)");
    run_ok(&mut admin, "INSERT INTO wide VALUES (1, 5, 50)");

    let mut writer = session_as(&server, "ultrasql");
    let update = "UPDATE wide SET b = b + 1 WHERE a = 5";
    run_ok(&mut writer, update);
    run_ok(&mut writer, update);

    run_ok(&mut admin, "ALTER TABLE wide DROP COLUMN a");
    let err = run_err(&mut writer, update);
    assert_eq!(
        err.sqlstate(),
        "42703",
        "stale UPDATE must re-bind and fail on the dropped column: {err}"
    );
    assert_eq!(count(&mut admin, "SELECT c FROM wide"), "50");
}
