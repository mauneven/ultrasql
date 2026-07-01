//! Round-trip coverage for server-side cursors (`DECLARE` / `FETCH` /
//! `CLOSE`): forward-only, `WITHOUT HOLD`, materialized at `DECLARE`
//! time inside an explicit transaction block.
//!
//! The tests drive the simple-query protocol (`simple_query`) exactly
//! like psql does, asserting consecutive `FETCH` windows, the drained
//! tail, PostgreSQL's cursor SQLSTATEs (`25P01`, `34000`, `42P03`),
//! the `0A000` rejections for the unimplemented forms (`WITH HOLD`,
//! `SCROLL`, `BINARY`, backward fetch, `MOVE`), and cursor lifetime
//! (gone after `COMMIT` / `ROLLBACK`).

pub mod support;

use support::{shutdown, start_persistent_server};
use tokio_postgres::SimpleQueryMessage;

/// SQLSTATE of a wire error.
fn sqlstate(err: &tokio_postgres::Error) -> String {
    err.code()
        .map_or_else(String::new, |c| c.code().to_string())
}

/// Collect the first-column values of the data rows in a simple-query
/// reply, plus the trailing command tag.
fn rows_and_tag(messages: &[SimpleQueryMessage]) -> (Vec<String>, String) {
    let mut rows = Vec::new();
    let mut tag = String::new();
    for msg in messages {
        match msg {
            SimpleQueryMessage::Row(row) => {
                rows.push(row.get(0).unwrap_or("").to_owned());
            }
            SimpleQueryMessage::CommandComplete(n) => tag = n.to_string(),
            _ => {}
        }
    }
    (rows, tag)
}

async fn setup_numbers_table(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE cur_nums (id INT NOT NULL)")
        .await
        .expect("create table");
    client
        .batch_execute(
            "INSERT INTO cur_nums VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)",
        )
        .await
        .expect("seed rows");
}

/// The core loop: consecutive FETCH windows return consecutive rows,
/// FETCH ALL drains the remainder, further FETCHes return zero rows,
/// CLOSE drops the cursor, and FETCH-after-CLOSE is 34000.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_windows_drain_and_close() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "cursor_windows").await;
    let client = &running.client;
    setup_numbers_table(client).await;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DECLARE c CURSOR WITHOUT HOLD FOR SELECT id FROM cur_nums ORDER BY id")
        .await
        .expect("declare");

    // Two consecutive 5-row windows. The command tag is `FETCH n`;
    // tokio_postgres surfaces the trailing count.
    let (rows, tag) = rows_and_tag(
        &client
            .simple_query("FETCH 5 FROM c")
            .await
            .expect("fetch 1"),
    );
    assert_eq!(rows, ["1", "2", "3", "4", "5"]);
    assert_eq!(tag, "5", "FETCH 5 command tag row count");
    let (rows, _) = rows_and_tag(
        &client
            .simple_query("FETCH FORWARD 5 IN c")
            .await
            .expect("fetch 2"),
    );
    assert_eq!(rows, ["6", "7", "8", "9", "10"]);

    // FETCH ALL drains the rest; a further FETCH returns zero rows
    // (not an error), matching PostgreSQL.
    let (rows, tag) = rows_and_tag(&client.simple_query("FETCH ALL FROM c").await.expect("all"));
    assert_eq!(rows, ["11", "12"]);
    assert_eq!(tag, "2", "FETCH ALL reports the drained row count");
    let (rows, _) = rows_and_tag(&client.simple_query("FETCH 5 FROM c").await.expect("empty"));
    assert!(rows.is_empty(), "drained cursor returns no rows");

    // CLOSE drops it; FETCH afterwards is invalid_cursor_name.
    client.batch_execute("CLOSE c").await.expect("close");
    let err = client
        .simple_query("FETCH 1 FROM c")
        .await
        .expect_err("fetch after close");
    assert_eq!(sqlstate(&err), "34000", "invalid_cursor_name: {err}");

    // The 34000 failed the block; ROLLBACK recovers the session.
    client.batch_execute("ROLLBACK").await.expect("rollback");
    shutdown(running).await;
}

/// Bare `FETCH c` and `FETCH NEXT FROM c` are single-row NEXT fetches;
/// `FETCH 0` returns zero rows without erroring or advancing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_and_zero_row_fetch_forms() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "cursor_next").await;
    let client = &running.client;
    setup_numbers_table(client).await;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DECLARE nc NO SCROLL CURSOR FOR SELECT id FROM cur_nums ORDER BY id")
        .await
        .expect("declare");

    let (rows, _) = rows_and_tag(&client.simple_query("FETCH nc").await.expect("bare fetch"));
    assert_eq!(rows, ["1"]);
    let (rows, _) = rows_and_tag(
        &client
            .simple_query("FETCH NEXT FROM nc")
            .await
            .expect("next"),
    );
    assert_eq!(rows, ["2"]);
    let (rows, _) = rows_and_tag(&client.simple_query("FETCH 0 FROM nc").await.expect("zero"));
    assert!(rows.is_empty(), "FETCH 0 returns no rows");
    let (rows, _) = rows_and_tag(
        &client
            .simple_query("FETCH 1 FROM nc")
            .await
            .expect("after 0"),
    );
    assert_eq!(rows, ["3"], "FETCH 0 did not advance the cursor");

    client.batch_execute("COMMIT").await.expect("commit");
    shutdown(running).await;
}

/// DECLARE outside a transaction block is PostgreSQL's 25P01.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declare_outside_transaction_block_is_25p01() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "cursor_no_txn").await;
    let client = &running.client;

    let err = client
        .batch_execute("DECLARE c CURSOR FOR SELECT 1")
        .await
        .expect_err("declare outside txn");
    assert_eq!(sqlstate(&err), "25P01", "no_active_sql_transaction: {err}");
    let db = err.as_db_error().expect("db error");
    assert_eq!(
        db.message(),
        "DECLARE CURSOR can only be used in transaction blocks"
    );

    // The session is untouched (no block to fail).
    client.batch_execute("SELECT 1").await.expect("session ok");
    shutdown(running).await;
}

/// The unimplemented cursor forms are honest 0A000 rejections with a
/// hint in the structured HINT field, and each failure aborts the
/// surrounding block like any in-transaction error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_cursor_forms_reject_0a000_with_hint() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "cursor_0a000").await;
    let client = &running.client;
    setup_numbers_table(client).await;

    for (sql, needle) in [
        ("DECLARE h CURSOR WITH HOLD FOR SELECT 1", "WITHOUT HOLD"),
        ("DECLARE s SCROLL CURSOR FOR SELECT 1", "forward-only"),
        ("DECLARE b BINARY CURSOR FOR SELECT 1", "text format"),
    ] {
        client.batch_execute("BEGIN").await.expect("begin");
        let err = client.batch_execute(sql).await.expect_err(sql);
        assert_eq!(sqlstate(&err), "0A000", "{sql}: {err}");
        let db = err.as_db_error().expect("db error");
        let hint = db.hint().unwrap_or_else(|| panic!("{sql}: hint expected"));
        assert!(
            hint.contains(needle),
            "{sql}: hint {hint:?} names the supported alternative"
        );
        client.batch_execute("ROLLBACK").await.expect("rollback");
    }

    // Backward fetch and MOVE on a live cursor.
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DECLARE c CURSOR FOR SELECT id FROM cur_nums ORDER BY id")
        .await
        .expect("declare");
    for sql in [
        "FETCH BACKWARD 2 FROM c",
        "FETCH PRIOR FROM c",
        "FETCH -1 FROM c",
    ] {
        let err = client.simple_query(sql).await.expect_err(sql);
        assert_eq!(sqlstate(&err), "0A000", "{sql}: {err}");
        // The 0A000 aborted the block; start a fresh one for the next form.
        client
            .batch_execute("ROLLBACK; BEGIN")
            .await
            .expect("reset");
        client
            .batch_execute("DECLARE c CURSOR FOR SELECT id FROM cur_nums ORDER BY id")
            .await
            .expect("redeclare");
    }
    let err = client
        .simple_query("MOVE FORWARD 2 FROM c")
        .await
        .expect_err("MOVE");
    assert_eq!(sqlstate(&err), "0A000", "MOVE: {err}");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    shutdown(running).await;
}

/// A second DECLARE under the same name is 42P03 duplicate_cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_declare_is_42p03() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "cursor_dup").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DECLARE dup CURSOR FOR SELECT 1")
        .await
        .expect("declare");
    let err = client
        .batch_execute("DECLARE dup CURSOR FOR SELECT 2")
        .await
        .expect_err("duplicate declare");
    assert_eq!(sqlstate(&err), "42P03", "duplicate_cursor: {err}");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    shutdown(running).await;
}

/// Cursors are WITHOUT HOLD: COMMIT and ROLLBACK both close them, and
/// a FETCH in the next transaction is invalid_cursor_name. CLOSE ALL
/// clears every open cursor at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursors_vanish_at_transaction_end() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "cursor_txn_end").await;
    let client = &running.client;
    setup_numbers_table(client).await;

    for end in ["COMMIT", "ROLLBACK"] {
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute("DECLARE tc CURSOR FOR SELECT id FROM cur_nums ORDER BY id")
            .await
            .expect("declare");
        let (rows, _) = rows_and_tag(&client.simple_query("FETCH 1 FROM tc").await.expect("fetch"));
        assert_eq!(rows, ["1"]);
        client.batch_execute(end).await.expect(end);

        client.batch_execute("BEGIN").await.expect("begin 2");
        let err = client
            .simple_query("FETCH 1 FROM tc")
            .await
            .expect_err("fetch after txn end");
        assert_eq!(sqlstate(&err), "34000", "after {end}: {err}");
        client.batch_execute("ROLLBACK").await.expect("recover");
    }

    // CLOSE ALL closes every cursor in one statement.
    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DECLARE a1 CURSOR FOR SELECT 1")
        .await
        .expect("declare a1");
    client
        .batch_execute("DECLARE a2 CURSOR FOR SELECT 2")
        .await
        .expect("declare a2");
    client.batch_execute("CLOSE ALL").await.expect("close all");
    let err = client
        .simple_query("FETCH 1 FROM a1")
        .await
        .expect_err("a1 closed");
    assert_eq!(sqlstate(&err), "34000");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    shutdown(running).await;
}

/// Cursor statements are a simple-query surface: routing them through
/// an extended-protocol prepared statement is rejected cleanly (0A000
/// at bind time) rather than hanging or corrupting the session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_protocol_cursor_statements_reject_cleanly() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "cursor_extended").await;
    let client = &running.client;

    client.batch_execute("BEGIN").await.expect("begin");
    // `query` drives Parse/Bind/Execute — the extended path.
    let err = client
        .query("DECLARE ec CURSOR FOR SELECT 1", &[])
        .await
        .expect_err("extended DECLARE");
    assert_eq!(sqlstate(&err), "0A000", "extended DECLARE: {err}");
    client.batch_execute("ROLLBACK").await.expect("rollback");
    client.batch_execute("SELECT 1").await.expect("session ok");

    shutdown(running).await;
}

/// A statement error inside the block leaves it Failed; FETCH is then
/// rejected with 25P02 like any other statement until ROLLBACK.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_in_failed_transaction_is_25p02() {
    let data_dir = tempfile::TempDir::new().unwrap();
    let running = start_persistent_server(data_dir.path(), "cursor_failed_txn").await;
    let client = &running.client;
    setup_numbers_table(client).await;

    client.batch_execute("BEGIN").await.expect("begin");
    client
        .batch_execute("DECLARE fc CURSOR FOR SELECT id FROM cur_nums ORDER BY id")
        .await
        .expect("declare");
    let _ = client
        .batch_execute("SELECT no_such_column FROM cur_nums")
        .await
        .expect_err("failing statement");
    let err = client
        .simple_query("FETCH 1 FROM fc")
        .await
        .expect_err("fetch in failed block");
    assert_eq!(sqlstate(&err), "25P02", "in_failed_sql_transaction: {err}");
    client.batch_execute("ROLLBACK").await.expect("rollback");

    shutdown(running).await;
}
