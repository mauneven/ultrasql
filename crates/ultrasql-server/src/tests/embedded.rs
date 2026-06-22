use super::*;

#[test]
fn embedded_memory_executes_ddl_dml_and_select_in_one_session() {
    let mut db = EmbeddedDatabase::open_memory();

    let create = db
        .execute("CREATE TABLE embedded_users (id int4, name text)")
        .expect("create table");
    assert_eq!(create.command_tag, "CREATE TABLE");
    assert!(create.columns.is_empty());
    assert!(create.rows.is_empty());

    let insert = db
        .execute("INSERT INTO embedded_users VALUES (1, 'Ada'), (2, 'Grace')")
        .expect("insert rows");
    assert_eq!(insert.command_tag, "INSERT 0 2");
    assert!(insert.columns.is_empty());
    assert!(insert.rows.is_empty());

    let select = db
        .execute("SELECT id, name FROM embedded_users ORDER BY id")
        .expect("select rows");
    assert_eq!(select.command_tag, "SELECT 2");
    assert_eq!(
        select
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "name"]
    );
    assert_eq!(
        select.rows,
        vec![
            vec![Some("1".to_owned()), Some("Ada".to_owned())],
            vec![Some("2".to_owned()), Some("Grace".to_owned())],
        ]
    );
}

/// BUG 2 — the embedded API materialises the whole result and cannot drive
/// a streaming handle. A large (>256 KiB) SELECT must return ALL its rows
/// with a correct `command_tag` and must NOT leak the autocommit XID.
///
/// Fails before the streaming-gating fix: `execute_embedded_query` →
/// `execute_query(allow_streaming: true)` produced a streaming handle that
/// `local_result_messages` ignored, so only window-0 rows came back with an
/// EMPTY command_tag (no CommandComplete decoded) and the dropped handle
/// left the XID InProgress forever.
#[test]
fn embedded_large_select_returns_all_rows_with_tag_and_leaks_no_xid() {
    // Build the session directly (mirroring `EmbeddedDatabase::from_server`)
    // so we keep a handle to the `Server` and can inspect the txn manager.
    let state = Arc::new(Server::with_empty_database());
    let (io, _peer) = tokio::io::duplex(1);
    let mut session = Session::new(io, Arc::clone(&state), None);

    session
        .execute_embedded_query("CREATE TABLE embedded_big (id int4, payload text)")
        .expect("create table");

    // Seed enough wide rows that a full-table SELECT body comfortably
    // exceeds the 256 KiB streaming window high-water (~80 bytes/row).
    let rows = 6_000usize;
    let payload = "x".repeat(64);
    let chunk = 500;
    let mut start = 0usize;
    while start < rows {
        let end = (start + chunk).min(rows);
        let mut values = String::new();
        for i in start..end {
            if i > start {
                values.push(',');
            }
            values.push_str(&format!("({i}, '{payload}')"));
        }
        session
            .execute_embedded_query(&format!(
                "INSERT INTO embedded_big (id, payload) VALUES {values}"
            ))
            .expect("insert chunk");
        start = end;
    }

    let select = session
        .execute_embedded_query("SELECT id, payload FROM embedded_big")
        .expect("select all rows");

    // ALL rows materialised, with a correct (non-empty) command_tag.
    assert_eq!(
        select.rows.len(),
        rows,
        "embedded large SELECT returned {} of {rows} rows (only window 0?)",
        select.rows.len()
    );
    assert_eq!(
        select.command_tag,
        format!("SELECT {rows}"),
        "embedded large SELECT lost its command_tag"
    );

    // No leaked XID: the autocommit txn for the SELECT is terminal.
    assert_eq!(
        state.txn_manager.oldest_in_progress(),
        state.txn_manager.next_xid(),
        "embedded large SELECT leaked an in-progress XID"
    );
}
