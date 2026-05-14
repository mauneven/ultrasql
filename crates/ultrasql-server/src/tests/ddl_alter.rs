//! Test sub-module; see `tests/mod.rs` for shared helpers.

#![allow(unused_imports)]

use super::*;

/// `DROP TABLE t` makes a subsequent `SELECT * FROM t` fail with a
/// PostgreSQL-style `undefined_table` error (SQLSTATE 42P01). The
/// session continues so the test pattern matches PostgreSQL's
/// behaviour.
#[tokio::test]
async fn drop_table_via_wire_then_select_fails_with_undefined_table() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let state_clone = Arc::clone(&state);
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE t (id INT)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "DROP TABLE t".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let tag = msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    });
    assert_eq!(tag.as_deref(), Some("DROP TABLE"));

    // Catalog snapshot no longer holds the dropped table.
    assert!(!state_clone.catalog_snapshot().tables.contains_key("t"));

    // Subsequent SELECT errors with relation-does-not-exist.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM t".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let err = msgs
        .iter()
        .find_map(|m| match m {
            BackendMessage::ErrorResponse { fields } => Some(fields.clone()),
            _ => None,
        })
        .expect("ErrorResponse on dropped table");
    let sqlstate = err
        .iter()
        .find_map(|(c, v)| (*c == b'C').then(|| v.clone()))
        .expect("SQLSTATE field present");
    assert_eq!(sqlstate, "42P01", "undefined_table SQLSTATE");

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

/// `DROP TABLE IF EXISTS missing` succeeds with the `DROP TABLE`
/// command tag and does not error.
#[tokio::test]
async fn drop_table_if_exists_missing_is_noop() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "DROP TABLE IF EXISTS nothing_here".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let tag = msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    });
    assert_eq!(tag.as_deref(), Some("DROP TABLE"));

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

/// End-to-end `ALTER TABLE t ADD COLUMN c` flow:
///
/// 1. Create a table, insert a row.
/// 2. ALTER ADD COLUMN — relation is rewritten so the pre-existing
///    row's new column reads as NULL.
/// 3. INSERT a new row with a value for the added column.
/// 4. SELECT and verify the pre-existing row reads NULL while the
///    new row reads the inserted value.
#[tokio::test]
async fn alter_table_add_column_via_wire_round_trips() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let state_clone = Arc::clone(&state);
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    // Setup
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE t (id INT NOT NULL, val INT)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "INSERT INTO t VALUES (1, 100)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    // ALTER ADD COLUMN
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "ALTER TABLE t ADD COLUMN c INTEGER".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let tag = msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    });
    assert_eq!(tag.as_deref(), Some("ALTER TABLE"));

    // Catalog snapshot now reflects 3 columns.
    let snap = state_clone.catalog_snapshot();
    let t = snap.tables.get("t").expect("t present");
    assert_eq!(t.schema.len(), 3);
    assert_eq!(t.schema.field_at(2).name, "c");

    // INSERT a new row including the new column.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "INSERT INTO t (id, val, c) VALUES (2, 200, 999)".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let tag = msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    });
    assert_eq!(tag.as_deref(), Some("INSERT 0 1"));

    // SELECT all three columns; verify the pre-existing row reads
    // NULL for `c` and the new row reads 999.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id, val, c FROM t".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let rows: Vec<Vec<Option<Vec<u8>>>> = msgs
        .iter()
        .filter_map(|m| match m {
            BackendMessage::DataRow { columns } => Some(columns.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 2, "expected 2 rows, got {msgs:?}");
    // Normalise into (id, val, c) tuples for assertion.
    let parsed: Vec<(i32, i32, Option<i32>)> = rows
        .iter()
        .map(|cols| {
            let id = std::str::from_utf8(cols[0].as_ref().unwrap())
                .unwrap()
                .parse::<i32>()
                .unwrap();
            let val = std::str::from_utf8(cols[1].as_ref().unwrap())
                .unwrap()
                .parse::<i32>()
                .unwrap();
            let c = cols[2]
                .as_ref()
                .map(|b| std::str::from_utf8(b).unwrap().parse::<i32>().unwrap());
            (id, val, c)
        })
        .collect();
    // Pre-existing row sees NULL for c; new row sees 999.
    assert!(parsed.contains(&(1, 100, None)), "got {parsed:?}");
    assert!(parsed.contains(&(2, 200, Some(999))), "got {parsed:?}");

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

/// `ALTER TABLE ADD COLUMN` on a table that does not exist must
/// fail at the binder layer (`PlanError::TableNotFound`) and
/// surface as a query-scoped error — the session survives.
#[tokio::test]
async fn alter_table_add_column_rejects_missing_relation() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "ALTER TABLE nope ADD COLUMN x INTEGER".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
        "expected ErrorResponse: {msgs:?}"
    );
    assert!(matches!(
        msgs.last().unwrap(),
        BackendMessage::ReadyForQuery { status: b'I' }
    ));

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

/// `TRUNCATE TABLE t` emits `TRUNCATE TABLE` as the command tag,
/// does not emit a `RowDescription`, and the relation is empty as
/// observed by a subsequent `SELECT *`.
#[tokio::test]
async fn truncate_via_wire_empties_relation() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE trunc_unit (id INT NOT NULL)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "INSERT INTO trunc_unit VALUES (1)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "INSERT INTO trunc_unit VALUES (2)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "TRUNCATE TABLE trunc_unit".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let tag = msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    });
    assert_eq!(tag.as_deref(), Some("TRUNCATE TABLE"));
    assert!(
        !msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::RowDescription { .. })),
        "DDL must not emit RowDescription"
    );

    // Post-truncate SELECT returns no DataRow messages.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id FROM trunc_unit".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let data_rows = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count();
    assert_eq!(data_rows, 0, "post-truncate SELECT must emit no DataRow");

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

/// `TRUNCATE TABLE nope` errors with the table-not-found SQLSTATE
/// (42P01) and the session survives — the binder rejects the
/// reference and the wire path surfaces it as a query-scoped
/// error, never tearing the connection.
#[tokio::test]
async fn truncate_rejects_missing_relation() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "TRUNCATE TABLE nope".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert!(
        msgs.iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
        "expected ErrorResponse: {msgs:?}"
    );
    assert!(matches!(
        msgs.last().unwrap(),
        BackendMessage::ReadyForQuery { status: b'I' }
    ));

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}
