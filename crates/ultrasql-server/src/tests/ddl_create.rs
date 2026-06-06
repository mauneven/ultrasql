//! Test sub-module; see `tests/mod.rs` for shared helpers.

use super::*;

#[tokio::test]
async fn create_table_persists_to_catalog_via_wire() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let state_clone = Arc::clone(&state);
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE accounts (id BIGINT NOT NULL, balance FLOAT8)".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;

    // Server emits CommandComplete "CREATE TABLE" then ReadyForQuery 'I'.
    let tag = msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    });
    assert_eq!(tag.as_deref(), Some("CREATE TABLE"));
    assert!(
        !msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::RowDescription { .. })),
        "DDL must not emit RowDescription"
    );
    assert!(matches!(
        msgs.last().unwrap(),
        BackendMessage::ReadyForQuery { status: b'I' }
    ));

    // Catalog observably contains the new relation.
    let snap = state_clone.catalog_snapshot();
    let accounts = snap.tables.get("accounts").expect("accounts persisted");
    assert_eq!(accounts.name, "accounts");
    assert_eq!(accounts.schema_name, "public");
    assert_eq!(accounts.schema.len(), 2);
    assert!(
        !accounts.schema.fields()[0].nullable,
        "NOT NULL constraint applied"
    );

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

#[tokio::test]
async fn create_insert_select_round_trip_through_wire() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    // CREATE TABLE — Int32 columns so the literal `1` / `100`
    // (default Int32 in the binder) types-match without casts.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE items (id INT NOT NULL, val INT)".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    assert!(matches!(
        msgs.last().unwrap(),
        BackendMessage::ReadyForQuery { status: b'I' }
    ));

    // INSERT three rows
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "INSERT INTO items VALUES (1, 100), (2, 200), (3, 300)".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let tag = msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    });
    assert_eq!(
        tag.as_deref(),
        Some("INSERT 0 3"),
        "INSERT must report 3 rows: {msgs:?}"
    );
    // INSERT must not emit a RowDescription.
    assert!(
        !msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::RowDescription { .. })),
        "INSERT must not emit RowDescription"
    );

    // SELECT * — runs SeqScan over the real heap.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "SELECT id, val FROM items".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|m| match m {
            BackendMessage::DataRow { columns } => Some(columns.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 3, "expected 3 rows, got {msgs:?}");
    let select_tag = msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    });
    assert_eq!(select_tag.as_deref(), Some("SELECT 3"));

    // Sanity-check the row contents (text encoding).
    let mut decoded: Vec<(i32, i32)> = rows
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
            (id, val)
        })
        .collect();
    decoded.sort_unstable();
    assert_eq!(decoded, vec![(1, 100), (2, 200), (3, 300)]);

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

#[tokio::test]
async fn create_table_duplicate_rejected_with_query_scoped_error() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));

    complete_startup(&mut client).await;
    // First create succeeds.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE t (id INT)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    // Second create on the same name errors but the session survives.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE t (id INT)".to_string(),
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
        .expect("ErrorResponse on duplicate");
    let sqlstate = err
        .iter()
        .find_map(|(c, v)| (*c == b'C').then(|| v.clone()))
        .expect("SQLSTATE field present");
    assert_eq!(sqlstate, "42P07", "duplicate_table SQLSTATE");
    // Session still healthy.
    assert!(matches!(
        msgs.last().unwrap(),
        BackendMessage::ReadyForQuery { status: b'I' }
    ));

    // Third attempt with IF NOT EXISTS succeeds as a no-op.
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE IF NOT EXISTS t (id INT)".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;
    let tag = msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    });
    assert_eq!(tag.as_deref(), Some("CREATE TABLE"));

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

#[tokio::test]
async fn integration_real_tcp_select_round_trips_rows() {
    // Use port 0 to let the kernel pick an ephemeral port.
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr parses");
    let (listener, bound) = bind_listener(addr).await.expect("bind");
    let state = server();
    let server_handle = tokio::spawn(serve_listener(listener, state));

    let mut stream = tokio::net::TcpStream::connect(bound)
        .await
        .expect("connect");
    complete_startup(&mut stream).await;
    send_frontend(
        &mut stream,
        &FrontendMessage::Query {
            sql: "SELECT id FROM users LIMIT 2".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut stream).await;
    let row_count = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count();
    assert_eq!(row_count, 2);

    send_frontend(&mut stream, &FrontendMessage::Terminate).await;
    drop(stream);
    server_handle.abort();
}

// -----------------------------------------------------------------------
// CREATE INDEX / DROP TABLE / ALTER TABLE — wire dispatch tests
// -----------------------------------------------------------------------

/// Drive `CREATE TABLE`, INSERT a few rows, then issue
/// `CREATE INDEX`. The catalog snapshot must reflect the new
/// index entry and the `IndexEntry`'s columns must match the
/// key column the binder resolved.
#[tokio::test]
async fn create_index_via_wire_registers_index_entry() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let state_clone = Arc::clone(&state);
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    // CREATE TABLE with an Int64 key (matches the v0.5 B-tree key shape).
    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE t (id BIGINT NOT NULL, val INT)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "INSERT INTO t VALUES (10, 1), (20, 2), (30, 3)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE INDEX ix_t_id ON t (id)".to_string(),
        },
    )
    .await;
    let msgs = drain_until_ready(&mut client).await;

    let tag = msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    });
    assert_eq!(tag.as_deref(), Some("CREATE INDEX"));
    assert!(
        !msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::RowDescription { .. })),
        "DDL must not emit RowDescription"
    );

    // Catalog snapshot must contain the new index.
    let snap = state_clone.catalog_snapshot();
    let idx = snap
        .indexes
        .get("ix_t_id")
        .expect("ix_t_id present in snapshot");
    assert_eq!(idx.name, "ix_t_id");
    assert_eq!(idx.columns, vec![0_u16], "indexes id column at attnum 0");
    // The table OID matches the registered table.
    let table = snap.tables.get("t").expect("t present");
    assert_eq!(idx.table_oid, table.oid);

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}

/// `CREATE INDEX IF NOT EXISTS` is a no-op when the index already
/// exists; the second invocation still returns `CREATE INDEX` as
/// the command tag and does not error.
#[tokio::test]
async fn create_index_if_not_exists_is_idempotent() {
    let (mut client, server_side) = tokio::io::duplex(8192);
    let state = Arc::new(Server::with_sample_database());
    let handle = tokio::spawn(handle_connection(server_side, state));
    complete_startup(&mut client).await;

    send_frontend(
        &mut client,
        &FrontendMessage::Query {
            sql: "CREATE TABLE t (id BIGINT NOT NULL)".to_string(),
        },
    )
    .await;
    let _ = drain_until_ready(&mut client).await;

    for _ in 0..2 {
        send_frontend(
            &mut client,
            &FrontendMessage::Query {
                sql: "CREATE INDEX IF NOT EXISTS ix_t_id ON t (id)".to_string(),
            },
        )
        .await;
        let msgs = drain_until_ready(&mut client).await;
        let tag = msgs.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        });
        assert_eq!(tag.as_deref(), Some("CREATE INDEX"));
    }

    send_frontend(&mut client, &FrontendMessage::Terminate).await;
    drop(client);
    handle.await.expect("task joins").expect("clean exit");
}
