//! `COPY` must enforce table privileges and row-level security on the Simple
//! Query wire path, like the SELECT/INSERT it stands in for.

use super::*;

/// A client connection that has finished the startup handshake as `user`.
struct Client {
    io: tokio::io::DuplexStream,
    handle: tokio::task::JoinHandle<Result<(), ServerError>>,
}

async fn connect(state: &Arc<Server>, user: &str) -> Client {
    let (mut io, server_side) = tokio::io::duplex(64 * 1024);
    let handle = tokio::spawn(handle_connection(server_side, Arc::clone(state)));
    send_frontend(
        &mut io,
        &FrontendMessage::StartupMessage {
            protocol_major: 3,
            protocol_minor: 0,
            params: vec![("user".to_owned(), user.to_owned())],
        },
    )
    .await;
    let msgs = drain_until_ready(&mut io).await;
    assert!(
        matches!(msgs.last(), Some(BackendMessage::ReadyForQuery { .. })),
        "startup as {user} failed: {msgs:?}"
    );
    Client { io, handle }
}

impl Client {
    /// Send one Simple Query. A `COPY ... FROM STDIN` that the server accepts
    /// is fed `copy_in` and finished, so a missing check cannot hang the test.
    async fn query(&mut self, sql: &str, copy_in: &[u8]) -> Vec<BackendMessage> {
        send_frontend(
            &mut self.io,
            &FrontendMessage::Query {
                sql: sql.to_owned(),
            },
        )
        .await;
        let mut buf = BytesMut::with_capacity(4096);
        let mut out = Vec::new();
        let mut tmp = [0_u8; 4096];
        loop {
            while let Some(msg) = ultrasql_protocol::decode_backend(&mut buf).expect("decode") {
                let ready = matches!(msg, BackendMessage::ReadyForQuery { .. });
                let copy_in_started = matches!(msg, BackendMessage::CopyInResponse { .. });
                out.push(msg);
                if ready {
                    return out;
                }
                if copy_in_started {
                    send_frontend(&mut self.io, &FrontendMessage::CopyData(copy_in.to_vec())).await;
                    send_frontend(&mut self.io, &FrontendMessage::CopyDone).await;
                }
            }
            let n = self.io.read(&mut tmp).await.expect("read");
            assert!(n > 0, "connection closed during {sql}: {out:?}");
            buf.extend_from_slice(&tmp[..n]);
        }
    }

    async fn ok(&mut self, sql: &str) -> Vec<BackendMessage> {
        let msgs = self.query(sql, b"").await;
        assert_eq!(sqlstate(&msgs), None, "{sql}: {msgs:?}");
        msgs
    }

    async fn close(mut self) {
        send_frontend(&mut self.io, &FrontendMessage::Terminate).await;
        drop(self.io);
        self.handle.await.expect("task joins").expect("clean exit");
    }
}

fn sqlstate(msgs: &[BackendMessage]) -> Option<String> {
    msgs.iter().find_map(|msg| match msg {
        BackendMessage::ErrorResponse { fields } => fields
            .iter()
            .find(|(code, _)| *code == b'C')
            .map(|(_, value)| value.clone()),
        _ => None,
    })
}

fn copy_out_text(msgs: &[BackendMessage]) -> String {
    let bytes: Vec<u8> = msgs
        .iter()
        .filter_map(|msg| match msg {
            BackendMessage::CopyData(data) => Some(data.as_slice()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect();
    String::from_utf8(bytes).expect("utf8 copy data")
}

fn single_value(msgs: &[BackendMessage]) -> String {
    msgs.iter()
        .find_map(|msg| match msg {
            BackendMessage::DataRow { columns } => columns
                .first()
                .cloned()
                .flatten()
                .map(|bytes| String::from_utf8(bytes).expect("utf8 value")),
            _ => None,
        })
        .expect("a data row")
}

#[tokio::test]
async fn copy_requires_table_privileges() {
    let state = server();
    let mut admin = connect(&state, "ultrasql").await;
    for sql in [
        "CREATE ROLE alice LOGIN",
        "CREATE TABLE secrets (id int, ssn text)",
        "INSERT INTO secrets VALUES (1, '123-45-6789')",
    ] {
        admin.ok(sql).await;
    }

    let mut alice = connect(&state, "alice").await;
    for sql in [
        "COPY secrets TO STDOUT",
        "COPY secrets (ssn) TO STDOUT",
        "COPY (SELECT ssn FROM secrets) TO STDOUT",
    ] {
        let msgs = alice.query(sql, b"").await;
        assert_eq!(sqlstate(&msgs).as_deref(), Some("42501"), "{sql}: {msgs:?}");
        assert_eq!(copy_out_text(&msgs), "", "{sql} leaked rows");
    }
    let msgs = alice.query("COPY secrets FROM STDIN", b"2\tforged\n").await;
    assert_eq!(sqlstate(&msgs).as_deref(), Some("42501"), "{msgs:?}");
    let msgs = admin.ok("SELECT count(*) FROM secrets").await;
    assert_eq!(
        single_value(&msgs),
        "1",
        "COPY FROM without INSERT wrote a row"
    );

    admin
        .ok("GRANT SELECT, INSERT ON TABLE secrets TO alice")
        .await;
    let msgs = alice.ok("COPY secrets TO STDOUT").await;
    assert_eq!(copy_out_text(&msgs), "1\t123-45-6789\n");
    alice
        .query("COPY secrets FROM STDIN", b"2\tgranted\n")
        .await;
    let msgs = admin.ok("SELECT count(*) FROM secrets").await;
    assert_eq!(single_value(&msgs), "2", "granted COPY FROM still works");

    alice.close().await;
    admin.close().await;
}

#[tokio::test]
async fn copy_applies_row_level_security() {
    let state = server();
    let mut admin = connect(&state, "ultrasql").await;
    for sql in [
        "CREATE ROLE alice LOGIN",
        "CREATE TABLE docs (tenant text, body text)",
        "INSERT INTO docs VALUES ('a', 'mine'), ('b', 'theirs')",
        "GRANT SELECT, INSERT ON TABLE docs TO alice",
        "ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY tenant_docs ON docs \
         USING (tenant = current_setting('app.tenant', true)) \
         WITH CHECK (tenant = current_setting('app.tenant', true))",
    ] {
        admin.ok(sql).await;
    }

    let mut alice = connect(&state, "alice").await;
    alice.ok("SET app.tenant = 'a'").await;
    let msgs = alice.ok("COPY docs TO STDOUT").await;
    assert_eq!(copy_out_text(&msgs), "a\tmine\n");
    let msgs = alice.ok("COPY docs (body) TO STDOUT").await;
    assert_eq!(copy_out_text(&msgs), "mine\n");
    let msgs = alice.ok("COPY (SELECT body FROM docs) TO STDOUT").await;
    assert_eq!(copy_out_text(&msgs), "mine\n");

    let msgs = alice.query("COPY docs FROM STDIN", b"b\tforged\n").await;
    assert_eq!(sqlstate(&msgs).as_deref(), Some("0A000"), "{msgs:?}");
    let msgs = admin.ok("SELECT count(*) FROM docs").await;
    assert_eq!(single_value(&msgs), "2");

    let msgs = admin.ok("COPY docs TO STDOUT").await;
    assert_eq!(
        copy_out_text(&msgs),
        "a\tmine\nb\ttheirs\n",
        "the owner bypasses RLS"
    );

    alice.close().await;
    admin.close().await;
}
