//! End-to-end `BIT` / `VARBIT` storage, operators, COPY, and wire metadata.

mod support;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

fn simple_rows(messages: Vec<SimpleQueryMessage>) -> Vec<Vec<String>> {
    messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|idx| row.get(idx).expect("column").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn copy_in_payload(client: &tokio_postgres::Client, sql: &str, payload: &[u8]) -> u64 {
    let sink = client
        .copy_in::<_, Bytes>(sql)
        .await
        .expect("copy in starts");
    futures::pin_mut!(sink);
    sink.as_mut()
        .send(Bytes::from(payload.to_vec()))
        .await
        .expect("copy bytes sent");
    sink.finish().await.expect("copy finishes")
}

async fn collect_copy_out(stream: tokio_postgres::CopyOutStream) -> Vec<u8> {
    let mut stream = Box::pin(stream);
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.expect("copy chunk"));
    }
    out
}

#[tokio::test]
async fn bit_and_varbit_storage_ops_and_wire_round_trip() {
    let running = start_sample_server("bit_varbit_round_trip").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE bit_probe (\
                id INT, \
                fixed BIT(4), \
                flex VARBIT(6), \
                alias BIT VARYING(5)\
            )",
        )
        .await
        .expect("create bit table");

    client
        .batch_execute(
            "INSERT INTO bit_probe VALUES \
             (1, B'1010', B'001', B'11111'), \
             (2, B'10'::BIT(4), B'1010101'::VARBIT(6), B'1'::BIT VARYING(5))",
        )
        .await
        .expect("insert bit values");

    let values = simple_rows(
        client
            .simple_query("SELECT fixed, flex, alias FROM bit_probe ORDER BY id")
            .await
            .expect("select bit values"),
    );
    assert_eq!(
        values,
        vec![
            vec!["1010".to_owned(), "001".to_owned(), "11111".to_owned()],
            vec!["1000".to_owned(), "101010".to_owned(), "1".to_owned()],
        ]
    );

    let ops = simple_rows(
        client
            .simple_query(
                "SELECT \
                    fixed & B'0011', \
                    fixed | B'0101', \
                    fixed # B'1111', \
                    ~fixed, \
                    fixed << 1, \
                    fixed >> 2, \
                    bit_count(fixed), \
                    bit_length(fixed), \
                    length(fixed), \
                    octet_length(fixed), \
                    get_bit(fixed, 0), \
                    set_bit(fixed, 1, 1) \
                 FROM bit_probe WHERE id = 1",
            )
            .await
            .expect("select bit ops"),
    );
    assert_eq!(
        ops,
        vec![vec![
            "0010".to_owned(),
            "1111".to_owned(),
            "0101".to_owned(),
            "0101".to_owned(),
            "0100".to_owned(),
            "0010".to_owned(),
            "2".to_owned(),
            "4".to_owned(),
            "4".to_owned(),
            "1".to_owned(),
            "1".to_owned(),
            "1110".to_owned(),
        ]]
    );

    let stmt = client
        .prepare("SELECT fixed, flex, alias FROM bit_probe")
        .await
        .expect("prepare bit select");
    let oids: Vec<u32> = stmt
        .columns()
        .iter()
        .map(|column| column.type_().oid())
        .collect();
    assert_eq!(oids, vec![1560, 1562, 1562]);

    let short = client
        .batch_execute("INSERT INTO bit_probe VALUES (3, B'10', B'0', B'0')")
        .await
        .expect_err("short BIT(n) insert must fail");
    assert_eq!(
        short.code().map(tokio_postgres::error::SqlState::code),
        Some("22001")
    );

    let long = client
        .batch_execute("INSERT INTO bit_probe VALUES (3, B'1010', B'1010101', B'0')")
        .await
        .expect_err("overlength VARBIT(n) insert must fail");
    assert_eq!(
        long.code().map(tokio_postgres::error::SqlState::code),
        Some("22001")
    );

    client
        .batch_execute("CREATE TABLE bit_copy (id INT, fixed BIT(4), flex VARBIT(6))")
        .await
        .expect("create bit copy table");
    let copied = copy_in_payload(client, "COPY bit_copy FROM STDIN", b"1\t1010\t001\n").await;
    assert_eq!(copied, 1);
    let out = collect_copy_out(
        client
            .copy_out("COPY bit_copy TO STDOUT")
            .await
            .expect("copy bit out"),
    )
    .await;
    assert_eq!(out, b"1\t1010\t001\n");

    shutdown(running).await;
}
