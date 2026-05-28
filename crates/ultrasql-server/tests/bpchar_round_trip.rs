//! End-to-end `CHAR(n)` / `bpchar` padding and comparison behavior.

mod support;

use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

#[tokio::test]
async fn char_padding_length_comparison_and_wire_oid_round_trip() {
    let running = start_sample_server("bpchar_round_trip").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE bpchar_codes (id INT, code CHAR(4), tag BPCHAR(3))")
        .await
        .expect("create bpchar table");
    client
        .batch_execute(
            "INSERT INTO bpchar_codes VALUES \
             (1, 'ok', 'xy'), \
             (2, 'ok  ', 'xy ')",
        )
        .await
        .expect("insert bpchar values");

    let rows = client
        .simple_query(
            "SELECT code, length(code) AS code_len, code = 'ok' AS eq_unknown, \
                    code = 'ok'::text AS eq_text, code LIKE 'ok' AS like_exact \
             FROM bpchar_codes ORDER BY id",
        )
        .await
        .expect("select bpchar values");
    let values: Vec<Vec<String>> = rows
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..5)
                    .map(|idx| row.get(idx).expect("column").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect();

    assert_eq!(
        values,
        vec![
            vec![
                "ok  ".to_owned(),
                "2".to_owned(),
                "t".to_owned(),
                "t".to_owned(),
                "f".to_owned(),
            ],
            vec![
                "ok  ".to_owned(),
                "2".to_owned(),
                "t".to_owned(),
                "t".to_owned(),
                "f".to_owned(),
            ],
        ]
    );

    let row = client
        .query_one("SELECT CAST('abcdef' AS CHAR(3)) AS code", &[])
        .await
        .expect("select char cast");
    assert_eq!(row.columns()[0].type_().oid(), 1042);
    assert_eq!(row.get::<_, String>(0), "abc");

    let err = client
        .batch_execute("INSERT INTO bpchar_codes VALUES (3, 'toolong', 'zz')")
        .await
        .expect_err("overlength char insert must fail");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22001")
    );

    shutdown(running).await;
}
