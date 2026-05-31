//! Wire-level full-text scalar surface coverage.

mod support;

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

#[tokio::test]
async fn full_text_constructors_match_and_rank_over_wire() {
    let running = start_sample_server("full_text_test").await;
    let client = &running.client;

    let rows = simple_rows(
        client
            .simple_query(
                "SELECT \
                    to_tsvector('The Quick brown fox'), \
                    to_tsquery('Quick fox'), \
                    to_tsvector('The Quick brown fox') @@ plainto_tsquery('quick fox'), \
                    to_tsvector('The Quick brown fox') @@ plainto_tsquery('missing'), \
                    ts_rank(to_tsvector('The Quick brown fox'), plainto_tsquery('quick missing')) > 0.4, \
                    ts_headline('The Quick brown fox.', plainto_tsquery('quick fox'))",
            )
            .await
            .expect("full-text query"),
    );

    assert_eq!(
        rows,
        vec![vec![
            "the:1 quick:2 brown:3 fox:4".to_owned(),
            "quick & fox".to_owned(),
            "t".to_owned(),
            "f".to_owned(),
            "t".to_owned(),
            "The <b>Quick</b> brown <b>fox</b>.".to_owned(),
        ]]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn full_text_constructors_advertise_dedicated_type_oids() {
    let running = start_sample_server("full_text_test").await;
    let client = &running.client;

    let stmt = client
        .prepare("SELECT to_tsvector('quick fox'), plainto_tsquery('quick fox')")
        .await
        .expect("prepare full-text constructor oid query");
    let columns = stmt.columns();

    assert_eq!(columns[0].type_().oid(), 3614);
    assert_eq!(columns[1].type_().oid(), 3615);

    let rows = simple_rows(
        client
            .simple_query(
                "SELECT typname, oid FROM pg_catalog.pg_type \
                 WHERE typname IN ('tsquery', 'tsvector') ORDER BY typname",
            )
            .await
            .expect("pg_type full-text rows"),
    );
    assert_eq!(
        rows,
        vec![
            vec!["tsquery".to_owned(), "3615".to_owned()],
            vec!["tsvector".to_owned(), "3614".to_owned()],
        ]
    );

    shutdown(running).await;
}
