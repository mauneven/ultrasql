//! `pg_proc` advertises supported XML and full-text builtin functions.

use super::*;

#[tokio::test]
async fn pg_proc_advertises_supported_xml_functions() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query(
            "SELECT proname, pronargs, pg_catalog.format_type(prorettype, NULL), proretset \
             FROM pg_catalog.pg_proc \
             WHERE proname IN ( \
                'xml_is_well_formed', \
                'xml_is_well_formed_content', \
                'xml_is_well_formed_document', \
                'xpath', \
                'xpath_exists') \
             ORDER BY proname, pronargs",
            &[],
        )
        .await
        .expect("xml pg_proc rows");

    let got = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, i16>(1),
                row.get::<_, String>(2),
                row.get::<_, bool>(3),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        got,
        vec![
            (
                "xml_is_well_formed".to_owned(),
                1,
                "boolean".to_owned(),
                false
            ),
            (
                "xml_is_well_formed_content".to_owned(),
                1,
                "boolean".to_owned(),
                false,
            ),
            (
                "xml_is_well_formed_document".to_owned(),
                1,
                "boolean".to_owned(),
                false,
            ),
            ("xpath".to_owned(), 2, "xml[]".to_owned(), false),
            ("xpath".to_owned(), 3, "xml[]".to_owned(), false),
            ("xpath_exists".to_owned(), 2, "boolean".to_owned(), false),
            ("xpath_exists".to_owned(), 3, "boolean".to_owned(), false),
        ]
    );

    shutdown(client, server_handle).await;
}

#[tokio::test]
async fn pg_proc_advertises_supported_full_text_functions() {
    let (_server, client, _conn, server_handle) = start_server_and_connect().await;

    let rows = client
        .query(
            "SELECT proname, pronargs, pg_catalog.format_type(prorettype, NULL), proretset \
             FROM pg_catalog.pg_proc \
             WHERE proname IN ( \
                'to_tsvector', \
                'to_tsquery', \
                'plainto_tsquery', \
                'websearch_to_tsquery', \
                'phraseto_tsquery', \
                'ts_rank', \
                'ts_rank_cd', \
                'ts_headline', \
                'numnode', \
                'querytree') \
             ORDER BY proname, pronargs",
            &[],
        )
        .await
        .expect("full-text pg_proc rows");

    let got = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, i16>(1),
                row.get::<_, String>(2),
                row.get::<_, bool>(3),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        got,
        vec![
            ("numnode".to_owned(), 1, "integer".to_owned(), false),
            (
                "phraseto_tsquery".to_owned(),
                1,
                "tsquery".to_owned(),
                false
            ),
            (
                "phraseto_tsquery".to_owned(),
                2,
                "tsquery".to_owned(),
                false
            ),
            ("plainto_tsquery".to_owned(), 1, "tsquery".to_owned(), false),
            ("plainto_tsquery".to_owned(), 2, "tsquery".to_owned(), false),
            ("querytree".to_owned(), 1, "text".to_owned(), false),
            ("to_tsquery".to_owned(), 1, "tsquery".to_owned(), false),
            ("to_tsquery".to_owned(), 2, "tsquery".to_owned(), false),
            ("to_tsvector".to_owned(), 1, "tsvector".to_owned(), false),
            ("to_tsvector".to_owned(), 2, "tsvector".to_owned(), false),
            ("ts_headline".to_owned(), 2, "text".to_owned(), false),
            ("ts_headline".to_owned(), 3, "text".to_owned(), false),
            (
                "ts_rank".to_owned(),
                2,
                "double precision".to_owned(),
                false
            ),
            (
                "ts_rank_cd".to_owned(),
                2,
                "double precision".to_owned(),
                false
            ),
            (
                "websearch_to_tsquery".to_owned(),
                1,
                "tsquery".to_owned(),
                false,
            ),
            (
                "websearch_to_tsquery".to_owned(),
                2,
                "tsquery".to_owned(),
                false,
            ),
        ]
    );

    shutdown(client, server_handle).await;
}
