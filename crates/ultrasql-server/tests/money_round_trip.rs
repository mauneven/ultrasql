//! End-to-end MONEY surface and PostgreSQL wire metadata.

mod support;

use support::{shutdown, start_sample_server};

#[tokio::test]
async fn money_cast_insert_select_and_extended_wire_type() {
    let running = start_sample_server("money_round_trip").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE ledger (id INT, amount MONEY)")
        .await
        .expect("create ledger");
    client
        .batch_execute(
            "INSERT INTO ledger VALUES \
             (1, '$1,234.56'::money), \
             (2, '-$1.23'::money), \
             (3, '12.345'::money)",
        )
        .await
        .expect("insert money");

    let messages = client
        .simple_query("SELECT amount FROM ledger ORDER BY id")
        .await
        .expect("select money");
    let values: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        values,
        vec![
            "$1,234.56".to_owned(),
            "-$1.23".to_owned(),
            "$12.35".to_owned()
        ]
    );

    let rows = client
        .query("SELECT '$12.34'::money AS amount", &[])
        .await
        .expect("extended money query");
    assert_eq!(rows[0].columns()[0].type_().oid(), 790);

    shutdown(running).await;
}
