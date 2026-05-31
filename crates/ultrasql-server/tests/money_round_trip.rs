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

    let arithmetic = client
        .simple_query(
            "SELECT '$1.25'::money + '$3.75'::money, \
                    '$5.00'::money - '$1.25'::money",
        )
        .await
        .expect("money arithmetic");
    let values: Vec<Vec<String>> = arithmetic
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .filter_map(|idx| row.get(idx).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec![vec!["$5.00".to_owned(), "$3.75".to_owned()]]);

    let signed = client
        .simple_query("SELECT -('$1.25'::money), +'$2.00'::money")
        .await
        .expect("money unary signs");
    let values: Vec<Vec<String>> = signed
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .filter_map(|idx| row.get(idx).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec![vec!["-$1.25".to_owned(), "$2.00".to_owned()]]);

    let division = client
        .simple_query("SELECT '$5.00'::money / '$2.00'::money, '$5.01'::money / 2")
        .await
        .expect("money division");
    let values: Vec<Vec<String>> = division
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .filter_map(|idx| row.get(idx).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec![vec!["2.5".to_owned(), "$2.50".to_owned()]]);

    let rounded_division = client
        .simple_query("SELECT '$5.01'::money / 2.0::float8")
        .await
        .expect("money float division");
    let values: Vec<String> = rounded_division
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec!["$2.51".to_owned()]);

    let multiplication = client
        .simple_query("SELECT '$1.25'::money * 3, 3 * '$1.25'::money, '$1.25'::money * 1.5::float8")
        .await
        .expect("money scalar multiplication");
    let values: Vec<Vec<String>> = multiplication
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .filter_map(|idx| row.get(idx).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(
        values,
        vec![vec![
            "$3.75".to_owned(),
            "$3.75".to_owned(),
            "$1.88".to_owned()
        ]]
    );

    client
        .batch_execute("CREATE TABLE money_casts (amount MONEY, qty INT, price NUMERIC(10,3))")
        .await
        .expect("create money casts");
    client
        .batch_execute("INSERT INTO money_casts VALUES ('$12.34'::money, 12, 12.345)")
        .await
        .expect("insert money casts");
    let casts = client
        .simple_query(
            "SELECT amount::numeric, amount::text, qty::money, price::money FROM money_casts",
        )
        .await
        .expect("money casts");
    let values: Vec<Vec<String>> = casts
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .filter_map(|idx| row.get(idx).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(
        values,
        vec![vec![
            "12.34".to_owned(),
            "$12.34".to_owned(),
            "$12.00".to_owned(),
            "$12.35".to_owned()
        ]]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn money_and_numeric_runtime_casts_from_columns() {
    let running = start_sample_server("money_runtime_casts").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE runtime_casts (
                amount_text TEXT NOT NULL,
                numeric_text TEXT NOT NULL,
                int_value INT NOT NULL,
                float_value DOUBLE PRECISION NOT NULL
            )",
        )
        .await
        .expect("create runtime cast table");
    client
        .batch_execute("INSERT INTO runtime_casts VALUES ('$12.34', '56.78', 42, 3.5)")
        .await
        .expect("insert runtime cast row");

    let rows = client
        .simple_query(
            "SELECT
                CAST(amount_text AS MONEY),
                CAST(numeric_text AS NUMERIC),
                CAST(int_value AS NUMERIC),
                CAST(float_value AS NUMERIC)
             FROM runtime_casts",
        )
        .await
        .expect("runtime money and numeric casts");
    let values: Vec<Vec<String>> = rows
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .filter_map(|idx| row.get(idx).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect();

    assert_eq!(
        values,
        vec![vec![
            "$12.34".to_owned(),
            "56.78".to_owned(),
            "42".to_owned(),
            "3.5".to_owned()
        ]]
    );

    shutdown(running).await;
}
