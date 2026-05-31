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

#[tokio::test]
async fn money_runtime_range_errors_use_precise_sqlstates() {
    let running = start_sample_server("money_runtime_range_errors").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE money_edges (
                amount MONEY NOT NULL,
                bump MONEY NOT NULL,
                divisor INT NOT NULL
            )",
        )
        .await
        .expect("create money edge table");
    client
        .batch_execute(
            "INSERT INTO money_edges VALUES (
                '$92233720368547758.07'::money,
                '$0.01'::money,
                0
            )",
        )
        .await
        .expect("insert money edge row");

    let overflow = client
        .simple_query("SELECT amount + bump FROM money_edges")
        .await
        .expect_err("money addition overflow must fail");
    assert_eq!(
        overflow.code().map(tokio_postgres::error::SqlState::code),
        Some("22003")
    );

    let div_zero = client
        .simple_query("SELECT amount / divisor FROM money_edges")
        .await
        .expect_err("money division by zero must fail");
    assert_eq!(
        div_zero.code().map(tokio_postgres::error::SqlState::code),
        Some("22012")
    );

    shutdown(running).await;
}

#[tokio::test]
async fn money_lc_monetary_formats_and_parses_common_locale_text() {
    let running = start_sample_server("money_lc_monetary_locale_text").await;
    let client = &running.client;
    let euro = "\u{20ac}";

    client
        .batch_execute("CREATE TABLE money_locale (id INT, amount MONEY)")
        .await
        .expect("create money locale table");
    client
        .batch_execute(&format!(
            "INSERT INTO money_locale VALUES \
             (1, '$1,234.56'::money), \
             (2, '1.234,56 {euro}'::money)"
        ))
        .await
        .expect("insert locale money");

    client
        .batch_execute("SET lc_monetary = 'de_DE.UTF-8'")
        .await
        .expect("set de_DE monetary locale");
    let german_rows = client
        .simple_query("SELECT amount FROM money_locale ORDER BY id")
        .await
        .expect("select german money");
    let german_values: Vec<String> = german_rows
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        german_values,
        vec![format!("1.234,56 {euro}"), format!("1.234,56 {euro}")]
    );

    client
        .batch_execute("SET lc_monetary = 'pt_BR'")
        .await
        .expect("set pt_BR monetary locale");
    let brazil_rows = client
        .simple_query("SELECT amount FROM money_locale WHERE id = 1")
        .await
        .expect("select brazil money");
    let brazil_values: Vec<String> = brazil_rows
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(brazil_values, vec!["R$ 1.234,56".to_owned()]);

    shutdown(running).await;
}
