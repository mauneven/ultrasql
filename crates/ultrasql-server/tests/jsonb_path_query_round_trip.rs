//! Wire-level regression tests for SQL/JSON path query table function.

pub mod support;

use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

#[tokio::test]
async fn jsonb_path_query_expands_selected_values() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"items\":[{\"id\":1},{\"id\":2}]}'::jsonb, '$.items[*].id') \
             ORDER BY value",
        )
        .await
        .expect("jsonb_path_query");
    let rows: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();

    assert_eq!(rows, vec!["1".to_owned(), "2".to_owned()]);

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_sql_json_filters_and_recursive_descent() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let document = "'{\"items\":[\
        {\"id\":1,\"score\":12,\"meta\":{\"kind\":\"guide\"}},\
        {\"id\":2,\"score\":25,\"meta\":{\"kind\":\"paper\"}},\
        {\"id\":3,\"score\":31,\"meta\":{\"kind\":\"guide\"}}\
    ],\"weird-key\":{\"id\":9}}'::jsonb";

    let filtered = client
        .simple_query(&format!(
            "SELECT value FROM jsonb_path_query(\
             {document}, '$.items[*] ? (@.meta.kind == \"guide\").id') \
             ORDER BY value"
        ))
        .await
        .expect("jsonb_path_query filter");
    let rows: Vec<String> = filtered
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(rows, vec!["1".to_owned(), "3".to_owned()]);

    let quoted_key = client
        .simple_query(&format!(
            "SELECT value FROM jsonb_path_query({document}, '$.\"weird-key\".id')"
        ))
        .await
        .expect("jsonb_path_query quoted key");
    let rows: Vec<String> = quoted_key
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(rows, vec!["9".to_owned()]);

    let recursive = client
        .simple_query(&format!(
            "SELECT value FROM jsonb_path_query({document}, '$.**.kind') \
             ORDER BY value"
        ))
        .await
        .expect("jsonb_path_query recursive descent");
    let rows: Vec<String> = recursive
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            "\"guide\"".to_owned(),
            "\"guide\"".to_owned(),
            "\"paper\"".to_owned(),
        ]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_accepts_strict_and_lax_prefixes() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT \
                jsonb_path_exists('{\"items\":[{\"id\":1},{\"id\":2}]}'::jsonb, \
                    'lax $.items[*] ? (@.id == 2)'), \
                jsonb_path_exists('{\"items\":[{\"id\":1},{\"id\":2}]}'::jsonb, \
                    'strict $.items[*] ? (@.id == 3)')",
        )
        .await
        .expect("jsonb_path_exists strict/lax prefixes");
    let row = messages
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("jsonb_path_exists strict/lax row");

    assert_eq!(row.get(0), Some("t"));
    assert_eq!(row.get(1), Some("f"));

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_strict_mode_reports_structural_errors() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let lax_rows = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"items\":[{}]}'::jsonb, \
             'lax $.items[*].missing')",
        )
        .await
        .expect("lax jsonb_path_query missing key");
    let rows: Vec<String> = lax_rows
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert!(rows.is_empty());

    let strict_error = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"items\":[{}]}'::jsonb, \
             'strict $.items[*].missing')",
        )
        .await
        .expect_err("strict jsonb_path_query missing key errors");
    let strict_db_error = strict_error
        .as_db_error()
        .expect("strict jsonpath error carries server message");
    assert!(
        strict_db_error
            .message()
            .contains("strict jsonpath structural error")
    );

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_exists_supports_variable_literals() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT \
                jsonb_path_exists('{\"items\":[{\"score\":12},{\"score\":25}]}'::jsonb, \
                    '$.items[*] ? (@.score >= $min)', '{\"min\":20}'::jsonb), \
                jsonb_path_exists('{\"items\":[{\"kind\":\"guide\"}]}'::jsonb, \
                    '$.items[*] ? (@.kind == $kind)', '{\"kind\":\"paper\"}'::jsonb)",
        )
        .await
        .expect("jsonb_path_exists variables");
    let row = messages
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("jsonb_path_exists variables row");

    assert_eq!(row.get(0), Some("t"));
    assert_eq!(row.get(1), Some("f"));

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_variable_literals() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"items\":[{\"id\":1,\"score\":12},{\"id\":2,\"score\":25}]}'::jsonb, \
             '$.items[*] ? (@.score >= $min).id', \
             '{\"min\":20}'::jsonb)",
        )
        .await
        .expect("jsonb_path_query variables");
    let rows: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();

    assert_eq!(rows, vec!["2".to_owned()]);

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_basic_methods() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"items\":[1,2,3],\"meta\":{\"ok\":true}}'::jsonb, \
             '$.items.size()') \
             UNION ALL \
             SELECT value FROM jsonb_path_query(\
             '{\"items\":[1,2,3],\"meta\":{\"ok\":true}}'::jsonb, \
             '$.meta.type()')",
        )
        .await
        .expect("jsonb_path_query basic methods");
    let rows: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();

    assert_eq!(rows, vec!["3".to_owned(), "\"object\"".to_owned()]);

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_numeric_methods() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT value FROM jsonb_path_query('{\"v\":-7.5}'::jsonb, '$.v.abs()') \
             UNION ALL \
             SELECT value FROM jsonb_path_query('{\"v\":2.7}'::jsonb, '$.v.floor()') \
             UNION ALL \
             SELECT value FROM jsonb_path_query('{\"v\":2.2}'::jsonb, '$.v.ceiling()') \
             UNION ALL \
             SELECT value FROM jsonb_path_query('{\"v\":\"3.5\"}'::jsonb, '$.v.double()')",
        )
        .await
        .expect("jsonb_path_query numeric methods");
    let rows: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();

    assert_eq!(
        rows,
        vec![
            "7.5".to_owned(),
            "2.0".to_owned(),
            "3.0".to_owned(),
            "3.5".to_owned(),
        ]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_conversion_methods() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT value FROM jsonb_path_query('[1,\"yes\",false]'::jsonb, '$[*].string()') \
             UNION ALL \
             SELECT value FROM jsonb_path_query('[1,\"off\",false,0]'::jsonb, '$[*].boolean()') \
             UNION ALL \
             SELECT value FROM jsonb_path_query('{\"v\":\"123.45\"}'::jsonb, '$.v.number()') \
             UNION ALL \
             SELECT value FROM jsonb_path_query('{\"v\":\"12345\"}'::jsonb, '$.v.integer()') \
             UNION ALL \
             SELECT value FROM jsonb_path_query('{\"v\":\"9876543219\"}'::jsonb, '$.v.bigint()')",
        )
        .await
        .expect("jsonb_path_query conversion methods");
    let rows: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();

    assert_eq!(
        rows,
        vec![
            "\"1\"".to_owned(),
            "\"yes\"".to_owned(),
            "\"false\"".to_owned(),
            "true".to_owned(),
            "false".to_owned(),
            "false".to_owned(),
            "false".to_owned(),
            "123.45".to_owned(),
            "12345".to_owned(),
            "9876543219".to_owned(),
        ]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_keyvalue_method() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"x\":\"20\",\"y\":32}'::jsonb, '$.keyvalue()')",
        )
        .await
        .expect("jsonb_path_query keyvalue");
    let mut rows: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    rows.sort();

    assert_eq!(
        rows,
        vec![
            "{\"id\":0,\"key\":\"x\",\"value\":\"20\"}".to_owned(),
            "{\"id\":0,\"key\":\"y\",\"value\":32}".to_owned(),
        ]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_decimal_method() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"values\":[\"1234.5678\",\"42.5\",\"bad\"]}'::jsonb, \
             '$.values[*].decimal(6, 2)')",
        )
        .await
        .expect("jsonb_path_query decimal method");
    let rows: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();

    assert_eq!(
        rows,
        vec!["1234.57".to_owned(), "42.5".to_owned(), "null".to_owned(),]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_iso_datetime_methods() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"values\":[\"2023-08-15\",\"12:34:56.789\",\
             \"2023-08-15 12:34:56.789 +05:30\",\"bad\"]}'::jsonb, \
             '$.values[*].datetime()')",
        )
        .await
        .expect("jsonb_path_query datetime method");
    let rows: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();

    assert_eq!(
        rows,
        vec![
            "\"2023-08-15\"".to_owned(),
            "\"12:34:56.789\"".to_owned(),
            "\"2023-08-15T12:34:56.789+05:30\"".to_owned(),
            "null".to_owned(),
        ]
    );

    let rounded = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"time\":\"12:34:56.789\",\"ts\":\"2023-08-15 12:34:56.789\"}'::jsonb, \
             '$.** ? (@.type() == \"string\").time(2)')",
        )
        .await
        .expect("jsonb_path_query time precision method");
    let rows: Vec<String> = rounded
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(rows, vec!["\"12:34:56.79\"".to_owned(), "null".to_owned()]);

    let templated = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"date\":\"20230815\",\"ts\":\"20230815123456\"}'::jsonb, \
             '$.** ? (@.type() == \"string\").datetime(\"YYYYMMDDHH24MISS\")')",
        )
        .await
        .expect("jsonb_path_query datetime template method");
    let rows: Vec<String> = templated
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        rows,
        vec!["null".to_owned(), "\"2023-08-15T12:34:56\"".to_owned()]
    );

    let minute_templated = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"date\":\"20230815\",\"ts\":\"202308151234\"}'::jsonb, \
             '$.** ? (@.type() == \"string\").datetime(\"YYYYMMDDHH24MI\")')",
        )
        .await
        .expect("jsonb_path_query minute datetime template method");
    let rows: Vec<String> = minute_templated
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        rows,
        vec!["null".to_owned(), "\"2023-08-15T12:34:00\"".to_owned()]
    );

    let fractional_templated = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"date\":\"2023-08-15\",\"ts\":\"2023-08-15 12:34:56.789123\"}'::jsonb, \
             '$.** ? (@.type() == \"string\").datetime(\"YYYY-MM-DD HH24:MI:SS.FF6\")')",
        )
        .await
        .expect("jsonb_path_query fractional datetime template method");
    let rows: Vec<String> = fractional_templated
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            "null".to_owned(),
            "\"2023-08-15T12:34:56.789123\"".to_owned()
        ]
    );

    let millisecond_templated = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"date\":\"2023-08-15\",\"ts\":\"2023-08-15T12:34:56.789\"}'::jsonb, \
             '$.** ? (@.type() == \"string\").datetime(\"YYYY-MM-DD\\\"T\\\"HH24:MI:SS.FF3\")')",
        )
        .await
        .expect("jsonb_path_query millisecond datetime template method");
    let rows: Vec<String> = millisecond_templated
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(
        rows,
        vec!["null".to_owned(), "\"2023-08-15T12:34:56.789\"".to_owned()]
    );

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_predicate_boolean_algebra() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let document = "'{\"items\":[\
        {\"id\":1,\"score\":12,\"meta\":{\"kind\":\"guide\"}},\
        {\"id\":2,\"score\":25,\"meta\":{\"kind\":\"paper\"}},\
        {\"id\":3,\"score\":31,\"meta\":{\"kind\":\"guide\"}}\
    ]}'::jsonb";

    let and_rows = client
        .simple_query(&format!(
            "SELECT value FROM jsonb_path_query(\
             {document}, '$.items[*] ? (@.score >= 20 && @.meta.kind == \"guide\").id')"
        ))
        .await
        .expect("jsonb_path_query boolean and");
    let rows: Vec<String> = and_rows
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(rows, vec!["3".to_owned()]);

    let or_rows = client
        .simple_query(&format!(
            "SELECT value FROM jsonb_path_query(\
             {document}, '$.items[*] ? (@.score < 15 || @.meta.kind == \"paper\").id') \
             ORDER BY value"
        ))
        .await
        .expect("jsonb_path_query boolean or");
    let rows: Vec<String> = or_rows
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(rows, vec!["1".to_owned(), "2".to_owned()]);

    let not_rows = client
        .simple_query(&format!(
            "SELECT value FROM jsonb_path_query(\
             {document}, '$.items[*] ? (!(@.meta.kind == \"paper\")).id') \
             ORDER BY value"
        ))
        .await
        .expect("jsonb_path_query boolean not");
    let rows: Vec<String> = not_rows
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(rows, vec!["1".to_owned(), "3".to_owned()]);

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_starts_with_predicates() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"items\":[{\"id\":1,\"name\":\"Alpha\"},\
             {\"id\":2,\"name\":\"Beta\"},{\"id\":3,\"name\":\"Alpine\"}]}'::jsonb, \
             '$.items[*] ? (@.name starts with \"Al\").id') \
             ORDER BY value",
        )
        .await
        .expect("jsonb_path_query starts with predicate");
    let rows: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();

    assert_eq!(rows, vec!["1".to_owned(), "3".to_owned()]);

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_exists_predicates() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"items\":[{\"id\":1,\"meta\":{\"kind\":\"guide\"}},\
             {\"id\":2},{\"id\":3,\"meta\":{\"kind\":\"paper\"}}]}'::jsonb, \
             '$.items[*] ? (exists(@.meta.kind)).id') \
             ORDER BY value",
        )
        .await
        .expect("jsonb_path_query exists predicate");
    let rows: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();

    assert_eq!(rows, vec!["1".to_owned(), "3".to_owned()]);

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_query_supports_like_regex_predicates() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT value FROM jsonb_path_query(\
             '{\"items\":[{\"id\":1,\"name\":\"Alpha\"},\
             {\"id\":2,\"name\":\"Beta\"},{\"id\":3,\"name\":\"alpine\"}]}'::jsonb, \
             '$.items[*] ? (@.name like_regex \"^al\" flag \"i\").id') \
             ORDER BY value",
        )
        .await
        .expect("jsonb_path_query like_regex predicate");
    let rows: Vec<String> = messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();

    assert_eq!(rows, vec!["1".to_owned(), "3".to_owned()]);

    shutdown(running).await;
}

#[tokio::test]
async fn jsonb_path_exists_evaluates_sql_json_predicates() {
    let running = start_sample_server("jsonb_path_query_test").await;
    let client = &running.client;

    let messages = client
        .simple_query(
            "SELECT \
                jsonb_path_exists('{\"items\":[{\"score\":12},{\"score\":25}]}'::jsonb, \
                    '$.items[*] ? (@.score >= 20)'), \
                jsonb_path_exists('{\"items\":[{\"score\":12},{\"score\":25}]}'::jsonb, \
                    '$.items[*] ? (@.score > 99)')",
        )
        .await
        .expect("jsonb_path_exists predicate");
    let row = messages
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("jsonb_path_exists row");

    assert_eq!(row.get(0), Some("t"));
    assert_eq!(row.get(1), Some("f"));

    shutdown(running).await;
}
