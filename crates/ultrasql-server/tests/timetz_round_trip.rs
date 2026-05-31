//! End-to-end `TIMETZ` plus ISO date/time display/coercion behavior.

mod support;

use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

#[tokio::test]
async fn timetz_and_temporal_display_round_trip() {
    let running = start_sample_server("timetz_round_trip").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE temporal_probe (\
                id INT, \
                t TIME, \
                z TIME WITH TIME ZONE, \
                ts TIMESTAMP, \
                tstz TIMESTAMP WITH TIME ZONE\
            )",
        )
        .await
        .expect("create temporal table");
    client
        .batch_execute(
            "INSERT INTO temporal_probe VALUES (\
                1, \
                TIME '04:05:06.789-08', \
                TIME WITH TIME ZONE '04:05:06.789-08:00', \
                TIMESTAMP '2000-01-02 03:04:05.006789+02', \
                TIMESTAMP WITH TIME ZONE '2000-01-02 03:04:05+02'\
            )",
        )
        .await
        .expect("insert temporal values");

    let rows = client
        .simple_query("SELECT t, z, ts, tstz FROM temporal_probe")
        .await
        .expect("select temporal values");
    let values: Vec<Vec<String>> = rows
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..4)
                    .map(|idx| row.get(idx).expect("column").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(
        values,
        vec![vec![
            "04:05:06.789".to_owned(),
            "04:05:06.789-08".to_owned(),
            "2000-01-02 03:04:05.006789".to_owned(),
            "2000-01-02 01:04:05+00".to_owned(),
        ]]
    );

    let stmt = client
        .prepare("SELECT t, z, ts, tstz FROM temporal_probe")
        .await
        .expect("prepare temporal select");
    let oids: Vec<u32> = stmt
        .columns()
        .iter()
        .map(|column| column.type_().oid())
        .collect();
    assert_eq!(oids, vec![1083, 1266, 1114, 1184]);

    let cast_rows = client
        .simple_query("SELECT '04:05:06-08'::time, '04:05:06-08'::timetz")
        .await
        .expect("select temporal casts");
    let cast_values: Vec<Vec<String>> = cast_rows
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..2)
                    .map(|idx| row.get(idx).expect("column").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(
        cast_values,
        vec![vec!["04:05:06".to_owned(), "04:05:06-08".to_owned()]]
    );

    let abbrev_rows = client
        .simple_query(
            "SELECT \
                TIME WITH TIME ZONE '04:05:06 EST', \
                TIMESTAMP WITH TIME ZONE '2000-01-02 03:04:05 EST'",
        )
        .await
        .expect("select temporal abbreviation casts");
    let abbrev_values: Vec<Vec<String>> = abbrev_rows
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..2)
                    .map(|idx| row.get(idx).expect("column").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(
        abbrev_values,
        vec![vec![
            "04:05:06-05".to_owned(),
            "2000-01-02 08:04:05+00".to_owned()
        ]]
    );

    let named_zone_rows = client
        .simple_query(
            "SELECT \
                TIMESTAMP WITH TIME ZONE '2000-01-01 00:00:00 America/New_York', \
                TIMESTAMP WITH TIME ZONE '2000-07-01 00:00:00 America/New_York'",
        )
        .await
        .expect("select temporal named-zone casts");
    let named_zone_values: Vec<Vec<String>> = named_zone_rows
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..2)
                    .map(|idx| row.get(idx).expect("column").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(
        named_zone_values,
        vec![vec![
            "2000-01-01 05:00:00+00".to_owned(),
            "2000-07-01 04:00:00+00".to_owned(),
        ]]
    );

    shutdown(running).await;
}
