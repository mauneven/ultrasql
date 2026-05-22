//! Wire-level coverage for core PostgreSQL scalar types already supported by
//! UltraSQL's parser, binder, row codec, executor, and protocol encoder.

mod support;

use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

#[tokio::test]
async fn core_scalar_types_round_trip_over_postgres_wire() {
    let running = start_sample_server("core_type_surface").await;
    let client = &running.client;

    client
        .batch_execute(
            "CREATE TABLE core_type_surface (
                s SMALLINT NOT NULL,
                i INTEGER NOT NULL,
                b BIGINT NOT NULL,
                r REAL NOT NULL,
                d DOUBLE PRECISION NOT NULL,
                txt TEXT NOT NULL,
                vc VARCHAR(8) NOT NULL,
                payload BYTEA NOT NULL,
                active BOOLEAN NOT NULL
             )",
        )
        .await
        .expect("create core type table");

    client
        .batch_execute(
            "INSERT INTO core_type_surface VALUES (
                CAST(7 AS SMALLINT),
                CAST(8 AS INTEGER),
                CAST(9 AS BIGINT),
                CAST(1.5 AS REAL),
                CAST(2.25 AS DOUBLE PRECISION),
                'plain text',
                'vartext',
                '\\xdeadbeef'::bytea,
                TRUE
             )",
        )
        .await
        .expect("insert core scalar values");

    let row = client
        .query_one(
            "SELECT s, i, b, r, d, txt, vc, active FROM core_type_surface",
            &[],
        )
        .await
        .expect("select core scalar values");

    assert_eq!(row.get::<_, i16>(0), 7);
    assert_eq!(row.get::<_, i32>(1), 8);
    assert_eq!(row.get::<_, i64>(2), 9);
    assert_eq!(row.get::<_, f32>(3), 1.5);
    assert_eq!(row.get::<_, f64>(4), 2.25);
    assert_eq!(row.get::<_, String>(5), "plain text");
    assert_eq!(row.get::<_, String>(6), "vartext");
    assert!(row.get::<_, bool>(7));

    let payload_rows = client
        .simple_query("SELECT payload FROM core_type_surface")
        .await
        .expect("select bytea text value");
    let payload = payload_rows.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.get(0),
        _ => None,
    });
    assert_eq!(payload, Some("\\xdeadbeef"));

    shutdown(running).await;
}
