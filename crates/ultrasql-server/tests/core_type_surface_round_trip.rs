//! Wire-level coverage for core PostgreSQL scalar types already supported by
//! UltraSQL's parser, binder, row codec, executor, and protocol encoder.

mod support;

use support::{shutdown, start_persistent_server, start_sample_server};
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

    let cast_row = client
        .query_one(
            "SELECT
                CAST(i AS BIGINT),
                CAST(b AS INTEGER),
                CAST(i AS SMALLINT),
                CAST(s AS BIGINT)
             FROM core_type_surface",
            &[],
        )
        .await
        .expect("runtime integer casts from columns");
    assert_eq!(cast_row.get::<_, i64>(0), 8);
    assert_eq!(cast_row.get::<_, i32>(1), 9);
    assert_eq!(cast_row.get::<_, i16>(2), 8);
    assert_eq!(cast_row.get::<_, i64>(3), 7);

    let float_cast_row = client
        .query_one(
            "SELECT
                CAST(i AS DOUBLE PRECISION),
                CAST(b AS REAL),
                CAST(r AS DOUBLE PRECISION),
                CAST(d AS REAL)
             FROM core_type_surface",
            &[],
        )
        .await
        .expect("runtime float casts from columns");
    assert_eq!(float_cast_row.get::<_, f64>(0), 8.0);
    assert_eq!(float_cast_row.get::<_, f32>(1), 9.0);
    assert_eq!(float_cast_row.get::<_, f64>(2), 1.5);
    assert_eq!(float_cast_row.get::<_, f32>(3), 2.25);

    let err = client
        .batch_execute(
            "INSERT INTO core_type_surface VALUES (
                1, 2, 3, 1.0, 2.0, 'x', 'too-long-varchar', '\\x00'::bytea, TRUE
             )",
        )
        .await
        .expect_err("overlength varchar insert must fail");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22001")
    );

    shutdown(running).await;
}

#[tokio::test]
async fn varchar_length_limit_survives_restart() {
    let data_dir = tempfile::TempDir::new().expect("temp dir");
    let running = start_persistent_server(data_dir.path(), "varchar_restart").await;
    running
        .client
        .batch_execute("CREATE TABLE varchar_restart (label VARCHAR(3) NOT NULL)")
        .await
        .expect("create varchar table");
    running
        .client
        .batch_execute("INSERT INTO varchar_restart VALUES ('abc')")
        .await
        .expect("insert bounded varchar before restart");
    shutdown(running).await;

    let running = start_persistent_server(data_dir.path(), "varchar_restart").await;
    let err = running
        .client
        .batch_execute("INSERT INTO varchar_restart VALUES ('abcd')")
        .await
        .expect_err("overlength varchar insert must fail after restart");
    assert_eq!(
        err.code().map(tokio_postgres::error::SqlState::code),
        Some("22001")
    );

    shutdown(running).await;
}
