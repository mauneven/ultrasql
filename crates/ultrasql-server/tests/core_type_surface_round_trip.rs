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

    client
        .batch_execute(
            "CREATE TABLE text_cast_surface (
                as_int TEXT NOT NULL,
                as_bool TEXT NOT NULL,
                as_float TEXT NOT NULL
             );
             INSERT INTO text_cast_surface VALUES ('42', 'true', '3.5')",
        )
        .await
        .expect("create text cast table");
    let text_cast_row = client
        .query_one(
            "SELECT
                CAST(as_int AS INTEGER),
                CAST(as_bool AS BOOLEAN),
                CAST(as_float AS DOUBLE PRECISION)
             FROM text_cast_surface",
            &[],
        )
        .await
        .expect("runtime text casts from columns");
    assert_eq!(text_cast_row.get::<_, i32>(0), 42);
    assert!(text_cast_row.get::<_, bool>(1));
    assert_eq!(text_cast_row.get::<_, f64>(2), 3.5);

    client
        .batch_execute(
            "SET TimeZone TO 'UTC';
             CREATE TABLE temporal_text_cast_surface (
                as_date TEXT NOT NULL,
                as_time TEXT NOT NULL,
                as_timestamp TEXT NOT NULL,
                as_timestamptz TEXT NOT NULL,
                as_timetz TEXT NOT NULL
             );
             INSERT INTO temporal_text_cast_surface
             VALUES (
                '2023-08-15',
                '04:05:06',
                '2023-08-15 04:05:06',
                '2023-08-15 04:05:06 UTC',
                '04:05:06-05'
             )",
        )
        .await
        .expect("create temporal text cast table");
    let temporal_cast_rows = client
        .simple_query(
            "SELECT
                CAST(as_date AS DATE),
                CAST(as_time AS TIME),
                CAST(as_timestamp AS TIMESTAMP),
                CAST(as_timestamptz AS TIMESTAMP WITH TIME ZONE),
                CAST(as_timetz AS TIME WITH TIME ZONE)
             FROM temporal_text_cast_surface",
        )
        .await
        .expect("runtime temporal text casts from columns");
    let temporal_values: Vec<Vec<String>> = temporal_cast_rows
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..5)
                    .map(|idx| row.get(idx).expect("temporal cast column").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(
        temporal_values,
        vec![vec![
            "2023-08-15".to_owned(),
            "04:05:06".to_owned(),
            "2023-08-15 04:05:06".to_owned(),
            "2023-08-15 04:05:06+00".to_owned(),
            "04:05:06-05".to_owned()
        ]]
    );

    client
        .batch_execute(
            "CREATE TABLE scalar_invalid_text_cast_surface (
                as_int TEXT NOT NULL,
                as_bool TEXT NOT NULL,
                as_float TEXT NOT NULL,
                as_date TEXT NOT NULL,
                as_time TEXT NOT NULL,
                as_timestamp TEXT NOT NULL,
                as_timestamptz TEXT NOT NULL,
                as_timetz TEXT NOT NULL
             );
             INSERT INTO scalar_invalid_text_cast_surface
             VALUES (
                'not-int',
                'maybe',
                'not-float',
                'not-date',
                'not-time',
                'not-timestamp',
                'not-timestamptz',
                'not-timetz'
             )",
        )
        .await
        .expect("create invalid scalar text cast table");
    for query in [
        "SELECT CAST(as_int AS INTEGER) FROM scalar_invalid_text_cast_surface",
        "SELECT CAST(as_bool AS BOOLEAN) FROM scalar_invalid_text_cast_surface",
        "SELECT CAST(as_float AS DOUBLE PRECISION) FROM scalar_invalid_text_cast_surface",
        "SELECT CAST(as_date AS DATE) FROM scalar_invalid_text_cast_surface",
        "SELECT CAST(as_time AS TIME) FROM scalar_invalid_text_cast_surface",
        "SELECT CAST(as_timestamp AS TIMESTAMP) FROM scalar_invalid_text_cast_surface",
        "SELECT CAST(as_timestamptz AS TIMESTAMP WITH TIME ZONE) FROM scalar_invalid_text_cast_surface",
        "SELECT CAST(as_timetz AS TIME WITH TIME ZONE) FROM scalar_invalid_text_cast_surface",
    ] {
        let err = client
            .simple_query(query)
            .await
            .expect_err("invalid scalar runtime text cast must fail");
        assert_eq!(
            err.code().map(tokio_postgres::error::SqlState::code),
            Some("22P02"),
            "{query}"
        );
    }
    let filter_err = client
        .simple_query(
            "SELECT 1 FROM scalar_invalid_text_cast_surface WHERE CAST(as_int AS INTEGER) = 1",
        )
        .await
        .expect_err("invalid scalar runtime text cast in filter must fail");
    assert_eq!(
        filter_err.code().map(tokio_postgres::error::SqlState::code),
        Some("22P02")
    );
    let result_err = client
        .simple_query("SELECT CAST('not-int' AS INTEGER)")
        .await
        .expect_err("invalid scalar constant cast must fail");
    assert_eq!(
        result_err.code().map(tokio_postgres::error::SqlState::code),
        Some("22P02")
    );

    client
        .batch_execute(
            "CREATE TABLE scalar_range_text_cast_surface (
                as_smallint TEXT NOT NULL,
                as_int TEXT NOT NULL,
                as_bigint TEXT NOT NULL
             );
             INSERT INTO scalar_range_text_cast_surface
             VALUES ('32768', '2147483648', '9223372036854775808')",
        )
        .await
        .expect("create range scalar text cast table");
    for query in [
        "SELECT CAST(as_smallint AS SMALLINT) FROM scalar_range_text_cast_surface",
        "SELECT CAST(as_int AS INTEGER) FROM scalar_range_text_cast_surface",
        "SELECT CAST(as_bigint AS BIGINT) FROM scalar_range_text_cast_surface",
    ] {
        let err = client
            .simple_query(query)
            .await
            .expect_err("out-of-range runtime integer text cast must fail");
        assert_eq!(
            err.code().map(tokio_postgres::error::SqlState::code),
            Some("22003"),
            "{query}"
        );
    }

    client
        .batch_execute(
            "CREATE TABLE structured_text_cast_surface (
                as_uuid TEXT NOT NULL,
                as_json TEXT NOT NULL,
                as_jsonb TEXT NOT NULL,
                as_xml TEXT NOT NULL
             );
             INSERT INTO structured_text_cast_surface
             VALUES (
                '12345678-9abc-def0-1234-56789abcdef0',
                '{\"a\":1}',
                '{\"a\":1}',
                '<root/>'
             )",
        )
        .await
        .expect("create structured text cast table");
    let structured_cast_rows = client
        .simple_query(
            "SELECT
                CAST(as_uuid AS UUID),
                CAST(as_json AS JSON),
                CAST(as_jsonb AS JSONB),
                CAST(as_xml AS XML)
             FROM structured_text_cast_surface",
        )
        .await
        .expect("runtime structured text casts from columns");
    let structured_values: Vec<Vec<String>> = structured_cast_rows
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..4)
                    .map(|idx| row.get(idx).expect("structured cast column").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(
        structured_values,
        vec![vec![
            "12345678-9abc-def0-1234-56789abcdef0".to_owned(),
            "{\"a\":1}".to_owned(),
            "{\"a\":1}".to_owned(),
            "<root/>".to_owned()
        ]]
    );

    client
        .batch_execute(
            "CREATE TABLE structured_invalid_text_cast_surface (
                as_uuid TEXT NOT NULL,
                as_json TEXT NOT NULL,
                as_jsonb TEXT NOT NULL
             );
             INSERT INTO structured_invalid_text_cast_surface
             VALUES ('not-a-uuid', '{bad', '{bad')",
        )
        .await
        .expect("create invalid structured text cast table");
    for query in [
        "SELECT CAST(as_uuid AS UUID) FROM structured_invalid_text_cast_surface",
        "SELECT CAST(as_json AS JSON) FROM structured_invalid_text_cast_surface",
        "SELECT CAST(as_jsonb AS JSONB) FROM structured_invalid_text_cast_surface",
    ] {
        let err = client
            .simple_query(query)
            .await
            .expect_err("invalid structured runtime text cast must fail");
        assert_eq!(
            err.code().map(tokio_postgres::error::SqlState::code),
            Some("22P02"),
            "{query}"
        );
    }

    client
        .batch_execute(
            "CREATE TABLE xml_invalid_text_cast_surface (as_xml TEXT NOT NULL);
             INSERT INTO xml_invalid_text_cast_surface VALUES ('<root>')",
        )
        .await
        .expect("create invalid XML text cast table");
    let xml_err = client
        .simple_query("SELECT CAST(as_xml AS XML) FROM xml_invalid_text_cast_surface")
        .await
        .expect_err("invalid runtime XML text cast must fail");
    assert_eq!(
        xml_err.code().map(tokio_postgres::error::SqlState::code),
        Some("2200M")
    );

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
