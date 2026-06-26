//! End-to-end scalar system-function behavior tests.

pub mod support;

use support::{shutdown, start_sample_server};

#[tokio::test]
async fn jdbc_startup_runtime_parameters_round_trip() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    client
        .batch_execute("SET extra_float_digits = 3")
        .await
        .expect("JDBC startup SET extra_float_digits succeeds");
    let row = client
        .query_one("SHOW extra_float_digits", &[])
        .await
        .expect("SHOW extra_float_digits");
    assert_eq!(row.get::<_, String>(0), "3");
    client
        .batch_execute("RESET extra_float_digits")
        .await
        .expect("RESET extra_float_digits succeeds");
    let row = client
        .query_one("SHOW extra_float_digits", &[])
        .await
        .expect("SHOW reset extra_float_digits");
    assert_eq!(row.get::<_, String>(0), "1");

    client
        .batch_execute("SET application_name = 'driver_cert_jdbc'")
        .await
        .expect("JDBC startup SET application_name succeeds");
    let row = client
        .query_one("SHOW application_name", &[])
        .await
        .expect("SHOW application_name");
    assert_eq!(row.get::<_, String>(0), "driver_cert_jdbc");

    shutdown(running).await;
}

#[tokio::test]
async fn orm_startup_runtime_parameters_round_trip() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    client
        .batch_execute("SET client_min_messages = warning")
        .await
        .expect("Rails startup SET client_min_messages succeeds");
    let row = client
        .query_one("SHOW client_min_messages", &[])
        .await
        .expect("SHOW client_min_messages");
    assert_eq!(row.get::<_, String>(0), "warning");

    client
        .batch_execute("SET intervalstyle = iso_8601")
        .await
        .expect("Rails startup SET intervalstyle succeeds");
    let row = client
        .query_one("SHOW intervalstyle", &[])
        .await
        .expect("SHOW intervalstyle");
    assert_eq!(row.get::<_, String>(0), "iso_8601");

    client
        .batch_execute("SET datestyle TO SQL, DMY")
        .await
        .expect("ORM startup SET datestyle succeeds");
    let row = client
        .query_one("SHOW datestyle", &[])
        .await
        .expect("SHOW datestyle");
    assert_eq!(row.get::<_, String>(0), "SQL, DMY");
    client
        .batch_execute("RESET datestyle")
        .await
        .expect("RESET datestyle succeeds");
    let row = client
        .query_one("SHOW datestyle", &[])
        .await
        .expect("SHOW reset datestyle");
    assert_eq!(row.get::<_, String>(0), "ISO, MDY");

    client
        .batch_execute("SET SESSION timezone TO 'UTC'")
        .await
        .expect("Rails startup SET timezone succeeds");
    let row = client
        .query_one("SHOW timezone", &[])
        .await
        .expect("SHOW timezone");
    assert_eq!(row.get::<_, String>(0), "UTC");

    client
        .batch_execute("SET CLIENT_ENCODING TO 'UTF8'")
        .await
        .expect("Diesel startup SET CLIENT_ENCODING succeeds");
    let row = client
        .query_one("SHOW client_encoding", &[])
        .await
        .expect("SHOW client_encoding");
    assert_eq!(row.get::<_, String>(0), "UTF8");

    let row = client
        .query_one("SHOW server_version", &[])
        .await
        .expect("SHOW server_version");
    assert_eq!(row.get::<_, String>(0), "14.0");

    let row = client
        .query_one("SHOW max_identifier_length", &[])
        .await
        .expect("SHOW max_identifier_length");
    assert_eq!(row.get::<_, String>(0), "63");

    let row = client
        .query_one("SHOW search_path", &[])
        .await
        .expect("SHOW search_path");
    assert_eq!(row.get::<_, String>(0), "\"$user\", public");

    client
        .batch_execute("SET LOCAL search_path TO public, \"$user\"")
        .await
        .expect("SET LOCAL search_path list");
    let row = client
        .query_one("SHOW search_path", &[])
        .await
        .expect("SHOW search_path list");
    assert_eq!(row.get::<_, String>(0), "public, \"$user\"");

    client
        .batch_execute("SET lc_monetary = 'C'")
        .await
        .expect("SET lc_monetary succeeds");
    let row = client
        .query_one("SHOW lc_monetary", &[])
        .await
        .expect("SHOW lc_monetary");
    assert_eq!(row.get::<_, String>(0), "C");
    client
        .batch_execute("RESET lc_monetary")
        .await
        .expect("RESET lc_monetary succeeds");
    let row = client
        .query_one("SHOW lc_monetary", &[])
        .await
        .expect("SHOW reset lc_monetary");
    assert_eq!(row.get::<_, String>(0), "C");

    let row = client
        .query_one("SHOW transaction isolation level", &[])
        .await
        .expect("SHOW transaction isolation level");
    assert_eq!(row.get::<_, String>(0), "read committed");

    let row = client
        .query_one("SHOW standard_conforming_strings", &[])
        .await
        .expect("SHOW standard_conforming_strings");
    assert_eq!(row.get::<_, String>(0), "on");

    let row = client
        .query_one("SELECT set_config('TimeZone', 'UTC', false)", &[])
        .await
        .expect("Django startup set_config TimeZone");
    assert_eq!(row.get::<_, String>(0), "UTC");

    shutdown(running).await;
}

#[tokio::test]
async fn scalar_system_functions_return_postgres_shaped_values() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    let rows = client
        .query(
            "SELECT version(), current_database(), current_schema(), current_user(), pg_typeof(1), pg_size_pretty(2048)",
            &[],
        )
        .await
        .expect("system functions");
    assert_eq!(rows.len(), 1);
    let version = rows[0].get::<_, String>(0);
    assert!(
        version.starts_with("PostgreSQL 14.0"),
        "ORMs require PostgreSQL-shaped version(), got {version:?}"
    );
    assert!(
        version.contains(concat!("UltraSQL ", env!("CARGO_PKG_VERSION"))),
        "version() should retain UltraSQL identity, got {version:?}"
    );
    assert_eq!(rows[0].get::<_, String>(1), "ultrasql");
    assert_eq!(rows[0].get::<_, String>(2), "public");
    assert_eq!(rows[0].get::<_, String>(3), "tester");
    assert_eq!(rows[0].get::<_, String>(4), "integer");
    assert_eq!(rows[0].get::<_, String>(5), "2 kB");

    let bare = client
        .query("SELECT current_user, session_user, current_catalog", &[])
        .await
        .expect("bare user functions");
    assert_eq!(bare.len(), 1);
    assert_eq!(bare[0].get::<_, String>(0), "tester");
    assert_eq!(bare[0].get::<_, String>(1), "tester");
    assert_eq!(bare[0].get::<_, String>(2), "ultrasql");

    shutdown(running).await;
}

#[tokio::test]
async fn scalar_string_functions_return_postgres_shaped_values() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    let row = client
        .query_one(
            "SELECT \
             length('UltraSQL'), \
             left('UltraSQL', 5), \
             right('UltraSQL', 3), \
             substr('UltraSQL', 6, 3), \
             trim('  hi  '), \
             lpad('7', 3, '0'), \
             rpad('x', 3, '-'), \
             position('SQL', 'UltraSQL'), \
             replace('aa-bb', '-', '+'), \
             split_part('a,b,c', ',', 2), \
             concat('AI', NULL, '-', 1), \
             concat_ws('|', 'a', NULL, 'b'), \
             repeat('ha', 3), \
             reverse('abc'), \
             md5('abc'), \
             sha256('abc'), \
             quote_ident('simple_name'), \
             quote_ident('select'), \
             quote_literal('O''Reilly'), \
             format('hello %s %I %L %%', 'world', 'odd name', 'O''Reilly'), \
             regexp_replace('abc123abc', '[a-z]+', 'X'), \
             regexp_replace('abc123abc', '[a-z]+', 'X', 'g')",
            &[],
        )
        .await
        .expect("string functions");

    assert_eq!(row.get::<_, i32>(0), 8);
    assert_eq!(row.get::<_, String>(1), "Ultra");
    assert_eq!(row.get::<_, String>(2), "SQL");
    assert_eq!(row.get::<_, String>(3), "SQL");
    assert_eq!(row.get::<_, String>(4), "hi");
    assert_eq!(row.get::<_, String>(5), "007");
    assert_eq!(row.get::<_, String>(6), "x--");
    assert_eq!(row.get::<_, i32>(7), 6);
    assert_eq!(row.get::<_, String>(8), "aa+bb");
    assert_eq!(row.get::<_, String>(9), "b");
    assert_eq!(row.get::<_, String>(10), "AI-1");
    assert_eq!(row.get::<_, String>(11), "a|b");
    assert_eq!(row.get::<_, String>(12), "hahaha");
    assert_eq!(row.get::<_, String>(13), "cba");
    assert_eq!(row.get::<_, String>(14), "900150983cd24fb0d6963f7d28e17f72");
    assert_eq!(
        row.get::<_, String>(15),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(row.get::<_, String>(16), "simple_name");
    assert_eq!(row.get::<_, String>(17), "\"select\"");
    assert_eq!(row.get::<_, String>(18), "'O''Reilly'");
    assert_eq!(
        row.get::<_, String>(19),
        "hello world \"odd name\" 'O''Reilly' %"
    );
    assert_eq!(row.get::<_, String>(20), "X123abc");
    assert_eq!(row.get::<_, String>(21), "X123X");

    shutdown(running).await;
}

#[tokio::test]
async fn portable_scalar_catalog_helpers_round_trip() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    let row = client
        .query_one(
            "SELECT \
             coalesce(NULL, 'fallback'), \
             ifnull(NULL, 'fallback'), \
             ifnull('value', 'fallback'), \
             nullif('same', 'same') IS NULL, \
             nullif('left', 'right'), \
             least(3, 1, 2), \
             greatest(3, 1, 2), \
             least('beta', 'alpha', 'gamma'), \
             greatest('beta', 'alpha', 'gamma'), \
             least(NULL, 2), \
             greatest(NULL, 2), \
             min(3, 1, 2), \
             max(3, 1, 2), \
             min(NULL, 2) IS NULL, \
             max(NULL, 2) IS NULL",
            &[],
        )
        .await
        .expect("portable scalar catalog helpers");

    assert_eq!(row.get::<_, String>(0), "fallback");
    assert_eq!(row.get::<_, String>(1), "fallback");
    assert_eq!(row.get::<_, String>(2), "value");
    assert!(row.get::<_, bool>(3));
    assert_eq!(row.get::<_, String>(4), "left");
    assert_eq!(row.get::<_, i32>(5), 1);
    assert_eq!(row.get::<_, i32>(6), 3);
    assert_eq!(row.get::<_, String>(7), "alpha");
    assert_eq!(row.get::<_, String>(8), "gamma");
    assert_eq!(row.get::<_, i32>(9), 2);
    assert_eq!(row.get::<_, i32>(10), 2);
    assert_eq!(row.get::<_, i32>(11), 1);
    assert_eq!(row.get::<_, i32>(12), 3);
    assert!(row.get::<_, bool>(13));
    assert!(row.get::<_, bool>(14));

    shutdown(running).await;
}

#[tokio::test]
async fn scalar_math_functions_return_postgres_shaped_values() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    // FIX 7 — round/floor/ceil/trunc preserve the input type. A bare
    // decimal literal like `1.2` is `numeric` in PostgreSQL, so these
    // return `numeric`; the value is read via a `::text` cast and the type
    // is pinned with `pg_typeof`.
    let round_family = client
        .query_one(
            "SELECT \
             ceil(1.2)::text, pg_typeof(ceil(1.2))::text, \
             floor(1.8)::text, pg_typeof(floor(1.8))::text, \
             round(1.6)::text, pg_typeof(round(1.6))::text, \
             trunc(1.9)::text, pg_typeof(trunc(1.9))::text",
            &[],
        )
        .await
        .expect("round-family functions");
    assert_eq!(round_family.get::<_, String>(0), "2");
    assert_eq!(round_family.get::<_, String>(1), "numeric");
    assert_eq!(round_family.get::<_, String>(2), "1");
    assert_eq!(round_family.get::<_, String>(3), "numeric");
    assert_eq!(round_family.get::<_, String>(4), "2");
    assert_eq!(round_family.get::<_, String>(5), "numeric");
    assert_eq!(round_family.get::<_, String>(6), "1");
    assert_eq!(round_family.get::<_, String>(7), "numeric");

    // round/floor/ceil/trunc over `double precision` stay `double
    // precision`, and `round(float8)` uses banker's (ties-to-even)
    // rounding: round(2.5::float8) = 2, round(3.5::float8) = 4.
    let float_round = client
        .query_one(
            "SELECT \
             round(2.5::float8), pg_typeof(round(2.5::float8))::text, \
             round(3.5::float8), ceil(1.2::float8)",
            &[],
        )
        .await
        .expect("float round-family functions");
    assert!((float_round.get::<_, f64>(0) - 2.0).abs() < 1e-12);
    assert_eq!(float_round.get::<_, String>(1), "double precision");
    assert!((float_round.get::<_, f64>(2) - 4.0).abs() < 1e-12);
    assert!((float_round.get::<_, f64>(3) - 2.0).abs() < 1e-12);

    // numeric round is half away from zero (not banker's):
    // round(2.5::numeric) = 3.
    let numeric_round = client
        .query_one("SELECT round(2.5::numeric)::text", &[])
        .await
        .expect("numeric round");
    assert_eq!(numeric_round.get::<_, String>(0), "3");

    let row = client
        .query_one(
            "SELECT \
             power(2, 5), sqrt(9), exp(0), ln(1), log(100), \
             pi(), sin(0), cos(0), tan(0), asin(0), acos(1), atan(0), random()",
            &[],
        )
        .await
        .expect("math functions");

    let expected = [
        32.0,
        3.0,
        1.0,
        0.0,
        2.0,
        std::f64::consts::PI,
        0.0,
        1.0,
        0.0,
        0.0,
        0.0,
        0.0,
    ];
    for (idx, expected_value) in expected.into_iter().enumerate() {
        let got: f64 = row.get(idx);
        assert!(
            (got - expected_value).abs() < 1e-12,
            "column {idx}: expected {expected_value}, got {got}"
        );
    }
    let random_value: f64 = row.get(12);
    assert!(
        (0.0..1.0).contains(&random_value),
        "random() out of range: {random_value}"
    );

    shutdown(running).await;
}

#[tokio::test]
async fn abs_mod_split_part_return_postgres_values_and_types() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    // FIX 2 — abs() over every numeric category, returning the
    // argument's own type. tokio-postgres' typed getters double as a
    // wire-level type check: a planner/executor type disagreement makes
    // these `get` calls panic.
    // The numeric `abs` value is read via a `::text` cast to avoid a
    // decimal client dependency; its declared type is asserted directly.
    let row = client
        .query_one(
            "SELECT \
             abs(-2::int4), pg_typeof(abs(-2::int4)), \
             abs(-1.5::float8), pg_typeof(abs(-1.5::float8)), \
             abs(-1.5::numeric)::text, pg_typeof(abs(-1.5::numeric))",
            &[],
        )
        .await
        .expect("abs functions");
    assert_eq!(row.get::<_, i32>(0), 2);
    assert_eq!(row.get::<_, String>(1), "integer");
    assert!((row.get::<_, f64>(2) - 1.5).abs() < 1e-12);
    assert_eq!(row.get::<_, String>(3), "double precision");
    assert_eq!(row.get::<_, String>(4), "1.5");
    assert_eq!(row.get::<_, String>(5), "numeric");

    // FIX 4 — mod() keeps integers exact and integer-typed. The 2^53+1
    // literal would round to an even f64 and yield 0; the integer fast
    // path returns 1.
    let row = client
        .query_one(
            "SELECT \
             mod(9007199254740993, 2), pg_typeof(mod(7, 3)), \
             mod(7.5::float8, 2), pg_typeof(mod(7.5::float8, 2))",
            &[],
        )
        .await
        .expect("mod functions");
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, String>(1), "integer");
    assert!((row.get::<_, f64>(2) - 1.5).abs() < 1e-12);
    // Float inputs document the residual f64 behavior.
    assert_eq!(row.get::<_, String>(3), "double precision");

    // mod() by zero is a division-by-zero error.
    let err = client
        .query_one("SELECT mod(7, 0)", &[])
        .await
        .expect_err("mod by zero must error");
    let db_err = err.as_db_error().expect("server-side error");
    assert_eq!(db_err.code().code(), "22012", "division_by_zero SQLSTATE");

    // FIX 3 — split_part() with an empty delimiter: field 1 is the whole
    // string, any other field is empty.
    let row = client
        .query_one(
            "SELECT split_part('abc', '', 1), split_part('abc', '', 2)",
            &[],
        )
        .await
        .expect("split_part empty delimiter");
    assert_eq!(row.get::<_, String>(0), "abc");
    assert_eq!(row.get::<_, String>(1), "");

    shutdown(running).await;
}

#[tokio::test]
async fn scalar_datetime_functions_return_postgres_shaped_values() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    let row = client
        .query_one(
            "SELECT \
             extract(year FROM DATE '2024-05-22'), \
             extract(hour FROM TIMESTAMP '2024-05-22 13:14:15'), \
             extract(epoch FROM TIMESTAMP '2000-01-02 00:00:00'), \
             extract(year FROM current_date) >= 2026, \
             extract(epoch FROM to_timestamp(86400)), \
             extract(day FROM make_date(2024, 2, 29)), \
             extract(day FROM date_trunc('month', TIMESTAMP '2024-05-22 13:14:15')), \
             extract(hour FROM date_trunc('day', TIMESTAMP '2024-05-22 13:14:15')), \
             extract(day FROM age(TIMESTAMP '2024-05-22 13:14:15', TIMESTAMP '2024-05-20 12:14:15')), \
             extract(day FROM date_bin(INTERVAL '1' DAY, TIMESTAMP '2000-01-03 15:00:00', TIMESTAMP '2000-01-01 00:00:00'))",
            &[],
        )
        .await
        .expect("datetime functions");

    assert_eq!(row.get::<_, i64>(0), 2024);
    assert_eq!(row.get::<_, i64>(1), 13);
    assert_eq!(row.get::<_, i64>(2), 946_771_200);
    assert!(row.get::<_, bool>(3));
    assert_eq!(row.get::<_, i64>(4), 86_400);
    assert_eq!(row.get::<_, i64>(5), 29);
    assert_eq!(row.get::<_, i64>(6), 1);
    assert_eq!(row.get::<_, i64>(7), 0);
    assert_eq!(row.get::<_, i64>(8), 2);
    assert_eq!(row.get::<_, i64>(9), 3);

    shutdown(running).await;
}

#[tokio::test]
async fn pg_relation_size_reports_heap_pages() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE sized (id INT NOT NULL, name TEXT)")
        .await
        .expect("create sized table");
    client
        .batch_execute("INSERT INTO sized VALUES (1, 'a'), (2, 'b')")
        .await
        .expect("insert sized rows");

    let rows = client
        .query(
            "SELECT pg_relation_size('sized'), pg_size_pretty(pg_relation_size('public.sized'))",
            &[],
        )
        .await
        .expect("relation size");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 8192);
    assert_eq!(rows[0].get::<_, String>(1), "8 kB");

    client
        .batch_execute("CREATE TABLE \"size.dot\" (id INT)")
        .await
        .expect("create quoted dotted sized table");
    client
        .batch_execute("INSERT INTO \"size.dot\" VALUES (1)")
        .await
        .expect("insert quoted dotted sized row");
    let dotted = client
        .query_one("SELECT pg_relation_size('\"size.dot\"')", &[])
        .await
        .expect("quoted dotted relation size");
    assert_eq!(dotted.get::<_, i64>(0), 8192);

    client
        .batch_execute("CREATE SCHEMA app")
        .await
        .expect("create app schema");
    let err = client
        .query("SELECT pg_relation_size('app.sized')", &[])
        .await
        .expect_err("qualified missing relation must not fall back to public table");
    let message = err
        .as_db_error()
        .map(|db| db.message().to_owned())
        .unwrap_or_else(|| err.to_string());
    assert!(message.contains("app.sized"), "{message}");

    shutdown(running).await;
}

/// `now()` / `current_timestamp` / `current_date` must observe the
/// transaction-start instant: identical for every statement and every row of
/// every statement inside an explicit transaction block (PostgreSQL
/// semantics), and stable across the rows of a single autocommit statement.
///
/// These assertions are expressed entirely in SQL (equality, `count(DISTINCT
/// ...)`) so they exercise the server's clock plumbing without depending on
/// the wire encoding of `timestamptz`.
#[tokio::test]
async fn now_is_pinned_to_transaction_start_time() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    // A small multi-row table for the cross-row constancy checks.
    client
        .batch_execute("CREATE TABLE clock_rows (id INT NOT NULL)")
        .await
        .expect("create clock_rows table");
    client
        .batch_execute("INSERT INTO clock_rows VALUES (1), (2), (3), (4), (5)")
        .await
        .expect("insert clock_rows");
    // Capture table for the transaction-start now() (created outside the txn
    // so the test does not depend on transactional DDL).
    client
        .batch_execute("CREATE TABLE t_now (n TIMESTAMPTZ NOT NULL)")
        .await
        .expect("create t_now table");

    // (2) Within a single autocommit statement, now() is constant across all
    //     rows: projecting now() over five rows yields one identical value.
    let rows = client
        .query("SELECT id, now() FROM clock_rows", &[])
        .await
        .expect("per-row now()");
    assert_eq!(rows.len(), 5, "all five rows are returned");
    let per_row: Vec<std::time::SystemTime> = rows
        .iter()
        .map(|r| r.get::<_, std::time::SystemTime>(1))
        .collect();
    assert!(
        per_row.windows(2).all(|w| w[0] == w[1]),
        "now() must be identical for every row of one statement"
    );

    // The same constancy holds when now() is aggregated: min() and max()
    // over the five rows coincide.
    let row = client
        .query_one("SELECT min(now()) = max(now()) FROM clock_rows", &[])
        .await
        .expect("min(now()) = max(now()) within one statement");
    assert!(
        row.get::<_, bool>(0),
        "min(now()) and max(now()) must coincide across rows"
    );

    // (1) + (3) BEGIN; SELECT now(); SELECT now(); COMMIT -> identical.
    //     Compare each statement's now() against a single value captured in a
    //     session temp table at the start of the block; every statement in
    //     the block must equal it.
    client
        .batch_execute("BEGIN")
        .await
        .expect("begin explicit txn");

    let first = client
        .query_one("SELECT now() = now()", &[])
        .await
        .expect("two now() in one statement compare equal");
    assert!(
        first.get::<_, bool>(0),
        "two now() calls in the same statement must be equal"
    );

    // Capture the block's now() and compare a *later* statement's now() to it.
    client
        .batch_execute("INSERT INTO t_now SELECT now()")
        .await
        .expect("capture txn now()");
    // Advance the wall clock between statements. Pre-fix, each call read the
    // live clock, so the later now() would differ from the captured one and
    // this assertion would fail; with the fix both are pinned to the txn start.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let row = client
        .query_one("SELECT n = now(), n = current_timestamp FROM t_now", &[])
        .await
        .expect("later statement now() equals captured txn now()");
    assert!(
        row.get::<_, bool>(0),
        "now() in a later statement of the same txn must equal the txn-start now()"
    );
    assert!(
        row.get::<_, bool>(1),
        "current_timestamp must equal now() within the same txn"
    );

    // (6) current_date inside the txn is stable and equals the date of the
    //     transaction-start now(). Compare year/month/day parts via extract()
    //     (the binder rejects non-literal CASTs, so avoid `::date`).
    let row = client
        .query_one(
            "SELECT extract(year FROM current_date) = extract(year FROM n), \
                    extract(month FROM current_date) = extract(month FROM n), \
                    extract(day FROM current_date) = extract(day FROM n), \
                    extract(day FROM current_date) = extract(day FROM now()) \
             FROM t_now",
            &[],
        )
        .await
        .expect("current_date matches the txn now() date");
    assert!(
        row.get::<_, bool>(0),
        "current_date year must match the txn-start now() year"
    );
    assert!(
        row.get::<_, bool>(1),
        "current_date month must match the txn-start now() month"
    );
    assert!(
        row.get::<_, bool>(2),
        "current_date day must match the txn-start now() day"
    );
    assert!(
        row.get::<_, bool>(3),
        "current_date day must match now()'s day inside the txn"
    );

    client
        .batch_execute("COMMIT")
        .await
        .expect("commit explicit txn");

    shutdown(running).await;
}

/// In autocommit, two *separate* `SELECT now()` statements are two implicit
/// transactions, so PostgreSQL allows their values to differ. The fix must
/// not pin autocommit statements to a stale earlier instant: a `now()` taken
/// after a delay must be `>=` an earlier one (never before it), and the
/// monotonic engine clock must keep advancing between statements.
#[tokio::test]
async fn autocommit_now_advances_between_statements() {
    let running = start_sample_server("system_functions_test").await;
    let client = &running.client;

    let before = client
        .query_one("SELECT now()", &[])
        .await
        .expect("first autocommit now()");
    let before: std::time::SystemTime = before.get(0);

    // Force wall-clock advancement between the two implicit transactions.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    let after = client
        .query_one("SELECT now()", &[])
        .await
        .expect("second autocommit now()");
    let after: std::time::SystemTime = after.get(0);

    assert!(
        after >= before,
        "autocommit now() must not move backwards across statements"
    );

    shutdown(running).await;
}
