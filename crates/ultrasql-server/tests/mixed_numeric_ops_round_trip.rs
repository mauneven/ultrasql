//! End-to-end coverage for mixed integer/float comparison and arithmetic and
//! the `^` (power) operator, matching PostgreSQL 14 semantics:
//!   * integer-vs-float column comparisons compare numerically (return bool),
//!   * mixed integer/float arithmetic promotes to double precision (float8),
//!   * `^` returns double precision and does not overflow on integer operands.

pub mod support;

use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

/// Wire OID for `double precision` (float8) in PostgreSQL.
const FLOAT8_OID: u32 = 701;
/// Wire OID for `boolean` in PostgreSQL.
const BOOL_OID: u32 = 16;
/// Wire OID for `real` (float4) in PostgreSQL.
const FLOAT4_OID: u32 = 700;

fn single_row(messages: Vec<SimpleQueryMessage>) -> tokio_postgres::SimpleQueryRow {
    messages
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("one row")
}

#[tokio::test]
async fn mixed_int_float_comparison_arithmetic_and_power() {
    let running = start_sample_server("mixed_numeric_ops_round_trip").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (i INT, b BIGINT, f FLOAT8, r REAL)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (3, 5, 2.5, 2.5)")
        .await
        .expect("insert row");

    // BUG 1: integer-vs-float comparisons compare numerically (return bool):
    // 3 < 2.5 -> false, 3 = 2.5 -> false, 2.5 > 3 -> false, 3 IN (2.5) -> false.
    let row = single_row(
        client
            .simple_query(
                "SELECT (i < f) AS i_lt_f, (i = r) AS i_eq_r, (f > i) AS f_gt_i, \
                        (i IN (f)) AS i_in_f FROM t",
            )
            .await
            .expect("comparison query"),
    );
    assert_eq!(row.get("i_lt_f"), Some("f"));
    assert_eq!(row.get("i_eq_r"), Some("f"));
    assert_eq!(row.get("f_gt_i"), Some("f"));
    assert_eq!(row.get("i_in_f"), Some("f"));

    // greatest(i, f) compares numerically and returns the larger value (3),
    // declared as double precision (PG behaviour).
    let row = single_row(
        client
            .simple_query("SELECT greatest(i, f) AS g FROM t")
            .await
            .expect("greatest query"),
    );
    assert_eq!(row.get("g"), Some("3"));

    // BUG 2: mixed integer/float arithmetic -> double precision, in both
    // operand orders and for both `real` and `float8`.
    let row = single_row(
        client
            .simple_query(
                "SELECT (b + f) AS bf, (f * b) AS fb, (r + i) AS ri, (i * r) AS ir FROM t",
            )
            .await
            .expect("arithmetic query"),
    );
    assert_eq!(row.get("bf"), Some("7.5")); // bigint + float8
    assert_eq!(row.get("fb"), Some("12.5")); // float8 * bigint
    assert_eq!(row.get("ri"), Some("5.5")); // real + int4 -> float8
    assert_eq!(row.get("ir"), Some("7.5")); // int4 * real -> float8

    // Extended protocol: confirm wire types -- bool for the comparison,
    // double precision (float8) for both the comparison's float arithmetic and
    // the `real + int4` arithmetic.
    let rows = client
        .query(
            "SELECT (i < f) AS cmp, (r + i) AS sum, greatest(i, f) AS g, (r + r) AS rr FROM t",
            &[],
        )
        .await
        .expect("extended type query");
    assert_eq!(rows[0].columns()[0].type_().oid(), BOOL_OID);
    assert_eq!(rows[0].columns()[1].type_().oid(), FLOAT8_OID);
    assert_eq!(rows[0].columns()[2].type_().oid(), FLOAT8_OID);
    // `real + real` stays `real` (we promote only mixed int/float to float8).
    assert_eq!(rows[0].columns()[3].type_().oid(), FLOAT4_OID);

    // BUG 3: `^` returns double precision and does not overflow.
    // 2 ^ 3 = 8, 10 ^ 19 = 1e19 (integer power would overflow), 2 ^ 0.5 =
    // sqrt(2), 2.5::numeric ^ 2 = 6.25 (returned as double precision).
    let row = single_row(
        client
            .simple_query(
                "SELECT (2 ^ 3) AS p1, (10 ^ 19) AS p2, (2 ^ 0.5) AS p3, \
                        (2.5::numeric ^ 2) AS p4",
            )
            .await
            .expect("power query"),
    );
    assert_eq!(row.get("p1"), Some("8"));
    // 1e19 exactly; no integer overflow error.
    assert_eq!(row.get("p2"), Some("10000000000000000000"));
    assert_eq!(row.get("p3"), Some("1.4142135623730951"));
    assert_eq!(row.get("p4"), Some("6.25"));

    let rows = client
        .query("SELECT (2 ^ 3) AS p", &[])
        .await
        .expect("extended power type query");
    assert_eq!(rows[0].columns()[0].type_().oid(), FLOAT8_OID);

    shutdown(running).await;
}
