//! End-to-end coverage for `CASE` result-branch type reconciliation, matching
//! PostgreSQL 14 semantics: the output type of a `CASE` is the common type of
//! its THEN/ELSE branches, and the value of the taken branch is returned at
//! that declared type — so `CASE … THEN int_col ELSE float8_col END` yields
//! double precision regardless of which branch is taken (it does not error out
//! when the integer branch is selected).

pub mod support;

use support::{shutdown, start_sample_server};
use tokio_postgres::SimpleQueryMessage;

/// Wire OID for `double precision` (float8) in PostgreSQL.
const FLOAT8_OID: u32 = 701;
/// Wire OID for `numeric` in PostgreSQL.
const NUMERIC_OID: u32 = 1700;

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
async fn case_mixed_branch_types_reconcile_to_common_type() {
    let running = start_sample_server("case_common_type_round_trip").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (i INT, f FLOAT8, n NUMERIC)")
        .await
        .expect("create table");
    client
        .batch_execute("INSERT INTO t VALUES (1, 2.5, 3.5)")
        .await
        .expect("insert row");

    // BUG: a taken `THEN int_col` branch against a declared double-precision
    // output type used to reach the projection layer as Int32 and error with
    // "projection: expected Float64 … got Int32". It must instead return the
    // integer value as double precision (1 -> 1.0, displayed "1").
    let row = single_row(
        client
            .simple_query(
                "SELECT CASE WHEN i > 0 THEN i ELSE f END AS then_int, \
                        CASE WHEN i < 0 THEN i ELSE f END AS else_float \
                 FROM t",
            )
            .await
            .expect("searched CASE query"),
    );
    assert_eq!(row.get("then_int"), Some("1")); // int branch taken -> float8 1
    assert_eq!(row.get("else_float"), Some("2.5")); // float branch taken

    // Simple CASE (operand form) reconciles its result branches the same way:
    // `CASE i WHEN 1 THEN i ELSE f END` matches, returns int 1 as double
    // precision.
    let row = single_row(
        client
            .simple_query("SELECT CASE i WHEN 1 THEN i ELSE f END AS s FROM t")
            .await
            .expect("simple CASE query"),
    );
    assert_eq!(row.get("s"), Some("1"));

    // Mixed int/numeric branches reconcile to numeric: the int branch is cast
    // to numeric (1 -> "1"), and the numeric branch passes through ("3.5").
    let row = single_row(
        client
            .simple_query(
                "SELECT CASE WHEN i > 0 THEN i ELSE n END AS then_int_num, \
                        CASE WHEN i < 0 THEN i ELSE n END AS else_num \
                 FROM t",
            )
            .await
            .expect("int/numeric CASE query"),
    );
    assert_eq!(row.get("then_int_num"), Some("1"));
    assert_eq!(row.get("else_num"), Some("3.5"));

    // A NULL branch is ignored for type reconciliation (common type stays
    // float8); the taken float branch returns its value, and a taken NULL
    // branch returns NULL.
    let row = single_row(
        client
            .simple_query(
                "SELECT CASE WHEN i > 0 THEN f ELSE NULL END AS non_null, \
                        CASE WHEN i < 0 THEN f ELSE NULL END AS is_null \
                 FROM t",
            )
            .await
            .expect("NULL branch CASE query"),
    );
    assert_eq!(row.get("non_null"), Some("2.5"));
    assert_eq!(row.get("is_null"), None);

    // Same-type (text) branches are unaffected by the reconciliation.
    let row = single_row(
        client
            .simple_query("SELECT CASE WHEN i > 0 THEN 'a' ELSE 'b' END AS txt FROM t")
            .await
            .expect("text CASE query"),
    );
    assert_eq!(row.get("txt"), Some("a"));

    // Extended protocol: the declared wire type is the reconciled common type,
    // not the taken branch's type -- double precision for int/float8 and
    // numeric for int/numeric, regardless of which branch is selected.
    let rows = client
        .query(
            "SELECT CASE WHEN i > 0 THEN i ELSE f END AS c_float, \
                    CASE WHEN i > 0 THEN i ELSE n END AS c_num \
             FROM t",
            &[],
        )
        .await
        .expect("extended type query");
    assert_eq!(rows[0].columns()[0].type_().oid(), FLOAT8_OID);
    assert_eq!(rows[0].columns()[1].type_().oid(), NUMERIC_OID);

    shutdown(running).await;
}
