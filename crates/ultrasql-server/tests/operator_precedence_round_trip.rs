//! End-to-end verification of PostgreSQL operator-precedence fixes,
//! array-type casts, and `DEFAULT` in `INSERT ... VALUES`.
//!
//! Every expected value in the precedence battery was computed against a
//! real PostgreSQL 14 server via `psql`; the value reveals the parse-tree
//! grouping. The cases cover the four bug fixes:
//!
//! - Bug #1: postfix `IS` / `IN` / `BETWEEN` honour minimum precedence, so
//!   they no longer wrongly attach to the RHS of a tighter operator.
//! - Bug #33: bitwise shift (`<<` `>>`) groups with the other non-arithmetic
//!   operators (bitwise/concat/JSON) at one PostgreSQL level, looser than
//!   `+`/`-`.
//! - Bug #2: `::type[]` and `CAST(x AS type[])` parse and bind to an array
//!   type.
//! - Bug #19: `DEFAULT` is accepted as a `VALUES` cell.

pub mod support;

use support::{shutdown, start_sample_server};

/// Assert a scalar `SELECT <expr>` returns the given boolean.
macro_rules! assert_bool {
    ($client:expr, $sql:expr, $expected:expr) => {{
        let rows = $client
            .query(&format!("SELECT {}", $sql), &[])
            .await
            .unwrap_or_else(|e| panic!("query `{}` failed: {e}", $sql));
        assert_eq!(rows.len(), 1, "`{}` should be one row", $sql);
        assert_eq!(
            rows[0].get::<_, bool>(0),
            $expected,
            "`{}` grouping differs from PostgreSQL",
            $sql
        );
    }};
}

/// Assert a scalar `SELECT <expr>` returns the given i32.
macro_rules! assert_i32 {
    ($client:expr, $sql:expr, $expected:expr) => {{
        let rows = $client
            .query(&format!("SELECT {}", $sql), &[])
            .await
            .unwrap_or_else(|e| panic!("query `{}` failed: {e}", $sql));
        assert_eq!(rows.len(), 1, "`{}` should be one row", $sql);
        assert_eq!(
            rows[0].get::<_, i32>(0),
            $expected,
            "`{}` grouping differs from PostgreSQL",
            $sql
        );
    }};
}

/// Twenty-plus mixed-operator expressions whose values were confirmed in
/// real PostgreSQL 14. Each value pins down the grouping.
#[tokio::test]
async fn precedence_battery_matches_postgres() {
    let running = start_sample_server("precedence_battery").await;
    let client = &running.client;

    // ── Bug #1: postfix IS / IN / BETWEEN vs tighter operators ──────────
    // `1+2 IN (3)` = (1+2) IN (3) = true (not 1 + (2 IN (3))).
    assert_bool!(client, "1 + 2 IN (3)", true);
    // `'a'||'b' IS NOT NULL` = ('a'||'b') IS NOT NULL = true.
    assert_bool!(client, "'a' || 'b' IS NOT NULL", true);
    // `(1=1) IS TRUE` = true.
    assert_bool!(client, "(1 = 1) IS TRUE", true);
    // `2*3 BETWEEN 5 AND 7` = (2*3) BETWEEN 5 AND 7 = true.
    assert_bool!(client, "2 * 3 BETWEEN 5 AND 7", true);
    // `1 + 2 BETWEEN 2 AND 4` = (1+2) BETWEEN 2 AND 4 = true.
    assert_bool!(client, "1 + 2 BETWEEN 2 AND 4", true);
    // IS binds looser than comparison: `1 < 2 IS TRUE` = (1<2) IS TRUE.
    assert_bool!(client, "1 < 2 IS TRUE", true);
    // BETWEEN binds tighter than comparison: `2 BETWEEN 1 AND 3 = true`.
    assert_bool!(client, "2 BETWEEN 1 AND 3 = true", true);
    // AND binds looser than BETWEEN: (2 BETWEEN 1 AND 5) AND false = false.
    assert_bool!(client, "2 BETWEEN 1 AND 5 AND false", false);
    // NOT binds looser than IS: NOT (true IS FALSE) = NOT false = true.
    assert_bool!(client, "NOT true IS FALSE", true);

    // ── Bug #33: shift groups with bitwise/concat, looser than +/- ──────
    // (1 # 2) << 1 = 3 << 1 = 6.
    assert_i32!(client, "1 # 2 << 1", 6);
    // (1 | 2) << 1 = 3 << 1 = 6.
    assert_i32!(client, "1 | 2 << 1", 6);
    // (5 & 3) << 1 = 1 << 1 = 2.
    assert_i32!(client, "5 & 3 << 1", 2);
    // +/- tighter than shift: 1 << (2 + 1) = 1 << 3 = 8.
    assert_i32!(client, "1 << 2 + 1", 8);
    // 8 >> (1 + 1) = 8 >> 2 = 2.
    assert_i32!(client, "8 >> 1 + 1", 2);
    // 2 + 3 << 1 = (2 + 3) << 1 = 10.
    assert_i32!(client, "2 + 3 << 1", 10);
    // All band-8 ops are left-assoc: (2 << 1) # 3 = 4 # 3 = 7.
    assert_i32!(client, "2 << 1 # 3", 7);
    // (6 & 3) | 1 = 2 | 1 = 3.
    assert_i32!(client, "6 & 3 | 1", 3);
    // (1 << 2) | 1 = 4 | 1 = 5.
    assert_i32!(client, "1 << 2 | 1", 5);
    // (4 >> 1) & 3 = 2 & 3 = 2.
    assert_i32!(client, "4 >> 1 & 3", 2);

    // ── concat / comparison / unary / power / cast binding ──────────────
    // concat tighter than comparison: ('a'||'b') = 'ab' = true.
    assert_bool!(client, "'a' || 'b' = 'ab'", true);
    // bitwise tighter than comparison: (1 & 3) = 1 = true.
    assert_bool!(client, "1 & 3 = 1", true);
    // binary minus looser than power: 0 - 2^2 = 0 - 4 = -4. `^` yields
    // double precision (PostgreSQL), so read the value as f64.
    {
        let rows = client
            .query("SELECT 0 - 2 ^ 2", &[])
            .await
            .expect("minus-power grouping");
        assert_eq!(rows[0].get::<_, f64>(0), -4.0, "0 - 2^2 grouping");
    }
    // power is left-assoc: (2^2)^3 = 64 (not 2^(2^3) = 256).
    {
        let rows = client
            .query("SELECT 2 ^ 2 ^ 3", &[])
            .await
            .expect("power left-assoc");
        assert_eq!(rows[0].get::<_, f64>(0), 64.0, "2^2^3 grouping");
    }
    // `::` binds tighter than `||`: ('1' || (2::text)) so result is '12'.
    {
        let rows = client
            .query("SELECT '1' || 2::text", &[])
            .await
            .expect("concat-cast");
        assert_eq!(rows[0].get::<_, String>(0), "12");
    }

    shutdown(running).await;
}

/// Array-type casts (`::type[]`, `CAST(x AS type[])`) parse, bind to a real
/// array type, and execute. Assertions go through the text protocol
/// (`simple_query`); the array's reported column type OID is checked via the
/// typed protocol.
#[tokio::test]
async fn array_type_casts_round_trip() {
    use tokio_postgres::types::Type;

    let running = start_sample_server("array_type_casts").await;
    let client = &running.client;

    // Helper: run via the text protocol and return the single text cell.
    async fn scalar_text(client: &tokio_postgres::Client, sql: &str) -> String {
        let msgs = client
            .simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
        for m in &msgs {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                return r.get(0).expect("cell").to_owned();
            }
        }
        panic!("`{sql}` returned no row");
    }

    // `'{1,2,3}'::int[]` — text literal coerced to int array.
    assert_eq!(
        scalar_text(client, "SELECT '{1,2,3}'::int[]").await,
        "{1,2,3}"
    );
    // `ARRAY[1,2]::int[]` — array literal cast to its own element type.
    assert_eq!(
        scalar_text(client, "SELECT ARRAY[1,2]::int[]").await,
        "{1,2}"
    );
    // `CAST('{a,b}' AS text[])` — prefix CAST form.
    assert_eq!(
        scalar_text(client, "SELECT CAST('{a,b}' AS text[])").await,
        "{a,b}"
    );
    // `::int[3]` — PostgreSQL ignores the declared size.
    assert_eq!(
        scalar_text(client, "SELECT '{1,2,3}'::int[3]").await,
        "{1,2,3}"
    );

    // The cast binds to a real array type: the column OID is the int4 array.
    let rows = client
        .query("SELECT '{1,2,3}'::int[]", &[])
        .await
        .expect("int[] cast column metadata");
    assert_eq!(
        rows[0].columns()[0].type_(),
        &Type::INT4_ARRAY,
        "cast should bind to the integer array type"
    );

    shutdown(running).await;
}

/// `DEFAULT` keyword inside `INSERT ... VALUES`.
#[tokio::test]
async fn insert_default_in_values_round_trip() {
    let running = start_sample_server("insert_default_values").await;
    let client = &running.client;

    client
        .batch_execute("CREATE TABLE t (a INT, b INT DEFAULT 99)")
        .await
        .expect("create table");

    // Single-row: `(1, DEFAULT)` fills b with 99.
    client
        .batch_execute("INSERT INTO t(a, b) VALUES (1, DEFAULT)")
        .await
        .expect("insert default");
    // Multi-row: one DEFAULT, one explicit.
    client
        .batch_execute("INSERT INTO t(a, b) VALUES (2, DEFAULT), (3, 5)")
        .await
        .expect("insert multi default");

    let rows = client
        .query("SELECT a, b FROM t ORDER BY a", &[])
        .await
        .expect("select back");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i32>(1), 99);
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, i32>(1), 99);
    assert_eq!(rows[2].get::<_, i32>(0), 3);
    assert_eq!(rows[2].get::<_, i32>(1), 5);

    // DEFAULT for a column with no declared default yields NULL.
    client
        .batch_execute("INSERT INTO t(a, b) VALUES (DEFAULT, 7)")
        .await
        .expect("insert default no-default-column");
    let rows = client
        .query("SELECT a FROM t WHERE b = 7", &[])
        .await
        .expect("select null default");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].try_get::<_, i32>(0).is_err(), "a should be NULL");

    shutdown(running).await;
}

/// `DEFAULT` outside a `VALUES` row is still a syntax error.
#[tokio::test]
async fn default_outside_values_is_rejected() {
    let running = start_sample_server("default_outside_values").await;
    let client = &running.client;

    let err = client.query("SELECT DEFAULT", &[]).await.err();
    assert!(err.is_some(), "SELECT DEFAULT must error");

    let err = client.query("SELECT 1 + DEFAULT", &[]).await.err();
    assert!(err.is_some(), "DEFAULT in an expression must error");

    shutdown(running).await;
}
