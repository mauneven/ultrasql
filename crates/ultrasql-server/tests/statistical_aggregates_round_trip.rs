//! Wire-level tests for the statistical aggregates added in v0.5.
//!
//! Closes the v0.5 ROADMAP item "Statistical aggregates: STDDEV,
//! VARIANCE" plus the ordered-set percentile aggregates.
//!
//! Each test issues the aggregate against a known input and asserts
//! the floating-point result inside a small tolerance.

mod support;

use support::{shutdown, start_sample_server};

const SEED_SQL: &str =
    "INSERT INTO t VALUES (1, 2), (2, 4), (3, 4), (4, 4), (5, 5), (6, 5), (7, 7), (8, 9)";

async fn seed(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE t (id INT NOT NULL, val INT NOT NULL)")
        .await
        .expect("create");
    client.batch_execute(SEED_SQL).await.expect("seed");
}

fn approx_eq(a: f64, b: f64, eps: f64) {
    assert!((a - b).abs() < eps, "expected {b} ± {eps}, got {a}");
}

/// `STDDEV(val)` and `STDDEV_SAMP(val)` are aliases and yield the
/// PostgreSQL-faithful sample standard deviation.
#[tokio::test]
async fn stddev_samp_matches_postgres() {
    let running = start_sample_server("stats_test").await;
    let client = &running.client;
    seed(client).await;

    let r = client
        .query_one("SELECT STDDEV(val) FROM t", &[])
        .await
        .expect("STDDEV");
    let got: f64 = r.get(0);
    approx_eq(got, 2.138_089_935, 1e-6);

    let r = client
        .query_one("SELECT STDDEV_SAMP(val) FROM t", &[])
        .await
        .expect("STDDEV_SAMP");
    let got: f64 = r.get(0);
    approx_eq(got, 2.138_089_935, 1e-6);

    shutdown(running).await;
}

/// `STDDEV_POP(val)` divides by N rather than N-1.
#[tokio::test]
async fn stddev_pop_matches_postgres() {
    let running = start_sample_server("stats_test").await;
    let client = &running.client;
    seed(client).await;

    let r = client
        .query_one("SELECT STDDEV_POP(val) FROM t", &[])
        .await
        .expect("STDDEV_POP");
    let got: f64 = r.get(0);
    approx_eq(got, 2.0, 1e-6);

    shutdown(running).await;
}

/// `VARIANCE(val)` and `VAR_SAMP(val)` are aliases and yield sample
/// variance.
#[tokio::test]
async fn variance_samp_matches_postgres() {
    let running = start_sample_server("stats_test").await;
    let client = &running.client;
    seed(client).await;

    let r = client
        .query_one("SELECT VARIANCE(val) FROM t", &[])
        .await
        .expect("VARIANCE");
    let got: f64 = r.get(0);
    approx_eq(got, 4.571_428_571, 1e-6);

    let r = client
        .query_one("SELECT VAR_SAMP(val) FROM t", &[])
        .await
        .expect("VAR_SAMP");
    let got: f64 = r.get(0);
    approx_eq(got, 4.571_428_571, 1e-6);

    shutdown(running).await;
}

/// `VAR_POP(val)` divides by N rather than N-1.
#[tokio::test]
async fn var_pop_matches_postgres() {
    let running = start_sample_server("stats_test").await;
    let client = &running.client;
    seed(client).await;

    let r = client
        .query_one("SELECT VAR_POP(val) FROM t", &[])
        .await
        .expect("VAR_POP");
    let got: f64 = r.get(0);
    approx_eq(got, 4.0, 1e-6);

    shutdown(running).await;
}

/// `CORR(y, x)` returns the Pearson correlation coefficient.
#[tokio::test]
async fn corr_matches_postgres() {
    let running = start_sample_server("stats_test").await;
    let client = &running.client;
    seed(client).await;

    let r = client
        .query_one("SELECT CORR(val, id) FROM t", &[])
        .await
        .expect("CORR");
    let got: f64 = r.get(0);
    approx_eq(got, 0.927_426_033_5, 1e-9);

    shutdown(running).await;
}

/// Ordered-set percentiles use the direct argument as the fraction and
/// the `WITHIN GROUP` sort expression as the sample. `PERCENTILE_CONT`
/// interpolates and returns `float8`; `PERCENTILE_DISC` returns the
/// first ordered input whose cumulative distribution meets the fraction.
#[tokio::test]
async fn ordered_set_percentiles_match_postgres() {
    let running = start_sample_server("stats_test").await;
    let client = &running.client;
    seed(client).await;

    let r = client
        .query_one(
            "SELECT \
             PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val), \
             PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) \
             FROM t",
            &[],
        )
        .await
        .expect("ordered-set percentiles");

    let continuous: f64 = r.get(0);
    let discrete: i32 = r.get(1);
    approx_eq(continuous, 4.5, 1e-9);
    assert_eq!(discrete, 4);

    let r = client
        .query_one(
            "SELECT \
             PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY val DESC), \
             PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY val DESC) \
             FROM t",
            &[],
        )
        .await
        .expect("ordered-set percentiles descending");

    let continuous_desc: f64 = r.get(0);
    let discrete_desc: i32 = r.get(1);
    approx_eq(continuous_desc, 5.5, 1e-9);
    assert_eq!(discrete_desc, 7);

    shutdown(running).await;
}

/// Empty input yields NULL for both sample and population
/// stddev/variance. PostgreSQL semantics: sample needs ≥ 2
/// non-null inputs, population needs ≥ 1.
#[tokio::test]
async fn stddev_variance_empty_input_returns_null() {
    let running = start_sample_server("stats_test").await;
    let client = &running.client;
    client
        .batch_execute("CREATE TABLE empty_t (val INT)")
        .await
        .expect("create");

    for sql in [
        "SELECT STDDEV(val) FROM empty_t",
        "SELECT STDDEV_POP(val) FROM empty_t",
        "SELECT VARIANCE(val) FROM empty_t",
        "SELECT VAR_POP(val) FROM empty_t",
    ] {
        let r = client.query_one(sql, &[]).await.expect(sql);
        let got: Option<f64> = r.get(0);
        assert!(got.is_none(), "{sql} should be NULL on empty input");
    }

    shutdown(running).await;
}
