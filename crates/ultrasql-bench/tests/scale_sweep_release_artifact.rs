//! Release-artifact scale-sweep contract.

use std::fs;
use std::path::PathBuf;

fn repo_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

fn repo_file(path: &str) -> String {
    let path = repo_path(path);
    fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

/// Full source of the `cross_compare_sql` driver: the binary entrypoint plus
/// every file in its `cross_compare_sql_support/` module directory. Workload
/// definitions are split across those modules, so contract assertions must
/// search the whole tree, not just the entrypoint.
fn cross_compare_sql_driver_source() -> String {
    let mut src = repo_file("crates/ultrasql-bench/src/bin/cross_compare_sql.rs");
    let dir = repo_path("crates/ultrasql-bench/src/bin/cross_compare_sql_support");
    let mut paths: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("read dir {}: {err}", dir.display()))
        .map(|entry| entry.expect("dir entry").path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("rs"))
        .collect();
    paths.sort();
    for p in paths {
        src.push('\n');
        src.push_str(
            &fs::read_to_string(&p).unwrap_or_else(|err| panic!("read {}: {err}", p.display())),
        );
    }
    src
}

#[test]
fn scale_sweep_script_uses_external_release_artifact() {
    let script = repo_file("benchmarks/run_scale_sweep.sh");

    assert!(script.contains("scripts/install.sh"));
    assert!(script.contains("ULTRASQLD_BIN"));
    assert!(script.contains("--server \"127.0.0.1:${port}\""));
    assert!(script.contains("SCALE_SWEEP_PROFILE:-release-ship"));
    assert!(script.contains("cargo build --profile \"$PROFILE\""));
    assert!(script.contains("\"status\": \"not_available\""));
    assert!(script.contains("SCALE_SWEEP_APPEND"));
    assert!(script.contains("SCALE_SWEEP_STORAGE"));
    assert!(script.contains("SCALE_SWEEP_DATA_ROOT"));
    assert!(script.contains("--data-dir \"$data_dir\""));
    assert!(script.contains("--storage-mode \"$STORAGE_MODE\""));
    assert!(script.contains("\"ultrasql_storage_mode\""));
    assert!(script.contains("\"host\""));
    assert!(script.contains("\"engine_versions\""));
    assert!(script.contains("benchmarks/scripts/render_scale_sweep.py"));
    assert!(script.contains("run_ultrasql_fresh_insert_samples"));
    assert!(script.contains("10k-row INSERT chunks"));
    assert!(script.contains("benchmarks/scripts/run_clickhouse_writes.sh"));
    assert!(script.contains("ClickHouse"));
    assert!(
        script.contains("run_competitor_script postgres benchmarks/scripts/run_postgres_writes.sh")
    );
    assert!(script.contains("benchmarks/scripts/check_supremacy.py \"$RAW\""));
    assert!(script.contains("mixed-correctness"));
    assert!(script.contains("mixed_correctness_100k"));
    assert!(script.contains("run_competitor_script"));
    assert!(script.contains("record_competitor_failure"));
    assert!(script.contains("BENCH_STORAGE_MODE=\"$STORAGE_MODE\""));
    assert!(script.contains("BENCH_DATA_ROOT=\"$DATA_ROOT/competitors\""));
    assert!(!script.contains("run_duckdb_writes.sh \"$selector\" || true"));
    assert!(!script.contains("run_sqlite3_writes.sh \"$selector\" || true"));
    assert!(!script.contains("run_postgres_writes.sh \"$selector\" || true"));
    assert!(!script.contains("run_clickhouse_writes.sh \"$selector\" || true"));
}

#[test]
fn scale_sweep_workflow_benchmarks_current_commit_binary() {
    let workflow = repo_file(".github/workflows/bench.yml");

    assert!(workflow.contains("name: build current ultrasqld"));
    assert!(
        workflow.contains(
            "cargo build --profile release-ship --package ultrasql-server --bin ultrasqld"
        )
    );
    assert!(workflow.contains("scripts/run-benchmark-certification.py"));
    assert!(workflow.contains("--mode full"));
    assert!(workflow.contains("--skip-build"));
    assert!(workflow.contains("--min-comparable-rows 17"));
    assert!(
        !workflow
            .contains(r#"ULTRASQL_RELEASE_VERSION: v${{ steps.package-version.outputs.version }}"#),
        "bench workflow must not gate current source on a stale published release"
    );
}

#[test]
fn release_sweep_workflow_enables_clickhouse_comparison() {
    let workflow = repo_file(".github/workflows/bench.yml");

    assert!(workflow.contains("brew install duckdb sqlite postgresql@14 clickhouse"));
    assert!(workflow.contains(r#"python3 -m venv "$RUNNER_TEMP/clickhouse-driver-venv""#));
    assert!(workflow.contains(
        r#""$RUNNER_TEMP/clickhouse-driver-venv/bin/python" -m pip install --disable-pip-version-check clickhouse-driver duckdb"#
    ));
    assert!(
        workflow.contains(r#"echo "$RUNNER_TEMP/clickhouse-driver-venv/bin" >> "$GITHUB_PATH""#)
    );
    assert!(!workflow.contains("python3 -m pip install clickhouse-driver"));
    assert!(workflow.contains("CH_BIN=\"$(command -v clickhouse)\""));
}

#[test]
fn postgres_runner_does_not_swallow_database_setup_failures() {
    // The PostgreSQL runner is a thin wrapper that delegates measurement to
    // run_postgres_writes.py over one persistent psycopg connection. A
    // connection or database-setup failure must surface as a not_available
    // artifact with a reason, never as a silently missing measurement.
    let driver = repo_file("benchmarks/scripts/run_postgres_writes.py");
    assert!(driver.contains("import psycopg"));
    assert!(driver.contains("psycopg.connect"));
    assert!(
        driver.contains("cannot connect to PostgreSQL"),
        "driver must record a connection failure as a not_available reason"
    );
    assert!(driver.contains("\"status\": \"not_available\""));
    assert!(driver.contains("reason"));

    let wrapper = repo_file("benchmarks/scripts/run_postgres_writes.sh");
    assert!(
        !wrapper.contains(":'db'"),
        "PostgreSQL runner must not use psql -c with :'db'; this client does not expand it"
    );
}

#[test]
fn clickhouse_runner_requires_tcp_readiness_before_measurement() {
    let script = repo_file("benchmarks/scripts/run_clickhouse_writes.sh");

    assert!(script.contains("clickhouse server did not become ready"));
    assert!(script.contains("clickhouse_ready=0"));
    assert!(script.contains("clickhouse_ready=1"));
    assert!(script.contains("if [[ \"$clickhouse_ready\" -ne 1 ]]; then"));
    assert!(script.contains("mark_unavailable \"clickhouse server did not become ready"));
}

#[test]
fn scale_sweep_competitor_raw_artifacts_use_strict_schema() {
    // Each competitor runner emits the full envelope. For PostgreSQL the
    // measured envelope lives in run_postgres_writes.py and the not_available
    // stub in the .sh wrapper, so the contract spans both files.
    let sources: [(&str, &[&str]); 4] = [
        ("duckdb", &["benchmarks/scripts/run_duckdb_writes.sh"]),
        ("sqlite3", &["benchmarks/scripts/run_sqlite3_writes.sh"]),
        (
            "postgres",
            &[
                "benchmarks/scripts/run_postgres_writes.sh",
                "benchmarks/scripts/run_postgres_writes.py",
            ],
        ),
        (
            "clickhouse",
            &["benchmarks/scripts/run_clickhouse_writes.sh"],
        ),
    ];
    for (engine, paths) in sources {
        let combined: String = paths.iter().map(|p| repo_file(p)).collect();
        for needle in [
            "\"schema_version\": 1",
            "\"status\": \"measured\"",
            "\"status\": \"not_available\"",
            "\"storage_mode\"",
            "\"durability_mode\"",
            "\"policy\"",
            "\"reason\"",
        ] {
            assert!(
                combined.contains(needle),
                "{engine} runner missing {needle}"
            );
        }
    }
}

#[test]
fn mixed_correctness_runner_uses_declared_storage_mode() {
    let helper = repo_file("benchmarks/scripts/run_mixed_correctness.py");
    assert!(helper.contains("--storage-mode"));
    assert!(helper.contains("--data-root"));
    assert!(helper.contains("def db_path_for("));
    assert!(helper.contains("duckdb.connect(str(db_path))"));
    assert!(helper.contains("sqlite3.connect(str(db_path), isolation_level=None)"));
    assert!(helper.contains(
        "table_kind = \"TABLE\" if storage_mode == \"data-dir\" else \"UNLOGGED TABLE\""
    ));

    // DuckDB and SQLite keep a run_mixed_correctness() bash function; the
    // PostgreSQL wrapper delegates the workload from run_one(). Both must pass
    // the declared storage mode and data root through to the shared helper.
    for path in [
        "benchmarks/scripts/run_duckdb_writes.sh",
        "benchmarks/scripts/run_sqlite3_writes.sh",
    ] {
        let script = repo_file(path);
        let start = script
            .find("run_mixed_correctness()")
            .expect("run_mixed_correctness");
        let body = &script[start..];
        assert!(
            body.contains("--storage-mode \"$BENCH_STORAGE_MODE\""),
            "{path}"
        );
        assert!(body.contains("--data-root \"$BENCH_DATA_ROOT\""), "{path}");
    }

    let postgres = repo_file("benchmarks/scripts/run_postgres_writes.sh");
    assert!(postgres.contains("run_mixed_correctness.py"));
    assert!(postgres.contains("--storage-mode \"$BENCH_STORAGE_MODE\""));
    assert!(postgres.contains("--data-root \"$BENCH_DATA_ROOT\""));
}

#[test]
fn bulk_write_competitors_match_unindexed_ultrasql_schema() {
    let driver = cross_compare_sql_driver_source();
    assert!(driver.contains("CREATE TABLE {table} (id INT NOT NULL, val INT)"));

    // DuckDB and SQLite keep bash run_insert/run_update/run_mixed functions.
    for path in [
        "benchmarks/scripts/run_duckdb_writes.sh",
        "benchmarks/scripts/run_sqlite3_writes.sh",
    ] {
        let script = repo_file(path);
        let update_start = script.find("run_update()").expect("run_update");
        let mixed_start = script.find("run_mixed()").expect("run_mixed");
        let bulk_body = &script[update_start..mixed_start];
        let insert_start = script.find("run_insert()").expect("run_insert");
        let insert_body = &script[insert_start..update_start];

        assert!(
            !bulk_body.contains("PRIMARY KEY"),
            "{path} bulk update/delete schema must not add a primary-key index"
        );
        assert!(
            !insert_body.contains("PRIMARY KEY"),
            "{path} bulk insert schema must not add a primary-key index"
        );
        assert!(
            script[mixed_start..].contains("PRIMARY KEY")
                || script[mixed_start..].contains("CREATE INDEX"),
            "{path} mixed OLTP still needs indexed point-read/write shape"
        );
    }

    // The PostgreSQL runner builds its tables in run_postgres_writes.py:
    // bulk insert/update/delete preload without a primary key (pk=False);
    // only mixed OLTP requests the indexed point-read/write shape (pk=True).
    let pg = repo_file("benchmarks/scripts/run_postgres_writes.py");
    let insert_start = pg.find("def run_insert(").expect("run_insert");
    let update_start = pg.find("def run_update(").expect("run_update");
    let mixed_start = pg.find("def run_mixed(").expect("run_mixed");
    let insert_body = &pg[insert_start..update_start];
    let bulk_body = &pg[update_start..mixed_start];
    assert!(
        !insert_body.contains("PRIMARY KEY") && !insert_body.contains("pk=True"),
        "postgres bulk insert must not add a primary-key index"
    );
    assert!(
        !bulk_body.contains("pk=True"),
        "postgres bulk update/delete must not add a primary-key index"
    );
    assert!(
        pg[mixed_start..].contains("pk=True"),
        "postgres mixed OLTP still needs the indexed point-read/write shape"
    );
}

#[test]
fn scale_sweep_writes_manifest_before_fastest_gate() {
    let script = repo_file("benchmarks/run_scale_sweep.sh");
    let manifest_write = script
        .find("\"$OUT/scale_sweep_manifest.json\"")
        .expect("scale sweep manifest writer");
    let lead_check = script
        .find("benchmarks/scripts/check_supremacy.py \"$RAW\"")
        .expect("scale sweep lead check");

    assert!(
        manifest_write < lead_check,
        "scale sweep must write manifest before the lead check so failed sweeps upload fresh status evidence"
    );
}

#[test]
fn ultrasql_delete_benchmark_uses_rollback_methodology() {
    let driver = cross_compare_sql_driver_source();
    let start = driver
        .find("async fn run_shared_delete")
        .expect("run_shared_delete");
    let tail = &driver[start..];
    let end = tail
        .find("async fn run_shared_update")
        .expect("next workload marker");
    let body = &tail[..end];

    assert!(body.contains("let table = \"bench_delete_shared\""));
    assert!(body.contains("preload_chunked(&client, table, n_rows).await?"));
    assert!(body.contains("for i in 0..total_iters"));
    assert!(body.contains("if i >= warmup"));
    assert!(!body.contains("bench_delete_{ix}"));

    let begin = body.find(".batch_execute(\"BEGIN\")").expect("BEGIN");
    let started = body
        .find("let started = Instant::now()")
        .expect("timed region start");
    let rollback = body.find(".batch_execute(\"ROLLBACK\")").expect("ROLLBACK");

    assert!(
        begin < started && started < rollback,
        "DELETE sweep must time only the DELETE statement inside BEGIN/ROLLBACK"
    );
}

#[test]
fn ultrasql_bulk_write_benchmarks_use_execute_style_wire_path() {
    let driver = cross_compare_sql_driver_source();
    for (fn_name, next_marker) in [
        ("async fn run_shared_delete", "async fn run_shared_update"),
        (
            "async fn run_shared_update",
            "fn push_mixed_correctness_row",
        ),
    ] {
        let start = driver.find(fn_name).expect(fn_name);
        let tail = &driver[start..];
        let end = tail.find(next_marker).expect(next_marker);
        let body = &tail[..end];

        assert!(
            body.contains(".batch_execute(&query)"),
            "{fn_name} should use execute-style protocol path for non-returning writes"
        );
        assert!(
            !body.contains(".simple_query(&query)"),
            "{fn_name} should not allocate SimpleQueryMessage rows for non-returning writes"
        );
    }
}

#[test]
fn ultrasql_raw_driver_records_storage_profile() {
    let driver = cross_compare_sql_driver_source();
    assert!(driver.contains("enum StorageMode"));
    assert!(driver.contains("#[arg(long, value_enum, default_value_t = StorageMode::Memory)]"));
    assert!(driver.contains("\"storage_mode\": args.storage_mode.as_str()"));
    assert!(driver.contains("\"durability_mode\": args.storage_mode.durability_mode()"));
}

#[test]
fn ultrasql_mixed_oltp_batches_wire_roundtrips() {
    let driver = cross_compare_sql_driver_source();
    let start = driver
        .find("async fn run_mixed_oltp_iter")
        .expect("run_mixed_oltp_iter");
    let tail = &driver[start..];
    let end = tail.find("struct SplitMix64").expect("next marker");
    let body = &tail[..end];

    assert!(body.contains("const MIXED_BATCH_OPS"));
    assert!(body.contains("for _ in 0..MIXED_BATCH_OPS"));
    assert!(body.contains("sql.push_str(\"BEGIN;\\n\")"));
    assert!(body.contains("sql.push_str(\"COMMIT;\\n\")"));
    assert!(body.contains(".batch_execute(&sql)"));
    assert!(body.contains("CREATE INDEX"));
    assert!(body.contains("bench_mixed_id_idx"));
}

#[test]
fn scale_sweep_renders_clickhouse_as_first_class_competitor() {
    let renderer = repo_file("benchmarks/scripts/render_scale_sweep.py");
    let scale_md = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.md");
    let scale_json = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.json");
    let readme = repo_file("README.md");
    let benchmarks = repo_file("BENCHMARKS.md");

    assert!(renderer.contains("\"clickhouse\""));
    assert!(renderer.contains("\"ClickHouse\""));
    assert!(scale_md.contains(
        "| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |"
    ));
    assert!(scale_md.contains("% slower"));
    assert!(scale_json.contains("\"clickhouse\""));
    assert!(readme.contains("ClickHouse"));
    assert!(benchmarks.contains("PostgreSQL, and ClickHouse"));
    assert!(benchmarks.contains("clients"));
    assert!(benchmarks.contains("benchmarks/scripts/run_clickhouse_writes.sh"));
    assert!(benchmarks.contains("benchmarks/scripts/check_supremacy.py"));
}

#[test]
fn readme_scale_sweep_matches_rendered_artifact() {
    let readme = repo_file("README.md");
    let scale_md = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.md");

    assert!(readme.contains("## Release-Artifact DB-vs-DB Benchmark"));
    assert!(readme.contains("benchmarks/run_scale_sweep.sh full"));
    assert!(readme.contains("Fastest"));
    assert!(!readme.contains("buffer-pool exhaustion"));

    for line in scale_md.lines().filter(|line| line.starts_with('|')) {
        assert!(
            readme.contains(line),
            "README missing scale-sweep row: {line}"
        );
    }
}

#[test]
fn scale_sweep_verifies_mixed_correctness_before_ranking() {
    let renderer = repo_file("benchmarks/scripts/render_scale_sweep.py");
    let rendered_json = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.json");
    let rendered_md = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.md");
    let readme = repo_file("README.md");
    let raw_ultrasql =
        repo_file("benchmarks/results/latest/scale-sweep/raw/mixed_correctness_100k-ultrasql.json");

    assert!(renderer.contains("ANSWER_REQUIRED_WORKLOADS"));
    assert!(renderer.contains("answer_sha256"));
    assert!(renderer.contains("correctness_status"));

    let raw: serde_json::Value =
        serde_json::from_str(&raw_ultrasql).expect("parse mixed_correctness_100k-ultrasql");
    assert_eq!(raw["engine"], "ultrasql");
    assert_eq!(raw["status"], "measured");
    assert_eq!(raw["workload"], "mixed_correctness_100k");
    assert_eq!(raw["n_rows"], 100_000);
    assert_eq!(
        raw["answer_sha256"].as_str().expect("answer hash").len(),
        64
    );
    assert!(raw["answer"].is_array());

    let rendered: serde_json::Value =
        serde_json::from_str(&rendered_json).expect("parse rendered scale_sweep.json");
    let rows = rendered["rows"].as_array().expect("rows array");
    let mixed = rows
        .iter()
        .find(|row| {
            row["workload"].as_str() == Some("mixed_correctness")
                && row["n_rows"].as_u64() == Some(100_000)
        })
        .expect("mixed correctness row");
    assert_eq!(mixed["correctness_status"].as_str(), Some("verified"));
    assert_eq!(
        mixed["answer_sha256"].as_str().expect("answer hash").len(),
        64
    );
    assert_eq!(mixed["fastest_engine"].as_str(), Some("ultrasql"));

    assert!(rendered_md.contains("| Mixed correctness | 100 000 | **"));
    assert!(readme.contains("| Mixed correctness | 100 000 | **"));
}

#[test]
fn scale_sweep_records_million_row_insert_honestly() {
    // The durable 1M INSERT currently fails — the 8 MiB WAL buffer rejects
    // records instead of applying backpressure — so it is recorded as an
    // explicit not_available artifact with a reason, never a fabricated win.
    // Tracked in ROADMAP P0 with a measurable exit condition.
    let raw =
        repo_file("benchmarks/results/latest/scale-sweep/raw/insert_throughput_1m-ultrasql.json");
    let value: serde_json::Value =
        serde_json::from_str(&raw).expect("parse insert_throughput_1m-ultrasql");

    assert_eq!(value["engine"], "ultrasql");
    assert_eq!(value["status"], "not_available");
    assert_eq!(value["n_rows"], 1_000_000);
    assert!(
        value["reason"]
            .as_str()
            .is_some_and(|r| !r.trim().is_empty()),
        "not_available artifact must carry a non-empty reason"
    );

    // Since UltraSQL is not_available for the 1M INSERT, a competitor is the
    // fastest measured engine for that rendered row.
    let rendered_json = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.json");
    let rendered: serde_json::Value =
        serde_json::from_str(&rendered_json).expect("parse rendered scale_sweep.json");
    let rows = rendered["rows"].as_array().expect("rows array");
    let one_m_insert = rows
        .iter()
        .find(|row| {
            row["workload"].as_str() == Some("insert_throughput")
                && row["n_rows"].as_u64() == Some(1_000_000)
        })
        .expect("1m insert row");
    let fastest = one_m_insert["fastest_engine"].as_str();
    assert!(
        fastest.is_some() && fastest != Some("ultrasql"),
        "1M INSERT must be won by a competitor while UltraSQL is not_available, got {fastest:?}"
    );
}

#[test]
fn scale_sweep_rendered_fastest_engine_matches_raw_medians() {
    // Honest gate: the renderer bolds the engine that is actually fastest for
    // each row from the raw medians, whoever that is. UltraSQL is NOT required
    // to win every row; the certification scoreboard
    // (scripts/validate-benchmark-certification.py) reports the wins and losses
    // and `ready` no longer demands a clean sweep.
    let rendered_json = repo_file("benchmarks/results/latest/scale-sweep/scale_sweep.json");
    let rendered: serde_json::Value =
        serde_json::from_str(&rendered_json).expect("parse rendered scale_sweep.json");
    let rows = rendered["rows"].as_array().expect("rows array");

    let mut ultrasql_fastest = 0usize;
    for row in rows {
        let engines = row["engines"].as_object().expect("engines object");
        let mut best: Option<(String, f64)> = None;
        for (name, entry) in engines {
            let Some(median) = entry["median_us"].as_f64() else {
                continue;
            };
            if median <= 0.0 {
                continue;
            }
            if best.as_ref().is_none_or(|(_, b)| median < *b) {
                best = Some((name.clone(), median));
            }
        }
        let Some((best_engine, best_median)) = best else {
            continue;
        };
        let label = format!(
            "{} rows={}",
            row["workload"].as_str().unwrap_or("<unknown>"),
            row["n_rows"].as_u64().unwrap_or(0)
        );
        assert_eq!(
            row["fastest_engine"].as_str(),
            Some(best_engine.as_str()),
            "{label}: rendered fastest_engine must be the lowest raw median"
        );
        let rendered_median = row["fastest_median_us"].as_f64().unwrap_or(-1.0);
        assert!(
            (rendered_median - best_median).abs() < 1e-6,
            "{label}: rendered fastest_median_us must match the lowest raw median"
        );
        if best_engine == "ultrasql" {
            ultrasql_fastest += 1;
        }
    }

    // Sanity: UltraSQL genuinely leads a meaningful share of rows (a real
    // competitor), without requiring a clean sweep.
    assert!(
        ultrasql_fastest > 0,
        "UltraSQL should be fastest on at least one published row"
    );
}

#[test]
fn current_scale_sweep_rendered_fastest_engine_matches_raw_medians() {
    // Optional "scale-sweep-current" artifact: when present, hold it to the same
    // honest contract as the published sweep — the rendered fastest engine must
    // be the lowest raw median, not a forced UltraSQL supremacy.
    let current_path = repo_path("benchmarks/results/latest/scale-sweep-current/scale_sweep.json");
    if !current_path.exists() {
        return;
    }
    let rendered_json = fs::read_to_string(&current_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", current_path.display()));
    let rendered: serde_json::Value =
        serde_json::from_str(&rendered_json).expect("parse current scale_sweep.json");
    let rows = rendered["rows"].as_array().expect("rows array");
    for row in rows {
        let engines = row["engines"].as_object().expect("engines object");
        let mut best: Option<(String, f64)> = None;
        for (name, entry) in engines {
            let Some(median) = entry["median_us"].as_f64() else {
                continue;
            };
            if median <= 0.0 {
                continue;
            }
            if best.as_ref().is_none_or(|(_, b)| median < *b) {
                best = Some((name.clone(), median));
            }
        }
        if let Some((best_engine, _)) = best {
            assert_eq!(
                row["fastest_engine"].as_str(),
                Some(best_engine.as_str()),
                "current sweep rendered fastest_engine must be the lowest raw median"
            );
        }
    }
}
