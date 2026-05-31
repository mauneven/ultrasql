//! End-to-end tests for the SQLLogicTest runner binary.

use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn in_process_mode_runs_portable_smoke_corpus() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/slt/portable/basic.slt")
        .canonicalize()
        .expect("portable SLT corpus exists");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg(suite)
        .output()
        .expect("run SQLLogicTest runner");

    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("passed=15"), "stdout:\n{stdout}");
    assert!(stdout.contains("failed=0"), "stdout:\n{stdout}");
}

#[test]
fn in_process_mode_compares_portable_smoke_against_sqlite_when_available() {
    if !command_available("sqlite3") {
        eprintln!("sqlite3 not available; skipping optional differential smoke");
        return;
    }
    run_reference_engine_smoke("sqlite");
}

#[test]
fn in_process_mode_compares_portable_smoke_against_duckdb_when_available() {
    if !command_available("duckdb") {
        eprintln!("duckdb not available; skipping optional differential smoke");
        return;
    }
    run_reference_engine_smoke("duckdb");
}

#[test]
fn in_process_mode_compares_against_multiple_reference_engines_when_available() {
    if !command_available("sqlite3") || !command_available("duckdb") {
        eprintln!("sqlite3 or duckdb not available; skipping optional multi-reference smoke");
        return;
    }

    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/slt/portable/basic.slt")
        .canonicalize()
        .expect("portable SLT corpus exists");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg("--reference-engine")
        .arg("sqlite")
        .arg("--reference-engine")
        .arg("duckdb")
        .arg(suite)
        .output()
        .expect("run SQLLogicTest runner");

    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("passed=15"), "stdout:\n{stdout}");
    assert!(stdout.contains("failed=0"), "stdout:\n{stdout}");
}

#[test]
fn reference_engine_probe_uses_engine_specific_version_flags() {
    assert_eq!(version_arg_for("sqlite3"), "-version");
    assert_eq!(version_arg_for("duckdb"), "--version");
    assert_eq!(version_arg_for("DuckDB"), "--version");
}

#[test]
fn in_process_mode_writes_benchmark_artifact() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/slt/portable/basic.slt")
        .canonicalize()
        .expect("portable SLT corpus exists");
    let output_path = temp_artifact_path("ultrasql-slt-benchmark", "json");
    let markdown_path = output_path.with_extension("md");
    let _ = fs::remove_file(&output_path);
    let _ = fs::remove_file(&markdown_path);

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg("--benchmark-runs")
        .arg("2")
        .arg("--benchmark-output")
        .arg(&output_path)
        .arg(suite)
        .output()
        .expect("run SQLLogicTest runner benchmark");

    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let json = fs::read_to_string(&output_path).expect("benchmark JSON artifact exists");
    assert!(
        json.contains("\"suite\": \"sqllogictest\""),
        "json:\n{json}"
    );
    assert!(json.contains("\"benchmark_runs\": 2"), "json:\n{json}");
    assert!(json.contains("\"name\": \"ultrasql\""), "json:\n{json}");
    let markdown = fs::read_to_string(&markdown_path).expect("benchmark Markdown artifact exists");
    assert!(
        markdown.contains("SQLLogicTest Speed Comparison"),
        "markdown:\n{markdown}"
    );
    let _ = fs::remove_file(output_path);
    let _ = fs::remove_file(markdown_path);
}

#[test]
fn in_process_mode_accepts_hash_threshold_and_hashed_results() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = temp_artifact_path("ultrasql-slt-hash", "test");
    fs::write(
        &suite,
        "hash-threshold 1\n\nquery I nosort\nSELECT 1\n----\n1 values hashing to b026324c6904b2a9cb4b88d6d61c81d1\n",
    )
    .expect("write temporary hash SLT");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg(&suite)
        .output()
        .expect("run SQLLogicTest runner");

    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("passed=1"), "stdout:\n{stdout}");
    assert!(stdout.contains("failed=0"), "stdout:\n{stdout}");
    let _ = fs::remove_file(suite);
}

#[test]
fn portable_corpus_includes_curated_filter_setops_shard() {
    let suite = repo_root().join("tests/slt/portable/filter_setops.slt");
    let text = fs::read_to_string(&suite).expect("curated portable SLT shard exists");
    assert!(
        text.contains("UltraSQL-authored portable SQLLogicTest shard"),
        "{} must document authored provenance",
        suite.display()
    );
    assert!(
        !text.contains("source=/"),
        "{} must not be an imported third-party dump",
        suite.display()
    );
    let case_count = count_slt_cases(&text);
    assert!(
        (12..=30).contains(&case_count),
        "{} must stay as a small reviewed shard, got {case_count} cases",
        suite.display()
    );
}

#[test]
fn portable_corpus_includes_scalar_expression_shard() {
    let suite = repo_root().join("tests/slt/portable/scalar_expressions.slt");
    let text = fs::read_to_string(&suite).expect("curated scalar expression SLT shard exists");
    assert!(
        text.contains("UltraSQL-authored portable SQLLogicTest shard"),
        "{} must document authored provenance",
        suite.display()
    );
    assert!(
        text.contains("portable scalar expression coverage"),
        "{} must name its reviewed scope",
        suite.display()
    );
    let case_count = count_slt_cases(&text);
    assert!(
        (8..=24).contains(&case_count),
        "{} must stay as a small reviewed shard, got {case_count} cases",
        suite.display()
    );
}

#[test]
fn imported_sqllogictest_shards_stay_small_and_provenanced() {
    let imported_root = repo_root().join("tests/slt/portable/imported");
    let mut imported_suites = 0usize;

    for entry in fs::read_dir(&imported_root).expect("imported SLT root exists") {
        let entry = entry.expect("read imported SLT suite entry");
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        imported_suites = imported_suites.saturating_add(1);
        for required in [
            "README.md",
            "IMPORT_MANIFEST.txt",
            "LICENSE.upstream",
            "upstream_commit.txt",
        ] {
            assert!(
                path.join(required).is_file(),
                "{} missing required provenance file {required}",
                path.display()
            );
        }
        let manifest = fs::read_to_string(path.join("IMPORT_MANIFEST.txt"))
            .expect("read imported SLT manifest");
        assert!(
            manifest.lines().any(|line| line.starts_with("commit=")),
            "{} manifest must pin upstream commit",
            path.display()
        );
        let file_count = manifest
            .lines()
            .filter(|line| line.starts_with("file="))
            .count();
        assert!(
            (1..=10).contains(&file_count),
            "{} imports {file_count} files; split reviewed shards instead of dumping suites",
            path.display()
        );
    }

    assert!(
        imported_suites > 0,
        "{} should contain at least one audited imported shard",
        imported_root.display()
    );
}

#[test]
fn sql_regression_subset_preserves_public_provenance() {
    let subset = repo_root().join("tests/slt/sql_regression/regression_subset");
    for required in ["README.md", "IMPORT_MANIFEST.txt", "LICENSE.upstream"] {
        assert!(
            subset.join(required).is_file(),
            "{} missing required provenance file {required}",
            subset.display()
        );
    }
    let manifest =
        fs::read_to_string(subset.join("IMPORT_MANIFEST.txt")).expect("read PostgreSQL manifest");
    assert!(
        manifest.contains("source=https://github.com/postgres/postgres"),
        "manifest:\n{manifest}"
    );
    assert!(
        manifest.contains("commit=ddd12d1a5c4d980c5f31dc7d096012547b724e55"),
        "manifest:\n{manifest}"
    );
    assert!(
        manifest.contains("derived_from=src/test/regress/sql/select.sql"),
        "manifest:\n{manifest}"
    );
    let shard = subset.join("select_basics.slt");
    let text = fs::read_to_string(&shard).expect("read engine-specific shard");
    assert!(
        text.contains("WHERE (id, score) IN ((1, 42), (3, 10), (5, 0))"),
        "{} must keep row-value IN PostgreSQL regression coverage active",
        shard.display()
    );
}

#[test]
fn sql_regression_subset_runs_all_active_shards_in_process() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let subset = repo_root().join("tests/slt/sql_regression/regression_subset");
    let mut suites = fs::read_dir(&subset)
        .expect("read PostgreSQL regression subset")
        .map(|entry| entry.expect("read PostgreSQL regression shard").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|ext| ext == std::ffi::OsStr::new("slt"))
        })
        .collect::<Vec<_>>();
    suites.sort();
    assert!(
        !suites.is_empty(),
        "{} has no active shards",
        subset.display()
    );

    let expected_cases = suites
        .iter()
        .map(|suite| {
            let text = fs::read_to_string(suite).expect("read SQLLogicTest shard");
            count_slt_cases(&text)
        })
        .sum::<usize>();

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .args(&suites)
        .output()
        .expect("run all active PostgreSQL regression subset shards");
    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&format!("passed={expected_cases}")),
        "stdout:\n{stdout}"
    );
    assert!(stdout.contains("failed=0"), "stdout:\n{stdout}");
    assert!(stdout.contains("skipped=0"), "stdout:\n{stdout}");
}

#[test]
fn sql_regression_parser_type_baseline_is_imported_and_provenanced() {
    let subset = repo_root().join("tests/slt/sql_regression/regression_subset");
    let manifest =
        fs::read_to_string(subset.join("IMPORT_MANIFEST.txt")).expect("read PostgreSQL manifest");
    let readme = fs::read_to_string(subset.join("README.md")).expect("read PostgreSQL README");
    let shard = subset.join("parser_type_baseline.slt");
    let text = fs::read_to_string(&shard).expect("read parser/type baseline shard");

    for source in [
        "derived_from=src/test/regress/sql/char.sql",
        "derived_from=src/test/regress/sql/varchar.sql",
        "derived_from=src/test/regress/sql/numeric.sql",
        "derived_from=src/test/regress/sql/type_sanity.sql",
    ] {
        assert!(manifest.contains(source), "manifest:\n{manifest}");
    }
    assert!(
        manifest.contains("file=parser_type_baseline.slt"),
        "manifest:\n{manifest}"
    );
    assert!(
        readme.contains("parser_type_baseline.slt"),
        "README:\n{readme}"
    );
    assert!(
        text.contains("PostgreSQL regression-derived parser/type baseline"),
        "{} must document reviewed scope",
        shard.display()
    );
    for surface in [
        "CHAR(4)",
        "VARCHAR(5)",
        "NUMERIC(6,2)",
        "DECIMAL(5,1)",
        "pg_typeof",
        "::regtype",
        "integer",
    ] {
        assert!(
            text.contains(surface),
            "{} missing {surface}",
            shard.display()
        );
    }
    assert!(
        !text.contains("# ultrasql:skip full PostgreSQL type_sanity catalog invariant"),
        "{} must keep active regtype text coverage instead of a stale skip",
        shard.display()
    );
    let case_count = count_slt_cases(&text);
    assert!(
        (10..=32).contains(&case_count),
        "{} must stay as a small reviewed shard, got {case_count} cases",
        shard.display()
    );
}

#[test]
fn sql_regression_join_setop_baseline_is_imported_and_provenanced() {
    let subset = repo_root().join("tests/slt/sql_regression/regression_subset");
    let manifest =
        fs::read_to_string(subset.join("IMPORT_MANIFEST.txt")).expect("read PostgreSQL manifest");
    let readme = fs::read_to_string(subset.join("README.md")).expect("read PostgreSQL README");
    let shard = subset.join("join_setop_baseline.slt");
    let text = fs::read_to_string(&shard).expect("read join/set-op baseline shard");

    assert!(
        manifest.contains("derived_from=src/test/regress/sql/select.sql"),
        "manifest:\n{manifest}"
    );
    assert!(
        manifest.contains("file=join_setop_baseline.slt"),
        "manifest:\n{manifest}"
    );
    assert!(
        readme.contains("join_setop_baseline.slt"),
        "README:\n{readme}"
    );
    assert!(
        text.contains("PostgreSQL regression-derived join and set-operation baseline"),
        "{} must document reviewed scope",
        shard.display()
    );
    for surface in [
        "JOIN slt_pg_join_child",
        "LEFT JOIN slt_pg_join_child",
        "WHERE EXISTS",
        "UNION",
        "INTERSECT",
        "EXCEPT",
    ] {
        assert!(
            text.contains(surface),
            "{} missing {surface}",
            shard.display()
        );
    }
    let case_count = count_slt_cases(&text);
    assert!(
        (8..=24).contains(&case_count),
        "{} must stay as a small reviewed shard, got {case_count} cases",
        shard.display()
    );
}

#[test]
fn sql_regression_index_constraint_operator_baseline_is_imported_and_provenanced() {
    let subset = repo_root().join("tests/slt/sql_regression/regression_subset");
    let manifest =
        fs::read_to_string(subset.join("IMPORT_MANIFEST.txt")).expect("read PostgreSQL manifest");
    let readme = fs::read_to_string(subset.join("README.md")).expect("read PostgreSQL README");
    let shard = subset.join("index_constraint_operator_baseline.slt");
    let text = fs::read_to_string(&shard).expect("read index/constraint/operator shard");

    for source in [
        "derived_from=src/test/regress/sql/create_index.sql",
        "derived_from=src/test/regress/sql/constraints.sql",
        "derived_from=src/test/regress/sql/create_operator.sql",
        "derived_from=src/test/regress/sql/opr_sanity.sql",
    ] {
        assert!(manifest.contains(source), "manifest:\n{manifest}");
    }
    assert!(
        manifest.contains("file=index_constraint_operator_baseline.slt"),
        "manifest:\n{manifest}"
    );
    assert!(
        readme.contains("index_constraint_operator_baseline.slt"),
        "README:\n{readme}"
    );
    assert!(
        text.contains("PostgreSQL regression-derived index/constraint/operator baseline"),
        "{} must document reviewed scope",
        shard.display()
    );
    for surface in [
        "CREATE INDEX",
        "CREATE UNIQUE INDEX",
        "PRIMARY KEY",
        "CHECK",
        "REFERENCES",
        "<>",
        "!=",
        "<=",
        ">=",
    ] {
        assert!(
            text.contains(surface),
            "{} missing {surface}",
            shard.display()
        );
    }
    assert!(
        !text.contains("# ultrasql:skip full PostgreSQL opr_sanity catalog/operator invariant"),
        "{} must keep active operator sanity coverage instead of a stale EOF skip",
        shard.display()
    );
    let case_count = count_slt_cases(&text);
    assert!(
        (14..=40).contains(&case_count),
        "{} must stay as a small reviewed shard, got {case_count} cases",
        shard.display()
    );
}

#[test]
fn sql_regression_type_specific_baseline_is_imported_and_provenanced() {
    let subset = repo_root().join("tests/slt/sql_regression/regression_subset");
    let manifest =
        fs::read_to_string(subset.join("IMPORT_MANIFEST.txt")).expect("read PostgreSQL manifest");
    let readme = fs::read_to_string(subset.join("README.md")).expect("read PostgreSQL README");
    let shard = subset.join("type_specific_baseline.slt");
    let text = fs::read_to_string(&shard).expect("read type-specific shard");

    for source in [
        "derived_from=src/test/regress/sql/numeric.sql",
        "derived_from=src/test/regress/sql/text.sql",
        "derived_from=src/test/regress/sql/date.sql",
        "derived_from=src/test/regress/sql/time.sql",
        "derived_from=src/test/regress/sql/timestamp.sql",
        "derived_from=src/test/regress/sql/timetz.sql",
        "derived_from=src/test/regress/sql/json.sql",
        "derived_from=src/test/regress/sql/jsonb.sql",
        "derived_from=src/test/regress/sql/arrays.sql",
    ] {
        assert!(manifest.contains(source), "manifest:\n{manifest}");
    }
    assert!(
        manifest.contains("file=type_specific_baseline.slt"),
        "manifest:\n{manifest}"
    );
    assert!(
        readme.contains("type_specific_baseline.slt"),
        "README:\n{readme}"
    );
    assert!(
        text.contains("PostgreSQL regression-derived type-specific baseline"),
        "{} must document reviewed scope",
        shard.display()
    );
    for surface in [
        "NUMERIC(8,2)",
        "TEXT",
        "DATE",
        "TIMESTAMP",
        "TIME",
        "JSONB",
        "INT[]",
        "array_length",
        "jsonb_path_exists",
        "_int4",
    ] {
        assert!(
            text.contains(surface),
            "{} missing {surface}",
            shard.display()
        );
    }
    assert!(
        !text.contains("# ultrasql:skip full PostgreSQL type-specific regression breadth"),
        "{} must keep pg_type array-row coverage active instead of a stale skip",
        shard.display()
    );
    let case_count = count_slt_cases(&text);
    assert!(
        (16..=42).contains(&case_count),
        "{} must stay as a small reviewed shard, got {case_count} cases",
        shard.display()
    );
}

#[test]
fn sql_regression_aggregate_window_baseline_is_imported_and_provenanced() {
    let subset = repo_root().join("tests/slt/sql_regression/regression_subset");
    let manifest =
        fs::read_to_string(subset.join("IMPORT_MANIFEST.txt")).expect("read PostgreSQL manifest");
    let readme = fs::read_to_string(subset.join("README.md")).expect("read PostgreSQL README");
    let shard = subset.join("aggregate_window_baseline.slt");
    let text = fs::read_to_string(&shard).expect("read aggregate/window shard");

    for source in [
        "derived_from=src/test/regress/sql/aggregates.sql",
        "derived_from=src/test/regress/sql/window.sql",
    ] {
        assert!(manifest.contains(source), "manifest:\n{manifest}");
    }
    assert!(
        manifest.contains("file=aggregate_window_baseline.slt"),
        "manifest:\n{manifest}"
    );
    assert!(
        readme.contains("aggregate_window_baseline.slt"),
        "README:\n{readme}"
    );
    assert!(
        text.contains("PostgreSQL regression-derived aggregate/window baseline"),
        "{} must document reviewed scope",
        shard.display()
    );
    for surface in [
        "GROUP BY",
        "HAVING",
        "COUNT(*)",
        "SUM(amount)",
        "AVG(amount)",
        "row_number() OVER",
        "rank() OVER",
        "lag(amount, 1, 0) OVER",
        "ntile(2) OVER",
    ] {
        assert!(
            text.contains(surface),
            "{} missing {surface}",
            shard.display()
        );
    }
    let case_count = count_slt_cases(&text);
    assert!(
        (12..=36).contains(&case_count),
        "{} must stay as a small reviewed shard, got {case_count} cases",
        shard.display()
    );
}

#[test]
fn sql_regression_type_coercion_baseline_is_imported_and_provenanced() {
    let subset = repo_root().join("tests/slt/sql_regression/regression_subset");
    let manifest =
        fs::read_to_string(subset.join("IMPORT_MANIFEST.txt")).expect("read PostgreSQL manifest");
    let readme = fs::read_to_string(subset.join("README.md")).expect("read PostgreSQL README");
    let shard = subset.join("type_coercion_baseline.slt");
    let text = fs::read_to_string(&shard).expect("read type-coercion shard");

    for source in [
        "derived_from=src/test/regress/sql/numeric.sql",
        "derived_from=src/test/regress/sql/char.sql",
        "derived_from=src/test/regress/sql/varchar.sql",
        "derived_from=src/test/regress/sql/select.sql",
    ] {
        assert!(manifest.contains(source), "manifest:\n{manifest}");
    }
    assert!(
        manifest.contains("file=type_coercion_baseline.slt"),
        "manifest:\n{manifest}"
    );
    assert!(
        readme.contains("type_coercion_baseline.slt"),
        "README:\n{readme}"
    );
    assert!(
        text.contains("PostgreSQL regression-derived type-coercion baseline"),
        "{} must document reviewed scope",
        shard.display()
    );
    for surface in [
        "CAST('42' AS INT)",
        "CAST(42 AS TEXT)",
        "CAST('12.30' AS NUMERIC(8,2))",
        "CAST(NULL AS INT)",
        "COALESCE(n, 0.00)",
        "CASE WHEN flag",
        "i + 1",
    ] {
        assert!(
            text.contains(surface),
            "{} missing {surface}",
            shard.display()
        );
    }
    let case_count = count_slt_cases(&text);
    assert!(
        (10..=32).contains(&case_count),
        "{} must stay as a small reviewed shard, got {case_count} cases",
        shard.display()
    );
}

#[test]
fn sql_regression_catalog_sanity_baseline_is_imported_and_provenanced() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let subset = repo_root().join("tests/slt/sql_regression/regression_subset");
    let manifest =
        fs::read_to_string(subset.join("IMPORT_MANIFEST.txt")).expect("read PostgreSQL manifest");
    let readme = fs::read_to_string(subset.join("README.md")).expect("read PostgreSQL README");
    let shard = subset.join("catalog_sanity_baseline.slt");
    let text = fs::read_to_string(&shard).expect("read catalog sanity shard");

    for source in [
        "derived_from=src/test/regress/sql/type_sanity.sql",
        "derived_from=src/test/regress/sql/opr_sanity.sql",
    ] {
        assert!(manifest.contains(source), "manifest:\n{manifest}");
    }
    assert!(
        manifest.contains("file=catalog_sanity_baseline.slt"),
        "manifest:\n{manifest}"
    );
    assert!(
        readme.contains("catalog_sanity_baseline.slt"),
        "README:\n{readme}"
    );
    assert!(
        text.contains("PostgreSQL regression-derived catalog sanity baseline"),
        "{} must document reviewed scope",
        shard.display()
    );
    for surface in [
        "pg_catalog.pg_class",
        "pg_catalog.pg_attribute",
        "pg_catalog.pg_constraint",
        "pg_catalog.pg_type",
        "pg_catalog.pg_table_is_visible",
        "format_type",
        "PRIMARY KEY",
        "CHECK",
    ] {
        assert!(
            text.contains(surface),
            "{} missing {surface}",
            shard.display()
        );
    }
    let case_count = count_slt_cases(&text);
    assert!(
        (5..=20).contains(&case_count),
        "{} must stay as a small reviewed shard, got {case_count} cases",
        shard.display()
    );

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg(&shard)
        .output()
        .expect("run catalog sanity SQLLogicTest shard");
    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("passed=7"), "stdout:\n{stdout}");
    assert!(stdout.contains("failed=0"), "stdout:\n{stdout}");
}

#[test]
fn skip_directive_requires_explicit_reason() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = temp_artifact_path("ultrasql-slt-empty-skip", "test");
    fs::write(
        &suite,
        "# ultrasql:skip\n\nquery I nosort\nSELECT 1\n----\n1\n",
    )
    .expect("write temporary SLT with empty skip");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg(&suite)
        .output()
        .expect("run SQLLogicTest runner");

    let _ = fs::remove_file(suite);
    assert!(
        !output.status.success(),
        "runner unexpectedly accepted empty skip reason\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("skip directive requires an explicit reason"),
        "stderr:\n{stderr}"
    );
}

#[test]
fn skip_filter_requires_explicit_reason() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = temp_artifact_path("ultrasql-slt-filter-reason", "test");
    let filter = temp_artifact_path("ultrasql-slt-filter-reason", "txt");
    fs::write(&suite, "query I nosort\nSELECT 1\n----\n1\n").expect("write temporary SLT");
    fs::write(&filter, "SELECT 1\n").expect("write skip filter without reason");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg("--skip-filter")
        .arg(&filter)
        .arg(&suite)
        .output()
        .expect("run SQLLogicTest runner");

    let _ = fs::remove_file(suite);
    let _ = fs::remove_file(filter);
    assert!(
        !output.status.success(),
        "runner unexpectedly accepted skip filter without reason\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("skip filter requires `pattern<TAB>reason`"),
        "stderr:\n{stderr}"
    );
}

#[test]
#[cfg(unix)]
fn differential_wrong_rows_fail() {
    let temp_dir = temp_dir("ultrasql-slt-bad-sqlite");
    fs::create_dir_all(&temp_dir).expect("create temp directory");
    let sqlite = temp_dir.join("sqlite3");
    write_executable(&sqlite, "#!/bin/sh\nprintf '999\\n'\nexit 0\n");
    let suite = temp_dir.join("wrong_rows.test");
    fs::write(&suite, "query I nosort\nSELECT 1\n----\n1\n").expect("write mismatch SLT");

    let path = format!(
        "{}:{}",
        temp_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner"))
        .env("PATH", path)
        .arg("--mode")
        .arg("in-process")
        .arg("--reference-engine")
        .arg("sqlite")
        .arg(&suite)
        .output()
        .expect("run SQLLogicTest runner");

    let _ = fs::remove_dir_all(&temp_dir);
    assert!(
        !output.status.success(),
        "runner unexpectedly accepted wrong comparison rows\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("reference mismatch"), "stderr:\n{stderr}");
}

#[test]
fn in_process_mode_case_limit_bounds_suite_execution() {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = temp_artifact_path("ultrasql-slt-limit", "test");
    fs::write(
        &suite,
        "query I nosort\nSELECT 1\n----\n1\n\nquery I nosort\nSELECT 2\n----\n999\n",
    )
    .expect("write temporary limited SLT");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg("--case-limit")
        .arg("1")
        .arg(&suite)
        .output()
        .expect("run SQLLogicTest runner");

    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cases=1"), "stdout:\n{stdout}");
    assert!(stdout.contains("passed=1"), "stdout:\n{stdout}");
    assert!(stdout.contains("failed=0"), "stdout:\n{stdout}");
    let _ = fs::remove_file(suite);
}

fn run_reference_engine_smoke(engine: &str) {
    let bin = env!("CARGO_BIN_EXE_ultrasql-sqllogictest-runner");
    let suite = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/slt/portable/basic.slt")
        .canonicalize()
        .expect("portable SLT corpus exists");

    let output = Command::new(bin)
        .arg("--mode")
        .arg("in-process")
        .arg("--reference-engine")
        .arg(engine)
        .arg(suite)
        .output()
        .expect("run SQLLogicTest runner");

    assert!(
        output.status.success(),
        "runner failed for {engine}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("passed=15"), "stdout:\n{stdout}");
    assert!(stdout.contains("failed=0"), "stdout:\n{stdout}");
}

fn command_available(program: &str) -> bool {
    command_status_with_timeout(program, version_arg_for(program), Duration::from_secs(5))
        .is_some_and(|status| status.success())
}

fn version_arg_for(program: &str) -> &'static str {
    if program.eq_ignore_ascii_case("duckdb") {
        "--version"
    } else {
        "-version"
    }
}

fn command_status_with_timeout(
    program: &str,
    arg: &str,
    timeout: Duration,
) -> Option<process::ExitStatus> {
    let mut child = Command::new(program)
        .arg(arg)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let deadline = std::time::Instant::now() + timeout;

    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(unix)]
fn write_executable(path: &Path, text: &str) {
    fs::write(path, text).expect("write executable script");
    let mut perms = fs::metadata(path)
        .expect("stat executable script")
        .permissions();
    use std::os::unix::fs::PermissionsExt;
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod executable script");
}

fn temp_artifact_path(prefix: &str, extension: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}.{extension}", process::id()))
}

#[cfg(unix)]
fn temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", process::id()))
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn count_slt_cases(text: &str) -> usize {
    text.lines()
        .map(str::trim)
        .filter(|line| line.starts_with("statement ") || line.starts_with("query "))
        .count()
}
