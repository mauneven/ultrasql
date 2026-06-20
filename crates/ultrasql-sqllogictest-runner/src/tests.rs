//! Unit tests for the SQLLogicTest runner, exercising parsing, filtering,
//! reference-engine selection, benchmark accounting, and result formatting.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::benchmark::{
    EngineBenchmark, escape_json, push_sql_statement, run_benchmark_suite,
    write_benchmark_artifacts,
};
use crate::cli::{Cli, Mode, ReferenceEngine};
use crate::model::{
    Directives, QueryExpectation, SkipFilters, SkipPattern, SortMode, StatementExpectation,
    TestCase, TestKind,
};
use crate::parser::{
    collect_input_files, collect_query, collect_until_blank, is_slt_file, parse_directive,
    parse_query_header, parse_script,
};
use crate::runner::{
    compare_query_expectation, effective_skip_reason, format_cli_reference_rows, hash_query_values,
};
use crate::target::selected_reference_engines;
use crate::{apply_case_limit, compact_sql};

fn empty_cli() -> Cli {
    Cli {
        paths: Vec::new(),
        mode: Mode::Wire,
        database_url: None,
        reference_url: None,
        reference_engine: Vec::new(),
        reference_db: None,
        benchmark_output: None,
        benchmark_runs: 1,
        case_limit: None,
        progress_every: 0,
        slow_case_ms: None,
        skip_filters: Vec::new(),
        features: Vec::new(),
    }
}

fn temp_path(prefix: &str, ext: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}.{ext}", std::process::id()))
}

fn case_with_sql(sql: &str) -> TestCase {
    TestCase {
        path: PathBuf::from("suite/basic.slt"),
        line: 7,
        kind: TestKind::Statement {
            expectation: StatementExpectation::Ok,
            sql: sql.to_owned(),
        },
        skip_reason: None,
        requires: Vec::new(),
    }
}

#[test]
fn reference_engine_metadata_is_explicit() {
    assert_eq!(ReferenceEngine::Postgres.command(), None);
    assert_eq!(ReferenceEngine::Duckdb.command(), Some("duckdb"));
    assert_eq!(ReferenceEngine::Sqlite.command(), Some("sqlite3"));
    assert_eq!(ReferenceEngine::Postgres.suffix(), "postgres");
    assert_eq!(ReferenceEngine::Duckdb.suffix(), "duckdb");
    assert_eq!(ReferenceEngine::Sqlite.suffix(), "sqlite");
}

#[test]
fn skip_filters_load_comments_match_sql_and_path() {
    let path = temp_path("ultrasql-slt-filter", "txt");
    std::fs::write(
        &path,
        "\n# comment\nSELECT 9\tunsupported scalar\nsuite/\timported shard\n",
    )
    .expect("write skip filter");

    let filters = SkipFilters::load_all(std::slice::from_ref(&path)).expect("load filters");
    assert_eq!(
        filters.skip_reason(Path::new("x.slt"), "SELECT 9"),
        Some("unsupported scalar (SELECT 9)".to_owned())
    );
    assert_eq!(
        filters.skip_reason(Path::new("suite/basic.slt"), "SELECT 1"),
        Some("imported shard (suite/)".to_owned())
    );
    assert_eq!(filters.skip_reason(Path::new("x.slt"), "SELECT 1"), None);

    let _ = std::fs::remove_file(path);
}

#[test]
fn skip_filters_reject_malformed_records() {
    let missing_tab = temp_path("ultrasql-slt-filter-missing-tab", "txt");
    std::fs::write(&missing_tab, "SELECT 1\n").expect("write filter");
    let err = SkipFilters::load_all(std::slice::from_ref(&missing_tab))
        .expect_err("missing reason separator must fail");
    assert!(err.to_string().contains("pattern<TAB>reason"));

    let empty_pattern = temp_path("ultrasql-slt-filter-empty-pattern", "txt");
    std::fs::write(&empty_pattern, "\tno pattern\n").expect("write filter");
    let err = SkipFilters::load_all(std::slice::from_ref(&empty_pattern))
        .expect_err("empty pattern must fail");
    assert!(err.to_string().contains("empty skip pattern"));

    let empty_reason = temp_path("ultrasql-slt-filter-empty-reason", "txt");
    std::fs::write(&empty_reason, "SELECT 1\t \n").expect("write filter");
    let err = SkipFilters::load_all(std::slice::from_ref(&empty_reason))
        .expect_err("empty reason must fail");
    assert!(err.to_string().contains("explicit reason"));

    let _ = std::fs::remove_file(missing_tab);
    let _ = std::fs::remove_file(empty_pattern);
    let _ = std::fs::remove_file(empty_reason);
}

#[test]
fn directives_are_consumed_per_case_and_file_scope_persists() {
    let mut directives = Directives::default();
    parse_directive("# ultrasql:file-skip whole file", &mut directives)
        .expect("file skip directive");
    parse_directive("# ultrasql:file-require json", &mut directives)
        .expect("file require directive");
    parse_directive("# ultrasql:skip next only", &mut directives).expect("next skip directive");
    parse_directive("# ultrasql:require xml", &mut directives).expect("next require directive");

    let (skip, requires) = directives.take_for_case();
    assert_eq!(skip, Some("whole file".to_owned()));
    assert_eq!(requires, vec!["json".to_owned(), "xml".to_owned()]);

    let (skip, requires) = directives.take_for_case();
    assert_eq!(skip, Some("whole file".to_owned()));
    assert_eq!(requires, vec!["json".to_owned()]);
}

#[test]
fn directives_reject_empty_and_unknown_directives() {
    let mut directives = Directives::default();
    let err =
        parse_directive("# ultrasql:skip", &mut directives).expect_err("empty skip must fail");
    assert!(err.to_string().contains("explicit reason"));

    let err = parse_directive("# ultrasql:file-skip", &mut directives)
        .expect_err("empty file skip must fail");
    assert!(err.to_string().contains("explicit reason"));

    let err = parse_directive("# ultrasql:unknown thing", &mut directives)
        .expect_err("unknown directive must fail");
    assert!(err.to_string().contains("unknown UltraSQL SLT directive"));

    assert!(!parse_directive("# other:skip", &mut directives).expect("non UltraSQL comment"));
}

#[test]
fn compact_sql_flattens_and_truncates_long_statements() {
    let sql = format!("SELECT\n{}\nFROM table", "x ".repeat(120));
    let compact = compact_sql(&sql);
    assert!(!compact.contains('\n'));
    assert!(compact.ends_with("..."));
    assert!(compact.len() <= 163);
}

#[test]
fn case_limit_truncates_across_files_and_drops_empty_tails() {
    let case_a = case_with_sql("SELECT 1");
    let case_b = case_with_sql("SELECT 2");
    let case_c = case_with_sql("SELECT 3");
    let mut cases_by_file = vec![
        (PathBuf::from("a.slt"), vec![case_a.clone(), case_b]),
        (PathBuf::from("b.slt"), vec![case_c]),
    ];

    apply_case_limit(&mut cases_by_file, 1);

    assert_eq!(cases_by_file.len(), 1);
    assert_eq!(cases_by_file[0].1.len(), 1);
    assert_eq!(cases_by_file[0].1[0].sql(), case_a.sql());
}

#[test]
fn selected_reference_engines_dedupe_and_validate_inputs() {
    let mut cli = empty_cli();
    cli.reference_engine = vec![ReferenceEngine::Duckdb, ReferenceEngine::Duckdb];
    assert_eq!(
        selected_reference_engines(&cli).expect("dedupe engines"),
        vec![ReferenceEngine::Duckdb]
    );

    cli.reference_url = Some("postgres://example".to_owned());
    let err = selected_reference_engines(&cli).expect_err("mixed URL and CLI engines fail");
    assert!(err.to_string().contains("only valid with postgres"));

    let mut cli = empty_cli();
    cli.reference_url = Some("postgres://example".to_owned());
    assert_eq!(
        selected_reference_engines(&cli).expect("reference URL implies postgres"),
        vec![ReferenceEngine::Postgres]
    );

    let mut cli = empty_cli();
    cli.reference_db = Some(PathBuf::from("ref.db"));
    cli.reference_engine = vec![ReferenceEngine::Postgres];
    let err = selected_reference_engines(&cli).expect_err("db path needs one CLI engine");
    assert!(err.to_string().contains("exactly one duckdb or sqlite"));

    let mut cli = empty_cli();
    cli.reference_db = Some(PathBuf::from("ref.db"));
    cli.reference_engine = vec![ReferenceEngine::Sqlite];
    assert_eq!(
        selected_reference_engines(&cli).expect("sqlite db path accepted"),
        vec![ReferenceEngine::Sqlite]
    );
}

#[tokio::test]
async fn benchmark_suite_rejects_zero_runs_before_connecting() {
    let cli = empty_cli();
    let err = run_benchmark_suite(
        &cli,
        &SkipFilters::default(),
        &BTreeSet::new(),
        &[case_with_sql("SELECT 1")],
        0,
    )
    .await
    .expect_err("zero benchmark runs fail");
    assert!(err.to_string().contains("greater than zero"));
}

#[test]
fn failed_benchmark_records_error_text() {
    let benchmark = EngineBenchmark::failed("duckdb", anyhow::anyhow!("missing binary"));
    assert_eq!(benchmark.engine, "duckdb");
    assert!(!benchmark.ok);
    assert_eq!(benchmark.error, Some("missing binary".to_owned()));
    assert_eq!(benchmark.query_iterations, 0);
}

#[test]
fn push_sql_statement_adds_one_terminator() {
    let mut script = String::new();
    push_sql_statement(&mut script, "SELECT 1");
    push_sql_statement(&mut script, "SELECT 2;\n");
    assert_eq!(script, "SELECT 1;\nSELECT 2;\n");
}

#[test]
fn benchmark_artifacts_escape_json_and_mark_fastest_engine() {
    let output = temp_path("ultrasql-slt-benchmark-artifact", "json");
    let markdown = output.with_extension("md");
    let benchmarks = vec![
        EngineBenchmark {
            engine: "ultra\"sql".to_owned(),
            ok: true,
            error: None,
            statements: 2,
            query_records: 1,
            query_iterations: 4,
            skipped: 0,
            total_ns: 4_000,
        },
        EngineBenchmark {
            engine: "slow".to_owned(),
            ok: false,
            error: Some("bad\nthing".to_owned()),
            statements: 0,
            query_records: 0,
            query_iterations: 0,
            skipped: 1,
            total_ns: 9_000,
        },
    ];

    write_benchmark_artifacts(
        &output,
        &[PathBuf::from("tests/slt/a\"b.slt")],
        &[case_with_sql("SELECT 1")],
        4,
        &benchmarks,
    )
    .expect("write artifacts");

    let json = std::fs::read_to_string(&output).expect("read benchmark json");
    assert!(json.contains("\"winner\": \"ultra\\\"sql\""));
    assert!(json.contains("\"bad\\nthing\""));
    assert!(json.contains("\"avg_ns_per_query_iteration\": 1000"));

    let md = std::fs::read_to_string(&markdown).expect("read benchmark markdown");
    assert!(md.contains("fastest_engine: `ultra\"sql`"));
    assert!(md.contains("| `ultra\"sql` | true | 2 | 1 | 4 | 0 | 0.004 | 1.000 |"));
    assert!(md.contains("| `slow` | false |"));

    let _ = std::fs::remove_file(output);
    let _ = std::fs::remove_file(markdown);
}

#[test]
fn escape_json_handles_quotes_slashes_and_controls() {
    assert_eq!(escape_json("\"\\\n\r\t\u{1f}"), "\\\"\\\\\\n\\r\\t\\u001f");
}

#[test]
fn cli_reference_rows_validate_shape_and_sort_rows() {
    assert_eq!(
        format_cli_reference_rows("b\r\na\n", "T", SortMode::RowSort).expect("format rows"),
        vec!["a".to_owned(), "b".to_owned()]
    );
    assert_eq!(
        format_cli_reference_rows("1\na\n2\nb\n", "IT", SortMode::NoSort)
            .expect("format two-column rows"),
        vec![
            "1".to_owned(),
            "a".to_owned(),
            "2".to_owned(),
            "b".to_owned()
        ]
    );

    let err = format_cli_reference_rows("", "", SortMode::NoSort)
        .expect_err("empty type string must fail");
    assert!(err.to_string().contains("at least one column"));

    let err = format_cli_reference_rows("1\n2\n3\n", "II", SortMode::NoSort)
        .expect_err("ragged values must fail");
    assert!(err.to_string().contains("not divisible"));
}

#[test]
fn effective_skip_reason_prefers_case_then_missing_feature_then_filter() {
    let filters = SkipFilters {
        patterns: vec![SkipPattern {
            pattern: "SELECT".to_owned(),
            reason: "filtered".to_owned(),
        }],
    };
    let mut case = case_with_sql("SELECT 1");
    case.skip_reason = Some("case skip".to_owned());
    case.requires = vec!["json".to_owned()];

    assert_eq!(
        effective_skip_reason(&filters, &BTreeSet::new(), &case),
        Some("case skip".to_owned())
    );

    case.skip_reason = None;
    assert_eq!(
        effective_skip_reason(&filters, &BTreeSet::new(), &case),
        Some("missing feature `json`".to_owned())
    );

    let enabled = BTreeSet::from(["json".to_owned()]);
    assert_eq!(
        effective_skip_reason(&filters, &enabled, &case),
        Some("filtered (SELECT)".to_owned())
    );
}

#[test]
fn query_expectation_values_and_hashes_report_mismatches() {
    compare_query_expectation(
        &["1".to_owned()],
        &QueryExpectation::Values(vec!["1".to_owned()]),
    )
    .expect("matching values");

    let err = compare_query_expectation(
        &["1".to_owned()],
        &QueryExpectation::Values(vec!["2".to_owned()]),
    )
    .expect_err("mismatched values fail");
    assert!(err.to_string().contains("expected values"));

    let digest = hash_query_values(&["1".to_owned()]);
    compare_query_expectation(
        &["1".to_owned()],
        &QueryExpectation::Hash {
            value_count: 1,
            digest,
        },
    )
    .expect("matching hash");

    let err = compare_query_expectation(
        &["1".to_owned(), "2".to_owned()],
        &QueryExpectation::Hash {
            value_count: 1,
            digest: "00000000000000000000000000000000".to_owned(),
        },
    )
    .expect_err("wrong hash count fails");
    assert!(err.to_string().contains("expected 1 hashed"));

    let err = compare_query_expectation(
        &["1".to_owned()],
        &QueryExpectation::Hash {
            value_count: 1,
            digest: "00000000000000000000000000000000".to_owned(),
        },
    )
    .expect_err("wrong hash digest fails");
    assert!(err.to_string().contains("expected hash"));
}

#[test]
fn collect_input_files_accepts_slt_and_test_files_only() {
    let root = temp_path("ultrasql-slt-collect", "dir");
    let nested = root.join("nested");
    std::fs::create_dir_all(&nested).expect("create nested directory");
    let slt = root.join("a.slt");
    let test = nested.join("b.test");
    let ignored = root.join("c.sql");
    std::fs::write(&slt, "").expect("write slt");
    std::fs::write(&test, "").expect("write test");
    std::fs::write(&ignored, "").expect("write ignored");

    let files = collect_input_files(std::slice::from_ref(&root)).expect("collect files");
    assert_eq!(files, vec![slt, test]);
    assert!(is_slt_file(Path::new("x.slt")));
    assert!(is_slt_file(Path::new("x.test")));
    assert!(!is_slt_file(Path::new("x.sql")));

    let missing = collect_input_files(&[root.join("missing")]).expect_err("missing path");
    assert!(missing.to_string().contains("test path does not exist"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn parse_script_handles_statements_queries_hashes_and_directives() {
    let path = Path::new("suite/basic.slt");
    let text = "\
# regular comment
hash-threshold 10
# ultrasql:require json
statement ok
CREATE TABLE t (x INTEGER)

statement error
SELECT nope

query IT rowsort
SELECT x, y FROM t
----
2
b
1
a

query I
SELECT 1
----
1 values hashing to B026324C6904B2A9CB4B88D6D61C81D1
";

    let cases = parse_script(path, text).expect("parse script");
    assert_eq!(cases.len(), 4);
    assert_eq!(cases[0].requires, vec!["json".to_owned()]);
    match &cases[0].kind {
        TestKind::Statement { expectation, sql } => {
            assert_eq!(*expectation, StatementExpectation::Ok);
            assert_eq!(sql, "CREATE TABLE t (x INTEGER)");
        }
        TestKind::Query { .. } => panic!("expected statement"),
    }
    match &cases[2].kind {
        TestKind::Query {
            type_string,
            sort_mode,
            expected,
            ..
        } => {
            assert_eq!(type_string, "IT");
            assert_eq!(*sort_mode, SortMode::RowSort);
            assert!(matches!(expected, QueryExpectation::Values(values) if values.len() == 4));
        }
        TestKind::Statement { .. } => panic!("expected query"),
    }
    match &cases[3].kind {
        TestKind::Query { expected, .. } => {
            assert!(matches!(
                expected,
                QueryExpectation::Hash {
                    value_count: 1,
                    digest
                } if digest == "b026324c6904b2a9cb4b88d6d61c81d1"
            ));
        }
        TestKind::Statement { .. } => panic!("expected query"),
    }
}

#[test]
fn parse_script_reports_malformed_records() {
    let path = Path::new("bad.slt");
    let cases = [
        (
            "statement maybe\nSELECT 1\n",
            "statement must declare `ok` or `error`",
        ),
        ("query\nSELECT 1\n----\n1\n", "query missing type string"),
        ("query I\nSELECT 1\n", "query missing ---- separator"),
        (
            "query I\nSELECT 1\n----\nnope values hashing to abc\n",
            "invalid hashed value count",
        ),
        (
            "query I\nSELECT 1\n----\n1 values hashing to xyz\n",
            "invalid SQLLogicTest MD5 digest",
        ),
        ("nonsense\n", "unsupported SQLLogicTest directive"),
    ];

    for (script, message) in cases {
        let err = parse_script(path, script).expect_err("malformed script must fail");
        assert!(
            format!("{err:#}").contains(message),
            "expected `{message}` in `{err:#}`"
        );
    }
}

#[test]
fn query_header_ignores_unknown_options_but_keeps_sort_contract() {
    assert_eq!(
        parse_query_header(Path::new("x.slt"), 1, " I nosort label").expect("parse nosort"),
        ("I".to_owned(), SortMode::NoSort)
    );
    assert_eq!(
        parse_query_header(Path::new("x.slt"), 1, " I sort").expect("parse sort"),
        ("I".to_owned(), SortMode::RowSort)
    );
    assert_eq!(
        parse_query_header(Path::new("x.slt"), 1, " I rowsort").expect("parse rowsort"),
        ("I".to_owned(), SortMode::RowSort)
    );
}

#[test]
fn collectors_stop_at_blank_lines() {
    let lines = ["SELECT 1", "FROM t", "", "ignored"];
    let (sql, idx) = collect_until_blank(&lines, 0);
    assert_eq!(sql, "SELECT 1\nFROM t");
    assert_eq!(idx, 3);

    let query_lines = ["SELECT 1", "----", "1", "", "ignored"];
    let (sql, expected, idx) = collect_query(&query_lines, 0).expect("collect query");
    assert_eq!(sql, "SELECT 1");
    assert!(matches!(expected, QueryExpectation::Values(values) if values == vec!["1"]));
    assert_eq!(idx, 4);
}
