//! Parquet table-function scan for local files and object-store URIs.

mod object_range;
mod paths;
mod plan_summary;
mod predicate;
mod pruning;
mod scan;
mod schema;

pub(crate) use plan_summary::{parquet_columns_read_for_plan, parquet_row_group_summary_for_plan};
pub(crate) use scan::ParquetTableScan;

#[cfg(test)]
use object_range::parquet_range_pos_add;
#[cfg(test)]
use paths::{expand_parquet_paths, wildcard_match};
#[cfg(test)]
use plan_summary::parquet_path_row_group_summary;
#[cfg(test)]
use predicate::ParquetLiteral;
#[cfg(test)]
use scan::open_regular_parquet_file;

pub(in crate::pipeline) use predicate::ParquetPredicate;

const PARQUET_BATCH_TARGET_ROWS: usize = 4096;
const MAX_LOCAL_WILDCARD_PATTERN_CHARS: usize = 4096;

/// Row-group pruning evidence for `read_parquet` scans.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ParquetRowGroupSummary {
    pub(crate) scanned: u64,
    pub(crate) skipped: u64,
}

impl ParquetRowGroupSummary {
    pub(super) fn add(&mut self, other: Self) {
        self.scanned = self.scanned.saturating_add(other.scanned);
        self.skipped = self.skipped.saturating_add(other.skipped);
    }
}

#[cfg(test)]
mod tests {
    use super::{ParquetPredicate, ParquetTableScan};
    use std::fs;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use ultrasql_core::{DataType, Value};
    use ultrasql_executor::Operator;
    use ultrasql_planner::{BinaryOp, ScalarExpr};
    use ultrasql_vec::column::Column;

    #[test]
    fn simple_column_literal_predicate_is_pushable() {
        let expr = ScalarExpr::Binary {
            op: BinaryOp::GtEq,
            left: Box::new(ScalarExpr::Column {
                name: "id".to_owned(),
                index: 0,
                data_type: DataType::Int64,
            }),
            right: Box::new(ScalarExpr::Literal {
                value: Value::Int64(100),
                data_type: DataType::Int64,
            }),
            data_type: DataType::Bool,
        };
        let predicate = ParquetPredicate::from_scalar(&expr).expect("pushable predicate");
        assert_eq!(predicate.column, "id");
        assert_eq!(predicate.op, BinaryOp::GtEq);
    }

    #[test]
    fn literal_column_predicate_reverses_operator() {
        let expr = ScalarExpr::Binary {
            op: BinaryOp::LtEq,
            left: Box::new(ScalarExpr::Literal {
                value: Value::Int64(100),
                data_type: DataType::Int64,
            }),
            right: Box::new(ScalarExpr::Column {
                name: "id".to_owned(),
                index: 0,
                data_type: DataType::Int64,
            }),
            data_type: DataType::Bool,
        };
        let predicate = ParquetPredicate::from_scalar(&expr).expect("pushable predicate");
        assert_eq!(predicate.column, "id");
        assert_eq!(predicate.op, BinaryOp::GtEq);
    }

    #[test]
    fn wildcard_match_supports_star_and_question_mark() {
        let paths = ParquetTableScan::from_path_specs(&[], None, None)
            .expect_err("empty path list must fail");
        assert!(paths.to_string().contains("path list cannot be empty"));
        assert!(super::wildcard_match("part-*.parquet", "part-001.parquet"));
        assert!(super::wildcard_match("part-??.parquet", "part-01.parquet"));
        assert!(!super::wildcard_match(
            "part-??.parquet",
            "part-001.parquet"
        ));
    }

    #[test]
    fn parquet_glob_rejects_oversized_wildcard_pattern() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pattern = dir.path().join(format!("{}*.parquet", "x".repeat(4096)));

        let err = super::expand_parquet_paths(&pattern.to_string_lossy())
            .expect_err("oversized wildcard pattern must fail before directory scan");

        assert!(
            err.to_string().contains("wildcard pattern too long"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn object_range_cursor_rejects_position_overflow() {
        let err =
            super::parquet_range_pos_add(u64::MAX, 1, "s3://bucket/file.parquet").unwrap_err();
        assert!(err.to_string().contains("range cursor position overflow"));
    }

    #[cfg(unix)]
    #[test]
    fn parquet_scan_rejects_symlinked_input_file() {
        use super::open_regular_parquet_file;
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("target.parquet");
        let link = dir.path().join("link.parquet");
        fs::write(&target, b"not parquet").expect("write target");
        symlink(&target, &link).expect("symlink parquet");

        assert!(open_regular_parquet_file(&link, &link.display().to_string(), "open").is_err());
    }

    #[test]
    fn parquet_scan_defers_later_file_batches_until_needed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = dir.path().join("first.parquet");
        let second = dir.path().join("second.parquet");
        write_i64_parquet(&first, &[1]);
        write_i64_parquet(&second, &[2]);
        let path_specs = vec![first.display().to_string(), second.display().to_string()];

        let mut scan =
            ParquetTableScan::from_path_specs(&path_specs, None, None).expect("construct scan");
        fs::remove_file(&second).expect("remove second parquet");

        let first_batch = scan
            .next_batch()
            .expect("read first file")
            .expect("first batch");
        let Column::Int64(values) = &first_batch.columns()[0] else {
            panic!("expected int64 column");
        };
        assert_eq!(values.data(), &[1]);

        let err = scan
            .next_batch()
            .expect_err("second file read should be lazy");
        let message = err.to_string();
        assert!(
            message.contains("cannot inspect") && message.contains("second.parquet"),
            "unexpected lazy read error: {message}"
        );
    }

    #[test]
    fn parquet_scan_splits_selected_row_groups_across_workers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("groups.parquet");
        write_i64_parquet_groups(&path, &[&[1], &[2], &[3], &[4]]);
        let path_specs = vec![path.display().to_string()];
        let predicate = ParquetPredicate {
            column: "id".to_owned(),
            op: BinaryOp::GtEq,
            literal: super::ParquetLiteral::Int64(2),
        };

        let mut scan = ParquetTableScan::from_path_specs(&path_specs, None, Some(&predicate))
            .expect("construct scan");
        let first_batch = scan
            .next_batch()
            .expect("read first parallel batch")
            .expect("first batch");
        let worker_count = scan
            .active
            .as_ref()
            .map_or(0, |active| active.workers.len());
        assert!(
            worker_count > 1,
            "selected row groups must split across workers, got {worker_count}"
        );

        let mut ids = collect_i64_ids(&first_batch);
        while let Some(batch) = scan.next_batch().expect("read next parallel batch") {
            ids.extend(collect_i64_ids(&batch));
        }
        ids.sort_unstable();
        assert_eq!(ids, vec![2, 3, 4]);
    }

    #[test]
    fn parquet_predicate_pushdown_skips_all_null_row_groups() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nulls.parquet");
        write_nullable_i64_parquet_groups(&path, &[vec![None, None], vec![Some(7), Some(9)]]);
        let predicate = ParquetPredicate {
            column: "id".to_owned(),
            op: BinaryOp::Eq,
            literal: super::ParquetLiteral::Int64(7),
        };

        let summary =
            super::parquet_path_row_group_summary(&path, Some(&predicate)).expect("summary");

        assert_eq!(
            summary,
            super::ParquetRowGroupSummary {
                scanned: 1,
                skipped: 1,
            }
        );
    }

    #[test]
    fn parquet_dictionary_pruning_skips_absent_text_values() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("dict.parquet");
        write_string_dictionary_parquet_groups(&path, &[&["alpha", "gamma"], &["delta"]]);
        let predicate = ParquetPredicate {
            column: "category".to_owned(),
            op: BinaryOp::Eq,
            literal: super::ParquetLiteral::Text("beta".to_owned()),
        };

        let summary =
            super::parquet_path_row_group_summary(&path, Some(&predicate)).expect("summary");

        assert_eq!(
            summary,
            super::ParquetRowGroupSummary {
                scanned: 0,
                skipped: 2,
            }
        );
    }

    fn write_i64_parquet(path: &std::path::Path, values: &[i64]) {
        write_i64_parquet_groups(path, &[values]);
    }

    fn write_i64_parquet_groups(path: &std::path::Path, groups: &[&[i64]]) {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let file = fs::File::create(path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("writer");
        for values in groups {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values.to_vec()))],
            )
            .expect("record batch");
            writer.write(&batch).expect("write parquet row group");
            writer.flush().expect("flush parquet row group");
        }
        writer.close().expect("close parquet");
    }

    fn write_nullable_i64_parquet_groups(path: &std::path::Path, groups: &[Vec<Option<i64>>]) {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int64,
            true,
        )]));
        let file = fs::File::create(path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("writer");
        for values in groups {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values.clone()))],
            )
            .expect("record batch");
            writer.write(&batch).expect("write parquet row group");
            writer.flush().expect("flush parquet row group");
        }
        writer.close().expect("close parquet");
    }

    fn write_string_dictionary_parquet_groups(path: &std::path::Path, groups: &[&[&str]]) {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "category",
            ArrowDataType::Utf8,
            false,
        )]));
        let props = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .build();
        let file = fs::File::create(path).expect("create parquet");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).expect("writer");
        for values in groups {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from_iter_values(
                    values.iter().copied(),
                ))],
            )
            .expect("record batch");
            writer.write(&batch).expect("write parquet row group");
            writer.flush().expect("flush parquet row group");
        }
        writer.close().expect("close parquet");
    }

    fn collect_i64_ids(batch: &ultrasql_vec::Batch) -> Vec<i64> {
        let Column::Int64(values) = &batch.columns()[0] else {
            panic!("expected int64 column");
        };
        values.data().to_vec()
    }
}
