import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
BENCH_FILES = [
    REPO / "crates" / "ultrasql-bench" / "src" / "bin" / "readme_render.rs",
    REPO / "crates" / "ultrasql-bench" / "src" / "bin" / "results_render.rs",
]
ANN_FILES = [
    REPO / "crates" / "ultrasql-bench" / "src" / "ai_gauntlet.rs",
    REPO / "crates" / "ultrasql-bench" / "src" / "ann_vector.rs",
]
SQL_BENCH_FILES = [
    REPO / "crates" / "ultrasql-bench" / "src" / "bin" / "cross_compare_sql.rs",
]
PUSH_VS_PULL_BENCH = REPO / "crates" / "ultrasql-bench" / "benches" / "push_vs_pull.rs"
TPCH_DATA_GEN_FILE = (
    REPO / "crates" / "ultrasql-bench" / "src" / "tpch" / "data_gen.rs"
)
TPCH_LOAD_FILE = REPO / "crates" / "ultrasql-bench" / "src" / "tpch" / "load.rs"
NUMERIC_AS_CAST = re.compile(
    r"\bas\s+(?:usize|u8|u16|u32|u64|i8|i16|i32|i64|isize|f32|f64)\b"
)
TPCH_DATA_GEN_CAST = re.compile(r"\bas\s+(?:usize|u32|u8|char)\b")
TPCH_LOAD_DATE_CAST = re.compile(r"\bas\s+(?:u32|i32)\b")
NEAREST_RANK_INDEX_CAST = re.compile(r"rank\.max\(1\.0\)\s+as\s+usize")
ANN_VECTOR_SEED_WIDTH_CAST = re.compile(r"\b(?:row_id|query_id|dim)\s+as\s+u64\b")
ANN_VECTOR_COMPONENT_FLOAT_CAST = re.compile(r"\)\s+as\s+f32\s*/\s*(?:37|41)\.0")
SQL_PERCENTILE_INDEX_CAST = re.compile(
    r"sorted_values\.len\(\)\s+as\s+f64.*ceil\(\)\s+as\s+usize"
)
SQL_RNG_WIDTH_CAST = re.compile(r"next_u64\(\)\s+as\s+i32")
PUSH_VS_PULL_BENCH_CASTS = re.compile(
    r"\bas\s+(?:usize|u32|u64|i32|i64)\b|clippy::cast_"
)
BTREE_SHUFFLE_INDEX_CAST = re.compile(r"\bs\s+as\s+usize\)\s*%\s*\(i\s*\+\s*1\)")
MIXED_OLTP_INDEX_CAST = re.compile(r"\bs\s+as\s+usize\s*>>\s*7\)\s*%")
MIXED_OLTP_KIND_WIDTH_CAST = re.compile(r"\(\s*s\s*%\s*100\s*\)\s+as\s+u8\b")
MIXED_OLTP_ITERATION_WIDTH_CAST = re.compile(
    r"\bctx\.(?:iterations|warmup_iterations)\s+as\s+usize\b"
)
TPCB_INDEX_CAST = re.compile(r"\bs\s+as\s+usize\)\s*%\s*(?:accounts|tellers)\.len\(\)")
TPCB_ITERATION_WIDTH_CAST = re.compile(r"\bctx\.iterations\s+as\s+usize\b")
TPCH_Q22_COUNTRY_INDEX_CAST = re.compile(
    r"\bs\s+as\s+usize\s*>>\s*8\)\s*%\s*COUNTRY_CODES\.len\(\)"
)
TPCH_Q22_ITERATION_WIDTH_CAST = re.compile(r"\bctx\.iterations\s+as\s+usize\b")
TPCC_CHOOSE_INDEX_CAST = re.compile(r"\bseed\s+as\s+usize\)\s*%\s*cardinality\b")
TPCC_SEED_WIDTH_CAST = re.compile(r"\b(?:client|tx)\s+as\s+u64\b")
TPCC_SELECTOR_WIDTH_CAST = re.compile(r"\(\s*seed\s*%\s*100\s*\)\s+as\s+u8\b")
TPCC_CHECKSUM_WIDTH_CAST = re.compile(
    r"\b(?:order_id|before|warehouse_ytd|district_ytd|balance|last_order|next_order|delivered_order)\s+as\s+u64\b"
)
TPCC_ITERATION_WIDTH_CAST = re.compile(r"\bctx\.iterations\s+as\s+usize\b")
CROSS_CONCURRENCY_THREAD_WIDTH_CAST = re.compile(r"\btid\s+as\s+u64\b")
CROSS_CONCURRENCY_MEASURE_WIDTH_CAST = re.compile(r"\bmeasure_secs\s+as\s+usize\b")
SELECT_AVG_ITERATION_WIDTH_CAST = re.compile(r"\bctx\.iterations\s+as\s+usize\b")
SELECT_SUM_ITERATION_WIDTH_CAST = re.compile(r"\bctx\.iterations\s+as\s+usize\b")
FILTER_SUM_ITERATION_WIDTH_CAST = re.compile(r"\bctx\.iterations\s+as\s+usize\b")
POINT_LOOKUP_ITERATION_WIDTH_CAST = re.compile(r"\bctx\.iterations\s+as\s+usize\b")
BTREE_POINT_LOOKUP_ITERATION_WIDTH_CAST = re.compile(
    r"\bctx\.iterations\s+as\s+usize\b"
)
HASH_AGGREGATE_ITERATION_WIDTH_CAST = re.compile(r"\bctx\.iterations\s+as\s+usize\b")
HASH_AGGREGATE_GROUP_WIDTH_CAST = re.compile(r"\bGROUP_COUNT\s+as\s+usize\b")
RANGE_SCAN_ITERATION_WIDTH_CAST = re.compile(r"\bctx\.iterations\s+as\s+usize\b")
SORT_LARGE_ITERATION_WIDTH_CAST = re.compile(r"\bctx\.iterations\s+as\s+usize\b")
DELETE_THROUGHPUT_ITERATION_WIDTH_CAST = re.compile(
    r"\bctx\.iterations\s+as\s+usize\b"
)
INSERT_THROUGHPUT_ITERATION_WIDTH_CAST = re.compile(
    r"\bctx\.iterations\s+as\s+usize\b"
)
UPDATE_THROUGHPUT_ITERATION_WIDTH_CAST = re.compile(
    r"\bctx\.(?:iterations|warmup_iterations)\s+as\s+usize\b"
)
HONESTY_ITERATION_WIDTH_CAST = re.compile(r"\bctx\.iterations\s+as\s+usize\b")
TPCH_Q1_ITERATION_WIDTH_CAST = re.compile(r"\bctx\.iterations\s+as\s+usize\b")


class BenchStyleTests(unittest.TestCase):
    def test_bench_renderer_uses_checked_numeric_conversions(self) -> None:
        offenders: list[str] = []
        for path in BENCH_FILES:
            for line_no, line in enumerate(path.read_text().splitlines(), start=1):
                code = line.split("//", maxsplit=1)[0]
                if NUMERIC_AS_CAST.search(code):
                    offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_ann_percentiles_use_checked_index_conversions(self) -> None:
        offenders: list[str] = []
        for path in ANN_FILES:
            for line_no, line in enumerate(path.read_text().splitlines(), start=1):
                code = line.split("//", maxsplit=1)[0]
                if NEAREST_RANK_INDEX_CAST.search(code):
                    offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_ann_vector_generators_use_checked_seed_conversions(self) -> None:
        offenders: list[str] = []
        for path in ANN_FILES:
            for line_no, line in enumerate(path.read_text().splitlines(), start=1):
                code = line.split("//", maxsplit=1)[0]
                if ANN_VECTOR_SEED_WIDTH_CAST.search(code):
                    offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_ann_vector_generators_use_checked_component_conversions(self) -> None:
        offenders: list[str] = []
        for path in ANN_FILES:
            for line_no, line in enumerate(path.read_text().splitlines(), start=1):
                code = line.split("//", maxsplit=1)[0]
                if ANN_VECTOR_COMPONENT_FLOAT_CAST.search(code):
                    offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_sql_bench_percentiles_use_checked_index_conversions(self) -> None:
        offenders: list[str] = []
        for path in SQL_BENCH_FILES:
            for line_no, line in enumerate(path.read_text().splitlines(), start=1):
                code = line.split("//", maxsplit=1)[0]
                if SQL_PERCENTILE_INDEX_CAST.search(code):
                    offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_sql_bench_rng_avoids_integer_width_casts(self) -> None:
        offenders: list[str] = []
        for path in SQL_BENCH_FILES:
            for line_no, line in enumerate(path.read_text().splitlines(), start=1):
                code = line.split("//", maxsplit=1)[0]
                if SQL_RNG_WIDTH_CAST.search(code):
                    offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_push_vs_pull_bench_uses_checked_integer_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(PUSH_VS_PULL_BENCH.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if PUSH_VS_PULL_BENCH_CASTS.search(code):
                offenders.append(f"{PUSH_VS_PULL_BENCH.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_tpch_data_gen_uses_checked_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(TPCH_DATA_GEN_FILE.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TPCH_DATA_GEN_CAST.search(code):
                offenders.append(
                    f"{TPCH_DATA_GEN_FILE.relative_to(REPO)}:{line_no}: {line.strip()}"
                )

        self.assertEqual([], offenders)

    def test_tpch_load_date_math_uses_checked_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(TPCH_LOAD_FILE.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TPCH_LOAD_DATE_CAST.search(code):
                offenders.append(f"{TPCH_LOAD_FILE.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_btree_bench_shuffle_uses_checked_index_conversion(self) -> None:
        offenders: list[str] = []
        paths = [
            REPO / "crates" / "ultrasql-bench" / "src" / "bin" / "cross_compare.rs",
            REPO / "crates" / "ultrasql-bench" / "src" / "bin" / "cross_concurrency.rs",
            REPO / "crates" / "ultrasql-bench" / "src" / "bin" / "point_lookup.rs",
            REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "point_lookup.rs",
        ]
        for path in paths:
            for line_no, line in enumerate(path.read_text().splitlines(), start=1):
                code = line.split("//", maxsplit=1)[0]
                if BTREE_SHUFFLE_INDEX_CAST.search(code):
                    offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_mixed_oltp_uses_checked_rng_index_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "mixed_oltp.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if MIXED_OLTP_INDEX_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_mixed_oltp_uses_checked_kind_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "mixed_oltp.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if MIXED_OLTP_KIND_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_mixed_oltp_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "mixed_oltp.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if MIXED_OLTP_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_tpcb_uses_checked_rng_index_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "tpcb.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TPCB_INDEX_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_tpcb_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "tpcb.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TPCB_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_tpch_q22_uses_checked_country_index_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "tpch_q22.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TPCH_Q22_COUNTRY_INDEX_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_tpch_q22_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "tpch_q22.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TPCH_Q22_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_tpcc_uses_checked_choose_index_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "tpcc.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TPCC_CHOOSE_INDEX_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_tpcc_uses_checked_seed_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "tpcc.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TPCC_SEED_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_tpcc_uses_checked_selector_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "tpcc.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TPCC_SELECTOR_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_tpcc_uses_checked_checksum_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "tpcc.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TPCC_CHECKSUM_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_tpcc_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "tpcc.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TPCC_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_cross_concurrency_uses_checked_thread_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "bin" / "cross_concurrency.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if CROSS_CONCURRENCY_THREAD_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_cross_concurrency_uses_checked_measurement_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "bin" / "cross_concurrency.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if CROSS_CONCURRENCY_MEASURE_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_select_avg_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "select_avg_10m.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if SELECT_AVG_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_select_sum_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "select_sum_65k.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if SELECT_SUM_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_filter_sum_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "filter_sum_10m.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if FILTER_SUM_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_point_lookup_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "point_lookup.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if POINT_LOOKUP_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_btree_point_lookup_uses_checked_iteration_width_conversions(
        self,
    ) -> None:
        offenders: list[str] = []
        path = (
            REPO
            / "crates"
            / "ultrasql-bench"
            / "src"
            / "runs"
            / "btree_point_lookup.rs"
        )
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if BTREE_POINT_LOOKUP_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_hash_aggregate_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "hash_aggregate.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if HASH_AGGREGATE_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_hash_aggregate_uses_checked_group_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "hash_aggregate.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if HASH_AGGREGATE_GROUP_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_range_scan_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "range_scan.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if RANGE_SCAN_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_sort_large_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "sort_large.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if SORT_LARGE_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_delete_throughput_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = (
            REPO
            / "crates"
            / "ultrasql-bench"
            / "src"
            / "runs"
            / "delete_throughput.rs"
        )
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if DELETE_THROUGHPUT_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_insert_throughput_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = (
            REPO
            / "crates"
            / "ultrasql-bench"
            / "src"
            / "runs"
            / "insert_throughput.rs"
        )
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if INSERT_THROUGHPUT_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_update_throughput_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = (
            REPO
            / "crates"
            / "ultrasql-bench"
            / "src"
            / "runs"
            / "update_throughput.rs"
        )
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if UPDATE_THROUGHPUT_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_honesty_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "tests" / "honesty.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if HONESTY_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_tpch_q1_uses_checked_iteration_width_conversions(self) -> None:
        offenders: list[str] = []
        path = REPO / "crates" / "ultrasql-bench" / "src" / "runs" / "tpch_q1.rs"
        for line_no, line in enumerate(path.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TPCH_Q1_ITERATION_WIDTH_CAST.search(code):
                offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
