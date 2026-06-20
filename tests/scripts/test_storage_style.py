import re
import unittest
from pathlib import Path

from _source_text import module_text


REPO = Path(__file__).resolve().parents[2]
STORAGE_PAGE_BENCH = REPO / "crates" / "ultrasql-storage" / "benches" / "page.rs"
STORAGE_EXTRAS_BENCH = (
    REPO / "crates" / "ultrasql-storage" / "benches" / "storage_extras.rs"
)
STORAGE_V08_BENCH = REPO / "crates" / "ultrasql-storage" / "benches" / "v08.rs"
STORAGE_BTREE_TESTS = REPO / "crates" / "ultrasql-storage" / "src" / "btree" / "tests.rs"
STORAGE_HEAP_TESTS = REPO / "crates" / "ultrasql-storage" / "src" / "heap" / "tests.rs"
STORAGE_VACUUM_TESTS = REPO / "crates" / "ultrasql-storage" / "tests" / "vacuum.rs"
STORAGE_RECOVERY_SIM_TESTS = REPO / "crates" / "ultrasql-storage" / "tests" / "recovery_sim.rs"
STORAGE_HEAP_WAL_EMIT = REPO / "crates" / "ultrasql-storage" / "src" / "heap" / "wal_emit.rs"
STORAGE_PAGE_THROUGHPUT_CAST = re.compile(
    r"\b(?:tuple_size|slots\.len\(\))\s+as\s+u64\b"
)
STORAGE_EXTRAS_THROUGHPUT_CAST = re.compile(r"\bsize\s+as\s+u64\b")
STORAGE_V08_BENCH_CASTS = re.compile(
    r"\bN\s+as\s+u64\b|\bN\s+as\s+i64\b|\bi\s+as\s+u32\b|clippy::cast_"
)
STORAGE_BTREE_TEST_CASTS = re.compile(
    r"\bas\s+(?:usize|u8|u16|u32|u64|i8|i16|i32|i64|isize|f32|f64)\b|clippy::cast_"
)
STORAGE_HEAP_TEST_CASTS = re.compile(
    r"\bN\s+as\s+usize\b|\(2 \* N\)\s+as\s+usize\b|\bi\s+as\s+u8\b|clippy::cast_"
)
STORAGE_VACUUM_TEST_CASTS = re.compile(r"\bi\s+as\s+i32\b|clippy::cast_")
STORAGE_RECOVERY_SIM_TEST_CASTS = re.compile(
    r"count\(\)\s+as\s+u64|\bINSERTS_PER_XID\s+as\s+u64\b|\bi\s+as\s+u16\b|"
    r"\bslot\s+as\s+u16\b|\bi\s+as\s+i32\b|clippy::cast_"
)


class StorageStyleTests(unittest.TestCase):
    def test_page_bench_uses_checked_throughput_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(module_text(STORAGE_PAGE_BENCH).splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if STORAGE_PAGE_THROUGHPUT_CAST.search(code):
                offenders.append(f"{STORAGE_PAGE_BENCH.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_storage_extras_bench_uses_checked_throughput_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(module_text(STORAGE_EXTRAS_BENCH).splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if STORAGE_EXTRAS_THROUGHPUT_CAST.search(code):
                offenders.append(
                    f"{STORAGE_EXTRAS_BENCH.relative_to(REPO)}:{line_no}: {line.strip()}"
                )

        self.assertEqual([], offenders)

    def test_v08_bench_uses_checked_integer_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(module_text(STORAGE_V08_BENCH).splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if STORAGE_V08_BENCH_CASTS.search(code):
                offenders.append(f"{STORAGE_V08_BENCH.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_btree_tests_use_checked_integer_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(module_text(STORAGE_BTREE_TESTS).splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if STORAGE_BTREE_TEST_CASTS.search(code):
                offenders.append(f"{STORAGE_BTREE_TESTS.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_heap_tests_use_checked_integer_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(module_text(STORAGE_HEAP_TESTS).splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if STORAGE_HEAP_TEST_CASTS.search(code):
                offenders.append(f"{STORAGE_HEAP_TESTS.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_vacuum_tests_use_checked_integer_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(module_text(STORAGE_VACUUM_TESTS).splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if STORAGE_VACUUM_TEST_CASTS.search(code):
                offenders.append(f"{STORAGE_VACUUM_TESTS.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_recovery_sim_tests_use_checked_integer_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(module_text(STORAGE_RECOVERY_SIM_TESTS).splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if STORAGE_RECOVERY_SIM_TEST_CASTS.search(code):
                offenders.append(
                    f"{STORAGE_RECOVERY_SIM_TESTS.relative_to(REPO)}:{line_no}: {line.strip()}"
                )

        self.assertEqual([], offenders)

    def test_fpw_lsn_skip_happens_before_page_image_copy(self) -> None:
        text = module_text(STORAGE_HEAP_WAL_EMIT)
        start = text.index("pub(super) fn maybe_emit_fpw")
        body = text[start : text.index("/// Emit a `HeapInsert`", start)]

        skip = body.index("if page_lsn >= checkpoint_lsn")
        copy = body.index("to_vec()")

        self.assertLess(skip, copy)


if __name__ == "__main__":
    unittest.main()
