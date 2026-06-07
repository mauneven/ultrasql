import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
STORAGE_PAGE_BENCH = REPO / "crates" / "ultrasql-storage" / "benches" / "page.rs"
STORAGE_EXTRAS_BENCH = (
    REPO / "crates" / "ultrasql-storage" / "benches" / "storage_extras.rs"
)
STORAGE_BTREE_TESTS = REPO / "crates" / "ultrasql-storage" / "src" / "btree" / "tests.rs"
STORAGE_PAGE_THROUGHPUT_CAST = re.compile(
    r"\b(?:tuple_size|slots\.len\(\))\s+as\s+u64\b"
)
STORAGE_EXTRAS_THROUGHPUT_CAST = re.compile(r"\bsize\s+as\s+u64\b")
STORAGE_BTREE_SHUFFLE_CAST = re.compile(r"\bs\s+as\s+usize\b")


class StorageStyleTests(unittest.TestCase):
    def test_page_bench_uses_checked_throughput_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(STORAGE_PAGE_BENCH.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if STORAGE_PAGE_THROUGHPUT_CAST.search(code):
                offenders.append(f"{STORAGE_PAGE_BENCH.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_storage_extras_bench_uses_checked_throughput_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(STORAGE_EXTRAS_BENCH.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if STORAGE_EXTRAS_THROUGHPUT_CAST.search(code):
                offenders.append(
                    f"{STORAGE_EXTRAS_BENCH.relative_to(REPO)}:{line_no}: {line.strip()}"
                )

        self.assertEqual([], offenders)

    def test_btree_shuffle_uses_checked_index_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(STORAGE_BTREE_TESTS.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if STORAGE_BTREE_SHUFFLE_CAST.search(code):
                offenders.append(f"{STORAGE_BTREE_TESTS.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
