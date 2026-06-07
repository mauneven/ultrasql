import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
STORAGE_PAGE_BENCH = REPO / "crates" / "ultrasql-storage" / "benches" / "page.rs"
STORAGE_EXTRAS_BENCH = (
    REPO / "crates" / "ultrasql-storage" / "benches" / "storage_extras.rs"
)
STORAGE_PAGE_THROUGHPUT_CAST = re.compile(
    r"\b(?:tuple_size|slots\.len\(\))\s+as\s+u64\b"
)
STORAGE_EXTRAS_THROUGHPUT_CAST = re.compile(r"\bsize\s+as\s+u64\b")


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


if __name__ == "__main__":
    unittest.main()
