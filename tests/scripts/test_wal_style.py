import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
WAL_RECORD_BENCH = REPO / "crates" / "ultrasql-wal" / "benches" / "record.rs"
WAL_BENCH_THROUGHPUT_CAST = re.compile(r"\bn\s+as\s+u64\b")


class WalStyleTests(unittest.TestCase):
    def test_wal_record_bench_uses_checked_throughput_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(WAL_RECORD_BENCH.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if WAL_BENCH_THROUGHPUT_CAST.search(code):
                offenders.append(f"{WAL_RECORD_BENCH.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
