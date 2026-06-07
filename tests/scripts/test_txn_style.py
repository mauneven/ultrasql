import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
TXN_FILES = [
    REPO / "crates" / "ultrasql-txn" / "src" / "lock.rs",
    REPO / "crates" / "ultrasql-txn" / "src" / "manager.rs",
]
TXN_V04_BENCH = REPO / "crates" / "ultrasql-txn" / "benches" / "txn_v04.rs"
INTEGER_AS_CAST = re.compile(r"\bas\s+(?:usize|u8|u16|u32|u64|i8|i16|i32|i64|isize)\b")
TXN_V04_BENCH_CASTS = re.compile(
    r"\bas\s+(?:usize|u8|u16|u32|u64|i8|i16|i32|i64|isize)\b|clippy::cast_"
)


class TxnStyleTests(unittest.TestCase):
    def test_txn_source_uses_checked_integer_conversions(self) -> None:
        offenders: list[str] = []
        for path in TXN_FILES:
            for line_no, line in enumerate(path.read_text().splitlines(), start=1):
                code = line.split("//", maxsplit=1)[0]
                if INTEGER_AS_CAST.search(code):
                    offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_txn_v04_bench_uses_checked_integer_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(TXN_V04_BENCH.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if TXN_V04_BENCH_CASTS.search(code):
                offenders.append(f"{TXN_V04_BENCH.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
