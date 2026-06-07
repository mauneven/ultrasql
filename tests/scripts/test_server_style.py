import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SERVER_SRC = REPO / "crates" / "ultrasql-server" / "src"
INDEX_SCAN_ROUND_TRIP = REPO / "crates" / "ultrasql-server" / "tests" / "index_scan_round_trip.rs"
FLOAT_AS_CAST = re.compile(r"\bas\s+(?:f32|f64)\b|allow\([^)]*clippy::cast_")


class ServerStyleTests(unittest.TestCase):
    def test_server_source_uses_checked_float_conversions(self) -> None:
        offenders: list[str] = []
        for path in sorted(SERVER_SRC.rglob("*.rs")):
            for line_no, line in enumerate(path.read_text().splitlines(), start=1):
                code = line.split("//", maxsplit=1)[0]
                if FLOAT_AS_CAST.search(code):
                    offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_index_scan_benchmark_uses_checked_float_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(INDEX_SCAN_ROUND_TRIP.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if FLOAT_AS_CAST.search(code):
                offenders.append(
                    f"{INDEX_SCAN_ROUND_TRIP.relative_to(REPO)}:{line_no}: {line.strip()}"
                )

        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
