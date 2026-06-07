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
NUMERIC_AS_CAST = re.compile(
    r"\bas\s+(?:usize|u8|u16|u32|u64|i8|i16|i32|i64|isize|f32|f64)\b"
)
NEAREST_RANK_INDEX_CAST = re.compile(r"rank\.max\(1\.0\)\s+as\s+usize")


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


if __name__ == "__main__":
    unittest.main()
