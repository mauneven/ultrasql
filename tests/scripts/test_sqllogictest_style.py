import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SQLLOGICTEST_MAIN = REPO / "crates" / "ultrasql-sqllogictest-runner" / "src" / "main.rs"
FLOAT_AS_CAST = re.compile(r"\bas\s+(?:f32|f64)\b|clippy::cast_")


class SqlLogicTestStyleTests(unittest.TestCase):
    def test_sqllogictest_runner_uses_checked_float_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(SQLLOGICTEST_MAIN.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if FLOAT_AS_CAST.search(code):
                offenders.append(f"{SQLLOGICTEST_MAIN.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
