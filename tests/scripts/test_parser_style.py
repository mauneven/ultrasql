import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
STATEMENTS = REPO / "crates" / "ultrasql-parser" / "src" / "statements"
DIRECT_SPAN_CAST = re.compile(
    r"(?:\.span(?:\(\))?|\b(?:\w+_)?span)\.(?:start|end)\s+as\s+usize"
)


class ParserStyleTests(unittest.TestCase):
    def test_statement_diagnostics_use_span_offset_accessors(self) -> None:
        offenders: list[str] = []
        for path in sorted(STATEMENTS.glob("*.rs")):
            for line_no, line in enumerate(path.read_text().splitlines(), start=1):
                if DIRECT_SPAN_CAST.search(line):
                    offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
