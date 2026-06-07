import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
PARSER_SRC = REPO / "crates" / "ultrasql-parser" / "src"
STATEMENTS = REPO / "crates" / "ultrasql-parser" / "src" / "statements"
DIRECT_SPAN_CAST = re.compile(
    r"(?:\.span(?:\(\))?|\b(?:\w+_)?span)\.(?:start|end)\s+as\s+usize"
)
INTEGER_AS_CAST = re.compile(r"\bas\s+(?:usize|u8|u16|u32|u64|i8|i16|i32|i64|isize)\b")


class ParserStyleTests(unittest.TestCase):
    def test_statement_diagnostics_use_span_offset_accessors(self) -> None:
        offenders: list[str] = []
        for path in sorted(STATEMENTS.glob("*.rs")):
            for line_no, line in enumerate(path.read_text().splitlines(), start=1):
                if DIRECT_SPAN_CAST.search(line):
                    offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_parser_source_uses_checked_integer_conversions(self) -> None:
        offenders: list[str] = []
        for path in sorted(PARSER_SRC.rglob("*.rs")):
            for line_no, line in enumerate(path.read_text().splitlines(), start=1):
                code = line.split("//", maxsplit=1)[0]
                if INTEGER_AS_CAST.search(code):
                    offenders.append(f"{path.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
