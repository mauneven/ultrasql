import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
ID_RS = REPO / "crates" / "ultrasql-core" / "src" / "id.rs"
INTEGER_AS_CAST = re.compile(r"\bas\s+(?:usize|u8|u16|u32|u64|i8|i16|i32|i64|isize)\b")


class CoreStyleTests(unittest.TestCase):
    def test_ids_use_checked_or_lossless_integer_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(ID_RS.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if INTEGER_AS_CAST.search(code):
                offenders.append(f"{ID_RS.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
