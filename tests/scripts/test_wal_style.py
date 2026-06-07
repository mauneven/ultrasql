import re
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
WAL_RECORD_BENCH = REPO / "crates" / "ultrasql-wal" / "benches" / "record.rs"
WAL_RECOVERY_SIM_TEST = REPO / "crates" / "ultrasql-wal" / "tests" / "recovery_sim.rs"
WAL_WRITER_RECOVERY_TEST = (
    REPO / "crates" / "ultrasql-wal" / "tests" / "writer_recovery.rs"
)
WAL_BENCH_THROUGHPUT_CAST = re.compile(r"\bn\s+as\s+u64\b")
WAL_RECOVERY_SLOT_CAST = re.compile(r"seq\s*%\s*1024\)\s+as\s+u16\b")
WAL_WRITER_PAYLOAD_CAST = re.compile(r"\bi\s*&\s*0xFF\)\s+as\s+u8\b")


class WalStyleTests(unittest.TestCase):
    def test_wal_record_bench_uses_checked_throughput_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(WAL_RECORD_BENCH.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if WAL_BENCH_THROUGHPUT_CAST.search(code):
                offenders.append(f"{WAL_RECORD_BENCH.relative_to(REPO)}:{line_no}: {line.strip()}")

        self.assertEqual([], offenders)

    def test_wal_recovery_sim_uses_checked_slot_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(WAL_RECOVERY_SIM_TEST.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if WAL_RECOVERY_SLOT_CAST.search(code):
                offenders.append(
                    f"{WAL_RECOVERY_SIM_TEST.relative_to(REPO)}:{line_no}: {line.strip()}"
                )

        self.assertEqual([], offenders)

    def test_wal_writer_recovery_uses_checked_payload_byte_conversions(self) -> None:
        offenders: list[str] = []
        for line_no, line in enumerate(WAL_WRITER_RECOVERY_TEST.read_text().splitlines(), start=1):
            code = line.split("//", maxsplit=1)[0]
            if WAL_WRITER_PAYLOAD_CAST.search(code):
                offenders.append(
                    f"{WAL_WRITER_RECOVERY_TEST.relative_to(REPO)}:{line_no}: {line.strip()}"
                )

        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
