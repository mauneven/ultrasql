import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
IMPORTER = ROOT / "third_party" / "sqllogictest" / "import.py"


class ImporterTests(unittest.TestCase):
    def test_importer_preserves_license_notice_and_manifest(self):
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as dest_dir:
            source = Path(source_dir)
            dest = Path(dest_dir) / "imported"
            (source / "LICENSE").write_text("MIT license\n", encoding="utf-8")
            (source / "NOTICE").write_text("notice text\n", encoding="utf-8")
            (source / "slt").mkdir()
            (source / "slt" / "one.test").write_text(
                "query I nosort\nSELECT 1\n----\n1\n", encoding="utf-8"
            )

            result = subprocess.run(
                [
                    "python3",
                    str(IMPORTER),
                    "--source",
                    str(source),
                    "--commit",
                    "abc123",
                    "--dest",
                    str(dest),
                    "--notice-dest",
                    str(dest / "NOTICE.upstream"),
                    "--license-dest",
                    str(dest / "LICENSE.upstream"),
                    "--upstream-commit-file",
                    str(dest / "upstream_commit.txt"),
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((dest / "slt" / "one.test").is_file())
            self.assertEqual(
                (dest / "LICENSE.upstream").read_text(encoding="utf-8"), "MIT license\n"
            )
            self.assertEqual(
                (dest / "NOTICE.upstream").read_text(encoding="utf-8"), "notice text\n"
            )
            self.assertEqual(
                (dest / "upstream_commit.txt").read_text(encoding="utf-8"), "abc123\n"
            )
            manifest = (dest / "IMPORT_MANIFEST.txt").read_text(encoding="utf-8")
            self.assertIn("commit=abc123", manifest)
            self.assertIn("notice=NOTICE", manifest)

    def test_include_overrides_default_patterns(self):
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as dest_dir:
            source = Path(source_dir)
            dest = Path(dest_dir) / "imported"
            (source / "LICENSE").write_text("MIT license\n", encoding="utf-8")
            (source / "keep").mkdir()
            (source / "skip").mkdir()
            (source / "keep" / "one.test").write_text(
                "query I nosort\nSELECT 1\n----\n1\n", encoding="utf-8"
            )
            (source / "skip" / "two.test").write_text(
                "query I nosort\nSELECT 2\n----\n2\n", encoding="utf-8"
            )

            result = subprocess.run(
                [
                    "python3",
                    str(IMPORTER),
                    "--source",
                    str(source),
                    "--commit",
                    "abc123",
                    "--dest",
                    str(dest),
                    "--include",
                    "keep/*.test",
                    "--license-dest",
                    str(dest / "LICENSE.upstream"),
                    "--upstream-commit-file",
                    str(dest / "upstream_commit.txt"),
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((dest / "keep" / "one.test").is_file())
            self.assertFalse((dest / "skip" / "two.test").exists())
