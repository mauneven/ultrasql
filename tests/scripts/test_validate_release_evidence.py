import json
import subprocess
import sys
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
AUDIT_SCRIPT = REPO / "scripts" / "validate-external-audits.py"
DRILL_SCRIPT = REPO / "scripts" / "validate-incident-drills.py"
COMMIT = "0123456789abcdef0123456789abcdef01234567"


def write_audit_report(
    path: Path,
    *,
    auditor_id: str,
    audit_type: str,
    critical_findings_open: int = 0,
    high_findings_open: int = 0,
    commit: str = COMMIT,
) -> None:
    path.write_text(
        json.dumps(
            {
                "auditor_id": auditor_id,
                "auditor_org": f"{auditor_id} security lab",
                "auditor_contact": f"{auditor_id}@example.invalid",
                "commit": commit,
                "audit_type": audit_type,
                "report_date_utc": "2026-02-01T00:00:00Z",
                "scope": "storage, WAL, SQL execution, release packaging",
                "methodology": "manual review, fuzz replay, threat modeling",
                "report_uri": f"https://example.invalid/{auditor_id}-{audit_type}.pdf",
                "critical_findings_open": critical_findings_open,
                "high_findings_open": high_findings_open,
                "medium_findings_open": 0,
                "low_findings_open": 0,
                "signed_off_by": "release reviewer",
                "signature": f"{auditor_id}-detached-signature",
            }
        )
        + "\n"
    )


def write_drill_report(
    path: Path,
    *,
    drill_id: str,
    drill_type: str,
    rto_actual_seconds: int = 20,
    rpo_actual_seconds: int = 0,
    data_loss_confirmed: bool = False,
    unresolved_sev0_count: int = 0,
    unresolved_sev1_count: int = 0,
    commit: str = COMMIT,
) -> None:
    path.write_text(
        json.dumps(
            {
                "drill_id": drill_id,
                "commit": commit,
                "drill_type": drill_type,
                "run_time_utc": "2026-02-01T00:00:00Z",
                "environment": "release-candidate staging",
                "scenario": f"{drill_type} production drill",
                "operator": "ops-a",
                "rto_target_seconds": 60,
                "rto_actual_seconds": rto_actual_seconds,
                "rpo_target_seconds": 0,
                "rpo_actual_seconds": rpo_actual_seconds,
                "data_loss_confirmed": data_loss_confirmed,
                "correctness_verified": True,
                "monitoring_alerted": True,
                "postmortem_uri": f"https://example.invalid/{drill_id}.md",
                "unresolved_sev0_count": unresolved_sev0_count,
                "unresolved_sev1_count": unresolved_sev1_count,
                "signed_off_by": "incident commander",
            }
        )
        + "\n"
    )


def run_script(script: Path, reports_dir: Path, *extra: str) -> dict:
    out = reports_dir / "status.json"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--reports-dir",
            str(reports_dir),
            "--now",
            "2026-02-02T00:00:00Z",
            "--commit",
            COMMIT,
            "--out",
            str(out),
            *extra,
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(out.read_text())


class ReleaseEvidenceValidatorTests(unittest.TestCase):
    def test_external_audits_require_independent_security_and_correctness_reports(self) -> None:
        with tempfile_dir() as tmp_path:
            write_audit_report(
                tmp_path / "security.json",
                auditor_id="auditor-a",
                audit_type="security",
            )
            write_audit_report(
                tmp_path / "correctness.json",
                auditor_id="auditor-b",
                audit_type="correctness",
            )

            status = run_script(
                AUDIT_SCRIPT,
                tmp_path,
                "--min-reports",
                "2",
                "--required-audit-types",
                "security,correctness",
            )

            self.assertTrue(status["ready"])
            self.assertEqual(status["status"], "ready")
            self.assertEqual(status["independent_auditor_count"], 2)
            self.assertEqual(status["covered_audit_types"], ["correctness", "security"])

    def test_external_audits_reject_open_critical_or_high_findings(self) -> None:
        with tempfile_dir() as tmp_path:
            write_audit_report(
                tmp_path / "security.json",
                auditor_id="auditor-a",
                audit_type="security",
                high_findings_open=1,
            )
            write_audit_report(
                tmp_path / "correctness.json",
                auditor_id="auditor-b",
                audit_type="correctness",
            )

            status = run_script(
                AUDIT_SCRIPT,
                tmp_path,
                "--min-reports",
                "2",
                "--required-audit-types",
                "security,correctness",
            )

            self.assertFalse(status["ready"])
            bad_report = next(
                report for report in status["reports"] if report["auditor_id"] == "auditor-a"
            )
            self.assertTrue(
                any("high_findings_open must be zero" in error for error in bad_report["errors"])
            )

    def test_incident_drills_require_backup_wal_and_disk_full_coverage(self) -> None:
        with tempfile_dir() as tmp_path:
            write_drill_report(
                tmp_path / "backup.json",
                drill_id="drill-backup",
                drill_type="backup_restore",
            )
            write_drill_report(
                tmp_path / "wal.json",
                drill_id="drill-wal",
                drill_type="wal_recovery",
            )
            write_drill_report(
                tmp_path / "disk.json",
                drill_id="drill-disk",
                drill_type="disk_full",
            )

            status = run_script(
                DRILL_SCRIPT,
                tmp_path,
                "--required-drill-types",
                "backup_restore,wal_recovery,disk_full",
            )

            self.assertTrue(status["ready"])
            self.assertEqual(status["status"], "ready")
            self.assertEqual(
                status["covered_drill_types"],
                ["backup_restore", "disk_full", "wal_recovery"],
            )

    def test_incident_drills_reject_rto_rpo_misses_and_data_loss(self) -> None:
        with tempfile_dir() as tmp_path:
            write_drill_report(
                tmp_path / "backup.json",
                drill_id="drill-backup",
                drill_type="backup_restore",
                rto_actual_seconds=120,
            )
            write_drill_report(
                tmp_path / "wal.json",
                drill_id="drill-wal",
                drill_type="wal_recovery",
                rpo_actual_seconds=5,
            )
            write_drill_report(
                tmp_path / "disk.json",
                drill_id="drill-disk",
                drill_type="disk_full",
                data_loss_confirmed=True,
                unresolved_sev1_count=1,
            )

            status = run_script(
                DRILL_SCRIPT,
                tmp_path,
                "--required-drill-types",
                "backup_restore,wal_recovery,disk_full",
            )

            self.assertFalse(status["ready"])
            errors = "\n".join(
                error for report in status["reports"] for error in report["errors"]
            )
            self.assertIn("rto_actual_seconds exceeds rto_target_seconds", errors)
            self.assertIn("rpo_actual_seconds exceeds rpo_target_seconds", errors)
            self.assertIn("data_loss_confirmed must be false", errors)
            self.assertIn("unresolved_sev1_count must be zero", errors)


class tempfile_dir:
    def __enter__(self) -> Path:
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        return Path(self._tmp.name)

    def __exit__(self, exc_type, exc, tb) -> None:
        self._tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
