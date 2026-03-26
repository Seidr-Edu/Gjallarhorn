from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from unittest import mock

from gjallarhorn.cli import main
from gjallarhorn.indexer import ObservabilityIndexer
from gjallarhorn.models import IndexedJob, IndexedRunBundle, RunIndexState
from gjallarhorn.simpleyaml import dumps


@dataclass
class _StoredRun:
    bundle: IndexedRunBundle
    observed_mtime: datetime
    upsert_count: int = 0


class _FakeRepository:
    def __init__(self) -> None:
        self.jobs: dict[str, IndexedJob] = {}
        self.runs: dict[str, _StoredRun] = {}
        self.global_state: dict[str, dict[str, object]] = {}
        self.ensure_schema_calls = 0
        self.prune_cutoffs: list[datetime] = []

    def ensure_schema(self) -> None:
        self.ensure_schema_calls += 1

    def get_run_state(self, run_id: str) -> RunIndexState | None:
        stored = self.runs.get(run_id)
        if stored is None:
            return None
        return RunIndexState(
            last_observed_mtime=stored.observed_mtime,
            last_successful_indexed_at=stored.observed_mtime,
        )

    def upsert_job(self, job: IndexedJob) -> None:
        self.jobs[job.job_id] = job

    def upsert_run_bundle(
        self, bundle: IndexedRunBundle, *, observed_mtime: datetime
    ) -> None:
        existing = self.runs.get(bundle.run.run_id)
        upsert_count = 1 if existing is None else existing.upsert_count + 1
        self.runs[bundle.run.run_id] = _StoredRun(
            bundle=bundle,
            observed_mtime=observed_mtime,
            upsert_count=upsert_count,
        )

    def set_global_state(self, key: str, *, payload: dict[str, object]) -> None:
        self.global_state[key] = payload

    def get_global_state(self, key: str) -> dict[str, object] | None:
        return self.global_state.get(key)

    def prune(self, *, cutoff: datetime) -> None:
        self.prune_cutoffs.append(cutoff)


class ObservabilityIndexerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp(prefix="gjallarhorn-observability-")
        self.root = Path(self.tempdir)
        self.queue_root = self.root / "queue"
        self.runs_root = self.root / "runs"
        self.queue_root.mkdir(parents=True, exist_ok=True)
        self.runs_root.mkdir(parents=True, exist_ok=True)
        self.repository = _FakeRepository()
        self.indexer = ObservabilityIndexer(
            queue_root=self.queue_root,
            runs_root=self.runs_root,
            repository=self.repository,
            poll_interval_sec=15,
            retention_days=30,
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def test_indexes_complete_run_and_job(self) -> None:
        run_root = self._create_complete_run("20260326T120000Z__demo")
        self._create_job("job-1", run_root)

        self.indexer.run_once()

        self.assertIn("job-1", self.repository.jobs)
        self.assertIn("20260326T120000Z__demo", self.repository.runs)
        stored = self.repository.runs["20260326T120000Z__demo"].bundle
        self.assertEqual(stored.run.status, "passed")
        self.assertEqual(len(stored.steps), 6)
        self.assertEqual(
            [report.service_name for report in stored.service_reports],
            [
                "brokk",
                "eitri",
                "lidskjalv-original",
                "andvari",
                "kvasir",
                "lidskjalv-generated",
            ],
        )
        kvasir_report = next(
            report for report in stored.service_reports if report.service_name == "kvasir"
        )
        self.assertEqual(kvasir_report.kvasir_behavioral_verdict, "pass")
        self.assertEqual(kvasir_report.kvasir_write_scope_violation_count, 0)
        self.assertEqual(kvasir_report.kvasir_retention_metrics["retention_ratio"], 1.0)
        self.assertEqual(len(stored.artifacts), 3)
        self.assertEqual(len(stored.sonar_follow_up), 2)

    def test_skips_run_when_observed_mtime_has_not_changed(self) -> None:
        self._create_complete_run("20260326T120000Z__demo")

        self.indexer.run_once()
        self.indexer.run_once()

        stored = self.repository.runs["20260326T120000Z__demo"]
        self.assertEqual(stored.upsert_count, 1)

    def test_reindexes_when_only_sonar_follow_up_changes(self) -> None:
        run_root = self._create_complete_run("20260326T120000Z__demo")

        self.indexer.run_once()

        sonar_path = run_root / "pipeline" / "outputs" / "sonar_follow_up.json"
        payload = json.loads(sonar_path.read_text(encoding="utf-8"))
        payload["steps"]["lidskjalv-generated"]["quality_gate_status"] = "ERROR"
        write_file(sonar_path, json.dumps(payload, indent=2) + "\n")
        self._bump_mtime(sonar_path)

        self.indexer.run_once()

        stored = self.repository.runs["20260326T120000Z__demo"]
        self.assertEqual(stored.upsert_count, 2)
        generated = next(
            item for item in stored.bundle.sonar_follow_up if item.step_name == "lidskjalv-generated"
        )
        self.assertEqual(generated.quality_gate_status, "ERROR")

    def test_skips_malformed_service_report_without_crashing(self) -> None:
        run_root = self._create_complete_run("20260326T120000Z__demo")
        bad_report = (
            run_root
            / "services"
            / "kvasir"
            / "run"
            / "outputs"
            / "test_port.json"
        )
        write_file(bad_report, "{invalid json\n")
        self._bump_mtime(bad_report)

        self.indexer.run_once()

        stored = self.repository.runs["20260326T120000Z__demo"].bundle
        service_names = [report.service_name for report in stored.service_reports]
        self.assertNotIn("kvasir", service_names)
        self.assertEqual(stored.run.status, "passed")

    def test_indexes_partial_run_from_state_file(self) -> None:
        run_root = self.runs_root / "20260326T130000Z__partial"
        write_file(
            run_root / "pipeline" / "state.json",
            json.dumps(
                {
                    "schema_version": "heimdall_state.v1",
                    "steps": {
                        "brokk": {"status": "passed", "started_at": "2026-03-26T13:00:00Z"},
                        "eitri": {
                            "status": "running",
                            "started_at": "2026-03-26T13:01:00Z",
                            "report_path": str(
                                run_root / "services" / "eitri" / "run" / "outputs" / "run_report.json"
                            ),
                        },
                    },
                    "artifacts": {},
                },
                indent=2,
            )
            + "\n",
        )

        self.indexer.run_once()

        stored = self.repository.runs["20260326T130000Z__partial"].bundle
        self.assertEqual(stored.run.status, "running")
        self.assertEqual([step.step_name for step in stored.steps], ["brokk", "eitri"])
        self.assertEqual(stored.steps[1].status, "running")

    def test_failed_run_preserves_blocked_downstream_steps(self) -> None:
        run_root = self._create_complete_run("20260326T140000Z__failed")
        pipeline_report_path = run_root / "pipeline" / "outputs" / "run_report.json"
        report = json.loads(pipeline_report_path.read_text(encoding="utf-8"))
        report["status"] = "error"
        report["reason"] = "missing-canonical-report"
        report["steps"]["eitri"]["status"] = "error"
        report["steps"]["eitri"]["reason"] = "missing-canonical-report"
        report["steps"]["andvari"]["status"] = "blocked"
        report["steps"]["andvari"]["reason"] = "blocked-by-upstream"
        report["steps"]["kvasir"]["status"] = "blocked"
        report["steps"]["kvasir"]["reason"] = "blocked-by-upstream"
        write_file(pipeline_report_path, json.dumps(report, indent=2) + "\n")
        self._bump_mtime(pipeline_report_path)

        self.indexer.run_once()

        stored = self.repository.runs["20260326T140000Z__failed"].bundle
        self.assertEqual(stored.run.status, "error")
        blocked_steps = {
            step.step_name: step.status for step in stored.steps if step.status == "blocked"
        }
        self.assertEqual(
            blocked_steps,
            {
                "andvari": "blocked",
                "kvasir": "blocked",
            },
        )

    def test_cli_observability_indexer_uses_env_dsn(self) -> None:
        with (
            mock.patch("gjallarhorn.cli.PostgresIndexerRepository") as repo_cls,
            mock.patch("gjallarhorn.cli.ObservabilityIndexer") as indexer_cls,
            mock.patch.dict(
                os.environ,
                {"GJALLARHORN_DB_DSN": "postgresql://demo:demo@postgres:5432/demo"},
                clear=False,
            ),
        ):
            indexer_cls.return_value.run_loop.return_value = 0

            exit_code = main(
                [
                    "indexer",
                    "--queue-root",
                    str(self.queue_root),
                    "--runs-root",
                    str(self.runs_root),
                    "--once",
                ]
            )

        self.assertEqual(exit_code, 0)
        repo_cls.assert_called_once_with(
            "postgresql://demo:demo@postgres:5432/demo", schema="pipeline_obs"
        )
        indexer_cls.assert_called_once()
        indexer_cls.return_value.run_loop.assert_called_once_with(once=True)

    def _create_job(self, job_id: str, run_root: Path) -> None:
        write_file(
            self.queue_root / "jobs" / job_id / "job.yaml",
            dumps(
                {
                    "schema_version": "heimdall_queue_job.v1",
                    "job_id": job_id,
                    "status": "passed",
                    "repo_url": "https://github.com/example/demo-repo.git",
                    "commit_sha": "0123456789abcdef0123456789abcdef01234567",
                    "run_id": run_root.name,
                    "run_dir": str(run_root),
                    "submitted_at": "2026-03-26T12:00:00Z",
                    "started_at": "2026-03-26T12:00:01Z",
                    "finished_at": "2026-03-26T12:01:00Z",
                    "pipeline_status": "passed",
                    "reason": None,
                }
            ),
        )

    def _create_complete_run(self, run_id: str) -> Path:
        run_root = self.runs_root / run_id
        steps = {
            "brokk": {"status": "passed", "report_status": "passed"},
            "eitri": {"status": "passed", "report_status": "passed"},
            "lidskjalv-original": {"status": "passed", "report_status": "passed"},
            "andvari": {"status": "passed", "report_status": "passed"},
            "kvasir": {"status": "passed", "report_status": "passed"},
            "lidskjalv-generated": {"status": "passed", "report_status": "passed"},
        }
        write_file(
            run_root / "pipeline" / "outputs" / "run_report.json",
            json.dumps(
                {
                    "schema_version": "heimdall_run_report.v1",
                    "run_id": run_id,
                    "status": "passed",
                    "reason": None,
                    "started_at": "2026-03-26T12:00:00Z",
                    "finished_at": "2026-03-26T12:01:00Z",
                    "steps": steps,
                    "artifacts": {
                        "source_manifest": {
                            "owner": "brokk",
                            "path": str(
                                run_root
                                / "services"
                                / "brokk"
                                / "run"
                                / "inputs"
                                / "source-manifest.json"
                            ),
                        },
                        "model_diagram": {
                            "owner": "eitri",
                            "path": str(
                                run_root
                                / "services"
                                / "eitri"
                                / "run"
                                / "artifacts"
                                / "model"
                                / "diagram.puml"
                            ),
                        },
                        "kvasir_report": {
                            "owner": "kvasir",
                            "path": str(
                                run_root
                                / "services"
                                / "kvasir"
                                / "run"
                                / "outputs"
                                / "test_port.json"
                            ),
                        },
                    },
                },
                indent=2,
            )
            + "\n",
        )
        write_file(
            run_root / "pipeline" / "artifact_index.json",
            json.dumps(
                {
                    "schema_version": "heimdall_artifact_index.v1",
                    "run_id": run_id,
                    "artifacts": {
                        "source_manifest": {
                            "owner": "brokk",
                            "path": str(
                                run_root
                                / "services"
                                / "brokk"
                                / "run"
                                / "inputs"
                                / "source-manifest.json"
                            ),
                        },
                        "model_diagram": {
                            "owner": "eitri",
                            "path": str(
                                run_root
                                / "services"
                                / "eitri"
                                / "run"
                                / "artifacts"
                                / "model"
                                / "diagram.puml"
                            ),
                        },
                        "generated_repo": {
                            "owner": "andvari",
                            "path": str(
                                run_root
                                / "services"
                                / "andvari"
                                / "run"
                                / "artifacts"
                                / "generated-repo"
                            ),
                        },
                    },
                },
                indent=2,
            )
            + "\n",
        )
        write_file(
            run_root / "pipeline" / "outputs" / "sonar_follow_up.json",
            json.dumps(
                {
                    "schema_version": "heimdall_sonar_follow_up.v1",
                    "run_id": run_id,
                    "status": "complete",
                    "steps": {
                        "lidskjalv-original": {
                            "status": "complete",
                            "project_key": "demo__original",
                            "sonar_task_id": "orig-task",
                            "quality_gate_status": "OK",
                            "measures": {"coverage": "82.1"},
                            "last_checked_at": "2026-03-26T12:01:10Z",
                        },
                        "lidskjalv-generated": {
                            "status": "complete",
                            "project_key": "demo__generated",
                            "sonar_task_id": "gen-task",
                            "quality_gate_status": "OK",
                            "measures": {"coverage": "79.3"},
                            "last_checked_at": "2026-03-26T12:01:11Z",
                        },
                    },
                },
                indent=2,
            )
            + "\n",
        )
        write_file(
            run_root / "pipeline" / "state.json",
            json.dumps(
                {
                    "schema_version": "heimdall_state.v1",
                    "steps": {
                        name: {
                            "status": info["status"],
                            "reason": None,
                            "blocked_by": [],
                            "started_at": "2026-03-26T12:00:00Z",
                            "finished_at": "2026-03-26T12:01:00Z",
                            "configured_image_ref": f"fake/{name}:1",
                            "resolved_image_id": f"sha256:{name}",
                            "fingerprint": f"fp-{name}",
                            "report_path": str(self._service_report_path(run_root, name)),
                            "report_status": info["report_status"],
                        }
                        for name, info in steps.items()
                    },
                    "artifacts": {},
                },
                indent=2,
            )
            + "\n",
        )
        self._write_service_reports(run_root)
        return run_root

    def _service_report_path(self, run_root: Path, step_name: str) -> Path:
        if step_name == "kvasir":
            return run_root / "services" / "kvasir" / "run" / "outputs" / "test_port.json"
        return run_root / "services" / step_name / "run" / "outputs" / "run_report.json"

    def _write_service_reports(self, run_root: Path) -> None:
        write_file(
            self._service_report_path(run_root, "brokk"),
            json.dumps(
                {
                    "service_schema_version": "brokk_service_report.v1",
                    "status": "passed",
                    "repo_url": "https://github.com/example/demo-repo.git",
                    "requested_commit": "0123456789abcdef0123456789abcdef01234567",
                    "resolved_commit": "0123456789abcdef0123456789abcdef01234567",
                    "submodules_materialized": True,
                    "lfs_materialized": False,
                },
                indent=2,
            )
            + "\n",
        )
        write_file(
            self._service_report_path(run_root, "eitri"),
            json.dumps(
                {
                    "service_schema_version": "eitri_service_report.v1",
                    "status": "passed",
                    "type_count": 8,
                    "relation_count": 5,
                    "artifacts": {
                        "diagram_path": str(
                            run_root
                            / "services"
                            / "eitri"
                            / "run"
                            / "artifacts"
                            / "model"
                            / "diagram.puml"
                        ),
                        "logs_dir": str(
                            run_root
                            / "services"
                            / "eitri"
                            / "run"
                            / "artifacts"
                            / "model"
                            / "logs"
                        ),
                    },
                },
                indent=2,
            )
            + "\n",
        )
        write_file(
            self._service_report_path(run_root, "andvari"),
            json.dumps(
                {
                    "service_schema_version": "andvari_service_report.v1",
                    "status": "passed",
                    "adapter": "codex",
                    "gating_mode": "model",
                    "latest_gate_version": "gate-7",
                    "gates": {"total": 4, "passed": 4, "failed": 0},
                    "outcomes": {"total": 4, "core": 3, "non_core": 1},
                    "artifacts": {
                        "generated_repo": str(
                            run_root
                            / "services"
                            / "andvari"
                            / "run"
                            / "artifacts"
                            / "generated-repo"
                        ),
                        "logs_dir": str(
                            run_root / "services" / "andvari" / "run" / "logs"
                        ),
                    },
                },
                indent=2,
            )
            + "\n",
        )
        write_file(
            self._service_report_path(run_root, "kvasir"),
            json.dumps(
                {
                    "service_schema_version": "kvasir_test_port.v1",
                    "status": "passed",
                    "behavioral_verdict": "pass",
                    "behavioral_verdict_reason": None,
                    "baseline_original_tests": {
                        "execution_summary": {"passed": 12, "failed": 0}
                    },
                    "baseline_generated_tests": {
                        "execution_summary": {"passed": 12, "failed": 0}
                    },
                    "ported_original_tests": {
                        "execution_summary": {"passed": 11, "failed": 0, "skipped": 1}
                    },
                    "suite_shape": {"retention_ratio": 1.0, "ported_test_count": 11},
                    "write_scope": {"violation_count": 0},
                    "adapter": {
                        "session_log": str(
                            run_root / "services" / "kvasir" / "run" / "logs" / "adapter.jsonl"
                        )
                    },
                },
                indent=2,
            )
            + "\n",
        )
        for step_name, scan_label in (
            ("lidskjalv-original", "original"),
            ("lidskjalv-generated", "generated"),
        ):
            write_file(
                self._service_report_path(run_root, step_name),
                json.dumps(
                    {
                        "service_schema_version": "lidskjalv_service_report.v1",
                        "status": "passed",
                        "scan_label": scan_label,
                        "project_key": f"demo__{scan_label}",
                        "project_name": f"demo-{scan_label}",
                        "scan": {
                            "build_tool": "maven",
                            "build_jdk": "21",
                            "build_subdir": "app",
                            "java_version_hint": "21",
                            "sonar_task_id": f"{scan_label}-task",
                            "ce_task_status": "SUCCESS",
                            "quality_gate_status": "OK",
                            "measures": {"coverage": "80.0", "bugs": "0"},
                        },
                    },
                    indent=2,
                )
                + "\n",
            )

    def _bump_mtime(self, path: Path) -> None:
        stat = path.stat()
        updated = stat.st_mtime + 1.0
        os.utime(path, (updated, updated))


def write_file(path: Path, content: str, mode: int = 0o644) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    path.chmod(mode)
    return path


if __name__ == "__main__":
    unittest.main()
