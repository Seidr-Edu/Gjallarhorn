from __future__ import annotations

import json
import socket
import sys
import time
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol

from .contracts import ALL_STEPS, STEP_DEFINITIONS
from .models import (
    IndexedArtifact,
    IndexedJob,
    IndexedRun,
    IndexedRunBundle,
    IndexedRunStep,
    IndexedServiceReport,
    IndexedSonarFollowUp,
    RunIndexState,
)
from .simpleyaml import YamlError, loads
from .state import load_existing_state
from .utils import read_json, timestamp_utc

_RUN_STATE_PREFIX = "run:"
_GLOBAL_SCAN_KEY = "global:scan"
_GLOBAL_PRUNE_KEY = "global:prune"


class IndexerRepository(Protocol):
    def ensure_schema(self) -> None: ...

    def get_run_state(self, run_id: str) -> RunIndexState | None: ...

    def upsert_job(self, job: IndexedJob) -> None: ...

    def upsert_run_bundle(
        self, bundle: IndexedRunBundle, *, observed_mtime: datetime
    ) -> None: ...

    def set_global_state(self, key: str, *, payload: dict[str, object]) -> None: ...

    def get_global_state(self, key: str) -> dict[str, object] | None: ...

    def prune(self, *, cutoff: datetime) -> None: ...


def _emit(event: str, **fields: object) -> None:
    payload = {
        "ts": timestamp_utc(),
        "host": socket.gethostname(),
        "event": event,
        **fields,
    }
    print(json.dumps(payload, sort_keys=True), file=sys.stderr, flush=True)


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped:
        return None
    normalized = stripped.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)


def _integer(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped and stripped.lstrip("-").isdigit():
            return int(stripped)
    return None


def _bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


class ObservabilityIndexer:
    def __init__(
        self,
        *,
        queue_root: Path,
        runs_root: Path,
        repository: IndexerRepository,
        poll_interval_sec: int = 15,
        retention_days: int = 30,
    ) -> None:
        self.queue_root = queue_root.resolve()
        self.runs_root = runs_root.resolve()
        self.repository = repository
        self.poll_interval_sec = poll_interval_sec
        self.retention_days = retention_days

    def run_loop(self, *, once: bool) -> int:
        self.repository.ensure_schema()
        _emit(
            "observability_indexer_start",
            queue_root=str(self.queue_root),
            runs_root=str(self.runs_root),
            poll_interval_sec=self.poll_interval_sec,
            retention_days=self.retention_days,
        )
        while True:
            self.run_once()
            if once:
                return 0
            time.sleep(self.poll_interval_sec)

    def run_once(self) -> None:
        self._index_jobs()
        self._index_runs()
        self.repository.set_global_state(
            _GLOBAL_SCAN_KEY,
            payload={"last_scanned_at": timestamp_utc()},
        )
        self._prune_if_due()

    def _index_jobs(self) -> None:
        jobs_root = self.queue_root / "jobs"
        if not jobs_root.is_dir():
            _emit("observability_indexer_warning", detail="missing_jobs_root")
            return
        for job_dir in sorted(path for path in jobs_root.iterdir() if path.is_dir()):
            job_path = job_dir / "job.yaml"
            if not job_path.is_file():
                continue
            document = _load_yaml_mapping(job_path, label="job")
            if document is None:
                _emit(
                    "observability_indexer_warning",
                    detail="invalid_job_yaml",
                    path=str(job_path),
                )
                continue
            pipeline_report_path = _run_report_path_from_job(document)
            pipeline_report = _load_json_document(pipeline_report_path)
            indexed = IndexedJob(
                job_id=_string(document.get("job_id")) or job_dir.name,
                queue_status=_string(document.get("status")) or "unknown",
                repo_url=_string(document.get("repo_url")),
                commit_sha=_string(document.get("commit_sha")),
                run_id=_string(document.get("run_id")),
                run_dir=_string(document.get("run_dir")),
                submitted_at=_parse_timestamp(document.get("submitted_at")),
                started_at=_parse_timestamp(document.get("started_at")),
                finished_at=_parse_timestamp(document.get("finished_at")),
                pipeline_status=_string(pipeline_report.get("status")),
                pipeline_reason=_string(pipeline_report.get("reason")),
                report_path=str(pipeline_report_path) if pipeline_report_path else None,
                raw_payload=document,
            )
            self.repository.upsert_job(indexed)

    def _index_runs(self) -> None:
        if not self.runs_root.is_dir():
            _emit("observability_indexer_warning", detail="missing_runs_root")
            return
        for run_root in sorted(path for path in self.runs_root.iterdir() if path.is_dir()):
            run_id = run_root.name
            observed_mtime = _latest_observed_mtime(run_root)
            if observed_mtime is None:
                continue
            state = self.repository.get_run_state(run_id)
            if (
                state is not None
                and state.last_observed_mtime is not None
                and observed_mtime <= state.last_observed_mtime
            ):
                continue
            bundle = _build_run_bundle(run_root)
            self.repository.upsert_run_bundle(bundle, observed_mtime=observed_mtime)
            _emit(
                "observability_indexed_run",
                run_id=run_id,
                observed_mtime=observed_mtime.isoformat(),
                status=bundle.run.status,
            )

    def _prune_if_due(self) -> None:
        state = self.repository.get_global_state(_GLOBAL_PRUNE_KEY) or {}
        last_pruned_at = _parse_timestamp(state.get("last_pruned_at"))
        now = datetime.now(timezone.utc)
        if last_pruned_at is not None and now - last_pruned_at < timedelta(hours=24):
            return
        cutoff = now - timedelta(days=self.retention_days)
        self.repository.prune(cutoff=cutoff)
        self.repository.set_global_state(
            _GLOBAL_PRUNE_KEY,
            payload={
                "last_pruned_at": now.isoformat(),
                "retention_days": self.retention_days,
            },
        )
        _emit("observability_pruned", cutoff=cutoff.isoformat())


def _latest_observed_mtime(run_root: Path) -> datetime | None:
    candidates = [
        run_root / "pipeline" / "outputs" / "run_report.json",
        run_root / "pipeline" / "outputs" / "sonar_follow_up.json",
        run_root / "pipeline" / "artifact_index.json",
        run_root / "pipeline" / "state.json",
        run_root / "pipeline" / "manifest.yaml",
        run_root / "pipeline" / "resolved.yaml",
    ]
    for definition in STEP_DEFINITIONS.values():
        candidates.append(
            run_root
            / "services"
            / definition.service_dir_name
            / "run"
            / definition.report_relative_path
        )
    mtimes = [
        datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        for path in candidates
        if path.exists()
    ]
    if not mtimes:
        return None
    return max(mtimes)


def _build_run_bundle(run_root: Path) -> IndexedRunBundle:
    run_id = run_root.name
    pipeline_report_path = run_root / "pipeline" / "outputs" / "run_report.json"
    pipeline_report = _load_json_document(pipeline_report_path)
    state = load_existing_state(run_root / "pipeline" / "state.json")
    artifact_index = _load_json_document(run_root / "pipeline" / "artifact_index.json")
    sonar_follow_up = _load_json_document(
        run_root / "pipeline" / "outputs" / "sonar_follow_up.json"
    )

    run = IndexedRun(
        run_id=run_id,
        status=_derive_run_status(pipeline_report, state),
        reason=_derive_run_reason(pipeline_report, state),
        started_at=_parse_timestamp(pipeline_report.get("started_at")),
        finished_at=_parse_timestamp(pipeline_report.get("finished_at")),
        manifest_path=str(run_root / "pipeline" / "manifest.yaml")
        if (run_root / "pipeline" / "manifest.yaml").exists()
        else None,
        resolved_path=str(run_root / "pipeline" / "resolved.yaml")
        if (run_root / "pipeline" / "resolved.yaml").exists()
        else None,
        raw_payload=pipeline_report,
    )
    return IndexedRunBundle(
        run=run,
        steps=_build_indexed_steps(run_id, pipeline_report, state),
        service_reports=_build_service_reports(run_root),
        artifacts=_build_artifacts(run_id, artifact_index),
        sonar_follow_up=_build_sonar_follow_up(run_id, sonar_follow_up),
    )


def _derive_run_status(
    pipeline_report: dict[str, object], state: dict[str, object]
) -> str:
    reported = _string(pipeline_report.get("status"))
    if reported is not None:
        return reported
    states = [step.status for step in state.values()]
    if any(status == "error" for status in states):
        return "error"
    if any(status in {"failed", "blocked"} for status in states):
        return "failed"
    if any(status == "running" for status in states):
        return "running"
    if states and all(status in {"passed", "skipped"} for status in states):
        return "passed"
    return "pending"


def _derive_run_reason(
    pipeline_report: dict[str, object], state: dict[str, object]
) -> str | None:
    reported = _string(pipeline_report.get("reason"))
    if reported is not None:
        return reported
    for step in ALL_STEPS:
        current = state.get(step)
        if current is None:
            continue
        if current.status in {"failed", "error", "blocked"}:
            return current.reason
    return None


def _build_indexed_steps(
    run_id: str,
    pipeline_report: dict[str, object],
    state: dict[str, object],
) -> list[IndexedRunStep]:
    result: list[IndexedRunStep] = []
    report_steps = _mapping(pipeline_report.get("steps"))
    for step in ALL_STEPS:
        report_state = _mapping(report_steps.get(step))
        current = state.get(step)
        if not report_state and current is None:
            continue
        status = _string(report_state.get("status")) or (
            current.status if current is not None else "pending"
        )
        result.append(
            IndexedRunStep(
                run_id=run_id,
                step_name=step,
                status=status,
                reason=_string(report_state.get("reason"))
                or (current.reason if current is not None else None),
                report_status=_string(report_state.get("report_status"))
                or (current.report_status if current is not None else None),
                started_at=_parse_timestamp(report_state.get("started_at"))
                or (current.started_at and _parse_timestamp(current.started_at)),
                finished_at=_parse_timestamp(report_state.get("finished_at"))
                or (current.finished_at and _parse_timestamp(current.finished_at)),
                configured_image_ref=_string(report_state.get("configured_image_ref"))
                or (current.configured_image_ref if current is not None else None),
                resolved_image_id=_string(report_state.get("resolved_image_id"))
                or (current.resolved_image_id if current is not None else None),
                fingerprint=_string(report_state.get("fingerprint"))
                or (current.fingerprint if current is not None else None),
                report_path=_string(report_state.get("report_path"))
                or (current.report_path if current is not None else None),
                raw_payload=report_state,
            )
        )
    return result


def _build_artifacts(
    run_id: str, artifact_index: dict[str, object]
) -> list[IndexedArtifact]:
    artifacts = _mapping(artifact_index.get("artifacts"))
    result: list[IndexedArtifact] = []
    for artifact_key, raw in sorted(artifacts.items()):
        if not isinstance(raw, Mapping):
            continue
        owner_step = _string(raw.get("owner"))
        path = _string(raw.get("path"))
        if owner_step is None or path is None:
            continue
        result.append(
            IndexedArtifact(
                run_id=run_id,
                artifact_key=artifact_key,
                owner_step=owner_step,
                path=path,
            )
        )
    return result


def _build_sonar_follow_up(
    run_id: str, document: dict[str, object]
) -> list[IndexedSonarFollowUp]:
    steps = _mapping(document.get("steps"))
    result: list[IndexedSonarFollowUp] = []
    for step_name, raw in sorted(steps.items()):
        entry = _mapping(raw)
        result.append(
            IndexedSonarFollowUp(
                run_id=run_id,
                step_name=step_name,
                status=_string(entry.get("status")),
                project_key=_string(entry.get("project_key")),
                sonar_task_id=_string(entry.get("sonar_task_id")),
                quality_gate_status=_string(entry.get("quality_gate_status")),
                measures=_mapping(entry.get("measures")),
                last_checked_at=_parse_timestamp(entry.get("last_checked_at")),
                raw_payload=entry,
            )
        )
    return result


def _build_service_reports(run_root: Path) -> list[IndexedServiceReport]:
    result: list[IndexedServiceReport] = []
    for step_name, definition in STEP_DEFINITIONS.items():
        report_path = (
            run_root
            / "services"
            / definition.service_dir_name
            / "run"
            / definition.report_relative_path
        )
        payload = _load_json_document(report_path)
        if not payload:
            continue
        try:
            result.append(_map_service_report(run_root.name, step_name, report_path, payload))
        except Exception as exc:
            _emit(
                "observability_indexer_warning",
                detail="service_report_mapping_failed",
                run_id=run_root.name,
                service=step_name,
                path=str(report_path),
                error=str(exc),
            )
    return result


def _map_service_report(
    run_id: str,
    service_name: str,
    report_path: Path,
    payload: dict[str, object],
) -> IndexedServiceReport:
    base = IndexedServiceReport(
        run_id=run_id,
        service_name=service_name,
        status=_string(payload.get("status")),
        reason=_string(payload.get("reason")),
        status_detail=_string(payload.get("status_detail")),
        started_at=_parse_timestamp(payload.get("started_at")),
        finished_at=_parse_timestamp(payload.get("finished_at")),
        report_path=str(report_path),
        schema_version=_string(payload.get("service_schema_version"))
        or _string(payload.get("schema_version"))
        or _string(payload.get("version")),
        raw_payload=payload,
    )
    if service_name == "brokk":
        return replace(
            base,
            brokk_repo_url=_string(payload.get("repo_url")),
            brokk_requested_commit=_string(payload.get("requested_commit")),
            brokk_resolved_commit=_string(payload.get("resolved_commit")),
            brokk_submodules_materialized=_bool(payload.get("submodules_materialized")),
            brokk_lfs_materialized=_bool(payload.get("lfs_materialized")),
        )
    if service_name == "eitri":
        artifacts = _mapping(payload.get("artifacts"))
        return replace(
            base,
            eitri_type_count=_integer(payload.get("type_count")),
            eitri_relation_count=_integer(payload.get("relation_count")),
            eitri_diagram_path=_string(artifacts.get("diagram_path")),
            eitri_logs_dir=_string(artifacts.get("logs_dir")),
        )
    if service_name == "andvari":
        gates = _mapping(payload.get("gates"))
        outcomes = _mapping(payload.get("outcomes"))
        artifacts = _mapping(payload.get("artifacts"))
        return replace(
            base,
            andvari_gating_mode=_string(payload.get("gating_mode")),
            andvari_adapter=_string(payload.get("adapter")),
            andvari_latest_gate_version=_string(payload.get("latest_gate_version")),
            andvari_gates_total=_integer(gates.get("total")),
            andvari_gates_passed=_integer(gates.get("passed")),
            andvari_gates_failed=_integer(gates.get("failed")),
            andvari_outcomes_total=_integer(outcomes.get("total")),
            andvari_outcomes_core=_integer(outcomes.get("core")),
            andvari_outcomes_non_core=_integer(outcomes.get("non_core")),
            andvari_generated_repo=_string(artifacts.get("generated_repo")),
            andvari_logs_dir=_string(artifacts.get("logs_dir")),
        )
    if service_name == "kvasir":
        return replace(
            base,
            kvasir_behavioral_verdict=_string(payload.get("behavioral_verdict")),
            kvasir_behavioral_verdict_reason=_string(
                payload.get("behavioral_verdict_reason")
            ),
            kvasir_baseline_original_summary=_mapping(
                _mapping(payload.get("baseline_original_tests")).get("execution_summary")
            ),
            kvasir_baseline_generated_summary=_mapping(
                _mapping(payload.get("baseline_generated_tests")).get("execution_summary")
            ),
            kvasir_ported_summary=_mapping(
                _mapping(payload.get("ported_original_tests")).get("execution_summary")
            ),
            kvasir_retention_metrics=_mapping(payload.get("suite_shape")),
            kvasir_write_scope_violation_count=_integer(
                _mapping(payload.get("write_scope")).get("violation_count")
            ),
            kvasir_adapter_log_paths=_mapping(payload.get("adapter")),
        )
    if service_name in {"lidskjalv-original", "lidskjalv-generated"}:
        scan = _mapping(payload.get("scan"))
        return replace(
            base,
            lidskjalv_scan_label=_string(payload.get("scan_label")),
            lidskjalv_project_key=_string(payload.get("project_key")),
            lidskjalv_project_name=_string(payload.get("project_name")),
            lidskjalv_build_tool=_string(scan.get("build_tool")),
            lidskjalv_build_jdk=_string(scan.get("build_jdk")),
            lidskjalv_build_subdir=_string(scan.get("build_subdir")),
            lidskjalv_java_version_hint=_string(scan.get("java_version_hint")),
            lidskjalv_sonar_task_id=_string(scan.get("sonar_task_id")),
            lidskjalv_ce_task_status=_string(scan.get("ce_task_status")),
            lidskjalv_quality_gate_status=_string(scan.get("quality_gate_status")),
            lidskjalv_measures=_mapping(scan.get("measures")),
        )
    return base


def _run_report_path_from_job(document: Mapping[str, object]) -> Path | None:
    run_dir = _string(document.get("run_dir"))
    if run_dir is None:
        return None
    return Path(run_dir) / "pipeline" / "outputs" / "run_report.json"


def _load_json_document(path: Path | None) -> dict[str, object]:
    if path is None or not path.is_file():
        return {}
    try:
        return read_json(path)
    except Exception as exc:
        _emit(
            "observability_indexer_warning",
            detail="invalid_json_document",
            path=str(path),
            error=str(exc),
        )
        return {}


def _load_yaml_mapping(path: Path, *, label: str) -> dict[str, object] | None:
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        loaded = loads(raw_text)
    except YamlError:
        return None
    if not isinstance(loaded, Mapping):
        return None
    return dict(loaded)


class PostgresIndexerRepository:
    def __init__(self, dsn: str, *, schema: str = "pipeline_obs") -> None:
        self.dsn = dsn
        self.schema = schema

    def ensure_schema(self) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(self._schema_sql())
            connection.commit()

    def get_run_state(self, run_id: str) -> RunIndexState | None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT last_observed_mtime, last_successful_indexed_at
                    FROM {self.schema}.indexer_state
                    WHERE state_key = %s
                    """,
                    (f"{_RUN_STATE_PREFIX}{run_id}",),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return RunIndexState(
            last_observed_mtime=row[0],
            last_successful_indexed_at=row[1],
        )

    def upsert_job(self, job: IndexedJob) -> None:
        Jsonb = self._jsonb_wrapper()
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {self.schema}.jobs (
                      job_id, queue_status, repo_url, commit_sha, run_id, run_dir,
                      submitted_at, started_at, finished_at, pipeline_status,
                      pipeline_reason, report_path, raw_payload, updated_at
                    ) VALUES (
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
                    )
                    ON CONFLICT (job_id) DO UPDATE SET
                      queue_status = EXCLUDED.queue_status,
                      repo_url = EXCLUDED.repo_url,
                      commit_sha = EXCLUDED.commit_sha,
                      run_id = EXCLUDED.run_id,
                      run_dir = EXCLUDED.run_dir,
                      submitted_at = EXCLUDED.submitted_at,
                      started_at = EXCLUDED.started_at,
                      finished_at = EXCLUDED.finished_at,
                      pipeline_status = EXCLUDED.pipeline_status,
                      pipeline_reason = EXCLUDED.pipeline_reason,
                      report_path = EXCLUDED.report_path,
                      raw_payload = EXCLUDED.raw_payload,
                      updated_at = now()
                    """,
                    (
                        job.job_id,
                        job.queue_status,
                        job.repo_url,
                        job.commit_sha,
                        job.run_id,
                        job.run_dir,
                        job.submitted_at,
                        job.started_at,
                        job.finished_at,
                        job.pipeline_status,
                        job.pipeline_reason,
                        job.report_path,
                        Jsonb(job.raw_payload),
                    ),
                )
            connection.commit()

    def upsert_run_bundle(
        self, bundle: IndexedRunBundle, *, observed_mtime: datetime
    ) -> None:
        Jsonb = self._jsonb_wrapper()
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {self.schema}.runs (
                      run_id, status, reason, started_at, finished_at,
                      manifest_path, resolved_path, raw_payload, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (run_id) DO UPDATE SET
                      status = EXCLUDED.status,
                      reason = EXCLUDED.reason,
                      started_at = EXCLUDED.started_at,
                      finished_at = EXCLUDED.finished_at,
                      manifest_path = EXCLUDED.manifest_path,
                      resolved_path = EXCLUDED.resolved_path,
                      raw_payload = EXCLUDED.raw_payload,
                      updated_at = now()
                    """,
                    (
                        bundle.run.run_id,
                        bundle.run.status,
                        bundle.run.reason,
                        bundle.run.started_at,
                        bundle.run.finished_at,
                        bundle.run.manifest_path,
                        bundle.run.resolved_path,
                        Jsonb(bundle.run.raw_payload),
                    ),
                )
                for step in bundle.steps:
                    cursor.execute(
                        f"""
                        INSERT INTO {self.schema}.run_steps (
                          run_id, step_name, status, reason, report_status,
                          started_at, finished_at, configured_image_ref,
                          resolved_image_id, fingerprint, report_path, raw_payload,
                          updated_at
                        ) VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
                        )
                        ON CONFLICT (run_id, step_name) DO UPDATE SET
                          status = EXCLUDED.status,
                          reason = EXCLUDED.reason,
                          report_status = EXCLUDED.report_status,
                          started_at = EXCLUDED.started_at,
                          finished_at = EXCLUDED.finished_at,
                          configured_image_ref = EXCLUDED.configured_image_ref,
                          resolved_image_id = EXCLUDED.resolved_image_id,
                          fingerprint = EXCLUDED.fingerprint,
                          report_path = EXCLUDED.report_path,
                          raw_payload = EXCLUDED.raw_payload,
                          updated_at = now()
                        """,
                        (
                            step.run_id,
                            step.step_name,
                            step.status,
                            step.reason,
                            step.report_status,
                            step.started_at,
                            step.finished_at,
                            step.configured_image_ref,
                            step.resolved_image_id,
                            step.fingerprint,
                            step.report_path,
                            Jsonb(step.raw_payload),
                        ),
                    )
                cursor.execute(
                    f"DELETE FROM {self.schema}.service_reports WHERE run_id = %s",
                    (bundle.run.run_id,),
                )
                for report in bundle.service_reports:
                    cursor.execute(
                        f"""
                        INSERT INTO {self.schema}.service_reports (
                          run_id, service_name, status, reason, status_detail,
                          started_at, finished_at, report_path, schema_version,
                          raw_payload,
                          brokk_repo_url, brokk_requested_commit, brokk_resolved_commit,
                          brokk_submodules_materialized, brokk_lfs_materialized,
                          eitri_type_count, eitri_relation_count, eitri_diagram_path,
                          eitri_logs_dir, andvari_gating_mode, andvari_adapter,
                          andvari_latest_gate_version, andvari_gates_total,
                          andvari_gates_passed, andvari_gates_failed,
                          andvari_outcomes_total, andvari_outcomes_core,
                          andvari_outcomes_non_core, andvari_generated_repo,
                          andvari_logs_dir, kvasir_behavioral_verdict,
                          kvasir_behavioral_verdict_reason,
                          kvasir_baseline_original_summary,
                          kvasir_baseline_generated_summary, kvasir_ported_summary,
                          kvasir_retention_metrics, kvasir_write_scope_violation_count,
                          kvasir_adapter_log_paths, lidskjalv_scan_label,
                          lidskjalv_project_key, lidskjalv_project_name,
                          lidskjalv_build_tool, lidskjalv_build_jdk,
                          lidskjalv_build_subdir, lidskjalv_java_version_hint,
                          lidskjalv_sonar_task_id, lidskjalv_ce_task_status,
                          lidskjalv_quality_gate_status, lidskjalv_measures, updated_at
                        ) VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, now()
                        )
                        """,
                        (
                            report.run_id,
                            report.service_name,
                            report.status,
                            report.reason,
                            report.status_detail,
                            report.started_at,
                            report.finished_at,
                            report.report_path,
                            report.schema_version,
                            Jsonb(report.raw_payload),
                            report.brokk_repo_url,
                            report.brokk_requested_commit,
                            report.brokk_resolved_commit,
                            report.brokk_submodules_materialized,
                            report.brokk_lfs_materialized,
                            report.eitri_type_count,
                            report.eitri_relation_count,
                            report.eitri_diagram_path,
                            report.eitri_logs_dir,
                            report.andvari_gating_mode,
                            report.andvari_adapter,
                            report.andvari_latest_gate_version,
                            report.andvari_gates_total,
                            report.andvari_gates_passed,
                            report.andvari_gates_failed,
                            report.andvari_outcomes_total,
                            report.andvari_outcomes_core,
                            report.andvari_outcomes_non_core,
                            report.andvari_generated_repo,
                            report.andvari_logs_dir,
                            report.kvasir_behavioral_verdict,
                            report.kvasir_behavioral_verdict_reason,
                            Jsonb(report.kvasir_baseline_original_summary or {}),
                            Jsonb(report.kvasir_baseline_generated_summary or {}),
                            Jsonb(report.kvasir_ported_summary or {}),
                            Jsonb(report.kvasir_retention_metrics or {}),
                            report.kvasir_write_scope_violation_count,
                            Jsonb(report.kvasir_adapter_log_paths or {}),
                            report.lidskjalv_scan_label,
                            report.lidskjalv_project_key,
                            report.lidskjalv_project_name,
                            report.lidskjalv_build_tool,
                            report.lidskjalv_build_jdk,
                            report.lidskjalv_build_subdir,
                            report.lidskjalv_java_version_hint,
                            report.lidskjalv_sonar_task_id,
                            report.lidskjalv_ce_task_status,
                            report.lidskjalv_quality_gate_status,
                            Jsonb(report.lidskjalv_measures or {}),
                        ),
                    )
                cursor.execute(
                    f"DELETE FROM {self.schema}.run_artifacts WHERE run_id = %s",
                    (bundle.run.run_id,),
                )
                for artifact in bundle.artifacts:
                    cursor.execute(
                        f"""
                        INSERT INTO {self.schema}.run_artifacts (
                          run_id, artifact_key, owner_step, path, updated_at
                        ) VALUES (%s, %s, %s, %s, now())
                        ON CONFLICT (run_id, artifact_key) DO UPDATE SET
                          owner_step = EXCLUDED.owner_step,
                          path = EXCLUDED.path,
                          updated_at = now()
                        """,
                        (
                            artifact.run_id,
                            artifact.artifact_key,
                            artifact.owner_step,
                            artifact.path,
                        ),
                    )
                cursor.execute(
                    f"DELETE FROM {self.schema}.sonar_follow_up WHERE run_id = %s",
                    (bundle.run.run_id,),
                )
                for follow_up in bundle.sonar_follow_up:
                    cursor.execute(
                        f"""
                        INSERT INTO {self.schema}.sonar_follow_up (
                          run_id, step_name, status, project_key, sonar_task_id,
                          quality_gate_status, measures, last_checked_at,
                          raw_payload, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (run_id, step_name) DO UPDATE SET
                          status = EXCLUDED.status,
                          project_key = EXCLUDED.project_key,
                          sonar_task_id = EXCLUDED.sonar_task_id,
                          quality_gate_status = EXCLUDED.quality_gate_status,
                          measures = EXCLUDED.measures,
                          last_checked_at = EXCLUDED.last_checked_at,
                          raw_payload = EXCLUDED.raw_payload,
                          updated_at = now()
                        """,
                        (
                            follow_up.run_id,
                            follow_up.step_name,
                            follow_up.status,
                            follow_up.project_key,
                            follow_up.sonar_task_id,
                            follow_up.quality_gate_status,
                            Jsonb(follow_up.measures),
                            follow_up.last_checked_at,
                            Jsonb(follow_up.raw_payload),
                        ),
                    )
                cursor.execute(
                    f"""
                    INSERT INTO {self.schema}.indexer_state (
                      state_key, last_observed_mtime, last_successful_indexed_at,
                      updated_at, payload
                    ) VALUES (%s, %s, now(), now(), %s)
                    ON CONFLICT (state_key) DO UPDATE SET
                      last_observed_mtime = EXCLUDED.last_observed_mtime,
                      last_successful_indexed_at = EXCLUDED.last_successful_indexed_at,
                      updated_at = now(),
                      payload = EXCLUDED.payload
                    """,
                    (
                        f"{_RUN_STATE_PREFIX}{bundle.run.run_id}",
                        observed_mtime,
                        Jsonb({"run_id": bundle.run.run_id}),
                    ),
                )
            connection.commit()

    def set_global_state(self, key: str, *, payload: dict[str, object]) -> None:
        Jsonb = self._jsonb_wrapper()
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {self.schema}.indexer_state (
                      state_key, updated_at, payload
                    ) VALUES (%s, now(), %s)
                    ON CONFLICT (state_key) DO UPDATE SET
                      updated_at = now(),
                      payload = EXCLUDED.payload
                    """,
                    (key, Jsonb(payload)),
                )
            connection.commit()

    def get_global_state(self, key: str) -> dict[str, object] | None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT payload FROM {self.schema}.indexer_state WHERE state_key = %s",
                    (key,),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        payload = row[0]
        return dict(payload) if isinstance(payload, Mapping) else None

    def prune(self, *, cutoff: datetime) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    DELETE FROM {self.schema}.jobs
                    WHERE finished_at IS NOT NULL
                      AND finished_at < %s
                      AND coalesce(queue_status, '') NOT IN ('pending', 'running')
                    """,
                    (cutoff,),
                )
                cursor.execute(
                    f"""
                    DELETE FROM {self.schema}.runs
                    WHERE finished_at IS NOT NULL
                      AND finished_at < %s
                      AND NOT EXISTS (
                        SELECT 1
                        FROM {self.schema}.sonar_follow_up sf
                        WHERE sf.run_id = {self.schema}.runs.run_id
                          AND sf.status = 'pending'
                      )
                    """,
                    (cutoff,),
                )
                cursor.execute(
                    f"""
                    DELETE FROM {self.schema}.indexer_state
                    WHERE state_key LIKE %s
                      AND last_successful_indexed_at IS NOT NULL
                      AND last_successful_indexed_at < %s
                    """,
                    (f"{_RUN_STATE_PREFIX}%", cutoff),
                )
            connection.commit()

    def _connect(self):
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - import guard
            raise RuntimeError(
                "psycopg is required for Gjallarhorn indexing"
            ) from exc
        return psycopg.connect(self.dsn)

    def _jsonb_wrapper(self):
        try:
            from psycopg.types.json import Jsonb
        except ImportError as exc:  # pragma: no cover - import guard
            raise RuntimeError(
                "psycopg is required for Gjallarhorn indexing"
            ) from exc
        return Jsonb

    def _schema_sql(self) -> str:
        return f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema};

        CREATE TABLE IF NOT EXISTS {self.schema}.jobs (
          job_id text PRIMARY KEY,
          queue_status text NOT NULL,
          repo_url text,
          commit_sha text,
          run_id text,
          run_dir text,
          submitted_at timestamptz,
          started_at timestamptz,
          finished_at timestamptz,
          pipeline_status text,
          pipeline_reason text,
          report_path text,
          raw_payload jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          updated_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS {self.schema}.runs (
          run_id text PRIMARY KEY,
          status text NOT NULL,
          reason text,
          started_at timestamptz,
          finished_at timestamptz,
          manifest_path text,
          resolved_path text,
          raw_payload jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          updated_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS {self.schema}.run_steps (
          run_id text NOT NULL REFERENCES {self.schema}.runs(run_id) ON DELETE CASCADE,
          step_name text NOT NULL,
          status text NOT NULL,
          reason text,
          report_status text,
          started_at timestamptz,
          finished_at timestamptz,
          configured_image_ref text,
          resolved_image_id text,
          fingerprint text,
          report_path text,
          raw_payload jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (run_id, step_name)
        );

        CREATE TABLE IF NOT EXISTS {self.schema}.service_reports (
          run_id text NOT NULL REFERENCES {self.schema}.runs(run_id) ON DELETE CASCADE,
          service_name text NOT NULL,
          status text,
          reason text,
          status_detail text,
          started_at timestamptz,
          finished_at timestamptz,
          report_path text NOT NULL,
          schema_version text,
          raw_payload jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          brokk_repo_url text,
          brokk_requested_commit text,
          brokk_resolved_commit text,
          brokk_submodules_materialized boolean,
          brokk_lfs_materialized boolean,
          eitri_type_count integer,
          eitri_relation_count integer,
          eitri_diagram_path text,
          eitri_logs_dir text,
          andvari_gating_mode text,
          andvari_adapter text,
          andvari_latest_gate_version text,
          andvari_gates_total integer,
          andvari_gates_passed integer,
          andvari_gates_failed integer,
          andvari_outcomes_total integer,
          andvari_outcomes_core integer,
          andvari_outcomes_non_core integer,
          andvari_generated_repo text,
          andvari_logs_dir text,
          kvasir_behavioral_verdict text,
          kvasir_behavioral_verdict_reason text,
          kvasir_baseline_original_summary jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          kvasir_baseline_generated_summary jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          kvasir_ported_summary jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          kvasir_retention_metrics jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          kvasir_write_scope_violation_count integer,
          kvasir_adapter_log_paths jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          lidskjalv_scan_label text,
          lidskjalv_project_key text,
          lidskjalv_project_name text,
          lidskjalv_build_tool text,
          lidskjalv_build_jdk text,
          lidskjalv_build_subdir text,
          lidskjalv_java_version_hint text,
          lidskjalv_sonar_task_id text,
          lidskjalv_ce_task_status text,
          lidskjalv_quality_gate_status text,
          lidskjalv_measures jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (run_id, service_name)
        );

        CREATE TABLE IF NOT EXISTS {self.schema}.run_artifacts (
          run_id text NOT NULL REFERENCES {self.schema}.runs(run_id) ON DELETE CASCADE,
          artifact_key text NOT NULL,
          owner_step text NOT NULL,
          path text NOT NULL,
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (run_id, artifact_key)
        );

        CREATE TABLE IF NOT EXISTS {self.schema}.sonar_follow_up (
          run_id text NOT NULL REFERENCES {self.schema}.runs(run_id) ON DELETE CASCADE,
          step_name text NOT NULL,
          status text,
          project_key text,
          sonar_task_id text,
          quality_gate_status text,
          measures jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          last_checked_at timestamptz,
          raw_payload jsonb NOT NULL DEFAULT '{{}}'::jsonb,
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (run_id, step_name)
        );

        CREATE TABLE IF NOT EXISTS {self.schema}.indexer_state (
          state_key text PRIMARY KEY,
          last_observed_mtime timestamptz,
          last_successful_indexed_at timestamptz,
          updated_at timestamptz NOT NULL DEFAULT now(),
          payload jsonb NOT NULL DEFAULT '{{}}'::jsonb
        );

        CREATE INDEX IF NOT EXISTS jobs_status_submitted_idx
          ON {self.schema}.jobs (queue_status, submitted_at DESC);
        CREATE INDEX IF NOT EXISTS runs_status_started_idx
          ON {self.schema}.runs (status, started_at DESC);
        CREATE INDEX IF NOT EXISTS run_steps_run_step_idx
          ON {self.schema}.run_steps (run_id, step_name);
        CREATE INDEX IF NOT EXISTS service_reports_name_status_idx
          ON {self.schema}.service_reports (service_name, status);
        CREATE INDEX IF NOT EXISTS service_reports_kvasir_verdict_idx
          ON {self.schema}.service_reports (kvasir_behavioral_verdict);
        CREATE INDEX IF NOT EXISTS service_reports_lidskjalv_qg_idx
          ON {self.schema}.service_reports (lidskjalv_quality_gate_status);
        CREATE INDEX IF NOT EXISTS sonar_follow_up_status_idx
          ON {self.schema}.sonar_follow_up (status);
        """
