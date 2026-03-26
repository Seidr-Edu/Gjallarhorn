from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class IndexedJob:
    job_id: str
    queue_status: str
    repo_url: str | None
    commit_sha: str | None
    run_id: str | None
    run_dir: str | None
    submitted_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    pipeline_status: str | None
    pipeline_reason: str | None
    report_path: str | None
    raw_payload: dict[str, object]


@dataclass(frozen=True)
class IndexedRun:
    run_id: str
    status: str
    reason: str | None
    started_at: datetime | None
    finished_at: datetime | None
    manifest_path: str | None
    resolved_path: str | None
    raw_payload: dict[str, object]


@dataclass(frozen=True)
class IndexedRunStep:
    run_id: str
    step_name: str
    status: str
    reason: str | None
    report_status: str | None
    started_at: datetime | None
    finished_at: datetime | None
    configured_image_ref: str | None
    resolved_image_id: str | None
    fingerprint: str | None
    report_path: str | None
    raw_payload: dict[str, object]


@dataclass(frozen=True)
class IndexedServiceReport:
    run_id: str
    service_name: str
    status: str | None
    reason: str | None
    status_detail: str | None
    started_at: datetime | None
    finished_at: datetime | None
    report_path: str
    schema_version: str | None
    raw_payload: dict[str, object]
    brokk_repo_url: str | None = None
    brokk_requested_commit: str | None = None
    brokk_resolved_commit: str | None = None
    brokk_submodules_materialized: bool | None = None
    brokk_lfs_materialized: bool | None = None
    eitri_type_count: int | None = None
    eitri_relation_count: int | None = None
    eitri_diagram_path: str | None = None
    eitri_logs_dir: str | None = None
    andvari_gating_mode: str | None = None
    andvari_adapter: str | None = None
    andvari_latest_gate_version: str | None = None
    andvari_gates_total: int | None = None
    andvari_gates_passed: int | None = None
    andvari_gates_failed: int | None = None
    andvari_outcomes_total: int | None = None
    andvari_outcomes_core: int | None = None
    andvari_outcomes_non_core: int | None = None
    andvari_generated_repo: str | None = None
    andvari_logs_dir: str | None = None
    kvasir_behavioral_verdict: str | None = None
    kvasir_behavioral_verdict_reason: str | None = None
    kvasir_baseline_original_summary: dict[str, object] | None = None
    kvasir_baseline_generated_summary: dict[str, object] | None = None
    kvasir_ported_summary: dict[str, object] | None = None
    kvasir_retention_metrics: dict[str, object] | None = None
    kvasir_write_scope_violation_count: int | None = None
    kvasir_adapter_log_paths: dict[str, object] | None = None
    lidskjalv_scan_label: str | None = None
    lidskjalv_project_key: str | None = None
    lidskjalv_project_name: str | None = None
    lidskjalv_build_tool: str | None = None
    lidskjalv_build_jdk: str | None = None
    lidskjalv_build_subdir: str | None = None
    lidskjalv_java_version_hint: str | None = None
    lidskjalv_sonar_task_id: str | None = None
    lidskjalv_ce_task_status: str | None = None
    lidskjalv_quality_gate_status: str | None = None
    lidskjalv_measures: dict[str, object] | None = None


@dataclass(frozen=True)
class IndexedArtifact:
    run_id: str
    artifact_key: str
    owner_step: str
    path: str


@dataclass(frozen=True)
class IndexedSonarFollowUp:
    run_id: str
    step_name: str
    status: str | None
    project_key: str | None
    sonar_task_id: str | None
    quality_gate_status: str | None
    measures: dict[str, object]
    last_checked_at: datetime | None
    raw_payload: dict[str, object]


@dataclass(frozen=True)
class IndexedRunBundle:
    run: IndexedRun
    steps: list[IndexedRunStep] = field(default_factory=list)
    service_reports: list[IndexedServiceReport] = field(default_factory=list)
    artifacts: list[IndexedArtifact] = field(default_factory=list)
    sonar_follow_up: list[IndexedSonarFollowUp] = field(default_factory=list)


@dataclass(frozen=True)
class RunIndexState:
    last_observed_mtime: datetime | None
    last_successful_indexed_at: datetime | None
