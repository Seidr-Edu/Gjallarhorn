from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class StepState:
    status: str = "pending"
    reason: str | None = None
    blocked_by: list[str] = field(default_factory=list)
    started_at: str | None = None
    finished_at: str | None = None
    configured_image_ref: str | None = None
    resolved_image_id: str | None = None
    fingerprint: str | None = None
    report_path: str | None = None
    report_status: str | None = None


def load_existing_state(path: Path) -> dict[str, StepState]:
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    steps = data.get("steps", {})
    if not isinstance(steps, dict):
        return {}
    result: dict[str, StepState] = {}
    for step, raw in steps.items():
        if not isinstance(raw, dict):
            continue
        result[step] = StepState(
            status=str(raw.get("status", "pending")),
            reason=_optional_string(raw.get("reason")),
            blocked_by=_string_list(raw.get("blocked_by")),
            started_at=_optional_string(raw.get("started_at")),
            finished_at=_optional_string(raw.get("finished_at")),
            configured_image_ref=_optional_string(raw.get("configured_image_ref")),
            resolved_image_id=_optional_string(raw.get("resolved_image_id")),
            fingerprint=_optional_string(raw.get("fingerprint")),
            report_path=_optional_string(raw.get("report_path")),
            report_status=_optional_string(raw.get("report_status")),
        )
    return result


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]
