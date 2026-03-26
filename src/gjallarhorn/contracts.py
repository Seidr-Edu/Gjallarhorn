from __future__ import annotations

from dataclasses import dataclass

STEP_BROKK = "brokk"
STEP_EITRI = "eitri"
STEP_ANDVARI = "andvari"
STEP_KVASIR = "kvasir"
STEP_LIDSKJALV_ORIGINAL = "lidskjalv-original"
STEP_LIDSKJALV_GENERATED = "lidskjalv-generated"

ALL_STEPS = (
    STEP_BROKK,
    STEP_EITRI,
    STEP_LIDSKJALV_ORIGINAL,
    STEP_ANDVARI,
    STEP_KVASIR,
    STEP_LIDSKJALV_GENERATED,
)


@dataclass(frozen=True)
class StepDefinition:
    name: str
    depends_on: tuple[str, ...]
    service_dir_name: str
    report_relative_path: str


STEP_DEFINITIONS: dict[str, StepDefinition] = {
    STEP_BROKK: StepDefinition(
        name=STEP_BROKK,
        depends_on=(),
        service_dir_name="brokk",
        report_relative_path="outputs/run_report.json",
    ),
    STEP_EITRI: StepDefinition(
        name=STEP_EITRI,
        depends_on=(STEP_BROKK,),
        service_dir_name="eitri",
        report_relative_path="outputs/run_report.json",
    ),
    STEP_LIDSKJALV_ORIGINAL: StepDefinition(
        name=STEP_LIDSKJALV_ORIGINAL,
        depends_on=(STEP_BROKK,),
        service_dir_name="lidskjalv-original",
        report_relative_path="outputs/run_report.json",
    ),
    STEP_ANDVARI: StepDefinition(
        name=STEP_ANDVARI,
        depends_on=(STEP_EITRI,),
        service_dir_name="andvari",
        report_relative_path="outputs/run_report.json",
    ),
    STEP_KVASIR: StepDefinition(
        name=STEP_KVASIR,
        depends_on=(STEP_BROKK, STEP_EITRI, STEP_ANDVARI),
        service_dir_name="kvasir",
        report_relative_path="outputs/test_port.json",
    ),
    STEP_LIDSKJALV_GENERATED: StepDefinition(
        name=STEP_LIDSKJALV_GENERATED,
        depends_on=(STEP_ANDVARI,),
        service_dir_name="lidskjalv-generated",
        report_relative_path="outputs/run_report.json",
    ),
}
