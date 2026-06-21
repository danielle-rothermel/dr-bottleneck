from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dr_queues.events.schema import EventKind

from dr_bottleneck.analysis.overlap import overlap_report
from dr_bottleneck.job import terminal_payload_to_job_dict
from dr_bottleneck.storage.reports import write_run_report
from dr_bottleneck.workflow.engine import Workflow


def _final_result(
    job: dict[str, Any],
    step_names: list[str],
) -> dict[str, Any]:
    step_outputs = job.get("step_outputs", {})
    last_step = step_names[-1] if step_names else None
    return {
        "step_outputs": step_outputs,
        "final_step": last_step,
        "final_value": step_outputs.get(last_step or "", ""),
    }


def _job_from_terminal_payload(payload: dict[str, Any]) -> dict[str, Any]:
    job = terminal_payload_to_job_dict(payload)
    step_executions = job.get("step_executions", {})
    stages = list(step_executions.values())
    stages.sort(key=lambda stage: stage.get("step_index", 0))
    return {
        "job_id": job.get("job_id"),
        "lane": job.get("lane"),
        "repeat": job.get("repeat"),
        "stages": stages,
        "final_result": _final_result(
            job,
            [stage.get("name", "") for stage in stages],
        ),
    }


def build_run_report(
    *,
    workflow: Workflow,
    run_id: str,
    repeats: int,
    workers_by_stage: dict[str, int],
    workflow_path: Path,
    profiles_path: Path,
    run_events: list[dict[str, Any]],
    step1_name: str,
    step2_name: str,
) -> dict[str, Any]:
    terminal_payloads = [
        event["payload"]
        for event in run_events
        if event.get("event") == EventKind.TERMINAL
    ]
    terminal_payloads.sort(
        key=lambda payload: (
            payload.get("lane", ""),
            payload.get("repeat", 0),
        ),
    )

    return {
        "run_id": run_id,
        "config": workflow.run_config(
            run_id=run_id,
            repeats=repeats,
            workers_by_stage=workers_by_stage,
            workflow_path=workflow_path,
            profiles_path=profiles_path,
        ),
        "jobs": [
            _job_from_terminal_payload(payload)
            for payload in terminal_payloads
        ],
        "overlap_report": overlap_report(
            run_events,
            step1_name=step1_name,
            step2_name=step2_name,
        ),
        "created_at": datetime.now(tz=UTC).isoformat(),
    }


def persist_run_report(report: dict[str, Any]) -> None:
    write_run_report(report)
