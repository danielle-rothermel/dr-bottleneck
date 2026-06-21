import json
from pathlib import Path
from typing import Any

from dr_queues.analyze import overlap_report
from dr_queues.models import DrainEventKind
from dr_queues.workflow import Workflow


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
    step_executions = payload.get("step_executions", {})
    stages = list(step_executions.values())
    stages.sort(key=lambda stage: stage.get("step_index", 0))
    return {
        "job_id": payload.get("job_id"),
        "lane": payload.get("lane"),
        "repeat": payload.get("repeat"),
        "stages": stages,
        "final_result": _final_result(
            payload,
            [stage.get("name", "") for stage in stages],
        ),
    }


def iter_run_report_records(report: dict[str, Any]) -> list[dict[str, Any]]:
    run_id = report["run_id"]
    records: list[dict[str, Any]] = [
        {
            "record_type": "config",
            "run_id": run_id,
            "config": report["config"],
        },
    ]
    for job in report["jobs"]:
        records.append(
            {
                "record_type": "job",
                "run_id": run_id,
                "job": job,
            },
        )
    records.append(
        {
            "record_type": "overlap_report",
            "run_id": run_id,
            "overlap_report": report["overlap_report"],
        },
    )
    return records


def write_run_report_jsonl(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in iter_run_report_records(report):
            handle.write(json.dumps(record, separators=(",", ":")))
            handle.write("\n")


def build_run_report(
    *,
    workflow: Workflow,
    run_id: str,
    repeats: int,
    workers: int,
    workflow_path: Path,
    profiles_path: Path,
    run_events: list[dict[str, Any]],
    step1_name: str,
    step2_name: str,
) -> dict[str, Any]:
    terminal_payloads = [
        event["payload"]
        for event in run_events
        if event.get("event") == DrainEventKind.TERMINAL
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
            workers=workers,
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
    }
