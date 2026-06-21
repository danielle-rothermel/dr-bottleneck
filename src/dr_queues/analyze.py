from datetime import datetime
from typing import Any

from dr_queues.drain import DrainEventKind


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def filter_run_events(
    events: list[dict[str, Any]],
    run_id: str,
) -> list[dict[str, Any]]:
    return [event for event in events if event.get("run_id") == run_id]


def overlap_report(
    events: list[dict[str, Any]],
    *,
    step1_name: str,
    step2_name: str,
) -> dict[str, Any]:
    step1_outputs = [
        _parse_ts(event["timestamp"])
        for event in events
        if event.get("event") == DrainEventKind.STAGE_OUTPUT
        and event.get("stage") == step1_name
    ]
    step2_starts = [
        _parse_ts(event["timestamp"])
        for event in events
        if event.get("event") == DrainEventKind.STAGE_STARTED
        and event.get("stage") == step2_name
    ]

    if not step1_outputs or not step2_starts:
        return {
            "passed": False,
            "reason": "missing stage events for overlap check",
            "step1_output_count": len(step1_outputs),
            "step2_started_count": len(step2_starts),
        }

    earliest_step2 = min(step2_starts)
    latest_step1 = max(step1_outputs)
    passed = earliest_step2 < latest_step1

    timeline = sorted(events, key=lambda item: item.get("timestamp", ""))[:10]

    return {
        "passed": passed,
        "earliest_step2_started": earliest_step2.isoformat(),
        "latest_step1_output": latest_step1.isoformat(),
        "step1_output_count": len(step1_outputs),
        "step2_started_count": len(step2_starts),
        "timeline_snippet": timeline,
    }


def format_overlap_report(report: dict[str, Any]) -> str:
    lines = ["=== Pipeline overlap report ==="]
    if report.get("reason"):
        lines.append(f"FAIL: {report['reason']}")
        return "\n".join(lines)

    status = "PASS" if report["passed"] else "FAIL"
    lines.append(
        f"{status}: stage 2 started before all stage 1 outputs finished",
    )
    lines.append(
        f"  earliest step2 started: {report['earliest_step2_started']}",
    )
    lines.append(f"  latest step1 output:    {report['latest_step1_output']}")
    lines.append(
        f"  step1 outputs: {report['step1_output_count']}, "
        f"step2 starts: {report['step2_started_count']}",
    )
    lines.append("  timeline (first 10 events):")
    for event in report.get("timeline_snippet", []):
        lines.append(
            f"    {event.get('timestamp')} "
            f"{event.get('stage')} {event.get('event')} "
            f"job={event.get('job_id')}",
        )
    return "\n".join(lines)
