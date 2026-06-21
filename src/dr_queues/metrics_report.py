from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def build_metrics_rows(
    terminal_payloads: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for payload in terminal_payloads:
        sample = payload.get("sample", {})
        metadata = payload.get("metadata", {})
        step_executions = payload.get("step_executions", {})
        step_process_results = payload.get("step_process_results", {})

        decode_exec = None
        for execution in step_executions.values():
            if decode_exec is None or execution.get(
                "step_index", 0
            ) > decode_exec.get(
                "step_index",
                0,
            ):
                decode_exec = execution

        evaluate_result: dict[str, Any] = {}
        for result in step_process_results.values():
            if not evaluate_result or result.get(
                "step_index", 0
            ) >= evaluate_result.get(
                "step_index",
                0,
            ):
                evaluate_result = result

        result_data = evaluate_result.get("result", {})
        model = decode_exec.get("model", "") if decode_exec else ""

        rows.append(
            {
                "model": model,
                "budget": metadata.get("budget", 0),
                "sample_id": sample.get("task_id", ""),
                "lane": payload.get("lane", ""),
                "repeat": payload.get("repeat", 0),
                "encoded_len_raw": result_data.get("encoded_len_raw", 0),
                "encoded_len_compressed": result_data.get(
                    "encoded_len_compressed",
                    0,
                ),
                "pass": result_data.get("ast_parse_ok", 0),
            },
        )
    return rows


def write_metrics_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")))
            handle.write("\n")


def summarize_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"total": 0, "pass_rate": 0.0, "by_model": {}, "by_budget": {}}

    total = len(rows)
    passed = sum(int(row.get("pass", 0)) for row in rows)
    by_model: dict[str, dict[str, int | float]] = {}
    by_budget: dict[int, dict[str, int | float]] = {}

    for row in rows:
        model = str(row.get("model", ""))
        budget = int(row.get("budget", 0))
        ok = int(row.get("pass", 0))

        model_stats = by_model.setdefault(model, {"total": 0, "passed": 0})
        model_stats["total"] = int(model_stats["total"]) + 1
        model_stats["passed"] = int(model_stats["passed"]) + ok

        budget_stats = by_budget.setdefault(budget, {"total": 0, "passed": 0})
        budget_stats["total"] = int(budget_stats["total"]) + 1
        budget_stats["passed"] = int(budget_stats["passed"]) + ok

    for stats in by_model.values():
        count = int(stats["total"])
        stats["pass_rate"] = int(stats["passed"]) / count if count else 0.0

    for stats in by_budget.values():
        count = int(stats["total"])
        stats["pass_rate"] = int(stats["passed"]) / count if count else 0.0

    return {
        "total": total,
        "passed": passed,
        "pass_rate": passed / total if total else 0.0,
        "by_model": by_model,
        "by_budget": by_budget,
    }
