from __future__ import annotations

from dr_bottleneck.analysis.metrics import (
    build_metrics_rows,
    summarize_metrics,
)


def test_build_metrics_rows_from_terminal_payload() -> None:
    payloads = [
        {
            "job_id": "job-1",
            "lane": "gemini",
            "repeat": 0,
            "workflow_id": "wf",
            "sample": {"task_id": "HumanEval/0"},
            "metadata": {"budget": 128},
            "step_outputs": {},
            "step_executions": {
                "decode": {
                    "step_index": 1,
                    "name": "decode",
                    "model": "openrouter/google/gemini-2.5-flash",
                },
            },
            "step_process_results": {
                "evaluate": {
                    "step_index": 2,
                    "name": "evaluate",
                    "handler": "humaneval_compress_ast",
                    "result": {
                        "encoded_len_raw": 100,
                        "encoded_len_compressed": 50,
                        "ast_parse_ok": 1,
                    },
                },
            },
        },
    ]

    rows = build_metrics_rows(payloads)
    assert len(rows) == 1
    assert rows[0]["model"] == "openrouter/google/gemini-2.5-flash"
    assert rows[0]["budget"] == 128
    assert rows[0]["pass"] == 1


def test_summarize_metrics() -> None:
    rows = [
        {"model": "m1", "budget": 64, "pass": 1},
        {"model": "m1", "budget": 64, "pass": 0},
    ]
    summary = summarize_metrics(rows)
    assert summary["total"] == 2
    assert summary["passed"] == 1
    assert summary["pass_rate"] == 0.5
    assert summary["by_model"]["m1"]["pass_rate"] == 0.5
