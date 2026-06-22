from __future__ import annotations

import ast
from typing import TYPE_CHECKING

from dr_code.datasets.humaneval_loader import load_humaneval_plus
from dr_code.models.attempts import AttemptRecord
from dr_code.models.humaneval import HumanEvalPlusTask
from dr_queues import JobEnvelope

from dr_bottleneck.job import (
    BottleneckPayload,
    llm_step_record,
    make_job_envelope,
    terminal_payload_to_job,
)

if TYPE_CHECKING:
    from dr_bottleneck.workflow.engine import Workflow

DEFAULT_BUDGETS = [32, 64, 128, 256, 512, 1024]
TINY_TASK_IDS = ["HumanEval/0", "HumanEval/1"]
TINY_LANE_ID = "gemini"
TINY_BUDGET = 128
ENCODE_STEP_NAME = "encode"
DECODE_STEP_NAME = "decode"


def load_humanevalplus() -> list[HumanEvalPlusTask]:
    return load_humaneval_plus()


def _strip_docstring(body: list[ast.stmt]) -> list[ast.stmt]:
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        return body[1:]
    return body


def strip_comments_and_docstrings(code: str) -> str:
    tree = ast.parse(code)

    doc_node_types = (
        ast.FunctionDef,
        ast.AsyncFunctionDef,
        ast.ClassDef,
        ast.Module,
    )
    for node in ast.walk(tree):
        if isinstance(node, doc_node_types):
            node.body = _strip_docstring(node.body)

    return ast.unparse(tree).strip()


def build_source_code(prompt: str, canonical_solution: str) -> str:
    combined = f"{prompt.rstrip()}\n{canonical_solution.rstrip()}\n"
    return strip_comments_and_docstrings(combined)


def expand_experiment_jobs(
    *,
    workflow: Workflow,
    run_id: str,
    tasks: list[HumanEvalPlusTask],
    budgets: list[int],
    lane_ids: list[str],
    repeats: int,
) -> list[JobEnvelope]:
    jobs: list[JobEnvelope] = []
    for lane_id in lane_ids:
        for task in tasks:
            for budget in budgets:
                for repeat in range(repeats):
                    jobs.append(
                        make_job_envelope(
                            run_id=run_id,
                            lane=lane_id,
                            repeat=repeat,
                            pipeline_id=workflow.config.id,
                            payload=BottleneckPayload(
                                sample=task.model_dump(mode="json"),
                                metadata={"budget": budget},
                                source_code=build_source_code(
                                    task.prompt,
                                    task.canonical_solution,
                                ),
                            ),
                        )
                    )
    return jobs


def tiny_experiment_filters(
    workflow: Workflow,
    *,
    budgets: list[int] | None = None,
) -> tuple[list[str], list[int], list[HumanEvalPlusTask]]:
    all_tasks = load_humanevalplus()
    task_by_id = {task.task_id: task for task in all_tasks}
    tasks = [task_by_id[task_id] for task_id in TINY_TASK_IDS]
    lane_ids = [TINY_LANE_ID]
    resolved_budgets = [TINY_BUDGET] if budgets is None else budgets
    if TINY_LANE_ID not in workflow.lane_ids():
        msg = f"Tiny mode requires lane {TINY_LANE_ID!r} in workflow."
        raise ValueError(msg)
    return lane_ids, resolved_budgets, tasks


def make_preview_job(
    *,
    task: HumanEvalPlusTask,
    budget: int,
    workflow_id: str,
    encode_output: str = "",
) -> JobEnvelope:
    job = make_job_envelope(
        run_id="preview",
        lane="preview",
        repeat=0,
        pipeline_id=workflow_id,
        payload=BottleneckPayload(
            sample=task.model_dump(mode="json"),
            metadata={"budget": budget},
            source_code=build_source_code(
                task.prompt,
                task.canonical_solution,
            ),
        ),
    )
    if encode_output:
        job.step_outputs[ENCODE_STEP_NAME] = encode_output
    return job


def filter_tasks(
    tasks: list[HumanEvalPlusTask],
    task_ids: list[str] | None,
) -> list[HumanEvalPlusTask]:
    if not task_ids:
        return tasks
    wanted = set(task_ids)
    return [task for task in tasks if task.task_id in wanted]


def attempts_from_terminal_payloads(
    *,
    run_id: str,
    terminal_payloads: list[dict],
) -> list[AttemptRecord]:
    attempts: list[AttemptRecord] = []
    for payload in terminal_payloads:
        job = terminal_payload_to_job(payload)
        sample = job.payload.get("sample", {})
        metadata = job.payload.get("metadata", {})
        if not isinstance(sample, dict):
            msg = f"Job {job.job_id!r} has invalid sample payload."
            raise ValueError(msg)
        encode_text = str(job.step_outputs.get(ENCODE_STEP_NAME, ""))
        decode_text = str(job.step_outputs.get(DECODE_STEP_NAME, ""))
        if not encode_text or not decode_text:
            msg = (
                f"Job {job.job_id!r} is missing encode/decode output "
                f"for run {run_id!r}."
            )
            raise ValueError(msg)
        encode_record = llm_step_record(job, ENCODE_STEP_NAME)
        decode_record = llm_step_record(job, DECODE_STEP_NAME)
        attempts.append(
            AttemptRecord.from_bottleneck_output(
                run_id=run_id,
                task_id=str(sample["task_id"]),
                entry_point=str(sample["entry_point"]),
                decoder_input=encode_text,
                raw_output=decode_text,
                encode_model=encode_record.model,
                decode_model=decode_record.model,
                encode_profile_id=encode_record.profile,
                decode_profile_id=decode_record.profile,
                extra={
                    "bottleneck_run_id": run_id,
                    "bottleneck_job_id": job.job_id,
                    "lane": job.lane,
                    "repeat": job.repeat,
                    "budget": int(metadata.get("budget", 0)),
                },
            )
        )
    return attempts


__all__ = [
    "DECODE_STEP_NAME",
    "DEFAULT_BUDGETS",
    "ENCODE_STEP_NAME",
    "attempts_from_terminal_payloads",
    "build_source_code",
    "expand_experiment_jobs",
    "filter_tasks",
    "load_humanevalplus",
    "make_preview_job",
    "strip_comments_and_docstrings",
    "tiny_experiment_filters",
]
