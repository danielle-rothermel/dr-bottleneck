from __future__ import annotations

import ast
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import zstandard
from pydantic import BaseModel

from dr_queues.job import JobEnvelope, ProcessStepResult
from dr_queues.process_handlers import register
from dr_queues.workflow import WorkflowStep

if TYPE_CHECKING:
    from dr_queues.workflow import Workflow


@register("humaneval_compress_ast")
def humaneval_compress_ast(
    job: JobEnvelope,
    step: WorkflowStep,
) -> JobEnvelope:
    encode_step = step.config.get("encode_step", "encode")
    decode_step = step.config.get("decode_step", "decode")
    zstd_level = int(step.config.get("zstd_level", 22))

    encoded_text = job.step_outputs.get(encode_step, "")
    decoded_text = job.step_outputs.get(decode_step, "")

    encoded_bytes = encoded_text.encode("utf-8")
    encoded_len_raw = len(encoded_bytes)

    compressor = zstandard.ZstdCompressor(level=zstd_level)
    compressed = compressor.compress(encoded_bytes)
    encoded_len_compressed = len(compressed)

    ast_parse_ok = 0
    try:
        ast.parse(decoded_text)
        ast_parse_ok = 1
    except SyntaxError:
        ast_parse_ok = 0

    result = {
        "encoded_len_raw": encoded_len_raw,
        "encoded_len_compressed": encoded_len_compressed,
        "ast_parse_ok": ast_parse_ok,
        "zstd_level": zstd_level,
    }

    job.step_outputs[step.name] = (
        f"raw={encoded_len_raw} compressed={encoded_len_compressed} "
        f"ast={ast_parse_ok}"
    )
    job.step_process_results[step.name] = ProcessStepResult(
        step_index=job.step_index,
        name=step.name,
        handler=step.handler or "humaneval_compress_ast",
        result=result,
        timestamp=datetime.now(tz=UTC).isoformat(),
    )
    return job


class HumanEvalSampleInfo(BaseModel):
    task_id: str = ""
    prompt: str = ""
    canonical_solution: str = ""
    entry_point: str = ""


class HumanEvalJobMetadata(BaseModel):
    budget: int = 0


def load_humanevalplus() -> list[dict]:
    from datasets import load_dataset

    dataset = load_dataset("evalplus/humanevalplus", split="test")
    return [dict(row) for row in dataset]


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


DEFAULT_BUDGETS = [32, 64, 128, 256, 512, 1024]
TINY_TASK_IDS = ["HumanEval/0", "HumanEval/1"]
TINY_LANE_ID = "gemini"
TINY_BUDGET = 128


def expand_experiment_jobs(
    *,
    workflow: Workflow,
    run_id: str,
    tasks: list[dict],
    budgets: list[int],
    lane_ids: list[str],
    repeats: int,
) -> list[JobEnvelope]:
    jobs: list[JobEnvelope] = []
    for lane_id in lane_ids:
        for task in tasks:
            for budget in budgets:
                for repeat in range(repeats):
                    sample = HumanEvalSampleInfo(
                        task_id=task["task_id"],
                        prompt=task["prompt"],
                        canonical_solution=task["canonical_solution"],
                        entry_point=task["entry_point"],
                    )
                    jobs.append(
                        JobEnvelope(
                            run_id=run_id,
                            lane=lane_id,
                            repeat=repeat,
                            step_index=0,
                            workflow_id=workflow.config.id,
                            sample=sample.model_dump(),
                            metadata=HumanEvalJobMetadata(
                                budget=budget,
                            ).model_dump(),
                            source_code=build_source_code(
                                task["prompt"],
                                task["canonical_solution"],
                            ),
                        ),
                    )
    return jobs


def tiny_experiment_filters(
    workflow: Workflow,
    *,
    budgets: list[int] | None = None,
) -> tuple[list[str], list[int], list[dict]]:
    all_tasks = load_humanevalplus()
    task_by_id = {task["task_id"]: task for task in all_tasks}
    tasks = [task_by_id[task_id] for task_id in TINY_TASK_IDS]
    lane_ids = [TINY_LANE_ID]
    resolved_budgets = [TINY_BUDGET] if budgets is None else budgets
    if TINY_LANE_ID not in workflow.lane_ids():
        msg = f"Tiny mode requires lane {TINY_LANE_ID!r} in workflow."
        raise ValueError(msg)
    return lane_ids, resolved_budgets, tasks


def make_preview_job(
    *,
    task: dict,
    budget: int,
    workflow_id: str,
    encode_output: str = "",
) -> JobEnvelope:
    job = JobEnvelope(
        run_id="preview",
        lane="preview",
        repeat=0,
        step_index=0,
        workflow_id=workflow_id,
        sample=HumanEvalSampleInfo(
            task_id=task["task_id"],
            prompt=task["prompt"],
            canonical_solution=task["canonical_solution"],
            entry_point=task["entry_point"],
        ).model_dump(),
        metadata=HumanEvalJobMetadata(budget=budget).model_dump(),
        source_code=build_source_code(
            task["prompt"],
            task["canonical_solution"],
        ),
    )
    if encode_output:
        job.step_outputs["encode"] = encode_output
    return job


def filter_tasks(
    tasks: list[dict],
    task_ids: list[str] | None,
) -> list[dict]:
    if not task_ids:
        return tasks
    wanted = set(task_ids)
    return [task for task in tasks if task["task_id"] in wanted]
