from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

from dr_queues.analyze import filter_run_events
from dr_queues.connection import PikaDeliveryMode
from dr_queues.drain import ensure_drain_queue, peek_drain
from dr_queues.job import JobEnvelope, seed_jobs
from dr_queues.manifest import (
    RunManifest,
    RunStageManifest,
    manifest_path,
    write_run_manifest,
)
from dr_queues.queues import build_stage_queues
from dr_queues.tap import TerminalTap
from dr_queues.workers import WorkerPool
from dr_queues.workflow import Workflow

RUNNER_QUEUE_PREFIX = "demo"


def setup_run_queues(
    *,
    workflow: Workflow,
    run_id: str,
    workers_by_stage: dict[str, int],
    workflow_path: Path,
    profiles_path: Path,
    expected_jobs: int,
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
) -> RunManifest:
    ensure_drain_queue(delivery_mode=delivery_mode)

    prefix = f"{RUNNER_QUEUE_PREFIX}.{run_id}"
    stage_count = len(workflow.config.steps)
    stage_queues_list = []
    previous_completed: str | None = None

    for index in range(stage_count):
        stage_prefix = f"{prefix}.s{index + 1}"
        if index == 0:
            queues = build_stage_queues(
                prefix=stage_prefix,
                delivery_mode=delivery_mode,
            )
        else:
            queues = build_stage_queues(
                prefix=stage_prefix,
                pending=previous_completed,
                delivery_mode=delivery_mode,
            )
        stage_queues_list.append(queues)
        previous_completed = queues.completed_name

    stages: list[RunStageManifest] = []
    for index, step in enumerate(workflow.config.steps):
        queues = stage_queues_list[index]
        stages.append(
            RunStageManifest(
                name=step.name,
                step_index=index,
                input_queue=queues.pending_name,
                output_queue=queues.completed_name,
                default_workers=workers_by_stage.get(step.name, 10),
            ),
        )

    manifest = RunManifest(
        run_id=run_id,
        workflow_path=str(workflow_path),
        profiles_path=str(profiles_path),
        expected_jobs=expected_jobs,
        queue_prefix=prefix,
        stages=stages,
    )
    write_run_manifest(manifest_path(run_id), manifest)
    return manifest


def _build_pools(
    *,
    manifest: RunManifest,
    workflow: Workflow,
    workers_by_stage: dict[str, int],
) -> list[WorkerPool]:
    pools: list[WorkerPool] = []
    for stage in manifest.stages:
        workers = workers_by_stage.get(stage.name, stage.default_workers)
        handler = workflow.make_handler(stage.step_index)
        pools.append(
            WorkerPool(
                input_queue=stage.input_queue,
                output_queue=stage.output_queue,
                handler=handler,
                workers=workers,
                stage_name=stage.name,
            ),
        )
    return pools


def run_workflow_in_process(
    *,
    manifest: RunManifest,
    workflow: Workflow,
    workers_by_stage: dict[str, int],
    completion_timeout: float,
    tap: TerminalTap | None = None,
) -> None:
    pools = _build_pools(
        manifest=manifest,
        workflow=workflow,
        workers_by_stage=workers_by_stage,
    )
    owned_tap = tap is None
    if tap is None:
        final_stage = manifest.stages[-1]
        tap = TerminalTap(
            completed_queue=final_stage.output_queue,
            run_id=manifest.run_id,
            expected_count=manifest.expected_jobs,
        )

    for pool in reversed(pools):
        pool.start()
    if owned_tap:
        tap.start()

    if not tap.wait_for_completion(timeout=completion_timeout):
        msg = "Timed out waiting for workflow completion."
        raise TimeoutError(msg)

    for pool in pools:
        pool.stop()
    if owned_tap:
        tap.stop()

    for pool in pools:
        pool.join(timeout=5)
    if owned_tap:
        tap.join(timeout=5)

    time.sleep(0.5)


def spawn_stage_worker_process(
    *,
    manifest_path: Path,
    stage: str,
    workers: int,
    replace: bool = True,
) -> subprocess.Popen[bytes]:
    cmd = [
        sys.executable,
        "scripts/run_stage_workers.py",
        "--manifest",
        str(manifest_path),
        "--stage",
        stage,
        "--workers",
        str(workers),
    ]
    if replace:
        cmd.append("--replace")
    return subprocess.Popen(cmd)


def spawn_all_stage_workers(
    *,
    manifest: RunManifest,
    workers_by_stage: dict[str, int],
) -> list[subprocess.Popen[bytes]]:
    path = manifest_path(manifest.run_id)
    processes: list[subprocess.Popen[bytes]] = []
    for stage in reversed(manifest.stages):
        workers = workers_by_stage.get(stage.name, stage.default_workers)
        processes.append(
            spawn_stage_worker_process(
                manifest_path=path,
                stage=stage.name,
                workers=workers,
                replace=True,
            ),
        )
    return processes


def peek_run_events(run_id: str) -> list[dict]:
    events = peek_drain()
    return filter_run_events(events, run_id)


def first_stage_input(manifest: RunManifest) -> str:
    return manifest.stages[0].input_queue


def seed_manifest_jobs(manifest: RunManifest, jobs: list[JobEnvelope]) -> None:
    seed_jobs(
        queue_name=first_stage_input(manifest),
        jobs=jobs,
        delivery_mode=PikaDeliveryMode.PERSISTENT,
    )
