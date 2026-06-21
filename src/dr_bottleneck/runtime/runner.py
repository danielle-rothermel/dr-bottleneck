from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

from dr_queues.amqp.connection import PikaDeliveryMode
from dr_queues.amqp.queues import build_stage_queues
from dr_queues.events.mongo import MongoEventSink
from dr_queues.events.sink import EventSink
from dr_queues.manifest import manifest_path, parse_workers_arg
from dr_queues.pipeline import TerminalTap, WorkerPool, seed_jobs

from dr_bottleneck.job import BottleneckJob
from dr_bottleneck.runtime.manifest import (
    BottleneckRunManifest,
    BottleneckStageManifest,
    write_bottleneck_manifest,
)
from dr_bottleneck.workflow.engine import Workflow

RUNNER_QUEUE_PREFIX = "bottleneck"
STAGE_WORKER_ENTRYPOINT = "dr-bottleneck-stage-worker"


def create_event_sink() -> EventSink:
    return MongoEventSink()


def setup_run_queues(
    *,
    workflow: Workflow,
    run_id: str,
    workers_by_stage: dict[str, int],
    workflow_path: Path,
    profiles_path: Path,
    expected_jobs: int,
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
) -> BottleneckRunManifest:
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

    stages: list[BottleneckStageManifest] = []
    for index, step in enumerate(workflow.config.steps):
        queues = stage_queues_list[index]
        stages.append(
            BottleneckStageManifest(
                name=step.name,
                step_index=index,
                input_queue=queues.pending_name,
                output_queue=queues.completed_name,
                default_workers=workers_by_stage.get(step.name, 10),
            ),
        )

    manifest = BottleneckRunManifest(
        run_id=run_id,
        workflow_id=workflow.config.id,
        workflow_path=str(workflow_path),
        profiles_path=str(profiles_path),
        expected_jobs=expected_jobs,
        queue_prefix=prefix,
        stages=stages,
    )
    write_bottleneck_manifest(manifest_path(run_id), manifest)
    return manifest


def _build_pools(
    *,
    manifest: BottleneckRunManifest,
    workflow: Workflow,
    workers_by_stage: dict[str, int],
    event_sink: EventSink,
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
                event_sink=event_sink,
                workers=workers,
                stage_name=stage.name,
            ),
        )
    return pools


def run_workflow_in_process(
    *,
    manifest: BottleneckRunManifest,
    workflow: Workflow,
    workers_by_stage: dict[str, int],
    completion_timeout: float,
    event_sink: EventSink | None = None,
    tap: TerminalTap | None = None,
) -> None:
    event_sink = event_sink or create_event_sink()
    pools = _build_pools(
        manifest=manifest,
        workflow=workflow,
        workers_by_stage=workers_by_stage,
        event_sink=event_sink,
    )
    owned_tap = tap is None
    if tap is None:
        final_stage = manifest.stages[-1]
        tap = TerminalTap(
            completed_queue=final_stage.output_queue,
            run_id=manifest.run_id,
            expected_count=manifest.expected_jobs,
            event_sink=event_sink,
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

    if hasattr(event_sink, "close"):
        event_sink.close()

    time.sleep(0.5)


def peek_run_events(run_id: str) -> list[dict]:
    sink = create_event_sink()
    try:
        events = sink.read_by_run_id(run_id)
    finally:
        if hasattr(sink, "close"):
            sink.close()
    return [event.model_dump() for event in events]


def first_stage_input(manifest: BottleneckRunManifest) -> str:
    return manifest.stages[0].input_queue


def seed_manifest_jobs(
    manifest: BottleneckRunManifest,
    jobs: list[BottleneckJob],
) -> None:
    seed_jobs(
        queue_name=first_stage_input(manifest),
        jobs=[job.to_queue_job() for job in jobs],
        delivery_mode=PikaDeliveryMode.PERSISTENT,
    )


def spawn_stage_worker_process(
    *,
    manifest_path_arg: Path,
    stage: str,
    workers: int,
    replace: bool = True,
) -> subprocess.Popen[bytes]:
    cmd = [
        STAGE_WORKER_ENTRYPOINT,
        "--manifest",
        str(manifest_path_arg),
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
    manifest: BottleneckRunManifest,
    workers_by_stage: dict[str, int],
) -> list[subprocess.Popen[bytes]]:
    path = manifest_path(manifest.run_id)
    processes: list[subprocess.Popen[bytes]] = []
    for stage in reversed(manifest.stages):
        workers = workers_by_stage.get(stage.name, stage.default_workers)
        processes.append(
            spawn_stage_worker_process(
                manifest_path_arg=path,
                stage=stage.name,
                workers=workers,
                replace=True,
            ),
        )
    return processes


def format_worker_commands(manifest: BottleneckRunManifest) -> list[str]:
    prefix = f"{sys.executable} -m dr_bottleneck.cli.stage_worker"
    commands: list[str] = []
    for stage in manifest.stages:
        commands.append(
            f"{prefix} "
            f"--run-id {manifest.run_id} "
            f"--stage {stage.name} "
            f"--workers {stage.default_workers} "
            "--replace",
        )
    return commands


__all__ = [
    "create_event_sink",
    "format_worker_commands",
    "parse_workers_arg",
    "peek_run_events",
    "run_workflow_in_process",
    "seed_manifest_jobs",
    "setup_run_queues",
    "spawn_all_stage_workers",
    "spawn_stage_worker_process",
]
