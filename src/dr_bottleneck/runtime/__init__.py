from __future__ import annotations

import sys
from pathlib import Path

from dr_queues import (
    JobEnvelope,
    MongoRunStore,
    Pipeline,
    RunManifest,
    get_run_status,
    parse_workers_arg,
    run_in_process,
    seed_run,
    start_stage_workers,
    stop_workers,
    wait_for_run,
)
from dr_queues import setup_run_queues as setup_dr_queues_run

from dr_bottleneck.handlers.queue import registry
from dr_bottleneck.workflow.engine import Workflow

HANDLERS_MODULE = "dr_bottleneck.handlers.queue"
RUNNER_QUEUE_PREFIX = "bottleneck"


def build_pipeline(workflow: Workflow) -> Pipeline:
    return Pipeline(workflow.to_pipeline_definition(), registry)


def setup_bottleneck_run(
    *,
    workflow: Workflow,
    run_id: str,
    workers_by_stage: dict[str, int],
    workflow_path: Path,
    profiles_path: Path,
    run_store: MongoRunStore | None = None,
    overwrite: bool = False,
) -> RunManifest:
    return setup_dr_queues_run(
        pipeline=build_pipeline(workflow),
        run_id=run_id,
        workers_by_stage=workers_by_stage,
        queue_prefix=f"{RUNNER_QUEUE_PREFIX}.{run_id}",
        run_store=run_store,
        overwrite=overwrite,
        metadata=workflow.metadata(
            workflow_path=workflow_path,
            profiles_path=profiles_path,
        ),
    )


def seed_bottleneck_run(
    manifest: RunManifest,
    jobs: list[JobEnvelope],
    *,
    run_store: MongoRunStore | None = None,
) -> None:
    seed_run(manifest, jobs, run_store=run_store)


def run_bottleneck_in_process(
    *,
    manifest: RunManifest,
    workflow: Workflow,
    workers_by_stage: dict[str, int],
    completion_timeout: float,
    run_store: MongoRunStore | None = None,
) -> None:
    run_in_process(
        manifest=manifest,
        pipeline=build_pipeline(workflow),
        workers_by_stage=workers_by_stage,
        run_store=run_store,
        completion_timeout=completion_timeout,
        handlers_module=HANDLERS_MODULE,
    )


def read_run_events(
    run_id: str,
    *,
    run_store: MongoRunStore | None = None,
) -> list[dict]:
    store = run_store or MongoRunStore()
    close_store = run_store is None
    try:
        return [
            event.model_dump(mode="json")
            for event in store.read_by_run_id(run_id)
        ]
    finally:
        if close_store:
            store.close()


def format_worker_commands(manifest: RunManifest) -> list[str]:
    prefix = f"{sys.executable} -m dr_queues.cli.stage_worker"
    return [
        f"{prefix} --run-id {manifest.run_id} --stage {stage.name} "
        f"--workers {stage.default_workers} --handlers-module {HANDLERS_MODULE}"
        for stage in manifest.stages
    ]


def start_bottleneck_workers(
    *,
    manifest: RunManifest,
    workers_by_stage: dict[str, int],
) -> list[int]:
    pids: list[int] = []
    for stage in reversed(manifest.stages):
        workers = workers_by_stage.get(stage.name, stage.default_workers)
        process = start_stage_workers(
            run_id=manifest.run_id,
            stage=stage.name,
            workers=workers,
            handlers_module=HANDLERS_MODULE,
        )
        pids.append(process.pid)
    return pids


__all__ = [
    "HANDLERS_MODULE",
    "RUNNER_QUEUE_PREFIX",
    "build_pipeline",
    "format_worker_commands",
    "get_run_status",
    "parse_workers_arg",
    "read_run_events",
    "run_bottleneck_in_process",
    "seed_bottleneck_run",
    "setup_bottleneck_run",
    "start_bottleneck_workers",
    "stop_workers",
    "wait_for_run",
]
