"""RabbitMQ worker for concrete workflow jobs."""

from __future__ import annotations

import signal
import time

import typer
from dr_queues import HandlerRegistry, JobEnvelope, MongoRunStore
from dr_queues.amqp.session import broker_session
from dr_queues.amqp.topology import declare_durable_queue
from dr_queues.pipeline.workers import WorkerPool
from dr_queues.runtime.lifecycle import WorkerHeartbeat, register_worker
from dr_queues.runtime.models import WorkerRuntime

from dr_bottleneck.workflow_jobs.runtime import (
    WORKFLOW_JOB_STAGE,
    output_queue_for_job,
    run_workflow_job_step,
)

DEFAULT_WORKFLOW_QUEUE = "dr-bottleneck.workflow.jobs"
DEFAULT_WORKER_RUN_ID = "workflow-job-workers"
HANDLERS_MODULE = "dr_bottleneck.workflow_jobs.worker"

registry = HandlerRegistry()
app = typer.Typer(add_completion=False)


@registry.register(WORKFLOW_JOB_STAGE)
def workflow_job_handler(job: JobEnvelope) -> JobEnvelope:
    """Execute the current concrete workflow-job step."""
    return run_workflow_job_step(job)


@app.command()
def main(
    input_queue: str = typer.Option(
        DEFAULT_WORKFLOW_QUEUE,
        "--input-queue",
    ),
    workers: int = typer.Option(1, "--workers"),
    worker_run_id: str = typer.Option(
        DEFAULT_WORKER_RUN_ID,
        "--worker-run-id",
    ),
) -> None:
    with broker_session() as broker:
        declare_durable_queue(broker.channel, input_queue)
    run_store = MongoRunStore()
    record = register_worker(
        run_store=run_store,
        run_id=worker_run_id,
        stage=WORKFLOW_JOB_STAGE,
        concurrency=workers,
        runtime=WorkerRuntime.DETACHED,
        handlers_module=HANDLERS_MODULE,
    )
    pool = WorkerPool(
        input_queue=input_queue,
        output_queue=None,
        output_queue_for_job=output_queue_for_job,
        handler=workflow_job_handler,
        event_sink=run_store,
        workers=workers,
        stage_name=WORKFLOW_JOB_STAGE,
        worker_id=record.worker_id,
    )
    heartbeat = WorkerHeartbeat(
        run_store=run_store,
        worker_id=record.worker_id,
        stop_worker=pool.stop,
    )
    typer.echo(
        f"worker_id={record.worker_id} stage={WORKFLOW_JOB_STAGE} "
        f"workers={workers} input={input_queue}",
    )

    def _shutdown(_signum: int, _frame: object) -> None:
        typer.echo(f"Stopping {WORKFLOW_JOB_STAGE} worker...")
        pool.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    heartbeat.start()
    pool.start()
    try:
        while not pool.is_stopped:
            time.sleep(0.5)
    finally:
        heartbeat.stop()
        pool.stop()
        pool.join(timeout=5)
        run_store.mark_worker_stopped(record.worker_id)
        run_store.close()


def run() -> None:
    app()


if __name__ == "__main__":
    run()
