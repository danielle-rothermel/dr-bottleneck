from __future__ import annotations

import signal
import time
from typing import Any

import typer
from dr_queues import HandlerRegistry, JobEnvelope, MongoRunStore
from dr_queues.amqp.session import broker_session
from dr_queues.amqp.topology import declare_durable_queue
from dr_queues.pipeline.workers import WorkerPool
from dr_queues.runtime.lifecycle import WorkerHeartbeat, register_worker
from dr_queues.runtime.models import WorkerRuntime

from dr_bottleneck.candidate_eval.evaluator import evaluate_candidate
from dr_bottleneck.candidate_eval.models import (
    CANDIDATE_EVAL_STAGE,
    DEFAULT_REQUEST_QUEUE,
    REQUEST_PAYLOAD_KEY,
    CandidateEvalRequest,
    CandidateEvalResult,
)

DEFAULT_WORKER_RUN_ID = "candidate-eval-workers"
HANDLERS_MODULE = "dr_bottleneck.candidate_eval.worker"

registry = HandlerRegistry()
app = typer.Typer(add_completion=False)


@registry.register(CANDIDATE_EVAL_STAGE)
def candidate_eval_handler(job: JobEnvelope) -> JobEnvelope:
    raw_request = job.payload.get(REQUEST_PAYLOAD_KEY, {})
    request = _request_or_none(raw_request)
    result_queue = _result_queue(raw_request, request)
    if result_queue:
        job.step_outputs["result_queue"] = result_queue

    try:
        if request is None:
            msg = (
                "Job payload does not contain a valid candidate eval request."
            )
            raise ValueError(msg)
        result = evaluate_candidate(request)
    except Exception as exc:
        result = CandidateEvalResult.failed(
            optimizer_run_id=_raw_text(
                raw_request,
                "optimizer_run_id",
                CandidateEvalResult.UNKNOWN_VALUE,
            ),
            candidate_id=_raw_text(
                raw_request,
                "candidate_id",
                CandidateEvalResult.UNKNOWN_VALUE,
            ),
            error=exc,
        )

    job.step_records[CANDIDATE_EVAL_STAGE] = result.model_dump(mode="json")
    job.step_outputs[CANDIDATE_EVAL_STAGE] = {
        "status": result.status,
        "candidate_id": result.candidate_id,
        "optimizer_run_id": result.optimizer_run_id,
        "bottleneck_run_id": result.bottleneck_run_id,
        "code_eval_run_id": result.code_eval_run_id,
    }
    return job


def _request_or_none(raw_request: Any) -> CandidateEvalRequest | None:
    try:
        return CandidateEvalRequest.model_validate(raw_request)
    except Exception:
        return None


def _result_queue(
    raw_request: Any,
    request: CandidateEvalRequest | None,
) -> str | None:
    if request is not None:
        return request.result_queue
    if isinstance(raw_request, dict):
        queue = raw_request.get("result_queue")
        if isinstance(queue, str) and queue:
            return queue
    return None


def _raw_text(raw_request: Any, key: str, default: str) -> str:
    if not isinstance(raw_request, dict):
        return default
    value = raw_request.get(key)
    if value is None:
        return default
    return str(value)


def _output_queue(job: JobEnvelope) -> str | None:
    queue = job.step_outputs.get("result_queue")
    if isinstance(queue, str) and queue:
        return queue
    raw_request = job.payload.get(REQUEST_PAYLOAD_KEY, {})
    return _result_queue(raw_request, _request_or_none(raw_request))


@app.command()
def main(
    request_queue: str = typer.Option(
        DEFAULT_REQUEST_QUEUE,
        "--request-queue",
    ),
    workers: int = typer.Option(1, "--workers"),
    worker_run_id: str = typer.Option(
        DEFAULT_WORKER_RUN_ID,
        "--worker-run-id",
    ),
) -> None:
    with broker_session() as broker:
        declare_durable_queue(broker.channel, request_queue)
    run_store = MongoRunStore()
    record = register_worker(
        run_store=run_store,
        run_id=worker_run_id,
        stage=CANDIDATE_EVAL_STAGE,
        concurrency=workers,
        runtime=WorkerRuntime.DETACHED,
        handlers_module=HANDLERS_MODULE,
    )
    pool = WorkerPool(
        input_queue=request_queue,
        output_queue=None,
        output_queue_for_job=_output_queue,
        handler=candidate_eval_handler,
        event_sink=run_store,
        workers=workers,
        stage_name=CANDIDATE_EVAL_STAGE,
        worker_id=record.worker_id,
    )
    heartbeat = WorkerHeartbeat(
        run_store=run_store,
        worker_id=record.worker_id,
        stop_worker=pool.stop,
    )
    typer.echo(
        f"worker_id={record.worker_id} stage={CANDIDATE_EVAL_STAGE} "
        f"workers={workers} input={request_queue}",
    )

    def _shutdown(_signum: int, _frame: object) -> None:
        typer.echo(f"Stopping {CANDIDATE_EVAL_STAGE} worker...")
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
