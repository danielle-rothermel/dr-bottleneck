from collections.abc import Callable
from datetime import UTC, datetime
from threading import Event, Thread
from typing import Any

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic

from dr_queues.connection import delivery_tag, open_connection
from dr_queues.drain import add_to_drain, finalize_message, publish_job
from dr_queues.models import DrainEvent, DrainEventKind, JobEnvelope

JobHandler = Callable[[JobEnvelope], JobEnvelope]


def _log(stage: str, event: str, job: JobEnvelope) -> None:
    timestamp = datetime.now(tz=UTC).isoformat()
    print(
        f"{timestamp} stage={stage} event={event} "
        f"job_id={job.job_id} lane={job.lane}",
        flush=True,
    )


class WorkerPool:
    def __init__(
        self,
        *,
        input_queue: str,
        output_queue: str | None,
        handler: JobHandler,
        workers: int = 1,
        stage_name: str = "stage",
    ) -> None:
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.handler = handler
        self.workers = workers
        self.stage_name = stage_name
        self._stop = Event()
        self._threads: list[Thread] = []

    def start(self) -> None:
        for index in range(self.workers):
            thread = Thread(
                target=self._run_worker,
                args=(index,),
                daemon=True,
                name=f"worker-{self.stage_name}-{index}",
            )
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._stop.set()

    def join(self, timeout: float | None = None) -> None:
        for thread in self._threads:
            thread.join(timeout=timeout)

    def _run_worker(self, _index: int) -> None:
        connection = open_connection()
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.input_queue,
            on_message_callback=self._on_message,
            auto_ack=False,
        )
        while not self._stop.is_set():
            connection.process_data_events(time_limit=0.5)
        if channel.is_open:
            channel.close()
        if connection.is_open:
            connection.close()

    def _on_message(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        _properties: Any,
        body: bytes,
    ) -> None:
        if self._stop.is_set():
            channel.basic_nack(delivery_tag=delivery_tag(method), requeue=True)
            return

        job = JobEnvelope.from_json(body)
        _log(self.stage_name, "started", job)
        add_to_drain(
            channel,
            DrainEvent(
                run_id=job.run_id,
                job_id=job.job_id,
                lane=job.lane,
                stage=self.stage_name,
                event=DrainEventKind.STAGE_STARTED,
                payload={"step_index": job.step_index},
            ),
        )

        try:
            job = self.handler(job)
        except Exception:
            _log(self.stage_name, "failed", job)
            raise

        _log(self.stage_name, "completed", job)

        def publish_next(ch: BlockingChannel) -> None:
            if self.output_queue is not None:
                publish_job(ch, self.output_queue, job.to_json())

        step_execution = job.step_executions.get(self.stage_name)
        finalize_message(
            channel,
            method,
            drain_payload=DrainEvent(
                run_id=job.run_id,
                job_id=job.job_id,
                lane=job.lane,
                stage=self.stage_name,
                event=DrainEventKind.STAGE_OUTPUT,
                payload={
                    "step_index": job.step_index,
                    "step_outputs": job.step_outputs,
                    "step_execution": (
                        step_execution.model_dump()
                        if step_execution is not None
                        else None
                    ),
                },
            ),
            publish_fn=publish_next if self.output_queue else None,
        )
