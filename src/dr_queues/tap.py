from threading import Event, Thread
from typing import Any

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic

from dr_queues.connection import delivery_tag, open_connection
from dr_queues.drain import add_to_drain
from dr_queues.models import DrainEvent, DrainEventKind, JobEnvelope


class TerminalTap:
    def __init__(
        self,
        *,
        completed_queue: str,
        run_id: str,
        expected_count: int,
    ) -> None:
        self.completed_queue = completed_queue
        self.run_id = run_id
        self.expected_count = expected_count
        self._stop = Event()
        self._thread: Thread | None = None
        self.terminal_count = 0
        self._done = Event()

    def start(self) -> None:
        self._thread = Thread(
            target=self._run,
            daemon=True,
            name="terminal-tap",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def join(self, timeout: float | None = None) -> None:
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def wait_for_completion(self, timeout: float | None = None) -> bool:
        if timeout is None:
            self._done.wait()
            return True
        return self._done.wait(timeout=timeout)

    def _run(self) -> None:
        connection = open_connection()
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.completed_queue,
            on_message_callback=self._on_message,
            auto_ack=False,
        )
        while not self._stop.is_set() and not self._done.is_set():
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
        job = JobEnvelope.from_json(body)
        if job.run_id != self.run_id:
            channel.basic_ack(delivery_tag=delivery_tag(method))
            return

        add_to_drain(
            channel,
            DrainEvent(
                run_id=job.run_id,
                job_id=job.job_id,
                lane=job.lane,
                stage="terminal",
                event=DrainEventKind.TERMINAL,
                payload=job.model_dump(),
            ),
        )
        channel.basic_ack(delivery_tag=delivery_tag(method))

        self.terminal_count += 1
        if self.terminal_count >= self.expected_count:
            self._done.set()
