from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

from dr_queues.connection import (
    ChannelSession,
    PikaDeliveryMode,
)

type JobMetadata = Any
type SampleInfo = Any
type StepExecution = Any


class ProcessStepResult(BaseModel):
    step_index: int
    name: str
    handler: str
    result: dict[str, Any] = Field(default_factory=dict)
    timestamp: str = Field(
        default_factory=lambda: datetime.now(tz=UTC).isoformat(),
    )


class JobEnvelope(BaseModel):
    run_id: str
    job_id: str = Field(default_factory=lambda: str(uuid4()))
    lane: str
    repeat: int
    step_index: int = 0
    step_outputs: dict[str, str] = Field(default_factory=dict)
    step_executions: dict[str, StepExecution] = Field(default_factory=dict)
    step_process_results: dict[str, ProcessStepResult] = Field(
        default_factory=dict,
    )
    workflow_id: str
    sample: SampleInfo = Field(default_factory=dict)
    metadata: JobMetadata = Field(default_factory=dict)
    source_code: str = ""

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json(cls, payload: bytes) -> "JobEnvelope":
        return cls.model_validate_json(payload)


# TODO: it seems strange for seed_jobs to need to create its own session
# and therefore know what delivery mode we want.  Check if this is the
# right way.
def seed_jobs(
    *,
    queue_name: str,
    jobs: list[JobEnvelope],
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
) -> None:
    seed_session = ChannelSession.open_session(delivery_mode=delivery_mode)
    try:
        for job in jobs:
            seed_session.publish_job(
                queue_name=queue_name,
                body=job.to_json(),
            )
    finally:
        seed_session.close()
