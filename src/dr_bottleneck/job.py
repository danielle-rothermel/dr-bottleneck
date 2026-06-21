from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from dr_queues.pipeline.job import JobEnvelope as QueueJobEnvelope
from pydantic import BaseModel, Field

StepHandler = Callable[[QueueJobEnvelope], QueueJobEnvelope]


class StepExecution(BaseModel):
    step_index: int
    name: str
    profile: str
    model: str
    temperature: float
    top_p: float
    reasoning_disabled: bool = False
    effort: str | None = None
    prompt: str
    messages: list[dict[str, str]]
    request: dict[str, Any]
    response: dict[str, Any]
    assistant_text: str
    latency_ms: int
    timestamp: str


class ProcessStepResult(BaseModel):
    step_index: int
    name: str
    handler: str
    result: dict[str, Any] = Field(default_factory=dict)
    timestamp: str = Field(
        default_factory=lambda: datetime.now(tz=UTC).isoformat(),
    )


class BottleneckJob(BaseModel):
    run_id: str
    job_id: str = Field(default_factory=lambda: str(uuid4()))
    lane: str
    repeat: int
    step_index: int = 0
    workflow_id: str
    step_outputs: dict[str, str] = Field(default_factory=dict)
    step_executions: dict[str, StepExecution] = Field(default_factory=dict)
    step_process_results: dict[str, ProcessStepResult] = Field(
        default_factory=dict,
    )
    sample: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    source_code: str = ""

    def to_queue_job(self) -> QueueJobEnvelope:
        step_records: dict[str, Any] = {}
        for name, execution in self.step_executions.items():
            step_records[name] = {
                "type": "llm",
                **execution.model_dump(),
            }
        for name, result in self.step_process_results.items():
            step_records[name] = {
                "type": "process",
                **result.model_dump(),
            }
        return QueueJobEnvelope(
            run_id=self.run_id,
            job_id=self.job_id,
            lane=self.lane,
            repeat=self.repeat,
            step_index=self.step_index,
            pipeline_id=self.workflow_id,
            payload={
                "sample": self.sample,
                "metadata": self.metadata,
                "source_code": self.source_code,
            },
            step_outputs=dict(self.step_outputs),
            step_records=step_records,
        )

    @classmethod
    def from_queue_job(cls, job: QueueJobEnvelope) -> BottleneckJob:
        step_executions: dict[str, StepExecution] = {}
        step_process_results: dict[str, ProcessStepResult] = {}
        for name, record in job.step_records.items():
            if record.get("type") == "process":
                step_process_results[name] = ProcessStepResult.model_validate(
                    {
                        key: value
                        for key, value in record.items()
                        if key != "type"
                    },
                )
            else:
                step_executions[name] = StepExecution.model_validate(
                    {
                        key: value
                        for key, value in record.items()
                        if key != "type"
                    },
                )

        payload = job.payload
        workflow_id = job.pipeline_id
        if "workflow_id" in payload:
            workflow_id = str(payload["workflow_id"])

        return cls(
            run_id=job.run_id,
            job_id=job.job_id,
            lane=job.lane,
            repeat=job.repeat,
            step_index=job.step_index,
            workflow_id=workflow_id,
            step_outputs={
                key: str(value) for key, value in job.step_outputs.items()
            },
            step_executions=step_executions,
            step_process_results=step_process_results,
            sample=dict(payload.get("sample", {})),
            metadata=dict(payload.get("metadata", {})),
            source_code=str(payload.get("source_code", "")),
        )


def adapt_handler(
    handler: Callable[[BottleneckJob], BottleneckJob],
) -> StepHandler:
    def wrapped(job: QueueJobEnvelope) -> QueueJobEnvelope:
        bottleneck_job = BottleneckJob.from_queue_job(job)
        updated = handler(bottleneck_job)
        return updated.to_queue_job()

    return wrapped


def terminal_payload_to_job_dict(payload: dict[str, Any]) -> dict[str, Any]:
    if "step_executions" in payload or "workflow_id" in payload:
        return payload

    queue_job = QueueJobEnvelope.model_validate(payload)
    job = BottleneckJob.from_queue_job(queue_job)
    return {
        "job_id": job.job_id,
        "lane": job.lane,
        "repeat": job.repeat,
        "workflow_id": job.workflow_id,
        "step_outputs": job.step_outputs,
        "step_executions": {
            name: execution.model_dump()
            for name, execution in job.step_executions.items()
        },
        "step_process_results": {
            name: result.model_dump()
            for name, result in job.step_process_results.items()
        },
        "sample": job.sample,
        "metadata": job.metadata,
        "source_code": job.source_code,
    }
