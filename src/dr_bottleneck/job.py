from __future__ import annotations

from typing import Any

from dr_queues import JobEnvelope
from pydantic import BaseModel, Field

PAYLOAD_SAMPLE_KEY = "sample"
PAYLOAD_METADATA_KEY = "metadata"
PAYLOAD_SOURCE_CODE_KEY = "source_code"
STEP_RECORD_TYPE_KEY = "type"
LLM_STEP_RECORD_TYPE = "llm"


class BottleneckPayload(BaseModel):
    sample: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    source_code: str = ""

    def to_job_payload(self) -> dict[str, Any]:
        return {
            PAYLOAD_SAMPLE_KEY: self.sample,
            PAYLOAD_METADATA_KEY: self.metadata,
            PAYLOAD_SOURCE_CODE_KEY: self.source_code,
        }


class LlmStepRecord(BaseModel):
    step_index: int
    name: str
    profile: str
    model: str
    prompt: str
    messages: list[dict[str, str]]
    request: dict[str, Any]
    response: dict[str, Any]
    assistant_text: str
    latency_ms: int
    timestamp: str

    def to_step_record(self) -> dict[str, Any]:
        return {
            STEP_RECORD_TYPE_KEY: LLM_STEP_RECORD_TYPE,
            **self.model_dump(mode="json"),
        }


def payload_from_job(job: JobEnvelope) -> BottleneckPayload:
    return BottleneckPayload(
        sample=dict(job.payload.get(PAYLOAD_SAMPLE_KEY, {})),
        metadata=dict(job.payload.get(PAYLOAD_METADATA_KEY, {})),
        source_code=str(job.payload.get(PAYLOAD_SOURCE_CODE_KEY, "")),
    )


def terminal_payload_to_job(payload: dict[str, Any]) -> JobEnvelope:
    return JobEnvelope.model_validate(payload)


def llm_step_record(job: JobEnvelope, step_name: str) -> LlmStepRecord:
    raw_record = job.step_records.get(step_name)
    if raw_record is None:
        msg = f"Job {job.job_id!r} has no step record for {step_name!r}."
        raise ValueError(msg)
    return LlmStepRecord.model_validate(
        {
            key: value
            for key, value in raw_record.items()
            if key != STEP_RECORD_TYPE_KEY
        }
    )


def make_job_envelope(
    *,
    run_id: str,
    lane: str,
    repeat: int,
    pipeline_id: str,
    payload: BottleneckPayload,
) -> JobEnvelope:
    return JobEnvelope(
        run_id=run_id,
        lane=lane,
        repeat=repeat,
        step_index=0,
        pipeline_id=pipeline_id,
        payload=payload.to_job_payload(),
    )


__all__ = [
    "LLM_STEP_RECORD_TYPE",
    "BottleneckPayload",
    "LlmStepRecord",
    "llm_step_record",
    "make_job_envelope",
    "payload_from_job",
    "terminal_payload_to_job",
]
