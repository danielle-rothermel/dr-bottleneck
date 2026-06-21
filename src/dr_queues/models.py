from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class DrainEventKind(StrEnum):
    STAGE_STARTED = "stage_started"
    STAGE_OUTPUT = "stage_output"
    TERMINAL = "terminal"


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


class SampleInfo(BaseModel):
    task_id: str = ""
    prompt: str = ""
    canonical_solution: str = ""
    entry_point: str = ""


class JobMetadata(BaseModel):
    budget: int = 0


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
    sample: SampleInfo = Field(default_factory=SampleInfo)
    metadata: JobMetadata = Field(default_factory=JobMetadata)
    source_code: str = ""

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json(cls, payload: bytes) -> "JobEnvelope":
        return cls.model_validate_json(payload)


class DrainEvent(BaseModel):
    run_id: str
    job_id: str
    lane: str
    stage: str
    event: DrainEventKind
    timestamp: str = Field(
        default_factory=lambda: datetime.now(tz=UTC).isoformat(),
    )
    payload: dict[str, Any] = Field(default_factory=dict)

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json(cls, payload: bytes) -> "DrainEvent":
        return cls.model_validate_json(payload)


class WorkflowStepKind(StrEnum):
    LLM = "llm"
    PROCESS = "process"


class WorkflowStep(BaseModel):
    name: str
    kind: WorkflowStepKind = WorkflowStepKind.LLM
    prompt: str | None = None
    prompt_template: str | None = None
    handler: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)


class LaneStepProfile(BaseModel):
    profile: str | None = None


class WorkflowLane(BaseModel):
    id: str
    steps: list[LaneStepProfile]


class WorkflowConfig(BaseModel):
    id: str
    steps: list[WorkflowStep]
    lanes: list[WorkflowLane]


class RunStageManifest(BaseModel):
    name: str
    step_index: int
    input_queue: str
    output_queue: str
    default_workers: int


class RunManifest(BaseModel):
    run_id: str
    workflow_path: str
    profiles_path: str
    expected_jobs: int
    queue_prefix: str
    stages: list[RunStageManifest]


class StageQueues(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    prefix: str
    pending_name: str
    completed_name: str
