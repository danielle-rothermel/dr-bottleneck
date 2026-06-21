from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


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
