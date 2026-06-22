from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class WorkflowStepKind(StrEnum):
    LLM = "llm"


class WorkflowStep(BaseModel):
    name: str
    kind: WorkflowStepKind = WorkflowStepKind.LLM
    prompt: str | None = None
    prompt_template: str | None = None


class LaneStepProfile(BaseModel):
    profile: str | None = None


class WorkflowLane(BaseModel):
    id: str
    steps: list[LaneStepProfile]


class WorkflowConfig(BaseModel):
    id: str
    steps: list[WorkflowStep]
    lanes: list[WorkflowLane]
