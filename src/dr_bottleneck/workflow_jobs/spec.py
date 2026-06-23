"""Public queue-facing workflow job schemas."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Any, Literal

from dr_providers import ReasoningSpec, SamplingControls
from dr_providers.names import EffortLevel
from pydantic import BaseModel, ConfigDict, Field

SAMPLING_CONFIG_ID_RE = re.compile(
    r"^(?P<provider>[a-z][a-z0-9_-]*)\."
    r"(?P<model>[a-zA-Z0-9_.-]+(?:__[a-zA-Z0-9_.-]+)*)\."
    r"reasoning-(?P<reasoning>off|low|medium|high)\."
    r"temp-(?P<temperature>[0-9]+p[0-9]+)\."
    r"top-p-(?P<top_p>[0-9]+p[0-9]+)\."
    r"v(?P<version>[0-9]+)$"
)


class StrictBaseModel(BaseModel):
    """Base model for stable workflow job contracts."""

    model_config = ConfigDict(extra="forbid")


class JobKind(StrEnum):
    """Executable workflow job kinds."""

    LLM_QUERY_STATIC = "llm_query_static"
    LLM_QUERY_FROM_PREVIOUS = "llm_query_from_previous"
    EVAL_FROM_PREVIOUS = "eval_from_previous"


class WorkflowFailureClass(StrEnum):
    """Workflow failure classes used by execution and scoring."""

    INFRA_NON_RETRYABLE = "infra_non_retryable"
    INFRA_RETRYABLE = "infra_retryable"
    MODEL_OR_EVAL_OUTCOME = "model_or_eval_outcome"


class LLMQueryStaticConfig(StrictBaseModel):
    """Send a static prompt to the configured model."""

    metadata: dict[str, Any] = Field(default_factory=dict)
    model_id: str
    prompt: str


class LLMQueryFromPreviousConfig(StrictBaseModel):
    """Render a prompt from the previous step output and query an LLM."""

    metadata: dict[str, Any] = Field(default_factory=dict)
    model_id: str
    prompt_template: str
    placeholder: str


class EvalFromPreviousConfig(StrictBaseModel):
    """Evaluate previous LLM output as generated code."""

    metadata: dict[str, Any] = Field(default_factory=dict)
    suite: Literal["humaneval_plus"]
    task_id: str
    decoder_input: str


StepConfig = (
    LLMQueryStaticConfig | LLMQueryFromPreviousConfig | EvalFromPreviousConfig
)


class WorkflowStepSpec(StrictBaseModel):
    """One executable step and its queue topology."""

    name: str
    job_kind: JobKind
    input_queue: str
    output_queue: str | None = None


class WorkflowJobPayload(StrictBaseModel):
    """Concrete workflow job submitted inside a JobEnvelope payload."""

    workflow_id: str
    steps: tuple[WorkflowStepSpec, ...]
    step_configs: dict[str, StepConfig]
    metadata: dict[str, Any] = Field(default_factory=dict)


class LLMQueryOutput(StrictBaseModel):
    """Canonical LLM step output."""

    output_text: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class CandidateFunction(StrictBaseModel):
    """Top-level generated function considered during eval."""

    name: str
    positional_arity: int


class EvalFromPreviousOutput(StrictBaseModel):
    """Canonical eval step output."""

    metadata: dict[str, Any] = Field(default_factory=dict)
    parse_success: bool
    test_pass_rate: float
    all_tests_passed: bool
    selected_function_name: str | None
    candidate_functions: tuple[CandidateFunction, ...]
    expected_entry_point_present: bool
    failure_bucket: str | None = None


class ParsedSamplingConfig(StrictBaseModel):
    """Typed provider request settings parsed from a SamplingConfigId."""

    original_id: str
    provider: Literal["openrouter"]
    model: str
    reasoning: ReasoningSpec | None
    sampling: SamplingControls
    version: Literal[0]


class SamplingConfigId(str):
    """Versioned model/sampling setup id."""

    __slots__ = ()

    @classmethod
    def parse(cls, value: str) -> ParsedSamplingConfig:
        match = SAMPLING_CONFIG_ID_RE.match(value)
        if match is None:
            msg = f"Invalid sampling config id: {value!r}"
            raise ValueError(msg)
        groups = match.groupdict()
        provider = groups["provider"]
        if provider != "openrouter":
            msg = f"Unsupported provider in sampling config id: {provider!r}"
            raise ValueError(msg)
        version = int(groups["version"])
        if version != 0:
            msg = f"Unsupported sampling config id version: {version}"
            raise ValueError(msg)
        return ParsedSamplingConfig(
            original_id=value,
            provider="openrouter",
            model=groups["model"].replace("__", "/"),
            reasoning=_parse_reasoning(groups["reasoning"]),
            sampling=SamplingControls(
                temperature=_parse_decimal(groups["temperature"]),
                top_p=_parse_decimal(groups["top_p"]),
            ),
            version=0,
        )


def _parse_reasoning(value: str) -> ReasoningSpec | None:
    if value == "off":
        return ReasoningSpec(enabled=False)
    return ReasoningSpec(effort=EffortLevel(value))


def _parse_decimal(value: str) -> float:
    return float(value.replace("p", ".", 1))
