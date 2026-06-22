from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml
from dr_providers import Message, MessageRole, ReasoningSpec, SamplingControls
from dr_providers.names import EffortLevel
from dr_queues import (
    JobEnvelope,
    PipelineDefinition,
    PipelineLane,
    PipelineStep,
)
from pydantic import BaseModel

from dr_bottleneck.job import (
    BottleneckPayload,
    LlmStepRecord,
    make_job_envelope,
    payload_from_job,
)
from dr_bottleneck.llm.client import call_llm
from dr_bottleneck.workflow.config import WorkflowConfig

LLM_HANDLER_KEY = "llm_step"
DEFAULT_PROFILES_PATH = Path("configs/openrouter_profiles.yaml")
PROFILE_DEFAULTS_KEY = "defaults"
PROFILES_KEY = "profiles"
PROFILE_MODEL_KEY = "model"
PROFILE_REASONING_DISABLED_KEY = "reasoning_disabled"
PROFILE_EFFORT_KEY = "effort"
DEFAULT_TEMPERATURE_KEY = "temperature"
DEFAULT_TOP_P_KEY = "top_p"


class ResolvedProfile(BaseModel):
    profile_id: str
    model: str
    reasoning: ReasoningSpec | None = None
    sampling: SamplingControls | None = None


class Workflow:
    def __init__(
        self,
        config: WorkflowConfig,
        profiles_path: Path,
    ) -> None:
        self.config = config
        self._profiles_path = profiles_path
        self._profiles_data = self._load_profiles()

    @classmethod
    def from_yaml(
        cls,
        path: Path,
        *,
        profiles_path: Path | None = None,
    ) -> Workflow:
        with path.open(encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
        return cls.from_raw_config(
            raw,
            profiles_path=profiles_path or DEFAULT_PROFILES_PATH,
        )

    @classmethod
    def from_raw_config(
        cls,
        raw: dict[str, Any],
        *,
        profiles_path: Path,
    ) -> Workflow:
        config = WorkflowConfig.model_validate(raw)
        return cls(config, profiles_path)

    def metadata(
        self,
        *,
        workflow_path: Path,
        profiles_path: Path,
    ) -> dict[str, Any]:
        return {
            "workflow_config": self.config.model_dump(mode="json"),
            "workflow_path": str(workflow_path),
            "profiles_path": str(profiles_path),
        }

    def _load_profiles(self) -> dict[str, Any]:
        with self._profiles_path.open(encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle)
        if not isinstance(loaded, dict):
            msg = f"Invalid profiles file: {self._profiles_path}"
            raise ValueError(msg)
        return loaded

    def to_pipeline_definition(self) -> PipelineDefinition:
        return PipelineDefinition(
            id=self.config.id,
            steps=[
                PipelineStep(name=step.name, handler_key=LLM_HANDLER_KEY)
                for step in self.config.steps
            ],
            lanes=[PipelineLane(id=lane.id) for lane in self.config.lanes],
        )

    def lane_ids(self) -> list[str]:
        return [lane.id for lane in self.config.lanes]

    def step_names(self) -> list[str]:
        return [step.name for step in self.config.steps]

    def make_seed_jobs(
        self,
        *,
        run_id: str,
        repeats: int,
    ) -> list[JobEnvelope]:
        jobs: list[JobEnvelope] = []
        for lane in self.config.lanes:
            for repeat in range(repeats):
                jobs.append(
                    make_job_envelope(
                        run_id=run_id,
                        lane=lane.id,
                        repeat=repeat,
                        pipeline_id=self.config.id,
                        payload=BottleneckPayload(),
                    )
                )
        return jobs

    def expected_job_count(self, repeats: int) -> int:
        return len(self.config.lanes) * repeats

    def step_name(self, step_index: int) -> str:
        return self.config.steps[step_index].name

    def run_config(
        self,
        *,
        run_id: str,
        repeats: int,
        workers_by_stage: dict[str, int],
        workflow_path: Path,
        profiles_path: Path,
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "workflow_id": self.config.id,
            "workflow_path": str(workflow_path),
            "profiles_path": str(profiles_path),
            "repeats": repeats,
            "workers_by_stage": workers_by_stage,
            "steps": [
                step.model_dump(mode="json") for step in self.config.steps
            ],
            "lanes": [
                lane.model_dump(mode="json") for lane in self.config.lanes
            ],
        }

    def _lane_profile(self, lane_id: str, step_index: int) -> str:
        for lane in self.config.lanes:
            if lane.id == lane_id:
                profile = lane.steps[step_index].profile
                if profile is None:
                    msg = f"Lane {lane_id} step {step_index} has no profile."
                    raise ValueError(msg)
                return profile
        msg = f"Unknown lane: {lane_id}"
        raise ValueError(msg)

    def resolve_profile(self, profile_name: str) -> ResolvedProfile:
        profiles = self._profiles_data.get(PROFILES_KEY, {})
        if not isinstance(profiles, dict) or profile_name not in profiles:
            msg = f"Unknown profile: {profile_name}"
            raise ValueError(msg)

        profile_config = profiles[profile_name]
        if not isinstance(profile_config, dict):
            msg = f"Invalid profile config for {profile_name}"
            raise ValueError(msg)
        model = profile_config.get(PROFILE_MODEL_KEY)
        if not isinstance(model, str) or not model:
            msg = f"Profile {profile_name} is missing model."
            raise ValueError(msg)

        return ResolvedProfile(
            profile_id=profile_name,
            model=model,
            reasoning=_reasoning_from_profile(profile_config),
            sampling=_sampling_from_config(self._profiles_data),
        )

    def _prompt_context(
        self,
        step_index: int,
        job: JobEnvelope,
    ) -> dict[str, str]:
        payload = payload_from_job(job)
        ctx: dict[str, str] = {
            "source_code": payload.source_code,
            "budget": str(payload.metadata.get("budget", "")),
            "task_id": str(payload.sample.get("task_id", "")),
            "entry_point": str(payload.sample.get("entry_point", "")),
            "prompt": str(payload.sample.get("prompt", "")),
            "canonical_solution": str(
                payload.sample.get("canonical_solution", "")
            ),
        }
        for index, step in enumerate(self.config.steps[:step_index]):
            output = job.step_outputs.get(step.name, "")
            ctx[step.name] = str(output)
            ctx[f"{step.name}_output"] = str(output)
            if index == step_index - 1:
                ctx["prev_output"] = str(output)
        return ctx

    def build_prompt(self, step_index: int, job: JobEnvelope) -> str:
        step = self.config.steps[step_index]
        if step.prompt is not None:
            return step.prompt
        if step.prompt_template is None:
            msg = f"Step {step.name} has no prompt configured."
            raise ValueError(msg)

        ctx = defaultdict(str, self._prompt_context(step_index, job))
        return step.prompt_template.format_map(ctx)

    def run_llm_step(self, job: JobEnvelope) -> JobEnvelope:
        step_index = job.step_index
        step = self.config.steps[step_index]
        profile_name = self._lane_profile(job.lane, step_index)
        resolved = self.resolve_profile(profile_name)
        prompt = self.build_prompt(step_index, job)
        messages = [Message(role=MessageRole.USER, content=prompt)]

        record = call_llm(
            model=resolved.model,
            messages=messages,
            reasoning=resolved.reasoning,
            sampling=resolved.sampling,
            profile=resolved.profile_id,
            run_id=job.run_id,
            job_id=job.job_id,
            metadata={"step": step.name, "step_index": step_index},
        )
        text = str(record["assistant_text"])
        job.step_outputs[step.name] = text
        job.step_records[step.name] = LlmStepRecord(
            step_index=step_index,
            name=step.name,
            profile=resolved.profile_id,
            model=resolved.model,
            prompt=prompt,
            messages=[message.model_dump(mode="json") for message in messages],
            request=record["request"],
            response=record["response"],
            assistant_text=text,
            latency_ms=int(record["latency_ms"]),
            timestamp=str(record["timestamp"]),
        ).to_step_record()
        return job


def _reasoning_from_profile(
    profile_config: dict[str, Any],
) -> ReasoningSpec | None:
    if profile_config.get(PROFILE_REASONING_DISABLED_KEY):
        return ReasoningSpec(enabled=False)
    effort = profile_config.get(PROFILE_EFFORT_KEY)
    if effort is not None:
        return ReasoningSpec(effort=EffortLevel(str(effort).lower()))
    return None


def _sampling_from_config(config: dict[str, Any]) -> SamplingControls | None:
    defaults = config.get(PROFILE_DEFAULTS_KEY, {})
    if not isinstance(defaults, dict):
        return None
    temperature = defaults.get(DEFAULT_TEMPERATURE_KEY)
    top_p = defaults.get(DEFAULT_TOP_P_KEY)
    if temperature is None and top_p is None:
        return None
    return SamplingControls(
        temperature=float(temperature) if temperature is not None else None,
        top_p=float(top_p) if top_p is not None else None,
    )


__all__ = [
    "DEFAULT_PROFILES_PATH",
    "LLM_HANDLER_KEY",
    "ResolvedProfile",
    "Workflow",
]
