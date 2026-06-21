from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

from dr_providers.client import assistant_text, call_llm
from dr_providers.openrouter import Message
from dr_queues import process_handlers as _process_handlers
from dr_queues.models import (
    JobEnvelope,
    StepExecution,
    WorkflowConfig,
    WorkflowStepKind,
)


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
    ) -> "Workflow":
        with path.open(encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
        config = WorkflowConfig.model_validate(raw)
        resolved_profiles = profiles_path or Path(
            "configs/openrouter_profiles.yaml",
        )
        return cls(config, resolved_profiles)

    def _load_profiles(self) -> dict:
        with self._profiles_path.open(encoding="utf-8") as handle:
            return yaml.safe_load(handle)

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
                    JobEnvelope(
                        run_id=run_id,
                        lane=lane.id,
                        repeat=repeat,
                        step_index=0,
                        workflow_id=self.config.id,
                    ),
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
            "steps": [step.model_dump() for step in self.config.steps],
            "lanes": [lane.model_dump() for lane in self.config.lanes],
        }

    def _lane_profile(self, lane_id: str, step_index: int) -> str:
        for lane in self.config.lanes:
            if lane.id == lane_id:
                profile = lane.steps[step_index].profile
                if profile is None:
                    msg = (
                        f"Lane {lane_id} step {step_index} has no profile."
                    )
                    raise ValueError(msg)
                return profile
        msg = f"Unknown lane: {lane_id}"
        raise ValueError(msg)

    def _resolve_profile(self, profile_name: str) -> dict:
        profiles = self._profiles_data.get("profiles", {})
        if profile_name not in profiles:
            msg = f"Unknown profile: {profile_name}"
            raise ValueError(msg)

        profile_config = profiles[profile_name]
        defaults = self._profiles_data.get("defaults", {})
        return {
            "model": profile_config["model"],
            "temperature": defaults.get("temperature", 0.7),
            "top_p": defaults.get("top_p", 0.95),
            "reasoning_disabled": profile_config.get(
                "reasoning_disabled",
                False,
            ),
            "effort": profile_config.get("effort"),
            "profile_name": profile_name,
        }

    def _prompt_context(
        self,
        step_index: int,
        job: JobEnvelope,
    ) -> dict[str, str]:
        ctx: dict[str, str] = {
            "source_code": job.source_code,
            "budget": str(job.metadata.budget),
            "task_id": job.sample.task_id,
            "entry_point": job.sample.entry_point,
            "prompt": job.sample.prompt,
            "canonical_solution": job.sample.canonical_solution,
        }
        for index, step in enumerate(self.config.steps[:step_index]):
            output = job.step_outputs.get(step.name, "")
            ctx[step.name] = output
            ctx[f"{step.name}_output"] = output
            if index == step_index - 1:
                ctx["prev_output"] = output
        return ctx

    def _build_prompt(self, step_index: int, job: JobEnvelope) -> str:
        step = self.config.steps[step_index]
        if step.prompt is not None:
            return step.prompt
        if step.prompt_template is None:
            msg = f"Step {step.name} has no prompt configured."
            raise ValueError(msg)

        ctx = defaultdict(str, self._prompt_context(step_index, job))
        return step.prompt_template.format_map(ctx)

    def build_prompt(self, step_index: int, job: JobEnvelope) -> str:
        return self._build_prompt(step_index, job)

    def _make_llm_handler(self, step_index: int):
        step = self.config.steps[step_index]

        def handler(job: JobEnvelope) -> JobEnvelope:
            profile_name = self._lane_profile(job.lane, step_index)
            resolved = self._resolve_profile(profile_name)
            prompt = self._build_prompt(step_index, job)
            messages: list[Message] = [{"role": "user", "content": prompt}]

            record = call_llm(
                resolved["model"],
                messages,
                temperature=resolved["temperature"],
                top_p=resolved["top_p"],
                reasoning_disabled=resolved["reasoning_disabled"],
                effort=resolved["effort"],
                profile=resolved["profile_name"],
            )
            text = assistant_text(record["response"])
            job.step_outputs[step.name] = text
            job.step_executions[step.name] = StepExecution(
                step_index=step_index,
                name=step.name,
                profile=resolved["profile_name"],
                model=resolved["model"],
                temperature=resolved["temperature"],
                top_p=resolved["top_p"],
                reasoning_disabled=resolved["reasoning_disabled"],
                effort=resolved["effort"],
                prompt=prompt,
                messages=messages,
                request=record["request"],
                response=record["response"],
                assistant_text=text,
                latency_ms=record["latency_ms"],
                timestamp=record["timestamp"],
            )
            job.step_index = step_index + 1
            return job

        return handler

    def _make_process_handler(self, step_index: int):
        step = self.config.steps[step_index]
        if step.handler is None:
            msg = f"Process step {step.name} has no handler configured."
            raise ValueError(msg)

        handler_fn = _process_handlers.get_process_handler(step.handler)

        def handler(job: JobEnvelope) -> JobEnvelope:
            updated = handler_fn(job, step)
            updated.step_index = step_index + 1
            if step.name in updated.step_process_results:
                result = updated.step_process_results[step.name]
                updated.step_process_results[step.name] = result.model_copy(
                    update={"step_index": step_index},
                )
            return updated

        return handler

    def make_handler(self, step_index: int):
        step = self.config.steps[step_index]
        if step.kind == WorkflowStepKind.PROCESS:
            return self._make_process_handler(step_index)
        return self._make_llm_handler(step_index)
