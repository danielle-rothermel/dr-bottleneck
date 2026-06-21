from pathlib import Path
from typing import Any

import yaml

from dr_providers.client import assistant_text, call_llm
from dr_providers.openrouter import Message
from dr_queues.models import JobEnvelope, StepExecution, WorkflowConfig


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
        workers: int,
        workflow_path: Path,
        profiles_path: Path,
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "workflow_id": self.config.id,
            "workflow_path": str(workflow_path),
            "profiles_path": str(profiles_path),
            "repeats": repeats,
            "workers": workers,
            "steps": [step.model_dump() for step in self.config.steps],
            "lanes": [lane.model_dump() for lane in self.config.lanes],
        }

    def _lane_profile(self, lane_id: str, step_index: int) -> str:
        for lane in self.config.lanes:
            if lane.id == lane_id:
                return lane.steps[step_index].profile
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

    def _build_prompt(self, step_index: int, job: JobEnvelope) -> str:
        step = self.config.steps[step_index]
        if step.prompt is not None:
            return step.prompt
        if step.prompt_template is None:
            msg = f"Step {step.name} has no prompt configured."
            raise ValueError(msg)

        if step_index == 0:
            msg = "prompt_template requires a previous step."
            raise ValueError(msg)

        prev_step = self.config.steps[step_index - 1]
        prev_output = job.step_outputs.get(prev_step.name, "")
        return step.prompt_template.format(prev_output=prev_output)

    def make_handler(self, step_index: int):
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
