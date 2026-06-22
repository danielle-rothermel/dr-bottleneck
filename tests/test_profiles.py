from __future__ import annotations

from pathlib import Path

from dr_providers import ReasoningSpec, SamplingControls

from dr_bottleneck.workflow import Workflow


def test_workflow_resolves_profile_to_provider_controls(
    tmp_path: Path,
) -> None:
    profiles_path = tmp_path / "profiles.yaml"
    profiles_path.write_text(
        """
defaults:
  temperature: 0.2
  top_p: 0.8
profiles:
  openrouter/demo/model/low/v1:
    model: demo/model
    effort: low
""",
        encoding="utf-8",
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        """
id: demo
steps:
  - name: step
    prompt: hi
lanes:
  - id: lane
    steps:
      - profile: openrouter/demo/model/low/v1
""",
        encoding="utf-8",
    )

    workflow = Workflow.from_yaml(
        workflow_path,
        profiles_path=profiles_path,
    )
    profile = workflow.resolve_profile("openrouter/demo/model/low/v1")

    assert profile.model == "demo/model"
    assert profile.reasoning == ReasoningSpec(effort="low")
    assert profile.sampling == SamplingControls(temperature=0.2, top_p=0.8)
