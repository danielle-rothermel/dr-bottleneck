from __future__ import annotations

from pathlib import Path

import typer
from dr_providers import Message, MessageRole, ReasoningSpec, SamplingControls
from dr_providers.names import EffortLevel

from dr_bottleneck.llm import assistant_text, call_llm
from dr_bottleneck.workflow.config import WorkflowConfig
from dr_bottleneck.workflow.engine import Workflow

DEFAULT_PROFILES_PATH = Path("configs/openrouter_profiles.yaml")

app = typer.Typer(add_completion=False)


def _profile_resolver(profiles_path: Path) -> Workflow:
    return Workflow(
        WorkflowConfig(id="query", steps=[], lanes=[]),
        profiles_path,
    )


def resolve_call_args(
    *,
    profile: str | None,
    model: str | None,
    temperature: float,
    top_p: float,
    reasoning_disabled: bool,
    effort: str | None,
    profiles_path: Path,
) -> tuple[str, SamplingControls, ReasoningSpec | None, str | None]:
    if profile is None:
        if model is None:
            msg = "Provide --model or --profile."
            raise typer.BadParameter(msg)
        reasoning = None
        if reasoning_disabled:
            reasoning = ReasoningSpec(enabled=False)
        elif effort is not None:
            reasoning = ReasoningSpec(effort=EffortLevel(effort.lower()))
        return (
            model,
            SamplingControls(temperature=temperature, top_p=top_p),
            reasoning,
            None,
        )

    resolved = _profile_resolver(profiles_path).resolve_profile(profile)
    return (
        resolved.model,
        (
            resolved.sampling
            or SamplingControls(temperature=temperature, top_p=top_p)
        ),
        resolved.reasoning,
        profile,
    )


@app.command()
def main(
    message: str = typer.Option(..., "--message", "-m"),
    temperature: float = typer.Option(0.7, "--temperature"),
    top_p: float = typer.Option(0.95, "--top-p"),
    model: str | None = typer.Option(None, "--model"),
    profile: str | None = typer.Option(None, "--profile"),
    reasoning_disabled: bool = typer.Option(
        False,
        "--reasoning-disabled",
    ),
    effort: str | None = typer.Option(None, "--effort"),
    profiles_path: Path = typer.Option(
        DEFAULT_PROFILES_PATH,
        "--profiles-path",
    ),
) -> None:
    try:
        resolved_model, sampling, reasoning, profile_name = resolve_call_args(
            profile=profile,
            model=model,
            temperature=temperature,
            top_p=top_p,
            reasoning_disabled=reasoning_disabled,
            effort=effort,
            profiles_path=profiles_path,
        )
    except typer.BadParameter as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    messages = [Message(role=MessageRole.USER, content=message)]

    try:
        record = call_llm(
            model=resolved_model,
            messages=messages,
            sampling=sampling,
            reasoning=reasoning,
            profile=profile_name,
        )
    except Exception as exc:
        typer.echo(f"LLM call failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(assistant_text(record["response"]))
    typer.echo(
        "Stored LLM call in MongoDB (dr_bottleneck.llm_calls). "
        "See MONGODB_QUICKSTART.md.",
    )


if __name__ == "__main__":
    app()
