from pathlib import Path

import typer
import yaml

from dr_providers import (
    MissingApiKeyError,
    append_record,
    assistant_text,
    call_llm,
    default_log_path,
)

DEFAULT_PROFILES_PATH = Path("configs/openrouter_profiles.yaml")

app = typer.Typer(add_completion=False)


def load_profiles(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def resolve_call_args(
    *,
    profile: str | None,
    model: str | None,
    temperature: float,
    top_p: float,
    reasoning_disabled: bool,
    effort: str | None,
    profiles_path: Path,
) -> tuple[str, float, float, bool, str | None, str | None]:
    profile_name = profile
    resolved_model = model
    resolved_temperature = temperature
    resolved_top_p = top_p
    resolved_reasoning_disabled = reasoning_disabled
    resolved_effort = effort

    if profile_name is not None:
        config = load_profiles(profiles_path)
        profiles = config.get("profiles", {})
        if profile_name not in profiles:
            msg = f"Unknown profile: {profile_name}"
            raise typer.BadParameter(msg)

        profile_config = profiles[profile_name]
        if resolved_model is None:
            resolved_model = profile_config.get("model")
        if profile_config.get("reasoning_disabled"):
            resolved_reasoning_disabled = True
        if (
            profile_config.get("effort") is not None
            and resolved_effort is None
        ):
            resolved_effort = profile_config.get("effort")

        defaults = config.get("defaults", {})
        if temperature == defaults.get("temperature", 0.7):
            resolved_temperature = defaults.get("temperature", temperature)
        if top_p == defaults.get("top_p", 0.95):
            resolved_top_p = defaults.get("top_p", top_p)

    if resolved_model is None:
        msg = "Provide --model or --profile."
        raise typer.BadParameter(msg)

    return (
        resolved_model,
        resolved_temperature,
        resolved_top_p,
        resolved_reasoning_disabled,
        resolved_effort,
        profile_name,
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
    log_file: Path | None = typer.Option(None, "--log-file"),
    profiles_path: Path = typer.Option(
        DEFAULT_PROFILES_PATH,
        "--profiles-path",
    ),
) -> None:
    try:
        (
            resolved_model,
            resolved_temperature,
            resolved_top_p,
            resolved_reasoning_disabled,
            resolved_effort,
            profile_name,
        ) = resolve_call_args(
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

    messages = [{"role": "user", "content": message}]

    try:
        record = call_llm(
            resolved_model,
            messages,
            temperature=resolved_temperature,
            top_p=resolved_top_p,
            reasoning_disabled=resolved_reasoning_disabled,
            effort=resolved_effort,
            profile=profile_name,
        )
    except MissingApiKeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    except Exception as exc:
        typer.echo(f"LLM call failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    append_record(log_file or default_log_path(), record)
    typer.echo(assistant_text(record["response"]))


if __name__ == "__main__":
    app()
