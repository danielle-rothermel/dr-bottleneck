import random
from pathlib import Path

import typer

from dr_queues.humaneval_data import (
    DEFAULT_BUDGETS,
    load_humanevalplus,
    make_preview_job,
)
from dr_queues.workflow import Workflow

DEFAULT_WORKFLOW = Path("configs/workflows/humaneval_encode_decode.yaml")
DEFAULT_ENCODE_PLACEHOLDER = "⟨encode model output⟩"

app = typer.Typer(add_completion=False)


def _parse_budgets(value: str) -> list[int]:
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def _format_section(title: str, body: str) -> str:
    return f"### {title}\n\n```\n{body.rstrip()}\n```\n"


@app.command()
def main(
    samples: int = typer.Option(3, "--samples", "-n"),
    seed: int | None = typer.Option(None, "--seed"),
    budgets: str = typer.Option(",".join(str(b) for b in DEFAULT_BUDGETS)),
    workflow_path: Path = typer.Option(DEFAULT_WORKFLOW, "--workflow"),
    profiles_path: Path = typer.Option(
        Path("configs/openrouter_profiles.yaml"),
        "--profiles-path",
    ),
    encode_placeholder: str = typer.Option(
        DEFAULT_ENCODE_PLACEHOLDER,
        "--encode-placeholder",
    ),
) -> None:
    if samples < 1:
        typer.echo("--samples must be at least 1.", err=True)
        raise typer.Exit(code=1)

    budget_options = _parse_budgets(budgets)
    if not budget_options:
        typer.echo("No budgets provided.", err=True)
        raise typer.Exit(code=1)

    rng = random.Random(seed)
    budget = rng.choice(budget_options)

    workflow = Workflow.from_yaml(
        workflow_path,
        profiles_path=profiles_path,
    )
    encode_index = workflow.step_names().index("encode")
    decode_index = workflow.step_names().index("decode")

    tasks = load_humanevalplus()
    chosen = rng.sample(tasks, k=min(samples, len(tasks)))

    sections: list[str] = []
    for task in chosen:
        job = make_preview_job(
            task=task,
            budget=budget,
            workflow_id=workflow.config.id,
        )
        encode_prompt = workflow.build_prompt(encode_index, job)

        decode_job = make_preview_job(
            task=task,
            budget=budget,
            workflow_id=workflow.config.id,
            encode_output=encode_placeholder,
        )
        decode_prompt = workflow.build_prompt(decode_index, decode_job)

        sections.append(
            "\n".join(
                [
                    f"## {task['task_id']}",
                    "",
                    f"Budget: {budget}",
                    "",
                    _format_section("Ground Truth", job.source_code),
                    _format_section("Encoder Prompt", encode_prompt),
                    _format_section("Decoder Prompt", decode_prompt),
                ],
            ),
        )

    typer.echo("\n\n".join(sections))


if __name__ == "__main__":
    app()
