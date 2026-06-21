from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import typer
from dr_queues.events.schema import EventKind

from dr_bottleneck import (
    DEFAULT_BUDGETS,
    TerminalTap,
    Workflow,
    build_metrics_rows,
    build_run_report,
    create_event_sink,
    expand_experiment_jobs,
    filter_tasks,
    load_humanevalplus,
    manifest_path,
    parse_workers_arg,
    peek_run_events,
    persist_run_metrics,
    persist_run_report,
    run_workflow_in_process,
    seed_manifest_jobs,
    setup_run_queues,
    spawn_all_stage_workers,
    summarize_metrics,
    tiny_experiment_filters,
)
from dr_bottleneck.runtime import format_worker_commands
from dr_bottleneck.workflow import WorkflowStepKind

DEFAULT_WORKFLOW = Path("configs/workflows/humaneval_encode_decode.yaml")
DEFAULT_WORKERS = "encode=8,decode=8,evaluate=32"

app = typer.Typer(add_completion=False)


def _parse_budgets(value: str) -> list[int]:
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def _parse_task_ids(value: str | None) -> list[str] | None:
    if value is None:
        return None
    ids = [part.strip() for part in value.split(",") if part.strip()]
    return ids or None


def _mongo_hint(run_id: str) -> None:
    typer.echo(
        "Inspect results: see MONGODB_QUICKSTART.md "
        f"(run_id={run_id})",
    )


@app.command()
def main(
    tiny: bool = typer.Option(False, "--tiny"),
    repeats: int = typer.Option(1, "--repeats"),
    workers: str = typer.Option(DEFAULT_WORKERS, "--workers"),
    start_workers: bool = typer.Option(False, "--start-workers"),
    no_wait: bool = typer.Option(False, "--no-wait"),
    workflow_path: Path = typer.Option(DEFAULT_WORKFLOW, "--workflow"),
    profiles_path: Path = typer.Option(
        Path("configs/openrouter_profiles.yaml"),
        "--profiles-path",
    ),
    run_id: str | None = typer.Option(None, "--run-id"),
    completion_timeout: float = typer.Option(3600.0, "--completion-timeout"),
    budgets: str = typer.Option(",".join(str(b) for b in DEFAULT_BUDGETS)),
    task_ids: str | None = typer.Option(None, "--task-ids"),
) -> None:
    resolved_run_id = run_id or f"humaneval-{uuid4().hex[:8]}"
    workflow = Workflow.from_yaml(
        workflow_path,
        profiles_path=profiles_path,
    )
    workers_by_stage = parse_workers_arg(
        workers,
        workflow.step_names(),
        default=10,
    )

    if tiny:
        lane_ids, budget_list, tasks = tiny_experiment_filters(workflow)
        effective_start_workers = False
    else:
        all_tasks = load_humanevalplus()
        tasks = filter_tasks(all_tasks, _parse_task_ids(task_ids))
        lane_ids = workflow.lane_ids()
        budget_list = _parse_budgets(budgets)
        effective_start_workers = start_workers or not no_wait

    jobs = expand_experiment_jobs(
        workflow=workflow,
        run_id=resolved_run_id,
        tasks=tasks,
        budgets=budget_list,
        lane_ids=lane_ids,
        repeats=repeats,
    )
    expected = len(jobs)
    llm_calls = expected * sum(
        1
        for step in workflow.config.steps
        if step.kind == WorkflowStepKind.LLM
    )

    typer.echo(f"run_id={resolved_run_id} expected_jobs={expected}")
    typer.echo(f"estimated_llm_calls={llm_calls}")
    typer.echo(f"manifest={manifest_path(resolved_run_id)}")

    manifest = setup_run_queues(
        workflow=workflow,
        run_id=resolved_run_id,
        workers_by_stage=workers_by_stage,
        workflow_path=workflow_path,
        profiles_path=profiles_path,
        expected_jobs=expected,
    )

    event_sink = create_event_sink()
    final_stage = manifest.stages[-1]
    tap = TerminalTap(
        completed_queue=final_stage.output_queue,
        run_id=resolved_run_id,
        expected_count=expected,
        event_sink=event_sink,
    )

    if no_wait:
        seed_manifest_jobs(manifest, jobs)
        typer.echo(f"Seeded {len(jobs)} jobs.")
        if effective_start_workers:
            spawn_all_stage_workers(
                manifest=manifest,
                workers_by_stage=workers_by_stage,
            )
            typer.echo("Started detached stage workers.")
        else:
            typer.echo("Start workers with:")
            for command in format_worker_commands(manifest):
                typer.echo(f"  {command}")
        return

    tap.start()
    seed_manifest_jobs(manifest, jobs)
    typer.echo(f"Seeded {len(jobs)} jobs.")

    if tiny or not effective_start_workers:
        typer.echo("Running in-process workers...")
        run_workflow_in_process(
            manifest=manifest,
            workflow=workflow,
            workers_by_stage=workers_by_stage,
            completion_timeout=completion_timeout,
            event_sink=event_sink,
            tap=tap,
        )
    else:
        spawn_all_stage_workers(
            manifest=manifest,
            workers_by_stage=workers_by_stage,
        )
        typer.echo("Waiting for terminal events (detached workers)...")
        if not tap.wait_for_completion(timeout=completion_timeout):
            typer.echo("Timed out waiting for workflow completion.", err=True)
            raise typer.Exit(code=1)
        tap.stop()
        tap.join(timeout=5)
        if hasattr(event_sink, "close"):
            event_sink.close()

    run_events = peek_run_events(resolved_run_id)
    terminal_payloads = [
        event["payload"]
        for event in run_events
        if event.get("event") == EventKind.TERMINAL
    ]
    metrics_rows = build_metrics_rows(terminal_payloads)
    summary = summarize_metrics(metrics_rows)

    persist_run_metrics(
        run_id=resolved_run_id,
        rows=metrics_rows,
        summary=summary,
    )
    typer.echo(
        f"Stored metrics in MongoDB for run_id={resolved_run_id}",
    )
    typer.echo(
        f"AST pass rate: {summary['passed']}/{summary['total']} "
        f"({summary['pass_rate']:.1%})",
    )

    report = build_run_report(
        workflow=workflow,
        run_id=resolved_run_id,
        repeats=repeats,
        workers_by_stage=workers_by_stage,
        workflow_path=workflow_path,
        profiles_path=profiles_path,
        run_events=run_events,
        step1_name=workflow.step_name(0),
        step2_name=workflow.step_name(1),
    )
    persist_run_report(report)
    typer.echo(f"Stored run report in MongoDB for run_id={resolved_run_id}")
    _mongo_hint(resolved_run_id)


if __name__ == "__main__":
    app()
