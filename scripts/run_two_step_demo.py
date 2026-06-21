from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import typer

from dr_bottleneck import (
    TerminalTap,
    Workflow,
    build_run_report,
    create_event_sink,
    format_overlap_report,
    manifest_path,
    parse_workers_arg,
    peek_run_events,
    persist_run_report,
    run_workflow_in_process,
    seed_manifest_jobs,
    setup_run_queues,
    spawn_all_stage_workers,
)
from dr_bottleneck.runtime import format_worker_commands

DEFAULT_WORKFLOW = Path("configs/workflows/two_step_random.yaml")
DEFAULT_WORKERS = "random_number=20,add_five=20"

app = typer.Typer(add_completion=False)


def _mongo_hint(run_id: str) -> None:
    typer.echo(
        "Inspect results: see MONGODB_QUICKSTART.md "
        f"(run_id={run_id})",
    )


@app.command()
def main(
    repeats: int = typer.Option(10, "--repeats"),
    workers: str = typer.Option(DEFAULT_WORKERS, "--workers"),
    start_workers: bool = typer.Option(False, "--start-workers"),
    no_wait: bool = typer.Option(False, "--no-wait"),
    workflow_path: Path = typer.Option(
        DEFAULT_WORKFLOW,
        "--workflow",
    ),
    profiles_path: Path = typer.Option(
        Path("configs/openrouter_profiles.yaml"),
        "--profiles-path",
    ),
    run_id: str | None = typer.Option(None, "--run-id"),
    completion_timeout: float = typer.Option(
        3600.0,
        "--completion-timeout",
    ),
) -> None:
    resolved_run_id = run_id or f"demo-{uuid4().hex[:8]}"
    workflow = Workflow.from_yaml(
        workflow_path,
        profiles_path=profiles_path,
    )
    expected = workflow.expected_job_count(repeats)
    workers_by_stage = parse_workers_arg(
        workers,
        workflow.step_names(),
        default=20,
    )

    typer.echo(f"run_id={resolved_run_id} expected_jobs={expected}")
    typer.echo(f"manifest={manifest_path(resolved_run_id)}")

    manifest = setup_run_queues(
        workflow=workflow,
        run_id=resolved_run_id,
        workers_by_stage=workers_by_stage,
        workflow_path=workflow_path,
        profiles_path=profiles_path,
        expected_jobs=expected,
    )

    jobs = workflow.make_seed_jobs(run_id=resolved_run_id, repeats=repeats)
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
        if start_workers:
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

    if start_workers:
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
    else:
        typer.echo("Running in-process workers...")
        run_workflow_in_process(
            manifest=manifest,
            workflow=workflow,
            workers_by_stage=workers_by_stage,
            completion_timeout=completion_timeout,
            event_sink=event_sink,
            tap=tap,
        )

    run_events = peek_run_events(resolved_run_id)
    step1_name = workflow.step_name(0)
    step2_name = workflow.step_name(1)
    report = build_run_report(
        workflow=workflow,
        run_id=resolved_run_id,
        repeats=repeats,
        workers_by_stage=workers_by_stage,
        workflow_path=workflow_path,
        profiles_path=profiles_path,
        run_events=run_events,
        step1_name=step1_name,
        step2_name=step2_name,
    )

    typer.echo(f"Jobs in report: {len(report['jobs'])}")
    persist_run_report(report)
    typer.echo(f"Stored run report in MongoDB for run_id={resolved_run_id}")
    _mongo_hint(resolved_run_id)

    typer.echo(format_overlap_report(report["overlap_report"]))

    if not report["overlap_report"].get("passed"):
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
