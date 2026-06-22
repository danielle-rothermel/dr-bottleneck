from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import typer

from dr_bottleneck import (
    MongoRunStore,
    Workflow,
    build_run_report,
    format_overlap_report,
    format_worker_commands,
    parse_workers_arg,
    persist_run_report,
    read_run_events,
    run_bottleneck_in_process,
    seed_bottleneck_run,
    setup_bottleneck_run,
    start_bottleneck_workers,
    stop_workers,
    wait_for_run,
)
from dr_bottleneck.storage.inspect import format_mongo_inspect_hints

DEFAULT_WORKFLOW = Path("configs/workflows/two_step_random.yaml")
DEFAULT_WORKERS = "random_number=20,add_five=20"

app = typer.Typer(add_completion=False)


def _mongo_hint(run_id: str) -> None:
    typer.echo(
        f"Inspect results: see MONGODB_QUICKSTART.md (run_id={run_id})",
    )
    for command in format_mongo_inspect_hints(run_id):
        typer.echo(command)


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
    typer.echo("manifest=dr_queues.run_manifests")

    store = MongoRunStore()
    try:
        manifest = setup_bottleneck_run(
            workflow=workflow,
            run_id=resolved_run_id,
            workers_by_stage=workers_by_stage,
            workflow_path=workflow_path,
            profiles_path=profiles_path,
            run_store=store,
        )
        jobs = workflow.make_seed_jobs(
            run_id=resolved_run_id,
            repeats=repeats,
        )
        seed_bottleneck_run(manifest, jobs, run_store=store)
        typer.echo(f"Seeded {len(jobs)} jobs.")

        if no_wait:
            if start_workers:
                start_bottleneck_workers(
                    manifest=manifest,
                    workers_by_stage=workers_by_stage,
                )
                typer.echo("Started detached stage workers.")
            else:
                typer.echo("Start workers with:")
                for command in format_worker_commands(manifest):
                    typer.echo(f"  {command}")
            return

        if start_workers:
            try:
                start_bottleneck_workers(
                    manifest=manifest,
                    workers_by_stage=workers_by_stage,
                )
                typer.echo("Waiting for terminal events (detached workers)...")
                status = wait_for_run(
                    resolved_run_id,
                    timeout=completion_timeout,
                    run_store=store,
                )
                if not status.is_complete:
                    typer.echo(
                        "Timed out waiting for workflow completion.",
                        err=True,
                    )
                    raise typer.Exit(code=1)
            finally:
                stop_workers(run_id=resolved_run_id, run_store=store)
        else:
            typer.echo("Running in-process workers...")
            run_bottleneck_in_process(
                manifest=manifest,
                workflow=workflow,
                workers_by_stage=workers_by_stage,
                completion_timeout=completion_timeout,
                run_store=store,
            )

        run_events = read_run_events(resolved_run_id, run_store=store)
    finally:
        store.close()

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
