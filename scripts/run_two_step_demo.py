import time
from pathlib import Path
from uuid import uuid4

import typer

from dr_queues import (
    TerminalTap,
    WorkerPool,
    Workflow,
    build_run_report,
    build_stage_queues,
    ensure_drain_queue,
    format_overlap_report,
    open_session,
    peek_drain,
    seed_jobs,
    write_run_report_jsonl,
)

DEFAULT_WORKFLOW = Path("configs/workflows/two_step_random.yaml")

app = typer.Typer(add_completion=False)


@app.command()
def main(
    repeats: int = typer.Option(10, "--repeats"),
    workers: int = typer.Option(20, "--workers"),
    workflow_path: Path = typer.Option(
        DEFAULT_WORKFLOW,
        "--workflow",
    ),
    profiles_path: Path = typer.Option(
        Path("configs/openrouter_profiles.yaml"),
        "--profiles-path",
    ),
    run_id: str | None = typer.Option(None, "--run-id"),
    dump_out: Path | None = typer.Option(None, "--dump-out"),
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

    typer.echo(f"run_id={resolved_run_id} expected_jobs={expected}")

    setup_session = open_session()
    try:
        ensure_drain_queue(setup_session.channel)
    finally:
        setup_session.close()

    prefix = f"demo.{resolved_run_id}"
    stage1 = build_stage_queues(prefix=f"{prefix}.s1")
    stage2 = build_stage_queues(
        prefix=f"{prefix}.s2",
        pending=stage1.completed_name,
    )

    step1_name = workflow.step_name(0)
    step2_name = workflow.step_name(1)

    stage2_pool = WorkerPool(
        input_queue=stage2.pending_name,
        output_queue=stage2.completed_name,
        handler=workflow.make_handler(1),
        workers=workers,
        stage_name=step2_name,
    )
    tap = TerminalTap(
        completed_queue=stage2.completed_name,
        run_id=resolved_run_id,
        expected_count=expected,
    )
    stage1_pool = WorkerPool(
        input_queue=stage1.pending_name,
        output_queue=stage1.completed_name,
        handler=workflow.make_handler(0),
        workers=workers,
        stage_name=step1_name,
    )

    typer.echo("Starting stage 2 workers...")
    stage2_pool.start()
    typer.echo("Starting terminal tap...")
    tap.start()
    typer.echo("Starting stage 1 workers...")
    stage1_pool.start()

    jobs = workflow.make_seed_jobs(run_id=resolved_run_id, repeats=repeats)
    typer.echo(f"Seeding {len(jobs)} jobs into {stage1.pending_name}...")
    seed_jobs(stage1.pending_name, jobs)

    typer.echo("Waiting for terminal events...")
    if not tap.wait_for_completion(timeout=completion_timeout):
        typer.echo("Timed out waiting for workflow completion.", err=True)
        raise typer.Exit(code=1)

    typer.echo("Workflow complete. Stopping workers...")
    stage1_pool.stop()
    stage2_pool.stop()
    tap.stop()
    stage1_pool.join(timeout=5)
    stage2_pool.join(timeout=5)
    tap.join(timeout=5)

    time.sleep(0.5)

    typer.echo("Reading drain queue (peek, no purge)...")
    drain_session = open_session()
    try:
        events = peek_drain(drain_session.channel)
    finally:
        drain_session.close()

    run_events = [
        event for event in events if event.get("run_id") == resolved_run_id
    ]
    report = build_run_report(
        workflow=workflow,
        run_id=resolved_run_id,
        repeats=repeats,
        workers=workers,
        workflow_path=workflow_path,
        profiles_path=profiles_path,
        run_events=run_events,
        step1_name=step1_name,
        step2_name=step2_name,
    )

    typer.echo(f"Jobs in report: {len(report['jobs'])}")

    output_path = dump_out or Path(f"exports/run-{resolved_run_id}.jsonl")
    write_run_report_jsonl(output_path, report)
    typer.echo(f"Wrote run report to {output_path}")

    typer.echo(format_overlap_report(report["overlap_report"]))

    if not report["overlap_report"].get("passed"):
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
