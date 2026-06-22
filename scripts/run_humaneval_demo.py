from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import typer
from dr_code.pipeline.lifecycle import IN_PROCESS_MODE, run_eval_once
from dr_code.pipeline.report import format_proof_summary

from dr_bottleneck import (
    DEFAULT_BUDGETS,
    EventKind,
    MongoRunStore,
    Workflow,
    attempts_from_terminal_payloads,
    build_run_report,
    expand_experiment_jobs,
    filter_tasks,
    format_worker_commands,
    load_humanevalplus,
    parse_workers_arg,
    persist_run_report,
    read_run_events,
    run_bottleneck_in_process,
    seed_bottleneck_run,
    setup_bottleneck_run,
    start_bottleneck_workers,
    stop_workers,
    tiny_experiment_filters,
    wait_for_run,
)
from dr_bottleneck.storage.inspect import format_mongo_inspect_hints

DEFAULT_WORKFLOW = Path("configs/workflows/humaneval_encode_decode.yaml")
DEFAULT_WORKERS = "encode=8,decode=8"
DEFAULT_CODE_EVAL_WORKERS = "parse=8,test=8"

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
        f"Inspect results: see MONGODB_QUICKSTART.md (run_id={run_id})",
    )
    for command in format_mongo_inspect_hints(
        run_id,
        include_code_eval=True,
    ):
        typer.echo(command)


@app.command()
def main(
    tiny: bool = typer.Option(False, "--tiny"),
    repeats: int = typer.Option(1, "--repeats"),
    workers: str = typer.Option(DEFAULT_WORKERS, "--workers"),
    code_eval_workers: str = typer.Option(
        DEFAULT_CODE_EVAL_WORKERS,
        "--code-eval-workers",
    ),
    code_eval_mode: str = typer.Option(IN_PROCESS_MODE, "--code-eval-mode"),
    start_workers: bool = typer.Option(False, "--start-workers"),
    no_wait: bool = typer.Option(False, "--no-wait"),
    workflow_path: Path = typer.Option(DEFAULT_WORKFLOW, "--workflow"),
    profiles_path: Path = typer.Option(
        Path("configs/openrouter_profiles.yaml"),
        "--profiles-path",
    ),
    run_id: str | None = typer.Option(None, "--run-id"),
    code_eval_run_id: str | None = typer.Option(None, "--code-eval-run-id"),
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
    llm_calls = expected * len(workflow.config.steps)

    typer.echo(f"run_id={resolved_run_id} expected_jobs={expected}")
    typer.echo(f"estimated_llm_calls={llm_calls}")
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
        seed_bottleneck_run(manifest, jobs, run_store=store)
        typer.echo(f"Seeded {len(jobs)} jobs.")

        if no_wait:
            if effective_start_workers:
                start_bottleneck_workers(
                    manifest=manifest,
                    workers_by_stage=workers_by_stage,
                )
                typer.echo("Started detached stage workers.")
            else:
                typer.echo("Start workers with:")
                for command in format_worker_commands(manifest):
                    typer.echo(f"  {command}")
            typer.echo("Skipping linked code eval because --no-wait was set.")
            return

        if tiny or not effective_start_workers:
            typer.echo("Running bottleneck workers in process...")
            run_bottleneck_in_process(
                manifest=manifest,
                workflow=workflow,
                workers_by_stage=workers_by_stage,
                completion_timeout=completion_timeout,
                run_store=store,
            )
        else:
            try:
                start_bottleneck_workers(
                    manifest=manifest,
                    workers_by_stage=workers_by_stage,
                )
                typer.echo("Waiting for bottleneck terminal events...")
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

        run_events = read_run_events(resolved_run_id, run_store=store)
    finally:
        store.close()

    terminal_payloads = [
        event["payload"]
        for event in run_events
        if event.get("event") == EventKind.TERMINAL
    ]
    attempts = attempts_from_terminal_payloads(
        run_id=resolved_run_id,
        terminal_payloads=terminal_payloads,
    )
    resolved_code_eval_run_id = (
        code_eval_run_id or f"{resolved_run_id}-code-eval"
    )
    typer.echo(
        f"Running linked dr-code eval run_id={resolved_code_eval_run_id} "
        f"attempts={len(attempts)}"
    )
    code_eval_result = run_eval_once(
        attempts,
        run_id=resolved_code_eval_run_id,
        mode=code_eval_mode,
        workers=code_eval_workers,
        completion_timeout=completion_timeout,
        skip_preflight=True,
        overwrite=True,
    )
    proof_report = code_eval_result.pipeline_result.proof_report
    typer.echo(format_proof_summary(proof_report))

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
        code_eval_run_id=resolved_code_eval_run_id,
        code_eval_summary=proof_report.payload,
    )
    persist_run_report(report)
    typer.echo(f"Stored run report in MongoDB for run_id={resolved_run_id}")
    _mongo_hint(resolved_run_id)


if __name__ == "__main__":
    app()
