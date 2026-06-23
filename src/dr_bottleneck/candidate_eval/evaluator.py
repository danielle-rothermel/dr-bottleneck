from __future__ import annotations

from pathlib import Path
from statistics import mean
from typing import Any
from uuid import uuid4

from dr_code.analysis.compress import decoder_input_compression
from dr_code.models.attempts import AttemptRecord
from dr_code.models.humaneval import HumanEvalPlusTask
from dr_code.models.outcomes import ParseOutcome, TestOutcome
from dr_code.pipeline.export import read_eval_run_outcomes
from dr_code.pipeline.lifecycle import (
    DETACHED_MODE,
    IN_PROCESS_MODE,
    run_eval_once,
)
from dr_queues import EventKind, JobEnvelope, MongoRunStore

from dr_bottleneck.candidate_eval.models import (
    CandidateAggregateMetrics,
    CandidateEvalRequest,
    CandidateEvalResult,
    CandidateEvalStatus,
    CandidateExampleResult,
    CandidateExecutionMode,
    FailureBucket,
)
from dr_bottleneck.experiments.humaneval import (
    DECODE_STEP_NAME,
    build_source_code,
    filter_tasks,
    load_humanevalplus,
)
from dr_bottleneck.experiments.humaneval_context import decoder_only_context
from dr_bottleneck.job import (
    BottleneckPayload,
    llm_step_record,
    make_job_envelope,
    terminal_payload_to_job,
)
from dr_bottleneck.runtime import (
    parse_workers_arg,
    read_run_events,
    run_bottleneck_in_process,
    seed_bottleneck_run,
    setup_bottleneck_run,
    start_bottleneck_workers,
    stop_workers,
    wait_for_run,
)
from dr_bottleneck.workflow.config import (
    LaneStepProfile,
    WorkflowConfig,
    WorkflowLane,
    WorkflowStep,
)
from dr_bottleneck.workflow.engine import DEFAULT_PROFILES_PATH, Workflow

DEFAULT_SOURCE_WORKFLOW = Path(
    "configs/workflows/humaneval_encode_decode.yaml"
)
CANDIDATE_WORKFLOW_ID = "candidate_decoder_only"
CANDIDATE_METADATA_KEY = "candidate_id"


def evaluate_candidate(
    request: CandidateEvalRequest,
    *,
    run_store: MongoRunStore | None = None,
) -> CandidateEvalResult:
    if not request.is_decoder_only:
        msg = "Only decoder-only candidate evaluation is implemented."
        raise NotImplementedError(msg)

    store = run_store or MongoRunStore()
    close_store = run_store is None
    bottleneck_run_id = _bottleneck_run_id(request)
    code_eval_run_id = f"{bottleneck_run_id}-code-eval"
    try:
        workflow = _decoder_only_workflow(request)
        tasks = filter_tasks(load_humanevalplus(), request.task_ids)
        jobs = _decoder_only_jobs(
            request=request,
            workflow=workflow,
            run_id=bottleneck_run_id,
            tasks=tasks,
        )
        workers_by_stage = parse_workers_arg(
            request.bottleneck_workers,
            workflow.step_names(),
            default=1,
        )
        manifest = setup_bottleneck_run(
            workflow=workflow,
            run_id=bottleneck_run_id,
            workers_by_stage=workers_by_stage,
            workflow_path=DEFAULT_SOURCE_WORKFLOW,
            profiles_path=DEFAULT_PROFILES_PATH,
            run_store=store,
        )
        seed_bottleneck_run(manifest, jobs, run_store=store)
        _run_generation(
            request=request,
            workflow=workflow,
            manifest=manifest,
            workers_by_stage=workers_by_stage,
            run_store=store,
        )
        run_events = read_run_events(bottleneck_run_id, run_store=store)
        terminal_payloads = [
            event["payload"]
            for event in run_events
            if event.get("event") == EventKind.TERMINAL
        ]
        attempts = _attempts_from_decoder_terminal_payloads(
            run_id=bottleneck_run_id,
            terminal_payloads=terminal_payloads,
        )
        code_eval_result = run_eval_once(
            attempts,
            run_id=code_eval_run_id,
            mode=_code_eval_mode(request.execution_mode),
            workers=request.code_eval_workers,
            completion_timeout=request.completion_timeout_seconds,
            skip_preflight=True,
            overwrite=True,
        )
        outcomes = read_eval_run_outcomes(run_id=code_eval_run_id)
        return _build_result(
            request=request,
            bottleneck_run_id=bottleneck_run_id,
            code_eval_run_id=code_eval_run_id,
            attempts=attempts,
            parse_outcomes=outcomes.parse_outcomes,
            test_outcomes=outcomes.test_outcomes,
            code_eval_summary=code_eval_result.pipeline_result.proof_report.payload,
        )
    finally:
        if close_store:
            store.close()


def _bottleneck_run_id(request: CandidateEvalRequest) -> str:
    safe_candidate_id = request.candidate_id.replace("/", "-")
    return f"{request.optimizer_run_id}-{safe_candidate_id}-{uuid4().hex[:8]}"


def _decoder_only_workflow(request: CandidateEvalRequest) -> Workflow:
    source = Workflow.from_yaml(
        DEFAULT_SOURCE_WORKFLOW,
        profiles_path=DEFAULT_PROFILES_PATH,
    )
    source_step_names = source.step_names()
    decode_index = source_step_names.index(DECODE_STEP_NAME)
    lanes = []
    for lane_id in request.lane_ids:
        profile = _lane_profile(
            source, lane_id=lane_id, step_index=decode_index
        )
        lanes.append(
            WorkflowLane(
                id=lane_id,
                steps=[LaneStepProfile(profile=profile)],
            )
        )
    config = WorkflowConfig(
        id=CANDIDATE_WORKFLOW_ID,
        steps=[
            WorkflowStep(
                name=DECODE_STEP_NAME,
                prompt_template=request.decoder_template_text,
            )
        ],
        lanes=lanes,
    )
    return Workflow(config, profiles_path=DEFAULT_PROFILES_PATH)


def _lane_profile(
    workflow: Workflow,
    *,
    lane_id: str,
    step_index: int,
) -> str:
    for lane in workflow.config.lanes:
        if lane.id != lane_id:
            continue
        profile = lane.steps[step_index].profile
        if profile is None:
            msg = f"Lane {lane_id!r} step {step_index} has no profile."
            raise ValueError(msg)
        return profile
    msg = f"Unknown lane: {lane_id!r}"
    raise ValueError(msg)


def _decoder_only_jobs(
    *,
    request: CandidateEvalRequest,
    workflow: Workflow,
    run_id: str,
    tasks: list[HumanEvalPlusTask],
) -> list[JobEnvelope]:
    jobs: list[JobEnvelope] = []
    for lane_id in request.lane_ids:
        for task in tasks:
            context = decoder_only_context(task)
            sample = {
                **task.model_dump(mode="json"),
                "signature": context.signature,
                "encoded_description": context.encoded_description,
            }
            for budget in request.budgets:
                for repeat in range(request.repeats):
                    jobs.append(
                        make_job_envelope(
                            run_id=run_id,
                            lane=lane_id,
                            repeat=repeat,
                            pipeline_id=workflow.config.id,
                            payload=BottleneckPayload(
                                sample=sample,
                                metadata={
                                    **request.slot_values,
                                    CANDIDATE_METADATA_KEY: request.candidate_id,
                                    "budget": budget,
                                },
                                source_code=build_source_code(
                                    task.prompt,
                                    task.canonical_solution,
                                ),
                            ),
                        )
                    )
    return jobs


def _run_generation(
    *,
    request: CandidateEvalRequest,
    workflow: Workflow,
    manifest: Any,
    workers_by_stage: dict[str, int],
    run_store: MongoRunStore,
) -> None:
    if request.execution_mode == CandidateExecutionMode.IN_PROCESS:
        run_bottleneck_in_process(
            manifest=manifest,
            workflow=workflow,
            workers_by_stage=workers_by_stage,
            completion_timeout=request.completion_timeout_seconds,
            run_store=run_store,
        )
        return

    try:
        start_bottleneck_workers(
            manifest=manifest,
            workers_by_stage=workers_by_stage,
        )
        status = wait_for_run(
            manifest.run_id,
            timeout=request.completion_timeout_seconds,
            run_store=run_store,
        )
        if not status.is_complete:
            msg = f"Timed out waiting for candidate run {manifest.run_id!r}."
            raise TimeoutError(msg)
    finally:
        stop_workers(run_id=manifest.run_id, run_store=run_store)


def _code_eval_mode(mode: CandidateExecutionMode) -> str:
    if mode == CandidateExecutionMode.IN_PROCESS:
        return IN_PROCESS_MODE
    return DETACHED_MODE


def _attempts_from_decoder_terminal_payloads(
    *,
    run_id: str,
    terminal_payloads: list[dict[str, Any]],
) -> list[AttemptRecord]:
    attempts: list[AttemptRecord] = []
    for payload in terminal_payloads:
        job = terminal_payload_to_job(payload)
        sample = job.payload.get("sample", {})
        metadata = job.payload.get("metadata", {})
        if not isinstance(sample, dict):
            msg = f"Job {job.job_id!r} has invalid sample payload."
            raise ValueError(msg)
        if not isinstance(metadata, dict):
            msg = f"Job {job.job_id!r} has invalid metadata payload."
            raise ValueError(msg)
        decode_text = str(job.step_outputs.get(DECODE_STEP_NAME, ""))
        if not decode_text:
            msg = (
                f"Job {job.job_id!r} is missing decode output "
                f"for run {run_id!r}."
            )
            raise ValueError(msg)
        decode_record = llm_step_record(job, DECODE_STEP_NAME)
        attempts.append(
            AttemptRecord.from_bottleneck_output(
                run_id=run_id,
                task_id=str(sample["task_id"]),
                decoder_input=str(sample["encoded_description"]),
                raw_output=decode_text,
                decode_model=decode_record.model,
                decode_profile_id=decode_record.profile,
                extra={
                    "bottleneck_run_id": run_id,
                    "bottleneck_job_id": job.job_id,
                    "lane": job.lane,
                    "repeat": job.repeat,
                    "budget": int(metadata.get("budget", 0)),
                    "candidate_id": str(
                        metadata.get(CANDIDATE_METADATA_KEY, "")
                    ),
                    "expected_signature": str(sample.get("signature", "")),
                },
            )
        )
    return attempts


def _build_result(
    *,
    request: CandidateEvalRequest,
    bottleneck_run_id: str,
    code_eval_run_id: str,
    attempts: list[AttemptRecord],
    parse_outcomes: list[ParseOutcome],
    test_outcomes: list[TestOutcome],
    code_eval_summary: dict[str, Any],
) -> CandidateEvalResult:
    parse_by_id = {outcome.sample_id: outcome for outcome in parse_outcomes}
    test_by_id = {outcome.sample_id: outcome for outcome in test_outcomes}
    examples = [
        _example_result(
            attempt=attempt,
            parse_outcome=parse_by_id.get(attempt.sample_id),
            test_outcome=test_by_id.get(attempt.sample_id),
        )
        for attempt in attempts
    ]
    return CandidateEvalResult(
        optimizer_run_id=request.optimizer_run_id,
        candidate_id=request.candidate_id,
        status=CandidateEvalStatus.COMPLETE,
        bottleneck_run_id=bottleneck_run_id,
        code_eval_run_id=code_eval_run_id,
        aggregate_metrics=_aggregate_metrics(examples),
        examples=examples,
        provenance_refs={
            "bottleneck_run_id": bottleneck_run_id,
            "code_eval_run_id": code_eval_run_id,
            "code_eval_complete": str(code_eval_summary.get("complete", "")),
        },
    )


def _example_result(
    *,
    attempt: AttemptRecord,
    parse_outcome: ParseOutcome | None,
    test_outcome: TestOutcome | None,
) -> CandidateExampleResult:
    expected_entry_point_present = bool(
        test_outcome and test_outcome.expected_entry_point_present
    )
    signature_compatible = bool(
        test_outcome and test_outcome.selected_function_name
    )
    bucket = _failure_bucket(
        parse_outcome=parse_outcome,
        test_outcome=test_outcome,
    )
    raw_bytes, compressed_bytes = decoder_input_compression(
        attempt.decoder_input
    )
    extra = attempt.provenance.extra
    return CandidateExampleResult(
        task_id=attempt.task_id,
        lane=str(extra.get("lane", "")),
        budget=int(extra.get("budget", 0) or 0),
        repeat=int(extra.get("repeat", 0) or 0),
        sample_id=attempt.sample_id,
        parse_success=bool(parse_outcome and parse_outcome.parse_success),
        entry_point_exists=expected_entry_point_present,
        signature_compatible=signature_compatible,
        tests_ran=bool(test_outcome and test_outcome.tests_ran),
        all_tests_passed=(
            test_outcome.all_tests_passed if test_outcome is not None else None
        ),
        test_pass_rate=(
            test_outcome.test_pass_rate if test_outcome is not None else None
        ),
        failure_bucket=bucket,
        feedback=_feedback(bucket, parse_outcome, test_outcome),
        decoder_input_bytes=raw_bytes,
        compressed_decoder_input_bytes=compressed_bytes,
    )


def _failure_bucket(
    *,
    parse_outcome: ParseOutcome | None,
    test_outcome: TestOutcome | None,
) -> FailureBucket:
    if parse_outcome is None or not parse_outcome.parse_success:
        return FailureBucket.INVALID_PYTHON
    if test_outcome is None:
        return FailureBucket.SKIPPED
    if not test_outcome.candidate_functions:
        return FailureBucket.MISSING_ENTRY_POINT
    if test_outcome.skip_reason == "no_arity_matching_functions":
        return FailureBucket.INCOMPATIBLE_SIGNATURE
    if test_outcome.skipped:
        return FailureBucket.SKIPPED
    if test_outcome.outcome_kind == "internal_error":
        return FailureBucket.INTERNAL_ERROR
    if test_outcome.outcome_kind == "infra_error":
        return FailureBucket.RUNTIME_ERROR
    if test_outcome.all_tests_passed:
        return FailureBucket.PASSED
    return FailureBucket.FAILED_ASSERTIONS


def _feedback(
    bucket: FailureBucket,
    parse_outcome: ParseOutcome | None,
    test_outcome: TestOutcome | None,
) -> str:
    if bucket == FailureBucket.PASSED:
        return "Generated code parsed and passed all tests."
    if bucket == FailureBucket.INVALID_PYTHON:
        reason = (
            parse_outcome.skip_reason
            if parse_outcome
            else "missing parse outcome"
        )
        return f"Generated code did not parse: {reason}"
    if bucket == FailureBucket.MISSING_ENTRY_POINT:
        return "Generated code parsed but defined no top-level functions."
    if bucket == FailureBucket.INCOMPATIBLE_SIGNATURE:
        return (
            "Generated code parsed but defined no arity-compatible functions."
        )
    if test_outcome is None:
        return "No test outcome was recorded for this attempt."
    if bucket == FailureBucket.SKIPPED:
        return f"Tests were skipped: {test_outcome.skip_reason}"
    if bucket in {FailureBucket.INTERNAL_ERROR, FailureBucket.RUNTIME_ERROR}:
        detail = test_outcome.internal_error or (
            test_outcome.infra_error.detail if test_outcome.infra_error else ""
        )
        return f"Test execution failed: {detail}"
    return "Generated code ran but failed one or more tests."


def _aggregate_metrics(
    examples: list[CandidateExampleResult],
) -> CandidateAggregateMetrics:
    attempt_count = len(examples)
    if attempt_count == 0:
        return CandidateAggregateMetrics()
    parse_success_count = sum(example.parse_success for example in examples)
    tests_ran_count = sum(example.tests_ran for example in examples)
    all_tests_passed_count = sum(
        example.all_tests_passed is True for example in examples
    )
    pass_rates = [
        example.test_pass_rate
        for example in examples
        if example.test_pass_rate is not None
    ]
    total_raw = sum(example.decoder_input_bytes for example in examples)
    total_compressed = sum(
        example.compressed_decoder_input_bytes for example in examples
    )
    return CandidateAggregateMetrics(
        attempt_count=attempt_count,
        parse_success_count=parse_success_count,
        tests_ran_count=tests_ran_count,
        all_tests_passed_count=all_tests_passed_count,
        parse_rate=parse_success_count / attempt_count,
        tests_ran_rate=tests_ran_count / attempt_count,
        all_tests_passed_rate=all_tests_passed_count / attempt_count,
        mean_test_pass_rate=mean(pass_rates) if pass_rates else None,
        total_decoder_input_bytes=total_raw,
        total_compressed_decoder_input_bytes=total_compressed,
        mean_decoder_input_bytes=total_raw / attempt_count,
        mean_compressed_decoder_input_bytes=total_compressed / attempt_count,
    )
