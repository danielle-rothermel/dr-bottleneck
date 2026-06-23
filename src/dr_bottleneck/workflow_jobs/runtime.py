"""Runtime execution for concrete workflow jobs."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dr_code.pipeline.one_job import OneJobEvalRequest, evaluate_generated_code
from dr_providers import Message, MessageRole
from dr_queues import JobEnvelope

from dr_bottleneck.llm.client import call_llm
from dr_bottleneck.workflow_jobs.spec import (
    CandidateFunction,
    EvalFromPreviousConfig,
    EvalFromPreviousOutput,
    JobKind,
    LLMQueryFromPreviousConfig,
    LLMQueryOutput,
    LLMQueryStaticConfig,
    SamplingConfigId,
    WorkflowFailureClass,
    WorkflowJobPayload,
    WorkflowStepSpec,
)

WORKFLOW_JOB_STAGE = "workflow_job"
WORKFLOW_JOB_PAYLOAD_KEY = "workflow_job"
WORKFLOW_OUTPUT_QUEUE_KEY = "_workflow_output_queue"
WORKFLOW_FAILURE_KEY = "workflow_failure"

LLMCaller = Callable[..., dict[str, Any]]


class WorkflowJobError(Exception):
    """Non-provider workflow execution error."""

    def __init__(
        self,
        message: str,
        *,
        failure_class: WorkflowFailureClass,
    ) -> None:
        super().__init__(message)
        self.failure_class = failure_class


def run_workflow_job_step(
    job: JobEnvelope,
    *,
    llm_caller: LLMCaller = call_llm,
) -> JobEnvelope:
    """Run the current concrete workflow step for a JobEnvelope."""
    payload = workflow_payload_from_job(job)
    step = _current_step(payload, job.step_index)
    config = _step_config(payload, step)
    try:
        if step.job_kind == JobKind.LLM_QUERY_STATIC:
            output, record = _run_llm_static(
                job=job,
                step=step,
                config=_expect_config(config, LLMQueryStaticConfig),
                llm_caller=llm_caller,
            )
        elif step.job_kind == JobKind.LLM_QUERY_FROM_PREVIOUS:
            output, record = _run_llm_from_previous(
                job=job,
                payload=payload,
                step=step,
                config=_expect_config(config, LLMQueryFromPreviousConfig),
                llm_caller=llm_caller,
            )
        elif step.job_kind == JobKind.EVAL_FROM_PREVIOUS:
            output, record = _run_eval_from_previous(
                job=job,
                payload=payload,
                step=step,
                config=_expect_config(config, EvalFromPreviousConfig),
            )
        else:
            msg = f"Unsupported job kind: {step.job_kind}"
            raise WorkflowJobError(
                msg,
                failure_class=WorkflowFailureClass.INFRA_NON_RETRYABLE,
            )
    except WorkflowJobError as exc:
        output = {
            "failure_class": exc.failure_class.value,
            "error": str(exc),
        }
        record = {
            "job_kind": step.job_kind.value,
            "status": "failed",
            **output,
        }

    job.step_outputs[step.name] = output
    job.step_records[step.name] = record
    job.step_outputs[WORKFLOW_OUTPUT_QUEUE_KEY] = step.output_queue
    if step.output_queue is not None:
        job.step_index += 1
    return job


def output_queue_for_job(job: JobEnvelope) -> str | None:
    """Return the output queue selected by the just-run workflow step."""
    value = job.step_outputs.get(WORKFLOW_OUTPUT_QUEUE_KEY)
    return value if isinstance(value, str) and value else None


def workflow_payload_from_job(job: JobEnvelope) -> WorkflowJobPayload:
    """Load the workflow payload from a JobEnvelope."""
    raw = job.payload.get(WORKFLOW_JOB_PAYLOAD_KEY, job.payload)
    return WorkflowJobPayload.model_validate(raw)


def _current_step(
    payload: WorkflowJobPayload,
    step_index: int,
) -> WorkflowStepSpec:
    try:
        return payload.steps[step_index]
    except IndexError as exc:
        msg = f"Workflow step_index out of range: {step_index}"
        raise WorkflowJobError(
            msg,
            failure_class=WorkflowFailureClass.INFRA_NON_RETRYABLE,
        ) from exc


def _step_config(
    payload: WorkflowJobPayload,
    step: WorkflowStepSpec,
) -> object:
    config = payload.step_configs.get(step.name)
    if config is None:
        msg = f"Workflow step {step.name!r} has no step config."
        raise WorkflowJobError(
            msg,
            failure_class=WorkflowFailureClass.INFRA_NON_RETRYABLE,
        )
    return config


def _expect_config(value: object, model: type[Any]) -> Any:
    if not isinstance(value, model):
        msg = f"Step config must be {model.__name__}."
        raise WorkflowJobError(
            msg,
            failure_class=WorkflowFailureClass.INFRA_NON_RETRYABLE,
        )
    return value


def _run_llm_static(
    *,
    job: JobEnvelope,
    step: WorkflowStepSpec,
    config: LLMQueryStaticConfig,
    llm_caller: LLMCaller,
) -> tuple[dict[str, Any], dict[str, Any]]:
    return _run_llm_query(
        job=job,
        step=step,
        model_id=config.model_id,
        prompt=config.prompt,
        metadata=config.metadata,
        llm_caller=llm_caller,
    )


def _run_llm_from_previous(
    *,
    job: JobEnvelope,
    payload: WorkflowJobPayload,
    step: WorkflowStepSpec,
    config: LLMQueryFromPreviousConfig,
    llm_caller: LLMCaller,
) -> tuple[dict[str, Any], dict[str, Any]]:
    previous = _previous_output_text(job, payload, step)
    marker = f"{{{config.placeholder}}}"
    count = config.prompt_template.count(marker)
    if count != 1:
        msg = (
            f"Prompt template for {step.name!r} must contain exactly one "
            f"{marker!r} placeholder; found {count}."
        )
        raise WorkflowJobError(
            msg,
            failure_class=WorkflowFailureClass.INFRA_NON_RETRYABLE,
        )
    prompt = config.prompt_template.replace(marker, previous)
    return _run_llm_query(
        job=job,
        step=step,
        model_id=config.model_id,
        prompt=prompt,
        metadata=config.metadata,
        llm_caller=llm_caller,
    )


def _run_llm_query(
    *,
    job: JobEnvelope,
    step: WorkflowStepSpec,
    model_id: str,
    prompt: str,
    metadata: dict[str, Any],
    llm_caller: LLMCaller,
) -> tuple[dict[str, Any], dict[str, Any]]:
    sampling = SamplingConfigId.parse(model_id)
    messages = [Message(role=MessageRole.USER, content=prompt)]
    record = llm_caller(
        model=sampling.model,
        messages=messages,
        reasoning=sampling.reasoning,
        sampling=sampling.sampling,
        profile=sampling.original_id,
        run_id=job.run_id,
        job_id=job.job_id,
        metadata={
            **metadata,
            "step": step.name,
            "step_index": job.step_index,
            "sampling_config_id": sampling.original_id,
        },
    )
    output = LLMQueryOutput(
        output_text=str(record.get("assistant_text", "")),
        metadata=metadata,
    ).model_dump(mode="json")
    return output, {
        "job_kind": step.job_kind.value,
        "status": "complete",
        "sampling_config_id": sampling.original_id,
        "parsed_sampling_config": sampling.model_dump(mode="json"),
        "prompt": prompt,
        "llm_record": record,
    }


def _run_eval_from_previous(
    *,
    job: JobEnvelope,
    payload: WorkflowJobPayload,
    step: WorkflowStepSpec,
    config: EvalFromPreviousConfig,
) -> tuple[dict[str, Any], dict[str, Any]]:
    generated_code = _previous_output_text(job, payload, step)
    result = evaluate_generated_code(
        OneJobEvalRequest(
            run_id=job.run_id,
            task_id=config.task_id,
            decoder_input=config.decoder_input,
            raw_output=generated_code,
            metadata=config.metadata,
        )
    )
    test = result.test_outcome
    output = EvalFromPreviousOutput(
        metadata=config.metadata,
        parse_success=result.parse_outcome.parse_success,
        test_pass_rate=test.test_pass_rate or 0.0,
        all_tests_passed=bool(test.all_tests_passed),
        selected_function_name=test.selected_function_name,
        candidate_functions=tuple(
            CandidateFunction(
                name=candidate.name,
                positional_arity=candidate.positional_arity,
            )
            for candidate in test.candidate_functions
        ),
        expected_entry_point_present=test.expected_entry_point_present,
        failure_bucket=_failure_bucket(
            result.parse_outcome.parse_success, test
        ),
    ).model_dump(mode="json")
    return output, {
        "job_kind": step.job_kind.value,
        "status": "complete",
        "attempt": result.attempt.model_dump(mode="json"),
        "parse_outcome": result.parse_outcome.model_dump(mode="json"),
        "test_outcome": test.model_dump(mode="json"),
    }


def _previous_output_text(
    job: JobEnvelope,
    payload: WorkflowJobPayload,
    step: WorkflowStepSpec,
) -> str:
    index = payload.steps.index(step)
    if index == 0:
        msg = f"Step {step.name!r} requires a previous step output."
        raise WorkflowJobError(
            msg,
            failure_class=WorkflowFailureClass.INFRA_NON_RETRYABLE,
        )
    previous_step = payload.steps[index - 1]
    previous = job.step_outputs.get(previous_step.name)
    if not isinstance(previous, dict):
        msg = f"Previous step {previous_step.name!r} has no object output."
        raise WorkflowJobError(
            msg,
            failure_class=WorkflowFailureClass.INFRA_NON_RETRYABLE,
        )
    output_text = previous.get("output_text")
    if not isinstance(output_text, str):
        msg = f"Previous step {previous_step.name!r} has no output_text."
        raise WorkflowJobError(
            msg,
            failure_class=WorkflowFailureClass.INFRA_NON_RETRYABLE,
        )
    return output_text


def _failure_bucket(parse_success: bool, test: Any) -> str | None:
    if not parse_success:
        return "invalid_python"
    if not test.candidate_functions:
        return "no_top_level_functions"
    if test.skip_reason == "no_arity_matching_functions":
        return "no_arity_matching_functions"
    if test.infra_error is not None:
        return "infra_error"
    if test.internal_error is not None:
        return "internal_error"
    if test.all_tests_passed:
        return None
    return "failed_assertions"
