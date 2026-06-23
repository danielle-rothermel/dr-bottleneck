from __future__ import annotations

from dr_code.models.attempts import (
    AttemptProvenance,
    AttemptRecord,
    AttemptSource,
)
from dr_code.models.outcomes import (
    CandidateFunction as CodeCandidateFunction,
)
from dr_code.models.outcomes import (
    ParseOutcome,
    TestOutcome,
)
from dr_code.pipeline.one_job import OneJobEvalResult
from dr_queues import JobEnvelope

from dr_bottleneck.workflow_jobs import (
    EvalFromPreviousConfig,
    JobKind,
    LLMQueryFromPreviousConfig,
    LLMQueryStaticConfig,
    SamplingConfigId,
    WorkflowFailureClass,
    WorkflowJobPayload,
    WorkflowStepSpec,
    output_queue_for_job,
    run_workflow_job_step,
)


def _job(payload: WorkflowJobPayload, *, step_index: int = 0) -> JobEnvelope:
    return JobEnvelope(
        run_id="run-1",
        lane="default",
        repeat=0,
        step_index=step_index,
        pipeline_id=payload.workflow_id,
        payload=payload.model_dump(mode="json"),
    )


def test_sampling_config_id_parses_provider_settings() -> None:
    parsed = SamplingConfigId.parse(
        "openrouter.openai__gpt-oss-20b.reasoning-low.temp-0p7.top-p-0p95.v0"
    )

    assert parsed.provider == "openrouter"
    assert parsed.model == "openai/gpt-oss-20b"
    assert parsed.reasoning is not None
    assert parsed.reasoning.effort == "low"
    assert parsed.sampling.temperature == 0.7
    assert parsed.sampling.top_p == 0.95


def test_llm_static_step_records_output_and_advances() -> None:
    payload = WorkflowJobPayload(
        workflow_id="wf",
        steps=(
            WorkflowStepSpec(
                name="decode",
                job_kind=JobKind.LLM_QUERY_STATIC,
                input_queue="in",
                output_queue="out",
            ),
        ),
        step_configs={
            "decode": LLMQueryStaticConfig(
                model_id=(
                    "openrouter.openai__gpt-oss-20b.reasoning-off."
                    "temp-0p7.top-p-0p95.v0"
                ),
                prompt="hello",
            )
        },
    )

    def fake_llm_caller(**kwargs):
        assert kwargs["model"] == "openai/gpt-oss-20b"
        return {"assistant_text": "def answer(): pass", "request": {}}

    job = run_workflow_job_step(_job(payload), llm_caller=fake_llm_caller)

    assert job.step_index == 1
    assert output_queue_for_job(job) == "out"
    assert job.step_outputs["decode"]["output_text"] == "def answer(): pass"
    assert job.step_records["decode"]["status"] == "complete"


def test_previous_step_placeholder_must_appear_once() -> None:
    payload = WorkflowJobPayload(
        workflow_id="wf",
        steps=(
            WorkflowStepSpec(
                name="encode",
                job_kind=JobKind.LLM_QUERY_STATIC,
                input_queue="in",
                output_queue="decode-q",
            ),
            WorkflowStepSpec(
                name="decode",
                job_kind=JobKind.LLM_QUERY_FROM_PREVIOUS,
                input_queue="decode-q",
            ),
        ),
        step_configs={
            "encode": LLMQueryStaticConfig(
                model_id=(
                    "openrouter.openai__gpt-oss-20b.reasoning-off."
                    "temp-0p7.top-p-0p95.v0"
                ),
                prompt="hello",
            ),
            "decode": LLMQueryFromPreviousConfig(
                model_id=(
                    "openrouter.openai__gpt-oss-20b.reasoning-off."
                    "temp-0p7.top-p-0p95.v0"
                ),
                prompt_template="no placeholder",
                placeholder="encoded_description",
            ),
        },
    )
    job = _job(payload, step_index=1)
    job.step_outputs["encode"] = {"output_text": "description"}

    result = run_workflow_job_step(job, llm_caller=lambda **_: {})

    assert result.step_records["decode"]["status"] == "failed"
    assert (
        result.step_outputs["decode"]["failure_class"]
        == WorkflowFailureClass.INFRA_NON_RETRYABLE
    )


def test_eval_from_previous_projects_dr_code_result(monkeypatch) -> None:
    payload = WorkflowJobPayload(
        workflow_id="wf",
        steps=(
            WorkflowStepSpec(
                name="decode",
                job_kind=JobKind.LLM_QUERY_STATIC,
                input_queue="in",
                output_queue="eval-q",
            ),
            WorkflowStepSpec(
                name="eval",
                job_kind=JobKind.EVAL_FROM_PREVIOUS,
                input_queue="eval-q",
            ),
        ),
        step_configs={
            "decode": LLMQueryStaticConfig(
                model_id=(
                    "openrouter.openai__gpt-oss-20b.reasoning-off."
                    "temp-0p7.top-p-0p95.v0"
                ),
                prompt="hello",
            ),
            "eval": EvalFromPreviousConfig(
                suite="humaneval_plus",
                task_id="HumanEval/0",
                decoder_input="description",
            ),
        },
    )
    job = _job(payload, step_index=1)
    job.step_outputs["decode"] = {
        "output_text": "def candidate(numbers, threshold): return False"
    }
    attempt = AttemptRecord(
        sample_id="sample-1",
        run_id="run-1",
        task_id="HumanEval/0",
        decoder_input="description",
        raw_output="code",
        provenance=AttemptProvenance(source=AttemptSource.BOTTLENECK),
    )

    def fake_evaluate_generated_code(request):
        assert request.raw_output.startswith("def candidate")
        return OneJobEvalResult(
            attempt=attempt,
            parse_outcome=ParseOutcome(
                sample_id="sample-1",
                run_id="run-1",
                task_id="HumanEval/0",
                parse_success=True,
            ),
            test_outcome=TestOutcome(
                sample_id="sample-1",
                run_id="run-1",
                task_id="HumanEval/0",
                parse_success=True,
                outcome_kind="tested",
                tests_ran=True,
                all_tests_passed=True,
                test_pass_rate=1.0,
                selected_function_name="candidate",
                candidate_functions=(
                    CodeCandidateFunction(
                        name="candidate",
                        positional_arity=2,
                        source_order=0,
                    ),
                ),
                expected_entry_point_present=False,
            ),
        )

    monkeypatch.setattr(
        "dr_bottleneck.workflow_jobs.runtime.evaluate_generated_code",
        fake_evaluate_generated_code,
    )

    result = run_workflow_job_step(job)

    assert result.step_outputs["eval"]["parse_success"] is True
    assert result.step_outputs["eval"]["test_pass_rate"] == 1.0
    assert result.step_outputs["eval"]["selected_function_name"] == "candidate"
    assert result.step_outputs["eval"]["failure_bucket"] is None
