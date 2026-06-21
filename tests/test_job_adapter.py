from __future__ import annotations

from dr_queues.pipeline.job import JobEnvelope

from dr_bottleneck.job import (
    BottleneckJob,
    ProcessStepResult,
    StepExecution,
    terminal_payload_to_job_dict,
)


def test_bottleneck_job_round_trip() -> None:
    job = BottleneckJob(
        run_id="run-1",
        lane="gemini",
        repeat=0,
        workflow_id="humaneval_encode_decode_eval",
        source_code="def foo(): pass",
        sample={"task_id": "HumanEval/0"},
        metadata={"budget": 128},
        step_outputs={"encode": "encoded text"},
        step_executions={
            "encode": StepExecution(
                step_index=0,
                name="encode",
                profile="openrouter/google/gemini-2.5-flash/off/v1",
                model="openrouter/google/gemini-2.5-flash",
                temperature=0.7,
                top_p=0.95,
                prompt="prompt",
                messages=[{"role": "user", "content": "prompt"}],
                request={"model": "openrouter/google/gemini-2.5-flash"},
                response={"choices": []},
                assistant_text="encoded text",
                latency_ms=100,
                timestamp="2026-06-21T12:00:00+00:00",
            ),
        },
        step_process_results={
            "evaluate": ProcessStepResult(
                step_index=2,
                name="evaluate",
                handler="humaneval_compress_ast",
                result={"ast_parse_ok": 1},
            ),
        },
    )

    queue_job = job.to_queue_job()
    restored = BottleneckJob.from_queue_job(queue_job)

    assert restored.run_id == job.run_id
    assert restored.workflow_id == job.workflow_id
    assert restored.sample == job.sample
    assert restored.metadata == job.metadata
    assert restored.source_code == job.source_code
    assert restored.step_outputs == job.step_outputs
    assert restored.step_executions["encode"].model == job.step_executions[
        "encode"
    ].model
    assert (
        restored.step_process_results["evaluate"].result
        == job.step_process_results["evaluate"].result
    )


def test_terminal_payload_to_job_dict_from_queue_envelope() -> None:
    job = BottleneckJob(
        run_id="run-1",
        lane="gemini",
        repeat=0,
        workflow_id="wf",
        metadata={"budget": 64},
        sample={"task_id": "HumanEval/1"},
    )
    payload = job.to_queue_job().model_dump()
    normalized = terminal_payload_to_job_dict(payload)

    assert normalized["metadata"]["budget"] == 64
    assert normalized["sample"]["task_id"] == "HumanEval/1"


def test_terminal_payload_to_job_dict_legacy_shape() -> None:
    payload = {
        "job_id": "job-1",
        "lane": "gemini",
        "repeat": 0,
        "workflow_id": "wf",
        "metadata": {"budget": 32},
        "sample": {"task_id": "HumanEval/0"},
        "step_executions": {},
        "step_process_results": {},
        "step_outputs": {},
    }
    normalized = terminal_payload_to_job_dict(payload)
    assert normalized["workflow_id"] == "wf"


def test_queue_job_envelope_fields() -> None:
    job = JobEnvelope(
        run_id="run-1",
        lane="lane-1",
        repeat=0,
        pipeline_id="wf",
        payload={"sample": {"task_id": "x"}},
    )
    assert job.pipeline_id == "wf"
