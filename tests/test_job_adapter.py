from __future__ import annotations

from dr_bottleneck.job import (
    BottleneckPayload,
    LlmStepRecord,
    llm_step_record,
    make_job_envelope,
    payload_from_job,
    terminal_payload_to_job,
)


def test_bottleneck_payload_round_trip() -> None:
    job = make_job_envelope(
        run_id="run-1",
        lane="gemini",
        repeat=0,
        pipeline_id="humaneval_encode_decode",
        payload=BottleneckPayload(
            source_code="def foo(): pass",
            sample={"task_id": "HumanEval/0"},
            metadata={"budget": 128},
        ),
    )

    restored = terminal_payload_to_job(job.model_dump(mode="json"))
    payload = payload_from_job(restored)

    assert restored.run_id == job.run_id
    assert restored.pipeline_id == "humaneval_encode_decode"
    assert payload.sample == {"task_id": "HumanEval/0"}
    assert payload.metadata == {"budget": 128}
    assert payload.source_code == "def foo(): pass"


def test_llm_step_record_round_trip() -> None:
    job = make_job_envelope(
        run_id="run-1",
        lane="gemini",
        repeat=0,
        pipeline_id="wf",
        payload=BottleneckPayload(),
    )
    job.step_records["encode"] = LlmStepRecord(
        step_index=0,
        name="encode",
        profile="openrouter/google/gemini-2.5-flash/off/v1",
        model="google/gemini-2.5-flash",
        prompt="prompt",
        messages=[{"role": "user", "content": "prompt"}],
        request={"model": "google/gemini-2.5-flash"},
        response={"text": "encoded"},
        assistant_text="encoded",
        latency_ms=100,
        timestamp="2026-06-21T12:00:00+00:00",
    ).to_step_record()

    restored = llm_step_record(job, "encode")

    assert restored.name == "encode"
    assert restored.model == "google/gemini-2.5-flash"
    assert restored.assistant_text == "encoded"
