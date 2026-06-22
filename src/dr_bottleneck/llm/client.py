from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from dr_providers import (
    LlmRequest,
    Message,
    OpenRouterProvider,
    ProviderName,
    ReasoningSpec,
    SamplingControls,
)

from dr_bottleneck.storage.llm_calls import append_llm_call


def call_llm(
    *,
    model: str,
    messages: list[Message],
    reasoning: ReasoningSpec | None = None,
    sampling: SamplingControls | None = None,
    max_tokens: int | None = None,
    profile: str | None = None,
    run_id: str | None = None,
    job_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_metadata = {
        **(metadata or {}),
        **({"profile": profile} if profile is not None else {}),
        **({"run_id": run_id} if run_id is not None else {}),
        **({"job_id": job_id} if job_id is not None else {}),
    }
    request = LlmRequest(
        provider=ProviderName.OPENROUTER,
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        reasoning=reasoning,
        sampling=sampling,
        metadata=request_metadata,
    )
    with OpenRouterProvider() as provider:
        response = provider.generate(request)

    record: dict[str, Any] = {
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "profile": profile,
        "run_id": run_id,
        "job_id": job_id,
        "request": request.model_dump(mode="json"),
        "response": response.model_dump(mode="json"),
        "assistant_text": response.text,
        "latency_ms": response.latency_ms,
    }
    append_llm_call(record)
    return record


def assistant_text(response: dict[str, Any]) -> str:
    text = response.get("text")
    return text if isinstance(text, str) else ""


__all__ = ["Message", "assistant_text", "call_llm"]
