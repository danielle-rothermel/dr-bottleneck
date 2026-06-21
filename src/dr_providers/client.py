import json
import time
from datetime import UTC, datetime
from typing import Any

import litellm

from dr_providers.openrouter import Message, build_completion_kwargs


def _serialize_response(response: Any) -> dict[str, Any]:
    if hasattr(response, "model_dump"):
        return response.model_dump()
    return json.loads(json.dumps(response, default=str))


def _request_for_record(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in kwargs.items() if key != "api_key"}


def assistant_text(response: Any) -> str:
    if isinstance(response, dict):
        choices = response.get("choices", [])
        if not choices:
            return ""
        message = choices[0].get("message", {})
        return message.get("content") or ""

    return response.choices[0].message.content or ""


def call_llm(
    model: str,
    messages: list[Message],
    *,
    temperature: float,
    top_p: float,
    reasoning_disabled: bool = False,
    effort: str | None = None,
    profile: str | None = None,
) -> dict[str, Any]:
    kwargs = build_completion_kwargs(
        model,
        messages,
        temperature=temperature,
        top_p=top_p,
        reasoning_disabled=reasoning_disabled,
        effort=effort,
    )

    started = time.perf_counter()
    response = litellm.completion(**kwargs)
    latency_ms = int((time.perf_counter() - started) * 1000)

    record: dict[str, Any] = {
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "profile": profile,
        "request": _request_for_record(kwargs),
        "response": _serialize_response(response),
        "latency_ms": latency_ms,
    }
    return record
