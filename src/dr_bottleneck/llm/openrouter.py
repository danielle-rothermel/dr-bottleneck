from __future__ import annotations

import os
from typing import Any

Message = dict[str, str]


class MissingApiKeyError(RuntimeError):
    pass


def require_openrouter_api_key() -> str:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        msg = "OPENROUTER_API_KEY is not set."
        raise MissingApiKeyError(msg)
    return api_key


def build_completion_kwargs(
    model: str,
    messages: list[Message],
    *,
    temperature: float,
    top_p: float,
    reasoning_disabled: bool = False,
    effort: str | None = None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "model": f"openrouter/{model}",
        "messages": messages,
        "temperature": temperature,
        "top_p": top_p,
        "api_key": require_openrouter_api_key(),
    }

    extra_body: dict[str, Any] = {}
    if reasoning_disabled:
        extra_body["reasoning"] = {"effort": "none"}
    elif effort is not None:
        extra_body["reasoning"] = {"effort": effort}

    if extra_body:
        kwargs["extra_body"] = extra_body

    return kwargs
