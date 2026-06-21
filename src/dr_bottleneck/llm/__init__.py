from __future__ import annotations

from typing import Any

from dr_bottleneck.llm.client import assistant_text, call_llm
from dr_bottleneck.llm.openrouter import (
    Message,
    MissingApiKeyError,
    build_completion_kwargs,
)
from dr_bottleneck.storage.llm_calls import append_llm_call


def append_record(_path: object | None, record: dict[str, Any]) -> None:
    append_llm_call(record)


__all__ = [
    "Message",
    "MissingApiKeyError",
    "append_record",
    "assistant_text",
    "build_completion_kwargs",
    "call_llm",
]
