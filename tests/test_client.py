from __future__ import annotations

from unittest.mock import MagicMock, patch

from dr_providers import LlmResponse, Message, MessageRole

from dr_bottleneck.llm.client import assistant_text, call_llm


def test_call_llm_returns_record_shape() -> None:
    provider = MagicMock()
    provider.__enter__.return_value = provider
    provider.generate.return_value = LlmResponse(
        raw_json={"choices": [{"message": {"content": "Hello!"}}]},
        provider="openrouter",
        model="google/gemini-2.5-flash",
        latency_ms=42,
        text="Hello!",
        finish_reason="stop",
    )

    with (
        patch(
            "dr_bottleneck.llm.client.OpenRouterProvider",
            return_value=provider,
        ) as provider_cls,
        patch("dr_bottleneck.llm.client.append_llm_call") as append_call,
    ):
        record = call_llm(
            model="google/gemini-2.5-flash",
            messages=[Message(role=MessageRole.USER, content="hi")],
            profile="openrouter/google/gemini-2.5-flash/off/v1",
        )

    provider_cls.assert_called_once()
    provider.generate.assert_called_once()
    append_call.assert_called_once()
    assert record["profile"] == "openrouter/google/gemini-2.5-flash/off/v1"
    assert record["request"]["model"] == "google/gemini-2.5-flash"
    assert "api_key" not in record["request"]
    assert record["response"]["text"] == "Hello!"
    assert record["assistant_text"] == "Hello!"
    assert record["latency_ms"] == 42
    assert record["timestamp"]


def test_assistant_text_from_provider_response_dict() -> None:
    response = {"text": "Hi there"}
    assert assistant_text(response) == "Hi there"
