from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from dr_bottleneck.llm.client import assistant_text, call_llm


def test_call_llm_returns_record_shape() -> None:
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "Hello!"
    mock_response.model_dump.return_value = {
        "choices": [{"message": {"content": "Hello!"}}],
    }

    with (
        patch.dict(os.environ, {"OPENROUTER_API_KEY": "test-key"}),
        patch(
            "dr_bottleneck.llm.client.litellm.completion",
            return_value=mock_response,
        ) as mock_completion,
    ):
        record = call_llm(
            "google/gemini-2.5-flash",
            [{"role": "user", "content": "hi"}],
            temperature=0.7,
            top_p=0.95,
            reasoning_disabled=True,
            profile="openrouter/google/gemini-2.5-flash/off/v1",
        )

    mock_completion.assert_called_once()
    assert record["profile"] == "openrouter/google/gemini-2.5-flash/off/v1"
    assert record["request"]["model"] == "openrouter/google/gemini-2.5-flash"
    assert record["request"]["extra_body"] == {"reasoning": {"effort": "none"}}
    assert "api_key" not in record["request"]
    assert record["response"]["choices"][0]["message"]["content"] == "Hello!"
    assert isinstance(record["latency_ms"], int)
    assert record["timestamp"]


def test_assistant_text_from_dict() -> None:
    response = {"choices": [{"message": {"content": "Hi there"}}]}
    assert assistant_text(response) == "Hi there"
