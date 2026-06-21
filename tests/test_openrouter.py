import os
from unittest.mock import patch

import pytest

from dr_providers.openrouter import (
    MissingApiKeyError,
    build_completion_kwargs,
)


def test_build_completion_kwargs_prefixes_model() -> None:
    with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test-key"}):
        kwargs = build_completion_kwargs(
            "google/gemini-2.5-flash",
            [{"role": "user", "content": "hi"}],
            temperature=0.7,
            top_p=0.95,
        )

    assert kwargs["model"] == "openrouter/google/gemini-2.5-flash"
    assert kwargs["api_key"] == "test-key"
    assert "extra_body" not in kwargs


def test_build_completion_kwargs_reasoning_disabled() -> None:
    with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test-key"}):
        kwargs = build_completion_kwargs(
            "google/gemini-2.5-flash",
            [{"role": "user", "content": "hi"}],
            temperature=0.7,
            top_p=0.95,
            reasoning_disabled=True,
        )

    assert kwargs["extra_body"] == {"reasoning": {"effort": "none"}}


def test_build_completion_kwargs_effort_low() -> None:
    with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test-key"}):
        kwargs = build_completion_kwargs(
            "openai/gpt-5-nano",
            [{"role": "user", "content": "hi"}],
            temperature=0.7,
            top_p=0.95,
            effort="low",
        )

    assert kwargs["extra_body"] == {"reasoning": {"effort": "low"}}


def test_require_openrouter_api_key_raises() -> None:
    with (
        patch.dict(os.environ, {}, clear=True),
        pytest.raises(MissingApiKeyError, match="OPENROUTER_API_KEY"),
    ):
        build_completion_kwargs(
            "google/gemini-2.5-flash",
            [{"role": "user", "content": "hi"}],
            temperature=0.7,
            top_p=0.95,
        )
