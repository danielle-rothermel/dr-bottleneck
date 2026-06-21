from __future__ import annotations

from unittest.mock import MagicMock, patch


@patch("dr_bottleneck.storage.llm_calls.get_bottleneck_collection")
def test_append_record_writes_to_mongo(mock_get_collection: MagicMock) -> None:
    from dr_bottleneck.llm import append_record

    collection = MagicMock()
    mock_get_collection.return_value = collection

    record = {
        "timestamp": "2026-06-21T12:00:00+00:00",
        "profile": "openrouter/google/gemini-2.5-flash/off/v1",
        "request": {"model": "openrouter/google/gemini-2.5-flash"},
        "response": {"choices": []},
        "latency_ms": 42,
    }

    append_record(None, record)

    collection.insert_one.assert_called_once()
    document = collection.insert_one.call_args.args[0]
    assert document["model"] == "openrouter/google/gemini-2.5-flash"
    assert document["latency_ms"] == 42
