from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from dr_bottleneck.storage.mongo import (
    ensure_bottleneck_indexes,
    get_bottleneck_collection,
)

LLM_CALLS_COLLECTION = "llm_calls"


def append_llm_call(record: dict[str, Any]) -> None:
    collection = get_bottleneck_collection(LLM_CALLS_COLLECTION)
    ensure_bottleneck_indexes(collection, unique_run_id=False)
    document = {
        "call_id": record.get("call_id", str(uuid4())),
        "timestamp": record.get(
            "timestamp",
            datetime.now(tz=UTC).isoformat(),
        ),
        "model": record.get("request", {}).get("model", ""),
        "profile": record.get("profile"),
        "run_id": record.get("run_id"),
        "job_id": record.get("job_id"),
        "request": record.get("request", {}),
        "response": record.get("response", {}),
        "latency_ms": record.get("latency_ms", 0),
    }
    collection.insert_one(document)
