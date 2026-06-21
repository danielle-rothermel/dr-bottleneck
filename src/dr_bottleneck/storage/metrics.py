from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from dr_bottleneck.storage.mongo import (
    ensure_bottleneck_indexes,
    get_bottleneck_collection,
    replace_prepared_document,
)

RUN_METRICS_COLLECTION = "run_metrics"


def write_run_metrics(
    *,
    run_id: str,
    rows: list[dict[str, Any]],
    summary: dict[str, Any],
) -> None:
    collection = get_bottleneck_collection(RUN_METRICS_COLLECTION)
    ensure_bottleneck_indexes(collection, unique_run_id=True)
    document = {
        "run_id": run_id,
        "created_at": datetime.now(tz=UTC).isoformat(),
        "rows": rows,
        "summary": summary,
    }
    replace_prepared_document(
        collection,
        filter_doc={"run_id": run_id},
        document=document,
        upsert=True,
    )
