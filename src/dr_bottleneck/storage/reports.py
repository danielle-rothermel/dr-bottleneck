from __future__ import annotations

from typing import Any

from dr_bottleneck.storage.mongo import (
    ensure_bottleneck_indexes,
    get_bottleneck_collection,
)

RUN_REPORTS_COLLECTION = "run_reports"


def write_run_report(report: dict[str, Any]) -> None:
    collection = get_bottleneck_collection(RUN_REPORTS_COLLECTION)
    ensure_bottleneck_indexes(collection, unique_run_id=True)
    collection.replace_one(
        {"run_id": report["run_id"]},
        report,
        upsert=True,
    )
