from __future__ import annotations

import os

from dr_bottleneck.storage.mongo import bottleneck_mongodb_url

DEFAULT_PIPELINE_MONGODB_URL = "mongodb://localhost:27017/dr_queues"


def pipeline_mongodb_url() -> str:
    return os.environ.get("MONGODB_URL", DEFAULT_PIPELINE_MONGODB_URL)


def format_mongo_inspect_hints(
    run_id: str,
    *,
    include_metrics: bool = False,
) -> list[str]:
    bottleneck_url = bottleneck_mongodb_url()
    pipeline_url = pipeline_mongodb_url()
    commands = [
        (
            "Run report:"
            f"\nmongosh {bottleneck_url} "
            f'--eval \'db.run_reports.findOne({{run_id: "{run_id}"}})\''
        ),
    ]
    if include_metrics:
        commands.append(
            (
                "Metrics:"
                f"\nmongosh {bottleneck_url} "
                f'--eval \'db.run_metrics.findOne({{run_id: "{run_id}"}})\''
            ),
        )
    commands.append(
        (
            "Pipeline event count:"
            f"\nmongosh {pipeline_url} "
            f'--eval \'db.pipeline_events.countDocuments({{run_id: "{run_id}"}})\''
        ),
    )
    return commands
