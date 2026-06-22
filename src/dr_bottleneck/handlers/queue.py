from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from dr_queues import HandlerRegistry, JobEnvelope, MongoRunStore

from dr_bottleneck.workflow.engine import LLM_HANDLER_KEY, Workflow

WORKFLOW_CONFIG_METADATA_KEY = "workflow_config"
PROFILES_PATH_METADATA_KEY = "profiles_path"

registry = HandlerRegistry()


@lru_cache(maxsize=64)
def _workflow_for_run(run_id: str) -> Workflow:
    store = MongoRunStore()
    try:
        record = store.get_run_record(run_id)
    finally:
        store.close()
    workflow_config = record.metadata.get(WORKFLOW_CONFIG_METADATA_KEY)
    if not isinstance(workflow_config, dict):
        msg = f"Run {run_id!r} has no workflow_config metadata."
        raise ValueError(msg)
    profiles_path = record.metadata.get(PROFILES_PATH_METADATA_KEY)
    if not isinstance(profiles_path, str) or not profiles_path:
        msg = f"Run {run_id!r} has no profiles_path metadata."
        raise ValueError(msg)
    return Workflow.from_raw_config(
        workflow_config,
        profiles_path=Path(profiles_path),
    )


@registry.register(LLM_HANDLER_KEY)
def run_llm_step(job: JobEnvelope) -> JobEnvelope:
    return _workflow_for_run(job.run_id).run_llm_step(job)


__all__ = ["registry", "run_llm_step"]
