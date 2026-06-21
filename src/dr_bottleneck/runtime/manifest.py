from __future__ import annotations

from pathlib import Path

from dr_queues.manifest import manifest_path
from pydantic import BaseModel


class BottleneckStageManifest(BaseModel):
    name: str
    step_index: int
    input_queue: str
    output_queue: str
    default_workers: int


class BottleneckRunManifest(BaseModel):
    run_id: str
    workflow_id: str
    workflow_path: str
    profiles_path: str
    expected_jobs: int
    queue_prefix: str
    stages: list[BottleneckStageManifest]


def write_bottleneck_manifest(path: Path, manifest: BottleneckRunManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )


def load_bottleneck_manifest(path: Path) -> BottleneckRunManifest:
    return BottleneckRunManifest.model_validate_json(
        path.read_text(encoding="utf-8"),
    )


__all__ = [
    "BottleneckRunManifest",
    "BottleneckStageManifest",
    "load_bottleneck_manifest",
    "manifest_path",
    "write_bottleneck_manifest",
]
