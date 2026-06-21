from __future__ import annotations

from collections.abc import Callable

from dr_bottleneck.job import BottleneckJob
from dr_bottleneck.workflow.config import WorkflowStep

ProcessHandler = Callable[[BottleneckJob, WorkflowStep], BottleneckJob]

_REGISTRY: dict[str, ProcessHandler] = {}


def register(name: str) -> Callable[[ProcessHandler], ProcessHandler]:
    def decorator(handler: ProcessHandler) -> ProcessHandler:
        _REGISTRY[name] = handler
        return handler

    return decorator


def get_process_handler(name: str) -> ProcessHandler:
    if name not in _REGISTRY:
        msg = f"Unknown process handler: {name}"
        raise ValueError(msg)
    return _REGISTRY[name]
